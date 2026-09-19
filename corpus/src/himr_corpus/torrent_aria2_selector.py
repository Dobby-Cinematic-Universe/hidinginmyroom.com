"""Build a path-free, offline aria2 selector receipt from a finalized torrent plan.

This module never imports or invokes aria2, opens a socket, joins a swarm, reads a
torrent payload piece, or writes catalogue state.  It verifies the planner's
canonical digest against the exact local torrent and performs the sole permitted
index conversion: corpus zero-based index + 1 = aria2 one-based index.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .importers import canonical_json, sha256_bytes
from .torrent_bracket_reconciler import (
    TorrentBracketReconciliationError,
    _parse_torrent,
    _raw_path_sha256,
    _stable_file,
    _strict_json_bytes,
)
from .torrent_selective_planner import PLAN_KIND, PLANNER_VERSION


SELECTOR_PLANNER_VERSION = "torrent_aria2_selector_v1"
MAX_PLAN_BYTES = 64 * 1024 * 1024
MAX_ARIA2_FILE_INDEX = 1_048_576
ARIA2_VERSION = "1.37.0"
ARIA2_FEDORA_NEVRA = "aria2-1.37.0-9.fc44.x86_64"
ARIA2_RPM_SHA256 = "72381bff2df034ea74453de5c5700a3080d9f7a25fadec16a97efe2da69344ac"
ARIA2_EXECUTABLE_SHA256 = (
    "02f07c5fc1764a71d118e79fbc95e0621aeab43e6fd4f1646b12c85494f55ffa"
)
ARIA2_RPM_SIGNER_FINGERPRINT = "36F612DCF27F7D1A48A835E4DBFCF71C6D9F90A6"

PLAN_KEYS = {
    "schema_version",
    "plan_kind",
    "planner_version",
    "inputs",
    "catalog_binding",
    "archive_binding",
    "evidence_binding_sha256",
    "coverage",
    "availability_probe_request",
    "availability_probe",
    "probe_candidates",
    "malformed_manual_review",
    "selected_files",
    "selected_torrent_file_indices",
    "statistics",
    "policy",
    "plan_id",
    "plan_sha256",
}


class TorrentAria2SelectorError(ValueError):
    """Raised when a client selector cannot be proven from the exact inputs."""


def _fail(message: str) -> None:
    raise TorrentAria2SelectorError(message)


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{label} must be an object")
    return value


def _nonnegative_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(f"{label} must be a non-negative integer")
    return value


def _bounded_text(value: object, label: str, maximum: int = 16384) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        _fail(f"{label} must be bounded non-empty text")
    return value


def _sha256(value: object, label: str) -> str:
    text = _bounded_text(value, label, 64)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        _fail(f"{label} must be lowercase SHA-256")
    return text


def _load_inputs(plan_path: Path, torrent_path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        _resolved_plan, plan_body = _stable_file(plan_path, MAX_PLAN_BYTES, "aria2 plan")
        _resolved_torrent, torrent_body = _stable_file(
            torrent_path, 32 * 1024 * 1024, "aria2 torrent manifest"
        )
        plan = _strict_json_bytes(plan_body, "aria2 plan")
    except TorrentBracketReconciliationError as error:
        raise TorrentAria2SelectorError(str(error)) from error
    return plan, torrent_body


def _verify_plan_envelope(plan: dict[str, Any]) -> None:
    if set(plan) != PLAN_KEYS:
        _fail("finalized plan has an unknown top-level shape")
    if plan.get("schema_version") != 1:
        _fail("finalized plan schema version differs")
    if plan.get("plan_kind") != PLAN_KIND or plan.get("planner_version") != PLANNER_VERSION:
        _fail("finalized plan kind or planner version differs")

    claimed_sha256 = _sha256(plan.get("plan_sha256"), "finalized plan SHA-256")
    core = {key: value for key, value in plan.items() if key not in {"plan_id", "plan_sha256"}}
    actual_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    if actual_sha256 != claimed_sha256:
        _fail("finalized plan canonical SHA-256 differs")
    if plan.get("plan_id") != f"tslp_{claimed_sha256[:32]}":
        _fail("finalized plan ID differs from its canonical SHA-256")

    if not isinstance(plan.get("availability_probe"), dict):
        _fail("a complete bound availability probe is required before client selection")
    _sha256(plan["availability_probe"].get("probe_sha256"), "availability probe SHA-256")
    policy = _object(plan.get("policy"), "finalized plan policy")
    required_policy = {
        "read_only_plan": True,
        "torrent_client_invoked": False,
        "torrent_swarm_joined": False,
        "payload_downloaded_or_read": False,
        "operator_review_required_before_client_use": True,
        "selective_file_index_client_required": True,
        "publication_authority": False,
    }
    if any(policy.get(key) is not expected for key, expected in required_policy.items()):
        _fail("finalized plan policy does not preserve the client review boundary")


def _piece_span_capacity(
    torrent: dict[str, Any], selected_indices: list[int], selected_bytes: int
) -> tuple[int, int]:
    selected = set(selected_indices)
    piece_length = torrent["piece_length_bytes"]
    total_bytes = torrent["total_bytes"]
    offset = 0
    required_piece_indices: set[int] = set()
    for row in torrent["files"]:
        length = row["byte_count"]
        if row["file_index"] in selected:
            if length < 1:
                _fail("a selected torrent file is empty")
            first_piece = offset // piece_length
            last_piece = (offset + length - 1) // piece_length
            required_piece_indices.update(range(first_piece, last_piece + 1))
        offset += length
    piece_span_bytes = sum(
        min(piece_length, total_bytes - piece_index * piece_length)
        for piece_index in required_piece_indices
    )
    if piece_span_bytes < selected_bytes:
        _fail("selected payload bytes exceed their torrent piece spans")
    return piece_span_bytes, piece_span_bytes - selected_bytes


def build_aria2_selector_receipt(plan_path: Path, torrent_path: Path) -> dict[str, Any]:
    """Verify a final full plan and return a deterministic, path-free selector receipt."""

    plan, torrent_body = _load_inputs(Path(plan_path), Path(torrent_path))
    _verify_plan_envelope(plan)
    try:
        torrent = _parse_torrent(torrent_body)
    except TorrentBracketReconciliationError as error:
        raise TorrentAria2SelectorError(str(error)) from error

    inputs = _object(plan.get("inputs"), "finalized plan inputs")
    if _sha256(inputs.get("torrent_sha256"), "plan torrent SHA-256") != torrent[
        "torrent_sha256"
    ]:
        _fail("finalized plan is bound to different torrent bytes")
    if _nonnegative_integer(inputs.get("torrent_byte_count"), "plan torrent byte count") != len(
        torrent_body
    ):
        _fail("finalized plan torrent byte count differs")
    if inputs.get("info_hash_sha1") != torrent["info_hash_sha1"]:
        _fail("finalized plan torrent info hash differs")

    raw_indices = plan.get("selected_torrent_file_indices")
    if not isinstance(raw_indices, list):
        _fail("finalized plan selected indices must be an array")
    indices = [
        _nonnegative_integer(value, f"selected index {position}")
        for position, value in enumerate(raw_indices)
    ]
    if not indices:
        _fail("finalized plan selected indices are empty; no client invocation is authorized")
    if indices != sorted(set(indices)):
        _fail("finalized plan selected indices must be sorted and unique")
    if indices[-1] >= torrent["file_count"]:
        _fail("finalized plan selects an out-of-range torrent file")

    selected_files = plan.get("selected_files")
    if not isinstance(selected_files, list) or len(selected_files) != len(indices):
        _fail("finalized plan selected-file rows disagree with selected indices")
    selected_row_indices = [
        row.get("torrent_file_index")
        for row in selected_files
        if isinstance(row, dict)
    ]
    if selected_row_indices != indices:
        _fail("finalized plan selected-file row order or indices differ")

    torrent_files = torrent["files"]
    probe_candidates = plan.get("probe_candidates")
    if not isinstance(probe_candidates, list):
        _fail("finalized plan probe candidates must be an array")
    candidates_by_index: dict[int, dict[str, Any]] = {}
    for candidate in probe_candidates:
        if not isinstance(candidate, dict):
            _fail("finalized plan has a malformed probe candidate")
        index = candidate.get("smallest_rendition_file_index")
        if isinstance(index, bool) or not isinstance(index, int):
            _fail("finalized plan has a malformed probe-candidate index")
        if index in candidates_by_index:
            _fail("finalized plan has duplicate probe-candidate indices")
        candidates_by_index[index] = candidate

    malformed = plan.get("malformed_manual_review")
    if not isinstance(malformed, list):
        _fail("finalized plan malformed-path review lane must be an array")
    malformed_indices = {
        row.get("torrent_file_index")
        for row in malformed
        if isinstance(row, dict)
    }
    if malformed_indices.intersection(indices):
        _fail("finalized plan selects a malformed-path review file")

    selected_bytes = 0
    for position, (index, selected_row) in enumerate(zip(indices, selected_files, strict=True)):
        row = _object(selected_row, f"selected file {position}")
        manifest = torrent_files[index]
        byte_count = _nonnegative_integer(row.get("byte_count"), "selected file byte count")
        if byte_count != manifest["byte_count"]:
            _fail("selected file byte count differs from the torrent manifest")
        if row.get("manifest_path") != manifest["manifest_path"]:
            _fail("selected file path differs from the torrent manifest")
        if row.get("manifest_path_sha256") != _raw_path_sha256(manifest["raw_components"]):
            _fail("selected file raw-path SHA-256 differs from the torrent manifest")
        candidate = candidates_by_index.get(index)
        if (
            candidate is None
            or candidate.get("availability_state") != "unavailable"
            or candidate.get("availability_evidence_code")
            not in {"private", "removed", "video_unavailable"}
            or candidate.get("availability_evidence_code")
            != row.get("availability_evidence_code")
        ):
            _fail("selected file lacks matching explicit-unavailability evidence")
        selected_bytes += byte_count

    statistics = _object(plan.get("statistics"), "finalized plan statistics")
    if _nonnegative_integer(statistics.get("selected_file_count"), "selected file count") != len(
        indices
    ):
        _fail("finalized plan selected-file count differs")
    if _nonnegative_integer(
        statistics.get("selected_payload_bytes"), "selected payload bytes"
    ) != selected_bytes:
        _fail("finalized plan selected-payload byte count differs")

    aria2_indices = [index + 1 for index in indices]
    if aria2_indices[-1] > MAX_ARIA2_FILE_INDEX:
        _fail("converted selector exceeds aria2's supported file-index range")
    selector = ",".join(str(index) for index in aria2_indices)
    piece_span_bytes, boundary_bytes = _piece_span_capacity(torrent, indices, selected_bytes)

    core = {
        "schema_version": 1,
        "receipt_kind": "aria2_selective_file_selector_receipt",
        "planner_version": SELECTOR_PLANNER_VERSION,
        "source_plan": {
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "availability_probe_sha256": plan["availability_probe"]["probe_sha256"],
        },
        "torrent": {
            "torrent_sha256": torrent["torrent_sha256"],
            "info_hash_sha1": torrent["info_hash_sha1"],
            "file_count": torrent["file_count"],
            "piece_length_bytes": torrent["piece_length_bytes"],
        },
        "client_pin": {
            "client": "aria2c",
            "version": ARIA2_VERSION,
            "fedora_nevra": ARIA2_FEDORA_NEVRA,
            "rpm_sha256": ARIA2_RPM_SHA256,
            "rpm_signer_fingerprint": ARIA2_RPM_SIGNER_FINGERPRINT,
            "executable_sha256": ARIA2_EXECUTABLE_SHA256,
        },
        "selection": {
            "plan_index_base": 0,
            "aria2_index_base": 1,
            "conversion": "aria2_index=torrent_file_index+1",
            "zero_based_file_indices": indices,
            "aria2_one_based_file_indices": aria2_indices,
            "aria2_select_file_value": selector,
            "selected_file_count": len(indices),
            "selected_payload_bytes": selected_bytes,
            "selected_piece_span_bytes": piece_span_bytes,
            "boundary_piece_bytes_upper_bound": boundary_bytes,
        },
        "policy": {
            "read_only_receipt": True,
            "network_actions_performed": False,
            "torrent_client_invoked": False,
            "torrent_swarm_joined": False,
            "payload_downloaded_or_read": False,
            "selector_must_be_present_at_client_start": True,
            "operator_review_required": True,
            "publication_authority": False,
        },
    }
    receipt_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "receipt_id": f"a2sr_{receipt_sha256[:32]}",
        "receipt_sha256": receipt_sha256,
    }


def publish_private_aria2_selector_receipt(
    receipt: dict[str, Any], output_path: Path
) -> dict[str, Any]:
    """Atomically publish one owner-read-only selector receipt without overwrite."""

    requested = Path(output_path)
    if not requested.is_absolute() or requested.name in {"", ".", ".."}:
        _fail("private selector-receipt output must be an absolute file path")
    try:
        resolved_parent = requested.parent.resolve(strict=True)
    except OSError as error:
        raise TorrentAria2SelectorError(
            "private selector-receipt output parent does not exist"
        ) from error
    if resolved_parent != requested.parent or not resolved_parent.is_dir():
        _fail("private selector-receipt output parent must be a real directory")
    destination = resolved_parent / requested.name
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    except OSError as error:
        raise TorrentAria2SelectorError(
            "private selector-receipt output cannot be inspected"
        ) from error
    else:
        _fail("private selector-receipt output already exists")

    body = (
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=resolved_parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written < 1:
                _fail("private selector-receipt output write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as error:
            raise TorrentAria2SelectorError(
                "private selector-receipt output already exists"
            ) from error
        directory_descriptor = os.open(
            resolved_parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)

    return {
        "valid": True,
        "selector_receipt_written": True,
        "receipt_id": receipt["receipt_id"],
        "receipt_sha256": receipt["receipt_sha256"],
        "output_byte_count": len(body),
        "output_sha256": sha256_bytes(body),
        "output_mode": "0400",
        "path_disclosed": False,
        "network_actions_performed": False,
        "torrent_client_invoked": False,
        "torrent_swarm_joined": False,
        "payload_downloaded_or_read": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a finalized selective-torrent plan and print a path-free aria2 "
            "index-conversion receipt; never invokes a torrent client"
        )
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--torrent", required=True, type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "atomically write the private receipt to this absolute new path and "
            "print only a path-free write summary"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        receipt = build_aria2_selector_receipt(args.plan, args.torrent)
    except TorrentAria2SelectorError as error:
        raise SystemExit(f"aria2 selector refused: {error}") from error
    if args.output:
        receipt = publish_private_aria2_selector_receipt(receipt, args.output)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
