#!/usr/bin/env python3
"""Seal the exact preprocess-receipt handoff to the portable GPU v5 lane.

This materializer deliberately delegates receipt, result, normalized-audio, and
private-handling replay to :mod:`preprocess_asr_queue_v03`.  It adds only the
restart-portable root/profile binding and the resource disposition needed by a
future GPU v5 batch materializer.  A handoff queue cannot execute code, import
results, mutate a catalogue, publish, identify people, or authorize chunking.
"""

from __future__ import annotations

import argparse
import copy
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import uuid
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

try:
    from . import preprocess_asr_queue_v03 as RECEIPT_REPLAY
    from .gpu import portable_root as PORTABLE_ROOT
    from .gpu import production_profile_v2 as PROFILE_V2
except ImportError:  # pragma: no cover - direct script execution
    import preprocess_asr_queue_v03 as RECEIPT_REPLAY  # type: ignore[no-redef]
    from gpu import portable_root as PORTABLE_ROOT  # type: ignore[no-redef]
    from gpu import production_profile_v2 as PROFILE_V2  # type: ignore[no-redef]


KIND = "himr_preprocess_gpu_asr_queue"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
MATERIALIZER = "himr-preprocess-gpu-asr-queue"
QUEUE_ID_PREFIX = "gpuasrqueue_"
MEMBER_ID_PREFIX = "gpuasrmember_"
SKIP_ID_PREFIX = "gpuasrskip_"
EXPECTED_ROOT_TIER = "hot_main_drive"

MAX_ITEMS = 128
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_PROFILE_BYTES = 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
QUEUE_ID_RE = re.compile(r"^gpuasrqueue_[0-9a-f]{32}$")

AUDIO_INPUT_MODES = frozenset({0o400, 0o444})
READ_ONLY_LINEAGE_MODES = frozenset({0o400, 0o440, 0o444})
SAFETY = {
    "visibility": "private",
    "network_access": False,
    "execution_authority": "none",
    "gpu_execution_authority": "none",
    "chunking_authority": "none",
    "result_import_authority": "none",
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
    "input_selection": "exact_completed_preprocess_receipts_only",
}


class QueueError(RuntimeError):
    """The handoff queue could not be constructed or replayed exactly."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise QueueError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise QueueError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise QueueError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise QueueError(f"{label} must be a lowercase SHA-256")
    return value


def _absolute_existing(value: str | Path, label: str) -> Path:
    try:
        return RECEIPT_REPLAY._absolute_path(value, label, existing=True)
    except RECEIPT_REPLAY.QueueError as error:
        raise QueueError(str(error)) from error


def _relative_to_root(
    root: PORTABLE_ROOT.RetainedRoot,
    value: str | Path,
    label: str,
) -> PurePosixPath:
    path = _absolute_existing(value, label)
    try:
        relative = path.relative_to(root.path)
    except ValueError as error:
        raise QueueError(f"{label} is outside the registered portable root") from error
    if not relative.parts:
        raise QueueError(f"{label} may not be the registered root itself")
    try:
        return PORTABLE_ROOT.normalized_relative_path(relative.as_posix(), label)
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(str(error)) from error


def _path_relationship_forbidden(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )


def _file_flags(*, writable: bool = False, create: bool = False) -> int:
    flags = os.O_RDWR if writable else os.O_RDONLY
    if create:
        flags |= os.O_CREAT
    return (
        flags
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _live_directory_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
    )


@contextmanager
def _retained_directory(
    root: PORTABLE_ROOT.RetainedRoot,
    path: Path,
    label: str,
    *,
    allowed_final_modes: frozenset[int] | None = None,
) -> Iterator[tuple[int, PurePosixPath, os.stat_result]]:
    """Retain one no-follow descendant directory for the current operation."""

    relative = _relative_to_root(root, path, label)
    root.verify()
    owner_uid = root.registration["owner"]["uid"]
    parent_fd = root.descriptor
    descriptors: list[int] = []
    final_stat: os.stat_result | None = None
    try:
        for component in relative.parts:
            try:
                inspected = os.stat(
                    component, dir_fd=parent_fd, follow_symlinks=False
                )
            except OSError as error:
                raise QueueError(
                    f"cannot inspect {label} component {component!r}: {error}"
                ) from error
            if (
                stat.S_ISLNK(inspected.st_mode)
                or not stat.S_ISDIR(inspected.st_mode)
                or inspected.st_uid != owner_uid
                or stat.S_IMODE(inspected.st_mode) & 0o022
            ):
                raise QueueError(
                    f"{label} has a symlink, unsafe mode, or unexpected owner"
                )
            try:
                descriptor = os.open(
                    component, _directory_flags(), dir_fd=parent_fd
                )
            except OSError as error:
                raise QueueError(
                    f"cannot retain {label} component {component!r}: {error}"
                ) from error
            descriptors.append(descriptor)
            opened = os.fstat(descriptor)
            if _live_directory_identity(opened) != _live_directory_identity(
                inspected
            ):
                raise QueueError(f"{label} changed while it was retained")
            try:
                PORTABLE_ROOT.require_live_same_filesystem(root, descriptor, label)
            except PORTABLE_ROOT.PortableRootError as error:
                raise QueueError(str(error)) from error
            parent_fd = descriptor
            final_stat = opened
        if final_stat is None:
            raise QueueError(f"{label} may not be the registered root")
        final_mode = stat.S_IMODE(final_stat.st_mode)
        if allowed_final_modes is not None and final_mode not in allowed_final_modes:
            raise QueueError(
                f"{label} mode {final_mode:04o} is outside "
                f"{sorted(f'{mode:04o}' for mode in allowed_final_modes)}"
            )
        yield descriptors[-1], relative, final_stat
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _registration_reference(
    registration: dict[str, Any],
    document_path: Path,
    document_sha256: str,
) -> dict[str, Any]:
    document_uid, document_mode = _control_document_policy(
        document_path, "portable root registration document"
    )
    return {
        "document_path": str(document_path),
        "document_sha256": document_sha256,
        "document_uid": document_uid,
        "document_mode": f"{document_mode:04o}",
        "registration_id": registration["registration_id"],
        "identity_sha256": registration["identity_sha256"],
        "root_id": registration["root_id"],
        "tier": registration["tier"],
        "path": registration["path"],
        "filesystem": copy.deepcopy(registration["filesystem"]),
        "owner": copy.deepcopy(registration["owner"]),
    }


def _control_document_policy(path: Path, label: str) -> tuple[int, int]:
    try:
        observed = path.lstat()
    except OSError as error:
        raise QueueError(f"cannot inspect {label}: {error}") from error
    owner_mode = (observed.st_uid, stat.S_IMODE(observed.st_mode))
    allowed = {(os.geteuid(), 0o400), (0, 0o444)}
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or owner_mode not in allowed
    ):
        raise QueueError(
            f"{label} must be current-user mode-0400 or root-owned mode-0444"
        )
    return owner_mode


def _load_registration(
    document_path_value: str | Path,
    document_sha256_value: str,
) -> tuple[dict[str, Any], Path, str]:
    document_path = PORTABLE_ROOT.normalized_absolute_path(
        document_path_value, "portable root registration document"
    )
    document_sha256 = _sha256(
        document_sha256_value, "portable root registration document SHA-256"
    )
    document_uid, document_mode = _control_document_policy(
        document_path, "portable root registration document"
    )
    try:
        registration = PORTABLE_ROOT.load_registration(
            document_path,
            document_sha256,
            expected_tier=EXPECTED_ROOT_TIER,
            expected_document_uid=document_uid,
            expected_document_mode=document_mode,
        )
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(f"portable root registration failed: {error}") from error
    return registration, document_path, document_sha256


def _load_profile(
    root: PORTABLE_ROOT.RetainedRoot,
    profile_path_value: str | Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    profile_path = _absolute_existing(profile_path_value, "production profile")
    relative = _relative_to_root(root, profile_path, "production profile")
    body: bytes | None = None
    document_uid: int | None = None
    document_mode: int | None = None
    policies = [(os.geteuid(), 0o400), (0, 0o444)]
    for owner_uid, mode in dict.fromkeys(policies):
        try:
            with root.open_file(
                relative,
                label="production profile",
                allowed_modes={mode},
                owner_uid=owner_uid,
                single_link=True,
            ) as retained:
                body = retained.read_bytes(MAX_PROFILE_BYTES)
                observed = os.fstat(retained.descriptor)
                document_uid = observed.st_uid
                document_mode = stat.S_IMODE(observed.st_mode)
            break
        except PORTABLE_ROOT.PortableRootError:
            continue
    if body is None or document_uid is None or document_mode is None:
        raise QueueError(
            "production profile retention failed: expected current-user mode-0400 "
            "or root-owned mode-0444"
        )
    try:
        raw = PORTABLE_ROOT.parse_json_bytes(body, "production profile")
        profile = PROFILE_V2.validate_profile(raw)
    except (PORTABLE_ROOT.PortableRootError, PROFILE_V2.ProfileError) as error:
        raise QueueError(f"production profile validation failed: {error}") from error
    if body != PROFILE_V2.canonical_bytes(profile):
        raise QueueError("production profile file is not canonical JSON")
    reference = {
        "path": str(profile_path),
        "relative_path": relative.as_posix(),
        "physical_sha256": sha256_bytes(body),
        "byte_count": len(body),
        "document_uid": document_uid,
        "document_mode": f"{document_mode:04o}",
        "profile_id": profile["profile_id"],
        "identity_sha256": profile["identity_sha256"],
    }
    return profile, reference


def _receipt_reference(value: Any, label: str) -> dict[str, Any]:
    item = _exact(
        value,
        label,
        {
            "path",
            "uri",
            "physical_sha256",
            "receipt_id",
            "receipt_sha256",
            "ordinal",
        },
    )
    ordinal = _integer(item["ordinal"], f"{label} ordinal", 1, MAX_ITEMS)
    path = _absolute_existing(item["path"], f"{label} path")
    if item["uri"] != path.as_uri():
        raise QueueError(f"{label} URI differs from its exact path")
    for key in ("physical_sha256", "receipt_sha256"):
        _sha256(item[key], f"{label} {key}")
    if not isinstance(item["receipt_id"], str) or not item["receipt_id"]:
        raise QueueError(f"{label} receipt_id is invalid")
    return {**copy.deepcopy(item), "ordinal": ordinal, "path": str(path)}


def _skip_handling_descriptor(
    skipped: dict[str, Any],
    control_entry: dict[str, Any],
) -> dict[str, Any]:
    """Recover a skipped receipt boundary without reimplementing its replay."""

    receipt_ref = _receipt_reference(skipped["receipt"], "skipped receipt")
    receipt_path = Path(receipt_ref["path"])
    try:
        observed_path, body = RECEIPT_REPLAY._stable_readonly(
            receipt_path,
            RECEIPT_REPLAY.preprocess_batch.MAX_RECEIPT_BYTES,
            "skipped preprocess receipt handling replay",
        )
        raw = RECEIPT_REPLAY.parse_json(body, "skipped preprocess receipt")
    except RECEIPT_REPLAY.QueueError as error:
        raise QueueError(str(error)) from error
    if (
        observed_path != receipt_path
        or sha256_bytes(body) != receipt_ref["physical_sha256"]
        or not isinstance(raw, dict)
        or raw.get("ordinal") != receipt_ref["ordinal"]
        or raw.get("receipt_id") != receipt_ref["receipt_id"]
        or raw.get("receipt_sha256") != receipt_ref["receipt_sha256"]
        or raw.get("entry_id") != control_entry["entry_id"]
        or "handling_boundary" not in raw
    ):
        raise QueueError("skipped receipt changed after exact v03 receipt replay")
    try:
        return RECEIPT_REPLAY._handling_descriptor(
            boundary=raw["handling_boundary"],
            control_entry=control_entry,
        )
    except RECEIPT_REPLAY.QueueError as error:
        raise QueueError(str(error)) from error


def _handling_by_ordinal(
    origin: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    control = origin.get("handling_control")
    supplied = {
        item["evidence"]["receipt"]["ordinal"]: item["handling"]
        for item in items
        if "handling" in item
    }
    if control is None:
        if supplied:
            raise QueueError("replayed items invent private handling without control")
        return {}
    try:
        validated = RECEIPT_REPLAY._validated_handling_control(control)
    except RECEIPT_REPLAY.QueueError as error:
        raise QueueError(str(error)) from error
    rows = {row["ordinal"]: row for row in validated["entries"]}
    descriptors: dict[int, dict[str, Any]] = {}
    for ordinal, descriptor in supplied.items():
        control_entry = rows.get(ordinal)
        if control_entry is None:
            raise QueueError("eligible item handling is absent from preprocess control")
        try:
            replayed = RECEIPT_REPLAY._handling_descriptor(
                boundary=descriptor["handling_boundary"],
                control_entry=control_entry,
            )
        except (KeyError, RECEIPT_REPLAY.QueueError) as error:
            raise QueueError("eligible private handling replay failed") from error
        if descriptor != replayed:
            raise QueueError("eligible private handling differs from exact v03 replay")
        descriptors[ordinal] = copy.deepcopy(replayed)
    for skipped in origin.get("ineligible_receipts", []):
        ordinal = _integer(
            skipped.get("ordinal"), "skipped receipt ordinal", 1, MAX_ITEMS
        )
        if ordinal in rows:
            descriptors[ordinal] = _skip_handling_descriptor(
                skipped, rows[ordinal]
            )
    if set(descriptors) != set(rows):
        missing = sorted(set(rows) - set(descriptors))
        raise QueueError(
            f"queue drops private handling descriptors for ordinals {missing}"
        )
    return descriptors


def _lineage(
    origin: dict[str, Any],
    receipt: dict[str, Any],
    result: dict[str, Any],
    source_media: dict[str, Any],
) -> dict[str, Any]:
    return {
        "preprocess_bundle": copy.deepcopy(origin["preprocess_bundle"]),
        "preprocess_receipt_state": {
            "state_root": origin["state_root"],
            "receipt_count": origin["receipt_count"],
            "receipt_state_sha256": origin["receipt_state_sha256"],
            "receipt_refs_sha256": origin["receipt_refs_sha256"],
        },
        "receipt": copy.deepcopy(receipt),
        "preprocess_result": copy.deepcopy(result),
        "source_media": copy.deepcopy(source_media),
    }


def _audio_format(audio: dict[str, Any]) -> dict[str, Any]:
    try:
        probe = audio["normalized_probe"]
        format_row = probe["format"]
        stream = probe["streams"][0]
        stream_audio = stream["audio"]
        result = {
            "format_name": format_row["format_name"],
            "codec_name": stream["codec_name"],
            "sample_rate_hz": stream_audio["sample_rate_hz"],
            "sample_format": stream_audio["sample_format"],
            "channels": stream_audio["channels"],
            "channel_layout": stream_audio["channel_layout"],
            "start_ms": format_row["start_ms"],
        }
    except (KeyError, IndexError, TypeError) as error:
        raise QueueError("replayed normalized audio format is incomplete") from error
    expected = {
        "format_name": "flac",
        "codec_name": "flac",
        "sample_rate_hz": 16_000,
        "sample_format": "s16",
        "channels": 1,
        "channel_layout": "mono",
        "start_ms": 0,
    }
    if result != expected:
        raise QueueError("replayed audio format differs from the GPU input contract")
    return result


def _resource_disposition(
    audio: dict[str, Any], profile: dict[str, Any]
) -> dict[str, Any]:
    limits = profile["item_limits"]
    byte_limit = limits["maximum_audio_bytes"]
    second_limit = Decimal(str(limits["maximum_audio_seconds"]))
    duration_limit_ms = second_limit * 1_000
    reasons: list[str] = []
    if audio["byte_count"] > byte_limit:
        reasons.append("maximum_audio_bytes_exceeded")
    if Decimal(audio["duration_ms"]) > duration_limit_ms:
        reasons.append("maximum_audio_duration_exceeded")
    return {
        "state": "requires_chunking" if reasons else "ready",
        "reasons": reasons,
        "evaluated_against": {
            "profile_id": profile["profile_id"],
            "profile_identity_sha256": profile["identity_sha256"],
            "maximum_audio_bytes": byte_limit,
            "maximum_audio_seconds": limits["maximum_audio_seconds"],
        },
        "chunk_plan": None,
    }


def _assert_unique(
    rows: list[dict[str, Any]], path: tuple[str, ...], label: str
) -> None:
    values: list[Any] = []
    for row in rows:
        value: Any = row
        for key in path:
            value = value[key]
        values.append(value)
    if len(set(values)) != len(values):
        raise QueueError(f"duplicate {label} is forbidden")


def _manifest_from_replay(
    *,
    origin_value: dict[str, Any],
    items_value: list[dict[str, Any]],
    handling_by_ordinal: dict[int, dict[str, Any]],
    profile_value: dict[str, Any],
    profile_reference: dict[str, Any],
    registration_reference: dict[str, Any],
    queue_root: Path,
    audio_sealed_modes: dict[int, str],
) -> dict[str, Any]:
    """Build the pure deterministic queue after v03 replay has completed."""

    origin = copy.deepcopy(origin_value)
    items = copy.deepcopy(items_value)
    try:
        profile = PROFILE_V2.validate_profile(profile_value)
    except PROFILE_V2.ProfileError as error:
        raise QueueError(f"production profile is invalid: {error}") from error
    if origin.get("mode") != "sealed_preprocess_receipts":
        raise QueueError("GPU handoff accepts only sealed preprocess receipts")
    receipt_count = _integer(
        origin.get("receipt_count"), "origin receipt_count", 1, MAX_ITEMS
    )
    skipped_rows = origin.get("ineligible_receipts", [])
    if not isinstance(skipped_rows, list):
        raise QueueError("origin ineligible_receipts must be a list")
    if not isinstance(audio_sealed_modes, dict) or any(
        isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or mode not in {"0400", "0444"}
        for ordinal, mode in audio_sealed_modes.items()
    ):
        raise QueueError("audio sealed-mode observations are invalid")

    records: list[dict[str, Any]] = []
    receipt_rows: list[dict[str, Any]] = []
    for item in items:
        try:
            receipt = _receipt_reference(
                item["evidence"]["receipt"], "eligible preprocess receipt"
            )
            result = item["result"]
            source_media = item["source_media"]
            audio = item["audio"]
        except (KeyError, TypeError) as error:
            raise QueueError("v03 eligible item is incomplete") from error
        ordinal = receipt["ordinal"]
        if ordinal not in audio_sealed_modes:
            raise QueueError("eligible audio lacks a retained sealed-mode observation")
        byte_count = _integer(
            audio.get("byte_count"), "audio byte_count", 1, 2**63 - 1
        )
        duration_ms = _integer(
            audio.get("duration_ms"), "audio duration_ms", 1, 2**63 - 1
        )
        digest = _sha256(audio.get("sha256"), "audio SHA-256")
        path = _absolute_existing(audio.get("path"), "audio path")
        artifact_id = audio.get("artifact_id")
        processing_run_id = audio.get("processing_run_id")
        if not all(
            isinstance(value, str) and value
            for value in (artifact_id, processing_run_id)
        ):
            raise QueueError("audio artifact/run identifiers are invalid")
        audio_descriptor = {
            "artifact_id": artifact_id,
            "artifact_kind": "audio_16khz_mono_flac",
            "processing_run_id": processing_run_id,
            "media_id": audio.get("media_id"),
            "path": str(path),
            "uri": path.as_uri(),
            "sha256": digest,
            "byte_count": byte_count,
            "duration_ms": duration_ms,
            "sealed_mode": audio_sealed_modes[ordinal],
            "format": _audio_format(audio),
            "normalized_probe": copy.deepcopy(audio["normalized_probe"]),
        }
        if audio.get("uri") != path.as_uri():
            raise QueueError("audio URI differs from its replayed path")
        disposition = _resource_disposition(audio_descriptor, profile)
        core = {
            "preprocess_ordinal": ordinal,
            "audio": audio_descriptor,
            "resource_disposition": disposition,
            "lineage": _lineage(origin, receipt, result, source_media),
            "routing_hint": copy.deepcopy(item.get("routing_hint")),
            "private_handling": copy.deepcopy(handling_by_ordinal.get(ordinal)),
        }
        records.append(core)
        receipt_rows.append(receipt)

    if set(audio_sealed_modes) != {
        row["preprocess_ordinal"] for row in records
    }:
        raise QueueError("audio sealed-mode observations differ from eligible receipts")

    skips: list[dict[str, Any]] = []
    for skipped in skipped_rows:
        try:
            ordinal = _integer(
                skipped["ordinal"], "skipped preprocess ordinal", 1, MAX_ITEMS
            )
            receipt = _receipt_reference(skipped["receipt"], "skipped receipt")
            reason = skipped["reason"]
            source_present = skipped["source_audio_stream_present"]
            operation_enabled = skipped["normalized_audio_operation_enabled"]
        except (KeyError, TypeError) as error:
            raise QueueError("v03 explicit skip is incomplete") from error
        if receipt["ordinal"] != ordinal:
            raise QueueError("skip ordinal differs from its receipt")
        if reason not in {
            "source_has_no_audio",
            "normalized_audio_operation_disabled",
        }:
            raise QueueError("preprocess skip reason is unsupported")
        if not isinstance(source_present, bool) or not isinstance(
            operation_enabled, bool
        ):
            raise QueueError("preprocess skip stream/operation flags must be boolean")
        core = {
            "preprocess_ordinal": ordinal,
            "disposition": {
                "state": "skipped",
                "reason": reason,
                "source_audio_stream_present": source_present,
                "normalized_audio_operation_enabled": operation_enabled,
            },
            "lineage": _lineage(
                origin,
                receipt,
                skipped["preprocess_result"],
                skipped["source_media"],
            ),
            "private_handling": copy.deepcopy(handling_by_ordinal.get(ordinal)),
        }
        identity = sha256_bytes(canonical_bytes(core))
        skips.append(
            {
                **core,
                "skip_id": f"{SKIP_ID_PREFIX}{identity[:32]}",
                "identity_sha256": identity,
            }
        )
        receipt_rows.append(receipt)

    records.sort(key=lambda row: row["preprocess_ordinal"])
    skips.sort(key=lambda row: row["preprocess_ordinal"])
    for queue_ordinal, row in enumerate(records, 1):
        row["ordinal"] = queue_ordinal
        core = {
            key: value
            for key, value in row.items()
            if key not in {"member_id", "identity_sha256"}
        }
        identity = sha256_bytes(canonical_bytes(core))
        row["member_id"] = f"{MEMBER_ID_PREFIX}{identity[:32]}"
        row["identity_sha256"] = identity
    if len(records) + len(skips) != receipt_count:
        raise QueueError("queue members and explicit skips do not cover every receipt")
    ordinals = [row["preprocess_ordinal"] for row in [*records, *skips]]
    if set(ordinals) != set(range(1, receipt_count + 1)):
        raise QueueError("queue receipt ordinals are incomplete or repeated")
    for key, label in (
        ("path", "receipt path"),
        ("receipt_id", "receipt ID"),
        ("receipt_sha256", "receipt identity"),
        ("physical_sha256", "physical receipt"),
    ):
        _assert_unique(receipt_rows, (key,), label)
    for path, label in (
        (("audio", "artifact_id"), "audio artifact ID"),
        (("audio", "path"), "audio path"),
        (("audio", "sha256"), "audio content"),
        (("lineage", "preprocess_result", "path"), "preprocess result path"),
    ):
        _assert_unique(records, path, label)

    control = origin.get("handling_control")
    expected_handling_ordinals = (
        set()
        if control is None
        else {row["ordinal"] for row in control["entries"]}
    )
    if set(handling_by_ordinal) != expected_handling_ordinals:
        raise QueueError("private handling descriptor set differs from its control")
    carried_handling = {
        row["preprocess_ordinal"]
        for row in [*records, *skips]
        if row["private_handling"] is not None
    }
    if carried_handling != expected_handling_ordinals:
        raise QueueError("queue does not carry every private handling descriptor")

    ready = [
        row
        for row in records
        if row["resource_disposition"]["state"] == "ready"
    ]
    chunking = [
        row
        for row in records
        if row["resource_disposition"]["state"] == "requires_chunking"
    ]
    totals = {
        "receipt_count": receipt_count,
        "member_count": len(records),
        "ready_count": len(ready),
        "requires_chunking_count": len(chunking),
        "explicit_skip_count": len(skips),
        "private_handling_descriptor_count": len(handling_by_ordinal),
        "audio_byte_count": sum(row["audio"]["byte_count"] for row in records),
        "audio_duration_ms": sum(row["audio"]["duration_ms"] for row in records),
        "ready_audio_byte_count": sum(row["audio"]["byte_count"] for row in ready),
        "ready_audio_duration_ms": sum(row["audio"]["duration_ms"] for row in ready),
        "requires_chunking_audio_byte_count": sum(
            row["audio"]["byte_count"] for row in chunking
        ),
        "requires_chunking_audio_duration_ms": sum(
            row["audio"]["duration_ms"] for row in chunking
        ),
    }
    core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "materializer": MATERIALIZER,
        "portable_root_registration": copy.deepcopy(registration_reference),
        "production_profile": {
            "reference": copy.deepcopy(profile_reference),
            "document": profile,
        },
        "origin": origin,
        "output": {"queue_root": str(queue_root)},
        "members": records,
        "explicit_skips": skips,
        "totals": totals,
        "handling_control": copy.deepcopy(control),
        "safety": dict(SAFETY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    queue_id = f"{QUEUE_ID_PREFIX}{identity[:32]}"
    manifest = {
        **core,
        "queue_id": queue_id,
        "identity_sha256": identity,
        "queue_relative_path": f"queues/{queue_id}",
    }
    if len(canonical_bytes(manifest)) > MAX_MANIFEST_BYTES:
        raise QueueError("GPU handoff manifest exceeds its byte cap")
    return manifest


@contextmanager
def _retained_audio_inputs(
    root: PORTABLE_ROOT.RetainedRoot,
    items: list[dict[str, Any]],
) -> Iterator[
    tuple[
        dict[int, str],
        dict[int, PORTABLE_ROOT.RetainedFile],
    ]
]:
    """Retain every eligible audio path through manifest identity construction."""

    retained_by_ordinal: dict[int, PORTABLE_ROOT.RetainedFile] = {}
    sealed_modes: dict[int, str] = {}
    try:
        for item in items:
            try:
                receipt = item["evidence"]["receipt"]
                audio = item["audio"]
                ordinal = _integer(
                    receipt["ordinal"], "eligible preprocess ordinal", 1, MAX_ITEMS
                )
                audio_path = _absolute_existing(audio["path"], "normalized audio")
                expected_bytes = _integer(
                    audio["byte_count"], "audio byte_count", 1, 2**63 - 1
                )
            except (KeyError, TypeError) as error:
                raise QueueError("v03 eligible item is incomplete") from error
            if ordinal in retained_by_ordinal:
                raise QueueError("duplicate eligible preprocess ordinal is forbidden")
            relative = _relative_to_root(root, audio_path, "normalized audio")
            try:
                retained = root.open_file(
                    relative,
                    label="normalized audio",
                    allowed_modes=set(AUDIO_INPUT_MODES),
                    owner_uid=root.registration["owner"]["uid"],
                    single_link=True,
                )
            except PORTABLE_ROOT.PortableRootError as error:
                raise QueueError(
                    f"normalized audio retention failed: {error}"
                ) from error
            retained_by_ordinal[ordinal] = retained
            observed = os.fstat(retained.descriptor)
            if observed.st_size != expected_bytes:
                raise QueueError("normalized audio size changed after v03 replay")
            sealed_modes[ordinal] = f"{stat.S_IMODE(observed.st_mode):04o}"
        yield sealed_modes, retained_by_ordinal
        for retained in retained_by_ordinal.values():
            retained.verify()
        root.verify()
    finally:
        for retained in reversed(list(retained_by_ordinal.values())):
            retained.close()


def _validate_replayed_paths(
    root: PORTABLE_ROOT.RetainedRoot,
    manifest: dict[str, Any],
    bundle: Path,
    state_root: Path,
    profile_path: Path,
    queue_root: Path,
    retained_audio: dict[int, PORTABLE_ROOT.RetainedFile],
) -> None:
    controlling_paths = [bundle, state_root, profile_path]
    for row in [*manifest["members"], *manifest["explicit_skips"]]:
        lineage = row["lineage"]
        controlling_paths.extend(
            [
                Path(lineage["receipt"]["path"]),
                Path(lineage["preprocess_result"]["path"]),
            ]
        )
        if "audio" in row:
            controlling_paths.append(Path(row["audio"]["path"]))
        source_path = Path(lineage["source_media"]["path"])
        if (
            not source_path.is_absolute()
            or os.path.normpath(str(source_path)) != str(source_path)
        ):
            raise QueueError("source-media lineage path is not normalized absolute")
        if _path_relationship_forbidden(queue_root, source_path):
            raise QueueError("queue root overlaps source-media lineage")
    for path in controlling_paths:
        _relative_to_root(root, path, "GPU handoff controlling input")
        if _path_relationship_forbidden(queue_root, path):
            raise QueueError("queue root overlaps a controlling input")

    for row in manifest["members"]:
        retained = retained_audio.get(row["preprocess_ordinal"])
        if retained is None:
            raise QueueError("normalized audio is not retained for this operation")
        try:
            retained.verify()
        except PORTABLE_ROOT.PortableRootError as error:
            raise QueueError(f"normalized audio retention failed: {error}") from error
        observed = os.fstat(retained.descriptor)
        if (
            retained.root is not root
            or (root.path / retained.relative_path) != Path(row["audio"]["path"])
            or observed.st_size != row["audio"]["byte_count"]
            or f"{stat.S_IMODE(observed.st_mode):04o}"
            != row["audio"]["sealed_mode"]
        ):
            raise QueueError("normalized audio differs from its retained descriptor")

    for row in [*manifest["members"], *manifest["explicit_skips"]]:
        lineage = row["lineage"]
        for key, modes in (
            ("receipt", {0o400}),
            ("preprocess_result", set(READ_ONLY_LINEAGE_MODES)),
        ):
            path = Path(lineage[key]["path"])
            relative = _relative_to_root(root, path, f"lineage {key}")
            try:
                with root.open_file(
                    relative,
                    label=f"lineage {key}",
                    allowed_modes=modes,
                    owner_uid=root.registration["owner"]["uid"],
                    single_link=True,
                ):
                    pass
            except PORTABLE_ROOT.PortableRootError as error:
                raise QueueError(f"lineage {key} retention failed: {error}") from error
    root.verify()


def build_queue(
    *,
    preprocess_bundle: Path,
    preprocess_state_root: Path,
    queue_root: Path,
    production_profile_path: Path,
    root_registration_path: Path,
    root_registration_sha256: str,
) -> dict[str, Any]:
    registration, registration_path, registration_digest = _load_registration(
        root_registration_path, root_registration_sha256
    )
    try:
        root_context = PORTABLE_ROOT.RetainedRoot.open(
            registration, expected_tier=EXPECTED_ROOT_TIER
        )
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(f"portable root retention failed: {error}") from error
    with root_context as root:
        if registration["owner"]["uid"] != os.geteuid():
            raise QueueError("queue writer must run as the registered root owner")
        bundle = _absolute_existing(preprocess_bundle, "preprocess bundle")
        state_root = _absolute_existing(
            preprocess_state_root, "preprocess state root"
        )
        output_root = _absolute_existing(queue_root, "GPU handoff queue root")
        profile_path = _absolute_existing(
            production_profile_path, "production profile"
        )
        for directory, label, modes in (
            (bundle, "preprocess bundle", frozenset({0o500, 0o700})),
            (state_root, "preprocess state root", frozenset({0o700})),
            (output_root, "GPU handoff queue root", frozenset({0o700})),
        ):
            with _retained_directory(
                root, directory, label, allowed_final_modes=modes
            ):
                pass
        profile, profile_reference = _load_profile(root, profile_path)
        try:
            origin, items = RECEIPT_REPLAY._collect_sealed_items(
                bundle, state_root
            )
        except RECEIPT_REPLAY.QueueError as error:
            raise QueueError(f"v03 preprocess replay failed: {error}") from error
        _integer(origin.get("receipt_count"), "origin receipt_count", 1, MAX_ITEMS)
        if len(items) > MAX_ITEMS:
            raise QueueError(f"eligible item count exceeds {MAX_ITEMS}")
        handling = _handling_by_ordinal(origin, items)
        registration_reference = _registration_reference(
            registration, registration_path, registration_digest
        )
        with _retained_audio_inputs(root, items) as (
            audio_sealed_modes,
            retained_audio,
        ):
            manifest = _manifest_from_replay(
                origin_value=origin,
                items_value=items,
                handling_by_ordinal=handling,
                profile_value=profile,
                profile_reference=profile_reference,
                registration_reference=registration_reference,
                queue_root=output_root,
                audio_sealed_modes=audio_sealed_modes,
            )
            _validate_replayed_paths(
                root,
                manifest,
                bundle,
                state_root,
                profile_path,
                output_root,
                retained_audio,
            )
            return manifest


def _open_or_create_directory_at(
    parent_fd: int,
    name: str,
    *,
    mode: int,
    owner_uid: int,
) -> int:
    try:
        os.mkdir(name, mode=mode, dir_fd=parent_fd)
    except FileExistsError:
        pass
    try:
        descriptor = os.open(name, _directory_flags(), dir_fd=parent_fd)
    except OSError as error:
        raise QueueError(
            f"cannot safely open output directory {name!r}: {error}"
        ) from error
    observed = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != owner_uid
        or stat.S_IMODE(observed.st_mode) != mode
    ):
        os.close(descriptor)
        raise QueueError(f"output directory {name!r} owner or mode is unsafe")
    return descriptor


def _writer_lock(queue_root_fd: int, owner_uid: int) -> int:
    name = ".preprocess-gpu-asr-queue-v1.lock"
    try:
        descriptor = os.open(
            name,
            _file_flags(writable=True, create=True),
            0o600,
            dir_fd=queue_root_fd,
        )
    except OSError as error:
        raise QueueError(f"cannot open queue writer lock: {error}") from error
    try:
        observed = os.fstat(descriptor)
        linked = os.stat(name, dir_fd=queue_root_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_uid != owner_uid
            or stat.S_IMODE(observed.st_mode) != 0o600
            or (
                observed.st_dev,
                observed.st_ino,
            )
            != (linked.st_dev, linked.st_ino)
        ):
            raise QueueError("queue writer lock is not a safe mode-0600 file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                raise QueueError("queue writer lock is busy") from error
            raise QueueError(f"queue writer lock failed: {error}") from error
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _write_new_file_at(parent_fd: int, name: str, body: bytes) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
    try:
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise QueueError("sealed manifest write made no progress")
            offset += written
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_file_at(parent_fd: int, name: str, owner_uid: int) -> bytes:
    try:
        inspected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        descriptor = os.open(name, _file_flags(), dir_fd=parent_fd)
    except OSError as error:
        raise QueueError(f"cannot safely read sealed manifest: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != owner_uid
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o400
            or (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mode,
            )
            != (
                inspected.st_dev,
                inspected.st_ino,
                inspected.st_size,
                inspected.st_mode,
            )
            or opened.st_size < 1
            or opened.st_size > MAX_MANIFEST_BYTES
        ):
            raise QueueError("sealed manifest file policy failed")
        chunks: list[bytes] = []
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(
                descriptor, min(1024 * 1024, opened.st_size - offset), offset
            )
            if not chunk:
                raise QueueError("sealed manifest ended during read")
            chunks.append(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        linked = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            _live_directory_identity(after)[:6]
            != _live_directory_identity(opened)[:6]
            or (
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            != (
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            or (
                linked.st_dev,
                linked.st_ino,
                linked.st_size,
                linked.st_mode,
            )
            != (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mode,
            )
        ):
            raise QueueError("sealed manifest changed during read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _verify_queue_at(
    queues_fd: int,
    queue_id: str,
    expected_body: bytes,
    owner_uid: int,
) -> None:
    try:
        inspected = os.stat(queue_id, dir_fd=queues_fd, follow_symlinks=False)
        queue_fd = os.open(queue_id, _directory_flags(), dir_fd=queues_fd)
    except OSError as error:
        raise QueueError(f"cannot open deterministic sealed queue: {error}") from error
    try:
        observed = os.fstat(queue_fd)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != owner_uid
            or stat.S_IMODE(observed.st_mode) != 0o500
            or _live_directory_identity(inspected)
            != _live_directory_identity(observed)
            or os.listdir(queue_fd) != ["manifest.json"]
        ):
            raise QueueError("sealed queue directory layout or policy failed")
        body = _read_file_at(queue_fd, "manifest.json", owner_uid)
        if body != expected_body:
            raise QueueError("existing deterministic queue differs from exact replay")
        linked = os.stat(queue_id, dir_fd=queues_fd, follow_symlinks=False)
        if _live_directory_identity(linked) != _live_directory_identity(observed):
            raise QueueError("sealed queue path changed during verification")
    finally:
        os.close(queue_fd)


def _verify_output_path_binding(
    root: PORTABLE_ROOT.RetainedRoot,
    queue_root: Path,
    queue_root_stat: os.stat_result,
    queues_fd: int,
    manifest: dict[str, Any],
    body: bytes,
) -> None:
    owner_uid = root.registration["owner"]["uid"]
    initial_queues = os.fstat(queues_fd)
    with _retained_directory(
        root,
        queue_root,
        "GPU handoff queue root replay",
        allowed_final_modes=frozenset({0o700}),
    ) as (reopened_root_fd, _relative, reopened_root_stat):
        if _live_directory_identity(reopened_root_stat) != _live_directory_identity(
            queue_root_stat
        ):
            raise QueueError("queue root path changed during materialization")
        try:
            reopened_queues_fd = os.open(
                "queues", _directory_flags(), dir_fd=reopened_root_fd
            )
        except OSError as error:
            raise QueueError("queue collection path changed") from error
        try:
            if _live_directory_identity(
                os.fstat(reopened_queues_fd)
            ) != _live_directory_identity(initial_queues):
                raise QueueError("queue collection path changed during materialization")
            _verify_queue_at(
                reopened_queues_fd,
                manifest["queue_id"],
                body,
                owner_uid,
            )
        finally:
            os.close(reopened_queues_fd)


def _seal_manifest(
    root: PORTABLE_ROOT.RetainedRoot,
    queue_root: Path,
    manifest: dict[str, Any],
) -> Path:
    body = canonical_bytes(manifest)
    owner_uid = root.registration["owner"]["uid"]
    with _retained_directory(
        root,
        queue_root,
        "GPU handoff queue root",
        allowed_final_modes=frozenset({0o700}),
    ) as (queue_root_fd, _relative, _initial_queue_root_stat):
        queues_fd = _open_or_create_directory_at(
            queue_root_fd, "queues", mode=0o700, owner_uid=owner_uid
        )
        bound_queue_root_stat = os.fstat(queue_root_fd)
        lock_fd: int | None = None
        try:
            try:
                PORTABLE_ROOT.require_live_same_filesystem(
                    root, queues_fd, "GPU handoff queue collection"
                )
            except PORTABLE_ROOT.PortableRootError as error:
                raise QueueError(str(error)) from error
            lock_fd = _writer_lock(queue_root_fd, owner_uid)
            queue_id = manifest["queue_id"]
            try:
                _verify_queue_at(queues_fd, queue_id, body, owner_uid)
                _verify_output_path_binding(
                    root,
                    queue_root,
                    bound_queue_root_stat,
                    queues_fd,
                    manifest,
                    body,
                )
                return queue_root / "queues" / queue_id / "manifest.json"
            except QueueError as existing_error:
                try:
                    os.stat(queue_id, dir_fd=queues_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise existing_error

            staging_name = (
                f".{queue_id}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
            )
            os.mkdir(staging_name, mode=0o700, dir_fd=queues_fd)
            staging_fd = os.open(
                staging_name, _directory_flags(), dir_fd=queues_fd
            )
            renamed = False
            try:
                _write_new_file_at(staging_fd, "manifest.json", body)
                os.fchmod(staging_fd, 0o500)
                os.fsync(staging_fd)
                os.rename(
                    staging_name,
                    queue_id,
                    src_dir_fd=queues_fd,
                    dst_dir_fd=queues_fd,
                )
                renamed = True
                os.fsync(queues_fd)
            finally:
                if not renamed:
                    try:
                        os.fchmod(staging_fd, 0o700)
                        os.unlink("manifest.json", dir_fd=staging_fd)
                    except FileNotFoundError:
                        pass
                    finally:
                        os.close(staging_fd)
                    try:
                        os.rmdir(staging_name, dir_fd=queues_fd)
                    except FileNotFoundError:
                        pass
                else:
                    os.close(staging_fd)
            _verify_queue_at(queues_fd, queue_id, body, owner_uid)
            _verify_output_path_binding(
                root,
                queue_root,
                bound_queue_root_stat,
                queues_fd,
                manifest,
                body,
            )
            return queue_root / "queues" / queue_id / "manifest.json"
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
            os.close(queues_fd)


def materialize_queue(**kwargs: Any) -> tuple[dict[str, Any], Path]:
    manifest = build_queue(**kwargs)
    registration, _path, _digest = _load_registration(
        kwargs["root_registration_path"], kwargs["root_registration_sha256"]
    )
    reference = manifest["portable_root_registration"]
    if _registration_reference(
        registration,
        Path(reference["document_path"]),
        reference["document_sha256"],
    ) != reference:
        raise QueueError("portable root changed between queue build and sealing")
    try:
        retained = PORTABLE_ROOT.RetainedRoot.open(
            registration, expected_tier=EXPECTED_ROOT_TIER
        )
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(f"portable root retention failed: {error}") from error
    with retained as root:
        manifest_path = _seal_manifest(
            root, Path(manifest["output"]["queue_root"]), manifest
        )
    return manifest, manifest_path


MANIFEST_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "materializer",
    "portable_root_registration",
    "production_profile",
    "origin",
    "output",
    "members",
    "explicit_skips",
    "totals",
    "handling_control",
    "safety",
    "queue_id",
    "identity_sha256",
    "queue_relative_path",
}


def _load_queue_manifest(
    *,
    manifest_path: Path,
    root_registration_path: Path,
    root_registration_sha256: str,
    expected_manifest_sha256: str | None,
) -> tuple[
    dict[str, Any],
    bytes,
    Path,
    dict[str, Any],
    Path,
    str,
]:
    """Load one sealed queue without replaying its media lineage.

    This is deliberately only the file-level half of :func:`validate_queue`.
    Callers may use it for an already-admitted queue when an external durable
    record pins ``expected_manifest_sha256``.  It authenticates the immutable
    manifest, its self-identity, root binding, and deterministic output path,
    but never rebuilds the queue from preprocess receipts or audio bytes.
    """

    registration, registration_path, registration_digest = _load_registration(
        root_registration_path, root_registration_sha256
    )
    expected_digest = (
        None
        if expected_manifest_sha256 is None
        else _sha256(
            expected_manifest_sha256, "GPU handoff manifest expected SHA-256"
        )
    )
    try:
        retained = PORTABLE_ROOT.RetainedRoot.open(
            registration, expected_tier=EXPECTED_ROOT_TIER
        )
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(f"portable root retention failed: {error}") from error
    with retained as root:
        absolute_manifest = _absolute_existing(manifest_path, "GPU handoff manifest")
        relative = _relative_to_root(root, absolute_manifest, "GPU handoff manifest")
        try:
            with root.open_file(
                relative,
                label="GPU handoff manifest",
                allowed_modes={0o400},
                owner_uid=registration["owner"]["uid"],
                single_link=True,
            ) as manifest_file:
                body = manifest_file.read_bytes(MAX_MANIFEST_BYTES)
        except PORTABLE_ROOT.PortableRootError as error:
            raise QueueError(
                f"GPU handoff manifest retention failed: {error}"
            ) from error
    if expected_digest is not None and sha256_bytes(body) != expected_digest:
        raise QueueError("GPU handoff manifest differs from its admitted SHA-256")
    try:
        parsed = PORTABLE_ROOT.parse_json_bytes(body, "GPU handoff manifest")
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(str(error)) from error
    supplied = _exact(parsed, "GPU handoff manifest", MANIFEST_KEYS)
    if (
        supplied["kind"] != KIND
        or supplied["schema_version"] != SCHEMA_VERSION
        or supplied["implementation_version"] != IMPLEMENTATION_VERSION
        or supplied["materializer"] != MATERIALIZER
        or supplied["safety"] != SAFETY
        or not isinstance(supplied["queue_id"], str)
        or not QUEUE_ID_RE.fullmatch(supplied["queue_id"])
    ):
        raise QueueError("GPU handoff manifest header or safety policy is unsupported")
    expected_registration_reference = _registration_reference(
        registration, registration_path, registration_digest
    )
    if supplied["portable_root_registration"] != expected_registration_reference:
        raise QueueError("manifest portable root differs from its external anchor")
    if body != canonical_bytes(supplied):
        raise QueueError("GPU handoff manifest is not canonical JSON")

    identity_core = {
        key: value
        for key, value in supplied.items()
        if key not in {"queue_id", "identity_sha256", "queue_relative_path"}
    }
    expected_identity = sha256_bytes(canonical_bytes(identity_core))
    expected_queue_id = f"{QUEUE_ID_PREFIX}{expected_identity[:32]}"
    if (
        supplied["identity_sha256"] != expected_identity
        or supplied["queue_id"] != expected_queue_id
        or supplied["queue_relative_path"] != f"queues/{expected_queue_id}"
    ):
        raise QueueError("GPU handoff manifest self-identity is invalid")

    origin = supplied["origin"]
    profile_block = supplied["production_profile"]
    output = supplied["output"]
    if not all(isinstance(value, dict) for value in (origin, profile_block, output)):
        raise QueueError("manifest origin/profile/output blocks must be objects")
    try:
        bundle_block = origin["preprocess_bundle"]
        state_root_value = origin["state_root"]
        profile_reference = profile_block["reference"]
        queue_root_value = output["queue_root"]
        bundle_path_value = bundle_block["path"]
        profile_path_value = profile_reference["path"]
    except (KeyError, TypeError) as error:
        raise QueueError("manifest replay references are incomplete") from error
    if not isinstance(bundle_block, dict) or not isinstance(
        profile_reference, dict
    ):
        raise QueueError("manifest bundle/profile references must be objects")
    if not all(
        isinstance(value, str) and value
        for value in (
            bundle_path_value,
            state_root_value,
            queue_root_value,
            profile_path_value,
        )
    ):
        raise QueueError("manifest replay paths must be nonempty strings")
    bundle_path = _absolute_existing(bundle_path_value, "preprocess bundle")
    state_root = _absolute_existing(state_root_value, "preprocess state root")
    queue_root = _absolute_existing(queue_root_value, "GPU handoff queue root")
    _absolute_existing(profile_path_value, "production profile")
    expected_path = queue_root / supplied["queue_relative_path"] / "manifest.json"
    if absolute_manifest != expected_path:
        raise QueueError("GPU handoff manifest is outside its deterministic path")
    if (
        str(bundle_path) != bundle_path_value
        or str(state_root) != state_root_value
        or str(queue_root) != queue_root_value
    ):
        raise QueueError("GPU handoff manifest paths are not normalized")

    try:
        retained = PORTABLE_ROOT.RetainedRoot.open(
            registration, expected_tier=EXPECTED_ROOT_TIER
        )
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(f"portable root retention failed: {error}") from error
    with retained as root:
        with _retained_directory(
            root,
            queue_root,
            "GPU handoff queue root",
            allowed_final_modes=frozenset({0o700}),
        ) as (queue_root_fd, _relative, _observed):
            queues_fd = os.open("queues", _directory_flags(), dir_fd=queue_root_fd)
            try:
                _verify_queue_at(
                    queues_fd,
                    supplied["queue_id"],
                    body,
                    registration["owner"]["uid"],
                )
            finally:
                os.close(queues_fd)
    return (
        supplied,
        body,
        absolute_manifest,
        registration,
        registration_path,
        registration_digest,
    )


def load_admitted_queue(
    *,
    manifest_path: Path,
    expected_manifest_sha256: str,
    root_registration_path: Path,
    root_registration_sha256: str,
) -> dict[str, Any]:
    """Load a SHA-pinned queue already admitted by durable controller state."""

    supplied, _body, _path, _registration, _registration_path, _digest = (
        _load_queue_manifest(
            manifest_path=manifest_path,
            root_registration_path=root_registration_path,
            root_registration_sha256=root_registration_sha256,
            expected_manifest_sha256=expected_manifest_sha256,
        )
    )
    return supplied


def validate_queue(
    *,
    manifest_path: Path,
    root_registration_path: Path,
    root_registration_sha256: str,
) -> dict[str, Any]:
    (
        supplied,
        body,
        absolute_manifest,
        registration,
        registration_path,
        registration_digest,
    ) = _load_queue_manifest(
        manifest_path=manifest_path,
        root_registration_path=root_registration_path,
        root_registration_sha256=root_registration_sha256,
        expected_manifest_sha256=None,
    )
    origin = supplied["origin"]
    profile_block = supplied["production_profile"]
    output = supplied["output"]
    bundle_block = origin["preprocess_bundle"]
    profile_reference = profile_block["reference"]
    bundle_path_value = bundle_block["path"]
    state_root_value = origin["state_root"]
    queue_root_value = output["queue_root"]
    profile_path_value = profile_reference["path"]
    rebuilt = build_queue(
        preprocess_bundle=Path(bundle_path_value),
        preprocess_state_root=Path(state_root_value),
        queue_root=Path(queue_root_value),
        production_profile_path=Path(profile_path_value),
        root_registration_path=registration_path,
        root_registration_sha256=registration_digest,
    )
    if supplied != rebuilt or body != canonical_bytes(rebuilt):
        raise QueueError("GPU handoff manifest differs from deterministic replay")
    # Deep replay spans source/receipt validation. Re-retain the sealed output
    # afterwards so a path replacement during that interval cannot inherit the
    # authority of the manifest loaded before replay.
    try:
        retained = PORTABLE_ROOT.RetainedRoot.open(
            registration, expected_tier=EXPECTED_ROOT_TIER
        )
    except PORTABLE_ROOT.PortableRootError as error:
        raise QueueError(f"portable root retention failed: {error}") from error
    with retained as root:
        with _retained_directory(
            root,
            Path(rebuilt["output"]["queue_root"]),
            "GPU handoff queue root",
            allowed_final_modes=frozenset({0o700}),
        ) as (queue_root_fd, _relative, _observed):
            queues_fd = os.open("queues", _directory_flags(), dir_fd=queue_root_fd)
            try:
                _verify_queue_at(
                    queues_fd,
                    rebuilt["queue_id"],
                    canonical_bytes(rebuilt),
                    registration["owner"]["uid"],
                )
            finally:
                os.close(queues_fd)
    return rebuilt


def _summary(manifest: dict[str, Any], path: Path) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "validated",
        "queue_id": manifest["queue_id"],
        "manifest_path": str(path),
        "manifest_sha256": sha256_bytes(canonical_bytes(manifest)),
        "totals": manifest["totals"],
        "safety": manifest["safety"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seal deterministic preprocess receipts for the GPU v5 lane"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    materialize = subparsers.add_parser("materialize")
    materialize.add_argument("--preprocess-bundle", required=True)
    materialize.add_argument("--preprocess-state-root", required=True)
    materialize.add_argument("--queue-root", required=True)
    materialize.add_argument("--production-profile", required=True)
    materialize.add_argument("--root-registration", required=True)
    materialize.add_argument("--root-registration-sha256", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--manifest", required=True)
    validate.add_argument("--root-registration", required=True)
    validate.add_argument("--root-registration-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "materialize":
            manifest, path = materialize_queue(
                preprocess_bundle=Path(args.preprocess_bundle),
                preprocess_state_root=Path(args.preprocess_state_root),
                queue_root=Path(args.queue_root),
                production_profile_path=Path(args.production_profile),
                root_registration_path=Path(args.root_registration),
                root_registration_sha256=args.root_registration_sha256,
            )
        else:
            path = Path(args.manifest)
            manifest = validate_queue(
                manifest_path=path,
                root_registration_path=Path(args.root_registration),
                root_registration_sha256=args.root_registration_sha256,
            )
        sys.stdout.buffer.write(canonical_bytes(_summary(manifest, path)))
        return 0
    except (
        QueueError,
        RECEIPT_REPLAY.QueueError,
        PORTABLE_ROOT.PortableRootError,
        PROFILE_V2.ProfileError,
        OSError,
    ) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(canonical_bytes(failure))
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
