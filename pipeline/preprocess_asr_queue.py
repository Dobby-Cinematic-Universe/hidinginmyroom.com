#!/usr/bin/env python3
"""Seal deterministic whisper.cpp work orders for preprocessed audio artifacts.

This materializer is deliberately separate from the local-window batch lane.
Its input timestamps are local to a full-media ``audio_16khz_mono_flac``
artifact.  A recording-relative transform is never inferred from matching
durations, a rendition row, or a derivation edge.

Two admission proofs are supported:

* ``sealed_preprocess_receipts`` replays an immutable preprocess bundle and
  every completed receipt; or
* ``catalog_admitted_preprocess_results`` binds explicitly supplied completed
  result envelopes to exact rows in a read-only catalog snapshot.

The module performs no ASR, network access, catalog writes, or publication.
It writes only private JSON queue manifests.  Legacy/public inputs retain ordinary
ASR work orders.  A v30 private-acquisition input instead receives a queue-specific
wrapper so its handling policy and seal lineage cannot be accepted by the generic
ASR adapter after being silently stripped.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlsplit

try:
    from . import asr_whispercpp, preprocess_batch, whispercpp_engine_profiles
except ImportError:  # pragma: no cover - direct script execution
    import asr_whispercpp  # type: ignore[no-redef]
    import preprocess_batch  # type: ignore[no-redef]
    import whispercpp_engine_profiles  # type: ignore[no-redef]


PIPELINE_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = PIPELINE_ROOT.parent
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"
if str(CORPUS_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORPUS_SOURCE_ROOT))

from himr_corpus.result_importers import (  # noqa: E402
    ResultImportError,
    validate_preprocess_result,
)


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
MATERIALIZER_NAME = "himr-preprocess-asr-queue"
PRIVATE_WORK_ORDER_KIND = "v30_private_preprocess_asr_queue_work_order"

MAX_ITEMS = 128
MAX_RESULT_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 32 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_AUDIO_BYTES = 16 * 1024 * 1024 * 1024
MAX_TOTAL_AUDIO_BYTES = 256 * 1024 * 1024 * 1024
MAX_TOTAL_AUDIO_MS = 7 * 24 * 60 * 60 * 1_000
MAX_DATABASE_BYTES = 16 * 1024 * 1024 * 1024
MAX_JSON_DEPTH = 128

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ARTIFACT_ID_RE = re.compile(r"^artifact_[0-9a-f]{32}$")
MEDIA_ID_RE = re.compile(r"^media_sha256_([0-9a-f]{64})$")
QUEUE_ID_RE = re.compile(r"^asrppqueue_[0-9a-f]{32}$")

EXPECTED_MODEL_SHA256 = (
    "c6138d6d58ecc8322097e0f987c32f1be8bb0a18532a3f88f734d1bbf9c41e5d"
)
EXPECTED_MODEL_BYTE_COUNT = 487_614_201
EXPECTED_MODEL = {
    "model_id": "model_whispercpp_small_en_c6138d6d58ec",
    "name": "Whisper small.en ggml",
    "revision": "ggerganov/whisper.cpp@5359861c739e955e79d9a303bcbc70fb988958b1",
    "source": (
        "https://huggingface.co/ggerganov/whisper.cpp/blob/"
        "5359861c739e955e79d9a303bcbc70fb988958b1/ggml-small.en.bin"
    ),
    "license_label": "MIT (model repository card reviewed 2026-08-26)",
}

RAW_INFERENCE = {
    "language": "en",
    "threads": 6,
    "translate": False,
    "split_on_word": True,
    "best_of": 5,
    "beam_size": 5,
    "max_segment_characters": 0,
    "word_threshold": 0.01,
    "entropy_threshold": 2.4,
    "logprob_threshold": -1.0,
    "no_speech_threshold": 0.6,
    "temperature": 0.0,
    "temperature_increment": 0.2,
    "no_fallback": False,
    "timeout_seconds": 7_200,
}

SAFETY = {
    "catalog_writes": False,
    "credentials_used": False,
    "identity_authority": "none",
    "network_access": "forbidden_by_contract_not_performed_by_materializer",
    "publication_authority": "none",
    "raw_or_source_media_copied": False,
    "source_bytes_preserved": True,
    "timestamp_translation_authority": "none",
    "visibility": "private",
}

PRIVATE_SAFETY = {
    "private_acquisition_seal_required": True,
    "handling_policy_propagation_required": True,
}

PRIVATE_WORK_ORDER_SAFETY = {
    "direct_adapter_execution_allowed": False,
    "handling_policy_propagation_required": True,
    "private_acquisition_seal_replay_required": True,
    "publication_authority": "none",
    "visibility": "private",
}


class QueueError(RuntimeError):
    """An input, provenance, coordinate, or immutable-output check failed."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise QueueError(f"value is not strict canonical JSON: {error}") from error


def pretty_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise QueueError(f"value is not strict JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_constant(value: str) -> None:
    raise QueueError(f"non-finite JSON constant is forbidden: {value}")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise QueueError(f"JSON object contains duplicate key: {key}")
        result[key] = value
    return result


def _check_depth(value: Any, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise QueueError(f"JSON nesting exceeds {MAX_JSON_DEPTH} levels")
    if isinstance(value, dict):
        for child in value.values():
            _check_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_depth(child, depth + 1)
    elif isinstance(value, float) and not math.isfinite(value):
        raise QueueError("non-finite JSON number is forbidden")


def parse_json(body: bytes, label: str) -> Any:
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except QueueError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise QueueError(f"{label} is not strict UTF-8 JSON") from error
    _check_depth(value)
    return value


def _exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QueueError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise QueueError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise QueueError(f"{label} must be an integer from {minimum} to {maximum}")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise QueueError(f"{label} must be a lowercase SHA-256")
    return value


def _bounded_string(value: Any, label: str, maximum: int = 8192) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise QueueError(f"{label} must be bounded non-empty text")
    return value


def _absolute_path(value: Any, label: str, *, existing: bool) -> Path:
    try:
        path = preprocess_batch.absolute_path(value, label, must_exist=existing)
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error
    lexical = Path(os.path.abspath(os.fspath(value)))
    if path != lexical:
        raise QueueError(f"{label} must already be normalized and resolved")
    return path


def _private_root(value: Any, label: str) -> Path:
    try:
        return preprocess_batch.private_output_root_reference(value, label)
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error


def _stable_readonly(path: Path, maximum: int, label: str) -> tuple[Path, bytes]:
    try:
        resolved, body, _ = preprocess_batch.readonly_file(path, maximum, label)
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error
    return resolved, body


def _stable_hash(
    path: Path,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    label: str,
) -> None:
    if expected_byte_count < 1 or expected_byte_count > MAX_AUDIO_BYTES:
        raise QueueError(f"{label} byte count is outside the private artifact cap")
    try:
        observed = path.lstat()
    except OSError as error:
        raise QueueError(f"{label} cannot be inspected") from error
    if stat.S_IMODE(observed.st_mode) & 0o222:
        raise QueueError(f"{label} must be sealed read-only")
    try:
        preprocess_batch.stable_hash_media(
            path,
            expected_sha256=expected_sha256,
            expected_byte_count=expected_byte_count,
            label=label,
        )
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error


def _file_uri_path(value: Any, label: str) -> Path:
    uri = _bounded_string(value, label)
    parsed = urlsplit(uri)
    if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
        raise QueueError(f"{label} must be a local file URI without authority/query/fragment")
    try:
        raw = unquote(parsed.path, errors="strict")
    except UnicodeDecodeError as error:
        raise QueueError(f"{label} has invalid percent encoding") from error
    path = _absolute_path(raw, label, existing=True)
    if path.as_uri() != uri:
        raise QueueError(f"{label} is not the canonical URI for its path")
    return path


def _normalized_audio_probe(value: Any, digest: str, byte_count: int) -> tuple[dict[str, Any], int]:
    probe = value
    if not isinstance(probe, dict):
        raise QueueError("preprocess audio normalized_probe must be an object")
    if probe.get("schema_version") != 1:
        raise QueueError("preprocess audio normalized_probe schema is unsupported")
    media = probe.get("media")
    primary = probe.get("primary_streams")
    format_row = probe.get("format")
    streams = probe.get("streams")
    if not all(isinstance(row, dict) for row in (media, primary, format_row)):
        raise QueueError("preprocess audio normalized_probe is incomplete")
    if (
        media.get("sha256") != digest
        or media.get("media_id") != f"media_sha256_{digest}"
        or media.get("byte_count") != byte_count
        or primary != {"audio_index": 0, "video_index": None}
        or format_row.get("format_name") != "flac"
        or format_row.get("start_ms") != 0
        or not isinstance(streams, list)
        or len(streams) != 1
        or not isinstance(streams[0], dict)
    ):
        raise QueueError("preprocess audio normalized_probe identity/stream layout is inconsistent")
    stream = streams[0]
    if (
        stream.get("index") != 0
        or stream.get("codec_type") != "audio"
        or stream.get("codec_name") != "flac"
        or stream.get("start_ms") != 0
        or stream.get("audio")
        != {
            "channel_layout": "mono",
            "channels": 1,
            "sample_format": "s16",
            "sample_rate_hz": 16_000,
        }
    ):
        raise QueueError("preprocess audio is not 16 kHz mono s16 FLAC from local zero")
    duration_ms = _integer(
        format_row.get("duration_ms"),
        "preprocess audio duration_ms",
        1,
        asr_whispercpp.MAX_WINDOW_MS,
    )
    if stream.get("duration_ms") != duration_ms:
        raise QueueError("preprocess audio stream and format durations disagree")
    return probe, duration_ms


def _validated_result_item(result_path: Path) -> dict[str, Any]:
    resolved, body = _stable_readonly(result_path, MAX_RESULT_BYTES, "preprocess result")
    raw = parse_json(body, "preprocess result")
    try:
        validate_preprocess_result(raw)
    except ResultImportError as error:
        raise QueueError(f"preprocess result contract failed: {error}") from error
    if not isinstance(raw, dict) or raw.get("result_path") != str(resolved):
        raise QueueError("preprocess result path differs from its exact envelope")
    output_root = _private_root(raw["layout"]["output_root"], "preprocess output root")
    try:
        resolved.relative_to(output_root)
    except ValueError as error:
        raise QueueError("preprocess result escapes its private output root") from error

    audio_rows = [
        row
        for row in raw["artifacts"]
        if row.get("artifact_kind") == "audio_16khz_mono_flac"
    ]
    if len(audio_rows) != 1:
        raise QueueError("preprocess result must contain exactly one normalized audio artifact")
    audio = audio_rows[0]
    artifact_id = _bounded_string(audio.get("artifact_id"), "audio artifact_id", 128)
    if not ARTIFACT_ID_RE.fullmatch(artifact_id):
        raise QueueError("audio artifact_id is malformed")
    digest = _sha256(audio.get("sha256"), "audio artifact SHA-256")
    byte_count = _integer(audio.get("byte_count"), "audio artifact byte_count", 1, MAX_AUDIO_BYTES)
    path = _absolute_path(audio.get("path"), "audio artifact path", existing=True)
    if audio.get("storage_uri") != path.as_uri() or audio.get("visibility") != "private":
        raise QueueError("audio artifact URI or visibility is inconsistent")
    if audio.get("media_kind") != "audio" or audio.get("mime_type") != "audio/flac":
        raise QueueError("audio artifact media/MIME kind is inconsistent")
    probe, duration_ms = _normalized_audio_probe(audio.get("normalized_probe"), digest, byte_count)
    _stable_hash(
        path,
        expected_sha256=digest,
        expected_byte_count=byte_count,
        label=f"preprocess audio artifact {artifact_id}",
    )
    replay_path, replay_body = _stable_readonly(
        resolved, MAX_RESULT_BYTES, "preprocess result replay"
    )
    if replay_path != resolved or replay_body != body:
        raise QueueError("preprocess result changed while its audio artifact was verified")

    source = raw["input"]
    source_digest = _sha256(source.get("sha256"), "preprocess source SHA-256")
    if source.get("media_id") != f"media_sha256_{source_digest}":
        raise QueueError("preprocess source media_id does not derive from its SHA-256")
    source_media_rows = [
        row
        for row in raw["catalog_records"]["media_objects"]
        if row.get("media_id") == source["media_id"]
    ]
    if len(source_media_rows) != 1:
        raise QueueError("preprocess result has no unique source media catalog handoff")
    source_media_row = source_media_rows[0]
    processing_run_id = _bounded_string(
        raw["processing_run"].get("processing_run_id"),
        "preprocess processing_run_id",
        128,
    )
    if audio.get("processing_run_id") != processing_run_id:
        raise QueueError("audio artifact belongs to a different processing run")
    routing_hint = None
    if isinstance(raw.get("routing"), dict):
        candidates = raw["routing"].get("routing_candidates")
        if isinstance(candidates, dict):
            routing_hint = candidates.get("asr")
    return {
        "result": {
            "path": str(resolved),
            "uri": resolved.as_uri(),
            "sha256": sha256_bytes(body),
            "byte_count": len(body),
            "job_id": raw["job_id"],
            "processing_run_id": processing_run_id,
            "recipe_sha256": raw["layout"]["recipe_sha256"],
        },
        "source_media": {
            "media_id": source["media_id"],
            "sha256": source_digest,
            "byte_count": source["byte_count"],
            "path": source["path"],
            "storage_uri": source["storage_uri"],
            "duration_ms": source_media_row.get("duration_ms"),
            "first_cataloged_at": source_media_row["first_cataloged_at"],
        },
        "audio": {
            "artifact_id": artifact_id,
            "artifact_kind": "audio_16khz_mono_flac",
            "processing_run_id": processing_run_id,
            "media_id": f"media_sha256_{digest}",
            "path": str(path),
            "uri": path.as_uri(),
            "sha256": digest,
            "byte_count": byte_count,
            "duration_ms": duration_ms,
            "normalized_probe": probe,
            "visibility": "private",
        },
        "routing_hint": routing_hint,
        "raw_result": raw,
    }


HANDLING_CONTROL_KEYS = {
    "kind",
    "private_entry_count",
    "entries",
    "seal_receipt_replay_required",
    "handling_policy_propagation_required",
    "publication_authority",
    "identity_sha256",
}

HANDLING_CONTROL_ENTRY_KEYS = {
    "ordinal",
    "entry_id",
    "handling_policy",
    "handling_boundary_sha256",
    "seal_receipt_sha256",
    "seal_plan_sha256",
    "source_byte_identity_claimed",
}


def _validated_handling_control(value: Any) -> dict[str, Any]:
    """Validate and normalize the exact preprocess v30 handling control."""

    control = _exact_object(value, "preprocess handling_control", HANDLING_CONTROL_KEYS)
    if (
        control["kind"] != "v30_private_acquisition_handling_control"
        or control["seal_receipt_replay_required"] is not True
        or control["handling_policy_propagation_required"] is not True
        or control["publication_authority"] != "none"
    ):
        raise QueueError("preprocess handling_control policy is unsupported")
    count = _integer(
        control["private_entry_count"],
        "preprocess handling_control private_entry_count",
        1,
        MAX_ITEMS,
    )
    rows = control["entries"]
    if not isinstance(rows, list) or len(rows) != count:
        raise QueueError("preprocess handling_control entry count is inconsistent")
    normalized_rows: list[dict[str, Any]] = []
    ordinals: set[int] = set()
    entry_ids: set[str] = set()
    for index, raw in enumerate(rows, 1):
        row = _exact_object(
            raw,
            f"preprocess handling_control entry {index}",
            HANDLING_CONTROL_ENTRY_KEYS,
        )
        ordinal = _integer(
            row["ordinal"],
            f"preprocess handling_control entry {index} ordinal",
            1,
            MAX_ITEMS,
        )
        entry_id = _bounded_string(
            row["entry_id"],
            f"preprocess handling_control entry {index} entry_id",
            128,
        )
        if ordinal in ordinals or entry_id in entry_ids:
            raise QueueError("preprocess handling_control repeats an ordinal or entry_id")
        ordinals.add(ordinal)
        entry_ids.add(entry_id)
        try:
            policy = preprocess_batch.handling_policy(
                row["handling_policy"],
                f"preprocess handling_control entry {index} handling_policy",
            )
        except preprocess_batch.BatchError as error:
            raise QueueError(str(error)) from error
        normalized_rows.append(
            {
                "ordinal": ordinal,
                "entry_id": entry_id,
                "handling_policy": policy,
                "handling_boundary_sha256": _sha256(
                    row["handling_boundary_sha256"],
                    f"preprocess handling_control entry {index} boundary SHA-256",
                ),
                "seal_receipt_sha256": _sha256(
                    row["seal_receipt_sha256"],
                    f"preprocess handling_control entry {index} seal receipt SHA-256",
                ),
                "seal_plan_sha256": _sha256(
                    row["seal_plan_sha256"],
                    f"preprocess handling_control entry {index} seal plan SHA-256",
                ),
                "source_byte_identity_claimed": row["source_byte_identity_claimed"],
            }
        )
        if row["source_byte_identity_claimed"] is not False:
            raise QueueError("private handling control may not claim source byte identity")
    if normalized_rows != rows:
        raise QueueError("preprocess handling_control is not already normalized")
    core = {key: control[key] for key in HANDLING_CONTROL_KEYS - {"identity_sha256"}}
    if control["identity_sha256"] != sha256_bytes(canonical_bytes(core)):
        raise QueueError("preprocess handling_control identity SHA-256 is inconsistent")
    return control


def _handling_descriptor(
    *,
    boundary: Any,
    control_entry: dict[str, Any],
) -> dict[str, Any]:
    """Replay a private boundary and bind it to its preprocess control row."""

    try:
        observed = preprocess_batch.replay_handling_boundary(boundary)
    except preprocess_batch.BatchError as error:
        raise QueueError(f"private handling boundary replay failed: {error}") from error
    seal = observed["private_acquisition_seal"]
    if (
        observed["handling_policy"] != control_entry["handling_policy"]
        or sha256_bytes(canonical_bytes(observed))
        != control_entry["handling_boundary_sha256"]
        or seal["receipt_sha256"] != control_entry["seal_receipt_sha256"]
        or seal["plan_sha256"] != control_entry["seal_plan_sha256"]
        or seal["source_byte_identity_claimed"] is not False
        or control_entry["source_byte_identity_claimed"] is not False
    ):
        raise QueueError("private handling boundary differs from its preprocess control row")
    return {
        "preprocess_control_entry": control_entry,
        "handling_boundary": observed,
    }


def _collect_sealed_items(
    bundle_dir: Path,
    state_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Replay a completed preprocess receipt set and return ASR-ready items."""

    try:
        manifest, selection, orders = preprocess_batch.validate_bundle(bundle_dir)
        receipts = preprocess_batch.existing_receipts(
            state_root,
            manifest=manifest,
            selection=selection,
            orders=orders,
        )
    except preprocess_batch.BatchError as error:
        raise QueueError(f"preprocess receipt replay failed: {error}") from error
    if len(receipts) != manifest["work_order_count"]:
        missing = sorted(set(range(1, manifest["work_order_count"] + 1)) - set(receipts))
        raise QueueError(f"preprocess receipt set is incomplete; missing ordinals={missing}")

    handling = manifest.get("handling_control")
    control = None if handling is None else _validated_handling_control(handling)
    control_by_ordinal = (
        {}
        if control is None
        else {row["ordinal"]: row for row in control["entries"]}
    )

    manifest_path = bundle_dir / "manifest.json"
    manifest_path, manifest_body = _stable_readonly(
        manifest_path,
        preprocess_batch.MAX_BUNDLE_MANIFEST_BYTES,
        "preprocess bundle manifest",
    )
    items: list[dict[str, Any]] = []
    receipt_refs: list[dict[str, Any]] = []
    for ordinal in range(1, manifest["work_order_count"] + 1):
        receipt_row = receipts[ordinal]
        receipt = receipt_row["receipt"]
        receipt_path = (
            state_root
            / "runs"
            / manifest["bundle_id"]
            / "receipts"
            / f"{ordinal:06d}.json"
        )
        item = _validated_result_item(Path(receipt["preprocess_result"]["path"]))
        result_ref = receipt["preprocess_result"]
        if any(
            item["result"][key] != result_ref[key]
            for key in ("path", "sha256", "byte_count", "processing_run_id", "recipe_sha256")
        ):
            raise QueueError(f"receipt {ordinal} result pin differs from the result envelope")
        receipt_audio = [
            row
            for row in receipt["artifacts"]
            if row.get("artifact_kind") == "audio_16khz_mono_flac"
        ]
        if len(receipt_audio) != 1:
            raise QueueError(f"receipt {ordinal} has no unique normalized audio artifact")
        audio = receipt_audio[0]
        for key in ("artifact_id", "path", "sha256", "byte_count"):
            if item["audio"][key] != audio[key]:
                raise QueueError(f"receipt {ordinal} audio {key} differs from the result")
        if item["audio"]["uri"] != audio["storage_uri"]:
            raise QueueError(f"receipt {ordinal} audio URI differs from the result")
        source = receipt["source_media"]
        for key in (
            "media_id",
            "sha256",
            "byte_count",
            "path",
            "duration_ms",
            "first_cataloged_at",
        ):
            if item["source_media"][key] != source[key]:
                raise QueueError(f"receipt {ordinal} source {key} differs from the result")

        control_entry = control_by_ordinal.get(ordinal)
        boundary = receipt.get("handling_boundary")
        if control_entry is None:
            if boundary is not None:
                raise QueueError(
                    f"receipt {ordinal} introduces a private handling boundary "
                    "absent from the preprocess control"
                )
        else:
            if receipt.get("entry_id") != control_entry["entry_id"]:
                raise QueueError(
                    f"receipt {ordinal} entry_id differs from the preprocess control"
                )
            if boundary is None:
                raise QueueError(
                    f"receipt {ordinal} drops its private handling boundary"
                )
            item["handling"] = _handling_descriptor(
                boundary=boundary,
                control_entry=control_entry,
            )

        item["evidence"] = {
            "mode": "sealed_preprocess_receipt",
            "receipt": {
                "path": str(receipt_path),
                "uri": receipt_path.as_uri(),
                "physical_sha256": receipt_row["physical_sha256"],
                "receipt_id": receipt["receipt_id"],
                "receipt_sha256": receipt["receipt_sha256"],
                "ordinal": ordinal,
            },
            "preprocess_result": dict(item["result"]),
            "source_media": dict(item["source_media"]),
        }
        receipt_refs.append(item["evidence"]["receipt"])
        items.append(item)

    origin = {
        "mode": "sealed_preprocess_receipts",
        "preprocess_bundle": {
            "path": str(bundle_dir),
            "manifest_path": str(manifest_path),
            "manifest_physical_sha256": sha256_bytes(manifest_body),
            "bundle_id": manifest["bundle_id"],
            "identity_sha256": manifest["identity_sha256"],
            "manifest_sha256": manifest["manifest_sha256"],
        },
        "state_root": str(state_root),
        "receipt_count": len(receipt_refs),
        "receipt_state_sha256": preprocess_batch.state_digest(receipts),
        "receipt_refs_sha256": sha256_bytes(canonical_bytes(receipt_refs)),
        **({"handling_control": control} if control is not None else {}),
    }
    if len([item for item in items if "handling" in item]) != len(control_by_ordinal):
        raise QueueError("preprocess handling control is not represented by the receipt set")
    return origin, items


def _software_document() -> dict[str, Any]:
    components = {
        "materializer": {
            "name": MATERIALIZER_NAME,
            "implementation_version": IMPLEMENTATION_VERSION,
            "path": Path(__file__).resolve(),
        },
        "asr_adapter": {
            "name": "himr-asr-whispercpp",
            "contract_version": asr_whispercpp.CONTRACT_VERSION,
            "implementation_version": asr_whispercpp.IMPLEMENTATION_VERSION,
            "path": Path(asr_whispercpp.__file__).resolve(),
        },
        "engine_profiles": {
            "name": "himr-whispercpp-engine-profiles",
            "contract_version": whispercpp_engine_profiles.PROFILE_CONTRACT_VERSION,
            "implementation_version": whispercpp_engine_profiles.PROFILE_IMPLEMENTATION_VERSION,
            "path": Path(whispercpp_engine_profiles.__file__).resolve(),
        },
    }
    observed: dict[str, Any] = {}
    for name, component in components.items():
        path = component["path"]
        try:
            resolved, body = _stable_readonly(path, 4 * 1024 * 1024, f"{name} implementation")
        except QueueError:
            # Repository source is ordinarily writable during development.  It
            # still receives a stable single-read hash; sealed queue replay will
            # fail if the bytes change.
            try:
                resolved, body, _ = preprocess_batch.stable_read(
                    path, 4 * 1024 * 1024, f"{name} implementation"
                )
            except preprocess_batch.BatchError as error:
                raise QueueError(str(error)) from error
        observed[name] = {
            key: value for key, value in component.items() if key != "path"
        } | {
            "path": str(resolved),
            "sha256": sha256_bytes(body),
            "byte_count": len(body),
        }
    return observed


def _engine_document(path: Path) -> dict[str, Any]:
    path = _absolute_path(path, "whisper.cpp executable", existing=True)
    if not os.access(path, os.X_OK):
        raise QueueError("whisper.cpp executable is not executable")
    try:
        observed_path, body, _ = preprocess_batch.stable_read(
            path,
            1024 * 1024 * 1024,
            "whisper.cpp executable",
        )
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error
    observed_sha256 = sha256_bytes(body)
    try:
        profile = whispercpp_engine_profiles.match_engine_profile(
            observed_sha256,
            len(body),
        )
    except whispercpp_engine_profiles.EngineProfileError as error:
        raise QueueError(f"whisper.cpp engine profile rejected: {error}") from error
    public = whispercpp_engine_profiles.public_engine_document(
        profile,
        str(observed_path),
    )
    if (
        public["expected_sha256"] != observed_sha256
        or public["byte_count"] != len(body)
        or profile.get("admission") != "current_new_batch"
        or profile.get("output_json_full_utf8_token_boundary_merge") is not True
    ):
        raise QueueError("selected whisper.cpp profile is not eligible for new UTF-8-safe work")
    return {
        **public,
        "profile_contract_version": whispercpp_engine_profiles.PROFILE_CONTRACT_VERSION,
        "profile_implementation_version": whispercpp_engine_profiles.PROFILE_IMPLEMENTATION_VERSION,
        "selected_profile": profile,
        "selected_profile_sha256": sha256_bytes(canonical_bytes(profile)),
    }


def _model_document(path: Path) -> dict[str, Any]:
    path = _absolute_path(path, "whisper.cpp model", existing=True)
    try:
        preprocess_batch.stable_hash_media(
            path,
            expected_sha256=EXPECTED_MODEL_SHA256,
            expected_byte_count=EXPECTED_MODEL_BYTE_COUNT,
            label="pinned whisper.cpp model",
        )
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error
    return {
        "path": str(path),
        "expected_sha256": EXPECTED_MODEL_SHA256,
        "byte_count": EXPECTED_MODEL_BYTE_COUNT,
        **EXPECTED_MODEL,
    }


def _coordinate_provenance(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_coordinate_system": "artifact_local_milliseconds",
        "artifact_start_ms": 0,
        "artifact_end_ms": item["audio"]["duration_ms"],
        "boundary": "half_open",
        "recording_transform_state": "unresolved",
        "recording_start_ms": None,
        "recording_end_ms": None,
        "translation_basis": "not_claimed_by_preprocess_asr_queue",
        "duration_equality_is_translation_evidence": False,
    }


def _audio_manifest_descriptor(audio: dict[str, Any]) -> dict[str, Any]:
    return {
        key: audio[key]
        for key in (
            "artifact_id",
            "artifact_kind",
            "processing_run_id",
            "media_id",
            "path",
            "uri",
            "sha256",
            "byte_count",
            "duration_ms",
            "visibility",
        )
    } | {
        "normalized_probe_sha256": sha256_bytes(
            canonical_bytes(audio["normalized_probe"])
        )
    }


def _queue_handling_control(
    origin: dict[str, Any], items: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Validate the all-or-explicit-subset private handling boundary."""

    raw_control = origin.get("handling_control")
    if raw_control is None:
        if any("handling" in item for item in items):
            raise QueueError("queue items may not introduce private handling control")
        return None
    if origin.get("mode") != "sealed_preprocess_receipts":
        raise QueueError("private handling control requires sealed preprocess receipts")
    control = _validated_handling_control(raw_control)
    by_ordinal = {row["ordinal"]: row for row in control["entries"]}
    observed_ordinals: set[int] = set()
    for item in items:
        evidence = item.get("evidence")
        receipt = evidence.get("receipt") if isinstance(evidence, dict) else None
        if not isinstance(receipt, dict):
            raise QueueError("private queue item has no sealed preprocess receipt")
        preprocess_ordinal = _integer(
            receipt.get("ordinal"), "preprocess receipt ordinal", 1, MAX_ITEMS
        )
        expected_entry = by_ordinal.get(preprocess_ordinal)
        supplied = item.get("handling")
        if expected_entry is None:
            if supplied is not None:
                raise QueueError("queue item introduces handling outside its control")
            continue
        if supplied is None:
            raise QueueError("queue item drops its private handling boundary")
        descriptor = _exact_object(
            supplied,
            "private queue handling descriptor",
            {"preprocess_control_entry", "handling_boundary"},
        )
        if descriptor["preprocess_control_entry"] != expected_entry:
            raise QueueError("queue handling descriptor uses the wrong control entry")
        replayed = _handling_descriptor(
            boundary=descriptor["handling_boundary"],
            control_entry=expected_entry,
        )
        if replayed != descriptor:
            raise QueueError("queue handling descriptor differs from exact seal replay")
        observed_ordinals.add(preprocess_ordinal)
    if observed_ordinals != set(by_ordinal):
        raise QueueError("queue does not carry every preprocess handling-control entry")
    return control


def _private_work_order_document(
    order: dict[str, Any], handling: dict[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": PRIVATE_WORK_ORDER_KIND,
        "asr_work_order": order,
        "handling": handling,
        "safety": dict(PRIVATE_WORK_ORDER_SAFETY),
    }


def validate_queue_work_order(
    value: Any,
    *,
    entry: dict[str, Any],
    manifest: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the normalized adapter order and its sealed queue document."""

    handling = entry.get("handling")
    if handling is None:
        if manifest.get("handling_control") is None:
            try:
                order = asr_whispercpp.validate_work_order(value)
            except asr_whispercpp.ASRError as error:
                raise QueueError(f"generated ASR work order is invalid: {error}") from error
            return order, order
        # A mixed batch may legitimately contain unrestricted entries.  Their
        # files remain ordinary adapter orders and the manifest fixes that fact.
        try:
            order = asr_whispercpp.validate_work_order(value)
        except asr_whispercpp.ASRError as error:
            raise QueueError(f"generated ASR work order is invalid: {error}") from error
        return order, order

    if manifest.get("handling_control") is None:
        raise QueueError("private queue work-order wrapper has no manifest control")
    wrapper = _exact_object(
        value,
        "private queue work-order wrapper",
        {"schema_version", "kind", "asr_work_order", "handling", "safety"},
    )
    if (
        wrapper["schema_version"] != 1
        or wrapper["kind"] != PRIVATE_WORK_ORDER_KIND
        or wrapper["handling"] != handling
        or wrapper["safety"] != PRIVATE_WORK_ORDER_SAFETY
    ):
        raise QueueError("private queue work-order wrapper is inconsistent")
    descriptor = _exact_object(
        handling,
        "private queue handling descriptor",
        {"preprocess_control_entry", "handling_boundary"},
    )
    control_entry = descriptor["preprocess_control_entry"]
    control_rows = manifest["handling_control"].get("entries")
    if not isinstance(control_rows, list) or control_rows.count(control_entry) != 1:
        raise QueueError("private work-order control entry is not unique in the manifest")
    if _handling_descriptor(
        boundary=descriptor["handling_boundary"],
        control_entry=control_entry,
    ) != descriptor:
        raise QueueError("private work-order handling boundary failed exact seal replay")
    try:
        order = asr_whispercpp.validate_work_order(wrapper["asr_work_order"])
    except asr_whispercpp.ASRError as error:
        raise QueueError(f"wrapped ASR work order is invalid: {error}") from error
    expected_job_identity = {
        "artifact_id": entry["audio_artifact"]["artifact_id"],
        "audio_sha256": entry["audio_artifact"]["sha256"],
        "parent_processing_run_id": entry["audio_artifact"]["processing_run_id"],
        "pass": "raw_full_preprocess_artifact_small_en_v1",
        "coordinate_system": "artifact_local_milliseconds",
        "handling_boundary_sha256": control_entry["handling_boundary_sha256"],
        "seal_receipt_sha256": control_entry["seal_receipt_sha256"],
        "seal_plan_sha256": control_entry["seal_plan_sha256"],
    }
    expected_job_id = (
        "asr-preprocess-"
        + sha256_bytes(canonical_bytes(expected_job_identity))[:32]
    )
    if order["job_id"] != expected_job_id:
        raise QueueError("wrapped ASR job identity does not commit to its handling boundary")
    normalized = _private_work_order_document(order, descriptor)
    if wrapper != normalized:
        raise QueueError("private queue work-order wrapper is not normalized")
    return order, normalized


def _asr_work_order(
    *,
    item: dict[str, Any],
    engine: dict[str, Any],
    model: dict[str, Any],
    asr_output_root: Path,
    handling: dict[str, Any] | None = None,
) -> dict[str, Any]:
    audio = item["audio"]
    job_identity = {
        "artifact_id": audio["artifact_id"],
        "audio_sha256": audio["sha256"],
        "parent_processing_run_id": audio["processing_run_id"],
        "pass": "raw_full_preprocess_artifact_small_en_v1",
        "coordinate_system": "artifact_local_milliseconds",
    }
    if handling is not None:
        control_entry = handling["preprocess_control_entry"]
        job_identity.update(
            {
                "handling_boundary_sha256": control_entry[
                    "handling_boundary_sha256"
                ],
                "seal_receipt_sha256": control_entry["seal_receipt_sha256"],
                "seal_plan_sha256": control_entry["seal_plan_sha256"],
            }
        )
    order = {
        "schema_version": 1,
        "job_id": "asr-preprocess-" + sha256_bytes(canonical_bytes(job_identity))[:32],
        "input": {
            "path": audio["path"],
            "expected_sha256": audio["sha256"],
            "media_id": audio["media_id"],
            "artifact_id": audio["artifact_id"],
            "parent_processing_run_id": audio["processing_run_id"],
        },
        "engine": {
            key: engine[key]
            for key in (
                "executable",
                "expected_sha256",
                "version_label",
                "version_evidence",
                "build",
            )
        },
        "model": {
            key: model[key]
            for key in (
                "path",
                "expected_sha256",
                "model_id",
                "name",
                "revision",
                "source",
                "license_label",
            )
        },
        "window": {"offset_ms": 0, "duration_ms": audio["duration_ms"]},
        "inference": dict(RAW_INFERENCE),
        "glossary": None,
        # A catalog row is evidence of artifact admission, not evidence that an
        # artifact millisecond is a recording millisecond.
        "catalog_context": None,
        "output": {"root": str(asr_output_root)},
    }
    try:
        normalized = asr_whispercpp.validate_work_order(order)
    except asr_whispercpp.ASRError as error:
        raise QueueError(f"generated ASR work order is invalid: {error}") from error
    if normalized != order:
        raise QueueError("generated ASR work order is not already in adapter-normalized form")
    return order


def _validate_roots(
    queue_root: Path,
    asr_output_root: Path,
    protected_paths: Iterable[Path],
) -> tuple[Path, Path]:
    queue_root = _private_root(queue_root, "queue root")
    asr_output_root = _private_root(asr_output_root, "ASR output root")
    if (
        queue_root == asr_output_root
        or queue_root in asr_output_root.parents
        or asr_output_root in queue_root.parents
    ):
        raise QueueError("queue root and ASR output root must be disjoint")
    for protected in protected_paths:
        if (
            queue_root == protected
            or queue_root in protected.parents
            or protected in queue_root.parents
            or asr_output_root == protected
            or asr_output_root in protected.parents
            or protected in asr_output_root.parents
        ):
            raise QueueError("queue/output roots may not overlap any controlling input")
    return queue_root, asr_output_root


def build_queue(
    *,
    queue_root: Path,
    asr_output_root: Path,
    engine_path: Path,
    model_path: Path,
    preprocess_bundle: Path | None = None,
    preprocess_state_root: Path | None = None,
    database_path: Path | None = None,
    result_paths: list[Path] | None = None,
) -> tuple[dict[str, Any], list[bytes]]:
    sealed_requested = preprocess_bundle is not None or preprocess_state_root is not None
    catalog_requested = database_path is not None or bool(result_paths)
    if sealed_requested == catalog_requested:
        raise QueueError("choose exactly one admission mode: sealed receipts or catalog results")
    if sealed_requested:
        if preprocess_bundle is None or preprocess_state_root is None:
            raise QueueError("sealed receipt mode requires both bundle and state root")
        bundle = _absolute_path(preprocess_bundle, "preprocess bundle", existing=True)
        state_root = _absolute_path(preprocess_state_root, "preprocess state root", existing=True)
        origin, items = _collect_sealed_items(bundle, state_root)
        protected = [bundle, state_root]
    else:
        if database_path is None or not result_paths:
            raise QueueError("catalog mode requires a database and preprocess result paths")
        database = _absolute_path(database_path, "catalog database", existing=True)
        origin, items = _collect_catalog_items(database, result_paths)
        protected = [database, *[Path(item["result"]["path"]) for item in items]]

    if not 1 <= len(items) <= MAX_ITEMS:
        raise QueueError(f"queue requires one to {MAX_ITEMS} admitted audio artifacts")
    private_control = _queue_handling_control(origin, items)
    protected.extend(
        Path(path)
        for item in items
        for path in (
            item["audio"]["path"],
            item["result"]["path"],
            item["source_media"]["path"],
        )
    )
    items.sort(
        key=lambda row: (
            row["audio"]["sha256"],
            row["audio"]["artifact_id"],
            row["result"]["sha256"],
            row["result"]["path"],
        )
    )
    if len({row["audio"]["artifact_id"] for row in items}) != len(items):
        raise QueueError("duplicate audio artifact IDs are forbidden")
    if len({row["audio"]["sha256"] for row in items}) != len(items):
        raise QueueError("duplicate audio content is forbidden within one ASR queue")
    total_bytes = sum(row["audio"]["byte_count"] for row in items)
    total_ms = sum(row["audio"]["duration_ms"] for row in items)
    if total_bytes > MAX_TOTAL_AUDIO_BYTES or total_ms > MAX_TOTAL_AUDIO_MS:
        raise QueueError("queue exceeds the cumulative private audio cap")

    engine_path = _absolute_path(engine_path, "whisper.cpp executable", existing=True)
    model_path = _absolute_path(model_path, "whisper.cpp model", existing=True)
    queue_root, asr_output_root = _validate_roots(
        queue_root,
        asr_output_root,
        [*protected, engine_path, model_path],
    )
    software = _software_document()
    for component in software.values():
        implementation_path = Path(component["path"])
        if (
            queue_root == implementation_path
            or queue_root in implementation_path.parents
            or asr_output_root == implementation_path
            or asr_output_root in implementation_path.parents
        ):
            raise QueueError("queue/output roots may not contain pipeline implementations")
    engine = _engine_document(engine_path)
    model = _model_document(model_path)

    entries: list[dict[str, Any]] = []
    work_order_bodies: list[bytes] = []
    for ordinal, item in enumerate(items, 1):
        order = _asr_work_order(
            item=item,
            engine=engine,
            model=model,
            asr_output_root=asr_output_root,
            handling=item.get("handling"),
        )
        document = (
            order
            if "handling" not in item
            else _private_work_order_document(order, item["handling"])
        )
        body = pretty_bytes(document)
        if len(body) > MAX_WORK_ORDER_BYTES:
            raise QueueError("generated ASR work order exceeds its byte cap")
        work_order_bodies.append(body)
        entries.append(
            {
                "ordinal": ordinal,
                "job_id": order["job_id"],
                "path": f"work-orders/{ordinal:06d}.json",
                "sha256": sha256_bytes(body),
                "canonical_sha256": sha256_bytes(canonical_bytes(document)),
                "byte_count": len(body),
                "audio_artifact": _audio_manifest_descriptor(item["audio"]),
                "preprocess_result": dict(item["result"]),
                "source_media": dict(item["source_media"]),
                "coordinate_provenance": _coordinate_provenance(item),
                "routing_hint": item["routing_hint"],
                "admission_evidence": item["evidence"],
                **(
                    {
                        "adapter_work_order_sha256": sha256_bytes(
                            canonical_bytes(order)
                        ),
                        "handling": item["handling"],
                    }
                    if "handling" in item
                    else {}
                ),
            }
        )
    if len({row["job_id"] for row in entries}) != len(entries):
        raise QueueError("generated ASR job identifiers collide")

    identity = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "materializer": MATERIALIZER_NAME,
        "origin": origin,
        "software": software,
        "engine": engine,
        "model": model,
        "profile": {
            "pass_kind": "raw_full_preprocess_artifact",
            "coordinate_system": "artifact_local_milliseconds",
            "window_offset_ms": 0,
            "duration_basis": "exact_preprocess_normalized_probe_full_duration",
            "recording_transform_state": "unresolved",
            "catalog_context_policy": "always_null_until_separate_translation_admission",
            "routing_hint_policy": "advisory_only_all_explicitly_admitted_audio_is_queued",
            "inference": dict(RAW_INFERENCE),
            "glossary": None,
        },
        "output": {
            "queue_root": str(queue_root),
            "asr_output_root": str(asr_output_root),
        },
        "work_orders": entries,
        **({"handling_control": private_control} if private_control is not None else {}),
        "safety": {
            **SAFETY,
            **(PRIVATE_SAFETY if private_control is not None else {}),
        },
    }
    identity_sha = sha256_bytes(canonical_bytes(identity))
    queue_id = f"asrppqueue_{identity_sha[:32]}"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "materializer": MATERIALIZER_NAME,
        "queue_id": queue_id,
        "identity_sha256": identity_sha,
        "queue_relative_path": f"queues/{queue_id}",
        "origin": origin,
        "software": software,
        "engine": engine,
        "model": model,
        "profile": identity["profile"],
        "output": identity["output"],
        "work_order_count": len(entries),
        "totals": {"audio_byte_count": total_bytes, "audio_duration_ms": total_ms},
        "work_orders": entries,
        **({"handling_control": private_control} if private_control is not None else {}),
        "safety": identity["safety"],
    }
    if len(pretty_bytes(manifest)) > MAX_MANIFEST_BYTES:
        raise QueueError("generated queue manifest exceeds its byte cap")
    return manifest, work_order_bodies


def _ensure_private_directory(path: Path, label: str) -> None:
    try:
        preprocess_batch.ensure_private_directory(path, label)
    except preprocess_batch.BatchError as error:
        raise QueueError(str(error)) from error


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_exclusive(path: Path, body: bytes, mode: int = 0o400) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)


def _remove_staging(path: Path) -> None:
    if not path.exists():
        return
    for current, directories, files in os.walk(path, topdown=False):
        current_path = Path(current)
        for name in files:
            candidate = current_path / name
            if not candidate.is_symlink():
                candidate.chmod(0o600)
            candidate.unlink()
        for name in directories:
            candidate = current_path / name
            if candidate.is_symlink():
                candidate.unlink()
            else:
                candidate.chmod(0o700)
                candidate.rmdir()
    path.chmod(0o700)
    path.rmdir()


def _verify_sealed_queue(
    manifest_path: Path,
    manifest: dict[str, Any],
    work_order_bodies: list[bytes],
) -> None:
    queue_dir = manifest_path.parent
    orders_dir = queue_dir / "work-orders"
    if (
        queue_dir.name != manifest["queue_id"]
        or queue_dir.parent.name != "queues"
        or queue_dir.parent.parent != Path(manifest["output"]["queue_root"])
    ):
        raise QueueError("queue manifest is outside its deterministic queue path")
    for path, label in ((queue_dir, "queue directory"), (orders_dir, "work-order directory")):
        observed = path.lstat()
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o500
        ):
            raise QueueError(f"sealed {label} must be a non-symlink mode-0500 directory")
    if {entry.name for entry in queue_dir.iterdir()} != {"manifest.json", "work-orders"}:
        raise QueueError("sealed queue directory has missing or extra entries")
    expected_names = {f"{ordinal:06d}.json" for ordinal in range(1, len(work_order_bodies) + 1)}
    if {entry.name for entry in orders_dir.iterdir()} != expected_names:
        raise QueueError("sealed queue work-order directory has missing or extra entries")
    manifest_file, manifest_body = _stable_readonly(
        manifest_path, MAX_MANIFEST_BYTES, "sealed queue manifest"
    )
    if manifest_file != manifest_path or manifest_body != pretty_bytes(manifest):
        raise QueueError("sealed queue manifest failed exact byte replay")
    if stat.S_IMODE(manifest_path.lstat().st_mode) != 0o400:
        raise QueueError("sealed queue manifest must have mode 0400")
    for ordinal, expected in enumerate(work_order_bodies, 1):
        path = orders_dir / f"{ordinal:06d}.json"
        observed_path, observed = _stable_readonly(
            path, MAX_WORK_ORDER_BYTES, f"sealed ASR work order {ordinal}"
        )
        if observed_path != path or observed != expected:
            raise QueueError(f"sealed ASR work order {ordinal} failed exact byte replay")
        if stat.S_IMODE(path.lstat().st_mode) != 0o400:
            raise QueueError(f"sealed ASR work order {ordinal} must have mode 0400")
        entry = manifest["work_orders"][ordinal - 1]
        if (
            entry["sha256"] != sha256_bytes(observed)
            or entry["byte_count"] != len(observed)
        ):
            raise QueueError(f"sealed ASR work order {ordinal} differs from its manifest pin")


def materialize_queue(**kwargs: Any) -> tuple[dict[str, Any], Path]:
    manifest, bodies = build_queue(**kwargs)
    root = Path(manifest["output"]["queue_root"])
    _ensure_private_directory(root, "queue root")
    queues = root / "queues"
    _ensure_private_directory(queues, "queue collection")
    lock_path = root / ".preprocess-asr-queue.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        lock_stat = os.fstat(descriptor)
        path_stat = lock_path.lstat()
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_nlink != 1
            or (lock_stat.st_dev, lock_stat.st_ino) != (path_stat.st_dev, path_stat.st_ino)
        ):
            raise QueueError("queue admission lock must be a single-link regular file")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        final = queues / manifest["queue_id"]
        manifest_path = final / "manifest.json"
        if final.exists():
            _verify_sealed_queue(manifest_path, manifest, bodies)
            return manifest, manifest_path
        staging = queues / f".{manifest['queue_id']}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            staging.mkdir(mode=0o700)
            orders_dir = staging / "work-orders"
            orders_dir.mkdir(mode=0o700)
            for ordinal, body in enumerate(bodies, 1):
                _write_exclusive(orders_dir / f"{ordinal:06d}.json", body)
            _write_exclusive(staging / "manifest.json", pretty_bytes(manifest))
            os.chmod(orders_dir, 0o500)
            _sync_directory(orders_dir)
            os.chmod(staging, 0o500)
            _sync_directory(staging)
            os.rename(staging, final)
            _sync_directory(queues)
        finally:
            _remove_staging(staging)
        _verify_sealed_queue(manifest_path, manifest, bodies)
        return manifest, manifest_path
    finally:
        os.close(descriptor)


MANIFEST_KEYS = {
    "schema_version",
    "implementation_version",
    "materializer",
    "queue_id",
    "identity_sha256",
    "queue_relative_path",
    "origin",
    "software",
    "engine",
    "model",
    "profile",
    "output",
    "work_order_count",
    "totals",
    "work_orders",
    "safety",
}


def validate_queue(manifest_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_path = _absolute_path(manifest_path, "queue manifest", existing=True)
    resolved, body = _stable_readonly(manifest_path, MAX_MANIFEST_BYTES, "queue manifest")
    parsed = parse_json(body, "queue manifest")
    manifest_keys = set(MANIFEST_KEYS)
    if isinstance(parsed, dict) and "handling_control" in parsed:
        manifest_keys.add("handling_control")
    supplied = _exact_object(parsed, "queue manifest", manifest_keys)
    expected_safety = {
        **SAFETY,
        **(
            PRIVATE_SAFETY
            if supplied.get("handling_control") is not None
            else {}
        ),
    }
    if (
        resolved != manifest_path
        or supplied["schema_version"] != SCHEMA_VERSION
        or supplied["implementation_version"] != IMPLEMENTATION_VERSION
        or supplied["materializer"] != MATERIALIZER_NAME
        or not isinstance(supplied["queue_id"], str)
        or not QUEUE_ID_RE.fullmatch(supplied["queue_id"])
        or supplied["queue_relative_path"] != f"queues/{supplied['queue_id']}"
        or supplied["safety"] != expected_safety
    ):
        raise QueueError("queue manifest contract, identity, or safety policy is unsupported")
    origin = supplied.get("origin")
    output = supplied.get("output")
    engine = supplied.get("engine")
    model = supplied.get("model")
    if not all(isinstance(row, dict) for row in (origin, output, engine, model)):
        raise QueueError("queue origin/output/engine/model blocks must be objects")
    common = {
        "queue_root": Path(output["queue_root"]),
        "asr_output_root": Path(output["asr_output_root"]),
        "engine_path": Path(engine["executable"]),
        "model_path": Path(model["path"]),
    }
    if origin.get("mode") == "sealed_preprocess_receipts":
        rebuilt, bodies = build_queue(
            **common,
            preprocess_bundle=Path(origin["preprocess_bundle"]["path"]),
            preprocess_state_root=Path(origin["state_root"]),
        )
    elif origin.get("mode") == "catalog_admitted_preprocess_results":
        rebuilt, bodies = build_queue(
            **common,
            database_path=Path(origin["catalog"]["database_path"]),
            result_paths=[Path(path) for path in origin["result_paths"]],
        )
    else:
        raise QueueError("queue origin mode is unsupported")
    if rebuilt != supplied or pretty_bytes(rebuilt) != body:
        raise QueueError("queue manifest failed deterministic evidence/input replay")
    _verify_sealed_queue(manifest_path, rebuilt, bodies)
    queue_documents: list[dict[str, Any]] = []
    for ordinal, order_body in enumerate(bodies, 1):
        raw = parse_json(order_body, f"ASR work order {ordinal}")
        entry = rebuilt["work_orders"][ordinal - 1]
        order, document = validate_queue_work_order(
            raw,
            entry=entry,
            manifest=rebuilt,
        )
        if pretty_bytes(document) != order_body:
            raise QueueError(f"ASR queue work order {ordinal} is not normalized")
        if sha256_bytes(canonical_bytes(document)) != entry["canonical_sha256"]:
            raise QueueError(
                f"ASR queue work order {ordinal} canonical hash differs from its pin"
            )
        expected_adapter_sha = entry.get(
            "adapter_work_order_sha256", entry["canonical_sha256"]
        )
        if sha256_bytes(canonical_bytes(order)) != expected_adapter_sha:
            raise QueueError(
                f"ASR adapter work order {ordinal} canonical hash differs from its pin"
            )
        if order["catalog_context"] is not None:
            raise QueueError("preprocess ASR work orders may not claim catalog coordinates")
        queue_documents.append(document)
    return rebuilt, queue_documents


def _summary(manifest: dict[str, Any], manifest_path: Path) -> dict[str, Any]:
    queue_dir = manifest_path.parent
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "validated",
        "materializer": MATERIALIZER_NAME,
        "queue_id": manifest["queue_id"],
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_bytes(pretty_bytes(manifest)),
        "work_order_count": manifest["work_order_count"],
        "totals": manifest["totals"],
        "work_order_paths": [
            str(queue_dir / entry["path"]) for entry in manifest["work_orders"]
        ],
        "coordinate_system": "artifact_local_milliseconds",
        "recording_transform_state": "unresolved",
        "catalog_context_policy": "always_null_until_separate_translation_admission",
        **(
            {"handling_control": manifest["handling_control"]}
            if "handling_control" in manifest
            else {}
        ),
        "safety": manifest["safety"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize private ASR work orders from admitted preprocess audio"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    materialize = subparsers.add_parser(
        "materialize", help="validate evidence and seal a deterministic private queue"
    )
    origin = materialize.add_mutually_exclusive_group(required=True)
    origin.add_argument("--preprocess-bundle", help="immutable preprocess bundle directory")
    origin.add_argument("--catalog", help="read-only catalog for admitted-result mode")
    materialize.add_argument("--preprocess-state-root")
    materialize.add_argument(
        "--preprocess-result",
        action="append",
        help=f"completed result.json; repeat up to {MAX_ITEMS} times in catalog mode",
    )
    materialize.add_argument("--queue-root", required=True)
    materialize.add_argument("--asr-output-root", required=True)
    materialize.add_argument("--engine", required=True)
    materialize.add_argument("--model", required=True)

    validate = subparsers.add_parser(
        "validate", help="replay every queue, evidence, software, model, and audio pin"
    )
    validate.add_argument("--manifest", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "materialize":
            if args.preprocess_bundle:
                if not args.preprocess_state_root or args.preprocess_result or args.catalog:
                    raise QueueError(
                        "sealed mode requires --preprocess-state-root and forbids catalog results"
                    )
                kwargs = {
                    "preprocess_bundle": Path(args.preprocess_bundle),
                    "preprocess_state_root": Path(args.preprocess_state_root),
                }
            else:
                if args.preprocess_state_root or not args.preprocess_result:
                    raise QueueError(
                        "catalog mode requires --preprocess-result and forbids preprocess state"
                    )
                kwargs = {
                    "database_path": Path(args.catalog),
                    "result_paths": [Path(path) for path in args.preprocess_result],
                }
            manifest, manifest_path = materialize_queue(
                queue_root=Path(args.queue_root),
                asr_output_root=Path(args.asr_output_root),
                engine_path=Path(args.engine),
                model_path=Path(args.model),
                **kwargs,
            )
            validate_queue(manifest_path)
        else:
            manifest_path = Path(args.manifest)
            manifest, _orders = validate_queue(manifest_path)
        sys.stdout.buffer.write(pretty_bytes(_summary(manifest, manifest_path)))
        return 0
    except (
        QueueError,
        preprocess_batch.BatchError,
        asr_whispercpp.ASRError,
        ResultImportError,
        OSError,
        sqlite3.Error,
    ) as error:
        failure = {
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


def _sqlite_authorizer(
    action: int,
    _one: str | None,
    _two: str | None,
    _database: str | None,
    _trigger: str | None,
) -> int:
    denied = {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_DETACH,
        sqlite3.SQLITE_REINDEX,
        sqlite3.SQLITE_ANALYZE,
    }
    return sqlite3.SQLITE_DENY if action in denied else sqlite3.SQLITE_OK


def _open_readonly_catalog(path: Path) -> sqlite3.Connection:
    path = _absolute_path(path, "catalog database", existing=True)
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_size < 1
        or observed.st_size > MAX_DATABASE_BYTES
    ):
        raise QueueError("catalog database is not a bounded regular file")
    try:
        connection = sqlite3.connect(
            f"{path.as_uri()}?mode=ro",
            uri=True,
            isolation_level=None,
            timeout=30,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.set_authorizer(_sqlite_authorizer)
        connection.execute("BEGIN")
    except sqlite3.Error as error:
        try:
            connection.close()
        except (NameError, sqlite3.Error):
            pass
        raise QueueError(f"cannot open a read-only catalog snapshot: {error}") from error
    return connection


def _query_rows(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...],
    label: str,
) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in connection.execute(query, parameters).fetchall()]
    except sqlite3.Error as error:
        raise QueueError(f"cannot query {label}: {error}") from error


def _one_row(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...],
    label: str,
) -> dict[str, Any]:
    rows = _query_rows(connection, query, parameters, label)
    if len(rows) != 1:
        raise QueueError(f"{label} must resolve to exactly one row; observed {len(rows)}")
    return rows[0]


def _strict_database_json(value: Any, label: str) -> Any:
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_RESULT_BYTES:
        raise QueueError(f"{label} is not bounded catalog JSON")
    return parse_json(value.encode("utf-8"), label)


def _catalog_timestamp(value: Any, label: str) -> datetime:
    text = _bounded_string(value, label, 128)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise QueueError(f"{label} is not an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise QueueError(f"{label} has no UTC offset")
    return parsed.astimezone(timezone.utc)


def _validated_catalog_source_probe(
    value: Any,
    *,
    source: dict[str, Any],
) -> dict[str, Any]:
    probe = _strict_database_json(value, "source media.ffprobe_json")
    if not isinstance(probe, dict) or probe.get("schema_version") != 1:
        raise QueueError("catalog source probe is not a schema-v1 object")
    if not isinstance(probe.get("format"), dict) or not isinstance(
        probe.get("streams"), list
    ):
        raise QueueError("catalog source probe has invalid format or streams")
    media = probe.get("media")
    if media is None:
        if not isinstance(probe.get("tool"), dict):
            raise QueueError("legacy catalog source probe has no tool object")
    elif (
        not isinstance(media, dict)
        or media.get("media_id") != source["media_id"]
        or media.get("sha256") != source["sha256"]
        or media.get("byte_count") != source["byte_count"]
    ):
        raise QueueError("catalog source probe identity differs from the result source")
    probed_duration = probe["format"].get("duration_ms")
    if (
        source["duration_ms"] is not None
        and probed_duration is not None
        and probed_duration != source["duration_ms"]
    ):
        raise QueueError("catalog source probe duration differs from the result source")
    return probe


def _catalog_media_snapshot(
    row: dict[str, Any], probe: dict[str, Any]
) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "media_id",
            "sha256",
            "byte_count",
            "media_kind",
            "mime_type",
            "container",
            "duration_ms",
            "first_cataloged_at",
            "integrity_state",
        )
    } | {"ffprobe_json_sha256": sha256_bytes(canonical_bytes(probe))}


def _catalog_location_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "media_location_id",
            "media_id",
            "storage_uri",
            "storage_class",
            "verified_at",
            "is_primary",
        )
    }


def _catalog_binding(connection: sqlite3.Connection, item: dict[str, Any]) -> dict[str, Any]:
    """Bind one validated result/audio descriptor to exact admitted rows."""

    raw = item["raw_result"]
    audio = item["audio"]
    source = item["source_media"]
    catalog_records = raw["catalog_records"]

    artifact = _one_row(
        connection,
        "SELECT * FROM artifacts WHERE artifact_id = ?",
        (audio["artifact_id"],),
        f"catalog artifact {audio['artifact_id']}",
    )
    producer_artifacts = [
        row for row in catalog_records["artifacts"] if row["artifact_id"] == audio["artifact_id"]
    ]
    if len(producer_artifacts) != 1:
        raise QueueError("result catalog handoff has no unique audio artifact row")
    producer_artifact = producer_artifacts[0]
    if _file_uri_path(artifact.get("storage_uri"), "catalog artifact storage_uri") != Path(
        audio["path"]
    ):
        raise QueueError("catalog artifact storage URI resolves to the wrong local file")
    expected_metadata = {
        "media_kind": "audio",
        "mime_type": "audio/flac",
        "normalized_probe": audio["normalized_probe"],
    }
    if (
        any(artifact.get(key) != producer_artifact.get(key) for key in (
            "artifact_id",
            "processing_run_id",
            "artifact_kind",
            "storage_uri",
            "sha256",
            "byte_count",
            "schema_version",
            "visibility",
        ))
        or _strict_database_json(artifact.get("metadata_json"), "artifact.metadata_json")
        != expected_metadata
    ):
        raise QueueError("catalog artifact differs from the completed result handoff")

    run = _one_row(
        connection,
        "SELECT * FROM processing_runs WHERE processing_run_id = ?",
        (audio["processing_run_id"],),
        "preprocess processing run",
    )
    result_run = raw["processing_run"]
    if (
        run.get("stage") != "media_preprocess"
        or run.get("status") != "completed"
        or run.get("implementation_version") != result_run["implementation_version"]
        or run.get("model_id") is not None
        or run.get("glossary_revision_id") is not None
        or run.get("random_seed") is not None
        or run.get("error_text") is not None
        or run.get("started_at") != result_run["started_at"]
        or run.get("completed_at") != result_run["completed_at"]
        or _strict_database_json(run.get("parameters_json"), "run.parameters_json")
        != result_run["parameters_json"]
        or _strict_database_json(run.get("environment_json"), "run.environment_json")
        != result_run["environment_json"]
    ):
        raise QueueError("catalog processing run differs from the completed result")

    run_input = _one_row(
        connection,
        "SELECT * FROM run_inputs WHERE processing_run_id = ? AND input_role = 'source_media'",
        (audio["processing_run_id"],),
        "preprocess source run input",
    )
    if (
        run_input.get("object_type") != "media"
        or run_input.get("object_id") != source["media_id"]
        or run_input.get("input_sha256") != source["sha256"]
    ):
        raise QueueError("catalog run input differs from the result source hash")

    source_media = _one_row(
        connection,
        "SELECT * FROM media_objects WHERE media_id = ?",
        (source["media_id"],),
        "preprocess source media object",
    )
    producer_source_media = [
        row
        for row in catalog_records["media_objects"]
        if row["media_id"] == source["media_id"]
    ]
    if len(producer_source_media) != 1:
        raise QueueError("result catalog handoff has no unique source media row")
    expected_source_media = producer_source_media[0]
    if any(
        expected_source_media.get(key) != source.get(key)
        for key in ("media_id", "sha256", "byte_count", "duration_ms", "first_cataloged_at")
    ):
        raise QueueError("result source summary differs from its catalog handoff")
    if (
        any(
            source_media.get(key) != expected_source_media.get(key)
            for key in ("media_id", "sha256", "byte_count", "media_kind", "integrity_state")
        )
        or source_media.get("integrity_state") != "verified"
        or any(
            expected_source_media.get(key) is not None
            and source_media.get(key) != expected_source_media.get(key)
            for key in ("mime_type", "container", "duration_ms")
        )
        or _catalog_timestamp(
            source_media.get("first_cataloged_at"), "source media.first_cataloged_at"
        )
        > _catalog_timestamp(
            expected_source_media.get("first_cataloged_at"),
            "result source media.first_cataloged_at",
        )
    ):
        raise QueueError("catalog source media differs from the completed result source")
    source_probe = _validated_catalog_source_probe(
        source_media.get("ffprobe_json"), source=source
    )
    source_location = _one_row(
        connection,
        "SELECT * FROM media_locations WHERE media_id = ? AND storage_uri = ?",
        (source["media_id"], source["storage_uri"]),
        "preprocess source media location",
    )
    producer_source_locations = [
        row
        for row in catalog_records["media_locations"]
        if row["media_id"] == source["media_id"]
        and row["storage_uri"] == source["storage_uri"]
    ]
    if len(producer_source_locations) != 1:
        raise QueueError("result catalog handoff has no unique source media location")
    expected_source_location = producer_source_locations[0]
    expected_verified_at = expected_source_location.get("verified_at")
    observed_verified_at = source_location.get("verified_at")
    if (
        expected_source_location.get("media_id") != source["media_id"]
        or expected_source_location.get("storage_uri") != source["storage_uri"]
        or expected_source_location.get("is_primary") != 1
        or source_location.get("media_id") != source["media_id"]
        or source_location.get("storage_uri") != source["storage_uri"]
        or source_location.get("storage_class")
        not in {expected_source_location.get("storage_class"), "local_hot_cache"}
        or source_location.get("is_primary") != 1
        or observed_verified_at is None
        or (
            expected_verified_at is not None
            and _catalog_timestamp(observed_verified_at, "source location.verified_at")
            < _catalog_timestamp(
                expected_verified_at, "result source location.verified_at"
            )
        )
    ):
        raise QueueError("catalog source location differs from the completed result source")

    media = _one_row(
        connection,
        "SELECT * FROM media_objects WHERE media_id = ?",
        (audio["media_id"],),
        "normalized audio media object",
    )
    producer_media = [
        row for row in catalog_records["media_objects"] if row["media_id"] == audio["media_id"]
    ]
    if len(producer_media) != 1:
        raise QueueError("result catalog handoff has no unique normalized audio media row")
    expected_media = producer_media[0]
    if (
        any(media.get(key) != expected_media.get(key) for key in (
            "media_id",
            "sha256",
            "byte_count",
            "media_kind",
            "mime_type",
            "container",
            "duration_ms",
            "first_cataloged_at",
            "integrity_state",
        ))
        or media.get("integrity_state") != "verified"
        or _strict_database_json(media.get("ffprobe_json"), "media.ffprobe_json")
        != audio["normalized_probe"]
    ):
        raise QueueError("catalog media object differs from the completed result audio")

    location = _one_row(
        connection,
        "SELECT * FROM media_locations WHERE media_id = ? AND storage_uri = ?",
        (audio["media_id"], audio["uri"]),
        "normalized audio location",
    )
    if (
        location.get("storage_class") != "local_derived"
        or location.get("is_primary") != 1
        or location.get("verified_at") is None
    ):
        raise QueueError("catalog audio location is not a verified primary local derivative")

    derivation = _one_row(
        connection,
        """
        SELECT * FROM media_derivations
        WHERE child_media_id = ? AND parent_media_id = ?
          AND derivation_kind = 'audio_normalization_16khz_mono_flac'
        """,
        (audio["media_id"], source["media_id"]),
        "normalized audio derivation",
    )
    if (
        derivation.get("processing_run_id") != audio["processing_run_id"]
        or not isinstance(
            _strict_database_json(derivation.get("metadata_json"), "derivation.metadata_json"),
            dict,
        )
    ):
        raise QueueError("catalog audio derivation differs from this preprocess run")

    renditions = _query_rows(
        connection,
        "SELECT * FROM renditions WHERE media_id = ? ORDER BY rendition_id",
        (audio["media_id"],),
        "normalized audio renditions",
    )
    normalized_renditions: list[dict[str, Any]] = []
    timeline_spans: list[dict[str, Any]] = []
    for rendition in renditions:
        rendition_metadata = _strict_database_json(
            rendition["metadata_json"], "rendition.metadata_json"
        )
        normalized_renditions.append({**rendition, "metadata_json": rendition_metadata})
        for span in _query_rows(
            connection,
            "SELECT * FROM timeline_map_spans WHERE rendition_id = ? ORDER BY ordinal",
            (rendition["rendition_id"],),
            "rendition timeline spans",
        ):
            timeline_spans.append(span)

    derivation_metadata = _strict_database_json(
        derivation["metadata_json"], "derivation.metadata_json replay"
    )
    snapshot = {
        "artifact": {
            key: artifact.get(key)
            for key in (
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "storage_uri",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
            )
        }
        | {"metadata_json_sha256": sha256_bytes(canonical_bytes(expected_metadata))},
        "processing_run": {
            key: run.get(key)
            for key in (
                "processing_run_id",
                "stage",
                "implementation_version",
                "model_id",
                "glossary_revision_id",
                "random_seed",
                "started_at",
                "completed_at",
                "status",
                "error_text",
            )
        }
        | {
            "parameters_json_sha256": sha256_bytes(
                canonical_bytes(result_run["parameters_json"])
            ),
            "environment_json_sha256": sha256_bytes(
                canonical_bytes(result_run["environment_json"])
            ),
        },
        "run_input": {
            key: run_input.get(key)
            for key in (
                "run_input_id",
                "processing_run_id",
                "object_type",
                "object_id",
                "input_role",
                "input_sha256",
            )
        },
        "source_media": _catalog_media_snapshot(source_media, source_probe),
        "source_location": _catalog_location_snapshot(source_location),
        "producer_source_media": _catalog_media_snapshot(
            expected_source_media, expected_source_media["ffprobe_json"]
        ),
        "producer_source_location": _catalog_location_snapshot(
            expected_source_location
        ),
        "media": _catalog_media_snapshot(media, audio["normalized_probe"]),
        "location": _catalog_location_snapshot(location),
        "derivation": {
            key: derivation.get(key)
            for key in (
                "parent_media_id",
                "child_media_id",
                "derivation_kind",
                "processing_run_id",
            )
        }
        | {
            "metadata_json_sha256": sha256_bytes(canonical_bytes(derivation_metadata))
        },
        "rendition_count": len(normalized_renditions),
        "renditions_sha256": sha256_bytes(canonical_bytes(normalized_renditions)),
        "timeline_span_count": len(timeline_spans),
        "timeline_spans_sha256": sha256_bytes(canonical_bytes(timeline_spans)),
    }
    return {
        "snapshot": snapshot,
        "binding_sha256": sha256_bytes(canonical_bytes(snapshot)),
    }


def _collect_catalog_items(
    database_path: Path,
    result_paths: list[Path],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not 1 <= len(result_paths) <= MAX_ITEMS:
        raise QueueError(f"catalog mode requires one to {MAX_ITEMS} preprocess results")
    normalized_paths = [
        _absolute_path(path, "preprocess result", existing=True) for path in result_paths
    ]
    if len(set(normalized_paths)) != len(normalized_paths):
        raise QueueError("duplicate preprocess result paths are forbidden")
    normalized_paths.sort()
    items = [_validated_result_item(path) for path in normalized_paths]
    database_path = _absolute_path(database_path, "catalog database", existing=True)
    connection = _open_readonly_catalog(database_path)
    try:
        for item in items:
            binding = _catalog_binding(connection, item)
            item["evidence"] = {
                "mode": "catalog_admitted_preprocess_result",
                "preprocess_result": dict(item["result"]),
                "source_media": dict(item["source_media"]),
                "catalog_binding": binding,
            }
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        if [row[0] for row in integrity] != ["ok"]:
            raise QueueError("catalog integrity_check did not return exactly 'ok'")
    except sqlite3.Error as error:
        raise QueueError(f"catalog verification failed: {error}") from error
    finally:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        connection.close()

    for item in items:
        _stable_hash(
            Path(item["audio"]["path"]),
            expected_sha256=item["audio"]["sha256"],
            expected_byte_count=item["audio"]["byte_count"],
            label=f"catalog-bound audio {item['audio']['artifact_id']} replay",
        )
        _, replay = _stable_readonly(
            Path(item["result"]["path"]), MAX_RESULT_BYTES, "catalog-bound result replay"
        )
        if sha256_bytes(replay) != item["result"]["sha256"]:
            raise QueueError("catalog-bound preprocess result changed during snapshot verification")

    binding_sha = sha256_bytes(
        canonical_bytes([item["evidence"]["catalog_binding"] for item in items])
    )
    origin = {
        "mode": "catalog_admitted_preprocess_results",
        "catalog": {
            "database_path": str(database_path),
            "access_mode": "read_only_snapshot",
            "query_contract_version": 1,
            "binding_sha256": binding_sha,
        },
        "result_paths": [str(path) for path in normalized_paths],
    }
    return origin, items


if __name__ == "__main__":
    raise SystemExit(main())
