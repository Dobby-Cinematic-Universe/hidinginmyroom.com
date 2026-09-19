#!/usr/bin/env python3
"""Restart-portable, contract-only GPU ASR work orders and result envelopes.

V1 through V4 are immutable historical evidence.  V5 deliberately contains no
inference entry point.  It creates and validates closed, content-addressed work
orders, validates completed-result envelopes, and reports whether the expected
result is absent or valid.  Execution remains the responsibility of the separately
admitted launcher and finite batch worker.

Durable placement is expressed by a portable-root registration and its Btrfs
filesystem UUID.  Kernel-local numeric filesystem observations are never part of
a work order, result, identity, or status response.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import secrets
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any


CONTRACT_VERSION = 5
IMPLEMENTATION_VERSION = "0.5.0"
WORK_ORDER_KIND = "himr_faster_whisper_gpu_work_order"
RESULT_KIND = "himr_faster_whisper_gpu_result"
CONTRACT_KIND = "himr_faster_whisper_gpu_contracts"
OUTPUT_CONTRACT = "private-faster-whisper-media-local-language-explicit-envelope-v5"
STAGE = "asr_faster_whisper_gpu"

MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_PATH_BYTES = 4096
MAX_TEXT_CHARACTERS = 16 * 1024 * 1024
MAX_TIMESTAMP_OVERRUN_MS = 2_000

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
WORK_ORDER_ID_RE = re.compile(r"^gpuasrwo5_[0-9a-f]{32}$")
RESULT_ID_RE = re.compile(r"^gpuasrresult5_[0-9a-f]{32}$")
ROOT_REGISTRATION_ID_RE = re.compile(r"^gpurootreg_[0-9a-f]{32}$")
PROFILE_ID_RE = re.compile(r"^gpuprofile_[0-9a-f]{32}$")
RUNTIME_RECEIPT_ID_RE = re.compile(r"^gpurtv2_[0-9a-f]{32}$")
GENERIC_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ARTIFACT_ID_RE = re.compile(r"^artifact_[0-9a-f]{32}$")
MEDIA_ID_RE = re.compile(r"^media_sha256_([0-9a-f]{64})$")
GPU_UUID_RE = re.compile(r"^GPU-[A-Za-z0-9-]{8,92}$")
UTC_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)

V4_SOURCE_SHA256 = "377d6ab33e970a9bbf40979f4a3832181b022fd31f726e22eef7b47ed0f26b79"
V1_SOURCE_SHA256 = "bb5a2557e693cea1daeaefa339b1f3aca2634f124ea72e2f29910331c5ba816c"
WORD_TIMING_FLAG_NAMES = (
    "precedes_segment_start",
    "extends_beyond_segment_end",
    "start_regresses_from_previous",
    "overlaps_previous",
)

MEDIA_FORMAT = {
    "container": "flac",
    "codec": "flac",
    "sample_rate_hz": 16_000,
    "channels": 1,
    "sample_format": "s16",
}

POLICY = {
    "visibility": "private",
    "machine_generated": True,
    "human_review_required": True,
    "scores_calibrated": False,
    "network_access": False,
    "inference_authority": "none_contract_only",
    "import_authority": "none",
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}

TRANSCRIPT_SEMANTICS = {
    "producer_coordinate_system": "media_ms",
    "source_offset_ms": 0,
    "recording_projection": "separate_reviewed_operation_required",
    "forced_language": "en",
    "language_selection_basis": "forced_by_inference_profile",
    "language_detection_performed": False,
    "forced_language_probability": None,
    "raw_model_word_times": "preserved",
    "finite_paired_word_times": "non_inverted_required",
    "word_vs_segment_or_previous": "retained_and_explicitly_flagged",
    "word_timing_flags": list(WORD_TIMING_FLAG_NAMES),
    "v4_semantics_source_sha256": V4_SOURCE_SHA256,
    "preserved_v1_source_sha256": V1_SOURCE_SHA256,
}

OUTPUT_LAYOUT = "asr/faster-whisper-gpu-v5/sha256-v1"
SOURCE_LINEAGE_PRODUCTION = "production_preprocess_v03"
SOURCE_LINEAGE_SYNTHETIC = "synthetic_canary"

WORK_ORDER_CORE_KEYS = frozenset(
    {
        "kind",
        "schema_version",
        "implementation_version",
        "job_id",
        "input",
        "source_lineage",
        "runtime_admission",
        "production_profile",
        "hot_root",
        "execution_contract",
        "transcript_semantics",
        "catalog_context",
        "output",
        "policy",
    }
)

RESULT_CORE_KEYS = frozenset(
    {
        "kind",
        "schema_version",
        "implementation_version",
        "status",
        "work_order",
        "runtime_admission",
        "production_profile",
        "input",
        "execution",
        "transcript",
        "artifacts",
        "policy",
    }
)


class ProductionASRV5Error(RuntimeError):
    """A v5 work order, result, or external binding is invalid."""


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
        raise ProductionASRV5Error(f"value is not canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _legacy_v03_canonical_bytes(value: Any) -> bytes:
    """Exact inherited preprocess-v03 encoding (intentionally no final newline)."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ProductionASRV5Error(
            f"preprocess-v03 value is not canonical JSON: {error}"
        ) from error


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProductionASRV5Error(f"JSON object contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise ProductionASRV5Error(f"JSON contains non-finite numeric constant {value}")


def parse_json_bytes(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ProductionASRV5Error(f"{label} is not strict UTF-8") from error
    try:
        return json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except ProductionASRV5Error:
        raise
    except (json.JSONDecodeError, ValueError) as error:
        raise ProductionASRV5Error(f"{label} is not strict JSON: {error}") from error


def _exact(value: Any, label: str, keys: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(keys):
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise ProductionASRV5Error(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ProductionASRV5Error(
            f"{label} must be an integer within [{minimum}, {maximum}]"
        )
    return value


def _number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProductionASRV5Error(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ProductionASRV5Error(
            f"{label} must be finite within [{minimum}, {maximum}]"
        )
    return result


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ProductionASRV5Error(f"{label} must be a lowercase SHA-256")
    return value


def _text(value: Any, label: str, maximum: int = 1024) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ProductionASRV5Error(f"{label} must be bounded nonempty text")
    return value


def normalized_absolute_path(value: Any, label: str) -> Path:
    if not isinstance(value, (str, os.PathLike)):
        raise ProductionASRV5Error(f"{label} must be a path")
    raw = os.fspath(value)
    if (
        not isinstance(raw, str)
        or not raw
        or len(raw.encode("utf-8")) > MAX_PATH_BYTES
        or "\x00" in raw
        or "\\" in raw
        or "//" in raw
        or not raw.startswith("/")
        or os.path.normpath(raw) != raw
        or raw == "/"
    ):
        raise ProductionASRV5Error(f"{label} must be one normalized absolute path")
    return Path(raw)


def _require_descendant(path: Path, root: Path, label: str, *, allow_equal: bool = False) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ProductionASRV5Error(f"{label} is outside the registered hot root") from error
    if not allow_equal and not relative.parts:
        raise ProductionASRV5Error(f"{label} must be below the registered hot root")


def _reference(value: Any, label: str) -> dict[str, Any]:
    item = _exact(value, label, {"path", "sha256"})
    return {
        "path": str(normalized_absolute_path(item["path"], f"{label}.path")),
        "sha256": _digest(item["sha256"], f"{label}.sha256"),
    }


def _validate_input(value: Any, hot_root_path: Path) -> dict[str, Any]:
    item = _exact(
        value,
        "input",
        {
            "path",
            "expected_sha256",
            "expected_byte_count",
            "expected_duration_ms",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "media_format",
            "sealed_mode",
            "timeline_offset_ms",
        },
    )
    path = normalized_absolute_path(item["path"], "input.path")
    _require_descendant(path, hot_root_path, "input.path")
    digest = _digest(item["expected_sha256"], "input.expected_sha256")
    media_id = _text(item["media_id"], "input.media_id", 96)
    match = MEDIA_ID_RE.fullmatch(media_id)
    if match is None or match.group(1) != digest:
        raise ProductionASRV5Error("input.media_id must derive from the input digest")
    artifact_id = _text(item["artifact_id"], "input.artifact_id", 96)
    if not ARTIFACT_ID_RE.fullmatch(artifact_id):
        raise ProductionASRV5Error("input.artifact_id is invalid")
    parent_run = _text(
        item["parent_processing_run_id"], "input.parent_processing_run_id", 256
    )
    if not GENERIC_ID_RE.fullmatch(parent_run):
        raise ProductionASRV5Error("input.parent_processing_run_id is invalid")
    media_format = _exact(item["media_format"], "input.media_format", set(MEDIA_FORMAT))
    if media_format != MEDIA_FORMAT:
        raise ProductionASRV5Error("input.media_format is not exact 16 kHz mono s16 FLAC")
    if item["sealed_mode"] not in {"0400", "0444"}:
        raise ProductionASRV5Error("input.sealed_mode must be 0400 or 0444")
    if item["timeline_offset_ms"] != 0:
        raise ProductionASRV5Error("v5 output is media-local; timeline_offset_ms must be zero")
    return {
        "path": str(path),
        "expected_sha256": digest,
        "expected_byte_count": _integer(
            item["expected_byte_count"], "input.expected_byte_count", 1, 2**63 - 1
        ),
        "expected_duration_ms": _integer(
            item["expected_duration_ms"], "input.expected_duration_ms", 1, 86_400_000
        ),
        "media_id": media_id,
        "artifact_id": artifact_id,
        "parent_processing_run_id": parent_run,
        "media_format": dict(MEDIA_FORMAT),
        "sealed_mode": item["sealed_mode"],
        "timeline_offset_ms": 0,
    }


def _validate_handling(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "source_lineage.handling",
        {
            "mode",
            "descriptor_sha256",
            "control_identity_sha256",
            "handling_boundary_sha256",
            "seal_receipt_sha256",
            "seal_plan_sha256",
            "source_byte_identity_claimed",
            "publication_authority",
        },
    )
    if item["mode"] not in {"none", "private_v30"}:
        raise ProductionASRV5Error("source lineage handling mode is unsupported")
    digest_fields = (
        "descriptor_sha256",
        "control_identity_sha256",
        "handling_boundary_sha256",
        "seal_receipt_sha256",
        "seal_plan_sha256",
    )
    if item["mode"] == "none":
        if any(item[name] is not None for name in digest_fields):
            raise ProductionASRV5Error("unrestricted lineage may not invent handling digests")
        normalized_digests = {name: None for name in digest_fields}
    else:
        normalized_digests = {
            name: _digest(item[name], f"source_lineage.handling.{name}")
            for name in digest_fields
        }
    if (
        item["source_byte_identity_claimed"] is not False
        or item["publication_authority"] != "none"
    ):
        raise ProductionASRV5Error("source handling safety policy is not exact")
    return {
        "mode": item["mode"],
        **normalized_digests,
        "source_byte_identity_claimed": False,
        "publication_authority": "none",
    }


def _validate_source_lineage(value: Any, hot_root_path: Path) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProductionASRV5Error("source_lineage must be an object")
    lineage_type = value.get("kind")
    if lineage_type == SOURCE_LINEAGE_PRODUCTION:
        item = _exact(
            value,
            "production preprocess lineage",
            {
                "kind",
                "implementation_version",
                "gpu_handoff",
                "bundle_manifest",
                "receipt",
                "preprocess_result",
                "handling",
                "corpus_authority",
            },
        )
        if item["implementation_version"] != "0.3.0":
            raise ProductionASRV5Error("production preprocess lineage must be v03")
        handoff = _exact(
            item["gpu_handoff"],
            "GPU handoff lineage",
            {
                "manifest_path",
                "manifest_sha256",
                "queue_id",
                "identity_sha256",
                "member_id",
                "member_identity_sha256",
                "queue_ordinal",
                "preprocess_ordinal",
            },
        )
        bundle = _exact(
            item["bundle_manifest"],
            "bundle_manifest",
            {"path", "sha256", "bundle_id", "identity_sha256", "manifest_sha256"},
        )
        receipt = _exact(
            item["receipt"],
            "preprocess receipt",
            {"path", "sha256", "receipt_id", "receipt_sha256", "ordinal"},
        )
        result = _exact(
            item["preprocess_result"],
            "preprocess result",
            {
                "path",
                "sha256",
                "byte_count",
                "processing_run_id",
                "recipe_sha256",
            },
        )
        paths = {
            "handoff": normalized_absolute_path(
                handoff["manifest_path"], "gpu_handoff.manifest_path"
            ),
            "bundle": normalized_absolute_path(bundle["path"], "bundle_manifest.path"),
            "receipt": normalized_absolute_path(receipt["path"], "receipt.path"),
            "result": normalized_absolute_path(result["path"], "preprocess_result.path"),
        }
        for name, path in paths.items():
            _require_descendant(path, hot_root_path, f"{name} lineage path")
        bundle_id = _text(bundle["bundle_id"], "bundle_manifest.bundle_id", 96)
        if not re.fullmatch(r"ppbatch_[0-9a-f]{32}", bundle_id):
            raise ProductionASRV5Error("bundle_manifest.bundle_id is invalid")
        receipt_id = _text(receipt["receipt_id"], "receipt.receipt_id", 96)
        if not re.fullmatch(r"ppreceipt_[0-9a-f]{32}", receipt_id):
            raise ProductionASRV5Error("receipt.receipt_id is invalid")
        processing_run_id = _text(
            result["processing_run_id"], "preprocess_result.processing_run_id", 256
        )
        if not GENERIC_ID_RE.fullmatch(processing_run_id):
            raise ProductionASRV5Error("preprocess result processing_run_id is invalid")
        if item["corpus_authority"] != "preprocess_receipt_lineage_only":
            raise ProductionASRV5Error("production source corpus authority is not exact")
        queue_id = _text(handoff["queue_id"], "gpu_handoff.queue_id", 96)
        member_id = _text(handoff["member_id"], "gpu_handoff.member_id", 96)
        if (
            not re.fullmatch(r"gpuasrqueue_[0-9a-f]{32}", queue_id)
            or not re.fullmatch(r"gpuasrmember_[0-9a-f]{32}", member_id)
        ):
            raise ProductionASRV5Error("GPU handoff queue or member ID is invalid")
        queue_ordinal = _integer(
            handoff["queue_ordinal"], "gpu_handoff.queue_ordinal", 1, 128
        )
        preprocess_ordinal = _integer(
            handoff["preprocess_ordinal"], "gpu_handoff.preprocess_ordinal", 1, 128
        )
        receipt_ordinal = _integer(receipt["ordinal"], "receipt.ordinal", 1, 128)
        if receipt_ordinal != preprocess_ordinal:
            raise ProductionASRV5Error("GPU handoff preprocess ordinal differs from receipt")
        return {
            "kind": SOURCE_LINEAGE_PRODUCTION,
            "implementation_version": "0.3.0",
            "gpu_handoff": {
                "manifest_path": str(paths["handoff"]),
                "manifest_sha256": _digest(
                    handoff["manifest_sha256"], "gpu_handoff.manifest_sha256"
                ),
                "queue_id": queue_id,
                "identity_sha256": _digest(
                    handoff["identity_sha256"], "gpu_handoff.identity_sha256"
                ),
                "member_id": member_id,
                "member_identity_sha256": _digest(
                    handoff["member_identity_sha256"],
                    "gpu_handoff.member_identity_sha256",
                ),
                "queue_ordinal": queue_ordinal,
                "preprocess_ordinal": preprocess_ordinal,
            },
            "bundle_manifest": {
                "path": str(paths["bundle"]),
                "sha256": _digest(bundle["sha256"], "bundle_manifest.sha256"),
                "bundle_id": bundle_id,
                "identity_sha256": _digest(
                    bundle["identity_sha256"], "bundle_manifest.identity_sha256"
                ),
                "manifest_sha256": _digest(
                    bundle["manifest_sha256"], "bundle_manifest.manifest_sha256"
                ),
            },
            "receipt": {
                "path": str(paths["receipt"]),
                "sha256": _digest(receipt["sha256"], "receipt.sha256"),
                "receipt_id": receipt_id,
                "receipt_sha256": _digest(
                    receipt["receipt_sha256"], "receipt.receipt_sha256"
                ),
                "ordinal": receipt_ordinal,
            },
            "preprocess_result": {
                "path": str(paths["result"]),
                "sha256": _digest(result["sha256"], "preprocess_result.sha256"),
                "byte_count": _integer(
                    result["byte_count"], "preprocess_result.byte_count", 1, 64 * 1024**2
                ),
                "processing_run_id": processing_run_id,
                "recipe_sha256": _digest(
                    result["recipe_sha256"], "preprocess_result.recipe_sha256"
                ),
            },
            "handling": _validate_handling(item["handling"]),
            "corpus_authority": "preprocess_receipt_lineage_only",
        }
    if lineage_type == SOURCE_LINEAGE_SYNTHETIC:
        item = _exact(
            value,
            "synthetic canary lineage",
            {
                "kind",
                "fixture_manifest",
                "fixture_case_id",
                "contains_corpus_media",
                "scope",
                "corpus_authority",
            },
        )
        fixture = _exact(
            item["fixture_manifest"],
            "fixture_manifest",
            {"path", "sha256", "identity_sha256", "fixture_id"},
        )
        path = normalized_absolute_path(fixture["path"], "fixture_manifest.path")
        _require_descendant(path, hot_root_path, "fixture_manifest.path")
        fixture_id = _text(fixture["fixture_id"], "fixture_manifest.fixture_id", 128)
        case_id = _text(item["fixture_case_id"], "fixture_case_id", 128)
        if not GENERIC_ID_RE.fullmatch(fixture_id) or not GENERIC_ID_RE.fullmatch(case_id):
            raise ProductionASRV5Error("synthetic fixture or case ID is invalid")
        if (
            item["contains_corpus_media"] is not False
            or item["scope"] != "purpose_built_synthetic_only"
            or item["corpus_authority"] != "none"
        ):
            raise ProductionASRV5Error("synthetic canary may not claim corpus authority")
        return {
            "kind": SOURCE_LINEAGE_SYNTHETIC,
            "fixture_manifest": {
                "path": str(path),
                "sha256": _digest(fixture["sha256"], "fixture_manifest.sha256"),
                "identity_sha256": _digest(
                    fixture["identity_sha256"], "fixture_manifest.identity_sha256"
                ),
                "fixture_id": fixture_id,
            },
            "fixture_case_id": case_id,
            "contains_corpus_media": False,
            "scope": "purpose_built_synthetic_only",
            "corpus_authority": "none",
        }
    raise ProductionASRV5Error("source_lineage.kind is unsupported")


def _validate_runtime_reference(value: Any, hot_root_path: Path) -> dict[str, Any]:
    item = _exact(
        value,
        "runtime_admission",
        {"receipt_path", "receipt_sha256", "receipt_id", "identity_sha256", "status"},
    )
    path = normalized_absolute_path(item["receipt_path"], "runtime receipt path")
    _require_descendant(path, hot_root_path, "runtime receipt path")
    receipt_id = _text(item["receipt_id"], "runtime receipt ID", 96)
    if not RUNTIME_RECEIPT_ID_RE.fullmatch(receipt_id):
        raise ProductionASRV5Error("runtime admission receipt ID is invalid")
    if item["status"] not in {"candidate", "admitted"}:
        raise ProductionASRV5Error("runtime admission status is unsupported")
    return {
        "receipt_path": str(path),
        "receipt_sha256": _digest(item["receipt_sha256"], "runtime receipt SHA-256"),
        "receipt_id": receipt_id,
        "identity_sha256": _digest(item["identity_sha256"], "runtime identity"),
        "status": item["status"],
    }


def _validate_profile_reference(value: Any, hot_root_path: Path) -> dict[str, Any]:
    item = _exact(
        value,
        "production_profile",
        {"path", "sha256", "profile_id", "identity_sha256"},
    )
    path = normalized_absolute_path(item["path"], "production profile path")
    _require_descendant(path, hot_root_path, "production profile path")
    profile_id = _text(item["profile_id"], "production profile ID", 96)
    if not PROFILE_ID_RE.fullmatch(profile_id):
        raise ProductionASRV5Error("production profile ID is invalid")
    return {
        "path": str(path),
        "sha256": _digest(item["sha256"], "production profile SHA-256"),
        "profile_id": profile_id,
        "identity_sha256": _digest(item["identity_sha256"], "profile identity"),
    }


def _validate_hot_root(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "hot_root",
        {
            "registration_path",
            "registration_sha256",
            "registration_id",
            "identity_sha256",
            "root_id",
            "path",
            "filesystem_uuid",
            "tier",
        },
    )
    registration_path = normalized_absolute_path(
        item["registration_path"], "hot_root.registration_path"
    )
    root_path = normalized_absolute_path(item["path"], "hot_root.path")
    registration_id = _text(item["registration_id"], "hot root registration ID", 96)
    if not ROOT_REGISTRATION_ID_RE.fullmatch(registration_id):
        raise ProductionASRV5Error("hot root registration ID is invalid")
    root_id = _text(item["root_id"], "hot root ID", 128)
    if not re.fullmatch(r"^[a-z][a-z0-9._-]{0,127}$", root_id):
        raise ProductionASRV5Error("hot root ID is invalid")
    filesystem_uuid = _text(item["filesystem_uuid"], "hot root filesystem UUID", 36)
    if not re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        filesystem_uuid,
    ):
        raise ProductionASRV5Error("hot root filesystem UUID is invalid")
    if item["tier"] != "hot_main_drive":
        raise ProductionASRV5Error("work orders require the hot_main_drive tier")
    return {
        "registration_path": str(registration_path),
        "registration_sha256": _digest(
            item["registration_sha256"], "hot root registration SHA-256"
        ),
        "registration_id": registration_id,
        "identity_sha256": _digest(item["identity_sha256"], "hot root identity"),
        "root_id": root_id,
        "path": str(root_path),
        "filesystem_uuid": filesystem_uuid,
        "tier": "hot_main_drive",
    }


def _normalize_execution_contract(value: Any, profile: dict[str, Any]) -> dict[str, Any]:
    item = _exact(
        value,
        "execution_contract",
        {"decoding", "item_limits", "batch_limits", "telemetry", "scheduler"},
    )
    expected = {
        "decoding": profile["decoding"],
        "item_limits": profile["item_limits"],
        "batch_limits": profile["batch_limits"],
        "telemetry": profile["telemetry"],
        "scheduler": profile["scheduler"],
    }
    if item != expected:
        raise ProductionASRV5Error("execution contract does not derive exactly from profile v2")
    return json.loads(canonical_bytes(expected))


def execution_contract_from_profile(profile: dict[str, Any]) -> dict[str, Any]:
    """Return the sole per-item projection of a validated production profile."""

    profile_module = _load_local_module(
        "himr_gpu_profile_for_asr_v5", "production_profile_v2.py"
    )
    try:
        normalized = profile_module.validate_profile(profile)
    except Exception as error:
        raise ProductionASRV5Error(f"production profile is invalid: {error}") from error
    return {
        "decoding": normalized["decoding"],
        "item_limits": normalized["item_limits"],
        "batch_limits": normalized["batch_limits"],
        "telemetry": normalized["telemetry"],
        "scheduler": normalized["scheduler"],
    }


def normalize_work_order_core(
    value: Any, *, profile_document: dict[str, Any]
) -> dict[str, Any]:
    item = _exact(value, "work-order core", WORK_ORDER_CORE_KEYS)
    if (
        item["kind"] != WORK_ORDER_KIND
        or item["schema_version"] != CONTRACT_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
    ):
        raise ProductionASRV5Error("v5 work-order header is unsupported")
    job_id = _text(item["job_id"], "job_id", 128)
    if not JOB_ID_RE.fullmatch(job_id):
        raise ProductionASRV5Error("job_id contains unsupported characters")
    hot_root = _validate_hot_root(item["hot_root"])
    hot_root_path = Path(hot_root["path"])
    profile = _validate_profile_reference(item["production_profile"], hot_root_path)
    if profile_document.get("identity_sha256") != profile["identity_sha256"]:
        raise ProductionASRV5Error("profile document identity differs from its reference")
    profile_module = _load_local_module(
        "himr_gpu_profile_for_asr_v5", "production_profile_v2.py"
    )
    try:
        profile_document = profile_module.validate_profile(profile_document)
    except Exception as error:
        raise ProductionASRV5Error(f"production profile failed validation: {error}") from error
    runtime = _validate_runtime_reference(item["runtime_admission"], hot_root_path)
    source = _validate_source_lineage(item["source_lineage"], hot_root_path)
    # A v5 work order is contract-only and grants no execution authority.  Both
    # admitted and candidate receipts may therefore bind production lineage;
    # the batch execution class and trusted launcher decide whether that exact
    # combination may execute.
    input_item = _validate_input(item["input"], hot_root_path)
    if source["kind"] == SOURCE_LINEAGE_PRODUCTION:
        if (
            source["preprocess_result"]["processing_run_id"]
            != input_item["parent_processing_run_id"]
        ):
            raise ProductionASRV5Error(
                "input processing run differs from preprocess result lineage"
            )
    limits = profile_document["item_limits"]
    if input_item["expected_byte_count"] > limits["maximum_audio_bytes"]:
        raise ProductionASRV5Error("input exceeds profile maximum_audio_bytes")
    if input_item["expected_duration_ms"] > round(
        limits["maximum_audio_seconds"] * 1000
    ):
        raise ProductionASRV5Error("input exceeds profile maximum_audio_seconds")
    if item["transcript_semantics"] != TRANSCRIPT_SEMANTICS:
        raise ProductionASRV5Error("transcript semantics are not the exact v5 contract")
    if item["catalog_context"] is not None:
        raise ProductionASRV5Error("v5 producer output requires null catalog_context")
    output = _exact(item["output"], "output", {"root", "layout", "atomic_no_replace"})
    output_root = normalized_absolute_path(output["root"], "output.root")
    _require_descendant(output_root, hot_root_path, "output.root")
    if output["layout"] != OUTPUT_LAYOUT or output["atomic_no_replace"] is not True:
        raise ProductionASRV5Error("output layout or write policy is unsupported")
    controlling_paths = [
        Path(input_item["path"]),
        Path(runtime["receipt_path"]),
        Path(profile["path"]),
        Path(hot_root["registration_path"]),
    ]
    if source["kind"] == SOURCE_LINEAGE_PRODUCTION:
        controlling_paths.extend(
            [
                Path(source["gpu_handoff"]["manifest_path"]),
                Path(source["bundle_manifest"]["path"]),
                Path(source["receipt"]["path"]),
                Path(source["preprocess_result"]["path"]),
            ]
        )
    else:
        controlling_paths.append(Path(source["fixture_manifest"]["path"]))
    if any(path == output_root or output_root in path.parents for path in controlling_paths):
        raise ProductionASRV5Error("output.root may not contain a controlling input")
    if item["policy"] != POLICY:
        raise ProductionASRV5Error("work-order safety policy is not exact")
    return {
        "kind": WORK_ORDER_KIND,
        "schema_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "job_id": job_id,
        "input": input_item,
        "source_lineage": source,
        "runtime_admission": runtime,
        "production_profile": profile,
        "hot_root": hot_root,
        "execution_contract": _normalize_execution_contract(
            item["execution_contract"], profile_document
        ),
        "transcript_semantics": json.loads(canonical_bytes(TRANSCRIPT_SEMANTICS)),
        "catalog_context": None,
        "output": {
            "root": str(output_root),
            "layout": OUTPUT_LAYOUT,
            "atomic_no_replace": True,
        },
        "policy": dict(POLICY),
    }


def make_work_order(
    core: Any,
    *,
    profile_document: dict[str, Any],
    replay_bindings: bool = False,
) -> dict[str, Any]:
    normalized = normalize_work_order_core(core, profile_document=profile_document)
    identity = sha256_bytes(canonical_bytes(normalized))
    result = {
        **normalized,
        "identity_sha256": identity,
        "work_order_id": f"gpuasrwo5_{identity[:32]}",
    }
    if replay_bindings:
        replay_external_bindings(result, profile_document=profile_document)
    return result


def validate_work_order(
    value: Any,
    *,
    profile_document: dict[str, Any],
    replay_bindings: bool = False,
) -> dict[str, Any]:
    item = _exact(
        value,
        "work order",
        WORK_ORDER_CORE_KEYS | {"identity_sha256", "work_order_id"},
    )
    expected = make_work_order(
        {key: item[key] for key in WORK_ORDER_CORE_KEYS},
        profile_document=profile_document,
        replay_bindings=False,
    )
    if item != expected:
        raise ProductionASRV5Error("work order is noncanonical or its identity is invalid")
    if replay_bindings:
        replay_external_bindings(expected, profile_document=profile_document)
    return expected


def _load_local_module(name: str, filename: str) -> ModuleType:
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    path = Path(__file__).resolve().with_name(filename)
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ProductionASRV5Error(f"cannot load local dependency {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


def _stable_file_bytes(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
    expected_sha256: str | None = None,
    current_user_modes: frozenset[int] = frozenset({0o400}),
) -> bytes:
    if (
        not isinstance(current_user_modes, frozenset)
        or not current_user_modes
        or not current_user_modes <= frozenset({0o400, 0o440, 0o444})
    ):
        raise ProductionASRV5Error(
            f"{label} current-user mode policy is invalid"
        )
    allowed_owner_modes = {
        (os.geteuid(), mode) for mode in current_user_modes
    } | {(0, 0o444)}
    path = normalized_absolute_path(path, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        initial = os.fstat(descriptor)
        lexical = path.lstat()
        identity = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_uid,
            value.st_gid,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        if (
            not stat.S_ISREG(initial.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or initial.st_nlink != 1
            or (
                initial.st_uid,
                stat.S_IMODE(initial.st_mode),
            )
            not in allowed_owner_modes
            or not 1 <= initial.st_size <= maximum_bytes
            or identity(initial) != identity(lexical)
        ):
            raise ProductionASRV5Error(f"{label} metadata is unsafe")
        parts: list[bytes] = []
        remaining = initial.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise ProductionASRV5Error(f"{label} ended before its sealed size")
            parts.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ProductionASRV5Error(f"{label} grew while it was read")
        final = os.fstat(descriptor)
        current = path.lstat()
        if identity(initial) != identity(final) or identity(initial) != identity(current):
            raise ProductionASRV5Error(f"{label} changed while it was read")
    finally:
        os.close(descriptor)
    body = b"".join(parts)
    if expected_sha256 is not None and sha256_bytes(body) != _digest(
        expected_sha256, f"{label} expected SHA-256"
    ):
        raise ProductionASRV5Error(f"{label} SHA-256 differs")
    return body


def load_profile_document(path_value: str | Path, expected_sha256: str) -> dict[str, Any]:
    path = normalized_absolute_path(path_value, "production profile path")
    body = _stable_file_bytes(
        path,
        label="production profile",
        maximum_bytes=1024 * 1024,
        expected_sha256=expected_sha256,
    )
    value = parse_json_bytes(body, "production profile")
    if body != canonical_bytes(value):
        raise ProductionASRV5Error("production profile is not canonical JSON")
    profile_module = _load_local_module(
        "himr_gpu_profile_for_asr_v5", "production_profile_v2.py"
    )
    try:
        return profile_module.validate_profile(value)
    except Exception as error:
        raise ProductionASRV5Error(f"production profile is invalid: {error}") from error


def hot_root_reference(
    registration_path_value: str | Path, registration_sha256: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Replay a portable root registration and return its compact v5 reference."""

    portable = _load_local_module("himr_gpu_portable_root_for_asr_v5", "portable_root.py")
    path = normalized_absolute_path(registration_path_value, "root registration path")
    try:
        registration_info = path.lstat()
        owner_mode = (
            registration_info.st_uid,
            stat.S_IMODE(registration_info.st_mode),
        )
        if owner_mode not in {(os.geteuid(), 0o400), (0, 0o444)}:
            raise ProductionASRV5Error(
                "root registration must be current-user mode 0400 or root-owned mode 0444"
            )
        registration = portable.load_registration(
            path,
            _digest(registration_sha256, "root registration SHA-256"),
            expected_tier="hot_main_drive",
            expected_document_uid=owner_mode[0],
            expected_document_mode=owner_mode[1],
        )
        with portable.RetainedRoot.open(
            registration,
            expected_root_id=registration["root_id"],
            expected_tier="hot_main_drive",
        ) as retained:
            retained.verify()
    except Exception as error:
        raise ProductionASRV5Error(f"hot-root registration replay failed: {error}") from error
    return (
        {
            "registration_path": str(path),
            "registration_sha256": registration_sha256,
            "registration_id": registration["registration_id"],
            "identity_sha256": registration["identity_sha256"],
            "root_id": registration["root_id"],
            "path": registration["path"],
            "filesystem_uuid": registration["filesystem"]["uuid"],
            "tier": "hot_main_drive",
        },
        registration,
    )


def _verify_output_directory(registration: dict[str, Any], path_value: str | Path) -> None:
    """Retain the output chain beneath the hot root for this replay only."""

    portable = _load_local_module("himr_gpu_portable_root_for_asr_v5", "portable_root.py")
    path = normalized_absolute_path(path_value, "output root")
    root_path = Path(registration["path"])
    _require_descendant(path, root_path, "output root")
    relative = path.relative_to(root_path)
    try:
        retained_context = portable.RetainedRoot.open(
            registration,
            expected_root_id=registration["root_id"],
            expected_tier="hot_main_drive",
        )
    except Exception as error:
        raise ProductionASRV5Error(f"cannot retain hot root for output replay: {error}") from error
    descriptors: list[int] = []
    with retained_context as retained:
        parent = retained.descriptor
        try:
            for ordinal, component in enumerate(relative.parts, 1):
                flags = (
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NONBLOCK", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_DIRECTORY", 0)
                )
                descriptor = os.open(component, flags, dir_fd=parent)
                descriptors.append(descriptor)
                observed = os.fstat(descriptor)
                is_final = ordinal == len(relative.parts)
                if (
                    not stat.S_ISDIR(observed.st_mode)
                    or observed.st_uid != registration["owner"]["uid"]
                    or stat.S_IMODE(observed.st_mode) & 0o022
                    or (is_final and stat.S_IMODE(observed.st_mode) != 0o700)
                ):
                    raise ProductionASRV5Error(
                        "output root chain owner or mode is unsafe"
                    )
                portable.require_live_same_filesystem(
                    retained, descriptor, "v5 output root"
                )
                parent = descriptor
            retained.verify()
        except Exception as error:
            if isinstance(error, ProductionASRV5Error):
                raise
            raise ProductionASRV5Error(f"output root replay failed: {error}") from error
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)


def runtime_admission_reference(
    receipt_path_value: str | Path,
    receipt_sha256: str,
    *,
    require_admitted: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    admission = _load_local_module(
        "himr_gpu_runtime_admission_v2_for_asr_v5", "admit_runtime_v2.py"
    )
    path = normalized_absolute_path(receipt_path_value, "runtime admission receipt path")
    try:
        receipt = admission.load_receipt(
            path,
            _digest(receipt_sha256, "runtime receipt SHA-256"),
            require_admitted=require_admitted,
            deep_image=False,
        )
    except Exception as error:
        raise ProductionASRV5Error(f"runtime admission replay failed: {error}") from error
    return (
        {
            "receipt_path": str(path),
            "receipt_sha256": receipt_sha256,
            "receipt_id": receipt["receipt_id"],
            "identity_sha256": receipt["identity_sha256"],
            "status": receipt["status"],
        },
        receipt,
    )


def profile_reference(
    profile_path_value: str | Path, profile_sha256: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    path = normalized_absolute_path(profile_path_value, "production profile path")
    profile = load_profile_document(path, profile_sha256)
    return (
        {
            "path": str(path),
            "sha256": profile_sha256,
            "profile_id": profile["profile_id"],
            "identity_sha256": profile["identity_sha256"],
        },
        profile,
    )


def source_lineage_from_preprocess_descriptor(
    entry: Any, manifest: Any
) -> dict[str, Any]:
    """Convert one exact preprocess-v03 queue entry without dropping handling pins."""

    entry = _exact(
        entry,
        "GPU handoff queue member",
        {
            "ordinal",
            "preprocess_ordinal",
            "member_id",
            "identity_sha256",
            "audio",
            "resource_disposition",
            "lineage",
            "routing_hint",
            "private_handling",
        },
    )
    manifest = _exact(
        manifest,
        "GPU handoff queue manifest",
        {
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
        },
    )
    if (
        manifest["kind"] != "himr_preprocess_gpu_asr_queue"
        or manifest["schema_version"] != 1
        or manifest["implementation_version"] != "0.1.0"
        or manifest["materializer"] != "himr-preprocess-gpu-asr-queue"
        or not isinstance(manifest["members"], list)
        or manifest["members"].count(entry) != 1
    ):
        raise ProductionASRV5Error("member is not unique in an exact GPU handoff queue")
    manifest_core = {
        key: manifest[key]
        for key in manifest
        if key not in {"queue_id", "identity_sha256", "queue_relative_path"}
    }
    manifest_identity = sha256_bytes(canonical_bytes(manifest_core))
    if (
        manifest["identity_sha256"] != manifest_identity
        or manifest["queue_id"] != f"gpuasrqueue_{manifest_identity[:32]}"
        or manifest["queue_relative_path"]
        != f"queues/gpuasrqueue_{manifest_identity[:32]}"
    ):
        raise ProductionASRV5Error("GPU handoff manifest identity is invalid")
    safety = manifest["safety"]
    if (
        not isinstance(safety, dict)
        or safety.get("gpu_execution_authority") != "none"
        or safety.get("result_import_authority") != "none"
        or safety.get("publication_authority") != "none"
        or safety.get("catalogue_mutation_authority") != "none"
        or safety.get("archive_authority") != "none"
        or safety.get("deletion_authority") != "none"
    ):
        raise ProductionASRV5Error("GPU handoff safety policy is not non-authoritative")
    member_core = {
        key: entry[key]
        for key in (
            "ordinal",
            "preprocess_ordinal",
            "audio",
            "resource_disposition",
            "lineage",
            "routing_hint",
            "private_handling",
        )
    }
    member_identity = sha256_bytes(canonical_bytes(member_core))
    if (
        entry["identity_sha256"] != member_identity
        or entry["member_id"] != f"gpuasrmember_{member_identity[:32]}"
    ):
        raise ProductionASRV5Error("GPU handoff member identity is invalid")
    disposition = _exact(
        entry["resource_disposition"],
        "GPU handoff resource disposition",
        {"state", "reasons", "evaluated_against", "chunk_plan"},
    )
    if (
        disposition["state"] != "ready"
        or disposition["reasons"] != []
        or disposition["chunk_plan"] is not None
    ):
        raise ProductionASRV5Error(
            "only ready GPU handoff members can become v5 work orders"
        )
    profile_block = _exact(
        manifest["production_profile"],
        "GPU handoff production profile",
        {"reference", "document"},
    )
    profile_module = _load_local_module(
        "himr_gpu_profile_for_asr_v5", "production_profile_v2.py"
    )
    try:
        queue_profile = profile_module.validate_profile(profile_block["document"])
    except Exception as error:
        raise ProductionASRV5Error(f"GPU handoff profile is invalid: {error}") from error
    profile_reference_value = profile_block["reference"]
    if (
        not isinstance(profile_reference_value, dict)
        or profile_reference_value.get("profile_id") != queue_profile["profile_id"]
        or profile_reference_value.get("identity_sha256")
        != queue_profile["identity_sha256"]
        or profile_reference_value.get("physical_sha256")
        != sha256_bytes(canonical_bytes(queue_profile))
    ):
        raise ProductionASRV5Error("GPU handoff profile reference differs from its document")
    evaluated = _exact(
        disposition["evaluated_against"],
        "GPU handoff evaluated profile",
        {
            "profile_id",
            "profile_identity_sha256",
            "maximum_audio_bytes",
            "maximum_audio_seconds",
        },
    )
    if evaluated != {
        "profile_id": queue_profile["profile_id"],
        "profile_identity_sha256": queue_profile["identity_sha256"],
        "maximum_audio_bytes": queue_profile["item_limits"]["maximum_audio_bytes"],
        "maximum_audio_seconds": queue_profile["item_limits"]["maximum_audio_seconds"],
    }:
        raise ProductionASRV5Error("GPU handoff resource disposition binds another profile")
    try:
        lineage = entry["lineage"]
        bundle = lineage["preprocess_bundle"]
        receipt = lineage["receipt"]
        result = lineage["preprocess_result"]
    except (KeyError, TypeError) as error:
        raise ProductionASRV5Error("preprocess v03 entry lacks sealed receipt lineage") from error
    if receipt.get("ordinal") != entry["preprocess_ordinal"]:
        raise ProductionASRV5Error("member preprocess ordinal differs from its receipt")
    handling_descriptor = entry["private_handling"]
    if handling_descriptor is None:
        control = manifest["handling_control"]
        if isinstance(control, dict) and any(
            isinstance(row, dict)
            and row.get("ordinal") == entry["preprocess_ordinal"]
            for row in control.get("entries", [])
        ):
            raise ProductionASRV5Error("GPU handoff member drops private handling")
        handling = {
            "mode": "none",
            "descriptor_sha256": None,
            "control_identity_sha256": None,
            "handling_boundary_sha256": None,
            "seal_receipt_sha256": None,
            "seal_plan_sha256": None,
            "source_byte_identity_claimed": False,
            "publication_authority": "none",
        }
    else:
        try:
            control = manifest["handling_control"]
            control_entry = handling_descriptor["preprocess_control_entry"]
        except (KeyError, TypeError) as error:
            raise ProductionASRV5Error("private v03 entry lacks handling control") from error
        control_core = {
            key: control[key] for key in control if key != "identity_sha256"
        }
        if (
            control.get("identity_sha256")
            != sha256_bytes(_legacy_v03_canonical_bytes(control_core))
            or not isinstance(control.get("entries"), list)
            or control["entries"].count(control_entry) != 1
            or control_entry.get("ordinal") != entry["preprocess_ordinal"]
            or control_entry.get("source_byte_identity_claimed") is not False
            or handling_descriptor.get("handling_boundary") is None
            or sha256_bytes(
                _legacy_v03_canonical_bytes(handling_descriptor["handling_boundary"])
            )
            != control_entry.get("handling_boundary_sha256")
        ):
            raise ProductionASRV5Error("private v03 handling claims source byte identity")
        handling = {
            "mode": "private_v30",
            "descriptor_sha256": sha256_bytes(canonical_bytes(handling_descriptor)),
            "control_identity_sha256": _digest(
                control.get("identity_sha256"), "handling control identity"
            ),
            "handling_boundary_sha256": _digest(
                control_entry.get("handling_boundary_sha256"), "handling boundary SHA-256"
            ),
            "seal_receipt_sha256": _digest(
                control_entry.get("seal_receipt_sha256"), "seal receipt SHA-256"
            ),
            "seal_plan_sha256": _digest(
                control_entry.get("seal_plan_sha256"), "seal plan SHA-256"
            ),
            "source_byte_identity_claimed": False,
            "publication_authority": "none",
        }
    return {
        "kind": SOURCE_LINEAGE_PRODUCTION,
        "implementation_version": "0.3.0",
        "gpu_handoff": {
            "manifest_path": str(
                Path(manifest["output"]["queue_root"])
                / manifest["queue_relative_path"]
                / "manifest.json"
            ),
            "manifest_sha256": sha256_bytes(canonical_bytes(manifest)),
            "queue_id": manifest["queue_id"],
            "identity_sha256": manifest["identity_sha256"],
            "member_id": entry["member_id"],
            "member_identity_sha256": entry["identity_sha256"],
            "queue_ordinal": entry["ordinal"],
            "preprocess_ordinal": entry["preprocess_ordinal"],
        },
        "bundle_manifest": {
            "path": bundle["manifest_path"],
            "sha256": bundle["manifest_physical_sha256"],
            "bundle_id": bundle["bundle_id"],
            "identity_sha256": bundle["identity_sha256"],
            "manifest_sha256": bundle["manifest_sha256"],
        },
        "receipt": {
            "path": receipt["path"],
            "sha256": receipt["physical_sha256"],
            "receipt_id": receipt["receipt_id"],
            "receipt_sha256": receipt["receipt_sha256"],
            "ordinal": receipt["ordinal"],
        },
        "preprocess_result": {
            "path": result["path"],
            "sha256": result["sha256"],
            "byte_count": result["byte_count"],
            "processing_run_id": result["processing_run_id"],
            "recipe_sha256": result["recipe_sha256"],
        },
        "handling": handling,
        "corpus_authority": "preprocess_receipt_lineage_only",
    }


def input_from_preprocess_descriptor(
    entry: Any, *, sealed_mode: str | None = None
) -> dict[str, Any]:
    """Project one ready handoff member into the exact v5 normalized-audio input."""

    if not isinstance(entry, dict):
        raise ProductionASRV5Error("GPU handoff member must be an object")
    try:
        audio = entry["audio"]
        disposition = entry["resource_disposition"]
    except (KeyError, TypeError) as error:
        raise ProductionASRV5Error("GPU handoff member lacks audio disposition") from error
    if not isinstance(disposition, dict) or disposition.get("state") != "ready":
        raise ProductionASRV5Error("only ready handoff members have v5 inputs")
    expected_format = {
        "format_name": "flac",
        "codec_name": "flac",
        "sample_rate_hz": 16_000,
        "sample_format": "s16",
        "channels": 1,
        "channel_layout": "mono",
        "start_ms": 0,
    }
    if not isinstance(audio, dict) or audio.get("format") != expected_format:
        raise ProductionASRV5Error("handoff audio format is not exact normalized FLAC")
    observed_mode = audio.get("sealed_mode")
    if observed_mode not in {"0400", "0444"}:
        raise ProductionASRV5Error("handoff audio must bind sealed_mode 0400 or 0444")
    if sealed_mode is not None and sealed_mode != observed_mode:
        raise ProductionASRV5Error("requested sealed_mode differs from the handoff member")
    return {
        "path": audio["path"],
        "expected_sha256": audio["sha256"],
        "expected_byte_count": audio["byte_count"],
        "expected_duration_ms": audio["duration_ms"],
        "media_id": audio["media_id"],
        "artifact_id": audio["artifact_id"],
        "parent_processing_run_id": audio["processing_run_id"],
        "media_format": dict(MEDIA_FORMAT),
        "sealed_mode": observed_mode,
        "timeline_offset_ms": 0,
    }


def hot_root_from_gpu_queue_manifest(manifest: Any) -> dict[str, Any]:
    """Project the handoff manifest's stable registration reference into v5."""

    if not isinstance(manifest, dict):
        raise ProductionASRV5Error("GPU handoff manifest must be an object")
    try:
        reference = manifest["portable_root_registration"]
        filesystem = reference["filesystem"]
    except (KeyError, TypeError) as error:
        raise ProductionASRV5Error("GPU handoff manifest lacks a portable root") from error
    if not isinstance(filesystem, dict) or filesystem.get("type") != "btrfs":
        raise ProductionASRV5Error("GPU handoff root is not a Btrfs registration")
    return _validate_hot_root(
        {
            "registration_path": reference["document_path"],
            "registration_sha256": reference["document_sha256"],
            "registration_id": reference["registration_id"],
            "identity_sha256": reference["identity_sha256"],
            "root_id": reference["root_id"],
            "path": reference["path"],
            "filesystem_uuid": filesystem["uuid"],
            "tier": reference["tier"],
        }
    )


def profile_from_gpu_queue_manifest(
    manifest: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the compact profile reference and exact validated profile document."""

    if not isinstance(manifest, dict):
        raise ProductionASRV5Error("GPU handoff manifest must be an object")
    try:
        block = manifest["production_profile"]
        reference = block["reference"]
        document = block["document"]
    except (KeyError, TypeError) as error:
        raise ProductionASRV5Error("GPU handoff manifest lacks a production profile") from error
    profile_module = _load_local_module(
        "himr_gpu_profile_for_asr_v5", "production_profile_v2.py"
    )
    try:
        document = profile_module.validate_profile(document)
    except Exception as error:
        raise ProductionASRV5Error(f"GPU handoff profile is invalid: {error}") from error
    if (
        reference.get("physical_sha256") != sha256_bytes(canonical_bytes(document))
        or reference.get("profile_id") != document["profile_id"]
        or reference.get("identity_sha256") != document["identity_sha256"]
    ):
        raise ProductionASRV5Error("GPU handoff profile reference is inconsistent")
    compact = {
        "path": reference["path"],
        "sha256": reference["physical_sha256"],
        "profile_id": reference["profile_id"],
        "identity_sha256": reference["identity_sha256"],
    }
    return compact, document


def work_order_from_preprocess_descriptor(
    *,
    entry: Any,
    manifest: Any,
    runtime_admission: Any,
    output_root: str | Path,
    sealed_mode: str | None = None,
    job_id: str | None = None,
    replay_bindings: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create one production v5 order from a ready, sealed handoff member."""

    source = source_lineage_from_preprocess_descriptor(entry, manifest)
    input_item = input_from_preprocess_descriptor(entry, sealed_mode=sealed_mode)
    hot_root = hot_root_from_gpu_queue_manifest(manifest)
    profile_reference_value, profile = profile_from_gpu_queue_manifest(manifest)
    runtime = _validate_runtime_reference(runtime_admission, Path(hot_root["path"]))
    if job_id is None:
        job_identity = {
            "member_id": entry["member_id"],
            "runtime_identity_sha256": runtime["identity_sha256"],
            "profile_identity_sha256": profile["identity_sha256"],
            "stage": STAGE,
            "schema_version": CONTRACT_VERSION,
        }
        job_id = "gpu-asr-v5-" + sha256_bytes(canonical_bytes(job_identity))[:32]
    core = {
        "kind": WORK_ORDER_KIND,
        "schema_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "job_id": job_id,
        "input": input_item,
        "source_lineage": source,
        "runtime_admission": runtime,
        "production_profile": profile_reference_value,
        "hot_root": hot_root,
        "execution_contract": execution_contract_from_profile(profile),
        "transcript_semantics": json.loads(canonical_bytes(TRANSCRIPT_SEMANTICS)),
        "catalog_context": None,
        "output": {
            "root": str(normalized_absolute_path(output_root, "output root")),
            "layout": OUTPUT_LAYOUT,
            "atomic_no_replace": True,
        },
        "policy": dict(POLICY),
    }
    return (
        make_work_order(
            core, profile_document=profile, replay_bindings=replay_bindings
        ),
        profile,
    )


def synthetic_canary_lineage(
    *,
    fixture_manifest_path: str | Path,
    fixture_manifest_sha256: str,
    fixture_manifest_identity_sha256: str,
    fixture_id: str,
    fixture_case_id: str,
) -> dict[str, Any]:
    return {
        "kind": SOURCE_LINEAGE_SYNTHETIC,
        "fixture_manifest": {
            "path": str(normalized_absolute_path(fixture_manifest_path, "fixture manifest")),
            "sha256": _digest(fixture_manifest_sha256, "fixture manifest SHA-256"),
            "identity_sha256": _digest(
                fixture_manifest_identity_sha256, "fixture manifest identity"
            ),
            "fixture_id": _text(fixture_id, "fixture ID", 128),
        },
        "fixture_case_id": _text(fixture_case_id, "fixture case ID", 128),
        "contains_corpus_media": False,
        "scope": "purpose_built_synthetic_only",
        "corpus_authority": "none",
    }


def replay_external_bindings(
    work_order: dict[str, Any], *, profile_document: dict[str, Any]
) -> dict[str, Any]:
    """Replay compact JSON/root bindings without reading the input media bytes."""

    normalized = validate_work_order(
        work_order, profile_document=profile_document, replay_bindings=False
    )
    hot, registration = hot_root_reference(
        normalized["hot_root"]["registration_path"],
        normalized["hot_root"]["registration_sha256"],
    )
    if hot != normalized["hot_root"]:
        raise ProductionASRV5Error("hot-root registration differs from work order")
    _verify_output_directory(registration, normalized["output"]["root"])
    require_admitted = normalized["runtime_admission"]["status"] == "admitted"
    runtime, receipt = runtime_admission_reference(
        normalized["runtime_admission"]["receipt_path"],
        normalized["runtime_admission"]["receipt_sha256"],
        require_admitted=require_admitted,
    )
    if runtime != normalized["runtime_admission"]:
        raise ProductionASRV5Error("runtime admission differs from work order")
    observed_profile = load_profile_document(
        normalized["production_profile"]["path"],
        normalized["production_profile"]["sha256"],
    )
    if observed_profile != profile_document:
        raise ProductionASRV5Error("profile file differs from supplied profile document")
    if receipt["production_profile"]["identity_sha256"] != observed_profile["identity_sha256"]:
        raise ProductionASRV5Error("runtime admission binds a different profile")
    receipt_root = receipt["root"]
    if (
        receipt_root["root_id"] != registration["root_id"]
        or receipt_root["tier"] != registration["tier"]
        or receipt_root["path"] != registration["path"]
        or receipt_root["filesystem"] != registration["filesystem"]
        or receipt["root_registration"]["registration_id"]
        != registration["registration_id"]
    ):
        raise ProductionASRV5Error("runtime admission binds a different hot root")
    source = normalized["source_lineage"]
    references: list[tuple[str, dict[str, Any]]] = []
    if source["kind"] == SOURCE_LINEAGE_PRODUCTION:
        references = [
            (
                "gpu_handoff_manifest",
                {
                    "path": source["gpu_handoff"]["manifest_path"],
                    "sha256": source["gpu_handoff"]["manifest_sha256"],
                },
            ),
            ("bundle_manifest", source["bundle_manifest"]),
            ("preprocess_receipt", source["receipt"]),
            ("preprocess_result", source["preprocess_result"]),
        ]
    else:
        references = [("fixture_manifest", source["fixture_manifest"])]
    for role, reference in references:
        _stable_file_bytes(
            Path(reference["path"]),
            label=f"source lineage JSON ({role})",
            maximum_bytes=64 * 1024**2,
            expected_sha256=reference["sha256"],
            # Historical preprocess result envelopes are intentionally sealed
            # read-only for the registered hot-root owner.  The queue validator
            # and trusted launcher already retain this exact 0400/0440/0444
            # policy.  Do not extend it to queue, bundle, receipt, fixture, or
            # general control documents.
            current_user_modes=(
                frozenset({0o400, 0o440, 0o444})
                if role == "preprocess_result"
                else frozenset({0o400})
            ),
        )
    return {
        "status": "replayed",
        "hot_root_registration_id": registration["registration_id"],
        "runtime_receipt_id": receipt["receipt_id"],
        "profile_id": observed_profile["profile_id"],
        "source_lineage_kind": source["kind"],
        "input_media_read": False,
        "gpu_queried": False,
        "files_written": False,
    }


def result_plan(work_order: dict[str, Any], *, profile_document: dict[str, Any]) -> dict[str, Any]:
    order = validate_work_order(work_order, profile_document=profile_document)
    identity_core = {
        "work_order_identity_sha256": order["identity_sha256"],
        "runtime_admission_identity_sha256": order["runtime_admission"]["identity_sha256"],
        "production_profile_identity_sha256": order["production_profile"]["identity_sha256"],
        "input_sha256": order["input"]["expected_sha256"],
        "output_contract": OUTPUT_CONTRACT,
    }
    result_key = sha256_bytes(canonical_bytes(identity_core))
    relative_dir = (
        f"asr/faster-whisper-gpu-v5/sha256/{order['input']['expected_sha256'][:2]}/"
        f"{order['input']['expected_sha256']}/results/{result_key}"
    )
    root = Path(order["output"]["root"])
    result_dir = root / relative_dir
    _require_descendant(result_dir, root, "result directory")
    return {
        "result_key": result_key,
        "result_relative_directory": relative_dir,
        "result_directory": str(result_dir),
        "result_path": str(result_dir / "result.json"),
        "raw_transcript_path": str(result_dir / "transcript.raw.json"),
        "normalized_transcript_path": str(result_dir / "transcript.normalized.json"),
    }


def _timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        raise ProductionASRV5Error(f"{label} must be whole-second UTC")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ProductionASRV5Error(f"{label} is invalid") from error
    if parsed.tzinfo != timezone.utc or parsed.microsecond:
        raise ProductionASRV5Error(f"{label} must be whole-second UTC")
    return value


def _histogram_summary(value: Any, label: str, *, maximum_value: int) -> dict[str, Any]:
    item = _exact(
        value,
        label,
        {"sample_count", "minimum", "mean", "p50", "p95", "maximum", "bin_width"},
    )
    count = _integer(item["sample_count"], f"{label}.sample_count", 1, 10**9)
    numbers = {
        name: _integer(item[name], f"{label}.{name}", 0, maximum_value)
        for name in ("minimum", "p50", "p95", "maximum")
    }
    if not numbers["minimum"] <= numbers["p50"] <= numbers["p95"] <= numbers["maximum"]:
        raise ProductionASRV5Error(f"{label} quantiles are not monotonic")
    mean = _number(item["mean"], f"{label}.mean", numbers["minimum"], numbers["maximum"])
    return {
        "sample_count": count,
        "minimum": numbers["minimum"],
        "mean": mean,
        "p50": numbers["p50"],
        "p95": numbers["p95"],
        "maximum": numbers["maximum"],
        "bin_width": _integer(item["bin_width"], f"{label}.bin_width", 1, maximum_value or 1),
    }


def _validate_telemetry(value: Any, profile: dict[str, Any]) -> dict[str, Any]:
    item = _exact(
        value,
        "execution.telemetry",
        {
            "implementation_version",
            "fast_interval_seconds",
            "slow_interval_seconds",
            "fast_sample_count",
            "slow_sample_count",
            "sample_span_seconds",
            "last_fast_sample_age_seconds",
            "last_slow_sample_age_seconds",
            "process_vram_measurement_seen",
            "process_peak_used_bytes",
            "global_peak_used_bytes",
            "minimum_free_bytes",
            "utilization_percent",
            "memory_controller_utilization_percent",
            "active_sample_fraction",
            "temperature_c",
            "power_mw",
            "estimated_energy_millijoules",
            "sm_clock_mhz",
            "throttle_reasons_bitmask_or",
            "sampler_error",
        },
    )
    limits = profile["telemetry"]
    fast_count = _integer(item["fast_sample_count"], "telemetry.fast_sample_count", 1, 10**9)
    slow_count = _integer(item["slow_sample_count"], "telemetry.slow_sample_count", 1, 10**9)
    utilization = _histogram_summary(
        item["utilization_percent"], "telemetry.utilization_percent", maximum_value=100
    )
    memory_utilization = _histogram_summary(
        item["memory_controller_utilization_percent"],
        "telemetry.memory_controller_utilization_percent",
        maximum_value=100,
    )
    temperature = _histogram_summary(
        item["temperature_c"], "telemetry.temperature_c", maximum_value=120
    )
    power = _histogram_summary(item["power_mw"], "telemetry.power_mw", maximum_value=1_000_000)
    clock = _histogram_summary(item["sm_clock_mhz"], "telemetry.sm_clock_mhz", maximum_value=10_000)
    for summary in (utilization, memory_utilization, temperature, power, clock):
        if summary["sample_count"] != slow_count:
            raise ProductionASRV5Error("telemetry histogram count differs from slow samples")
    maximum_age = limits["maximum_heartbeat_age_ms"] / 1000
    normalized = {
        "implementation_version": _text(
            item["implementation_version"], "telemetry implementation version", 32
        ),
        "fast_interval_seconds": _number(
            item["fast_interval_seconds"], "telemetry fast interval", 0.001, 10
        ),
        "slow_interval_seconds": _number(
            item["slow_interval_seconds"], "telemetry slow interval", 0.001, 10
        ),
        "fast_sample_count": fast_count,
        "slow_sample_count": slow_count,
        "sample_span_seconds": _number(
            item["sample_span_seconds"], "telemetry sample span", 0, 7 * 24 * 3600
        ),
        "last_fast_sample_age_seconds": _number(
            item["last_fast_sample_age_seconds"], "telemetry fast age", 0, maximum_age
        ),
        "last_slow_sample_age_seconds": _number(
            item["last_slow_sample_age_seconds"], "telemetry slow age", 0, maximum_age
        ),
        "process_vram_measurement_seen": item["process_vram_measurement_seen"],
        "process_peak_used_bytes": _integer(
            item["process_peak_used_bytes"], "telemetry process peak", 0, 2**63 - 1
        ),
        "global_peak_used_bytes": _integer(
            item["global_peak_used_bytes"], "telemetry global peak", 0, 2**63 - 1
        ),
        "minimum_free_bytes": _integer(
            item["minimum_free_bytes"], "telemetry minimum free", 0, 2**63 - 1
        ),
        "utilization_percent": utilization,
        "memory_controller_utilization_percent": memory_utilization,
        "active_sample_fraction": _number(
            item["active_sample_fraction"], "telemetry active fraction", 0, 1
        ),
        "temperature_c": temperature,
        "power_mw": power,
        "estimated_energy_millijoules": _number(
            item["estimated_energy_millijoules"], "telemetry energy", 0, 10**15
        ),
        "sm_clock_mhz": clock,
        "throttle_reasons_bitmask_or": _integer(
            item["throttle_reasons_bitmask_or"], "telemetry throttle mask", 0, 2**64 - 1
        ),
        "sampler_error": item["sampler_error"],
    }
    if (
        normalized["implementation_version"] != "0.1.0"
        or normalized["fast_interval_seconds"] != limits["fast_interval_ms"] / 1000
        or normalized["slow_interval_seconds"] != limits["slow_interval_ms"] / 1000
        or normalized["process_vram_measurement_seen"] is not True
        or normalized["sampler_error"] is not None
        or normalized["process_peak_used_bytes"] > limits["maximum_process_vram_bytes"]
        or normalized["minimum_free_bytes"] < limits["minimum_free_vram_bytes"]
        or normalized["temperature_c"]["maximum"] > limits["maximum_temperature_c"]
    ):
        raise ProductionASRV5Error("telemetry violates the exact production profile")
    return normalized


def _artifact(value: Any, label: str, *, expected_kind: str, expected_path: str, maximum: int) -> dict[str, Any]:
    item = _exact(
        value,
        label,
        {"artifact_kind", "path", "sha256", "byte_count", "document_id", "identity_sha256"},
    )
    if item["artifact_kind"] != expected_kind:
        raise ProductionASRV5Error(f"{label} artifact kind differs")
    path = normalized_absolute_path(item["path"], f"{label}.path")
    if str(path) != expected_path:
        raise ProductionASRV5Error(f"{label} path differs from result plan")
    document_id = _text(item["document_id"], f"{label}.document_id", 96)
    expected_prefix = "gpuasrraw_" if expected_kind.startswith("faster") else "gpuasrnorm_"
    if not re.fullmatch(re.escape(expected_prefix) + r"[0-9a-f]{32}", document_id):
        raise ProductionASRV5Error(f"{label} document ID is invalid")
    return {
        "artifact_kind": expected_kind,
        "path": str(path),
        "sha256": _digest(item["sha256"], f"{label}.sha256"),
        "byte_count": _integer(item["byte_count"], f"{label}.byte_count", 1, maximum),
        "document_id": document_id,
        "identity_sha256": _digest(item["identity_sha256"], f"{label}.identity_sha256"),
    }


def normalize_result_core(
    value: Any, *, work_order: dict[str, Any], profile_document: dict[str, Any]
) -> dict[str, Any]:
    order = validate_work_order(work_order, profile_document=profile_document)
    item = _exact(value, "result core", RESULT_CORE_KEYS)
    if (
        item["kind"] != RESULT_KIND
        or item["schema_version"] != CONTRACT_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["status"] != "completed"
    ):
        raise ProductionASRV5Error("v5 result header is unsupported")
    expected_work_order = {
        "work_order_id": order["work_order_id"],
        "identity_sha256": order["identity_sha256"],
    }
    if item["work_order"] != expected_work_order:
        raise ProductionASRV5Error("result binds a different work order")
    expected_runtime = {
        "receipt_id": order["runtime_admission"]["receipt_id"],
        "identity_sha256": order["runtime_admission"]["identity_sha256"],
    }
    if item["runtime_admission"] != expected_runtime:
        raise ProductionASRV5Error("result binds a different runtime admission")
    expected_profile = {
        "profile_id": order["production_profile"]["profile_id"],
        "identity_sha256": order["production_profile"]["identity_sha256"],
    }
    if item["production_profile"] != expected_profile:
        raise ProductionASRV5Error("result binds a different production profile")
    expected_input = {
        "sha256": order["input"]["expected_sha256"],
        "byte_count": order["input"]["expected_byte_count"],
        "duration_ms": order["input"]["expected_duration_ms"],
        "media_id": order["input"]["media_id"],
        "artifact_id": order["input"]["artifact_id"],
        "timeline_offset_ms": 0,
    }
    if item["input"] != expected_input:
        raise ProductionASRV5Error("result input summary differs from work order")
    execution = _exact(
        item["execution"],
        "execution",
        {
            "attempt_id",
            "started_at",
            "completed_at",
            "wall_seconds",
            "model_load_seconds",
            "inference_seconds",
            "phase_seconds",
            "model_load_count",
            "live_gpu_uuid",
            "telemetry",
        },
    )
    attempt_id = _text(execution["attempt_id"], "execution.attempt_id", 128)
    if not re.fullmatch(r"gpuasrattempt_[0-9a-f]{32}", attempt_id):
        raise ProductionASRV5Error("execution attempt ID is invalid")
    started_at = _timestamp(execution["started_at"], "execution.started_at")
    completed_at = _timestamp(execution["completed_at"], "execution.completed_at")
    if completed_at < started_at:
        raise ProductionASRV5Error("execution completion precedes start")
    gpu_uuid = _text(execution["live_gpu_uuid"], "execution.live_gpu_uuid", 96)
    if not GPU_UUID_RE.fullmatch(gpu_uuid) or gpu_uuid != profile_document["hardware"]["gpu_uuid"]:
        raise ProductionASRV5Error("live GPU UUID differs from the admitted profile")
    wall_seconds = _number(
        execution["wall_seconds"],
        "execution.wall_seconds",
        0,
        profile_document["item_limits"]["maximum_wall_seconds"],
    )
    model_load_seconds = _number(
        execution["model_load_seconds"], "execution.model_load_seconds", 0, wall_seconds
    )
    inference_seconds = _number(
        execution["inference_seconds"], "execution.inference_seconds", 0, wall_seconds
    )
    phase_value = _exact(
        execution["phase_seconds"],
        "execution.phase_seconds",
        {
            "preflight_input_hash",
            "preflight_ffprobe",
            "transcribe_setup",
            "model_iterator",
            "transcript_normalization",
            "artifact_serialization",
        },
    )
    phase_seconds = {
        name: _number(
            phase_value[name], f"execution.phase_seconds.{name}", 0, wall_seconds
        )
        for name in (
            "preflight_input_hash",
            "preflight_ffprobe",
            "transcribe_setup",
            "model_iterator",
            "transcript_normalization",
            "artifact_serialization",
        )
    }
    if abs(
        inference_seconds
        - phase_seconds["transcribe_setup"]
        - phase_seconds["model_iterator"]
    ) > 0.001:
        raise ProductionASRV5Error(
            "execution inference time differs from setup plus iterator time"
        )
    if model_load_seconds + inference_seconds > wall_seconds + 0.001:
        raise ProductionASRV5Error("execution phase times exceed wall time")
    normalized_execution = {
        "attempt_id": attempt_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "wall_seconds": wall_seconds,
        "model_load_seconds": model_load_seconds,
        "inference_seconds": inference_seconds,
        "phase_seconds": phase_seconds,
        "model_load_count": _integer(
            execution["model_load_count"],
            "execution.model_load_count",
            profile_document["batch_limits"]["model_load_count"],
            profile_document["batch_limits"]["model_load_count"],
        ),
        "live_gpu_uuid": gpu_uuid,
        "telemetry": _validate_telemetry(execution["telemetry"], profile_document),
    }
    plan = result_plan(order, profile_document=profile_document)
    maximum_result = profile_document["item_limits"]["maximum_result_bytes"]
    artifacts = item["artifacts"]
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ProductionASRV5Error("result must contain exactly two transcript artifacts")
    normalized_artifacts = [
        _artifact(
            artifacts[0],
            "raw transcript",
            expected_kind="faster_whisper_raw_transcript_json",
            expected_path=plan["raw_transcript_path"],
            maximum=maximum_result,
        ),
        _artifact(
            artifacts[1],
            "normalized transcript",
            expected_kind="transcript_normalized_json",
            expected_path=plan["normalized_transcript_path"],
            maximum=maximum_result,
        ),
    ]
    transcript = _exact(
        item["transcript"],
        "transcript summary",
        {
            "language",
            "timeline",
            "segment_count",
            "word_count",
            "text_character_count",
            "word_timing_anomalies",
            "review_status",
            "semantics",
        },
    )
    language = _exact(
        transcript["language"],
        "transcript language",
        {"value", "selection_basis", "detection_performed", "raw_probability", "calibrated_probability"},
    )
    expected_language = {
        "value": "en",
        "selection_basis": "forced_by_inference_profile",
        "detection_performed": False,
        "raw_probability": None,
        "calibrated_probability": None,
    }
    if language != expected_language:
        raise ProductionASRV5Error("transcript language provenance is inconsistent")
    timeline = _exact(
        transcript["timeline"],
        "transcript timeline",
        {"coordinate_system", "source_duration_ms", "source_offset_ms", "end_ms"},
    )
    expected_timeline = {
        "coordinate_system": "media_ms",
        "source_duration_ms": order["input"]["expected_duration_ms"],
        "source_offset_ms": 0,
        "end_ms": order["input"]["expected_duration_ms"],
    }
    if timeline != expected_timeline:
        raise ProductionASRV5Error("transcript timeline is not media-local")
    anomalies = _exact(
        transcript["word_timing_anomalies"],
        "word timing anomalies",
        {"anomalous_word_count", "total_flag_count", "flag_counts", "human_review_required"},
    )
    flag_counts = _exact(
        anomalies["flag_counts"], "word timing flag counts", set(WORD_TIMING_FLAG_NAMES)
    )
    normalized_flags = {
        name: _integer(flag_counts[name], f"word timing flag {name}", 0, profile_document["item_limits"]["maximum_words"])
        for name in WORD_TIMING_FLAG_NAMES
    }
    anomaly_count = _integer(
        anomalies["anomalous_word_count"],
        "anomalous word count",
        0,
        profile_document["item_limits"]["maximum_words"],
    )
    total_flags = _integer(
        anomalies["total_flag_count"],
        "total timing flag count",
        0,
        4 * profile_document["item_limits"]["maximum_words"],
    )
    if total_flags != sum(normalized_flags.values()) or anomalies["human_review_required"] is not True:
        raise ProductionASRV5Error("word timing anomaly summary is inconsistent")
    segment_count = _integer(
        transcript["segment_count"],
        "transcript.segment_count",
        0,
        profile_document["item_limits"]["maximum_segments"],
    )
    word_count = _integer(
        transcript["word_count"],
        "transcript.word_count",
        0,
        profile_document["item_limits"]["maximum_words"],
    )
    if anomaly_count > word_count:
        raise ProductionASRV5Error("anomalous word count exceeds total words")
    if transcript["review_status"] != "unreviewed_machine_output":
        raise ProductionASRV5Error("v5 producer may not claim human review")
    if transcript["semantics"] != TRANSCRIPT_SEMANTICS:
        raise ProductionASRV5Error("result transcript semantics differ from work order")
    if item["policy"] != POLICY:
        raise ProductionASRV5Error("result safety policy is not exact")
    return {
        "kind": RESULT_KIND,
        "schema_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "completed",
        "work_order": expected_work_order,
        "runtime_admission": expected_runtime,
        "production_profile": expected_profile,
        "input": expected_input,
        "execution": normalized_execution,
        "transcript": {
            "language": expected_language,
            "timeline": expected_timeline,
            "segment_count": segment_count,
            "word_count": word_count,
            "text_character_count": _integer(
                transcript["text_character_count"],
                "transcript.text_character_count",
                0,
                MAX_TEXT_CHARACTERS,
            ),
            "word_timing_anomalies": {
                "anomalous_word_count": anomaly_count,
                "total_flag_count": total_flags,
                "flag_counts": normalized_flags,
                "human_review_required": True,
            },
            "review_status": "unreviewed_machine_output",
            "semantics": json.loads(canonical_bytes(TRANSCRIPT_SEMANTICS)),
        },
        "artifacts": normalized_artifacts,
        "policy": dict(POLICY),
    }


def make_result(
    core: Any, *, work_order: dict[str, Any], profile_document: dict[str, Any]
) -> dict[str, Any]:
    normalized = normalize_result_core(
        core, work_order=work_order, profile_document=profile_document
    )
    identity = sha256_bytes(canonical_bytes(normalized))
    return {
        **normalized,
        "identity_sha256": identity,
        "result_id": f"gpuasrresult5_{identity[:32]}",
    }


def validate_result(
    value: Any, *, work_order: dict[str, Any], profile_document: dict[str, Any]
) -> dict[str, Any]:
    item = _exact(
        value,
        "result",
        RESULT_CORE_KEYS | {"identity_sha256", "result_id"},
    )
    expected = make_result(
        {key: item[key] for key in RESULT_CORE_KEYS},
        work_order=work_order,
        profile_document=profile_document,
    )
    if item != expected:
        raise ProductionASRV5Error("result is noncanonical or its identity is invalid")
    return expected


def load_work_order(
    path_value: str | Path,
    *,
    profile_document: dict[str, Any],
    expected_sha256: str | None = None,
    replay_bindings: bool = False,
) -> dict[str, Any]:
    path = normalized_absolute_path(path_value, "work order path")
    body = _stable_file_bytes(
        path,
        label="work order",
        maximum_bytes=MAX_JSON_BYTES,
        expected_sha256=expected_sha256,
    )
    value = parse_json_bytes(body, "work order")
    if body != canonical_bytes(value):
        raise ProductionASRV5Error("work order is not canonical JSON")
    return validate_work_order(
        value, profile_document=profile_document, replay_bindings=replay_bindings
    )


def status(
    work_order: dict[str, Any], *, profile_document: dict[str, Any]
) -> dict[str, Any]:
    order = validate_work_order(work_order, profile_document=profile_document)
    plan = result_plan(order, profile_document=profile_document)
    path = Path(plan["result_path"])
    if not path.exists() and not path.is_symlink():
        return {
            "status": "absent",
            "work_order_id": order["work_order_id"],
            "result_path": str(path),
            "inference_performed": False,
            "files_written": False,
        }
    body = _stable_file_bytes(
        path,
        label="completed v5 result",
        maximum_bytes=profile_document["item_limits"]["maximum_result_bytes"],
    )
    value = parse_json_bytes(body, "completed v5 result")
    if body != canonical_bytes(value):
        raise ProductionASRV5Error("completed result is not canonical JSON")
    result = validate_result(
        value, work_order=order, profile_document=profile_document
    )
    return {
        "status": "completed",
        "work_order_id": order["work_order_id"],
        "result_id": result["result_id"],
        "result_path": str(path),
        "result_sha256": sha256_bytes(body),
        "inference_performed": False,
        "files_written": False,
    }


WORK_ORDER_CONTRACT_DESCRIPTOR = {
    "kind": WORK_ORDER_KIND,
    "schema_version": CONTRACT_VERSION,
    "implementation_version": IMPLEMENTATION_VERSION,
    "closed_core_fields": sorted(WORK_ORDER_CORE_KEYS),
    "source_lineage_types": ["production_preprocess_v03", "synthetic_canary"],
    "portable_placement": "registered_hot_root_and_btrfs_filesystem_uuid",
    "profile": "exact_himr_gpu_production_profile_v2_projection",
    "runtime": "himr_gpu_runtime_admission_receipt_v2_identity",
    "coordinate_system": "media_ms",
    "output_contract": OUTPUT_CONTRACT,
    "execution_entry_point": None,
    "policy": POLICY,
}

RESULT_CONTRACT_DESCRIPTOR = {
    "kind": RESULT_KIND,
    "schema_version": CONTRACT_VERSION,
    "implementation_version": IMPLEMENTATION_VERSION,
    "closed_core_fields": sorted(RESULT_CORE_KEYS),
    "live_gpu_binding": "exact_nvidia_uuid_equal_to_profile",
    "telemetry": "bounded_fail_closed_gpu_telemetry_summary_v1",
    "transcript_semantics": TRANSCRIPT_SEMANTICS,
    "output_contract": OUTPUT_CONTRACT,
    "policy": POLICY,
}


def contract_document() -> dict[str, Any]:
    return {
        "kind": CONTRACT_KIND,
        "schema_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "work_order": {
            "descriptor": WORK_ORDER_CONTRACT_DESCRIPTOR,
            "identity_sha256": sha256_bytes(canonical_bytes(WORK_ORDER_CONTRACT_DESCRIPTOR)),
        },
        "result": {
            "descriptor": RESULT_CONTRACT_DESCRIPTOR,
            "identity_sha256": sha256_bytes(canonical_bytes(RESULT_CONTRACT_DESCRIPTOR)),
        },
        "commands": ["contract", "create", "validate", "status"],
        "inference_entry_point": None,
    }


def _write_exclusive(path_value: str | Path, value: Any) -> None:
    path = normalized_absolute_path(path_value, "output path")
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace {path}")
    parent = path.parent
    info = parent.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ProductionASRV5Error("output parent must be current-user-owned mode 0700")
    body = canonical_bytes(value)
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o400)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path, follow_symlinks=False)
        os.unlink(temporary)
        parent_descriptor = os.open(
            parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
    except Exception:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _load_core_spec(path_value: str | Path, expected_sha256: str) -> Any:
    path = normalized_absolute_path(path_value, "creation spec path")
    body = _stable_file_bytes(
        path,
        label="creation spec",
        maximum_bytes=MAX_JSON_BYTES,
        expected_sha256=expected_sha256,
    )
    value = parse_json_bytes(body, "creation spec")
    if body != canonical_bytes(value):
        raise ProductionASRV5Error("creation spec is not canonical JSON")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contract")
    create = commands.add_parser("create")
    create.add_argument("--spec", required=True)
    create.add_argument("--expected-spec-sha256", required=True)
    create.add_argument("--profile", required=True)
    create.add_argument("--expected-profile-sha256", required=True)
    create.add_argument("--output", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    validate.add_argument("--expected-work-order-sha256")
    validate.add_argument("--profile", required=True)
    validate.add_argument("--expected-profile-sha256", required=True)
    validate.add_argument("--replay-bindings", action="store_true")
    state = commands.add_parser("status")
    state.add_argument("--work-order", required=True)
    state.add_argument("--expected-work-order-sha256")
    state.add_argument("--profile", required=True)
    state.add_argument("--expected-profile-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contract":
            response = contract_document()
        else:
            profile = load_profile_document(args.profile, args.expected_profile_sha256)
            if args.command == "create":
                core = _load_core_spec(args.spec, args.expected_spec_sha256)
                order = make_work_order(
                    core, profile_document=profile, replay_bindings=True
                )
                _write_exclusive(args.output, order)
                response = {
                    "status": "created",
                    "work_order_id": order["work_order_id"],
                    "identity_sha256": order["identity_sha256"],
                    "path": str(normalized_absolute_path(args.output, "output")),
                    "sha256": sha256_bytes(canonical_bytes(order)),
                    "inference_performed": False,
                }
            else:
                order = load_work_order(
                    args.work_order,
                    profile_document=profile,
                    expected_sha256=args.expected_work_order_sha256,
                    replay_bindings=(
                        args.replay_bindings if args.command == "validate" else False
                    ),
                )
                if args.command == "validate":
                    response = {
                        "status": "validated",
                        "work_order_id": order["work_order_id"],
                        "identity_sha256": order["identity_sha256"],
                        "bindings_replayed": args.replay_bindings,
                        "inference_performed": False,
                        "files_written": False,
                    }
                else:
                    response = status(order, profile_document=profile)
    except (OSError, ValueError, ProductionASRV5Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(canonical_bytes(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
