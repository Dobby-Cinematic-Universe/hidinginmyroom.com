#!/usr/bin/env python3
"""Canonical admission-gate summaries for the portable GPU production lane.

Gate documents contain aggregate, text-free evidence only.  They do not grant
publication, identity, catalogue, archive, or deletion authority.  The validator
enforces minimum dataset/soak coverage and maximum resource/performance regressions
so a short smoke cannot be mislabeled as production evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any


KIND = "himr_gpu_admission_gate"
SCHEMA_VERSION = 2
IMPLEMENTATION_VERSION = "0.2.0"
GATES = (
    "accuracy",
    "semantic_compatibility",
    "packed_30m",
    "packed_2h",
    "thermal_8h",
    "batch_32",
    "scheduler",
    "crash_recovery",
    "launcher_isolation",
)
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


POLICY = {
    "visibility": "private",
    "machine_generated": True,
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "archive_authority": "none",
}


class AdmissionEvidenceError(ValueError):
    """An admission summary is malformed or does not satisfy its gate."""


def canonical_bytes(value: Any) -> bytes:
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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise AdmissionEvidenceError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise AdmissionEvidenceError(f"{label} must be an integer within [{minimum}, {maximum}]")
    return value


def _number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AdmissionEvidenceError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise AdmissionEvidenceError(f"{label} must be finite within [{minimum}, {maximum}]")
    return result


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise AdmissionEvidenceError(f"{label} must be a lowercase SHA-256")
    return value


METRIC_FIELDS = {
    "case_count",
    "repetitions",
    "audio_seconds",
    "wall_seconds",
    "item_count",
    "failed_item_count",
    "p95_inference_rtf",
    "p95_end_to_end_rtf",
    "wer_absolute_regression",
    "wer_relative_regression",
    "term_recall_absolute_regression",
    "exact_output_match",
    "peak_process_vram_bytes",
    "minimum_free_vram_bytes",
    "maximum_temperature_c",
    "thermal_throttle_observed",
    "oom_observed",
    "deadline_exit_observed",
    "network_isolated",
    "cold_storage_visible",
    "writable_root_expansion_observed",
    "crash_case_count",
    "crash_recovery_failure_count",
    "lock_contention_passed",
    "lock_crash_release_passed",
}

RAW_EVIDENCE_KIND = "himr_gpu_admission_gate_raw_evidence"
RAW_EVIDENCE_SCHEMA_VERSION = 1
RAW_EVIDENCE_IMPLEMENTATION_VERSION = "0.1.0"
REDUCER_KIND = "himr_gpu_admission_gate_reducer"
REDUCER_SCHEMA_VERSION = 1
REDUCER_IMPLEMENTATION_VERSION = "0.1.0"
MAX_OBSERVATIONS = 100_000
FIXED_POINT_SCALE = 1_000_000_000

# Observation rows deliberately contain only bounded integers and booleans.  In
# particular, there are no labels, command lines, transcripts, exception strings,
# or other producer-controlled text fields which could make an admitted receipt a
# covert publication channel.  Ratios/regressions use signed parts-per-billion;
# durations use integer milliseconds and temperature uses millicelsius.
OBSERVATION_FIELDS = {
    "ordinal",
    "case_count",
    "repetitions",
    "audio_milliseconds",
    "wall_milliseconds",
    "item_count",
    "failed_item_count",
    "inference_rtf_ppb",
    "end_to_end_rtf_ppb",
    "wer_absolute_regression_ppb",
    "wer_relative_regression_ppb",
    "term_recall_absolute_regression_ppb",
    "exact_output_match",
    "peak_process_vram_bytes",
    "minimum_free_vram_bytes",
    "maximum_temperature_millicelsius",
    "thermal_throttle_observed",
    "oom_observed",
    "deadline_exit_observed",
    "network_isolated",
    "cold_storage_visible",
    "writable_root_expansion_observed",
    "crash_case_count",
    "crash_recovery_failure_count",
    "lock_contention_passed",
    "lock_crash_release_passed",
}

REDUCER_DESCRIPTOR = {
    "kind": REDUCER_KIND,
    "schema_version": REDUCER_SCHEMA_VERSION,
    "implementation_version": REDUCER_IMPLEMENTATION_VERSION,
    "input_kind": RAW_EVIDENCE_KIND,
    "observation_schema_version": 1,
    "fixed_point_scale": FIXED_POINT_SCALE,
    "duration_unit": "milliseconds",
    "temperature_unit": "millicelsius",
    "percentile": "nearest_rank_p95",
    "counter_aggregation": "sum",
    "regression_aggregation": "maximum",
    "resource_aggregation": "worst_case",
    "adverse_boolean_aggregation": "any",
    "success_boolean_aggregation": "all",
    "maximum_observations": MAX_OBSERVATIONS,
}
REDUCER_IDENTITY_SHA256 = sha256_bytes(canonical_bytes(REDUCER_DESCRIPTOR))
REDUCER_REFERENCE = {
    "kind": REDUCER_KIND,
    "schema_version": REDUCER_SCHEMA_VERSION,
    "implementation_version": REDUCER_IMPLEMENTATION_VERSION,
    "identity_sha256": REDUCER_IDENTITY_SHA256,
}

RAW_POLICY = {
    **POLICY,
    "arbitrary_text_fields": False,
    "maximum_observation_count": MAX_OBSERVATIONS,
}

RAW_CORE_FIELDS = {
    "kind",
    "schema_version",
    "implementation_version",
    "gate",
    "production_profile_identity_sha256",
    "execution_image_identity_sha256",
    "runtime_candidate_identity_sha256",
    "reducer",
    "observations",
    "policy",
}

RAW_REFERENCE_FIELDS = {
    "path",
    "sha256",
    "byte_count",
    "kind",
    "schema_version",
    "identity_sha256",
    "reducer_identity_sha256",
}


def _normalize_metrics(value: Any) -> dict[str, Any]:
    item = _exact(value, "gate metrics", METRIC_FIELDS)
    result = {
        "case_count": _integer(item["case_count"], "metrics.case_count", 0),
        "repetitions": _integer(item["repetitions"], "metrics.repetitions", 0),
        "audio_seconds": _number(item["audio_seconds"], "metrics.audio_seconds", 0, 10**9),
        "wall_seconds": _number(item["wall_seconds"], "metrics.wall_seconds", 0, 10**9),
        "item_count": _integer(item["item_count"], "metrics.item_count", 0),
        "failed_item_count": _integer(item["failed_item_count"], "metrics.failed_item_count", 0),
        "p95_inference_rtf": _number(item["p95_inference_rtf"], "metrics.p95_inference_rtf", 0, 100),
        "p95_end_to_end_rtf": _number(item["p95_end_to_end_rtf"], "metrics.p95_end_to_end_rtf", 0, 100),
        "wer_absolute_regression": _number(
            item["wer_absolute_regression"], "metrics.wer_absolute_regression", -1, 1
        ),
        "wer_relative_regression": _number(
            item["wer_relative_regression"], "metrics.wer_relative_regression", -100, 100
        ),
        "term_recall_absolute_regression": _number(
            item["term_recall_absolute_regression"], "metrics.term_recall_absolute_regression", -1, 1
        ),
        "peak_process_vram_bytes": _integer(
            item["peak_process_vram_bytes"], "metrics.peak_process_vram_bytes", 0
        ),
        "minimum_free_vram_bytes": _integer(
            item["minimum_free_vram_bytes"], "metrics.minimum_free_vram_bytes", 0
        ),
        "maximum_temperature_c": _number(
            item["maximum_temperature_c"], "metrics.maximum_temperature_c", 0, 150
        ),
        "crash_case_count": _integer(item["crash_case_count"], "metrics.crash_case_count", 0),
        "crash_recovery_failure_count": _integer(
            item["crash_recovery_failure_count"], "metrics.crash_recovery_failure_count", 0
        ),
    }
    for name in (
        "exact_output_match",
        "thermal_throttle_observed",
        "oom_observed",
        "deadline_exit_observed",
        "network_isolated",
        "cold_storage_visible",
        "writable_root_expansion_observed",
        "lock_contention_passed",
        "lock_crash_release_passed",
    ):
        if not isinstance(item[name], bool):
            raise AdmissionEvidenceError(f"metrics.{name} must be boolean")
        result[name] = item[name]
    return result


def empty_observation() -> dict[str, Any]:
    """Return one complete, text-free observation row for an evidence producer."""

    return {
        "ordinal": 0,
        "case_count": 0,
        "repetitions": 0,
        "audio_milliseconds": 0,
        "wall_milliseconds": 0,
        "item_count": 0,
        "failed_item_count": 0,
        "inference_rtf_ppb": 0,
        "end_to_end_rtf_ppb": 0,
        "wer_absolute_regression_ppb": 0,
        "wer_relative_regression_ppb": 0,
        "term_recall_absolute_regression_ppb": 0,
        "exact_output_match": False,
        "peak_process_vram_bytes": 0,
        "minimum_free_vram_bytes": 2 * 1024**3,
        "maximum_temperature_millicelsius": 0,
        "thermal_throttle_observed": False,
        "oom_observed": False,
        "deadline_exit_observed": False,
        "network_isolated": False,
        "cold_storage_visible": False,
        "writable_root_expansion_observed": False,
        "crash_case_count": 0,
        "crash_recovery_failure_count": 0,
        "lock_contention_passed": False,
        "lock_crash_release_passed": False,
    }


def observation_from_metrics(metrics: Any, *, ordinal: int = 0) -> dict[str, Any]:
    """Encode an existing metric sample using the raw fixed-point representation.

    This is a producer convenience, not an admission shortcut: the resulting row
    must still be sealed inside a typed raw envelope and deep-reduced by admission.
    """

    item = _normalize_metrics(metrics)
    return {
        "ordinal": _integer(ordinal, "observation ordinal", 0, MAX_OBSERVATIONS - 1),
        "case_count": item["case_count"],
        "repetitions": item["repetitions"],
        "audio_milliseconds": round(item["audio_seconds"] * 1000),
        "wall_milliseconds": round(item["wall_seconds"] * 1000),
        "item_count": item["item_count"],
        "failed_item_count": item["failed_item_count"],
        "inference_rtf_ppb": round(item["p95_inference_rtf"] * FIXED_POINT_SCALE),
        "end_to_end_rtf_ppb": round(item["p95_end_to_end_rtf"] * FIXED_POINT_SCALE),
        "wer_absolute_regression_ppb": round(
            item["wer_absolute_regression"] * FIXED_POINT_SCALE
        ),
        "wer_relative_regression_ppb": round(
            item["wer_relative_regression"] * FIXED_POINT_SCALE
        ),
        "term_recall_absolute_regression_ppb": round(
            item["term_recall_absolute_regression"] * FIXED_POINT_SCALE
        ),
        "exact_output_match": item["exact_output_match"],
        "peak_process_vram_bytes": item["peak_process_vram_bytes"],
        "minimum_free_vram_bytes": item["minimum_free_vram_bytes"],
        "maximum_temperature_millicelsius": round(
            item["maximum_temperature_c"] * 1000
        ),
        "thermal_throttle_observed": item["thermal_throttle_observed"],
        "oom_observed": item["oom_observed"],
        "deadline_exit_observed": item["deadline_exit_observed"],
        "network_isolated": item["network_isolated"],
        "cold_storage_visible": item["cold_storage_visible"],
        "writable_root_expansion_observed": item[
            "writable_root_expansion_observed"
        ],
        "crash_case_count": item["crash_case_count"],
        "crash_recovery_failure_count": item["crash_recovery_failure_count"],
        "lock_contention_passed": item["lock_contention_passed"],
        "lock_crash_release_passed": item["lock_crash_release_passed"],
    }


def _normalize_observation(value: Any, ordinal: int) -> dict[str, Any]:
    item = _exact(value, f"observation {ordinal}", OBSERVATION_FIELDS)
    result = {
        "ordinal": _integer(item["ordinal"], f"observation {ordinal}.ordinal", 0),
        "case_count": _integer(item["case_count"], f"observation {ordinal}.case_count", 0),
        "repetitions": _integer(item["repetitions"], f"observation {ordinal}.repetitions", 0),
        "audio_milliseconds": _integer(
            item["audio_milliseconds"], f"observation {ordinal}.audio_milliseconds", 0, 10**12
        ),
        "wall_milliseconds": _integer(
            item["wall_milliseconds"], f"observation {ordinal}.wall_milliseconds", 0, 10**12
        ),
        "item_count": _integer(item["item_count"], f"observation {ordinal}.item_count", 0),
        "failed_item_count": _integer(
            item["failed_item_count"], f"observation {ordinal}.failed_item_count", 0
        ),
        "inference_rtf_ppb": _integer(
            item["inference_rtf_ppb"], f"observation {ordinal}.inference_rtf_ppb", 0, 100 * FIXED_POINT_SCALE
        ),
        "end_to_end_rtf_ppb": _integer(
            item["end_to_end_rtf_ppb"], f"observation {ordinal}.end_to_end_rtf_ppb", 0, 100 * FIXED_POINT_SCALE
        ),
        "wer_absolute_regression_ppb": _integer(
            item["wer_absolute_regression_ppb"],
            f"observation {ordinal}.wer_absolute_regression_ppb",
            -FIXED_POINT_SCALE,
            FIXED_POINT_SCALE,
        ),
        "wer_relative_regression_ppb": _integer(
            item["wer_relative_regression_ppb"],
            f"observation {ordinal}.wer_relative_regression_ppb",
            -100 * FIXED_POINT_SCALE,
            100 * FIXED_POINT_SCALE,
        ),
        "term_recall_absolute_regression_ppb": _integer(
            item["term_recall_absolute_regression_ppb"],
            f"observation {ordinal}.term_recall_absolute_regression_ppb",
            -FIXED_POINT_SCALE,
            FIXED_POINT_SCALE,
        ),
        "peak_process_vram_bytes": _integer(
            item["peak_process_vram_bytes"], f"observation {ordinal}.peak_process_vram_bytes", 0
        ),
        "minimum_free_vram_bytes": _integer(
            item["minimum_free_vram_bytes"], f"observation {ordinal}.minimum_free_vram_bytes", 0
        ),
        "maximum_temperature_millicelsius": _integer(
            item["maximum_temperature_millicelsius"],
            f"observation {ordinal}.maximum_temperature_millicelsius",
            0,
            150_000,
        ),
        "crash_case_count": _integer(
            item["crash_case_count"], f"observation {ordinal}.crash_case_count", 0
        ),
        "crash_recovery_failure_count": _integer(
            item["crash_recovery_failure_count"],
            f"observation {ordinal}.crash_recovery_failure_count",
            0,
        ),
    }
    if result["ordinal"] != ordinal:
        raise AdmissionEvidenceError("observation ordinals must be contiguous from zero")
    for name in (
        "exact_output_match",
        "thermal_throttle_observed",
        "oom_observed",
        "deadline_exit_observed",
        "network_isolated",
        "cold_storage_visible",
        "writable_root_expansion_observed",
        "lock_contention_passed",
        "lock_crash_release_passed",
    ):
        if not isinstance(item[name], bool):
            raise AdmissionEvidenceError(f"observation {ordinal}.{name} must be boolean")
        result[name] = item[name]
    return result


def make_raw_evidence(core: Any) -> dict[str, Any]:
    """Canonicalize a bounded observation envelope and assign its content ID."""

    item = _exact(core, "raw admission evidence", RAW_CORE_FIELDS)
    if (
        item["kind"] != RAW_EVIDENCE_KIND
        or item["schema_version"] != RAW_EVIDENCE_SCHEMA_VERSION
        or item["implementation_version"] != RAW_EVIDENCE_IMPLEMENTATION_VERSION
        or item["gate"] not in GATES
        or item["reducer"] != REDUCER_REFERENCE
        or item["policy"] != RAW_POLICY
    ):
        raise AdmissionEvidenceError("raw admission evidence header or policy is unsupported")
    observations = item["observations"]
    if not isinstance(observations, list) or not 1 <= len(observations) <= MAX_OBSERVATIONS:
        raise AdmissionEvidenceError(
            f"raw observations must contain between 1 and {MAX_OBSERVATIONS} rows"
        )
    normalized = {
        "kind": RAW_EVIDENCE_KIND,
        "schema_version": RAW_EVIDENCE_SCHEMA_VERSION,
        "implementation_version": RAW_EVIDENCE_IMPLEMENTATION_VERSION,
        "gate": item["gate"],
        "production_profile_identity_sha256": _digest(
            item["production_profile_identity_sha256"], "raw production profile identity"
        ),
        "execution_image_identity_sha256": _digest(
            item["execution_image_identity_sha256"], "raw execution image identity"
        ),
        "runtime_candidate_identity_sha256": _digest(
            item["runtime_candidate_identity_sha256"], "raw runtime candidate identity"
        ),
        "reducer": dict(REDUCER_REFERENCE),
        "observations": [
            _normalize_observation(row, ordinal)
            for ordinal, row in enumerate(observations)
        ],
        "policy": dict(RAW_POLICY),
    }
    identity = sha256_bytes(canonical_bytes(normalized))
    return {
        **normalized,
        "identity_sha256": identity,
        "evidence_id": f"gpugateraw_{identity[:32]}",
    }


def validate_raw_evidence(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "raw admission evidence",
        RAW_CORE_FIELDS | {"identity_sha256", "evidence_id"},
    )
    expected = make_raw_evidence({key: item[key] for key in RAW_CORE_FIELDS})
    if canonical_bytes(item) != canonical_bytes(expected):
        raise AdmissionEvidenceError(
            "raw admission evidence is noncanonical or its identity is invalid"
        )
    return expected


def _nearest_rank_p95(values: list[int]) -> int:
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def _reduce_validated_raw(evidence: dict[str, Any]) -> dict[str, Any]:
    rows = evidence["observations"]
    adverse = (
        "thermal_throttle_observed",
        "oom_observed",
        "deadline_exit_observed",
        "cold_storage_visible",
        "writable_root_expansion_observed",
    )
    success = (
        "exact_output_match",
        "network_isolated",
        "lock_contention_passed",
        "lock_crash_release_passed",
    )
    metrics: dict[str, Any] = {
        "case_count": sum(row["case_count"] for row in rows),
        "repetitions": sum(row["repetitions"] for row in rows),
        "audio_seconds": sum(row["audio_milliseconds"] for row in rows) / 1000.0,
        "wall_seconds": sum(row["wall_milliseconds"] for row in rows) / 1000.0,
        "item_count": sum(row["item_count"] for row in rows),
        "failed_item_count": sum(row["failed_item_count"] for row in rows),
        "p95_inference_rtf": _nearest_rank_p95(
            [row["inference_rtf_ppb"] for row in rows]
        )
        / FIXED_POINT_SCALE,
        "p95_end_to_end_rtf": _nearest_rank_p95(
            [row["end_to_end_rtf_ppb"] for row in rows]
        )
        / FIXED_POINT_SCALE,
        "wer_absolute_regression": max(
            row["wer_absolute_regression_ppb"] for row in rows
        )
        / FIXED_POINT_SCALE,
        "wer_relative_regression": max(
            row["wer_relative_regression_ppb"] for row in rows
        )
        / FIXED_POINT_SCALE,
        "term_recall_absolute_regression": max(
            row["term_recall_absolute_regression_ppb"] for row in rows
        )
        / FIXED_POINT_SCALE,
        "peak_process_vram_bytes": max(row["peak_process_vram_bytes"] for row in rows),
        "minimum_free_vram_bytes": min(row["minimum_free_vram_bytes"] for row in rows),
        "maximum_temperature_c": max(
            row["maximum_temperature_millicelsius"] for row in rows
        )
        / 1000.0,
        "crash_case_count": sum(row["crash_case_count"] for row in rows),
        "crash_recovery_failure_count": sum(
            row["crash_recovery_failure_count"] for row in rows
        ),
    }
    metrics.update({name: any(row[name] for row in rows) for name in adverse})
    metrics.update({name: all(row[name] for row in rows) for name in success})
    return _normalize_metrics(metrics)


def reduce_raw_evidence(value: Any) -> dict[str, Any]:
    """Replay the identity-pinned deterministic reducer over a raw envelope."""

    return _reduce_validated_raw(validate_raw_evidence(value))


def make_raw_reference(path: str, evidence: Any) -> dict[str, Any]:
    """Return the exact gate-side reference for canonical raw evidence bytes."""

    value = validate_raw_evidence(evidence)
    if not isinstance(path, str) or not path.startswith("/") or "\x00" in path:
        raise AdmissionEvidenceError("raw_evidence.path must be absolute")
    body = canonical_bytes(value)
    return {
        "path": path,
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "kind": RAW_EVIDENCE_KIND,
        "schema_version": RAW_EVIDENCE_SCHEMA_VERSION,
        "identity_sha256": value["identity_sha256"],
        "reducer_identity_sha256": REDUCER_IDENTITY_SHA256,
    }


def _normalize_raw_reference(value: Any) -> dict[str, Any]:
    raw = _exact(value, "raw_evidence", RAW_REFERENCE_FIELDS)
    if not isinstance(raw["path"], str) or not raw["path"].startswith("/") or "\x00" in raw["path"]:
        raise AdmissionEvidenceError("raw_evidence.path must be absolute")
    if (
        raw["kind"] != RAW_EVIDENCE_KIND
        or raw["schema_version"] != RAW_EVIDENCE_SCHEMA_VERSION
        or raw["reducer_identity_sha256"] != REDUCER_IDENTITY_SHA256
    ):
        raise AdmissionEvidenceError("raw_evidence contract or reducer is unsupported")
    return {
        "path": raw["path"],
        "sha256": _digest(raw["sha256"], "raw evidence SHA-256"),
        "byte_count": _integer(raw["byte_count"], "raw evidence byte_count", 1),
        "kind": RAW_EVIDENCE_KIND,
        "schema_version": RAW_EVIDENCE_SCHEMA_VERSION,
        "identity_sha256": _digest(raw["identity_sha256"], "raw evidence identity"),
        "reducer_identity_sha256": REDUCER_IDENTITY_SHA256,
    }


def _require_common_success(metrics: dict[str, Any]) -> None:
    if (
        metrics["failed_item_count"] != 0
        or metrics["oom_observed"]
        or metrics["deadline_exit_observed"]
        or metrics["peak_process_vram_bytes"] > 4 * 1024**3
        or metrics["minimum_free_vram_bytes"] < 2 * 1024**3
        or metrics["maximum_temperature_c"] > 80
        or metrics["thermal_throttle_observed"]
    ):
        raise AdmissionEvidenceError("gate metrics violate common resource/success thresholds")


def _require_gate(gate: str, metrics: dict[str, Any]) -> None:
    _require_common_success(metrics)
    if gate == "accuracy":
        if (
            metrics["case_count"] < 12
            or metrics["repetitions"] < 3
            or metrics["audio_seconds"] < 3_600
            or metrics["wer_absolute_regression"] > 0.005
            or metrics["wer_relative_regression"] > 0.03
            or metrics["term_recall_absolute_regression"] > 0.005
        ):
            raise AdmissionEvidenceError("accuracy coverage or regression threshold failed")
    elif gate == "semantic_compatibility":
        if metrics["item_count"] < 3 or not metrics["exact_output_match"]:
            raise AdmissionEvidenceError("semantic compatibility requires exact multi-item equality")
    elif gate == "packed_30m":
        if metrics["audio_seconds"] < 1_800 or metrics["p95_end_to_end_rtf"] > 1 / 30:
            raise AdmissionEvidenceError("30-minute packed throughput gate failed")
    elif gate == "packed_2h":
        if metrics["audio_seconds"] < 7_200 or metrics["p95_end_to_end_rtf"] > 1 / 30:
            raise AdmissionEvidenceError("two-hour packed throughput gate failed")
    elif gate == "thermal_8h":
        if metrics["wall_seconds"] < 8 * 60 * 60:
            raise AdmissionEvidenceError("thermal gate requires eight wall-clock hours")
    elif gate == "batch_32":
        if metrics["item_count"] < 32:
            raise AdmissionEvidenceError("batch gate requires at least 32 completed items")
    elif gate == "scheduler":
        if not metrics["lock_contention_passed"] or not metrics["lock_crash_release_passed"]:
            raise AdmissionEvidenceError("scheduler lock gates failed")
    elif gate == "crash_recovery":
        if metrics["crash_case_count"] < 8 or metrics["crash_recovery_failure_count"]:
            raise AdmissionEvidenceError("crash-recovery coverage or result failed")
    elif gate == "launcher_isolation":
        if (
            not metrics["network_isolated"]
            or metrics["cold_storage_visible"]
            or metrics["writable_root_expansion_observed"]
        ):
            raise AdmissionEvidenceError("trusted launcher isolation gate failed")
    else:  # pragma: no cover - gated by validate header.
        raise AdmissionEvidenceError("unsupported gate")


CORE_FIELDS = {
    "kind",
    "schema_version",
    "implementation_version",
    "gate",
    "status",
    "production_profile_identity_sha256",
    "execution_image_identity_sha256",
    "runtime_candidate_identity_sha256",
    "raw_evidence",
    "metrics",
    "policy",
}


def make_gate(core: Any) -> dict[str, Any]:
    item = _exact(core, "admission gate", CORE_FIELDS)
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["gate"] not in GATES
        or item["status"] != "passed"
        or item["policy"] != POLICY
    ):
        raise AdmissionEvidenceError("admission gate header or policy is unsupported")
    raw = _normalize_raw_reference(item["raw_evidence"])
    normalized = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "gate": item["gate"],
        "status": "passed",
        "production_profile_identity_sha256": _digest(
            item["production_profile_identity_sha256"], "production profile identity"
        ),
        "execution_image_identity_sha256": _digest(
            item["execution_image_identity_sha256"], "execution image identity"
        ),
        "runtime_candidate_identity_sha256": _digest(
            item["runtime_candidate_identity_sha256"],
            "runtime candidate closure identity",
        ),
        "raw_evidence": raw,
        "metrics": _normalize_metrics(item["metrics"]),
        "policy": dict(POLICY),
    }
    _require_gate(normalized["gate"], normalized["metrics"])
    identity = sha256_bytes(canonical_bytes(normalized))
    return {
        **normalized,
        "identity_sha256": identity,
        "gate_id": f"gpugate_{identity[:32]}",
    }


def validate_gate(value: Any) -> dict[str, Any]:
    item = _exact(value, "admission gate", CORE_FIELDS | {"identity_sha256", "gate_id"})
    expected = make_gate({key: item[key] for key in CORE_FIELDS})
    if canonical_bytes(item) != canonical_bytes(expected):
        raise AdmissionEvidenceError("admission gate is noncanonical or its identity is invalid")
    return expected


def validate_gate_against_raw(
    gate_value: Any,
    raw_value: Any,
    *,
    raw_body: bytes | None = None,
) -> dict[str, Any]:
    """Deep-replay a gate against the exact typed raw evidence it references."""

    gate = validate_gate(gate_value)
    evidence = validate_raw_evidence(raw_value)
    reference = gate["raw_evidence"]
    canonical = canonical_bytes(evidence)
    if raw_body is not None and raw_body != canonical:
        raise AdmissionEvidenceError("raw admission evidence is not canonical JSON")
    bound_body = canonical if raw_body is None else raw_body
    if (
        len(bound_body) != reference["byte_count"]
        or sha256_bytes(bound_body) != reference["sha256"]
        or evidence["identity_sha256"] != reference["identity_sha256"]
        or evidence["reducer"]["identity_sha256"]
        != reference["reducer_identity_sha256"]
    ):
        raise AdmissionEvidenceError("raw admission evidence differs from its gate binding")
    if (
        evidence["gate"] != gate["gate"]
        or evidence["production_profile_identity_sha256"]
        != gate["production_profile_identity_sha256"]
        or evidence["execution_image_identity_sha256"]
        != gate["execution_image_identity_sha256"]
        or evidence["runtime_candidate_identity_sha256"]
        != gate["runtime_candidate_identity_sha256"]
    ):
        raise AdmissionEvidenceError("raw admission evidence lineage is inconsistent")
    if gate["metrics"] != _reduce_validated_raw(evidence):
        raise AdmissionEvidenceError(
            "gate aggregate metrics do not replay from raw observations"
        )
    return gate


def empty_metrics() -> dict[str, Any]:
    """Return a complete zero/false metric vector for evidence producers."""

    return {
        "case_count": 0,
        "repetitions": 0,
        "audio_seconds": 0.0,
        "wall_seconds": 0.0,
        "item_count": 0,
        "failed_item_count": 0,
        "p95_inference_rtf": 0.0,
        "p95_end_to_end_rtf": 0.0,
        "wer_absolute_regression": 0.0,
        "wer_relative_regression": 0.0,
        "term_recall_absolute_regression": 0.0,
        "exact_output_match": False,
        "peak_process_vram_bytes": 0,
        "minimum_free_vram_bytes": 2 * 1024**3,
        "maximum_temperature_c": 0.0,
        "thermal_throttle_observed": False,
        "oom_observed": False,
        "deadline_exit_observed": False,
        "network_isolated": False,
        "cold_storage_visible": False,
        "writable_root_expansion_observed": False,
        "crash_case_count": 0,
        "crash_recovery_failure_count": 0,
        "lock_contention_passed": False,
        "lock_crash_release_passed": False,
    }
