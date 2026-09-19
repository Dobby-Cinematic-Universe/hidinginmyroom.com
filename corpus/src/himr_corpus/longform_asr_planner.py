"""Pure planning for whole-recording and long-form logical-span ASR.

The planner operates only on JSON metadata.  It never opens the parent media,
decodes audio, invokes a media tool, or materializes audio chunks.  Every time
coordinate is an integer frame in the normalized mono 16 kHz parent timeline.
Presentation timestamps are deliberately downstream of this contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
PLANNER_VERSION = "himr-longform-asr-planner/1"

MANIFEST_KIND = "himr_longform_recording_input_manifest"
POLICY_KIND = "himr_longform_asr_planning_policy"
PLAN_KIND = "himr_longform_asr_plan"

SAMPLE_RATE_HZ = 16_000
CHANNELS = 1
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_BOUNDARY_CANDIDATES = 1_000_000
MAX_POLICY_SPAN_COUNT = 4_096
MAX_SIGNED_64 = (1 << 63) - 1

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
BOUNDARY_KIND_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

MANIFEST_KEYS = {
    "boundary_candidates",
    "kind",
    "recording",
    "schema_version",
}
RECORDING_KEYS = {"input", "media_id", "recording_id"}
INPUT_KEYS = {
    "artifact_id",
    "byte_count",
    "channels",
    "duration_ms",
    "path",
    "sample_rate_hz",
    "sha256",
    "total_samples",
}
BOUNDARY_KEYS = {"confidence_millionths", "kind", "sample"}
POLICY_KEYS = {
    "adaptive",
    "direct_max_samples",
    "kind",
    "schema_version",
}
ADAPTIVE_POLICY_KEYS = {
    "boundary_search_samples",
    "max_core_samples",
    "max_span_count",
    "min_core_samples",
    "padding_samples",
    "target_core_samples",
}
PLAN_KEYS = {
    "boundary_candidates",
    "coverage",
    "execution_contract",
    "identity_sha256",
    "input_digests",
    "kind",
    "plan_id",
    "planner_version",
    "policy",
    "recording",
    "schema_version",
    "spans",
    "strategy",
}
SPAN_KEYS = {
    "analysis_end_sample",
    "analysis_start_sample",
    "boundary_confidence_millionths",
    "boundary_reason",
    "core_end_sample",
    "core_start_sample",
    "ordinal",
    "padding",
    "span_id",
}
PADDING_KEYS = {
    "applied_left_samples",
    "applied_right_samples",
    "clipped_left_samples",
    "clipped_right_samples",
    "requested_left_samples",
    "requested_right_samples",
}


class LongformPlanningError(ValueError):
    """A long-form planning input or generated plan failed closed."""


def canonical_json(value: object) -> str:
    """Return the repository's compact, sorted UTF-8 JSON representation."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _exact_object(value: object, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LongformPlanningError(f"{label} must be an object")
    actual = set(value)
    if actual != keys:
        raise LongformPlanningError(
            f"{label} has unknown shape "
            f"(missing={sorted(keys - actual)}, extra={sorted(actual - keys)})"
        )
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_SIGNED_64,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LongformPlanningError(f"{label} must be an integer")
    if value < minimum or value > maximum:
        raise LongformPlanningError(
            f"{label} must be between {minimum} and {maximum}"
        )
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or IDENTIFIER_RE.fullmatch(value) is None:
        raise LongformPlanningError(f"{label} is not a bounded identifier")
    return value


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise LongformPlanningError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _absolute_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 4096
        or "\x00" in value
        or any(ord(character) < 32 for character in value)
        or not Path(value).is_absolute()
    ):
        raise LongformPlanningError(f"{label} must be a bounded absolute path")
    return value


def duration_ms_for_samples(total_samples: int) -> int:
    """Nearest integer millisecond; sample coordinates remain authoritative."""

    samples = _integer(total_samples, "total samples", minimum=1)
    return (samples * 1_000 + SAMPLE_RATE_HZ // 2) // SAMPLE_RATE_HZ


def validate_recording_manifest(value: object) -> dict[str, Any]:
    """Validate and normalize one metadata-only recording input manifest."""

    manifest = _exact_object(value, MANIFEST_KEYS, "recording manifest")
    if (
        manifest["kind"] != MANIFEST_KIND
        or manifest["schema_version"] != SCHEMA_VERSION
    ):
        raise LongformPlanningError(
            "recording manifest kind or schema version is unsupported"
        )

    recording = _exact_object(manifest["recording"], RECORDING_KEYS, "recording")
    recording_id = _identifier(recording["recording_id"], "recording.recording_id")
    media_id = _identifier(recording["media_id"], "recording.media_id")
    input_value = _exact_object(recording["input"], INPUT_KEYS, "recording.input")
    artifact_id = _identifier(input_value["artifact_id"], "recording.input.artifact_id")
    input_path = _absolute_path(input_value["path"], "recording.input.path")
    input_sha256 = _sha256(input_value["sha256"], "recording.input.sha256")
    byte_count = _integer(
        input_value["byte_count"], "recording.input.byte_count", minimum=1
    )
    sample_rate = _integer(
        input_value["sample_rate_hz"], "recording.input.sample_rate_hz", minimum=1
    )
    channels = _integer(input_value["channels"], "recording.input.channels", minimum=1)
    if sample_rate != SAMPLE_RATE_HZ or channels != CHANNELS:
        raise LongformPlanningError(
            "recording input must be normalized mono 16000 Hz audio"
        )
    total_samples = _integer(
        input_value["total_samples"], "recording.input.total_samples", minimum=1
    )
    duration_ms = _integer(input_value["duration_ms"], "recording.input.duration_ms")
    if duration_ms != duration_ms_for_samples(total_samples):
        raise LongformPlanningError(
            "recording.input.duration_ms is inconsistent with total_samples at 16000 Hz"
        )

    candidates_value = manifest["boundary_candidates"]
    if not isinstance(candidates_value, list):
        raise LongformPlanningError("boundary_candidates must be an array")
    if len(candidates_value) > MAX_BOUNDARY_CANDIDATES:
        raise LongformPlanningError("boundary_candidates exceeds the bounded count")
    candidates: list[dict[str, Any]] = []
    seen_candidates: set[tuple[int, str, int]] = set()
    for ordinal, raw_candidate in enumerate(candidates_value):
        candidate = _exact_object(
            raw_candidate, BOUNDARY_KEYS, f"boundary_candidates[{ordinal}]"
        )
        sample = _integer(
            candidate["sample"],
            f"boundary_candidates[{ordinal}].sample",
            minimum=1,
            maximum=total_samples - 1,
        )
        kind = candidate["kind"]
        if not isinstance(kind, str) or BOUNDARY_KIND_RE.fullmatch(kind) is None:
            raise LongformPlanningError(
                f"boundary_candidates[{ordinal}].kind is not a bounded label"
            )
        confidence = _integer(
            candidate["confidence_millionths"],
            f"boundary_candidates[{ordinal}].confidence_millionths",
            maximum=1_000_000,
        )
        identity = (sample, kind, confidence)
        if identity in seen_candidates:
            raise LongformPlanningError("boundary_candidates contains a duplicate")
        seen_candidates.add(identity)
        candidates.append(
            {
                "confidence_millionths": confidence,
                "kind": kind,
                "sample": sample,
            }
        )
    candidates.sort(
        key=lambda item: (
            item["sample"],
            item["kind"],
            item["confidence_millionths"],
        )
    )
    return {
        "boundary_candidates": candidates,
        "kind": MANIFEST_KIND,
        "recording": {
            "input": {
                "artifact_id": artifact_id,
                "byte_count": byte_count,
                "channels": CHANNELS,
                "duration_ms": duration_ms,
                "path": input_path,
                "sample_rate_hz": SAMPLE_RATE_HZ,
                "sha256": input_sha256,
                "total_samples": total_samples,
            },
            "media_id": media_id,
            "recording_id": recording_id,
        },
        "schema_version": SCHEMA_VERSION,
    }


def validate_planning_policy(value: object) -> dict[str, Any]:
    """Validate the explicit direct/adaptive decision and span policy."""

    policy = _exact_object(value, POLICY_KEYS, "planning policy")
    if policy["kind"] != POLICY_KIND or policy["schema_version"] != SCHEMA_VERSION:
        raise LongformPlanningError(
            "planning policy kind or schema version is unsupported"
        )
    direct_max = _integer(
        policy["direct_max_samples"], "planning policy direct_max_samples", minimum=1
    )
    adaptive = _exact_object(
        policy["adaptive"], ADAPTIVE_POLICY_KEYS, "planning policy adaptive"
    )
    minimum = _integer(
        adaptive["min_core_samples"], "adaptive.min_core_samples", minimum=1
    )
    target = _integer(
        adaptive["target_core_samples"], "adaptive.target_core_samples", minimum=1
    )
    maximum = _integer(
        adaptive["max_core_samples"], "adaptive.max_core_samples", minimum=1
    )
    search = _integer(
        adaptive["boundary_search_samples"], "adaptive.boundary_search_samples"
    )
    padding = _integer(adaptive["padding_samples"], "adaptive.padding_samples")
    max_spans = _integer(
        adaptive["max_span_count"],
        "adaptive.max_span_count",
        minimum=1,
        maximum=MAX_POLICY_SPAN_COUNT,
    )
    if not minimum <= target <= maximum:
        raise LongformPlanningError(
            "adaptive core sizes must satisfy min_core_samples <= "
            "target_core_samples <= max_core_samples"
        )
    if direct_max < maximum:
        raise LongformPlanningError(
            "direct_max_samples must be at least adaptive.max_core_samples"
        )
    if padding > maximum:
        raise LongformPlanningError(
            "adaptive.padding_samples must not exceed max_core_samples"
        )
    return {
        "adaptive": {
            "boundary_search_samples": search,
            "max_core_samples": maximum,
            "max_span_count": max_spans,
            "min_core_samples": minimum,
            "padding_samples": padding,
            "target_core_samples": target,
        },
        "direct_max_samples": direct_max,
        "kind": POLICY_KIND,
        "schema_version": SCHEMA_VERSION,
    }


def _adaptive_cores(
    total_samples: int,
    policy: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> tuple[list[tuple[int, int]], list[tuple[str, int | None]]]:
    adaptive = policy["adaptive"]
    minimum = adaptive["min_core_samples"]
    target_size = adaptive["target_core_samples"]
    maximum = adaptive["max_core_samples"]
    search = adaptive["boundary_search_samples"]
    max_spans = adaptive["max_span_count"]

    cores: list[tuple[int, int]] = []
    boundaries: list[tuple[str, int | None]] = []
    start = 0
    while total_samples - start > maximum:
        earliest = start + minimum
        latest = min(start + maximum, total_samples - minimum)
        if earliest > latest:
            raise LongformPlanningError(
                "adaptive policy cannot leave a valid final core"
            )
        target = min(start + target_size, latest)
        eligible = [
            candidate
            for candidate in candidates
            if earliest <= candidate["sample"] <= latest
            and abs(candidate["sample"] - target) <= search
        ]
        if eligible:
            chosen = min(
                eligible,
                key=lambda candidate: (
                    abs(candidate["sample"] - target),
                    -candidate["confidence_millionths"],
                    candidate["sample"],
                    candidate["kind"],
                ),
            )
            end = chosen["sample"]
            boundary = (
                f"candidate_{chosen['kind']}",
                chosen["confidence_millionths"],
            )
        else:
            end = max(earliest, min(start + target_size, latest))
            boundary = ("fixed_target", None)
        cores.append((start, end))
        boundaries.append(boundary)
        if len(cores) >= max_spans:
            raise LongformPlanningError("adaptive plan exceeds policy max_span_count")
        start = end

    cores.append((start, total_samples))
    boundaries.append(("recording_end", None))
    if len(cores) > max_spans:
        raise LongformPlanningError("adaptive plan exceeds policy max_span_count")
    return cores, boundaries


def _span(
    *,
    ordinal: int,
    core_start: int,
    core_end: int,
    total_samples: int,
    requested_padding: int,
    boundary_reason: str,
    boundary_confidence: int | None,
    manifest_sha256: str,
    policy_sha256: str,
) -> dict[str, Any]:
    analysis_start = max(0, core_start - requested_padding)
    analysis_end = min(total_samples, core_end + requested_padding)
    applied_left = core_start - analysis_start
    applied_right = analysis_end - core_end
    padding = {
        "applied_left_samples": applied_left,
        "applied_right_samples": applied_right,
        "clipped_left_samples": requested_padding - applied_left,
        "clipped_right_samples": requested_padding - applied_right,
        "requested_left_samples": requested_padding,
        "requested_right_samples": requested_padding,
    }
    span_core = {
        "analysis_end_sample": analysis_end,
        "analysis_start_sample": analysis_start,
        "boundary_confidence_millionths": boundary_confidence,
        "boundary_reason": boundary_reason,
        "core_end_sample": core_end,
        "core_start_sample": core_start,
        "ordinal": ordinal,
        "padding": padding,
    }
    span_identity = canonical_sha256(
        {
            "kind": "himr_longform_asr_logical_span",
            "manifest_sha256": manifest_sha256,
            "policy_sha256": policy_sha256,
            "schema_version": SCHEMA_VERSION,
            "span": span_core,
        }
    )
    return {**span_core, "span_id": f"lfspan_{span_identity[:32]}"}


def build_longform_asr_plan(manifest: object, policy: object) -> dict[str, Any]:
    """Build a deterministic plan without reading or writing any media bytes."""

    normalized_manifest = validate_recording_manifest(manifest)
    normalized_policy = validate_planning_policy(policy)
    manifest_sha256 = canonical_sha256(normalized_manifest)
    policy_sha256 = canonical_sha256(normalized_policy)
    total_samples = normalized_manifest["recording"]["input"]["total_samples"]
    candidates = normalized_manifest["boundary_candidates"]

    if total_samples <= normalized_policy["direct_max_samples"]:
        strategy = "direct"
        cores = [(0, total_samples)]
        boundaries: list[tuple[str, int | None]] = [("recording_end", None)]
        requested_padding = 0
    else:
        strategy = "adaptive_spans"
        cores, boundaries = _adaptive_cores(
            total_samples, normalized_policy, candidates
        )
        requested_padding = normalized_policy["adaptive"]["padding_samples"]

    spans = [
        _span(
            ordinal=ordinal,
            core_start=core[0],
            core_end=core[1],
            total_samples=total_samples,
            requested_padding=requested_padding,
            boundary_reason=boundaries[ordinal][0],
            boundary_confidence=boundaries[ordinal][1],
            manifest_sha256=manifest_sha256,
            policy_sha256=policy_sha256,
        )
        for ordinal, core in enumerate(cores)
    ]
    core_sample_count_sum = sum(
        span["core_end_sample"] - span["core_start_sample"] for span in spans
    )
    analysis_sample_count_sum = sum(
        span["analysis_end_sample"] - span["analysis_start_sample"] for span in spans
    )
    padding_sample_count_sum = sum(
        span["padding"]["applied_left_samples"]
        + span["padding"]["applied_right_samples"]
        for span in spans
    )
    coverage = {
        "analysis_overlap_samples": analysis_sample_count_sum - total_samples,
        "analysis_sample_count_sum": analysis_sample_count_sum,
        "analysis_unique_sample_count": total_samples,
        "core_gap_samples": 0,
        "core_overlap_samples": 0,
        "core_sample_count_sum": core_sample_count_sum,
        "padding_sample_count_sum": padding_sample_count_sum,
        "recording_end_sample": total_samples,
        "recording_sample_count": total_samples,
        "recording_start_sample": 0,
        "span_count": len(spans),
    }
    plan_core = {
        "boundary_candidates": candidates,
        "coverage": coverage,
        "execution_contract": {
            "analysis_input": "direct_read_from_single_parent_media",
            "audio_chunk_materialization": "forbidden",
            "coordinate_space": "decoded_parent_pcm_samples",
            "core_ownership": "exactly_one_span",
            "interval_semantics": "zero_based_half_open",
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "timestamp_authority": "integer_sample_coordinates",
        },
        "input_digests": {
            "planning_policy_sha256": policy_sha256,
            "recording_manifest_sha256": manifest_sha256,
        },
        "kind": PLAN_KIND,
        "planner_version": PLANNER_VERSION,
        "policy": normalized_policy,
        "recording": normalized_manifest["recording"],
        "schema_version": SCHEMA_VERSION,
        "spans": spans,
        "strategy": strategy,
    }
    identity_sha256 = canonical_sha256(plan_core)
    return {
        **plan_core,
        "identity_sha256": identity_sha256,
        "plan_id": f"lfplan_{identity_sha256[:32]}",
    }


def validate_longform_asr_plan(value: object) -> dict[str, Any]:
    """Validate an entire plan by deterministically rebuilding it."""

    plan = _exact_object(value, PLAN_KEYS, "long-form ASR plan")
    manifest = {
        "boundary_candidates": plan["boundary_candidates"],
        "kind": MANIFEST_KIND,
        "recording": plan["recording"],
        "schema_version": SCHEMA_VERSION,
    }
    expected = build_longform_asr_plan(manifest, plan["policy"])
    if canonical_json(plan) != canonical_json(expected):
        raise LongformPlanningError(
            "long-form ASR plan differs from its deterministic contract"
        )
    return expected


def load_strict_json(path_value: str | Path, label: str) -> dict[str, Any]:
    """Load bounded UTF-8 JSON while rejecting duplicate keys and NaN values."""

    path = Path(path_value)
    try:
        with path.open("rb") as handle:
            body = handle.read(MAX_JSON_BYTES + 1)
    except OSError as error:
        raise LongformPlanningError(f"{label} cannot be read: {error}") from error
    if len(body) > MAX_JSON_BYTES:
        raise LongformPlanningError(f"{label} exceeds the bounded byte cap")

    def reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise LongformPlanningError(f"{label} contains duplicate key {key!r}")
            result[key] = item
        return result

    def reject_constant(value: str) -> None:
        raise LongformPlanningError(f"{label} contains non-finite number {value}")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LongformPlanningError(
            f"{label} is not strict UTF-8 JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise LongformPlanningError(f"{label} must contain one JSON object")
    return value


__all__ = [
    "CHANNELS",
    "LongformPlanningError",
    "MANIFEST_KIND",
    "PLAN_KIND",
    "PLANNER_VERSION",
    "POLICY_KIND",
    "SAMPLE_RATE_HZ",
    "SCHEMA_VERSION",
    "SPAN_KEYS",
    "build_longform_asr_plan",
    "canonical_json",
    "canonical_sha256",
    "duration_ms_for_samples",
    "load_strict_json",
    "validate_longform_asr_plan",
    "validate_planning_policy",
    "validate_recording_manifest",
]
