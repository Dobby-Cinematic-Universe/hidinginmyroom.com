#!/usr/bin/env python3
"""Isolated long-form ASR span execution contracts.

This module is a successor seam, not an amendment to the running GPU v5/batch-v2
lane.  It turns one validated long-form plan span into one closed execution work
order, calls an injected engine exactly once, and produces transcript-only
artifacts.  Audio is always addressed as a logical view of the immutable parent
recording; this module neither creates nor names a persistent audio chunk.

Exact 16 kHz, half-open sample coordinates are authoritative in work orders,
``transcript.json``, and results.  Transcript rows are compact decode-span-local
hypotheses.  Parent offsets, policy, engine provenance, and aggregate overlap/core
dispositions live once in the companion result for the overlap-aware assembler.

The engine is dependency-injected.  ``FasterWhisperModelEngine`` is a thin adapter
that passes a retained parent descriptor directly for a whole recording and
requires an ephemeral in-memory decoder for a logical span.  Unit tests use a
fake engine and perform no CUDA work.
"""

from __future__ import annotations

import ctypes
import hashlib
import importlib.util
import json
import math
import os
import re
import secrets
import stat
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MethodType
from typing import Any, Callable, Mapping, Protocol, Sequence


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
WORK_ORDER_KIND = "himr_longform_asr_span_work_order"
RESULT_KIND = "himr_longform_asr_span_result"
NORMALIZED_TRANSCRIPT_KIND = "himr_longform_asr_span_transcript"
PLAN_KIND = "himr_longform_asr_plan"

SAMPLE_RATE_HZ = 16_000
CHANNELS = 1
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_TEXT_CHARACTERS = 16 * 1024 * 1024
MAX_SEGMENTS = 1_000_000
MAX_WORDS = 10_000_000
MAX_PROMPT_CHARACTERS = 8_192
MAX_HOTWORDS = 512
MAX_HOTWORD_CHARACTERS = 128

SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
WORK_ORDER_ID_RE = re.compile(r"longasrwo1_[0-9a-f]{32}\Z")
RESULT_ID_RE = re.compile(r"longasrresult1_[0-9a-f]{32}\Z")
DOCUMENT_ID_RE = re.compile(r"longasr(?:raw|transcript)1_[0-9a-f]{32}\Z")

STRATEGIES = frozenset({"direct", "adaptive_spans"})
DISPOSITIONS = frozenset(
    {
        "left_context_only",
        "right_context_only",
        "core_owned",
        "crosses_left_core_boundary",
        "crosses_right_core_boundary",
        "crosses_both_core_boundaries",
    }
)

POLICY = {
    "visibility": "private",
    "machine_generated": True,
    "scores_calibrated": False,
    "network_access": False,
    "persistent_audio_chunks": False,
    "input_authority": "immutable_parent_audio_logical_view_only",
    "coordinate_authority": "exact_16000hz_half_open_samples",
    "overlap_ownership_authority": "downstream_assembler_only",
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "biometric_authority": "none",
    "wiki_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}

TRANSCRIPT_SEMANTICS = {
    "coordinate_system": "decode_span_local_16000hz_half_open_samples",
    "parent_projection": "add_result_span_analysis_start_sample",
    "recording_projection": "overlap_aware_assembler_required",
    "forced_language": "en",
    "language_selection_basis": "forced_by_execution_work_order",
    "language_detection_performed": False,
    "forced_language_probability": None,
    "raw_model_word_times": "nearest_16000hz_sample_preserved",
    "finite_paired_word_times": "non_inverted_required",
    "word_vs_segment_or_previous": "retained_and_explicitly_flagged",
    "word_timing_flags": [
        "precedes_segment_start",
        "extends_beyond_segment_end",
        "start_regresses_from_previous",
        "overlaps_previous",
    ],
}

TRANSCRIPT_CORE_KEYS = frozenset(
    {
        "kind",
        "schema_version",
        "work_order",
        "plan",
        "span_id",
        "analysis_sample_count",
        "language",
        "segments",
        "text",
        "segment_count",
        "word_count",
        "review_status",
        "semantics",
    }
)

WORK_ORDER_CORE_KEYS = frozenset(
    {
        "kind",
        "schema_version",
        "implementation_version",
        "job_id",
        "plan",
        "recording",
        "strategy",
        "span",
        "decoding",
        "execution_lineage",
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
        "plan",
        "recording",
        "span",
        "execution_lineage",
        "engine",
        "execution",
        "coverage",
        "dispositions",
        "artifacts",
        "policy",
    }
)


class LongFormExecutionError(RuntimeError):
    """A long-form plan projection, engine response, or result is invalid."""


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
        raise LongFormExecutionError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LongFormExecutionError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def parse_json(body: bytes, label: str) -> Any:
    try:
        return json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                LongFormExecutionError(f"{label} contains non-finite value {value}")
            ),
        )
    except LongFormExecutionError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise LongFormExecutionError(f"{label} is not strict JSON: {error}") from error


def _exact(value: Any, label: str, fields: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise LongFormExecutionError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise LongFormExecutionError(
            f"{label} must be an integer within [{minimum}, {maximum}]"
        )
    return value


def _number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LongFormExecutionError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise LongFormExecutionError(
            f"{label} must be finite within [{minimum}, {maximum}]"
        )
    return result


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise LongFormExecutionError(f"{label} must be a lowercase SHA-256")
    return value


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
        raise LongFormExecutionError(f"{label} is invalid")
    return value


def _text(value: Any, label: str, maximum: int) -> str:
    if not isinstance(value, str) or len(value) > maximum:
        raise LongFormExecutionError(f"{label} must be a string of at most {maximum} characters")
    if any(ord(character) < 32 and character not in "\t\n\r" for character in value):
        raise LongFormExecutionError(f"{label} contains unsupported control characters")
    return value


def _absolute(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise LongFormExecutionError(f"{label} must be a bounded absolute path")
    path = Path(value)
    if not path.is_absolute() or path == Path("/") or os.path.normpath(value) != value or "//" in value:
        raise LongFormExecutionError(f"{label} must be one normalized absolute non-root path")
    return path


def samples_to_ms(sample: int) -> int:
    """Nearest integer millisecond for an exact non-negative 16 kHz coordinate."""

    _integer(sample, "sample coordinate", 0)
    return (sample * 1000 + SAMPLE_RATE_HZ // 2) // SAMPLE_RATE_HZ


def seconds_to_samples(value: Any, label: str, maximum_samples: int) -> tuple[int, bool]:
    maximum_seconds = maximum_samples / SAMPLE_RATE_HZ
    seconds = _number(value, label, 0, maximum_seconds + 2.0)
    observed = math.floor(seconds * SAMPLE_RATE_HZ + 0.5)
    if observed > maximum_samples + 2 * SAMPLE_RATE_HZ:
        raise LongFormExecutionError(f"{label} exceeds the decode interval")
    return min(observed, maximum_samples), observed > maximum_samples


def _sample_interval(value: Any, label: str, *, total_samples: int) -> dict[str, int]:
    item = _exact(value, label, {"start_sample", "end_sample", "sample_count"})
    start = _integer(item["start_sample"], f"{label}.start_sample", 0, total_samples)
    end = _integer(item["end_sample"], f"{label}.end_sample", start, total_samples)
    count = _integer(item["sample_count"], f"{label}.sample_count", 0, total_samples)
    if count != end - start:
        raise LongFormExecutionError(f"{label}.sample_count is inconsistent")
    return {"start_sample": start, "end_sample": end, "sample_count": count}


def _normalize_recording(value: Any) -> dict[str, Any]:
    item = _exact(value, "recording", {"recording_id", "media_id", "input"})
    source = _exact(
        item["input"],
        "recording.input",
        {
            "artifact_id",
            "path",
            "sha256",
            "byte_count",
            "sample_rate_hz",
            "channels",
            "total_samples",
            "duration_ms",
        },
    )
    total_samples = _integer(source["total_samples"], "recording total samples", 1)
    duration_ms = _integer(source["duration_ms"], "recording duration", 0)
    if source["sample_rate_hz"] != SAMPLE_RATE_HZ or source["channels"] != CHANNELS:
        raise LongFormExecutionError("long-form input must be exact 16 kHz mono audio")
    if duration_ms != samples_to_ms(total_samples):
        raise LongFormExecutionError("recording duration is inconsistent with exact samples")
    return {
        "recording_id": _identifier(item["recording_id"], "recording_id"),
        "media_id": _identifier(item["media_id"], "media_id"),
        "input": {
            "artifact_id": _identifier(source["artifact_id"], "artifact_id"),
            "path": str(_absolute(source["path"], "recording input path")),
            "sha256": _digest(source["sha256"], "recording input SHA-256"),
            "byte_count": _integer(source["byte_count"], "recording input byte count", 1),
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "channels": CHANNELS,
            "total_samples": total_samples,
            "duration_ms": duration_ms,
        },
    }


def _span_from_planner(value: Any, recording: dict[str, Any]) -> dict[str, Any]:
    item = _exact(
        value,
        "planner span",
        {
            "span_id",
            "ordinal",
            "analysis_start_sample",
            "analysis_end_sample",
            "core_start_sample",
            "core_end_sample",
            "boundary_reason",
            "boundary_confidence_millionths",
            "padding",
        },
    )
    total = recording["input"]["total_samples"]
    analysis = _sample_interval(
        {
            "start_sample": item["analysis_start_sample"],
            "end_sample": item["analysis_end_sample"],
            "sample_count": item["analysis_end_sample"] - item["analysis_start_sample"]
            if isinstance(item["analysis_start_sample"], int)
            and not isinstance(item["analysis_start_sample"], bool)
            and isinstance(item["analysis_end_sample"], int)
            and not isinstance(item["analysis_end_sample"], bool)
            else -1,
        },
        "analysis interval",
        total_samples=total,
    )
    core = _sample_interval(
        {
            "start_sample": item["core_start_sample"],
            "end_sample": item["core_end_sample"],
            "sample_count": item["core_end_sample"] - item["core_start_sample"]
            if isinstance(item["core_start_sample"], int)
            and not isinstance(item["core_start_sample"], bool)
            and isinstance(item["core_end_sample"], int)
            and not isinstance(item["core_end_sample"], bool)
            else -1,
        },
        "core interval",
        total_samples=total,
    )
    if analysis["sample_count"] < 1 or core["sample_count"] < 1:
        raise LongFormExecutionError("analysis and core intervals must be non-empty")
    if not (
        analysis["start_sample"] <= core["start_sample"]
        and core["end_sample"] <= analysis["end_sample"]
    ):
        raise LongFormExecutionError("analysis interval must contain the core interval")
    padding = _exact(
        item["padding"],
        "planner span padding",
        {
            "requested_left_samples",
            "requested_right_samples",
            "applied_left_samples",
            "applied_right_samples",
            "clipped_left_samples",
            "clipped_right_samples",
        },
    )
    normalized_padding = {
        "requested_left_samples": _integer(
            padding["requested_left_samples"], "requested left padding", 0, total
        ),
        "requested_right_samples": _integer(
            padding["requested_right_samples"], "requested right padding", 0, total
        ),
        "applied_left_samples": _integer(
            padding["applied_left_samples"], "applied left padding", 0, total
        ),
        "applied_right_samples": _integer(
            padding["applied_right_samples"], "applied right padding", 0, total
        ),
        "clipped_left_samples": _integer(
            padding["clipped_left_samples"], "clipped left padding", 0, total
        ),
        "clipped_right_samples": _integer(
            padding["clipped_right_samples"], "clipped right padding", 0, total
        ),
    }
    if normalized_padding["applied_left_samples"] != core["start_sample"] - analysis["start_sample"]:
        raise LongFormExecutionError("left padding differs from analysis/core intervals")
    if normalized_padding["applied_right_samples"] != analysis["end_sample"] - core["end_sample"]:
        raise LongFormExecutionError("right padding differs from analysis/core intervals")
    if (
        normalized_padding["applied_left_samples"]
        + normalized_padding["clipped_left_samples"]
        != normalized_padding["requested_left_samples"]
        or normalized_padding["applied_right_samples"]
        + normalized_padding["clipped_right_samples"]
        != normalized_padding["requested_right_samples"]
    ):
        raise LongFormExecutionError("applied and clipped padding do not replay")
    confidence_value = item["boundary_confidence_millionths"]
    confidence = (
        None
        if confidence_value is None
        else _integer(confidence_value, "boundary confidence", 0, 1_000_000)
    )
    return {
        "span_id": _identifier(item["span_id"], "span_id"),
        "ordinal": _integer(item["ordinal"], "span ordinal", 0, 999_999_999),
        "analysis": analysis,
        "core": core,
        "padding": normalized_padding,
        "boundary": {
            "reason": _text(item["boundary_reason"], "boundary reason", 128),
            "confidence_millionths": confidence,
        },
    }


def _normalize_decoding(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "decoding",
        {
            "language",
            "beam_size",
            "best_of",
            "temperature",
            "word_timestamps",
            "vad_filter",
            "condition_on_previous_text",
            "initial_prompt",
            "hotwords",
        },
    )
    if item["language"] != "en":
        raise LongFormExecutionError("v1 long-form execution forces English")
    for name, expected in (
        ("word_timestamps", True),
        ("vad_filter", False),
    ):
        if item[name] is not expected:
            raise LongFormExecutionError(f"decoding.{name} must be {expected!r}")
    if not isinstance(item["condition_on_previous_text"], bool):
        raise LongFormExecutionError("condition_on_previous_text must be Boolean")
    initial_prompt = item["initial_prompt"]
    if initial_prompt is not None:
        initial_prompt = _text(initial_prompt, "initial prompt", MAX_PROMPT_CHARACTERS)
        if not initial_prompt.strip():
            raise LongFormExecutionError("initial prompt may not be blank")
    hotwords = item["hotwords"]
    if not isinstance(hotwords, list) or len(hotwords) > MAX_HOTWORDS:
        raise LongFormExecutionError("hotwords must be a bounded array")
    normalized_hotwords = []
    for ordinal, word in enumerate(hotwords):
        word = _text(word, f"hotword {ordinal}", MAX_HOTWORD_CHARACTERS)
        if not word.strip() or word != word.strip():
            raise LongFormExecutionError(f"hotword {ordinal} is blank or not trimmed")
        normalized_hotwords.append(word)
    if normalized_hotwords != sorted(set(normalized_hotwords), key=str.casefold):
        raise LongFormExecutionError("hotwords must be unique and casefold-sorted")
    return {
        "language": "en",
        "beam_size": _integer(item["beam_size"], "beam size", 1, 32),
        "best_of": _integer(item["best_of"], "best_of", 1, 32),
        "temperature": _number(item["temperature"], "temperature", 0, 10),
        "word_timestamps": True,
        "vad_filter": False,
        "condition_on_previous_text": item["condition_on_previous_text"],
        "initial_prompt": initial_prompt,
        "hotwords": normalized_hotwords,
    }


def default_decoding(
    *,
    initial_prompt: str | None = None,
    hotwords: Sequence[str] = (),
    condition_on_previous_text: bool = True,
) -> dict[str, Any]:
    return _normalize_decoding(
        {
            "language": "en",
            "beam_size": 5,
            "best_of": 5,
            "temperature": 0.0,
            "word_timestamps": True,
            "vad_filter": False,
            "condition_on_previous_text": condition_on_previous_text,
            "initial_prompt": initial_prompt,
            "hotwords": sorted(set(hotwords), key=str.casefold),
        }
    )


def _normalize_execution_lineage(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "execution lineage",
        {
            "longform_scope_status",
            "production_profile",
            "runtime_admission",
            "model",
        },
    )
    if item["longform_scope_status"] != "candidate_unsoaked":
        raise LongFormExecutionError("long-form execution scope status differs")
    profile = _exact(
        item["production_profile"],
        "execution lineage production profile",
        {"profile_id", "identity_sha256", "physical_sha256"},
    )
    runtime = _exact(
        item["runtime_admission"],
        "execution lineage runtime",
        {"receipt_id", "identity_sha256", "physical_sha256", "status"},
    )
    model = _exact(
        item["model"],
        "execution lineage model",
        {"repository", "revision", "identity_sha256"},
    )
    status = runtime["status"]
    if status not in {"candidate", "admitted"}:
        raise LongFormExecutionError("execution runtime status is unsupported")
    repository = _text(model["repository"], "execution model repository", 512)
    revision = _text(model["revision"], "execution model revision", 512)
    if not repository or not revision:
        raise LongFormExecutionError("execution model binding strings may not be empty")
    return {
        "longform_scope_status": "candidate_unsoaked",
        "production_profile": {
            "profile_id": _identifier(profile["profile_id"], "production profile ID"),
            "identity_sha256": _digest(
                profile["identity_sha256"], "production profile identity"
            ),
            "physical_sha256": _digest(
                profile["physical_sha256"], "production profile physical SHA-256"
            ),
        },
        "runtime_admission": {
            "receipt_id": _identifier(runtime["receipt_id"], "runtime receipt ID"),
            "identity_sha256": _digest(
                runtime["identity_sha256"], "runtime admission identity"
            ),
            "physical_sha256": _digest(
                runtime["physical_sha256"], "runtime admission physical SHA-256"
            ),
            "status": status,
        },
        "model": {
            "repository": repository,
            "revision": revision,
            "identity_sha256": _digest(
                model["identity_sha256"], "execution model identity"
            ),
        },
    }


def _semantic_document(core: dict[str, Any], *, id_name: str, prefix: str) -> dict[str, Any]:
    identity = sha256_bytes(canonical_bytes(core))
    return {**core, "identity_sha256": identity, id_name: f"{prefix}{identity[:32]}"}


_PLANNER_MODULE: Any | None = None


def _validated_planner_plan(value: Any) -> dict[str, Any]:
    """Replay the frozen planner rather than maintaining a second plan validator."""

    global _PLANNER_MODULE
    if _PLANNER_MODULE is None:
        path = (
            Path(__file__).resolve().parents[2]
            / "corpus"
            / "src"
            / "himr_corpus"
            / "longform_asr_planner.py"
        )
        spec = importlib.util.spec_from_file_location(
            "himr_longform_asr_planner_for_execution_v1", path
        )
        if spec is None or spec.loader is None:
            raise LongFormExecutionError(f"cannot load long-form planner contract {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _PLANNER_MODULE = module
    try:
        return _PLANNER_MODULE.validate_longform_asr_plan(value)
    except Exception as error:
        raise LongFormExecutionError(f"long-form plan replay failed: {error}") from error


def _make_work_order_from_validated_plan(
    plan: dict[str, Any],
    *,
    span_ordinal: int,
    output_root: str | Path,
    execution_lineage: Mapping[str, Any],
    decoding: Mapping[str, Any] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Project one span from an already replayed canonical planner document."""

    if plan.get("kind") != PLAN_KIND or plan.get("schema_version") != 1:
        raise LongFormExecutionError("plan kind or schema version is unsupported")
    plan_identity = _digest(plan.get("identity_sha256"), "plan identity")
    plan_id = _identifier(plan.get("plan_id"), "plan_id")
    recording = _normalize_recording(plan.get("recording"))
    strategy = plan.get("strategy")
    if strategy not in STRATEGIES:
        raise LongFormExecutionError("plan strategy is unsupported")
    spans = plan.get("spans")
    if not isinstance(spans, list) or not spans:
        raise LongFormExecutionError("plan spans must be a non-empty array")
    ordinal = _integer(span_ordinal, "requested span ordinal", 0, len(spans) - 1)
    row = spans[ordinal]
    if not isinstance(row, dict) or row.get("ordinal") != ordinal:
        raise LongFormExecutionError("requested span ordinal is absent or out of order")
    span = _span_from_planner(row, recording)
    total = recording["input"]["total_samples"]
    if strategy == "direct":
        if len(spans) != 1 or span["analysis"] != {
            "start_sample": 0,
            "end_sample": total,
            "sample_count": total,
        } or span["core"] != span["analysis"]:
            raise LongFormExecutionError("direct strategy must contain one full recording span")
    output = _absolute(str(output_root), "long-form result root")
    if output == Path("/mnt/archive/HIMR") or Path("/mnt/archive/HIMR") in output.parents:
        raise LongFormExecutionError("long-form execution output must remain on the hot tier")
    core = {
        "kind": WORK_ORDER_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "job_id": _identifier(job_id or f"long-asr-{plan_identity[:16]}-{ordinal:06d}", "job_id"),
        "plan": {"plan_id": plan_id, "identity_sha256": plan_identity},
        "recording": recording,
        "strategy": strategy,
        "span": span,
        "decoding": _normalize_decoding(decoding or default_decoding()),
        "execution_lineage": _normalize_execution_lineage(execution_lineage),
        "output": {
            "root": str(output),
            "layout": "asr/longform-span-v1/sha256-v1",
            "atomic_no_replace": True,
        },
        "policy": dict(POLICY),
    }
    return _semantic_document(core, id_name="work_order_id", prefix="longasrwo1_")


def make_work_order_from_plan(
    plan: Mapping[str, Any],
    *,
    span_ordinal: int,
    output_root: str | Path,
    execution_lineage: Mapping[str, Any],
    decoding: Mapping[str, Any] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Validate one complete plan and project one closed span work order."""

    if not isinstance(plan, Mapping):
        raise LongFormExecutionError("plan must be an object")
    validated = _validated_planner_plan(dict(plan))
    return _make_work_order_from_validated_plan(
        validated,
        span_ordinal=span_ordinal,
        output_root=output_root,
        execution_lineage=execution_lineage,
        decoding=decoding,
        job_id=job_id,
    )


def make_work_orders_from_plan(
    plan: Mapping[str, Any],
    *,
    output_root: str | Path,
    execution_lineage: Mapping[str, Any],
    decoding: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Validate one plan once and project all spans in planner order."""

    if not isinstance(plan, Mapping):
        raise LongFormExecutionError("plan must be an object")
    validated = _validated_planner_plan(dict(plan))
    return [
        _make_work_order_from_validated_plan(
            validated,
            span_ordinal=span["ordinal"],
            output_root=output_root,
            execution_lineage=execution_lineage,
            decoding=decoding,
        )
        for span in validated["spans"]
    ]


def validate_work_order(value: Any) -> dict[str, Any]:
    item = _exact(value, "work order", WORK_ORDER_CORE_KEYS | {"identity_sha256", "work_order_id"})
    core = {key: item[key] for key in WORK_ORDER_CORE_KEYS}
    if core["kind"] != WORK_ORDER_KIND or core["schema_version"] != SCHEMA_VERSION:
        raise LongFormExecutionError("work order kind or schema version is unsupported")
    if core["implementation_version"] != IMPLEMENTATION_VERSION:
        raise LongFormExecutionError("work order implementation version differs")
    _identifier(core["job_id"], "job_id")
    plan = _exact(core["plan"], "plan binding", {"plan_id", "identity_sha256"})
    _identifier(plan["plan_id"], "plan_id")
    _digest(plan["identity_sha256"], "plan identity")
    recording = _normalize_recording(core["recording"])
    if core["strategy"] not in STRATEGIES:
        raise LongFormExecutionError("work order strategy is unsupported")
    span = _exact(
        core["span"],
        "work order span",
        {"span_id", "ordinal", "analysis", "core", "padding", "boundary"},
    )
    total = recording["input"]["total_samples"]
    normalized_span = {
        "span_id": _identifier(span["span_id"], "span_id"),
        "ordinal": _integer(span["ordinal"], "span ordinal", 0, 999_999_999),
        "analysis": _sample_interval(span["analysis"], "analysis interval", total_samples=total),
        "core": _sample_interval(span["core"], "core interval", total_samples=total),
        "padding": span["padding"],
        "boundary": span["boundary"],
    }
    # Replay the exact planner-shape conversion for padding/boundary validation.
    replay_span = _span_from_planner(
        {
            "span_id": normalized_span["span_id"],
            "ordinal": normalized_span["ordinal"],
            "analysis_start_sample": normalized_span["analysis"]["start_sample"],
            "analysis_end_sample": normalized_span["analysis"]["end_sample"],
            "core_start_sample": normalized_span["core"]["start_sample"],
            "core_end_sample": normalized_span["core"]["end_sample"],
            "boundary_reason": span["boundary"].get("reason") if isinstance(span["boundary"], dict) else None,
            "boundary_confidence_millionths": span["boundary"].get("confidence_millionths") if isinstance(span["boundary"], dict) else None,
            "padding": span["padding"],
        },
        recording,
    )
    if replay_span != normalized_span:
        raise LongFormExecutionError("work order span is not canonical")
    if core["strategy"] == "direct":
        expected = {"start_sample": 0, "end_sample": total, "sample_count": total}
        if normalized_span["ordinal"] != 0 or normalized_span["analysis"] != expected or normalized_span["core"] != expected:
            raise LongFormExecutionError("direct work order is not the full recording")
    decoding = _normalize_decoding(core["decoding"])
    execution_lineage = _normalize_execution_lineage(core["execution_lineage"])
    output = _exact(core["output"], "output", {"root", "layout", "atomic_no_replace"})
    _absolute(output["root"], "output root")
    if output["layout"] != "asr/longform-span-v1/sha256-v1" or output["atomic_no_replace"] is not True:
        raise LongFormExecutionError("output contract differs")
    if core["policy"] != POLICY:
        raise LongFormExecutionError("work order policy differs")
    normalized_core = {
        **core,
        "recording": recording,
        "span": normalized_span,
        "decoding": decoding,
        "execution_lineage": execution_lineage,
    }
    expected = _semantic_document(normalized_core, id_name="work_order_id", prefix="longasrwo1_")
    if expected != item or not WORK_ORDER_ID_RE.fullmatch(item["work_order_id"]):
        raise LongFormExecutionError("work order semantic identity differs")
    return item


@dataclass(frozen=True)
class SpanSourceView:
    """A non-materialized logical view of one immutable normalized recording."""

    source_path: str
    retained_path: str
    retained_descriptor: int
    source_sha256: str
    source_byte_count: int
    source_total_samples: int
    analysis_start_sample: int
    analysis_end_sample: int
    sample_rate_hz: int = SAMPLE_RATE_HZ
    channels: int = CHANNELS

    @property
    def analysis_sample_count(self) -> int:
        return self.analysis_end_sample - self.analysis_start_sample

    @property
    def is_whole_recording(self) -> bool:
        return self.analysis_start_sample == 0 and self.analysis_end_sample == self.source_total_samples


@dataclass(frozen=True)
class SpanEngineRequest:
    source: SpanSourceView
    strategy: str
    span_id: str
    decoding: Mapping[str, Any]


class SpanEngine(Protocol):
    def transcribe(self, request: SpanEngineRequest) -> Mapping[str, Any]:
        """Return one sample-local engine transcript for exactly ``request.source``."""


def _file_identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_uid,
        info.st_gid,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


class RetainedSource:
    """Stable, hashed parent-audio descriptor retained across a recording run."""

    def __init__(self, recording_input: Mapping[str, Any]) -> None:
        path = _absolute(recording_input["path"], "recording input path")
        flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise LongFormExecutionError(f"recording input cannot be retained: {error}") from error
        self.descriptor = descriptor
        self.path = path
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink < 1:
                raise LongFormExecutionError("recording input is not a linked regular file")
            if before.st_size != recording_input["byte_count"]:
                raise LongFormExecutionError("recording input byte count differs")
            digest = hashlib.sha256()
            offset = 0
            while offset < before.st_size:
                chunk = os.pread(descriptor, min(1024 * 1024, before.st_size - offset), offset)
                if not chunk:
                    raise LongFormExecutionError("recording input ended during hashing")
                digest.update(chunk)
                offset += len(chunk)
            after = os.fstat(descriptor)
            if _file_identity(after) != _file_identity(before):
                raise LongFormExecutionError("recording input changed during hashing")
            if digest.hexdigest() != recording_input["sha256"]:
                raise LongFormExecutionError("recording input SHA-256 differs")
            self.identity = _file_identity(after)
            self.sha256 = digest.hexdigest()
            self.byte_count = after.st_size
        except BaseException:
            os.close(descriptor)
            raise

    @property
    def proc_path(self) -> str:
        return f"/proc/self/fd/{self.descriptor}"

    def verify(self) -> None:
        try:
            observed = os.fstat(self.descriptor)
        except OSError as error:
            raise LongFormExecutionError(f"retained recording input was lost: {error}") from error
        if _file_identity(observed) != self.identity:
            raise LongFormExecutionError("retained recording input identity changed")

    def matches(self, recording_input: Mapping[str, Any]) -> bool:
        return (
            self.path == Path(recording_input["path"])
            and self.sha256 == recording_input["sha256"]
            and self.byte_count == recording_input["byte_count"]
        )

    def close(self) -> None:
        descriptor = getattr(self, "descriptor", -1)
        if descriptor >= 0:
            os.close(descriptor)
            self.descriptor = -1

    def __enter__(self) -> "RetainedSource":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def engine_request(
    work_order: Mapping[str, Any], retained_source: RetainedSource
) -> SpanEngineRequest:
    order = validate_work_order(work_order)
    source = order["recording"]["input"]
    if not retained_source.matches(source):
        raise LongFormExecutionError("retained source differs from the work order input")
    retained_source.verify()
    analysis = order["span"]["analysis"]
    return SpanEngineRequest(
        source=SpanSourceView(
            source_path=source["path"],
            retained_path=retained_source.proc_path,
            retained_descriptor=retained_source.descriptor,
            source_sha256=source["sha256"],
            source_byte_count=source["byte_count"],
            source_total_samples=source["total_samples"],
            analysis_start_sample=analysis["start_sample"],
            analysis_end_sample=analysis["end_sample"],
        ),
        strategy=order["strategy"],
        span_id=order["span"]["span_id"],
        decoding=order["decoding"],
    )


def _score_fields(value: Any, label: str) -> dict[str, float | None]:
    item = _exact(
        value,
        label,
        {
            "temperature_raw",
            "average_log_probability_raw",
            "compression_ratio_raw",
            "no_speech_probability_raw",
        },
    )
    bounds = {
        "temperature_raw": (0, 10),
        "average_log_probability_raw": (-1000, 1000),
        "compression_ratio_raw": (0, 1_000_000),
        "no_speech_probability_raw": (0, 1),
    }
    result: dict[str, float | None] = {}
    for name, (minimum, maximum) in bounds.items():
        result[name] = None if item[name] is None else _number(item[name], f"{label}.{name}", minimum, maximum)
    return result


def _validate_engine_output(value: Any, request: SpanEngineRequest) -> dict[str, Any]:
    item = _exact(
        value,
        "engine output",
        {"engine", "language", "duration_samples", "segments"},
    )
    engine = _exact(
        item["engine"],
        "engine descriptor",
        {
            "library",
            "library_version",
            "model_revision",
            "model_identity_sha256",
            "input_decoder",
        },
    )
    decoder = _exact(
        engine["input_decoder"],
        "input decoder descriptor",
        {
            "kind",
            "executable_path",
            "executable_sha256",
            "execution_mode",
            "persistent_audio_chunks",
        },
    )
    if decoder["persistent_audio_chunks"] is not False:
        raise LongFormExecutionError("input decoder may not materialize audio chunks")
    executable_path = decoder["executable_path"]
    executable_sha = decoder["executable_sha256"]
    if (executable_path is None) != (executable_sha is None):
        raise LongFormExecutionError("input decoder executable binding is incomplete")
    if executable_path is not None:
        executable_path = str(_absolute(executable_path, "input decoder executable"))
        executable_sha = _digest(executable_sha, "input decoder executable SHA-256")
    normalized_decoder = {
        "kind": _identifier(decoder["kind"], "input decoder kind"),
        "executable_path": executable_path,
        "executable_sha256": executable_sha,
        "execution_mode": _identifier(decoder["execution_mode"], "input decoder mode"),
        "persistent_audio_chunks": False,
    }
    normalized_engine = {
        "library": _text(engine["library"], "engine library", 128),
        "library_version": _text(engine["library_version"], "engine library version", 128),
        "model_revision": _text(engine["model_revision"], "model revision", 512),
        "model_identity_sha256": _digest(engine["model_identity_sha256"], "model identity"),
        "input_decoder": normalized_decoder,
    }
    if not all(normalized_engine[name] for name in ("library", "library_version", "model_revision")):
        raise LongFormExecutionError("engine descriptor strings may not be empty")
    language = _exact(
        item["language"],
        "engine language",
        {"value", "selection_basis", "detection_performed", "probability_raw"},
    )
    if language != {
        "value": "en",
        "selection_basis": "forced_by_execution_work_order",
        "detection_performed": False,
        "probability_raw": None,
    }:
        raise LongFormExecutionError("engine language differs from the forced language contract")
    duration = request.source.analysis_sample_count
    if item["duration_samples"] != duration:
        raise LongFormExecutionError("engine duration differs from the logical analysis span")
    rows = item["segments"]
    if not isinstance(rows, list) or len(rows) > MAX_SEGMENTS:
        raise LongFormExecutionError("engine segments must be a bounded array")
    segments = []
    previous_start = 0
    total_words = 0
    for ordinal, row_value in enumerate(rows):
        row = _exact(
            row_value,
            f"engine segment {ordinal}",
            {"ordinal", "start_sample", "end_sample", "text", "words", "scores", "timing_clipped"},
        )
        if row["ordinal"] != ordinal:
            raise LongFormExecutionError("engine segment ordinals are not contiguous")
        start = _integer(row["start_sample"], f"segment {ordinal} start", 0, duration)
        end = _integer(row["end_sample"], f"segment {ordinal} end", start, duration)
        if start < previous_start:
            raise LongFormExecutionError("engine segment starts regress")
        previous_start = start
        if not isinstance(row["timing_clipped"], bool):
            raise LongFormExecutionError("segment timing_clipped must be Boolean")
        words_value = row["words"]
        if not isinstance(words_value, list):
            raise LongFormExecutionError(f"segment {ordinal} words must be an array")
        words = []
        for word_ordinal, word_value in enumerate(words_value):
            word = _exact(
                word_value,
                f"engine word {ordinal}.{word_ordinal}",
                {"ordinal", "start_sample", "end_sample", "text", "probability_raw", "timing_clipped"},
            )
            if word["ordinal"] != word_ordinal:
                raise LongFormExecutionError("engine word ordinals are not contiguous")
            word_start = word["start_sample"]
            word_end = word["end_sample"]
            if (word_start is None) != (word_end is None):
                raise LongFormExecutionError("engine word timing must be paired or wholly absent")
            if word_start is not None:
                word_start = _integer(word_start, f"word {ordinal}.{word_ordinal} start", 0, duration)
                word_end = _integer(word_end, f"word {ordinal}.{word_ordinal} end", word_start, duration)
            probability = word["probability_raw"]
            if probability is not None:
                probability = _number(probability, f"word {ordinal}.{word_ordinal} probability", 0, 1)
            if not isinstance(word["timing_clipped"], bool):
                raise LongFormExecutionError("word timing_clipped must be Boolean")
            words.append(
                {
                    "ordinal": word_ordinal,
                    "start_sample": word_start,
                    "end_sample": word_end,
                    "text": _text(word["text"], f"word {ordinal}.{word_ordinal} text", 16_384),
                    "probability_raw": probability,
                    "timing_clipped": word["timing_clipped"],
                }
            )
        total_words += len(words)
        if total_words > MAX_WORDS:
            raise LongFormExecutionError("engine word count exceeds its bound")
        segments.append(
            {
                "ordinal": ordinal,
                "start_sample": start,
                "end_sample": end,
                "text": _text(row["text"], f"segment {ordinal} text", MAX_TEXT_CHARACTERS),
                "words": words,
                "scores": _score_fields(row["scores"], f"segment {ordinal} scores"),
                "timing_clipped": row["timing_clipped"],
            }
        )
    return {
        "engine": normalized_engine,
        "language": language,
        "duration_samples": duration,
        "segments": segments,
    }


def _disposition(start: int | None, end: int | None, core_start: int, core_end: int) -> str | None:
    if start is None or end is None:
        return None
    if end <= core_start:
        return "left_context_only"
    if start >= core_end:
        return "right_context_only"
    crosses_left = start < core_start
    crosses_right = end > core_end
    if crosses_left and crosses_right:
        return "crosses_both_core_boundaries"
    if crosses_left:
        return "crosses_left_core_boundary"
    if crosses_right:
        return "crosses_right_core_boundary"
    return "core_owned"


def _build_transcript_document(order: dict[str, Any], engine: dict[str, Any]) -> dict[str, Any]:
    """Build one compact, sample-native span hypothesis artifact.

    Coordinate basis, engine provenance, policy, and parent offsets live once in
    the result envelope.  Word rows retain only local evidence and anomaly flags.
    """

    analysis = order["span"]["analysis"]
    segments = []
    for row in engine["segments"]:
        words = []
        previous_word_start: int | None = None
        previous_word_end: int | None = None
        for word in row["words"]:
            start = word["start_sample"]
            end = word["end_sample"]
            compact_word: dict[str, Any] = {
                "text": word["text"],
                "start_sample": start,
                "end_sample": end,
            }
            if word["probability_raw"] is not None:
                compact_word["probability_raw"] = word["probability_raw"]
            if word["timing_clipped"]:
                compact_word["timing_clipped"] = True
            if start is not None:
                anomalies = []
                if start < row["start_sample"]:
                    anomalies.append("precedes_segment_start")
                if end > row["end_sample"]:
                    anomalies.append("extends_beyond_segment_end")
                if previous_word_start is not None and start < previous_word_start:
                    anomalies.append("start_regresses_from_previous")
                if previous_word_end is not None and start < previous_word_end:
                    anomalies.append("overlaps_previous")
                if anomalies:
                    compact_word["anomaly_flags"] = anomalies
                previous_word_start = start
                previous_word_end = end
            words.append(compact_word)
        compact_segment = {
            "ordinal": row["ordinal"],
            "start_sample": row["start_sample"],
            "end_sample": row["end_sample"],
            "text": row["text"],
            "words": words,
            "scores": row["scores"],
        }
        if row["timing_clipped"]:
            compact_segment["timing_clipped"] = True
        segments.append(
            compact_segment
        )
    core_value = {
        "kind": NORMALIZED_TRANSCRIPT_KIND,
        "schema_version": SCHEMA_VERSION,
        "work_order": {
            "work_order_id": order["work_order_id"],
            "identity_sha256": order["identity_sha256"],
        },
        "plan": dict(order["plan"]),
        "span_id": order["span"]["span_id"],
        "analysis_sample_count": analysis["sample_count"],
        "language": "en",
        "segments": segments,
        "text": "".join(str(row["text"]) for row in segments).strip(),
        "segment_count": len(segments),
        "word_count": sum(len(row["words"]) for row in segments),
        "review_status": "unreviewed_machine_output",
        "semantics": dict(TRANSCRIPT_SEMANTICS),
    }
    return _semantic_document(
        core_value, id_name="document_id", prefix="longasrtranscript1_"
    )


def result_plan(work_order: Mapping[str, Any]) -> dict[str, str]:
    order = validate_work_order(work_order)
    source_sha = order["recording"]["input"]["sha256"]
    result_key = sha256_bytes(
        canonical_bytes(
            {
                "work_order_identity_sha256": order["identity_sha256"],
                "span_id": order["span"]["span_id"],
            }
        )
    )
    directory = (
        Path(order["output"]["root"])
        / "asr"
        / "longform-span-v1"
        / "sha256"
        / source_sha[:2]
        / source_sha
        / "spans"
        / order["span"]["span_id"]
        / result_key
    )
    return {
        "result_key": result_key,
        "result_directory": str(directory),
        "result_path": str(directory / "result.json"),
        "transcript_path": str(directory / "transcript.json"),
    }


def _artifact(value: dict[str, Any], path: str, kind: str) -> dict[str, Any]:
    body = canonical_bytes(value)
    return {
        "artifact_kind": kind,
        "path": path,
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "identity_sha256": value["identity_sha256"],
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def execute_work_order(
    work_order: Mapping[str, Any],
    engine: SpanEngine,
    *,
    utc_clock: Callable[[], str] = _utc_now,
    monotonic_clock: Callable[[], float] = time.monotonic,
    attempt_id: str | None = None,
    retained_source: RetainedSource | None = None,
) -> dict[str, Any]:
    """Call ``engine`` exactly once and return a complete in-memory result bundle."""

    order = validate_work_order(work_order)
    owns_source = retained_source is None
    source_guard = retained_source or RetainedSource(order["recording"]["input"])
    try:
        request = engine_request(order, source_guard)
        started_at = utc_clock()
        started = monotonic_clock()
        try:
            engine_value = engine.transcribe(request)
        except Exception as error:
            raise LongFormExecutionError(
                f"span engine failed for {order['span']['span_id']}: {type(error).__name__}: {error}"
            ) from error
        engine_output = _validate_engine_output(engine_value, request)
        admitted_model = order["execution_lineage"]["model"]
        if (
            engine_output["engine"]["model_revision"] != admitted_model["revision"]
            or engine_output["engine"]["model_identity_sha256"]
            != admitted_model["identity_sha256"]
        ):
            raise LongFormExecutionError(
                "span engine model differs from the work-order execution lineage"
            )
        source_guard.verify()
    finally:
        if owns_source:
            source_guard.close()
    completed = monotonic_clock()
    completed_at = utc_clock()
    wall_seconds = completed - started
    if not math.isfinite(wall_seconds) or wall_seconds < 0:
        raise LongFormExecutionError("execution monotonic duration is invalid")
    transcript = _build_transcript_document(order, engine_output)
    plan = result_plan(order)
    transcript_artifact = _artifact(
        transcript,
        plan["transcript_path"],
        "longform_span_transcript_json",
    )
    dispositions = {name: 0 for name in sorted(DISPOSITIONS)}
    unknown_word_times = 0
    analysis = order["span"]["analysis"]
    core = order["span"]["core"]
    core_local_start = core["start_sample"] - analysis["start_sample"]
    core_local_end = core["end_sample"] - analysis["start_sample"]
    for segment in engine_output["segments"]:
        disposition = _disposition(
            segment["start_sample"],
            segment["end_sample"],
            core_local_start,
            core_local_end,
        )
        assert disposition is not None
        dispositions[disposition] += 1
        unknown_word_times += sum(
            word["start_sample"] is None for word in segment["words"]
        )
    result_core = {
        "kind": RESULT_KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "completed",
        "work_order": {
            "work_order_id": order["work_order_id"],
            "identity_sha256": order["identity_sha256"],
        },
        "plan": dict(order["plan"]),
        "recording": {
            "recording_id": order["recording"]["recording_id"],
            "media_id": order["recording"]["media_id"],
            "input_sha256": order["recording"]["input"]["sha256"],
            "total_samples": order["recording"]["input"]["total_samples"],
        },
        "span": {
            "span_id": order["span"]["span_id"],
            "ordinal": order["span"]["ordinal"],
            "analysis": analysis,
            "core": core,
            "core_local": {
                "start_sample": core["start_sample"] - analysis["start_sample"],
                "end_sample": core["end_sample"] - analysis["start_sample"],
                "sample_count": core["sample_count"],
            },
        },
        "execution_lineage": dict(order["execution_lineage"]),
        "engine": engine_output["engine"],
        "execution": {
            "attempt_id": _identifier(
                attempt_id or f"longasrattempt_{secrets.token_hex(16)}", "attempt_id"
            ),
            "started_at": started_at,
            "completed_at": completed_at,
            "wall_seconds": wall_seconds,
            "engine_call_count": 1,
            "input_mode": "parent_path_once"
            if request.source.is_whole_recording
            else "ephemeral_in_memory_logical_span",
            "persistent_audio_chunks_created": False,
        },
        "coverage": {
            "analysis_start_sample": analysis["start_sample"],
            "analysis_end_sample": analysis["end_sample"],
            "analysis_sample_count": analysis["sample_count"],
            "core_start_sample": core["start_sample"],
            "core_end_sample": core["end_sample"],
            "core_sample_count": core["sample_count"],
            "coordinate_system": "parent_media_16000hz_half_open_samples",
            "engine_reported_analysis_complete": True,
        },
        "dispositions": {
            "segment_counts": dispositions,
            "unknown_word_timing_count": unknown_word_times,
            "ownership_decision": "deferred_to_downstream_assembler",
        },
        "artifacts": [transcript_artifact],
        "policy": dict(POLICY),
    }
    result = _semantic_document(result_core, id_name="result_id", prefix="longasrresult1_")
    return {
        "work_order": order,
        "transcript": transcript,
        "result": result,
    }


def _validate_identity_document(value: Any, *, kind: str, prefix: str, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("kind") != kind:
        raise LongFormExecutionError(f"{label} kind differs")
    if set(value) < {"identity_sha256", "document_id"}:
        raise LongFormExecutionError(f"{label} lacks semantic identity")
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", "document_id"}}
    identity = sha256_bytes(canonical_bytes(core))
    if value["identity_sha256"] != identity or value["document_id"] != f"{prefix}{identity[:32]}":
        raise LongFormExecutionError(f"{label} semantic identity differs")
    if not DOCUMENT_ID_RE.fullmatch(value["document_id"]):
        raise LongFormExecutionError(f"{label} document ID is invalid")
    return value


def _validate_transcript_document(value: Any, order: dict[str, Any]) -> dict[str, Any]:
    """Strictly replay the compact persisted transcript contract.

    The engine's richer word rows are deliberately not accepted here.  Optional
    evidence fields must be omitted when empty/false, which prevents replay from
    silently reintroducing redundant per-word metadata.
    """

    item = _exact(
        value,
        "span transcript",
        TRANSCRIPT_CORE_KEYS | {"identity_sha256", "document_id"},
    )
    _validate_identity_document(
        item,
        kind=NORMALIZED_TRANSCRIPT_KIND,
        prefix="longasrtranscript1_",
        label="span transcript",
    )
    expected_work_order = {
        "work_order_id": order["work_order_id"],
        "identity_sha256": order["identity_sha256"],
    }
    if (
        item["schema_version"] != SCHEMA_VERSION
        or item["work_order"] != expected_work_order
        or item["plan"] != order["plan"]
        or item["span_id"] != order["span"]["span_id"]
        or item["analysis_sample_count"]
        != order["span"]["analysis"]["sample_count"]
        or item["language"] != "en"
        or item["review_status"] != "unreviewed_machine_output"
        or item["semantics"] != TRANSCRIPT_SEMANTICS
    ):
        raise LongFormExecutionError("span transcript envelope differs")
    duration = _integer(
        item["analysis_sample_count"], "span transcript analysis sample count", 1
    )
    rows = item["segments"]
    if not isinstance(rows, list) or len(rows) > MAX_SEGMENTS:
        raise LongFormExecutionError("span transcript segments must be a bounded array")
    previous_segment_start = 0
    total_words = 0
    normalized_text = []
    anomaly_names = set(TRANSCRIPT_SEMANTICS["word_timing_flags"])
    for ordinal, row_value in enumerate(rows):
        if not isinstance(row_value, dict):
            raise LongFormExecutionError(f"span transcript segment {ordinal} is not an object")
        required_segment = {"ordinal", "start_sample", "end_sample", "text", "words", "scores"}
        allowed_segment = required_segment | {"timing_clipped"}
        if not required_segment <= set(row_value) or not set(row_value) <= allowed_segment:
            raise LongFormExecutionError(f"span transcript segment {ordinal} fields differ")
        if row_value["ordinal"] != ordinal:
            raise LongFormExecutionError("span transcript segment ordinals are not contiguous")
        start = _integer(row_value["start_sample"], f"transcript segment {ordinal} start", 0, duration)
        end = _integer(row_value["end_sample"], f"transcript segment {ordinal} end", start, duration)
        if start < previous_segment_start:
            raise LongFormExecutionError("span transcript segment starts regress")
        previous_segment_start = start
        if "timing_clipped" in row_value and row_value["timing_clipped"] is not True:
            raise LongFormExecutionError("span transcript segment timing flag must be true when present")
        segment_text = _text(
            row_value["text"], f"transcript segment {ordinal} text", MAX_TEXT_CHARACTERS
        )
        normalized_text.append(segment_text)
        _score_fields(row_value["scores"], f"transcript segment {ordinal} scores")
        words = row_value["words"]
        if not isinstance(words, list):
            raise LongFormExecutionError(f"span transcript segment {ordinal} words must be an array")
        previous_word_start: int | None = None
        previous_word_end: int | None = None
        for word_ordinal, word_value in enumerate(words):
            if not isinstance(word_value, dict):
                raise LongFormExecutionError(
                    f"span transcript word {ordinal}.{word_ordinal} is not an object"
                )
            required_word = {"text", "start_sample", "end_sample"}
            allowed_word = required_word | {
                "probability_raw",
                "timing_clipped",
                "anomaly_flags",
            }
            if not required_word <= set(word_value) or not set(word_value) <= allowed_word:
                raise LongFormExecutionError(
                    f"span transcript word {ordinal}.{word_ordinal} fields differ"
                )
            _text(
                word_value["text"],
                f"transcript word {ordinal}.{word_ordinal} text",
                16_384,
            )
            word_start = word_value["start_sample"]
            word_end = word_value["end_sample"]
            if (word_start is None) != (word_end is None):
                raise LongFormExecutionError("span transcript word timing is not paired")
            derived_anomalies: list[str] = []
            if word_start is not None:
                word_start = _integer(
                    word_start,
                    f"transcript word {ordinal}.{word_ordinal} start",
                    0,
                    duration,
                )
                word_end = _integer(
                    word_end,
                    f"transcript word {ordinal}.{word_ordinal} end",
                    word_start,
                    duration,
                )
                if word_start < start:
                    derived_anomalies.append("precedes_segment_start")
                if word_end > end:
                    derived_anomalies.append("extends_beyond_segment_end")
                if previous_word_start is not None and word_start < previous_word_start:
                    derived_anomalies.append("start_regresses_from_previous")
                if previous_word_end is not None and word_start < previous_word_end:
                    derived_anomalies.append("overlaps_previous")
                previous_word_start = word_start
                previous_word_end = word_end
            if "probability_raw" in word_value:
                _number(
                    word_value["probability_raw"],
                    f"transcript word {ordinal}.{word_ordinal} probability",
                    0,
                    1,
                )
            if "timing_clipped" in word_value and word_value["timing_clipped"] is not True:
                raise LongFormExecutionError("span transcript word timing flag must be true when present")
            persisted_anomalies = word_value.get("anomaly_flags", [])
            if (
                not isinstance(persisted_anomalies, list)
                or any(not isinstance(name, str) or name not in anomaly_names for name in persisted_anomalies)
                or len(set(persisted_anomalies)) != len(persisted_anomalies)
                or persisted_anomalies != derived_anomalies
            ):
                raise LongFormExecutionError("span transcript word anomaly flags differ")
            if not derived_anomalies and "anomaly_flags" in word_value:
                raise LongFormExecutionError("empty span transcript anomaly flags must be omitted")
        total_words += len(words)
        if total_words > MAX_WORDS:
            raise LongFormExecutionError("span transcript word count exceeds its bound")
    segment_count = _integer(item["segment_count"], "span transcript segment count", 0)
    word_count = _integer(item["word_count"], "span transcript word count", 0)
    if (
        segment_count != len(rows)
        or word_count != total_words
        or item["text"] != "".join(normalized_text).strip()
    ):
        raise LongFormExecutionError("span transcript summary differs")
    return item


def _validate_result_document(
    value: Any, order: dict[str, Any], transcript: dict[str, Any]
) -> dict[str, Any]:
    result = _exact(
        value,
        "result",
        RESULT_CORE_KEYS | {"identity_sha256", "result_id"},
    )
    core = {key: result[key] for key in RESULT_CORE_KEYS}
    expected_identity = _semantic_document(
        core, id_name="result_id", prefix="longasrresult1_"
    )
    if result != expected_identity or not RESULT_ID_RE.fullmatch(result["result_id"]):
        raise LongFormExecutionError("result semantic identity differs")
    if (
        result["kind"] != RESULT_KIND
        or result["schema_version"] != SCHEMA_VERSION
        or result["implementation_version"] != IMPLEMENTATION_VERSION
        or result["status"] != "completed"
    ):
        raise LongFormExecutionError("result envelope kind, version, or status differs")
    expected_work_order = {
        "work_order_id": order["work_order_id"],
        "identity_sha256": order["identity_sha256"],
    }
    if result["work_order"] != expected_work_order or result["plan"] != order["plan"]:
        raise LongFormExecutionError("result work-order or plan binding differs")
    expected_recording = {
        "recording_id": order["recording"]["recording_id"],
        "media_id": order["recording"]["media_id"],
        "input_sha256": order["recording"]["input"]["sha256"],
        "total_samples": order["recording"]["input"]["total_samples"],
    }
    if result["recording"] != expected_recording:
        raise LongFormExecutionError("result recording binding differs")
    analysis = order["span"]["analysis"]
    core_interval = order["span"]["core"]
    expected_span = {
        "span_id": order["span"]["span_id"],
        "ordinal": order["span"]["ordinal"],
        "analysis": analysis,
        "core": core_interval,
        "core_local": {
            "start_sample": core_interval["start_sample"] - analysis["start_sample"],
            "end_sample": core_interval["end_sample"] - analysis["start_sample"],
            "sample_count": core_interval["sample_count"],
        },
    }
    if result["span"] != expected_span:
        raise LongFormExecutionError("result span binding differs")
    if result["execution_lineage"] != order["execution_lineage"]:
        raise LongFormExecutionError("result execution lineage differs")

    engine = _exact(
        result["engine"],
        "result engine",
        {
            "library",
            "library_version",
            "model_revision",
            "model_identity_sha256",
            "input_decoder",
        },
    )
    for name in ("library", "library_version", "model_revision"):
        if not _text(engine[name], f"result engine {name}", 512):
            raise LongFormExecutionError(f"result engine {name} may not be empty")
    _digest(engine["model_identity_sha256"], "result engine model identity")
    admitted_model = order["execution_lineage"]["model"]
    if (
        engine["model_revision"] != admitted_model["revision"]
        or engine["model_identity_sha256"] != admitted_model["identity_sha256"]
    ):
        raise LongFormExecutionError("result engine model differs from execution lineage")
    decoder = _exact(
        engine["input_decoder"],
        "result input decoder",
        {
            "kind",
            "executable_path",
            "executable_sha256",
            "execution_mode",
            "persistent_audio_chunks",
        },
    )
    _identifier(decoder["kind"], "result input decoder kind")
    _identifier(decoder["execution_mode"], "result input decoder mode")
    if decoder["persistent_audio_chunks"] is not False:
        raise LongFormExecutionError("result input decoder materialized audio")
    if (decoder["executable_path"] is None) != (
        decoder["executable_sha256"] is None
    ):
        raise LongFormExecutionError("result input decoder binding is incomplete")
    if decoder["executable_path"] is not None:
        _absolute(decoder["executable_path"], "result input decoder executable")
        _digest(decoder["executable_sha256"], "result input decoder SHA-256")

    execution = _exact(
        result["execution"],
        "result execution",
        {
            "attempt_id",
            "started_at",
            "completed_at",
            "wall_seconds",
            "engine_call_count",
            "input_mode",
            "persistent_audio_chunks_created",
        },
    )
    _identifier(execution["attempt_id"], "result attempt ID")
    for name in ("started_at", "completed_at"):
        timestamp = _text(execution[name], f"result execution {name}", 64)
        if not timestamp.endswith("Z"):
            raise LongFormExecutionError("result execution timestamps must be UTC Z")
        try:
            datetime.fromisoformat(timestamp[:-1] + "+00:00")
        except ValueError as error:
            raise LongFormExecutionError("result execution timestamp is invalid") from error
    _number(execution["wall_seconds"], "result wall seconds", 0, 10**12)
    if execution["engine_call_count"] != 1 or isinstance(
        execution["engine_call_count"], bool
    ):
        raise LongFormExecutionError("result engine call count differs")
    expected_input_mode = (
        "parent_path_once"
        if order["strategy"] == "direct"
        else "ephemeral_in_memory_logical_span"
    )
    if (
        execution["input_mode"] != expected_input_mode
        or execution["persistent_audio_chunks_created"] is not False
    ):
        raise LongFormExecutionError("result execution input mode or persistence differs")

    expected_coverage = {
        "analysis_start_sample": analysis["start_sample"],
        "analysis_end_sample": analysis["end_sample"],
        "analysis_sample_count": analysis["sample_count"],
        "core_start_sample": core_interval["start_sample"],
        "core_end_sample": core_interval["end_sample"],
        "core_sample_count": core_interval["sample_count"],
        "coordinate_system": "parent_media_16000hz_half_open_samples",
        "engine_reported_analysis_complete": True,
    }
    if result["coverage"] != expected_coverage:
        raise LongFormExecutionError("result coverage differs")
    dispositions = _exact(
        result["dispositions"],
        "result dispositions",
        {"segment_counts", "unknown_word_timing_count", "ownership_decision"},
    )
    counts = _exact(
        dispositions["segment_counts"], "result disposition counts", DISPOSITIONS
    )
    normalized_counts = {
        name: _integer(value, f"result disposition {name}", 0)
        for name, value in counts.items()
    }
    if sum(normalized_counts.values()) != transcript["segment_count"]:
        raise LongFormExecutionError("result disposition counts differ from transcript")
    unknown_words = sum(
        word["start_sample"] is None
        for segment in transcript["segments"]
        for word in segment["words"]
    )
    if (
        _integer(
            dispositions["unknown_word_timing_count"],
            "result unknown word timing count",
            0,
        )
        != unknown_words
        or dispositions["ownership_decision"]
        != "deferred_to_downstream_assembler"
    ):
        raise LongFormExecutionError("result word timing or ownership summary differs")
    if result["policy"] != POLICY:
        raise LongFormExecutionError("result policy differs")
    return result


def validate_result_bundle(bundle: Any) -> dict[str, Any]:
    value = _exact(
        bundle,
        "result bundle",
        {"work_order", "transcript", "result"},
    )
    order = validate_work_order(value["work_order"])
    transcript = _validate_transcript_document(value["transcript"], order)
    result = _validate_result_document(value["result"], order, transcript)
    if result["plan"] != order["plan"] or transcript["plan"] != order["plan"]:
        raise LongFormExecutionError("result or transcript plan binding differs")
    plan = result_plan(order)
    artifacts = result["artifacts"]
    if not isinstance(artifacts, list) or len(artifacts) != 1:
        raise LongFormExecutionError("result artifact list differs")
    expected_artifacts = [
        _artifact(transcript, plan["transcript_path"], "longform_span_transcript_json")
    ]
    if artifacts != expected_artifacts:
        raise LongFormExecutionError("result artifact bindings differ")
    if (
        transcript["work_order"] != result["work_order"]
        or transcript["span_id"] != order["span"]["span_id"]
        or transcript["analysis_sample_count"]
        != order["span"]["analysis"]["sample_count"]
    ):
        raise LongFormExecutionError("span transcript lineage differs")
    return value


def _rename_noreplace(source: Path, target: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise LongFormExecutionError("Linux renameat2 is required for atomic result publication")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if renameat2(-100, os.fsencode(source), -100, os.fsencode(target), 1) != 0:
        observed = ctypes.get_errno()
        if observed == 17:
            raise FileExistsError(observed, os.strerror(observed), str(target))
        raise OSError(observed, os.strerror(observed), str(target))


def materialize_result_bundle(bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Exclusively write transcript/result JSON; never write an audio artifact."""

    value = validate_result_bundle(bundle)
    plan = result_plan(value["work_order"])
    directory = Path(plan["result_directory"])
    directory.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    stage = directory.parent / f".{directory.name}.stage-{os.getpid()}-{secrets.token_hex(16)}"
    stage.mkdir(mode=0o700)
    leaves = {
        "transcript.json": canonical_bytes(value["transcript"]),
        "result.json": canonical_bytes(value["result"]),
    }
    created: list[Path] = []
    try:
        for name, body in leaves.items():
            if len(body) > MAX_JSON_BYTES:
                raise LongFormExecutionError(f"{name} exceeds the JSON byte bound")
            path = stage / name
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o400)
            try:
                view = memoryview(body)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise LongFormExecutionError(f"short write for {path}")
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            created.append(path)
        directory_fd = os.open(stage, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        _rename_noreplace(stage, directory)
        parent_fd = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except BaseException as error:
        for path in reversed(created):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        try:
            stage.rmdir()
        except OSError:
            pass
        if isinstance(error, FileExistsError):
            raise LongFormExecutionError(
                f"refusing to replace result directory {directory}"
            ) from error
        raise
    return {
        "status": "materialized",
        "result_id": value["result"]["result_id"],
        "result_directory": str(directory),
        "result_path": plan["result_path"],
        "transcript_path": plan["transcript_path"],
        "persistent_audio_chunks_created": False,
    }


def replay_materialized_result(work_order: Mapping[str, Any]) -> dict[str, Any]:
    order = validate_work_order(work_order)
    plan = result_plan(order)
    paths = {
        "transcript": Path(plan["transcript_path"]),
        "result": Path(plan["result_path"]),
    }
    documents: dict[str, Any] = {"work_order": order}
    for name, path in paths.items():
        body = _read_stable_result_leaf(path, f"materialized {name}")
        value = parse_json(body, f"materialized {name}")
        if body != canonical_bytes(value):
            raise LongFormExecutionError(f"materialized {name} is not canonical JSON")
        documents[name] = value
    return validate_result_bundle(documents)


def _read_stable_result_leaf(path: Path, label: str) -> bytes:
    """Read one sealed leaf through a retained descriptor and replay its entry."""

    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise LongFormExecutionError(f"{label} cannot be retained: {error}") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o400
            or before.st_size < 1
            or before.st_size > MAX_JSON_BYTES
        ):
            raise LongFormExecutionError(
                f"{label} is not an owned bounded mode-0400 singleton file"
            )
        chunks = []
        offset = 0
        while offset < before.st_size:
            body = os.pread(
                descriptor, min(1024 * 1024, before.st_size - offset), offset
            )
            if not body:
                raise LongFormExecutionError(f"{label} ended during retained read")
            chunks.append(body)
            offset += len(body)
        after = os.fstat(descriptor)
        if _file_identity(after) != _file_identity(before):
            raise LongFormExecutionError(f"{label} changed during retained read")
        try:
            linked = path.stat(follow_symlinks=False)
        except OSError as error:
            raise LongFormExecutionError(f"{label} directory entry cannot be replayed") from error
        if (linked.st_dev, linked.st_ino) != (after.st_dev, after.st_ino):
            raise LongFormExecutionError(f"{label} directory entry differs from held inode")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _faster_whisper_121_find_alignment_with_empty_guard(
    model: Any,
    tokenizer: Any,
    text_tokens: Sequence[list[int]],
    encoder_output: Any,
    num_frames: int,
    median_filter_width: int = 7,
) -> list[list[dict[str, Any]]]:
    """Run Faster-Whisper 1.2.1 alignment while accepting an empty CT2 result.

    This is the 1.2.1 ``WhisperModel.find_alignment`` implementation with the
    empty-alignment guard proposed upstream in faster-whisper PR #1460.  CTranslate2
    4.8 can legitimately return no pairs for a sub-stride terminal window.  The
    unguarded release builds a one-element boolean mask and indexes an empty array,
    aborting the entire recording.  An empty word list is already supported by
    Faster-Whisper's caller and by the HIMR transcript contract.
    """

    if len(text_tokens) == 0:
        return []

    results = model.model.align(
        encoder_output,
        tokenizer.sot_sequence,
        text_tokens,
        num_frames,
        median_filter_width=median_filter_width,
    )
    return_list: list[list[dict[str, Any]]] = []
    for result, text_token in zip(results, text_tokens):
        text_token_probs = result.text_token_probs
        alignments = result.alignments
        if len(alignments) == 0:
            return_list.append([])
            continue

        # NumPy remains a runtime-only Faster-Whisper dependency. Keeping this
        # import local preserves dependency-free contract replay for this module.
        import numpy as np

        text_indices = np.array([pair[0] for pair in alignments])
        time_indices = np.array([pair[1] for pair in alignments])

        words, word_tokens = tokenizer.split_to_word_tokens(
            text_token + [tokenizer.eot]
        )
        if len(word_tokens) <= 1:
            return_list.append([])
            continue
        word_boundaries = np.pad(
            np.cumsum([len(tokens) for tokens in word_tokens[:-1]]), (1, 0)
        )
        if len(word_boundaries) <= 1:
            return_list.append([])
            continue

        jumps = np.pad(
            np.diff(text_indices), (1, 0), constant_values=1
        ).astype(bool)
        jump_times = time_indices[jumps] / model.tokens_per_second
        start_times = jump_times[word_boundaries[:-1]]
        end_times = jump_times[word_boundaries[1:]]
        word_probabilities = [
            np.mean(text_token_probs[start:end])
            for start, end in zip(word_boundaries[:-1], word_boundaries[1:])
        ]

        return_list.append(
            [
                {
                    "word": word,
                    "tokens": tokens,
                    "start": start,
                    "end": end,
                    "probability": probability,
                }
                for word, tokens, start, end, probability in zip(
                    words,
                    word_tokens,
                    start_times,
                    end_times,
                    word_probabilities,
                )
            ]
        )
    return return_list


def _install_faster_whisper_empty_alignment_guard(
    model: Any, library_version: str
) -> bool:
    """Install the reviewed 1.2.1-only compatibility method when required."""

    if library_version != "1.2.1":
        return False
    current = getattr(model, "find_alignment", None)
    if not callable(current):
        raise LongFormExecutionError(
            "faster-whisper 1.2.1 model lacks the required alignment method"
        )
    if (
        getattr(current, "__func__", None)
        is _faster_whisper_121_find_alignment_with_empty_guard
    ):
        return True
    try:
        model.find_alignment = MethodType(
            _faster_whisper_121_find_alignment_with_empty_guard, model
        )
    except (AttributeError, TypeError) as error:
        raise LongFormExecutionError(
            "cannot install the faster-whisper 1.2.1 empty-alignment guard"
        ) from error
    return True


class FasterWhisperModelEngine:
    """Adapter for an already loaded ``faster_whisper.WhisperModel``.

    ``ephemeral_decoder`` must return an in-memory float32 waveform for a logical
    source range.  It is never called for a whole recording.  The executor does not
    accept a filesystem path from that callback, preventing accidental persistence
    from crossing this interface.
    """

    def __init__(
        self,
        model: Any,
        *,
        library_version: str,
        model_revision: str,
        model_identity_sha256: str,
        ephemeral_decoder: Callable[[SpanSourceView], Any] | None = None,
    ) -> None:
        self.model = model
        self.library_version = _text(library_version, "library version", 128)
        self.model_revision = _text(model_revision, "model revision", 512)
        self.model_identity_sha256 = _digest(model_identity_sha256, "model identity")
        self.ephemeral_decoder = ephemeral_decoder
        _install_faster_whisper_empty_alignment_guard(model, self.library_version)

    @staticmethod
    def _optional_score(value: Any, minimum: float, maximum: float) -> float | None:
        if value is None:
            return None
        return _number(value, "model score", minimum, maximum)

    def transcribe(self, request: SpanEngineRequest) -> Mapping[str, Any]:
        if request.source.is_whole_recording:
            audio: Any = request.source.retained_path
            decoder_provenance = {
                "kind": "faster_whisper_pyav",
                "executable_path": None,
                "executable_sha256": None,
                "execution_mode": "retained_parent_descriptor_once",
                "persistent_audio_chunks": False,
            }
        else:
            if self.ephemeral_decoder is None:
                raise LongFormExecutionError(
                    "logical span execution requires an ephemeral in-memory decoder"
                )
            audio = self.ephemeral_decoder(request.source)
            if isinstance(audio, (str, os.PathLike)):
                raise LongFormExecutionError(
                    "ephemeral decoder returned a path; persistent audio chunks are forbidden"
                )
            try:
                observed_samples = len(audio)
            except TypeError as error:
                raise LongFormExecutionError("ephemeral decoder output has no sample length") from error
            if observed_samples != request.source.analysis_sample_count:
                raise LongFormExecutionError("ephemeral decoder returned the wrong sample count")
            decoder_provenance = getattr(self.ephemeral_decoder, "provenance", None)
            if not isinstance(decoder_provenance, dict):
                raise LongFormExecutionError(
                    "ephemeral decoder lacks an explicit provenance descriptor"
                )
        decoding = request.decoding
        hotwords = " ".join(decoding["hotwords"]) or None
        iterator, info = self.model.transcribe(
            audio,
            language=decoding["language"],
            beam_size=decoding["beam_size"],
            best_of=decoding["best_of"],
            temperature=decoding["temperature"],
            word_timestamps=True,
            vad_filter=False,
            condition_on_previous_text=decoding["condition_on_previous_text"],
            initial_prompt=decoding["initial_prompt"],
            hotwords=hotwords,
        )
        maximum = request.source.analysis_sample_count
        segments = []
        for ordinal, segment in enumerate(iterator):
            start, start_clipped = seconds_to_samples(
                getattr(segment, "start"), f"segment {ordinal} start", maximum
            )
            end, end_clipped = seconds_to_samples(
                getattr(segment, "end"), f"segment {ordinal} end", maximum
            )
            if end < start:
                raise LongFormExecutionError(f"model segment {ordinal} timing is inverted")
            words = []
            for word_ordinal, word in enumerate(getattr(segment, "words", None) or []):
                word_start_value = getattr(word, "start", None)
                word_end_value = getattr(word, "end", None)
                if word_start_value is None or word_end_value is None:
                    word_start = word_end = None
                    word_clipped = False
                else:
                    word_start, word_start_clipped = seconds_to_samples(
                        word_start_value,
                        f"word {ordinal}.{word_ordinal} start",
                        maximum,
                    )
                    word_end, word_end_clipped = seconds_to_samples(
                        word_end_value,
                        f"word {ordinal}.{word_ordinal} end",
                        maximum,
                    )
                    if word_end < word_start:
                        raise LongFormExecutionError(
                            f"model word {ordinal}.{word_ordinal} timing is inverted"
                        )
                    word_clipped = word_start_clipped or word_end_clipped
                words.append(
                    {
                        "ordinal": word_ordinal,
                        "start_sample": word_start,
                        "end_sample": word_end,
                        "text": str(getattr(word, "word", "")),
                        "probability_raw": self._optional_score(
                            getattr(word, "probability", None), 0, 1
                        ),
                        "timing_clipped": word_clipped,
                    }
                )
            segments.append(
                {
                    "ordinal": ordinal,
                    "start_sample": start,
                    "end_sample": end,
                    "text": str(getattr(segment, "text", "")),
                    "words": words,
                    "scores": {
                        "temperature_raw": self._optional_score(
                            getattr(segment, "temperature", None), 0, 10
                        ),
                        "average_log_probability_raw": self._optional_score(
                            getattr(segment, "avg_logprob", None), -1000, 1000
                        ),
                        "compression_ratio_raw": self._optional_score(
                            getattr(segment, "compression_ratio", None), 0, 1_000_000
                        ),
                        "no_speech_probability_raw": self._optional_score(
                            getattr(segment, "no_speech_prob", None), 0, 1
                        ),
                    },
                    "timing_clipped": start_clipped or end_clipped,
                }
            )
        observed_duration_seconds = _number(
            getattr(info, "duration", None),
            "model decoded duration",
            0,
            maximum / SAMPLE_RATE_HZ + 2.0,
        )
        observed_duration_samples = math.floor(
            observed_duration_seconds * SAMPLE_RATE_HZ + 0.5
        )
        if abs(observed_duration_samples - maximum) > 1:
            raise LongFormExecutionError(
                "model decoded duration differs from the exact logical span sample count"
            )
        return {
            "engine": {
                "library": "faster-whisper",
                "library_version": self.library_version,
                "model_revision": self.model_revision,
                "model_identity_sha256": self.model_identity_sha256,
                "input_decoder": decoder_provenance,
            },
            "language": {
                "value": "en",
                "selection_basis": "forced_by_execution_work_order",
                "detection_performed": False,
                "probability_raw": None,
            },
            "duration_samples": maximum,
            "segments": segments,
        }


class SequentialFFmpegSpanDecoder:
    """One forward-only FFmpeg decode with a bounded rolling overlap buffer.

    Ordered logical spans may overlap.  Bytes before the first pending span are
    streamed and discarded once; adjacent overlap is retained once.  The parent is
    never reopened and no media bytes are written to a filesystem.
    """

    BYTES_PER_SAMPLE = 4
    IO_CHUNK_BYTES = 1024 * 1024

    def __init__(
        self,
        retained_source: RetainedSource,
        *,
        total_samples: int,
        executable: str | Path,
        executable_sha256: str,
    ) -> None:
        import subprocess

        self.source = retained_source
        self.total_samples = _integer(total_samples, "decoder total samples", 1)
        self.executable_path = _absolute(str(executable), "FFmpeg executable")
        expected_executable_sha = _digest(executable_sha256, "FFmpeg executable SHA-256")
        flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        try:
            self.executable_fd = os.open(self.executable_path, flags)
        except OSError as error:
            raise LongFormExecutionError(f"FFmpeg executable cannot be retained: {error}") from error
        try:
            executable_info = os.fstat(self.executable_fd)
            if not stat.S_ISREG(executable_info.st_mode) or executable_info.st_mode & 0o111 == 0:
                raise LongFormExecutionError("FFmpeg executable is not an executable regular file")
            digest = hashlib.sha256()
            offset = 0
            while offset < executable_info.st_size:
                body = os.pread(
                    self.executable_fd,
                    min(self.IO_CHUNK_BYTES, executable_info.st_size - offset),
                    offset,
                )
                if not body:
                    raise LongFormExecutionError("FFmpeg executable ended during hashing")
                digest.update(body)
                offset += len(body)
            if digest.hexdigest() != expected_executable_sha:
                raise LongFormExecutionError("FFmpeg executable SHA-256 differs")
            self.executable_identity = _file_identity(os.fstat(self.executable_fd))
            self.executable_sha256 = digest.hexdigest()
        except BaseException:
            os.close(self.executable_fd)
            raise
        self.source.verify()
        command = [
            f"/proc/self/fd/{self.executable_fd}",
            "-hide_banner",
            "-loglevel",
            "error",
            "-xerror",
            "-nostdin",
            "-i",
            self.source.proc_path,
            "-map",
            "0:a:0",
            "-ac",
            "1",
            "-ar",
            str(SAMPLE_RATE_HZ),
            "-f",
            "f32le",
            "pipe:1",
        ]
        try:
            self.process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                pass_fds=(self.source.descriptor, self.executable_fd),
            )
        except BaseException:
            os.close(self.executable_fd)
            raise
        if self.process.stdout is None or self.process.stderr is None:
            self.process.kill()
            self.process.wait()
            os.close(self.executable_fd)
            raise LongFormExecutionError("FFmpeg pipes were not created")
        self.decoded_samples = 0
        self.buffer_start_sample = 0
        self.buffer = bytearray()
        self.previous_analysis_start = -1
        self.closed = False
        self.finished = False
        self._stderr_tail = bytearray()
        self._stderr_lock = threading.Lock()
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr,
            name="himr-longform-ffmpeg-stderr",
            daemon=True,
        )
        self._stderr_thread.start()
        self.provenance = {
            "kind": "ffmpeg_single_pass_ephemeral_pcm_f32le",
            "executable_path": str(self.executable_path),
            "executable_sha256": self.executable_sha256,
            "execution_mode": "single_forward_decode_rolling_overlap_buffer",
            "persistent_audio_chunks": False,
        }

    def _drain_stderr(self) -> None:
        assert self.process.stderr is not None
        while True:
            try:
                body = self.process.stderr.read(4096)
            except OSError:
                return
            if not body:
                return
            with self._stderr_lock:
                self._stderr_tail.extend(body)
                if len(self._stderr_tail) > 4096:
                    del self._stderr_tail[:-4096]

    def _failure_message(self) -> str:
        with self._stderr_lock:
            body = bytes(self._stderr_tail)
        return body.decode("utf-8", errors="replace")

    def _read_exact(self, byte_count: int, *, retain: bool) -> None:
        assert self.process.stdout is not None
        remaining = byte_count
        while remaining:
            body = self.process.stdout.read(min(remaining, self.IO_CHUNK_BYTES))
            if not body:
                code = self.process.poll()
                raise LongFormExecutionError(
                    f"sequential FFmpeg decode ended early (status={code}): {self._failure_message()}"
                )
            if len(body) % self.BYTES_PER_SAMPLE:
                raise LongFormExecutionError("FFmpeg returned a partial float32 sample")
            if retain:
                self.buffer.extend(body)
            samples = len(body) // self.BYTES_PER_SAMPLE
            self.decoded_samples += samples
            remaining -= len(body)

    def __call__(self, source: SpanSourceView) -> Any:
        if self.closed:
            raise LongFormExecutionError("sequential decoder is closed")
        if (
            source.retained_descriptor != self.source.descriptor
            or source.source_sha256 != self.source.sha256
            or source.source_total_samples != self.total_samples
        ):
            raise LongFormExecutionError("sequential decoder source binding differs")
        start = source.analysis_start_sample
        end = source.analysis_end_sample
        if start < self.previous_analysis_start:
            raise LongFormExecutionError("logical spans must be requested in forward order")
        self.previous_analysis_start = start
        if start < self.buffer_start_sample:
            raise LongFormExecutionError("requested overlap is no longer in the rolling buffer")
        trim_samples = min(start, self.decoded_samples) - self.buffer_start_sample
        if trim_samples:
            del self.buffer[: trim_samples * self.BYTES_PER_SAMPLE]
            self.buffer_start_sample += trim_samples
        if start > self.decoded_samples:
            if self.buffer:
                raise LongFormExecutionError("decoder buffer accounting differs before a gap")
            discard_samples = start - self.decoded_samples
            self._read_exact(discard_samples * self.BYTES_PER_SAMPLE, retain=False)
            self.buffer_start_sample = start
        if end > self.decoded_samples:
            self._read_exact(
                (end - self.decoded_samples) * self.BYTES_PER_SAMPLE,
                retain=True,
            )
        expected_buffer_samples = self.decoded_samples - self.buffer_start_sample
        if len(self.buffer) != expected_buffer_samples * self.BYTES_PER_SAMPLE:
            raise LongFormExecutionError("rolling decoder buffer accounting differs")
        requested_samples = end - start
        body = bytes(self.buffer[: requested_samples * self.BYTES_PER_SAMPLE])
        if len(body) != requested_samples * self.BYTES_PER_SAMPLE:
            raise LongFormExecutionError("rolling decoder lacks the requested logical span")
        try:
            import numpy
        except ImportError as error:  # pragma: no cover - admitted runtime only
            raise LongFormExecutionError("NumPy is required for sequential span decoding") from error
        waveform = numpy.frombuffer(body, dtype="<f4")
        if len(waveform) != requested_samples:
            raise LongFormExecutionError("rolling decoder waveform length differs")
        self.source.verify()
        return waveform

    def finish(self, *, required_end_sample: int | None = None) -> None:
        if self.closed or self.finished:
            return
        required_end = _integer(
            self.total_samples if required_end_sample is None else required_end_sample,
            "decoder required end sample",
            1,
            self.total_samples,
        )
        if self.decoded_samples < required_end:
            self._read_exact(
                (required_end - self.decoded_samples) * self.BYTES_PER_SAMPLE,
                retain=False,
            )
        if required_end < self.total_samples:
            assert self.process.stdout is not None
            self.process.stdout.close()
            if self.process.poll() is None:
                self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except Exception:
                self.process.kill()
                self.process.wait()
            self._stderr_thread.join(timeout=5)
            if self._stderr_thread.is_alive():
                raise LongFormExecutionError("FFmpeg stderr drain did not terminate")
            if _file_identity(os.fstat(self.executable_fd)) != self.executable_identity:
                raise LongFormExecutionError("retained FFmpeg executable identity changed")
            self.source.verify()
            self.finished = True
            return
        assert self.process.stdout is not None
        extra = self.process.stdout.read(1)
        if extra:
            raise LongFormExecutionError("FFmpeg emitted more samples than the parent manifest")
        returncode = self.process.wait()
        self._stderr_thread.join(timeout=5)
        if self._stderr_thread.is_alive():
            raise LongFormExecutionError("FFmpeg stderr drain did not terminate")
        if returncode != 0:
            raise LongFormExecutionError(
                f"sequential FFmpeg decode exited {returncode}: {self._failure_message()}"
            )
        if _file_identity(os.fstat(self.executable_fd)) != self.executable_identity:
            raise LongFormExecutionError("retained FFmpeg executable identity changed")
        self.source.verify()
        self.finished = True

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except Exception:
                self.process.kill()
                self.process.wait()
        if self.process.stdout is not None:
            self.process.stdout.close()
        if self.process.stderr is not None:
            self.process.stderr.close()
        self._stderr_thread.join(timeout=5)
        os.close(self.executable_fd)

    def __enter__(self) -> "SequentialFFmpegSpanDecoder":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


__all__ = [
    "FasterWhisperModelEngine",
    "LongFormExecutionError",
    "RetainedSource",
    "SequentialFFmpegSpanDecoder",
    "SpanEngine",
    "SpanEngineRequest",
    "SpanSourceView",
    "canonical_bytes",
    "default_decoding",
    "engine_request",
    "execute_work_order",
    "make_work_order_from_plan",
    "make_work_orders_from_plan",
    "materialize_result_bundle",
    "replay_materialized_result",
    "result_plan",
    "samples_to_ms",
    "validate_result_bundle",
    "validate_work_order",
]
