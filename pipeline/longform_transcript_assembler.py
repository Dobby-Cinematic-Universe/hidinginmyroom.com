#!/usr/bin/env python3
"""Assemble private span-local ASR hypotheses on one parent recording timeline.

The assembler is deliberately downstream of planning and inference.  It reads a
sample-accurate long-form plan plus independently completed normalized transcript
artifacts, projects local millisecond timestamps onto the 16 kHz parent timeline,
and writes one new recording-level machine transcript.  Source artifacts are never
rewritten, deleted, imported, catalogued, or published.

Half-open PCM sample intervals are authoritative.  Milliseconds in the output are
rounded presentation values and must not be used to reconstruct sample positions.
"""

from __future__ import annotations

import argparse
from bisect import bisect_right
import difflib
import hashlib
import json
import math
import os
import re
import stat
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"
if str(CORPUS_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORPUS_SOURCE_ROOT))

from himr_corpus.longform_asr_planner import (  # noqa: E402
    LongformPlanningError,
    validate_longform_asr_plan,
)


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
PLAN_KIND = "himr_longform_asr_plan"
BINDINGS_KIND = "himr_longform_span_transcript_bindings"
OUTPUT_KIND = "himr_longform_recording_transcript"
SAMPLE_RATE_HZ = 16_000
MAX_JSON_BYTES = 512 * 1024 * 1024
MAX_SPANS = 4_096
MAX_SEGMENTS_PER_SPAN = 1_000_000
MAX_WORDS_TOTAL = 20_000_000
WORD_MATCH_TOLERANCE_SAMPLES = 19_200  # 1.2 seconds at 16 kHz
SEGMENT_MATCH_TOLERANCE_SAMPLES = 48_000  # 3 seconds at 16 kHz
BOUNDARY_SIMILARITY_MILLIONTHS = 650_000

SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
SPAN_ID_RE = re.compile(r"lfspan_[0-9a-f]{32}\Z")
PLAN_ID_RE = re.compile(r"lfplan_[0-9a-f]{32}\Z")
SAFE_CODE_RE = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")
TOKEN_RE = re.compile(r"[^\W_]+(?:['’][^\W_]+)?", re.UNICODE)
PLAN_KEYS = {
    "kind",
    "schema_version",
    "planner_version",
    "plan_id",
    "identity_sha256",
    "strategy",
    "recording",
    "boundary_candidates",
    "policy",
    "input_digests",
    "execution_contract",
    "spans",
    "coverage",
}
RECORDING_KEYS = {"recording_id", "media_id", "input"}
INPUT_KEYS = {
    "artifact_id",
    "path",
    "sha256",
    "byte_count",
    "sample_rate_hz",
    "channels",
    "total_samples",
    "duration_ms",
}
SPAN_KEYS = {
    "span_id",
    "ordinal",
    "analysis_start_sample",
    "analysis_end_sample",
    "core_start_sample",
    "core_end_sample",
    "boundary_reason",
    "boundary_confidence_millionths",
    "padding",
}
BINDINGS_KEYS = {
    "kind",
    "schema_version",
    "parent_manifest_sha256",
    "spans",
    "policy",
}
BINDING_KEYS = {"span_id", "status", "result", "transcript", "failure_code"}
TRANSCRIPT_BINDING_KEYS = {"path", "sha256"}
NATIVE_TRANSCRIPT_KEYS = {
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
    "identity_sha256",
    "document_id",
}
NATIVE_SEGMENT_REQUIRED_KEYS = {
    "ordinal",
    "start_sample",
    "end_sample",
    "text",
    "words",
    "scores",
}
NATIVE_SEGMENT_ALLOWED_KEYS = NATIVE_SEGMENT_REQUIRED_KEYS | {"timing_clipped"}
NATIVE_WORD_REQUIRED_KEYS = {
    "start_sample",
    "end_sample",
    "text",
}
NATIVE_WORD_ALLOWED_KEYS = NATIVE_WORD_REQUIRED_KEYS | {
    "probability_raw",
    "timing_clipped",
    "anomaly_flags",
}
NATIVE_SCORE_KEYS = {
    "temperature_raw",
    "average_log_probability_raw",
    "compression_ratio_raw",
    "no_speech_probability_raw",
}
NATIVE_EXECUTION_LINEAGE_KEYS = {
    "longform_scope_status",
    "production_profile",
    "runtime_admission",
    "model",
}
NATIVE_PROFILE_BINDING_KEYS = {
    "profile_id",
    "identity_sha256",
    "physical_sha256",
}
NATIVE_RUNTIME_BINDING_KEYS = {
    "receipt_id",
    "identity_sha256",
    "physical_sha256",
    "status",
}
NATIVE_MODEL_BINDING_KEYS = {"repository", "revision", "identity_sha256"}
NATIVE_ENGINE_KEYS = {
    "library",
    "library_version",
    "model_revision",
    "model_identity_sha256",
    "input_decoder",
}
NATIVE_RESULT_KEYS = {
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
    "identity_sha256",
    "result_id",
}
BINDINGS_POLICY = {
    "visibility": "private",
    "source_artifact_mutation": False,
    "catalogue_mutation_authority": "none",
    "publication_authority": "none",
}
OUTPUT_POLICY = {
    "visibility": "private",
    "machine_generated": True,
    "scores_calibrated": False,
    "source_artifact_mutation": False,
    "network_access": False,
    "catalogue_mutation_authority": "none",
    "publication_authority": "none",
    "wiki_authority": "none",
    "identity_authority": "none",
    "deletion_authority": "none",
}


class AssemblyError(RuntimeError):
    """An input or output violates the isolated assembly contract."""


@dataclass(frozen=True)
class Span:
    span_id: str
    ordinal: int
    analysis_start: int
    analysis_end: int
    core_start: int
    core_end: int
    boundary_reason: str
    boundary_confidence_millionths: int | None

    @property
    def analysis_samples(self) -> int:
        return self.analysis_end - self.analysis_start

    @property
    def core_samples(self) -> int:
        return self.core_end - self.core_start


@dataclass(frozen=True)
class CoreOwnershipIndex:
    """Immutable logarithmic lookup over exactly tiled half-open span cores."""

    spans: tuple[Span, ...]
    core_starts: tuple[int, ...]
    core_ends: tuple[int, ...]

    @classmethod
    def build(cls, spans: Sequence[Span]) -> CoreOwnershipIndex:
        frozen_spans = tuple(spans)
        if not frozen_spans:
            raise AssemblyError("core ownership index needs at least one span")
        core_starts = tuple(span.core_start for span in frozen_spans)
        core_ends = tuple(span.core_end for span in frozen_spans)
        cursor = 0
        for span in frozen_spans:
            if span.core_start != cursor or span.core_end <= span.core_start:
                raise AssemblyError(
                    "core ownership index requires sorted spans that exactly tile "
                    "the parent timeline"
                )
            cursor = span.core_end
        return cls(
            spans=frozen_spans,
            core_starts=core_starts,
            core_ends=core_ends,
        )

    def owner_for_anchor(self, anchor: int) -> Span:
        if anchor < self.core_starts[0] or anchor >= self.core_ends[-1]:
            raise AssemblyError("projected unit anchor is outside the parent timeline")
        # Half-open cores make an anchor exactly on an end belong to the next core.
        position = bisect_right(self.core_ends, anchor)
        if not (
            position < len(self.spans)
            and self.core_starts[position] <= anchor < self.core_ends[position]
        ):
            raise AssemblyError("projected unit anchor has no core owner")
        return self.spans[position]


@dataclass
class Unit:
    unit_id: str
    unit_kind: str
    span: Span
    segment_ordinal: int
    word_ordinal: int | None
    text: str
    normalized_text: str
    start_sample: int
    end_sample: int
    anchor_sample: int
    owner_span_id: str
    core_owned: bool
    core_margin_samples: int
    raw_probability: float | None
    anomaly_flags: tuple[str, ...]
    retained: bool = False
    duplicate_group_id: str | None = None


@dataclass
class SegmentCandidate:
    span: Span
    local_ordinal: int
    projected_start_sample: int
    projected_end_sample: int
    original_text: str
    units: list[Unit] = field(default_factory=list)
    raw_score_provenance: dict[str, Any] | None = None


@dataclass
class LoadedSpan:
    span: Span
    status: str
    failure_code: str | None
    result_path: str | None
    result_sha256: str | None
    result_identity_sha256: str | None
    transcript_path: str | None
    transcript_sha256: str | None
    transcript_identity_sha256: str | None
    segments: list[SegmentCandidate]


def canonical_bytes(value: Any, *, trailing_newline: bool = False) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise AssemblyError(f"value is not strict canonical JSON: {error}") from error
    if trailing_newline:
        encoded += "\n"
    return encoded.encode("utf-8")


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
        raise AssemblyError(f"value is not strict JSON: {error}") from error


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def stable_id(prefix: str, *parts: object) -> str:
    body = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(body).hexdigest()[:32]}"


def _reject_constant(value: str) -> None:
    raise AssemblyError(f"non-finite JSON constant is forbidden: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AssemblyError(f"JSON object contains duplicate key: {key}")
        result[key] = value
    return result


def parse_json(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8", errors="strict")
        return json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except AssemblyError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise AssemblyError(f"{label} is not strict UTF-8 JSON: {error}") from error


def read_stable_file(path: Path, label: str, maximum_bytes: int = MAX_JSON_BYTES) -> bytes:
    """Read one regular non-symlink file through a stable descriptor."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise AssemblyError(f"{label} cannot be opened safely: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise AssemblyError(f"{label} must be a regular non-symlink file")
        if before.st_size <= 0 or before.st_size > maximum_bytes:
            raise AssemblyError(f"{label} byte count is outside the supported range")
        chunks: list[bytes] = []
        remaining = maximum_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise AssemblyError(f"{label} changed while being read")
        if len(body) != before.st_size or len(body) > maximum_bytes:
            raise AssemblyError(f"{label} could not be read within its byte bound")
        return body
    finally:
        os.close(descriptor)


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AssemblyError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise AssemblyError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def integer(value: Any, label: str, minimum: int = 0, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise AssemblyError(f"{label} must be an integer within [{minimum}, {maximum}]")
    return value


def finite_optional(value: Any, label: str, minimum: float, maximum: float) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AssemblyError(f"{label} must be null or numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise AssemblyError(f"{label} is outside its finite range")
    return result


def required_text(value: Any, label: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise AssemblyError(f"{label} must be non-empty bounded text")
    return value


def required_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise AssemblyError(f"{label} must be a lowercase SHA-256")
    return value


def resolve_input_path(value: Any, base: Path, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value or "://" in value:
        raise AssemblyError(f"{label} must be a local filesystem path")
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        return candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise AssemblyError(f"{label} cannot be resolved: {error}") from error


def presentation_ms(sample: int) -> int:
    """Round exact samples to nearest integer millisecond, ties upward."""

    return (sample * 1_000 + SAMPLE_RATE_HZ // 2) // SAMPLE_RATE_HZ


def local_ms_to_samples(local_ms: int) -> int:
    # At exactly 16 kHz, every integer millisecond is exactly 16 samples.
    return local_ms * (SAMPLE_RATE_HZ // 1_000)


def normalize_piece(text: str) -> str:
    value = unicodedata.normalize("NFKC", text).casefold().replace("’", "'")
    return " ".join(TOKEN_RE.findall(value))


def normalized_tokens(text: str) -> list[str]:
    value = normalize_piece(text)
    return value.split() if value else []


def _validate_plan(value: Any) -> tuple[dict[str, Any], list[Span]]:
    plan = exact_object(value, "parent plan", PLAN_KEYS)
    if plan["kind"] != PLAN_KIND or plan["schema_version"] != SCHEMA_VERSION:
        raise AssemblyError("parent plan kind or schema version is unsupported")
    if not isinstance(plan["plan_id"], str) or not PLAN_ID_RE.fullmatch(plan["plan_id"]):
        raise AssemblyError("parent plan ID is malformed")
    plan_identity = required_sha256(plan["identity_sha256"], "parent plan identity")
    try:
        replayed = validate_longform_asr_plan(plan)
    except LongformPlanningError as error:
        raise AssemblyError(
            f"parent plan does not exactly replay from the frozen planner contract: {error}"
        ) from error
    if replayed != plan or plan["plan_id"] != f"lfplan_{plan_identity[:32]}":
        raise AssemblyError("parent plan differs from exact deterministic replay")
    recording = exact_object(plan["recording"], "parent recording", RECORDING_KEYS)
    required_text(recording["recording_id"], "recording ID")
    required_text(recording["media_id"], "media ID")
    source = exact_object(recording["input"], "parent input", INPUT_KEYS)
    required_text(source["artifact_id"], "parent artifact ID")
    required_text(source["path"], "parent artifact path", 16_384)
    required_sha256(source["sha256"], "parent artifact SHA-256")
    integer(source["byte_count"], "parent byte count", 1)
    if source["sample_rate_hz"] != SAMPLE_RATE_HZ or source["channels"] != 1:
        raise AssemblyError("parent audio must be 16 kHz mono")
    total_samples = integer(source["total_samples"], "parent total samples", 1)
    duration_ms = integer(source["duration_ms"], "parent duration milliseconds", 0)
    if abs(local_ms_to_samples(duration_ms) - total_samples) > 16:
        raise AssemblyError("parent duration milliseconds disagree with exact sample count")

    rows = plan["spans"]
    if not isinstance(rows, list) or not rows or len(rows) > MAX_SPANS:
        raise AssemblyError("parent plan spans must be a non-empty bounded array")
    spans: list[Span] = []
    identifiers: set[str] = set()
    ordinals: list[int] = []
    for index, raw in enumerate(rows):
        row = exact_object(raw, f"span {index}", SPAN_KEYS)
        span_id = row["span_id"]
        if not isinstance(span_id, str) or not SPAN_ID_RE.fullmatch(span_id):
            raise AssemblyError(f"span {index} ID is malformed")
        if span_id in identifiers:
            raise AssemblyError("parent plan repeats a span ID")
        identifiers.add(span_id)
        ordinal = integer(row["ordinal"], f"span {index} ordinal", 0, MAX_SPANS)
        ordinals.append(ordinal)
        analysis_start = integer(
            row["analysis_start_sample"], f"span {index} analysis start", 0, total_samples
        )
        analysis_end = integer(
            row["analysis_end_sample"], f"span {index} analysis end", 1, total_samples
        )
        core_start = integer(
            row["core_start_sample"], f"span {index} core start", 0, total_samples
        )
        core_end = integer(
            row["core_end_sample"], f"span {index} core end", 1, total_samples
        )
        if not analysis_start <= core_start < core_end <= analysis_end:
            raise AssemblyError(f"span {index} core is not contained in its analysis interval")
        padding = exact_object(
            row["padding"],
            f"span {index} padding",
            {
                "requested_left_samples",
                "requested_right_samples",
                "applied_left_samples",
                "applied_right_samples",
                "clipped_left_samples",
                "clipped_right_samples",
            },
        )
        for name, amount in padding.items():
            integer(amount, f"span {index} padding {name}")
        reason = required_text(row["boundary_reason"], f"span {index} boundary reason")
        raw_confidence = row["boundary_confidence_millionths"]
        confidence = (
            None
            if raw_confidence is None
            else integer(
                raw_confidence,
                f"span {index} boundary confidence",
                0,
                1_000_000,
            )
        )
        spans.append(
            Span(
                span_id=span_id,
                ordinal=ordinal,
                analysis_start=analysis_start,
                analysis_end=analysis_end,
                core_start=core_start,
                core_end=core_end,
                boundary_reason=reason,
                boundary_confidence_millionths=confidence,
            )
        )
    spans.sort(key=lambda item: item.ordinal)
    if [item.ordinal for item in spans] != list(range(len(spans))):
        raise AssemblyError("span ordinals must be contiguous and start at zero")
    cursor = 0
    for span in spans:
        if span.core_start != cursor:
            raise AssemblyError("span cores do not exactly tile the parent timeline")
        cursor = span.core_end
    if cursor != total_samples:
        raise AssemblyError("span cores do not cover the complete parent timeline")
    return plan, spans


def _validate_bindings(
    value: Any, plan_sha256: str, spans: Sequence[Span]
) -> dict[str, dict[str, Any]]:
    manifest = exact_object(value, "span result bindings", BINDINGS_KEYS)
    if manifest["kind"] != BINDINGS_KIND or manifest["schema_version"] != SCHEMA_VERSION:
        raise AssemblyError("span result bindings kind or schema version is unsupported")
    if manifest["parent_manifest_sha256"] != plan_sha256:
        raise AssemblyError("span result bindings refer to different parent plan bytes")
    if manifest["policy"] != BINDINGS_POLICY:
        raise AssemblyError("span result bindings policy is not the exact private contract")
    rows = manifest["spans"]
    if not isinstance(rows, list) or len(rows) != len(spans):
        raise AssemblyError("span result bindings must contain exactly one row per plan span")
    expected = {span.span_id for span in spans}
    bound: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(rows):
        row = exact_object(raw, f"span result binding {index}", BINDING_KEYS)
        span_id = row["span_id"]
        if span_id not in expected or span_id in bound:
            raise AssemblyError("span result bindings contain an unknown or repeated span ID")
        status_value = row["status"]
        if status_value not in {"completed", "failed", "pending"}:
            raise AssemblyError(f"span result binding {index} status is unsupported")
        transcript = row["transcript"]
        result = row["result"]
        failure_code = row["failure_code"]
        if status_value == "completed":
            document = exact_object(
                transcript, f"span result binding {index} transcript", TRANSCRIPT_BINDING_KEYS
            )
            required_text(document["path"], f"span result binding {index} path", 16_384)
            required_sha256(document["sha256"], f"span result binding {index} SHA-256")
            result_document = exact_object(
                result, f"span result binding {index} result", TRANSCRIPT_BINDING_KEYS
            )
            required_text(
                result_document["path"], f"span result binding {index} result path", 16_384
            )
            required_sha256(
                result_document["sha256"], f"span result binding {index} result SHA-256"
            )
            if failure_code is not None:
                raise AssemblyError("completed span result binding cannot have a failure code")
        else:
            if transcript is not None or result is not None:
                raise AssemblyError(
                    "non-completed span result binding cannot bind result artifacts"
                )
            if status_value == "failed":
                if not isinstance(failure_code, str) or not SAFE_CODE_RE.fullmatch(
                    failure_code
                ):
                    raise AssemblyError(
                        "failed span result binding needs a machine-safe failure code"
                    )
            elif failure_code is not None:
                raise AssemblyError("pending span result binding cannot have a failure code")
        bound[span_id] = row
    if set(bound) != expected:
        raise AssemblyError("span result bindings do not exactly cover the plan spans")
    return bound


def _transcript_identity(document: dict[str, Any], label: str) -> str | None:
    identity = document.get("identity_sha256")
    document_id = document.get("document_id")
    if identity is None and document_id is None:
        return None
    expected = required_sha256(identity, f"{label} identity")
    if not isinstance(document_id, str) or not document_id.endswith(expected[:32]):
        raise AssemblyError(f"{label} document ID does not bind its identity")
    core = {
        key: value
        for key, value in document.items()
        if key not in {"identity_sha256", "document_id"}
    }
    observed = sha256_bytes(canonical_bytes(core, trailing_newline=True))
    if observed != expected:
        raise AssemblyError(f"{label} semantic identity does not replay")
    return expected


def _unit_anchor(start: int, end: int, total_samples: int) -> int:
    value = start + (end - start) // 2
    return min(max(value, 0), total_samples - 1)


def _anomaly_names(word: dict[str, Any]) -> tuple[str, ...]:
    flags = word.get("timing_anomaly_flags")
    result: set[str] = set()
    if flags is not None:
        if not isinstance(flags, dict) or any(not isinstance(name, str) for name in flags):
            raise AssemblyError("word timing anomaly flags must be an object")
        result.update(name for name, present in flags.items() if present is True)
    compact = word.get("anomaly_flags")
    if compact is not None:
        if (
            not isinstance(compact, list)
            or any(
                not isinstance(name, str) or not SAFE_CODE_RE.fullmatch(name)
                for name in compact
            )
            or len(set(compact)) != len(compact)
        ):
            raise AssemblyError("word anomaly flags must be a unique machine-safe array")
        result.update(compact)
    if word.get("timing_clipped") is True:
        result.add("timing_clipped")
    return tuple(sorted(result))


def _validate_native_result_linkage(
    value: Any,
    *,
    span: Span,
    transcript_path: Path,
    transcript_sha256: str,
    transcript_identity_sha256: str | None,
    transcript_work_order: Any,
    transcript_plan: Any,
    expected_plan_binding: dict[str, str],
    recording: dict[str, Any],
) -> str:
    if not isinstance(value, dict):
        raise AssemblyError(f"{span.span_id} result must be an object")
    exact_object(value, f"{span.span_id} result", NATIVE_RESULT_KEYS)
    if (
        value.get("kind") != "himr_longform_asr_span_result"
        or value.get("schema_version") != 1
        or value.get("implementation_version") != "0.1.0"
        or value.get("status") != "completed"
    ):
        raise AssemblyError(f"{span.span_id} result is not a completed native span result")
    identity = value.get("identity_sha256")
    result_id = value.get("result_id")
    expected_identity = required_sha256(identity, f"{span.span_id} result identity")
    if not isinstance(result_id, str) or not result_id.endswith(expected_identity[:32]):
        raise AssemblyError(f"{span.span_id} result ID does not bind its identity")
    if result_id != f"longasrresult1_{expected_identity[:32]}":
        raise AssemblyError(f"{span.span_id} result ID prefix is unsupported")
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", "result_id"}}
    if sha256_bytes(canonical_bytes(core, trailing_newline=True)) != expected_identity:
        raise AssemblyError(f"{span.span_id} result semantic identity does not replay")
    lineage = exact_object(
        value.get("execution_lineage"),
        f"{span.span_id} result execution lineage",
        NATIVE_EXECUTION_LINEAGE_KEYS,
    )
    if lineage["longform_scope_status"] != "candidate_unsoaked":
        raise AssemblyError(f"{span.span_id} result long-form scope status differs")
    profile = exact_object(
        lineage["production_profile"],
        f"{span.span_id} result production-profile binding",
        NATIVE_PROFILE_BINDING_KEYS,
    )
    required_text(profile["profile_id"], f"{span.span_id} production profile ID", 256)
    required_sha256(
        profile["identity_sha256"], f"{span.span_id} production profile identity"
    )
    required_sha256(
        profile["physical_sha256"], f"{span.span_id} production profile physical hash"
    )
    runtime = exact_object(
        lineage["runtime_admission"],
        f"{span.span_id} result runtime binding",
        NATIVE_RUNTIME_BINDING_KEYS,
    )
    required_text(runtime["receipt_id"], f"{span.span_id} runtime receipt ID", 256)
    required_sha256(runtime["identity_sha256"], f"{span.span_id} runtime identity")
    required_sha256(runtime["physical_sha256"], f"{span.span_id} runtime physical hash")
    if runtime["status"] not in {"candidate", "admitted"}:
        raise AssemblyError(f"{span.span_id} result runtime status differs")
    model = exact_object(
        lineage["model"],
        f"{span.span_id} result model binding",
        NATIVE_MODEL_BINDING_KEYS,
    )
    required_text(model["repository"], f"{span.span_id} model repository", 512)
    required_text(model["revision"], f"{span.span_id} model revision", 512)
    required_sha256(model["identity_sha256"], f"{span.span_id} model identity")
    engine = exact_object(
        value.get("engine"),
        f"{span.span_id} result engine",
        NATIVE_ENGINE_KEYS,
    )
    required_text(engine["library"], f"{span.span_id} engine library", 128)
    required_text(engine["library_version"], f"{span.span_id} engine version", 128)
    if (
        engine["model_revision"] != model["revision"]
        or engine["model_identity_sha256"] != model["identity_sha256"]
    ):
        raise AssemblyError(f"{span.span_id} result engine differs from model lineage")
    result_recording = value.get("recording")
    if not isinstance(result_recording, dict) or (
        result_recording.get("recording_id") != recording["recording_id"]
        or result_recording.get("media_id") != recording["media_id"]
        or result_recording.get("input_sha256") != recording["input"]["sha256"]
        or result_recording.get("total_samples") != recording["input"]["total_samples"]
    ):
        raise AssemblyError(f"{span.span_id} result recording lineage differs from the plan")
    result_span = value.get("span")
    if (
        not isinstance(result_span, dict)
        or result_span.get("span_id") != span.span_id
        or result_span.get("ordinal") != span.ordinal
    ):
        raise AssemblyError(f"{span.span_id} result span lineage differs")
    analysis = result_span.get("analysis")
    core_interval = result_span.get("core")
    if not isinstance(analysis, dict) or not isinstance(core_interval, dict):
        raise AssemblyError(f"{span.span_id} result lacks exact span intervals")
    if analysis != {
        "start_sample": span.analysis_start,
        "end_sample": span.analysis_end,
        "sample_count": span.analysis_samples,
    } or core_interval != {
        "start_sample": span.core_start,
        "end_sample": span.core_end,
        "sample_count": span.core_samples,
    }:
        raise AssemblyError(f"{span.span_id} result intervals differ from the plan")
    if result_span.get("core_local") != {
        "start_sample": span.core_start - span.analysis_start,
        "end_sample": span.core_end - span.analysis_start,
        "sample_count": span.core_samples,
    }:
        raise AssemblyError(f"{span.span_id} result local core transform differs")
    if value.get("coverage") != {
        "analysis_start_sample": span.analysis_start,
        "analysis_end_sample": span.analysis_end,
        "analysis_sample_count": span.analysis_samples,
        "core_start_sample": span.core_start,
        "core_end_sample": span.core_end,
        "core_sample_count": span.core_samples,
        "coordinate_system": "parent_media_16000hz_half_open_samples",
        "engine_reported_analysis_complete": True,
    }:
        raise AssemblyError(f"{span.span_id} result does not prove complete span coverage")
    if value.get("work_order") != transcript_work_order:
        raise AssemblyError(f"{span.span_id} result and transcript work-order lineage differ")
    if (
        transcript_plan != expected_plan_binding
        or value.get("plan") != expected_plan_binding
    ):
        raise AssemblyError(f"{span.span_id} result/transcript refer to a different parent plan")
    execution = value.get("execution")
    if (
        not isinstance(execution, dict)
        or execution.get("engine_call_count") != 1
        or execution.get("persistent_audio_chunks_created") is not False
    ):
        raise AssemblyError(f"{span.span_id} result execution completion proof differs")
    policy = value.get("policy")
    if (
        not isinstance(policy, dict)
        or policy.get("network_access") is not False
        or policy.get("persistent_audio_chunks") is not False
        or policy.get("coordinate_authority")
        != "exact_16000hz_half_open_samples"
        or policy.get("publication_authority") != "none"
        or policy.get("catalogue_mutation_authority") != "none"
    ):
        raise AssemblyError(f"{span.span_id} result safety policy differs")
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 1:
        raise AssemblyError(f"{span.span_id} result artifact bindings are absent")
    matches = [
        artifact
        for artifact in artifacts
        if isinstance(artifact, dict)
        and artifact.get("artifact_kind") == "longform_span_transcript_json"
    ]
    if len(matches) != 1 or matches[0].get("sha256") != transcript_sha256:
        raise AssemblyError(f"{span.span_id} result does not bind the normalized transcript")
    if (
        matches[0].get("byte_count") != transcript_path.stat().st_size
        or matches[0].get("identity_sha256") != transcript_identity_sha256
    ):
        raise AssemblyError(f"{span.span_id} result transcript artifact metadata differs")
    artifact_path = matches[0].get("path")
    if not isinstance(artifact_path, str):
        raise AssemblyError(f"{span.span_id} normalized artifact path is malformed")
    try:
        resolved_artifact = Path(artifact_path).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise AssemblyError(
            f"{span.span_id} normalized artifact path cannot be resolved"
        ) from error
    if resolved_artifact != transcript_path:
        raise AssemblyError(f"{span.span_id} result binds a different transcript path")
    return expected_identity


def _load_transcript(
    span: Span,
    binding: dict[str, Any],
    bindings_base: Path,
    ownership_index: CoreOwnershipIndex,
    total_samples: int,
    recording: dict[str, Any],
    plan_binding: dict[str, str],
) -> LoadedSpan:
    if binding["status"] != "completed":
        return LoadedSpan(
            span=span,
            status=binding["status"],
            failure_code=binding["failure_code"],
            result_path=None,
            result_sha256=None,
            result_identity_sha256=None,
            transcript_path=None,
            transcript_sha256=None,
            transcript_identity_sha256=None,
            segments=[],
        )
    transcript_binding = binding["transcript"]
    result_binding = binding["result"]
    path = resolve_input_path(
        transcript_binding["path"], bindings_base, f"{span.span_id} transcript"
    )
    body = read_stable_file(path, f"{span.span_id} transcript")
    observed_sha256 = sha256_bytes(body)
    if observed_sha256 != transcript_binding["sha256"]:
        raise AssemblyError(f"{span.span_id} transcript differs from its SHA-256 binding")
    document = parse_json(body, f"{span.span_id} transcript")
    if not isinstance(document, dict):
        raise AssemblyError(f"{span.span_id} transcript must be an object")
    kind = document.get("kind")
    if kind != "himr_longform_asr_span_transcript":
        raise AssemblyError(f"{span.span_id} transcript is not native long-form ASR v1")
    identity = _transcript_identity(document, f"{span.span_id} transcript")
    exact_object(document, f"{span.span_id} native transcript", NATIVE_TRANSCRIPT_KEYS)
    if document.get("schema_version") != 1 or document.get("span_id") != span.span_id:
        raise AssemblyError(f"{span.span_id} native transcript lineage differs")
    if document.get("document_id") != f"longasrtranscript1_{identity[:32]}":
        raise AssemblyError(f"{span.span_id} native transcript ID prefix is unsupported")
    if (
        document.get("plan") != plan_binding
        or document.get("language") != "en"
        or document.get("review_status") != "unreviewed_machine_output"
    ):
        raise AssemblyError(f"{span.span_id} native transcript contract differs")
    work_order_binding = exact_object(
        document.get("work_order"),
        f"{span.span_id} native transcript work order",
        {"work_order_id", "identity_sha256"},
    )
    required_text(work_order_binding["work_order_id"], f"{span.span_id} work-order ID")
    required_sha256(
        work_order_binding["identity_sha256"], f"{span.span_id} work-order identity"
    )
    semantics = document.get("semantics")
    if (
        not isinstance(semantics, dict)
        or semantics.get("coordinate_system")
        != "decode_span_local_16000hz_half_open_samples"
        or semantics.get("parent_projection")
        != "add_result_span_analysis_start_sample"
        or semantics.get("recording_projection")
        != "overlap_aware_assembler_required"
    ):
        raise AssemblyError(f"{span.span_id} native transcript coordinate semantics differ")
    analysis_sample_count = integer(
        document.get("analysis_sample_count"),
        f"{span.span_id} native transcript analysis sample count",
        1,
    )
    if analysis_sample_count != span.analysis_samples:
        raise AssemblyError(f"{span.span_id} native transcript duration differs from its span")
    result_path = resolve_input_path(
        result_binding["path"], bindings_base, f"{span.span_id} result"
    )
    result_body = read_stable_file(result_path, f"{span.span_id} result")
    result_sha256 = sha256_bytes(result_body)
    if result_sha256 != result_binding["sha256"]:
        raise AssemblyError(f"{span.span_id} result differs from its SHA-256 binding")
    result_identity = _validate_native_result_linkage(
        parse_json(result_body, f"{span.span_id} result"),
        span=span,
        transcript_path=path,
        transcript_sha256=observed_sha256,
        transcript_identity_sha256=identity,
        transcript_work_order=document.get("work_order"),
        transcript_plan=document.get("plan"),
        expected_plan_binding=plan_binding,
        recording=recording,
    )
    rows = document.get("segments")
    if not isinstance(rows, list) or len(rows) > MAX_SEGMENTS_PER_SPAN:
        raise AssemblyError(f"{span.span_id} transcript segment array is invalid")
    if document.get("segment_count") != len(rows):
        raise AssemblyError(f"{span.span_id} native transcript segment count differs")
    observed_word_count = sum(
        len(row.get("words", []))
        if isinstance(row, dict) and isinstance(row.get("words"), list)
        else -1
        for row in rows
    )
    if document.get("word_count") != observed_word_count:
        raise AssemblyError(f"{span.span_id} native transcript word count differs")
    expected_text = "".join(
        str(row.get("text", "")) for row in rows if isinstance(row, dict)
    ).strip()
    if document.get("text") != expected_text:
        raise AssemblyError(f"{span.span_id} native transcript joined text differs")

    segments: list[SegmentCandidate] = []
    previous_start = 0
    for fallback_ordinal, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise AssemblyError(f"{span.span_id} segment {fallback_ordinal} is not an object")
        if not (
            NATIVE_SEGMENT_REQUIRED_KEYS <= set(raw) <= NATIVE_SEGMENT_ALLOWED_KEYS
        ):
            raise AssemblyError(
                f"{span.span_id} native segment {fallback_ordinal} has unsupported fields"
            )
        local_ordinal = integer(
            raw.get("ordinal", fallback_ordinal),
            f"{span.span_id} segment {fallback_ordinal} ordinal",
            0,
            MAX_SEGMENTS_PER_SPAN,
        )
        if local_ordinal != fallback_ordinal:
            raise AssemblyError(f"{span.span_id} native segment ordinals are not contiguous")
        local_start_sample = integer(
            raw.get("start_sample"),
            f"{span.span_id} segment {local_ordinal} start sample",
            0,
            span.analysis_samples,
        )
        local_end_sample = integer(
            raw.get("end_sample"),
            f"{span.span_id} segment {local_ordinal} end sample",
            local_start_sample,
            span.analysis_samples,
        )
        projected_start = span.analysis_start + local_start_sample
        projected_end = span.analysis_start + local_end_sample
        if local_start_sample < previous_start:
            raise AssemblyError(
                f"{span.span_id} segment timing is inverted or non-monotonic"
            )
        previous_start = local_start_sample
        text = raw.get("text")
        if not isinstance(text, str) or len(text) > 16 * 1024 * 1024:
            raise AssemblyError(f"{span.span_id} segment {local_ordinal} text is invalid")
        if "timing_clipped" in raw and raw["timing_clipped"] is not True:
            raise AssemblyError(
                f"{span.span_id} native segment compact timing flag is invalid"
            )
        scores = exact_object(
            raw.get("scores"),
            f"{span.span_id} native segment {local_ordinal} scores",
            NATIVE_SCORE_KEYS,
        )
        score_bounds = {
            "temperature_raw": (0, 10),
            "average_log_probability_raw": (-1_000, 1_000),
            "compression_ratio_raw": (0, 1_000_000),
            "no_speech_probability_raw": (0, 1),
        }
        for score_name, (minimum, maximum) in score_bounds.items():
            finite_optional(
                scores[score_name],
                f"{span.span_id} native segment {local_ordinal} {score_name}",
                minimum,
                maximum,
            )
        candidate = SegmentCandidate(
            span=span,
            local_ordinal=local_ordinal,
            projected_start_sample=projected_start,
            projected_end_sample=projected_end,
            original_text=text,
            raw_score_provenance=(
                raw.get("score_provenance")
                if isinstance(raw.get("score_provenance"), dict)
                else raw.get("raw_scores")
                if isinstance(raw.get("raw_scores"), dict)
                else raw.get("scores")
                if isinstance(raw.get("scores"), dict)
                else None
            ),
        )
        words = raw.get("words")
        if words is None:
            words = []
        if not isinstance(words, list):
            raise AssemblyError(f"{span.span_id} segment {local_ordinal} words are invalid")
        timed_word_units: list[Unit] = []
        has_unknown_word_timing = False
        for word_ordinal, word in enumerate(words):
            if not isinstance(word, dict):
                raise AssemblyError(
                    f"{span.span_id} word {local_ordinal}.{word_ordinal} is invalid"
                )
            if not (
                NATIVE_WORD_REQUIRED_KEYS <= set(word) <= NATIVE_WORD_ALLOWED_KEYS
            ):
                raise AssemblyError(
                    f"{span.span_id} native word {local_ordinal}.{word_ordinal} "
                    "has unsupported fields"
                )
            if "timing_clipped" in word and word["timing_clipped"] is not True:
                raise AssemblyError(
                    f"{span.span_id} native word {local_ordinal}.{word_ordinal} "
                    "compact timing flag is invalid"
                )
            word_text = word.get("text")
            if not isinstance(word_text, str) or len(word_text) > 1_000_000:
                raise AssemblyError(
                    f"{span.span_id} word {local_ordinal}.{word_ordinal} text is invalid"
                )
            word_start = word.get("start_sample")
            word_end = word.get("end_sample")
            if (word_start is None) != (word_end is None):
                raise AssemblyError(
                    f"{span.span_id} word {local_ordinal}.{word_ordinal} timing is not paired"
                )
            probability = word.get("probability_raw", word.get("raw_probability"))
            probability = finite_optional(
                probability,
                f"{span.span_id} word {local_ordinal}.{word_ordinal} raw probability",
                0,
                1,
            )
            anomaly_flags = _anomaly_names(word)
            if word_start is None or word_end is None:
                has_unknown_word_timing = True
                continue
            local_word_start = integer(
                word_start,
                f"{span.span_id} word {local_ordinal}.{word_ordinal} start sample",
                0,
                span.analysis_samples,
            )
            local_word_end = integer(
                word_end,
                f"{span.span_id} word {local_ordinal}.{word_ordinal} end sample",
                local_word_start,
                span.analysis_samples,
            )
            projected_word_start = span.analysis_start + local_word_start
            projected_word_end = span.analysis_start + local_word_end
            anchor = _unit_anchor(projected_word_start, projected_word_end, total_samples)
            owner = ownership_index.owner_for_anchor(anchor)
            core_owned = owner.span_id == span.span_id
            margin = (
                min(anchor - span.core_start, span.core_end - 1 - anchor)
                if core_owned
                else -1
            )
            unit_id = stable_id(
                "lfunit",
                span.span_id,
                local_ordinal,
                word_ordinal,
                projected_word_start,
                projected_word_end,
                word_text,
            )
            timed_word_units.append(
                Unit(
                    unit_id=unit_id,
                    unit_kind="word",
                    span=span,
                    segment_ordinal=local_ordinal,
                    word_ordinal=word_ordinal,
                    text=word_text,
                    normalized_text=normalize_piece(word_text),
                    start_sample=projected_word_start,
                    end_sample=projected_word_end,
                    anchor_sample=anchor,
                    owner_span_id=owner.span_id,
                    core_owned=core_owned,
                    core_margin_samples=margin,
                    raw_probability=probability,
                    anomaly_flags=anomaly_flags,
                    retained=core_owned,
                )
            )
        # A single untimed word makes per-word ownership and exact projection
        # unknowable for the whole segment.  Keep the immutable native transcript as
        # the word-level evidence and assemble one segment-owned fallback instead of
        # fabricating exact word coordinates from the containing segment interval.
        if not has_unknown_word_timing:
            candidate.units.extend(timed_word_units)
        if not candidate.units:
            anchor = _unit_anchor(projected_start, projected_end, total_samples)
            owner = ownership_index.owner_for_anchor(anchor)
            core_owned = owner.span_id == span.span_id
            margin = (
                min(anchor - span.core_start, span.core_end - 1 - anchor)
                if core_owned
                else -1
            )
            candidate.units.append(
                Unit(
                    unit_id=stable_id(
                        "lfunit",
                        span.span_id,
                        local_ordinal,
                        "segment",
                        projected_start,
                        projected_end,
                        text,
                    ),
                    unit_kind="segment_fallback",
                    span=span,
                    segment_ordinal=local_ordinal,
                    word_ordinal=None,
                    text=text,
                    normalized_text=normalize_piece(text),
                    start_sample=projected_start,
                    end_sample=projected_end,
                    anchor_sample=anchor,
                    owner_span_id=owner.span_id,
                    core_owned=core_owned,
                    core_margin_samples=margin,
                    raw_probability=None,
                    anomaly_flags=(),
                    retained=core_owned,
                )
            )
        segments.append(candidate)
    return LoadedSpan(
        span=span,
        status="completed",
        failure_code=None,
        result_path=str(result_path),
        result_sha256=result_sha256,
        result_identity_sha256=result_identity,
        transcript_path=str(path),
        transcript_sha256=observed_sha256,
        transcript_identity_sha256=identity,
        segments=segments,
    )


class DisjointSet:
    def __init__(self, identifiers: Iterable[str]) -> None:
        self.parent = {identifier: identifier for identifier in identifiers}

    def find(self, item: str) -> str:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        self.parent[max(left_root, right_root)] = min(left_root, right_root)


def _interval_gap(left: Unit, right: Unit) -> int:
    if left.end_sample < right.start_sample:
        return right.start_sample - left.end_sample
    if right.end_sample < left.start_sample:
        return left.start_sample - right.end_sample
    return 0


def _candidate_match_score(left: Unit, right: Unit) -> tuple[int, int] | None:
    if left.unit_kind != right.unit_kind or not left.normalized_text or not right.normalized_text:
        return None
    if left.unit_kind == "word":
        if left.normalized_text != right.normalized_text:
            return None
        tolerance = WORD_MATCH_TOLERANCE_SAMPLES
        similarity = 1_000_000
    else:
        ratio = difflib.SequenceMatcher(
            None,
            left.normalized_text,
            right.normalized_text,
            autojunk=False,
        ).ratio()
        similarity = round(ratio * 1_000_000)
        if similarity < 800_000:
            return None
        tolerance = SEGMENT_MATCH_TOLERANCE_SAMPLES
    gap = _interval_gap(left, right)
    midpoint_delta = abs(left.anchor_sample - right.anchor_sample)
    if gap > tolerance and midpoint_delta > tolerance:
        return None
    return similarity, midpoint_delta


def _units(loaded: LoadedSpan) -> list[Unit]:
    return [unit for segment in loaded.segments for unit in segment.units]


def _intersects(unit: Unit, start: int, end: int) -> bool:
    if unit.start_sample == unit.end_sample:
        return start <= unit.start_sample < end
    return unit.start_sample < end and unit.end_sample > start


def _boundary_tokens(units: Sequence[Unit], start: int, end: int) -> list[str]:
    selected = sorted(
        (unit for unit in units if _intersects(unit, start, end)),
        key=lambda unit: (
            unit.start_sample,
            unit.end_sample,
            unit.segment_ordinal,
            -1 if unit.word_ordinal is None else unit.word_ordinal,
            unit.unit_id,
        ),
    )
    tokens: list[str] = []
    for unit in selected:
        tokens.extend(normalized_tokens(unit.text))
    return tokens


def _match_adjacent_units(
    left: LoadedSpan,
    right: LoadedSpan,
    shared_start: int,
    shared_end: int,
    sets: DisjointSet,
) -> list[tuple[str, str]]:
    left_units = [unit for unit in _units(left) if _intersects(unit, shared_start, shared_end)]
    right_units = [unit for unit in _units(right) if _intersects(unit, shared_start, shared_end)]
    edges: list[tuple[int, int, str, str]] = []
    by_id = {unit.unit_id: unit for unit in [*left_units, *right_units]}
    for left_unit in left_units:
        for right_unit in right_units:
            score = _candidate_match_score(left_unit, right_unit)
            if score is None:
                continue
            similarity, midpoint_delta = score
            edges.append((-similarity, midpoint_delta, left_unit.unit_id, right_unit.unit_id))
    matched_left: set[str] = set()
    matched_right: set[str] = set()
    selected: list[tuple[str, str]] = []
    for _negative_similarity, _delta, left_id, right_id in sorted(edges):
        if left_id in matched_left or right_id in matched_right:
            continue
        # The dictionary lookup also protects against accidental duplicate IDs.
        if left_id not in by_id or right_id not in by_id:
            raise AssemblyError("duplicate reconciliation referenced an unknown unit")
        matched_left.add(left_id)
        matched_right.add(right_id)
        sets.union(left_id, right_id)
        selected.append((left_id, right_id))
    return selected


def _winner_key(unit: Unit) -> tuple[int, int, int, int, str]:
    return (
        -unit.core_margin_samples,
        unit.span.ordinal,
        unit.segment_ordinal,
        -1 if unit.word_ordinal is None else unit.word_ordinal,
        unit.unit_id,
    )


def _assemble_duplicate_groups(
    loaded: Sequence[LoadedSpan],
    matches_by_boundary: dict[str, list[tuple[str, str]]],
    sets: DisjointSet,
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    unit_by_id = {unit.unit_id: unit for item in loaded for unit in _units(item)}
    grouped: dict[str, list[Unit]] = {}
    for unit in unit_by_id.values():
        grouped.setdefault(sets.find(unit.unit_id), []).append(unit)
    boundary_groups: dict[str, list[str]] = {key: [] for key in matches_by_boundary}
    output: list[dict[str, Any]] = []
    for members in grouped.values():
        if len({member.span.span_id for member in members}) < 2:
            continue
        members.sort(key=_winner_key)
        owned = [member for member in members if member.core_owned]
        winner = min(owned, key=_winner_key) if owned else None
        group_id = stable_id("lfdupe", *(sorted(member.unit_id for member in members)))
        for member in members:
            member.duplicate_group_id = group_id
            member.retained = member is winner
        boundary_ids: list[str] = []
        member_ids = {member.unit_id for member in members}
        for boundary_id, pairs in matches_by_boundary.items():
            if any(left in member_ids and right in member_ids for left, right in pairs):
                boundary_groups[boundary_id].append(group_id)
                boundary_ids.append(boundary_id)
        output.append(
            {
                "duplicate_group_id": group_id,
                "unit_kind": members[0].unit_kind,
                "normalized_text_sha256": sha256_bytes(
                    members[0].normalized_text.encode("utf-8")
                ),
                "winner_unit_id": winner.unit_id if winner else None,
                "decision": (
                    "single_core_owner"
                    if len(owned) == 1
                    else "deterministic_core_margin_tiebreak"
                    if len(owned) > 1
                    else "context_only_no_output"
                ),
                "core_owner_collision": len(owned) > 1,
                "boundary_ids": sorted(boundary_ids),
                "candidates": [
                    {
                        "unit_id": member.unit_id,
                        "span_id": member.span.span_id,
                        "source_segment_ordinal": member.segment_ordinal,
                        "source_word_ordinal": member.word_ordinal,
                        "start_sample": member.start_sample,
                        "end_sample": member.end_sample,
                        "core_owned": member.core_owned,
                        "retained": member is winner,
                    }
                    for member in sorted(
                        members,
                        key=lambda unit: (
                            unit.span.ordinal,
                            unit.segment_ordinal,
                            -1 if unit.word_ordinal is None else unit.word_ordinal,
                        ),
                    )
                ],
            }
        )
    output.sort(key=lambda row: row["duplicate_group_id"])
    for values in boundary_groups.values():
        values.sort()
    return output, boundary_groups


def _build_boundaries(
    loaded: Sequence[LoadedSpan],
    matches_by_boundary: dict[str, list[tuple[str, str]]],
    boundary_groups: dict[str, list[str]],
    duplicate_groups: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    group_by_id = {row["duplicate_group_id"]: row for row in duplicate_groups}
    boundaries: list[dict[str, Any]] = []
    for left, right in zip(loaded, loaded[1:], strict=False):
        boundary_sample = left.span.core_end
        boundary_id = stable_id(
            "lfboundary", left.span.span_id, right.span.span_id, boundary_sample
        )
        shared_start = max(left.span.analysis_start, right.span.analysis_start)
        shared_end = min(left.span.analysis_end, right.span.analysis_end)
        flags: list[str] = []
        similarity: int | None = None
        left_tokens: list[str] = []
        right_tokens: list[str] = []
        if left.status != "completed":
            flags.append("missing_left_completed_transcript")
        if right.status != "completed":
            flags.append("missing_right_completed_transcript")
        if shared_end <= shared_start:
            flags.append("no_shared_analysis_overlap")
        if not flags:
            left_tokens = _boundary_tokens(_units(left), shared_start, shared_end)
            right_tokens = _boundary_tokens(_units(right), shared_start, shared_end)
            if not left_tokens and not right_tokens:
                similarity = 1_000_000
            elif not left_tokens or not right_tokens:
                similarity = 0
                flags.append("speech_presence_disagreement")
            else:
                similarity = round(
                    difflib.SequenceMatcher(
                        None, left_tokens, right_tokens, autojunk=False
                    ).ratio()
                    * 1_000_000
                )
                if similarity < BOUNDARY_SIMILARITY_MILLIONTHS:
                    flags.append("text_disagreement")
        groups = boundary_groups.get(boundary_id, [])
        if any(group_by_id[group]["core_owner_collision"] for group in groups):
            flags.append("duplicate_core_ownership_collision")
        if any(
            flag.startswith("missing_") or flag == "no_shared_analysis_overlap"
            for flag in flags
        ):
            status_value = "unassessed"
        elif flags:
            status_value = "conflict"
        else:
            status_value = "consistent"
        boundaries.append(
            {
                "boundary_id": boundary_id,
                "at_sample": boundary_sample,
                "at_ms": presentation_ms(boundary_sample),
                "left_span_id": left.span.span_id,
                "right_span_id": right.span.span_id,
                "shared_analysis_start_sample": shared_start if shared_end > shared_start else None,
                "shared_analysis_end_sample": shared_end if shared_end > shared_start else None,
                "status": status_value,
                "conflict_flags": sorted(set(flags)),
                "quality_warning": status_value != "consistent",
                "review_recommended": status_value != "consistent",
                "comparison": {
                    "method": "normalized_token_sequence_matcher_v1",
                    "similarity_millionths": similarity,
                    "minimum_consistent_millionths": BOUNDARY_SIMILARITY_MILLIONTHS,
                    "left_token_count": len(left_tokens),
                    "right_token_count": len(right_tokens),
                    "left_token_sequence_sha256": (
                        sha256_bytes("\x1f".join(left_tokens).encode("utf-8"))
                        if left_tokens
                        else None
                    ),
                    "right_token_sequence_sha256": (
                        sha256_bytes("\x1f".join(right_tokens).encode("utf-8"))
                        if right_tokens
                        else None
                    ),
                    "matched_duplicate_pair_count": len(matches_by_boundary.get(boundary_id, [])),
                    "duplicate_group_ids": groups,
                },
            }
        )
    return boundaries


def _compact_word(unit: Unit) -> dict[str, Any]:
    row: dict[str, Any] = {
        "text": unit.text,
        "start_sample": unit.start_sample,
        "end_sample": unit.end_sample,
    }
    if unit.raw_probability is not None:
        row["raw_probability"] = unit.raw_probability
    if unit.anomaly_flags:
        row["anomaly_flags"] = list(unit.anomaly_flags)
    return row


def _source_counts(item: LoadedSpan) -> dict[str, int]:
    units = _units(item)
    return {
        "source_segment_count": len(item.segments),
        "source_unit_count": len(units),
        "core_owned_unit_count": sum(unit.core_owned for unit in units),
        "context_unit_count": sum(not unit.core_owned for unit in units),
        "retained_unit_count": sum(unit.retained for unit in units),
        "duplicate_excluded_unit_count": sum(
            unit.core_owned and not unit.retained and unit.duplicate_group_id is not None
            for unit in units
        ),
    }


def _output_segments(
    loaded: Sequence[LoadedSpan], boundaries: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    conflicts = [row for row in boundaries if row["status"] != "consistent"]
    rows: list[dict[str, Any]] = []
    for item in loaded:
        if item.status != "completed":
            continue
        for source_segment in item.segments:
            retained = [unit for unit in source_segment.units if unit.retained]
            if not retained:
                continue
            retained_by_time = sorted(
                retained,
                key=lambda unit: (
                    unit.start_sample,
                    unit.end_sample,
                    -1 if unit.word_ordinal is None else unit.word_ordinal,
                    unit.unit_id,
                )
            )
            start_sample = min(unit.start_sample for unit in retained)
            end_sample = max(unit.end_sample for unit in retained)
            if all(unit.unit_kind == "word" for unit in retained):
                retained_by_source = sorted(
                    retained,
                    key=lambda unit: (
                        -1 if unit.word_ordinal is None else unit.word_ordinal,
                        unit.unit_id,
                    ),
                )
                text = "".join(unit.text for unit in retained_by_source).strip()
                # Compact assembled words omit their source ordinal, so array order
                # must retain it even when raw model timestamps regress.  Exact times
                # and the corresponding anomaly flags remain unchanged.
                words = [_compact_word(unit) for unit in retained_by_source]
                selection_basis = "word_anchor_in_source_core_then_duplicate_reconciliation"
            else:
                text = source_segment.original_text.strip()
                words = []
                selection_basis = "segment_anchor_in_source_core_then_duplicate_reconciliation"
            conflict_ids = [
                boundary["boundary_id"]
                for boundary in conflicts
                if (
                    boundary["shared_analysis_start_sample"] is not None
                    and start_sample < boundary["shared_analysis_end_sample"]
                    and end_sample > boundary["shared_analysis_start_sample"]
                )
            ]
            segment_id = stable_id(
                "lfsegment",
                item.span.span_id,
                source_segment.local_ordinal,
                *(unit.unit_id for unit in retained_by_time),
            )
            row: dict[str, Any] = {
                "segment_id": segment_id,
                "ordinal": 0,
                "text": text,
                "start_sample": start_sample,
                "end_sample": end_sample,
                "start_ms": presentation_ms(start_sample),
                "end_ms": presentation_ms(end_sample),
                "words": words,
                "source": {
                    "span_id": item.span.span_id,
                    "source_segment_ordinal": source_segment.local_ordinal,
                },
                "ownership": {
                    "basis": selection_basis,
                    "core_start_sample": item.span.core_start,
                    "core_end_sample": item.span.core_end,
                    "source_unit_count": len(source_segment.units),
                    "retained_unit_count": len(retained),
                    "context_excluded_unit_count": sum(
                        not unit.core_owned for unit in source_segment.units
                    ),
                    "duplicate_excluded_unit_count": sum(
                        unit.core_owned and not unit.retained for unit in source_segment.units
                    ),
                },
                "duplicate_group_ids": sorted(
                    {
                        unit.duplicate_group_id
                        for unit in retained
                        if unit.duplicate_group_id is not None
                    }
                ),
                "boundary_conflict_ids": sorted(conflict_ids),
            }
            if source_segment.raw_score_provenance is not None:
                row["raw_score_provenance"] = source_segment.raw_score_provenance
            rows.append(row)
    rows.sort(
        key=lambda row: (
            row["start_sample"],
            row["end_sample"],
            row["source"]["span_id"],
            row["source"]["source_segment_ordinal"],
            row["segment_id"],
        )
    )
    for ordinal, row in enumerate(rows):
        row["ordinal"] = ordinal
    return rows


def _coverage(loaded: Sequence[LoadedSpan], total_samples: int) -> dict[str, Any]:
    intervals: list[dict[str, Any]] = []
    counts: dict[str, int] = {
        "decoded_speech": 0,
        "decoded_no_speech_detected": 0,
        "failed": 0,
        "pending": 0,
    }
    for item in loaded:
        units = _units(item)
        intersects_core = any(
            _intersects(unit, item.span.core_start, item.span.core_end) for unit in units
        )
        if item.status == "completed":
            status_value = (
                "decoded_speech" if intersects_core else "decoded_no_speech_detected"
            )
        else:
            status_value = item.status
        counts[status_value] += item.span.core_samples
        intervals.append(
            {
                "span_id": item.span.span_id,
                "start_sample": item.span.core_start,
                "end_sample": item.span.core_end,
                "start_ms": presentation_ms(item.span.core_start),
                "end_ms": presentation_ms(item.span.core_end),
                "status": status_value,
                "decoded": item.status == "completed",
                "speech_units_detected": sum(
                    _intersects(unit, item.span.core_start, item.span.core_end)
                    for unit in units
                ),
                "retained_units": sum(unit.retained for unit in units),
                "failure_code": item.failure_code,
            }
        )
    decoded_samples = counts["decoded_speech"] + counts["decoded_no_speech_detected"]
    if sum(counts.values()) != total_samples:
        raise AssemblyError("coverage accounting does not equal the parent sample count")
    return {
        "coordinate_system": "parent_pcm_samples_half_open",
        "complete": decoded_samples == total_samples,
        "decoded_samples": decoded_samples,
        "total_samples": total_samples,
        "decoded_fraction_millionths": decoded_samples * 1_000_000 // total_samples,
        "status_sample_counts": counts,
        "intervals": intervals,
    }


def assemble(
    parent_plan: dict[str, Any],
    parent_plan_path: Path,
    parent_plan_sha256: str,
    bindings: dict[str, Any],
    bindings_path: Path,
    bindings_sha256: str,
) -> dict[str, Any]:
    plan, spans = _validate_plan(parent_plan)
    bound = _validate_bindings(bindings, parent_plan_sha256, spans)
    total_samples = plan["recording"]["input"]["total_samples"]
    ownership_index = CoreOwnershipIndex.build(spans)
    loaded = [
        _load_transcript(
            span,
            bound[span.span_id],
            bindings_path.parent,
            ownership_index,
            total_samples,
            plan["recording"],
            {
                "plan_id": plan["plan_id"],
                "identity_sha256": plan["identity_sha256"],
            },
        )
        for span in spans
    ]
    all_units = [unit for item in loaded for unit in _units(item)]
    if len(all_units) > MAX_WORDS_TOTAL:
        raise AssemblyError("assembled source unit count exceeds the safety bound")
    sets = DisjointSet(unit.unit_id for unit in all_units)
    matches_by_boundary: dict[str, list[tuple[str, str]]] = {}
    for left, right in zip(loaded, loaded[1:], strict=False):
        boundary_id = stable_id(
            "lfboundary", left.span.span_id, right.span.span_id, left.span.core_end
        )
        shared_start = max(left.span.analysis_start, right.span.analysis_start)
        shared_end = min(left.span.analysis_end, right.span.analysis_end)
        if (
            left.status == "completed"
            and right.status == "completed"
            and shared_end > shared_start
        ):
            matches_by_boundary[boundary_id] = _match_adjacent_units(
                left, right, shared_start, shared_end, sets
            )
        else:
            matches_by_boundary[boundary_id] = []
    duplicate_groups, boundary_groups = _assemble_duplicate_groups(
        loaded, matches_by_boundary, sets
    )
    boundaries = _build_boundaries(
        loaded, matches_by_boundary, boundary_groups, duplicate_groups
    )
    output_segments = _output_segments(loaded, boundaries)
    coverage = _coverage(loaded, total_samples)
    sources = []
    for item in loaded:
        source: dict[str, Any] = {
            "span_id": item.span.span_id,
            "span_ordinal": item.span.ordinal,
            "status": item.status,
            "analysis_start_sample": item.span.analysis_start,
            "analysis_end_sample": item.span.analysis_end,
            "core_start_sample": item.span.core_start,
            "core_end_sample": item.span.core_end,
            "transcript": (
                {
                    "path": item.transcript_path,
                    "sha256": item.transcript_sha256,
                    "identity_sha256": item.transcript_identity_sha256,
                }
                if item.status == "completed"
                else None
            ),
            "result": (
                {
                    "path": item.result_path,
                    "sha256": item.result_sha256,
                    "identity_sha256": item.result_identity_sha256,
                }
                if item.status == "completed"
                else None
            ),
            "failure_code": item.failure_code,
            "dispositions": _source_counts(item),
        }
        sources.append(source)
    # Re-read every completed source after all reconciliation work.  The output is
    # not allowed to cite a path that changed after its first bounded read.
    for item in loaded:
        if item.status != "completed":
            continue
        for label, path_text, expected in (
            ("result", item.result_path, item.result_sha256),
            ("transcript", item.transcript_path, item.transcript_sha256),
        ):
            if path_text is None or expected is None:
                raise AssemblyError(
                    f"{item.span.span_id} completed source binding is incomplete"
                )
            if (
                sha256_bytes(
                    read_stable_file(
                        Path(path_text), f"{item.span.span_id} final {label}"
                    )
                )
                != expected
            ):
                raise AssemblyError(f"{item.span.span_id} {label} changed during assembly")
    text_value = " ".join(
        row["text"].strip() for row in output_segments if row["text"].strip()
    )
    core = {
        "kind": OUTPUT_KIND,
        "schema_version": SCHEMA_VERSION,
        "assembler_version": IMPLEMENTATION_VERSION,
        "parent_plan": {
            "path": str(parent_plan_path),
            "sha256": parent_plan_sha256,
            "plan_id": plan["plan_id"],
            "identity_sha256": plan["identity_sha256"],
        },
        "span_result_bindings": {
            "path": str(bindings_path),
            "sha256": bindings_sha256,
        },
        "recording": plan["recording"],
        "timeline": {
            "coordinate_system": "parent_pcm_samples_half_open",
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "start_sample": 0,
            "end_sample": total_samples,
            "duration_ms_presentation": presentation_ms(total_samples),
            "millisecond_projection": "nearest_integer_half_up_presentation_only",
        },
        "language": {
            "value": "en",
            "selection_basis": "inherited_forced_span_asr_contract",
            "scores_calibrated": False,
        },
        "sources": sources,
        "coverage": coverage,
        "boundaries": boundaries,
        "duplicate_groups": duplicate_groups,
        "segments": output_segments,
        "text": text_value,
        "counts": {
            "span_count": len(spans),
            "completed_span_count": sum(item.status == "completed" for item in loaded),
            "failed_span_count": sum(item.status == "failed" for item in loaded),
            "pending_span_count": sum(item.status == "pending" for item in loaded),
            "source_segment_count": sum(len(item.segments) for item in loaded),
            "source_unit_count": len(all_units),
            "retained_segment_count": len(output_segments),
            "retained_word_count": sum(len(row["words"]) for row in output_segments),
            "duplicate_group_count": len(duplicate_groups),
            "boundary_count": len(boundaries),
            "boundary_conflict_count": sum(
                row["status"] == "conflict" for row in boundaries
            ),
            "boundary_unassessed_count": sum(
                row["status"] == "unassessed" for row in boundaries
            ),
        },
        "disclaimer": {
            "code": "machine_generated_unreviewed_not_verified_quotation_v1",
            "text": "Machine-generated and unreviewed; may be wrong; not a verified quotation.",
        },
        "verified_quotation": False,
        "review_status": "unreviewed_machine_assembly",
        "policy": dict(OUTPUT_POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core, trailing_newline=True))
    return {
        **core,
        "identity_sha256": identity,
        "assembly_id": f"lfassembly_{identity[:32]}",
    }


def load_and_assemble(parent_path: Path, bindings_path: Path) -> dict[str, Any]:
    try:
        parent_resolved = parent_path.resolve(strict=True)
        bindings_resolved = bindings_path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise AssemblyError(f"input manifest path cannot be resolved: {error}") from error
    parent_body = read_stable_file(parent_resolved, "parent plan")
    bindings_body = read_stable_file(bindings_resolved, "span result bindings")
    return assemble(
        parse_json(parent_body, "parent plan"),
        parent_resolved,
        sha256_bytes(parent_body),
        parse_json(bindings_body, "span result bindings"),
        bindings_resolved,
        sha256_bytes(bindings_body),
    )


def write_new_output(path: Path, value: dict[str, Any]) -> tuple[int, str]:
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve(strict=False)
    parent = path.parent.resolve(strict=True)
    target = parent / path.name
    body = pretty_bytes(value)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(target, flags, 0o600)
    except FileExistsError as error:
        raise AssemblyError(
            "output already exists; immutable assembly is never overwritten"
        ) from error
    except OSError as error:
        raise AssemblyError(f"output cannot be created: {error}") from error
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AssemblyError("output write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    except Exception:
        try:
            os.unlink(target)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)
    return len(body), sha256_bytes(body)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Assemble sample-authoritative private long-form span transcripts"
    )
    parser.add_argument("--parent-manifest", type=Path, required=True)
    parser.add_argument("--span-results", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate and assemble in memory without creating an output file",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.validate_only == (args.output is not None):
        parser.error("choose exactly one of --validate-only or --output")
    try:
        value = load_and_assemble(args.parent_manifest, args.span_results)
        response: dict[str, Any] = {
            "status": "valid" if args.validate_only else "completed",
            "assembly_id": value["assembly_id"],
            "identity_sha256": value["identity_sha256"],
            "coverage_complete": value["coverage"]["complete"],
            "segment_count": value["counts"]["retained_segment_count"],
            "boundary_conflict_count": value["counts"]["boundary_conflict_count"],
            "boundary_unassessed_count": value["counts"]["boundary_unassessed_count"],
        }
        if args.output is not None:
            output_path = args.output
            if not output_path.is_absolute():
                output_path = (Path.cwd() / output_path).resolve(strict=False)
            byte_count, digest = write_new_output(output_path, value)
            response["output"] = {
                "path": str(output_path),
                "sha256": digest,
                "byte_count": byte_count,
            }
        sys.stdout.buffer.write(canonical_bytes(response, trailing_newline=True))
        return 0
    except AssemblyError as error:
        sys.stderr.buffer.write(
            canonical_bytes(
                {"status": "error", "error_type": type(error).__name__, "message": str(error)},
                trailing_newline=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
