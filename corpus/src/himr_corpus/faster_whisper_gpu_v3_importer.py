"""Strict private admission for sealed faster-whisper GPU v3 results.

The GPU producer has no catalog authority.  This module independently replays the
canonical JSON identities, work-order/result/artifact lineage, media-local timing,
raw-to-normalized transcript projection, and optional resident-batch completion
ancestry before admitting machine text to the existing private media-local lane.

It deliberately creates no recording/source coordinates, speaker assignments,
identity/event/claim evidence, publication decisions, or calibrated scores.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import stat
from pathlib import Path
from typing import Any

from .asr_result_importer import (
    _array,
    _exact_keys,
    _identifier,
    _integer,
    _number,
    _object,
    _sha256,
    _stable_read,
    _string,
    _timestamp,
    _timestamp_value,
    _verify_hash,
)
from .db import transaction
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .media_local_asr_bridge import (
    _insert_exact_artifact,
    _insert_exact_processing_run,
    _insert_exact_run_input,
    _insert_or_match,
)
from .result_importers import ResultImportError


IMPORTER_NAME = "private_faster_whisper_gpu_v3_result_v1"
IMPORTER_VERSION = "private-faster-whisper-gpu-v3-admission/1"
WORK_ORDER_SCHEMA_VERSION = 3
RESULT_SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
STAGE = "asr_faster_whisper_gpu"
WORK_ORDER_KIND = "himr_faster_whisper_gpu_work_order"
RESULT_KIND = "himr_faster_whisper_gpu_result"
RAW_KIND = "himr_faster_whisper_raw_transcript"
NORMALIZED_KIND = "himr_machine_transcript"
OUTPUT_CONTRACT = "private-faster-whisper-raw-media-local-word-timing-envelope-v3"
RAW_ARTIFACT_KIND = "faster_whisper_raw_transcript_json"
NORMALIZED_ARTIFACT_KIND = "transcript_normalized_json"
MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_AUDIO_BYTES = 2 * 1024 * 1024 * 1024
MAX_SEGMENTS = 100_000
MAX_WORDS = 2_000_000
MAX_TEXT_CHARACTERS = 1_000_000
MAX_TIMESTAMP_OVERRUN_MS = 10_000
RUN_ID_PREFIX = "run_asr_faster_whisper_gpu_"
WORD_TIMING_FLAGS = (
    "precedes_segment_start",
    "extends_beyond_segment_end",
    "start_regresses_from_previous",
    "overlaps_previous",
)
WORD_TIMING_NOTICE = (
    "Finite non-inverted raw model word times are preserved. Word timing can "
    "precede its segment or regress/overlap another word; these machine timing "
    "anomalies are explicit and require review rather than silent repair."
)
CONFIDENCE_NOTICE = (
    "Raw faster-whisper scores are not calibrated probabilities and must not "
    "be presented as verified confidence."
)
IDENTITY_NOTICE = "No speaker or person identity is inferred by this adapter."
POLICY = {
    "biometric_authority": "none",
    "catalogue_mutation_authority": "none",
    "existing_asr_rerun_authority": "none",
    "human_review_required": True,
    "identity_authority": "none",
    "machine_generated": True,
    "network_access": False,
    "publication_authority": "none",
    "scores_calibrated": False,
    "visibility": "private",
    "wiki_authority": "none",
}
BATCH_SAFETY = {
    "archive_authority": "none",
    "biometric_authority": "none",
    "catalogue_mutation_authority": "none",
    "deletion_authority": "none",
    "dispatch_order": "sealed_ordinal_fail_stop",
    "identity_authority": "none",
    "inference_mode": "single_resident_model_sequential_whispermodel_transcribe",
    "network_access": False,
    "neural_batching": False,
    "publication_authority": "none",
    "resume_policy": "exact_completed_v3_results_are_replayed_and_reused",
    "visibility": "private",
    "wiki_authority": "none",
}

WORK_ORDER_KEYS = {
    "catalog_context", "gpu", "identity_sha256", "implementation_version",
    "inference", "input", "job_id", "kind", "model", "output", "policy",
    "runtime", "schema_version", "work_order_id",
}
WORK_ORDER_INPUT_KEYS = {
    "artifact_id", "expected_byte_count", "expected_duration_ms",
    "expected_sha256", "media_format", "media_id", "parent_processing_run_id",
    "path", "sealed_mode", "timeline_offset_ms",
}
RESULT_KEYS = {
    "artifacts", "catalog_context", "commands", "errors", "gpu_lock",
    "hardware", "identity_sha256", "inference", "input", "job_id", "kind",
    "model", "policy", "processing_run", "recipe", "recipe_id",
    "recipe_sha256", "result_id", "result_key", "result_path", "runtime",
    "schema_version", "status", "transcript", "work_order",
}
RESULT_ARTIFACT_KEYS = {
    "artifact_id", "artifact_kind", "byte_count", "mime_type", "mode", "path",
    "processing_run_id", "sha256", "storage_uri", "visibility",
}
RAW_KEYS = {
    "document_id", "duration_after_vad_seconds_raw", "duration_seconds_raw",
    "engine", "identity_sha256", "input", "kind", "language", "policy",
    "schema_version", "score_semantics", "segments",
}
RAW_SEGMENT_KEYS = {
    "average_log_probability_raw", "compression_ratio_raw", "end_seconds",
    "engine_segment_id", "no_speech_probability_raw", "ordinal", "seek",
    "start_seconds", "temperature_raw", "text", "token_ids", "words",
}
RAW_WORD_KEYS = {
    "end_seconds", "ordinal", "probability_raw", "start_seconds", "text",
}
NORMALIZED_KEYS = {
    "confidence_notice", "document_id", "human_reviewed", "identity_notice",
    "identity_sha256", "kind", "language", "machine_generated", "policy",
    "schema_version", "scores_calibrated", "segment_count", "segments",
    "timeline", "verified_quotation", "word_count", "word_timing_anomalies",
}
NORMALIZED_SEGMENT_KEYS = {
    "calibrated_confidence", "end_ms", "ordinal", "raw_scores",
    "source_end_ms", "source_start_ms", "speaker", "start_ms", "text",
    "timing_clipped_to_input", "word_timing_anomaly_count",
    "word_timing_anomaly_flag_counts", "words",
}
NORMALIZED_WORD_KEYS = {
    "calibrated_probability", "end_ms", "ordinal", "raw_probability",
    "source_end_ms", "source_start_ms", "start_ms", "text",
    "timing_anomaly_flags", "timing_clipped_to_input",
}
BATCH_COMPLETION_KEYS = {
    "batch_id", "common_bindings", "completion_id", "execution", "hardware",
    "identity_sha256", "implementation_version", "kind", "manifest", "results",
    "safety", "schema_version", "status",
}
BATCH_RESULT_KEYS = {
    "disposition", "job_id", "ordinal", "processing_run_id", "result_byte_count",
    "result_id", "result_identity_sha256", "result_key", "result_path",
    "result_sha256",
}
BATCH_ITEM_KEYS = {
    "batch_work_order_path", "input_duration_ms", "input_sha256", "job_id",
    "ordinal", "recipe_sha256", "result_key", "result_path", "source_work_order",
    "work_order_byte_count", "work_order_id", "work_order_identity_sha256",
    "work_order_sha256",
}


def _duplicate_key_guard(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ResultImportError(f"GPU v3 JSON contains duplicate key {key!r}")
        value[key] = item
    return value


def _canonical_bytes(value: object) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def _sealed_path(path: Path, label: str, *, modes: set[int]) -> Path:
    try:
        resolved = path.resolve(strict=True)
        observed = path.lstat()
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if path != resolved:
        raise ResultImportError(f"{label} must be a resolved non-symlink path")
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) not in modes
        or observed.st_nlink != 1
        or observed.st_uid != os.getuid()
    ):
        expected = "/".join(f"{mode:04o}" for mode in sorted(modes))
        raise ResultImportError(
            f"{label} must be an owner-controlled, single-link file mode {expected}"
        )
    return resolved


def _sealed_directory(path: Path, label: str, expected_names: set[str]) -> Path:
    try:
        resolved = path.resolve(strict=True)
        observed = path.lstat()
        names = {child.name for child in path.iterdir()}
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if (
        path != resolved
        or stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o500
        or observed.st_uid != os.getuid()
        or names != expected_names
    ):
        raise ResultImportError(f"{label} is not an exact sealed mode-0500 directory")
    return resolved


def _read_canonical_json(
    path: Path, label: str, *, maximum: int, modes: set[int] = {0o400}
) -> tuple[dict[str, Any], bytes, str]:
    path = _sealed_path(path.resolve(strict=True), label, modes=modes)
    body = _stable_read(path, label, maximum_bytes=maximum)
    try:
        parsed = json.loads(body.decode("utf-8"), object_pairs_hook=_duplicate_key_guard)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"{label} is not canonical UTF-8 JSON: {error}") from error
    value = _object(parsed, label)
    if body != _canonical_bytes(value):
        raise ResultImportError(f"{label} must use the producer's canonical JSON encoding")
    return value, body, sha256_bytes(body)


def _identity_document(
    value: dict[str, Any], label: str, *, identifier_key: str, prefix: str
) -> str:
    identity = _sha256(value.get("identity_sha256"), f"{label}.identity_sha256")
    identifier = _identifier(value.get(identifier_key), f"{label}.{identifier_key}")
    core = {
        key: item
        for key, item in value.items()
        if key not in {"identity_sha256", identifier_key}
    }
    expected = sha256_bytes(_canonical_bytes(core))
    if identity != expected or identifier != f"{prefix}_{expected[:32]}":
        raise ResultImportError(f"{label} immutable identity failed replay")
    return identity


def _policy(value: object, label: str) -> None:
    if value != POLICY:
        raise ResultImportError(f"{label} must retain exact private/no-authority policy")


def _read_work_order(path: Path) -> dict[str, Any]:
    value, body, raw_sha = _read_canonical_json(
        path.resolve(strict=True), "GPU v3 work order", maximum=MAX_RESULT_BYTES
    )
    _exact_keys(value, "GPU v3 work order", WORK_ORDER_KEYS)
    identity = _identity_document(
        value, "GPU v3 work order", identifier_key="work_order_id", prefix="gpuasrwo"
    )
    if (
        value["kind"] != WORK_ORDER_KIND
        or value["schema_version"] != WORK_ORDER_SCHEMA_VERSION
        or value["implementation_version"] != IMPLEMENTATION_VERSION
        or value["catalog_context"] is not None
    ):
        raise ResultImportError("GPU v3 work order header or media-local context is invalid")
    _policy(value["policy"], "GPU v3 work order.policy")
    _identifier(value["job_id"], "GPU v3 work order.job_id")
    input_item = _object(value["input"], "GPU v3 work order.input")
    _exact_keys(input_item, "GPU v3 work order.input", WORK_ORDER_INPUT_KEYS)
    input_sha = _sha256(input_item["expected_sha256"], "GPU v3 input digest")
    input_bytes = _integer(
        input_item["expected_byte_count"], "GPU v3 input byte count", minimum=1,
        maximum=MAX_AUDIO_BYTES,
    )
    duration = _integer(
        input_item["expected_duration_ms"], "GPU v3 input duration", minimum=1
    )
    if (
        input_item["timeline_offset_ms"] != 0
        or input_item["media_id"] != f"media_sha256_{input_sha}"
        or input_item["sealed_mode"] not in {"0400", "0444"}
    ):
        raise ResultImportError("GPU v3 input is not sealed media-local audio")
    input_path = Path(_string(input_item["path"], "GPU v3 input.path"))
    input_path = _sealed_path(
        input_path, "GPU v3 input", modes={int(input_item["sealed_mode"], 8)}
    )
    _verify_hash(input_path, input_sha, input_bytes, "GPU v3 input")
    inference = _object(value["inference"], "GPU v3 work order.inference")
    for key in ("max_result_bytes", "max_segments", "max_words"):
        _integer(inference.get(key), f"GPU v3 inference.{key}", minimum=1)
    if (
        inference.get("word_timestamps") is not True
        or inference.get("condition_on_previous_text") is not False
        or inference.get("vad_filter") is not False
        or inference["max_result_bytes"] > MAX_RESULT_BYTES
        or inference["max_segments"] > MAX_SEGMENTS
        or inference["max_words"] > MAX_WORDS
    ):
        raise ResultImportError("GPU v3 inference policy is outside the admitted bounds")
    return {
        "value": value,
        "body": body,
        "raw_sha256": raw_sha,
        "identity_sha256": identity,
        "path": path.resolve(strict=True),
        "input_path": input_path,
        "input_sha256": input_sha,
        "input_byte_count": input_bytes,
        "input_duration_ms": duration,
    }


def _timestamp_ms(value: object, label: str, duration_ms: int) -> tuple[int, bool]:
    seconds = _number(
        value,
        label,
        minimum=0,
        maximum=(duration_ms + MAX_TIMESTAMP_OVERRUN_MS) / 1_000,
    )
    milliseconds = round(seconds * 1_000)
    if milliseconds > duration_ms + MAX_TIMESTAMP_OVERRUN_MS:
        raise ResultImportError(f"{label} exceeds the admitted media duration")
    return min(milliseconds, duration_ms), milliseconds > duration_ms


def _nullable_number(
    value: object, label: str, *, minimum: float, maximum: float
) -> float | None:
    return None if value is None else _number(value, label, minimum=minimum, maximum=maximum)


def _validate_raw_header(raw: dict[str, Any], work_order: dict[str, Any]) -> None:
    _exact_keys(raw, "GPU v3 raw transcript", RAW_KEYS)
    _identity_document(
        raw, "GPU v3 raw transcript", identifier_key="document_id", prefix="gpuasrraw"
    )
    if (
        raw["kind"] != RAW_KIND
        or raw["schema_version"] != 1
        or raw["score_semantics"] != "raw_model_outputs_uncalibrated"
    ):
        raise ResultImportError("GPU v3 raw transcript header is invalid")
    _policy(raw["policy"], "GPU v3 raw transcript.policy")
    raw_input = _object(raw["input"], "GPU v3 raw transcript.input")
    if raw_input != {
        "sha256": work_order["input"]["expected_sha256"],
        "duration_ms": work_order["input"]["expected_duration_ms"],
        "timeline_offset_ms": 0,
    }:
        raise ResultImportError("GPU v3 raw transcript input lineage failed replay")
    engine = _object(raw["engine"], "GPU v3 raw transcript.engine")
    if (
        engine.get("library") != "faster-whisper"
        or engine.get("library_version")
        != _object(work_order["runtime"], "GPU v3 work order.runtime")
        .get("packages", {})
        .get("faster-whisper")
        or engine.get("model_identity_sha256")
        != _object(work_order["model"], "GPU v3 work order.model").get("identity_sha256")
        or engine.get("model_revision") != work_order["model"].get("revision")
    ):
        raise ResultImportError("GPU v3 raw transcript engine lineage failed replay")


def _validate_transcripts(
    raw: dict[str, Any], normalized: dict[str, Any], work_order: dict[str, Any]
) -> dict[str, Any]:
    _validate_raw_header(raw, work_order)
    _exact_keys(normalized, "GPU v3 normalized transcript", NORMALIZED_KEYS)
    normalized_identity = _identity_document(
        normalized,
        "GPU v3 normalized transcript",
        identifier_key="document_id",
        prefix="gpuasrnorm",
    )
    _policy(normalized["policy"], "GPU v3 normalized transcript.policy")
    duration = work_order["input"]["expected_duration_ms"]
    raw_language = _object(raw["language"], "GPU v3 raw transcript.language")
    language_value = _string(raw_language.get("value"), "GPU v3 raw language", maximum=64)
    language_probability = _number(
        raw_language.get("probability_raw"), "GPU v3 raw language probability",
        minimum=0, maximum=1,
    )
    expected_header = {
        "kind": NORMALIZED_KIND,
        "schema_version": 1,
        "machine_generated": True,
        "human_reviewed": False,
        "verified_quotation": False,
        "scores_calibrated": False,
        "language": {
            "value": language_value,
            "raw_probability": language_probability,
            "calibrated_probability": None,
        },
        "timeline": {
            "coordinate_system": "media_ms",
            "source_duration_ms": duration,
            "source_offset_ms": 0,
            "end_ms": duration,
        },
        "confidence_notice": CONFIDENCE_NOTICE,
        "identity_notice": IDENTITY_NOTICE,
        "policy": POLICY,
    }
    for key, expected in expected_header.items():
        if normalized.get(key) != expected:
            raise ResultImportError(f"GPU v3 normalized transcript.{key} failed replay")

    raw_segments = _array(raw["segments"], "GPU v3 raw transcript.segments")
    normalized_segments = _array(
        normalized["segments"], "GPU v3 normalized transcript.segments"
    )
    if len(raw_segments) > work_order["inference"]["max_segments"]:
        raise ResultImportError("GPU v3 transcript has an unsupported segment count")
    if len(normalized_segments) != len(raw_segments):
        raise ResultImportError("GPU v3 normalized segment count differs from raw")

    segment_rows: list[dict[str, Any]] = []
    word_rows: list[dict[str, Any]] = []
    total_flag_counts = {name: 0 for name in WORD_TIMING_FLAGS}
    anomalous_word_count = 0
    word_count = 0
    previous_raw_segment_start = 0.0
    previous_local_segment_start = 0

    for segment_ordinal, (raw_item, normalized_item) in enumerate(
        zip(raw_segments, normalized_segments, strict=True)
    ):
        raw_segment = _object(raw_item, f"GPU v3 raw segment[{segment_ordinal}]")
        segment = _object(
            normalized_item, f"GPU v3 normalized segment[{segment_ordinal}]"
        )
        _exact_keys(raw_segment, f"GPU v3 raw segment[{segment_ordinal}]", RAW_SEGMENT_KEYS)
        _exact_keys(
            segment,
            f"GPU v3 normalized segment[{segment_ordinal}]",
            NORMALIZED_SEGMENT_KEYS,
        )
        if raw_segment["ordinal"] != segment_ordinal:
            raise ResultImportError("GPU v3 raw segment ordinals must be contiguous")
        raw_start = _number(
            raw_segment["start_seconds"], f"raw segment[{segment_ordinal}].start_seconds",
            minimum=0, maximum=(duration + MAX_TIMESTAMP_OVERRUN_MS) / 1_000,
        )
        raw_end = _number(
            raw_segment["end_seconds"], f"raw segment[{segment_ordinal}].end_seconds",
            minimum=0, maximum=(duration + MAX_TIMESTAMP_OVERRUN_MS) / 1_000,
        )
        start_ms, start_clipped = _timestamp_ms(
            raw_start, f"raw segment[{segment_ordinal}].start_seconds", duration
        )
        end_ms, end_clipped = _timestamp_ms(
            raw_end, f"raw segment[{segment_ordinal}].end_seconds", duration
        )
        if end_ms == start_ms:
            # The sealed producer permits this, but the existing media-local
            # half-open interval schema requires end > start. Refuse rather than
            # manufacture duration and falsify producer timing.
            raise ResultImportError(
                "GPU v3 zero-duration segment cannot be represented by the "
                "existing half-open media-local interval schema"
            )
        if (
            raw_end < raw_start
            or raw_start < previous_raw_segment_start
            or end_ms < start_ms
            or start_ms < previous_local_segment_start
        ):
            raise ResultImportError("GPU v3 segment timing is inverted or non-monotonic")
        previous_raw_segment_start = raw_start
        previous_local_segment_start = start_ms
        text = _string(
            raw_segment["text"], f"raw segment[{segment_ordinal}].text",
            allow_empty=True, maximum=MAX_TEXT_CHARACTERS,
        )
        raw_words = _array(raw_segment["words"], f"raw segment[{segment_ordinal}].words")
        normalized_words = _array(segment["words"], f"normalized segment[{segment_ordinal}].words")
        if len(raw_words) != len(normalized_words):
            raise ResultImportError("GPU v3 normalized word count differs from raw")

        segment_flag_counts = {name: 0 for name in WORD_TIMING_FLAGS}
        segment_anomalous_words = 0
        previous_raw_word_start: float | None = None
        previous_raw_word_end: float | None = None
        expected_words: list[dict[str, Any]] = []
        compact_words: list[dict[str, Any]] = []
        for word_ordinal, (raw_word_item, normalized_word_item) in enumerate(
            zip(raw_words, normalized_words, strict=True)
        ):
            raw_word = _object(
                raw_word_item, f"GPU v3 raw word[{segment_ordinal}:{word_ordinal}]"
            )
            normalized_word = _object(
                normalized_word_item,
                f"GPU v3 normalized word[{segment_ordinal}:{word_ordinal}]",
            )
            _exact_keys(raw_word, "GPU v3 raw word", RAW_WORD_KEYS)
            _exact_keys(normalized_word, "GPU v3 normalized word", NORMALIZED_WORD_KEYS)
            if raw_word["ordinal"] != word_ordinal:
                raise ResultImportError("GPU v3 raw word ordinals must be contiguous")
            word_text = _string(
                raw_word["text"], "GPU v3 raw word.text", allow_empty=True,
                maximum=MAX_TEXT_CHARACTERS,
            )
            raw_probability = _nullable_number(
                raw_word["probability_raw"], "GPU v3 raw word probability",
                minimum=0, maximum=1,
            )
            raw_word_start = raw_word["start_seconds"]
            raw_word_end = raw_word["end_seconds"]
            flags = {name: False for name in WORD_TIMING_FLAGS}
            if raw_word_start is None or raw_word_end is None:
                word_start_ms = None
                word_end_ms = None
                word_clipped = False
            else:
                raw_word_start = _number(
                    raw_word_start, "GPU v3 raw word.start_seconds", minimum=0,
                    maximum=(duration + MAX_TIMESTAMP_OVERRUN_MS) / 1_000,
                )
                raw_word_end = _number(
                    raw_word_end, "GPU v3 raw word.end_seconds", minimum=0,
                    maximum=(duration + MAX_TIMESTAMP_OVERRUN_MS) / 1_000,
                )
                word_start_ms, word_start_clipped = _timestamp_ms(
                    raw_word_start, "GPU v3 raw word.start_seconds", duration
                )
                word_end_ms, word_end_clipped = _timestamp_ms(
                    raw_word_end, "GPU v3 raw word.end_seconds", duration
                )
                if raw_word_end < raw_word_start or word_end_ms < word_start_ms:
                    raise ResultImportError("GPU v3 raw word timing is inverted")
                word_clipped = word_start_clipped or word_end_clipped
                flags = {
                    "precedes_segment_start": raw_word_start < raw_start,
                    "extends_beyond_segment_end": raw_word_end > raw_end,
                    "start_regresses_from_previous": (
                        previous_raw_word_start is not None
                        and raw_word_start < previous_raw_word_start
                    ),
                    "overlaps_previous": (
                        previous_raw_word_end is not None
                        and raw_word_start < previous_raw_word_end
                    ),
                }
                previous_raw_word_start = raw_word_start
                previous_raw_word_end = raw_word_end
            expected_word = {
                "ordinal": word_ordinal,
                "start_ms": word_start_ms,
                "end_ms": word_end_ms,
                "source_start_ms": word_start_ms,
                "source_end_ms": word_end_ms,
                "text": word_text,
                "raw_probability": raw_probability,
                "calibrated_probability": None,
                "timing_clipped_to_input": word_clipped,
                "timing_anomaly_flags": flags,
            }
            if normalized_word != expected_word:
                raise ResultImportError(
                    f"GPU v3 normalized word[{segment_ordinal}:{word_ordinal}] failed raw replay"
                )
            if any(flags.values()):
                anomalous_word_count += 1
                segment_anomalous_words += 1
            for name, present in flags.items():
                if present:
                    total_flag_counts[name] += 1
                    segment_flag_counts[name] += 1
            expected_words.append(expected_word)
            compact_words.append(
                {
                    "ordinal": word_ordinal,
                    "start_ms": word_start_ms,
                    "end_ms": word_end_ms,
                    "token": word_text,
                    "raw_probability": raw_probability,
                    "timing_clipped_to_input": word_clipped,
                    "timing_anomaly_flags": flags,
                }
            )
            word_count += 1
            if word_count > work_order["inference"]["max_words"]:
                raise ResultImportError("GPU v3 transcript exceeds its word bound")

        raw_scores = {
            "temperature": _nullable_number(
                raw_segment["temperature_raw"], "GPU v3 segment temperature",
                minimum=-1_000, maximum=1_000_000,
            ),
            "average_log_probability": _nullable_number(
                raw_segment["average_log_probability_raw"],
                "GPU v3 segment average log probability",
                minimum=-1_000, maximum=1_000_000,
            ),
            "compression_ratio": _nullable_number(
                raw_segment["compression_ratio_raw"], "GPU v3 segment compression ratio",
                minimum=-1_000, maximum=1_000_000,
            ),
            "no_speech_probability": _nullable_number(
                raw_segment["no_speech_probability_raw"],
                "GPU v3 segment no-speech probability",
                minimum=-1_000, maximum=1_000_000,
            ),
        }
        expected_segment = {
            "ordinal": segment_ordinal,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "source_start_ms": start_ms,
            "source_end_ms": end_ms,
            "text": text,
            "words": expected_words,
            "raw_scores": raw_scores,
            "calibrated_confidence": None,
            "timing_clipped_to_input": start_clipped or end_clipped,
            "speaker": None,
            "word_timing_anomaly_count": segment_anomalous_words,
            "word_timing_anomaly_flag_counts": segment_flag_counts,
        }
        if segment != expected_segment:
            raise ResultImportError(
                f"GPU v3 normalized segment[{segment_ordinal}] failed raw replay"
            )
        segment_rows.append(
            {
                "ordinal": segment_ordinal,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "text": text,
                "raw_scores": raw_scores,
                "timing_clipped_to_input": start_clipped or end_clipped,
                "word_timing_anomaly_count": segment_anomalous_words,
                "word_timing_anomaly_flag_counts": segment_flag_counts,
                "words": compact_words,
            }
        )

    expected_summary = {
        "anomalous_word_count": anomalous_word_count,
        "total_flag_count": sum(total_flag_counts.values()),
        "flag_counts": total_flag_counts,
        "notice": WORD_TIMING_NOTICE,
    }
    if (
        normalized["segment_count"] != len(segment_rows)
        or normalized["word_count"] != word_count
        or normalized["word_timing_anomalies"] != expected_summary
    ):
        raise ResultImportError("GPU v3 normalized transcript summary failed replay")
    return {
        "language": language_value,
        "normalized_identity_sha256": normalized_identity,
        "segments": segment_rows,
        "segment_count": len(segment_rows),
        "word_count": word_count,
        "anomalous_word_count": anomalous_word_count,
        "timing_anomaly_flag_count": sum(total_flag_counts.values()),
        "timing_anomaly_flag_counts": total_flag_counts,
    }


def _require_catalog_input(
    connection: sqlite3.Connection, work_order: dict[str, Any], input_path: Path
) -> None:
    item = work_order["input"]
    media = connection.execute(
        "SELECT * FROM media_objects WHERE media_id = ?", (item["media_id"],)
    ).fetchone()
    if media is None or (
        media["sha256"] != item["expected_sha256"]
        or media["byte_count"] != item["expected_byte_count"]
        or media["duration_ms"] != item["expected_duration_ms"]
        or media["media_kind"] != "audio"
        or media["integrity_state"] != "verified"
    ):
        raise ResultImportError("GPU v3 input media is not exactly admitted in the catalog")
    artifact = connection.execute(
        "SELECT * FROM artifacts WHERE artifact_id = ?", (item["artifact_id"],)
    ).fetchone()
    if artifact is None or (
        artifact["processing_run_id"] != item["parent_processing_run_id"]
        or artifact["storage_uri"] != input_path.as_uri()
        or artifact["sha256"] != item["expected_sha256"]
        or artifact["byte_count"] != item["expected_byte_count"]
        or artifact["visibility"] != "private"
    ):
        raise ResultImportError("GPU v3 input artifact lineage is not exact")


def _validate_result_bundle(
    connection: sqlite3.Connection, result_path: Path, work: dict[str, Any]
) -> dict[str, Any]:
    result_path = result_path.resolve(strict=True)
    if result_path.name != "result.json":
        raise ResultImportError("GPU v3 result path must end in result.json")
    result_dir = _sealed_directory(
        result_path.parent,
        "GPU v3 result directory",
        {"result.json", "transcript.raw.json", "transcript.normalized.json"},
    )
    result, result_body, result_raw_sha = _read_canonical_json(
        result_path, "GPU v3 result", maximum=MAX_RESULT_BYTES
    )
    raw_path = result_dir / "transcript.raw.json"
    normalized_path = result_dir / "transcript.normalized.json"
    raw, raw_body, raw_sha = _read_canonical_json(
        raw_path, "GPU v3 raw transcript", maximum=MAX_RESULT_BYTES
    )
    normalized, normalized_body, normalized_sha = _read_canonical_json(
        normalized_path, "GPU v3 normalized transcript", maximum=MAX_RESULT_BYTES
    )

    _exact_keys(result, "GPU v3 result", RESULT_KEYS)
    result_identity = _identity_document(
        result, "GPU v3 result", identifier_key="result_id", prefix="gpuasrresult"
    )
    if (
        result["kind"] != RESULT_KIND
        or result["schema_version"] != RESULT_SCHEMA_VERSION
        or result["status"] != "completed"
        or result["catalog_context"] is not None
        or result["errors"] != []
        or result["job_id"] != work["value"]["job_id"]
        or result["result_path"] != str(result_path)
    ):
        raise ResultImportError("GPU v3 result header or media-local lineage is invalid")
    _policy(result["policy"], "GPU v3 result.policy")
    result_key = _sha256(result["result_key"], "GPU v3 result.result_key")
    expected_result_path = (
        Path(work["value"]["output"]["root"])
        / "asr" / "faster-whisper-gpu" / "sha256"
        / work["input_sha256"][:2] / work["input_sha256"]
        / "results" / result_key / "result.json"
    )
    if result_path != expected_result_path:
        raise ResultImportError("GPU v3 result path escaped its deterministic result plan")
    recipe = _object(result["recipe"], "GPU v3 result.recipe")
    recipe_sha = sha256_bytes(_canonical_bytes(recipe))
    if (
        result["recipe_sha256"] != recipe_sha
        or result["recipe_id"] != f"recipe_gpu_asr_{recipe_sha[:32]}"
        or recipe.get("contract_version") != WORK_ORDER_SCHEMA_VERSION
        or recipe.get("implementation_version") != IMPLEMENTATION_VERSION
        or recipe.get("stage") != STAGE
        or recipe.get("output_contract") != OUTPUT_CONTRACT
        or recipe.get("timeline_offset_ms") != 0
        or recipe.get("confidence_contract") != "raw-model-scores-uncalibrated-v1"
    ):
        raise ResultImportError("GPU v3 result recipe failed replay")
    work_reference = _object(result["work_order"], "GPU v3 result.work_order")
    _exact_keys(
        work_reference,
        "GPU v3 result.work_order",
        {"byte_count", "identity_sha256", "path", "sha256", "work_order_id"},
    )
    if work_reference != {
        "byte_count": len(work["body"]),
        "identity_sha256": work["identity_sha256"],
        "path": str(work["path"]),
        "sha256": work["raw_sha256"],
        "work_order_id": work["value"]["work_order_id"],
    }:
        raise ResultImportError("GPU v3 result work-order reference failed replay")

    run = _object(result["processing_run"], "GPU v3 result.processing_run")
    _exact_keys(
        run,
        "GPU v3 result.processing_run",
        {
            "completed_at", "duration_ms", "implementation_version",
            "processing_run_id", "random_seed", "stage", "started_at", "status",
        },
    )
    run_id = _identifier(run["processing_run_id"], "GPU v3 processing run ID")
    started_at = _timestamp(run["started_at"], "GPU v3 processing run start")
    completed_at = _timestamp(run["completed_at"], "GPU v3 processing run completion")
    if (
        not run_id.startswith(RUN_ID_PREFIX)
        or len(run_id) != len(RUN_ID_PREFIX) + 32
        or run["stage"] != STAGE
        or run["implementation_version"] != IMPLEMENTATION_VERSION
        or run["status"] != "completed"
        or run["random_seed"] is not None
        or completed_at < started_at
    ):
        raise ResultImportError("GPU v3 processing run header is invalid")

    result_input = _object(result["input"], "GPU v3 result.input")
    _exact_keys(
        result_input,
        "GPU v3 result.input",
        {"byte_count", "device", "inode", "link_count", "mode", "path", "probe", "sha256"},
    )
    observed_input = work["input_path"].stat()
    _integer(result_input["device"], "GPU v3 observed input device", minimum=0)
    _integer(result_input["inode"], "GPU v3 observed input inode", minimum=0)
    observed_link_count = _integer(
        result_input["link_count"], "GPU v3 observed input link count", minimum=1,
        maximum=1,
    )
    observed_mode = _integer(
        result_input["mode"], "GPU v3 observed input mode", minimum=0,
        maximum=0o7777,
    )
    expected_input_mode = int(work["value"]["input"]["sealed_mode"], 8)
    if (
        result_input["path"] != str(work["input_path"])
        or result_input["sha256"] != work["input_sha256"]
        or result_input["byte_count"] != work["input_byte_count"]
        or observed_link_count != 1
        or observed_mode != expected_input_mode
        or observed_input.st_nlink != 1
        or stat.S_IMODE(observed_input.st_mode) != expected_input_mode
        or _object(result_input["probe"], "GPU v3 result.input.probe").get("duration_ms")
        != work["input_duration_ms"]
    ):
        raise ResultImportError("GPU v3 result input observation failed current replay")
    _require_catalog_input(connection, work["value"], work["input_path"])

    artifact_rows: dict[str, dict[str, Any]] = {}
    artifacts = _array(result["artifacts"], "GPU v3 result.artifacts", length=2)
    for item in artifacts:
        artifact = _object(item, "GPU v3 result artifact")
        _exact_keys(artifact, "GPU v3 result artifact", RESULT_ARTIFACT_KEYS)
        kind = artifact.get("artifact_kind")
        if kind in artifact_rows or kind not in {RAW_ARTIFACT_KIND, NORMALIZED_ARTIFACT_KIND}:
            raise ResultImportError("GPU v3 result artifacts are duplicated or unsupported")
        expected_path, expected_body, expected_sha = (
            (raw_path, raw_body, raw_sha)
            if kind == RAW_ARTIFACT_KIND
            else (normalized_path, normalized_body, normalized_sha)
        )
        if (
            artifact["path"] != str(expected_path)
            or artifact["storage_uri"] != expected_path.as_uri()
            or artifact["sha256"] != expected_sha
            or artifact["byte_count"] != len(expected_body)
            or artifact["processing_run_id"] != run_id
            or artifact["mime_type"] != "application/json"
            or artifact["mode"] != "0400"
            or artifact["visibility"] != "private"
        ):
            raise ResultImportError("GPU v3 result artifact failed physical replay")
        artifact_rows[kind] = artifact

    transcript_data = _validate_transcripts(raw, normalized, work["value"])
    raw_identity = _sha256(raw["identity_sha256"], "GPU v3 raw identity")
    transcript_summary = _object(result["transcript"], "GPU v3 result.transcript")
    if transcript_summary != {
        "raw_identity_sha256": raw_identity,
        "normalized_identity_sha256": transcript_data["normalized_identity_sha256"],
        "language": normalized["language"],
        "segment_count": transcript_data["segment_count"],
        "word_count": transcript_data["word_count"],
        "scores_calibrated": False,
        "human_reviewed": False,
    }:
        raise ResultImportError("GPU v3 result transcript summary failed replay")
    result_inference = _object(result["inference"], "GPU v3 result.inference")
    _exact_keys(
        result_inference,
        "GPU v3 result.inference",
        {
            "compute_type", "device", "device_index", "library",
            "model_local_files_only", "parameters", "task",
        },
    )
    gpu = _object(work["value"]["gpu"], "GPU v3 work order.gpu")
    if (
        result_inference["library"] != "faster-whisper"
        or result_inference["device"] != "cuda"
        or result_inference["device_index"] != gpu.get("device_index")
        or result_inference["compute_type"] != gpu.get("compute_type")
        or result_inference["parameters"] != work["value"]["inference"]
        or result_inference["model_local_files_only"] is not True
        or result_inference["task"] != "transcribe"
    ):
        raise ResultImportError("GPU v3 result inference failed work-order replay")
    result_model = _object(result["model"], "GPU v3 result.model")
    work_model = _object(work["value"]["model"], "GPU v3 work order.model")
    hardware = _object(result["hardware"], "GPU v3 result.hardware")
    if (
        result_model.get("identity_sha256") != work_model.get("identity_sha256")
        or result_model.get("revision") != work_model.get("revision")
        or hardware.get("uuid") != gpu.get("expected_uuid")
    ):
        raise ResultImportError("GPU v3 model or physical GPU lineage failed replay")
    return {
        "value": result,
        "body": result_body,
        "raw_sha256": result_raw_sha,
        "identity_sha256": result_identity,
        "path": result_path,
        "run": {**run, "started_at": started_at, "completed_at": completed_at},
        "raw": raw,
        "normalized": normalized,
        "raw_artifact": artifact_rows[RAW_ARTIFACT_KIND],
        "normalized_artifact": artifact_rows[NORMALIZED_ARTIFACT_KIND],
        "transcript": transcript_data,
    }


def _batch_lineage(
    completion_path: Path | None,
    *,
    result: dict[str, Any],
    work: dict[str, Any],
) -> dict[str, Any]:
    if completion_path is None:
        return {
            # The ordinary v3 result envelope is intentionally identical for
            # standalone and resident-batch execution. Without the completion
            # receipt there is no evidence from which to assert either origin.
            "execution_mode": "unasserted",
            "batch_completion_uri": None,
            "batch_completion_raw_sha256": None,
            "batch_completion_identity_sha256": None,
            "batch_completion_id": None,
            "gpu_batch_id": None,
            "gpu_batch_ordinal": None,
            "manifest": None,
        }
    completion_path = completion_path.resolve(strict=True)
    _sealed_directory(
        completion_path.parent, "GPU v3 batch completion directory", {"receipt.json"}
    )
    completion, body, raw_sha = _read_canonical_json(
        completion_path, "GPU v3 batch completion", maximum=MAX_RESULT_BYTES
    )
    _exact_keys(completion, "GPU v3 batch completion", BATCH_COMPLETION_KEYS)
    identity = _identity_document(
        completion,
        "GPU v3 batch completion",
        identifier_key="completion_id",
        prefix="gpuasrbatchdone",
    )
    if (
        completion["kind"] != "himr_faster_whisper_gpu_batch_completion"
        or completion["schema_version"] != 1
        or completion["status"] != "completed"
        or completion["safety"] != BATCH_SAFETY
    ):
        raise ResultImportError("GPU v3 batch completion header or safety policy is invalid")
    batch_id = _identifier(completion["batch_id"], "GPU v3 batch ID")
    manifest_reference = _object(
        completion["manifest"], "GPU v3 batch completion.manifest"
    )
    _exact_keys(
        manifest_reference,
        "GPU v3 batch completion.manifest",
        {"byte_count", "identity_sha256", "path", "sha256"},
    )
    manifest_path = Path(_string(manifest_reference["path"], "GPU v3 batch manifest path"))
    manifest, manifest_body, manifest_raw_sha = _read_canonical_json(
        manifest_path.resolve(strict=True), "GPU v3 batch manifest", maximum=MAX_RESULT_BYTES
    )
    manifest_core = {
        key: item
        for key, item in manifest.items()
        if key not in {"identity_sha256", "batch_id", "batch_relative_path"}
    }
    manifest_identity = sha256_bytes(_canonical_bytes(manifest_core))
    if (
        manifest_reference["byte_count"] != len(manifest_body)
        or manifest_reference["sha256"] != manifest_raw_sha
        or manifest_reference["identity_sha256"] != manifest_identity
        or manifest.get("identity_sha256") != manifest_identity
        or manifest.get("batch_id") != batch_id
        or manifest.get("kind") != "himr_faster_whisper_gpu_batch_manifest"
        or manifest.get("schema_version") != 1
        or manifest.get("safety") != BATCH_SAFETY
    ):
        raise ResultImportError("GPU v3 batch manifest ancestry failed replay")
    items = _array(manifest.get("items"), "GPU v3 batch manifest.items")
    matching_items = []
    for item_value in items:
        item = _object(item_value, "GPU v3 batch item")
        _exact_keys(item, "GPU v3 batch item", BATCH_ITEM_KEYS)
        if item["result_key"] == result["value"]["result_key"]:
            matching_items.append(item)
    if len(matching_items) != 1:
        raise ResultImportError("GPU v3 result does not have one exact batch item")
    item = matching_items[0]
    expected_batch_work_order = manifest_path.parent / item["batch_work_order_path"]
    if (
        expected_batch_work_order != work["path"]
        or item["work_order_sha256"] != work["raw_sha256"]
        or item["work_order_byte_count"] != len(work["body"])
        or item["work_order_identity_sha256"] != work["identity_sha256"]
        or item["work_order_id"] != work["value"]["work_order_id"]
        or item["job_id"] != result["value"]["job_id"]
        or item["result_path"] != str(result["path"])
        or item["input_sha256"] != work["input_sha256"]
        or item["input_duration_ms"] != work["input_duration_ms"]
        or item["recipe_sha256"] != result["value"]["recipe_sha256"]
    ):
        raise ResultImportError("GPU v3 batch item failed work-order/result replay")
    completion_results = _array(
        completion["results"], "GPU v3 batch completion.results"
    )
    matching_results = []
    for member_value in completion_results:
        member = _object(member_value, "GPU v3 batch completion result")
        _exact_keys(member, "GPU v3 batch completion result", BATCH_RESULT_KEYS)
        if member["result_key"] == result["value"]["result_key"]:
            matching_results.append(member)
    if len(matching_results) != 1:
        raise ResultImportError("GPU v3 result does not have one completion member")
    member = matching_results[0]
    if member != {
        "ordinal": item["ordinal"],
        "job_id": result["value"]["job_id"],
        "disposition": member["disposition"],
        "result_key": result["value"]["result_key"],
        "result_path": str(result["path"]),
        "result_sha256": result["raw_sha256"],
        "result_byte_count": len(result["body"]),
        "result_identity_sha256": result["identity_sha256"],
        "result_id": result["value"]["result_id"],
        "processing_run_id": result["run"]["processing_run_id"],
    } or member["disposition"] not in {"inferred", "reused"}:
        raise ResultImportError("GPU v3 batch completion member failed replay")
    return {
        "execution_mode": "batch",
        "batch_completion_uri": completion_path.as_uri(),
        "batch_completion_raw_sha256": raw_sha,
        "batch_completion_identity_sha256": identity,
        "batch_completion_id": completion["completion_id"],
        "gpu_batch_id": batch_id,
        "gpu_batch_ordinal": item["ordinal"],
        "manifest": {
            "uri": manifest_path.resolve(strict=True).as_uri(),
            "raw_sha256": manifest_raw_sha,
            "identity_sha256": manifest_identity,
        },
    }


def _build_plan(
    connection: sqlite3.Connection,
    result_path: str | Path,
    work_order_path: str | Path,
    batch_completion_path: str | Path | None,
) -> dict[str, Any]:
    work = _read_work_order(Path(work_order_path).resolve(strict=True))
    result = _validate_result_bundle(
        connection, Path(result_path).resolve(strict=True), work
    )
    batch = _batch_lineage(
        None if batch_completion_path is None else Path(batch_completion_path),
        result=result,
        work=work,
    )
    run = result["run"]
    media_id = work["value"]["input"]["media_id"]
    input_artifact_id = work["value"]["input"]["artifact_id"]
    revision_id = stable_id(
        "mltr", "faster-whisper-gpu-v3", run["processing_run_id"],
        media_id, result["identity_sha256"],
    )
    raw_artifact = result["raw_artifact"]
    normalized_artifact = result["normalized_artifact"]
    segment_rows: list[dict[str, Any]] = []
    word_rows: list[dict[str, Any]] = []
    for segment in result["transcript"]["segments"]:
        segment_id = stable_id("mlts", revision_id, segment["ordinal"])
        segment_rows.append(
            {
                "media_local_segment_id": segment_id,
                "media_local_revision_id": revision_id,
                "ordinal": segment["ordinal"],
                "media_start_ms": segment["start_ms"],
                "media_end_ms": segment["end_ms"],
                "input_boundary_overrun_ms": 0,
                "text": segment["text"],
                "normalized_text": None,
                "speaker_label": None,
                "language": result["transcript"]["language"],
                "confidence_band": None,
                "calibrated_probability": None,
                "metadata_json": canonical_json(
                    {
                        "raw_scores": segment["raw_scores"],
                        "timing_clipped_to_input": segment["timing_clipped_to_input"],
                        "word_timing_anomaly_count": segment[
                            "word_timing_anomaly_count"
                        ],
                        "word_timing_anomaly_flag_counts": segment[
                            "word_timing_anomaly_flag_counts"
                        ],
                    }
                ),
            }
        )
        for word in segment["words"]:
            probability = word["raw_probability"]
            word_rows.append(
                {
                    "media_local_word_id": stable_id(
                        "mltw", segment_id, word["ordinal"]
                    ),
                    "media_local_segment_id": segment_id,
                    "ordinal": word["ordinal"],
                    "media_start_ms": word["start_ms"],
                    "media_end_ms": word["end_ms"],
                    "token": word["token"],
                    "normalized_token": None,
                    "asr_log_probability": (
                        math.log(probability)
                        if probability is not None and probability > 0
                        else None
                    ),
                    "alignment_score": None,
                    "calibrated_probability": None,
                    "metadata_json": canonical_json(
                        {
                            "timing_anomaly_flags": word["timing_anomaly_flags"],
                            "timing_clipped_to_input": word[
                                "timing_clipped_to_input"
                            ],
                        }
                    ),
                }
            )
    revision = {
        "media_local_revision_id": revision_id,
        "media_id": media_id,
        "input_artifact_id": input_artifact_id,
        "processing_run_id": run["processing_run_id"],
        "revision_kind": "raw_asr",
        "origin": "faster-whisper GPU media-local v3",
        "language": result["transcript"]["language"],
        "glossary_revision_id": None,
        "review_state": "machine",
        "coordinate_system": "media_ms",
        "boundary": "half_open",
        "input_duration_ms": work["input_duration_ms"],
        "requested_start_ms": 0,
        "requested_end_ms": work["input_duration_ms"],
        "max_segment_end_ms": max(
            (row["media_end_ms"] for row in segment_rows), default=0
        ),
        "input_boundary_overrun_ms": 0,
        "source_coordinate_state": "unasserted_catalog_context_null",
        "recording_coordinate_state": "unasserted_catalog_context_null",
        "created_at": run["completed_at"],
        "metadata_json": canonical_json(
            {
                "catalog_context": None,
                "engine": "faster-whisper",
                "machine_generated": True,
                "normalized_transcript_artifact_id": normalized_artifact[
                    "artifact_id"
                ],
                "scores_calibrated": False,
                "speaker_assignment": None,
                "word_timing_contract": OUTPUT_CONTRACT,
            }
        ),
    }
    processing_run = {
        "processing_run_id": run["processing_run_id"],
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": canonical_json(
            {
                "inference": result["value"]["inference"],
                "recipe_id": result["value"]["recipe_id"],
                "recipe_sha256": result["value"]["recipe_sha256"],
            }
        ),
        "environment_json": canonical_json(
            {
                "gpu_lock": result["value"]["gpu_lock"],
                "hardware": result["value"]["hardware"],
                "model": result["value"]["model"],
                "runtime": result["value"]["runtime"],
            }
        ),
        "random_seed": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "error_text": None,
    }
    run_input = {
        "run_input_id": stable_id(
            "rin", run["processing_run_id"], "media", media_id, "normalized_audio"
        ),
        "processing_run_id": run["processing_run_id"],
        "object_type": "media",
        "object_id": media_id,
        "input_role": "normalized_audio",
        "input_sha256": work["input_sha256"],
    }
    artifacts = []
    for artifact, document_identity in (
        (raw_artifact, result["raw"]["identity_sha256"]),
        (normalized_artifact, result["normalized"]["identity_sha256"]),
    ):
        artifacts.append(
            {
                "artifact_id": artifact["artifact_id"],
                "processing_run_id": run["processing_run_id"],
                "artifact_kind": artifact["artifact_kind"],
                "storage_uri": artifact["storage_uri"],
                "sha256": artifact["sha256"],
                "byte_count": artifact["byte_count"],
                "schema_version": 1,
                "visibility": "private",
                "metadata_json": canonical_json(
                    {
                        "document_identity_sha256": document_identity,
                        "mime_type": "application/json",
                        "sealed_mode": "0400",
                    }
                ),
            }
        )

    statistics = {
        "artifacts": 2,
        "media_local_revisions": 1,
        "media_local_segments": len(segment_rows),
        "media_local_words": len(word_rows),
        "private_gpu_v3_asr_imports": 1,
        "publication_decisions": 0,
        "recording_coordinate_rows": 0,
        "speaker_assignments": 0,
    }
    core = {
        "schema_version": 1,
        "importer_version": IMPORTER_VERSION,
        "result": {
            "uri": result["path"].as_uri(),
            "raw_sha256": result["raw_sha256"],
            "identity_sha256": result["identity_sha256"],
            "byte_count": len(result["body"]),
            "result_id": result["value"]["result_id"],
            "result_key": result["value"]["result_key"],
            "processing_run_id": run["processing_run_id"],
            "media_local_revision_id": revision_id,
        },
        "work_order": {
            "uri": work["path"].as_uri(),
            "raw_sha256": work["raw_sha256"],
            "identity_sha256": work["identity_sha256"],
            "byte_count": len(work["body"]),
            "work_order_id": work["value"]["work_order_id"],
        },
        "artifacts": {
            "raw_transcript_artifact_id": raw_artifact["artifact_id"],
            "raw_transcript_identity_sha256": result["raw"]["identity_sha256"],
            "normalized_transcript_artifact_id": normalized_artifact["artifact_id"],
            "normalized_transcript_identity_sha256": result["normalized"][
                "identity_sha256"
            ],
        },
        "input": {
            "media_id": media_id,
            "artifact_id": input_artifact_id,
            "sha256": work["input_sha256"],
            "duration_ms": work["input_duration_ms"],
        },
        "batch": batch,
        "coordinates": {
            "coordinate_system": "media_ms",
            "boundary": "half_open",
            "recording_coordinate_state": "unasserted_catalog_context_null",
            "source_coordinate_state": "unasserted_catalog_context_null",
        },
        "transcript": {
            "segment_count": result["transcript"]["segment_count"],
            "word_count": result["transcript"]["word_count"],
            "anomalous_word_count": result["transcript"]["anomalous_word_count"],
            "timing_anomaly_flag_count": result["transcript"][
                "timing_anomaly_flag_count"
            ],
            "timing_anomaly_flag_counts": result["transcript"][
                "timing_anomaly_flag_counts"
            ],
        },
        "statistics": statistics,
        "safety": {
            "biometric_authority": "none",
            "event_authority": "none",
            "identity_authority": "none",
            "publication_authority": "none",
            "scores_calibrated": False,
            "speaker_assignment": "none",
            "transcript_text_in_plan": False,
            "visibility": "private",
            "wiki_authority": "none",
        },
    }
    plan_sha = sha256_bytes(canonical_json(core).encode("utf-8"))
    public = {"status": "validated", "plan_sha256": plan_sha, **core}
    import_batch_id = stable_id("imp", IMPORTER_NAME, result["identity_sha256"])
    receipt = {
        "private_gpu_v3_asr_import_id": stable_id(
            "gpuv3i", result["identity_sha256"], revision_id
        ),
        "import_batch_id": import_batch_id,
        "media_local_revision_id": revision_id,
        "processing_run_id": run["processing_run_id"],
        "result_uri": result["path"].as_uri(),
        "result_raw_sha256": result["raw_sha256"],
        "result_identity_sha256": result["identity_sha256"],
        "result_byte_count": len(result["body"]),
        "result_id": result["value"]["result_id"],
        "result_key": result["value"]["result_key"],
        "work_order_uri": work["path"].as_uri(),
        "work_order_raw_sha256": work["raw_sha256"],
        "work_order_identity_sha256": work["identity_sha256"],
        "work_order_byte_count": len(work["body"]),
        "work_order_id": work["value"]["work_order_id"],
        "raw_transcript_artifact_id": raw_artifact["artifact_id"],
        "raw_transcript_identity_sha256": result["raw"]["identity_sha256"],
        "normalized_transcript_artifact_id": normalized_artifact["artifact_id"],
        "normalized_transcript_identity_sha256": result["normalized"][
            "identity_sha256"
        ],
        "input_media_id": media_id,
        "input_artifact_id": input_artifact_id,
        "input_duration_ms": work["input_duration_ms"],
        "segment_count": result["transcript"]["segment_count"],
        "word_count": result["transcript"]["word_count"],
        "anomalous_word_count": result["transcript"]["anomalous_word_count"],
        "timing_anomaly_flag_count": result["transcript"][
            "timing_anomaly_flag_count"
        ],
        "execution_mode": batch["execution_mode"],
        "batch_completion_uri": batch["batch_completion_uri"],
        "batch_completion_raw_sha256": batch["batch_completion_raw_sha256"],
        "batch_completion_identity_sha256": batch[
            "batch_completion_identity_sha256"
        ],
        "batch_completion_id": batch["batch_completion_id"],
        "gpu_batch_id": batch["gpu_batch_id"],
        "gpu_batch_ordinal": batch["gpu_batch_ordinal"],
        "coordinate_system": "media_ms",
        "boundary": "half_open",
        "visibility": "private",
        "review_state": "machine",
        "human_review": "required",
        "score_calibration": "not_calibrated",
        "speaker_assignment": "none",
        "identity_authority": "none",
        "biometric_authority": "none",
        "event_authority": "none",
        "publication_authority": "none",
        "wiki_authority": "none",
        "export_authority": "none",
        "recording_coordinate_state": "unasserted_catalog_context_null",
        "source_coordinate_state": "unasserted_catalog_context_null",
        "plan_sha256": plan_sha,
        "imported_at": run["completed_at"],
        "metadata_json": canonical_json(
            {
                "batch_manifest": batch["manifest"],
                "output_contract": OUTPUT_CONTRACT,
                "timing_anomaly_flag_counts": result["transcript"][
                    "timing_anomaly_flag_counts"
                ],
            }
        ),
    }
    return {
        "public": public,
        "import_batch_id": import_batch_id,
        "processing_run": processing_run,
        "run_input": run_input,
        "artifacts": artifacts,
        "revision": revision,
        "segments": segment_rows,
        "words": word_rows,
        "receipt": receipt,
    }


def build_faster_whisper_gpu_v3_admission_plan(
    connection: sqlite3.Connection,
    result_path: str | Path,
    work_order_path: str | Path,
    *,
    batch_completion_path: str | Path | None = None,
) -> dict[str, Any]:
    """Return a transcript-text-free digest-gated private admission plan."""

    return _build_plan(
        connection, result_path, work_order_path, batch_completion_path
    )["public"]


def _insert_import_batch(connection: sqlite3.Connection, plan: dict[str, Any]) -> None:
    run = plan["processing_run"]
    row = {
        "import_batch_id": plan["import_batch_id"],
        "importer_name": IMPORTER_NAME,
        "importer_version": IMPORTER_VERSION,
        "input_sha256": plan["receipt"]["result_identity_sha256"],
        "source_snapshot_date": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "statistics_json": canonical_json(plan["public"]["statistics"]),
    }
    _insert_or_match(
        connection, table="import_batches", key_column="import_batch_id", row=row
    )


def _existing_replay(
    connection: sqlite3.Connection, plan: dict[str, Any]
) -> bool:
    receipt = plan["receipt"]
    existing = connection.execute(
        "SELECT * FROM private_gpu_v3_asr_imports "
        "WHERE private_gpu_v3_asr_import_id = ?",
        (receipt["private_gpu_v3_asr_import_id"],),
    ).fetchone()
    if existing is None:
        return False
    for key, value in receipt.items():
        if existing[key] != value:
            raise ResultImportError("GPU v3 import receipt ID already has different data")
    revision_id = receipt["media_local_revision_id"]
    counts = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM media_local_transcript_segments
           WHERE media_local_revision_id = ?) AS segments,
          (SELECT count(*)
           FROM media_local_transcript_words AS word
           JOIN media_local_transcript_segments AS segment
             ON segment.media_local_segment_id = word.media_local_segment_id
           WHERE segment.media_local_revision_id = ?) AS words
        """,
        (revision_id, revision_id),
    ).fetchone()
    if (
        counts["segments"] != receipt["segment_count"]
        or counts["words"] != receipt["word_count"]
    ):
        raise ResultImportError("GPU v3 replay found incomplete private transcript rows")
    return True


def import_faster_whisper_gpu_v3_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    work_order_path: str | Path,
    *,
    expected_plan_sha256: str,
    batch_completion_path: str | Path | None = None,
) -> dict[str, Any]:
    """Atomically admit one separately reviewed GPU v3 plan to private search."""

    _sha256(expected_plan_sha256, "expected GPU v3 plan digest")
    with transaction(connection):
        plan = _build_plan(
            connection, result_path, work_order_path, batch_completion_path
        )
        if plan["public"]["plan_sha256"] != expected_plan_sha256:
            raise ResultImportError(
                "GPU v3 result changed or was not the separately reviewed plan"
            )
        if _existing_replay(connection, plan):
            return {**plan["public"], "status": "admitted"}
        _insert_import_batch(connection, plan)
        _insert_exact_processing_run(connection, plan["processing_run"])
        _insert_exact_run_input(connection, plan["run_input"])
        for artifact in plan["artifacts"]:
            _insert_exact_artifact(connection, artifact)
        revision = plan["revision"]
        connection.execute(
            """
            INSERT INTO media_local_transcript_revisions(
                media_local_revision_id, media_id, input_artifact_id,
                processing_run_id, revision_kind, origin, language,
                glossary_revision_id, review_state, coordinate_system, boundary,
                input_duration_ms, requested_start_ms, requested_end_ms,
                max_segment_end_ms, input_boundary_overrun_ms,
                source_coordinate_state, recording_coordinate_state, created_at,
                metadata_json
            ) VALUES(
                :media_local_revision_id, :media_id, :input_artifact_id,
                :processing_run_id, :revision_kind, :origin, :language,
                :glossary_revision_id, :review_state, :coordinate_system, :boundary,
                :input_duration_ms, :requested_start_ms, :requested_end_ms,
                :max_segment_end_ms, :input_boundary_overrun_ms,
                :source_coordinate_state, :recording_coordinate_state, :created_at,
                :metadata_json
            )
            """,
            revision,
        )
        connection.executemany(
            """
            INSERT INTO media_local_transcript_segments(
                media_local_segment_id, media_local_revision_id, ordinal,
                media_start_ms, media_end_ms, input_boundary_overrun_ms, text,
                normalized_text, speaker_label, language, confidence_band,
                calibrated_probability, metadata_json
            ) VALUES(
                :media_local_segment_id, :media_local_revision_id, :ordinal,
                :media_start_ms, :media_end_ms, :input_boundary_overrun_ms, :text,
                :normalized_text, :speaker_label, :language, :confidence_band,
                :calibrated_probability, :metadata_json
            )
            """,
            plan["segments"],
        )
        connection.executemany(
            """
            INSERT INTO media_local_transcript_words(
                media_local_word_id, media_local_segment_id, ordinal,
                media_start_ms, media_end_ms, token, normalized_token,
                asr_log_probability, alignment_score, calibrated_probability,
                metadata_json
            ) VALUES(
                :media_local_word_id, :media_local_segment_id, :ordinal,
                :media_start_ms, :media_end_ms, :token, :normalized_token,
                :asr_log_probability, :alignment_score, :calibrated_probability,
                :metadata_json
            )
            """,
            plan["words"],
        )
        receipt = plan["receipt"]
        columns = tuple(receipt)
        connection.execute(
            f"INSERT INTO private_gpu_v3_asr_imports({', '.join(columns)}) "
            f"VALUES({', '.join('?' for _ in columns)})",
            tuple(receipt[column] for column in columns),
        )
    return {**plan["public"], "status": "admitted"}
