#!/usr/bin/env python3
"""Deterministic, text-private comparison of paired whisper.cpp ASR results.

This tool is an audit aid, not an accuracy scorer.  It compares a completed raw
result with a completed glossary-assisted result over fixed media-local time
blocks and emits only counts, distances, timing deltas, and glossary-term count
deltas.  Transcript wording never enters the output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any

try:
    import asr_whispercpp
except ModuleNotFoundError:  # pragma: no cover - package-style test import
    from pipeline import asr_whispercpp  # type: ignore


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
COMPARATOR = "himr-contextual-asr-text-private-diff"
DEFAULT_BLOCK_MS = 30_000
MIN_BLOCK_MS = 5_000
MAX_BLOCK_MS = 120_000
MAX_RESULT_BYTES = 256 * 1024 * 1024
WHISPER_GPT2_VOCAB_SIZE = 50_257

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CORPUS_SOURCE_ROOT = (REPOSITORY_ROOT / "corpus" / "src").resolve()

RESULT_KEYS = {
    "artifacts",
    "catalog_context",
    "catalog_records",
    "commands",
    "dry_run",
    "duration_ms",
    "engine",
    "errors",
    "glossary",
    "input",
    "job_id",
    "model",
    "processing_run",
    "recipe_id",
    "recipe_sha256",
    "result_key",
    "result_path",
    "run_input",
    "schema_version",
    "status",
    "transcript",
    "window",
    "work_order_sha256",
}


class DiffError(RuntimeError):
    """The pair or its retained evidence violates the comparison contract."""


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
        raise DiffError(f"value is not strict canonical JSON: {error}") from error


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
        raise DiffError(f"value is not strict JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_constant(value: str) -> None:
    raise DiffError(f"non-finite JSON constant is forbidden: {value}")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DiffError(f"JSON object contains duplicate key: {key}")
        result[key] = value
    return result


def parse_json(body: bytes, label: str) -> Any:
    try:
        return json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DiffError(f"{label} is not strict UTF-8 JSON: {error}") from error


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DiffError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise DiffError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def stable_file(path: Path, label: str, maximum_bytes: int) -> tuple[bytes, os.stat_result]:
    try:
        before = path.lstat()
    except OSError as error:
        raise DiffError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise DiffError(f"{label} must be a regular non-symlink file")
    if before.st_size <= 0 or before.st_size > maximum_bytes:
        raise DiffError(f"{label} size is outside the supported range")
    body = asr_whispercpp.stable_file_bytes(
        path,
        maximum_bytes=maximum_bytes,
        label=label,
    )
    after = path.lstat()
    if (
        stat.S_ISLNK(after.st_mode)
        or not stat.S_ISREG(after.st_mode)
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
    ):
        raise DiffError(f"{label} changed while being read")
    return body, after


def _absolute_result_path(value: str, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve(strict=True)
    else:
        path = path.resolve(strict=True)
    if path.name != "result.json":
        raise DiffError(f"{label} must end in result.json")
    return path


def _catalog_free_validate(path: Path, label: str) -> dict[str, Any]:
    """Apply the catalog importer's full nested-envelope validator read-only."""

    source = str(CORPUS_SOURCE_ROOT)
    added = source not in sys.path
    if added:
        sys.path.insert(0, source)
    try:
        from himr_corpus.asr_result_importer import (  # noqa: PLC0415
            ResultImportError,
            validate_asr_whispercpp_result_file,
        )
    except ImportError as error:
        raise DiffError(
            f"{label} catalog-free ASR validator could not be loaded: {error}"
        ) from error
    try:
        return validate_asr_whispercpp_result_file(path)
    except ResultImportError as error:
        raise DiffError(
            f"{label} failed strict catalog-free validation: {error}"
        ) from error
    finally:
        if added:
            sys.path.remove(source)


def _load_completed_result(value: str, label: str) -> dict[str, Any]:
    path = _absolute_result_path(value, label)
    body, _ = stable_file(path, label, MAX_RESULT_BYTES)
    supplied = exact_object(parse_json(body, label), label, RESULT_KEYS)
    if (
        supplied.get("schema_version") != 1
        or supplied.get("status") != "completed"
        or supplied.get("dry_run") is not False
        or supplied.get("errors") != []
    ):
        raise DiffError(f"{label} is not a supported completed ASR envelope")
    result_key = supplied.get("result_key")
    recipe_id = supplied.get("recipe_id")
    if not isinstance(result_key, str) or not re.fullmatch(r"[0-9a-f]{64}", result_key):
        raise DiffError(f"{label}.result_key is malformed")
    if not isinstance(recipe_id, str) or not re.fullmatch(
        r"recipe_asr_whispercpp_[0-9a-f]{32}", recipe_id
    ):
        raise DiffError(f"{label}.recipe_id is malformed")
    strict_summary = _catalog_free_validate(path, label)
    processing_run = supplied.get("processing_run")
    transcript = supplied.get("transcript")
    catalog_context = supplied.get("catalog_context")
    if not isinstance(processing_run, dict) or not isinstance(transcript, dict):
        raise DiffError(f"{label} lacks strict processing/transcript evidence")
    recording_id = None
    rendition_id = None
    if isinstance(catalog_context, dict):
        recording_id = catalog_context.get("recording_id")
        rendition_id = catalog_context.get("rendition_id")
    expected_summary = {
        "result_envelope_sha256": sha256_bytes(canonical_bytes(supplied)),
        "job_id": supplied.get("job_id"),
        "processing_run_id": processing_run.get("processing_run_id"),
        "recipe_id": recipe_id,
        "result_key": result_key,
        "model_id": supplied.get("model", {}).get("model_id")
        if isinstance(supplied.get("model"), dict)
        else None,
        "glossary_revision_id": processing_run.get("glossary_revision_id"),
        "recording_id": recording_id,
        "rendition_id": rendition_id,
        "artifact_count": len(supplied.get("artifacts", []))
        if isinstance(supplied.get("artifacts"), list)
        else None,
        "segment_count": transcript.get("segment_count"),
        "token_count": transcript.get("token_count"),
        "quality_flags": transcript.get("quality_flags"),
    }
    if strict_summary != expected_summary:
        raise DiffError(f"{label} strict validation summary differs from its envelope")
    try:
        replayed = asr_whispercpp.validate_completed_reuse(
            path,
            result_key=result_key,
            recipe_id=recipe_id,
            run_dir=path.parent,
        )
    except (asr_whispercpp.ASRError, OSError) as error:
        raise DiffError(f"{label} immutable result validation failed: {error}") from error
    if replayed != supplied:
        raise DiffError(f"{label} changed during immutable validation")
    replay_body, _ = stable_file(path, f"{label} replay", MAX_RESULT_BYTES)
    if replay_body != body:
        raise DiffError(f"{label} changed during comparison admission")
    return {
        "path": str(path),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "result": supplied,
    }


def _strict_json_text(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise DiffError(f"{label} must be strict JSON text")
    parsed = parse_json(value.encode("utf-8"), label)
    if not isinstance(parsed, dict):
        raise DiffError(f"{label} must decode to an object")
    if canonical_bytes(parsed).decode("utf-8") != value:
        raise DiffError(f"{label} is not canonical JSON text")
    return parsed


def _hash_bound_file(
    block: Any,
    label: str,
    *,
    executable: bool = False,
    cache: dict[tuple[str, str, int], tuple[int, int, int, int, int]] | None = None,
) -> None:
    if not isinstance(block, dict):
        raise DiffError(f"{label} must be an object")
    path_value = block.get("path")
    expected = block.get("sha256")
    expected_bytes = block.get("byte_count")
    if (
        not isinstance(path_value, str)
        or not isinstance(expected, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected)
        or not isinstance(expected_bytes, int)
        or isinstance(expected_bytes, bool)
        or expected_bytes <= 0
    ):
        raise DiffError(f"{label} path/hash/size evidence is malformed")
    path = Path(path_value)
    if not path.is_absolute():
        raise DiffError(f"{label}.path must be absolute")
    path = path.resolve(strict=True)
    cache_key = (str(path), expected, expected_bytes)
    observed = path.lstat()
    observed_identity = (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )
    if cache is not None and cache_key in cache:
        if cache[cache_key] != observed_identity:
            raise DiffError(f"{label} changed between paired evidence checks")
    else:
        body, observed = stable_file(path, label, max(expected_bytes, 1))
        if len(body) != expected_bytes or sha256_bytes(body) != expected:
            raise DiffError(f"{label} current bytes differ from the result evidence")
        observed_identity = (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )
        if cache is not None:
            cache[cache_key] = observed_identity
    if executable and not (observed.st_mode & stat.S_IXUSR):
        raise DiffError(f"{label} is not owner-executable")


def _semantic_pair(raw: dict[str, Any], contextual: dict[str, Any]) -> dict[str, Any]:
    if raw.get("glossary") is not None:
        raise DiffError("baseline result must be an unprompted raw ASR result")
    glossary_block = contextual.get("glossary")
    if not isinstance(glossary_block, dict):
        raise DiffError("contextual result must retain a glossary block")

    file_cache: dict[tuple[str, str, int], tuple[int, int, int, int, int]] = {}
    for label, result in (("baseline", raw), ("contextual", contextual)):
        processing = result.get("processing_run")
        if not isinstance(processing, dict):
            raise DiffError(f"{label}.processing_run must be an object")
        if (
            processing.get("stage") != "asr_whispercpp"
            or processing.get("status") != "completed"
            or processing.get("implementation_version") != asr_whispercpp.IMPLEMENTATION_VERSION
        ):
            raise DiffError(f"{label} is not a current completed whisper.cpp adapter run")
        _hash_bound_file(result.get("input"), f"{label} input", cache=file_cache)
        _hash_bound_file(
            result.get("engine"),
            f"{label} engine",
            executable=True,
            cache=file_cache,
        )
        _hash_bound_file(result.get("model"), f"{label} model", cache=file_cache)

    direct_fields = ("catalog_context", "window")
    for field in direct_fields:
        if raw.get(field) != contextual.get(field):
            raise DiffError(f"paired results differ in {field}")

    input_fields = (
        "path",
        "sha256",
        "byte_count",
        "media_id",
        "artifact_id",
        "parent_processing_run_id",
        "probe",
    )
    engine_fields = ("path", "sha256", "byte_count", "version", "version_evidence", "build")
    model_fields = (
        "path",
        "sha256",
        "byte_count",
        "model_id",
        "name",
        "revision",
        "source",
        "license_label",
    )
    for block_name, fields in (
        ("input", input_fields),
        ("engine", engine_fields),
        ("model", model_fields),
    ):
        left = raw.get(block_name)
        right = contextual.get(block_name)
        if not isinstance(left, dict) or not isinstance(right, dict):
            raise DiffError(f"paired {block_name} evidence must be objects")
        if any(left.get(field) != right.get(field) for field in fields):
            raise DiffError(f"paired results differ in {block_name} identity")

    raw_parameters = _strict_json_text(
        raw["processing_run"].get("parameters_json"),
        "baseline processing parameters",
    )
    contextual_parameters = _strict_json_text(
        contextual["processing_run"].get("parameters_json"),
        "contextual processing parameters",
    )
    if raw_parameters.get("glossary") is not None:
        raise DiffError("baseline processing parameters are not raw/null-glossary")
    contextual_parameter_glossary = contextual_parameters.get("glossary")
    if not isinstance(contextual_parameter_glossary, dict):
        raise DiffError("contextual processing parameters lack glossary provenance")
    raw_without_glossary = dict(raw_parameters)
    contextual_without_glossary = dict(contextual_parameters)
    raw_without_glossary.pop("glossary", None)
    contextual_without_glossary.pop("glossary", None)
    if raw_without_glossary != contextual_without_glossary:
        raise DiffError("paired processing recipes differ beyond the glossary")

    glossary_path = glossary_block.get("path")
    glossary_sha = glossary_block.get("sha256")
    glossary_bytes = glossary_block.get("byte_count")
    if (
        not isinstance(glossary_path, str)
        or not Path(glossary_path).is_absolute()
        or not isinstance(glossary_sha, str)
        or not re.fullmatch(r"[0-9a-f]{64}", glossary_sha)
        or not isinstance(glossary_bytes, int)
        or glossary_bytes <= 0
    ):
        raise DiffError("contextual glossary file evidence is malformed")
    glossary_body, _ = stable_file(
        Path(glossary_path).resolve(strict=True),
        "contextual glossary",
        1024 * 1024,
    )
    if len(glossary_body) != glossary_bytes or sha256_bytes(glossary_body) != glossary_sha:
        raise DiffError("contextual glossary current bytes differ from result evidence")
    glossary_raw = parse_json(glossary_body, "contextual glossary")
    try:
        glossary = asr_whispercpp.validate_glossary_document(
            glossary_raw,
            contextual_parameters.get("inference", {}).get("language"),
        )
    except asr_whispercpp.ASRError as error:
        raise DiffError(f"contextual glossary validation failed: {error}") from error
    for field in ("glossary_revision_id", "revision", "language", "prompt_sha256"):
        if glossary_block.get(field) != glossary.get(field):
            raise DiffError(f"contextual glossary {field} differs from retained bytes")
    if glossary_block.get("term_count") != len(glossary["terms"]):
        raise DiffError("contextual glossary term_count differs from retained bytes")
    expected_parameter_glossary = {
        "glossary_revision_id": glossary["glossary_revision_id"],
        "revision": glossary["revision"],
        "sha256": glossary_sha,
        "prompt_sha256": glossary["prompt_sha256"],
    }
    if contextual_parameter_glossary != expected_parameter_glossary:
        raise DiffError("contextual processing parameters disagree with glossary evidence")
    if raw["processing_run"].get("glossary_revision_id") is not None:
        raise DiffError("baseline processing run unexpectedly names a glossary")
    if contextual["processing_run"].get("glossary_revision_id") != glossary["glossary_revision_id"]:
        raise DiffError("contextual processing run glossary ID is inconsistent")
    return {
        "schema_version": glossary["schema_version"],
        "glossary_revision_id": glossary["glossary_revision_id"],
        "revision": glossary["revision"],
        "language": glossary["language"],
        "sha256": glossary_sha,
        "byte_count": glossary_bytes,
        "prompt_sha256": glossary["prompt_sha256"],
        "term_count": len(glossary["terms"]),
        "terms": glossary["terms"],
    }


def _normal_text(value: str) -> str:
    return " ".join(value.split())


def _lexical_tokens(
    result: dict[str, Any], window_start: int, window_end: int
) -> list[dict[str, Any]]:
    transcript = result.get("transcript")
    if not isinstance(transcript, dict) or not isinstance(transcript.get("segments"), list):
        raise DiffError("result transcript segments are malformed")
    tokens: list[dict[str, Any]] = []
    last_segment_ordinal = -1
    for segment in transcript["segments"]:
        if not isinstance(segment, dict):
            raise DiffError("transcript segment must be an object")
        ordinal = segment.get("ordinal")
        start = segment.get("start_ms")
        end = segment.get("end_ms")
        segment_tokens = segment.get("tokens")
        if (
            not isinstance(ordinal, int)
            or isinstance(ordinal, bool)
            or ordinal <= last_segment_ordinal
            or not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
            or start < 0
            or end < start
            or not isinstance(segment_tokens, list)
        ):
            raise DiffError("transcript segment ordering or timing is invalid")
        last_segment_ordinal = ordinal
        last_token_ordinal = -1
        for token in segment_tokens:
            if not isinstance(token, dict):
                raise DiffError("transcript token must be an object")
            token_ordinal = token.get("ordinal")
            token_id = token.get("token_id")
            text = token.get("text")
            probability = token.get("raw_probability")
            token_start = token.get("start_ms")
            token_end = token.get("end_ms")
            if (
                not isinstance(token_ordinal, int)
                or isinstance(token_ordinal, bool)
                or token_ordinal <= last_token_ordinal
                or not isinstance(token_id, int)
                or isinstance(token_id, bool)
                or not isinstance(text, str)
                or not isinstance(probability, (int, float))
                or isinstance(probability, bool)
                or not math.isfinite(float(probability))
                or probability < 0
                or probability > 1
            ):
                raise DiffError("transcript token identity/text/score is malformed")
            last_token_ordinal = token_ordinal
            if token_id >= WHISPER_GPT2_VOCAB_SIZE:
                continue
            timing_available = (
                isinstance(token_start, int)
                and not isinstance(token_start, bool)
                and isinstance(token_end, int)
                and not isinstance(token_end, bool)
                and 0 <= token_start <= token_end
            )
            midpoint = (
                (token_start + token_end) // 2
                if timing_available
                else (start + end) // 2
            )
            midpoint = min(max(midpoint, window_start), window_end - 1)
            tokens.append(
                {
                    "text": text,
                    "raw_probability": float(probability),
                    "start_ms": token_start if timing_available else None,
                    "end_ms": token_end if timing_available else None,
                    "midpoint_ms": midpoint,
                    "timing_available": timing_available,
                }
            )
    return tokens


def _levenshtein(left: str, right: str) -> int:
    if len(left) < len(right):
        left, right = right, left
    prior = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            current.append(
                min(
                    current[-1] + 1,
                    prior[right_index] + 1,
                    prior[right_index - 1] + (left_char != right_char),
                )
            )
        prior = current
    return prior[-1]


def _score_summary(tokens: list[dict[str, Any]]) -> dict[str, Any]:
    values = sorted(token["raw_probability"] for token in tokens)
    if not values:
        return {"count": 0, "minimum": None, "mean": None, "median": None}
    middle = len(values) // 2
    median = (
        values[middle]
        if len(values) % 2
        else (values[middle - 1] + values[middle]) / 2
    )
    return {
        "count": len(values),
        "minimum": min(values),
        "mean": sum(values) / len(values),
        "median": median,
    }


def _max_repeated_word_run(text: str) -> int:
    words = re.findall(r"\w+", text.casefold(), flags=re.UNICODE)
    maximum = 0
    current = 0
    previous: str | None = None
    for word in words:
        if word == previous:
            current += 1
        else:
            previous = word
            current = 1
        maximum = max(maximum, current)
    return maximum


def _term_count(text: str, term: str) -> int:
    expression = re.compile(
        rf"(?<!\w){re.escape(term.casefold())}(?!\w)",
        flags=re.UNICODE,
    )
    return len(expression.findall(text.casefold()))


def _timing_extent(tokens: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    starts = [token["start_ms"] for token in tokens if token["start_ms"] is not None]
    ends = [token["end_ms"] for token in tokens if token["end_ms"] is not None]
    return (min(starts) if starts else None, max(ends) if ends else None)


def build_diff(
    *, baseline_result: str, contextual_result: str, block_ms: int = DEFAULT_BLOCK_MS
) -> dict[str, Any]:
    if (
        not isinstance(block_ms, int)
        or isinstance(block_ms, bool)
        or not MIN_BLOCK_MS <= block_ms <= MAX_BLOCK_MS
    ):
        raise DiffError(
            f"block_ms must be between {MIN_BLOCK_MS} and {MAX_BLOCK_MS}"
        )
    baseline = _load_completed_result(baseline_result, "baseline result")
    contextual = _load_completed_result(contextual_result, "contextual result")
    if baseline["path"] == contextual["path"]:
        raise DiffError("baseline and contextual result paths must differ")
    glossary = _semantic_pair(baseline["result"], contextual["result"])
    window = baseline["result"]["window"]
    if not isinstance(window, dict):
        raise DiffError("paired result window is malformed")
    window_start = window.get("offset_ms")
    window_end = window.get("end_ms")
    if (
        not isinstance(window_start, int)
        or isinstance(window_start, bool)
        or not isinstance(window_end, int)
        or isinstance(window_end, bool)
        or window_start < 0
        or window_end <= window_start
    ):
        raise DiffError("paired result window is invalid")
    baseline_tokens = _lexical_tokens(
        baseline["result"], window_start, window_end
    )
    contextual_tokens = _lexical_tokens(
        contextual["result"], window_start, window_end
    )

    changed_blocks: list[dict[str, Any]] = []
    aggregate_terms = {
        term: {"baseline_count": 0, "contextual_count": 0}
        for term in glossary["terms"]
    }
    total_distance = 0
    empty_transitions = 0
    maximum_abs_start_drift = 0
    maximum_abs_end_drift = 0
    block_index = 0
    for start in range(window_start, window_end, block_ms):
        end = min(window_end, start + block_ms)
        raw_block = [token for token in baseline_tokens if start <= token["midpoint_ms"] < end]
        contextual_block = [
            token for token in contextual_tokens if start <= token["midpoint_ms"] < end
        ]
        raw_text = _normal_text("".join(token["text"] for token in raw_block))
        contextual_text = _normal_text(
            "".join(token["text"] for token in contextual_block)
        )
        if raw_text == contextual_text:
            block_index += 1
            continue
        distance = _levenshtein(raw_text, contextual_text)
        total_distance += distance
        if not raw_text and contextual_text:
            empty_state = "baseline_empty_contextual_nonempty"
            empty_transitions += 1
        elif raw_text and not contextual_text:
            empty_state = "baseline_nonempty_contextual_empty"
            empty_transitions += 1
        else:
            empty_state = "neither_empty"
        term_deltas = []
        for term in glossary["terms"]:
            raw_count = _term_count(raw_text, term)
            contextual_count = _term_count(contextual_text, term)
            aggregate_terms[term]["baseline_count"] += raw_count
            aggregate_terms[term]["contextual_count"] += contextual_count
            if raw_count or contextual_count:
                term_deltas.append(
                    {
                        "term": term,
                        "baseline_count": raw_count,
                        "contextual_count": contextual_count,
                        "delta": contextual_count - raw_count,
                    }
                )
        raw_extent = _timing_extent(raw_block)
        contextual_extent = _timing_extent(contextual_block)
        start_drift = (
            None
            if raw_extent[0] is None or contextual_extent[0] is None
            else contextual_extent[0] - raw_extent[0]
        )
        end_drift = (
            None
            if raw_extent[1] is None or contextual_extent[1] is None
            else contextual_extent[1] - raw_extent[1]
        )
        if start_drift is not None:
            maximum_abs_start_drift = max(maximum_abs_start_drift, abs(start_drift))
        if end_drift is not None:
            maximum_abs_end_drift = max(maximum_abs_end_drift, abs(end_drift))
        changed_blocks.append(
            {
                "block_index": block_index,
                "start_ms": start,
                "end_ms": end,
                "baseline_character_count": len(raw_text),
                "contextual_character_count": len(contextual_text),
                "baseline_lexical_token_count": len(raw_block),
                "contextual_lexical_token_count": len(contextual_block),
                "character_edit_distance": distance,
                "distance_over_max_characters": (
                    distance / max(len(raw_text), len(contextual_text))
                    if raw_text or contextual_text
                    else 0.0
                ),
                "empty_transition": empty_state,
                "baseline_raw_score_summary": _score_summary(raw_block),
                "contextual_raw_score_summary": _score_summary(contextual_block),
                "baseline_untimed_lexical_tokens": sum(
                    not token["timing_available"] for token in raw_block
                ),
                "contextual_untimed_lexical_tokens": sum(
                    not token["timing_available"] for token in contextual_block
                ),
                "baseline_max_repeated_word_run": _max_repeated_word_run(raw_text),
                "contextual_max_repeated_word_run": _max_repeated_word_run(
                    contextual_text
                ),
                "first_timed_token_start_drift_ms": start_drift,
                "last_timed_token_end_drift_ms": end_drift,
                "glossary_term_counts": term_deltas,
            }
        )
        block_index += 1

    aggregate_term_rows = []
    for term in glossary["terms"]:
        raw_count = aggregate_terms[term]["baseline_count"]
        contextual_count = aggregate_terms[term]["contextual_count"]
        aggregate_term_rows.append(
            {
                "term": term,
                "baseline_count_in_changed_blocks": raw_count,
                "contextual_count_in_changed_blocks": contextual_count,
                "delta": contextual_count - raw_count,
            }
        )

    identity = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "comparator": COMPARATOR,
        "baseline_result_sha256": baseline["sha256"],
        "contextual_result_sha256": contextual["sha256"],
        "glossary_sha256": glossary["sha256"],
        "block_ms": block_ms,
    }
    pair_id = "contextual_asr_diff_" + sha256_bytes(canonical_bytes(identity))[:32]
    return {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "comparator": COMPARATOR,
        "pair_id": pair_id,
        "identity_sha256": sha256_bytes(canonical_bytes(identity)),
        "baseline": {
            "result_path": baseline["path"],
            "result_sha256": baseline["sha256"],
            "result_byte_count": baseline["byte_count"],
            "result_key": baseline["result"]["result_key"],
            "processing_run_id": baseline["result"]["processing_run"][
                "processing_run_id"
            ],
        },
        "contextual": {
            "result_path": contextual["path"],
            "result_sha256": contextual["sha256"],
            "result_byte_count": contextual["byte_count"],
            "result_key": contextual["result"]["result_key"],
            "processing_run_id": contextual["result"]["processing_run"][
                "processing_run_id"
            ],
        },
        "glossary": {key: value for key, value in glossary.items() if key != "terms"},
        "alignment": {
            "coordinate_system": "input_media_milliseconds",
            "window_start_ms": window_start,
            "window_end_ms": window_end,
            "block_ms": block_ms,
            "token_assignment": "lexical_token_midpoint_fixed_half_open_block",
            "untimed_token_assignment": "parent_segment_midpoint",
            "segmentation_independent_accuracy_claim": False,
        },
        "summary": {
            "total_blocks": block_index,
            "changed_blocks": len(changed_blocks),
            "unchanged_blocks": block_index - len(changed_blocks),
            "total_character_edit_distance": total_distance,
            "empty_nonempty_transitions": empty_transitions,
            "maximum_absolute_first_token_start_drift_ms": maximum_abs_start_drift,
            "maximum_absolute_last_token_end_drift_ms": maximum_abs_end_drift,
            "baseline_lexical_tokens": len(baseline_tokens),
            "contextual_lexical_tokens": len(contextual_tokens),
            "baseline_untimed_lexical_tokens": sum(
                not token["timing_available"] for token in baseline_tokens
            ),
            "contextual_untimed_lexical_tokens": sum(
                not token["timing_available"] for token in contextual_tokens
            ),
            "glossary_term_counts": aggregate_term_rows,
        },
        "changed_block_metrics": changed_blocks,
        "policy": {
            "visibility": "private",
            "transcript_text_included": False,
            "decoder_scores_calibrated": False,
            "accuracy_claimed": False,
            "improvement_claimed": False,
            "preferred_revision_selected": False,
            "human_review_claimed": False,
            "automatic_merge_allowed": False,
            "publication_authority": "none",
        },
    }


def _write_exact(path_value: str, body: bytes) -> None:
    path = Path(path_value)
    if not path.is_absolute():
        raise DiffError("--output must be an absolute path")
    if path.name in ("", ".", "..") or path.parent in (
        Path("/"),
        Path("/tmp"),
        Path("/var/tmp"),
    ):
        raise DiffError("--output must be a specific durable private file")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        existing, _ = stable_file(path, "existing diff output", MAX_RESULT_BYTES)
        if existing != body:
            raise DiffError("existing diff output bytes differ")
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a deterministic text-private raw/contextual ASR diff"
    )
    parser.add_argument("--baseline-result", required=True)
    parser.add_argument("--contextual-result", required=True)
    parser.add_argument("--block-ms", type=int, default=DEFAULT_BLOCK_MS)
    parser.add_argument("--output")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = build_diff(
            baseline_result=args.baseline_result,
            contextual_result=args.contextual_result,
            block_ms=args.block_ms,
        )
        body = pretty_bytes(result)
        if args.output:
            _write_exact(args.output, body)
        sys.stdout.buffer.write(body)
        return 0
    except (DiffError, OSError, asr_whispercpp.ASRError) as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.buffer.write(pretty_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
