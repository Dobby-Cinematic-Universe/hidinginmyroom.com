"""Strict transactional admission for private audio-fingerprint evidence."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .db import transaction
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError
from .asr_result_importer import (
    _absolute_observed_path,
    _array,
    _canonical_equal,
    _exact_keys,
    _identifier,
    _integer,
    _local_file_uri,
    _number,
    _object,
    _producer_id,
    _sha256,
    _stable_read,
    _string,
    _timestamp,
    _timestamp_value,
    _verify_hash,
)


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "audio_fingerprint_chromaprint"
COMPARE_STAGE = "audio_fingerprint_exact_compare"
COMPARE_V2_STAGE = "audio_fingerprint_exact_compare_v2"
COMPARE_V2_IMPLEMENTATION_VERSION = "0.2.0"
RAW_FORMAT = "ffmpeg_chromaprint_fp_format_raw"
MAX_RESULT_BYTES = 128 * 1024 * 1024
MAX_FINGERPRINT_BYTES = 64 * 1024 * 1024
SAMPLE_RATE_HZ = 16_000
CHANNELS = 1
FINGERPRINT_FLAGS = {
    "short_window_under_10s",
    "short_window_under_30s",
    "partial_tail_chunk",
    "empty_raw_fingerprint",
}
COMPARE_FLAGS = {
    "exact_comparison_only_no_alignment",
    "cross_duration_windows",
    "short_query_window_under_10s",
    "short_candidate_window_under_10s",
    "empty_query_fingerprint",
    "empty_candidate_fingerprint",
}
COMPARE_V2_FLAGS = COMPARE_FLAGS | {
    "cross_recording",
    "cross_rendition",
    "cross_input_media",
    "cross_extraction_recipe",
}


def _reject_json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ResultImportError(f"fingerprint result contains duplicate JSON key {key!r}")
        value[key] = item
    return value


def _reject_json_constant(value: str) -> None:
    raise ResultImportError(
        f"fingerprint result contains non-finite JSON constant {value}"
    )


def _canonical_json_string(value: object, label: str) -> str:
    text = _string(value, label)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        raise ResultImportError(f"{label} must contain JSON") from error
    if canonical_json(parsed) != text:
        raise ResultImportError(f"{label} must use canonical JSON encoding")
    return text


def _sorted_flags(value: object, label: str, allowed: set[str]) -> list[str]:
    flags = _array(value, label)
    normalized = [_string(flag, f"{label}[]", maximum=128) for flag in flags]
    if normalized != sorted(set(normalized)) or not set(normalized) <= allowed:
        raise ResultImportError(f"{label} is unsupported, duplicated, or unsorted")
    return normalized


def _load_sealed_result(path: Path) -> tuple[dict[str, Any], bytes, Path]:
    if not path.is_absolute():
        path = path.resolve()
    observed = _absolute_observed_path(str(path), "fingerprint result")
    if observed.lstat().st_mode & 0o222:
        raise ResultImportError("fingerprint result must be sealed read-only")
    body = _stable_read(observed, "fingerprint result", maximum_bytes=MAX_RESULT_BYTES)
    try:
        result = _object(
            json.loads(
                body,
                object_pairs_hook=_reject_json_pairs,
                parse_constant=_reject_json_constant,
            ),
            "fingerprint result",
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError("fingerprint result must be UTF-8 JSON") from error
    return result, body, observed


def _engine(value: object) -> tuple[dict[str, Any], Path]:
    engine = _object(value, "fingerprint engine")
    _exact_keys(
        engine,
        "fingerprint engine",
        {
            "name", "path", "sha256", "byte_count", "version_label", "version_output",
            "version_output_sha256", "build_configuration", "muxer_help", "muxer_help_sha256",
        },
    )
    if engine["name"] != "ffmpeg":
        raise ResultImportError("fingerprint engine.name must equal ffmpeg")
    path = _absolute_observed_path(engine["path"], "fingerprint engine.path")
    digest = _sha256(engine["sha256"], "fingerprint engine.sha256")
    byte_count = _integer(engine["byte_count"], "fingerprint engine.byte_count", minimum=1)
    version = _string(engine["version_label"], "fingerprint engine.version_label")
    version_output = _string(engine["version_output"], "fingerprint engine.version_output")
    version_sha = _sha256(
        engine["version_output_sha256"], "fingerprint engine.version_output_sha256"
    )
    if version_output.splitlines()[0].strip() != version:
        raise ResultImportError("fingerprint engine version label disagrees with build output")
    if sha256_bytes(version_output.encode("utf-8")) != version_sha:
        raise ResultImportError("fingerprint engine version-output digest is inconsistent")
    build_configuration = engine["build_configuration"]
    if build_configuration is not None:
        _string(build_configuration, "fingerprint engine.build_configuration")
    muxer_help = _string(engine["muxer_help"], "fingerprint engine.muxer_help")
    muxer_sha = _sha256(engine["muxer_help_sha256"], "fingerprint engine.muxer_help_sha256")
    if sha256_bytes(muxer_help.encode("utf-8")) != muxer_sha:
        raise ResultImportError("fingerprint muxer-help digest is inconsistent")
    if "Muxer chromaprint" not in muxer_help or "fp_format" not in muxer_help:
        raise ResultImportError("fingerprint engine evidence lacks the Chromaprint muxer")
    _verify_hash(path, digest, byte_count, "fingerprint FFmpeg executable")
    return dict(engine), path


def _input(value: object) -> tuple[dict[str, Any], Path]:
    row = _object(value, "fingerprint input")
    _exact_keys(
        row,
        "fingerprint input",
        {
            "path", "storage_uri", "sha256", "byte_count", "media_id", "artifact_id",
            "parent_processing_run_id", "duration_ms", "sample_rate_hz", "channels",
            "sample_format", "unchanged",
        },
    )
    path = _absolute_observed_path(row["path"], "fingerprint input.path")
    if path.lstat().st_mode & 0o222:
        raise ResultImportError("fingerprint input must be a sealed read-only artifact")
    uri_path = _local_file_uri(row["storage_uri"], "fingerprint input.storage_uri")
    if uri_path != path:
        raise ResultImportError("fingerprint input path and URI disagree")
    digest = _sha256(row["sha256"], "fingerprint input.sha256")
    byte_count = _integer(row["byte_count"], "fingerprint input.byte_count", minimum=1)
    if _identifier(row["media_id"], "fingerprint input.media_id") != f"media_sha256_{digest}":
        raise ResultImportError("fingerprint media ID is not derived from input bytes")
    _identifier(row["artifact_id"], "fingerprint input.artifact_id")
    _identifier(row["parent_processing_run_id"], "fingerprint input.parent_processing_run_id")
    _integer(row["duration_ms"], "fingerprint input.duration_ms", minimum=1)
    if row["sample_rate_hz"] != SAMPLE_RATE_HZ or row["channels"] != CHANNELS:
        raise ResultImportError("fingerprint input is not normalized to 16 kHz mono")
    if row["sample_format"] != "s16" or row["unchanged"] is not True:
        raise ResultImportError("fingerprint input sample format/integrity state is invalid")
    _verify_hash(path, digest, byte_count, "fingerprint normalized-audio input")
    return dict(row), path


def _selection(value: object) -> dict[str, Any]:
    selection = _object(value, "fingerprint selection")
    mode = selection.get("mode")
    if mode == "full_track":
        _exact_keys(selection, "fingerprint selection", {"mode"})
    elif mode == "explicit_windows":
        _exact_keys(selection, "fingerprint selection", {"mode", "windows"})
        windows = _array(selection["windows"], "fingerprint explicit windows")
        for index, window_value in enumerate(windows):
            window = _object(window_value, f"fingerprint explicit windows[{index}]")
            _exact_keys(window, f"fingerprint explicit windows[{index}]", {"window_id", "start_ms", "end_ms"})
            _identifier(window["window_id"], f"fingerprint explicit windows[{index}].window_id")
            _integer(window["start_ms"], f"fingerprint explicit windows[{index}].start_ms")
            _integer(window["end_ms"], f"fingerprint explicit windows[{index}].end_ms", minimum=1)
    elif mode == "fixed_chunks":
        _exact_keys(
            selection,
            "fingerprint selection",
            {"mode", "chunk_duration_ms", "hop_ms", "include_partial_tail", "minimum_tail_ms"},
        )
        chunk = _integer(selection["chunk_duration_ms"], "fingerprint chunk_duration_ms", minimum=1_000)
        hop = _integer(selection["hop_ms"], "fingerprint hop_ms", minimum=1)
        tail = _integer(selection["minimum_tail_ms"], "fingerprint minimum_tail_ms", minimum=1)
        if hop > chunk or tail > chunk or not isinstance(selection["include_partial_tail"], bool):
            raise ResultImportError("fingerprint fixed-chunk parameters are inconsistent")
    else:
        raise ResultImportError("fingerprint selection mode is unsupported")
    return dict(selection)


def _configuration(value: object) -> dict[str, Any]:
    config = _object(value, "fingerprint configuration")
    _exact_keys(
        config,
        "fingerprint configuration",
        {"algorithm", "raw_format", "sample_rate_hz", "channels", "threads", "timeout_seconds", "selection"},
    )
    _integer(config["algorithm"], "fingerprint algorithm")
    if config["raw_format"] != RAW_FORMAT or config["sample_rate_hz"] != SAMPLE_RATE_HZ or config["channels"] != CHANNELS:
        raise ResultImportError("fingerprint raw format or normalization is unsupported")
    _integer(config["threads"], "fingerprint threads", minimum=1, maximum=64)
    _integer(config["timeout_seconds"], "fingerprint timeout", minimum=1, maximum=86_400)
    normalized = dict(config)
    normalized["selection"] = _selection(config["selection"])
    return normalized


def _expanded_windows(value: object, duration_ms: int) -> list[dict[str, Any]]:
    rows = _array(value, "fingerprint expanded_windows")
    if not 1 <= len(rows) <= 10_000:
        raise ResultImportError("fingerprint expanded_windows must contain 1..10000 items")
    result = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        row = _object(raw, f"fingerprint expanded_windows[{index}]")
        _exact_keys(row, f"fingerprint expanded_windows[{index}]", {"window_id", "window_kind", "start_ms", "end_ms"})
        window_id = _identifier(row["window_id"], f"fingerprint window[{index}].window_id")
        kind = row["window_kind"]
        if kind not in {"full_track", "explicit_window", "fixed_chunk", "partial_tail_chunk"}:
            raise ResultImportError(f"fingerprint window[{index}].window_kind is unsupported")
        start = _integer(row["start_ms"], f"fingerprint window[{index}].start_ms")
        end = _integer(row["end_ms"], f"fingerprint window[{index}].end_ms", minimum=1)
        if window_id in seen or start >= end or end > duration_ms:
            raise ResultImportError("fingerprint window IDs/half-open boundaries are invalid")
        seen.add(window_id)
        result.append({"window_id": window_id, "window_kind": kind, "start_ms": start, "end_ms": end})
    return result


def _expected_windows(config: dict[str, Any], duration_ms: int) -> list[dict[str, Any]]:
    selection = config["selection"]
    if selection["mode"] == "full_track":
        return [{"window_id": "full_track", "window_kind": "full_track", "start_ms": 0, "end_ms": duration_ms}]
    if selection["mode"] == "explicit_windows":
        return [
            {"window_id": row["window_id"], "window_kind": "explicit_window", "start_ms": row["start_ms"], "end_ms": row["end_ms"]}
            for row in selection["windows"]
        ]
    rows: list[dict[str, Any]] = []
    start = 0
    ordinal = 0
    chunk = selection["chunk_duration_ms"]
    while start < duration_ms:
        end = min(start + chunk, duration_ms)
        partial = end - start < chunk
        if not partial or (selection["include_partial_tail"] and end - start >= selection["minimum_tail_ms"]):
            rows.append({"window_id": f"chunk_{ordinal:06d}", "window_kind": "partial_tail_chunk" if partial else "fixed_chunk", "start_ms": start, "end_ms": end})
        start += selection["hop_ms"]
        ordinal += 1
        if len(rows) > 10_000:
            raise ResultImportError("fingerprint fixed-chunk expansion exceeds 10000 windows")
    return rows


def _context(value: object) -> dict[str, str] | None:
    if value is None:
        return None
    row = _object(value, "fingerprint catalog_context")
    _exact_keys(row, "fingerprint catalog_context", {"recording_id", "rendition_id"})
    return {
        "recording_id": _identifier(row["recording_id"], "fingerprint catalog_context.recording_id"),
        "rendition_id": _identifier(row["rendition_id"], "fingerprint catalog_context.rendition_id"),
    }


def _expected_command(engine_path: Path, input_path: Path, config: dict[str, Any], window: dict[str, Any]) -> list[str]:
    start_sample = window["start_ms"] * SAMPLE_RATE_HZ // 1_000
    end_sample = window["end_ms"] * SAMPLE_RATE_HZ // 1_000
    return [str(engine_path), "-hide_banner", "-nostdin", "-loglevel", "error", "-threads", str(config["threads"]), "-i", str(input_path), "-map", "0:a:0", "-af", f"atrim=start_sample={start_sample}:end_sample={end_sample},asetpts=PTS-STARTPTS", "-vn", "-sn", "-dn", "-ac", "1", "-ar", "16000", "-sample_fmt", "s16", "-f", "chromaprint", "-algorithm", str(config["algorithm"]), "-fp_format", "raw", "pipe:1"]


def _window_flags(window: dict[str, Any], byte_count: int) -> list[str]:
    duration = window["end_ms"] - window["start_ms"]
    flags = set()
    if duration < 10_000:
        flags.add("short_window_under_10s")
    if duration < 30_000:
        flags.add("short_window_under_30s")
    if window["window_kind"] == "partial_tail_chunk":
        flags.add("partial_tail_chunk")
    if byte_count == 0:
        flags.add("empty_raw_fingerprint")
    return sorted(flags)


def _run(value: object, recipe_id: str, input_sha: str, *, compare: bool = False) -> dict[str, Any]:
    run = _object(value, "fingerprint processing_run")
    _exact_keys(run, "fingerprint processing_run", {"processing_run_id", "execution_nonce", "started_at", "completed_at", "status"})
    nonce = _string(run["execution_nonce"], "fingerprint execution_nonce", maximum=32)
    if len(nonce) != 32 or any(character not in "0123456789abcdef" for character in nonce):
        raise ResultImportError("fingerprint execution_nonce must be 32 lowercase hex characters")
    run_id = _identifier(run["processing_run_id"], "fingerprint processing_run_id")
    expected = _producer_id(
        "run_audio_fingerprint_compare" if compare else "run_audio_fingerprint",
        recipe_id,
        nonce,
    ) if compare else _producer_id("run_audio_fingerprint", input_sha, recipe_id, nonce)
    if run_id != expected:
        raise ResultImportError("fingerprint processing-run ID is inconsistent")
    started = _timestamp(run["started_at"], "fingerprint started_at")
    completed = _timestamp(run["completed_at"], "fingerprint completed_at")
    if _timestamp_value(completed) < _timestamp_value(started) or run["status"] != "completed":
        raise ResultImportError("fingerprint run timestamps/status are inconsistent")
    return {**run, "started_at": started, "completed_at": completed}


def _validate_artifact(
    value: object,
    *,
    result_path: Path,
    run_id: str,
    window: dict[str, Any],
    input_media_id: str,
    recipe_id: str,
    algorithm: int,
) -> tuple[dict[str, Any], Path, str, int, list[str]]:
    item = _object(value, f"fingerprint item {window['window_id']}")
    _exact_keys(
        item,
        f"fingerprint item {window['window_id']}",
        {"fingerprint_id", "implementation_version", "window_id", "window_kind", "start_ms", "end_ms", "algorithm", "raw_format", "sample_rate_hz", "channels", "fingerprint_word_count", "quality_flags", "artifact"},
    )
    if any(item[key] != window[key] for key in ("window_id", "window_kind", "start_ms", "end_ms")):
        raise ResultImportError("fingerprint item does not mirror its expanded window")
    expected_fingerprint_id = _producer_id("fingerprint", input_media_id, recipe_id, window["start_ms"], window["end_ms"])
    if item["fingerprint_id"] != expected_fingerprint_id:
        raise ResultImportError("fingerprint ID is inconsistent")
    expected_impl = f"ffmpeg-chromaprint/{IMPLEMENTATION_VERSION}/{recipe_id}"
    if item["implementation_version"] != expected_impl:
        raise ResultImportError("fingerprint implementation identity is inconsistent")
    if item["algorithm"] != algorithm or item["raw_format"] != RAW_FORMAT or item["sample_rate_hz"] != SAMPLE_RATE_HZ or item["channels"] != CHANNELS:
        raise ResultImportError("fingerprint item format disagrees with its recipe")
    word_count = _integer(item["fingerprint_word_count"], "fingerprint word count")
    artifact = _object(item["artifact"], "fingerprint artifact")
    _exact_keys(artifact, "fingerprint artifact", {"artifact_id", "processing_run_id", "artifact_kind", "path", "storage_uri", "sha256", "byte_count", "schema_version", "visibility", "metadata_json"})
    if artifact["processing_run_id"] != run_id or artifact["artifact_kind"] != "audio_fingerprint_chromaprint_raw" or artifact["schema_version"] != 1 or artifact["visibility"] != "private":
        raise ResultImportError("fingerprint artifact catalog attributes are invalid")
    path = _absolute_observed_path(artifact["path"], "fingerprint artifact.path")
    uri_path = _local_file_uri(artifact["storage_uri"], "fingerprint artifact.storage_uri")
    if path != uri_path or path.lstat().st_mode & 0o222:
        raise ResultImportError("fingerprint artifact must be a sealed canonical local file")
    digest = _sha256(artifact["sha256"], "fingerprint artifact.sha256")
    byte_count = _integer(
        artifact["byte_count"],
        "fingerprint artifact.byte_count",
        maximum=MAX_FINGERPRINT_BYTES,
    )
    expected_path = result_path.parent / "artifacts" / window["window_id"] / "sha256" / digest[:2] / f"{digest}.chromaprint.raw"
    if path != expected_path:
        raise ResultImportError("fingerprint artifact path is outside its exact execution/window tree")
    if byte_count % 4 or word_count != byte_count // 4:
        raise ResultImportError("fingerprint raw byte/word counts are inconsistent")
    flags = _sorted_flags(item["quality_flags"], "fingerprint quality_flags", FINGERPRINT_FLAGS)
    if flags != _window_flags(window, byte_count):
        raise ResultImportError("fingerprint quality flags are inconsistent")
    expected_artifact_id = _producer_id("artifact_audio_fingerprint", run_id, window["window_id"], digest)
    if artifact["artifact_id"] != expected_artifact_id:
        raise ResultImportError("fingerprint artifact ID is inconsistent")
    metadata = _canonical_json_string(artifact["metadata_json"], "fingerprint artifact.metadata_json")
    expected_metadata = canonical_json({"algorithm": algorithm, "confidence_calibration": "none", "fingerprint_id": item["fingerprint_id"], "quality_flags": flags, "raw_format": RAW_FORMAT, "requires_human_review": True, "window_id": window["window_id"], "window_kind": window["window_kind"]})
    if metadata != expected_metadata:
        raise ResultImportError("fingerprint artifact metadata is inconsistent")
    _verify_hash(path, digest, byte_count, "fingerprint raw artifact")
    return dict(item), path, digest, byte_count, flags


def _recipe_payload(input_row: dict[str, Any], engine: dict[str, Any], config: dict[str, Any], windows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION, "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "input_sha256": input_row["sha256"], "input_duration_ms": input_row["duration_ms"],
        "engine": {key: engine[key] for key in ("sha256", "byte_count", "version_label", "version_output_sha256", "build_configuration", "muxer_help_sha256")},
        "fingerprint": config, "expanded_windows": windows,
    }


def validate_audio_fingerprint_result_file(path: Path) -> dict[str, Any]:
    result, body, result_path = _load_sealed_result(path)
    _exact_keys(result, "fingerprint result", {"schema_version", "stage", "implementation_version", "status", "dry_run", "job_id", "recipe_id", "recipe_sha256", "input", "engine", "fingerprint", "expanded_windows", "processing_run", "commands", "fingerprints", "quality_flags", "catalog_context", "result_path", "errors"})
    if result["schema_version"] != SCHEMA_VERSION or result["stage"] != STAGE or result["implementation_version"] != IMPLEMENTATION_VERSION:
        raise ResultImportError("fingerprint result contract identity is unsupported")
    if result["status"] != "completed" or result["dry_run"] is not False or result["errors"] != []:
        raise ResultImportError("only successful non-dry-run fingerprint results are importable")
    _identifier(result["job_id"], "fingerprint job_id")
    if result["result_path"] != str(result_path):
        raise ResultImportError("fingerprint result_path does not identify the imported file")
    input_row, input_path = _input(result["input"])
    engine, engine_path = _engine(result["engine"])
    config = _configuration(result["fingerprint"])
    windows = _expanded_windows(result["expanded_windows"], input_row["duration_ms"])
    if windows != _expected_windows(config, input_row["duration_ms"]):
        raise ResultImportError("fingerprint expanded windows disagree with selection")
    recipe = _recipe_payload(input_row, engine, config, windows)
    recipe_sha = sha256_bytes(canonical_json(recipe).encode("utf-8"))
    recipe_id = f"recipe_audio_fingerprint_{recipe_sha[:32]}"
    if result["recipe_sha256"] != recipe_sha or result["recipe_id"] != recipe_id:
        raise ResultImportError("fingerprint recipe identity is inconsistent")
    run = _run(result["processing_run"], recipe_id, input_row["sha256"])
    commands = _array(result["commands"], "fingerprint commands")
    expected_commands = [_expected_command(engine_path, input_path, config, window) for window in windows]
    if not _canonical_equal(commands, expected_commands):
        raise ResultImportError("fingerprint commands disagree with the exact recipe")
    raw_items = _array(result["fingerprints"], "fingerprints")
    if len(raw_items) != len(windows):
        raise ResultImportError("fingerprint item/window counts disagree")
    items = []
    aggregate: set[str] = set()
    for raw_item, window in zip(raw_items, windows, strict=True):
        item, artifact_path, digest, byte_count, flags = _validate_artifact(raw_item, result_path=result_path, run_id=run["processing_run_id"], window=window, input_media_id=input_row["media_id"], recipe_id=recipe_id, algorithm=config["algorithm"])
        item["_artifact_path"] = artifact_path
        items.append(item)
        aggregate.update(flags)
    if _sorted_flags(result["quality_flags"], "fingerprint aggregate quality_flags", FINGERPRINT_FLAGS) != sorted(aggregate):
        raise ResultImportError("fingerprint aggregate quality flags are inconsistent")
    context = _context(result["catalog_context"])
    normalized = dict(result)
    normalized.update({"input": input_row, "engine": engine, "fingerprint": config, "expanded_windows": windows, "processing_run": run, "fingerprints": items, "catalog_context": context, "_result_path": result_path, "_result_sha256": sha256_bytes(body), "_result_byte_count": len(body)})
    return normalized


def _require_extraction_dependencies(connection, result: dict[str, Any]) -> None:
    input_row = result["input"]
    media = connection.execute("SELECT sha256, byte_count, duration_ms FROM media_objects WHERE media_id = ?", (input_row["media_id"],)).fetchone()
    if media is None or media["sha256"] != input_row["sha256"] or media["byte_count"] != input_row["byte_count"] or media["duration_ms"] != input_row["duration_ms"]:
        raise ResultImportError("fingerprint input media dependency is missing or differs")
    artifact = connection.execute("SELECT processing_run_id, storage_uri, sha256, byte_count, visibility FROM artifacts WHERE artifact_id = ?", (input_row["artifact_id"],)).fetchone()
    if artifact is None or artifact["processing_run_id"] != input_row["parent_processing_run_id"] or artifact["storage_uri"] != input_row["storage_uri"] or artifact["sha256"] != input_row["sha256"] or artifact["byte_count"] != input_row["byte_count"] or artifact["visibility"] != "private":
        raise ResultImportError("fingerprint normalized-audio artifact dependency is missing or differs")
    context = result["catalog_context"]
    if context is None:
        return
    rendition = connection.execute("SELECT recording_id, media_id FROM renditions WHERE rendition_id = ?", (context["rendition_id"],)).fetchone()
    if rendition is None or rendition["recording_id"] != context["recording_id"] or rendition["media_id"] != input_row["media_id"]:
        raise ResultImportError("fingerprint context rendition/recording/media dependency differs")


def _reverify_extraction_files(result: dict[str, Any]) -> None:
    """Close the validation/transaction gap for every local evidence file."""

    for label, path, digest, byte_count in (
        (
            "fingerprint sealed result",
            result["_result_path"],
            result["_result_sha256"],
            result["_result_byte_count"],
        ),
        (
            "fingerprint normalized-audio input",
            Path(result["input"]["path"]),
            result["input"]["sha256"],
            result["input"]["byte_count"],
        ),
        (
            "fingerprint FFmpeg executable",
            Path(result["engine"]["path"]),
            result["engine"]["sha256"],
            result["engine"]["byte_count"],
        ),
    ):
        if path.lstat().st_mode & 0o222 and label != "fingerprint FFmpeg executable":
            raise ResultImportError(f"{label} is no longer sealed read-only")
        _verify_hash(path, digest, byte_count, label)
    for item in result["fingerprints"]:
        path = item["_artifact_path"]
        if path.lstat().st_mode & 0o222:
            raise ResultImportError("fingerprint raw artifact is no longer sealed read-only")
        _verify_hash(
            path,
            item["artifact"]["sha256"],
            item["artifact"]["byte_count"],
            "fingerprint raw artifact",
        )


def _insert_processing_run(connection, result: dict[str, Any], *, compare: bool = False) -> None:
    run = result["processing_run"]
    stage = COMPARE_STAGE if compare else STAGE
    parameters = canonical_json({"recipe_id": result["recipe_id"], "recipe_sha256": result["recipe_sha256"], **({"method": result["method"]} if compare else {"fingerprint": result["fingerprint"], "expanded_windows": result["expanded_windows"]})})
    environment = canonical_json(
        {
            "comparison_runtime": "python-standard-library",
            "implementation_version": result["implementation_version"],
        }
        if compare
        else {"engine": result["engine"]}
    )
    values = {"stage": stage, "implementation_version": result["implementation_version"], "model_id": None, "glossary_revision_id": None, "parameters_json": parameters, "environment_json": environment, "random_seed": None, "started_at": run["started_at"], "completed_at": run["completed_at"], "status": "completed", "error_text": None}
    existing = connection.execute("SELECT stage, implementation_version, model_id, glossary_revision_id, parameters_json, environment_json, random_seed, started_at, completed_at, status, error_text FROM processing_runs WHERE processing_run_id = ?", (run["processing_run_id"],)).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in values.items()):
            raise ResultImportError("fingerprint processing run ID already has different data")
        return
    connection.execute("INSERT INTO processing_runs(processing_run_id, stage, implementation_version, model_id, glossary_revision_id, parameters_json, environment_json, random_seed, started_at, completed_at, status, error_text) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (run["processing_run_id"], *values.values()))


def _insert_run_input(connection, run_id: str, object_type: str, object_id: str, role: str, digest: str | None) -> None:
    row_id = _producer_id("run_input", run_id, object_type, object_id, role)
    values = (run_id, object_type, object_id, role, digest)
    existing = connection.execute("SELECT processing_run_id, object_type, object_id, input_role, input_sha256 FROM run_inputs WHERE run_input_id = ?", (row_id,)).fetchone()
    if existing is not None:
        if tuple(existing) != values:
            raise ResultImportError("fingerprint run-input ID already has different data")
        return
    collision = connection.execute("SELECT run_input_id FROM run_inputs WHERE processing_run_id = ? AND object_type = ? AND object_id = ? AND input_role = ?", values[:4]).fetchone()
    if collision is not None:
        raise ResultImportError("fingerprint logical run input already has a different ID")
    connection.execute("INSERT INTO run_inputs(run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256) VALUES(?, ?, ?, ?, ?, ?)", (row_id, *values))


def _insert_artifact(connection, row: dict[str, Any]) -> None:
    columns = ("processing_run_id", "artifact_kind", "storage_uri", "sha256", "byte_count", "schema_version", "visibility", "metadata_json")
    existing = connection.execute(f"SELECT {', '.join(columns)} FROM artifacts WHERE artifact_id = ?", (row["artifact_id"],)).fetchone()
    if existing is not None:
        if any(existing[key] != row[key] for key in columns):
            raise ResultImportError("fingerprint artifact ID already has different data")
        return
    collision = connection.execute("SELECT artifact_id FROM artifacts WHERE storage_uri = ? AND sha256 = ?", (row["storage_uri"], row["sha256"])).fetchone()
    if collision is not None:
        raise ResultImportError("fingerprint artifact URI/digest already has a different ID")
    connection.execute("INSERT INTO artifacts(artifact_id, processing_run_id, artifact_kind, storage_uri, sha256, byte_count, schema_version, visibility, metadata_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", tuple(row[key] for key in ("artifact_id", *columns)))


def _insert_fingerprint_evidence(connection, result: dict[str, Any]) -> tuple[int, int]:
    context = result["catalog_context"]
    if context is None:
        return 0, 0
    fingerprint_count = 0
    observation_count = 0
    for item in result["fingerprints"]:
        fingerprint_columns = {"media_id": result["input"]["media_id"], "fingerprint_kind": "chromaprint_raw", "implementation_version": item["implementation_version"], "start_ms": item["start_ms"], "end_ms": item["end_ms"], "value_text": None}
        existing = connection.execute("SELECT media_id, fingerprint_kind, implementation_version, start_ms, end_ms, value_text, artifact_uri FROM fingerprints WHERE fingerprint_id = ?", (item["fingerprint_id"],)).fetchone()
        if existing is None:
            collision = connection.execute("SELECT fingerprint_id FROM fingerprints WHERE media_id = ? AND fingerprint_kind = ? AND implementation_version = ? AND start_ms = ? AND end_ms = ?", (fingerprint_columns["media_id"], fingerprint_columns["fingerprint_kind"], fingerprint_columns["implementation_version"], fingerprint_columns["start_ms"], fingerprint_columns["end_ms"])).fetchone()
            if collision is not None:
                raise ResultImportError("fingerprint semantic identity already uses a different ID")
            connection.execute(
                "INSERT INTO fingerprints(fingerprint_id, media_id, fingerprint_kind, implementation_version, start_ms, end_ms, value_text, artifact_uri) VALUES(?, ?, ?, ?, ?, ?, NULL, ?)",
                (
                    item["fingerprint_id"],
                    fingerprint_columns["media_id"],
                    fingerprint_columns["fingerprint_kind"],
                    fingerprint_columns["implementation_version"],
                    fingerprint_columns["start_ms"],
                    fingerprint_columns["end_ms"],
                    item["artifact"]["storage_uri"],
                ),
            )
            fingerprint_count += 1
        elif any(existing[key] != value for key, value in fingerprint_columns.items()):
            raise ResultImportError("fingerprint ID already has different semantic data")
        observation_id = _producer_id("observation_audio_fingerprint", result["processing_run"]["processing_run_id"], item["fingerprint_id"], context["recording_id"], context["rendition_id"])
        metadata = canonical_json({"algorithm": item["algorithm"], "artifact_id": item["artifact"]["artifact_id"], "confidence_calibration": "none", "fingerprint_id": item["fingerprint_id"], "raw_format": item["raw_format"], "requires_human_review": True, "window_id": item["window_id"], "window_kind": item["window_kind"]})
        obs_values = {"observation_kind": "audio_fingerprint", "recording_id": context["recording_id"], "rendition_id": context["rendition_id"], "processing_run_id": result["processing_run"]["processing_run_id"], "start_ms": item["start_ms"], "end_ms": item["end_ms"], "visibility": "private", "review_state": "machine", "payload_schema_version": 1, "metadata_json": metadata, "created_at": result["processing_run"]["completed_at"]}
        existing_obs = connection.execute(f"SELECT {', '.join(obs_values)} FROM observations WHERE observation_id = ?", (observation_id,)).fetchone()
        if existing_obs is None:
            connection.execute(f"INSERT INTO observations(observation_id, {', '.join(obs_values)}) VALUES(?, {', '.join('?' for _ in obs_values)})", (observation_id, *obs_values.values()))
            connection.execute("INSERT INTO audio_fingerprint_observations(observation_id, fingerprint_id, artifact_id, window_kind, algorithm, raw_format, sample_rate_hz, channels, fingerprint_word_count, requires_human_review) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 1)", (observation_id, item["fingerprint_id"], item["artifact"]["artifact_id"], item["window_kind"], item["algorithm"], item["raw_format"], item["sample_rate_hz"], item["channels"], item["fingerprint_word_count"]))
            score_id = _producer_id("observation_score", observation_id, "fingerprint_word_count")
            connection.execute("INSERT INTO observation_scores(observation_score_id, observation_id, score_name, raw_score, calibrated_probability, calibration_set_id, quality_flags_json) VALUES(?, ?, 'fingerprint_word_count', ?, NULL, NULL, ?)", (score_id, observation_id, float(item["fingerprint_word_count"]), canonical_json(item["quality_flags"])))
            observation_count += 1
        elif any(existing_obs[key] != value for key, value in obs_values.items()):
            raise ResultImportError("fingerprint observation ID already has different data")
        else:
            subtype = connection.execute(
                """
                SELECT fingerprint_id, artifact_id, window_kind, algorithm, raw_format,
                       sample_rate_hz, channels, fingerprint_word_count,
                       requires_human_review
                FROM audio_fingerprint_observations WHERE observation_id = ?
                """,
                (observation_id,),
            ).fetchone()
            expected_subtype = (
                item["fingerprint_id"], item["artifact"]["artifact_id"],
                item["window_kind"], item["algorithm"], item["raw_format"],
                item["sample_rate_hz"], item["channels"],
                item["fingerprint_word_count"], 1,
            )
            if subtype is None or tuple(subtype) != expected_subtype:
                raise ResultImportError(
                    "fingerprint observation subtype is missing or differs"
                )
            score = connection.execute(
                """
                SELECT score_name, raw_score, calibrated_probability,
                       calibration_set_id, quality_flags_json
                FROM observation_scores
                WHERE observation_score_id = ?
                """,
                (_producer_id("observation_score", observation_id, "fingerprint_word_count"),),
            ).fetchone()
            expected_score = (
                "fingerprint_word_count", float(item["fingerprint_word_count"]),
                None, None, canonical_json(item["quality_flags"]),
            )
            if score is None or tuple(score) != expected_score:
                raise ResultImportError(
                    "fingerprint observation score is missing or differs"
                )
    return fingerprint_count, observation_count


def _insert_import_ledger(connection, result: dict[str, Any], kind: str) -> None:
    batch_id = _producer_id("audio_fingerprint_import", kind, result["_result_sha256"])
    values = (kind, result["_result_sha256"], result["processing_run"]["processing_run_id"], result["recipe_id"], canonical_json(result["catalog_context"]))
    existing = connection.execute("SELECT result_kind, result_sha256, processing_run_id, recipe_id, catalog_context_json FROM audio_fingerprint_result_imports WHERE import_batch_id = ?", (batch_id,)).fetchone()
    if existing is not None:
        if tuple(existing) != values:
            raise ResultImportError("fingerprint import batch ID already has different data")
        return
    connection.execute("INSERT INTO audio_fingerprint_result_imports(import_batch_id, result_kind, result_sha256, processing_run_id, recipe_id, catalog_context_json, imported_at) VALUES(?, ?, ?, ?, ?, ?, ?)", (batch_id, *values, datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")))


def import_audio_fingerprint_result(connection, path: Path) -> dict[str, Any]:
    result = validate_audio_fingerprint_result_file(path)
    with transaction(connection):
        _reverify_extraction_files(result)
        _require_extraction_dependencies(connection, result)
        _insert_processing_run(connection, result)
        _insert_run_input(connection, result["processing_run"]["processing_run_id"], "media_object", result["input"]["media_id"], "normalized_audio", result["input"]["sha256"])
        for item in result["fingerprints"]:
            _insert_artifact(connection, item["artifact"])
        fingerprint_count, observation_count = _insert_fingerprint_evidence(connection, result)
        _insert_import_ledger(connection, result, "extraction")
    return {"importer_version": __version__, "result_sha256": result["_result_sha256"], "recipe_id": result["recipe_id"], "processing_run_id": result["processing_run"]["processing_run_id"], "catalog_context": result["catalog_context"], "artifacts": len(result["fingerprints"]), "fingerprints_inserted_or_present": len(result["fingerprints"]) if result["catalog_context"] else 0, "observations_inserted_or_present": len(result["fingerprints"]) if result["catalog_context"] else 0, "provenance_only": result["catalog_context"] is None, "publication_decisions": 0}


def _compare_side(value: object, label: str) -> tuple[dict[str, Any], Path, bytes]:
    side = _object(value, label)
    _exact_keys(side, label, {"role", "path", "storage_uri", "sha256", "byte_count", "artifact_id", "fingerprint_id", "media_id", "implementation_version", "algorithm", "raw_format", "sample_rate_hz", "channels", "window_kind", "start_ms", "end_ms", "fingerprint_word_count", "quality_flags", "unchanged"})
    if side["role"] not in {"query", "candidate"} or side["unchanged"] is not True:
        raise ResultImportError(f"{label} role/integrity is invalid")
    path = _absolute_observed_path(side["path"], f"{label}.path")
    if path.lstat().st_mode & 0o222:
        raise ResultImportError(f"{label} must be a sealed read-only artifact")
    if _local_file_uri(side["storage_uri"], f"{label}.storage_uri") != path:
        raise ResultImportError(f"{label} path and URI disagree")
    digest = _sha256(side["sha256"], f"{label}.sha256")
    byte_count = _integer(
        side["byte_count"],
        f"{label}.byte_count",
        maximum=MAX_FINGERPRINT_BYTES,
    )
    _identifier(side["artifact_id"], f"{label}.artifact_id"); _identifier(side["fingerprint_id"], f"{label}.fingerprint_id"); _identifier(side["media_id"], f"{label}.media_id")
    implementation = _string(side["implementation_version"], f"{label}.implementation_version")
    expected_prefix = f"ffmpeg-chromaprint/{IMPLEMENTATION_VERSION}/recipe_audio_fingerprint_"
    suffix = implementation.removeprefix(expected_prefix)
    if (
        not implementation.startswith(expected_prefix)
        or len(suffix) != 32
        or any(character not in "0123456789abcdef" for character in suffix)
    ):
        raise ResultImportError(f"{label}.implementation_version is unsupported")
    _integer(side["algorithm"], f"{label}.algorithm")
    if side["raw_format"] != RAW_FORMAT or side["sample_rate_hz"] != SAMPLE_RATE_HZ or side["channels"] != CHANNELS:
        raise ResultImportError(f"{label} raw format/normalization is unsupported")
    _string(side["window_kind"], f"{label}.window_kind")
    if side["window_kind"] not in {"full_track", "explicit_window", "fixed_chunk", "partial_tail_chunk"}:
        raise ResultImportError(f"{label}.window_kind is unsupported")
    start = _integer(side["start_ms"], f"{label}.start_ms"); end = _integer(side["end_ms"], f"{label}.end_ms", minimum=1)
    words = _integer(side["fingerprint_word_count"], f"{label}.fingerprint_word_count")
    if end <= start or byte_count % 4 or words != byte_count // 4:
        raise ResultImportError(f"{label} interval/raw word count is invalid")
    flags = _sorted_flags(side["quality_flags"], f"{label}.quality_flags", FINGERPRINT_FLAGS)
    if flags != _window_flags(side, byte_count):
        raise ResultImportError(f"{label}.quality_flags disagree with its exact window/bytes")
    body = _stable_read(path, label, maximum_bytes=MAX_FINGERPRINT_BYTES)
    if len(body) != byte_count or sha256_bytes(body) != digest:
        raise ResultImportError(f"{label} current bytes differ from their digest/count")
    return dict(side), path, body


def _pair_context(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    context = _object(value, "fingerprint pair catalog_context")
    _exact_keys(context, "fingerprint pair catalog_context", {"query", "candidate"})
    result = {}
    for name in ("query", "candidate"):
        side = _object(context[name], f"fingerprint pair context.{name}")
        _exact_keys(side, f"fingerprint pair context.{name}", {"recording_id", "rendition_id"})
        result[name] = {"recording_id": _identifier(side["recording_id"], f"pair {name} recording_id"), "rendition_id": _identifier(side["rendition_id"], f"pair {name} rendition_id")}
    return result


def _expected_compare_flags(query: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    flags = {"exact_comparison_only_no_alignment"}
    query_duration = query["end_ms"] - query["start_ms"]
    candidate_duration = candidate["end_ms"] - candidate["start_ms"]
    if query_duration != candidate_duration: flags.add("cross_duration_windows")
    if query_duration < 10_000: flags.add("short_query_window_under_10s")
    if candidate_duration < 10_000: flags.add("short_candidate_window_under_10s")
    if query["byte_count"] == 0: flags.add("empty_query_fingerprint")
    if candidate["byte_count"] == 0: flags.add("empty_candidate_fingerprint")
    return sorted(flags)


def _v2_engine_binding(engine: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": engine["name"],
        "path": engine["path"],
        "sha256": engine["sha256"],
        "byte_count": engine["byte_count"],
        "version_label": engine["version_label"],
        "version_output_sha256": engine["version_output_sha256"],
        "build_configuration": engine["build_configuration"],
        "muxer_help_sha256": engine["muxer_help_sha256"],
        "unchanged": True,
    }


def _v2_compatibility_engine(engine: dict[str, Any]) -> dict[str, Any]:
    return {
        key: engine[key]
        for key in (
            "name",
            "sha256",
            "byte_count",
            "version_label",
            "version_output_sha256",
            "build_configuration",
            "muxer_help_sha256",
        )
    }


def _expected_compare_v2_side(
    role: str, extraction: dict[str, Any], item: dict[str, Any]
) -> dict[str, Any]:
    artifact = item["artifact"]
    return {
        "role": role,
        "extraction_result": {
            "path": str(extraction["_result_path"]),
            "storage_uri": extraction["_result_path"].as_uri(),
            "sha256": extraction["_result_sha256"],
            "byte_count": extraction["_result_byte_count"],
            "recipe_id": extraction["recipe_id"],
            "recipe_sha256": extraction["recipe_sha256"],
            "processing_run_id": extraction["processing_run"]["processing_run_id"],
            "implementation_version": extraction["implementation_version"],
            "unchanged": True,
        },
        "catalog_context": extraction["catalog_context"],
        "input": {
            key: extraction["input"][key]
            for key in (
                "path",
                "storage_uri",
                "sha256",
                "byte_count",
                "media_id",
                "artifact_id",
                "parent_processing_run_id",
                "unchanged",
            )
        },
        "engine": _v2_engine_binding(extraction["engine"]),
        "fingerprint": {
            "fingerprint_id": item["fingerprint_id"],
            "producer_implementation_version": item["implementation_version"],
            **{
                key: item[key]
                for key in (
                    "window_id",
                    "window_kind",
                    "start_ms",
                    "end_ms",
                    "algorithm",
                    "raw_format",
                    "sample_rate_hz",
                    "channels",
                    "fingerprint_word_count",
                    "quality_flags",
                )
            },
            "artifact": {
                key: artifact[key]
                for key in (
                    "artifact_id",
                    "processing_run_id",
                    "path",
                    "storage_uri",
                    "sha256",
                    "byte_count",
                    "visibility",
                )
            }
            | {"unchanged": True},
        },
    }


def _compare_v2_side(value: object, role: str) -> dict[str, Any]:
    side = _object(value, f"fingerprint v2 {role}")
    _exact_keys(
        side,
        f"fingerprint v2 {role}",
        {"role", "extraction_result", "catalog_context", "input", "engine", "fingerprint"},
    )
    if side["role"] != role:
        raise ResultImportError(f"fingerprint v2 {role} role disagrees with its position")
    envelope = _object(
        side["extraction_result"], f"fingerprint v2 {role}.extraction_result"
    )
    _exact_keys(
        envelope,
        f"fingerprint v2 {role}.extraction_result",
        {
            "path",
            "storage_uri",
            "sha256",
            "byte_count",
            "recipe_id",
            "recipe_sha256",
            "processing_run_id",
            "implementation_version",
            "unchanged",
        },
    )
    result_path = _absolute_observed_path(
        envelope["path"], f"fingerprint v2 {role} extraction result path"
    )
    if result_path.lstat().st_mode & 0o222:
        raise ResultImportError(f"fingerprint v2 {role} extraction result must be sealed")
    if _local_file_uri(
        envelope["storage_uri"], f"fingerprint v2 {role} extraction result URI"
    ) != result_path:
        raise ResultImportError(f"fingerprint v2 {role} extraction result path/URI disagree")
    expected_result_sha = _sha256(
        envelope["sha256"], f"fingerprint v2 {role} extraction result SHA-256"
    )
    extraction = validate_audio_fingerprint_result_file(result_path)
    if extraction["catalog_context"] is None:
        raise ResultImportError(
            f"fingerprint v2 {role} extraction requires catalog lineage"
        )
    if extraction["_result_sha256"] != expected_result_sha:
        raise ResultImportError(
            f"fingerprint v2 {role} extraction result differs from its expected SHA-256"
        )
    fingerprint = _object(side["fingerprint"], f"fingerprint v2 {role}.fingerprint")
    selected_id = _identifier(
        fingerprint.get("fingerprint_id"), f"fingerprint v2 {role}.fingerprint_id"
    )
    selected = [
        item for item in extraction["fingerprints"] if item["fingerprint_id"] == selected_id
    ]
    if len(selected) != 1:
        raise ResultImportError(
            f"fingerprint v2 {role} must select exactly one extraction fingerprint"
        )
    item = selected[0]
    expected = _expected_compare_v2_side(role, extraction, item)
    if not _canonical_equal(side, expected):
        raise ResultImportError(
            f"fingerprint v2 {role} does not exactly mirror its extraction envelope"
        )
    artifact_path = Path(item["artifact"]["path"])
    artifact_body = _stable_read(
        artifact_path,
        f"fingerprint v2 {role} selected raw artifact",
        maximum_bytes=MAX_FINGERPRINT_BYTES,
    )
    return {
        **expected,
        "_extraction": extraction,
        "_item": item,
        "_artifact_path": artifact_path,
        "_artifact_body": artifact_body,
    }


def _compare_v2_recipe_side(side: dict[str, Any]) -> dict[str, Any]:
    fingerprint = side["fingerprint"]
    return {
        "role": side["role"],
        "extraction_result_sha256": side["extraction_result"]["sha256"],
        "extraction_recipe_id": side["extraction_result"]["recipe_id"],
        "extraction_recipe_sha256": side["extraction_result"]["recipe_sha256"],
        "extraction_processing_run_id": side["extraction_result"]["processing_run_id"],
        "catalog_context": side["catalog_context"],
        "media_id": side["input"]["media_id"],
        "input_sha256": side["input"]["sha256"],
        "engine": _v2_compatibility_engine(side["engine"]),
        "fingerprint_id": fingerprint["fingerprint_id"],
        "producer_implementation_version": fingerprint["producer_implementation_version"],
        "artifact_id": fingerprint["artifact"]["artifact_id"],
        "artifact_sha256": fingerprint["artifact"]["sha256"],
        "artifact_byte_count": fingerprint["artifact"]["byte_count"],
        **{
            key: fingerprint[key]
            for key in (
                "algorithm",
                "raw_format",
                "sample_rate_hz",
                "channels",
                "start_ms",
                "end_ms",
            )
        },
    }


def _compare_v2_recipe(query: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "stage": COMPARE_V2_STAGE,
        "implementation_version": COMPARE_V2_IMPLEMENTATION_VERSION,
        "method": "exact_raw_bytes_v2",
        "compatibility_contract": "engine_build_algorithm_format_normalization_v2",
        "query": _compare_v2_recipe_side(query),
        "candidate": _compare_v2_recipe_side(candidate),
    }


def _expected_compare_v2_flags(
    query: dict[str, Any], candidate: dict[str, Any]
) -> list[str]:
    query_fingerprint = query["fingerprint"]
    candidate_fingerprint = candidate["fingerprint"]
    flags = set(
        _expected_compare_flags(
            {
                "start_ms": query_fingerprint["start_ms"],
                "end_ms": query_fingerprint["end_ms"],
                "byte_count": query_fingerprint["artifact"]["byte_count"],
            },
            {
                "start_ms": candidate_fingerprint["start_ms"],
                "end_ms": candidate_fingerprint["end_ms"],
                "byte_count": candidate_fingerprint["artifact"]["byte_count"],
            },
        )
    )
    if query["catalog_context"]["recording_id"] != candidate["catalog_context"]["recording_id"]:
        flags.add("cross_recording")
    if query["catalog_context"]["rendition_id"] != candidate["catalog_context"]["rendition_id"]:
        flags.add("cross_rendition")
    if query["input"]["media_id"] != candidate["input"]["media_id"]:
        flags.add("cross_input_media")
    if query["extraction_result"]["recipe_id"] != candidate["extraction_result"]["recipe_id"]:
        flags.add("cross_extraction_recipe")
    return sorted(flags)


def _compare_v2_run(value: object, recipe_id: str) -> dict[str, Any]:
    run = _object(value, "fingerprint v2 processing_run")
    _exact_keys(
        run,
        "fingerprint v2 processing_run",
        {"processing_run_id", "execution_nonce", "started_at", "completed_at", "status"},
    )
    nonce = _string(run["execution_nonce"], "fingerprint v2 execution_nonce", maximum=32)
    if len(nonce) != 32 or any(character not in "0123456789abcdef" for character in nonce):
        raise ResultImportError(
            "fingerprint v2 execution_nonce must be 32 lowercase hex characters"
        )
    run_id = _identifier(run["processing_run_id"], "fingerprint v2 processing_run_id")
    expected = _producer_id("run_audio_fingerprint_compare_v2", recipe_id, nonce)
    if run_id != expected:
        raise ResultImportError("fingerprint v2 processing-run ID is inconsistent")
    started = _timestamp(run["started_at"], "fingerprint v2 started_at")
    completed = _timestamp(run["completed_at"], "fingerprint v2 completed_at")
    if _timestamp_value(completed) < _timestamp_value(started) or run["status"] != "completed":
        raise ResultImportError("fingerprint v2 run timestamps/status are inconsistent")
    return {**run, "started_at": started, "completed_at": completed}


def _validate_audio_fingerprint_compare_v2_loaded(
    result: dict[str, Any], body: bytes, result_path: Path
) -> dict[str, Any]:
    _exact_keys(
        result,
        "fingerprint compare v2 result",
        {
            "schema_version",
            "stage",
            "implementation_version",
            "status",
            "dry_run",
            "job_id",
            "recipe_id",
            "recipe_sha256",
            "method",
            "compatibility_contract",
            "query",
            "candidate",
            "comparison",
            "processing_run",
            "catalog_context",
            "visibility",
            "publication_authority",
            "result_path",
            "errors",
        },
    )
    if (
        result["schema_version"] != 2
        or result["stage"] != COMPARE_V2_STAGE
        or result["implementation_version"] != COMPARE_V2_IMPLEMENTATION_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
        or result["method"] != "exact_raw_bytes_v2"
        or result["compatibility_contract"]
        != "engine_build_algorithm_format_normalization_v2"
        or result["visibility"] != "private"
        or result["publication_authority"] != "none"
    ):
        raise ResultImportError("fingerprint compare v2 contract/status is unsupported")
    _identifier(result["job_id"], "fingerprint compare v2 job_id")
    if result["result_path"] != str(result_path):
        raise ResultImportError("fingerprint compare v2 result_path disagrees with imported file")
    query = _compare_v2_side(result["query"], "query")
    candidate = _compare_v2_side(result["candidate"], "candidate")
    if query["fingerprint"]["fingerprint_id"] == candidate["fingerprint"]["fingerprint_id"]:
        raise ResultImportError("fingerprint compare v2 self-pair is invalid")
    if _v2_compatibility_engine(query["engine"]) != _v2_compatibility_engine(candidate["engine"]):
        raise ResultImportError("fingerprint compare v2 requires identical engine/build identity")
    for key in ("algorithm", "raw_format", "sample_rate_hz", "channels"):
        if query["fingerprint"][key] != candidate["fingerprint"][key]:
            raise ResultImportError(f"fingerprint compare v2 requires identical {key}")
    context = _pair_context(result["catalog_context"])
    expected_context = {
        "query": query["catalog_context"],
        "candidate": candidate["catalog_context"],
    }
    if context != expected_context:
        raise ResultImportError("fingerprint compare v2 catalog lineage is inconsistent")
    recipe = _compare_v2_recipe(query, candidate)
    recipe_sha = sha256_bytes(canonical_json(recipe).encode("utf-8"))
    recipe_id = f"recipe_audio_fingerprint_compare_v2_{recipe_sha[:32]}"
    if result["recipe_sha256"] != recipe_sha or result["recipe_id"] != recipe_id:
        raise ResultImportError("fingerprint compare v2 recipe identity is inconsistent")
    run = _compare_v2_run(result["processing_run"], recipe_id)
    comparison = _object(result["comparison"], "fingerprint v2 comparison")
    _exact_keys(
        comparison,
        "fingerprint v2 comparison",
        {
            "match_candidate_id",
            "exact_raw_equal",
            "raw_score",
            "score_semantics",
            "calibration_state",
            "calibrated_probability",
            "quality_flags",
            "decision_state",
            "requires_human_review",
            "relationship_asserted",
            "visibility",
            "publication_authority",
        },
    )
    expected_match_id = _producer_id(
        "fingerprint_match_candidate_v2",
        run["processing_run_id"],
        query["fingerprint"]["fingerprint_id"],
        candidate["fingerprint"]["fingerprint_id"],
    )
    if (
        comparison["match_candidate_id"] != expected_match_id
        or not isinstance(comparison["exact_raw_equal"], bool)
        or _number(comparison["raw_score"], "fingerprint v2 raw score") not in {0.0, 1.0}
    ):
        raise ResultImportError("fingerprint v2 exact comparison identity/score is invalid")
    observed_equal = query["_artifact_body"] == candidate["_artifact_body"]
    if (
        comparison["exact_raw_equal"] != observed_equal
        or comparison["raw_score"] != (1.0 if observed_equal else 0.0)
    ):
        raise ResultImportError(
            "fingerprint v2 exact comparison disagrees with current raw bytes"
        )
    if (
        comparison["score_semantics"] != "boolean_raw_byte_equality_not_probability"
        or comparison["calibration_state"] != "not_calibrated"
        or comparison["calibrated_probability"] is not None
        or comparison["decision_state"] != "candidate"
        or comparison["requires_human_review"] is not True
        or comparison["relationship_asserted"] is not False
        or comparison["visibility"] != "private"
        or comparison["publication_authority"] != "none"
    ):
        raise ResultImportError(
            "fingerprint v2 comparison must remain private uncalibrated review evidence"
        )
    flags = _sorted_flags(
        comparison["quality_flags"],
        "fingerprint compare v2 quality_flags",
        COMPARE_V2_FLAGS,
    )
    if flags != _expected_compare_v2_flags(query, candidate):
        raise ResultImportError("fingerprint compare v2 quality flags are inconsistent")
    normalized = dict(result)
    normalized.update(
        {
            "query": query,
            "candidate": candidate,
            "comparison": dict(comparison),
            "processing_run": run,
            "catalog_context": context,
            "_result_path": result_path,
            "_result_sha256": sha256_bytes(body),
            "_result_byte_count": len(body),
        }
    )
    return normalized


def validate_audio_fingerprint_compare_result_file(path: Path) -> dict[str, Any]:
    result, body, result_path = _load_sealed_result(path)
    if result.get("schema_version") == 2:
        return _validate_audio_fingerprint_compare_v2_loaded(result, body, result_path)
    _exact_keys(result, "fingerprint compare result", {"schema_version", "stage", "implementation_version", "status", "dry_run", "job_id", "recipe_id", "recipe_sha256", "method", "query", "candidate", "comparison", "processing_run", "catalog_context", "result_path", "errors"})
    if result["schema_version"] != 1 or result["stage"] != COMPARE_STAGE or result["implementation_version"] != IMPLEMENTATION_VERSION or result["status"] != "completed" or result["dry_run"] is not False or result["errors"] != [] or result["method"] != "exact_raw_bytes_v1":
        raise ResultImportError("fingerprint compare result contract/status is unsupported")
    _identifier(result["job_id"], "fingerprint compare job_id")
    if result["result_path"] != str(result_path):
        raise ResultImportError("fingerprint compare result_path disagrees with imported file")
    query, query_path, query_body = _compare_side(result["query"], "fingerprint query")
    candidate, candidate_path, candidate_body = _compare_side(
        result["candidate"], "fingerprint candidate"
    )
    if query["role"] != "query" or candidate["role"] != "candidate" or query["fingerprint_id"] == candidate["fingerprint_id"]:
        raise ResultImportError("fingerprint compare roles/self-pair are invalid")
    for key in ("implementation_version", "algorithm", "raw_format", "sample_rate_hz", "channels"):
        if query[key] != candidate[key]:
            raise ResultImportError(f"fingerprint comparison requires identical {key}")
    recipe = {"schema_version": 1, "stage": COMPARE_STAGE, "implementation_version": IMPLEMENTATION_VERSION, "method": "exact_raw_bytes_v1", "query": {key: query[key] for key in ("fingerprint_id", "sha256", "byte_count", "implementation_version", "algorithm", "raw_format", "sample_rate_hz", "channels", "start_ms", "end_ms")}, "candidate": {key: candidate[key] for key in ("fingerprint_id", "sha256", "byte_count", "implementation_version", "algorithm", "raw_format", "sample_rate_hz", "channels", "start_ms", "end_ms")}}
    recipe_sha = sha256_bytes(canonical_json(recipe).encode("utf-8")); recipe_id = f"recipe_audio_fingerprint_compare_{recipe_sha[:32]}"
    if result["recipe_sha256"] != recipe_sha or result["recipe_id"] != recipe_id:
        raise ResultImportError("fingerprint compare recipe identity is inconsistent")
    run = _run(result["processing_run"], recipe_id, "", compare=True)
    comparison = _object(result["comparison"], "fingerprint comparison")
    _exact_keys(comparison, "fingerprint comparison", {"match_candidate_id", "exact_raw_equal", "raw_score", "score_semantics", "calibrated_probability", "quality_flags", "decision_state", "requires_human_review", "relationship_asserted"})
    expected_match_id = _producer_id("fingerprint_match_candidate", run["processing_run_id"], query["fingerprint_id"], candidate["fingerprint_id"])
    if comparison["match_candidate_id"] != expected_match_id or not isinstance(comparison["exact_raw_equal"], bool) or _number(comparison["raw_score"], "fingerprint raw score") not in {0.0, 1.0}:
        raise ResultImportError("fingerprint exact comparison identity/score is invalid")
    observed_equal = query_body == candidate_body
    if comparison["exact_raw_equal"] != observed_equal or comparison["raw_score"] != (1.0 if observed_equal else 0.0):
        raise ResultImportError("fingerprint exact comparison disagrees with current raw bytes")
    if comparison["score_semantics"] != "boolean_raw_byte_equality_not_probability" or comparison["calibrated_probability"] is not None or comparison["decision_state"] != "candidate" or comparison["requires_human_review"] is not True or comparison["relationship_asserted"] is not False:
        raise ResultImportError("fingerprint comparison must remain uncalibrated human-review evidence")
    if _sorted_flags(comparison["quality_flags"], "fingerprint compare quality_flags", COMPARE_FLAGS) != _expected_compare_flags(query, candidate):
        raise ResultImportError("fingerprint compare quality flags are inconsistent")
    context = _pair_context(result["catalog_context"])
    normalized = dict(result); normalized.update({"query": query, "candidate": candidate, "comparison": dict(comparison), "processing_run": run, "catalog_context": context, "_result_path": result_path, "_result_sha256": sha256_bytes(body), "_result_byte_count": len(body), "_query_path": query_path, "_candidate_path": candidate_path})
    return normalized


def _require_pair_dependencies(connection, result: dict[str, Any]) -> None:
    for name in ("query", "candidate"):
        side = result[name]
        fingerprint = connection.execute("SELECT media_id, fingerprint_kind, implementation_version, start_ms, end_ms, artifact_uri FROM fingerprints WHERE fingerprint_id = ?", (side["fingerprint_id"],)).fetchone()
        if fingerprint is None or fingerprint["media_id"] != side["media_id"] or fingerprint["fingerprint_kind"] != "chromaprint_raw" or fingerprint["implementation_version"] != side["implementation_version"] or fingerprint["start_ms"] != side["start_ms"] or fingerprint["end_ms"] != side["end_ms"]:
            raise ResultImportError(f"fingerprint {name} catalog dependency is missing or differs")
        artifact = connection.execute("SELECT storage_uri, sha256, byte_count, visibility FROM artifacts WHERE artifact_id = ?", (side["artifact_id"],)).fetchone()
        if artifact is None or artifact["storage_uri"] != side["storage_uri"] or artifact["sha256"] != side["sha256"] or artifact["byte_count"] != side["byte_count"] or artifact["visibility"] != "private":
            raise ResultImportError(f"fingerprint {name} artifact dependency is missing or differs")
    context = result["catalog_context"]
    if context is None:
        return
    for name in ("query", "candidate"):
        row = connection.execute("SELECT recording_id, media_id FROM renditions WHERE rendition_id = ?", (context[name]["rendition_id"],)).fetchone()
        if row is None or row["recording_id"] != context[name]["recording_id"] or row["media_id"] != result[name]["media_id"]:
            raise ResultImportError(f"fingerprint {name} context dependency is missing or differs")


def _reverify_pair_files(result: dict[str, Any]) -> None:
    for label, path, digest, byte_count in (
        (
            "fingerprint comparison sealed result",
            result["_result_path"],
            result["_result_sha256"],
            result["_result_byte_count"],
        ),
        (
            "fingerprint query artifact",
            result["_query_path"],
            result["query"]["sha256"],
            result["query"]["byte_count"],
        ),
        (
            "fingerprint candidate artifact",
            result["_candidate_path"],
            result["candidate"]["sha256"],
            result["candidate"]["byte_count"],
        ),
    ):
        if path.lstat().st_mode & 0o222:
            raise ResultImportError(f"{label} is no longer sealed read-only")
        _verify_hash(path, digest, byte_count, label)


def _reverify_pair_v2_files(result: dict[str, Any]) -> None:
    result_path = result["_result_path"]
    if result_path.lstat().st_mode & 0o222:
        raise ResultImportError("fingerprint v2 comparison result is no longer sealed")
    _verify_hash(
        result_path,
        result["_result_sha256"],
        result["_result_byte_count"],
        "fingerprint v2 comparison result",
    )
    for role in ("query", "candidate"):
        _reverify_extraction_files(result[role]["_extraction"])
    query_body = _stable_read(
        result["query"]["_artifact_path"],
        "fingerprint v2 query raw artifact",
        maximum_bytes=MAX_FINGERPRINT_BYTES,
    )
    candidate_body = _stable_read(
        result["candidate"]["_artifact_path"],
        "fingerprint v2 candidate raw artifact",
        maximum_bytes=MAX_FINGERPRINT_BYTES,
    )
    observed_equal = query_body == candidate_body
    if result["comparison"]["exact_raw_equal"] != observed_equal:
        raise ResultImportError(
            "fingerprint v2 comparison changed before catalog transaction"
        )


def _require_pair_v2_dependencies(connection, result: dict[str, Any]) -> None:
    for role in ("query", "candidate"):
        side = result[role]
        extraction = side["_extraction"]
        item = side["_item"]
        _require_extraction_dependencies(connection, extraction)
        receipt = connection.execute(
            """
            SELECT result_sha256, processing_run_id, recipe_id, catalog_context_json
            FROM audio_fingerprint_result_imports
            WHERE result_kind = 'extraction' AND result_sha256 = ?
            """,
            (extraction["_result_sha256"],),
        ).fetchone()
        expected_receipt = (
            extraction["_result_sha256"],
            extraction["processing_run"]["processing_run_id"],
            extraction["recipe_id"],
            canonical_json(extraction["catalog_context"]),
        )
        if receipt is None or tuple(receipt) != expected_receipt:
            raise ResultImportError(
                f"fingerprint v2 {role} extraction receipt is missing or differs"
            )
        fingerprint = connection.execute(
            """
            SELECT media_id, fingerprint_kind, implementation_version,
                   start_ms, end_ms, value_text, artifact_uri
            FROM fingerprints WHERE fingerprint_id = ?
            """,
            (item["fingerprint_id"],),
        ).fetchone()
        expected_fingerprint = (
            extraction["input"]["media_id"],
            "chromaprint_raw",
            item["implementation_version"],
            item["start_ms"],
            item["end_ms"],
            None,
            item["artifact"]["storage_uri"],
        )
        if fingerprint is None or tuple(fingerprint) != expected_fingerprint:
            raise ResultImportError(
                f"fingerprint v2 {role} fingerprint dependency is missing or differs"
            )
        artifact = connection.execute(
            """
            SELECT processing_run_id, artifact_kind, storage_uri, sha256,
                   byte_count, visibility
            FROM artifacts WHERE artifact_id = ?
            """,
            (item["artifact"]["artifact_id"],),
        ).fetchone()
        expected_artifact = (
            extraction["processing_run"]["processing_run_id"],
            "audio_fingerprint_chromaprint_raw",
            item["artifact"]["storage_uri"],
            item["artifact"]["sha256"],
            item["artifact"]["byte_count"],
            "private",
        )
        if artifact is None or tuple(artifact) != expected_artifact:
            raise ResultImportError(
                f"fingerprint v2 {role} artifact dependency is missing or differs"
            )
        lineage = connection.execute(
            """
            SELECT observation.recording_id, observation.rendition_id,
                   observation.processing_run_id, observation.visibility,
                   observation.review_state, typed.artifact_id
            FROM audio_fingerprint_observations AS typed
            JOIN observations AS observation
              ON observation.observation_id = typed.observation_id
            WHERE typed.fingerprint_id = ?
              AND typed.artifact_id = ?
              AND observation.recording_id = ?
              AND observation.rendition_id = ?
            """,
            (
                item["fingerprint_id"],
                item["artifact"]["artifact_id"],
                extraction["catalog_context"]["recording_id"],
                extraction["catalog_context"]["rendition_id"],
            ),
        ).fetchone()
        expected_lineage = (
            extraction["catalog_context"]["recording_id"],
            extraction["catalog_context"]["rendition_id"],
            extraction["processing_run"]["processing_run_id"],
            "private",
            "machine",
            item["artifact"]["artifact_id"],
        )
        if lineage is None or tuple(lineage) != expected_lineage:
            raise ResultImportError(
                f"fingerprint v2 {role} observation/catalog lineage is missing or differs"
            )


def _insert_processing_run_v2(connection, result: dict[str, Any]) -> None:
    run = result["processing_run"]
    parameters = canonical_json(
        {
            "recipe_id": result["recipe_id"],
            "recipe_sha256": result["recipe_sha256"],
            "method": result["method"],
            "compatibility_contract": result["compatibility_contract"],
        }
    )
    environment = canonical_json(
        {
            "comparison_runtime": "python-standard-library",
            "implementation_version": result["implementation_version"],
            "query_engine": _v2_compatibility_engine(result["query"]["engine"]),
            "candidate_engine": _v2_compatibility_engine(result["candidate"]["engine"]),
            "query_extraction_result_sha256": result["query"]["extraction_result"]["sha256"],
            "candidate_extraction_result_sha256": result["candidate"]["extraction_result"]["sha256"],
        }
    )
    values = {
        "stage": COMPARE_V2_STAGE,
        "implementation_version": result["implementation_version"],
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": parameters,
        "environment_json": environment,
        "random_seed": None,
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "status": "completed",
        "error_text": None,
    }
    existing = connection.execute(
        """
        SELECT stage, implementation_version, model_id, glossary_revision_id,
               parameters_json, environment_json, random_seed, started_at,
               completed_at, status, error_text
        FROM processing_runs WHERE processing_run_id = ?
        """,
        (run["processing_run_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in values.items()):
            raise ResultImportError("fingerprint v2 processing run ID already differs")
        return
    connection.execute(
        """
        INSERT INTO processing_runs(
            processing_run_id, stage, implementation_version, model_id,
            glossary_revision_id, parameters_json, environment_json, random_seed,
            started_at, completed_at, status, error_text
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run["processing_run_id"], *values.values()),
    )


def _insert_compare_v2_inputs(connection, result: dict[str, Any]) -> None:
    run_id = result["processing_run"]["processing_run_id"]
    for role in ("query", "candidate"):
        side = result[role]
        _insert_run_input(
            connection,
            run_id,
            "audio_fingerprint_extraction_result",
            side["extraction_result"]["processing_run_id"],
            f"{role}_result",
            side["extraction_result"]["sha256"],
        )
        _insert_run_input(
            connection,
            run_id,
            "fingerprint",
            side["fingerprint"]["fingerprint_id"],
            role,
            side["fingerprint"]["artifact"]["sha256"],
        )


def _insert_compare_v2_generic_candidate(connection, result: dict[str, Any]) -> None:
    comparison = result["comparison"]
    run_id = result["processing_run"]["processing_run_id"]
    query = result["query"]
    candidate = result["candidate"]
    metadata = canonical_json(
        {
            "calibration_state": "not_calibrated",
            "catalog_context": result["catalog_context"],
            "exact_raw_equal": comparison["exact_raw_equal"],
            "publication_authority": "none",
            "quality_flags": comparison["quality_flags"],
            "relationship_asserted": False,
            "requires_human_review": True,
            "score_semantics": comparison["score_semantics"],
            "visibility": "private",
        }
    )
    values = (
        "fingerprint",
        query["fingerprint"]["fingerprint_id"],
        "fingerprint",
        candidate["fingerprint"]["fingerprint_id"],
        "chromaprint_exact_raw_bytes_v2",
        comparison["raw_score"],
        None,
        "candidate",
        metadata,
    )
    existing = connection.execute(
        """
        SELECT left_object_type, left_object_id, right_object_type, right_object_id,
               match_method, raw_score, calibrated_probability, decision_state,
               metadata_json
        FROM match_candidates WHERE match_candidate_id = ?
        """,
        (comparison["match_candidate_id"],),
    ).fetchone()
    if existing is None:
        connection.execute(
            """
            INSERT INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (comparison["match_candidate_id"], *values),
        )
        return
    if tuple(existing) != values:
        raise ResultImportError("fingerprint v2 match-candidate ID already differs")


def _compare_v2_receipt_values(result: dict[str, Any]) -> tuple[Any, ...]:
    comparison = result["comparison"]
    return (
        comparison["match_candidate_id"],
        result["processing_run"]["processing_run_id"],
        str(result["_result_path"]),
        result["_result_byte_count"],
        result["recipe_id"],
        result["recipe_sha256"],
        result["query"]["extraction_result"]["sha256"],
        result["candidate"]["extraction_result"]["sha256"],
        result["catalog_context"]["query"]["recording_id"],
        result["catalog_context"]["query"]["rendition_id"],
        result["catalog_context"]["candidate"]["recording_id"],
        result["catalog_context"]["candidate"]["rendition_id"],
        int(comparison["exact_raw_equal"]),
        canonical_json(comparison["quality_flags"]),
        "not_calibrated",
        "private",
        "none",
    )


def _compare_v2_side_values(
    result: dict[str, Any], role: str
) -> tuple[Any, ...]:
    match_id = result["comparison"]["match_candidate_id"]
    side = result[role]
    extraction = side["_extraction"]
    item = side["_item"]
    input_row = extraction["input"]
    engine = extraction["engine"]
    artifact = item["artifact"]
    extraction_parameters = canonical_json(
        {
            "recipe_id": extraction["recipe_id"],
            "recipe_sha256": extraction["recipe_sha256"],
            "fingerprint": extraction["fingerprint"],
            "expanded_windows": extraction["expanded_windows"],
        }
    )
    extraction_environment = canonical_json({"engine": engine})
    return (
        match_id,
        role,
        extraction["_result_sha256"],
        str(extraction["_result_path"]),
        extraction["_result_byte_count"],
        extraction["processing_run"]["processing_run_id"],
        extraction["recipe_id"],
        extraction["recipe_sha256"],
        extraction_parameters,
        extraction_environment,
        len(extraction["fingerprints"]),
        input_row["media_id"],
        input_row["path"],
        input_row["artifact_id"],
        input_row["storage_uri"],
        input_row["parent_processing_run_id"],
        input_row["sha256"],
        input_row["byte_count"],
        input_row["duration_ms"],
        extraction["catalog_context"]["recording_id"],
        extraction["catalog_context"]["rendition_id"],
        item["fingerprint_id"],
        item["implementation_version"],
        item["window_kind"],
        item["start_ms"],
        item["end_ms"],
        item["fingerprint_word_count"],
        artifact["artifact_id"],
        artifact["storage_uri"],
        artifact["sha256"],
        artifact["byte_count"],
        engine["path"],
        engine["sha256"],
        engine["byte_count"],
        engine["version_label"],
        engine["version_output_sha256"],
        engine["build_configuration"],
        engine["muxer_help_sha256"],
        item["algorithm"],
        item["raw_format"],
        item["sample_rate_hz"],
        item["channels"],
    )


def _insert_compare_v2_bindings(connection, result: dict[str, Any]) -> None:
    comparison_sha = result["_result_sha256"]
    receipt_columns = (
        "match_candidate_id", "processing_run_id", "result_path",
        "result_byte_count", "recipe_id", "recipe_sha256",
        "query_result_sha256", "candidate_result_sha256",
        "query_recording_id", "query_rendition_id",
        "candidate_recording_id", "candidate_rendition_id", "exact_raw_equal",
        "quality_flags_json", "calibration_state", "visibility",
        "publication_authority",
    )
    receipt_values = _compare_v2_receipt_values(result)
    receipt = connection.execute(
        f"SELECT {', '.join(receipt_columns)} "
        "FROM audio_fingerprint_compare_v2_receipts "
        "WHERE comparison_result_sha256 = ?",
        (comparison_sha,),
    ).fetchone()
    if receipt is None:
        connection.execute(
            f"INSERT INTO audio_fingerprint_compare_v2_receipts("
            f"comparison_result_sha256, {', '.join(receipt_columns)}) "
            f"VALUES({', '.join('?' for _ in range(len(receipt_columns) + 1))})",
            (comparison_sha, *receipt_values),
        )
    elif tuple(receipt) != receipt_values:
        raise ResultImportError("fingerprint v2 comparison receipt already differs")

    side_columns = (
        "match_candidate_id", "role", "extraction_result_sha256",
        "extraction_result_path", "extraction_result_byte_count",
        "extraction_run_id", "extraction_recipe_id", "extraction_recipe_sha256",
        "extraction_parameters_json", "extraction_environment_json",
        "extraction_fingerprint_count", "input_media_id", "input_path",
        "input_artifact_id", "input_artifact_uri",
        "input_parent_processing_run_id", "input_sha256", "input_byte_count",
        "input_duration_ms", "recording_id", "rendition_id", "fingerprint_id",
        "producer_implementation_version", "window_kind", "fingerprint_start_ms",
        "fingerprint_end_ms", "fingerprint_word_count", "artifact_id",
        "artifact_uri", "artifact_sha256", "artifact_byte_count", "engine_path",
        "engine_sha256", "engine_byte_count", "engine_version_label",
        "engine_version_output_sha256", "engine_build_configuration",
        "engine_muxer_help_sha256", "algorithm", "raw_format", "sample_rate_hz",
        "channels",
    )
    match_id = result["comparison"]["match_candidate_id"]
    for role in ("query", "candidate"):
        side_values = _compare_v2_side_values(result, role)
        existing = connection.execute(
            f"SELECT {', '.join(side_columns)} "
            "FROM audio_fingerprint_compare_v2_sides "
            "WHERE match_candidate_id = ? AND role = ?",
            (match_id, role),
        ).fetchone()
        if existing is None:
            connection.execute(
                f"INSERT INTO audio_fingerprint_compare_v2_sides("
                f"{', '.join(side_columns)}) "
                f"VALUES({', '.join('?' for _ in side_columns)})",
                side_values,
            )
        elif tuple(existing) != side_values:
            raise ResultImportError(f"fingerprint v2 {role} side binding already differs")


def _insert_compare_v2_subtype(connection, result: dict[str, Any]) -> None:
    comparison = result["comparison"]
    run_id = result["processing_run"]["processing_run_id"]
    query = result["query"]
    candidate = result["candidate"]
    subtype_values = (
        run_id,
        query["extraction_result"]["processing_run_id"],
        candidate["extraction_result"]["processing_run_id"],
        query["extraction_result"]["sha256"],
        candidate["extraction_result"]["sha256"],
        query["fingerprint"]["fingerprint_id"],
        candidate["fingerprint"]["fingerprint_id"],
        "exact_raw_bytes_v2",
        "boolean_raw_byte_equality_not_probability",
        "not_calibrated",
        1,
        0,
        "private",
        "none",
    )
    subtype = connection.execute(
        """
        SELECT processing_run_id, query_extraction_run_id,
               candidate_extraction_run_id, query_result_sha256,
               candidate_result_sha256, query_fingerprint_id,
               candidate_fingerprint_id, comparison_method, score_semantics,
               calibration_state, requires_human_review, relationship_asserted,
               visibility, publication_authority
        FROM audio_fingerprint_match_candidates_v2
        WHERE match_candidate_id = ?
        """,
        (comparison["match_candidate_id"],),
    ).fetchone()
    if subtype is None:
        connection.execute(
            """
            INSERT INTO audio_fingerprint_match_candidates_v2(
                match_candidate_id, processing_run_id, query_extraction_run_id,
                candidate_extraction_run_id, query_result_sha256,
                candidate_result_sha256, query_fingerprint_id,
                candidate_fingerprint_id, comparison_method, score_semantics,
                calibration_state, requires_human_review, relationship_asserted,
                visibility, publication_authority
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (comparison["match_candidate_id"], *subtype_values),
        )
    elif tuple(subtype) != subtype_values:
        raise ResultImportError("fingerprint v2 match subtype is missing or differs")


def _import_audio_fingerprint_compare_v2_result(
    connection, result: dict[str, Any]
) -> dict[str, Any]:
    with transaction(connection):
        _reverify_pair_v2_files(result)
        _require_pair_v2_dependencies(connection, result)
        _insert_processing_run_v2(connection, result)
        _insert_compare_v2_inputs(connection, result)
        _insert_compare_v2_generic_candidate(connection, result)
        _insert_import_ledger(connection, result, "exact_comparison")
        _insert_compare_v2_bindings(connection, result)
        _insert_compare_v2_subtype(connection, result)
    return {
        "importer_version": __version__,
        "contract_version": 2,
        "result_sha256": result["_result_sha256"],
        "recipe_id": result["recipe_id"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "catalog_context": result["catalog_context"],
        "match_candidates_inserted_or_present": 1,
        "provenance_only": False,
        "calibration_state": "not_calibrated",
        "calibrated_probability": None,
        "relationship_asserted": False,
        "visibility": "private",
        "publication_authority": "none",
        "publication_decisions": 0,
    }


def import_audio_fingerprint_compare_result(connection, path: Path) -> dict[str, Any]:
    result = validate_audio_fingerprint_compare_result_file(path)
    if result["schema_version"] == 2:
        return _import_audio_fingerprint_compare_v2_result(connection, result)
    with transaction(connection):
        _reverify_pair_files(result)
        _require_pair_dependencies(connection, result)
        _insert_processing_run(connection, result, compare=True)
        run_id = result["processing_run"]["processing_run_id"]
        _insert_run_input(connection, run_id, "fingerprint", result["query"]["fingerprint_id"], "query", result["query"]["sha256"])
        _insert_run_input(connection, run_id, "fingerprint", result["candidate"]["fingerprint_id"], "candidate", result["candidate"]["sha256"])
        if result["catalog_context"] is not None:
            comparison = result["comparison"]
            metadata = canonical_json({"catalog_context": result["catalog_context"], "exact_raw_equal": comparison["exact_raw_equal"], "quality_flags": comparison["quality_flags"], "relationship_asserted": False, "requires_human_review": True, "score_semantics": comparison["score_semantics"]})
            existing = connection.execute("SELECT left_object_type, left_object_id, right_object_type, right_object_id, match_method, raw_score, calibrated_probability, decision_state, metadata_json FROM match_candidates WHERE match_candidate_id = ?", (comparison["match_candidate_id"],)).fetchone()
            values = ("fingerprint", result["query"]["fingerprint_id"], "fingerprint", result["candidate"]["fingerprint_id"], "chromaprint_exact_raw_bytes_v1", comparison["raw_score"], None, "candidate", metadata)
            if existing is None:
                connection.execute("INSERT INTO match_candidates(match_candidate_id, left_object_type, left_object_id, right_object_type, right_object_id, match_method, raw_score, calibrated_probability, decision_state, metadata_json) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (comparison["match_candidate_id"], *values))
                connection.execute("INSERT INTO audio_fingerprint_match_candidates(match_candidate_id, processing_run_id, query_fingerprint_id, candidate_fingerprint_id, comparison_method, score_semantics, requires_human_review, relationship_asserted) VALUES(?, ?, ?, ?, 'exact_raw_bytes_v1', 'boolean_raw_byte_equality_not_probability', 1, 0)", (comparison["match_candidate_id"], run_id, result["query"]["fingerprint_id"], result["candidate"]["fingerprint_id"]))
            elif tuple(existing) != values:
                raise ResultImportError("fingerprint match-candidate ID already has different data")
            else:
                subtype = connection.execute(
                    """
                    SELECT processing_run_id, query_fingerprint_id,
                           candidate_fingerprint_id, comparison_method,
                           score_semantics, requires_human_review,
                           relationship_asserted
                    FROM audio_fingerprint_match_candidates
                    WHERE match_candidate_id = ?
                    """,
                    (comparison["match_candidate_id"],),
                ).fetchone()
                expected_subtype = (
                    run_id, result["query"]["fingerprint_id"],
                    result["candidate"]["fingerprint_id"], "exact_raw_bytes_v1",
                    "boolean_raw_byte_equality_not_probability", 1, 0,
                )
                if subtype is None or tuple(subtype) != expected_subtype:
                    raise ResultImportError(
                        "fingerprint match subtype is missing or differs"
                    )
        _insert_import_ledger(connection, result, "exact_comparison")
    return {"importer_version": __version__, "result_sha256": result["_result_sha256"], "recipe_id": result["recipe_id"], "processing_run_id": result["processing_run"]["processing_run_id"], "catalog_context": result["catalog_context"], "match_candidates_inserted_or_present": 1 if result["catalog_context"] else 0, "provenance_only": result["catalog_context"] is None, "calibrated_probability": None, "relationship_asserted": False, "publication_decisions": 0}
