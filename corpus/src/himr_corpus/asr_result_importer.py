"""Strict admission of completed ``asr_whispercpp`` result envelopes.

The offline ASR producer cannot write to the catalog.  This module is the trust
boundary between its private, immutable files and SQLite.  Validation is intentionally
duplicated at this boundary: producer output is untrusted input, even when it already
conforms to the producer's JSON Schema.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .db import transaction
from .ids import stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError


SCHEMA_VERSION = 1
STAGE = "asr_whispercpp"
MAX_RESULT_BYTES = 512 * 1024 * 1024
MAX_WINDOW_MS = 24 * 60 * 60 * 1_000
MAX_BOUNDARY_OVERRUN_MS = 30_000
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
RUN_ID_RE = re.compile(r"^run_asr_whispercpp_[0-9a-f]{32}$")
RECIPE_ID_RE = re.compile(r"^recipe_asr_whispercpp_[0-9a-f]{32}$")
PROC_FD_RE = re.compile(r"^/proc/self/fd/[0-9]+$")
DESCRIPTOR_EXECUTION_POLICY = "linux_proc_self_fd_retained_verified_v1"
LOGICAL_COMMANDS_DEFINITION = (
    "deterministic_original_input_paths_and_final_output_path_v1"
)
RESULT_COMMANDS_DEFINITION = "exact_child_facing_argv_v1"


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    """Internal race fingerprint; public historical stat objects stay unchanged."""

    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        stat.S_IMODE(value.st_mode),
        value.st_nlink,
    )


def _exact_keys(
    value: dict[str, Any], label: str, required: set[str], optional: set[str] | None = None
) -> None:
    optional = optional or set()
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - required - optional)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise ResultImportError(f"{label} has " + "; ".join(details))


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResultImportError(f"{label} must be an object")
    return value


def _array(value: object, label: str, *, length: int | None = None) -> list[Any]:
    if not isinstance(value, list):
        raise ResultImportError(f"{label} must be an array")
    if length is not None and len(value) != length:
        raise ResultImportError(f"{label} must contain exactly {length} items")
    return value


def _string(
    value: object,
    label: str,
    *,
    allow_empty: bool = False,
    maximum: int = 1_000_000,
) -> str:
    if (
        not isinstance(value, str)
        or (not allow_empty and not value)
        or len(value) > maximum
        or "\x00" in value
    ):
        qualifier = "a bounded string" if allow_empty else "a non-empty bounded string"
        raise ResultImportError(f"{label} must be {qualifier}")
    return value


def _nullable_string(value: object, label: str) -> str | None:
    return None if value is None else _string(value, label)


def _identifier(value: object, label: str) -> str:
    text = _string(value, label, maximum=256)
    if not ID_RE.fullmatch(text):
        raise ResultImportError(f"{label} contains unsupported identifier characters")
    return text


def _integer(
    value: object, label: str, *, minimum: int | None = None, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResultImportError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise ResultImportError(f"{label} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ResultImportError(f"{label} must be <= {maximum}")
    return value


def _number(
    value: object, label: str, *, minimum: float | None = None, maximum: float | None = None
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResultImportError(f"{label} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ResultImportError(f"{label} must be a finite number")
    if minimum is not None and result < minimum:
        raise ResultImportError(f"{label} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise ResultImportError(f"{label} must be <= {maximum}")
    return result


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ResultImportError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _timestamp(value: object, label: str) -> str:
    text = _string(value, label, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ResultImportError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ResultImportError(f"{label} must include an offset")
    normalized = parsed.astimezone(timezone.utc)
    if normalized.microsecond:
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _timestamp_value(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _json_text(value: object, label: str) -> tuple[str, object]:
    text = _string(value, label)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        raise ResultImportError(f"{label} must contain valid JSON") from error
    if canonical_json(parsed) != text:
        raise ResultImportError(f"{label} must use canonical JSON encoding")
    return text, parsed


def _canonical_equal(left: object, right: object) -> bool:
    return canonical_json(left) == canonical_json(right)


def _producer_id(prefix: str, *parts: object) -> str:
    digest = sha256_bytes(canonical_json(list(parts)).encode("utf-8"))
    return f"{prefix}_{digest[:32]}"


def _stable_read(path: Path, label: str, *, maximum_bytes: int | None = None) -> bytes:
    """Read a regular file and reject path replacement or in-place mutation."""

    if not path.is_absolute():
        raise ResultImportError(f"{label} path must be absolute")
    try:
        path_before = path.stat()
        handle = path.open("rb")
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} is not a readable current file: {error}") from error
    try:
        descriptor_before = os.fstat(handle.fileno())
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise ResultImportError(f"{label} must be a regular file")
        before = _stat_identity(descriptor_before)
        if before != _stat_identity(path_before):
            raise ResultImportError(f"{label} was replaced while being opened")
        if maximum_bytes is not None and descriptor_before.st_size > maximum_bytes:
            raise ResultImportError(f"{label} exceeds the {maximum_bytes}-byte limit")
        body = handle.read()
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        path_after = path.stat()
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} disappeared after verification: {error}") from error
    after_descriptor = _stat_identity(descriptor_after)
    after_path = _stat_identity(path_after)
    if before != after_descriptor or before != after_path or len(body) != before[2]:
        raise ResultImportError(f"{label} changed while it was being verified")
    return body


def _absolute_observed_path(value: object, label: str) -> Path:
    text = _string(value, label)
    if "://" in text:
        raise ResultImportError(f"{label} must be an absolute local path, not a URI")
    path = Path(text)
    if not path.is_absolute():
        raise ResultImportError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} is not a readable current file: {error}") from error
    # Producer paths are resolved before emission.  Refuse aliases, dot traversal,
    # and symlink paths so a result cannot disguise its actual local target.
    if path != resolved:
        raise ResultImportError(f"{label} must be a resolved path without traversal or symlinks")
    if not resolved.is_file():
        raise ResultImportError(f"{label} must identify a regular file")
    return resolved


def _local_file_uri(value: object, label: str) -> Path:
    uri = _string(value, label)
    parsed = urllib.parse.urlsplit(uri)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        raise ResultImportError(f"{label} must be a local file URI")
    if parsed.query or parsed.fragment:
        raise ResultImportError(f"{label} must not contain a query or fragment")
    try:
        decoded = urllib.parse.unquote(parsed.path, errors="strict")
    except UnicodeError as error:
        raise ResultImportError(f"{label} contains invalid path encoding") from error
    path = _absolute_observed_path(decoded, f"{label} path")
    if path.as_uri() != uri:
        raise ResultImportError(f"{label} must be a canonical local file URI")
    return path


def _verify_hash(
    path: Path,
    digest: str,
    byte_count: int,
    label: str,
    *,
    capture: bool = False,
) -> bytes | None:
    """Stream-hash a regular file with race checks, optionally retaining JSON bytes."""

    try:
        path_before = path.stat()
        handle = path.open("rb")
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} is not a readable current file: {error}") from error
    chunks: list[bytes] | None = [] if capture else None
    observed_digest = hashlib.sha256()
    try:
        descriptor_before = os.fstat(handle.fileno())
        if not stat.S_ISREG(descriptor_before.st_mode):
            raise ResultImportError(f"{label} must be a regular file")
        before = _stat_identity(descriptor_before)
        if before != _stat_identity(path_before):
            raise ResultImportError(f"{label} was replaced while being opened")
        if descriptor_before.st_size != byte_count:
            raise ResultImportError(f"{label} byte_count differs from the result envelope")
        if capture and byte_count > MAX_RESULT_BYTES:
            raise ResultImportError(f"{label} exceeds the JSON artifact size limit")
        while chunk := handle.read(8 * 1024 * 1024):
            observed_digest.update(chunk)
            if chunks is not None:
                chunks.append(chunk)
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        path_after = path.stat()
    except (FileNotFoundError, OSError) as error:
        raise ResultImportError(f"{label} disappeared after verification: {error}") from error
    after_descriptor = _stat_identity(descriptor_after)
    after_path = _stat_identity(path_after)
    if before != after_descriptor or before != after_path:
        raise ResultImportError(f"{label} changed while it was being verified")
    if observed_digest.hexdigest() != digest:
        raise ResultImportError(f"{label} SHA-256 differs from the result envelope")
    return b"".join(chunks) if chunks is not None else None


def _validate_stat(value: object, label: str, expected_bytes: int) -> dict[str, int]:
    row = _object(value, label)
    _exact_keys(row, label, {"device", "inode", "byte_count", "mtime_ns"})
    normalized = {
        "device": _integer(row["device"], f"{label}.device"),
        "inode": _integer(row["inode"], f"{label}.inode", minimum=0),
        "byte_count": _integer(row["byte_count"], f"{label}.byte_count", minimum=0),
        "mtime_ns": _integer(row["mtime_ns"], f"{label}.mtime_ns"),
    }
    if normalized["byte_count"] != expected_bytes:
        raise ResultImportError(f"{label}.byte_count disagrees with its observation")
    return normalized


def _validate_observation(
    value: object,
    label: str,
    *,
    extra_keys: set[str],
) -> tuple[dict[str, Any], Path]:
    row = _object(value, label)
    common = {"path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged"}
    _exact_keys(row, label, common | extra_keys)
    digest = _sha256(row["sha256"], f"{label}.sha256")
    byte_count = _integer(row["byte_count"], f"{label}.byte_count", minimum=1)
    before = _validate_stat(row["stat_before"], f"{label}.stat_before", byte_count)
    after = _validate_stat(row["stat_after"], f"{label}.stat_after", byte_count)
    if row["unchanged"] is not True or before != after:
        raise ResultImportError(f"{label} must be unchanged across the producer run")
    path = _absolute_observed_path(row["path"], f"{label}.path")
    return row, path


def _read_result(path_value: str | Path) -> tuple[dict[str, Any], str, Path]:
    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    body = _stable_read(path, "ASR result JSON", maximum_bytes=MAX_RESULT_BYTES)
    try:
        raw = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"ASR result JSON is invalid: {error}") from error
    result = _object(raw, "ASR result")
    return result, sha256_bytes(canonical_json(result).encode("utf-8")), path.resolve()


def _validate_probe(value: object, label: str, *, input_duration_limit: bool = False) -> dict[str, Any]:
    probe = _object(value, label)
    _exact_keys(
        probe,
        label,
        {
            "ffprobe_path",
            "codec_name",
            "sample_format",
            "sample_rate_hz",
            "channels",
            "channel_layout",
            "format_name",
            "duration_ms",
        },
    )
    _absolute_observed_path(probe["ffprobe_path"], f"{label}.ffprobe_path")
    if (
        probe["codec_name"] != "flac"
        or probe["sample_format"] != "s16"
        or probe["sample_rate_hz"] != 16_000
        or probe["channels"] != 1
    ):
        raise ResultImportError(f"{label} is not normalized mono 16 kHz s16 FLAC")
    _nullable_string(probe["channel_layout"], f"{label}.channel_layout")
    _nullable_string(probe["format_name"], f"{label}.format_name")
    _integer(probe["duration_ms"], f"{label}.duration_ms", minimum=1, maximum=MAX_WINDOW_MS)
    return probe


def _validate_build(value: object, label: str) -> dict[str, Any]:
    build = _object(value, label)
    _exact_keys(build, label, {"repository", "revision", "target", "configuration"})
    for key in ("repository", "revision", "target"):
        _string(build[key], f"{label}.{key}", maximum=2_000)
    configuration = _array(build["configuration"], f"{label}.configuration")
    if len(configuration) > 64:
        raise ResultImportError(f"{label}.configuration has too many items")
    for index, item in enumerate(configuration):
        _string(item, f"{label}.configuration[{index}]", maximum=2_000)
    return build


def _validate_window(value: object, label: str) -> dict[str, int]:
    window = _object(value, label)
    _exact_keys(window, label, {"offset_ms", "duration_ms", "end_ms"})
    offset = _integer(window["offset_ms"], f"{label}.offset_ms", minimum=0, maximum=MAX_WINDOW_MS)
    duration = _integer(window["duration_ms"], f"{label}.duration_ms", minimum=1, maximum=MAX_WINDOW_MS)
    end = _integer(window["end_ms"], f"{label}.end_ms", minimum=1)
    if end != offset + duration:
        raise ResultImportError(f"{label}.end_ms must equal offset_ms + duration_ms")
    return {"offset_ms": offset, "duration_ms": duration, "end_ms": end}


def _validate_command(value: object, label: str) -> list[str]:
    command = _array(value, label)
    if not command:
        raise ResultImportError(f"{label} must not be empty")
    for index, argument in enumerate(command):
        _string(argument, f"{label}[{index}]", allow_empty=True, maximum=16_384)
    return command


def _validate_descriptor_command_provenance(
    environment: object,
    commands: list[Any],
    *,
    run: dict[str, Any],
    recipe: dict[str, Any],
    result_path: Path,
    result_key: str,
    probe: dict[str, Any],
    input_path: Path,
    engine_path: Path,
    model_path: Path,
    glossary: dict[str, Any] | None,
) -> None:
    """Validate the exact proc-fd argv and its deterministic logical counterpart."""

    value = _object(environment, "ASR processing_run environment")
    _exact_keys(
        value,
        "ASR processing_run environment",
        {
            "python",
            "cpu_only",
            "network",
            "engine_version",
            "engine_version_evidence",
            "command_provenance",
        },
    )
    _string(value["python"], "ASR processing_run environment.python", maximum=128)
    if value["cpu_only"] is not True or value["network"] != "not_used":
        raise ResultImportError("ASR processing_run environment safety fields are invalid")
    if (
        value["engine_version"] != recipe["engine"]["version"]
        or value["engine_version_evidence"]
        != recipe["engine"]["version_evidence"]
    ):
        raise ResultImportError(
            "ASR processing_run environment engine provenance disagrees with the recipe"
        )

    provenance = _object(
        value["command_provenance"],
        "ASR processing_run environment.command_provenance",
    )
    _exact_keys(
        provenance,
        "ASR processing_run environment.command_provenance",
        {
            "descriptor_execution_policy",
            "logical_commands",
            "logical_commands_definition",
            "result_commands_location",
            "result_commands_definition",
            "result_command_states",
        },
    )
    if (
        provenance["descriptor_execution_policy"] != DESCRIPTOR_EXECUTION_POLICY
        or provenance["logical_commands_definition"]
        != LOGICAL_COMMANDS_DEFINITION
        or provenance["result_commands_location"] != "result.commands"
        or provenance["result_commands_definition"] != RESULT_COMMANDS_DEFINITION
        or provenance["result_command_states"] != ["executed", "executed"]
    ):
        raise ResultImportError("ASR command provenance policy is invalid")
    logical_commands = _array(
        provenance["logical_commands"],
        "ASR logical commands",
        length=2,
    )
    logical_probe = _validate_command(logical_commands[0], "ASR logical ffprobe command")
    logical_whisper = _validate_command(
        logical_commands[1], "ASR logical whisper.cpp command"
    )
    executed_probe = commands[0]
    executed_whisper = commands[1]

    expected_probe = [
        probe["ffprobe_path"],
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "format=duration,format_name:stream=index,codec_name,sample_fmt,sample_rate,channels,channel_layout,duration",
        "-of",
        "json",
        str(input_path),
    ]
    if logical_probe != expected_probe:
        raise ResultImportError("ASR logical ffprobe command disagrees with provenance")
    if (
        executed_probe[:-1] != expected_probe[:-1]
        or not PROC_FD_RE.fullmatch(executed_probe[-1])
    ):
        raise ResultImportError("ASR executed ffprobe command is not descriptor-backed")

    inference = recipe["inference"]
    expected_whisper = [
        str(engine_path),
        "--model",
        str(model_path),
        "--file",
        str(input_path),
        "--language",
        inference["language"],
        "--threads",
        str(inference["threads"]),
        "--offset-t",
        str(recipe["window"]["offset_ms"]),
        "--duration",
        str(recipe["window"]["duration_ms"]),
        "--max-len",
        str(inference["max_segment_characters"]),
        "--best-of",
        str(inference["best_of"]),
        "--beam-size",
        str(inference["beam_size"]),
        "--word-thold",
        str(inference["word_threshold"]),
        "--entropy-thold",
        str(inference["entropy_threshold"]),
        "--logprob-thold",
        str(inference["logprob_threshold"]),
        "--no-speech-thold",
        str(inference["no_speech_threshold"]),
        "--temperature",
        str(inference["temperature"]),
        "--temperature-inc",
        str(inference["temperature_increment"]),
        "--output-json-full",
        "--output-file",
        str(result_path.parent / "whisper-output"),
        "--no-prints",
        "--no-gpu",
    ]
    if inference["split_on_word"]:
        expected_whisper.append("--split-on-word")
    if inference["no_fallback"]:
        expected_whisper.append("--no-fallback")
    if glossary is not None:
        try:
            prompt_index = logical_whisper.index("--prompt")
            prompt = logical_whisper[prompt_index + 1]
        except (ValueError, IndexError) as error:
            raise ResultImportError("ASR logical whisper.cpp command omits its glossary prompt") from error
        if sha256_bytes(prompt.encode("utf-8")) != glossary["prompt_sha256"]:
            raise ResultImportError("ASR logical whisper.cpp glossary prompt hash is invalid")
        expected_whisper.extend(["--prompt", prompt])
    if logical_whisper != expected_whisper:
        raise ResultImportError("ASR logical whisper.cpp command disagrees with the recipe")

    if len(executed_whisper) != len(logical_whisper):
        raise ResultImportError("ASR executed whisper.cpp command has invalid arity")
    engine_index = 0
    model_index = logical_whisper.index("--model") + 1
    input_index = logical_whisper.index("--file") + 1
    output_index = logical_whisper.index("--output-file") + 1
    descriptor_indices = {engine_index, model_index, input_index}
    for index, (logical, executed) in enumerate(
        zip(logical_whisper, executed_whisper, strict=True)
    ):
        if index in descriptor_indices:
            if not PROC_FD_RE.fullmatch(executed):
                raise ResultImportError(
                    "ASR executed whisper.cpp command is not descriptor-backed"
                )
        elif index == output_index:
            expected_staged_prefix = (
                result_path.parent.parent
                / f".{result_key}.tmp-{run['processing_run_id']}"
                / "whisper-output"
            )
            if executed != str(expected_staged_prefix):
                raise ResultImportError(
                    "ASR executed whisper.cpp staging path is invalid"
                )
        elif executed != logical:
            raise ResultImportError(
                "ASR executed whisper.cpp argv disagrees with logical provenance"
            )
    descriptor_paths = [executed_whisper[index] for index in descriptor_indices]
    if len(set(descriptor_paths)) != 3 or executed_probe[-1] != executed_whisper[input_index]:
        raise ResultImportError("ASR descriptor command bindings are inconsistent")


def _validate_recipe(
    recipe: object,
    *,
    run: dict[str, Any],
    engine: dict[str, Any],
    model: dict[str, Any],
    glossary: dict[str, Any] | None,
    window: dict[str, int],
) -> dict[str, Any]:
    value = _object(recipe, "ASR recipe")
    implementation_version = value.get("implementation_version")
    recipe_keys = {
        "contract_version",
        "implementation_version",
        "stage",
        "engine",
        "model",
        "window",
        "inference",
        "glossary",
        "output_contract",
    }
    if implementation_version == "0.3.0":
        recipe_keys.add("descriptor_execution_policy")
    _exact_keys(
        value,
        "ASR recipe",
        recipe_keys,
    )
    if value["contract_version"] != 1 or value["stage"] != STAGE:
        raise ResultImportError("ASR recipe contract or stage is unsupported")
    if value["implementation_version"] != run["implementation_version"]:
        raise ResultImportError("ASR recipe implementation version disagrees with the run")
    if (
        implementation_version == "0.3.0"
        and value["descriptor_execution_policy"] != DESCRIPTOR_EXECUTION_POLICY
    ):
        raise ResultImportError("ASR recipe descriptor execution policy is invalid")
    if value["output_contract"] != "whisper.cpp-output-json-full-normalized-v1":
        raise ResultImportError("ASR recipe output contract is unsupported")
    recipe_engine = _object(value["engine"], "ASR recipe.engine")
    _exact_keys(
        recipe_engine,
        "ASR recipe.engine",
        {"sha256", "version", "version_evidence", "build"},
    )
    expected_engine = {
        "sha256": engine["sha256"],
        "version": engine["version"],
        "version_evidence": engine["version_evidence"],
        "build": engine["build"],
    }
    if not _canonical_equal(recipe_engine, expected_engine):
        raise ResultImportError("ASR recipe.engine disagrees with the engine observation")
    recipe_model = _object(value["model"], "ASR recipe.model")
    _exact_keys(
        recipe_model,
        "ASR recipe.model",
        {"model_id", "name", "revision", "source", "license_label", "sha256"},
    )
    expected_model = {
        key: model[key]
        for key in ("model_id", "name", "revision", "source", "license_label", "sha256")
    }
    if not _canonical_equal(recipe_model, expected_model):
        raise ResultImportError("ASR recipe.model disagrees with the model observation")
    if not _canonical_equal(value["window"], window):
        raise ResultImportError("ASR recipe.window disagrees with the result window")
    inference = _object(value["inference"], "ASR recipe.inference")
    _exact_keys(
        inference,
        "ASR recipe.inference",
        {
            "language",
            "threads",
            "translate",
            "split_on_word",
            "best_of",
            "beam_size",
            "max_segment_characters",
            "word_threshold",
            "entropy_threshold",
            "logprob_threshold",
            "no_speech_threshold",
            "temperature",
            "temperature_increment",
            "no_fallback",
            "timeout_seconds",
        },
    )
    _string(inference["language"], "ASR recipe.inference.language", maximum=64)
    for key in ("threads", "best_of", "beam_size", "max_segment_characters", "timeout_seconds"):
        _integer(inference[key], f"ASR recipe.inference.{key}", minimum=0)
    for key in (
        "word_threshold",
        "entropy_threshold",
        "logprob_threshold",
        "no_speech_threshold",
        "temperature",
        "temperature_increment",
    ):
        _number(inference[key], f"ASR recipe.inference.{key}")
    for key in ("translate", "split_on_word", "no_fallback"):
        if not isinstance(inference[key], bool):
            raise ResultImportError(f"ASR recipe.inference.{key} must be boolean")
    if inference["translate"] is not False:
        raise ResultImportError("translated ASR output is outside this importer contract")
    expected_glossary = None
    if glossary is not None:
        expected_glossary = {
            "glossary_revision_id": glossary["glossary_revision_id"],
            "revision": glossary["revision"],
            "sha256": glossary["sha256"],
            "prompt_sha256": glossary["prompt_sha256"],
        }
    if not _canonical_equal(value["glossary"], expected_glossary):
        raise ResultImportError("ASR recipe.glossary disagrees with its observation")
    return value


def validate_asr_whispercpp_result(
    raw: object, *, result_file_path: Path | None = None
) -> dict[str, Any]:
    """Validate all structural and cross-array invariants of a completed result."""

    result = _object(raw, "ASR result")
    _exact_keys(
        result,
        "ASR result",
        {
            "schema_version",
            "job_id",
            "status",
            "dry_run",
            "work_order_sha256",
            "recipe_id",
            "recipe_sha256",
            "result_key",
            "processing_run",
            "run_input",
            "input",
            "engine",
            "model",
            "glossary",
            "catalog_context",
            "window",
            "commands",
            "artifacts",
            "transcript",
            "catalog_records",
            "result_path",
            "duration_ms",
            "errors",
        },
    )
    if result["schema_version"] != SCHEMA_VERSION:
        raise ResultImportError("ASR result.schema_version must equal 1")
    if result["status"] != "completed" or result["dry_run"] is not False:
        raise ResultImportError("ASR result must be completed and non-dry-run")
    if _array(result["errors"], "ASR result.errors"):
        raise ResultImportError("completed ASR result.errors must be empty")
    job_id = _identifier(result["job_id"], "ASR result.job_id")
    work_order_sha = _sha256(result["work_order_sha256"], "ASR result.work_order_sha256")
    recipe_id = _string(result["recipe_id"], "ASR result.recipe_id", maximum=128)
    if not RECIPE_ID_RE.fullmatch(recipe_id):
        raise ResultImportError("ASR result.recipe_id is invalid")
    recipe_sha = _sha256(result["recipe_sha256"], "ASR result.recipe_sha256")
    result_key = _sha256(result["result_key"], "ASR result.result_key")
    _integer(result["duration_ms"], "ASR result.duration_ms", minimum=0)
    result_path_text = _string(result["result_path"], "ASR result.result_path")
    result_path = Path(result_path_text)
    if not result_path.is_absolute() or "://" in result_path_text:
        raise ResultImportError("ASR result.result_path must be an absolute local path")
    if result_path.name != "result.json" or result_path.resolve(strict=False) != result_path:
        raise ResultImportError("ASR result.result_path must be a resolved immutable result.json path")
    if result_file_path is not None and result_path.resolve(strict=False) != result_file_path.resolve():
        raise ResultImportError("ASR result.result_path does not identify the imported result file")

    run = _object(result["processing_run"], "ASR processing_run")
    _exact_keys(
        run,
        "ASR processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "model_id",
            "glossary_revision_id",
            "parameters_json",
            "environment_json",
            "random_seed",
            "started_at",
            "completed_at",
            "status",
            "error_text",
        },
    )
    run_id = _string(run["processing_run_id"], "ASR processing_run.processing_run_id", maximum=128)
    if not RUN_ID_RE.fullmatch(run_id):
        raise ResultImportError("ASR processing_run_id is invalid")
    if run["stage"] != STAGE or run["status"] != "completed" or run["error_text"] is not None:
        raise ResultImportError("ASR processing_run must be a successful asr_whispercpp run")
    _string(run["implementation_version"], "ASR processing_run.implementation_version", maximum=128)
    model_id = _identifier(run["model_id"], "ASR processing_run.model_id")
    glossary_revision_id = (
        None
        if run["glossary_revision_id"] is None
        else _identifier(run["glossary_revision_id"], "ASR processing_run.glossary_revision_id")
    )
    if run["random_seed"] is not None:
        _integer(run["random_seed"], "ASR processing_run.random_seed")
    started_at = _timestamp(run["started_at"], "ASR processing_run.started_at")
    completed_at = _timestamp(run["completed_at"], "ASR processing_run.completed_at")
    if run["started_at"] != started_at or run["completed_at"] != completed_at:
        raise ResultImportError("ASR processing-run timestamps must use canonical UTC")
    if _timestamp_value(completed_at) < _timestamp_value(started_at):
        raise ResultImportError("ASR processing_run.completed_at precedes started_at")
    parameters_text, recipe = _json_text(run["parameters_json"], "ASR processing_run.parameters_json")
    environment_text, environment = _json_text(run["environment_json"], "ASR processing_run.environment_json")
    if not isinstance(environment, dict):
        raise ResultImportError("ASR processing_run.environment_json must encode an object")

    input_row, input_path = _validate_observation(
        result["input"],
        "ASR input",
        extra_keys={"media_id", "artifact_id", "parent_processing_run_id", "probe"},
    )
    input_sha = input_row["sha256"]
    input_media_id = _string(input_row["media_id"], "ASR input.media_id", maximum=96)
    if input_media_id != f"media_sha256_{input_sha}":
        raise ResultImportError("ASR input.media_id must derive from input SHA-256")
    input_artifact_id = _identifier(input_row["artifact_id"], "ASR input.artifact_id")
    parent_run_id = _identifier(
        input_row["parent_processing_run_id"], "ASR input.parent_processing_run_id"
    )
    probe = _validate_probe(input_row["probe"], "ASR input.probe")

    engine, engine_path = _validate_observation(
        result["engine"],
        "ASR engine",
        extra_keys={"version", "version_evidence", "build"},
    )
    _string(engine["version"], "ASR engine.version", maximum=1_000)
    if engine["version_evidence"] != "source_revision_plus_executable_sha256":
        raise ResultImportError("ASR engine.version_evidence is unsupported")
    engine["build"] = _validate_build(engine["build"], "ASR engine.build")

    model, model_path = _validate_observation(
        result["model"],
        "ASR model",
        extra_keys={"model_id", "name", "revision", "source", "license_label"},
    )
    if _identifier(model["model_id"], "ASR model.model_id") != model_id:
        raise ResultImportError("ASR model.model_id disagrees with processing_run.model_id")
    for key in ("name", "revision", "source", "license_label"):
        _string(model[key], f"ASR model.{key}", maximum=4_000)

    glossary: dict[str, Any] | None = None
    glossary_path: Path | None = None
    if result["glossary"] is not None:
        glossary, glossary_path = _validate_observation(
            result["glossary"],
            "ASR glossary",
            extra_keys={
                "glossary_revision_id",
                "revision",
                "language",
                "prompt_sha256",
                "term_count",
            },
        )
        if _identifier(
            glossary["glossary_revision_id"], "ASR glossary.glossary_revision_id"
        ) != glossary_revision_id:
            raise ResultImportError("ASR glossary revision disagrees with processing_run")
        _string(glossary["revision"], "ASR glossary.revision", maximum=1_000)
        _string(glossary["language"], "ASR glossary.language", maximum=64)
        _sha256(glossary["prompt_sha256"], "ASR glossary.prompt_sha256")
        _integer(glossary["term_count"], "ASR glossary.term_count", minimum=1, maximum=128)
    elif glossary_revision_id is not None:
        raise ResultImportError("ASR processing_run references an absent glossary observation")

    context: dict[str, Any] | None = None
    if result["catalog_context"] is not None:
        context = _object(result["catalog_context"], "ASR catalog_context")
        _exact_keys(context, "ASR catalog_context", {"recording_id", "rendition_id"})
        _identifier(context["recording_id"], "ASR catalog_context.recording_id")
        if context["rendition_id"] is not None:
            _identifier(context["rendition_id"], "ASR catalog_context.rendition_id")

    window = _validate_window(result["window"], "ASR window")
    if window["end_ms"] > probe["duration_ms"]:
        raise ResultImportError("ASR window exceeds input duration")
    commands = _array(result["commands"], "ASR result.commands", length=2)
    for index, command in enumerate(commands):
        _validate_command(command, f"ASR result.commands[{index}]")

    recipe = _validate_recipe(
        recipe,
        run=run,
        engine=engine,
        model=model,
        glossary=glossary,
        window=window,
    )
    if not isinstance(recipe, dict):  # for type narrowing
        raise AssertionError
    calculated_recipe_sha = sha256_bytes(canonical_json(recipe).encode("utf-8"))
    if recipe_sha != calculated_recipe_sha:
        raise ResultImportError("ASR recipe_sha256 disagrees with processing parameters")
    if recipe_id != f"recipe_asr_whispercpp_{recipe_sha[:32]}":
        raise ResultImportError("ASR recipe_id does not derive from recipe_sha256")
    run_input = _object(result["run_input"], "ASR run_input")
    _exact_keys(
        run_input,
        "ASR run_input",
        {
            "run_input_id",
            "processing_run_id",
            "object_type",
            "object_id",
            "input_role",
            "input_sha256",
        },
    )
    expected_run_input_id = _producer_id("run_input", run_id, input_media_id, input_artifact_id)
    if (
        _identifier(run_input["run_input_id"], "ASR run_input.run_input_id")
        != expected_run_input_id
        or run_input["processing_run_id"] != run_id
        or run_input["object_type"] != "media"
        or run_input["object_id"] != input_media_id
        or run_input["input_role"] != "normalized_audio"
        or run_input["input_sha256"] != input_sha
    ):
        raise ResultImportError("ASR run_input disagrees with the input observation")

    expected_result_key = sha256_bytes(
        canonical_json(
            {
                "work_order_sha256": work_order_sha,
                "input_sha256": input_sha,
                "input_media_id": input_media_id,
                "input_artifact_id": input_artifact_id,
                "parent_processing_run_id": parent_run_id,
                "recipe_id": recipe_id,
                "catalog_context": context,
            }
        ).encode("utf-8")
    )
    if result_key != expected_result_key:
        raise ResultImportError("ASR result_key does not derive from the result identity")

    artifacts = _array(result["artifacts"], "ASR result.artifacts", length=2)
    normalized_artifacts: list[dict[str, Any]] = []
    artifact_paths: dict[str, Path] = {}
    artifact_ids: set[str] = set()
    artifact_kinds: set[str] = set()
    for index, artifact_value in enumerate(artifacts):
        artifact = _object(artifact_value, f"ASR artifact[{index}]")
        _exact_keys(
            artifact,
            f"ASR artifact[{index}]",
            {
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "storage_uri",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
                "metadata_json",
            },
        )
        artifact_id = _identifier(artifact["artifact_id"], f"ASR artifact[{index}].artifact_id")
        kind = _string(artifact["artifact_kind"], f"ASR artifact[{index}].artifact_kind")
        if kind not in {"whispercpp_output_json_full", "transcript_normalized_json"}:
            raise ResultImportError(f"ASR artifact[{index}].artifact_kind is unsupported")
        digest = _sha256(artifact["sha256"], f"ASR artifact[{index}].sha256")
        byte_count = _integer(artifact["byte_count"], f"ASR artifact[{index}].byte_count", minimum=1)
        if artifact["processing_run_id"] != run_id or artifact["schema_version"] != 1:
            raise ResultImportError(f"ASR artifact[{index}] run or schema is inconsistent")
        if artifact["visibility"] != "private":
            raise ResultImportError("ASR artifacts must remain private")
        metadata_text, metadata = _json_text(
            artifact["metadata_json"], f"ASR artifact[{index}].metadata_json"
        )
        if not isinstance(metadata, dict):
            raise ResultImportError(f"ASR artifact[{index}].metadata_json must encode an object")
        expected_id = _producer_id("artifact", run_id, kind, digest)
        if artifact_id != expected_id:
            raise ResultImportError(f"ASR artifact[{index}].artifact_id is not deterministic")
        path = _local_file_uri(artifact["storage_uri"], f"ASR artifact[{index}].storage_uri")
        if artifact_id in artifact_ids or kind in artifact_kinds:
            raise ResultImportError("ASR artifacts contain duplicate IDs or kinds")
        artifact_ids.add(artifact_id)
        artifact_kinds.add(kind)
        artifact_paths[kind] = path
        normalized_artifacts.append({**artifact, "metadata_json": metadata_text})
    if artifact_kinds != {"whispercpp_output_json_full", "transcript_normalized_json"}:
        raise ResultImportError("ASR result must contain both expected artifacts")
    expected_artifact_paths = {
        "whispercpp_output_json_full": result_path.parent / "whisper.raw.json",
        "transcript_normalized_json": result_path.parent / "transcript.normalized.json",
    }
    if any(artifact_paths[kind] != path for kind, path in expected_artifact_paths.items()):
        raise ResultImportError("ASR artifact paths must use the immutable result directory layout")

    transcript = _validate_transcript(result["transcript"], window=window, recipe=recipe)
    records = _validate_catalog_records(
        result["catalog_records"],
        run=run,
        run_input=run_input,
        artifacts=normalized_artifacts,
        transcript=transcript,
        context=context,
        glossary_revision_id=glossary_revision_id,
    )
    if recipe["implementation_version"] == "0.3.0":
        _validate_descriptor_command_provenance(
            environment,
            commands,
            run=run,
            recipe=recipe,
            result_path=result_path,
            result_key=result_key,
            probe=probe,
            input_path=input_path,
            engine_path=engine_path,
            model_path=model_path,
            glossary=glossary,
        )

    normalized = dict(result)
    normalized["processing_run"] = {
        **run,
        "parameters_json": parameters_text,
        "environment_json": environment_text,
        "started_at": started_at,
        "completed_at": completed_at,
    }
    normalized["input"] = input_row
    normalized["engine"] = engine
    normalized["model"] = model
    normalized["glossary"] = glossary
    normalized["catalog_context"] = context
    normalized["window"] = window
    normalized["artifacts"] = normalized_artifacts
    normalized["transcript"] = transcript
    normalized["catalog_records"] = records
    normalized["_paths"] = {
        "input": input_path,
        "engine": engine_path,
        "model": model_path,
        "glossary": glossary_path,
        **artifact_paths,
    }
    normalized["_job_id"] = job_id
    return normalized


def _validate_transcript(
    value: object, *, window: dict[str, int], recipe: dict[str, Any]
) -> dict[str, Any]:
    transcript = _object(value, "ASR transcript")
    _exact_keys(
        transcript,
        "ASR transcript",
        {
            "schema_version",
            "language",
            "window",
            "segment_count",
            "token_count",
            "quality_flags",
            "segments",
            "engine_metadata_json",
        },
    )
    if transcript["schema_version"] != 1 or not _canonical_equal(transcript["window"], window):
        raise ResultImportError("ASR transcript schema or window disagrees with the envelope")
    language = _object(transcript["language"], "ASR transcript.language")
    _exact_keys(language, "ASR transcript.language", {"requested", "detected"})
    requested = _string(language["requested"], "ASR transcript.language.requested", maximum=64)
    detected = _string(language["detected"], "ASR transcript.language.detected", maximum=64)
    if requested != recipe["inference"]["language"]:
        raise ResultImportError("ASR transcript requested language disagrees with the recipe")
    engine_metadata_text, engine_metadata = _json_text(
        transcript["engine_metadata_json"], "ASR transcript.engine_metadata_json"
    )
    if not isinstance(engine_metadata, dict):
        raise ResultImportError("ASR transcript.engine_metadata_json must encode an object")
    timing_contract = recipe["implementation_version"] in {"0.2.3", "0.3.0"}
    quality_flags = _array(transcript["quality_flags"], "ASR transcript.quality_flags")
    supported_transcript_flags = {
        "segment_end_after_requested_window",
        "token_timing_unavailable",
    }
    if len(quality_flags) != len(set(quality_flags)) or any(
        item not in supported_transcript_flags for item in quality_flags
    ):
        raise ResultImportError("ASR transcript.quality_flags is unsupported or duplicated")
    segments_raw = _array(transcript["segments"], "ASR transcript.segments")
    if transcript["segment_count"] != len(segments_raw):
        raise ResultImportError("ASR transcript.segment_count is inconsistent")
    segments: list[dict[str, Any]] = []
    token_count = 0
    expected_transcript_flags: set[str] = set()
    for segment_index, segment_value in enumerate(segments_raw):
        label = f"ASR transcript.segments[{segment_index}]"
        segment = _object(segment_value, label)
        _exact_keys(
            segment,
            label,
            {
                "ordinal",
                "start_ms",
                "end_ms",
                "text",
                "tokens",
                "quality_flags",
                "window_overrun_ms",
                "metadata_json",
            },
        )
        if segment["ordinal"] != segment_index:
            raise ResultImportError(f"{label}.ordinal must be contiguous from zero")
        start = _integer(segment["start_ms"], f"{label}.start_ms", minimum=0)
        end = _integer(segment["end_ms"], f"{label}.end_ms", minimum=1)
        if end <= start or start < window["offset_ms"] or start >= window["end_ms"]:
            raise ResultImportError(f"{label} lies outside the requested half-open window")
        overrun = max(0, end - window["end_ms"])
        if overrun > MAX_BOUNDARY_OVERRUN_MS or segment["window_overrun_ms"] != overrun:
            raise ResultImportError(f"{label}.window_overrun_ms is inconsistent")
        expected_flags = ["end_after_requested_window"] if overrun else []
        if overrun:
            expected_transcript_flags.add("segment_end_after_requested_window")
        _string(segment["text"], f"{label}.text", allow_empty=True)
        metadata_text, metadata = _json_text(segment["metadata_json"], f"{label}.metadata_json")
        if not isinstance(metadata, dict):
            raise ResultImportError(f"{label}.metadata_json must encode an object")
        tokens_raw = _array(segment["tokens"], f"{label}.tokens")
        tokens: list[dict[str, Any]] = []
        token_timing_unavailable = False
        for token_index, token_value in enumerate(tokens_raw):
            token_label = f"{label}.tokens[{token_index}]"
            token = _object(token_value, token_label)
            token_keys = {
                "ordinal",
                "start_ms",
                "end_ms",
                "text",
                "token_id",
                "raw_probability",
                "raw_dtw_timestamp",
                "metadata_json",
            }
            timing_keys = {
                "timing_state",
                "timing_quality_flags",
                "original_offsets",
            }
            present_timing_keys = set(token) & timing_keys
            if present_timing_keys and (
                not timing_contract or present_timing_keys != timing_keys
            ):
                raise ResultImportError(
                    f"{token_label} timing anomaly fields require the exact 0.2.3/0.3.0 trio"
                )
            token_keys |= present_timing_keys
            _exact_keys(
                token,
                token_label,
                token_keys,
            )
            if token["ordinal"] != token_index:
                raise ResultImportError(f"{token_label}.ordinal must be contiguous from zero")
            _string(token["text"], f"{token_label}.text", allow_empty=True)
            _integer(token["token_id"], f"{token_label}.token_id")
            _number(token["raw_probability"], f"{token_label}.raw_probability", minimum=0, maximum=1)
            _number(token["raw_dtw_timestamp"], f"{token_label}.raw_dtw_timestamp")
            token_metadata_text, token_metadata = _json_text(
                token["metadata_json"], f"{token_label}.metadata_json"
            )
            if not isinstance(token_metadata, dict):
                raise ResultImportError(f"{token_label}.metadata_json must encode an object")
            token_start = token["start_ms"]
            token_end = token["end_ms"]
            if (token_start is None) != (token_end is None):
                raise ResultImportError(f"{token_label} timestamp endpoints must both be null or integers")
            if token_start is not None:
                token_start = _integer(token_start, f"{token_label}.start_ms", minimum=0)
                token_end = _integer(token_end, f"{token_label}.end_ms", minimum=0)
                if token_end < token_start or token_start < start or token_end > end:
                    raise ResultImportError(f"{token_label} timestamp lies outside its segment")
            normalized_token = {**token, "metadata_json": token_metadata_text}
            if timing_contract:
                raw_offsets = token_metadata.get("offsets")
                if present_timing_keys:
                    if (
                        token_start is not None
                        or token["timing_state"] != "unavailable"
                        or token["timing_quality_flags"]
                        != ["invalid_upstream_inverted"]
                    ):
                        raise ResultImportError(
                            f"{token_label} unavailable timing fields are inconsistent"
                        )
                    original = _object(
                        token["original_offsets"], f"{token_label}.original_offsets"
                    )
                    _exact_keys(
                        original, f"{token_label}.original_offsets", {"from", "to"}
                    )
                    original_from = _integer(
                        original["from"],
                        f"{token_label}.original_offsets.from",
                        minimum=0,
                        maximum=MAX_WINDOW_MS,
                    )
                    original_to = _integer(
                        original["to"],
                        f"{token_label}.original_offsets.to",
                        minimum=0,
                        maximum=MAX_WINDOW_MS,
                    )
                    if original_to >= original_from:
                        raise ResultImportError(
                            f"{token_label}.original_offsets is not inverted"
                        )
                    raw_offsets = _object(raw_offsets, f"{token_label} raw offsets")
                    _exact_keys(raw_offsets, f"{token_label} raw offsets", {"from", "to"})
                    raw_from = _integer(
                        raw_offsets["from"],
                        f"{token_label} raw offsets.from",
                        minimum=0,
                        maximum=MAX_WINDOW_MS,
                    )
                    raw_to = _integer(
                        raw_offsets["to"],
                        f"{token_label} raw offsets.to",
                        minimum=0,
                        maximum=MAX_WINDOW_MS,
                    )
                    if raw_from != original_from or raw_to != original_to:
                        raise ResultImportError(
                            f"{token_label} original timing differs from preserved raw offsets"
                        )
                    normalized_token["original_offsets"] = {
                        "from": original_from,
                        "to": original_to,
                    }
                    token_timing_unavailable = True
                elif token_start is None:
                    if raw_offsets is not None:
                        raise ResultImportError(
                            f"{token_label} null timing disagrees with preserved raw offsets"
                        )
                else:
                    raw_offsets = _object(raw_offsets, f"{token_label} raw offsets")
                    _exact_keys(raw_offsets, f"{token_label} raw offsets", {"from", "to"})
                    raw_from = _integer(
                        raw_offsets["from"],
                        f"{token_label} raw offsets.from",
                        minimum=0,
                        maximum=MAX_WINDOW_MS,
                    )
                    raw_to = _integer(
                        raw_offsets["to"],
                        f"{token_label} raw offsets.to",
                        minimum=0,
                        maximum=MAX_WINDOW_MS,
                    )
                    if raw_from != token_start or raw_to != token_end:
                        raise ResultImportError(
                            f"{token_label} timing differs from preserved raw offsets"
                        )
            tokens.append(normalized_token)
        if token_timing_unavailable:
            expected_flags.append("token_timing_unavailable")
            expected_transcript_flags.add("token_timing_unavailable")
        if segment["quality_flags"] != expected_flags:
            raise ResultImportError(f"{label}.quality_flags is inconsistent")
        token_count += len(tokens)
        segments.append({**segment, "tokens": tokens, "metadata_json": metadata_text})
    if transcript["token_count"] != token_count:
        raise ResultImportError("ASR transcript.token_count is inconsistent")
    if quality_flags != sorted(expected_transcript_flags):
        raise ResultImportError("ASR transcript quality flags do not match segment overruns")
    return {
        **transcript,
        "language": {"requested": requested, "detected": detected},
        "window": window,
        "segments": segments,
        "engine_metadata_json": engine_metadata_text,
    }


def _expected_transcript_rows(
    *,
    run: dict[str, Any],
    transcript: dict[str, Any],
    context: dict[str, Any],
    glossary_revision_id: str | None,
) -> dict[str, list[dict[str, Any]]]:
    run_id = run["processing_run_id"]
    revision_id = _producer_id(
        "transcript_revision", run_id, context["recording_id"], context["rendition_id"]
    )
    revision = {
        "revision_id": revision_id,
        "recording_id": context["recording_id"],
        "rendition_id": context["rendition_id"],
        "processing_run_id": run_id,
        "revision_kind": "contextual_asr" if glossary_revision_id else "raw_asr",
        "origin": "whisper.cpp output-json-full",
        "language": transcript["language"]["detected"],
        "glossary_revision_id": glossary_revision_id,
        "review_state": "machine",
        "created_at": run["completed_at"],
        "metadata_json": canonical_json(
            {
                "confidence_calibration": "none",
                "quality_flags": transcript["quality_flags"],
                "raw_scores_preserved": True,
            }
        ),
    }
    segments: list[dict[str, Any]] = []
    words: list[dict[str, Any]] = []
    for segment in transcript["segments"]:
        segment_id = _producer_id("transcript_segment", revision_id, segment["ordinal"])
        segment_metadata = {
            "engine_segment": json.loads(segment["metadata_json"]),
            "quality_flags": segment["quality_flags"],
            "window_overrun_ms": segment["window_overrun_ms"],
        }
        token_timing_anomalies = [
            {
                "ordinal": token["ordinal"],
                "timing_state": token["timing_state"],
                "timing_quality_flags": token["timing_quality_flags"],
                "original_offsets": token["original_offsets"],
            }
            for token in segment["tokens"]
            if token.get("timing_state") == "unavailable"
        ]
        if token_timing_anomalies:
            segment_metadata["token_timing_anomalies"] = token_timing_anomalies
        segments.append(
            {
                "segment_id": segment_id,
                "revision_id": revision_id,
                "ordinal": segment["ordinal"],
                "start_ms": segment["start_ms"],
                "end_ms": segment["end_ms"],
                "text": segment["text"],
                "normalized_text": None,
                "speaker_label": None,
                "language": transcript["language"]["detected"],
                "confidence_band": None,
                "calibrated_probability": None,
                "metadata_json": canonical_json(segment_metadata),
            }
        )
        for token in segment["tokens"]:
            probability = token["raw_probability"]
            words.append(
                {
                    "word_id": _producer_id("transcript_word", segment_id, token["ordinal"]),
                    "segment_id": segment_id,
                    "ordinal": token["ordinal"],
                    "start_ms": token["start_ms"],
                    "end_ms": token["end_ms"],
                    "token": token["text"],
                    "normalized_token": None,
                    "asr_log_probability": math.log(probability) if probability > 0 else None,
                    "alignment_score": None,
                    "calibrated_probability": None,
                }
            )
    return {
        "transcript_revisions": [revision],
        "transcript_segments": segments,
        "transcript_words": words,
    }

def _validate_catalog_records(
    value: object,
    *,
    run: dict[str, Any],
    run_input: dict[str, Any],
    artifacts: list[dict[str, Any]],
    transcript: dict[str, Any],
    context: dict[str, Any] | None,
    glossary_revision_id: str | None,
) -> dict[str, Any]:
    records = _object(value, "ASR catalog_records")
    base_keys = {"processing_runs", "run_inputs", "artifacts"}
    transcript_keys = {"transcript_revisions", "transcript_segments", "transcript_words"}
    _exact_keys(records, "ASR catalog_records", base_keys | (transcript_keys if context else set()))
    processing_runs = _array(records["processing_runs"], "ASR catalog processing_runs", length=1)
    run_inputs = _array(records["run_inputs"], "ASR catalog run_inputs", length=1)
    catalog_artifacts = _array(records["artifacts"], "ASR catalog artifacts", length=2)
    if not _canonical_equal(processing_runs[0], run):
        raise ResultImportError("ASR catalog processing run differs from the envelope")
    if not _canonical_equal(run_inputs[0], run_input):
        raise ResultImportError("ASR catalog run input differs from the envelope")
    if not _canonical_equal(catalog_artifacts, artifacts):
        raise ResultImportError("ASR catalog artifacts differ from the envelope")
    normalized = dict(records)
    if context is None:
        return normalized
    expected = _expected_transcript_rows(
        run=run,
        transcript=transcript,
        context=context,
        glossary_revision_id=glossary_revision_id,
    )
    for key, expected_rows in expected.items():
        rows = _array(records[key], f"ASR catalog {key}")
        if not _canonical_equal(rows, expected_rows):
            raise ResultImportError(f"ASR catalog {key} differs from normalized transcript data")
        normalized[key] = expected_rows
    return normalized


def _validate_artifact_contents(result: dict[str, Any]) -> None:
    paths = result["_paths"]
    # Rehash every local provenance input as well as both output artifacts.  This
    # makes an old, copied result envelope insufficient without its exact bytes.
    for key, row in (
        ("input", result["input"]),
        ("engine", result["engine"]),
        ("model", result["model"]),
    ):
        _verify_hash(paths[key], row["sha256"], row["byte_count"], f"ASR {key}")
    if result["glossary"] is not None:
        _verify_hash(
            paths["glossary"],
            result["glossary"]["sha256"],
            result["glossary"]["byte_count"],
            "ASR glossary",
        )
    artifact_by_kind = {row["artifact_kind"]: row for row in result["artifacts"]}
    bodies: dict[str, bytes] = {}
    for kind, artifact in artifact_by_kind.items():
        captured = _verify_hash(
            paths[kind],
            artifact["sha256"],
            artifact["byte_count"],
            f"ASR artifact {kind}",
            capture=True,
        )
        assert captured is not None
        bodies[kind] = captured
    try:
        raw_json = json.loads(bodies["whispercpp_output_json_full"].decode("utf-8"))
        normalized_json = json.loads(bodies["transcript_normalized_json"].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"ASR artifact is not valid UTF-8 JSON: {error}") from error
    if not _canonical_equal(normalized_json, result["transcript"]):
        raise ResultImportError("normalized transcript artifact differs from the result envelope")
    raw = _object(raw_json, "whisper.cpp raw artifact")
    raw_segments = _array(raw.get("transcription"), "whisper.cpp raw transcription")
    if len(raw_segments) != len(result["transcript"]["segments"]):
        raise ResultImportError("raw whisper segment count differs from normalized transcript")
    raw_engine_metadata = {key: value for key, value in raw.items() if key != "transcription"}
    if canonical_json(raw_engine_metadata) != result["transcript"]["engine_metadata_json"]:
        raise ResultImportError("raw whisper engine metadata differs from the normalized transcript")
    for segment_index, (raw_segment, segment) in enumerate(
        zip(raw_segments, result["transcript"]["segments"], strict=True)
    ):
        if canonical_json(raw_segment) != segment["metadata_json"]:
            raise ResultImportError(
                f"raw whisper segment[{segment_index}] differs from preserved metadata"
            )
        raw_tokens = _object(raw_segment, f"raw segment[{segment_index}]").get("tokens")
        raw_tokens = _array(raw_tokens, f"raw segment[{segment_index}].tokens")
        if len(raw_tokens) != len(segment["tokens"]):
            raise ResultImportError(f"raw whisper segment[{segment_index}] token count differs")
        for token_index, (raw_token, token) in enumerate(
            zip(raw_tokens, segment["tokens"], strict=True)
        ):
            if canonical_json(raw_token) != token["metadata_json"]:
                raise ResultImportError(
                    f"raw whisper token[{segment_index}:{token_index}] differs from preserved metadata"
                )


def _catalog_json_object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise ResultImportError(f"{label} must be catalog JSON")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        parsed: dict[str, Any] = {}
        for key, item in pairs:
            if key in parsed:
                raise ResultImportError(f"{label} contains duplicate key {key!r}")
            parsed[key] = item
        return parsed

    try:
        parsed = json.loads(value, object_pairs_hook=reject_duplicates)
    except json.JSONDecodeError as error:
        raise ResultImportError(f"{label} is invalid") from error
    if not isinstance(parsed, dict):
        raise ResultImportError(f"{label} must be a JSON object")
    return parsed


def _is_local_window_context(
    *,
    input_artifact: sqlite3.Row,
    artifact_metadata: dict[str, Any],
    rendition: sqlite3.Row | None,
    rendition_metadata: dict[str, Any] | None,
) -> bool:
    """Return whether catalog evidence identifies a derived local window."""

    lineage_keys = {
        "local_window_result_sha256",
        "local_window_result_uri",
        "source_time_mapping",
    }
    return (
        input_artifact["artifact_kind"] == "window_audio_16khz_mono_flac"
        or bool(lineage_keys & artifact_metadata.keys())
        or (
            rendition is not None
            and rendition["rendition_kind"].startswith("local_window:")
        )
        or (
            rendition_metadata is not None
            and bool(lineage_keys & rendition_metadata.keys())
        )
    )


def _reject_unsupported_local_window_context(
    connection: sqlite3.Connection,
    *,
    media: sqlite3.Row,
    input_artifact: sqlite3.Row,
    artifact_metadata: dict[str, Any],
    rendition: sqlite3.Row | None,
    rendition_metadata: dict[str, Any] | None,
) -> None:
    """Keep artifact-local ASR out of the recording-coordinate transcript tables.

    A local-window result remains valid private provenance when ``catalog_context``
    is null.  Contextual admission is a different operation: transcript timestamps
    are recording coordinates, while the ASR producer reports coordinates relative
    to its input artifact.  This importer has no sealed translation contract, so it
    must not infer one from source offsets or mutable catalogue metadata.
    """

    evidence = _is_local_window_context(
        input_artifact=input_artifact,
        artifact_metadata=artifact_metadata,
        rendition=rendition,
        rendition_metadata=rendition_metadata,
    )
    if not evidence:
        return

    exact_timeline = False
    if rendition is not None:
        spans = connection.execute(
            """
            SELECT ordinal, media_start_ms, media_end_ms, recording_start_ms,
                   recording_end_ms, mapping_kind, confidence_state
            FROM timeline_map_spans
            WHERE rendition_id = ?
            ORDER BY ordinal
            """,
            (rendition["rendition_id"],),
        ).fetchall()
        if len(spans) == 1:
            span = spans[0]
            exact_timeline = (
                span["ordinal"] == 0
                and span["media_start_ms"] == 0
                and span["media_end_ms"] == media["duration_ms"]
                and span["recording_start_ms"] is not None
                and span["recording_end_ms"] is not None
                and span["mapping_kind"] == "exact"
                and span["confidence_state"] in {"metadata_only", "reviewed"}
                and span["recording_end_ms"] - span["recording_start_ms"]
                == span["media_end_ms"] - span["media_start_ms"]
            )
    if not exact_timeline:
        raise ResultImportError(
            "contextual local-window ASR has no full-coverage exact catalog "
            "timeline; artifact-local timestamps cannot be admitted as recording "
            "coordinates"
        )
    raise ResultImportError(
        "contextual local-window ASR requires a separately sealed "
        "artifact-to-recording translation bridge; this importer does not infer "
        "recording timestamps"
    )


def _require_catalog_dependencies(
    connection: sqlite3.Connection,
    result: dict[str, Any],
    *,
    allow_rendition_local: bool = False,
) -> None:
    input_row = result["input"]
    media = connection.execute(
        "SELECT media_id, sha256, byte_count, duration_ms FROM media_objects WHERE media_id = ?",
        (input_row["media_id"],),
    ).fetchone()
    if media is None:
        raise ResultImportError("ASR input media_id is not registered in the catalog")
    if media["sha256"] != input_row["sha256"] or media["byte_count"] != input_row["byte_count"]:
        raise ResultImportError("ASR input media registry row differs from the observed bytes")
    input_artifact = connection.execute(
        """
        SELECT processing_run_id, artifact_kind, storage_uri, sha256, byte_count,
               visibility, metadata_json
        FROM artifacts WHERE artifact_id = ?
        """,
        (input_row["artifact_id"],),
    ).fetchone()
    if input_artifact is None:
        raise ResultImportError("ASR input artifact_id is not registered in the catalog")
    input_uri = result["_paths"]["input"].as_uri()
    if (
        input_artifact["processing_run_id"] != input_row["parent_processing_run_id"]
        or input_artifact["storage_uri"] != input_uri
        or input_artifact["sha256"] != input_row["sha256"]
        or input_artifact["byte_count"] != input_row["byte_count"]
        or input_artifact["visibility"] != "private"
    ):
        raise ResultImportError("ASR input artifact registry row differs from the result")
    model_row = connection.execute(
        """
        SELECT task, name, version, weights_sha256, license_label, configuration_json
        FROM models WHERE model_id = ?
        """,
        (result["model"]["model_id"],),
    ).fetchone()
    if model_row is None:
        raise ResultImportError("ASR model_id must already exist in the reviewed model registry")
    try:
        model_configuration = json.loads(model_row["configuration_json"])
    except json.JSONDecodeError as error:  # database CHECK should make this unreachable.
        raise ResultImportError("registered ASR model configuration is invalid") from error
    if (
        model_row["task"] != "asr"
        or model_row["name"] != result["model"]["name"]
        or model_row["version"] != result["model"]["revision"]
        or model_row["weights_sha256"] != result["model"]["sha256"]
        or model_row["license_label"] != result["model"]["license_label"]
        or not isinstance(model_configuration, dict)
        or model_configuration.get("source") != result["model"]["source"]
    ):
        raise ResultImportError("registered ASR model does not exactly match result provenance")
    registry_snapshots = connection.execute(
        """
        SELECT link.model_snapshot_json
        FROM model_registry_manifest_models AS link
        JOIN model_registry_manifest_imports AS manifest
          ON manifest.manifest_id = link.manifest_id
        WHERE link.model_id = ?
        ORDER BY manifest.imported_at, link.manifest_id
        """,
        (result["model"]["model_id"],),
    ).fetchall()
    matching_snapshot = False
    for snapshot_row in registry_snapshots:
        try:
            snapshot = json.loads(snapshot_row["model_snapshot_json"])
        except json.JSONDecodeError as error:  # database CHECK should make this unreachable.
            raise ResultImportError("registered ASR model snapshot is invalid") from error
        if (
            isinstance(snapshot, dict)
            and snapshot.get("model_id") == result["model"]["model_id"]
            and snapshot.get("task") == "asr"
            and snapshot.get("name") == result["model"]["name"]
            and snapshot.get("version") == result["model"]["revision"]
            and snapshot.get("weights_uri") == result["_paths"]["model"].as_uri()
            and snapshot.get("weights_sha256") == result["model"]["sha256"]
            and snapshot.get("weights_byte_count") == result["model"]["byte_count"]
            and snapshot.get("license_label") == result["model"]["license_label"]
            and _canonical_equal(snapshot.get("configuration_json"), model_configuration)
        ):
            matching_snapshot = True
            break
    if not matching_snapshot:
        raise ResultImportError(
            "ASR model lacks an exact checksummed model-registry manifest snapshot"
        )

    glossary = result["glossary"]
    if glossary is not None:
        glossary_row = connection.execute(
            """
            SELECT sha256, artifact_uri FROM glossary_revisions
            WHERE glossary_revision_id = ?
            """,
            (glossary["glossary_revision_id"],),
        ).fetchone()
        if glossary_row is None:
            raise ResultImportError("ASR glossary revision must already exist in the registry")
        if (
            glossary_row["sha256"] != glossary["sha256"]
            or glossary_row["artifact_uri"] != result["_paths"]["glossary"].as_uri()
        ):
            raise ResultImportError("registered ASR glossary revision does not match result provenance")

    context = result["catalog_context"]
    if context is None:
        return
    artifact_metadata = _catalog_json_object(
        input_artifact["metadata_json"], "ASR input artifact.metadata_json"
    )
    recording = connection.execute(
        "SELECT recording_id FROM recordings WHERE recording_id = ?",
        (context["recording_id"],),
    ).fetchone()
    if recording is None:
        raise ResultImportError("ASR catalog_context recording_id does not exist")
    if context["rendition_id"] is None:
        if allow_rendition_local:
            raise ResultImportError(
                "rendition-local ASR admission requires an explicit rendition_id"
            )
        _reject_unsupported_local_window_context(
            connection,
            media=media,
            input_artifact=input_artifact,
            artifact_metadata=artifact_metadata,
            rendition=None,
            rendition_metadata=None,
        )
    if context["rendition_id"] is not None:
        rendition = connection.execute(
            """
            SELECT rendition_id, recording_id, media_id, rendition_kind, metadata_json
            FROM renditions WHERE rendition_id = ?
            """,
            (context["rendition_id"],),
        ).fetchone()
        if rendition is None:
            raise ResultImportError("ASR catalog_context rendition_id does not exist")
        if (
            rendition["recording_id"] != context["recording_id"]
            or rendition["media_id"] != input_row["media_id"]
        ):
            raise ResultImportError("ASR catalog_context rendition does not match recording/input media")
        rendition_metadata = _catalog_json_object(
            rendition["metadata_json"], "ASR catalog_context rendition.metadata_json"
        )
        if allow_rendition_local:
            if not _is_local_window_context(
                input_artifact=input_artifact,
                artifact_metadata=artifact_metadata,
                rendition=rendition,
                rendition_metadata=rendition_metadata,
            ):
                raise ResultImportError(
                    "rendition-local ASR admission requires exact local-window lineage"
                )
            return
        _reject_unsupported_local_window_context(
            connection,
            media=media,
            input_artifact=input_artifact,
            artifact_metadata=artifact_metadata,
            rendition=rendition,
            rendition_metadata=rendition_metadata,
        )


def _insert_exact_processing_run(connection: sqlite3.Connection, run: dict[str, Any]) -> None:
    expected = {
        key: run[key]
        for key in (
            "stage",
            "implementation_version",
            "model_id",
            "glossary_revision_id",
            "parameters_json",
            "environment_json",
            "random_seed",
            "started_at",
            "completed_at",
            "status",
            "error_text",
        )
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
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("ASR processing_run_id already has different data")
        return
    connection.execute(
        """
        INSERT INTO processing_runs(
            processing_run_id, stage, implementation_version, model_id,
            glossary_revision_id, parameters_json, environment_json, random_seed,
            started_at, completed_at, status, error_text
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run["processing_run_id"],
            run["stage"],
            run["implementation_version"],
            run["model_id"],
            run["glossary_revision_id"],
            run["parameters_json"],
            run["environment_json"],
            run["random_seed"],
            run["started_at"],
            run["completed_at"],
            run["status"],
            run["error_text"],
        ),
    )


def _insert_exact_run_input(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    expected = {
        key: row[key]
        for key in ("processing_run_id", "object_type", "object_id", "input_role", "input_sha256")
    }
    existing = connection.execute(
        """
        SELECT processing_run_id, object_type, object_id, input_role, input_sha256
        FROM run_inputs WHERE run_input_id = ?
        """,
        (row["run_input_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("ASR run_input_id already has different data")
        return
    collision = connection.execute(
        """
        SELECT run_input_id FROM run_inputs
        WHERE processing_run_id = ? AND object_type = ? AND object_id = ? AND input_role = ?
        """,
        (row["processing_run_id"], row["object_type"], row["object_id"], row["input_role"]),
    ).fetchone()
    if collision is not None and collision["run_input_id"] != row["run_input_id"]:
        raise ResultImportError("ASR logical run input already uses a different ID")
    connection.execute(
        """
        INSERT INTO run_inputs(
            run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256
        ) VALUES(?, ?, ?, ?, ?, ?)
        """,
        (
            row["run_input_id"], row["processing_run_id"], row["object_type"],
            row["object_id"], row["input_role"], row["input_sha256"],
        ),
    )


def _insert_exact_artifact(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    expected = {
        key: row[key]
        for key in (
            "processing_run_id",
            "artifact_kind",
            "storage_uri",
            "sha256",
            "byte_count",
            "schema_version",
            "visibility",
            "metadata_json",
        )
    }
    existing = connection.execute(
        """
        SELECT processing_run_id, artifact_kind, storage_uri, sha256, byte_count,
               schema_version, visibility, metadata_json
        FROM artifacts WHERE artifact_id = ?
        """,
        (row["artifact_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("ASR artifact_id already has different data")
        return
    collision = connection.execute(
        "SELECT artifact_id FROM artifacts WHERE storage_uri = ? AND sha256 = ?",
        (row["storage_uri"], row["sha256"]),
    ).fetchone()
    if collision is not None and collision["artifact_id"] != row["artifact_id"]:
        raise ResultImportError("ASR artifact URI and digest already use a different ID")
    connection.execute(
        """
        INSERT INTO artifacts(
            artifact_id, processing_run_id, artifact_kind, storage_uri, sha256,
            byte_count, schema_version, visibility, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["artifact_id"], row["processing_run_id"], row["artifact_kind"],
            row["storage_uri"], row["sha256"], row["byte_count"],
            row["schema_version"], row["visibility"], row["metadata_json"],
        ),
    )


def _insert_exact_transcript_rows(connection: sqlite3.Connection, records: dict[str, Any]) -> None:
    revision = records["transcript_revisions"][0]
    revision_columns = (
        "recording_id", "rendition_id", "processing_run_id", "revision_kind", "origin",
        "language", "glossary_revision_id", "review_state", "created_at", "metadata_json",
    )
    existing = connection.execute(
        """
        SELECT recording_id, rendition_id, processing_run_id, revision_kind, origin,
               language, glossary_revision_id, review_state, created_at, metadata_json
        FROM transcript_revisions WHERE revision_id = ?
        """,
        (revision["revision_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[key] != revision[key] for key in revision_columns):
            raise ResultImportError("ASR transcript revision ID already has different data")
    else:
        connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, rendition_id, processing_run_id,
                revision_kind, origin, language, glossary_revision_id, review_state,
                created_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (revision["revision_id"], *(revision[key] for key in revision_columns)),
        )

    segment_columns = (
        "revision_id", "ordinal", "start_ms", "end_ms", "text", "normalized_text",
        "speaker_label", "language", "confidence_band", "calibrated_probability", "metadata_json",
    )
    for segment in records["transcript_segments"]:
        existing = connection.execute(
            """
            SELECT revision_id, ordinal, start_ms, end_ms, text, normalized_text,
                   speaker_label, language, confidence_band, calibrated_probability,
                   metadata_json
            FROM transcript_segments WHERE segment_id = ?
            """,
            (segment["segment_id"],),
        ).fetchone()
        if existing is not None:
            if any(existing[key] != segment[key] for key in segment_columns):
                raise ResultImportError("ASR transcript segment ID already has different data")
        else:
            connection.execute(
                """
                INSERT INTO transcript_segments(
                    segment_id, revision_id, ordinal, start_ms, end_ms, text,
                    normalized_text, speaker_label, language, confidence_band,
                    calibrated_probability, metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (segment["segment_id"], *(segment[key] for key in segment_columns)),
            )

    word_columns = (
        "segment_id", "ordinal", "start_ms", "end_ms", "token", "normalized_token",
        "asr_log_probability", "alignment_score", "calibrated_probability",
    )
    for word in records["transcript_words"]:
        existing = connection.execute(
            """
            SELECT segment_id, ordinal, start_ms, end_ms, token, normalized_token,
                   asr_log_probability, alignment_score, calibrated_probability
            FROM transcript_words WHERE word_id = ?
            """,
            (word["word_id"],),
        ).fetchone()
        if existing is not None:
            if any(existing[key] != word[key] for key in word_columns):
                raise ResultImportError("ASR transcript word ID already has different data")
        else:
            connection.execute(
                """
                INSERT INTO transcript_words(
                    word_id, segment_id, ordinal, start_ms, end_ms, token,
                    normalized_token, asr_log_probability, alignment_score,
                    calibrated_probability
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (word["word_id"], *(word[key] for key in word_columns)),
            )


def _begin_batch(
    connection: sqlite3.Connection, *, digest: str, started_at: str
) -> str:
    batch_id = stable_id("imp", "asr_whispercpp_result_v1", digest)
    existing = connection.execute(
        """
        SELECT importer_name, importer_version, input_sha256, started_at, status
        FROM import_batches WHERE import_batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    if existing is not None:
        expected = {
            "importer_name": "asr_whispercpp_result_v1",
            "importer_version": __version__,
            "input_sha256": digest,
            "started_at": started_at,
        }
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("ASR import batch ID already has different provenance")
        connection.execute(
            """
            UPDATE import_batches
            SET completed_at = NULL, status = 'running', statistics_json = '{}'
            WHERE import_batch_id = ?
            """,
            (batch_id,),
        )
        return batch_id
    connection.execute(
        """
        INSERT INTO import_batches(
            import_batch_id, importer_name, importer_version, input_sha256,
            source_snapshot_date, started_at, status, statistics_json
        ) VALUES(?, 'asr_whispercpp_result_v1', ?, ?, NULL, ?, 'running', '{}')
        """,
        (batch_id, __version__, digest, started_at),
    )
    return batch_id


def _upsert_job(connection: sqlite3.Connection, result: dict[str, Any]) -> tuple[str, str]:
    run = result["processing_run"]
    job_id = stable_id("job", STAGE, "asr_result", result["result_key"])
    connection.execute(
        """
        INSERT INTO jobs(
            job_id, stage, target_type, target_id, priority, state, max_attempts,
            created_at, updated_at
        ) VALUES(?, ?, 'asr_result', ?, 100, 'completed', 3, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            state = 'completed',
            updated_at = excluded.updated_at
        """,
        (job_id, STAGE, result["result_key"], run["started_at"], run["completed_at"]),
    )
    attempt_id = stable_id("jat", job_id, run["processing_run_id"])
    existing = connection.execute(
        """
        SELECT job_id, attempt_number, processing_run_id, started_at, completed_at, status
        FROM job_attempts WHERE job_attempt_id = ?
        """,
        (attempt_id,),
    ).fetchone()
    if existing is not None:
        expected = {
            "job_id": job_id,
            "attempt_number": 1,
            "processing_run_id": run["processing_run_id"],
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "status": "completed",
        }
        if any(existing[key] != value for key, value in expected.items()):
            raise ResultImportError("ASR job attempt already has different data")
    else:
        connection.execute(
            """
            INSERT INTO job_attempts(
                job_attempt_id, job_id, attempt_number, processing_run_id,
                started_at, completed_at, status, error_text
            ) VALUES(?, ?, 1, ?, ?, ?, 'completed', NULL)
            """,
            (
                attempt_id,
                job_id,
                run["processing_run_id"],
                run["started_at"],
                run["completed_at"],
            ),
        )
    provenance_values = {
        "asr_whispercpp_producer_job_id": (
            result["_job_id"],
            "Completed ASR result envelope job_id",
        ),
        "asr_whispercpp_work_order_sha256": (
            result["work_order_sha256"],
            "Completed ASR result work-order digest",
        ),
        "asr_whispercpp_recipe_id": (
            result["recipe_id"],
            "Validated ASR recipe identity",
        ),
        "asr_whispercpp_result_key": (
            result["result_key"],
            "Validated ASR immutable result identity",
        ),
        "asr_input_artifact_id": (
            result["input"]["artifact_id"],
            "Exact normalized-audio artifact cited by the ASR result",
        ),
        "asr_parent_processing_run_id": (
            result["input"]["parent_processing_run_id"],
            "Parent preprocessing run cited by the ASR result",
        ),
    }
    for namespace, (external_value, basis) in provenance_values.items():
        external_id = stable_id(
            "ext", "processing_run", run["processing_run_id"], namespace, external_value
        )
        existing_external = connection.execute(
            """
            SELECT object_type, object_id, namespace, external_value,
                   confidence_state, basis, source_id
            FROM external_ids WHERE external_id_id = ?
            """,
            (external_id,),
        ).fetchone()
        expected_external = {
            "object_type": "processing_run",
            "object_id": run["processing_run_id"],
            "namespace": namespace,
            "external_value": external_value,
            "confidence_state": "metadata_only",
            "basis": basis,
            "source_id": None,
        }
        if existing_external is not None:
            if any(
                existing_external[key] != expected for key, expected in expected_external.items()
            ):
                raise ResultImportError("ASR processing-run provenance ID already has different data")
            continue
        connection.execute(
            """
            INSERT INTO external_ids(
                external_id_id, object_type, object_id, namespace, external_value,
                confidence_state, basis, source_id
            ) VALUES(?, 'processing_run', ?, ?, ?, 'metadata_only', ?, NULL)
            """,
            (external_id, run["processing_run_id"], namespace, external_value, basis),
        )
    return job_id, attempt_id


def import_asr_whispercpp_result(
    connection: sqlite3.Connection, result_path: str | Path
) -> dict[str, Any]:
    """Verify and atomically import one completed whisper.cpp result.

    Model, glossary, input-media, and input-artifact registry rows must already
    exist.  The importer never creates recordings, renditions, review decisions, or
    publication decisions.
    """

    raw, digest, imported_path = _read_result(result_path)
    result = validate_asr_whispercpp_result(raw, result_file_path=imported_path)
    _validate_artifact_contents(result)
    run = result["processing_run"]
    records = result["catalog_records"]
    has_transcript_context = result["catalog_context"] is not None
    with transaction(connection):
        _require_catalog_dependencies(connection, result)
        batch_id = _begin_batch(connection, digest=digest, started_at=run["started_at"])
        _insert_exact_processing_run(connection, run)
        _insert_exact_run_input(connection, result["run_input"])
        for artifact in result["artifacts"]:
            _insert_exact_artifact(connection, artifact)
        if has_transcript_context:
            _insert_exact_transcript_rows(connection, records)
        job_id, attempt_id = _upsert_job(connection, result)
        statistics = {
            "artifacts": len(result["artifacts"]),
            "jobs": 1,
            "processing_run_provenance_ids": 6,
            "processing_runs": 1,
            "publication_decisions_added": 0,
            "run_inputs": 1,
            "transcript_revisions": 1 if has_transcript_context else 0,
            "transcript_segments": len(records.get("transcript_segments", [])),
            "transcript_words": len(records.get("transcript_words", [])),
        }
        connection.execute(
            """
            UPDATE import_batches
            SET completed_at = ?, status = 'completed', statistics_json = ?
            WHERE import_batch_id = ?
            """,
            (run["completed_at"], canonical_json(statistics), batch_id),
        )
    return {
        "import_batch_id": batch_id,
        "job_id": job_id,
        "job_attempt_id": attempt_id,
        "processing_run_id": run["processing_run_id"],
        "recording_id": (
            result["catalog_context"]["recording_id"] if has_transcript_context else None
        ),
        "result_key": result["result_key"],
        **statistics,
    }


def validate_asr_whispercpp_result_file(result_path: str | Path) -> dict[str, Any]:
    """Validate one result and all current local bytes without opening a catalog."""

    raw, digest, imported_path = _read_result(result_path)
    result = validate_asr_whispercpp_result(raw, result_file_path=imported_path)
    _validate_artifact_contents(result)
    return {
        "result_envelope_sha256": digest,
        "job_id": result["_job_id"],
        "processing_run_id": result["processing_run"]["processing_run_id"],
        "recipe_id": result["recipe_id"],
        "result_key": result["result_key"],
        "model_id": result["model"]["model_id"],
        "glossary_revision_id": result["processing_run"]["glossary_revision_id"],
        "recording_id": (
            result["catalog_context"]["recording_id"]
            if result["catalog_context"] is not None
            else None
        ),
        "rendition_id": (
            result["catalog_context"]["rendition_id"]
            if result["catalog_context"] is not None
            else None
        ),
        "artifact_count": len(result["artifacts"]),
        "segment_count": result["transcript"]["segment_count"],
        "token_count": result["transcript"]["token_count"],
        "quality_flags": result["transcript"]["quality_flags"],
    }
