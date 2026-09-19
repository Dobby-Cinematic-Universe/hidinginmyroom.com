#!/usr/bin/env python3
"""Strict offline FFmpeg/Chromaprint extraction and exact comparison.

The producer has no network code and never opens the corpus database.  It accepts
only resolved absolute local paths, pins the exact FFmpeg executable/build, emits
sealed private artifacts, and leaves all relationship decisions to human review.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "audio_fingerprint_chromaprint"
COMPARE_STAGE = "audio_fingerprint_exact_compare"
COMPARE_V2_SCHEMA_VERSION = 2
COMPARE_V2_IMPLEMENTATION_VERSION = "0.2.0"
COMPARE_V2_STAGE = "audio_fingerprint_exact_compare_v2"
RAW_FORMAT = "ffmpeg_chromaprint_fp_format_raw"
SAMPLE_RATE_HZ = 16_000
CHANNELS = 1
SAMPLE_FORMAT = "s16"
MAX_WINDOWS = 10_000
MAX_DURATION_MS = 24 * 60 * 60 * 1_000
MAX_FINGERPRINT_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 128 * 1024 * 1024
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class FingerprintError(RuntimeError):
    """A work order, local dependency, or engine result failed closed."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(list(parts)))[:32]}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def exact_keys(value: dict[str, Any], label: str, required: set[str]) -> None:
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - required)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise FingerprintError(f"{label} has " + "; ".join(details))


def object_value(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FingerprintError(f"{label} must be an object")
    return value


def string_value(value: object, label: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise FingerprintError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: object, label: str) -> str:
    text = string_value(value, label, maximum=128)
    if not ID_RE.fullmatch(text):
        raise FingerprintError(f"{label} contains unsupported characters")
    return text


def digest_value(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise FingerprintError(f"{label} must be a lowercase SHA-256 digest")
    return value


def integer(
    value: object, label: str, *, minimum: int = 0, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FingerprintError(f"{label} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise FingerprintError(f"{label} must be <= {maximum}")
    return value


def boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise FingerprintError(f"{label} must be boolean")
    return value


def resolved_file(value: object, label: str) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise FingerprintError(f"{label} must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute():
        raise FingerprintError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
        link_stat = path.lstat()
    except (FileNotFoundError, OSError) as error:
        raise FingerprintError(f"{label} is not a readable current file: {error}") from error
    if path != resolved or stat.S_ISLNK(link_stat.st_mode):
        raise FingerprintError(f"{label} must be resolved without symlinks or traversal")
    if not stat.S_ISREG(link_stat.st_mode):
        raise FingerprintError(f"{label} must be a regular file")
    return path


def output_root(value: object) -> Path:
    text = string_value(value, "output.root")
    if "://" in text:
        raise FingerprintError("output.root must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute() or path == Path("/"):
        raise FingerprintError("output.root must be a specific absolute directory")
    normalized = Path(os.path.normpath(str(path)))
    if normalized != path:
        raise FingerprintError("output.root must not contain dot traversal")
    for unsafe in (Path("/tmp"), Path("/var/tmp")):
        try:
            path.relative_to(unsafe)
        except ValueError:
            pass
        else:
            raise FingerprintError(f"output.root must not be under {unsafe}")
    if path.exists() and not path.is_dir():
        raise FingerprintError("output.root must not be an existing file")
    parent = path
    while not parent.exists():
        if parent.parent == parent:
            raise FingerprintError("output.root has no existing parent")
        parent = parent.parent
    if parent.resolve(strict=True) != parent:
        raise FingerprintError("output.root must not traverse a symlinked parent")
    return path


def stable_read(path: Path, label: str) -> tuple[bytes, os.stat_result]:
    try:
        before_path = path.stat()
        handle = path.open("rb")
    except (FileNotFoundError, OSError) as error:
        raise FingerprintError(f"{label} is not readable: {error}") from error
    try:
        before_fd = os.fstat(handle.fileno())
        before = (
            before_fd.st_dev,
            before_fd.st_ino,
            before_fd.st_size,
            before_fd.st_mtime_ns,
        )
        if before != (
            before_path.st_dev,
            before_path.st_ino,
            before_path.st_size,
            before_path.st_mtime_ns,
        ):
            raise FingerprintError(f"{label} was replaced while opening")
        body = handle.read()
        after_fd = os.fstat(handle.fileno())
    finally:
        handle.close()
    after_path = path.stat()
    if before != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ) or before != (
        after_path.st_dev,
        after_path.st_ino,
        after_path.st_size,
        after_path.st_mtime_ns,
    ):
        raise FingerprintError(f"{label} changed while reading")
    return body, after_path


def file_identity(path: Path) -> tuple[int, int, int, int]:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def run_text(command: list[str], *, timeout: int = 60) -> str:
    completed = subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "LC_ALL": "C", "LANG": "C", "TZ": "UTC"},
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise FingerprintError(
            f"command failed ({completed.returncode}): {command[0]}\n"
            + "\n".join(completed.stdout.splitlines()[-30:])
        )
    return completed.stdout.strip()


def capture_engine(executable: Path) -> dict[str, Any]:
    before = file_identity(executable)
    executable_sha256 = sha256_file(executable)
    version_output = run_text([str(executable), "-hide_banner", "-version"])
    muxer_help = run_text([str(executable), "-hide_banner", "-h", "muxer=chromaprint"])
    after = file_identity(executable)
    if before != after or sha256_file(executable) != executable_sha256:
        raise FingerprintError("FFmpeg executable changed while provenance was captured")
    if "Muxer chromaprint" not in muxer_help or "fp_format" not in muxer_help:
        raise FingerprintError("pinned FFmpeg build does not expose the Chromaprint muxer")
    version_lines = version_output.splitlines()
    configuration = next(
        (
            line.partition(":")[2].strip()
            for line in version_lines
            if line.startswith("configuration:")
        ),
        None,
    )
    return {
        "name": "ffmpeg",
        "path": str(executable),
        "sha256": executable_sha256,
        "byte_count": before[2],
        "version_label": version_lines[0].strip(),
        "version_output": version_output,
        "version_output_sha256": sha256_bytes(version_output.encode("utf-8")),
        "build_configuration": configuration,
        "muxer_help": muxer_help,
        "muxer_help_sha256": sha256_bytes(muxer_help.encode("utf-8")),
    }


def validate_engine(value: object) -> tuple[dict[str, Any], Path]:
    engine = object_value(value, "engine")
    exact_keys(
        engine,
        "engine",
        {
            "executable",
            "expected_sha256",
            "expected_byte_count",
            "expected_version_output_sha256",
            "expected_muxer_help_sha256",
            "version_label",
        },
    )
    executable = resolved_file(engine["executable"], "engine.executable")
    expected = {
        "sha256": digest_value(engine["expected_sha256"], "engine.expected_sha256"),
        "byte_count": integer(
            engine["expected_byte_count"], "engine.expected_byte_count", minimum=1
        ),
        "version_output_sha256": digest_value(
            engine["expected_version_output_sha256"],
            "engine.expected_version_output_sha256",
        ),
        "muxer_help_sha256": digest_value(
            engine["expected_muxer_help_sha256"], "engine.expected_muxer_help_sha256"
        ),
        "version_label": string_value(engine["version_label"], "engine.version_label"),
    }
    observed = capture_engine(executable)
    for key, expected_value in expected.items():
        if observed[key] != expected_value:
            raise FingerprintError(f"engine {key} differs from its work-order pin")
    return observed, executable


def validate_input(value: object) -> tuple[dict[str, Any], Path, tuple[int, int, int, int]]:
    input_value = object_value(value, "input")
    exact_keys(
        input_value,
        "input",
        {
            "path",
            "expected_sha256",
            "expected_byte_count",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "duration_ms",
            "sample_rate_hz",
            "channels",
            "sample_format",
        },
    )
    path = resolved_file(input_value["path"], "input.path")
    if path.lstat().st_mode & 0o222:
        raise FingerprintError("input.path must be a sealed read-only normalized artifact")
    digest = digest_value(input_value["expected_sha256"], "input.expected_sha256")
    byte_count = integer(input_value["expected_byte_count"], "input.expected_byte_count", minimum=1)
    if path.stat().st_size != byte_count or sha256_file(path) != digest:
        raise FingerprintError("input bytes differ from the work-order pin")
    media_id = identifier(input_value["media_id"], "input.media_id")
    if media_id != f"media_sha256_{digest}":
        raise FingerprintError("input.media_id must be derived from expected_sha256")
    artifact_id = identifier(input_value["artifact_id"], "input.artifact_id")
    parent_run_id = identifier(
        input_value["parent_processing_run_id"], "input.parent_processing_run_id"
    )
    duration_ms = integer(
        input_value["duration_ms"], "input.duration_ms", minimum=1, maximum=MAX_DURATION_MS
    )
    if input_value["sample_rate_hz"] != SAMPLE_RATE_HZ:
        raise FingerprintError(f"input.sample_rate_hz must equal {SAMPLE_RATE_HZ}")
    if input_value["channels"] != CHANNELS:
        raise FingerprintError(f"input.channels must equal {CHANNELS}")
    if input_value["sample_format"] != SAMPLE_FORMAT:
        raise FingerprintError(f"input.sample_format must equal {SAMPLE_FORMAT}")
    normalized = {
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": digest,
        "byte_count": byte_count,
        "media_id": media_id,
        "artifact_id": artifact_id,
        "parent_processing_run_id": parent_run_id,
        "duration_ms": duration_ms,
        "sample_rate_hz": SAMPLE_RATE_HZ,
        "channels": CHANNELS,
        "sample_format": SAMPLE_FORMAT,
        "unchanged": True,
    }
    return normalized, path, file_identity(path)


def validate_config(value: object, duration_ms: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = object_value(value, "fingerprint")
    exact_keys(
        config,
        "fingerprint",
        {"algorithm", "raw_format", "threads", "timeout_seconds", "selection"},
    )
    algorithm = integer(config["algorithm"], "fingerprint.algorithm", maximum=2_147_483_647)
    if config["raw_format"] != RAW_FORMAT:
        raise FingerprintError(f"fingerprint.raw_format must equal {RAW_FORMAT}")
    threads = integer(config["threads"], "fingerprint.threads", minimum=1, maximum=64)
    timeout_seconds = integer(
        config["timeout_seconds"], "fingerprint.timeout_seconds", minimum=1, maximum=86_400
    )
    selection = object_value(config["selection"], "fingerprint.selection")
    mode = selection.get("mode")
    windows: list[dict[str, Any]] = []
    if mode == "full_track":
        exact_keys(selection, "fingerprint.selection", {"mode"})
        windows = [
            {
                "window_id": "full_track",
                "window_kind": "full_track",
                "start_ms": 0,
                "end_ms": duration_ms,
            }
        ]
    elif mode == "explicit_windows":
        exact_keys(selection, "fingerprint.selection", {"mode", "windows"})
        raw_windows = selection["windows"]
        if not isinstance(raw_windows, list) or not 1 <= len(raw_windows) <= MAX_WINDOWS:
            raise FingerprintError(
                f"fingerprint.selection.windows must contain 1..{MAX_WINDOWS} items"
            )
        seen: set[str] = set()
        for index, raw in enumerate(raw_windows):
            window = object_value(raw, f"fingerprint.selection.windows[{index}]")
            exact_keys(
                window,
                f"fingerprint.selection.windows[{index}]",
                {"window_id", "start_ms", "end_ms"},
            )
            window_id = identifier(window["window_id"], f"window[{index}].window_id")
            start = integer(window["start_ms"], f"window[{index}].start_ms")
            end = integer(window["end_ms"], f"window[{index}].end_ms", minimum=1)
            if window_id in seen or start >= end or end > duration_ms:
                raise FingerprintError(
                    f"window[{index}] must have a unique ID and satisfy 0 <= start < end <= duration"
                )
            seen.add(window_id)
            windows.append(
                {
                    "window_id": window_id,
                    "window_kind": "explicit_window",
                    "start_ms": start,
                    "end_ms": end,
                }
            )
    elif mode == "fixed_chunks":
        exact_keys(
            selection,
            "fingerprint.selection",
            {
                "mode",
                "chunk_duration_ms",
                "hop_ms",
                "include_partial_tail",
                "minimum_tail_ms",
            },
        )
        chunk = integer(
            selection["chunk_duration_ms"],
            "fingerprint.selection.chunk_duration_ms",
            minimum=1_000,
            maximum=3_600_000,
        )
        hop = integer(
            selection["hop_ms"],
            "fingerprint.selection.hop_ms",
            minimum=1,
            maximum=chunk,
        )
        include_tail = boolean(
            selection["include_partial_tail"], "fingerprint.selection.include_partial_tail"
        )
        minimum_tail = integer(
            selection["minimum_tail_ms"],
            "fingerprint.selection.minimum_tail_ms",
            minimum=1,
            maximum=chunk,
        )
        start = 0
        ordinal = 0
        while start < duration_ms:
            end = min(start + chunk, duration_ms)
            partial = end - start < chunk
            if not partial or (include_tail and end - start >= minimum_tail):
                windows.append(
                    {
                        "window_id": f"chunk_{ordinal:06d}",
                        "window_kind": "partial_tail_chunk" if partial else "fixed_chunk",
                        "start_ms": start,
                        "end_ms": end,
                    }
                )
            if len(windows) > MAX_WINDOWS:
                raise FingerprintError(f"fixed chunk selection exceeds {MAX_WINDOWS} windows")
            start += hop
            ordinal += 1
        if not windows:
            raise FingerprintError("fixed chunk selection produced no windows")
    else:
        raise FingerprintError("fingerprint.selection.mode is unsupported")
    normalized = {
        "algorithm": algorithm,
        "raw_format": RAW_FORMAT,
        "sample_rate_hz": SAMPLE_RATE_HZ,
        "channels": CHANNELS,
        "threads": threads,
        "timeout_seconds": timeout_seconds,
        "selection": selection,
    }
    return normalized, windows


def validate_context(value: object) -> dict[str, str] | None:
    if value is None:
        return None
    context = object_value(value, "catalog_context")
    exact_keys(context, "catalog_context", {"recording_id", "rendition_id"})
    return {
        "recording_id": identifier(context["recording_id"], "catalog_context.recording_id"),
        "rendition_id": identifier(context["rendition_id"], "catalog_context.rendition_id"),
    }


def validate_work_order(value: object) -> dict[str, Any]:
    order = object_value(value, "work order")
    exact_keys(
        order,
        "work order",
        {
            "schema_version",
            "job_id",
            "input",
            "engine",
            "fingerprint",
            "catalog_context",
            "output",
        },
    )
    if order["schema_version"] != SCHEMA_VERSION:
        raise FingerprintError(f"schema_version must equal {SCHEMA_VERSION}")
    job_id = identifier(order["job_id"], "job_id")
    input_value, input_path, input_stat = validate_input(order["input"])
    engine, executable = validate_engine(order["engine"])
    config, windows = validate_config(order["fingerprint"], input_value["duration_ms"])
    context = validate_context(order["catalog_context"])
    output = object_value(order["output"], "output")
    exact_keys(output, "output", {"root"})
    root = output_root(output["root"])
    try:
        input_path.relative_to(root)
    except ValueError:
        pass
    else:
        raise FingerprintError("input.path must not be under output.root")
    return {
        "schema_version": SCHEMA_VERSION,
        "job_id": job_id,
        "input": input_value,
        "_input_path": input_path,
        "_input_stat": input_stat,
        "engine": engine,
        "_engine_path": executable,
        "fingerprint": config,
        "expanded_windows": windows,
        "catalog_context": context,
        "output": {"root": str(root)},
    }


def recipe_payload(order: dict[str, Any]) -> dict[str, Any]:
    engine = order["engine"]
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "input_sha256": order["input"]["sha256"],
        "input_duration_ms": order["input"]["duration_ms"],
        "engine": {
            key: engine[key]
            for key in (
                "sha256",
                "byte_count",
                "version_label",
                "version_output_sha256",
                "build_configuration",
                "muxer_help_sha256",
            )
        },
        "fingerprint": order["fingerprint"],
        "expanded_windows": order["expanded_windows"],
    }


def fingerprint_command(
    executable: Path,
    input_path: Path,
    config: dict[str, Any],
    window: dict[str, Any],
) -> list[str]:
    start_sample = window["start_ms"] * SAMPLE_RATE_HZ // 1_000
    end_sample = window["end_ms"] * SAMPLE_RATE_HZ // 1_000
    return [
        str(executable),
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "error",
        "-threads",
        str(config["threads"]),
        "-i",
        str(input_path),
        "-map",
        "0:a:0",
        "-af",
        f"atrim=start_sample={start_sample}:end_sample={end_sample},asetpts=PTS-STARTPTS",
        "-vn",
        "-sn",
        "-dn",
        "-ac",
        str(CHANNELS),
        "-ar",
        str(SAMPLE_RATE_HZ),
        "-sample_fmt",
        SAMPLE_FORMAT,
        "-f",
        "chromaprint",
        "-algorithm",
        str(config["algorithm"]),
        "-fp_format",
        "raw",
        "pipe:1",
    ]


def run_binary(command: list[str], timeout: int) -> bytes:
    completed = subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "LC_ALL": "C", "LANG": "C", "TZ": "UTC"},
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        tail = completed.stderr.decode("utf-8", "replace").splitlines()[-30:]
        raise FingerprintError(
            f"FFmpeg Chromaprint failed ({completed.returncode})\n" + "\n".join(tail)
        )
    if len(completed.stdout) % 4:
        raise FingerprintError("FFmpeg raw Chromaprint output is not uint32-word aligned")
    if len(completed.stdout) > MAX_FINGERPRINT_BYTES:
        raise FingerprintError("FFmpeg raw Chromaprint output exceeds the 64 MiB bound")
    return completed.stdout


def atomic_immutable(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise FingerprintError(f"immutable output already exists: {path}") from error
        os.chmod(path, 0o444)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def window_flags(window: dict[str, Any], byte_count: int) -> list[str]:
    duration = window["end_ms"] - window["start_ms"]
    flags: set[str] = set()
    if duration < 10_000:
        flags.add("short_window_under_10s")
    if duration < 30_000:
        flags.add("short_window_under_30s")
    if window["window_kind"] == "partial_tail_chunk":
        flags.add("partial_tail_chunk")
    if byte_count == 0:
        flags.add("empty_raw_fingerprint")
    return sorted(flags)


def planned_result(order: dict[str, Any]) -> dict[str, Any]:
    recipe = recipe_payload(order)
    recipe_digest = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_audio_fingerprint_{recipe_digest[:32]}"
    commands = [
        fingerprint_command(
            order["_engine_path"], order["_input_path"], order["fingerprint"], window
        )
        for window in order["expanded_windows"]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "planned",
        "dry_run": True,
        "job_id": order["job_id"],
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_digest,
        "input": order["input"],
        "engine": order["engine"],
        "fingerprint": order["fingerprint"],
        "expanded_windows": order["expanded_windows"],
        "processing_run": None,
        "commands": commands,
        "fingerprints": [],
        "quality_flags": [],
        "catalog_context": order["catalog_context"],
        "result_path": None,
        "errors": [],
    }


def execute(order: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    planned = planned_result(order)
    if dry_run:
        return planned
    nonce = uuid.uuid4().hex
    run_id = stable_id(
        "run_audio_fingerprint", order["input"]["sha256"], planned["recipe_id"], nonce
    )
    started = utc_now()
    execution_dir = (
        Path(order["output"]["root"])
        / "audio-fingerprint"
        / "ffmpeg-chromaprint"
        / "sha256"
        / order["input"]["sha256"][:2]
        / order["input"]["sha256"]
        / "recipes"
        / planned["recipe_sha256"]
        / "executions"
        / run_id
    )
    if execution_dir.exists():
        raise FingerprintError(f"execution path already exists: {execution_dir}")
    fingerprints: list[dict[str, Any]] = []
    commands: list[list[str]] = []
    all_flags: set[str] = set()
    for window in order["expanded_windows"]:
        command = fingerprint_command(
            order["_engine_path"], order["_input_path"], order["fingerprint"], window
        )
        commands.append(command)
        raw = run_binary(command, order["fingerprint"]["timeout_seconds"])
        raw_sha = sha256_bytes(raw)
        artifact_path = (
            execution_dir
            / "artifacts"
            / window["window_id"]
            / "sha256"
            / raw_sha[:2]
            / f"{raw_sha}.chromaprint.raw"
        )
        atomic_immutable(artifact_path, raw)
        flags = window_flags(window, len(raw))
        all_flags.update(flags)
        fingerprint_id = stable_id(
            "fingerprint",
            order["input"]["media_id"],
            planned["recipe_id"],
            window["start_ms"],
            window["end_ms"],
        )
        artifact_id = stable_id(
            "artifact_audio_fingerprint", run_id, window["window_id"], raw_sha
        )
        metadata_json = canonical_bytes(
            {
                "algorithm": order["fingerprint"]["algorithm"],
                "confidence_calibration": "none",
                "fingerprint_id": fingerprint_id,
                "quality_flags": flags,
                "raw_format": RAW_FORMAT,
                "requires_human_review": True,
                "window_id": window["window_id"],
                "window_kind": window["window_kind"],
            }
        ).decode("utf-8")
        fingerprints.append(
            {
                "fingerprint_id": fingerprint_id,
                "implementation_version": (
                    f"ffmpeg-chromaprint/{IMPLEMENTATION_VERSION}/{planned['recipe_id']}"
                ),
                **window,
                "algorithm": order["fingerprint"]["algorithm"],
                "raw_format": RAW_FORMAT,
                "sample_rate_hz": SAMPLE_RATE_HZ,
                "channels": CHANNELS,
                "fingerprint_word_count": len(raw) // 4,
                "quality_flags": flags,
                "artifact": {
                    "artifact_id": artifact_id,
                    "processing_run_id": run_id,
                    "artifact_kind": "audio_fingerprint_chromaprint_raw",
                    "path": str(artifact_path),
                    "storage_uri": artifact_path.as_uri(),
                    "sha256": raw_sha,
                    "byte_count": len(raw),
                    "schema_version": 1,
                    "visibility": "private",
                    "metadata_json": metadata_json,
                },
            }
        )
    if file_identity(order["_input_path"]) != order["_input_stat"]:
        raise FingerprintError("input changed during fingerprint extraction")
    recaptured = capture_engine(order["_engine_path"])
    if recaptured != order["engine"]:
        raise FingerprintError("FFmpeg executable/build changed during extraction")
    completed = utc_now()
    result_path = execution_dir / "result.json"
    result = {
        **planned,
        "status": "completed",
        "dry_run": False,
        "processing_run": {
            "processing_run_id": run_id,
            "execution_nonce": nonce,
            "started_at": started,
            "completed_at": completed,
            "status": "completed",
        },
        "commands": commands,
        "fingerprints": fingerprints,
        "quality_flags": sorted(all_flags),
        "result_path": str(result_path),
    }
    atomic_immutable(result_path, pretty_bytes(result))
    return result


def validate_compare_side(value: object, label: str) -> tuple[dict[str, Any], Path, tuple[int, int, int, int]]:
    side = object_value(value, label)
    exact_keys(
        side,
        label,
        {
            "role",
            "path",
            "expected_sha256",
            "expected_byte_count",
            "artifact_id",
            "fingerprint_id",
            "media_id",
            "implementation_version",
            "algorithm",
            "raw_format",
            "sample_rate_hz",
            "channels",
            "window_kind",
            "start_ms",
            "end_ms",
            "fingerprint_word_count",
            "quality_flags",
        },
    )
    role = side["role"]
    if role not in {"query", "candidate"}:
        raise FingerprintError(f"{label}.role is unsupported")
    path = resolved_file(side["path"], f"{label}.path")
    if path.lstat().st_mode & 0o222:
        raise FingerprintError(f"{label}.path must be a sealed read-only fingerprint artifact")
    digest = digest_value(side["expected_sha256"], f"{label}.expected_sha256")
    byte_count = integer(side["expected_byte_count"], f"{label}.expected_byte_count")
    if byte_count > MAX_FINGERPRINT_BYTES:
        raise FingerprintError(f"{label}.expected_byte_count exceeds the 64 MiB bound")
    body, observed = stable_read(path, label)
    if len(body) != byte_count or sha256_bytes(body) != digest:
        raise FingerprintError(f"{label} bytes differ from their pins")
    if byte_count % 4:
        raise FingerprintError(f"{label} raw bytes are not uint32-word aligned")
    start = integer(side["start_ms"], f"{label}.start_ms")
    end = integer(side["end_ms"], f"{label}.end_ms", minimum=1)
    if end <= start:
        raise FingerprintError(f"{label} must use a non-empty half-open interval")
    flags = side["quality_flags"]
    if (
        not isinstance(flags, list)
        or any(not isinstance(flag, str) or not flag for flag in flags)
        or flags != sorted(set(flags))
    ):
        raise FingerprintError(f"{label}.quality_flags must be sorted and unique")
    normalized = {
        "role": role,
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": digest,
        "byte_count": byte_count,
        "artifact_id": identifier(side["artifact_id"], f"{label}.artifact_id"),
        "fingerprint_id": identifier(side["fingerprint_id"], f"{label}.fingerprint_id"),
        "media_id": identifier(side["media_id"], f"{label}.media_id"),
        "implementation_version": string_value(
            side["implementation_version"], f"{label}.implementation_version"
        ),
        "algorithm": integer(side["algorithm"], f"{label}.algorithm"),
        "raw_format": side["raw_format"],
        "sample_rate_hz": integer(side["sample_rate_hz"], f"{label}.sample_rate_hz"),
        "channels": integer(side["channels"], f"{label}.channels", minimum=1),
        "window_kind": string_value(side["window_kind"], f"{label}.window_kind"),
        "start_ms": start,
        "end_ms": end,
        "fingerprint_word_count": integer(
            side["fingerprint_word_count"], f"{label}.fingerprint_word_count"
        ),
        "quality_flags": flags,
        "unchanged": True,
    }
    if normalized["window_kind"] not in {
        "full_track", "explicit_window", "fixed_chunk", "partial_tail_chunk"
    }:
        raise FingerprintError(f"{label}.window_kind is unsupported")
    expected_implementation = re.compile(
        r"^ffmpeg-chromaprint/0\.1\.0/recipe_audio_fingerprint_[0-9a-f]{32}$"
    )
    if not expected_implementation.fullmatch(normalized["implementation_version"]):
        raise FingerprintError(f"{label}.implementation_version is unsupported")
    if normalized["raw_format"] != RAW_FORMAT:
        raise FingerprintError(f"{label}.raw_format is unsupported")
    if normalized["sample_rate_hz"] != SAMPLE_RATE_HZ or normalized["channels"] != CHANNELS:
        raise FingerprintError(f"{label} normalization parameters are unsupported")
    if normalized["fingerprint_word_count"] != byte_count // 4:
        raise FingerprintError(f"{label}.fingerprint_word_count disagrees with raw bytes")
    if normalized["quality_flags"] != window_flags(normalized, byte_count):
        raise FingerprintError(f"{label}.quality_flags disagree with its exact window/bytes")
    return normalized, path, (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
    )


def validate_pair_context(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    context = object_value(value, "catalog_context")
    exact_keys(context, "catalog_context", {"query", "candidate"})
    normalized: dict[str, dict[str, str]] = {}
    for side_name in ("query", "candidate"):
        side = object_value(context[side_name], f"catalog_context.{side_name}")
        exact_keys(side, f"catalog_context.{side_name}", {"recording_id", "rendition_id"})
        normalized[side_name] = {
            "recording_id": identifier(
                side["recording_id"], f"catalog_context.{side_name}.recording_id"
            ),
            "rendition_id": identifier(
                side["rendition_id"], f"catalog_context.{side_name}.rendition_id"
            ),
        }
    return normalized


def validate_compare_order(value: object) -> dict[str, Any]:
    order = object_value(value, "compare work order")
    exact_keys(
        order,
        "compare work order",
        {"schema_version", "job_id", "method", "query", "candidate", "catalog_context", "output"},
    )
    if order["schema_version"] != SCHEMA_VERSION:
        raise FingerprintError(f"schema_version must equal {SCHEMA_VERSION}")
    if order["method"] != "exact_raw_bytes_v1":
        raise FingerprintError("method must equal exact_raw_bytes_v1")
    query, query_path, query_stat = validate_compare_side(order["query"], "query")
    candidate, candidate_path, candidate_stat = validate_compare_side(
        order["candidate"], "candidate"
    )
    if query["role"] != "query" or candidate["role"] != "candidate":
        raise FingerprintError("query/candidate roles disagree with their positions")
    if query["fingerprint_id"] == candidate["fingerprint_id"]:
        raise FingerprintError("a fingerprint cannot be compared with itself")
    for key in ("implementation_version", "algorithm", "raw_format", "sample_rate_hz", "channels"):
        if query[key] != candidate[key]:
            raise FingerprintError(f"comparison requires identical {key}")
    output = object_value(order["output"], "output")
    exact_keys(output, "output", {"root"})
    return {
        "schema_version": SCHEMA_VERSION,
        "job_id": identifier(order["job_id"], "job_id"),
        "method": "exact_raw_bytes_v1",
        "query": query,
        "candidate": candidate,
        "_query_path": query_path,
        "_candidate_path": candidate_path,
        "_query_stat": query_stat,
        "_candidate_stat": candidate_stat,
        "catalog_context": validate_pair_context(order["catalog_context"]),
        "output": {"root": str(output_root(output["root"]))},
    }


def compare_flags(query: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    flags = {"exact_comparison_only_no_alignment"}
    query_duration = query["end_ms"] - query["start_ms"]
    candidate_duration = candidate["end_ms"] - candidate["start_ms"]
    if query_duration != candidate_duration:
        flags.add("cross_duration_windows")
    if query_duration < 10_000:
        flags.add("short_query_window_under_10s")
    if candidate_duration < 10_000:
        flags.add("short_candidate_window_under_10s")
    if query["byte_count"] == 0:
        flags.add("empty_query_fingerprint")
    if candidate["byte_count"] == 0:
        flags.add("empty_candidate_fingerprint")
    return sorted(flags)


def compare_recipe(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": COMPARE_STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "method": order["method"],
        "query": {
            key: order["query"][key]
            for key in (
                "fingerprint_id",
                "sha256",
                "byte_count",
                "implementation_version",
                "algorithm",
                "raw_format",
                "sample_rate_hz",
                "channels",
                "start_ms",
                "end_ms",
            )
        },
        "candidate": {
            key: order["candidate"][key]
            for key in (
                "fingerprint_id",
                "sha256",
                "byte_count",
                "implementation_version",
                "algorithm",
                "raw_format",
                "sample_rate_hz",
                "channels",
                "start_ms",
                "end_ms",
            )
        },
    }


def compare_execute(order: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    recipe = compare_recipe(order)
    recipe_sha = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_audio_fingerprint_compare_{recipe_sha[:32]}"
    flags = compare_flags(order["query"], order["candidate"])
    base = {
        "schema_version": SCHEMA_VERSION,
        "stage": COMPARE_STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "planned" if dry_run else "completed",
        "dry_run": dry_run,
        "job_id": order["job_id"],
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha,
        "method": order["method"],
        "query": order["query"],
        "candidate": order["candidate"],
        "comparison": None,
        "processing_run": None,
        "catalog_context": order["catalog_context"],
        "result_path": None,
        "errors": [],
    }
    if dry_run:
        return base
    query_body, _ = stable_read(order["_query_path"], "query")
    candidate_body, _ = stable_read(order["_candidate_path"], "candidate")
    if file_identity(order["_query_path"]) != order["_query_stat"]:
        raise FingerprintError("query changed during comparison")
    if file_identity(order["_candidate_path"]) != order["_candidate_stat"]:
        raise FingerprintError("candidate changed during comparison")
    exact_equal = query_body == candidate_body
    nonce = uuid.uuid4().hex
    run_id = stable_id("run_audio_fingerprint_compare", recipe_id, nonce)
    started = utc_now()
    completed = utc_now()
    result_path = (
        Path(order["output"]["root"])
        / "audio-fingerprint-comparisons"
        / "recipes"
        / recipe_sha
        / "executions"
        / run_id
        / "result.json"
    )
    comparison_id = stable_id(
        "fingerprint_match_candidate",
        run_id,
        order["query"]["fingerprint_id"],
        order["candidate"]["fingerprint_id"],
    )
    result = {
        **base,
        "comparison": {
            "match_candidate_id": comparison_id,
            "exact_raw_equal": exact_equal,
            "raw_score": 1.0 if exact_equal else 0.0,
            "score_semantics": "boolean_raw_byte_equality_not_probability",
            "calibrated_probability": None,
            "quality_flags": flags,
            "decision_state": "candidate",
            "requires_human_review": True,
            "relationship_asserted": False,
        },
        "processing_run": {
            "processing_run_id": run_id,
            "execution_nonce": nonce,
            "started_at": started,
            "completed_at": completed,
            "status": "completed",
        },
        "result_path": str(result_path),
    }
    atomic_immutable(result_path, pretty_bytes(result))
    return result


def _timestamp(value: object, label: str) -> str:
    text = string_value(value, label, maximum=64)
    if not text.endswith("Z"):
        raise FingerprintError(f"{label} must be an RFC 3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(text.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise FingerprintError(f"{label} must be an RFC 3339 UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise FingerprintError(f"{label} must be an RFC 3339 UTC timestamp")
    return text


def _validate_completed_extraction_input(
    value: object, label: str
) -> tuple[dict[str, Any], Path, tuple[int, int, int, int]]:
    row = object_value(value, f"{label}.input")
    exact_keys(
        row,
        f"{label}.input",
        {
            "path",
            "storage_uri",
            "sha256",
            "byte_count",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "duration_ms",
            "sample_rate_hz",
            "channels",
            "sample_format",
            "unchanged",
        },
    )
    path = resolved_file(row["path"], f"{label}.input.path")
    if path.lstat().st_mode & 0o222:
        raise FingerprintError(f"{label}.input.path must be a sealed read-only artifact")
    if row["storage_uri"] != path.as_uri():
        raise FingerprintError(f"{label}.input path and storage_uri disagree")
    digest = digest_value(row["sha256"], f"{label}.input.sha256")
    byte_count = integer(row["byte_count"], f"{label}.input.byte_count", minimum=1)
    if path.stat().st_size != byte_count or sha256_file(path) != digest:
        raise FingerprintError(f"{label}.input current bytes differ from its envelope")
    media_id = identifier(row["media_id"], f"{label}.input.media_id")
    if media_id != f"media_sha256_{digest}":
        raise FingerprintError(f"{label}.input.media_id is not derived from its SHA-256")
    if (
        row["sample_rate_hz"] != SAMPLE_RATE_HZ
        or row["channels"] != CHANNELS
        or row["sample_format"] != SAMPLE_FORMAT
        or row["unchanged"] is not True
    ):
        raise FingerprintError(f"{label}.input normalization/integrity is unsupported")
    normalized = {
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": digest,
        "byte_count": byte_count,
        "media_id": media_id,
        "artifact_id": identifier(row["artifact_id"], f"{label}.input.artifact_id"),
        "parent_processing_run_id": identifier(
            row["parent_processing_run_id"],
            f"{label}.input.parent_processing_run_id",
        ),
        "duration_ms": integer(
            row["duration_ms"],
            f"{label}.input.duration_ms",
            minimum=1,
            maximum=MAX_DURATION_MS,
        ),
        "sample_rate_hz": SAMPLE_RATE_HZ,
        "channels": CHANNELS,
        "sample_format": SAMPLE_FORMAT,
        "unchanged": True,
    }
    return normalized, path, file_identity(path)


def _validate_completed_extraction_engine(
    value: object, label: str
) -> tuple[dict[str, Any], Path, tuple[int, int, int, int]]:
    row = object_value(value, f"{label}.engine")
    exact_keys(
        row,
        f"{label}.engine",
        {
            "name",
            "path",
            "sha256",
            "byte_count",
            "version_label",
            "version_output",
            "version_output_sha256",
            "build_configuration",
            "muxer_help",
            "muxer_help_sha256",
        },
    )
    if row["name"] != "ffmpeg":
        raise FingerprintError(f"{label}.engine.name must equal ffmpeg")
    path = resolved_file(row["path"], f"{label}.engine.path")
    version_output = string_value(
        row["version_output"], f"{label}.engine.version_output", maximum=1_000_000
    )
    muxer_help = string_value(
        row["muxer_help"], f"{label}.engine.muxer_help", maximum=1_000_000
    )
    version_label = string_value(row["version_label"], f"{label}.engine.version_label")
    if version_output.splitlines()[0].strip() != version_label:
        raise FingerprintError(f"{label}.engine version label disagrees with its output")
    version_sha = digest_value(
        row["version_output_sha256"], f"{label}.engine.version_output_sha256"
    )
    muxer_sha = digest_value(
        row["muxer_help_sha256"], f"{label}.engine.muxer_help_sha256"
    )
    if sha256_bytes(version_output.encode("utf-8")) != version_sha:
        raise FingerprintError(f"{label}.engine version output digest is inconsistent")
    if sha256_bytes(muxer_help.encode("utf-8")) != muxer_sha:
        raise FingerprintError(f"{label}.engine muxer help digest is inconsistent")
    build_configuration = row["build_configuration"]
    if build_configuration is not None:
        build_configuration = string_value(
            build_configuration, f"{label}.engine.build_configuration", maximum=1_000_000
        )
    normalized = {
        "name": "ffmpeg",
        "path": str(path),
        "sha256": digest_value(row["sha256"], f"{label}.engine.sha256"),
        "byte_count": integer(
            row["byte_count"], f"{label}.engine.byte_count", minimum=1
        ),
        "version_label": version_label,
        "version_output": version_output,
        "version_output_sha256": version_sha,
        "build_configuration": build_configuration,
        "muxer_help": muxer_help,
        "muxer_help_sha256": muxer_sha,
    }
    observed = capture_engine(path)
    if observed != normalized:
        raise FingerprintError(f"{label}.engine current executable/build differs from envelope")
    return normalized, path, file_identity(path)


def _validate_completed_extraction_run(
    value: object, *, label: str, input_sha256: str, recipe_id: str
) -> dict[str, Any]:
    run = object_value(value, f"{label}.processing_run")
    exact_keys(
        run,
        f"{label}.processing_run",
        {"processing_run_id", "execution_nonce", "started_at", "completed_at", "status"},
    )
    nonce = string_value(run["execution_nonce"], f"{label}.execution_nonce", maximum=32)
    if not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise FingerprintError(f"{label}.execution_nonce must be 32 lowercase hex characters")
    run_id = identifier(run["processing_run_id"], f"{label}.processing_run_id")
    if run_id != stable_id("run_audio_fingerprint", input_sha256, recipe_id, nonce):
        raise FingerprintError(f"{label}.processing_run_id is inconsistent")
    started = _timestamp(run["started_at"], f"{label}.started_at")
    completed = _timestamp(run["completed_at"], f"{label}.completed_at")
    if completed < started or run["status"] != "completed":
        raise FingerprintError(f"{label}.processing_run timestamps/status are inconsistent")
    return {
        "processing_run_id": run_id,
        "execution_nonce": nonce,
        "started_at": started,
        "completed_at": completed,
        "status": "completed",
    }


def _validate_completed_extraction_envelope(
    path_value: object, expected_sha256: object, label: str
) -> dict[str, Any]:
    result_path = resolved_file(path_value, f"{label}.result_path")
    if result_path.lstat().st_mode & 0o222:
        raise FingerprintError(f"{label}.result_path must be a sealed read-only envelope")
    if result_path.stat().st_size > MAX_RESULT_BYTES:
        raise FingerprintError(f"{label}.result_path exceeds the 128 MiB bound")
    expected_result_sha = digest_value(expected_sha256, f"{label}.expected_result_sha256")
    body, observed_result = stable_read(result_path, f"{label}.result envelope")
    if sha256_bytes(body) != expected_result_sha:
        raise FingerprintError(f"{label}.result envelope differs from its expected SHA-256")
    try:
        result = object_value(json.loads(body.decode("utf-8")), f"{label}.result envelope")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FingerprintError(f"{label}.result envelope must be UTF-8 JSON") from error
    exact_keys(
        result,
        f"{label}.result envelope",
        {
            "schema_version",
            "stage",
            "implementation_version",
            "status",
            "dry_run",
            "job_id",
            "recipe_id",
            "recipe_sha256",
            "input",
            "engine",
            "fingerprint",
            "expanded_windows",
            "processing_run",
            "commands",
            "fingerprints",
            "quality_flags",
            "catalog_context",
            "result_path",
            "errors",
        },
    )
    if (
        result["schema_version"] != SCHEMA_VERSION
        or result["stage"] != STAGE
        or result["implementation_version"] != IMPLEMENTATION_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
    ):
        raise FingerprintError(f"{label}.result envelope contract/status is unsupported")
    identifier(result["job_id"], f"{label}.job_id")
    if result["result_path"] != str(result_path):
        raise FingerprintError(f"{label}.result_path disagrees with the envelope")
    input_row, input_path, input_stat = _validate_completed_extraction_input(
        result["input"], label
    )
    engine, engine_path, engine_stat = _validate_completed_extraction_engine(
        result["engine"], label
    )
    result_config = object_value(result["fingerprint"], f"{label}.fingerprint")
    exact_keys(
        result_config,
        f"{label}.fingerprint",
        {
            "algorithm",
            "raw_format",
            "sample_rate_hz",
            "channels",
            "threads",
            "timeout_seconds",
            "selection",
        },
    )
    if (
        result_config["sample_rate_hz"] != SAMPLE_RATE_HZ
        or result_config["channels"] != CHANNELS
    ):
        raise FingerprintError(f"{label}.fingerprint normalization is unsupported")
    config, expected_windows = validate_config(
        {
            key: result_config[key]
            for key in (
                "algorithm",
                "raw_format",
                "threads",
                "timeout_seconds",
                "selection",
            )
        },
        input_row["duration_ms"],
    )
    raw_windows = result["expanded_windows"]
    if not isinstance(raw_windows, list) or len(raw_windows) != len(expected_windows):
        raise FingerprintError(f"{label}.expanded_windows disagree with extraction selection")
    windows: list[dict[str, Any]] = []
    for index, raw_window in enumerate(raw_windows):
        window = object_value(raw_window, f"{label}.expanded_windows[{index}]")
        exact_keys(
            window,
            f"{label}.expanded_windows[{index}]",
            {"window_id", "window_kind", "start_ms", "end_ms"},
        )
        normalized_window = {
            "window_id": identifier(
                window["window_id"], f"{label}.expanded_windows[{index}].window_id"
            ),
            "window_kind": string_value(
                window["window_kind"],
                f"{label}.expanded_windows[{index}].window_kind",
            ),
            "start_ms": integer(
                window["start_ms"], f"{label}.expanded_windows[{index}].start_ms"
            ),
            "end_ms": integer(
                window["end_ms"],
                f"{label}.expanded_windows[{index}].end_ms",
                minimum=1,
            ),
        }
        windows.append(normalized_window)
    if windows != expected_windows:
        raise FingerprintError(f"{label}.expanded_windows disagree with extraction selection")
    recipe = recipe_payload(
        {
            "input": input_row,
            "engine": engine,
            "fingerprint": config,
            "expanded_windows": windows,
        }
    )
    recipe_sha = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_audio_fingerprint_{recipe_sha[:32]}"
    if result["recipe_sha256"] != recipe_sha or result["recipe_id"] != recipe_id:
        raise FingerprintError(f"{label}.result recipe identity is inconsistent")
    run = _validate_completed_extraction_run(
        result["processing_run"],
        label=label,
        input_sha256=input_row["sha256"],
        recipe_id=recipe_id,
    )
    expected_commands = [
        fingerprint_command(engine_path, input_path, config, window)
        for window in windows
    ]
    if result["commands"] != expected_commands:
        raise FingerprintError(f"{label}.commands disagree with the exact recipe")
    raw_items = result["fingerprints"]
    if not isinstance(raw_items, list) or len(raw_items) != len(windows):
        raise FingerprintError(f"{label}.fingerprints disagree with expanded windows")
    items: list[dict[str, Any]] = []
    aggregate_flags: set[str] = set()
    for index, (raw_item, window) in enumerate(zip(raw_items, windows, strict=True)):
        item_label = f"{label}.fingerprints[{index}]"
        item = object_value(raw_item, item_label)
        exact_keys(
            item,
            item_label,
            {
                "fingerprint_id",
                "implementation_version",
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
                "artifact",
            },
        )
        if any(item[key] != window[key] for key in window):
            raise FingerprintError(f"{item_label} disagrees with its expanded window")
        fingerprint_id = identifier(item["fingerprint_id"], f"{item_label}.fingerprint_id")
        expected_fingerprint_id = stable_id(
            "fingerprint",
            input_row["media_id"],
            recipe_id,
            window["start_ms"],
            window["end_ms"],
        )
        if fingerprint_id != expected_fingerprint_id:
            raise FingerprintError(f"{item_label}.fingerprint_id is inconsistent")
        producer_implementation = (
            f"ffmpeg-chromaprint/{IMPLEMENTATION_VERSION}/{recipe_id}"
        )
        if item["implementation_version"] != producer_implementation:
            raise FingerprintError(f"{item_label}.implementation_version is inconsistent")
        if (
            item["algorithm"] != config["algorithm"]
            or item["raw_format"] != RAW_FORMAT
            or item["sample_rate_hz"] != SAMPLE_RATE_HZ
            or item["channels"] != CHANNELS
        ):
            raise FingerprintError(f"{item_label} format disagrees with its recipe")
        artifact = object_value(item["artifact"], f"{item_label}.artifact")
        exact_keys(
            artifact,
            f"{item_label}.artifact",
            {
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "path",
                "storage_uri",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
                "metadata_json",
            },
        )
        if (
            artifact["processing_run_id"] != run["processing_run_id"]
            or artifact["artifact_kind"] != "audio_fingerprint_chromaprint_raw"
            or artifact["schema_version"] != 1
            or artifact["visibility"] != "private"
        ):
            raise FingerprintError(f"{item_label}.artifact catalog attributes are invalid")
        artifact_path = resolved_file(artifact["path"], f"{item_label}.artifact.path")
        if artifact_path.lstat().st_mode & 0o222:
            raise FingerprintError(f"{item_label}.artifact must be sealed read-only")
        if artifact["storage_uri"] != artifact_path.as_uri():
            raise FingerprintError(f"{item_label}.artifact path and storage_uri disagree")
        artifact_sha = digest_value(artifact["sha256"], f"{item_label}.artifact.sha256")
        artifact_bytes = integer(
            artifact["byte_count"],
            f"{item_label}.artifact.byte_count",
            maximum=MAX_FINGERPRINT_BYTES,
        )
        expected_artifact_path = (
            result_path.parent
            / "artifacts"
            / window["window_id"]
            / "sha256"
            / artifact_sha[:2]
            / f"{artifact_sha}.chromaprint.raw"
        )
        if artifact_path != expected_artifact_path:
            raise FingerprintError(f"{item_label}.artifact path is outside its execution tree")
        raw_body, artifact_observed = stable_read(artifact_path, f"{item_label}.artifact")
        if len(raw_body) != artifact_bytes or sha256_bytes(raw_body) != artifact_sha:
            raise FingerprintError(f"{item_label}.artifact current bytes differ from envelope")
        words = integer(
            item["fingerprint_word_count"], f"{item_label}.fingerprint_word_count"
        )
        if artifact_bytes % 4 or words != artifact_bytes // 4:
            raise FingerprintError(f"{item_label} raw byte/word counts are inconsistent")
        flags = item["quality_flags"]
        if (
            not isinstance(flags, list)
            or any(not isinstance(flag, str) or not flag for flag in flags)
            or flags != sorted(set(flags))
            or flags != window_flags(window, artifact_bytes)
        ):
            raise FingerprintError(f"{item_label}.quality_flags are inconsistent")
        artifact_id = identifier(artifact["artifact_id"], f"{item_label}.artifact_id")
        if artifact_id != stable_id(
            "artifact_audio_fingerprint",
            run["processing_run_id"],
            window["window_id"],
            artifact_sha,
        ):
            raise FingerprintError(f"{item_label}.artifact_id is inconsistent")
        expected_metadata = canonical_bytes(
            {
                "algorithm": config["algorithm"],
                "confidence_calibration": "none",
                "fingerprint_id": fingerprint_id,
                "quality_flags": flags,
                "raw_format": RAW_FORMAT,
                "requires_human_review": True,
                "window_id": window["window_id"],
                "window_kind": window["window_kind"],
            }
        ).decode("utf-8")
        if artifact["metadata_json"] != expected_metadata:
            raise FingerprintError(f"{item_label}.artifact metadata is inconsistent")
        normalized_artifact = {
            "artifact_id": artifact_id,
            "processing_run_id": run["processing_run_id"],
            "artifact_kind": "audio_fingerprint_chromaprint_raw",
            "path": str(artifact_path),
            "storage_uri": artifact_path.as_uri(),
            "sha256": artifact_sha,
            "byte_count": artifact_bytes,
            "schema_version": 1,
            "visibility": "private",
            "metadata_json": expected_metadata,
        }
        normalized_item = {
            "fingerprint_id": fingerprint_id,
            "implementation_version": producer_implementation,
            **window,
            "algorithm": config["algorithm"],
            "raw_format": RAW_FORMAT,
            "sample_rate_hz": SAMPLE_RATE_HZ,
            "channels": CHANNELS,
            "fingerprint_word_count": words,
            "quality_flags": flags,
            "artifact": normalized_artifact,
            "_artifact_path": artifact_path,
            "_artifact_stat": (
                artifact_observed.st_dev,
                artifact_observed.st_ino,
                artifact_observed.st_size,
                artifact_observed.st_mtime_ns,
            ),
            "_artifact_body": raw_body,
        }
        items.append(normalized_item)
        aggregate_flags.update(flags)
    quality_flags = result["quality_flags"]
    if quality_flags != sorted(aggregate_flags):
        raise FingerprintError(f"{label}.quality_flags are inconsistent")
    context = validate_context(result["catalog_context"])
    if context is None:
        raise FingerprintError(f"{label}.catalog_context is required for compare v2 lineage")
    return {
        "result_path": str(result_path),
        "result_storage_uri": result_path.as_uri(),
        "result_sha256": expected_result_sha,
        "result_byte_count": len(body),
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha,
        "implementation_version": IMPLEMENTATION_VERSION,
        "input": input_row,
        "engine": engine,
        "fingerprint": config,
        "expanded_windows": windows,
        "processing_run": run,
        "fingerprints": items,
        "catalog_context": context,
        "_result_path": result_path,
        "_result_stat": (
            observed_result.st_dev,
            observed_result.st_ino,
            observed_result.st_size,
            observed_result.st_mtime_ns,
        ),
        "_input_path": input_path,
        "_input_stat": input_stat,
        "_engine_path": engine_path,
        "_engine_stat": engine_stat,
    }


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


def _v2_side_output(role: str, envelope: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    artifact = item["artifact"]
    return {
        "role": role,
        "extraction_result": {
            "path": envelope["result_path"],
            "storage_uri": envelope["result_storage_uri"],
            "sha256": envelope["result_sha256"],
            "byte_count": envelope["result_byte_count"],
            "recipe_id": envelope["recipe_id"],
            "recipe_sha256": envelope["recipe_sha256"],
            "processing_run_id": envelope["processing_run"]["processing_run_id"],
            "implementation_version": envelope["implementation_version"],
            "unchanged": True,
        },
        "catalog_context": envelope["catalog_context"],
        "input": {
            key: envelope["input"][key]
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
        "engine": _v2_engine_binding(envelope["engine"]),
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


def validate_compare_v2_side(value: object, expected_role: str) -> dict[str, Any]:
    side = object_value(value, expected_role)
    exact_keys(
        side,
        expected_role,
        {"role", "result_path", "expected_result_sha256", "fingerprint_id"},
    )
    if side["role"] != expected_role:
        raise FingerprintError(f"{expected_role}.role disagrees with its position")
    selected_id = identifier(side["fingerprint_id"], f"{expected_role}.fingerprint_id")
    envelope = _validate_completed_extraction_envelope(
        side["result_path"], side["expected_result_sha256"], expected_role
    )
    selected = [
        item for item in envelope["fingerprints"] if item["fingerprint_id"] == selected_id
    ]
    if len(selected) != 1:
        raise FingerprintError(
            f"{expected_role}.fingerprint_id must select exactly one envelope fingerprint"
        )
    return {
        "role": expected_role,
        "envelope": envelope,
        "item": selected[0],
        "output": _v2_side_output(expected_role, envelope, selected[0]),
    }


def validate_compare_v2_order(value: object) -> dict[str, Any]:
    order = object_value(value, "compare v2 work order")
    exact_keys(
        order,
        "compare v2 work order",
        {"schema_version", "job_id", "method", "query", "candidate", "output"},
    )
    if order["schema_version"] != COMPARE_V2_SCHEMA_VERSION:
        raise FingerprintError(
            f"schema_version must equal {COMPARE_V2_SCHEMA_VERSION} for compare v2"
        )
    if order["method"] != "exact_raw_bytes_v2":
        raise FingerprintError("method must equal exact_raw_bytes_v2")
    query = validate_compare_v2_side(order["query"], "query")
    candidate = validate_compare_v2_side(order["candidate"], "candidate")
    if query["item"]["fingerprint_id"] == candidate["item"]["fingerprint_id"]:
        raise FingerprintError("a fingerprint cannot be compared with itself")
    query_engine = _v2_compatibility_engine(query["envelope"]["engine"])
    candidate_engine = _v2_compatibility_engine(candidate["envelope"]["engine"])
    if query_engine != candidate_engine:
        raise FingerprintError("compare v2 requires identical pinned engine/build identity")
    for key in ("algorithm", "raw_format", "sample_rate_hz", "channels"):
        if query["item"][key] != candidate["item"][key]:
            raise FingerprintError(f"compare v2 requires identical {key}")
    output = object_value(order["output"], "output")
    exact_keys(output, "output", {"root"})
    root = output_root(output["root"])
    for role, side_value in (("query", query), ("candidate", candidate)):
        for path in (
            side_value["envelope"]["_result_path"],
            side_value["item"]["_artifact_path"],
        ):
            try:
                path.relative_to(root)
            except ValueError:
                pass
            else:
                raise FingerprintError(f"{role} evidence must not be under output.root")
    return {
        "schema_version": COMPARE_V2_SCHEMA_VERSION,
        "job_id": identifier(order["job_id"], "job_id"),
        "method": "exact_raw_bytes_v2",
        "query": query,
        "candidate": candidate,
        "output": {"root": str(root)},
    }


def _compare_v2_recipe_side(side: dict[str, Any]) -> dict[str, Any]:
    output = side["output"]
    fingerprint = output["fingerprint"]
    return {
        "role": output["role"],
        "extraction_result_sha256": output["extraction_result"]["sha256"],
        "extraction_recipe_id": output["extraction_result"]["recipe_id"],
        "extraction_recipe_sha256": output["extraction_result"]["recipe_sha256"],
        "extraction_processing_run_id": output["extraction_result"]["processing_run_id"],
        "catalog_context": output["catalog_context"],
        "media_id": output["input"]["media_id"],
        "input_sha256": output["input"]["sha256"],
        "engine": _v2_compatibility_engine(output["engine"]),
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


def compare_v2_recipe(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": COMPARE_V2_SCHEMA_VERSION,
        "stage": COMPARE_V2_STAGE,
        "implementation_version": COMPARE_V2_IMPLEMENTATION_VERSION,
        "method": "exact_raw_bytes_v2",
        "compatibility_contract": "engine_build_algorithm_format_normalization_v2",
        "query": _compare_v2_recipe_side(order["query"]),
        "candidate": _compare_v2_recipe_side(order["candidate"]),
    }


def compare_v2_flags(query: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    query_output = query["output"]
    candidate_output = candidate["output"]
    query_fingerprint = query_output["fingerprint"]
    candidate_fingerprint = candidate_output["fingerprint"]
    flags = set(compare_flags(
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
    ))
    if query_output["catalog_context"]["recording_id"] != candidate_output["catalog_context"]["recording_id"]:
        flags.add("cross_recording")
    if query_output["catalog_context"]["rendition_id"] != candidate_output["catalog_context"]["rendition_id"]:
        flags.add("cross_rendition")
    if query_output["input"]["media_id"] != candidate_output["input"]["media_id"]:
        flags.add("cross_input_media")
    if query_output["extraction_result"]["recipe_id"] != candidate_output["extraction_result"]["recipe_id"]:
        flags.add("cross_extraction_recipe")
    return sorted(flags)


def _reverify_compare_v2_side(side: dict[str, Any], label: str) -> bytes:
    envelope = side["envelope"]
    item = side["item"]
    checks = (
        (envelope["_result_path"], envelope["_result_stat"], envelope["result_sha256"], label + " result envelope"),
        (envelope["_input_path"], envelope["_input_stat"], envelope["input"]["sha256"], label + " input"),
        (envelope["_engine_path"], envelope["_engine_stat"], envelope["engine"]["sha256"], label + " engine"),
        (item["_artifact_path"], item["_artifact_stat"], item["artifact"]["sha256"], label + " raw artifact"),
    )
    selected_body = b""
    for path, expected_stat, expected_sha, check_label in checks:
        body, _ = stable_read(path, check_label)
        if file_identity(path) != expected_stat or sha256_bytes(body) != expected_sha:
            raise FingerprintError(f"{check_label} changed during compare v2 validation")
        if path == item["_artifact_path"]:
            selected_body = body
    return selected_body


def compare_v2_execute(order: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    recipe = compare_v2_recipe(order)
    recipe_sha = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_audio_fingerprint_compare_v2_{recipe_sha[:32]}"
    query_output = order["query"]["output"]
    candidate_output = order["candidate"]["output"]
    base = {
        "schema_version": COMPARE_V2_SCHEMA_VERSION,
        "stage": COMPARE_V2_STAGE,
        "implementation_version": COMPARE_V2_IMPLEMENTATION_VERSION,
        "status": "planned" if dry_run else "completed",
        "dry_run": dry_run,
        "job_id": order["job_id"],
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha,
        "method": "exact_raw_bytes_v2",
        "compatibility_contract": "engine_build_algorithm_format_normalization_v2",
        "query": query_output,
        "candidate": candidate_output,
        "comparison": None,
        "processing_run": None,
        "catalog_context": {
            "query": query_output["catalog_context"],
            "candidate": candidate_output["catalog_context"],
        },
        "visibility": "private",
        "publication_authority": "none",
        "result_path": None,
        "errors": [],
    }
    if dry_run:
        return base
    query_body = _reverify_compare_v2_side(order["query"], "query")
    candidate_body = _reverify_compare_v2_side(order["candidate"], "candidate")
    exact_equal = query_body == candidate_body
    nonce = uuid.uuid4().hex
    run_id = stable_id("run_audio_fingerprint_compare_v2", recipe_id, nonce)
    started = utc_now()
    completed = utc_now()
    result_path = (
        Path(order["output"]["root"])
        / "audio-fingerprint-comparisons-v2"
        / "recipes"
        / recipe_sha
        / "executions"
        / run_id
        / "result.json"
    )
    comparison_id = stable_id(
        "fingerprint_match_candidate_v2",
        run_id,
        query_output["fingerprint"]["fingerprint_id"],
        candidate_output["fingerprint"]["fingerprint_id"],
    )
    result = {
        **base,
        "comparison": {
            "match_candidate_id": comparison_id,
            "exact_raw_equal": exact_equal,
            "raw_score": 1.0 if exact_equal else 0.0,
            "score_semantics": "boolean_raw_byte_equality_not_probability",
            "calibration_state": "not_calibrated",
            "calibrated_probability": None,
            "quality_flags": compare_v2_flags(order["query"], order["candidate"]),
            "decision_state": "candidate",
            "requires_human_review": True,
            "relationship_asserted": False,
            "visibility": "private",
            "publication_authority": "none",
        },
        "processing_run": {
            "processing_run_id": run_id,
            "execution_nonce": nonce,
            "started_at": started,
            "completed_at": completed,
            "status": "completed",
        },
        "result_path": str(result_path),
    }
    atomic_immutable(result_path, pretty_bytes(result))
    return result


def load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FingerprintError(f"cannot read work order {path}: {error}") from error


def engine_inspect(path_value: str | None) -> dict[str, Any]:
    candidate = path_value or shutil.which("ffmpeg")
    if not candidate:
        raise FingerprintError("FFmpeg is not available on PATH")
    path = resolved_file(str(Path(candidate).resolve()), "engine executable")
    observed = capture_engine(path)
    return {
        "executable": observed["path"],
        "expected_sha256": observed["sha256"],
        "expected_byte_count": observed["byte_count"],
        "expected_version_output_sha256": observed["version_output_sha256"],
        "expected_muxer_help_sha256": observed["muxer_help_sha256"],
        "version_label": observed["version_label"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    inspect = subparsers.add_parser("inspect-engine")
    inspect.add_argument("--executable")
    for name in ("validate", "run"):
        command = subparsers.add_parser(name)
        command.add_argument("--work-order", required=True)
        if name == "run":
            command.add_argument("--dry-run", action="store_true")
    for name in ("validate-compare", "compare"):
        command = subparsers.add_parser(name)
        command.add_argument("--work-order", required=True)
        if name == "compare":
            command.add_argument("--dry-run", action="store_true")
    for name in ("validate-compare-v2", "compare-v2"):
        command = subparsers.add_parser(name)
        command.add_argument("--work-order", required=True)
        if name == "compare-v2":
            command.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inspect-engine":
            result = engine_inspect(args.executable)
        elif args.command in {"validate", "run"}:
            order = validate_work_order(load_json(Path(args.work_order)))
            result = (
                {"valid": True, "job_id": order["job_id"], "window_count": len(order["expanded_windows"])}
                if args.command == "validate"
                else execute(order, dry_run=args.dry_run)
            )
        elif args.command in {"validate-compare", "compare"}:
            order = validate_compare_order(load_json(Path(args.work_order)))
            result = (
                {"valid": True, "job_id": order["job_id"], "method": order["method"]}
                if args.command == "validate-compare"
                else compare_execute(order, dry_run=args.dry_run)
            )
        else:
            order = validate_compare_v2_order(load_json(Path(args.work_order)))
            result = (
                {"valid": True, "job_id": order["job_id"], "method": order["method"]}
                if args.command == "validate-compare-v2"
                else compare_v2_execute(order, dry_run=args.dry_run)
            )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    except (FingerprintError, subprocess.TimeoutExpired) as error:
        print(f"audio-fingerprint: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
