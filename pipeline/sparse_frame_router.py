#!/usr/bin/env python3
"""Offline, deterministic sparse frame extraction and OCR-candidate routing.

This stage consumes a sealed media-preprocess result.  It extracts a bounded set
of lossless PNG frames from the preprocess CFR proxy at scene-change and periodic
coverage timestamps.  It does not run OCR, detect text, identify people, or make
content claims.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import struct
import subprocess
import sys
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "sparse_frame_router"
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_LOG_BYTES = 2 * 1024 * 1024
MAX_FRAMES = 256
MAX_MEDIA_DURATION_MS = 7 * 24 * 60 * 60 * 1_000
MAX_PIXELS = 1280 * 720
MAX_FRAME_BYTES = 64 * 1024 * 1024
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHOWINFO_CONFIG_RE = re.compile(
    r"config in time_base:\s*([0-9]+)/([0-9]+),\s*frame_rate:"
)
SHOWINFO_FRAME_RE = re.compile(
    r"showinfo[^\n]*\bn:\s*0\s+pts:\s*(-?[0-9]+)\s+"
    r"pts_time:[^\s]+\s+duration:\s*(-?[0-9]+)\s+duration_time:"
)
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
SELECTION_REASON_ORDER = (
    "FRAME_RECORDING_START",
    "FRAME_SCENE_CHANGE",
    "FRAME_PERIODIC_COVERAGE",
)
OCR_REASON_BY_SELECTION = {
    "FRAME_RECORDING_START": "OCR_CANDIDATE_RECORDING_START",
    "FRAME_SCENE_CHANGE": "OCR_CANDIDATE_SCENE_CHANGE",
    "FRAME_PERIODIC_COVERAGE": "OCR_CANDIDATE_PERIODIC_COVERAGE",
}


class SparseFrameError(RuntimeError):
    """A strict contract, provenance, integrity, or extraction failure."""


class CommandError(SparseFrameError):
    def __init__(self, command: list[str], returncode: int, stderr: str):
        tail = "\n".join(stderr.splitlines()[-40:])
        super().__init__(
            f"Command exited with status {returncode}: {command[0]}\n{tail}".rstrip()
        )
        self.command = command
        self.returncode = returncode


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_" + sha256_bytes(canonical_bytes(list(parts)))[:32]


def load_json(path: Path, *, maximum_bytes: int = MAX_JSON_BYTES) -> Any:
    if path.stat().st_size > maximum_bytes:
        raise SparseFrameError(f"JSON file exceeds the {maximum_bytes}-byte limit: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SparseFrameError(f"Invalid UTF-8 JSON in {path}: {error}") from error


def atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write(path, pretty_json(value).encode("utf-8"))


def require_exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unexpected = sorted(set(value) - keys)
    if missing:
        raise SparseFrameError(f"{label} is missing keys: {', '.join(missing)}")
    if unexpected:
        raise SparseFrameError(
            f"{label} has unsupported keys: {', '.join(unexpected)}"
        )


def bounded_text(value: Any, label: str, maximum: int = 2_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise SparseFrameError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: Any, label: str) -> str:
    result = bounded_text(value, label, 256)
    if not ID_RE.fullmatch(result):
        raise SparseFrameError(f"{label} contains unsupported characters")
    return result


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise SparseFrameError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SparseFrameError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise SparseFrameError(f"{label} must be between {minimum} and {maximum}")
    return value


def number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SparseFrameError(f"{label} must be a number")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise SparseFrameError(f"{label} must be between {minimum} and {maximum}")
    return result


def absolute_file(value: Any, label: str, *, executable: bool = False) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise SparseFrameError(f"{label} must be an absolute local file path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise SparseFrameError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise SparseFrameError(f"{label} does not exist: {path}") from error
    if not resolved.is_file():
        raise SparseFrameError(f"{label} must identify a regular file")
    if executable and not os.access(resolved, os.X_OK):
        raise SparseFrameError(f"{label} is not executable")
    return resolved


def absolute_output_root(value: Any) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise SparseFrameError("output.root must be an absolute local directory path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise SparseFrameError("output.root must be absolute")
    resolved = path.resolve(strict=False)
    if resolved == Path("/"):
        raise SparseFrameError("output.root may not be the filesystem root")
    if resolved.exists() and not resolved.is_dir():
        raise SparseFrameError("output.root must identify a directory or a new path")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if resolved == forbidden or forbidden in resolved.parents:
            raise SparseFrameError(f"output.root may not be under {forbidden}")
    return resolved


def validate_reference(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SparseFrameError("preprocess_result must be a JSON object")
    require_exact_keys(raw, "preprocess_result", {"path", "expected_sha256"})
    raw_path = raw["path"]
    if not isinstance(raw_path, str) or not raw_path or "://" in raw_path:
        raise SparseFrameError("preprocess_result.path must be an absolute local file path")
    unexpanded = Path(raw_path).expanduser()
    if not unexpanded.is_absolute():
        raise SparseFrameError("preprocess_result.path must be absolute")
    try:
        if stat.S_ISLNK(unexpanded.lstat().st_mode):
            raise SparseFrameError("preprocess_result.path may not be a symlink")
    except FileNotFoundError as error:
        raise SparseFrameError(f"preprocess_result.path does not exist: {unexpanded}") from error
    return {
        "path": str(absolute_file(raw_path, "preprocess_result.path")),
        "expected_sha256": sha256_value(
            raw["expected_sha256"], "preprocess_result.expected_sha256"
        ),
    }


def validate_ffmpeg(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SparseFrameError("ffmpeg must be a JSON object")
    require_exact_keys(
        raw,
        "ffmpeg",
        {"path", "expected_sha256", "expected_version_output_sha256"},
    )
    return {
        "path": str(absolute_file(raw["path"], "ffmpeg.path", executable=True)),
        "expected_sha256": sha256_value(
            raw["expected_sha256"], "ffmpeg.expected_sha256"
        ),
        "expected_version_output_sha256": sha256_value(
            raw["expected_version_output_sha256"],
            "ffmpeg.expected_version_output_sha256",
        ),
    }


def validate_sampling(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SparseFrameError("sampling must be a JSON object")
    require_exact_keys(
        raw,
        "sampling",
        {
            "include_recording_start",
            "scene_changes",
            "periodic",
            "min_separation_ms",
        },
    )
    if raw["include_recording_start"] is not True:
        raise SparseFrameError(
            "sampling.include_recording_start must be true in contract version 1"
        )
    scene = raw["scene_changes"]
    periodic = raw["periodic"]
    if not isinstance(scene, dict) or not isinstance(periodic, dict):
        raise SparseFrameError("sampling scene_changes and periodic must be objects")
    require_exact_keys(scene, "sampling.scene_changes", {"enabled", "max_frames", "offset_ms"})
    require_exact_keys(periodic, "sampling.periodic", {"enabled", "interval_ms", "max_frames"})
    if not isinstance(scene["enabled"], bool) or not isinstance(periodic["enabled"], bool):
        raise SparseFrameError("sampling enabled fields must be boolean")
    result = {
        "include_recording_start": True,
        "scene_changes": {
            "enabled": scene["enabled"],
            "max_frames": integer(
                scene["max_frames"], "sampling.scene_changes.max_frames", 0, MAX_FRAMES - 1
            ),
            "offset_ms": integer(
                scene["offset_ms"], "sampling.scene_changes.offset_ms", 0, 60_000
            ),
        },
        "periodic": {
            "enabled": periodic["enabled"],
            "interval_ms": integer(
                periodic["interval_ms"], "sampling.periodic.interval_ms", 1_000, 3_600_000
            ),
            "max_frames": integer(
                periodic["max_frames"], "sampling.periodic.max_frames", 0, MAX_FRAMES - 1
            ),
        },
        "min_separation_ms": integer(
            raw["min_separation_ms"], "sampling.min_separation_ms", 0, 60_000
        ),
    }
    if result["scene_changes"]["enabled"] and result["scene_changes"]["max_frames"] == 0:
        raise SparseFrameError("enabled scene sampling requires max_frames greater than zero")
    if result["periodic"]["enabled"] and result["periodic"]["max_frames"] == 0:
        raise SparseFrameError("enabled periodic sampling requires max_frames greater than zero")
    return result


def validate_limits(raw: Any, sampling: dict[str, Any]) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise SparseFrameError("limits must be a JSON object")
    require_exact_keys(
        raw,
        "limits",
        {
            "max_frames",
            "max_media_duration_ms",
            "max_input_pixels",
            "max_frame_bytes",
            "max_timestamp_drift_ms",
            "timeout_seconds_per_frame",
        },
    )
    result = {
        "max_frames": integer(raw["max_frames"], "limits.max_frames", 1, MAX_FRAMES),
        "max_media_duration_ms": integer(
            raw["max_media_duration_ms"],
            "limits.max_media_duration_ms",
            1,
            MAX_MEDIA_DURATION_MS,
        ),
        "max_input_pixels": integer(
            raw["max_input_pixels"], "limits.max_input_pixels", 1, MAX_PIXELS
        ),
        "max_frame_bytes": integer(
            raw["max_frame_bytes"], "limits.max_frame_bytes", 1_024, MAX_FRAME_BYTES
        ),
        "max_timestamp_drift_ms": integer(
            raw["max_timestamp_drift_ms"],
            "limits.max_timestamp_drift_ms",
            0,
            10_000,
        ),
        "timeout_seconds_per_frame": integer(
            raw["timeout_seconds_per_frame"],
            "limits.timeout_seconds_per_frame",
            1,
            600,
        ),
    }
    reserved = 1
    if sampling["scene_changes"]["enabled"]:
        reserved += sampling["scene_changes"]["max_frames"]
    if sampling["periodic"]["enabled"]:
        reserved += sampling["periodic"]["max_frames"]
    if reserved > result["max_frames"]:
        raise SparseFrameError(
            "limits.max_frames must cover recording start plus enabled scene and periodic caps"
        )
    return result


def validate_work_order(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SparseFrameError("work order must be a JSON object")
    require_exact_keys(
        raw,
        "work order",
        {
            "schema_version",
            "job_id",
            "preprocess_result",
            "ffmpeg",
            "sampling",
            "limits",
            "output",
        },
    )
    if raw["schema_version"] != CONTRACT_VERSION:
        raise SparseFrameError(f"schema_version must be {CONTRACT_VERSION}")
    job_id = raw["job_id"]
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise SparseFrameError("job_id contains unsupported characters or is too long")
    output = raw["output"]
    if not isinstance(output, dict):
        raise SparseFrameError("output must be a JSON object")
    require_exact_keys(output, "output", {"root"})
    sampling = validate_sampling(raw["sampling"])
    return {
        "schema_version": CONTRACT_VERSION,
        "job_id": job_id,
        "preprocess_result": validate_reference(raw["preprocess_result"]),
        "ffmpeg": validate_ffmpeg(raw["ffmpeg"]),
        "sampling": sampling,
        "limits": validate_limits(raw["limits"], sampling),
        "output": {"root": str(absolute_output_root(output["root"]))},
    }


def file_stat(path: Path) -> dict[str, int]:
    value = path.stat()
    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "byte_count": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def require_sealed_file(path: Path, label: str) -> os.stat_result:
    try:
        value = path.lstat()
    except FileNotFoundError as error:
        raise SparseFrameError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise SparseFrameError(f"{label} must be a regular non-symlink file: {path}")
    if value.st_mode & 0o222:
        raise SparseFrameError(f"{label} must be sealed read-only: {path}")
    return value


def require_sealed_directory(path: Path, label: str) -> os.stat_result:
    try:
        value = path.lstat()
    except FileNotFoundError as error:
        raise SparseFrameError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise SparseFrameError(f"{label} must be a non-symlink directory: {path}")
    if value.st_mode & 0o222:
        raise SparseFrameError(f"{label} must be sealed read-only: {path}")
    return value


def observe_file(path: Path, expected_sha256: str, label: str, *, sealed: bool) -> dict[str, Any]:
    before = require_sealed_file(path, label) if sealed else path.stat()
    if before.st_size == 0:
        raise SparseFrameError(f"{label} may not be empty")
    digest = sha256_file(path)
    if digest != expected_sha256:
        raise SparseFrameError(
            f"{label} SHA-256 mismatch: expected {expected_sha256}, observed {digest}"
        )
    return {
        "path": str(path),
        "sha256": digest,
        "byte_count": before.st_size,
        "stat_before": file_stat(path),
        "stat_after": None,
        "unchanged": None,
    }


def verify_observation(observation: dict[str, Any], label: str, *, sealed: bool) -> None:
    path = Path(observation["path"])
    if sealed:
        require_sealed_file(path, label)
    after = file_stat(path)
    if after != observation["stat_before"] or sha256_file(path) != observation["sha256"]:
        raise SparseFrameError(f"{label} changed during sparse frame processing")
    observation["stat_after"] = after
    observation["unchanged"] = True


def minimal_environment() -> dict[str, str]:
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": "/nonexistent-home",
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
        "NO_PROXY": "*",
        "no_proxy": "*",
    }


def bounded_output(value: str) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= MAX_LOG_BYTES:
        return value
    suffix = b"\n[output truncated by sparse frame router]\n"
    return (encoded[: MAX_LOG_BYTES - len(suffix)] + suffix).decode(
        "utf-8", errors="replace"
    )


def terminate_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=3)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    process.wait(timeout=3)


def run_command(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=minimal_environment(),
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        terminate_group(process)
        raise SparseFrameError(
            f"Command exceeded the {timeout_seconds}-second timeout: {command[0]}"
        ) from error
    stdout = bounded_output(stdout)
    stderr = bounded_output(stderr)
    if process.returncode != 0:
        raise CommandError(command, process.returncode, stderr)
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def observe_ffmpeg(work_order: dict[str, Any]) -> dict[str, Any]:
    path = Path(work_order["ffmpeg"]["path"])
    observation = observe_file(
        path, work_order["ffmpeg"]["expected_sha256"], "ffmpeg executable", sealed=False
    )
    completed = run_command([str(path), "-version"], timeout_seconds=30)
    version_output = completed.stdout.strip()
    if not version_output:
        raise SparseFrameError("ffmpeg -version returned no build information")
    version_digest = sha256_bytes(version_output.encode("utf-8"))
    expected = work_order["ffmpeg"]["expected_version_output_sha256"]
    if version_digest != expected:
        raise SparseFrameError(
            f"ffmpeg version output SHA-256 mismatch: expected {expected}, observed {version_digest}"
        )
    observation.update(
        {
            "version": version_output.splitlines()[0],
            "version_output": version_output,
            "version_output_sha256": version_digest,
        }
    )
    return observation


def artifact_path_from_uri(storage_uri: Any) -> Path:
    if not isinstance(storage_uri, str):
        raise SparseFrameError("preprocess proxy storage_uri must be a local file URI")
    parsed = urllib.parse.urlsplit(storage_uri)
    if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
        raise SparseFrameError("preprocess proxy storage_uri must be a local file URI")
    return Path(urllib.parse.unquote(parsed.path)).resolve(strict=True)


def proxy_probe(artifact: dict[str, Any], limits: dict[str, int]) -> dict[str, Any]:
    probe = artifact.get("normalized_probe")
    if not isinstance(probe, dict) or probe.get("schema_version") != 1:
        raise SparseFrameError("preprocess proxy requires a version-1 normalized probe")
    media = probe.get("media")
    primary = probe.get("primary_streams")
    format_row = probe.get("format")
    streams = probe.get("streams")
    if not all(isinstance(value, dict) for value in (media, primary, format_row)) or not isinstance(streams, list):
        raise SparseFrameError("preprocess proxy normalized probe is incomplete")
    if (
        media.get("sha256") != artifact.get("sha256")
        or media.get("byte_count") != artifact.get("byte_count")
        or media.get("media_id") != f"media_sha256_{artifact.get('sha256')}"
    ):
        raise SparseFrameError("preprocess proxy normalized probe media identity disagrees with artifact")
    video_index = integer(primary.get("video_index"), "proxy video stream index", 0, 1024)
    stream = next(
        (
            value
            for value in streams
            if isinstance(value, dict)
            and value.get("index") == video_index
            and value.get("codec_type") == "video"
        ),
        None,
    )
    if stream is None:
        raise SparseFrameError("preprocess proxy has no primary video stream")
    video = stream.get("video")
    if not isinstance(video, dict):
        raise SparseFrameError("preprocess proxy video probe is incomplete")
    width = integer(video.get("width"), "proxy width", 1, 1280)
    height = integer(video.get("height"), "proxy height", 1, 1280)
    if width * height > limits["max_input_pixels"]:
        raise SparseFrameError("preprocess proxy exceeds limits.max_input_pixels")
    frame_rate = video.get("average_frame_rate")
    if not isinstance(frame_rate, dict):
        raise SparseFrameError("preprocess proxy requires a positive CFR average frame rate")
    require_exact_keys(
        frame_rate,
        "proxy average_frame_rate",
        {"text", "numerator", "denominator", "decimal"},
    )
    numerator = integer(frame_rate.get("numerator"), "proxy frame-rate numerator", 1, 1_000_000)
    denominator = integer(frame_rate.get("denominator"), "proxy frame-rate denominator", 1, 1_000_000)
    try:
        text_rate = Fraction(bounded_text(frame_rate.get("text"), "proxy frame-rate text", 64))
    except (ValueError, ZeroDivisionError) as error:
        raise SparseFrameError("preprocess proxy frame-rate text is not rational") from error
    decimal_rate = number(frame_rate.get("decimal"), "proxy frame-rate decimal", 0.001, 1000)
    if text_rate != Fraction(numerator, denominator) or abs(float(text_rate) - decimal_rate) > 0.000001:
        raise SparseFrameError("preprocess proxy average frame-rate representations disagree")
    duration_ms = integer(
        format_row.get("duration_ms"), "proxy duration_ms", 1, MAX_MEDIA_DURATION_MS
    )
    if duration_ms > limits["max_media_duration_ms"]:
        raise SparseFrameError("preprocess proxy exceeds limits.max_media_duration_ms")
    return {
        "video_stream_index": video_index,
        "width": width,
        "height": height,
        "pixel_format": video.get("pixel_format"),
        "average_frame_rate": frame_rate,
        "duration_ms": duration_ms,
        "start_ms": format_row.get("start_ms"),
        "video_start_ms": stream.get("start_ms"),
    }


def load_preprocess_handoff(work_order: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    reference = work_order["preprocess_result"]
    result_path = Path(reference["path"])
    result_observation = observe_file(
        result_path,
        reference["expected_sha256"],
        "media-preprocess result",
        sealed=True,
    )
    result = load_json(result_path)
    if not isinstance(result, dict):
        raise SparseFrameError("media-preprocess result must be a JSON object")
    require_exact_keys(
        result,
        "media-preprocess result",
        {
            "schema_version",
            "job_id",
            "status",
            "dry_run",
            "duration_ms",
            "processing_run",
            "input",
            "layout",
            "steps",
            "artifacts",
            "routing",
            "reuse",
            "catalog_records",
            "errors",
            "result_path",
        },
    )
    if (
        result.get("schema_version") != 1
        or result.get("status") != "completed"
        or result.get("dry_run") is not False
        or result.get("errors") != []
        or result.get("result_path") != str(result_path)
    ):
        raise SparseFrameError("media-preprocess result is not a successful sealed version-1 envelope")
    processing = result.get("processing_run")
    layout = result.get("layout")
    routing = result.get("routing")
    artifacts = result.get("artifacts")
    if not isinstance(processing, dict) or not isinstance(layout, dict) or not isinstance(routing, dict) or not isinstance(artifacts, list):
        raise SparseFrameError("media-preprocess handoff fields are malformed")
    if processing.get("stage") != "media_preprocess" or processing.get("status") != "completed":
        raise SparseFrameError("preprocess processing_run is not completed media_preprocess")
    processing_run_id = identifier(
        processing.get("processing_run_id"), "preprocess processing_run_id"
    )
    run_dir = Path(bounded_text(layout.get("run_dir"), "preprocess layout.run_dir", 10_000))
    if not run_dir.is_absolute() or result_path != run_dir / "result.json":
        raise SparseFrameError("preprocess result path disagrees with layout.run_dir")
    proxy_matches = [
        value
        for value in artifacts
        if isinstance(value, dict) and value.get("artifact_kind") == "low_resolution_cfr_proxy"
    ]
    if len(proxy_matches) != 1:
        raise SparseFrameError("preprocess result must contain exactly one low_resolution_cfr_proxy")
    proxy = proxy_matches[0]
    require_exact_keys(
        proxy,
        "preprocess proxy artifact",
        {
            "artifact_id",
            "processing_run_id",
            "artifact_kind",
            "storage_uri",
            "path",
            "sha256",
            "byte_count",
            "schema_version",
            "visibility",
            "media_kind",
            "mime_type",
            "normalized_probe",
        },
    )
    proxy_sha256 = sha256_value(proxy.get("sha256"), "preprocess proxy sha256")
    raw_proxy_path = Path(bounded_text(proxy.get("path"), "preprocess proxy path", 10_000))
    if not raw_proxy_path.is_absolute():
        raise SparseFrameError("preprocess proxy path must be absolute")
    require_sealed_file(raw_proxy_path, "preprocess proxy")
    proxy_path = absolute_file(str(raw_proxy_path), "preprocess proxy path")
    if artifact_path_from_uri(proxy.get("storage_uri")) != proxy_path:
        raise SparseFrameError("preprocess proxy path and storage_uri disagree")
    if run_dir != proxy_path and run_dir not in proxy_path.parents:
        raise SparseFrameError("preprocess proxy escapes its processing run directory")
    if (
        proxy.get("schema_version") != 1
        or proxy.get("visibility") != "private"
        or proxy.get("media_kind") != "video"
        or proxy.get("mime_type") != "video/mp4"
        or proxy.get("processing_run_id") != processing_run_id
    ):
        raise SparseFrameError("preprocess proxy metadata is not a private version-1 video artifact")
    byte_count = integer(proxy.get("byte_count"), "preprocess proxy byte_count", 1, 1 << 63)
    expected_artifact_id = "artifact_" + sha256_bytes(
        canonical_bytes(
            {
                "processing_run_id": processing_run_id,
                "kind": "low_resolution_cfr_proxy",
                "sha256": proxy_sha256,
            }
        )
    )[:32]
    if proxy.get("artifact_id") != expected_artifact_id:
        raise SparseFrameError("preprocess proxy artifact identity is inconsistent")
    proxy_observation = observe_file(
        proxy_path, proxy_sha256, "preprocess proxy", sealed=True
    )
    if proxy_observation["byte_count"] != byte_count:
        raise SparseFrameError("preprocess proxy byte count disagrees with artifact envelope")
    probe = proxy_probe(proxy, work_order["limits"])
    coverage = routing.get("coverage")
    scenes = routing.get("scene_changes")
    if (
        routing.get("schema_version") != 1
        or not isinstance(coverage, dict)
        or coverage.get("has_video") is not True
        or not isinstance(scenes, list)
    ):
        raise SparseFrameError("preprocess routing has no valid video scene handoff")
    normalized_scenes: list[dict[str, Any]] = []
    for index, scene in enumerate(scenes):
        if not isinstance(scene, dict) or set(scene) != {"timestamp_ms", "score_percent"}:
            raise SparseFrameError(f"preprocess scene_changes[{index}] is malformed")
        timestamp = integer(
            scene["timestamp_ms"], f"preprocess scene_changes[{index}].timestamp_ms", 0, MAX_MEDIA_DURATION_MS
        )
        score = number(
            scene["score_percent"], f"preprocess scene_changes[{index}].score_percent", 0, 100
        )
        normalized_scenes.append({"timestamp_ms": timestamp, "score_percent": score})
    normalized_scenes.sort(key=lambda value: (value["timestamp_ms"], value["score_percent"]))
    handoff = {
        "processing_run_id": processing_run_id,
        "implementation_version": bounded_text(
            processing.get("implementation_version"), "preprocess implementation_version", 256
        ),
        "routing_scene_changes": normalized_scenes,
        "routing_duration_ms": coverage.get("duration_ms"),
    }
    proxy_input = {
        **proxy_observation,
        "media_id": f"media_sha256_{proxy_sha256}",
        "artifact_id": proxy["artifact_id"],
        "parent_processing_run_id": processing_run_id,
        "probe": probe,
    }
    return result_observation, proxy_input, handoff


def uniform_cap(values: list[Any], limit: int) -> tuple[list[Any], bool]:
    if len(values) <= limit:
        return values, False
    if limit == 1:
        return [values[0]], True
    indexes = [index * (len(values) - 1) // (limit - 1) for index in range(limit)]
    return [values[index] for index in indexes], True


def build_selection(
    *, duration_ms: int, scenes: list[dict[str, Any]], sampling: dict[str, Any]
) -> dict[str, Any]:
    limit_reasons: list[str] = []
    eligible_scenes: list[dict[str, Any]] = []
    if sampling["scene_changes"]["enabled"]:
        offset = sampling["scene_changes"]["offset_ms"]
        for scene in scenes:
            requested = scene["timestamp_ms"] + offset
            if requested >= duration_ms:
                continue
            eligible_scenes.append({**scene, "requested_timestamp_ms": requested})
    selected_scenes, scene_capped = uniform_cap(
        eligible_scenes, sampling["scene_changes"]["max_frames"]
    )
    if scene_capped:
        limit_reasons.append("SCENE_CANDIDATES_UNIFORMLY_CAPPED")

    periodic_candidates: list[int] = []
    if sampling["periodic"]["enabled"]:
        interval = sampling["periodic"]["interval_ms"]
        periodic_candidates = list(range(interval, duration_ms, interval))
    selected_periodic, periodic_capped = uniform_cap(
        periodic_candidates, sampling["periodic"]["max_frames"]
    )
    if periodic_capped:
        limit_reasons.append("PERIODIC_CANDIDATES_UNIFORMLY_CAPPED")

    accepted: list[dict[str, Any]] = [
        {
            "requested_timestamp_ms": 0,
            "selection_reason_codes": ["FRAME_RECORDING_START"],
            "source_scene_timestamps_ms": [],
        }
    ]
    merged_count = 0

    def add_candidate(timestamp_ms: int, reason: str, source_scene: int | None = None) -> None:
        nonlocal merged_count
        separation = sampling["min_separation_ms"]
        nearby = [
            value
            for value in accepted
            if abs(value["requested_timestamp_ms"] - timestamp_ms) <= separation
        ]
        if nearby:
            target = min(
                nearby,
                key=lambda value: (
                    abs(value["requested_timestamp_ms"] - timestamp_ms),
                    value["requested_timestamp_ms"],
                ),
            )
            if reason not in target["selection_reason_codes"]:
                target["selection_reason_codes"].append(reason)
            if source_scene is not None and source_scene not in target["source_scene_timestamps_ms"]:
                target["source_scene_timestamps_ms"].append(source_scene)
            merged_count += 1
            return
        accepted.append(
            {
                "requested_timestamp_ms": timestamp_ms,
                "selection_reason_codes": [reason],
                "source_scene_timestamps_ms": [] if source_scene is None else [source_scene],
            }
        )

    for scene in selected_scenes:
        add_candidate(
            scene["requested_timestamp_ms"],
            "FRAME_SCENE_CHANGE",
            scene["timestamp_ms"],
        )
    for timestamp in selected_periodic:
        add_candidate(timestamp, "FRAME_PERIODIC_COVERAGE")
    if merged_count:
        limit_reasons.append("NEARBY_CANDIDATES_MERGED")

    reason_rank = {value: index for index, value in enumerate(SELECTION_REASON_ORDER)}
    accepted.sort(key=lambda value: value["requested_timestamp_ms"])
    for ordinal, value in enumerate(accepted):
        value["ordinal"] = ordinal
        value["selection_reason_codes"].sort(key=reason_rank.__getitem__)
        value["source_scene_timestamps_ms"].sort()
    return {
        "parameters": sampling,
        "coverage": {"duration_ms": duration_ms, "timeline_origin_ms": 0},
        "candidate_counts": {
            "preprocess_scene_changes": len(scenes),
            "eligible_scene_candidates": len(eligible_scenes),
            "retained_scene_candidates": len(selected_scenes),
            "periodic_candidates": len(periodic_candidates),
            "retained_periodic_candidates": len(selected_periodic),
            "merged_candidates": merged_count,
            "planned_frames": len(accepted),
        },
        "limit_reason_codes": limit_reasons,
        "planned_frames": accepted,
    }


def seconds_text(timestamp_ms: int) -> str:
    return f"{timestamp_ms // 1000}.{timestamp_ms % 1000:03d}"


def frame_command(
    ffmpeg: Path,
    proxy: Path,
    video_stream_index: int,
    requested_timestamp_ms: int,
    output: Path,
) -> list[str]:
    return [
        str(ffmpeg),
        "-hide_banner",
        "-nostdin",
        "-nostats",
        "-loglevel",
        "info",
        "-threads",
        "1",
        "-fflags",
        "+bitexact",
        "-copyts",
        "-ss",
        seconds_text(requested_timestamp_ms),
        "-i",
        str(proxy),
        "-map",
        f"0:{video_stream_index}",
        "-frames:v",
        "1",
        "-an",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-vf",
        "format=rgb24,showinfo",
        "-fps_mode",
        "passthrough",
        "-c:v",
        "png",
        "-compression_level",
        "9",
        "-pred",
        "mixed",
        "-flags:v",
        "+bitexact",
        "-threads:v",
        "1",
        "-f",
        "image2",
        str(output),
    ]


def parse_showinfo(stderr: str) -> dict[str, Any]:
    config = SHOWINFO_CONFIG_RE.search(stderr)
    frame = SHOWINFO_FRAME_RE.search(stderr)
    if not config or not frame:
        raise SparseFrameError("ffmpeg did not emit exact showinfo PTS metadata for frame zero")
    numerator = int(config.group(1))
    denominator = int(config.group(2))
    pts = int(frame.group(1))
    duration_pts = int(frame.group(2))
    if numerator <= 0 or denominator <= 0 or pts < 0 or duration_pts <= 0:
        raise SparseFrameError("ffmpeg showinfo emitted an invalid frame PTS/time base")
    timestamp = Fraction(pts * numerator, denominator)
    duration = Fraction(duration_pts * numerator, denominator)
    timestamp_us = round(timestamp * 1_000_000)
    duration_us = round(duration * 1_000_000)
    return {
        "pts": pts,
        "duration_pts": duration_pts,
        "time_base_numerator": numerator,
        "time_base_denominator": denominator,
        "timestamp_us": timestamp_us,
        "timestamp_ms": round(timestamp * 1_000),
        "duration_us": duration_us,
    }


def inspect_png(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        header = handle.read(33)
    if len(header) < 33 or header[:8] != PNG_SIGNATURE or header[12:16] != b"IHDR":
        raise SparseFrameError("ffmpeg output is not a valid PNG with an IHDR header")
    width, height, bit_depth, color_type, compression, filtering, interlace = struct.unpack(
        ">IIBBBBB", header[16:29]
    )
    if (
        width <= 0
        or height <= 0
        or bit_depth != 8
        or color_type != 2
        or compression != 0
        or filtering != 0
        or interlace != 0
    ):
        raise SparseFrameError("PNG output is not non-interlaced 8-bit RGB lossless data")
    return {
        "width": width,
        "height": height,
        "pixel_format": "rgb24",
        "bit_depth": bit_depth,
        "color_type": "truecolor",
        "interlaced": False,
    }


def ocr_route(selection_reason_codes: list[str]) -> dict[str, Any]:
    return {
        "route": "queue_candidate",
        "evaluation_state": "not_evaluated",
        "text_presence": "unknown",
        "reason_codes": [
            OCR_REASON_BY_SELECTION[value]
            for value in SELECTION_REASON_ORDER
            if value in selection_reason_codes
        ],
        "warning": (
            "This is an extraction/routing candidate only. OCR and text-presence "
            "evaluation have not run, and no person or content is identified."
        ),
    }


def artifact_row(
    *,
    processing_run_id: str,
    ordinal: int,
    final_path: Path,
    staged_path: Path,
    image: dict[str, Any],
) -> dict[str, Any]:
    size = staged_path.stat().st_size
    digest = sha256_file(staged_path)
    return {
        "artifact_id": stable_id(
            "artifact", processing_run_id, "sparse_frame_png", ordinal, digest
        ),
        "processing_run_id": processing_run_id,
        "artifact_kind": "sparse_frame_png",
        "ordinal": ordinal,
        "storage_uri": final_path.resolve(strict=False).as_uri(),
        "path": str(final_path.resolve(strict=False)),
        "sha256": digest,
        "byte_count": size,
        "schema_version": 1,
        "visibility": "private",
        "media_kind": "image",
        "mime_type": "image/png",
        "image": image,
    }


def verify_inputs(
    preprocess_observation: dict[str, Any],
    proxy_observation: dict[str, Any],
    ffmpeg_observation: dict[str, Any],
) -> None:
    verify_observation(preprocess_observation, "media-preprocess result", sealed=True)
    verify_observation(proxy_observation, "preprocess proxy", sealed=True)
    verify_observation(ffmpeg_observation, "ffmpeg executable", sealed=False)
    completed = run_command([ffmpeg_observation["path"], "-version"], timeout_seconds=30)
    observed = sha256_bytes(completed.stdout.strip().encode("utf-8"))
    if observed != ffmpeg_observation["version_output_sha256"]:
        raise SparseFrameError("ffmpeg version output changed during sparse frame processing")


def validate_completed_reuse(
    result_path: Path,
    *,
    result_key: str,
    recipe_id: str,
    recipe: dict[str, Any],
    recipe_sha256: str,
    job_id: str,
    processing_run_id: str,
    work_order_sha256: str,
    selection: dict[str, Any],
    handoff: dict[str, Any],
    preprocess_observation: dict[str, Any],
    proxy_observation: dict[str, Any],
    ffmpeg_observation: dict[str, Any],
) -> dict[str, Any]:
    require_sealed_file(result_path, "existing sparse frame result")
    require_sealed_directory(result_path.parent, "existing sparse frame result directory")
    require_sealed_directory(result_path.parent / "frames", "existing sparse frame frames directory")
    result = load_json(result_path)
    if not isinstance(result, dict):
        raise SparseFrameError("existing sparse frame result must be an object")
    require_exact_keys(
        result,
        "existing sparse frame result",
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
            "preprocess_result",
            "input_proxy",
            "ffmpeg",
            "selection",
            "commands",
            "frames",
            "artifacts",
            "result_path",
            "duration_ms",
            "errors",
        },
    )
    if (
        result.get("schema_version") != 1
        or result.get("status") != "completed"
        or result.get("dry_run") is not False
        or result.get("errors") != []
        or result.get("result_key") != result_key
        or result.get("recipe_id") != recipe_id
        or result.get("recipe_sha256") != recipe_sha256
        or result.get("job_id") != job_id
        or result.get("work_order_sha256") != work_order_sha256
        or result.get("result_path") != str(result_path)
        or result.get("selection") != selection
    ):
        raise SparseFrameError("existing immutable sparse frame result identity is invalid")
    preprocess_result = result.get("preprocess_result")
    input_proxy = result.get("input_proxy")
    ffmpeg = result.get("ffmpeg")
    if not all(isinstance(value, dict) for value in (preprocess_result, input_proxy, ffmpeg)):
        raise SparseFrameError("existing sparse frame input/tool observations are malformed")
    require_exact_keys(
        preprocess_result,
        "existing preprocess result observation",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "processing_run_id",
            "implementation_version",
        },
    )
    require_exact_keys(
        input_proxy,
        "existing proxy observation",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "probe",
        },
    )
    require_exact_keys(
        ffmpeg,
        "existing ffmpeg observation",
        {
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "version",
            "version_output",
            "version_output_sha256",
        },
    )
    expected_preprocess = {
        key: preprocess_observation[key]
        for key in ("path", "sha256", "byte_count", "stat_before", "stat_after", "unchanged")
    }
    expected_proxy = {
        key: proxy_observation[key]
        for key in (
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "probe",
        )
    }
    expected_ffmpeg = {
        key: ffmpeg_observation[key]
        for key in (
            "path",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "version",
            "version_output",
            "version_output_sha256",
        )
    }
    if (
        any(preprocess_result.get(key) != value for key, value in expected_preprocess.items())
        or preprocess_result.get("processing_run_id") != handoff["processing_run_id"]
        or preprocess_result.get("implementation_version") != handoff["implementation_version"]
        or input_proxy != expected_proxy
        or ffmpeg != expected_ffmpeg
    ):
        raise SparseFrameError("existing sparse frame result input/tool identity is invalid")
    processing = result.get("processing_run")
    frames = result.get("frames")
    artifacts = result.get("artifacts")
    commands = result.get("commands")
    if not isinstance(processing, dict) or not isinstance(frames, list) or not isinstance(artifacts, list) or not isinstance(commands, list):
        raise SparseFrameError("existing sparse frame result collections are malformed")
    require_exact_keys(
        processing,
        "existing sparse frame processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "parameters_json",
            "environment_json",
            "started_at",
            "completed_at",
            "status",
            "error_text",
        },
    )
    environment = processing.get("environment_json")
    if not isinstance(environment, dict):
        raise SparseFrameError("existing sparse frame processing environment is malformed")
    require_exact_keys(
        environment,
        "existing sparse frame processing environment",
        {"python", "cpu_only", "network", "tool_paths"},
    )
    tool_paths = environment.get("tool_paths")
    if not isinstance(tool_paths, dict):
        raise SparseFrameError("existing sparse frame tool_paths is malformed")
    require_exact_keys(tool_paths, "existing sparse frame tool_paths", {"ffmpeg"})
    run_id = processing.get("processing_run_id")
    if (
        processing.get("stage") != STAGE
        or processing.get("status") != "completed"
        or run_id != processing_run_id
        or processing.get("implementation_version") != IMPLEMENTATION_VERSION
        or processing.get("parameters_json") != canonical_json_text(recipe)
        or not isinstance(environment.get("python"), str)
        or not environment["python"]
        or environment.get("cpu_only") is not True
        or environment.get("network") != "not_used"
        or tool_paths.get("ffmpeg") != ffmpeg_observation["path"]
        or processing.get("error_text") is not None
        or len(frames) != len(selection["planned_frames"])
        or len(artifacts) != len(frames)
        or len(commands) != len(frames)
    ):
        raise SparseFrameError("existing sparse frame processing run is inconsistent")
    artifact_by_id: dict[str, dict[str, Any]] = {}
    run_dir = result_path.parent
    for ordinal, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            raise SparseFrameError("existing sparse frame artifact is malformed")
        require_exact_keys(
            artifact,
            "existing sparse frame artifact",
            {
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "ordinal",
                "storage_uri",
                "path",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
                "media_kind",
                "mime_type",
                "image",
            },
        )
        raw_path = Path(bounded_text(artifact.get("path"), "artifact.path", 10_000))
        if not raw_path.is_absolute():
            raise SparseFrameError("existing sparse frame artifact path must be absolute")
        require_sealed_file(raw_path, "existing sparse frame artifact")
        path = raw_path.resolve(strict=True)
        if path.parent != run_dir / "frames" or artifact.get("ordinal") != ordinal:
            raise SparseFrameError("existing sparse frame artifact path/ordinal is invalid")
        digest = sha256_file(path)
        image = inspect_png(path)
        if (
            artifact.get("sha256") != digest
            or artifact.get("byte_count") != path.stat().st_size
            or artifact.get("image") != image
            or artifact.get("processing_run_id") != run_id
            or artifact.get("artifact_kind") != "sparse_frame_png"
            or artifact.get("visibility") != "private"
            or artifact.get("storage_uri") != path.as_uri()
            or artifact.get("schema_version") != 1
            or artifact.get("media_kind") != "image"
            or artifact.get("mime_type") != "image/png"
        ):
            raise SparseFrameError("existing sparse frame artifact metadata/hash is invalid")
        expected_artifact_id = stable_id(
            "artifact", run_id, "sparse_frame_png", ordinal, digest
        )
        if artifact.get("artifact_id") != expected_artifact_id:
            raise SparseFrameError("existing sparse frame artifact identity is invalid")
        artifact_by_id[expected_artifact_id] = artifact
    for ordinal, frame in enumerate(frames):
        if not isinstance(frame, dict) or frame.get("ordinal") != ordinal:
            raise SparseFrameError("existing sparse frame row ordinal is invalid")
        require_exact_keys(
            frame,
            "existing sparse frame row",
            {
                "frame_id",
                "ordinal",
                "requested_timestamp_ms",
                "selection_reason_codes",
                "source_scene_timestamps_ms",
                "timestamp",
                "timestamp_drift_ms",
                "artifact_id",
                "ocr_routing",
            },
        )
        planned = selection["planned_frames"][ordinal]
        if any(frame.get(key) != planned[key] for key in planned):
            raise SparseFrameError("existing sparse frame selection relationship is invalid")
        artifact = artifact_by_id.get(frame.get("artifact_id"))
        timestamp = frame.get("timestamp")
        if artifact is None or not isinstance(timestamp, dict):
            raise SparseFrameError("existing sparse frame artifact/timestamp link is invalid")
        require_exact_keys(
            timestamp,
            "existing sparse frame timestamp",
            {
                "pts",
                "duration_pts",
                "time_base_numerator",
                "time_base_denominator",
                "timestamp_us",
                "timestamp_ms",
                "duration_us",
            },
        )
        try:
            exact_timestamp = Fraction(
                timestamp["pts"] * timestamp["time_base_numerator"],
                timestamp["time_base_denominator"],
            )
            exact_duration = Fraction(
                timestamp["duration_pts"] * timestamp["time_base_numerator"],
                timestamp["time_base_denominator"],
            )
        except (KeyError, TypeError, ZeroDivisionError) as error:
            raise SparseFrameError("existing sparse frame timestamp is invalid") from error
        expected_timestamp_fields = {
            "timestamp_us": round(exact_timestamp * 1_000_000),
            "timestamp_ms": round(exact_timestamp * 1_000),
            "duration_us": round(exact_duration * 1_000_000),
        }
        if (
            any(timestamp.get(key) != value for key, value in expected_timestamp_fields.items())
            or frame.get("timestamp_drift_ms")
            != abs(timestamp["timestamp_ms"] - planned["requested_timestamp_ms"])
            or frame["timestamp_drift_ms"] > recipe["limits"]["max_timestamp_drift_ms"]
        ):
            raise SparseFrameError("existing sparse frame exact timestamp evidence is invalid")
        expected_frame_id = stable_id(
            "frame",
            result_key,
            ordinal,
            planned["requested_timestamp_ms"],
            timestamp.get("pts"),
            timestamp.get("time_base_numerator"),
            timestamp.get("time_base_denominator"),
            artifact["sha256"],
        )
        if frame.get("frame_id") != expected_frame_id or frame.get("ocr_routing") != ocr_route(planned["selection_reason_codes"]):
            raise SparseFrameError("existing sparse frame row identity/routing is invalid")
    for command in commands:
        if (
            not isinstance(command, list)
            or not 1 <= len(command) <= 80
            or any(not isinstance(argument, str) or not argument for argument in command)
            or command[0] != ffmpeg_observation["path"]
        ):
            raise SparseFrameError("existing sparse frame command provenance is invalid")
    return result


def extraction_recipe(work_order: dict[str, Any], ffmpeg_observation: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "stage": STAGE,
        "sampling": work_order["sampling"],
        "limits": work_order["limits"],
        "decoder": {
            "name": "ffmpeg",
            "executable_sha256": ffmpeg_observation["sha256"],
            "version_output_sha256": ffmpeg_observation["version_output_sha256"],
            "version": ffmpeg_observation["version"],
        },
        "extraction": {
            "input": "media_preprocess.low_resolution_cfr_proxy",
            "seek": "input_accurate_copyts",
            "threads": 1,
            "format": "png",
            "pixel_format": "rgb24",
            "compression_level": 9,
            "prediction": "mixed",
            "bitexact_flags": True,
            "metadata_removed": True,
            "timestamp_evidence": "ffmpeg_showinfo_pts_and_time_base",
        },
        "output_contract": "sparse-frame-png-ocr-candidate-routing-v1",
    }


def result_layout(output_root: Path, proxy_sha256: str, result_key: str) -> tuple[Path, Path]:
    run_dir = (
        output_root
        / "vision"
        / "sparse-frames"
        / "sha256"
        / proxy_sha256[:2]
        / proxy_sha256
        / "results"
        / result_key
    )
    return run_dir, run_dir / "result.json"


def run_sparse_frames(work_order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    started_clock = time.monotonic()
    preprocess_observation, proxy_observation, handoff = load_preprocess_handoff(work_order)
    ffmpeg_observation = observe_ffmpeg(work_order)
    selection = build_selection(
        duration_ms=proxy_observation["probe"]["duration_ms"],
        scenes=handoff["routing_scene_changes"],
        sampling=work_order["sampling"],
    )
    if selection["candidate_counts"]["planned_frames"] > work_order["limits"]["max_frames"]:
        raise SparseFrameError("planned frame count unexpectedly exceeds limits.max_frames")
    recipe = extraction_recipe(work_order, ffmpeg_observation)
    recipe_sha256 = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_sparse_frames_{recipe_sha256[:32]}"
    work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
    result_identity = {
        "work_order_sha256": work_order_sha256,
        "preprocess_result_sha256": preprocess_observation["sha256"],
        "proxy_sha256": proxy_observation["sha256"],
        "proxy_artifact_id": proxy_observation["artifact_id"],
        "parent_processing_run_id": proxy_observation["parent_processing_run_id"],
        "recipe_id": recipe_id,
        "selection": selection,
    }
    result_key = sha256_bytes(canonical_bytes(result_identity))
    processing_run_id = f"run_sparse_frames_{result_key[:32]}"
    run_dir, result_path = result_layout(
        Path(work_order["output"]["root"]), proxy_observation["sha256"], result_key
    )
    planned_commands = [
        frame_command(
            Path(ffmpeg_observation["path"]),
            Path(proxy_observation["path"]),
            proxy_observation["probe"]["video_stream_index"],
            planned["requested_timestamp_ms"],
            run_dir / "frames" / f"frame-{planned['ordinal']:04d}-{planned['requested_timestamp_ms']:012d}.png",
        )
        for planned in selection["planned_frames"]
    ]
    base_processing = {
        "processing_run_id": processing_run_id,
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "parameters_json": canonical_json_text(recipe),
        "environment_json": {
            "python": sys.version.split()[0],
            "cpu_only": True,
            "network": "not_used",
            "tool_paths": {"ffmpeg": ffmpeg_observation["path"]},
        },
        "started_at": started_at,
        "completed_at": utc_now(),
        "status": "queued" if dry_run else "completed",
        "error_text": None,
    }
    common = {
        "schema_version": 1,
        "job_id": work_order["job_id"],
        "work_order_sha256": work_order_sha256,
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha256,
        "result_key": result_key,
        "processing_run": base_processing,
        "preprocess_result": {
            **preprocess_observation,
            "processing_run_id": handoff["processing_run_id"],
            "implementation_version": handoff["implementation_version"],
        },
        "input_proxy": proxy_observation,
        "ffmpeg": ffmpeg_observation,
        "selection": selection,
        "result_path": str(result_path),
        "duration_ms": round((time.monotonic() - started_clock) * 1_000),
        "errors": [],
    }
    if dry_run:
        return {
            **common,
            "status": "planned",
            "dry_run": True,
            "commands": planned_commands,
            "frames": [],
            "artifacts": [],
        }

    run_dir.parent.mkdir(parents=True, exist_ok=True)
    lock_path = run_dir.parent / f".{result_key}.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SparseFrameError(
                f"another sparse frame writer holds the result lock: {run_dir}"
            ) from error
        if result_path.is_file():
            verify_inputs(preprocess_observation, proxy_observation, ffmpeg_observation)
            return validate_completed_reuse(
                result_path,
                result_key=result_key,
                recipe_id=recipe_id,
                recipe=recipe,
                recipe_sha256=recipe_sha256,
                job_id=work_order["job_id"],
                processing_run_id=processing_run_id,
                work_order_sha256=work_order_sha256,
                selection=selection,
                handoff=handoff,
                preprocess_observation=preprocess_observation,
                proxy_observation=proxy_observation,
                ffmpeg_observation=ffmpeg_observation,
            )
        if run_dir.exists():
            raise SparseFrameError("immutable sparse frame result directory exists without a reusable result")

        stage_dir = run_dir.parent / f".{result_key}.tmp-{uuid.uuid4().hex}"
        stage_frames = stage_dir / "frames"
        stage_frames.mkdir(parents=True, mode=0o700)
        commands: list[list[str]] = []
        frames: list[dict[str, Any]] = []
        artifacts: list[dict[str, Any]] = []
        try:
            for planned in selection["planned_frames"]:
                ordinal = planned["ordinal"]
                filename = f"frame-{ordinal:04d}-{planned['requested_timestamp_ms']:012d}.png"
                staged_path = stage_frames / filename
                final_path = run_dir / "frames" / filename
                command = frame_command(
                    Path(ffmpeg_observation["path"]),
                    Path(proxy_observation["path"]),
                    proxy_observation["probe"]["video_stream_index"],
                    planned["requested_timestamp_ms"],
                    staged_path,
                )
                completed = run_command(
                    command,
                    timeout_seconds=work_order["limits"]["timeout_seconds_per_frame"],
                )
                if not staged_path.is_file() or staged_path.stat().st_size == 0:
                    raise SparseFrameError("ffmpeg did not create a usable PNG frame")
                if staged_path.stat().st_size > work_order["limits"]["max_frame_bytes"]:
                    raise SparseFrameError("PNG frame exceeds limits.max_frame_bytes")
                image = inspect_png(staged_path)
                if (
                    image["width"] != proxy_observation["probe"]["width"]
                    or image["height"] != proxy_observation["probe"]["height"]
                ):
                    raise SparseFrameError("PNG dimensions disagree with preprocess proxy")
                timestamp = parse_showinfo(completed.stderr)
                drift = abs(timestamp["timestamp_ms"] - planned["requested_timestamp_ms"])
                if drift > work_order["limits"]["max_timestamp_drift_ms"]:
                    raise SparseFrameError(
                        f"extracted frame timestamp drift {drift} ms exceeds configured limit"
                    )
                artifact = artifact_row(
                    processing_run_id=processing_run_id,
                    ordinal=ordinal,
                    final_path=final_path,
                    staged_path=staged_path,
                    image=image,
                )
                frame_id = stable_id(
                    "frame",
                    result_key,
                    ordinal,
                    planned["requested_timestamp_ms"],
                    timestamp["pts"],
                    timestamp["time_base_numerator"],
                    timestamp["time_base_denominator"],
                    artifact["sha256"],
                )
                frames.append(
                    {
                        "frame_id": frame_id,
                        **planned,
                        "timestamp": timestamp,
                        "timestamp_drift_ms": drift,
                        "artifact_id": artifact["artifact_id"],
                        "ocr_routing": ocr_route(planned["selection_reason_codes"]),
                    }
                )
                artifacts.append(artifact)
                commands.append(command)
                os.chmod(staged_path, 0o444)

            for artifact in artifacts:
                staged_path = stage_frames / Path(artifact["path"]).name
                require_sealed_file(staged_path, "staged sparse frame artifact")
                if (
                    staged_path.stat().st_size != artifact["byte_count"]
                    or sha256_file(staged_path) != artifact["sha256"]
                    or inspect_png(staged_path) != artifact["image"]
                ):
                    raise SparseFrameError("staged sparse frame changed before result admission")

            verify_inputs(preprocess_observation, proxy_observation, ffmpeg_observation)
            completed_at = utc_now()
            processing = {**base_processing, "completed_at": completed_at, "status": "completed"}
            result = {
                **common,
                "status": "completed",
                "dry_run": False,
                "processing_run": processing,
                "preprocess_result": {
                    **preprocess_observation,
                    "processing_run_id": handoff["processing_run_id"],
                    "implementation_version": handoff["implementation_version"],
                },
                "input_proxy": proxy_observation,
                "ffmpeg": ffmpeg_observation,
                "commands": commands,
                "frames": frames,
                "artifacts": artifacts,
                "duration_ms": round((time.monotonic() - started_clock) * 1_000),
            }
            atomic_write_json(stage_dir / "result.json", result)
            os.chmod(stage_dir / "result.json", 0o444)
            os.chmod(stage_frames, 0o555)
            os.chmod(stage_dir, 0o555)
            try:
                os.rename(stage_dir, run_dir)
            except OSError as error:
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                os.chmod(stage_dir, 0o755)
                os.chmod(stage_frames, 0o755)
                shutil.rmtree(stage_dir)
                return validate_completed_reuse(
                    result_path,
                    result_key=result_key,
                    recipe_id=recipe_id,
                    recipe=recipe,
                    recipe_sha256=recipe_sha256,
                    job_id=work_order["job_id"],
                    processing_run_id=processing_run_id,
                    work_order_sha256=work_order_sha256,
                    selection=selection,
                    handoff=handoff,
                    preprocess_observation=preprocess_observation,
                    proxy_observation=proxy_observation,
                    ffmpeg_observation=ffmpeg_observation,
                )
            return result
        finally:
            if stage_dir.exists():
                os.chmod(stage_dir, 0o755)
                if stage_frames.exists():
                    os.chmod(stage_frames, 0o755)
                shutil.rmtree(stage_dir)


def default_work_order(job_id: str, preprocess_path: Path, output_root: Path) -> dict[str, Any]:
    preprocess_sha256 = sha256_file(preprocess_path)
    result = load_json(preprocess_path)
    try:
        ffmpeg_path = Path(
            result["processing_run"]["environment_json"]["tool_paths"]["ffmpeg"]
        ).resolve(strict=True)
    except (KeyError, TypeError, FileNotFoundError) as error:
        raise SparseFrameError(
            "preprocess result does not expose a usable ffmpeg tool path"
        ) from error
    completed = run_command([str(ffmpeg_path), "-version"], timeout_seconds=30)
    version_output = completed.stdout.strip()
    return {
        "schema_version": 1,
        "job_id": job_id,
        "preprocess_result": {
            "path": str(preprocess_path),
            "expected_sha256": preprocess_sha256,
        },
        "ffmpeg": {
            "path": str(ffmpeg_path),
            "expected_sha256": sha256_file(ffmpeg_path),
            "expected_version_output_sha256": sha256_bytes(version_output.encode("utf-8")),
        },
        "sampling": {
            "include_recording_start": True,
            "scene_changes": {"enabled": True, "max_frames": 64, "offset_ms": 0},
            "periodic": {"enabled": True, "interval_ms": 60_000, "max_frames": 64},
            "min_separation_ms": 1_000,
        },
        "limits": {
            "max_frames": 129,
            "max_media_duration_ms": 24 * 60 * 60 * 1_000,
            "max_input_pixels": 640 * 360,
            "max_frame_bytes": 16 * 1024 * 1024,
            "max_timestamp_drift_ms": 1_000,
            "timeout_seconds_per_frame": 120,
        },
        "output": {"root": str(output_root)},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline sparse PNG frame extraction and OCR-candidate routing"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-work-order")
    create.add_argument("--job-id", required=True)
    create.add_argument("--preprocess-result", required=True)
    create.add_argument("--output-root", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", required=True)
    run.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    job_id: str | None = None
    try:
        if args.command == "create-work-order":
            if not JOB_ID_RE.fullmatch(args.job_id):
                raise SparseFrameError("job_id contains unsupported characters or is too long")
            preprocess = absolute_file(args.preprocess_result, "--preprocess-result")
            output_root = absolute_output_root(args.output_root)
            generated = default_work_order(args.job_id, preprocess, output_root)
            result = validate_work_order(generated)
        else:
            work_order_path = absolute_file(args.work_order, "--work-order")
            raw = load_json(work_order_path, maximum_bytes=4 * 1024 * 1024)
            if (
                isinstance(raw, dict)
                and isinstance(raw.get("job_id"), str)
                and JOB_ID_RE.fullmatch(raw["job_id"])
            ):
                job_id = raw["job_id"]
            result = validate_work_order(raw)
            if args.command == "run":
                result = run_sparse_frames(result, dry_run=args.dry_run)
        sys.stdout.write(pretty_json(result))
        return 0
    except (SparseFrameError, OSError, subprocess.SubprocessError) as error:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "job_id": job_id,
            "error": {"type": type(error).__name__, "message": str(error)},
            "errors": [{"type": type(error).__name__, "message": str(error)}],
        }
        sys.stderr.write(pretty_json(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
