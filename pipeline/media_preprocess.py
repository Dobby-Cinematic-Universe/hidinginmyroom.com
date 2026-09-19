#!/usr/bin/env python3
"""Offline, CPU-first preprocessing for local HIMR media.

The program deliberately has no network code and uses only the Python standard
library plus ffmpeg/ffprobe.  A work order always names an absolute local source
path and an explicit, non-temporary output root.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import mimetypes
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.3"
# FFmpeg's decoded audio/video tail can extend slightly beyond the normalized
# container duration. Keep the declared routing coordinate space authoritative, but
# reject a discrepancy large enough to indicate a bad probe or discontinuity rather
# than silently hiding it.
MAX_ROUTING_TAIL_OVERRUN_MS = 250
PIPELINE_ROOT = Path(__file__).resolve().parent
DEFAULT_PROFILE = PIPELINE_ROOT / "profiles" / "cpu-balanced-v1.json"
OPERATIONS = ("probe", "audio_flac", "proxy", "routing")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SCENE_RE = re.compile(
    r"lavfi\.scd\.score:\s*([0-9]+(?:\.[0-9]+)?),\s*"
    r"lavfi\.scd\.time:\s*([0-9]+(?:\.[0-9]+)?)"
)
SILENCE_START_RE = re.compile(r"silence_start:\s*(-?[0-9]+(?:\.[0-9]+)?)")
SILENCE_END_RE = re.compile(
    r"silence_end:\s*(-?[0-9]+(?:\.[0-9]+)?)"
    r"(?:\s*\|\s*silence_duration:\s*([0-9]+(?:\.[0-9]+)?))?"
)


class PipelineError(RuntimeError):
    """A user-actionable contract or processing failure."""


class CommandError(PipelineError):
    def __init__(self, command: list[str], returncode: int, stderr: str):
        tail = "\n".join(stderr.splitlines()[-40:])
        super().__init__(
            f"Command exited with status {returncode}: {command[0]}\n{tail}".rstrip()
        )
        self.command = command
        self.returncode = returncode
        self.stderr_tail = tail


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


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


def atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write(path, pretty_json(value).encode("utf-8"))


def atomic_write_immutable(path: Path, body: bytes) -> None:
    """Atomically create a read-only file without replacing an existing path."""

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
            raise PipelineError(f"immutable output already exists: {path}") from error
        os.chmod(path, 0o444)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_json_immutable(path: Path, value: Any) -> None:
    atomic_write_immutable(path, pretty_json(value).encode("utf-8"))


@contextmanager
def recipe_writer_lock(recipe_dir: Path):
    """Serialize discovery and admission for one source/recipe directory."""

    recipe_dir.mkdir(parents=True, exist_ok=True)
    lock_path = recipe_dir / ".preprocess.lock"
    with lock_path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise PipelineError(
                f"another preprocessing writer holds the recipe lock: {recipe_dir}"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def subprocess_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.update({"LC_ALL": "C", "LANG": "C", "TZ": "UTC"})
    return environment


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=subprocess_environment(),
        check=False,
    )
    if completed.returncode != 0:
        raise CommandError(command, completed.returncode, completed.stderr)
    return completed


def require_tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise PipelineError(f"Required executable is not available on PATH: {name}")
    return str(Path(path).resolve())


def tool_version_output(path: str) -> str:
    completed = run_command([path, "-version"])
    output = completed.stdout.strip()
    if not output:
        raise PipelineError(f"{path} -version returned no build information")
    return output


def executable_provenance(path: str, name: str) -> dict[str, Any]:
    """Hash an executable around its version query and retain build provenance."""

    executable = Path(path)
    stat_before = source_stat(executable)
    executable_sha256 = sha256_file(executable)
    version_output = tool_version_output(path)
    stat_after = source_stat(executable)
    if stat_before != stat_after or sha256_file(executable) != executable_sha256:
        raise PipelineError(f"{name} executable changed while provenance was captured")
    lines = version_output.splitlines()
    configuration = next(
        (line.partition(":")[2].strip() for line in lines if line.startswith("configuration:")),
        None,
    )
    return {
        "name": name,
        "path": str(executable.resolve()),
        "executable_sha256": executable_sha256,
        "executable_byte_count": stat_before["byte_count"],
        "version": lines[0].strip(),
        "version_output": version_output,
        "version_output_sha256": sha256_bytes(version_output.encode("utf-8")),
        "build_configuration": configuration,
    }


def verify_executable_provenance(provenance: dict[str, Any]) -> None:
    path = Path(provenance["path"])
    before = source_stat(path)
    observed_sha256 = sha256_file(path)
    observed_version_output = tool_version_output(str(path))
    after = source_stat(path)
    if (
        before != after
        or before["byte_count"] != provenance["executable_byte_count"]
        or observed_sha256 != provenance["executable_sha256"]
        or observed_version_output != provenance["version_output"]
    ):
        raise PipelineError(
            f"{provenance['name']} executable/build changed during processing"
        )


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise PipelineError(f"{label} must be between {minimum} and {maximum}")
    return value


def number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PipelineError(f"{label} must be a number")
    result = float(value)
    if not minimum <= result <= maximum:
        raise PipelineError(f"{label} must be between {minimum} and {maximum}")
    return result


def require_exact_keys(value: dict[str, Any], label: str, required: set[str]) -> None:
    missing = sorted(required - value.keys())
    unknown = sorted(value.keys() - required)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing keys: {', '.join(missing)}")
        if unknown:
            details.append(f"unknown keys: {', '.join(unknown)}")
        raise PipelineError(f"{label} has " + "; ".join(details))


def validate_profile(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise PipelineError("profile must be a JSON object")
    required = {
        "profile_id",
        "ffmpeg_threads",
        "audio_sample_rate_hz",
        "audio_channels",
        "audio_sample_format",
        "flac_compression_level",
        "proxy_width",
        "proxy_height",
        "proxy_fps",
        "proxy_video_codec",
        "proxy_preset",
        "proxy_crf",
        "proxy_audio_codec",
        "proxy_audio_bitrate",
        "scene_threshold_percent",
        "silence_noise_db",
        "silence_min_duration_ms",
        "near_silent_fraction",
    }
    missing = sorted(required - raw.keys())
    unknown = sorted(raw.keys() - required)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing keys: {', '.join(missing)}")
        if unknown:
            details.append(f"unknown keys: {', '.join(unknown)}")
        raise PipelineError("profile has " + "; ".join(details))
    profile = dict(raw)
    if not isinstance(profile["profile_id"], str) or not profile["profile_id"]:
        raise PipelineError("profile.profile_id must be a non-empty string")
    profile["ffmpeg_threads"] = integer(
        profile["ffmpeg_threads"], "profile.ffmpeg_threads", 1, 32
    )
    profile["audio_sample_rate_hz"] = integer(
        profile["audio_sample_rate_hz"], "profile.audio_sample_rate_hz", 8_000, 192_000
    )
    profile["audio_channels"] = integer(
        profile["audio_channels"], "profile.audio_channels", 1, 8
    )
    if profile["audio_sample_rate_hz"] != 16_000 or profile["audio_channels"] != 1:
        raise PipelineError(
            "contract version 1 requires 16 kHz mono normalized audio"
        )
    if profile["audio_sample_format"] != "s16":
        raise PipelineError(
            "contract version 1 requires profile.audio_sample_format to be s16"
        )
    profile["flac_compression_level"] = integer(
        profile["flac_compression_level"], "profile.flac_compression_level", 0, 12
    )
    profile["proxy_width"] = integer(
        profile["proxy_width"], "profile.proxy_width", 2, 1_280
    )
    profile["proxy_height"] = integer(
        profile["proxy_height"], "profile.proxy_height", 2, 1_280
    )
    if profile["proxy_width"] % 2 or profile["proxy_height"] % 2:
        raise PipelineError("proxy dimensions must both be even")
    if profile["proxy_width"] * profile["proxy_height"] > 1_280 * 720:
        raise PipelineError("proxy resolution may not exceed 1280x720 pixels")
    profile["proxy_fps"] = integer(profile["proxy_fps"], "profile.proxy_fps", 1, 120)
    profile["proxy_crf"] = integer(profile["proxy_crf"], "profile.proxy_crf", 0, 51)
    profile["scene_threshold_percent"] = number(
        profile["scene_threshold_percent"],
        "profile.scene_threshold_percent",
        0.0,
        100.0,
    )
    profile["silence_noise_db"] = number(
        profile["silence_noise_db"], "profile.silence_noise_db", -100.0, 0.0
    )
    profile["silence_min_duration_ms"] = integer(
        profile["silence_min_duration_ms"],
        "profile.silence_min_duration_ms",
        1,
        3_600_000,
    )
    profile["near_silent_fraction"] = number(
        profile["near_silent_fraction"], "profile.near_silent_fraction", 0.0, 1.0
    )
    for key in (
        "proxy_video_codec",
        "proxy_preset",
        "proxy_audio_codec",
        "proxy_audio_bitrate",
    ):
        if not isinstance(profile[key], str) or not profile[key]:
            raise PipelineError(f"profile.{key} must be a non-empty string")
    return profile


def absolute_local_path(value: Any, label: str, must_exist: bool) -> Path:
    if not isinstance(value, str) or not value:
        raise PipelineError(f"{label} must be a non-empty path string")
    if "://" in value:
        raise PipelineError(f"{label} must be a local path, not a URL")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise PipelineError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=must_exist)
    except FileNotFoundError as error:
        raise PipelineError(f"{label} does not exist: {path}") from error
    if must_exist and not resolved.is_file():
        raise PipelineError(f"{label} must identify a regular file: {resolved}")
    return resolved


def validate_output_root(path: Path) -> None:
    if path == Path("/"):
        raise PipelineError("output.root may not be the filesystem root")
    if path.exists() and not path.is_dir():
        raise PipelineError("output.root must identify a directory or a new path")
    forbidden = (Path("/tmp"), Path("/var/tmp"))
    for root in forbidden:
        if path == root or root in path.parents:
            raise PipelineError(f"output.root may not be under {root}")


def validate_work_order(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise PipelineError("work order must be a JSON object")
    require_exact_keys(
        raw,
        "work order",
        {"schema_version", "job_id", "source", "output", "operations", "profile"},
    )
    if raw.get("schema_version") != CONTRACT_VERSION:
        raise PipelineError(f"work order schema_version must be {CONTRACT_VERSION}")
    job_id = raw.get("job_id")
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise PipelineError("job_id contains unsupported characters or is too long")
    source_raw = raw.get("source")
    output_raw = raw.get("output")
    operations_raw = raw.get("operations")
    if not isinstance(source_raw, dict) or not isinstance(output_raw, dict):
        raise PipelineError("source and output must be JSON objects")
    if not isinstance(operations_raw, dict):
        raise PipelineError("operations must be a JSON object")
    require_exact_keys(
        source_raw,
        "source",
        {"path", "expected_sha256", "first_cataloged_at"},
    )
    require_exact_keys(output_raw, "output", {"root"})
    require_exact_keys(operations_raw, "operations", set(OPERATIONS))
    source = absolute_local_path(source_raw.get("path"), "source.path", must_exist=True)
    output_root = absolute_local_path(
        output_raw.get("root"), "output.root", must_exist=False
    )
    validate_output_root(output_root)
    expected_sha256 = source_raw.get("expected_sha256")
    if expected_sha256 is not None:
        if not isinstance(expected_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", expected_sha256
        ):
            raise PipelineError("source.expected_sha256 must be a lowercase SHA-256")
    first_cataloged_at = source_raw.get("first_cataloged_at")
    if first_cataloged_at is not None:
        if (
            not isinstance(first_cataloged_at, str)
            or not first_cataloged_at
            or len(first_cataloged_at) > 100
            or "\x00" in first_cataloged_at
        ):
            raise PipelineError(
                "source.first_cataloged_at must be null or a bounded timestamp string"
            )
        try:
            parsed_catalog_time = datetime.fromisoformat(
                first_cataloged_at.replace("Z", "+00:00")
            )
        except ValueError as error:
            raise PipelineError(
                "source.first_cataloged_at must be an RFC 3339 timestamp"
            ) from error
        if parsed_catalog_time.tzinfo is None:
            raise PipelineError(
                "source.first_cataloged_at must include a UTC offset"
            )
        normalized_catalog_time = parsed_catalog_time.astimezone(timezone.utc)
        first_cataloged_at = normalized_catalog_time.isoformat(
            timespec="microseconds" if normalized_catalog_time.microsecond else "seconds"
        ).replace("+00:00", "Z")
    operations: dict[str, bool] = {}
    for operation in OPERATIONS:
        value = operations_raw.get(operation)
        if not isinstance(value, bool):
            raise PipelineError(f"operations.{operation} must be boolean")
        operations[operation] = value
    if not operations["probe"]:
        raise PipelineError("operations.probe must be true in contract version 1")
    profile = validate_profile(raw.get("profile"))
    return {
        "schema_version": CONTRACT_VERSION,
        "job_id": job_id,
        "source": {
            "path": str(source),
            "expected_sha256": expected_sha256,
            "first_cataloged_at": first_cataloged_at,
        },
        "output": {"root": str(output_root)},
        "operations": operations,
        "profile": profile,
    }


def parse_optional_float(value: Any) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def milliseconds(value: Any) -> int | None:
    parsed = parse_optional_float(value)
    return None if parsed is None else round(parsed * 1_000)


def optional_integer(value: Any) -> int | None:
    parsed = parse_optional_float(value)
    return None if parsed is None else round(parsed)


def rational(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str) or value in ("", "0/0", "N/A"):
        return None
    try:
        parsed = Fraction(value)
    except (ValueError, ZeroDivisionError):
        return None
    return {
        "text": value,
        "numerator": parsed.numerator,
        "denominator": parsed.denominator,
        "decimal": round(float(parsed), 8),
    }


def safe_tags(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    allowlist = {"language", "title", "handler_name"}
    return {
        key: str(value[key])
        for key in sorted(value)
        if key in allowlist and value[key] is not None
    }


def normalize_probe(
    raw: dict[str, Any],
    *,
    media_id: str,
    sha256: str,
    byte_count: int,
    basename: str,
    ffprobe_version: str,
) -> dict[str, Any]:
    streams: list[dict[str, Any]] = []
    for stream in sorted(raw.get("streams") or [], key=lambda item: item.get("index", 0)):
        if not isinstance(stream, dict):
            continue
        disposition = stream.get("disposition") if isinstance(stream.get("disposition"), dict) else {}
        side_data = []
        for item in stream.get("side_data_list") or []:
            if not isinstance(item, dict):
                continue
            selected = {
                key: item[key]
                for key in ("side_data_type", "rotation")
                if key in item
            }
            if selected:
                side_data.append(selected)
        normalized = {
            "index": stream.get("index"),
            "codec_type": stream.get("codec_type"),
            "codec_name": stream.get("codec_name"),
            "codec_long_name": stream.get("codec_long_name"),
            "profile": stream.get("profile"),
            "duration_ms": milliseconds(stream.get("duration")),
            "start_ms": milliseconds(stream.get("start_time")),
            "bit_rate_bps": optional_integer(stream.get("bit_rate")),
            "time_base": stream.get("time_base"),
            "disposition": {
                key: int(bool(disposition.get(key, 0)))
                for key in ("default", "forced", "attached_pic")
            },
            "tags": safe_tags(stream.get("tags")),
            "side_data": side_data,
        }
        if stream.get("codec_type") == "video":
            normalized["video"] = {
                "width": stream.get("width"),
                "height": stream.get("height"),
                "pixel_format": stream.get("pix_fmt"),
                "sample_aspect_ratio": stream.get("sample_aspect_ratio"),
                "display_aspect_ratio": stream.get("display_aspect_ratio"),
                "average_frame_rate": rational(stream.get("avg_frame_rate")),
                "reported_frame_rate": rational(stream.get("r_frame_rate")),
                "frame_count": optional_integer(stream.get("nb_frames")),
            }
        elif stream.get("codec_type") == "audio":
            normalized["audio"] = {
                "sample_rate_hz": optional_integer(stream.get("sample_rate")),
                "channels": stream.get("channels"),
                "channel_layout": stream.get("channel_layout"),
                "sample_format": stream.get("sample_fmt"),
            }
        streams.append(normalized)

    video_indexes = [
        stream["index"]
        for stream in streams
        if stream["codec_type"] == "video"
        and not stream.get("disposition", {}).get("attached_pic")
    ]
    audio_indexes = [
        stream["index"] for stream in streams if stream["codec_type"] == "audio"
    ]
    raw_format = raw.get("format") if isinstance(raw.get("format"), dict) else {}
    duration_ms = milliseconds(raw_format.get("duration"))
    if duration_ms is None:
        stream_durations = [
            stream["duration_ms"] for stream in streams if stream["duration_ms"] is not None
        ]
        duration_ms = max(stream_durations, default=None)
    chapters = []
    for ordinal, chapter in enumerate(raw.get("chapters") or []):
        if not isinstance(chapter, dict):
            continue
        chapters.append(
            {
                "ordinal": ordinal,
                "start_ms": milliseconds(chapter.get("start_time")),
                "end_ms": milliseconds(chapter.get("end_time")),
                "tags": safe_tags(chapter.get("tags")),
            }
        )
    return {
        "schema_version": CONTRACT_VERSION,
        "media": {
            "media_id": media_id,
            "sha256": sha256,
            "byte_count": byte_count,
            "basename": basename,
        },
        "tool": {"name": "ffprobe", "version": ffprobe_version},
        "format": {
            "format_name": raw_format.get("format_name"),
            "format_long_name": raw_format.get("format_long_name"),
            "duration_ms": duration_ms,
            "start_ms": milliseconds(raw_format.get("start_time")),
            "bit_rate_bps": optional_integer(raw_format.get("bit_rate")),
            "probe_score": optional_integer(raw_format.get("probe_score")),
            "tags": safe_tags(raw_format.get("tags")),
        },
        "primary_streams": {
            "video_index": video_indexes[0] if video_indexes else None,
            "audio_index": audio_indexes[0] if audio_indexes else None,
        },
        "streams": streams,
        "chapters": chapters,
    }


def ffprobe_command(ffprobe: str, source: Path) -> list[str]:
    return [
        ffprobe,
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-show_chapters",
        "-of",
        "json",
        str(source),
    ]


def probe_file(
    ffprobe: str,
    source: Path,
    *,
    media_id: str,
    sha256: str,
    ffprobe_version: str,
) -> tuple[dict[str, Any], list[str]]:
    command = ffprobe_command(ffprobe, source)
    completed = run_command(command)
    try:
        raw = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise PipelineError(f"ffprobe returned invalid JSON for {source}") from error
    normalized = normalize_probe(
        raw,
        media_id=media_id,
        sha256=sha256,
        byte_count=source.stat().st_size,
        basename=source.name,
        ffprobe_version=ffprobe_version,
    )
    return normalized, command


def base_ffmpeg_command(ffmpeg: str, threads: int, loglevel: str) -> list[str]:
    return [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-nostats",
        "-loglevel",
        loglevel,
        "-threads",
        str(threads),
    ]


def audio_command(
    ffmpeg: str,
    source: Path,
    output: Path,
    audio_index: int,
    profile: dict[str, Any],
) -> list[str]:
    return base_ffmpeg_command(ffmpeg, profile["ffmpeg_threads"], "error") + [
        "-i",
        str(source),
        "-map",
        f"0:{audio_index}",
        "-vn",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-ac",
        str(profile["audio_channels"]),
        "-ar",
        str(profile["audio_sample_rate_hz"]),
        "-sample_fmt",
        profile["audio_sample_format"],
        "-c:a",
        "flac",
        "-compression_level",
        str(profile["flac_compression_level"]),
        str(output),
    ]


def proxy_command(
    ffmpeg: str,
    source: Path,
    output: Path,
    video_index: int,
    audio_index: int | None,
    profile: dict[str, Any],
) -> list[str]:
    width = profile["proxy_width"]
    height = profile["proxy_height"]
    fps = profile["proxy_fps"]
    video_filter = (
        f"scale=w={width}:h={height}:force_original_aspect_ratio=decrease:"
        f"force_divisible_by=2,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,"
        f"setsar=1,fps={fps}"
    )
    command = base_ffmpeg_command(ffmpeg, profile["ffmpeg_threads"], "error") + [
        "-i",
        str(source),
        "-map",
        f"0:{video_index}",
    ]
    if audio_index is not None:
        command += ["-map", f"0:{audio_index}"]
    command += [
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-vf",
        video_filter,
        "-c:v",
        profile["proxy_video_codec"],
        "-preset",
        profile["proxy_preset"],
        "-crf",
        str(profile["proxy_crf"]),
        "-pix_fmt",
        "yuv420p",
        "-fps_mode",
        "cfr",
    ]
    if audio_index is not None:
        command += [
            "-c:a",
            profile["proxy_audio_codec"],
            "-b:a",
            profile["proxy_audio_bitrate"],
            "-ar",
            "48000",
            "-ac",
            "2",
        ]
    else:
        command += ["-an"]
    command += ["-movflags", "+faststart", "-avoid_negative_ts", "make_zero", str(output)]
    return command


def routing_command(
    ffmpeg: str,
    source: Path,
    video_index: int | None,
    audio_index: int | None,
    profile: dict[str, Any],
) -> list[str] | None:
    if video_index is None and audio_index is None:
        return None
    command = base_ffmpeg_command(ffmpeg, profile["ffmpeg_threads"], "info") + [
        "-i",
        str(source),
    ]
    scene_filter = (
        # sc_pass=1 drops every frame until a scene crosses the threshold. A static
        # source can therefore reach EOF without one video packet and make FFmpeg's
        # combined null output fail before audio silence metadata is drained.
        f"scdet=threshold={profile['scene_threshold_percent']}:sc_pass=0"
    )
    silence_filter = (
        f"silencedetect=noise={profile['silence_noise_db']}dB:"
        f"duration={profile['silence_min_duration_ms'] / 1000:g}"
    )
    if video_index is not None and audio_index is not None:
        command += [
            "-filter_complex",
            f"[0:{video_index}]{scene_filter}[vroute];"
            f"[0:{audio_index}]{silence_filter}[aroute]",
            "-map",
            "[vroute]",
            "-map",
            "[aroute]",
        ]
    elif video_index is not None:
        command += ["-map", f"0:{video_index}", "-vf", scene_filter]
    else:
        command += ["-map", f"0:{audio_index}", "-af", silence_filter]
    command += ["-f", "null", "-"]
    return command


def temporary_media_path(final_path: Path) -> Path:
    return final_path.with_name(
        f".{final_path.stem}.tmp-{os.getpid()}{final_path.suffix}"
    )


def create_media_artifact(final_path: Path, command_builder) -> list[str]:
    """Create a new read-only media artifact atomically without overwriting."""

    if final_path.exists():
        raise PipelineError(f"immutable media output already exists: {final_path}")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary_media_path(final_path)
    if temporary.exists():
        temporary.unlink()
    command = command_builder(temporary)
    try:
        run_command(command)
        if not temporary.is_file() or temporary.stat().st_size == 0:
            raise PipelineError(f"ffmpeg did not create a usable artifact: {temporary}")
        try:
            os.link(temporary, final_path)
        except FileExistsError as error:
            raise PipelineError(f"immutable media output already exists: {final_path}") from error
        os.chmod(final_path, 0o444)
    finally:
        if temporary.exists():
            temporary.unlink()
    return command


def require_immutable_regular_file(path: Path, label: str) -> os.stat_result:
    try:
        file_stat = path.lstat()
    except FileNotFoundError as error:
        raise PipelineError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
        raise PipelineError(f"{label} must be a regular non-symlink file: {path}")
    if file_stat.st_mode & 0o222:
        raise PipelineError(f"{label} is not sealed read-only: {path}")
    return file_stat


def hardlink_immutable(source: Path, destination: Path, label: str) -> None:
    """Make a run-local immutable hard link to a verified prior artifact."""

    require_immutable_regular_file(source, label)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, destination, follow_symlinks=False)
    except FileExistsError as error:
        raise PipelineError(f"immutable reuse destination already exists: {destination}") from error
    require_immutable_regular_file(destination, f"reused {label}")


def source_stat(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "byte_count": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def stream_by_index(probe: dict[str, Any], index: int | None) -> dict[str, Any] | None:
    return next(
        (stream for stream in probe["streams"] if stream["index"] == index), None
    )


def validate_audio_artifact(probe: dict[str, Any], profile: dict[str, Any]) -> None:
    stream = stream_by_index(probe, probe["primary_streams"]["audio_index"])
    if not stream or stream.get("codec_name") != "flac":
        raise PipelineError("existing/generated audio artifact is not FLAC")
    audio = stream.get("audio") or {}
    if audio.get("sample_rate_hz") != profile["audio_sample_rate_hz"]:
        raise PipelineError("audio artifact has an unexpected sample rate")
    if audio.get("channels") != profile["audio_channels"]:
        raise PipelineError("audio artifact has an unexpected channel count")
    if audio.get("sample_format") != profile["audio_sample_format"]:
        raise PipelineError("audio artifact has an unexpected sample format")


def validate_proxy_artifact(probe: dict[str, Any], profile: dict[str, Any]) -> None:
    stream = stream_by_index(probe, probe["primary_streams"]["video_index"])
    if not stream:
        raise PipelineError("existing/generated proxy has no video stream")
    video = stream.get("video") or {}
    if video.get("width") != profile["proxy_width"] or video.get("height") != profile["proxy_height"]:
        raise PipelineError("proxy artifact has unexpected dimensions")
    frame_rate = video.get("average_frame_rate") or video.get("reported_frame_rate") or {}
    if abs(float(frame_rate.get("decimal", 0)) - profile["proxy_fps"]) > 0.001:
        raise PipelineError("proxy artifact is not at the requested CFR frame rate")


def bounded_routing_tail_coordinate(
    value_ms: int, duration_ms: int | None, label: str
) -> int:
    """Clip one decoded tail coordinate or fail when its overrun is not credible."""

    if duration_ms is None or value_ms <= duration_ms:
        return value_ms
    overrun_ms = value_ms - duration_ms
    if overrun_ms > MAX_ROUTING_TAIL_OVERRUN_MS:
        raise PipelineError(
            f"{label} exceeds normalized media duration by {overrun_ms} ms "
            f"(maximum tolerated decoder/container tail drift is "
            f"{MAX_ROUTING_TAIL_OVERRUN_MS} ms)"
        )
    return duration_ms


def expected_routing_summary(
    *,
    duration_ms: int | None,
    has_audio: bool,
    scene_changes: list[dict[str, Any]],
    silence_intervals: list[dict[str, int]],
) -> dict[str, Any]:
    silent_ms = sum(item["duration_ms"] for item in silence_intervals)
    if duration_ms is not None:
        silent_ms = min(silent_ms, duration_ms)
    silent_fraction = (
        round(silent_ms / duration_ms, 6)
        if has_audio and duration_ms is not None and duration_ms > 0
        else None
    )
    return {
        "scene_change_count": len(scene_changes),
        "silence_interval_count": len(silence_intervals),
        "silent_duration_ms": silent_ms if has_audio else None,
        "silent_fraction": silent_fraction,
    }


def validate_routing_integrity(
    routing: Any,
    *,
    expected_duration_ms: int | None,
    expected_has_video: bool,
    expected_has_audio: bool,
    expected_source_media_id: str,
    expected_profile: dict[str, Any],
    label: str,
) -> None:
    """Re-derive all routing coordinates and summaries from sealed primitive rows."""

    if not isinstance(routing, dict):
        raise PipelineError(f"{label} must be an object")
    require_exact_keys(
        routing,
        label,
        {
            "schema_version",
            "parameters",
            "coverage",
            "scene_changes",
            "silence_intervals",
            "summary",
            "routing_candidates",
            "warning",
            "source_media_id",
        },
    )
    if routing.get("schema_version") != CONTRACT_VERSION:
        raise PipelineError(f"{label}.schema_version must be {CONTRACT_VERSION}")
    if routing.get("source_media_id") != expected_source_media_id:
        raise PipelineError(f"{label}.source_media_id disagrees with the source media")

    parameters = routing.get("parameters")
    if not isinstance(parameters, dict):
        raise PipelineError(f"{label}.parameters must be an object")
    require_exact_keys(
        parameters,
        f"{label}.parameters",
        {
            "scene_threshold_percent",
            "silence_noise_db",
            "silence_min_duration_ms",
            "near_silent_fraction",
        },
    )
    expected_parameters = {
        "scene_threshold_percent": expected_profile["scene_threshold_percent"],
        "silence_noise_db": expected_profile["silence_noise_db"],
        "silence_min_duration_ms": expected_profile["silence_min_duration_ms"],
        "near_silent_fraction": expected_profile["near_silent_fraction"],
    }
    if canonical_bytes(parameters) != canonical_bytes(expected_parameters):
        raise PipelineError(f"{label}.parameters disagree with the recipe profile")

    coverage = routing.get("coverage")
    if not isinstance(coverage, dict):
        raise PipelineError(f"{label}.coverage must be an object")
    require_exact_keys(
        coverage, f"{label}.coverage", {"duration_ms", "has_video", "has_audio"}
    )
    duration_ms = coverage.get("duration_ms")
    if duration_ms is not None and (
        isinstance(duration_ms, bool)
        or not isinstance(duration_ms, int)
        or duration_ms < 0
    ):
        raise PipelineError(f"{label}.coverage.duration_ms must be null or non-negative")
    if duration_ms != expected_duration_ms:
        raise PipelineError(f"{label}.coverage.duration_ms disagrees with the source probe")
    for key, expected in (
        ("has_video", expected_has_video),
        ("has_audio", expected_has_audio),
    ):
        if coverage.get(key) is not expected:
            raise PipelineError(f"{label}.coverage.{key} disagrees with the source probe")

    scene_changes = routing.get("scene_changes")
    if not isinstance(scene_changes, list):
        raise PipelineError(f"{label}.scene_changes must be an array")
    previous_timestamp: int | None = None
    for index, scene in enumerate(scene_changes):
        scene_label = f"{label}.scene_changes[{index}]"
        if not isinstance(scene, dict):
            raise PipelineError(f"{scene_label} must be an object")
        require_exact_keys(scene, scene_label, {"timestamp_ms", "score_percent"})
        timestamp = scene.get("timestamp_ms")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
            raise PipelineError(f"{scene_label}.timestamp_ms must be non-negative")
        if duration_ms is not None and timestamp > duration_ms:
            raise PipelineError(f"{scene_label}.timestamp_ms exceeds routing coverage")
        number(scene.get("score_percent"), f"{scene_label}.score_percent", 0, 100)
        if previous_timestamp is not None and timestamp <= previous_timestamp:
            raise PipelineError(
                f"{label}.scene_changes must be strictly sorted with unique timestamps"
            )
        previous_timestamp = timestamp
    if scene_changes and not expected_has_video:
        raise PipelineError(f"{label}.scene_changes require a video stream")

    silence_intervals = routing.get("silence_intervals")
    if not isinstance(silence_intervals, list):
        raise PipelineError(f"{label}.silence_intervals must be an array")
    previous_end: int | None = None
    for index, interval in enumerate(silence_intervals):
        interval_label = f"{label}.silence_intervals[{index}]"
        if not isinstance(interval, dict):
            raise PipelineError(f"{interval_label} must be an object")
        require_exact_keys(
            interval, interval_label, {"start_ms", "end_ms", "duration_ms"}
        )
        values: dict[str, int] = {}
        for key in ("start_ms", "end_ms", "duration_ms"):
            value = interval.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise PipelineError(f"{interval_label}.{key} must be non-negative")
            values[key] = value
        if (
            values["end_ms"] <= values["start_ms"]
            or values["duration_ms"] != values["end_ms"] - values["start_ms"]
            or (duration_ms is not None and values["end_ms"] > duration_ms)
        ):
            raise PipelineError(f"{interval_label} is not an exact in-coverage interval")
        if previous_end is not None and values["start_ms"] < previous_end:
            raise PipelineError(
                f"{label}.silence_intervals must be sorted, unique, and nonoverlapping"
            )
        previous_end = values["end_ms"]
    if silence_intervals and not expected_has_audio:
        raise PipelineError(f"{label}.silence_intervals require an audio stream")

    summary = routing.get("summary")
    if not isinstance(summary, dict):
        raise PipelineError(f"{label}.summary must be an object")
    require_exact_keys(
        summary,
        f"{label}.summary",
        {
            "scene_change_count",
            "silence_interval_count",
            "silent_duration_ms",
            "silent_fraction",
        },
    )
    for key in ("scene_change_count", "silence_interval_count"):
        value = summary.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise PipelineError(f"{label}.summary.{key} must be non-negative")
    if summary.get("silent_duration_ms") is not None and (
        isinstance(summary["silent_duration_ms"], bool)
        or not isinstance(summary["silent_duration_ms"], int)
        or summary["silent_duration_ms"] < 0
    ):
        raise PipelineError(f"{label}.summary.silent_duration_ms is invalid")
    if summary.get("silent_fraction") is not None:
        number(summary["silent_fraction"], f"{label}.summary.silent_fraction", 0, 1)
    expected_summary = expected_routing_summary(
        duration_ms=duration_ms,
        has_audio=expected_has_audio,
        scene_changes=scene_changes,
        silence_intervals=silence_intervals,
    )
    if canonical_bytes(summary) != canonical_bytes(expected_summary):
        raise PipelineError(f"{label}.summary disagrees with exact routing arithmetic")


def parse_routing_log(
    stderr: str,
    *,
    duration_ms: int | None,
    has_video: bool,
    has_audio: bool,
    profile: dict[str, Any],
) -> dict[str, Any]:
    if duration_ms is not None and (
        isinstance(duration_ms, bool)
        or not isinstance(duration_ms, int)
        or duration_ms < 0
    ):
        raise PipelineError("routing duration must be null or a non-negative integer")

    # Some builds may emit the same metadata line more than once. Tail clipping can
    # also collapse multiple decoded frames onto the declared endpoint, so retain the
    # greatest score at each unique integer-millisecond timestamp.
    scene_scores: dict[int, float] = {}
    for match in SCENE_RE.finditer(stderr):
        timestamp = bounded_routing_tail_coordinate(
            max(0, round(float(match.group(2)) * 1_000)),
            duration_ms,
            "scene timestamp",
        )
        score = float(match.group(1))
        scene_scores[timestamp] = max(scene_scores.get(timestamp, score), score)
    scene_changes = [
        {"timestamp_ms": timestamp, "score_percent": scene_scores[timestamp]}
        for timestamp in sorted(scene_scores)
    ]

    silence_intervals: list[dict[str, int]] = []
    open_start: int | None = None
    for line in stderr.splitlines():
        start_match = SILENCE_START_RE.search(line)
        if start_match:
            open_start = max(0, round(float(start_match.group(1)) * 1_000))
        end_match = SILENCE_END_RE.search(line)
        if end_match:
            raw_end = max(0, round(float(end_match.group(1)) * 1_000))
            if open_start is None and end_match.group(2):
                open_start = max(
                    0,
                    raw_end - round(float(end_match.group(2)) * 1_000),
                )
            end = bounded_routing_tail_coordinate(
                raw_end, duration_ms, "silence end"
            )
            if open_start is not None and end > open_start:
                silence_intervals.append(
                    {
                        "start_ms": open_start,
                        "end_ms": end,
                        "duration_ms": end - open_start,
                    }
                )
            open_start = None
    if open_start is not None and duration_ms is not None and duration_ms > open_start:
        silence_intervals.append(
            {
                "start_ms": open_start,
                "end_ms": duration_ms,
                "duration_ms": duration_ms - open_start,
            }
        )
    silence_intervals = list(
        {
            (item["start_ms"], item["end_ms"]): item
            for item in silence_intervals
        }.values()
    )
    silence_intervals.sort(key=lambda item: (item["start_ms"], item["end_ms"]))
    previous_end: int | None = None
    for interval in silence_intervals:
        if previous_end is not None and interval["start_ms"] < previous_end:
            raise PipelineError(
                "parsed silence intervals are overlapping after deterministic normalization"
            )
        previous_end = interval["end_ms"]
    summary = expected_routing_summary(
        duration_ms=duration_ms,
        has_audio=has_audio,
        scene_changes=scene_changes,
        silence_intervals=silence_intervals,
    )
    silent_fraction = summary["silent_fraction"]
    if not has_audio:
        asr_route = "skip_no_audio"
    elif silent_fraction is not None and silent_fraction >= profile["near_silent_fraction"]:
        asr_route = "review_near_silent_candidate"
    else:
        asr_route = "process"
    if not has_video:
        ocr_route = "skip_no_video"
        visual_route = "skip_no_video"
    else:
        ocr_route = "scene_keyframes" if scene_changes else "sparse_keyframes"
        visual_route = "scene_and_speech_windows" if has_audio else "scene_keyframes"
    return {
        "schema_version": CONTRACT_VERSION,
        "parameters": {
            "scene_threshold_percent": profile["scene_threshold_percent"],
            "silence_noise_db": profile["silence_noise_db"],
            "silence_min_duration_ms": profile["silence_min_duration_ms"],
            "near_silent_fraction": profile["near_silent_fraction"],
        },
        "coverage": {
            "duration_ms": duration_ms,
            "has_video": has_video,
            "has_audio": has_audio,
        },
        "scene_changes": scene_changes,
        "silence_intervals": silence_intervals,
        "summary": summary,
        "routing_candidates": {
            "asr": asr_route,
            "ocr": ocr_route,
            "visual": visual_route,
            "diarization": "router_pending",
            "active_speaker": "router_pending",
        },
        "warning": (
            "Routing values are machine-generated workload suggestions, not content findings. "
            "They do not identify a speaker or establish that a video is single-speaker."
        ),
    }


def artifact_descriptor(
    *,
    kind: str,
    path: Path,
    processing_run_id: str,
    media_kind: str,
    mime_type: str | None,
    schema_version: int = 1,
    normalized_probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    before = require_immutable_regular_file(path, f"{kind} artifact")
    digest = sha256_file(path)
    after = require_immutable_regular_file(path, f"{kind} artifact")
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_mode,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_mode,
    ):
        raise PipelineError(f"{kind} artifact changed while it was hashed")
    return {
        "artifact_id": "artifact_" + sha256_bytes(
            canonical_bytes(
                {
                    "processing_run_id": processing_run_id,
                    "kind": kind,
                    "sha256": digest,
                }
            )
        )[:32],
        "processing_run_id": processing_run_id,
        "artifact_kind": kind,
        "storage_uri": path.resolve().as_uri(),
        "path": str(path.resolve()),
        "sha256": digest,
        "byte_count": after.st_size,
        "schema_version": schema_version,
        "visibility": "private",
        "media_kind": media_kind,
        "mime_type": mime_type,
        "normalized_probe": normalized_probe,
    }


def media_object_from_probe(
    *,
    digest: str,
    byte_count: int,
    media_kind: str,
    mime_type: str | None,
    probe: dict[str, Any],
    first_cataloged_at: str,
) -> dict[str, Any]:
    return {
        "media_id": f"media_sha256_{digest}",
        "sha256": digest,
        "byte_count": byte_count,
        "media_kind": media_kind,
        "mime_type": mime_type,
        "container": probe["format"].get("format_name"),
        "duration_ms": probe["format"].get("duration_ms"),
        "ffprobe_json": probe,
        "first_cataloged_at": first_cataloged_at,
        "integrity_state": "verified",
    }


def media_kind(probe: dict[str, Any]) -> str:
    if probe["primary_streams"]["video_index"] is not None:
        return "video"
    if probe["primary_streams"]["audio_index"] is not None:
        return "audio"
    return "other"


def recipe_tool_provenance(provenance: dict[str, Any]) -> dict[str, Any]:
    """Select reproducibility fields that belong to deterministic recipe identity."""

    return {
        key: provenance[key]
        for key in (
            "name",
            "executable_sha256",
            "executable_byte_count",
            "version",
            "version_output",
            "version_output_sha256",
            "build_configuration",
        )
    }


def work_recipe(
    work_order: dict[str, Any],
    ffmpeg_provenance: dict[str, Any],
    ffprobe_provenance: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    recipe = {
        "contract_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "operations": work_order["operations"],
        "profile": work_order["profile"],
        "tools": {
            "ffmpeg": recipe_tool_provenance(ffmpeg_provenance),
            "ffprobe": recipe_tool_provenance(ffprobe_provenance),
        },
    }
    return recipe, sha256_bytes(canonical_bytes(recipe))


def execution_run_id(
    source_sha256: str, recipe_sha256: str, execution_nonce: str
) -> str:
    return stable_id(
        "run_preprocess", source_sha256, recipe_sha256, execution_nonce
    )


def artifact_specs(
    run_dir: Path,
    work_order: dict[str, Any],
    *,
    video_index: int | None,
    audio_index: int | None,
) -> dict[str, dict[str, Any]]:
    profile = work_order["profile"]
    specs: dict[str, dict[str, Any]] = {
        "ffprobe_normalized_json": {
            "path": run_dir / "probe.normalized.json",
            "media_kind": "document",
            "mime_type": "application/json",
            "normalized_probe": False,
        }
    }
    if audio_index is not None and work_order["operations"]["audio_flac"]:
        specs["audio_16khz_mono_flac"] = {
            "path": run_dir / "artifacts" / "audio-16khz-mono.flac",
            "media_kind": "audio",
            "mime_type": "audio/flac",
            "normalized_probe": True,
        }
    if video_index is not None and work_order["operations"]["proxy"]:
        specs["low_resolution_cfr_proxy"] = {
            "path": run_dir
            / "artifacts"
            / f"proxy-{profile['proxy_width']}x{profile['proxy_height']}-"
            f"{profile['proxy_fps']}fps.mp4",
            "media_kind": "video",
            "mime_type": "video/mp4",
            "normalized_probe": True,
        }
    if work_order["operations"]["routing"] and (
        video_index is not None or audio_index is not None
    ):
        specs["scene_silence_routing_json"] = {
            "path": run_dir / "routing.json",
            "media_kind": "document",
            "mime_type": "application/json",
            "normalized_probe": False,
        }
    return specs


def checked_file_digest(
    path: Path, *, label: str, expected_sha256: str, expected_byte_count: int
) -> None:
    before = require_immutable_regular_file(path, label)
    if before.st_size != expected_byte_count:
        raise PipelineError(f"{label} byte count disagrees with its prior envelope")
    observed = sha256_file(path)
    after = require_immutable_regular_file(path, label)
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_mode,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_mode,
    )
    if before_identity != after_identity:
        raise PipelineError(f"{label} changed while its digest was verified")
    if observed != expected_sha256:
        raise PipelineError(f"{label} SHA-256 disagrees with its prior envelope")


def read_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PipelineError(f"{label} is not valid UTF-8 JSON: {path}") from error
    if not isinstance(value, dict):
        raise PipelineError(f"{label} must contain a JSON object: {path}")
    return value


def artifact_id_for(run_id: str, kind: str, digest: str) -> str:
    return "artifact_" + sha256_bytes(
        canonical_bytes(
            {"processing_run_id": run_id, "kind": kind, "sha256": digest}
        )
    )[:32]


def validate_prior_result(
    result_path: Path,
    *,
    source_sha256: str,
    source_byte_count: int,
    recipe: dict[str, Any],
    recipe_sha256: str,
    recipe_id: str,
    recipe_dir: Path,
    work_order: dict[str, Any],
    ffprobe: str,
    ffprobe_version: str,
    source_duration_ms: int | None,
    video_index: int | None,
    audio_index: int | None,
) -> dict[str, Any]:
    """Validate one sealed result and all run-local objects before reuse."""

    result_stat = require_immutable_regular_file(result_path, "prior result envelope")
    result_body = result_path.read_bytes()
    result_stat_after = require_immutable_regular_file(
        result_path, "prior result envelope"
    )
    if (
        result_stat.st_dev,
        result_stat.st_ino,
        result_stat.st_size,
        result_stat.st_mtime_ns,
        result_stat.st_mode,
    ) != (
        result_stat_after.st_dev,
        result_stat_after.st_ino,
        result_stat_after.st_size,
        result_stat_after.st_mtime_ns,
        result_stat_after.st_mode,
    ):
        raise PipelineError("prior result envelope changed while it was read")
    result_digest = sha256_bytes(result_body)
    try:
        result = json.loads(result_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PipelineError(f"prior result envelope is invalid JSON: {result_path}") from error
    if not isinstance(result, dict):
        raise PipelineError(f"prior result envelope must be a JSON object: {result_path}")
    require_exact_keys(
        result,
        "prior result envelope",
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
        result["schema_version"] != CONTRACT_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
    ):
        raise PipelineError("prior result envelope is not a successful immutable run")

    run = result.get("processing_run")
    if not isinstance(run, dict):
        raise PipelineError("prior processing_run must be an object")
    require_exact_keys(
        run,
        "prior processing_run",
        {
            "processing_run_id",
            "stage",
            "implementation_version",
            "parameters_json",
            "environment_json",
            "started_at",
            "completed_at",
            "status",
        },
    )
    run_id = run.get("processing_run_id")
    environment = run.get("environment_json")
    if not isinstance(run_id, str) or not isinstance(environment, dict):
        raise PipelineError("prior run identity or environment is invalid")
    require_exact_keys(
        environment,
        "prior run environment",
        {"platform", "python", "cpu_only", "execution_nonce", "tool_paths"},
    )
    nonce = environment.get("execution_nonce")
    if not isinstance(nonce, str) or not re.fullmatch(r"[0-9a-f]{32}", nonce):
        raise PipelineError("prior execution nonce is invalid")
    tool_paths = environment.get("tool_paths")
    if not isinstance(tool_paths, dict):
        raise PipelineError("prior tool_paths must be an object")
    require_exact_keys(tool_paths, "prior tool_paths", {"ffmpeg", "ffprobe"})
    if any(
        not isinstance(path, str) or not path or not Path(path).is_absolute()
        for path in tool_paths.values()
    ):
        raise PipelineError("prior tool paths must be non-empty absolute paths")
    if environment.get("cpu_only") is not True:
        raise PipelineError("prior processing environment is not CPU-only")
    if run_id != execution_run_id(source_sha256, recipe_sha256, nonce):
        raise PipelineError("prior processing_run_id is inconsistent with its execution nonce")
    if (
        run["stage"] != "media_preprocess"
        or run["status"] != "completed"
        or run["implementation_version"] != IMPLEMENTATION_VERSION
        or canonical_bytes(run["parameters_json"]) != canonical_bytes(recipe)
    ):
        raise PipelineError("prior processing run does not match the requested recipe")

    layout = result.get("layout")
    if not isinstance(layout, dict):
        raise PipelineError("prior result layout must be an object")
    require_exact_keys(
        layout,
        "prior result layout",
        {
            "output_root",
            "object_dir",
            "recipe_dir",
            "run_dir",
            "recipe_id",
            "recipe_sha256",
        },
    )
    prior_run_dir = recipe_dir / "executions" / run_id
    try:
        prior_run_stat = prior_run_dir.lstat()
    except FileNotFoundError as error:
        raise PipelineError("prior execution directory is missing") from error
    if stat.S_ISLNK(prior_run_stat.st_mode) or not stat.S_ISDIR(prior_run_stat.st_mode):
        raise PipelineError("prior execution directory must be a non-symlink directory")
    if (
        layout["recipe_sha256"] != recipe_sha256
        or layout["recipe_id"] != recipe_id
        or Path(layout["object_dir"]) != recipe_dir.parent.parent
        or Path(layout["output_root"]) != recipe_dir.parent.parent.parents[3]
        or Path(layout["recipe_dir"]) != recipe_dir
        or Path(layout["run_dir"]) != prior_run_dir
        or result["result_path"] != str(result_path)
        or result_path != prior_run_dir / "result.json"
    ):
        raise PipelineError("prior result layout or path is inconsistent")

    input_row = result.get("input")
    if not isinstance(input_row, dict):
        raise PipelineError("prior result input must be an object")
    require_exact_keys(
        input_row,
        "prior result input",
        {
            "path",
            "storage_uri",
            "media_id",
            "sha256",
            "byte_count",
            "stat_before",
            "stat_after",
            "unchanged",
            "catalog_observation",
        },
    )
    if (
        input_row.get("sha256") != source_sha256
        or input_row.get("media_id") != f"media_sha256_{source_sha256}"
        or input_row.get("byte_count") != source_byte_count
        or input_row.get("unchanged") is not True
        or input_row.get("stat_before") != input_row.get("stat_after")
    ):
        raise PipelineError("prior result input identity is inconsistent")
    for stat_name in ("stat_before", "stat_after"):
        file_stat = input_row.get(stat_name)
        if not isinstance(file_stat, dict):
            raise PipelineError(f"prior input {stat_name} must be an object")
        require_exact_keys(
            file_stat,
            f"prior input {stat_name}",
            {"device", "inode", "byte_count", "mtime_ns"},
        )
        if file_stat.get("byte_count") != source_byte_count:
            raise PipelineError(f"prior input {stat_name} byte count is inconsistent")

    specs = artifact_specs(
        prior_run_dir,
        work_order,
        video_index=video_index,
        audio_index=audio_index,
    )
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != len(specs):
        raise PipelineError("prior result has an incomplete artifact set")
    artifacts_by_kind: dict[str, dict[str, Any]] = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise PipelineError("prior artifact descriptor must be an object")
        require_exact_keys(
            artifact,
            "prior artifact descriptor",
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
        kind = artifact.get("artifact_kind")
        spec = specs.get(kind)
        if spec is None or kind in artifacts_by_kind:
            raise PipelineError("prior result has an unexpected or duplicate artifact")
        artifact_path = spec["path"].resolve()
        digest = artifact.get("sha256")
        byte_count = artifact.get("byte_count")
        if (
            artifact.get("processing_run_id") != run_id
            or artifact.get("path") != str(artifact_path)
            or artifact.get("storage_uri") != artifact_path.as_uri()
            or artifact.get("media_kind") != spec["media_kind"]
            or artifact.get("mime_type") != spec["mime_type"]
            or artifact.get("schema_version") != CONTRACT_VERSION
            or artifact.get("visibility") != "private"
            or not isinstance(digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count <= 0
        ):
            raise PipelineError(f"prior {kind} descriptor is inconsistent")
        if artifact.get("artifact_id") != artifact_id_for(run_id, kind, digest):
            raise PipelineError(f"prior {kind} artifact identity is inconsistent")
        checked_file_digest(
            artifact_path,
            label=f"prior {kind} artifact",
            expected_sha256=digest,
            expected_byte_count=byte_count,
        )
        normalized_probe = artifact.get("normalized_probe")
        if spec["normalized_probe"]:
            artifact_media_id = f"media_sha256_{digest}"
            fresh_probe, _ = probe_file(
                ffprobe,
                artifact_path,
                media_id=artifact_media_id,
                sha256=digest,
                ffprobe_version=ffprobe_version,
            )
            if canonical_bytes(normalized_probe) != canonical_bytes(fresh_probe):
                raise PipelineError(f"prior {kind} normalized probe is stale")
        elif normalized_probe is not None:
            raise PipelineError(f"prior {kind} must not carry a media probe")
        artifacts_by_kind[kind] = artifact

    probe_document = read_json_object(
        Path(artifacts_by_kind["ffprobe_normalized_json"]["path"]),
        "prior normalized source probe",
    )
    if (
        probe_document.get("media", {}).get("sha256") != source_sha256
        or probe_document.get("media", {}).get("media_id")
        != f"media_sha256_{source_sha256}"
        or probe_document.get("media", {}).get("byte_count") != source_byte_count
        or probe_document.get("tool", {}).get("version") != ffprobe_version
        or probe_document.get("format", {}).get("duration_ms") != source_duration_ms
        or probe_document.get("primary_streams", {}).get("video_index") != video_index
        or probe_document.get("primary_streams", {}).get("audio_index") != audio_index
    ):
        raise PipelineError("prior normalized source probe is stale or inconsistent")
    routing_artifact = artifacts_by_kind.get("scene_silence_routing_json")
    if routing_artifact is None:
        if result.get("routing") is not None:
            raise PipelineError("prior result has routing data without its artifact")
    else:
        routing_document = read_json_object(
            Path(routing_artifact["path"]), "prior routing artifact"
        )
        if canonical_bytes(routing_document) != canonical_bytes(result.get("routing")):
            raise PipelineError("prior routing artifact disagrees with the envelope")
        validate_routing_integrity(
            routing_document,
            expected_duration_ms=source_duration_ms,
            expected_has_video=video_index is not None,
            expected_has_audio=audio_index is not None,
            expected_source_media_id=f"media_sha256_{source_sha256}",
            expected_profile=work_order["profile"],
            label="prior routing artifact",
        )

    steps = result.get("steps")
    if not isinstance(steps, list) or len(steps) != len(OPERATIONS):
        raise PipelineError("prior result steps are incomplete")
    step_map = {step.get("name"): step for step in steps if isinstance(step, dict)}
    if set(step_map) != set(OPERATIONS):
        raise PipelineError("prior result does not contain each processing step once")
    expected_step_artifact = {
        "probe": "ffprobe_normalized_json",
        "audio_flac": "audio_16khz_mono_flac",
        "proxy": "low_resolution_cfr_proxy",
        "routing": "scene_silence_routing_json",
    }
    for name, kind in expected_step_artifact.items():
        step = step_map[name]
        require_exact_keys(step, f"prior {name} step", {"name", "status", "command", "output_path"})
        if kind in artifacts_by_kind:
            if step["status"] not in {"completed", "reused"}:
                raise PipelineError(f"prior {name} step is not complete")
            if step["output_path"] != artifacts_by_kind[kind]["path"]:
                raise PipelineError(f"prior {name} step points at the wrong artifact")
        elif step["status"] not in {"not_applicable", "disabled"} or step["output_path"] is not None:
            raise PipelineError(f"prior {name} disabled/not-applicable relationship is invalid")

    records = result.get("catalog_records")
    if not isinstance(records, dict):
        raise PipelineError("prior catalog handoff must be an object")
    require_exact_keys(
        records,
        "prior catalog handoff",
        {
            "processing_runs",
            "run_inputs",
            "media_objects",
            "media_locations",
            "media_derivations",
            "artifacts",
        },
    )
    if records["processing_runs"] != [run]:
        raise PipelineError("prior catalog run does not match its envelope")
    expected_run_input = {
        "run_input_id": stable_id(
            "run_input", run_id, f"media_sha256_{source_sha256}", "source_media"
        ),
        "processing_run_id": run_id,
        "object_type": "media",
        "object_id": f"media_sha256_{source_sha256}",
        "input_role": "source_media",
        "input_sha256": source_sha256,
    }
    if records["run_inputs"] != [expected_run_input]:
        raise PipelineError("prior catalog run input is inconsistent")
    catalog_artifacts = records.get("artifacts")
    expected_catalog_artifacts = [
        {
            key: artifact[key]
            for key in (
                "artifact_id",
                "processing_run_id",
                "artifact_kind",
                "storage_uri",
                "sha256",
                "byte_count",
                "schema_version",
                "visibility",
            )
        }
        for artifact in artifacts
    ]
    if canonical_bytes(catalog_artifacts) != canonical_bytes(expected_catalog_artifacts):
        raise PipelineError("prior catalog artifacts disagree with envelope artifacts")

    media_rows = records.get("media_objects")
    locations = records.get("media_locations")
    derivations = records.get("media_derivations")
    if not all(isinstance(value, list) for value in (media_rows, locations, derivations)):
        raise PipelineError("prior catalog media relationships are malformed")
    media_by_id = {
        row.get("media_id"): row for row in media_rows if isinstance(row, dict)
    }
    if len(media_by_id) != len(media_rows):
        raise PipelineError("prior catalog has duplicate or malformed media objects")
    source_media_id = f"media_sha256_{source_sha256}"
    source_media = media_by_id.get(source_media_id)
    if (
        not isinstance(source_media, dict)
        or source_media.get("sha256") != source_sha256
        or source_media.get("byte_count") != source_byte_count
        or canonical_bytes(source_media.get("ffprobe_json"))
        != canonical_bytes(probe_document)
    ):
        raise PipelineError("prior catalog source media is inconsistent")
    derived_ids: set[str] = set()
    for kind, artifact in artifacts_by_kind.items():
        if artifact.get("normalized_probe") is None:
            continue
        derived_id = f"media_sha256_{artifact['sha256']}"
        derived = media_by_id.get(derived_id)
        if (
            not isinstance(derived, dict)
            or derived.get("sha256") != artifact["sha256"]
            or derived.get("byte_count") != artifact["byte_count"]
            or derived.get("media_kind") != artifact["media_kind"]
            or derived.get("mime_type") != artifact["mime_type"]
            or canonical_bytes(derived.get("ffprobe_json"))
            != canonical_bytes(artifact["normalized_probe"])
        ):
            raise PipelineError(f"prior catalog media for {kind} is inconsistent")
        derived_ids.add(derived_id)
    if set(media_by_id) != {source_media_id, *derived_ids}:
        raise PipelineError("prior catalog media set does not match its artifacts")
    location_pairs = {
        (row.get("media_id"), row.get("storage_uri"))
        for row in locations
        if isinstance(row, dict)
    }
    expected_location_pairs = {
        (source_media_id, input_row.get("storage_uri")),
        *{
            (f"media_sha256_{artifact['sha256']}", artifact["storage_uri"])
            for artifact in artifacts_by_kind.values()
            if artifact.get("normalized_probe") is not None
        },
    }
    if location_pairs != expected_location_pairs or len(locations) != len(location_pairs):
        raise PipelineError("prior catalog media locations do not exactly match the run")
    for location in locations:
        if not isinstance(location, dict):
            raise PipelineError("prior catalog media location must be an object")
        require_exact_keys(
            location,
            "prior catalog media location",
            {
                "media_location_id",
                "media_id",
                "storage_uri",
                "storage_class",
                "verified_at",
                "is_primary",
            },
        )
        if location.get("media_location_id") != stable_id(
            "media_location", location.get("media_id"), location.get("storage_uri")
        ):
            raise PipelineError("prior catalog media_location_id is inconsistent")
        if location.get("is_primary") != 1:
            raise PipelineError("prior catalog media location must be primary")
    for artifact in artifacts_by_kind.values():
        if artifact.get("normalized_probe") is not None and (
            f"media_sha256_{artifact['sha256']}", artifact["storage_uri"]
        ) not in location_pairs:
            raise PipelineError("prior derived media is missing its artifact location")
    if len(derivations) != len(derived_ids):
        raise PipelineError("prior derivation count does not match derived media")
    for derivation in derivations:
        if (
            not isinstance(derivation, dict)
            or derivation.get("parent_media_id") != source_media_id
            or derivation.get("child_media_id") not in derived_ids
            or derivation.get("processing_run_id") != run_id
        ):
            raise PipelineError("prior media derivation relationship is inconsistent")
        require_exact_keys(
            derivation,
            "prior media derivation",
            {
                "child_media_id",
                "parent_media_id",
                "derivation_kind",
                "processing_run_id",
                "metadata_json",
            },
        )
    if {row["child_media_id"] for row in derivations} != derived_ids:
        raise PipelineError("prior derivations do not cover each derived artifact once")

    reuse = result.get("reuse")
    if not isinstance(reuse, dict):
        raise PipelineError("prior reuse lineage must be an object")
    require_exact_keys(
        reuse,
        "prior reuse lineage",
        {
            "mode",
            "prior_processing_run_id",
            "prior_result_path",
            "prior_result_sha256",
            "verified_at",
        },
    )
    if reuse.get("mode") == "none":
        if any(reuse.get(key) is not None for key in reuse if key != "mode"):
            raise PipelineError("initial prior result has inconsistent reuse lineage")
    elif reuse.get("mode") == "verified_prior_result":
        if not all(
            isinstance(reuse.get(key), str) and reuse.get(key)
            for key in (
                "prior_processing_run_id",
                "prior_result_path",
                "prior_result_sha256",
                "verified_at",
            )
        ):
            raise PipelineError("prior result has incomplete reuse lineage")
    else:
        raise PipelineError("prior result has an unsupported reuse mode")
    result["_result_sha256"] = result_digest
    result["_result_path"] = str(result_path)
    result["_artifacts_by_kind"] = artifacts_by_kind
    return result


def discover_reusable_result(
    executions_dir: Path,
    **validation_context: Any,
) -> dict[str, Any] | None:
    result_paths = sorted(executions_dir.glob("*/result.json"))
    validated = [
        validate_prior_result(path, **validation_context) for path in result_paths
    ]
    by_path = {item["_result_path"]: item for item in validated}
    lineage_edges: dict[str, str] = {}
    for item in validated:
        lineage = item["reuse"]
        if lineage["mode"] != "verified_prior_result":
            continue
        referenced = by_path.get(lineage["prior_result_path"])
        if (
            referenced is None
            or referenced["processing_run"]["processing_run_id"]
            != lineage["prior_processing_run_id"]
            or referenced["_result_sha256"] != lineage["prior_result_sha256"]
            or referenced is item
        ):
            raise PipelineError("prior result reuse lineage references an invalid envelope")
        try:
            verified_at = datetime.fromisoformat(
                lineage["verified_at"].replace("Z", "+00:00")
            )
            current_started = datetime.fromisoformat(
                item["processing_run"]["started_at"].replace("Z", "+00:00")
            )
            referenced_completed = datetime.fromisoformat(
                referenced["processing_run"]["completed_at"].replace("Z", "+00:00")
            )
        except (AttributeError, ValueError) as error:
            raise PipelineError("prior result reuse lineage has invalid timestamps") from error
        if not (
            referenced_completed <= verified_at
            and current_started <= verified_at
        ):
            raise PipelineError("prior result reuse lineage is not chronological")
        lineage_edges[item["_result_path"]] = referenced["_result_path"]
    for origin in lineage_edges:
        visited: set[str] = set()
        cursor = origin
        while cursor in lineage_edges:
            if cursor in visited:
                raise PipelineError("prior result reuse lineage contains a cycle")
            visited.add(cursor)
            cursor = lineage_edges[cursor]
    if not validated:
        return None
    return max(
        validated,
        key=lambda item: (
            item["processing_run"]["completed_at"],
            item["processing_run"]["processing_run_id"],
        ),
    )


def run_work_order(work_order: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    started_clock = time.monotonic()
    source = Path(work_order["source"]["path"])
    source_first_cataloged_at = (
        work_order["source"].get("first_cataloged_at") or started_at
    )
    source_catalog_time_basis = (
        "upstream_work_order"
        if work_order["source"].get("first_cataloged_at")
        else "preprocessing_observation"
    )
    output_root = Path(work_order["output"]["root"])
    stat_before = source_stat(source)
    source_sha256 = sha256_file(source)
    expected = work_order["source"].get("expected_sha256")
    if expected and expected != source_sha256:
        raise PipelineError(
            f"source SHA-256 mismatch: expected {expected}, observed {source_sha256}"
        )
    source_media_id = f"media_sha256_{source_sha256}"
    ffmpeg = require_tool("ffmpeg")
    ffprobe = require_tool("ffprobe")
    ffmpeg_provenance = executable_provenance(ffmpeg, "ffmpeg")
    ffprobe_provenance = executable_provenance(ffprobe, "ffprobe")
    ffprobe_version = ffprobe_provenance["version"]
    source_probe, probe_command_line = probe_file(
        ffprobe,
        source,
        media_id=source_media_id,
        sha256=source_sha256,
        ffprobe_version=ffprobe_version,
    )
    recipe, recipe_sha256 = work_recipe(
        work_order, ffmpeg_provenance, ffprobe_provenance
    )
    recipe_id = f"recipe_preprocess_{recipe_sha256[:32]}"
    execution_nonce = uuid.uuid4().hex
    processing_run_id = execution_run_id(
        source_sha256, recipe_sha256, execution_nonce
    )
    object_dir = (
        output_root
        / "media"
        / "sha256"
        / source_sha256[:2]
        / source_sha256
    )
    recipe_dir = object_dir / "recipes" / recipe_sha256
    executions_dir = recipe_dir / "executions"
    run_dir = executions_dir / processing_run_id
    specs = artifact_specs(
        run_dir,
        work_order,
        video_index=source_probe["primary_streams"]["video_index"],
        audio_index=source_probe["primary_streams"]["audio_index"],
    )
    probe_path = specs["ffprobe_normalized_json"]["path"]
    routing_path = run_dir / "routing.json"
    audio_path = run_dir / "artifacts" / "audio-16khz-mono.flac"
    proxy_path = run_dir / "artifacts" / (
        f"proxy-{work_order['profile']['proxy_width']}x"
        f"{work_order['profile']['proxy_height']}-"
        f"{work_order['profile']['proxy_fps']}fps.mp4"
    )
    video_index = source_probe["primary_streams"]["video_index"]
    audio_index = source_probe["primary_streams"]["audio_index"]
    planned_audio_command = (
        audio_command(
            ffmpeg,
            source,
            audio_path,
            audio_index,
            work_order["profile"],
        )
        if audio_index is not None and work_order["operations"]["audio_flac"]
        else None
    )
    planned_proxy_command = (
        proxy_command(
            ffmpeg,
            source,
            proxy_path,
            video_index,
            audio_index,
            work_order["profile"],
        )
        if video_index is not None and work_order["operations"]["proxy"]
        else None
    )
    planned_routing_command = (
        routing_command(
            ffmpeg,
            source,
            video_index,
            audio_index,
            work_order["profile"],
        )
        if work_order["operations"]["routing"]
        else None
    )
    result: dict[str, Any] = {
        "schema_version": CONTRACT_VERSION,
        "job_id": work_order["job_id"],
        "status": "planned" if dry_run else "running",
        "dry_run": dry_run,
        "processing_run": {
            "processing_run_id": processing_run_id,
            "stage": "media_preprocess",
            "implementation_version": IMPLEMENTATION_VERSION,
            "parameters_json": recipe,
            "environment_json": {
                "platform": sys.platform,
                "python": sys.version.split()[0],
                "cpu_only": True,
                "execution_nonce": execution_nonce,
                "tool_paths": {
                    "ffmpeg": ffmpeg_provenance["path"],
                    "ffprobe": ffprobe_provenance["path"],
                },
            },
            "started_at": started_at,
            "completed_at": None,
            "status": "queued" if dry_run else "running",
        },
        "input": {
            "path": str(source),
            "storage_uri": source.as_uri(),
            "media_id": source_media_id,
            "sha256": source_sha256,
            "byte_count": stat_before["byte_count"],
            "stat_before": stat_before,
            "stat_after": None,
            "unchanged": None,
            "catalog_observation": {
                "first_cataloged_at": source_first_cataloged_at,
                "basis": source_catalog_time_basis,
                "acquisition_timestamp_state": "not_claimed_by_preprocessing",
            },
        },
        "layout": {
            "output_root": str(output_root),
            "object_dir": str(object_dir),
            "recipe_dir": str(recipe_dir),
            "run_dir": str(run_dir),
            "recipe_id": recipe_id,
            "recipe_sha256": recipe_sha256,
        },
        "steps": [
            {
                "name": "probe",
                "status": "completed_read_only",
                "command": probe_command_line,
                "output_path": str(probe_path),
            },
            {
                "name": "audio_flac",
                "status": (
                    "planned"
                    if planned_audio_command
                    else "not_applicable" if audio_index is None else "disabled"
                ),
                "command": planned_audio_command,
                "output_path": str(audio_path) if planned_audio_command else None,
            },
            {
                "name": "proxy",
                "status": (
                    "planned"
                    if planned_proxy_command
                    else "not_applicable" if video_index is None else "disabled"
                ),
                "command": planned_proxy_command,
                "output_path": str(proxy_path) if planned_proxy_command else None,
            },
            {
                "name": "routing",
                "status": (
                    "planned"
                    if planned_routing_command
                    else "not_applicable"
                    if video_index is None and audio_index is None
                    else "disabled"
                ),
                "command": planned_routing_command,
                "output_path": str(routing_path) if planned_routing_command else None,
            },
        ],
        "artifacts": [],
        "routing": None,
        "reuse": {
            "mode": "none",
            "prior_processing_run_id": None,
            "prior_result_path": None,
            "prior_result_sha256": None,
            "verified_at": None,
        },
        "catalog_records": None,
        "errors": [],
    }
    if dry_run:
        verify_executable_provenance(ffmpeg_provenance)
        verify_executable_provenance(ffprobe_provenance)
        completed_at = utc_now()
        result["processing_run"].update(
            {"completed_at": completed_at, "status": "queued"}
        )
        result["duration_ms"] = round((time.monotonic() - started_clock) * 1_000)
        return result

    with recipe_writer_lock(recipe_dir):
        prior = discover_reusable_result(
            executions_dir,
            source_sha256=source_sha256,
            source_byte_count=stat_before["byte_count"],
            recipe=recipe,
            recipe_sha256=recipe_sha256,
            recipe_id=recipe_id,
            recipe_dir=recipe_dir,
            work_order=work_order,
            ffprobe=ffprobe,
            ffprobe_version=ffprobe_version,
            source_duration_ms=source_probe["format"]["duration_ms"],
            video_index=video_index,
            audio_index=audio_index,
        )
        run_dir.mkdir(parents=True, exist_ok=False)
        if prior is not None:
            prior_artifacts = prior["_artifacts_by_kind"]
            for kind, spec in specs.items():
                hardlink_immutable(
                    Path(prior_artifacts[kind]["path"]),
                    spec["path"],
                    f"verified prior {kind} artifact",
                )
            verified_at = utc_now()
            result["reuse"] = {
                "mode": "verified_prior_result",
                "prior_processing_run_id": prior["processing_run"]["processing_run_id"],
                "prior_result_path": prior["_result_path"],
                "prior_result_sha256": prior["_result_sha256"],
                "verified_at": verified_at,
            }
            for step in result["steps"]:
                if step["output_path"] is not None:
                    step["status"] = "reused"
        else:
            atomic_write_json_immutable(probe_path, source_probe)
            result["steps"][0]["status"] = "completed"
            if planned_audio_command:
                command = create_media_artifact(
                    audio_path,
                    lambda target: audio_command(
                        ffmpeg,
                        source,
                        target,
                        audio_index,
                        work_order["profile"],
                    ),
                )
                result["steps"][1].update({"status": "completed", "command": command})
            if planned_proxy_command:
                command = create_media_artifact(
                    proxy_path,
                    lambda target: proxy_command(
                        ffmpeg,
                        source,
                        target,
                        video_index,
                        audio_index,
                        work_order["profile"],
                    ),
                )
                result["steps"][2].update({"status": "completed", "command": command})
            if planned_routing_command:
                completed = run_command(planned_routing_command)
                routing = parse_routing_log(
                    completed.stderr,
                    duration_ms=source_probe["format"]["duration_ms"],
                    has_video=video_index is not None,
                    has_audio=audio_index is not None,
                    profile=work_order["profile"],
                )
                routing["source_media_id"] = source_media_id
                validate_routing_integrity(
                    routing,
                    expected_duration_ms=source_probe["format"]["duration_ms"],
                    expected_has_video=video_index is not None,
                    expected_has_audio=audio_index is not None,
                    expected_source_media_id=source_media_id,
                    expected_profile=work_order["profile"],
                    label="generated routing",
                )
                atomic_write_json_immutable(routing_path, routing)
                result["steps"][3]["status"] = "completed"

        catalog_source_probe = json.loads(probe_path.read_text(encoding="utf-8"))
        result["artifacts"].append(
            artifact_descriptor(
                kind="ffprobe_normalized_json",
                path=probe_path,
                processing_run_id=processing_run_id,
                media_kind="document",
                mime_type="application/json",
            )
        )
        derived_media: list[dict[str, Any]] = []
        derivations: list[dict[str, Any]] = []
        if "audio_16khz_mono_flac" in specs:
            audio_sha256 = sha256_file(audio_path)
            audio_media_id = f"media_sha256_{audio_sha256}"
            audio_probe, _ = probe_file(
                ffprobe,
                audio_path,
                media_id=audio_media_id,
                sha256=audio_sha256,
                ffprobe_version=ffprobe_version,
            )
            validate_audio_artifact(audio_probe, work_order["profile"])
            descriptor = artifact_descriptor(
                kind="audio_16khz_mono_flac",
                path=audio_path,
                processing_run_id=processing_run_id,
                media_kind="audio",
                mime_type="audio/flac",
                normalized_probe=audio_probe,
            )
            result["artifacts"].append(descriptor)
            derived_media.append(
                media_object_from_probe(
                    digest=descriptor["sha256"],
                    byte_count=descriptor["byte_count"],
                    media_kind="audio",
                    mime_type="audio/flac",
                    probe=audio_probe,
                    first_cataloged_at=started_at,
                )
            )
            derivations.append(
                {
                    "child_media_id": audio_media_id,
                    "parent_media_id": source_media_id,
                    "derivation_kind": "audio_normalization_16khz_mono_flac",
                    "processing_run_id": processing_run_id,
                    "metadata_json": {
                        "sample_rate_hz": work_order["profile"]["audio_sample_rate_hz"],
                        "channels": work_order["profile"]["audio_channels"],
                        "sample_format": work_order["profile"]["audio_sample_format"],
                    },
                }
            )
        if "low_resolution_cfr_proxy" in specs:
            proxy_sha256 = sha256_file(proxy_path)
            proxy_media_id = f"media_sha256_{proxy_sha256}"
            proxy_probe, _ = probe_file(
                ffprobe,
                proxy_path,
                media_id=proxy_media_id,
                sha256=proxy_sha256,
                ffprobe_version=ffprobe_version,
            )
            validate_proxy_artifact(proxy_probe, work_order["profile"])
            descriptor = artifact_descriptor(
                kind="low_resolution_cfr_proxy",
                path=proxy_path,
                processing_run_id=processing_run_id,
                media_kind="video",
                mime_type="video/mp4",
                normalized_probe=proxy_probe,
            )
            result["artifacts"].append(descriptor)
            derived_media.append(
                media_object_from_probe(
                    digest=descriptor["sha256"],
                    byte_count=descriptor["byte_count"],
                    media_kind="video",
                    mime_type="video/mp4",
                    probe=proxy_probe,
                    first_cataloged_at=started_at,
                )
            )
            derivations.append(
                {
                    "child_media_id": proxy_media_id,
                    "parent_media_id": source_media_id,
                    "derivation_kind": "low_resolution_cfr_proxy",
                    "processing_run_id": processing_run_id,
                    "metadata_json": {
                        "width": work_order["profile"]["proxy_width"],
                        "height": work_order["profile"]["proxy_height"],
                        "fps": work_order["profile"]["proxy_fps"],
                    },
                }
            )
        if "scene_silence_routing_json" in specs:
            routing = json.loads(routing_path.read_text(encoding="utf-8"))
            result["routing"] = routing
            result["artifacts"].append(
                artifact_descriptor(
                    kind="scene_silence_routing_json",
                    path=routing_path,
                    processing_run_id=processing_run_id,
                    media_kind="document",
                    mime_type="application/json",
                )
            )

        stat_before_final_hash = source_stat(source)
        final_source_sha256 = sha256_file(source)
        stat_after = source_stat(source)
        unchanged = stat_before_final_hash == stat_before == stat_after
        if not unchanged or final_source_sha256 != source_sha256:
            raise PipelineError("source file changed while preprocessing")
        verify_executable_provenance(ffmpeg_provenance)
        verify_executable_provenance(ffprobe_provenance)
        completed_at = utc_now()
        source_mime = mimetypes.guess_type(source.name)[0]
        source_media = media_object_from_probe(
            digest=source_sha256,
            byte_count=stat_before["byte_count"],
            media_kind=media_kind(catalog_source_probe),
            mime_type=source_mime,
            probe=catalog_source_probe,
            first_cataloged_at=source_first_cataloged_at,
        )
        result["input"].update({"stat_after": stat_after, "unchanged": unchanged})
        result["processing_run"].update(
            {"completed_at": completed_at, "status": "completed"}
        )
        result["catalog_records"] = {
            "processing_runs": [result["processing_run"]],
            "run_inputs": [
                {
                    "run_input_id": stable_id(
                        "run_input", processing_run_id, source_media_id, "source_media"
                    ),
                    "processing_run_id": processing_run_id,
                    "object_type": "media",
                    "object_id": source_media_id,
                    "input_role": "source_media",
                    "input_sha256": source_sha256,
                }
            ],
            "media_objects": [source_media, *derived_media],
            "media_locations": [
                {
                    "media_location_id": stable_id(
                        "media_location", source_media_id, source.as_uri()
                    ),
                    "media_id": source_media_id,
                    "storage_uri": source.as_uri(),
                    "storage_class": "local",
                    "verified_at": completed_at,
                    "is_primary": 1,
                },
                *[
                    {
                        "media_location_id": stable_id(
                            "media_location",
                            item["media_id"],
                            next(
                                artifact["storage_uri"]
                                for artifact in result["artifacts"]
                                if artifact["sha256"] == item["sha256"]
                            ),
                        ),
                        "media_id": item["media_id"],
                        "storage_uri": next(
                            artifact["storage_uri"]
                            for artifact in result["artifacts"]
                            if artifact["sha256"] == item["sha256"]
                        ),
                        "storage_class": "local_derived",
                        "verified_at": completed_at,
                        "is_primary": 1,
                    }
                    for item in derived_media
                ],
            ],
            "media_derivations": derivations,
            "artifacts": [
                {
                    key: artifact[key]
                    for key in (
                        "artifact_id",
                        "processing_run_id",
                        "artifact_kind",
                        "storage_uri",
                        "sha256",
                        "byte_count",
                        "schema_version",
                        "visibility",
                    )
                }
                for artifact in result["artifacts"]
            ],
        }
        result["status"] = "completed"
        result["duration_ms"] = round((time.monotonic() - started_clock) * 1_000)
        result_path = run_dir / "result.json"
        result["result_path"] = str(result_path)
        atomic_write_json_immutable(result_path, result)
        return result


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise PipelineError(f"Invalid JSON in {path}: {error}") from error


def create_work_order(args: argparse.Namespace) -> dict[str, Any]:
    source = absolute_local_path(args.source, "--source", must_exist=True)
    output_root = absolute_local_path(args.output_root, "--output-root", must_exist=False)
    validate_output_root(output_root)
    profile_path = Path(args.profile).expanduser().resolve(strict=True)
    profile = validate_profile(load_json(profile_path))
    enabled = set(args.operations.split(","))
    unknown = sorted(enabled - set(OPERATIONS))
    if unknown:
        raise PipelineError(f"Unknown operations: {', '.join(unknown)}")
    raw = {
        "schema_version": CONTRACT_VERSION,
        "job_id": args.job_id,
        "source": {
            "path": str(source),
            "expected_sha256": args.expected_sha256,
            "first_cataloged_at": args.source_first_cataloged_at,
        },
        "output": {"root": str(output_root)},
        "operations": {operation: operation in enabled for operation in OPERATIONS},
        "profile": profile,
    }
    return validate_work_order(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline, content-addressed HIMR media preprocessing"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser(
        "create-work-order", help="print a validated work order to stdout"
    )
    create.add_argument("--job-id", required=True)
    create.add_argument("--source", required=True)
    create.add_argument("--output-root", required=True)
    create.add_argument("--expected-sha256")
    create.add_argument("--source-first-cataloged-at")
    create.add_argument("--profile", default=str(DEFAULT_PROFILE))
    create.add_argument("--operations", default=",".join(OPERATIONS))

    validate = subparsers.add_parser("validate", help="validate a work order")
    validate.add_argument("--work-order", required=True)

    run = subparsers.add_parser("run", help="execute a validated work order")
    run.add_argument("--work-order", required=True)
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="hash and probe the source, print commands, and write nothing",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "create-work-order":
            result = create_work_order(args)
        else:
            work_order_path = absolute_local_path(
                args.work_order, "--work-order", must_exist=True
            )
            result = validate_work_order(load_json(work_order_path))
            if args.command == "run":
                result = run_work_order(result, dry_run=args.dry_run)
        sys.stdout.write(pretty_json(result))
        return 0
    except (PipelineError, OSError) as error:
        failure = {
            "schema_version": CONTRACT_VERSION,
            "status": "failed",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
        sys.stderr.write(pretty_json(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
