#!/usr/bin/env python3
"""Hash-bound, resumable local windows for long recordings.

Materialization consumes one completed guarded-acquisition result and emits an
immutable bundle of independent half-open source-time work orders.  Execution
transcodes only local, admitted bytes into analysis audio and a CFR proxy.  It never
downloads, publishes, identifies a person, or treats a transcoded window as original
source evidence.
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
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_WINDOWS = 256
MONITOR_INTERVAL_SECONDS = 0.05


class WindowError(RuntimeError):
    pass


class CommandError(WindowError):
    def __init__(self, command: list[str], returncode: int, stderr: str):
        super().__init__(
            f"command failed with exit code {returncode}: {command!r}; "
            f"stderr={stderr[-4000:]}"
        )


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
        raise WindowError(f"value cannot be encoded canonically: {error}") from error


def pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: str) -> str:
    return f"{prefix}_" + hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:32]


def exact_object(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WindowError(f"{label} must be a JSON object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise WindowError(
            f"{label} keys differ from the exact contract; missing={missing}, unknown={unknown}"
        )
    return value


def positive_integer(value: Any, label: str, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WindowError(f"{label} must be a positive integer")
    if maximum is not None and value > maximum:
        raise WindowError(f"{label} may not exceed {maximum}")
    return value


def nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WindowError(f"{label} must be a non-negative integer")
    return value


def required_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise WindowError(f"{label} must be a lowercase SHA-256")
    return value


def absolute_path(value: Any, label: str, *, must_exist: bool) -> Path:
    if not isinstance(value, str) or not value or "://" in value:
        raise WindowError(f"{label} must be a non-empty local path")
    path = Path(value)
    if not path.is_absolute():
        raise WindowError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=must_exist)
    except (OSError, RuntimeError) as error:
        raise WindowError(f"{label} does not exist or is unsafe: {path}") from error
    if must_exist and (resolved.is_symlink() or not resolved.is_file()):
        raise WindowError(f"{label} must identify a regular file")
    return resolved


def validate_output_root(path: Path, label: str) -> None:
    if path == Path("/"):
        raise WindowError(f"{label} may not be the filesystem root")
    for forbidden in (Path("/tmp"), Path("/var/tmp")):
        if path == forbidden or forbidden in path.parents:
            raise WindowError(f"{label} may not be under {forbidden}")
    if path.exists() and (path.is_symlink() or not path.is_dir()):
        raise WindowError(f"{label} must be a real directory or a new path")


def reject_constant(value: str) -> None:
    raise WindowError(f"non-finite JSON constant is forbidden: {value}")


def reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise WindowError(f"JSON object contains duplicate key: {key}")
        value[key] = item
    return value


def load_json(path: Path, label: str) -> tuple[Any, bytes]:
    before = path.stat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_JSON_BYTES:
        raise WindowError(f"{label} is not a bounded regular file")
    body = path.read_bytes()
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise WindowError(f"{label} changed while being read")
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise WindowError(f"cannot parse {label}: {error}") from error
    return value, body


def file_stat(path: Path) -> dict[str, int]:
    value = path.stat()
    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "byte_count": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def stable_file_hash(path: Path, expected: str | None, label: str) -> tuple[str, dict[str, int]]:
    before = file_stat(path)
    observed = sha256_file(path)
    after = file_stat(path)
    if before != after:
        raise WindowError(f"{label} changed while being hashed")
    if expected is not None and observed != expected:
        raise WindowError(f"{label} SHA-256 does not match its pin")
    return observed, after


def subprocess_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.update({"LC_ALL": "C", "LANG": "C", "TZ": "UTC"})
    return environment


def run_capture(command: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        env=subprocess_environment(),
        check=False,
    )
    if completed.returncode:
        raise CommandError(command, completed.returncode, completed.stderr)
    return completed


def inspect_tool(path_text: str, expected_sha256: str, label: str) -> dict[str, Any]:
    path = absolute_path(path_text, label, must_exist=True)
    if not os.access(path, os.X_OK):
        raise WindowError(f"{label} must be executable")
    digest, observed_stat = stable_file_hash(path, expected_sha256, label)
    version = run_capture([str(path), "-version"]).stdout.strip()
    if not version:
        raise WindowError(f"{label} returned empty version output")
    digest_after, stat_after = stable_file_hash(path, digest, label)
    if digest_after != digest or stat_after != observed_stat:
        raise WindowError(f"{label} changed during inspection")
    return {
        "path": str(path),
        "sha256": digest,
        "byte_count": observed_stat["byte_count"],
        "version_output_sha256": sha256_bytes(version.encode("utf-8")),
        "version_first_line": version.splitlines()[0][:1000],
    }


def probe_raw(ffprobe: Path, source: Path) -> tuple[dict[str, Any], list[str]]:
    command = [
        str(ffprobe),
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-of",
        "json",
        str(source),
    ]
    completed = run_capture(command)
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise WindowError("ffprobe returned invalid JSON") from error
    if not isinstance(value, dict):
        raise WindowError("ffprobe result must be an object")
    return value, command


def milliseconds(value: Any) -> int | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return round(number * 1000)


def probe_summary(raw: dict[str, Any]) -> dict[str, Any]:
    raw_format = raw.get("format") if isinstance(raw.get("format"), dict) else {}
    streams = [item for item in raw.get("streams", []) if isinstance(item, dict)]
    duration_ms = milliseconds(raw_format.get("duration"))
    if duration_ms is None:
        duration_ms = max(
            (duration for item in streams if (duration := milliseconds(item.get("duration"))) is not None),
            default=None,
        )
    video = next(
        (item for item in streams if item.get("codec_type") == "video" and not (item.get("disposition") or {}).get("attached_pic")),
        None,
    )
    audio = next((item for item in streams if item.get("codec_type") == "audio"), None)
    return {
        "duration_ms": duration_ms,
        "video_stream_index": None if video is None else video.get("index"),
        "audio_stream_index": None if audio is None else audio.get("index"),
    }


def validate_acquisition_result(value: Any, body: bytes) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WindowError("acquisition result must be a JSON object")
    required_top = {
        "schema_version", "job_id", "adapter", "status", "dry_run", "reused",
        "work_order_sha256", "started_at", "completed_at", "duration_ms", "source",
        "limits", "capacity_before", "capacity_after", "commands", "source_observation",
        "selected_remote_metadata", "admission", "catalog_records", "result_path", "errors",
    }
    missing = sorted(required_top - set(value))
    unknown = sorted(set(value) - required_top - {"handling_policy"})
    if missing or unknown:
        raise WindowError(
            "acquisition result keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    if value["schema_version"] != 1 or value["status"] != "completed":
        raise WindowError("acquisition result is not a completed version-1 result")
    if value["dry_run"] is not False or value["errors"] != []:
        raise WindowError("planned or failed acquisition results cannot materialize windows")
    if value["adapter"] not in {"local_file", "direct_http", "yt_dlp"}:
        raise WindowError("acquisition result adapter is unsupported")
    if "handling_policy" in value:
        policy = exact_object(
            value["handling_policy"],
            "acquisition result.handling_policy",
            {
                "storage_scope",
                "publication_disposition",
                "publication_authority",
                "basis",
            },
        )
        if value["adapter"] != "local_file":
            raise WindowError("handling_policy is supported only for local_file results")
        if policy["storage_scope"] != "private_canonical_cache":
            raise WindowError("handling_policy.storage_scope is unsupported")
        if policy["publication_disposition"] not in {
            "no_publication_authority",
            "never_publish",
        }:
            raise WindowError("handling_policy.publication_disposition is unsupported")
        if policy["publication_authority"] != "none":
            raise WindowError("handling_policy.publication_authority must be none")
        basis = policy["basis"]
        if (
            not isinstance(basis, str)
            or not basis.strip()
            or len(basis) > 1_000
            or "\x00" in basis
        ):
            raise WindowError("handling_policy.basis must be non-empty bounded text")
    source = exact_object(
        value["source"],
        "acquisition result.source",
        {"platform", "source_kind", "native_id", "canonical_url", "title", "published_at", "access_state"},
    )
    required_access = "unknown" if value["adapter"] == "local_file" else "public"
    if source["access_state"] != required_access:
        raise WindowError(
            f"{value['adapter']} acquisition source.access_state must be {required_access}"
        )
    admission = exact_object(
        value["admission"],
        "acquisition result.admission",
        {"media_id", "sha256", "byte_count", "path", "storage_uri", "normalized_probe"},
    )
    digest = required_sha256(admission["sha256"], "acquisition result admission.sha256")
    byte_count = positive_integer(admission["byte_count"], "acquisition result admission.byte_count")
    if admission["media_id"] != f"media_sha256_{digest}":
        raise WindowError("acquisition result media_id is inconsistent")
    path = absolute_path(admission["path"], "acquisition result admission.path", must_exist=True)
    observed, source_stat = stable_file_hash(path, digest, "acquired parent media")
    if observed != digest or source_stat["byte_count"] != byte_count:
        raise WindowError("acquired parent media identity differs from admission")
    normalized_probe = admission["normalized_probe"]
    if not isinstance(normalized_probe, dict):
        raise WindowError("acquisition normalized_probe must be an object")
    acquisition_duration = (
        normalized_probe.get("format", {}).get("duration_ms")
        if isinstance(normalized_probe.get("format"), dict)
        else None
    )
    positive_integer(acquisition_duration, "acquisition normalized duration_ms")
    return {
        "result_sha256": sha256_bytes(body),
        "result_path": value["result_path"],
        "source": source,
        "media_id": admission["media_id"],
        "sha256": digest,
        "byte_count": byte_count,
        "path": str(path),
        "duration_ms": acquisition_duration,
        "stat": source_stat,
    }


def validate_work_order(value: Any) -> dict[str, Any]:
    order = exact_object(
        value,
        "local-window work order",
        {"schema_version", "job_id", "bundle_id", "source", "window", "tools", "profile", "limits", "output", "safety"},
    )
    if order["schema_version"] != 1:
        raise WindowError("work order schema_version must equal 1")
    if not isinstance(order["job_id"], str) or not IDENTIFIER.fullmatch(order["job_id"]):
        raise WindowError("work order job_id is invalid")
    if not isinstance(order["bundle_id"], str) or not re.fullmatch(r"windowbundle_[0-9a-f]{32}", order["bundle_id"]):
        raise WindowError("work order bundle_id is invalid")
    source = exact_object(
        order["source"], "work order.source",
        {"path", "expected_sha256", "byte_count", "media_id", "duration_ms", "acquisition_result_path", "acquisition_result_sha256"},
    )
    source_path = absolute_path(source["path"], "work order source.path", must_exist=True)
    digest = required_sha256(source["expected_sha256"], "work order source.expected_sha256")
    if source["media_id"] != f"media_sha256_{digest}":
        raise WindowError("work order source.media_id is inconsistent")
    positive_integer(source["byte_count"], "work order source.byte_count")
    duration_ms = positive_integer(source["duration_ms"], "work order source.duration_ms")
    absolute_path(source["acquisition_result_path"], "work order acquisition result path", must_exist=True)
    required_sha256(source["acquisition_result_sha256"], "work order acquisition result SHA-256")
    window = exact_object(
        order["window"], "work order.window",
        {"window_id", "ordinal", "start_ms", "end_ms", "boundary", "is_partial_tail"},
    )
    if not isinstance(window["window_id"], str) or not re.fullmatch(r"window_[0-9]{6}", window["window_id"]):
        raise WindowError("window.window_id is invalid")
    ordinal = positive_integer(window["ordinal"], "window.ordinal", MAX_WINDOWS)
    if window["window_id"] != f"window_{ordinal:06d}":
        raise WindowError("window_id does not match ordinal")
    start = nonnegative_integer(window["start_ms"], "window.start_ms")
    end = positive_integer(window["end_ms"], "window.end_ms")
    if start >= end or end > duration_ms:
        raise WindowError("window must be a non-empty interval within source duration")
    if window["boundary"] != "half_open" or not isinstance(window["is_partial_tail"], bool):
        raise WindowError("window boundary/tail marker is invalid")
    tools = exact_object(order["tools"], "work order.tools", {"ffmpeg", "ffprobe"})
    for name in ("ffmpeg", "ffprobe"):
        tool = exact_object(
            tools[name], f"work order.tools.{name}",
            {"path", "sha256", "byte_count", "version_output_sha256", "version_first_line"},
        )
        absolute_path(tool["path"], f"work order {name} path", must_exist=True)
        required_sha256(tool["sha256"], f"work order {name} SHA-256")
        positive_integer(tool["byte_count"], f"work order {name} byte_count")
        required_sha256(tool["version_output_sha256"], f"work order {name} version SHA-256")
        if not isinstance(tool["version_first_line"], str) or not tool["version_first_line"]:
            raise WindowError(f"work order {name} version_first_line is invalid")
    profile = exact_object(
        order["profile"], "work order.profile",
        {"profile_id", "ffmpeg_threads", "audio_sample_rate_hz", "audio_channels", "audio_sample_format", "flac_compression_level", "proxy_width", "proxy_height", "proxy_fps", "proxy_video_codec", "proxy_preset", "proxy_crf", "proxy_audio_codec", "proxy_audio_bitrate", "video_stream_index", "audio_stream_index"},
    )
    if profile["profile_id"] != "long-window-cpu-v1":
        raise WindowError("unsupported profile_id")
    if profile["audio_sample_rate_hz"] != 16000 or profile["audio_channels"] != 1 or profile["audio_sample_format"] != "s16":
        raise WindowError("audio normalization must be 16 kHz mono s16")
    for key, minimum, maximum in (
        ("ffmpeg_threads", 1, 32), ("flac_compression_level", 0, 12),
        ("proxy_width", 2, 1280), ("proxy_height", 2, 1280),
        ("proxy_fps", 1, 60), ("proxy_crf", 0, 51),
    ):
        value = profile[key]
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
            raise WindowError(f"profile.{key} is outside its allowed range")
    if profile["proxy_width"] % 2 or profile["proxy_height"] % 2 or profile["proxy_width"] * profile["proxy_height"] > 1280 * 720:
        raise WindowError("proxy dimensions must be even and no larger than 1280x720")
    for key in ("proxy_video_codec", "proxy_preset", "proxy_audio_codec", "proxy_audio_bitrate"):
        if not isinstance(profile[key], str) or not profile[key] or len(profile[key]) > 100:
            raise WindowError(f"profile.{key} is invalid")
    indexes = (profile["video_stream_index"], profile["audio_stream_index"])
    if all(index is None for index in indexes):
        raise WindowError("at least one primary stream is required")
    for index in indexes:
        if index is not None and (isinstance(index, bool) or not isinstance(index, int) or index < 0):
            raise WindowError("primary stream indexes must be null or non-negative integers")
    limits = exact_object(
        order["limits"], "work order.limits",
        {"max_window_output_bytes", "free_space_floor_bytes", "timeout_seconds"},
    )
    positive_integer(limits["max_window_output_bytes"], "limits.max_window_output_bytes")
    nonnegative_integer(limits["free_space_floor_bytes"], "limits.free_space_floor_bytes")
    positive_integer(limits["timeout_seconds"], "limits.timeout_seconds", 86400)
    output = exact_object(order["output"], "work order.output", {"root"})
    output_root = absolute_path(output["root"], "work order output.root", must_exist=False)
    validate_output_root(output_root, "work order output.root")
    safety = exact_object(
        order["safety"], "work order.safety",
        {"network_allowed", "credentials_allowed", "publication_authority", "identity_claims_allowed", "source_bytes_preserved", "remote_section_download"},
    )
    if safety != {
        "network_allowed": False,
        "credentials_allowed": False,
        "publication_authority": "none",
        "identity_claims_allowed": False,
        "source_bytes_preserved": True,
        "remote_section_download": False,
    }:
        raise WindowError("work order safety policy is not the required fail-closed policy")
    normalized = json.loads(json.dumps(order))
    normalized["source"]["path"] = str(source_path)
    normalized["output"]["root"] = str(output_root)
    return normalized


def seconds_text(milliseconds_value: int) -> str:
    return f"{milliseconds_value // 1000}.{milliseconds_value % 1000:03d}"


def tool_from_order(value: dict[str, Any], label: str) -> dict[str, Any]:
    inspected = inspect_tool(value["path"], value["sha256"], label)
    if inspected != value:
        raise WindowError(f"{label} build/version differs from the work-order pin")
    return inspected


def base_ffmpeg(tool: dict[str, Any], threads: int) -> list[str]:
    return [
        tool["path"], "-hide_banner", "-nostdin", "-nostats", "-loglevel", "error",
        "-threads", str(threads),
    ]


def artifact_commands(order: dict[str, Any], stage: Path) -> list[dict[str, Any]]:
    profile = order["profile"]
    window = order["window"]
    duration = window["end_ms"] - window["start_ms"]
    prefix = base_ffmpeg(order["tools"]["ffmpeg"], profile["ffmpeg_threads"]) + [
        "-ss", seconds_text(window["start_ms"]), "-accurate_seek", "-i", order["source"]["path"],
        "-t", seconds_text(duration),
    ]
    commands: list[dict[str, Any]] = []
    audio_index = profile["audio_stream_index"]
    video_index = profile["video_stream_index"]
    if audio_index is not None:
        audio_path = stage / "audio-16khz-mono.flac"
        command = prefix + [
            "-map", f"0:{audio_index}", "-vn", "-sn", "-dn", "-map_metadata", "-1",
            "-map_chapters", "-1", "-af",
            f"atrim=start=0:end={seconds_text(duration)},asetpts=PTS-STARTPTS,aresample=16000",
            "-ac", "1", "-ar", "16000", "-sample_fmt", "s16", "-c:a", "flac",
            "-compression_level", str(profile["flac_compression_level"]), str(audio_path),
        ]
        commands.append({"artifact_kind": "window_audio_16khz_mono_flac", "path": str(audio_path), "command": command})
    if video_index is not None:
        proxy_path = stage / "proxy-640x360-25fps.mp4"
        width, height, fps = profile["proxy_width"], profile["proxy_height"], profile["proxy_fps"]
        video_filter = (
            f"trim=start=0:end={seconds_text(duration)},setpts=PTS-STARTPTS,"
            f"scale=w={width}:h={height}:force_original_aspect_ratio=decrease:force_divisible_by=2,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps={fps}"
        )
        command = prefix + ["-map", f"0:{video_index}"]
        if audio_index is not None:
            command += ["-map", f"0:{audio_index}"]
        command += [
            "-sn", "-dn", "-map_metadata", "-1", "-map_chapters", "-1",
            "-vf", video_filter, "-c:v", profile["proxy_video_codec"], "-preset",
            profile["proxy_preset"], "-crf", str(profile["proxy_crf"]), "-pix_fmt",
            "yuv420p", "-fps_mode", "cfr",
        ]
        if audio_index is None:
            command += ["-an"]
        else:
            command += [
                "-af", f"atrim=start=0:end={seconds_text(duration)},asetpts=PTS-STARTPTS,aresample=48000",
                "-c:a", profile["proxy_audio_codec"], "-b:a", profile["proxy_audio_bitrate"],
                "-ar", "48000", "-ac", "2",
            ]
        command += ["-movflags", "+faststart", "-avoid_negative_ts", "make_zero", str(proxy_path)]
        commands.append({"artifact_kind": "window_low_resolution_cfr_proxy", "path": str(proxy_path), "command": command})
    return commands


def create_windows(duration_ms: int, chunk_duration_ms: int, max_windows: int) -> list[dict[str, Any]]:
    count = (duration_ms + chunk_duration_ms - 1) // chunk_duration_ms
    if count > max_windows:
        raise WindowError(
            f"window count {count} exceeds --max-windows {max_windows}; increase chunk duration"
        )
    windows = []
    for ordinal in range(1, count + 1):
        start = (ordinal - 1) * chunk_duration_ms
        end = min(start + chunk_duration_ms, duration_ms)
        windows.append(
            {
                "window_id": f"window_{ordinal:06d}",
                "ordinal": ordinal,
                "start_ms": start,
                "end_ms": end,
                "boundary": "half_open",
                "is_partial_tail": end - start < chunk_duration_ms,
            }
        )
    if not windows or windows[0]["start_ms"] != 0 or windows[-1]["end_ms"] != duration_ms:
        raise AssertionError("window partition failed")
    if any(left["end_ms"] != right["start_ms"] for left, right in zip(windows, windows[1:])):
        raise AssertionError("window partition is not contiguous")
    return windows


def work_order_for(
    *,
    bundle_id: str,
    source: dict[str, Any],
    window: dict[str, Any],
    tools: dict[str, Any],
    output_root: Path,
    max_window_output_bytes: int,
    free_space_floor_bytes: int,
    timeout_seconds: int,
    video_stream_index: int | None,
    audio_stream_index: int | None,
) -> dict[str, Any]:
    raw = {
        "schema_version": 1,
        "job_id": f"local-window-{window['ordinal']:06d}",
        "bundle_id": bundle_id,
        "source": source,
        "window": window,
        "tools": tools,
        "profile": {
            "profile_id": "long-window-cpu-v1",
            "ffmpeg_threads": 4,
            "audio_sample_rate_hz": 16000,
            "audio_channels": 1,
            "audio_sample_format": "s16",
            "flac_compression_level": 8,
            "proxy_width": 640,
            "proxy_height": 360,
            "proxy_fps": 25,
            "proxy_video_codec": "libx264",
            "proxy_preset": "veryfast",
            "proxy_crf": 28,
            "proxy_audio_codec": "aac",
            "proxy_audio_bitrate": "96k",
            "video_stream_index": video_stream_index,
            "audio_stream_index": audio_stream_index,
        },
        "limits": {
            "max_window_output_bytes": max_window_output_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
            "timeout_seconds": timeout_seconds,
        },
        "output": {"root": str(output_root)},
        "safety": {
            "network_allowed": False,
            "credentials_allowed": False,
            "publication_authority": "none",
            "identity_claims_allowed": False,
            "source_bytes_preserved": True,
            "remote_section_download": False,
        },
    }
    return validate_work_order(raw)


def manifest_and_orders(
    acquisition: dict[str, Any],
    *,
    acquisition_result_path: Path,
    tools: dict[str, Any],
    output_root: Path,
    chunk_duration_ms: int,
    max_windows: int,
    max_window_output_bytes: int,
    free_space_floor_bytes: int,
    timeout_seconds: int,
    probe: dict[str, Any],
) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    summary = probe_summary(probe)
    duration = summary["duration_ms"]
    if duration != acquisition["duration_ms"]:
        raise WindowError(
            "pinned local ffprobe duration differs from the acquisition result; "
            "refresh metadata before windowing"
        )
    if summary["video_stream_index"] is None and summary["audio_stream_index"] is None:
        raise WindowError("acquired parent has no usable audio or video stream")
    windows = create_windows(duration, chunk_duration_ms, max_windows)
    source = {
        "path": acquisition["path"],
        "expected_sha256": acquisition["sha256"],
        "byte_count": acquisition["byte_count"],
        "media_id": acquisition["media_id"],
        "duration_ms": duration,
        "acquisition_result_path": str(acquisition_result_path),
        "acquisition_result_sha256": acquisition["result_sha256"],
    }
    identity_core = {
        "schema_version": 1,
        "source": source,
        "tools": tools,
        "policy": {
            "chunk_duration_ms": chunk_duration_ms,
            "max_windows": max_windows,
            "max_window_output_bytes": max_window_output_bytes,
            "free_space_floor_bytes": free_space_floor_bytes,
            "timeout_seconds": timeout_seconds,
            "profile_id": "long-window-cpu-v1",
        },
        "windows": windows,
    }
    bundle_id = stable_id("windowbundle", sha256_bytes(canonical_bytes(identity_core)))
    order_files: list[tuple[str, bytes]] = []
    entries = []
    for window in windows:
        order = work_order_for(
            bundle_id=bundle_id,
            source=source,
            window=window,
            tools=tools,
            output_root=output_root,
            max_window_output_bytes=max_window_output_bytes,
            free_space_floor_bytes=free_space_floor_bytes,
            timeout_seconds=timeout_seconds,
            video_stream_index=summary["video_stream_index"],
            audio_stream_index=summary["audio_stream_index"],
        )
        body = pretty_bytes(order)
        relative = f"work-orders/{window['ordinal']:06d}.json"
        order_files.append((relative, body))
        entries.append(
            {
                **window,
                "job_id": order["job_id"],
                "path": relative,
                "sha256": sha256_bytes(body),
                "byte_count": len(body),
            }
        )
    manifest = {
        "bundle_id": bundle_id,
        "bundle_relative_path": f"bundles/{bundle_id}",
        "schema_version": 1,
        "materializer": {"name": "himr-local-window", "version": IMPLEMENTATION_VERSION},
        "source": source,
        "source_probe": summary,
        "tools": tools,
        "policy": identity_core["policy"],
        "safety": {
            "bundle_class": "private_local_analysis_windows",
            "network_access_performed": False,
            "credentials_allowed": False,
            "publication_authority": "none",
            "identity_claims_allowed": False,
            "remote_time_sections_allowed": False,
            "full_parent_hash_required": True,
            "window_boundary": "half_open",
            "representation_is_original_source": False,
        },
        "work_order_count": len(entries),
        "work_orders": entries,
    }
    return manifest, order_files


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_tree(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    for child in sorted(path.rglob("*"), reverse=True):
        try:
            child.chmod(0o700 if child.is_dir() else 0o600)
        except OSError:
            pass
    try:
        path.chmod(0o700)
    except OSError:
        pass
    shutil.rmtree(path, ignore_errors=True)


def existing_ancestor(path: Path) -> Path:
    candidate = path
    while not candidate.exists():
        if candidate.parent == candidate:
            raise WindowError(f"cannot find an existing filesystem ancestor for {path}")
        candidate = candidate.parent
    if candidate.is_symlink() or not candidate.is_dir():
        raise WindowError(f"filesystem ancestor is not a real directory: {candidate}")
    return candidate


def private_directory_chain(root: Path, parts: list[str]) -> Path:
    current = root
    for part in parts:
        current = current / part
        if current.exists() or current.is_symlink():
            if current.is_symlink() or not current.is_dir():
                raise WindowError(f"private output component is unsafe: {current}")
        else:
            current.mkdir(mode=0o700)
        if stat.S_IMODE(current.stat().st_mode) & 0o077:
            raise WindowError(f"private output component has group/world permissions: {current}")
    return current


@contextmanager
def writer_lock(root: Path, filename: str) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = root / filename
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EINVAL}:
            raise WindowError(f"unsafe lock path: {path}") from error
        raise
    with os.fdopen(descriptor, "a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise WindowError(f"another local-window writer holds {path}") from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def verify_bundle(final: Path, manifest_body: bytes, orders: list[tuple[str, bytes]]) -> None:
    if final.is_symlink() or not final.is_dir() or stat.S_IMODE(final.stat().st_mode) != 0o500:
        raise WindowError("existing immutable window bundle directory is unsafe or writable")
    expected = {"manifest.json", "work-orders", *(path for path, _ in orders)}
    observed = {path.relative_to(final).as_posix() for path in final.rglob("*")}
    if observed != expected:
        raise WindowError("existing immutable window bundle has missing or extra entries")
    files = [("manifest.json", manifest_body), *orders]
    for relative, body in files:
        path = final / relative
        if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o400 or path.read_bytes() != body:
            raise WindowError(f"existing immutable window bundle entry failed replay: {relative}")


def admit_bundle(root: Path, manifest: dict[str, Any], orders: list[tuple[str, bytes]]) -> Path:
    validate_output_root(root, "--bundle-root")
    manifest_body = pretty_bytes(manifest)
    with writer_lock(root, ".local-window-materializer.lock"):
        bundles = root / "bundles"
        bundles.mkdir(exist_ok=True, mode=0o700)
        if bundles.is_symlink() or not bundles.is_dir():
            raise WindowError("bundle admission directory is unsafe")
        final = bundles / manifest["bundle_id"]
        if final.exists() or final.is_symlink():
            verify_bundle(final, manifest_body, orders)
            return final
        staging = root / ".staging"
        staging.mkdir(exist_ok=True, mode=0o700)
        stage = Path(tempfile.mkdtemp(prefix=f".{manifest['bundle_id']}.", dir=staging))
        try:
            work_orders = stage / "work-orders"
            work_orders.mkdir(mode=0o700)
            for relative, body in orders:
                path = stage / relative
                with path.open("xb") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                path.chmod(0o400)
            with (stage / "manifest.json").open("xb") as handle:
                handle.write(manifest_body)
                handle.flush()
                os.fsync(handle.fileno())
            (stage / "manifest.json").chmod(0o400)
            work_orders.chmod(0o500)
            fsync_dir(work_orders)
            fsync_dir(stage)
            os.rename(stage, final)
            final.chmod(0o500)
            fsync_dir(bundles)
        except Exception:
            remove_tree(stage)
            raise
        verify_bundle(final, manifest_body, orders)
        return final


def tree_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for child in path.rglob("*"):
        if child.is_file() and not child.is_symlink():
            total += child.stat().st_size
    return total


def terminate_group(process: subprocess.Popen[Any]) -> None:
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


def run_monitored(command: list[str], stage: Path, limits: dict[str, Any]) -> None:
    stderr_path = stage / ".ffmpeg.stderr"
    started = time.monotonic()
    violation: str | None = None
    with stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            env=subprocess_environment(),
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                elapsed = time.monotonic() - started
                used = tree_bytes(stage)
                free = shutil.disk_usage(stage).free
                if elapsed > limits["timeout_seconds"]:
                    violation = "window command exceeded limits.timeout_seconds"
                elif used > limits["max_window_output_bytes"]:
                    violation = "window staging exceeded limits.max_window_output_bytes"
                elif free < limits["free_space_floor_bytes"]:
                    violation = "window staging crossed limits.free_space_floor_bytes"
                elif stderr_path.stat().st_size > 8 * 1024 * 1024:
                    violation = "window diagnostic output exceeded 8 MiB"
                if violation:
                    terminate_group(process)
                    break
                time.sleep(MONITOR_INTERVAL_SECONDS)
        except BaseException:
            terminate_group(process)
            raise
        returncode = process.wait()
    stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace")[-4000:]
    stderr_path.unlink(missing_ok=True)
    if violation:
        raise WindowError(violation)
    if returncode:
        raise CommandError(command, returncode, stderr_text)
    used = tree_bytes(stage)
    free = shutil.disk_usage(stage).free
    if used > limits["max_window_output_bytes"]:
        raise WindowError("window staging exceeded limits.max_window_output_bytes")
    if free < limits["free_space_floor_bytes"]:
        raise WindowError("window staging crossed limits.free_space_floor_bytes")


def artifact_probe(
    ffprobe: Path, path: Path, profile: dict[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    raw, command = probe_raw(ffprobe, path)
    summary = probe_summary(raw)
    raw_streams = raw.get("streams")
    if not isinstance(raw_streams, list) or any(
        not isinstance(item, dict) for item in raw_streams
    ):
        raise WindowError("artifact FFprobe streams are malformed")
    streams = list(raw_streams)
    video = next((item for item in streams if item.get("codec_type") == "video"), None)
    audio = next((item for item in streams if item.get("codec_type") == "audio"), None)
    stream_types = [item.get("codec_type") for item in streams]
    stream_indexes = [item.get("index") for item in streams]
    if path.name == "audio-16khz-mono.flac":
        if (
            stream_types != ["audio"]
            or stream_indexes != [summary["audio_stream_index"]]
        ):
            raise WindowError("normalized audio must contain exactly one audio stream")
    elif path.name == "proxy-640x360-25fps.mp4":
        expected_types = ["video"] + (
            ["audio"] if profile["audio_stream_index"] is not None else []
        )
        expected_indexes = [summary["video_stream_index"]] + (
            [summary["audio_stream_index"]]
            if profile["audio_stream_index"] is not None
            else []
        )
        if (
            stream_types != expected_types
            or stream_indexes != expected_indexes
            or (streams[0].get("disposition") or {}).get("attached_pic")
        ):
            raise WindowError("proxy must contain exactly one video and one audio stream")
    else:
        raise WindowError("artifact path does not match the supported profile")
    frame_rate: float | None = None
    if video is not None:
        rate_text = video.get("avg_frame_rate") or video.get("r_frame_rate")
        try:
            rate = Fraction(rate_text)
            frame_rate = round(float(rate), 8) if rate.denominator else None
        except (TypeError, ValueError, ZeroDivisionError):
            frame_rate = None
    def optional_int(value: Any) -> int | None:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None
    summary["video"] = None if video is None else {
        "codec_name": video.get("codec_name"),
        "width": optional_int(video.get("width")),
        "height": optional_int(video.get("height")),
        "pixel_format": video.get("pix_fmt"),
        "average_frame_rate": frame_rate,
    }
    summary["audio"] = None if audio is None else {
        "codec_name": audio.get("codec_name"),
        "sample_rate_hz": optional_int(audio.get("sample_rate")),
        "channels": optional_int(audio.get("channels")),
        "sample_format": audio.get("sample_fmt"),
    }
    return summary, command


def verify_artifact(
    kind: str,
    path: Path,
    probe: dict[str, Any],
    profile: dict[str, Any],
    window_duration_ms: int,
) -> None:
    duration = probe["duration_ms"]
    if (
        duration is None
        or duration <= 0
        or abs(duration - window_duration_ms) > 250
    ):
        raise WindowError(f"{kind} duration is invalid for its source-time window")
    if kind == "window_audio_16khz_mono_flac":
        if probe["audio_stream_index"] is None or probe["video_stream_index"] is not None:
            raise WindowError("normalized window audio stream layout is invalid")
        audio = probe["audio"] or {}
        if (
            audio.get("codec_name") != "flac"
            or audio.get("sample_rate_hz") != profile["audio_sample_rate_hz"]
            or audio.get("channels") != profile["audio_channels"]
            or audio.get("sample_format") != profile["audio_sample_format"]
        ):
            raise WindowError("normalized window audio parameters differ from the profile")
    else:
        if probe["video_stream_index"] is None:
            raise WindowError("window proxy has no video stream")
        video = probe["video"] or {}
        if (
            video.get("codec_name") != "h264"
            or video.get("width") != profile["proxy_width"]
            or video.get("height") != profile["proxy_height"]
            or video.get("pixel_format") != "yuv420p"
            or video.get("average_frame_rate") is None
            or abs(video["average_frame_rate"] - profile["proxy_fps"]) > 0.001
        ):
            raise WindowError("window proxy video parameters differ from the profile")
        audio = probe["audio"]
        if profile["audio_stream_index"] is None:
            if audio is not None:
                raise WindowError("video-only window proxy unexpectedly contains audio")
        elif (
            audio is None
            or audio.get("codec_name") != "aac"
            or audio.get("sample_rate_hz") != 48000
            or audio.get("channels") != 2
        ):
            raise WindowError("window proxy audio parameters differ from the profile")


def validate_completed_result(result: Any, order: dict[str, Any], final: Path, work_order_sha: str) -> dict[str, Any]:
    result = exact_object(
        result,
        "local-window result",
        {"schema_version", "implementation_version", "status", "dry_run", "job_id", "bundle_id", "work_order_sha256", "source", "window", "tools", "profile", "limits", "commands", "artifacts", "time_mapping", "safety", "result_path"},
    )
    expected_scalars = {
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "completed",
        "dry_run": False,
        "job_id": order["job_id"],
        "bundle_id": order["bundle_id"],
        "work_order_sha256": work_order_sha,
        "window": order["window"],
        "tools": order["tools"],
        "profile": order["profile"],
        "limits": order["limits"],
        "safety": order["safety"],
        "result_path": str(final / "result.json"),
    }
    for key, expected in expected_scalars.items():
        if result[key] != expected:
            raise WindowError(f"existing local-window result has inconsistent {key}")
    source = exact_object(
        result["source"], "local-window result.source",
        {*order["source"].keys(), "stat_before", "stat_after", "unchanged"},
    )
    if any(source[key] != value for key, value in order["source"].items()):
        raise WindowError("existing local-window result source identity is inconsistent")
    if source["stat_before"] != source["stat_after"] or source["unchanged"] is not True:
        raise WindowError("existing local-window result source stability is inconsistent")
    expected_mapping = {
        "boundary": "half_open",
        "source_start_ms": order["window"]["start_ms"],
        "source_end_ms": order["window"]["end_ms"],
        "artifact_zero_maps_to_source_ms": order["window"]["start_ms"],
        "coordinate_precision": "integer_millisecond_contract",
        "extraction_method": "ffmpeg_accurate_seek_transcode",
        "byte_exact_source_fragment": False,
    }
    if result["time_mapping"] != expected_mapping:
        raise WindowError("existing local-window time mapping is inconsistent")
    if not isinstance(result["commands"], list) or not result["commands"] or any(
        not isinstance(command, list) or not command or any(not isinstance(part, str) or not part for part in command)
        for command in result["commands"]
    ):
        raise WindowError("existing local-window command provenance is malformed")
    expected_kinds = {
        *( ["window_audio_16khz_mono_flac"] if order["profile"]["audio_stream_index"] is not None else [] ),
        *( ["window_low_resolution_cfr_proxy"] if order["profile"]["video_stream_index"] is not None else [] ),
    }
    if not isinstance(result["artifacts"], list) or {
        item.get("artifact_kind") for item in result["artifacts"] if isinstance(item, dict)
    } != expected_kinds or len(result["artifacts"]) != len(expected_kinds):
        raise WindowError("existing local-window artifact set is inconsistent")
    for artifact in result["artifacts"]:
        exact_object(
            artifact, "local-window result artifact",
            {"artifact_id", "artifact_kind", "path", "sha256", "byte_count", "visibility", "normalized_probe"},
        )
        digest = required_sha256(artifact["sha256"], "local-window result artifact SHA-256")
        positive_integer(artifact["byte_count"], "local-window result artifact byte_count")
        expected_id = stable_id(
            "artifact", order["bundle_id"], order["window"]["window_id"], artifact["artifact_kind"], digest
        )
        if artifact["artifact_id"] != expected_id or artifact["visibility"] != "private":
            raise WindowError("existing local-window artifact identity is inconsistent")
        if not isinstance(artifact["path"], str) or not artifact["path"].startswith("/"):
            raise WindowError("existing local-window artifact path is malformed")
        if artifact["path"] != str(final / Path(artifact["path"]).name):
            raise WindowError("existing local-window artifact path is inconsistent")
        if not isinstance(artifact["normalized_probe"], dict):
            raise WindowError("existing local-window artifact probe is malformed")
    return result


def verify_existing_window(
    final: Path,
    result_body: bytes,
    result: dict[str, Any],
    order: dict[str, Any],
    work_order_sha: str,
) -> None:
    validate_completed_result(result, order, final, work_order_sha)
    if final.is_symlink() or not final.is_dir() or stat.S_IMODE(final.stat().st_mode) != 0o500:
        raise WindowError("existing immutable window result directory is unsafe or writable")
    expected = {"result.json", *(Path(item["path"]).name for item in result["artifacts"])}
    observed = {path.relative_to(final).as_posix() for path in final.iterdir()}
    if observed != expected:
        raise WindowError("existing immutable window result has missing or extra entries")
    result_path = final / "result.json"
    if result_path.is_symlink() or stat.S_IMODE(result_path.stat().st_mode) != 0o400 or result_path.read_bytes() != result_body:
        raise WindowError("existing immutable local-window result failed exact replay")
    for artifact in result["artifacts"]:
        path = final / Path(artifact["path"]).name
        if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o400:
            raise WindowError("existing immutable local-window artifact is unsafe")
        observed_sha = sha256_file(path)
        if observed_sha != artifact["sha256"] or path.stat().st_size != artifact["byte_count"]:
            raise WindowError("existing immutable local-window artifact failed integrity replay")


def planned_result(order: dict[str, Any], work_order_sha: str, commands: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "planned",
        "dry_run": True,
        "job_id": order["job_id"],
        "bundle_id": order["bundle_id"],
        "work_order_sha256": work_order_sha,
        "source": order["source"],
        "window": order["window"],
        "tools": order["tools"],
        "profile": order["profile"],
        "limits": order["limits"],
        "commands": [item["command"] for item in commands],
        "artifacts": [],
        "time_mapping": {
            "boundary": "half_open",
            "source_start_ms": order["window"]["start_ms"],
            "source_end_ms": order["window"]["end_ms"],
            "artifact_zero_maps_to_source_ms": order["window"]["start_ms"],
            "coordinate_precision": "integer_millisecond_contract",
            "extraction_method": "ffmpeg_accurate_seek_transcode",
            "byte_exact_source_fragment": False,
        },
        "safety": order["safety"],
        "result_path": None,
    }


def execute_window(order: dict[str, Any], work_order_body: bytes, dry_run: bool) -> dict[str, Any]:
    work_order_sha = sha256_bytes(canonical_bytes(order))
    source_path = Path(order["source"]["path"])
    acquisition_path = Path(order["source"]["acquisition_result_path"])
    if sha256_file(acquisition_path) != order["source"]["acquisition_result_sha256"]:
        raise WindowError("acquisition result differs from the work-order pin")
    source_digest, source_before = stable_file_hash(
        source_path, order["source"]["expected_sha256"], "acquired parent media"
    )
    if source_before["byte_count"] != order["source"]["byte_count"]:
        raise WindowError("acquired parent byte count differs from the work order")
    ffmpeg = tool_from_order(order["tools"]["ffmpeg"], "ffmpeg")
    ffprobe = tool_from_order(order["tools"]["ffprobe"], "ffprobe")
    source_probe_raw, _ = probe_raw(Path(ffprobe["path"]), source_path)
    source_probe = probe_summary(source_probe_raw)
    if source_probe["duration_ms"] != order["source"]["duration_ms"]:
        raise WindowError("source duration differs from the work order")
    if (
        source_probe["video_stream_index"] != order["profile"]["video_stream_index"]
        or source_probe["audio_stream_index"] != order["profile"]["audio_stream_index"]
    ):
        raise WindowError("source primary stream routing differs from the work order")
    output_root = Path(order["output"]["root"])
    final = (
        output_root / "windows" / source_digest[:2] / source_digest / order["bundle_id"]
        / order["window"]["window_id"]
    )
    command_stage = final if dry_run else output_root / ".staging" / (
        f"{order['bundle_id']}-{order['window']['window_id']}-{work_order_sha[:12]}"
    )
    commands = artifact_commands(order, command_stage)
    if dry_run:
        return planned_result(order, work_order_sha, commands)
    capacity_path = existing_ancestor(output_root)
    if shutil.disk_usage(capacity_path).free - order["limits"]["max_window_output_bytes"] < order["limits"]["free_space_floor_bytes"]:
        raise WindowError("capacity reservation would cross the free-space floor")
    with writer_lock(output_root, ".local-window-writer.lock"):
        # If a deterministic result already exists, validate it from its own bytes.
        if final.exists() or final.is_symlink():
            value, body = load_json(final / "result.json", "existing local-window result")
            if not isinstance(value, dict) or value.get("work_order_sha256") != work_order_sha:
                raise WindowError("existing local-window result belongs to a different work order")
            verify_existing_window(final, body, value, order, work_order_sha)
            return value
        if command_stage.exists() or command_stage.is_symlink():
            remove_tree(command_stage)
        private_directory_chain(output_root, [".staging"])
        command_stage.mkdir(mode=0o700)
        try:
            artifacts = []
            command_arrays = []
            for item in commands:
                run_monitored(item["command"], command_stage, order["limits"])
                path = Path(item["path"])
                if not path.is_file() or path.is_symlink() or path.stat().st_size <= 0:
                    raise WindowError("ffmpeg did not create a regular non-empty artifact")
                digest, _ = stable_file_hash(path, None, item["artifact_kind"])
                probe, probe_command = artifact_probe(
                    Path(ffprobe["path"]), path, order["profile"]
                )
                verify_artifact(
                    item["artifact_kind"], path, probe, order["profile"],
                    order["window"]["end_ms"] - order["window"]["start_ms"],
                )
                artifact_id = stable_id(
                    "artifact", order["bundle_id"], order["window"]["window_id"],
                    item["artifact_kind"], digest,
                )
                artifacts.append(
                    {
                        "artifact_id": artifact_id,
                        "artifact_kind": item["artifact_kind"],
                        "path": str(final / path.name),
                        "sha256": digest,
                        "byte_count": path.stat().st_size,
                        "visibility": "private",
                        "normalized_probe": probe,
                    }
                )
                command_arrays.extend([item["command"], probe_command])
            source_after = file_stat(source_path)
            if source_after != source_before or sha256_file(source_path) != source_digest:
                raise WindowError("acquired parent media changed during window extraction")
            result = {
                "schema_version": 1,
                "implementation_version": IMPLEMENTATION_VERSION,
                "status": "completed",
                "dry_run": False,
                "job_id": order["job_id"],
                "bundle_id": order["bundle_id"],
                "work_order_sha256": work_order_sha,
                "source": {
                    **order["source"],
                    "stat_before": source_before,
                    "stat_after": source_after,
                    "unchanged": True,
                },
                "window": order["window"],
                "tools": order["tools"],
                "profile": order["profile"],
                "limits": order["limits"],
                "commands": command_arrays,
                "artifacts": artifacts,
                "time_mapping": {
                    "boundary": "half_open",
                    "source_start_ms": order["window"]["start_ms"],
                    "source_end_ms": order["window"]["end_ms"],
                    "artifact_zero_maps_to_source_ms": order["window"]["start_ms"],
                    "coordinate_precision": "integer_millisecond_contract",
                    "extraction_method": "ffmpeg_accurate_seek_transcode",
                    "byte_exact_source_fragment": False,
                },
                "safety": order["safety"],
                "result_path": str(final / "result.json"),
            }
            result_body = pretty_bytes(result)
            if tree_bytes(command_stage) + len(result_body) > order["limits"]["max_window_output_bytes"]:
                raise WindowError(
                    "sealed window result would exceed limits.max_window_output_bytes"
                )
            for path in command_stage.iterdir():
                path.chmod(0o400)
            with (command_stage / "result.json").open("xb") as handle:
                handle.write(result_body)
                handle.flush()
                os.fsync(handle.fileno())
            (command_stage / "result.json").chmod(0o400)
            fsync_dir(command_stage)
            private_directory_chain(
                output_root,
                [
                    "windows",
                    source_digest[:2],
                    source_digest,
                    order["bundle_id"],
                ],
            )
            os.rename(command_stage, final)
            final.chmod(0o500)
            fsync_dir(final)
            fsync_dir(final.parent)
            verify_existing_window(final, result_body, result, order, work_order_sha)
            return result
        except Exception:
            remove_tree(command_stage)
            raise


def materialize_command(args: argparse.Namespace) -> dict[str, Any]:
    result_path = absolute_path(args.acquisition_result, "--acquisition-result", must_exist=True)
    result_value, result_body = load_json(result_path, "acquisition result")
    acquisition = validate_acquisition_result(result_value, result_body)
    bundle_root = absolute_path(args.bundle_root, "--bundle-root", must_exist=False)
    output_root = absolute_path(args.window_output_root, "--window-output-root", must_exist=False)
    validate_output_root(bundle_root, "--bundle-root")
    validate_output_root(output_root, "--window-output-root")
    tools = {
        "ffmpeg": inspect_tool(args.ffmpeg, args.ffmpeg_sha256, "--ffmpeg"),
        "ffprobe": inspect_tool(args.ffprobe, args.ffprobe_sha256, "--ffprobe"),
    }
    probe, _ = probe_raw(Path(tools["ffprobe"]["path"]), Path(acquisition["path"]))
    manifest, orders = manifest_and_orders(
        acquisition,
        acquisition_result_path=result_path,
        tools=tools,
        output_root=output_root,
        chunk_duration_ms=positive_integer(args.chunk_duration_ms, "--chunk-duration-ms", 7_200_000),
        max_windows=positive_integer(args.max_windows, "--max-windows", MAX_WINDOWS),
        max_window_output_bytes=positive_integer(args.max_window_output_bytes, "--max-window-output-bytes"),
        free_space_floor_bytes=nonnegative_integer(args.free_space_floor_bytes, "--free-space-floor-bytes"),
        timeout_seconds=positive_integer(args.timeout_seconds, "--timeout-seconds", 86400),
        probe=probe,
    )
    admit_bundle(bundle_root, manifest, orders)
    return manifest


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Immutable local windows for admitted long recordings")
    commands = result.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--acquisition-result", required=True)
    materialize.add_argument("--bundle-root", required=True)
    materialize.add_argument("--window-output-root", required=True)
    materialize.add_argument("--ffmpeg", required=True)
    materialize.add_argument("--ffmpeg-sha256", required=True)
    materialize.add_argument("--ffprobe", required=True)
    materialize.add_argument("--ffprobe-sha256", required=True)
    materialize.add_argument("--chunk-duration-ms", type=int, default=1_800_000)
    materialize.add_argument("--max-windows", type=int, default=64)
    materialize.add_argument("--max-window-output-bytes", type=int, default=4 * 1024**3)
    materialize.add_argument("--free-space-floor-bytes", type=int, required=True)
    materialize.add_argument("--timeout-seconds", type=int, default=7200)
    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", required=True)
    run.add_argument("--dry-run", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "materialize":
            result = materialize_command(args)
        else:
            path = absolute_path(args.work_order, "--work-order", must_exist=True)
            raw, body = load_json(path, "local-window work order")
            order = validate_work_order(raw)
            result = order if args.command == "validate" else execute_window(order, body, args.dry_run)
        sys.stdout.buffer.write(pretty_bytes(result))
        return 0
    except (WindowError, OSError, subprocess.TimeoutExpired) as error:
        sys.stderr.buffer.write(
            pretty_bytes(
                {
                    "schema_version": 1,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
