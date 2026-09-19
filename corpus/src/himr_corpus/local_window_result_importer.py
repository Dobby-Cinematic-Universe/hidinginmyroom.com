"""Strict catalog admission for sealed ``local_window.py`` result envelopes.

Local-window result contract v1 deliberately contains no producer execution
timestamps or catalog processing-run row.  This importer must therefore not invent
an extraction run.  It records a distinct, real catalog admission/verification run
at an operator-supplied observation time.  The admitted audio artifact can then be
truthfully named as an ASR input whose parent run is that admission run.

The boundary is intentionally redundant with the producer.  It reparses the exact
envelope, reconstructs the work order and commands, reinspects pinned tool builds,
independently probes and rehashes the derivatives and acquired parent, rehashes the
acquisition envelope, and proves that exact acquisition semantics were already
admitted to the catalog.  It creates no publication decision and no identity
authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import subprocess
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any

from . import __version__
from .db import transaction
from .ids import source_id, stable_id
from .importers import canonical_json, sha256_bytes
from .result_importers import ResultImportError, validate_acquisition_result


CONTRACT_VERSION = 1
SUPPORTED_PRODUCER_VERSION = "0.1.0"
IMPORTER_NAME = "local_window_result_admission_v1"
IMPORTER_VERSION = "local-window-catalog-bridge/1"
STAGE = "local_window_result_admission"
RUN_SEMANTICS = "catalog_admission_verification_not_extraction_execution"
MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_ACQUISITION_RESULT_BYTES = 64 * 1024 * 1024
MAX_JSON_DEPTH = 128
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
BUNDLE_RE = re.compile(r"^windowbundle_[0-9a-f]{32}$")
WINDOW_RE = re.compile(r"^window_[0-9]{6}$")
ARTIFACT_KINDS = {
    "window_audio_16khz_mono_flac": {
        "filename": "audio-16khz-mono.flac",
        "media_kind": "audio",
        "mime_type": "audio/flac",
        "container": "flac",
        "rendition_label": "Local 16 kHz mono analysis window",
    },
    "window_low_resolution_cfr_proxy": {
        "filename": "proxy-640x360-25fps.mp4",
        "media_kind": "video",
        "mime_type": "video/mp4",
        "container": "mp4",
        "rendition_label": "Local low-resolution CFR analysis window",
    },
}

SUPPORTED_PROFILE = {
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
}
TOOL_TIMEOUT_SECONDS = 120
FULL_SOURCE_MAPPING_ROLES = {
    "archive_original_file",
    "complete_source",
    "current_platform_listing",
    "legacy_catalog_mapping",
    "validated_platform_listing",
}


def _reject_constant(value: str) -> None:
    raise ResultImportError(f"non-finite JSON constant is forbidden: {value}")


def _reject_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ResultImportError(f"JSON object contains duplicate key: {key}")
        value[key] = item
    return value


def _check_depth(value: Any, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise ResultImportError("JSON nesting exceeds the admission limit")
    if isinstance(value, dict):
        for child in value.values():
            _check_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_depth(child, depth + 1)


def _parse_json(body: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResultImportError(f"{label} is not strict UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise ResultImportError(f"{label} must contain a JSON object")
    _check_depth(value)
    return value


def _exact_keys(value: object, label: str, expected: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResultImportError(f"{label} must be an object")
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise ResultImportError(
            f"{label} keys differ from the exact contract; "
            f"missing={missing}, unknown={unknown}"
        )
    return value


def _array(value: object, label: str, *, length: int | None = None) -> list[Any]:
    if not isinstance(value, list):
        raise ResultImportError(f"{label} must be an array")
    if length is not None and len(value) != length:
        raise ResultImportError(f"{label} must contain exactly {length} items")
    return value


def _string(value: object, label: str, maximum: int = 8_192) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ResultImportError(f"{label} must be a non-empty bounded string")
    return value


def _identifier(value: object, label: str) -> str:
    result = _string(value, label, 256)
    if not IDENTIFIER_RE.fullmatch(result):
        raise ResultImportError(f"{label} contains unsupported identifier characters")
    return result


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ResultImportError(f"{label} must be a lowercase SHA-256")
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ResultImportError(f"{label} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise ResultImportError(f"{label} must be <= {maximum}")
    return value


def _number(value: object, label: str, *, minimum: float = 0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResultImportError(f"{label} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise ResultImportError(f"{label} must be finite and >= {minimum}")
    return result


def _timestamp(value: object, label: str) -> str:
    text = _string(value, label, 64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ResultImportError(f"{label} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ResultImportError(f"{label} must include a UTC offset")
    normalized = parsed.astimezone(timezone.utc)
    if normalized.microsecond:
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _timestamp_value(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _resolved_absolute_file(value: object, label: str) -> Path:
    text = _string(value, label)
    if "://" in text:
        raise ResultImportError(f"{label} must be an absolute local path")
    path = Path(text)
    if not path.is_absolute():
        raise ResultImportError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ResultImportError(f"{label} is unavailable or unsafe: {path}") from error
    if resolved != path:
        raise ResultImportError(f"{label} must already be a resolved path")
    try:
        link_stat = path.lstat()
    except OSError as error:
        raise ResultImportError(f"{label} cannot be inspected: {error}") from error
    if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISREG(link_stat.st_mode):
        raise ResultImportError(f"{label} must be a regular non-symlink file")
    return path


def _stable_read(
    path: Path,
    label: str,
    *,
    maximum_bytes: int,
    exact_mode: int | None = None,
) -> bytes:
    try:
        link_stat = path.lstat()
        path_stat = path.stat()
        handle = path.open("rb")
    except OSError as error:
        raise ResultImportError(f"{label} cannot be opened safely: {error}") from error
    if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISREG(link_stat.st_mode):
        handle.close()
        raise ResultImportError(f"{label} must be a regular non-symlink file")
    if exact_mode is not None and stat.S_IMODE(link_stat.st_mode) != exact_mode:
        handle.close()
        raise ResultImportError(f"{label} mode must be exactly {exact_mode:04o}")
    if path_stat.st_size > maximum_bytes:
        handle.close()
        raise ResultImportError(f"{label} exceeds the {maximum_bytes}-byte limit")
    try:
        descriptor_before = os.fstat(handle.fileno())
        identity = (
            descriptor_before.st_dev,
            descriptor_before.st_ino,
            descriptor_before.st_size,
            descriptor_before.st_mtime_ns,
        )
        if identity != (
            path_stat.st_dev,
            path_stat.st_ino,
            path_stat.st_size,
            path_stat.st_mtime_ns,
        ):
            raise ResultImportError(f"{label} was replaced while opening")
        body = handle.read()
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        after = path.stat()
    except OSError as error:
        raise ResultImportError(f"{label} disappeared after reading: {error}") from error
    if identity != (
        descriptor_after.st_dev,
        descriptor_after.st_ino,
        descriptor_after.st_size,
        descriptor_after.st_mtime_ns,
    ) or identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ResultImportError(f"{label} changed while being read")
    if len(body) != identity[2]:
        raise ResultImportError(f"{label} short read")
    return body


def _stable_hash_file(
    path: Path,
    label: str,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    exact_mode: int | None = None,
) -> None:
    try:
        link_stat = path.lstat()
        path_stat = path.stat()
        handle = path.open("rb")
    except OSError as error:
        raise ResultImportError(f"{label} cannot be opened safely: {error}") from error
    if stat.S_ISLNK(link_stat.st_mode) or not stat.S_ISREG(link_stat.st_mode):
        handle.close()
        raise ResultImportError(f"{label} must be a regular non-symlink file")
    if exact_mode is not None and stat.S_IMODE(link_stat.st_mode) != exact_mode:
        handle.close()
        raise ResultImportError(f"{label} mode must be exactly {exact_mode:04o}")
    if path_stat.st_size != expected_byte_count:
        handle.close()
        raise ResultImportError(f"{label} byte count differs from its envelope")
    try:
        descriptor_before = os.fstat(handle.fileno())
        identity = (
            descriptor_before.st_dev,
            descriptor_before.st_ino,
            descriptor_before.st_size,
            descriptor_before.st_mtime_ns,
        )
        if identity != (
            path_stat.st_dev,
            path_stat.st_ino,
            path_stat.st_size,
            path_stat.st_mtime_ns,
        ):
            raise ResultImportError(f"{label} was replaced while opening")
        digest = hashlib.sha256()
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
        descriptor_after = os.fstat(handle.fileno())
    finally:
        handle.close()
    try:
        after = path.stat()
    except OSError as error:
        raise ResultImportError(f"{label} disappeared after hashing: {error}") from error
    if identity != (
        descriptor_after.st_dev,
        descriptor_after.st_ino,
        descriptor_after.st_size,
        descriptor_after.st_mtime_ns,
    ) or identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ResultImportError(f"{label} changed while being hashed")
    if digest.hexdigest() != expected_sha256:
        raise ResultImportError(f"{label} SHA-256 differs from its envelope")


def _run_capture(command: list[str], label: str) -> str:
    environment = os.environ.copy()
    environment.update({"LC_ALL": "C", "LANG": "C", "TZ": "UTC"})
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=TOOL_TIMEOUT_SECONDS,
            env=environment,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ResultImportError(f"{label} could not be executed safely: {error}") from error
    if completed.returncode:
        raise ResultImportError(
            f"{label} failed with exit code {completed.returncode}: "
            f"{completed.stderr[-2_000:]}"
        )
    return completed.stdout


def _inspect_pinned_tool(value: dict[str, Any], name: str) -> Path:
    path = _resolved_absolute_file(value["path"], f"local-window tools.{name}.path")
    if not os.access(path, os.X_OK):
        raise ResultImportError(f"local-window tools.{name}.path is not executable")
    _stable_hash_file(
        path,
        f"local-window tools.{name}",
        expected_sha256=value["sha256"],
        expected_byte_count=value["byte_count"],
    )
    version = _run_capture([str(path), "-version"], f"pinned {name}").strip()
    if not version:
        raise ResultImportError(f"pinned {name} returned empty version output")
    if (
        sha256_bytes(version.encode("utf-8")) != value["version_output_sha256"]
        or version.splitlines()[0][:1_000] != value["version_first_line"]
    ):
        raise ResultImportError(f"pinned {name} build/version differs from the result")
    _stable_hash_file(
        path,
        f"local-window tools.{name}",
        expected_sha256=value["sha256"],
        expected_byte_count=value["byte_count"],
    )
    return path


def _milliseconds(value: object) -> int | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return round(number * 1_000)


def _optional_nonnegative_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _probe_raw(ffprobe: Path, media_path: Path, label: str) -> tuple[dict[str, Any], list[str]]:
    command = [
        str(ffprobe),
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-of",
        "json",
        str(media_path),
    ]
    stdout = _run_capture(command, f"pinned FFprobe for {label}")
    try:
        raw = json.loads(
            stdout,
            object_pairs_hook=_reject_pairs,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as error:
        raise ResultImportError(f"pinned FFprobe returned invalid JSON for {label}") from error
    if not isinstance(raw, dict):
        raise ResultImportError(f"pinned FFprobe result for {label} must be an object")
    _check_depth(raw)
    return raw, command


def _probe_summary(raw: dict[str, Any], *, artifact: bool) -> dict[str, Any]:
    raw_format = raw.get("format") if isinstance(raw.get("format"), dict) else {}
    streams = [item for item in raw.get("streams", []) if isinstance(item, dict)]
    duration_ms = _milliseconds(raw_format.get("duration"))
    if duration_ms is None:
        duration_ms = max(
            (
                duration
                for item in streams
                if (duration := _milliseconds(item.get("duration"))) is not None
            ),
            default=None,
        )
    video = next(
        (
            item
            for item in streams
            if item.get("codec_type") == "video"
            and not (item.get("disposition") or {}).get("attached_pic")
        ),
        None,
    )
    audio = next((item for item in streams if item.get("codec_type") == "audio"), None)
    summary: dict[str, Any] = {
        "duration_ms": duration_ms,
        "video_stream_index": None if video is None else video.get("index"),
        "audio_stream_index": None if audio is None else audio.get("index"),
    }
    if not artifact:
        return summary
    frame_rate: float | None = None
    if video is not None:
        rate_text = video.get("avg_frame_rate") or video.get("r_frame_rate")
        try:
            rate = Fraction(rate_text)
            frame_rate = round(float(rate), 8) if rate.denominator else None
        except (TypeError, ValueError, ZeroDivisionError):
            frame_rate = None
    summary["video"] = (
        None
        if video is None
        else {
            "codec_name": video.get("codec_name"),
            "width": _optional_nonnegative_int(video.get("width")),
            "height": _optional_nonnegative_int(video.get("height")),
            "pixel_format": video.get("pix_fmt"),
            "average_frame_rate": frame_rate,
        }
    )
    summary["audio"] = (
        None
        if audio is None
        else {
            "codec_name": audio.get("codec_name"),
            "sample_rate_hz": _optional_nonnegative_int(audio.get("sample_rate")),
            "channels": _optional_nonnegative_int(audio.get("channels")),
            "sample_format": audio.get("sample_fmt"),
        }
    )
    return summary


def _seconds_text(milliseconds_value: int) -> str:
    return f"{milliseconds_value // 1_000}.{milliseconds_value % 1_000:03d}"


def _producer_artifact_id(
    bundle_id: str, window_id: str, artifact_kind: str, digest: str
) -> str:
    key = "\x1f".join((bundle_id, window_id, artifact_kind, digest)).encode("utf-8")
    return f"artifact_{hashlib.sha256(key).hexdigest()[:32]}"


def _stat_contract(value: object, label: str, expected_bytes: int) -> dict[str, int]:
    row = _exact_keys(value, label, {"device", "inode", "byte_count", "mtime_ns"})
    normalized = {
        key: _integer(row[key], f"{label}.{key}")
        for key in ("device", "inode", "byte_count", "mtime_ns")
    }
    if normalized["byte_count"] != expected_bytes:
        raise ResultImportError(f"{label}.byte_count disagrees with source identity")
    return normalized


def _validate_probe(value: object, label: str) -> dict[str, Any]:
    probe = _exact_keys(
        value,
        label,
        {"duration_ms", "video_stream_index", "audio_stream_index", "video", "audio"},
    )
    duration = _integer(probe["duration_ms"], f"{label}.duration_ms", minimum=1)
    for key in ("video_stream_index", "audio_stream_index"):
        if probe[key] is not None:
            _integer(probe[key], f"{label}.{key}")
    video = probe["video"]
    if video is not None:
        video = _exact_keys(
            video,
            f"{label}.video",
            {"codec_name", "width", "height", "pixel_format", "average_frame_rate"},
        )
        for key in ("codec_name", "pixel_format"):
            if video[key] is not None:
                _string(video[key], f"{label}.video.{key}", 128)
        for key in ("width", "height"):
            if video[key] is not None:
                _integer(video[key], f"{label}.video.{key}")
        if video["average_frame_rate"] is not None:
            _number(video["average_frame_rate"], f"{label}.video.average_frame_rate")
    audio = probe["audio"]
    if audio is not None:
        audio = _exact_keys(
            audio,
            f"{label}.audio",
            {"codec_name", "sample_rate_hz", "channels", "sample_format"},
        )
        for key in ("codec_name", "sample_format"):
            if audio[key] is not None:
                _string(audio[key], f"{label}.audio.{key}", 128)
        for key in ("sample_rate_hz", "channels"):
            if audio[key] is not None:
                _integer(audio[key], f"{label}.audio.{key}")
    return {**probe, "duration_ms": duration, "video": video, "audio": audio}


def _result_output_root(
    result_path: Path,
    *,
    source_sha256: str,
    bundle_id: str,
    window_id: str,
) -> Path:
    if len(result_path.parents) < 6:
        raise ResultImportError("local-window result path is too shallow for producer layout")
    output_root = result_path.parents[5]
    expected = (
        output_root
        / "windows"
        / source_sha256[:2]
        / source_sha256
        / bundle_id
        / window_id
        / "result.json"
    )
    if expected != result_path:
        raise ResultImportError("local-window result path does not match producer layout")
    if output_root == Path("/") or any(
        forbidden == output_root or forbidden in output_root.parents
        for forbidden in (Path("/tmp"), Path("/var/tmp"))
    ):
        raise ResultImportError("local-window output root violates producer path policy")
    return output_root


def _work_order_from_result(
    result: dict[str, Any],
    *,
    output_root: Path,
) -> dict[str, Any]:
    source = result["source"]
    return {
        "schema_version": 1,
        "job_id": result["job_id"],
        "bundle_id": result["bundle_id"],
        "source": {
            key: source[key]
            for key in (
                "path",
                "expected_sha256",
                "byte_count",
                "media_id",
                "duration_ms",
                "acquisition_result_path",
                "acquisition_result_sha256",
            )
        },
        "window": result["window"],
        "tools": result["tools"],
        "profile": result["profile"],
        "limits": result["limits"],
        "output": {"root": str(output_root)},
        "safety": result["safety"],
    }


def _expected_commands(
    result: dict[str, Any],
    *,
    output_root: Path,
    work_order_sha256: str,
) -> list[list[str]]:
    profile = result["profile"]
    window = result["window"]
    duration = window["end_ms"] - window["start_ms"]
    stage = output_root / ".staging" / (
        f"{result['bundle_id']}-{window['window_id']}-{work_order_sha256[:12]}"
    )
    prefix = [
        result["tools"]["ffmpeg"]["path"],
        "-hide_banner",
        "-nostdin",
        "-nostats",
        "-loglevel",
        "error",
        "-threads",
        str(profile["ffmpeg_threads"]),
        "-ss",
        _seconds_text(window["start_ms"]),
        "-accurate_seek",
        "-i",
        result["source"]["path"],
        "-t",
        _seconds_text(duration),
    ]
    audio_path = stage / ARTIFACT_KINDS["window_audio_16khz_mono_flac"]["filename"]
    proxy_path = stage / ARTIFACT_KINDS["window_low_resolution_cfr_proxy"]["filename"]
    audio_command = prefix + [
        "-map",
        f"0:{profile['audio_stream_index']}",
        "-vn",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-af",
        (
            f"atrim=start=0:end={_seconds_text(duration)},"
            "asetpts=PTS-STARTPTS,aresample=16000"
        ),
        "-ac",
        "1",
        "-ar",
        "16000",
        "-sample_fmt",
        "s16",
        "-c:a",
        "flac",
        "-compression_level",
        str(profile["flac_compression_level"]),
        str(audio_path),
    ]
    audio_probe = [
        result["tools"]["ffprobe"]["path"],
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-of",
        "json",
        str(audio_path),
    ]
    video_filter = (
        f"trim=start=0:end={_seconds_text(duration)},setpts=PTS-STARTPTS,"
        f"scale=w={profile['proxy_width']}:h={profile['proxy_height']}:"
        "force_original_aspect_ratio=decrease:force_divisible_by=2,"
        f"pad={profile['proxy_width']}:{profile['proxy_height']}:"
        "(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,"
        f"fps={profile['proxy_fps']}"
    )
    proxy_command = prefix + [
        "-map",
        f"0:{profile['video_stream_index']}",
        "-map",
        f"0:{profile['audio_stream_index']}",
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
        "-af",
        (
            f"atrim=start=0:end={_seconds_text(duration)},"
            "asetpts=PTS-STARTPTS,aresample=48000"
        ),
        "-c:a",
        profile["proxy_audio_codec"],
        "-b:a",
        profile["proxy_audio_bitrate"],
        "-ar",
        "48000",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
        "-avoid_negative_ts",
        "make_zero",
        str(proxy_path),
    ]
    proxy_probe = [
        result["tools"]["ffprobe"]["path"],
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-of",
        "json",
        str(proxy_path),
    ]
    return [audio_command, audio_probe, proxy_command, proxy_probe]


def _verify_current_execution_inputs(result: dict[str, Any]) -> None:
    _inspect_pinned_tool(result["tools"]["ffmpeg"], "ffmpeg")
    ffprobe = _inspect_pinned_tool(result["tools"]["ffprobe"], "ffprobe")
    source_raw, _ = _probe_raw(ffprobe, result["_source_path"], "acquired parent")
    source_probe = _probe_summary(source_raw, artifact=False)
    if source_probe != {
        "duration_ms": result["source"]["duration_ms"],
        "video_stream_index": result["profile"]["video_stream_index"],
        "audio_stream_index": result["profile"]["audio_stream_index"],
    }:
        raise ResultImportError(
            "current acquired-parent FFprobe result differs from the work-order routing"
        )
    for artifact in result["artifacts"]:
        raw, _ = _probe_raw(
            ffprobe,
            Path(artifact["path"]),
            artifact["artifact_kind"],
        )
        current_probe = _probe_summary(raw, artifact=True)
        if current_probe != artifact["normalized_probe"]:
            raise ResultImportError(
                f"current {artifact['artifact_kind']} FFprobe result differs from its envelope"
            )
        raw_streams = raw.get("streams")
        if not isinstance(raw_streams, list) or any(
            not isinstance(stream, dict) for stream in raw_streams
        ):
            raise ResultImportError("current derivative has malformed FFprobe streams")
        stream_types = [stream.get("codec_type") for stream in raw_streams]
        stream_indexes = [stream.get("index") for stream in raw_streams]
        if artifact["artifact_kind"] == "window_audio_16khz_mono_flac":
            if (
                stream_types != ["audio"]
                or stream_indexes != [current_probe["audio_stream_index"]]
            ):
                raise ResultImportError(
                    "current normalized audio must contain exactly one audio stream"
                )
        elif (
            stream_types != ["video", "audio"]
            or stream_indexes
            != [
                current_probe["video_stream_index"],
                current_probe["audio_stream_index"],
            ]
            or (raw_streams[0].get("disposition") or {}).get("attached_pic")
        ):
            raise ResultImportError(
                "current proxy must contain exactly one video and one audio stream"
            )
        raw_format = raw.get("format") if isinstance(raw.get("format"), dict) else {}
        format_names = set(str(raw_format.get("format_name") or "").split(","))
        if artifact["artifact_kind"] == "window_audio_16khz_mono_flac":
            if format_names != {"flac"}:
                raise ResultImportError("current normalized audio is not a FLAC container")
        elif "mp4" not in format_names:
            raise ResultImportError("current proxy is not an MP4 container")


def _validate_local_result(result: dict[str, Any], result_path: Path) -> dict[str, Any]:
    _exact_keys(
        result,
        "local-window result",
        {
            "schema_version", "implementation_version", "status", "dry_run",
            "job_id", "bundle_id", "work_order_sha256", "source", "window",
            "tools", "profile", "limits", "commands", "artifacts",
            "time_mapping", "safety", "result_path",
        },
    )
    if (
        result["schema_version"] != CONTRACT_VERSION
        or result["implementation_version"] != SUPPORTED_PRODUCER_VERSION
        or result["status"] != "completed"
        or result["dry_run"] is not False
    ):
        raise ResultImportError("local-window result is not a supported completed v1 result")
    job_id = _identifier(result["job_id"], "local-window result.job_id")
    bundle_id = _string(result["bundle_id"], "local-window result.bundle_id", 64)
    if not BUNDLE_RE.fullmatch(bundle_id):
        raise ResultImportError("local-window result.bundle_id is invalid")
    work_order_sha = _sha256(
        result["work_order_sha256"], "local-window result.work_order_sha256"
    )
    embedded_result_path = _resolved_absolute_file(
        result["result_path"], "local-window result.result_path"
    )
    if embedded_result_path != result_path:
        raise ResultImportError("local-window result_path does not identify the imported file")

    source = _exact_keys(
        result["source"],
        "local-window result.source",
        {
            "path", "expected_sha256", "byte_count", "media_id", "duration_ms",
            "acquisition_result_path", "acquisition_result_sha256", "stat_before",
            "stat_after", "unchanged",
        },
    )
    source_path = _resolved_absolute_file(source["path"], "local-window source.path")
    source_sha = _sha256(source["expected_sha256"], "local-window source.expected_sha256")
    source_bytes = _integer(source["byte_count"], "local-window source.byte_count", minimum=1)
    source_duration = _integer(source["duration_ms"], "local-window source.duration_ms", minimum=1)
    if source["media_id"] != f"media_sha256_{source_sha}":
        raise ResultImportError("local-window source.media_id does not derive from its SHA-256")
    acquisition_path = _resolved_absolute_file(
        source["acquisition_result_path"], "local-window source.acquisition_result_path"
    )
    acquisition_sha = _sha256(
        source["acquisition_result_sha256"],
        "local-window source.acquisition_result_sha256",
    )
    before = _stat_contract(source["stat_before"], "local-window source.stat_before", source_bytes)
    after = _stat_contract(source["stat_after"], "local-window source.stat_after", source_bytes)
    if source["unchanged"] is not True or before != after:
        raise ResultImportError("local-window source stability evidence is inconsistent")
    current = source_path.stat()
    current_identity = {
        "device": current.st_dev,
        "inode": current.st_ino,
        "byte_count": current.st_size,
        "mtime_ns": current.st_mtime_ns,
    }
    if current_identity != after:
        raise ResultImportError("local-window parent source no longer matches extraction stat evidence")

    window = _exact_keys(
        result["window"],
        "local-window result.window",
        {"window_id", "ordinal", "start_ms", "end_ms", "boundary", "is_partial_tail"},
    )
    window_id = _string(window["window_id"], "local-window window.window_id", 32)
    if not WINDOW_RE.fullmatch(window_id):
        raise ResultImportError("local-window window_id is invalid")
    ordinal = _integer(window["ordinal"], "local-window window.ordinal", minimum=1, maximum=256)
    if window_id != f"window_{ordinal:06d}" or job_id != f"local-window-{ordinal:06d}":
        raise ResultImportError("local-window job/window identity is inconsistent")
    start = _integer(window["start_ms"], "local-window window.start_ms")
    end = _integer(window["end_ms"], "local-window window.end_ms", minimum=1)
    if start >= end or end > source_duration or window["boundary"] != "half_open":
        raise ResultImportError("local-window interval is outside the acquired parent")
    if not isinstance(window["is_partial_tail"], bool):
        raise ResultImportError("local-window is_partial_tail must be boolean")
    if window["is_partial_tail"] and end != source_duration:
        raise ResultImportError("a partial tail must end at the source duration")

    tools = _exact_keys(result["tools"], "local-window result.tools", {"ffmpeg", "ffprobe"})
    for name in ("ffmpeg", "ffprobe"):
        tool = _exact_keys(
            tools[name],
            f"local-window tools.{name}",
            {"path", "sha256", "byte_count", "version_output_sha256", "version_first_line"},
        )
        path = _resolved_absolute_file(tool["path"], f"local-window tools.{name}.path")
        if str(path) != tool["path"]:
            raise ResultImportError(f"local-window tools.{name}.path is not normalized")
        _sha256(tool["sha256"], f"local-window tools.{name}.sha256")
        _integer(tool["byte_count"], f"local-window tools.{name}.byte_count", minimum=1)
        _sha256(
            tool["version_output_sha256"],
            f"local-window tools.{name}.version_output_sha256",
        )
        _string(tool["version_first_line"], f"local-window tools.{name}.version_first_line", 1_000)

    profile = _exact_keys(
        result["profile"],
        "local-window result.profile",
        {
            "profile_id", "ffmpeg_threads", "audio_sample_rate_hz", "audio_channels",
            "audio_sample_format", "flac_compression_level", "proxy_width",
            "proxy_height", "proxy_fps", "proxy_video_codec", "proxy_preset",
            "proxy_crf", "proxy_audio_codec", "proxy_audio_bitrate",
            "video_stream_index", "audio_stream_index",
        },
    )
    video_stream_index = _integer(
        profile["video_stream_index"], "local-window profile.video_stream_index"
    )
    audio_stream_index = _integer(
        profile["audio_stream_index"], "local-window profile.audio_stream_index"
    )
    expected_profile = {
        **SUPPORTED_PROFILE,
        "video_stream_index": video_stream_index,
        "audio_stream_index": audio_stream_index,
    }
    if profile != expected_profile:
        raise ResultImportError(
            "local-window profile differs from the exact supported producer profile"
        )

    limits = _exact_keys(
        result["limits"],
        "local-window result.limits",
        {"max_window_output_bytes", "free_space_floor_bytes", "timeout_seconds"},
    )
    _integer(limits["max_window_output_bytes"], "local-window limits.max_window_output_bytes", minimum=1)
    _integer(limits["free_space_floor_bytes"], "local-window limits.free_space_floor_bytes")
    _integer(limits["timeout_seconds"], "local-window limits.timeout_seconds", minimum=1, maximum=86400)

    commands = _array(result["commands"], "local-window result.commands", length=4)
    for index, raw_command in enumerate(commands):
        command = _array(raw_command, f"local-window commands[{index}]")
        if not command or len(command) > 256:
            raise ResultImportError(f"local-window commands[{index}] is empty or unbounded")
        for arg_index, argument in enumerate(command):
            _string(argument, f"local-window commands[{index}][{arg_index}]", 16_384)
        expected_executable = tools["ffmpeg" if index % 2 == 0 else "ffprobe"]["path"]
        if command[0] != expected_executable:
            raise ResultImportError("local-window command/tool provenance ordering is inconsistent")

    mapping = _exact_keys(
        result["time_mapping"],
        "local-window result.time_mapping",
        {
            "boundary", "source_start_ms", "source_end_ms",
            "artifact_zero_maps_to_source_ms", "coordinate_precision",
            "extraction_method", "byte_exact_source_fragment",
        },
    )
    expected_mapping = {
        "boundary": "half_open",
        "source_start_ms": start,
        "source_end_ms": end,
        "artifact_zero_maps_to_source_ms": start,
        "coordinate_precision": "integer_millisecond_contract",
        "extraction_method": "ffmpeg_accurate_seek_transcode",
        "byte_exact_source_fragment": False,
    }
    if mapping != expected_mapping:
        raise ResultImportError("local-window source-time mapping is inconsistent")

    safety = _exact_keys(
        result["safety"],
        "local-window result.safety",
        {
            "network_allowed", "credentials_allowed", "publication_authority",
            "identity_claims_allowed", "source_bytes_preserved", "remote_section_download",
        },
    )
    expected_safety = {
        "network_allowed": False,
        "credentials_allowed": False,
        "publication_authority": "none",
        "identity_claims_allowed": False,
        "source_bytes_preserved": True,
        "remote_section_download": False,
    }
    if safety != expected_safety:
        raise ResultImportError("local-window safety policy is not fail closed")

    output_root = _result_output_root(
        result_path,
        source_sha256=source_sha,
        bundle_id=bundle_id,
        window_id=window_id,
    )
    reconstructed_order = _work_order_from_result(result, output_root=output_root)
    reconstructed_work_order_sha = sha256_bytes(
        canonical_json(reconstructed_order).encode("utf-8")
    )
    if reconstructed_work_order_sha != work_order_sha:
        raise ResultImportError(
            "local-window work_order_sha256 does not match the exact reconstructed order"
        )
    if commands != _expected_commands(
        result,
        output_root=output_root,
        work_order_sha256=reconstructed_work_order_sha,
    ):
        raise ResultImportError(
            "local-window commands differ from the exact supported producer commands"
        )

    artifacts_raw = _array(result["artifacts"], "local-window result.artifacts", length=2)
    artifacts: list[dict[str, Any]] = []
    kinds: set[str] = set()
    window_duration = end - start
    for index, raw_artifact in enumerate(artifacts_raw):
        artifact = _exact_keys(
            raw_artifact,
            f"local-window artifact[{index}]",
            {"artifact_id", "artifact_kind", "path", "sha256", "byte_count", "visibility", "normalized_probe"},
        )
        kind = _string(artifact["artifact_kind"], f"local-window artifact[{index}].artifact_kind", 128)
        if kind not in ARTIFACT_KINDS or kind in kinds:
            raise ResultImportError("local-window artifacts must contain the two distinct supported kinds")
        kinds.add(kind)
        digest = _sha256(artifact["sha256"], f"local-window artifact[{index}].sha256")
        byte_count = _integer(
            artifact["byte_count"], f"local-window artifact[{index}].byte_count", minimum=1
        )
        artifact_path = _resolved_absolute_file(
            artifact["path"], f"local-window artifact[{index}].path"
        )
        expected_path = result_path.parent / ARTIFACT_KINDS[kind]["filename"]
        if artifact_path != expected_path:
            raise ResultImportError("local-window artifact path/layout is inconsistent")
        expected_id = _producer_artifact_id(bundle_id, window_id, kind, digest)
        if artifact["artifact_id"] != expected_id or artifact["visibility"] != "private":
            raise ResultImportError("local-window artifact identity/visibility is inconsistent")
        probe = _validate_probe(
            artifact["normalized_probe"], f"local-window artifact[{index}].normalized_probe"
        )
        if abs(probe["duration_ms"] - window_duration) > 250:
            raise ResultImportError(
                "local-window artifact duration differs from its bounded window"
            )
        if kind == "window_audio_16khz_mono_flac":
            audio = probe["audio"] or {}
            if (
                probe["audio_stream_index"] is None
                or probe["video_stream_index"] is not None
                or probe["video"] is not None
                or audio.get("codec_name") != "flac"
                or audio.get("sample_rate_hz") != 16000
                or audio.get("channels") != 1
                or audio.get("sample_format") != "s16"
            ):
                raise ResultImportError("local-window normalized audio probe is inconsistent")
        else:
            video = probe["video"] or {}
            audio = probe["audio"] or {}
            if (
                probe["video_stream_index"] is None
                or probe["audio_stream_index"] is None
                or video.get("codec_name") != "h264"
                or video.get("width") != profile["proxy_width"]
                or video.get("height") != profile["proxy_height"]
                or video.get("pixel_format") != "yuv420p"
                or video.get("average_frame_rate") is None
                or abs(float(video["average_frame_rate"]) - profile["proxy_fps"]) > 0.001
                or audio.get("codec_name") != "aac"
                or audio.get("sample_rate_hz") != 48000
                or audio.get("channels") != 2
            ):
                raise ResultImportError("local-window proxy probe is inconsistent")
        artifacts.append(
            {
                **artifact,
                "path": str(artifact_path),
                "sha256": digest,
                "byte_count": byte_count,
                "normalized_probe": probe,
            }
        )
    if kinds != set(ARTIFACT_KINDS):
        raise ResultImportError("local-window result is missing a required derivative")
    if sum(artifact["byte_count"] for artifact in artifacts) >= limits[
        "max_window_output_bytes"
    ]:
        # The producer must also fit result.json inside the cap; the exact envelope
        # byte count is checked once it has been read by _read_sealed_result.
        raise ResultImportError(
            "local-window derivatives leave no capacity for the sealed result envelope"
        )

    return {
        **result,
        "source": {
            **source,
            "path": str(source_path),
            "acquisition_result_path": str(acquisition_path),
            "stat_before": before,
            "stat_after": after,
        },
        "window": dict(window),
        "time_mapping": dict(mapping),
        "artifacts": artifacts,
        "_result_path": result_path,
        "_source_path": source_path,
        "_acquisition_path": acquisition_path,
        "_output_root": output_root,
        "_reconstructed_work_order": reconstructed_order,
        "_work_order_sha256": work_order_sha,
        "_acquisition_sha256": acquisition_sha,
    }


def _read_sealed_result(result_path: str | Path) -> dict[str, Any]:
    requested = Path(result_path)
    try:
        path = requested.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ResultImportError(f"local-window result is unavailable: {error}") from error
    if not path.is_absolute() or path.name != "result.json":
        raise ResultImportError("local-window result must be a resolved result.json path")
    try:
        directory_stat = path.parent.lstat()
    except OSError as error:
        raise ResultImportError(f"local-window result directory cannot be inspected: {error}") from error
    if (
        stat.S_ISLNK(directory_stat.st_mode)
        or not stat.S_ISDIR(directory_stat.st_mode)
        or stat.S_IMODE(directory_stat.st_mode) != 0o500
    ):
        raise ResultImportError("local-window result directory mode must be exactly 0500")
    body = _stable_read(
        path,
        "local-window result envelope",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
    )
    result = _validate_local_result(_parse_json(body, "local-window result envelope"), path)
    expected_entries = {
        "result.json",
        *(ARTIFACT_KINDS[kind]["filename"] for kind in ARTIFACT_KINDS),
    }
    try:
        observed_entries = {entry.name for entry in path.parent.iterdir()}
    except OSError as error:
        raise ResultImportError(f"local-window result tree cannot be enumerated: {error}") from error
    if observed_entries != expected_entries:
        raise ResultImportError(
            "local-window result directory has missing or extra entries; "
            f"expected={sorted(expected_entries)}, observed={sorted(observed_entries)}"
        )
    for artifact in result["artifacts"]:
        _stable_hash_file(
            Path(artifact["path"]),
            f"local-window {artifact['artifact_kind']} artifact",
            expected_sha256=artifact["sha256"],
            expected_byte_count=artifact["byte_count"],
            exact_mode=0o400,
        )
    _verify_current_execution_inputs(result)
    for artifact in result["artifacts"]:
        _stable_hash_file(
            Path(artifact["path"]),
            f"local-window {artifact['artifact_kind']} artifact after FFprobe",
            expected_sha256=artifact["sha256"],
            expected_byte_count=artifact["byte_count"],
            exact_mode=0o400,
        )
    if _stable_read(
        path,
        "local-window result envelope after media verification",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
    ) != body:
        raise ResultImportError("local-window result envelope changed during verification")
    result["_result_body"] = body
    result["_result_sha256"] = sha256_bytes(body)
    result["_result_byte_count"] = len(body)
    if (
        sum(artifact["byte_count"] for artifact in result["artifacts"])
        + result["_result_byte_count"]
        > result["limits"]["max_window_output_bytes"]
    ):
        raise ResultImportError(
            "local-window result tree exceeds limits.max_window_output_bytes"
        )
    return result


def _read_acquisition(result: dict[str, Any]) -> dict[str, Any]:
    path: Path = result["_acquisition_path"]
    body = _stable_read(
        path,
        "pinned acquisition result",
        maximum_bytes=MAX_ACQUISITION_RESULT_BYTES,
    )
    raw = _parse_json(body, "pinned acquisition result")
    raw_sha = sha256_bytes(body)
    if raw_sha != result["_acquisition_sha256"]:
        raise ResultImportError("acquisition result bytes differ from the local-window pin")
    acquisition = validate_acquisition_result(raw)
    embedded_path = _resolved_absolute_file(
        acquisition["result_path"], "acquisition result.result_path"
    )
    if embedded_path != path:
        raise ResultImportError("acquisition result_path does not identify the pinned file")
    admission = acquisition["admission"]
    source = result["source"]
    normalized_probe = admission["normalized_probe"]
    format_probe = (
        normalized_probe.get("format")
        if isinstance(normalized_probe, dict) and isinstance(normalized_probe.get("format"), dict)
        else {}
    )
    if (
        admission["media_id"] != source["media_id"]
        or admission["sha256"] != source["expected_sha256"]
        or admission["byte_count"] != source["byte_count"]
        or _resolved_absolute_file(admission["path"], "acquisition admission.path")
        != result["_source_path"]
        or format_probe.get("duration_ms") != source["duration_ms"]
    ):
        raise ResultImportError("local-window parent identity differs from its acquisition result")
    _stable_hash_file(
        result["_source_path"],
        "acquired parent media",
        expected_sha256=source["expected_sha256"],
        expected_byte_count=source["byte_count"],
    )
    return {
        "raw": raw,
        "normalized": acquisition,
        "path": path,
        "raw_sha256": raw_sha,
        # Acquisition result ingestion v1 keys its existing import batch to this
        # canonical semantic digest, while local_window.py pins the exact raw bytes.
        "catalog_import_sha256": sha256_bytes(canonical_json(raw).encode("utf-8")),
    }


def _reverify_files_inside_transaction(
    result: dict[str, Any], acquisition: dict[str, Any]
) -> None:
    result_path: Path = result["_result_path"]
    try:
        directory_stat = result_path.parent.lstat()
        entries = {entry.name for entry in result_path.parent.iterdir()}
    except OSError as error:
        raise ResultImportError(
            f"local-window result tree changed before catalog admission: {error}"
        ) from error
    if (
        stat.S_ISLNK(directory_stat.st_mode)
        or not stat.S_ISDIR(directory_stat.st_mode)
        or stat.S_IMODE(directory_stat.st_mode) != 0o500
        or entries
        != {
            "result.json",
            *(policy["filename"] for policy in ARTIFACT_KINDS.values()),
        }
    ):
        raise ResultImportError("local-window result tree changed before catalog admission")
    result_body = _stable_read(
        result_path,
        "local-window result envelope inside admission transaction",
        maximum_bytes=MAX_RESULT_BYTES,
        exact_mode=0o400,
    )
    if (
        result_body != result["_result_body"]
        or sha256_bytes(result_body) != result["_result_sha256"]
    ):
        raise ResultImportError("local-window result bytes changed before catalog admission")
    for artifact in result["artifacts"]:
        _stable_hash_file(
            Path(artifact["path"]),
            f"local-window {artifact['artifact_kind']} inside admission transaction",
            expected_sha256=artifact["sha256"],
            expected_byte_count=artifact["byte_count"],
            exact_mode=0o400,
        )
    acquisition_body = _stable_read(
        acquisition["path"],
        "pinned acquisition result inside admission transaction",
        maximum_bytes=MAX_ACQUISITION_RESULT_BYTES,
    )
    if sha256_bytes(acquisition_body) != acquisition["raw_sha256"]:
        raise ResultImportError("acquisition result changed before catalog admission")
    source_path: Path = result["_source_path"]
    current = source_path.stat()
    if {
        "device": current.st_dev,
        "inode": current.st_ino,
        "byte_count": current.st_size,
        "mtime_ns": current.st_mtime_ns,
    } != result["source"]["stat_after"]:
        raise ResultImportError("acquired parent stat evidence changed before admission")
    _stable_hash_file(
        source_path,
        "acquired parent inside admission transaction",
        expected_sha256=result["source"]["expected_sha256"],
        expected_byte_count=result["source"]["byte_count"],
    )
    for name in ("ffmpeg", "ffprobe"):
        tool = result["tools"][name]
        _stable_hash_file(
            Path(tool["path"]),
            f"pinned {name} inside admission transaction",
            expected_sha256=tool["sha256"],
            expected_byte_count=tool["byte_count"],
        )


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else dict(row)


def _require_exact_row(
    row: sqlite3.Row | None,
    expected: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    if row is None:
        raise ResultImportError(f"{label} is absent from the catalog")
    observed = dict(row)
    if observed != expected:
        raise ResultImportError(f"{label} differs from exact provenance; observed={observed!r}")
    return observed


def _require_acquisition_catalog_lineage(
    connection: sqlite3.Connection,
    result: dict[str, Any],
    acquisition: dict[str, Any],
) -> dict[str, Any]:
    envelope = acquisition["normalized"]
    parent_media_id = result["source"]["media_id"]
    import_batch = connection.execute(
        """
        SELECT import_batch_id, importer_name, input_sha256, started_at,
               completed_at, status
        FROM import_batches
        WHERE importer_name = 'acquisition_result_v1' AND input_sha256 = ?
        """,
        (acquisition["catalog_import_sha256"],),
    ).fetchone()
    if import_batch is None or import_batch["status"] != "completed":
        raise ResultImportError(
            "the pinned acquisition result has not completed the acquisition_result_v1 catalog boundary"
        )

    media = connection.execute(
        """
        SELECT media_id, sha256, byte_count, duration_ms, integrity_state
        FROM media_objects WHERE media_id = ?
        """,
        (parent_media_id,),
    ).fetchone()
    _require_exact_row(
        media,
        {
            "media_id": parent_media_id,
            "sha256": result["source"]["expected_sha256"],
            "byte_count": result["source"]["byte_count"],
            "duration_ms": result["source"]["duration_ms"],
            "integrity_state": "verified",
        },
        "acquired parent media row",
    )
    source_uri = result["_source_path"].as_uri()
    location = connection.execute(
        """
        SELECT media_id, storage_uri FROM media_locations
        WHERE media_id = ? AND storage_uri = ?
        """,
        (parent_media_id, source_uri),
    ).fetchone()
    _require_exact_row(
        location,
        {"media_id": parent_media_id, "storage_uri": source_uri},
        "acquired parent media location",
    )

    source = envelope["source"]
    canonical_source_id = source_id(source["platform"], source["source_kind"], source["native_id"])
    catalog_source = connection.execute(
        """
        SELECT source_id, platform, source_kind, native_id, access_state, review_state
        FROM sources WHERE source_id = ?
        """,
        (canonical_source_id,),
    ).fetchone()
    if catalog_source is None:
        raise ResultImportError("acquisition source is absent from the catalog")
    if (
        catalog_source["platform"] != source["platform"]
        or catalog_source["source_kind"] != source["source_kind"]
        or catalog_source["native_id"] != source["native_id"]
        or catalog_source["access_state"] not in {"public", "unknown"}
        or catalog_source["review_state"]
        not in {"metadata_only", "unreviewed", "reviewed"}
    ):
        raise ResultImportError(
            "catalog acquisition source differs from the pinned source identity/access boundary"
        )
    media_source = connection.execute(
        """
        SELECT media_id, source_id FROM media_sources
        WHERE media_id = ? AND source_id = ?
        """,
        (parent_media_id, canonical_source_id),
    ).fetchone()
    _require_exact_row(
        media_source,
        {"media_id": parent_media_id, "source_id": canonical_source_id},
        "acquisition media/source link",
    )

    acquisition_run_id = stable_id(
        "run", "media_acquisition", envelope["work_order_sha256"], parent_media_id
    )
    acquisition_run = connection.execute(
        """
        SELECT processing_run_id, stage, implementation_version, model_id,
               glossary_revision_id, parameters_json, environment_json, random_seed,
               started_at, completed_at, status, error_text
        FROM processing_runs WHERE processing_run_id = ?
        """,
        (acquisition_run_id,),
    ).fetchone()
    _require_exact_row(
        acquisition_run,
        {
            "processing_run_id": acquisition_run_id,
            "stage": "media_acquisition",
            "implementation_version": "acquisition-result-contract-v1",
            "model_id": None,
            "glossary_revision_id": None,
            "parameters_json": canonical_json(
                {
                    "adapter": envelope["adapter"],
                    "work_order_sha256": envelope["work_order_sha256"],
                    **(
                        {"handling_policy": envelope["handling_policy"]}
                        if "handling_policy" in envelope
                        else {}
                    ),
                }
            ),
            "environment_json": "{}",
            "random_seed": None,
            "started_at": envelope["started_at"],
            "completed_at": envelope["completed_at"],
            "status": "completed",
            "error_text": None,
        },
        "acquisition processing run",
    )
    acquisition_input = connection.execute(
        """
        SELECT object_type, object_id, input_role, input_sha256
        FROM run_inputs
        WHERE processing_run_id = ? AND object_type = 'source'
          AND object_id = ? AND input_role = 'retrieval_source'
        """,
        (acquisition_run_id, canonical_source_id),
    ).fetchone()
    _require_exact_row(
        acquisition_input,
        {
            "object_type": "source",
            "object_id": canonical_source_id,
            "input_role": "retrieval_source",
            "input_sha256": None,
        },
        "acquisition run input",
    )

    parent_renditions = connection.execute(
        """
        SELECT rendition.rendition_id, rendition.recording_id, rendition.review_state
        FROM renditions AS rendition
        JOIN recordings AS recording
          ON recording.recording_id = rendition.recording_id
        WHERE rendition.media_id = ?
          AND rendition.rendition_kind = 'acquired_source_media'
          AND rendition.review_state IN ('unreviewed', 'reviewed')
          AND recording.review_state IN ('metadata_only', 'unreviewed', 'reviewed')
          AND recording.merged_into_recording_id IS NULL
        ORDER BY rendition.recording_id, rendition.rendition_id
        """,
        (parent_media_id,),
    ).fetchall()
    eligible_parents: list[dict[str, Any]] = []
    source_start_ms = result["time_mapping"]["source_start_ms"]
    source_end_ms = result["time_mapping"]["source_end_ms"]
    for parent in parent_renditions:
        mappings = connection.execute(
            """
            SELECT recording_source_id, mapping_role, source_start_ms, source_end_ms,
                   recording_start_ms, recording_end_ms, mapping_method,
                   confidence_state
            FROM recording_sources
            WHERE recording_id = ? AND source_id = ?
              AND confidence_state IN ('metadata_only', 'reviewed')
            ORDER BY recording_source_id
            """,
            (parent["recording_id"], canonical_source_id),
        ).fetchall()
        eligible_mappings: list[dict[str, Any]] = []
        for mapping_row in mappings:
            mapping = dict(mapping_row)
            coordinates = (
                mapping["source_start_ms"],
                mapping["source_end_ms"],
                mapping["recording_start_ms"],
                mapping["recording_end_ms"],
            )
            explicit_coverage = (
                mapping["mapping_role"] in FULL_SOURCE_MAPPING_ROLES
                and mapping["source_start_ms"] is not None
                and mapping["source_end_ms"] is not None
                and mapping["source_start_ms"] <= source_start_ms
                and mapping["source_end_ms"] >= source_end_ms
            )
            declared_full_source = (
                all(value is None for value in coordinates)
                and mapping["mapping_role"] in FULL_SOURCE_MAPPING_ROLES
            )
            if explicit_coverage or declared_full_source:
                eligible_mappings.append(mapping)
        if eligible_mappings:
            eligible_parents.append(
                {**dict(parent), "source_mappings": eligible_mappings}
            )
    if not eligible_parents:
        raise ResultImportError(
            "acquired parent has no safe full-source or interval-covering recording "
            "mapping; ASR catalog context cannot be asserted"
        )
    return {
        "acquisition_import_batch_id": import_batch["import_batch_id"],
        "acquisition_import_sha256": acquisition["catalog_import_sha256"],
        "acquisition_processing_run_id": acquisition_run_id,
        "source_id": canonical_source_id,
        "parent_renditions": eligible_parents,
    }


def _scale_coordinate(
    value: int,
    *,
    source_start: int,
    source_end: int,
    recording_start: int,
    recording_end: int,
) -> int:
    ratio = Fraction(recording_end - recording_start, source_end - source_start)
    translated = Fraction(recording_start) + Fraction(value - source_start) * ratio
    # Python's round is deterministic and records below remain metadata_only/
    # estimated.  The underlying source-time contract stays integer-exact in
    # rendition metadata even when the recording transform is not integral.
    return round(translated)


def _recording_coordinate_plan(
    connection: sqlite3.Connection,
    *,
    parent_rendition_id: str,
    source_mappings: list[dict[str, Any]],
    source_start_ms: int,
    mapped_duration_ms: int,
) -> dict[str, Any]:
    source_end_ms = source_start_ms + mapped_duration_ms
    parent_spans = connection.execute(
        """
        SELECT timeline_map_span_id, media_start_ms, media_end_ms,
               recording_start_ms, recording_end_ms, mapping_kind,
               confidence_state
        FROM timeline_map_spans
        WHERE rendition_id = ?
          AND media_start_ms <= ? AND media_end_ms >= ?
          AND recording_start_ms IS NOT NULL AND recording_end_ms IS NOT NULL
          AND confidence_state IN ('metadata_only', 'reviewed')
          AND mapping_kind IN ('exact', 'estimated')
          AND media_end_ms > media_start_ms
        ORDER BY ordinal, timeline_map_span_id
        """,
        (parent_rendition_id, source_start_ms, source_end_ms),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for span in parent_spans:
        recording_start = _scale_coordinate(
            source_start_ms,
            source_start=span["media_start_ms"],
            source_end=span["media_end_ms"],
            recording_start=span["recording_start_ms"],
            recording_end=span["recording_end_ms"],
        )
        recording_end = _scale_coordinate(
            source_end_ms,
            source_start=span["media_start_ms"],
            source_end=span["media_end_ms"],
            recording_start=span["recording_start_ms"],
            recording_end=span["recording_end_ms"],
        )
        if recording_end > recording_start:
            candidates.append(
                {
                    "recording_start_ms": recording_start,
                    "recording_end_ms": recording_end,
                    "state": "estimated_from_parent_rendition_span",
                    "basis_ids": [span["timeline_map_span_id"]],
                }
            )

    for mapping in source_mappings:
        s_start = mapping["source_start_ms"]
        s_end = mapping["source_end_ms"]
        r_start = mapping["recording_start_ms"]
        r_end = mapping["recording_end_ms"]
        candidate: dict[str, Any] | None = None
        if (
            s_start is not None
            and s_end is not None
            and r_start is not None
            and r_end is not None
            and s_start <= source_start_ms
            and s_end >= source_end_ms
            and s_end > s_start
            and r_end > r_start
        ):
            rec_start = _scale_coordinate(
                source_start_ms,
                source_start=s_start,
                source_end=s_end,
                recording_start=r_start,
                recording_end=r_end,
            )
            rec_end = _scale_coordinate(
                source_end_ms,
                source_start=s_start,
                source_end=s_end,
                recording_start=r_start,
                recording_end=r_end,
            )
            if rec_end > rec_start:
                candidate = {
                    "recording_start_ms": rec_start,
                    "recording_end_ms": rec_end,
                    "state": "estimated_from_recording_source_span",
                    "basis_ids": [mapping["recording_source_id"]],
                }
        elif (
            s_start is not None
            and s_end is None
            and r_start is not None
            and r_end is None
            and source_start_ms >= s_start
        ):
            delta = source_start_ms - s_start
            candidate = {
                "recording_start_ms": r_start + delta,
                "recording_end_ms": r_start + delta + mapped_duration_ms,
                "state": "estimated_from_recording_source_offset",
                "basis_ids": [mapping["recording_source_id"]],
            }
        if candidate is not None:
            candidates.append(candidate)

    unique = {
        (candidate["recording_start_ms"], candidate["recording_end_ms"]): candidate
        for candidate in candidates
    }
    if len(unique) == 1:
        candidate = next(iter(unique.values()))
        basis_ids = sorted(
            {
                basis
                for item in candidates
                if (
                    item["recording_start_ms"], item["recording_end_ms"]
                ) == (
                    candidate["recording_start_ms"], candidate["recording_end_ms"]
                )
                for basis in item["basis_ids"]
            }
        )
        return {**candidate, "basis_ids": basis_ids, "timeline_mapping_kind": "estimated"}
    return {
        "recording_start_ms": None,
        "recording_end_ms": None,
        "state": "unasserted_no_unique_catalog_transform",
        "basis_ids": sorted(row["recording_source_id"] for row in source_mappings),
        "timeline_mapping_kind": "unknown",
    }


def _build_admission_plan(
    connection: sqlite3.Connection,
    result: dict[str, Any],
    acquisition: dict[str, Any],
    lineage: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    parent_media_id = result["source"]["media_id"]
    result_sha = result["_result_sha256"]
    import_batch_id = stable_id("imp", IMPORTER_NAME, result_sha)
    run_id = stable_id(
        "run",
        STAGE,
        result_sha,
        parent_media_id,
        lineage["acquisition_import_batch_id"],
    )
    parameters = {
        "contract_version": 1,
        "run_semantics": RUN_SEMANTICS,
        "local_window_result_sha256": result_sha,
        "local_window_result_uri": result["_result_path"].as_uri(),
        "local_window_implementation_version": result["implementation_version"],
        "work_order_sha256": result["work_order_sha256"],
        "bundle_id": result["bundle_id"],
        "window": result["window"],
        "time_mapping": result["time_mapping"],
        "acquisition_result_sha256": acquisition["raw_sha256"],
        "acquisition_import_sha256": lineage["acquisition_import_sha256"],
        "acquisition_import_batch_id": lineage["acquisition_import_batch_id"],
        "acquisition_processing_run_id": lineage["acquisition_processing_run_id"],
        "catalog_context_basis": [
            {
                "recording_id": parent["recording_id"],
                "parent_rendition_id": parent["rendition_id"],
                "parent_rendition_review_state": parent["review_state"],
                "source_mappings": parent["source_mappings"],
            }
            for parent in lineage["parent_renditions"]
        ],
    }
    environment = {
        "observation_timestamp_basis": "operator_supplied_catalog_admission_time",
        "producer_extraction_time_state": "not_present_in_local_window_result_v1",
        "network_access_performed": False,
        "credentials_used": False,
        "publication_authority": "none",
        "identity_claims_allowed": False,
    }
    processing_run = {
        "processing_run_id": run_id,
        "stage": STAGE,
        "implementation_version": IMPORTER_VERSION,
        "model_id": None,
        "glossary_revision_id": None,
        "parameters_json": canonical_json(parameters),
        "environment_json": canonical_json(environment),
        "random_seed": None,
        "started_at": observed_at,
        "completed_at": observed_at,
        "status": "completed",
        "error_text": None,
    }
    run_inputs = [
        {
            "run_input_id": stable_id(
                "rin", run_id, "media", parent_media_id, "verified_acquired_parent_media"
            ),
            "processing_run_id": run_id,
            "object_type": "media",
            "object_id": parent_media_id,
            "input_role": "verified_acquired_parent_media",
            "input_sha256": result["source"]["expected_sha256"],
        },
        {
            "run_input_id": stable_id(
                "rin",
                run_id,
                "import_batch",
                lineage["acquisition_import_batch_id"],
                "verified_acquisition_catalog_admission",
            ),
            "processing_run_id": run_id,
            "object_type": "import_batch",
            "object_id": lineage["acquisition_import_batch_id"],
            "input_role": "verified_acquisition_catalog_admission",
            "input_sha256": acquisition["raw_sha256"],
        },
    ]

    media_rows: list[dict[str, Any]] = []
    location_rows: list[dict[str, Any]] = []
    derivation_rows: list[dict[str, Any]] = []
    artifact_rows: list[dict[str, Any]] = []
    rendition_rows: list[dict[str, Any]] = []
    timeline_rows: list[dict[str, Any]] = []
    artifact_summaries: list[dict[str, Any]] = []
    asr_inputs: list[dict[str, Any]] = []
    window_token = f"{result['bundle_id']}:{result['window']['window_id']}"
    for artifact in sorted(result["artifacts"], key=lambda row: row["artifact_kind"]):
        kind = artifact["artifact_kind"]
        kind_policy = ARTIFACT_KINDS[kind]
        media_id = f"media_sha256_{artifact['sha256']}"
        duration_ms = artifact["normalized_probe"]["duration_ms"]
        media_rows.append(
            {
                "media_id": media_id,
                "sha256": artifact["sha256"],
                "byte_count": artifact["byte_count"],
                "media_kind": kind_policy["media_kind"],
                "mime_type": kind_policy["mime_type"],
                "container": kind_policy["container"],
                "duration_ms": duration_ms,
                "ffprobe_json": canonical_json(artifact["normalized_probe"]),
                "first_cataloged_at": observed_at,
                "integrity_state": "verified",
            }
        )
        artifact_uri = Path(artifact["path"]).as_uri()
        location_rows.append(
            {
                "media_location_id": stable_id("mlc", media_id, artifact_uri),
                "media_id": media_id,
                "storage_uri": artifact_uri,
                "storage_class": "private_local",
                "verified_at": observed_at,
                "is_primary": 1,
            }
        )
        evidence = {
            "contract_version": 1,
            "run_semantics": RUN_SEMANTICS,
            "local_window_result_sha256": result_sha,
            "local_window_result_uri": result["_result_path"].as_uri(),
            "acquisition_result_sha256": acquisition["raw_sha256"],
            "acquisition_import_batch_id": lineage["acquisition_import_batch_id"],
            "source_media_id": parent_media_id,
            "source_time_mapping": result["time_mapping"],
            "window": result["window"],
            "normalized_probe": artifact["normalized_probe"],
            "boundary_calibration_state": "not_calibrated",
            "representation_is_original_source": False,
            "publication_state": "withheld_by_default",
            "identity_authority": "none",
        }
        derivation_kind = f"local_window:{kind}:{window_token}"
        derivation_rows.append(
            {
                "child_media_id": media_id,
                "parent_media_id": parent_media_id,
                "derivation_kind": derivation_kind,
                "processing_run_id": run_id,
                "metadata_json": canonical_json(evidence),
            }
        )
        artifact_rows.append(
            {
                "artifact_id": artifact["artifact_id"],
                "processing_run_id": run_id,
                "artifact_kind": kind,
                "storage_uri": artifact_uri,
                "sha256": artifact["sha256"],
                "byte_count": artifact["byte_count"],
                "schema_version": 1,
                "visibility": "private",
                "metadata_json": canonical_json({**evidence, "media_id": media_id}),
            }
        )
        rendition_contexts: list[dict[str, str]] = []
        mapped_duration = min(
            duration_ms,
            result["time_mapping"]["source_end_ms"]
            - result["time_mapping"]["source_start_ms"],
        )
        for parent in lineage["parent_renditions"]:
            recording_id = parent["recording_id"]
            rendition_kind = f"local_window:{kind}:{window_token}"
            rendition_id = stable_id("rnd", recording_id, media_id, rendition_kind)
            recording_mapping = _recording_coordinate_plan(
                connection,
                parent_rendition_id=parent["rendition_id"],
                source_mappings=parent["source_mappings"],
                source_start_ms=result["time_mapping"]["source_start_ms"],
                mapped_duration_ms=mapped_duration,
            )
            rendition_metadata = {
                **evidence,
                "parent_rendition_id": parent["rendition_id"],
                "recording_coordinate_mapping": recording_mapping,
            }
            rendition_rows.append(
                {
                    "rendition_id": rendition_id,
                    "recording_id": recording_id,
                    "media_id": media_id,
                    "rendition_kind": rendition_kind,
                    "label": (
                        f"{kind_policy['rendition_label']} "
                        f"[{result['window']['start_ms']}, {result['window']['end_ms']}) ms"
                    ),
                    "review_state": "unreviewed",
                    "metadata_json": canonical_json(rendition_metadata),
                }
            )
            timeline_rows.append(
                {
                    "timeline_map_span_id": stable_id("tms", rendition_id, 0),
                    "rendition_id": rendition_id,
                    "ordinal": 0,
                    "media_start_ms": 0,
                    "media_end_ms": mapped_duration,
                    "recording_start_ms": recording_mapping["recording_start_ms"],
                    "recording_end_ms": recording_mapping["recording_end_ms"],
                    "mapping_kind": recording_mapping["timeline_mapping_kind"],
                    "confidence_state": "metadata_only",
                }
            )
            rendition_contexts.append(
                {"recording_id": recording_id, "rendition_id": rendition_id}
            )
            if kind == "window_audio_16khz_mono_flac":
                asr_inputs.append(
                    {
                        "input": {
                            "path": artifact["path"],
                            "expected_sha256": artifact["sha256"],
                            "media_id": media_id,
                            "artifact_id": artifact["artifact_id"],
                            "parent_processing_run_id": run_id,
                        },
                        "catalog_context": {
                            "recording_id": recording_id,
                            "rendition_id": rendition_id,
                        },
                        "window": {"offset_ms": 0, "duration_ms": None},
                        "source_time_mapping": result["time_mapping"],
                        "boundary_calibration_state": "not_calibrated",
                    }
                )
        artifact_summaries.append(
            {
                "artifact_id": artifact["artifact_id"],
                "artifact_kind": kind,
                "media_id": media_id,
                "path": artifact["path"],
                "sha256": artifact["sha256"],
                "byte_count": artifact["byte_count"],
                "duration_ms": duration_ms,
                "visibility": "private",
                "rendition_contexts": rendition_contexts,
            }
        )
    counts = {
        "processing_runs": 1,
        "run_inputs": len(run_inputs),
        "media_objects": len(media_rows),
        "media_locations": len(location_rows),
        "media_derivations": len(derivation_rows),
        "artifacts": len(artifact_rows),
        "renditions": len(rendition_rows),
        "timeline_map_spans": len(timeline_rows),
        "publication_decisions": 0,
        "identity_claims": 0,
    }
    public = {
        "schema_version": 1,
        "status": "validated",
        "importer_version": IMPORTER_VERSION,
        "catalog_package_version": __version__,
        "observed_at": observed_at,
        "import_batch_id": import_batch_id,
        "result_sha256": result_sha,
        "result_byte_count": result["_result_byte_count"],
        "acquisition_result_sha256": acquisition["raw_sha256"],
        "acquisition_import_sha256": lineage["acquisition_import_sha256"],
        "acquisition_import_batch_id": lineage["acquisition_import_batch_id"],
        "acquisition_processing_run_id": lineage["acquisition_processing_run_id"],
        "admission_processing_run_id": run_id,
        "run_semantics": RUN_SEMANTICS,
        "parent_media_id": parent_media_id,
        "source_id": lineage["source_id"],
        "window": result["window"],
        "time_mapping": result["time_mapping"],
        "artifacts": artifact_summaries,
        "asr_work_order_inputs": sorted(
            asr_inputs,
            key=lambda row: (
                row["catalog_context"]["recording_id"],
                row["catalog_context"]["rendition_id"],
            ),
        ),
        "statistics": counts,
        "safety": {
            "visibility": "private",
            "publication_authority": "none",
            "identity_authority": "none",
            "network_access_performed": False,
            "credentials_used": False,
            "producer_extraction_time_known": False,
            "source_boundary_calibrated": False,
        },
    }
    return {
        "public": public,
        "processing_run": processing_run,
        "run_inputs": run_inputs,
        "media_objects": media_rows,
        "media_locations": location_rows,
        "media_derivations": derivation_rows,
        "artifacts": artifact_rows,
        "renditions": rendition_rows,
        "timeline_map_spans": timeline_rows,
    }


def _insert_exact_processing_run(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = (
        "stage", "implementation_version", "model_id", "glossary_revision_id",
        "parameters_json", "environment_json", "random_seed", "started_at",
        "completed_at", "status", "error_text",
    )
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM processing_runs WHERE processing_run_id = ?",
        (row["processing_run_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window admission processing_run_id collision")
        return
    connection.execute(
        """
        INSERT INTO processing_runs(
            processing_run_id, stage, implementation_version, model_id,
            glossary_revision_id, parameters_json, environment_json, random_seed,
            started_at, completed_at, status, error_text
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (row["processing_run_id"], *(row[column] for column in columns)),
    )


def _insert_exact_run_input(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = ("processing_run_id", "object_type", "object_id", "input_role", "input_sha256")
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM run_inputs WHERE run_input_id = ?",
        (row["run_input_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window admission run_input_id collision")
        return
    logical = connection.execute(
        """
        SELECT run_input_id FROM run_inputs
        WHERE processing_run_id = ? AND object_type = ? AND object_id = ? AND input_role = ?
        """,
        (
            row["processing_run_id"], row["object_type"], row["object_id"], row["input_role"]
        ),
    ).fetchone()
    if logical is not None and logical["run_input_id"] != row["run_input_id"]:
        raise ResultImportError("local-window logical run input uses a different ID")
    connection.execute(
        """
        INSERT INTO run_inputs(
            run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256
        ) VALUES(?, ?, ?, ?, ?, ?)
        """,
        (row["run_input_id"], *(row[column] for column in columns)),
    )


def _insert_exact_media_object(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    identity_rows = connection.execute(
        """
        SELECT media_id, sha256, byte_count FROM media_objects
        WHERE media_id = ? OR sha256 = ?
        """,
        (row["media_id"], row["sha256"]),
    ).fetchall()
    for identity in identity_rows:
        if dict(identity) != {
            "media_id": row["media_id"],
            "sha256": row["sha256"],
            "byte_count": row["byte_count"],
        }:
            raise ResultImportError("local-window derivative media identity collision")
    existing = connection.execute(
        """
        SELECT media_kind, mime_type, container, duration_ms, ffprobe_json,
               first_cataloged_at, integrity_state
        FROM media_objects WHERE media_id = ?
        """,
        (row["media_id"],),
    ).fetchone()
    if existing is not None:
        expected = {
            key: row[key]
            for key in (
                "media_kind", "mime_type", "container", "duration_ms", "ffprobe_json",
                "integrity_state",
            )
        }
        observed = dict(existing)
        existing_first_cataloged = observed.pop("first_cataloged_at")
        if observed != expected or _timestamp_value(existing_first_cataloged) > _timestamp_value(
            row["first_cataloged_at"]
        ):
            raise ResultImportError("local-window derivative media row collision")
        return
    connection.execute(
        """
        INSERT INTO media_objects(
            media_id, sha256, byte_count, media_kind, mime_type, container,
            duration_ms, ffprobe_json, first_cataloged_at, integrity_state
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["media_id"], row["sha256"], row["byte_count"], row["media_kind"],
            row["mime_type"], row["container"], row["duration_ms"], row["ffprobe_json"],
            row["first_cataloged_at"], row["integrity_state"],
        ),
    )


def _insert_exact_media_location(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = ("media_id", "storage_uri", "storage_class", "verified_at", "is_primary")
    occupants = connection.execute(
        "SELECT media_location_id, media_id FROM media_locations WHERE storage_uri = ?",
        (row["storage_uri"],),
    ).fetchall()
    if any(occupant["media_id"] != row["media_id"] for occupant in occupants):
        raise ResultImportError(
            "local-window storage URI is already bound to different media bytes"
        )
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM media_locations WHERE media_location_id = ?",
        (row["media_location_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window media_location_id collision")
        return
    logical = connection.execute(
        """
        SELECT media_location_id FROM media_locations
        WHERE media_id = ? AND storage_uri = ?
        """,
        (row["media_id"], row["storage_uri"]),
    ).fetchone()
    if logical is not None and logical["media_location_id"] != row["media_location_id"]:
        raise ResultImportError("local-window derivative location uses a different ID")
    connection.execute(
        """
        INSERT INTO media_locations(
            media_location_id, media_id, storage_uri, storage_class, verified_at, is_primary
        ) VALUES(?, ?, ?, ?, ?, ?)
        """,
        (row["media_location_id"], *(row[column] for column in columns)),
    )


def _insert_exact_derivation(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = ("processing_run_id", "metadata_json")
    existing = connection.execute(
        """
        SELECT processing_run_id, metadata_json FROM media_derivations
        WHERE child_media_id = ? AND parent_media_id = ? AND derivation_kind = ?
        """,
        (row["child_media_id"], row["parent_media_id"], row["derivation_kind"]),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window media derivation collision")
        return
    connection.execute(
        """
        INSERT INTO media_derivations(
            child_media_id, parent_media_id, derivation_kind, processing_run_id, metadata_json
        ) VALUES(?, ?, ?, ?, ?)
        """,
        (
            row["child_media_id"], row["parent_media_id"], row["derivation_kind"],
            row["processing_run_id"], row["metadata_json"],
        ),
    )


def _insert_exact_artifact(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = (
        "processing_run_id", "artifact_kind", "storage_uri", "sha256", "byte_count",
        "schema_version", "visibility", "metadata_json",
    )
    occupants = connection.execute(
        "SELECT artifact_id, sha256 FROM artifacts WHERE storage_uri = ?",
        (row["storage_uri"],),
    ).fetchall()
    if any(
        occupant["artifact_id"] != row["artifact_id"]
        or occupant["sha256"] != row["sha256"]
        for occupant in occupants
    ):
        raise ResultImportError(
            "local-window artifact URI is already bound to different admitted bytes"
        )
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM artifacts WHERE artifact_id = ?",
        (row["artifact_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window artifact_id collision")
        return
    logical = connection.execute(
        "SELECT artifact_id FROM artifacts WHERE storage_uri = ? AND sha256 = ?",
        (row["storage_uri"], row["sha256"]),
    ).fetchone()
    if logical is not None and logical["artifact_id"] != row["artifact_id"]:
        raise ResultImportError("local-window artifact bytes/location use a different ID")
    connection.execute(
        """
        INSERT INTO artifacts(
            artifact_id, processing_run_id, artifact_kind, storage_uri, sha256,
            byte_count, schema_version, visibility, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (row["artifact_id"], *(row[column] for column in columns)),
    )


def _require_no_window_replacement(
    connection: sqlite3.Connection,
    result: dict[str, Any],
) -> None:
    rows = connection.execute(
        """
        SELECT processing_run_id, parameters_json
        FROM processing_runs
        WHERE stage = ?
          AND json_extract(parameters_json, '$.bundle_id') = ?
          AND json_extract(parameters_json, '$.window.window_id') = ?
        ORDER BY processing_run_id
        """,
        (STAGE, result["bundle_id"], result["window"]["window_id"]),
    ).fetchall()
    for row in rows:
        parameters = json.loads(row["parameters_json"])
        if (
            parameters.get("local_window_result_sha256") != result["_result_sha256"]
            or parameters.get("local_window_result_uri")
            != result["_result_path"].as_uri()
        ):
            raise ResultImportError(
                "this producer bundle/window was already admitted from different result bytes"
            )


def _insert_exact_rendition(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = (
        "recording_id", "media_id", "rendition_kind", "label", "review_state", "metadata_json"
    )
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM renditions WHERE rendition_id = ?",
        (row["rendition_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window rendition_id collision")
        return
    logical = connection.execute(
        """
        SELECT rendition_id FROM renditions
        WHERE recording_id = ? AND media_id = ? AND rendition_kind = ?
        """,
        (row["recording_id"], row["media_id"], row["rendition_kind"]),
    ).fetchone()
    if logical is not None and logical["rendition_id"] != row["rendition_id"]:
        raise ResultImportError("local-window logical rendition uses a different ID")
    connection.execute(
        """
        INSERT INTO renditions(
            rendition_id, recording_id, media_id, rendition_kind,
            label, review_state, metadata_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (row["rendition_id"], *(row[column] for column in columns)),
    )


def _insert_exact_timeline_span(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    columns = (
        "rendition_id", "ordinal", "media_start_ms", "media_end_ms",
        "recording_start_ms", "recording_end_ms", "mapping_kind", "confidence_state",
    )
    existing = connection.execute(
        f"SELECT {', '.join(columns)} FROM timeline_map_spans WHERE timeline_map_span_id = ?",
        (row["timeline_map_span_id"],),
    ).fetchone()
    if existing is not None:
        if any(existing[column] != row[column] for column in columns):
            raise ResultImportError("local-window timeline_map_span_id collision")
        return
    logical = connection.execute(
        "SELECT timeline_map_span_id FROM timeline_map_spans WHERE rendition_id = ? AND ordinal = ?",
        (row["rendition_id"], row["ordinal"]),
    ).fetchone()
    if logical is not None and logical["timeline_map_span_id"] != row["timeline_map_span_id"]:
        raise ResultImportError("local-window rendition timeline ordinal uses a different ID")
    connection.execute(
        """
        INSERT INTO timeline_map_spans(
            timeline_map_span_id, rendition_id, ordinal, media_start_ms, media_end_ms,
            recording_start_ms, recording_end_ms, mapping_kind, confidence_state
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (row["timeline_map_span_id"], *(row[column] for column in columns)),
    )


def _insert_exact_import_batch(
    connection: sqlite3.Connection,
    plan: dict[str, Any],
) -> None:
    public = plan["public"]
    statistics_json = canonical_json(public["statistics"])
    expected = {
        "importer_name": IMPORTER_NAME,
        "importer_version": IMPORTER_VERSION,
        "input_sha256": public["result_sha256"],
        "source_snapshot_date": None,
        "started_at": public["observed_at"],
        "completed_at": public["observed_at"],
        "status": "completed",
        "statistics_json": statistics_json,
    }
    existing = connection.execute(
        """
        SELECT importer_name, importer_version, input_sha256, source_snapshot_date,
               started_at, completed_at, status, statistics_json
        FROM import_batches WHERE import_batch_id = ?
        """,
        (public["import_batch_id"],),
    ).fetchone()
    if existing is not None:
        if dict(existing) != expected:
            raise ResultImportError("local-window import batch ID already has different data")
        return
    logical = connection.execute(
        "SELECT import_batch_id FROM import_batches WHERE importer_name = ? AND input_sha256 = ?",
        (IMPORTER_NAME, public["result_sha256"]),
    ).fetchone()
    if logical is not None and logical["import_batch_id"] != public["import_batch_id"]:
        raise ResultImportError("local-window result digest uses a different import batch ID")
    connection.execute(
        """
        INSERT INTO import_batches(
            import_batch_id, importer_name, importer_version, input_sha256,
            source_snapshot_date, started_at, completed_at, status, statistics_json
        ) VALUES(?, ?, ?, ?, NULL, ?, ?, 'completed', ?)
        """,
        (
            public["import_batch_id"], IMPORTER_NAME, IMPORTER_VERSION,
            public["result_sha256"], public["observed_at"], public["observed_at"],
            statistics_json,
        ),
    )


def _prepare(
    connection: sqlite3.Connection,
    result_path: str | Path,
    *,
    observed_at: str,
) -> dict[str, Any]:
    normalized_observed_at = _timestamp(observed_at, "--observed-at")
    result = _read_sealed_result(result_path)
    acquisition = _read_acquisition(result)
    if _timestamp_value(normalized_observed_at) < _timestamp_value(
        acquisition["normalized"]["completed_at"]
    ):
        raise ResultImportError(
            "--observed-at cannot precede the completed acquisition it verifies"
        )
    lineage = _require_acquisition_catalog_lineage(connection, result, acquisition)
    plan = _build_admission_plan(
        connection, result, acquisition, lineage, normalized_observed_at
    )
    plan["_result"] = result
    plan["_acquisition"] = acquisition
    return plan


def validate_local_window_result_file(
    connection: sqlite3.Connection,
    result_path: str | Path,
    *,
    observed_at: str,
) -> dict[str, Any]:
    """Validate a sealed result and catalog lineage without writing any rows."""

    plan = _prepare(connection, result_path, observed_at=observed_at)
    return dict(plan["public"])


def import_local_window_result(
    connection: sqlite3.Connection,
    result_path: str | Path,
    *,
    observed_at: str,
) -> dict[str, Any]:
    """Admit a verified local window as private catalog-backed ASR input."""

    preflight = _prepare(connection, result_path, observed_at=observed_at)
    with transaction(connection):
        result = preflight["_result"]
        acquisition = preflight["_acquisition"]
        _reverify_files_inside_transaction(result, acquisition)
        lineage = _require_acquisition_catalog_lineage(
            connection, result, acquisition
        )
        plan = _build_admission_plan(
            connection,
            result,
            acquisition,
            lineage,
            preflight["public"]["observed_at"],
        )
        if plan["public"] != preflight["public"]:
            raise ResultImportError(
                "catalog lineage/context changed between validation and admission"
            )
        _require_no_window_replacement(connection, result)
        _insert_exact_processing_run(connection, plan["processing_run"])
        for row in plan["run_inputs"]:
            _insert_exact_run_input(connection, row)
        for row in plan["media_objects"]:
            _insert_exact_media_object(connection, row)
        for row in plan["media_locations"]:
            _insert_exact_media_location(connection, row)
        for row in plan["media_derivations"]:
            _insert_exact_derivation(connection, row)
        for row in plan["artifacts"]:
            _insert_exact_artifact(connection, row)
        for row in plan["renditions"]:
            _insert_exact_rendition(connection, row)
        for row in plan["timeline_map_spans"]:
            _insert_exact_timeline_span(connection, row)
        _insert_exact_import_batch(connection, plan)
    public = dict(plan["public"])
    public["status"] = "admitted"
    return public
