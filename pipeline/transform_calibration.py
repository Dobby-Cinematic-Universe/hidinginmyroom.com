#!/usr/bin/env python3
"""Private, catalog-free source/recording transform calibration evidence.

The producer compares one earlier guarded yt-dlp acquisition with a finalized
credential-free reacquisition of the same exact YouTube formats.  It hashes and
revalidates both acquisition envelopes, probes both local media objects, performs
distributed visual and audio comparisons, and writes one deterministic private
receipt.  The receipt is evidence only: this program never opens SQLite, creates a
review decision, asserts a relationship, projects transcript coordinates, or grants
publication authority.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import stat
import subprocess
import sys
import uuid
from fractions import Fraction
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
STAGE = "private_source_recording_transform_calibration"
ALGORITHM = "distributed_av_shared_prefix_affine_v1"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ACQUISITION_MODULE = REPOSITORY_ROOT / "acquisition" / "acquire.py"
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_MEDIA_BYTES = 16 * 1024**3
MAX_CHECKPOINTS = 16
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SSIM_RE = re.compile(
    r"SSIM Y:([0-9.]+) \([^)]*\) U:([0-9.]+) \([^)]*\) "
    r"V:([0-9.]+) \([^)]*\) All:([0-9.]+) \([^)]*\)"
)


class TransformCalibrationError(RuntimeError):
    """A closed input, tool, comparison, or receipt invariant failed."""


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


def stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(list(parts)))[:32]}"


def duplicate_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise TransformCalibrationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path, label: str) -> tuple[dict[str, Any], bytes]:
    binding = regular_file(path, label)
    if binding["byte_count"] > MAX_JSON_BYTES:
        raise TransformCalibrationError(f"{label} exceeds {MAX_JSON_BYTES} bytes")
    body = path.read_bytes()
    try:
        value = json.loads(body, object_pairs_hook=duplicate_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TransformCalibrationError(f"cannot parse {label}: {error}") from error
    if not isinstance(value, dict):
        raise TransformCalibrationError(f"{label} must contain one JSON object")
    return value, body


def string(value: object, label: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise TransformCalibrationError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: object, label: str) -> str:
    text = string(value, label, 128)
    if not ID_RE.fullmatch(text):
        raise TransformCalibrationError(f"{label} contains unsupported characters")
    return text


def positive_integer(value: object, label: str, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > maximum:
        raise TransformCalibrationError(f"{label} must be an integer in [1,{maximum}]")
    return value


def nonnegative_integer(value: object, label: str, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > maximum:
        raise TransformCalibrationError(f"{label} must be an integer in [0,{maximum}]")
    return value


def absolute_regular_path(value: object, label: str) -> Path:
    text = string(value, label)
    if "://" in text:
        raise TransformCalibrationError(f"{label} must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute():
        raise TransformCalibrationError(f"{label} must be absolute")
    try:
        mode = path.lstat().st_mode
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise TransformCalibrationError(f"cannot resolve {label}: {error}") from error
    if path != resolved or stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise TransformCalibrationError(f"{label} must be a resolved regular file")
    return path


def regular_file(
    path: Path,
    label: str,
    maximum: int = MAX_MEDIA_BYTES,
    *,
    require_single_link: bool = True,
    allow_empty: bool = False,
) -> dict[str, Any]:
    path = absolute_regular_path(str(path), label)
    before = path.stat()
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    if require_single_link and before.st_nlink != 1:
        raise TransformCalibrationError(f"{label} must have exactly one hard link")
    if (before.st_size == 0 and not allow_empty) or before.st_size < 0 or before.st_size > maximum:
        raise TransformCalibrationError(f"{label} has an unsupported byte count")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        opened = os.fstat(handle.fileno())
        if identity != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns):
            raise TransformCalibrationError(f"{label} changed while opening")
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
        after_fd = os.fstat(handle.fileno())
    after = path.stat()
    if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise TransformCalibrationError(f"{label} changed while hashing")
    if identity != (
        after_fd.st_dev,
        after_fd.st_ino,
        after_fd.st_size,
        after_fd.st_mtime_ns,
    ):
        raise TransformCalibrationError(f"{label} changed while reading")
    return {
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": digest.hexdigest(),
        "byte_count": before.st_size,
        "mode": stat.S_IMODE(before.st_mode),
        "nlink": before.st_nlink,
        "stat": {
            "device": before.st_dev,
            "inode": before.st_ino,
            "mtime_ns": before.st_mtime_ns,
        },
        "unchanged": True,
    }


def _load_acquisition_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "himr_transform_calibration_acquisition", ACQUISITION_MODULE
    )
    if spec is None or spec.loader is None:
        raise TransformCalibrationError("cannot load guarded acquisition validator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _minimal_remote_metadata(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TransformCalibrationError(f"{label} selected metadata is invalid")
    required = {
        "id": str,
        "format_id": str,
        "duration": (int, float),
        "live_status": str,
        "availability": str,
        "timestamp": int,
        "upload_date": str,
    }
    for key, kind in required.items():
        if isinstance(value.get(key), bool) or not isinstance(value.get(key), kind):
            raise TransformCalibrationError(f"{label} metadata.{key} has an invalid type")
    return {key: value[key] for key in required}


def validate_acquisition_side(
    role: str, work_order_path: Path, result_path: Path
) -> dict[str, Any]:
    acquisition = _load_acquisition_module()
    work_order_raw, work_order_body = load_json(work_order_path, f"{role} work order")
    result, result_body = load_json(result_path, f"{role} acquisition result")
    try:
        work_order = acquisition.validate_work_order(work_order_raw)
    except Exception as error:  # acquisition owns its closed error type
        raise TransformCalibrationError(
            f"{role} acquisition work order is invalid: {error}"
        ) from error
    output_root = Path(work_order["output"]["root"])
    try:
        reusable = acquisition.validate_reusable_result(
            result,
            output_root,
            work_order,
            result_path=result_path,
        )
    except Exception as error:
        raise TransformCalibrationError(
            f"{role} acquisition result validation failed: {error}"
        ) from error
    if not reusable:
        raise TransformCalibrationError(f"{role} acquisition result is not reusable exact evidence")
    source = work_order["source"]
    if (
        work_order["adapter"] != "yt_dlp"
        or source["platform"] != "youtube"
        or source["access_state"] != "public"
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
    ):
        raise TransformCalibrationError(f"{role} is not a completed public YouTube acquisition")
    metadata = _minimal_remote_metadata(result["selected_remote_metadata"], role)
    if metadata["id"] != source["native_id"] or metadata["availability"] != "public":
        raise TransformCalibrationError(f"{role} public source identity is inconsistent")
    media_path = absolute_regular_path(result["admission"]["path"], f"{role} media")
    media = regular_file(media_path, f"{role} media")
    if (
        media["sha256"] != result["admission"]["sha256"]
        or media["byte_count"] != result["admission"]["byte_count"]
    ):
        raise TransformCalibrationError(f"{role} media differs from its acquisition admission")
    yt = result["source_observation"]["yt_dlp"]
    return {
        "role": role,
        "work_order": {
            **regular_file(work_order_path, f"{role} work order", MAX_JSON_BYTES),
            "canonical_sha256": sha256_bytes(acquisition.canonical_bytes(work_order)),
        },
        "result": regular_file(result_path, f"{role} acquisition result", MAX_JSON_BYTES),
        "job_id": result["job_id"],
        "work_order_sha256": result["work_order_sha256"],
        "source": {
            "platform": source["platform"],
            "native_id": source["native_id"],
            "canonical_url": source["canonical_url"],
            "access_state": source["access_state"],
        },
        "selected_remote_metadata": metadata,
        "media": media,
        "acquisition_probe": result["admission"]["normalized_probe"],
        "yt_dlp": {
            "executable": yt["executable"],
            "executable_sha256": yt["executable_sha256"],
            "version": yt["version"],
            "stat": yt["stat_before"],
            "unchanged": yt["unchanged"],
        },
        "credential_free": True,
        "publication_authority": "none",
        "raw_work_order_sha256": sha256_bytes(work_order_body),
        "raw_result_sha256": sha256_bytes(result_body),
    }


def inspect_executable(path: Path, label: str, version_arguments: list[str]) -> dict[str, Any]:
    binding = regular_file(path, label, 1024 * 1024 * 1024)
    try:
        completed = subprocess.run(
            [str(path), *version_arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TransformCalibrationError(f"cannot inspect {label}: {error}") from error
    lines = completed.stdout.splitlines()
    if not lines or not lines[0] or len(lines[0]) > 1000:
        raise TransformCalibrationError(f"{label} version output is invalid")
    after = regular_file(path, label, 1024 * 1024 * 1024)
    if binding != after:
        raise TransformCalibrationError(f"{label} changed during version inspection")
    return {**binding, "version_line": lines[0], "unchanged_during_run": True}


def package_tree(package: Any, label: str) -> dict[str, Any]:
    init_path = absolute_regular_path(package.__file__, f"{label} module")
    root = init_path.parent
    rows: list[dict[str, Any]] = []
    total = 0
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path.is_symlink() or "__pycache__" in path.parts:
            continue
        if path.suffix in {".pyc", ".pyo"}:
            continue
        relative = path.relative_to(root).as_posix()
        binding = regular_file(
            path.resolve(),
            f"{label} runtime file",
            1024 * 1024 * 1024,
            require_single_link=False,
            allow_empty=True,
        )
        total += binding["byte_count"]
        if len(rows) >= 20_000 or total > 2 * 1024**3:
            raise TransformCalibrationError(f"{label} runtime tree exceeds safety bounds")
        rows.append(
            {
                "path": relative,
                "sha256": binding["sha256"],
                "byte_count": binding["byte_count"],
            }
        )
    if not rows:
        raise TransformCalibrationError(f"{label} runtime tree is empty")
    return {
        "name": label,
        "version": string(package.__version__, f"{label} version", 200),
        "root": str(root),
        "tree_sha256": sha256_bytes(canonical_bytes(rows)),
        "file_count": len(rows),
        "byte_count": total,
    }


def fraction_text(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def rounded_milliseconds(value: Fraction) -> int:
    milliseconds = value * 1000
    return (milliseconds.numerator * 2 + milliseconds.denominator) // (
        2 * milliseconds.denominator
    )


def parse_fraction(value: object, label: str) -> Fraction:
    text = string(value, label, 100)
    try:
        result = Fraction(text)
    except (ValueError, ZeroDivisionError) as error:
        raise TransformCalibrationError(f"{label} is not a rational number") from error
    if result <= 0:
        raise TransformCalibrationError(f"{label} must be positive")
    return result


def exact_probe(ffprobe: Path, media: Path, label: str) -> dict[str, Any]:
    command = [
        str(ffprobe),
        "-v",
        "error",
        "-show_entries",
        (
            "format=format_name,start_time,duration,size:"
            "stream=index,codec_name,codec_type,time_base,start_pts,start_time,"
            "duration_ts,duration,nb_frames,r_frame_rate,avg_frame_rate,sample_rate,"
            "channels,width,height,pix_fmt"
        ),
        "-of",
        "json",
        str(media),
    ]
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
            check=True,
        )
        raw = json.loads(completed.stdout, object_pairs_hook=duplicate_object)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        raise TransformCalibrationError(f"cannot probe {label}: {error}") from error
    streams = raw.get("streams")
    format_value = raw.get("format")
    if not isinstance(streams, list) or not isinstance(format_value, dict):
        raise TransformCalibrationError(f"{label} probe lacks format or streams")
    normalized_streams: list[dict[str, Any]] = []
    for stream in streams:
        if not isinstance(stream, dict) or stream.get("codec_type") not in {"video", "audio"}:
            continue
        time_base = parse_fraction(stream.get("time_base"), f"{label} stream time_base")
        duration_ts = positive_integer(
            stream.get("duration_ts"), f"{label} stream duration_ts"
        )
        duration = time_base * duration_ts
        start_pts = nonnegative_integer(stream.get("start_pts"), f"{label} stream start_pts")
        row = {
            "index": nonnegative_integer(stream.get("index"), f"{label} stream index", 1000),
            "codec_type": stream["codec_type"],
            "codec_name": string(stream.get("codec_name"), f"{label} stream codec", 100),
            "time_base": fraction_text(time_base),
            "start_pts": start_pts,
            "start_time": string(stream.get("start_time"), f"{label} stream start_time", 100),
            "duration_ts": duration_ts,
            "duration": string(stream.get("duration"), f"{label} stream duration", 100),
            "duration_ms": rounded_milliseconds(duration),
            "r_frame_rate": string(stream.get("r_frame_rate", "0/0"), f"{label} rate", 100),
            "avg_frame_rate": string(
                stream.get("avg_frame_rate", "0/0"), f"{label} average rate", 100
            ),
            "nb_frames": stream.get("nb_frames"),
            "sample_rate": stream.get("sample_rate"),
            "channels": stream.get("channels"),
            "width": stream.get("width"),
            "height": stream.get("height"),
            "pixel_format": stream.get("pix_fmt"),
        }
        normalized_streams.append(row)
    video = [row for row in normalized_streams if row["codec_type"] == "video"]
    audio = [row for row in normalized_streams if row["codec_type"] == "audio"]
    if len(video) != 1 or len(audio) != 1:
        raise TransformCalibrationError(f"{label} must have exactly one audio and video stream")
    if video[0]["start_pts"] != 0 or audio[0]["start_pts"] != 0:
        raise TransformCalibrationError(f"{label} streams must start at PTS zero")
    return {
        "command": command,
        "raw_stdout_sha256": sha256_bytes(completed.stdout),
        "format": {
            "format_name": string(format_value.get("format_name"), f"{label} format", 200),
            "start_time": string(format_value.get("start_time"), f"{label} start", 100),
            "duration": string(format_value.get("duration"), f"{label} duration", 100),
            "size": positive_integer(int(format_value.get("size")), f"{label} probe size"),
        },
        "streams": sorted(normalized_streams, key=lambda row: row["index"]),
        "video_duration_ms": video[0]["duration_ms"],
        "audio_duration_ms": audio[0]["duration_ms"],
    }


def seconds_text(milliseconds: Fraction | int) -> str:
    value = Fraction(milliseconds, 1000) if isinstance(milliseconds, int) else milliseconds / 1000
    return f"{float(value):.9f}"


def visual_ssim(
    ffmpeg: Path,
    old_media: Path,
    finalized_media: Path,
    old_start_ms: int,
    finalized_start_ms: Fraction,
    window_ms: int,
) -> dict[str, Any]:
    command = [
        str(ffmpeg),
        "-hide_banner",
        "-nostdin",
        "-threads",
        "1",
        "-ss",
        seconds_text(old_start_ms),
        "-t",
        seconds_text(window_ms),
        "-i",
        str(old_media),
        "-ss",
        seconds_text(finalized_start_ms),
        "-t",
        seconds_text(window_ms),
        "-i",
        str(finalized_media),
        "-filter_complex",
        "[0:v:0]settb=AVTB,setpts=PTS-STARTPTS[v0];"
        "[1:v:0]settb=AVTB,setpts=PTS-STARTPTS[v1];[v0][v1]ssim",
        "-an",
        "-f",
        "null",
        "-",
    ]
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=180,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TransformCalibrationError(f"visual comparison failed: {error}") from error
    matches = SSIM_RE.findall(completed.stderr)
    if len(matches) != 1:
        raise TransformCalibrationError("visual comparison emitted no unique SSIM summary")
    y, u, v, overall = (round(float(item), 9) for item in matches[0])
    summary = {
        "ssim_y": y,
        "ssim_u": u,
        "ssim_v": v,
        "ssim_all": overall,
    }
    return {
        "old_start_ms": old_start_ms,
        "finalized_start": fraction_text(finalized_start_ms),
        "window_ms": window_ms,
        **summary,
        # FFmpeg prefixes the summary with a process-specific filter pointer.
        # Hash only the parsed numeric payload, never the nondeterministic log line.
        "summary_sha256": sha256_bytes(canonical_bytes(summary)),
    }


def decode_pcm(
    ffmpeg: Path,
    media: Path,
    start_ms: Fraction,
    duration_ms: int,
    sample_rate_hz: int,
) -> bytes:
    command = [
        str(ffmpeg),
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "error",
        "-threads",
        "1",
        "-ss",
        seconds_text(start_ms),
        "-t",
        seconds_text(duration_ms),
        "-i",
        str(media),
        "-map",
        "0:a:0",
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(sample_rate_hz),
        "-f",
        "s16le",
        "pipe:1",
    ]
    try:
        body = subprocess.check_output(
            command, stdin=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=180
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TransformCalibrationError(f"audio decode failed: {error}") from error
    if len(body) % 2:
        raise TransformCalibrationError("audio decode returned an odd PCM byte count")
    return body


def audio_alignment(
    *,
    scipy_signal: Any,
    numpy: Any,
    ffmpeg: Path,
    old_media: Path,
    finalized_media: Path,
    old_start_ms: int,
    target_start_ms: Fraction,
    window_ms: int,
    search_radius_ms: int,
    sample_rate_hz: int,
) -> dict[str, Any]:
    expected_samples = window_ms * sample_rate_hz // 1000
    if expected_samples <= 0:
        raise TransformCalibrationError("audio window yields no samples")
    search_start = max(Fraction(0), target_start_ms - search_radius_ms)
    left_radius = target_start_ms - search_start
    search_duration = window_ms + search_radius_ms + rounded_milliseconds(left_radius / 1000)
    local_body = decode_pcm(
        ffmpeg, old_media, Fraction(old_start_ms), window_ms, sample_rate_hz
    )
    search_body = decode_pcm(
        ffmpeg, finalized_media, search_start, search_duration, sample_rate_hz
    )
    local = numpy.frombuffer(local_body, dtype="<i2").astype(numpy.float64)
    search = numpy.frombuffer(search_body, dtype="<i2").astype(numpy.float64)
    if local.size < expected_samples or search.size < expected_samples:
        raise TransformCalibrationError("audio decode returned fewer samples than requested")
    local = local[:expected_samples]
    local_centered = local - local.mean()
    search_centered = search - search.mean()
    if float(numpy.linalg.norm(local_centered)) == 0.0:
        raise TransformCalibrationError("audio comparison window has zero energy")
    correlation = scipy_signal.correlate(
        search_centered, local_centered, mode="valid", method="fft"
    )
    peak = int(numpy.argmax(correlation))
    matched = search_centered[peak : peak + local.size]
    denominator = float(
        numpy.linalg.norm(local_centered) * numpy.linalg.norm(matched)
    )
    if denominator == 0.0:
        raise TransformCalibrationError("matched audio comparison window has zero energy")
    coefficient = float(numpy.dot(local_centered, matched) / denominator)
    match_start = search_start + Fraction(peak * 1000, sample_rate_hz)
    return {
        "old_start_ms": old_start_ms,
        "target_start": fraction_text(target_start_ms),
        "search_start": fraction_text(search_start),
        "search_radius_ms": search_radius_ms,
        "window_ms": window_ms,
        "sample_rate_hz": sample_rate_hz,
        "local_pcm_sha256": sha256_bytes(local_body[: expected_samples * 2]),
        "search_pcm_sha256": sha256_bytes(search_body),
        "local_sample_count": int(local.size),
        "search_sample_count": int(search.size),
        "peak_sample_offset": peak,
        "best_finalized_start": fraction_text(match_start),
        "best_offset_from_old_ms": round(float(match_start - old_start_ms), 6),
        "normalized_correlation": round(coefficient, 9),
    }


def ensure_ignored_output(path: Path) -> None:
    if not path.is_absolute() or path == Path("/") or path.name in {"", ".", ".."}:
        raise TransformCalibrationError("receipt output must be a specific absolute path")
    normalized = Path(os.path.normpath(str(path)))
    if normalized != path:
        raise TransformCalibrationError("receipt output must not contain traversal")
    try:
        completed = subprocess.run(
            ["git", "check-ignore", "-q", "--", str(path)],
            cwd=REPOSITORY_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TransformCalibrationError(f"cannot prove output is private: {error}") from error
    if completed.returncode != 0:
        raise TransformCalibrationError("receipt output must be under a Git-ignored private path")


def atomic_private_receipt(path: Path, body: bytes) -> None:
    ensure_ignored_output(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    if path.exists():
        existing = regular_file(path, "existing receipt", MAX_JSON_BYTES)
        if path.read_bytes() != body or existing["mode"] != 0o400:
            raise TransformCalibrationError("existing receipt differs or is not sealed mode 0400")
        return
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o400)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def build_receipt(
    *,
    old_work_order: Path,
    old_result: Path,
    finalized_work_order: Path,
    finalized_result: Path,
    ffmpeg: Path,
    ffprobe: Path,
    identity_candidate_id: str,
    scaled_candidate_id: str,
    declared_recording_duration_ms: int,
    checkpoints_ms: list[int],
    visual_window_ms: int,
    audio_window_ms: int,
    identity_audio_search_radius_ms: int,
    scaled_audio_search_radius_ms: int,
    audio_sample_rate_hz: int,
    minimum_identity_ssim: float,
    minimum_identity_audio_correlation: float,
    minimum_visual_margin: float,
    minimum_audio_margin: float,
    receipt_path: Path,
) -> dict[str, Any]:
    import numpy
    import scipy
    import scipy.signal

    identity_candidate_id = identifier(identity_candidate_id, "identity candidate ID")
    scaled_candidate_id = identifier(scaled_candidate_id, "scaled candidate ID")
    if identity_candidate_id == scaled_candidate_id:
        raise TransformCalibrationError("candidate IDs must differ")
    declared_recording_duration_ms = positive_integer(
        declared_recording_duration_ms, "declared recording duration"
    )
    if not 3 <= len(checkpoints_ms) <= MAX_CHECKPOINTS:
        raise TransformCalibrationError("three to sixteen checkpoints are required")
    checkpoints_ms = sorted(
        {nonnegative_integer(item, "checkpoint", 7 * 24 * 60 * 60 * 1000) for item in checkpoints_ms}
    )
    if len(checkpoints_ms) < 3:
        raise TransformCalibrationError("checkpoints must contain at least three unique values")
    old = validate_acquisition_side("earlier_post_live", old_work_order, old_result)
    finalized = validate_acquisition_side(
        "finalized_was_live", finalized_work_order, finalized_result
    )
    if old["source"] != finalized["source"]:
        raise TransformCalibrationError("acquisitions do not identify the same public source")
    old_metadata = old["selected_remote_metadata"]
    finalized_metadata = finalized["selected_remote_metadata"]
    if (
        old_metadata["format_id"] != finalized_metadata["format_id"]
        or "+" not in old_metadata["format_id"]
        or old_metadata["live_status"] != "post_live"
        or finalized_metadata["live_status"] != "was_live"
    ):
        raise TransformCalibrationError(
            "comparison requires the same exact video+audio formats across post_live and was_live"
        )
    if old["yt_dlp"] != finalized["yt_dlp"]:
        raise TransformCalibrationError("acquisitions do not share one exact yt-dlp identity")
    ffmpeg_before = inspect_executable(ffmpeg, "ffmpeg", ["-version"])
    ffprobe_before = inspect_executable(ffprobe, "ffprobe", ["-version"])
    old_media_path = Path(old["media"]["path"])
    finalized_media_path = Path(finalized["media"]["path"])
    old_probe = exact_probe(ffprobe, old_media_path, "earlier media")
    finalized_probe = exact_probe(ffprobe, finalized_media_path, "finalized media")
    if old_probe["format"]["size"] != old["media"]["byte_count"]:
        raise TransformCalibrationError("earlier probe size differs from acquired bytes")
    if finalized_probe["format"]["size"] != finalized["media"]["byte_count"]:
        raise TransformCalibrationError("finalized probe size differs from acquired bytes")
    old_video = next(row for row in old_probe["streams"] if row["codec_type"] == "video")
    finalized_video = next(
        row for row in finalized_probe["streams"] if row["codec_type"] == "video"
    )
    old_audio = next(row for row in old_probe["streams"] if row["codec_type"] == "audio")
    finalized_audio = next(
        row for row in finalized_probe["streams"] if row["codec_type"] == "audio"
    )
    for key in ("codec_name", "r_frame_rate", "width", "height", "pixel_format"):
        if old_video[key] != finalized_video[key]:
            raise TransformCalibrationError(f"video stream compatibility differs at {key}")
    for key in ("codec_name", "sample_rate", "channels"):
        if old_audio[key] != finalized_audio[key]:
            raise TransformCalibrationError(f"audio stream compatibility differs at {key}")
    source_duration_ms = old_probe["video_duration_ms"]
    finalized_duration_ms = finalized_probe["video_duration_ms"]
    if declared_recording_duration_ms >= source_duration_ms:
        raise TransformCalibrationError("scaled candidate must shorten the earlier media duration")
    if finalized_duration_ms <= source_duration_ms:
        raise TransformCalibrationError("finalized media must contain a positive uncovered tail")
    if checkpoints_ms[0] > 1000:
        raise TransformCalibrationError("checkpoint coverage must include the beginning")
    if not any(source_duration_ms // 4 <= item <= source_duration_ms * 3 // 4 for item in checkpoints_ms):
        raise TransformCalibrationError("checkpoint coverage must include the middle")
    if checkpoints_ms[-1] < source_duration_ms * 9 // 10:
        raise TransformCalibrationError("checkpoint coverage must include the final tenth")
    for checkpoint in checkpoints_ms:
        if checkpoint + max(visual_window_ms, audio_window_ms) > min(
            old_probe["video_duration_ms"], old_probe["audio_duration_ms"]
        ):
            raise TransformCalibrationError("a checkpoint window exceeds earlier media coverage")
    scale = Fraction(declared_recording_duration_ms, source_duration_ms)
    comparisons: list[dict[str, Any]] = []
    for ordinal, checkpoint in enumerate(checkpoints_ms, start=1):
        identity_target = Fraction(checkpoint)
        scaled_target = Fraction(checkpoint) * scale
        identity_visual = visual_ssim(
            ffmpeg,
            old_media_path,
            finalized_media_path,
            checkpoint,
            identity_target,
            visual_window_ms,
        )
        scaled_visual = visual_ssim(
            ffmpeg,
            old_media_path,
            finalized_media_path,
            checkpoint,
            scaled_target,
            visual_window_ms,
        )
        identity_audio = audio_alignment(
            scipy_signal=scipy.signal,
            numpy=numpy,
            ffmpeg=ffmpeg,
            old_media=old_media_path,
            finalized_media=finalized_media_path,
            old_start_ms=checkpoint,
            target_start_ms=identity_target,
            window_ms=audio_window_ms,
            search_radius_ms=identity_audio_search_radius_ms,
            sample_rate_hz=audio_sample_rate_hz,
        )
        scaled_audio = audio_alignment(
            scipy_signal=scipy.signal,
            numpy=numpy,
            ffmpeg=ffmpeg,
            old_media=old_media_path,
            finalized_media=finalized_media_path,
            old_start_ms=checkpoint,
            target_start_ms=scaled_target,
            window_ms=audio_window_ms,
            search_radius_ms=scaled_audio_search_radius_ms,
            sample_rate_hz=audio_sample_rate_hz,
        )
        drift = scaled_target - checkpoint
        visual_margin = round(
            identity_visual["ssim_all"] - scaled_visual["ssim_all"], 9
        )
        audio_margin = round(
            identity_audio["normalized_correlation"]
            - scaled_audio["normalized_correlation"],
            9,
        )
        discriminating = abs(float(drift)) >= 1000.0
        contradicts_scaled = discriminating and (
            visual_margin >= minimum_visual_margin
            and audio_margin >= minimum_audio_margin
        )
        comparisons.append(
            {
                "ordinal": ordinal,
                "checkpoint_ms": checkpoint,
                "identity_target": fraction_text(identity_target),
                "scaled_target": fraction_text(scaled_target),
                "scaled_drift_ms": round(float(drift), 6),
                "discriminating": discriminating,
                "identity_visual": identity_visual,
                "scaled_visual": scaled_visual,
                "visual_margin": visual_margin,
                "identity_audio": identity_audio,
                "scaled_audio": scaled_audio,
                "audio_margin": audio_margin,
                "contradicts_scaled": contradicts_scaled,
            }
        )
    identity_offsets = [
        row["identity_audio"]["best_offset_from_old_ms"] for row in comparisons
    ]
    offset_range = round(max(identity_offsets) - min(identity_offsets), 6)
    identity_supported = (
        all(row["identity_visual"]["ssim_all"] >= minimum_identity_ssim for row in comparisons)
        and all(
            row["identity_audio"]["normalized_correlation"]
            >= minimum_identity_audio_correlation
            for row in comparisons
        )
        and offset_range <= 2.0
    )
    contradicting_count = sum(row["contradicts_scaled"] for row in comparisons)
    scaled_contradicted = contradicting_count >= 2
    if not identity_supported or not scaled_contradicted:
        raise TransformCalibrationError(
            "comparison thresholds do not establish identity-prefix support and scaled contradiction"
        )
    ffmpeg_after = inspect_executable(ffmpeg, "ffmpeg", ["-version"])
    ffprobe_after = inspect_executable(ffprobe, "ffprobe", ["-version"])
    if ffmpeg_before != ffmpeg_after or ffprobe_before != ffprobe_after:
        raise TransformCalibrationError("ffmpeg or ffprobe changed during calibration")
    implementation = regular_file(Path(__file__).resolve(), "calibration implementation")
    python_binding = regular_file(Path(sys.executable).resolve(), "Python executable", 1024**3)
    recipe = {
        "algorithm": ALGORITHM,
        "checkpoints_ms": checkpoints_ms,
        "visual_window_ms": visual_window_ms,
        "audio_window_ms": audio_window_ms,
        "identity_audio_search_radius_ms": identity_audio_search_radius_ms,
        "scaled_audio_search_radius_ms": scaled_audio_search_radius_ms,
        "audio_sample_rate_hz": audio_sample_rate_hz,
        "thresholds": {
            "minimum_identity_ssim": minimum_identity_ssim,
            "minimum_identity_audio_correlation": minimum_identity_audio_correlation,
            "minimum_visual_margin": minimum_visual_margin,
            "minimum_audio_margin": minimum_audio_margin,
            "maximum_identity_audio_offset_range_ms": 2.0,
            "minimum_discriminating_drift_ms": 1000.0,
            "minimum_contradicting_checkpoints": 2,
        },
    }
    semantic: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "completed",
        "visibility": "private",
        "receipt_path": str(receipt_path),
        "source": old["source"],
        "candidate_references": {
            "catalog_accessed": False,
            "relationship_asserted": False,
            "identity": {
                "transform_candidate_id": identity_candidate_id,
                "candidate_kind": "identity_from_acquired_media_duration_hypothesis",
                "scale_numerator": 1,
                "scale_denominator": 1,
            },
            "scaled": {
                "transform_candidate_id": scaled_candidate_id,
                "candidate_kind": "linear_scale_to_declared_recording_duration_hypothesis",
                "scale_numerator": declared_recording_duration_ms,
                "scale_denominator": source_duration_ms,
            },
        },
        "acquisitions": {"earlier": old, "finalized": finalized},
        "tools": {
            "implementation": implementation,
            "python": {
                **python_binding,
                "version": sys.version,
                "platform": platform.platform(),
            },
            "ffmpeg": ffmpeg_before,
            "ffprobe": ffprobe_before,
            "numpy": package_tree(numpy, "numpy"),
            "scipy": package_tree(scipy, "scipy"),
        },
        "probes": {"earlier": old_probe, "finalized": finalized_probe},
        "recipe": recipe,
        "duration_accounting": {
            "declared_recording_duration_ms": declared_recording_duration_ms,
            "earlier_video_duration_ms": source_duration_ms,
            "earlier_audio_duration_ms": old_probe["audio_duration_ms"],
            "finalized_video_duration_ms": finalized_duration_ms,
            "finalized_audio_duration_ms": finalized_probe["audio_duration_ms"],
            "stale_metadata_delta_ms": source_duration_ms
            - declared_recording_duration_ms,
            "uncovered_finalized_video_tail": {
                "boundary": "half_open",
                "start_ms": source_duration_ms,
                "end_ms": finalized_duration_ms,
                "duration_ms": finalized_duration_ms - source_duration_ms,
            },
            "uncovered_finalized_audio_tail_ms": finalized_probe["audio_duration_ms"]
            - old_probe["audio_duration_ms"],
        },
        "comparisons": comparisons,
        "conclusions": {
            "evidence_class": "deterministic_private_machine_calibration_evidence",
            "shared_prefix_identity_mapping_supported": True,
            "shared_prefix_mapping": {
                "boundary": "half_open",
                "source_start_ms": 0,
                "source_end_ms": source_duration_ms,
                "finalized_start_ms": 0,
                "finalized_end_ms": source_duration_ms,
                "scale_numerator": 1,
                "scale_denominator": 1,
                "offset_ms": 0,
            },
            "identity_audio_offset_range_ms": offset_range,
            "scaled_candidate_contradicted": True,
            "scaled_contradicting_checkpoint_count": contradicting_count,
            "catalog_transform_decision": None,
            "review_decision": None,
            "publication_decision": None,
            "relationship_decision": None,
            "timeline_application_allowed": False,
            "transcript_projection_allowed": False,
            "requires_separate_catalog_promotion_contract": True,
        },
        "safety": {
            "catalog_opened": False,
            "catalog_mutated": False,
            "credentials_allowed": False,
            "credentials_used": False,
            "network_access_by_receipt_producer": False,
            "source_acquisitions_used_network": True,
            "publication_authority": "none",
            "relationship_authority": "none",
            "review_authority": "none",
            "transcript_text_read": False,
            "transcript_text_emitted": False,
        },
    }
    identity = sha256_bytes(canonical_bytes(semantic))
    return {
        **semantic,
        "identity_sha256": identity,
        "receipt_id": f"trcalreceipt_{identity[:32]}",
    }


def threshold(value: object, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise TransformCalibrationError(f"{label} must be numeric") from error
    if not math.isfinite(result) or result < 0 or result > 1:
        raise TransformCalibrationError(f"{label} must be in [0,1]")
    return result


def build_from_arguments(args: argparse.Namespace, *, receipt_path: Path) -> dict[str, Any]:
    return build_receipt(
        old_work_order=absolute_regular_path(args.old_work_order, "old work order"),
        old_result=absolute_regular_path(args.old_result, "old result"),
        finalized_work_order=absolute_regular_path(
            args.finalized_work_order, "finalized work order"
        ),
        finalized_result=absolute_regular_path(args.finalized_result, "finalized result"),
        ffmpeg=absolute_regular_path(args.ffmpeg, "ffmpeg"),
        ffprobe=absolute_regular_path(args.ffprobe, "ffprobe"),
        identity_candidate_id=args.identity_candidate_id,
        scaled_candidate_id=args.scaled_candidate_id,
        declared_recording_duration_ms=positive_integer(
            args.declared_recording_duration_ms, "declared recording duration"
        ),
        checkpoints_ms=args.checkpoint_ms,
        visual_window_ms=positive_integer(args.visual_window_ms, "visual window", 60_000),
        audio_window_ms=positive_integer(args.audio_window_ms, "audio window", 120_000),
        identity_audio_search_radius_ms=positive_integer(
            args.identity_audio_search_radius_ms, "identity audio search radius", 10_000
        ),
        scaled_audio_search_radius_ms=positive_integer(
            args.scaled_audio_search_radius_ms, "scaled audio search radius", 10_000
        ),
        audio_sample_rate_hz=positive_integer(
            args.audio_sample_rate_hz, "audio sample rate", 48_000
        ),
        minimum_identity_ssim=threshold(
            args.minimum_identity_ssim, "minimum identity SSIM"
        ),
        minimum_identity_audio_correlation=threshold(
            args.minimum_identity_audio_correlation,
            "minimum identity audio correlation",
        ),
        minimum_visual_margin=threshold(
            args.minimum_visual_margin, "minimum visual margin"
        ),
        minimum_audio_margin=threshold(
            args.minimum_audio_margin, "minimum audio margin"
        ),
        receipt_path=receipt_path,
    )


def produce(args: argparse.Namespace) -> dict[str, Any]:
    path = Path(args.output)
    if not path.is_absolute():
        raise TransformCalibrationError("--output must be absolute")
    ensure_ignored_output(path)
    receipt = build_from_arguments(args, receipt_path=path)
    atomic_private_receipt(path, pretty_bytes(receipt))
    return receipt


def validate_receipt(path: Path) -> dict[str, Any]:
    path = absolute_regular_path(str(path), "receipt")
    body = path.read_bytes()
    receipt, _ = load_json(path, "receipt")
    if body != pretty_bytes(receipt):
        raise TransformCalibrationError("receipt must be canonical pretty JSON plus one newline")
    if receipt.get("stage") != STAGE or receipt.get("schema_version") != SCHEMA_VERSION:
        raise TransformCalibrationError("receipt stage or schema version is unsupported")
    if receipt.get("receipt_path") != str(path):
        raise TransformCalibrationError("receipt path binding differs")
    semantic = {
        key: value
        for key, value in receipt.items()
        if key not in {"identity_sha256", "receipt_id"}
    }
    identity = sha256_bytes(canonical_bytes(semantic))
    if receipt.get("identity_sha256") != identity:
        raise TransformCalibrationError("receipt semantic identity is invalid")
    if receipt.get("receipt_id") != f"trcalreceipt_{identity[:32]}":
        raise TransformCalibrationError("receipt ID is invalid")
    recipe = receipt["recipe"]
    candidates = receipt["candidate_references"]
    rebuilt = build_receipt(
        old_work_order=Path(receipt["acquisitions"]["earlier"]["work_order"]["path"]),
        old_result=Path(receipt["acquisitions"]["earlier"]["result"]["path"]),
        finalized_work_order=Path(
            receipt["acquisitions"]["finalized"]["work_order"]["path"]
        ),
        finalized_result=Path(receipt["acquisitions"]["finalized"]["result"]["path"]),
        ffmpeg=Path(receipt["tools"]["ffmpeg"]["path"]),
        ffprobe=Path(receipt["tools"]["ffprobe"]["path"]),
        identity_candidate_id=candidates["identity"]["transform_candidate_id"],
        scaled_candidate_id=candidates["scaled"]["transform_candidate_id"],
        declared_recording_duration_ms=receipt["duration_accounting"][
            "declared_recording_duration_ms"
        ],
        checkpoints_ms=recipe["checkpoints_ms"],
        visual_window_ms=recipe["visual_window_ms"],
        audio_window_ms=recipe["audio_window_ms"],
        identity_audio_search_radius_ms=recipe["identity_audio_search_radius_ms"],
        scaled_audio_search_radius_ms=recipe["scaled_audio_search_radius_ms"],
        audio_sample_rate_hz=recipe["audio_sample_rate_hz"],
        minimum_identity_ssim=recipe["thresholds"]["minimum_identity_ssim"],
        minimum_identity_audio_correlation=recipe["thresholds"]
        ["minimum_identity_audio_correlation"],
        minimum_visual_margin=recipe["thresholds"]["minimum_visual_margin"],
        minimum_audio_margin=recipe["thresholds"]["minimum_audio_margin"],
        receipt_path=path,
    )
    if rebuilt != receipt or pretty_bytes(rebuilt) != body:
        raise TransformCalibrationError("receipt does not replay from current exact evidence")
    binding = regular_file(path, "receipt", MAX_JSON_BYTES)
    if binding["mode"] != 0o400:
        raise TransformCalibrationError("receipt must be sealed mode 0400")
    return receipt


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--old-work-order", required=True)
    parser.add_argument("--old-result", required=True)
    parser.add_argument("--finalized-work-order", required=True)
    parser.add_argument("--finalized-result", required=True)
    parser.add_argument("--ffmpeg", required=True)
    parser.add_argument("--ffprobe", required=True)
    parser.add_argument("--identity-candidate-id", required=True)
    parser.add_argument("--scaled-candidate-id", required=True)
    parser.add_argument("--declared-recording-duration-ms", type=int, required=True)
    parser.add_argument("--checkpoint-ms", type=int, action="append", required=True)
    parser.add_argument("--visual-window-ms", type=int, default=5000)
    parser.add_argument("--audio-window-ms", type=int, default=30000)
    parser.add_argument("--identity-audio-search-radius-ms", type=int, default=1000)
    parser.add_argument("--scaled-audio-search-radius-ms", type=int, default=250)
    parser.add_argument("--audio-sample-rate-hz", type=int, default=4000)
    parser.add_argument("--minimum-identity-ssim", type=float, default=0.98)
    parser.add_argument("--minimum-identity-audio-correlation", type=float, default=0.95)
    parser.add_argument("--minimum-visual-margin", type=float, default=0.03)
    parser.add_argument("--minimum-audio-margin", type=float, default=0.10)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Produce or validate private catalog-free transform calibration evidence"
    )
    subparsers = result.add_subparsers(dest="command", required=True)
    produce_parser = subparsers.add_parser("produce")
    add_common_arguments(produce_parser)
    produce_parser.add_argument("--output", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--receipt", required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "produce":
            receipt = produce(args)
        else:
            receipt = validate_receipt(Path(args.receipt))
        summary = {
            "schema_version": SCHEMA_VERSION,
            "status": "completed",
            "receipt_id": receipt["receipt_id"],
            "identity_sha256": receipt["identity_sha256"],
            "receipt_path": receipt["receipt_path"],
            "shared_prefix_identity_mapping_supported": receipt["conclusions"]
            ["shared_prefix_identity_mapping_supported"],
            "scaled_candidate_contradicted": receipt["conclusions"]
            ["scaled_candidate_contradicted"],
            "publication_authority": "none",
            "catalog_mutated": False,
        }
        sys.stdout.write(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        return 0
    except (TransformCalibrationError, OSError, KeyError, ImportError) as error:
        sys.stderr.write(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
