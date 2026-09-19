#!/usr/bin/env python3
"""Deterministic sparse visual fingerprints and conservative pair comparison.

This offline stage decodes caller-selected frames from an exact sealed local video,
stores private 32x32 grayscale evidence, and computes a fixed-integer DCT perceptual
hash.  Its comparison command emits uncalibrated review candidates only.  It never
asserts person identity, duplicate/parent status, ownership, or any relationship.
"""

from __future__ import annotations

import argparse
import fcntl
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
from fractions import Fraction
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
EXTRACT_STAGE = "visual_fingerprint_extract"
COMPARE_STAGE = "visual_fingerprint_compare"
ALGORITHM = "fixed_q20_dct_phash_8x8_v1"
COMPARE_METHOD = "minimum_pairwise_phash_hamming_v1"
WIDTH = 32
HEIGHT = 32
PIXEL_FORMAT = "gray"
PHASH_BITS = 64
MAX_SAMPLES = 4096
MAX_SELECTED_FRAMES = 256
MAX_PAIRWISE_COMPARISONS = 65_536
MAX_DURATION_MS = 7 * 24 * 60 * 60 * 1_000
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PHASH_RE = re.compile(r"^[0-9a-f]{16}$")
SHOWINFO_CONFIG_RE = re.compile(
    r"config in time_base:\s*([0-9]+)/([0-9]+),\s*frame_rate:"
)
SHOWINFO_FRAME_RE = re.compile(
    r"showinfo[^\n]*\bn:\s*0\s+pts:\s*(-?[0-9]+)\s+"
    r"pts_time:[^\s]+\s+duration:\s*(-?[0-9]+)\s+duration_time:"
    r"[^\s]+.*?\biskey:([01])\b"
)

# cos((2*x+1)*u*pi/64), rounded once to signed Q20 integers.  Committing the
# matrix avoids NumPy/SciPy and platform-dependent runtime trigonometry.  Common
# positive normalization factors are intentionally omitted: per-frequency signs
# and ordering around the within-frame median are unchanged by them.
DCT_Q20: tuple[tuple[int, ...], ...] = (
    (1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576, 1048576),
    (1047313, 1037227, 1017151, 987281, 947901, 899394, 842224, 776944, 704181, 624636, 539076, 448324, 353255, 254783, 153858, 51451, -51451, -153858, -254783, -353255, -448324, -539076, -624636, -704181, -776944, -842224, -899394, -947901, -987281, -1017151, -1037227, -1047313),
    (1043527, 1003425, 924761, 810560, 665210, 494295, 304386, 102778, -102778, -304386, -494295, -665210, -810560, -924761, -1003425, -1043527, -1043527, -1003425, -924761, -810560, -665210, -494295, -304386, -102778, 102778, 304386, 494295, 665210, 810560, 924761, 1003425, 1043527),
    (1037227, 947901, 776944, 539076, 254783, -51451, -353255, -624636, -842224, -987281, -1047313, -1017151, -899394, -704181, -448324, -153858, 153858, 448324, 704181, 899394, 1017151, 1047313, 987281, 842224, 624636, 353255, 51451, -254783, -539076, -776944, -947901, -1037227),
    (1028428, 871859, 582558, 204567, -204567, -582558, -871859, -1028428, -1028428, -871859, -582558, -204567, 204567, 582558, 871859, 1028428, 1028428, 871859, 582558, 204567, -204567, -582558, -871859, -1028428, -1028428, -871859, -582558, -204567, 204567, 582558, 871859, 1028428),
    (1017151, 776944, 353255, -153858, -624636, -947901, -1047313, -899394, -539076, -51451, 448324, 842224, 1037227, 987281, 704181, 254783, -254783, -704181, -987281, -1037227, -842224, -448324, 51451, 539076, 899394, 1047313, 947901, 624636, 153858, -353255, -776944, -1017151),
    (1003425, 665210, 102778, -494295, -924761, -1043527, -810560, -304386, 304386, 810560, 1043527, 924761, 494295, -102778, -665210, -1003425, -1003425, -665210, -102778, 494295, 924761, 1043527, 810560, 304386, -304386, -810560, -1043527, -924761, -494295, 102778, 665210, 1003425),
    (987281, 539076, -153858, -776944, -1047313, -842224, -254783, 448324, 947901, 1017151, 624636, -51451, -704181, -1037227, -899394, -353255, 353255, 899394, 1037227, 704181, 51451, -624636, -1017151, -947901, -448324, 254783, 842224, 1047313, 776944, 153858, -539076, -987281),
)


class VisualFingerprintError(RuntimeError):
    """A strict contract, integrity, provenance, or extraction failure."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def pretty_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )


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


def exact_keys(value: dict[str, Any], label: str, keys: set[str]) -> None:
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise VisualFingerprintError(f"{label} has " + "; ".join(details))


def object_value(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VisualFingerprintError(f"{label} must be an object")
    return value


def string_value(value: object, label: str, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise VisualFingerprintError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: object, label: str) -> str:
    text = string_value(value, label, 128)
    if not ID_RE.fullmatch(text):
        raise VisualFingerprintError(f"{label} contains unsupported characters")
    return text


def digest_value(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise VisualFingerprintError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise VisualFingerprintError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise VisualFingerprintError(f"{label} must be between {minimum} and {maximum}")
    return value


def boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise VisualFingerprintError(f"{label} must be boolean")
    return value


def resolved_file(value: object, label: str, *, sealed: bool) -> Path:
    text = string_value(value, label)
    if "://" in text:
        raise VisualFingerprintError(f"{label} must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute():
        raise VisualFingerprintError(f"{label} must be absolute")
    try:
        mode = path.lstat().st_mode
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise VisualFingerprintError(f"{label} is not a readable current file: {error}") from error
    if path != resolved or stat.S_ISLNK(mode):
        raise VisualFingerprintError(f"{label} must be resolved without symlinks or traversal")
    if not stat.S_ISREG(mode):
        raise VisualFingerprintError(f"{label} must be a regular file")
    if sealed and mode & 0o222:
        raise VisualFingerprintError(f"{label} must be sealed read-only")
    return path


def validate_output_root(value: object) -> Path:
    text = string_value(value, "output.root")
    if "://" in text:
        raise VisualFingerprintError("output.root must be a local path, not a URL")
    path = Path(text)
    if not path.is_absolute() or path == Path("/"):
        raise VisualFingerprintError("output.root must be a specific absolute directory")
    if Path(os.path.normpath(str(path))) != path:
        raise VisualFingerprintError("output.root must not contain traversal")
    for unsafe in (Path("/tmp"), Path("/var/tmp")):
        try:
            path.relative_to(unsafe)
        except ValueError:
            pass
        else:
            raise VisualFingerprintError(f"output.root must not be under {unsafe}")
    if path.exists() and not path.is_dir():
        raise VisualFingerprintError("output.root must not be an existing file")
    parent = path
    while not parent.exists():
        if parent.parent == parent:
            raise VisualFingerprintError("output.root has no existing parent")
        parent = parent.parent
    if parent.resolve(strict=True) != parent:
        raise VisualFingerprintError("output.root must not traverse a symlinked parent")
    return path


def load_json(path: Path) -> Any:
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            raise VisualFingerprintError(f"JSON exceeds {MAX_JSON_BYTES} bytes: {path}")
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise VisualFingerprintError(f"cannot read JSON {path}: {error}") from error


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


def stable_observe(path: Path, expected_sha256: str, expected_size: int, label: str) -> dict[str, Any]:
    before = path.stat()
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        opened = os.fstat(handle.fileno())
        if before_identity != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns):
            raise VisualFingerprintError(f"{label} changed while opening")
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
        after_fd = os.fstat(handle.fileno())
    after = path.stat()
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    fd_identity = (after_fd.st_dev, after_fd.st_ino, after_fd.st_size, after_fd.st_mtime_ns)
    if before_identity != after_identity or before_identity != fd_identity:
        raise VisualFingerprintError(f"{label} changed while reading")
    observed_sha = digest.hexdigest()
    if observed_sha != expected_sha256 or before.st_size != expected_size:
        raise VisualFingerprintError(f"{label} bytes differ from the pinned digest/size")
    return {
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": observed_sha,
        "byte_count": before.st_size,
        "stat": {
            "device": before.st_dev,
            "inode": before.st_ino,
            "mtime_ns": before.st_mtime_ns,
        },
        "unchanged": True,
    }


def run_text(command: list[str], timeout: int = 60) -> str:
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
        raise VisualFingerprintError(
            f"command failed ({completed.returncode}): {command[0]}\n"
            + "\n".join(completed.stdout.splitlines()[-30:])
        )
    return completed.stdout.strip()


def capture_engine(executable: Path) -> dict[str, Any]:
    before = executable.stat()
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    executable_sha256 = sha256_file(executable)
    version_output = run_text([str(executable), "-hide_banner", "-version"])
    capability_commands = (
        ("scale", [str(executable), "-hide_banner", "-h", "filter=scale"]),
        ("showinfo", [str(executable), "-hide_banner", "-h", "filter=showinfo"]),
        ("rawvideo", [str(executable), "-hide_banner", "-h", "encoder=rawvideo"]),
    )
    capability_rows = [
        {"name": name, "command": command, "output": run_text(command)}
        for name, command in capability_commands
    ]
    capabilities_output = canonical_bytes(capability_rows).decode("utf-8")
    after = executable.stat()
    if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise VisualFingerprintError("FFmpeg executable changed while provenance was captured")
    if sha256_file(executable) != executable_sha256:
        raise VisualFingerprintError("FFmpeg executable digest changed while provenance was captured")
    version_lines = version_output.splitlines()
    if not version_lines:
        raise VisualFingerprintError("FFmpeg emitted no version label")
    return {
        "name": "ffmpeg",
        "path": str(executable),
        "sha256": executable_sha256,
        "byte_count": before.st_size,
        "version_label": version_lines[0].strip(),
        "version_output": version_output,
        "version_output_sha256": sha256_bytes(version_output.encode("utf-8")),
        "capabilities_output": capabilities_output,
        "capabilities_output_sha256": sha256_bytes(capabilities_output.encode("utf-8")),
    }


def validate_engine(raw: object) -> tuple[dict[str, Any], Path]:
    value = object_value(raw, "engine")
    exact_keys(
        value,
        "engine",
        {
            "executable",
            "expected_sha256",
            "expected_byte_count",
            "expected_version_output_sha256",
            "expected_capabilities_output_sha256",
            "version_label",
        },
    )
    executable = resolved_file(value["executable"], "engine.executable", sealed=False)
    if not os.access(executable, os.X_OK):
        raise VisualFingerprintError("engine.executable is not executable")
    observed = capture_engine(executable)
    expected = {
        "sha256": digest_value(value["expected_sha256"], "engine.expected_sha256"),
        "byte_count": integer(value["expected_byte_count"], "engine.expected_byte_count", 1, 1 << 63),
        "version_output_sha256": digest_value(
            value["expected_version_output_sha256"], "engine.expected_version_output_sha256"
        ),
        "capabilities_output_sha256": digest_value(
            value["expected_capabilities_output_sha256"],
            "engine.expected_capabilities_output_sha256",
        ),
        "version_label": string_value(value["version_label"], "engine.version_label", 512),
    }
    for key, expected_value in expected.items():
        if observed[key] != expected_value:
            raise VisualFingerprintError(f"current FFmpeg {key} differs from its pin")
    return observed, executable


def validate_context(raw: object, label: str = "catalog_context") -> dict[str, str] | None:
    if raw is None:
        return None
    value = object_value(raw, label)
    exact_keys(value, label, {"recording_id", "rendition_id"})
    return {
        "recording_id": identifier(value["recording_id"], f"{label}.recording_id"),
        "rendition_id": identifier(value["rendition_id"], f"{label}.rendition_id"),
    }


def validate_extraction_order(raw: object) -> dict[str, Any]:
    value = object_value(raw, "work order")
    exact_keys(
        value,
        "work order",
        {"schema_version", "job_id", "input", "engine", "extraction", "catalog_context", "output"},
    )
    if value["schema_version"] != SCHEMA_VERSION:
        raise VisualFingerprintError("schema_version must be 1")
    input_raw = object_value(value["input"], "input")
    exact_keys(
        input_raw,
        "input",
        {
            "path",
            "expected_sha256",
            "expected_byte_count",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "duration_ms",
            "timeline_origin_ms",
            "video_stream_selector",
            "sealed",
        },
    )
    input_path = resolved_file(input_raw["path"], "input.path", sealed=True)
    input_value = {
        "path": str(input_path),
        "expected_sha256": digest_value(input_raw["expected_sha256"], "input.expected_sha256"),
        "expected_byte_count": integer(input_raw["expected_byte_count"], "input.expected_byte_count", 1, 1 << 63),
        "media_id": identifier(input_raw["media_id"], "input.media_id"),
        "artifact_id": identifier(input_raw["artifact_id"], "input.artifact_id"),
        "parent_processing_run_id": identifier(input_raw["parent_processing_run_id"], "input.parent_processing_run_id"),
        "duration_ms": integer(input_raw["duration_ms"], "input.duration_ms", 1, MAX_DURATION_MS),
        "timeline_origin_ms": integer(input_raw["timeline_origin_ms"], "input.timeline_origin_ms", 0, MAX_DURATION_MS),
        "video_stream_selector": string_value(input_raw["video_stream_selector"], "input.video_stream_selector", 16),
        "sealed": boolean(input_raw["sealed"], "input.sealed"),
    }
    if input_value["video_stream_selector"] != "0:v:0":
        raise VisualFingerprintError("input.video_stream_selector must be 0:v:0")
    if input_value["sealed"] is not True:
        raise VisualFingerprintError("input.sealed must be true")
    if input_value["media_id"] != f"media_sha256_{input_value['expected_sha256']}":
        raise VisualFingerprintError("input.media_id must be derived from expected_sha256")
    extraction_raw = object_value(value["extraction"], "extraction")
    exact_keys(
        extraction_raw,
        "extraction",
        {
            "algorithm",
            "pixel_format",
            "width",
            "height",
            "scale_flags",
            "threads",
            "timeout_seconds_per_frame",
            "max_timestamp_drift_ms",
            "samples",
        },
    )
    if extraction_raw["algorithm"] != ALGORITHM:
        raise VisualFingerprintError(f"extraction.algorithm must be {ALGORITHM}")
    if extraction_raw["pixel_format"] != PIXEL_FORMAT:
        raise VisualFingerprintError("extraction.pixel_format must be gray")
    if extraction_raw["scale_flags"] != "bilinear":
        raise VisualFingerprintError("extraction.scale_flags must be bilinear")
    if extraction_raw["width"] != WIDTH or extraction_raw["height"] != HEIGHT:
        raise VisualFingerprintError("extraction dimensions must be 32x32")
    if extraction_raw["threads"] != 1:
        raise VisualFingerprintError("extraction.threads must be 1")
    timeout = integer(
        extraction_raw["timeout_seconds_per_frame"],
        "extraction.timeout_seconds_per_frame",
        1,
        600,
    )
    maximum_drift = integer(
        extraction_raw["max_timestamp_drift_ms"],
        "extraction.max_timestamp_drift_ms",
        0,
        10_000,
    )
    samples_raw = extraction_raw["samples"]
    if not isinstance(samples_raw, list) or not 1 <= len(samples_raw) <= MAX_SAMPLES:
        raise VisualFingerprintError(f"extraction.samples must have 1..{MAX_SAMPLES} entries")
    samples: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, raw_sample in enumerate(samples_raw):
        sample = object_value(raw_sample, f"extraction.samples[{index}]")
        exact_keys(
            sample,
            f"extraction.samples[{index}]",
            {
                "sample_id",
                "window_id",
                "start_ms",
                "end_ms",
                "requested_timestamp_ms",
                "timestamp_kind",
            },
        )
        sample_id = identifier(sample["sample_id"], f"extraction.samples[{index}].sample_id")
        if sample_id in seen_ids:
            raise VisualFingerprintError("extraction sample IDs must be unique")
        seen_ids.add(sample_id)
        start_ms = integer(sample["start_ms"], f"extraction.samples[{index}].start_ms", 0, input_value["duration_ms"] - 1)
        end_ms = integer(sample["end_ms"], f"extraction.samples[{index}].end_ms", 1, input_value["duration_ms"])
        requested = integer(
            sample["requested_timestamp_ms"],
            f"extraction.samples[{index}].requested_timestamp_ms",
            0,
            input_value["duration_ms"] - 1,
        )
        if not start_ms < end_ms or not start_ms <= requested < end_ms:
            raise VisualFingerprintError(
                "every sample must satisfy start_ms <= requested_timestamp_ms < end_ms"
            )
        timestamp_kind = string_value(
            sample["timestamp_kind"], f"extraction.samples[{index}].timestamp_kind", 32
        )
        if timestamp_kind not in {"explicit", "keyframe"}:
            raise VisualFingerprintError("sample.timestamp_kind must be explicit or keyframe")
        samples.append(
            {
                "sample_id": sample_id,
                "window_id": identifier(sample["window_id"], f"extraction.samples[{index}].window_id"),
                "start_ms": start_ms,
                "end_ms": end_ms,
                "requested_timestamp_ms": requested,
                "timestamp_kind": timestamp_kind,
            }
        )
    observed_engine, _ = validate_engine(value["engine"])
    engine_pin = {
        "executable": observed_engine["path"],
        "expected_sha256": observed_engine["sha256"],
        "expected_byte_count": observed_engine["byte_count"],
        "expected_version_output_sha256": observed_engine["version_output_sha256"],
        "expected_capabilities_output_sha256": observed_engine["capabilities_output_sha256"],
        "version_label": observed_engine["version_label"],
    }
    output = object_value(value["output"], "output")
    exact_keys(output, "output", {"root"})
    normalized = {
        "schema_version": 1,
        "job_id": identifier(value["job_id"], "job_id"),
        "input": input_value,
        "engine": engine_pin,
        "extraction": {
            "algorithm": ALGORITHM,
            "pixel_format": PIXEL_FORMAT,
            "width": WIDTH,
            "height": HEIGHT,
            "scale_flags": "bilinear",
            "threads": 1,
            "timeout_seconds_per_frame": timeout,
            "max_timestamp_drift_ms": maximum_drift,
            "samples": samples,
        },
        "catalog_context": validate_context(value["catalog_context"]),
        "output": {"root": str(validate_output_root(output["root"]))},
    }
    normalized["_engine_observation"] = observed_engine
    return normalized


def public_order(work_order: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in work_order.items() if not key.startswith("_")}


def implementation_observation() -> dict[str, Any]:
    path = Path(__file__).resolve(strict=True)
    before = path.stat()
    digest = sha256_file(path)
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise VisualFingerprintError("visual fingerprint implementation changed while hashing")
    return {
        "path": str(path),
        "sha256": digest,
        "byte_count": before.st_size,
        "python_version": sys.version.split()[0],
    }


def seconds_text(timestamp_ms: int) -> str:
    return f"{timestamp_ms // 1000}.{timestamp_ms % 1000:03d}"


def frame_command(ffmpeg: str, input_path: str, seek_ms: int) -> list[str]:
    return [
        ffmpeg,
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
        seconds_text(seek_ms),
        "-i",
        input_path,
        "-map",
        "0:v:0",
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
        "scale=32:32:flags=bilinear,format=gray,showinfo",
        "-fps_mode",
        "passthrough",
        "-pix_fmt",
        "gray",
        "-c:v",
        "rawvideo",
        "-flags:v",
        "+bitexact",
        "-threads:v",
        "1",
        "-f",
        "rawvideo",
        "pipe:1",
    ]


def decode_frame(command: list[str], timeout_seconds: int) -> tuple[bytes, str]:
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "LC_ALL": "C", "LANG": "C", "TZ": "UTC"},
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise VisualFingerprintError(
            f"FFmpeg frame extraction exceeded {timeout_seconds} seconds"
        ) from error
    stderr = completed.stderr.decode("utf-8", errors="replace")
    if completed.returncode != 0:
        raise VisualFingerprintError(
            f"FFmpeg frame extraction failed ({completed.returncode})\n"
            + "\n".join(stderr.splitlines()[-30:])
        )
    if len(completed.stdout) != WIDTH * HEIGHT:
        raise VisualFingerprintError(
            f"FFmpeg produced {len(completed.stdout)} grayscale bytes; expected {WIDTH * HEIGHT}"
        )
    return completed.stdout, stderr


def parse_showinfo(stderr: str, timeline_origin_ms: int) -> dict[str, Any]:
    config = SHOWINFO_CONFIG_RE.search(stderr)
    frame = SHOWINFO_FRAME_RE.search(stderr)
    if not config or not frame:
        raise VisualFingerprintError("FFmpeg did not emit exact showinfo time-base/PTS evidence")
    numerator, denominator = int(config.group(1)), int(config.group(2))
    pts, duration_pts, keyframe = int(frame.group(1)), int(frame.group(2)), frame.group(3) == "1"
    if numerator <= 0 or denominator <= 0 or pts < 0 or duration_pts <= 0:
        raise VisualFingerprintError("FFmpeg emitted an invalid PTS, duration, or time base")
    absolute = Fraction(pts * numerator, denominator)
    duration = Fraction(duration_pts * numerator, denominator)
    relative = absolute - Fraction(timeline_origin_ms, 1000)
    return {
        "pts": pts,
        "duration_pts": duration_pts,
        "time_base_numerator": numerator,
        "time_base_denominator": denominator,
        "absolute_timestamp_us": round(absolute * 1_000_000),
        "relative_timestamp_us": round(relative * 1_000_000),
        "relative_timestamp_ms": round(relative * 1_000),
        "duration_us": round(duration * 1_000_000),
        "is_keyframe": keyframe,
    }


def perceptual_hash(gray: bytes) -> tuple[str, int]:
    if len(gray) != WIDTH * HEIGHT:
        raise VisualFingerprintError("grayscale evidence has the wrong dimensions")
    # Separable fixed-integer transform: rows[y][u], then coefficients[v][u].
    rows = [
        [sum(gray[y * WIDTH + x] * DCT_Q20[u][x] for x in range(WIDTH)) for u in range(8)]
        for y in range(HEIGHT)
    ]
    coefficients = [
        sum(rows[y][u] * DCT_Q20[v][y] for y in range(HEIGHT))
        for v in range(8)
        for u in range(8)
    ]
    ac = sorted(coefficients[1:])
    median = ac[len(ac) // 2]
    bits = [False, *[coefficient > median for coefficient in coefficients[1:]]]
    value = 0
    for bit in bits:
        value = (value << 1) | int(bit)
    return f"{value:016x}", value.bit_count()


def quality_flags(gray: bytes, sample: dict[str, Any], timestamp: dict[str, Any], drift_us: int) -> list[str]:
    flags: list[str] = []
    if len(set(gray)) < 4 or max(gray) - min(gray) < 8:
        flags.append("low_visual_variance")
    if drift_us:
        flags.append("decoded_timestamp_differs_from_request")
    if sample["timestamp_kind"] == "keyframe" and not timestamp["is_keyframe"]:
        flags.append("requested_keyframe_timestamp_decoded_non_keyframe")
    return flags


def extraction_recipe(work_order: dict[str, Any], implementation: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "stage": EXTRACT_STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "implementation_sha256": implementation["sha256"],
        "input": {
            key: work_order["input"][key]
            for key in (
                "expected_sha256",
                "expected_byte_count",
                "media_id",
                "artifact_id",
                "parent_processing_run_id",
                "duration_ms",
                "timeline_origin_ms",
                "video_stream_selector",
            )
        },
        "engine": {
            "sha256": work_order["_engine_observation"]["sha256"],
            "version_output_sha256": work_order["_engine_observation"]["version_output_sha256"],
            "capabilities_output_sha256": work_order["_engine_observation"]["capabilities_output_sha256"],
        },
        "extraction": work_order["extraction"],
        "hash_definition": {
            "grayscale_bytes": 1024,
            "dct_matrix": "committed_signed_q20_cosine_8x32",
            "coefficients": "top_left_8x8",
            "dc_bit": 0,
            "threshold": "strictly_greater_than_median_of_63_ac_coefficients",
            "bit_order": "row_major_most_significant_bit_first",
        },
    }


def result_layout(root: Path, input_sha: str, recipe_sha: str, result_key: str) -> Path:
    return (
        root
        / "vision"
        / "visual-fingerprints"
        / "sha256"
        / input_sha[:2]
        / input_sha
        / "recipes"
        / recipe_sha
        / "results"
        / result_key
    )


def seal_path(path: Path) -> None:
    path.chmod(path.stat().st_mode & ~0o222)


def validate_frame_record(frame: object, run_dir: Path, result_key: str) -> dict[str, Any]:
    value = object_value(frame, "visual fingerprint frame")
    exact_keys(
        value,
        "visual fingerprint frame",
        {
            "fingerprint_id",
            "ordinal",
            "sample_id",
            "window_id",
            "start_ms",
            "end_ms",
            "requested_timestamp_ms",
            "timestamp_kind",
            "decoded_timestamp",
            "timestamp_drift_us",
            "algorithm",
            "phash_bits",
            "phash_hex",
            "phash_popcount",
            "exact_gray_sha256",
            "quality_flags",
            "artifact",
        },
    )
    if value["algorithm"] != ALGORITHM or value["phash_bits"] != PHASH_BITS:
        raise VisualFingerprintError("visual fingerprint frame algorithm is incompatible")
    phash = value["phash_hex"]
    if not isinstance(phash, str) or not PHASH_RE.fullmatch(phash):
        raise VisualFingerprintError("visual fingerprint phash_hex is invalid")
    if value["phash_popcount"] != int(phash, 16).bit_count():
        raise VisualFingerprintError("visual fingerprint popcount is invalid")
    artifact = object_value(value["artifact"], "visual fingerprint artifact")
    exact_keys(
        artifact,
        "visual fingerprint artifact",
        {
            "artifact_id",
            "artifact_kind",
            "path",
            "storage_uri",
            "sha256",
            "byte_count",
            "width",
            "height",
            "pixel_format",
            "visibility",
        },
    )
    path = resolved_file(artifact["path"], "visual fingerprint artifact.path", sealed=True)
    if path.parent != run_dir / "frames" or artifact["storage_uri"] != path.as_uri():
        raise VisualFingerprintError("visual fingerprint artifact escapes its result frame directory")
    if artifact["artifact_kind"] != "visual_fingerprint_gray32" or artifact["visibility"] != "private":
        raise VisualFingerprintError("visual fingerprint artifact kind/visibility is invalid")
    if artifact["width"] != WIDTH or artifact["height"] != HEIGHT or artifact["pixel_format"] != PIXEL_FORMAT:
        raise VisualFingerprintError("visual fingerprint artifact dimensions/format are invalid")
    body = path.read_bytes()
    observed_sha = sha256_bytes(body)
    if len(body) != WIDTH * HEIGHT or artifact["byte_count"] != len(body):
        raise VisualFingerprintError("visual fingerprint artifact byte count is invalid")
    if artifact["sha256"] != observed_sha or value["exact_gray_sha256"] != observed_sha:
        raise VisualFingerprintError("visual fingerprint artifact digest is invalid")
    observed_phash, observed_popcount = perceptual_hash(body)
    if phash != observed_phash or value["phash_popcount"] != observed_popcount:
        raise VisualFingerprintError("visual fingerprint evidence no longer reproduces its pHash")
    expected_artifact_id = stable_id("artifact", result_key, value["ordinal"], observed_sha)
    if artifact["artifact_id"] != expected_artifact_id:
        raise VisualFingerprintError("visual fingerprint artifact ID is invalid")
    expected_fingerprint_id = stable_id(
        "visual_fingerprint",
        result_key,
        value["sample_id"],
        value["decoded_timestamp"],
        observed_sha,
        phash,
    )
    if value["fingerprint_id"] != expected_fingerprint_id:
        raise VisualFingerprintError("visual fingerprint ID is invalid")
    return value


def load_completed_extraction(
    result_path: Path, expected_sha256: str | None = None
) -> tuple[dict[str, Any], str]:
    path = resolved_file(str(result_path), "visual fingerprint result", sealed=True)
    if path.stat().st_size > MAX_RESULT_BYTES:
        raise VisualFingerprintError("visual fingerprint result exceeds its byte cap")
    observed_sha = sha256_file(path)
    if expected_sha256 is not None and observed_sha != expected_sha256:
        raise VisualFingerprintError("visual fingerprint result differs from its pinned digest")
    result = object_value(load_json(path), "visual fingerprint result")
    exact_keys(
        result,
        "visual fingerprint result",
        {
            "schema_version",
            "stage",
            "implementation_version",
            "status",
            "dry_run",
            "job_id",
            "work_order_sha256",
            "recipe",
            "recipe_id",
            "recipe_sha256",
            "result_key",
            "implementation",
            "input",
            "engine",
            "configuration",
            "processing_run",
            "commands",
            "frames",
            "quality_flags",
            "catalog_context",
            "result_path",
            "errors",
        },
    )
    if (
        result["schema_version"] != 1
        or result["stage"] != EXTRACT_STAGE
        or result["status"] != "completed"
        or result["dry_run"] is not False
        or result["errors"] != []
        or result["result_path"] != str(path)
    ):
        raise VisualFingerprintError("visual fingerprint result is not a completed v1 envelope")
    input_value = object_value(result["input"], "visual fingerprint result input")
    exact_keys(
        input_value,
        "visual fingerprint result input",
        {
            "path",
            "storage_uri",
            "sha256",
            "byte_count",
            "stat",
            "unchanged",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
            "duration_ms",
            "timeline_origin_ms",
            "video_stream_selector",
            "sealed",
        },
    )
    if (
        not isinstance(input_value["path"], str)
        or not Path(input_value["path"]).is_absolute()
        or input_value["storage_uri"] != Path(input_value["path"]).as_uri()
        or not SHA256_RE.fullmatch(str(input_value["sha256"]))
        or input_value["media_id"] != f"media_sha256_{input_value['sha256']}"
        or input_value["unchanged"] is not True
        or input_value["sealed"] is not True
        or input_value["video_stream_selector"] != "0:v:0"
    ):
        raise VisualFingerprintError("visual fingerprint result input metadata is invalid")
    input_stat = object_value(input_value["stat"], "visual fingerprint result input.stat")
    exact_keys(input_stat, "visual fingerprint result input.stat", {"device", "inode", "mtime_ns"})
    if any(isinstance(input_stat[key], bool) or not isinstance(input_stat[key], int) for key in input_stat):
        raise VisualFingerprintError("visual fingerprint result input stat is invalid")
    engine = object_value(result["engine"], "visual fingerprint result engine")
    exact_keys(
        engine,
        "visual fingerprint result engine",
        {
            "name",
            "path",
            "sha256",
            "byte_count",
            "version_label",
            "version_output",
            "version_output_sha256",
            "capabilities_output",
            "capabilities_output_sha256",
        },
    )
    if (
        engine["name"] != "ffmpeg"
        or not isinstance(engine["path"], str)
        or not Path(engine["path"]).is_absolute()
        or not SHA256_RE.fullmatch(str(engine["sha256"]))
        or engine["version_output_sha256"]
        != sha256_bytes(string_value(engine["version_output"], "engine.version_output", 1_000_000).encode("utf-8"))
        or engine["capabilities_output_sha256"]
        != sha256_bytes(string_value(engine["capabilities_output"], "engine.capabilities_output", 4_000_000).encode("utf-8"))
    ):
        raise VisualFingerprintError("visual fingerprint result engine provenance is invalid")
    implementation = object_value(result["implementation"], "visual fingerprint implementation")
    exact_keys(implementation, "visual fingerprint implementation", {"path", "sha256", "byte_count", "python_version"})
    if (
        not isinstance(implementation["path"], str)
        or not Path(implementation["path"]).is_absolute()
        or not SHA256_RE.fullmatch(str(implementation["sha256"]))
        or isinstance(implementation["byte_count"], bool)
        or not isinstance(implementation["byte_count"], int)
        or implementation["byte_count"] <= 0
    ):
        raise VisualFingerprintError("visual fingerprint implementation provenance is invalid")
    configuration = object_value(result["configuration"], "visual fingerprint configuration")
    exact_keys(
        configuration,
        "visual fingerprint configuration",
        {
            "algorithm",
            "pixel_format",
            "width",
            "height",
            "scale_flags",
            "threads",
            "timeout_seconds_per_frame",
            "max_timestamp_drift_ms",
            "samples",
        },
    )
    if (
        configuration["algorithm"] != ALGORITHM
        or configuration["pixel_format"] != PIXEL_FORMAT
        or configuration["width"] != WIDTH
        or configuration["height"] != HEIGHT
        or configuration["scale_flags"] != "bilinear"
        or configuration["threads"] != 1
    ):
        raise VisualFingerprintError("visual fingerprint result configuration is incompatible")
    samples = configuration["samples"]
    if not isinstance(samples, list) or not 1 <= len(samples) <= MAX_SAMPLES:
        raise VisualFingerprintError("visual fingerprint result sample count is invalid")
    sample_ids: set[str] = set()
    for index, sample_raw in enumerate(samples):
        sample = object_value(sample_raw, f"visual fingerprint configuration sample {index}")
        exact_keys(
            sample,
            f"visual fingerprint configuration sample {index}",
            {"sample_id", "window_id", "start_ms", "end_ms", "requested_timestamp_ms", "timestamp_kind"},
        )
        sample_id = identifier(sample["sample_id"], f"visual fingerprint configuration sample {index}.sample_id")
        if sample_id in sample_ids:
            raise VisualFingerprintError("visual fingerprint result sample IDs are not unique")
        sample_ids.add(sample_id)
        if (
            sample["timestamp_kind"] not in {"explicit", "keyframe"}
            or not isinstance(sample["start_ms"], int)
            or not isinstance(sample["end_ms"], int)
            or not isinstance(sample["requested_timestamp_ms"], int)
            or not sample["start_ms"] <= sample["requested_timestamp_ms"] < sample["end_ms"]
            or sample["end_ms"] > input_value["duration_ms"]
        ):
            raise VisualFingerprintError("visual fingerprint result sample window is invalid")
    recipe = object_value(result["recipe"], "visual fingerprint recipe")
    exact_keys(
        recipe,
        "visual fingerprint recipe",
        {
            "schema_version",
            "stage",
            "implementation_version",
            "implementation_sha256",
            "input",
            "engine",
            "extraction",
            "hash_definition",
        },
    )
    recipe_input = object_value(recipe["input"], "visual fingerprint recipe input")
    recipe_engine = object_value(recipe["engine"], "visual fingerprint recipe engine")
    recipe_hash = object_value(recipe["hash_definition"], "visual fingerprint hash definition")
    exact_keys(
        recipe_input,
        "visual fingerprint recipe input",
        {"expected_sha256", "expected_byte_count", "media_id", "artifact_id", "parent_processing_run_id", "duration_ms", "timeline_origin_ms", "video_stream_selector"},
    )
    exact_keys(
        recipe_engine,
        "visual fingerprint recipe engine",
        {"sha256", "version_output_sha256", "capabilities_output_sha256"},
    )
    exact_keys(
        recipe_hash,
        "visual fingerprint hash definition",
        {"grayscale_bytes", "dct_matrix", "coefficients", "dc_bit", "threshold", "bit_order"},
    )
    expected_recipe_input = {
        "expected_sha256": input_value["sha256"],
        "expected_byte_count": input_value["byte_count"],
        "media_id": input_value["media_id"],
        "artifact_id": input_value["artifact_id"],
        "parent_processing_run_id": input_value["parent_processing_run_id"],
        "duration_ms": input_value["duration_ms"],
        "timeline_origin_ms": input_value["timeline_origin_ms"],
        "video_stream_selector": input_value["video_stream_selector"],
    }
    expected_recipe_engine = {
        "sha256": engine["sha256"],
        "version_output_sha256": engine["version_output_sha256"],
        "capabilities_output_sha256": engine["capabilities_output_sha256"],
    }
    expected_hash_definition = {
        "grayscale_bytes": 1024,
        "dct_matrix": "committed_signed_q20_cosine_8x32",
        "coefficients": "top_left_8x8",
        "dc_bit": 0,
        "threshold": "strictly_greater_than_median_of_63_ac_coefficients",
        "bit_order": "row_major_most_significant_bit_first",
    }
    if (
        recipe["schema_version"] != 1
        or recipe["stage"] != EXTRACT_STAGE
        or recipe["implementation_version"] != result["implementation_version"]
        or recipe["implementation_sha256"] != implementation["sha256"]
        or recipe_input != expected_recipe_input
        or recipe_engine != expected_recipe_engine
        or recipe["extraction"] != configuration
        or recipe_hash != expected_hash_definition
    ):
        raise VisualFingerprintError("visual fingerprint recipe dependencies are inconsistent")
    if result["recipe_sha256"] != sha256_bytes(canonical_bytes(result["recipe"])):
        raise VisualFingerprintError("visual fingerprint recipe digest is invalid")
    if result["recipe_id"] != f"recipe_visual_fingerprint_{result['recipe_sha256'][:32]}":
        raise VisualFingerprintError("visual fingerprint recipe ID is invalid")
    expected_key = sha256_bytes(
        canonical_bytes(
            {
                "job_id": result["job_id"],
                "work_order_sha256": result["work_order_sha256"],
                "recipe_sha256": result["recipe_sha256"],
            }
        )
    )
    if result["result_key"] != expected_key or path.parent.name != expected_key:
        raise VisualFingerprintError("visual fingerprint result key/path is invalid")
    if path.parent.stat().st_mode & 0o222 or (path.parent / "frames").stat().st_mode & 0o222:
        raise VisualFingerprintError("visual fingerprint result directories must be sealed")
    frames = result["frames"]
    if not isinstance(frames, list) or len(frames) != len(samples):
        raise VisualFingerprintError("visual fingerprint result has an invalid frame count")
    for ordinal, frame in enumerate(frames):
        validated = validate_frame_record(frame, path.parent, result["result_key"])
        if validated["ordinal"] != ordinal:
            raise VisualFingerprintError("visual fingerprint frame ordinals are invalid")
        sample = samples[ordinal]
        if any(validated[key] != sample[key] for key in sample):
            raise VisualFingerprintError("visual fingerprint frame/sample relationship is invalid")
        timestamp = object_value(validated["decoded_timestamp"], "visual fingerprint decoded timestamp")
        exact_keys(
            timestamp,
            "visual fingerprint decoded timestamp",
            {"pts", "duration_pts", "time_base_numerator", "time_base_denominator", "absolute_timestamp_us", "relative_timestamp_us", "relative_timestamp_ms", "duration_us", "is_keyframe"},
        )
        try:
            absolute = Fraction(
                timestamp["pts"] * timestamp["time_base_numerator"],
                timestamp["time_base_denominator"],
            )
            duration = Fraction(
                timestamp["duration_pts"] * timestamp["time_base_numerator"],
                timestamp["time_base_denominator"],
            )
        except (TypeError, ZeroDivisionError) as error:
            raise VisualFingerprintError("visual fingerprint decoded time base is invalid") from error
        relative = absolute - Fraction(input_value["timeline_origin_ms"], 1000)
        drift = abs(relative * 1000 - sample["requested_timestamp_ms"])
        expected_timestamp = {
            "pts": timestamp["pts"],
            "duration_pts": timestamp["duration_pts"],
            "time_base_numerator": timestamp["time_base_numerator"],
            "time_base_denominator": timestamp["time_base_denominator"],
            "absolute_timestamp_us": round(absolute * 1_000_000),
            "relative_timestamp_us": round(relative * 1_000_000),
            "relative_timestamp_ms": round(relative * 1_000),
            "duration_us": round(duration * 1_000_000),
            "is_keyframe": timestamp["is_keyframe"],
        }
        if (
            timestamp != expected_timestamp
            or timestamp["pts"] < 0
            or timestamp["duration_pts"] <= 0
            or timestamp["time_base_numerator"] <= 0
            or timestamp["time_base_denominator"] <= 0
            or not Fraction(sample["start_ms"], 1) <= relative * 1000 < Fraction(sample["end_ms"], 1)
            or drift > configuration["max_timestamp_drift_ms"]
            or validated["timestamp_drift_us"] != round(drift * 1000)
        ):
            raise VisualFingerprintError("visual fingerprint decoded timing evidence is invalid")
        body = Path(validated["artifact"]["path"]).read_bytes()
        if validated["quality_flags"] != quality_flags(
            body, sample, timestamp, validated["timestamp_drift_us"]
        ):
            raise VisualFingerprintError("visual fingerprint quality flags are invalid")
    commands = result["commands"]
    expected_commands = [
        frame_command(
            engine["path"],
            input_value["path"],
            input_value["timeline_origin_ms"] + sample["requested_timestamp_ms"],
        )
        for sample in samples
    ]
    if commands != expected_commands:
        raise VisualFingerprintError("visual fingerprint command provenance is invalid")
    expected_quality = sorted({flag for frame in frames for flag in frame["quality_flags"]})
    if result["quality_flags"] != expected_quality:
        raise VisualFingerprintError("visual fingerprint global quality flags are invalid")
    processing = object_value(result["processing_run"], "visual fingerprint processing run")
    exact_keys(processing, "visual fingerprint processing run", {"processing_run_id", "started_at", "completed_at", "status"})
    if (
        processing["processing_run_id"] != f"run_visual_fingerprint_{result['result_key'][:32]}"
        or processing["status"] != "completed"
    ):
        raise VisualFingerprintError("visual fingerprint processing run identity is invalid")
    if validate_context(result["catalog_context"]) != result["catalog_context"]:
        raise VisualFingerprintError("visual fingerprint catalog context is invalid")
    return result, observed_sha


def validate_completed_replay(
    result_path: Path,
    expected: dict[str, Any],
) -> dict[str, Any]:
    result, _ = load_completed_extraction(result_path)
    for key in (
        "job_id",
        "work_order_sha256",
        "recipe",
        "recipe_id",
        "recipe_sha256",
        "result_key",
        "implementation",
        "input",
        "engine",
        "configuration",
        "commands",
        "catalog_context",
    ):
        if result[key] != expected[key]:
            raise VisualFingerprintError(f"existing visual fingerprint result has invalid {key}")
    return result


def run_extraction(work_order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    implementation = implementation_observation()
    public = public_order(work_order)
    work_order_sha = sha256_bytes(canonical_bytes(public))
    source_path = Path(work_order["input"]["path"])
    source = stable_observe(
        source_path,
        work_order["input"]["expected_sha256"],
        work_order["input"]["expected_byte_count"],
        "input video",
    )
    source.update(
        {
            key: work_order["input"][key]
            for key in (
                "media_id",
                "artifact_id",
                "parent_processing_run_id",
                "duration_ms",
                "timeline_origin_ms",
                "video_stream_selector",
                "sealed",
            )
        }
    )
    recipe = extraction_recipe(work_order, implementation)
    recipe_sha = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_visual_fingerprint_{recipe_sha[:32]}"
    result_key = sha256_bytes(
        canonical_bytes(
            {
                "job_id": work_order["job_id"],
                "work_order_sha256": work_order_sha,
                "recipe_sha256": recipe_sha,
            }
        )
    )
    run_id = f"run_visual_fingerprint_{result_key[:32]}"
    run_dir = result_layout(
        Path(work_order["output"]["root"]), source["sha256"], recipe_sha, result_key
    )
    result_path = run_dir / "result.json"
    commands = [
        frame_command(
            work_order["_engine_observation"]["path"],
            source["path"],
            source["timeline_origin_ms"] + sample["requested_timestamp_ms"],
        )
        for sample in work_order["extraction"]["samples"]
    ]
    expected = {
        "job_id": work_order["job_id"],
        "work_order_sha256": work_order_sha,
        "recipe": recipe,
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha,
        "result_key": result_key,
        "implementation": implementation,
        "input": source,
        "engine": work_order["_engine_observation"],
        "configuration": work_order["extraction"],
        "commands": commands,
        "catalog_context": work_order["catalog_context"],
    }
    if dry_run:
        return {
            "schema_version": 1,
            "stage": EXTRACT_STAGE,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "planned",
            "dry_run": True,
            **expected,
            "processing_run": None,
            "frames": [],
            "quality_flags": ["not_executed"],
            "result_path": str(result_path),
            "errors": [],
        }
    lock_path = run_dir.parent / f".{result_key}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if result_path.exists():
            return validate_completed_replay(result_path, expected)
        stage_dir = run_dir.parent / f".{result_key}.tmp-{uuid.uuid4().hex}"
        frames_dir = stage_dir / "frames"
        frames_dir.mkdir(parents=True)
        frames: list[dict[str, Any]] = []
        try:
            for ordinal, (sample, command) in enumerate(
                zip(work_order["extraction"]["samples"], commands, strict=True)
            ):
                gray, stderr = decode_frame(
                    command, work_order["extraction"]["timeout_seconds_per_frame"]
                )
                timestamp = parse_showinfo(stderr, source["timeline_origin_ms"])
                exact_relative_ms = Fraction(
                    timestamp["pts"]
                    * timestamp["time_base_numerator"]
                    * 1000,
                    timestamp["time_base_denominator"],
                ) - source["timeline_origin_ms"]
                if not Fraction(sample["start_ms"], 1) <= exact_relative_ms < Fraction(sample["end_ms"], 1):
                    raise VisualFingerprintError(
                        "decoded frame PTS falls outside its declared half-open window"
                    )
                drift = abs(exact_relative_ms - sample["requested_timestamp_ms"])
                if drift > work_order["extraction"]["max_timestamp_drift_ms"]:
                    raise VisualFingerprintError(
                        "decoded frame timestamp drift exceeds max_timestamp_drift_ms"
                    )
                drift_us = round(drift * 1000)
                gray_sha = sha256_bytes(gray)
                phash, popcount = perceptual_hash(gray)
                filename = f"frame-{ordinal:04d}-{sample['sample_id']}.gray"
                staged_artifact = frames_dir / filename
                atomic_write(staged_artifact, gray)
                final_artifact = run_dir / "frames" / filename
                artifact_id = stable_id("artifact", result_key, ordinal, gray_sha)
                artifact = {
                    "artifact_id": artifact_id,
                    "artifact_kind": "visual_fingerprint_gray32",
                    "path": str(final_artifact),
                    "storage_uri": final_artifact.as_uri(),
                    "sha256": gray_sha,
                    "byte_count": len(gray),
                    "width": WIDTH,
                    "height": HEIGHT,
                    "pixel_format": PIXEL_FORMAT,
                    "visibility": "private",
                }
                fingerprint_id = stable_id(
                    "visual_fingerprint",
                    result_key,
                    sample["sample_id"],
                    timestamp,
                    gray_sha,
                    phash,
                )
                frames.append(
                    {
                        "fingerprint_id": fingerprint_id,
                        "ordinal": ordinal,
                        **sample,
                        "decoded_timestamp": timestamp,
                        "timestamp_drift_us": drift_us,
                        "algorithm": ALGORITHM,
                        "phash_bits": PHASH_BITS,
                        "phash_hex": phash,
                        "phash_popcount": popcount,
                        "exact_gray_sha256": gray_sha,
                        "quality_flags": quality_flags(gray, sample, timestamp, drift_us),
                        "artifact": artifact,
                    }
                )
            after_source = stable_observe(
                source_path, source["sha256"], source["byte_count"], "input video"
            )
            if after_source["stat"] != source["stat"]:
                raise VisualFingerprintError("input video metadata changed during extraction")
            after_engine = capture_engine(Path(work_order["_engine_observation"]["path"]))
            if after_engine != work_order["_engine_observation"]:
                raise VisualFingerprintError("FFmpeg changed during extraction")
            if implementation_observation() != implementation:
                raise VisualFingerprintError("visual fingerprint implementation changed during extraction")
            completed_at = utc_now()
            global_flags = sorted({flag for frame in frames for flag in frame["quality_flags"]})
            result = {
                "schema_version": 1,
                "stage": EXTRACT_STAGE,
                "implementation_version": IMPLEMENTATION_VERSION,
                "status": "completed",
                "dry_run": False,
                **expected,
                "processing_run": {
                    "processing_run_id": run_id,
                    "started_at": started_at,
                    "completed_at": completed_at,
                    "status": "completed",
                },
                "frames": frames,
                "quality_flags": global_flags,
                "result_path": str(result_path),
                "errors": [],
            }
            staged_result = stage_dir / "result.json"
            atomic_write(staged_result, pretty_bytes(result))
            for artifact in frames_dir.iterdir():
                seal_path(artifact)
            seal_path(staged_result)
            seal_path(frames_dir)
            seal_path(stage_dir)
            try:
                os.rename(stage_dir, run_dir)
            except FileExistsError:
                shutil.rmtree(stage_dir, ignore_errors=True)
            return validate_completed_replay(result_path, expected)
        except Exception:
            if stage_dir.exists():
                for path in sorted(stage_dir.rglob("*"), reverse=True):
                    try:
                        path.chmod(path.stat().st_mode | 0o700)
                    except OSError:
                        pass
                shutil.rmtree(stage_dir, ignore_errors=True)
            raise


def validate_side(raw: object, role: str) -> dict[str, Any]:
    value = object_value(raw, role)
    exact_keys(value, role, {"role", "result_path", "expected_sha256", "frame_ids"})
    if value["role"] != role:
        raise VisualFingerprintError(f"{role}.role must be {role}")
    result_path = resolved_file(value["result_path"], f"{role}.result_path", sealed=True)
    frame_ids = value["frame_ids"]
    if not isinstance(frame_ids, list) or not 1 <= len(frame_ids) <= MAX_SELECTED_FRAMES:
        raise VisualFingerprintError(
            f"{role}.frame_ids must contain 1..{MAX_SELECTED_FRAMES} IDs"
        )
    normalized_ids = [identifier(frame_id, f"{role}.frame_ids") for frame_id in frame_ids]
    if len(set(normalized_ids)) != len(normalized_ids):
        raise VisualFingerprintError(f"{role}.frame_ids must be unique")
    return {
        "role": role,
        "result_path": str(result_path),
        "expected_sha256": digest_value(value["expected_sha256"], f"{role}.expected_sha256"),
        "frame_ids": normalized_ids,
    }


def validate_compare_context(raw: object) -> dict[str, Any] | None:
    if raw is None:
        return None
    value = object_value(raw, "catalog_context")
    exact_keys(value, "catalog_context", {"query", "candidate"})
    return {
        "query": validate_context(value["query"], "catalog_context.query"),
        "candidate": validate_context(value["candidate"], "catalog_context.candidate"),
    }


def validate_compare_order(raw: object) -> dict[str, Any]:
    value = object_value(raw, "compare work order")
    exact_keys(
        value,
        "compare work order",
        {
            "schema_version",
            "job_id",
            "method",
            "query",
            "candidate",
            "threshold",
            "catalog_context",
            "output",
        },
    )
    if value["schema_version"] != 1 or value["method"] != COMPARE_METHOD:
        raise VisualFingerprintError(f"compare requires schema 1 and method {COMPARE_METHOD}")
    query = validate_side(value["query"], "query")
    candidate = validate_side(value["candidate"], "candidate")
    if query["result_path"] == candidate["result_path"] or query["expected_sha256"] == candidate["expected_sha256"]:
        raise VisualFingerprintError("comparison rejects a result paired with itself")
    threshold = object_value(value["threshold"], "threshold")
    exact_keys(
        threshold,
        "threshold",
        {"maximum_hamming_distance", "top_k", "max_pairwise_comparisons"},
    )
    maximum_hamming = integer(
        threshold["maximum_hamming_distance"], "threshold.maximum_hamming_distance", 0, PHASH_BITS
    )
    top_k = integer(threshold["top_k"], "threshold.top_k", 1, 100)
    maximum_pairs = integer(
        threshold["max_pairwise_comparisons"],
        "threshold.max_pairwise_comparisons",
        1,
        MAX_PAIRWISE_COMPARISONS,
    )
    if len(query["frame_ids"]) * len(candidate["frame_ids"]) > maximum_pairs:
        raise VisualFingerprintError("selected frames exceed threshold.max_pairwise_comparisons")
    output = object_value(value["output"], "output")
    exact_keys(output, "output", {"root"})
    return {
        "schema_version": 1,
        "job_id": identifier(value["job_id"], "job_id"),
        "method": COMPARE_METHOD,
        "query": query,
        "candidate": candidate,
        "threshold": {
            "maximum_hamming_distance": maximum_hamming,
            "top_k": top_k,
            "max_pairwise_comparisons": maximum_pairs,
        },
        "catalog_context": validate_compare_context(value["catalog_context"]),
        "output": {"root": str(validate_output_root(output["root"]))},
    }


def selected_side(side: dict[str, Any]) -> dict[str, Any]:
    result, observed_sha = load_completed_extraction(
        Path(side["result_path"]), side["expected_sha256"]
    )
    by_id = {frame["fingerprint_id"]: frame for frame in result["frames"]}
    missing = [frame_id for frame_id in side["frame_ids"] if frame_id not in by_id]
    if missing:
        raise VisualFingerprintError(f"{side['role']} references unknown fingerprint IDs: {missing}")
    frames = [
        {
            "fingerprint_id": frame_id,
            "sample_id": by_id[frame_id]["sample_id"],
            "requested_timestamp_ms": by_id[frame_id]["requested_timestamp_ms"],
            "decoded_relative_timestamp_ms": by_id[frame_id]["decoded_timestamp"]["relative_timestamp_ms"],
            "phash_hex": by_id[frame_id]["phash_hex"],
            "exact_gray_sha256": by_id[frame_id]["exact_gray_sha256"],
            "quality_flags": by_id[frame_id]["quality_flags"],
        }
        for frame_id in side["frame_ids"]
    ]
    return {
        "role": side["role"],
        "result_path": side["result_path"],
        "result_sha256": observed_sha,
        "result_key": result["result_key"],
        "media_id": result["input"]["media_id"],
        "algorithm": ALGORITHM,
        "phash_bits": PHASH_BITS,
        "frames": frames,
        "unchanged": True,
    }


def compare_recipe(work_order: dict[str, Any], query: dict[str, Any], candidate: dict[str, Any], implementation: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "stage": COMPARE_STAGE,
        "implementation_version": IMPLEMENTATION_VERSION,
        "implementation_sha256": implementation["sha256"],
        "method": COMPARE_METHOD,
        "algorithm": ALGORITHM,
        "phash_bits": PHASH_BITS,
        "query_result_sha256": query["result_sha256"],
        "candidate_result_sha256": candidate["result_sha256"],
        "query_frame_ids": [frame["fingerprint_id"] for frame in query["frames"]],
        "candidate_frame_ids": [frame["fingerprint_id"] for frame in candidate["frames"]],
        "threshold": work_order["threshold"],
        "calibration_state": "not_calibrated",
    }


def comparison_evidence(query: dict[str, Any], candidate: dict[str, Any], threshold: dict[str, Any]) -> dict[str, Any]:
    pairs: list[dict[str, Any]] = []
    for query_frame in query["frames"]:
        query_hash = int(query_frame["phash_hex"], 16)
        for candidate_frame in candidate["frames"]:
            distance = (query_hash ^ int(candidate_frame["phash_hex"], 16)).bit_count()
            pairs.append(
                {
                    "query_fingerprint_id": query_frame["fingerprint_id"],
                    "candidate_fingerprint_id": candidate_frame["fingerprint_id"],
                    "hamming_distance": distance,
                    "normalized_hamming_distance": round(distance / PHASH_BITS, 6),
                    "raw_similarity": round((PHASH_BITS - distance) / PHASH_BITS, 6),
                    "exact_gray_equal": query_frame["exact_gray_sha256"] == candidate_frame["exact_gray_sha256"],
                }
            )
    pairs.sort(
        key=lambda pair: (
            pair["hamming_distance"],
            pair["query_fingerprint_id"],
            pair["candidate_fingerprint_id"],
        )
    )
    best = pairs[0]["hamming_distance"]
    meets = best <= threshold["maximum_hamming_distance"]
    return {
        "match_candidate_id": stable_id(
            "visual_match_candidate",
            query["result_sha256"],
            candidate["result_sha256"],
            [frame["fingerprint_id"] for frame in query["frames"]],
            [frame["fingerprint_id"] for frame in candidate["frames"]],
            threshold,
        ),
        "pairwise_comparisons": len(pairs),
        "best_hamming_distance": best,
        "best_normalized_hamming_distance": round(best / PHASH_BITS, 6),
        "best_raw_similarity": round((PHASH_BITS - best) / PHASH_BITS, 6),
        "exact_gray_pair_count": sum(pair["exact_gray_equal"] for pair in pairs),
        "top_pairs": pairs[: threshold["top_k"]],
        "threshold_state": (
            "meets_configured_threshold" if meets else "does_not_meet_configured_threshold"
        ),
        "candidate_emitted": meets,
        "decision_state": (
            "candidate_for_human_review" if meets else "below_configured_threshold"
        ),
        "score_semantics": "raw_64_bit_phash_hamming_not_probability",
        "calibration_state": "not_calibrated",
        "calibrated_probability": None,
        "requires_human_review": True,
        "assertions": {
            "person_identity": False,
            "duplicate": False,
            "parent": False,
            "ownership": False,
            "relationship": False,
            "unrelated": False,
        },
        "warning": (
            "Candidate-routing evidence only. The raw threshold is not calibrated and "
            "does not establish identity, duplicate/parent status, ownership, a relationship, "
            "or that below-threshold material is unrelated."
        ),
    }


def compare_layout(root: Path, query_sha: str, candidate_sha: str, result_key: str) -> Path:
    pair_digest = sha256_bytes(canonical_bytes(sorted((query_sha, candidate_sha))))
    return root / "vision" / "visual-fingerprint-comparisons" / "sha256" / pair_digest[:2] / pair_digest / "results" / result_key


def validate_completed_compare(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    result_path = resolved_file(str(path), "visual comparison result", sealed=True)
    result = object_value(load_json(result_path), "visual comparison result")
    exact_keys(
        result,
        "visual comparison result",
        {
            "schema_version",
            "stage",
            "implementation_version",
            "status",
            "dry_run",
            "job_id",
            "work_order_sha256",
            "recipe",
            "recipe_id",
            "recipe_sha256",
            "result_key",
            "implementation",
            "method",
            "query",
            "candidate",
            "threshold",
            "comparison",
            "processing_run",
            "catalog_context",
            "result_path",
            "errors",
        },
    )
    if result["schema_version"] != 1 or result["stage"] != COMPARE_STAGE or result["status"] != "completed" or result["dry_run"] is not False or result["errors"] != [] or result["result_path"] != str(path):
        raise VisualFingerprintError("visual comparison result is not a completed v1 envelope")
    for key, value in expected.items():
        if result[key] != value:
            raise VisualFingerprintError(f"existing visual comparison has invalid {key}")
    expected_comparison = comparison_evidence(
        expected["query"], expected["candidate"], expected["threshold"]
    )
    if result["comparison"] != expected_comparison:
        raise VisualFingerprintError("existing visual comparison evidence is invalid")
    if result["recipe_sha256"] != sha256_bytes(canonical_bytes(result["recipe"])):
        raise VisualFingerprintError("existing visual comparison recipe digest is invalid")
    expected_key = sha256_bytes(
        canonical_bytes(
            {
                "job_id": result["job_id"],
                "work_order_sha256": result["work_order_sha256"],
                "recipe_sha256": result["recipe_sha256"],
            }
        )
    )
    if result["result_key"] != expected_key or result_path.parent.name != expected_key:
        raise VisualFingerprintError("existing visual comparison result key is invalid")
    if result_path.parent.stat().st_mode & 0o222:
        raise VisualFingerprintError("visual comparison result directory must be sealed")
    return result


def run_compare(work_order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    implementation = implementation_observation()
    query = selected_side(work_order["query"])
    candidate = selected_side(work_order["candidate"])
    if set(frame["fingerprint_id"] for frame in query["frames"]) & set(
        frame["fingerprint_id"] for frame in candidate["frames"]
    ):
        raise VisualFingerprintError("comparison sides may not share a fingerprint ID")
    recipe = compare_recipe(work_order, query, candidate, implementation)
    recipe_sha = sha256_bytes(canonical_bytes(recipe))
    recipe_id = f"recipe_visual_fingerprint_compare_{recipe_sha[:32]}"
    work_order_sha = sha256_bytes(canonical_bytes(work_order))
    result_key = sha256_bytes(
        canonical_bytes(
            {
                "job_id": work_order["job_id"],
                "work_order_sha256": work_order_sha,
                "recipe_sha256": recipe_sha,
            }
        )
    )
    result_path = compare_layout(
        Path(work_order["output"]["root"]),
        query["result_sha256"],
        candidate["result_sha256"],
        result_key,
    ) / "result.json"
    expected = {
        "job_id": work_order["job_id"],
        "work_order_sha256": work_order_sha,
        "recipe": recipe,
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha,
        "result_key": result_key,
        "implementation": implementation,
        "method": COMPARE_METHOD,
        "query": query,
        "candidate": candidate,
        "threshold": work_order["threshold"],
        "catalog_context": work_order["catalog_context"],
    }
    if dry_run:
        return {
            "schema_version": 1,
            "stage": COMPARE_STAGE,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "planned",
            "dry_run": True,
            **expected,
            "comparison": None,
            "processing_run": None,
            "result_path": str(result_path),
            "errors": [],
        }
    lock_path = result_path.parent.parent / f".{result_key}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if result_path.exists():
            return validate_completed_compare(result_path, expected)
        stage_dir = result_path.parent.parent / f".{result_key}.tmp-{uuid.uuid4().hex}"
        stage_dir.mkdir()
        try:
            started_at = utc_now()
            comparison = comparison_evidence(query, candidate, work_order["threshold"])
            if implementation_observation() != implementation:
                raise VisualFingerprintError("visual fingerprint implementation changed during comparison")
            result = {
                "schema_version": 1,
                "stage": COMPARE_STAGE,
                "implementation_version": IMPLEMENTATION_VERSION,
                "status": "completed",
                "dry_run": False,
                **expected,
                "comparison": comparison,
                "processing_run": {
                    "processing_run_id": f"run_visual_fingerprint_compare_{result_key[:32]}",
                    "started_at": started_at,
                    "completed_at": utc_now(),
                    "status": "completed",
                },
                "result_path": str(result_path),
                "errors": [],
            }
            staged_result = stage_dir / "result.json"
            atomic_write(staged_result, pretty_bytes(result))
            seal_path(staged_result)
            seal_path(stage_dir)
            try:
                os.rename(stage_dir, result_path.parent)
            except FileExistsError:
                stage_dir.chmod(stage_dir.stat().st_mode | 0o700)
                shutil.rmtree(stage_dir, ignore_errors=True)
            return validate_completed_compare(result_path, expected)
        except Exception:
            if stage_dir.exists():
                stage_dir.chmod(stage_dir.stat().st_mode | 0o700)
                shutil.rmtree(stage_dir, ignore_errors=True)
            raise


def read_work_order(path_text: str, mode: str) -> dict[str, Any]:
    path = resolved_file(path_text, "work-order path", sealed=False)
    raw = load_json(path)
    return validate_extraction_order(raw) if mode == "extract" else validate_compare_order(raw)


def inspect_engine() -> dict[str, Any]:
    discovered = shutil.which("ffmpeg")
    if not discovered:
        raise VisualFingerprintError("ffmpeg is not available on PATH")
    path = Path(discovered).resolve(strict=True)
    observed = capture_engine(path)
    return {
        "executable": observed["path"],
        "expected_sha256": observed["sha256"],
        "expected_byte_count": observed["byte_count"],
        "expected_version_output_sha256": observed["version_output_sha256"],
        "expected_capabilities_output_sha256": observed["capabilities_output_sha256"],
        "version_label": observed["version_label"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inspect-engine")
    validate = commands.add_parser("validate")
    validate.add_argument("--work-order", required=True)
    run = commands.add_parser("run")
    run.add_argument("--work-order", required=True)
    run.add_argument("--dry-run", action="store_true")
    compare = commands.add_parser("compare")
    compare.add_argument("--work-order", required=True)
    compare.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inspect-engine":
            result = inspect_engine()
        elif args.command == "validate":
            validate_extraction_order(load_json(resolved_file(args.work_order, "work-order path", sealed=False)))
            result = {"status": "valid"}
        elif args.command == "run":
            result = run_extraction(read_work_order(args.work_order, "extract"), dry_run=args.dry_run)
        else:
            result = run_compare(read_work_order(args.work_order, "compare"), dry_run=args.dry_run)
    except (VisualFingerprintError, OSError, subprocess.SubprocessError) as error:
        print(f"visual-fingerprint: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
