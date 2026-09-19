#!/usr/bin/env python3
"""Strict private whisper.cpp/Silero voice-activity adapter.

The adapter emits speech-candidate intervals only.  It does not count speakers,
attribute voices, associate faces, detect actions, or create calibrated confidence.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

try:
    from .asr_whispercpp import (
        ASRError,
        RetainedFile,
        atomic_write,
        canonical_bytes,
        minimal_environment,
        pretty_json,
        retained_verified_file,
        sha256_bytes,
        stable_file_bytes,
        sync_directory,
        verify_retained_file,
    )
    from . import whispercpp_vad_profiles as profiles
except ImportError:  # Direct execution places pipeline/ itself on sys.path.
    from asr_whispercpp import (  # type: ignore[no-redef]
        ASRError,
        RetainedFile,
        atomic_write,
        canonical_bytes,
        minimal_environment,
        pretty_json,
        retained_verified_file,
        sha256_bytes,
        stable_file_bytes,
        sync_directory,
        verify_retained_file,
    )
    import whispercpp_vad_profiles as profiles  # type: ignore[no-redef]


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.3.0"
STAGE = "vad_whispercpp"
SEGMENTATION_PROFILE = "whispercpp-v1.8.7-silero-v6.2.0-cli-defaults-v1"
PARAMETER_BINDING = "reviewed_source_defaults_with_broken_cli_fields_omitted_v1"
DESCRIPTOR_POLICY = "linux_sealed_memfd_retained_component_io_v3"
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_STDOUT_BYTES = 8 * 1024 * 1024
MAX_STDERR_BYTES = 1024 * 1024
MAX_INPUT_BYTES = 512 * 1024 * 1024
MAX_AUDIO_DURATION_MS = 2 * 60 * 60 * 1_000
MAX_SEGMENTS = 100_000
MAX_TAIL_OVERRUN_MS = 64
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_SECOND_RE = re.compile(
    r"^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])"
    r"T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$"
)
HEADER_RE = re.compile(r"^Detected ([0-9]{1,6}) speech segments:$")
SEGMENT_RE = re.compile(
    r"^Speech segment ([0-9]{1,6}): start = ([0-9]{1,9}\.00), "
    r"end = ([0-9]{1,9}\.00)$"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_OUTPUT_ROOTS = (
    REPOSITORY_ROOT / "public",
    REPOSITORY_ROOT / "src" / "data" / "corpus",
    REPOSITORY_ROOT / "dist",
)
PRIVATE_REPOSITORY_OUTPUT_ROOTS = (
    REPOSITORY_ROOT / "research" / "corpus",
    REPOSITORY_ROOT / "pipeline" / ".test-whispercpp-vad",
)


class VADError(RuntimeError):
    """The VAD work order, provenance, execution, or result failed closed."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def canonical_utc_second(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or UTC_SECOND_RE.fullmatch(value) is None:
        raise VADError(f"{label} must be canonical UTC with whole-second precision")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise VADError(f"{label} is not a real UTC timestamp") from error
    return parsed


def duplicate_rejecting_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VADError(f"JSON contains duplicate object key {key!r}")
        result[key] = value
    return result


def strict_json_bytes(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise VADError(
            f"{label} is not strict UTF-8 at bytes [{error.start},{error.end})"
        ) from error
    try:
        return json.loads(text, object_pairs_hook=duplicate_rejecting_object)
    except json.JSONDecodeError as error:
        raise VADError(f"{label} is not valid JSON: {error}") from error


def strict_json_file(path: Path, label: str) -> Any:
    try:
        body = stable_file_bytes(path, maximum_bytes=MAX_JSON_BYTES, label=label)
    except ASRError as error:
        raise VADError(str(error)) from error
    return strict_json_bytes(body, label)


def exact_keys(value: Any, label: str, required: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise VADError(f"{label} must be an object")
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - required)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise VADError(f"{label} has " + "; ".join(details))
    return value


def text_value(value: Any, label: str, maximum: int = 1_000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise VADError(f"{label} must be a non-empty bounded string")
    return value


def identifier(value: Any, label: str) -> str:
    result = text_value(value, label, 256)
    if not ID_RE.fullmatch(result):
        raise VADError(f"{label} contains unsupported characters")
    return result


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise VADError(f"{label} must be a lowercase SHA-256")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise VADError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise VADError(f"{label} must be between {minimum} and {maximum}")
    return value


def absolute_file(value: Any, label: str, *, executable: bool = False) -> Path:
    raw = text_value(value, label, 4_096)
    if "://" in raw:
        raise VADError(f"{label} must be a local path")
    path = Path(raw)
    if not path.is_absolute():
        raise VADError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
        observed = path.lstat()
    except OSError as error:
        raise VADError(f"{label} is not a current readable file: {error}") from error
    if resolved != path or stat.S_ISLNK(observed.st_mode) or not path.is_file():
        raise VADError(f"{label} must be a resolved regular file without symlinks")
    if executable and not os.access(path, os.X_OK):
        raise VADError(f"{label} is not executable")
    return path


def absolute_output_root(value: Any) -> Path:
    raw = text_value(value, "output.root", 4_096)
    if "://" in raw:
        raise VADError("output.root must be a local path")
    path = Path(raw)
    if not path.is_absolute() or path == Path("/"):
        raise VADError("output.root must be a specific absolute directory")
    normalized = Path(os.path.normpath(str(path)))
    if normalized != path:
        raise VADError("output.root must not contain dot traversal")
    for forbidden in (Path("/tmp"), Path("/var/tmp"), *FORBIDDEN_OUTPUT_ROOTS):
        if path == forbidden or forbidden in path.parents:
            raise VADError(f"output.root may not be under {forbidden}")
    if (
        (path == REPOSITORY_ROOT or REPOSITORY_ROOT in path.parents)
        and not any(root == path or root in path.parents for root in PRIVATE_REPOSITORY_OUTPUT_ROOTS)
    ):
        raise VADError(
            "repository-local output.root must be under the ignored research/corpus tree"
        )
    parent = path.parent
    if not parent.exists():
        raise VADError("output.root direct parent must already exist")
    if parent.resolve(strict=True) != parent or not parent.is_dir():
        raise VADError("output.root must not traverse a symlinked parent")
    if path.exists() and (path.resolve(strict=True) != path or not path.is_dir()):
        raise VADError("output.root must be a resolved directory or a new path")
    return path


def validate_file_reference(
    raw: Any, label: str, *, executable: bool = False
) -> dict[str, Any]:
    value = exact_keys(
        raw, label, {"path", "expected_sha256", "expected_byte_count"}
    )
    return {
        "path": str(absolute_file(value["path"], f"{label}.path", executable=executable)),
        "expected_sha256": sha256_value(
            value["expected_sha256"], f"{label}.expected_sha256"
        ),
        "expected_byte_count": integer(
            value["expected_byte_count"],
            f"{label}.expected_byte_count",
            1,
            1 << 50,
        ),
    }


def validate_input(raw: Any) -> dict[str, Any]:
    value = exact_keys(
        raw,
        "input",
        {
            "path",
            "expected_sha256",
            "expected_byte_count",
            "media_id",
            "artifact_id",
            "parent_processing_run_id",
        },
    )
    reference = validate_file_reference(
        {key: value[key] for key in ("path", "expected_sha256", "expected_byte_count")},
        "input",
    )
    if reference["expected_byte_count"] > MAX_INPUT_BYTES:
        raise VADError(f"input exceeds the {MAX_INPUT_BYTES}-byte work-unit limit")
    media_id = identifier(value["media_id"], "input.media_id")
    if media_id != f"media_sha256_{reference['expected_sha256']}":
        raise VADError("input.media_id must equal media_sha256_<input.expected_sha256>")
    return reference | {
        "media_id": media_id,
        "artifact_id": identifier(value["artifact_id"], "input.artifact_id"),
        "parent_processing_run_id": identifier(
            value["parent_processing_run_id"], "input.parent_processing_run_id"
        ),
    }


def validate_engine(raw: Any) -> dict[str, Any]:
    value = exact_keys(
        raw,
        "engine",
        {
            "executable",
            "expected_sha256",
            "expected_byte_count",
            "profile_id",
            "version_label",
            "version_evidence",
            "build",
        },
    )
    reference = validate_file_reference(
        {
            "path": value["executable"],
            "expected_sha256": value["expected_sha256"],
            "expected_byte_count": value["expected_byte_count"],
        },
        "engine",
        executable=True,
    )
    build = exact_keys(
        value["build"],
        "engine.build",
        {"repository", "revision", "target", "configuration"},
    )
    configuration = build["configuration"]
    if not isinstance(configuration, list) or len(configuration) > 64:
        raise VADError("engine.build.configuration must be a bounded array")
    return {
        "executable": reference["path"],
        "expected_sha256": reference["expected_sha256"],
        "expected_byte_count": reference["expected_byte_count"],
        "profile_id": identifier(value["profile_id"], "engine.profile_id"),
        "version_label": text_value(value["version_label"], "engine.version_label", 500),
        "version_evidence": text_value(
            value["version_evidence"], "engine.version_evidence", 500
        ),
        "build": {
            "repository": text_value(build["repository"], "engine.build.repository"),
            "revision": text_value(build["revision"], "engine.build.revision", 256),
            "target": text_value(build["target"], "engine.build.target", 256),
            "configuration": [
                text_value(item, f"engine.build.configuration[{index}]", 256)
                for index, item in enumerate(configuration)
            ],
        },
    }


def validate_model(raw: Any) -> dict[str, Any]:
    value = exact_keys(
        raw,
        "model",
        {
            "path",
            "expected_sha256",
            "expected_byte_count",
            "profile_id",
            "model_id",
            "name",
            "revision",
            "source",
            "license_label",
        },
    )
    reference = validate_file_reference(
        {key: value[key] for key in ("path", "expected_sha256", "expected_byte_count")},
        "model",
    )
    return reference | {
        "profile_id": identifier(value["profile_id"], "model.profile_id"),
        "model_id": identifier(value["model_id"], "model.model_id"),
        "name": text_value(value["name"], "model.name", 500),
        "revision": text_value(value["revision"], "model.revision", 500),
        "source": text_value(value["source"], "model.source"),
        "license_label": text_value(value["license_label"], "model.license_label", 500),
    }


def validate_parameters(raw: Any) -> dict[str, Any]:
    value = exact_keys(
        raw,
        "parameters",
        {
            "segmentation_profile",
            "parameter_binding",
            "threads",
            "threshold",
            "min_speech_duration_ms",
            "min_silence_duration_ms",
            "max_speech_duration_state",
            "speech_pad_ms",
            "samples_overlap_seconds",
            "use_gpu",
            "timeout_seconds",
        },
    )
    if type(value["use_gpu"]) is not bool:
        raise VADError("parameters.use_gpu must be a boolean")
    expected = {
        "segmentation_profile": SEGMENTATION_PROFILE,
        "parameter_binding": PARAMETER_BINDING,
        "threshold": 0.5,
        "min_speech_duration_ms": 250,
        "min_silence_duration_ms": 100,
        "max_speech_duration_state": "float_max_default",
        "speech_pad_ms": 30,
        "samples_overlap_seconds": 0.1,
        "use_gpu": False,
    }
    for key, expected_value in expected.items():
        if value[key] != expected_value:
            raise VADError(f"parameters.{key} must equal {expected_value!r}")
    return expected | {
        "threads": integer(value["threads"], "parameters.threads", 1, 16),
        "timeout_seconds": integer(
            value["timeout_seconds"], "parameters.timeout_seconds", 1, 7_200
        ),
    }


def validate_catalog_context(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    value = exact_keys(
        raw,
        "catalog_context",
        {"recording_id", "rendition_id", "coordinate_system", "source_start_ms"},
    )
    if value["coordinate_system"] != "rendition_media_ms":
        raise VADError("catalog_context.coordinate_system must be rendition_media_ms")
    return {
        "recording_id": identifier(value["recording_id"], "catalog_context.recording_id"),
        "rendition_id": identifier(value["rendition_id"], "catalog_context.rendition_id"),
        "coordinate_system": "rendition_media_ms",
        "source_start_ms": integer(
            value["source_start_ms"],
            "catalog_context.source_start_ms",
            0,
            7 * 24 * 60 * 60 * 1_000,
        ),
    }


def validate_work_order(raw: Any) -> dict[str, Any]:
    value = exact_keys(
        raw,
        "work order",
        {
            "schema_version",
            "job_id",
            "input",
            "engine",
            "model",
            "parameters",
            "catalog_context",
            "output",
        },
    )
    if type(value["schema_version"]) is not int or value["schema_version"] != CONTRACT_VERSION:
        raise VADError(f"schema_version must be {CONTRACT_VERSION}")
    job_id = value["job_id"]
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise VADError("job_id contains unsupported characters or is too long")
    output = exact_keys(value["output"], "output", {"root"})
    result = {
        "schema_version": CONTRACT_VERSION,
        "job_id": job_id,
        "input": validate_input(value["input"]),
        "engine": validate_engine(value["engine"]),
        "model": validate_model(value["model"]),
        "parameters": validate_parameters(value["parameters"]),
        "catalog_context": validate_catalog_context(value["catalog_context"]),
        "output": {"root": str(absolute_output_root(output["root"]))},
    }
    output_root = Path(result["output"]["root"])
    for path in (
        Path(result["input"]["path"]),
        Path(result["engine"]["executable"]),
        Path(result["model"]["path"]),
    ):
        if path == output_root or output_root in path.parents:
            raise VADError("input, executable, and model must be outside output.root")
    return result


def require_expected_size(
    retained: RetainedFile, expected: int, label: str
) -> None:
    observed = retained.observation["byte_count"]
    if observed != expected:
        raise VADError(
            f"{label} byte count mismatch: expected {expected}, observed {observed}"
        )


def require_sealed_input(retained: RetainedFile) -> None:
    mode = retained.identity[6]
    links = retained.identity[7]
    if mode != 0o400 or links != 1:
        raise VADError(
            "VAD input FLAC must be a sealed mode-0400 file with exactly one hard link"
        )


def require_secure_parent_chain(path: Path, label: str) -> None:
    """Reject writable or foreign-owned ancestors before executing pinned assets.

    Retained descriptors close path-replacement races for the exact inode.  This
    additional operational boundary prevents the reviewed engine/model path from
    living beneath a group/world-writable or unrelated-user directory.  The root
    directory is intentionally allowed to be root-owned.
    """

    current = path.parent
    while True:
        try:
            observed = current.lstat()
        except OSError as error:
            raise VADError(f"{label} parent cannot be inspected: {error}") from error
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise VADError(f"{label} parent chain must contain only real directories")
        mode = stat.S_IMODE(observed.st_mode)
        if mode & 0o002 or (observed.st_uid != os.geteuid() and mode & 0o020):
            raise VADError(
                f"{label} parent chain must not be world-writable or foreign-owned "
                "and group-writable"
            )
        if current.parent == current:
            break
        current = current.parent


def require_sealed_asset(
    retained: RetainedFile, label: str, *, executable: bool
) -> None:
    observed = os.fstat(retained.descriptor)
    expected_mode = 0o500 if executable else 0o400
    if (
        observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != expected_mode
        or observed.st_nlink != 1
    ):
        raise VADError(
            f"{label} must be current-user-owned, mode-{expected_mode:04o}, "
            "and have exactly one hard link"
        )
    require_secure_parent_chain(retained.path, label)


@contextmanager
def sealed_execution_copy(
    retained: RetainedFile, label: str, *, executable: bool
) -> Iterator[tuple[int, str]]:
    """Copy one verified asset into a write-sealed anonymous Linux descriptor."""

    required_os = ("memfd_create", "MFD_ALLOW_SEALING")
    required_fcntl = ("F_ADD_SEALS", "F_GET_SEALS", "F_SEAL_SEAL", "F_SEAL_WRITE",
                      "F_SEAL_GROW", "F_SEAL_SHRINK")
    if any(not hasattr(os, name) for name in required_os) or any(
        not hasattr(fcntl, name) for name in required_fcntl
    ):
        raise VADError("Linux memfd sealing is unavailable; refusing native VAD execution")
    flags = os.MFD_ALLOW_SEALING | getattr(os, "MFD_CLOEXEC", 0)
    descriptor = os.memfd_create(f"himr-vad-{label}", flags=flags)
    try:
        expected_size = retained.identity[2]
        digest = hashlib.sha256()
        offset = 0
        while offset < expected_size:
            chunk = os.pread(retained.descriptor, min(1024 * 1024, expected_size - offset), offset)
            if not chunk:
                raise VADError(f"{label} ended while creating its sealed execution copy")
            digest.update(chunk)
            written = 0
            while written < len(chunk):
                count = os.write(descriptor, chunk[written:])
                if count <= 0:
                    raise VADError(f"{label} sealed execution copy could not be written")
                written += count
            offset += len(chunk)
        if (
            offset != expected_size
            or digest.hexdigest() != retained.observation["sha256"]
        ):
            raise VADError(f"{label} sealed execution copy differs from verified bytes")
        os.fchmod(descriptor, 0o500 if executable else 0o400)
        seals = (
            fcntl.F_SEAL_SEAL
            | fcntl.F_SEAL_WRITE
            | fcntl.F_SEAL_GROW
            | fcntl.F_SEAL_SHRINK
        )
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS, seals)
        if fcntl.fcntl(descriptor, fcntl.F_GET_SEALS) != seals:
            raise VADError(f"{label} anonymous execution descriptor is not fully sealed")
        proc_path = f"/proc/self/fd/{descriptor}"
        observed = os.stat(proc_path)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_size != expected_size
            or (executable and not os.access(proc_path, os.X_OK))
        ):
            raise VADError(f"{label} sealed execution descriptor is unusable")
        yield descriptor, proc_path
    finally:
        os.close(descriptor)


def require_profile_metadata(
    requested: dict[str, Any], observed_profile: dict[str, Any], label: str
) -> None:
    keys = (
        ("profile_id", "profile_id"),
        ("version_label", "version_label"),
        ("version_evidence", "version_evidence"),
    ) if label == "engine" else (
        ("profile_id", "profile_id"),
        ("model_id", "model_id"),
        ("name", "name"),
        ("revision", "revision"),
        ("source", "source"),
        ("license_label", "license_label"),
    )
    for requested_key, profile_key in keys:
        if requested[requested_key] != observed_profile[profile_key]:
            raise VADError(
                f"{label}.{requested_key} does not match the exact reviewed profile"
            )
    if label == "engine" and requested["build"] != observed_profile["build"]:
        raise VADError("engine.build does not match the exact reviewed profile")


def parse_flac_streaminfo(retained: RetainedFile) -> dict[str, Any]:
    header = os.pread(retained.descriptor, 42, 0)
    if len(header) != 42 or header[:4] != b"fLaC":
        raise VADError("input must begin with a complete FLAC STREAMINFO block")
    block_type = header[4] & 0x7F
    block_length = int.from_bytes(header[5:8], "big")
    if block_type != 0 or block_length != 34:
        raise VADError("input first FLAC metadata block must be 34-byte STREAMINFO")
    streaminfo = header[8:42]
    packed = int.from_bytes(streaminfo[10:18], "big")
    sample_rate_hz = (packed >> 44) & 0xFFFFF
    channels = ((packed >> 41) & 0x7) + 1
    bits_per_sample = ((packed >> 36) & 0x1F) + 1
    total_samples = packed & ((1 << 36) - 1)
    if sample_rate_hz != 16_000 or channels != 1 or bits_per_sample != 16:
        raise VADError(
            "input must be normalized 16 kHz mono 16-bit FLAC; observed "
            f"{sample_rate_hz} Hz, {channels} channel(s), {bits_per_sample} bit"
        )
    if total_samples <= 0:
        raise VADError("input FLAC STREAMINFO must declare a positive sample count")
    duration_ms = (total_samples * 1_000 + sample_rate_hz // 2) // sample_rate_hz
    if duration_ms <= 0:
        raise VADError("input FLAC duration must round to at least one millisecond")
    if duration_ms > MAX_AUDIO_DURATION_MS:
        raise VADError(
            f"input duration exceeds the {MAX_AUDIO_DURATION_MS}-ms VAD work-unit limit"
        )
    return {
        "container": "flac",
        "sample_rate_hz": sample_rate_hz,
        "channels": channels,
        "bits_per_sample": bits_per_sample,
        "total_samples": total_samples,
        "duration_ms": duration_ms,
        "duration_coordinate_basis": "flac_streaminfo_total_samples_nearest_ms",
    }


def decimal_centiseconds(raw: str, label: str) -> int:
    try:
        value = Decimal(raw)
    except InvalidOperation as error:
        raise VADError(f"{label} is not a finite decimal") from error
    if not value.is_finite() or value < 0 or value != value.to_integral_value():
        raise VADError(f"{label} must be an integer centisecond rendered with .00")
    result = int(value)
    if result > MAX_AUDIO_DURATION_MS // 10 + 10:
        raise VADError(f"{label} exceeds the VAD work-unit bound")
    return result


def parse_engine_stdout(
    body: bytes,
    *,
    input_duration_ms: int,
    result_key: str,
    catalog_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if len(body) > MAX_STDOUT_BYTES:
        raise VADError("VAD stdout exceeds its bounded output limit")
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise VADError(
            f"VAD stdout is not strict UTF-8 at bytes [{error.start},{error.end})"
        ) from error
    lines = text.splitlines()
    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise VADError("VAD stdout is empty")
    header = HEADER_RE.fullmatch(lines[0])
    if header is None:
        raise VADError("VAD stdout has an unexpected header")
    declared = int(header.group(1))
    if declared > MAX_SEGMENTS:
        raise VADError("VAD stdout declares too many segments")
    if len(lines) != declared + 1:
        raise VADError("VAD stdout segment count does not match its header")
    segments: list[dict[str, Any]] = []
    previous_raw_end_ms = 0
    for ordinal, line in enumerate(lines[1:]):
        match = SEGMENT_RE.fullmatch(line)
        if match is None or int(match.group(1)) != ordinal:
            raise VADError(f"VAD stdout segment {ordinal} has unexpected syntax or ordinal")
        start_cs = decimal_centiseconds(match.group(2), f"segment {ordinal} start")
        end_cs = decimal_centiseconds(match.group(3), f"segment {ordinal} end")
        raw_start_ms = start_cs * 10
        raw_end_ms = end_cs * 10
        if raw_end_ms <= raw_start_ms:
            raise VADError(f"VAD segment {ordinal} is empty or inverted")
        if ordinal and raw_start_ms < previous_raw_end_ms:
            raise VADError(f"VAD segment {ordinal} overlaps or is out of order")
        if raw_start_ms >= input_duration_ms:
            raise VADError(f"VAD segment {ordinal} starts outside the input")
        overrun_ms = max(0, raw_end_ms - input_duration_ms)
        if overrun_ms > MAX_TAIL_OVERRUN_MS:
            raise VADError(
                f"VAD segment {ordinal} exceeds the input by {overrun_ms} ms"
            )
        end_ms = min(raw_end_ms, input_duration_ms)
        start_ms = raw_start_ms
        if end_ms <= start_ms:
            raise VADError(f"VAD segment {ordinal} vanishes after tail clipping")
        source_start_ms = None
        source_end_ms = None
        if catalog_context is not None:
            source_start_ms = catalog_context["source_start_ms"] + start_ms
            source_end_ms = catalog_context["source_start_ms"] + end_ms
        segment_identity = {
            "result_key": result_key,
            "ordinal": ordinal,
            "raw_start_centiseconds": start_cs,
            "raw_end_centiseconds": end_cs,
        }
        segments.append(
            {
                "segment_id": "vadseg_"
                + sha256_bytes(canonical_bytes(segment_identity))[:32],
                "ordinal": ordinal,
                "label": "speech_candidate",
                "artifact_start_ms": start_ms,
                "artifact_end_ms": end_ms,
                "raw_engine_start_centiseconds": start_cs,
                "raw_engine_end_centiseconds": end_cs,
                "raw_engine_start_text": match.group(2),
                "raw_engine_end_text": match.group(3),
                "source_start_ms": source_start_ms,
                "source_end_ms": source_end_ms,
                "timing_quality": (
                    "tail_clipped_to_flac_duration"
                    if overrun_ms
                    else "exact_10ms_engine_grid"
                ),
                "tail_overrun_ms": overrun_ms,
                "score": None,
                "calibrated_probability": None,
                "calibration_state": "not_calibrated",
            }
        )
        previous_raw_end_ms = raw_end_ms
    return segments


def terminate_group(process: subprocess.Popen[bytes]) -> None:
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


def run_child(
    command: list[str],
    *,
    pass_fds: tuple[int, ...],
    timeout_seconds: int,
) -> tuple[bytes, bytes]:
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=minimal_environment(Path("/nonexistent-home")),
        start_new_session=True,
        pass_fds=pass_fds,
    )
    streams = {
        "stdout": (process.stdout, MAX_STDOUT_BYTES),
        "stderr": (process.stderr, MAX_STDERR_BYTES),
    }
    buffers: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + timeout_seconds
    try:
        for name, (stream, _limit) in streams.items():
            if stream is None:  # pragma: no cover - Popen contract is fixed above.
                raise VADError(f"VAD {name} pipe was not created")
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise VADError(
                    f"VAD engine exceeded the {timeout_seconds}-second timeout"
                )
            events = selector.select(timeout=min(remaining, 0.25))
            for key, _mask in events:
                name = key.data
                try:
                    chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                buffers[name].extend(chunk)
                limit = streams[name][1]
                if len(buffers[name]) > limit:
                    raise VADError(f"VAD {name} exceeds its bounded output limit")
        remaining = max(0.001, deadline - time.monotonic())
        try:
            return_code = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired as error:
            raise VADError(
                f"VAD engine exceeded the {timeout_seconds}-second timeout"
            ) from error
        if return_code != 0:
            raise VADError(f"VAD engine exited with status {return_code}")
        return bytes(buffers["stdout"]), bytes(buffers["stderr"])
    except Exception:
        terminate_group(process)
        raise
    finally:
        selector.close()
        for stream, _limit in streams.values():
            if stream is not None:
                stream.close()


def verify_inputs(*pairs: tuple[RetainedFile, str]) -> None:
    for retained, label in pairs:
        verify_retained_file(retained, label)


def command_for(
    work_order: dict[str, Any],
    *,
    executable: str,
    model: str,
    input_path: str,
) -> list[str]:
    return [
        executable,
        "--threads",
        str(work_order["parameters"]["threads"]),
        "--vad-model",
        model,
        "--file",
        input_path,
        "--no-prints",
    ]


def output_directory_identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        stat.S_IFMT(observed.st_mode),
        stat.S_IMODE(observed.st_mode),
        observed.st_uid,
    )


def preflight_output_tree(
    output_root: Path, run_dir: Path
) -> list[tuple[Path, tuple[int, ...]]]:
    """Read-only validation of every currently existing output-path component."""

    retained_observations: list[tuple[Path, tuple[int, ...]]] = []
    current = Path("/")
    for part in run_dir.parts[1:]:
        candidate = current / part
        if not os.path.lexists(candidate):
            break
        try:
            observed = candidate.lstat()
        except OSError as error:
            raise VADError(f"VAD output path cannot be inspected: {error}") from error
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise VADError("VAD output path contains a non-directory or symlink")
        mode = stat.S_IMODE(observed.st_mode)
        if mode & 0o002 or (observed.st_uid != os.geteuid() and mode & 0o020):
            raise VADError(
                "VAD output parent chain must not be world-writable or foreign-owned "
                "and group-writable"
            )
        if candidate == output_root or output_root in candidate.parents:
            if observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) & 0o077:
                raise VADError("existing managed VAD output directories must be owner-private")
        retained_observations.append((candidate, output_directory_identity(observed)))
        current = candidate
    return retained_observations


@contextmanager
def retained_private_path(
    root: Path,
    relative_parts: tuple[str, ...],
    observations: list[tuple[Path, tuple[int, ...]]],
) -> Iterator[tuple[Path, int]]:
    """Create and retain a managed output path without pathname traversal races.

    Every component is opened relative to the already retained parent with
    ``O_NOFOLLOW``. Missing managed components are created with ``mkdirat`` semantics.
    Keeping the whole descriptor chain means replacement of a group-writable logical
    ancestor cannot redirect later creation or publication into another tree.
    """

    if not root.is_absolute() or any(
        not part or part in {".", ".."} or "/" in part for part in relative_parts
    ):
        raise VADError("VAD managed output path is not an exact absolute path")
    root_parts = root.parts[1:]
    if not root_parts:
        raise VADError("VAD output root cannot be the filesystem root")
    parts = (*root_parts, *relative_parts)
    managed_start = len(root_parts) - 1
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    descriptors: list[int] = []
    retained_components: list[tuple[Path, int]] = []
    current_path = Path("/")
    try:
        current_fd = os.open("/", flags)
        descriptors.append(current_fd)
        for index, part in enumerate(parts):
            try:
                next_fd = os.open(part, flags, dir_fd=current_fd)
            except FileNotFoundError:
                if index < managed_start:
                    raise VADError(
                        "VAD output ancestor disappeared during retained traversal"
                    ) from None
                try:
                    os.mkdir(part, mode=0o700, dir_fd=current_fd)
                    os.fsync(current_fd)
                except FileExistsError:
                    pass
                try:
                    next_fd = os.open(part, flags, dir_fd=current_fd)
                except OSError as error:
                    raise VADError(
                        f"VAD created output component cannot be retained: {error}"
                    ) from error
            except OSError as error:
                raise VADError(
                    f"VAD output component cannot be retained: {error}"
                ) from error
            descriptors.append(next_fd)
            current_fd = next_fd
            current_path /= part
            retained_components.append((current_path, current_fd))
            observed = os.fstat(current_fd)
            if index >= managed_start and (
                observed.st_uid != os.geteuid()
                or stat.S_IMODE(observed.st_mode) & 0o077
            ):
                raise VADError(
                    "retained managed VAD output directories must be owner-private"
                )
        verify_output_tree(observations)
        yield current_path, current_fd
        verify_output_tree(observations)
        for component_path, component_fd in retained_components:
            try:
                linked = component_path.lstat()
            except OSError as error:
                raise VADError(
                    f"retained VAD output path changed after creation: {error}"
                ) from error
            if output_directory_identity(
                os.fstat(component_fd)
            ) != output_directory_identity(linked):
                raise VADError("retained VAD output component was replaced")
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def verify_output_tree(observations: list[tuple[Path, tuple[int, ...]]]) -> None:
    for path, expected in observations:
        try:
            observed = path.lstat()
        except OSError as error:
            raise VADError(f"VAD output path changed after preflight: {error}") from error
        if output_directory_identity(observed) != expected:
            raise VADError("VAD output path identity changed after preflight")


def immutable_write_at(directory_fd: int, name: str, body: bytes) -> None:
    temporary = f".{name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, flags, 0o600, dir_fd=directory_fd)
        offset = 0
        while offset < len(body):
            count = os.write(descriptor, body[offset:])
            if count <= 0:
                raise VADError(f"could not write immutable VAD artifact {name}")
            offset += count
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.rename(
            temporary,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass


def cleanup_staging_directory(parent_fd: int, stage_name: str, stage_fd: int) -> None:
    os.fchmod(stage_fd, 0o700)
    for name in os.listdir(stage_fd):
        observed = os.stat(name, dir_fd=stage_fd, follow_symlinks=False)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
            raise VADError("unexpected non-file in VAD staging directory")
        file_fd = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=stage_fd,
        )
        try:
            os.fchmod(file_fd, 0o600)
        finally:
            os.close(file_fd)
        os.unlink(name, dir_fd=stage_fd)
    os.fsync(stage_fd)
    os.rmdir(stage_name, dir_fd=parent_fd)
    os.fsync(parent_fd)


def atomic_publish_directory_at(
    parent_fd: int, staging_name: str, final_name: str
) -> None:
    """Linux no-replace directory publication; never replace an appeared result."""

    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise VADError("host libc lacks renameat2; refusing unsafe VAD publication")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if renameat2(
        parent_fd,
        os.fsencode(staging_name),
        parent_fd,
        os.fsencode(final_name),
        1,
    ) != 0:
        error_number = ctypes.get_errno()
        if error_number in (errno.EEXIST, errno.ENOTEMPTY):
            raise FileExistsError(error_number, os.strerror(error_number), final_name)
        raise VADError(
            f"atomic VAD result publication failed: {os.strerror(error_number)}"
        )


def immutable_file_bytes(path: Path, label: str, maximum: int) -> bytes:
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o400
        or observed.st_nlink != 1
    ):
        raise VADError(f"{label} must be an immutable owner-private single-link file")
    try:
        return stable_file_bytes(
            path,
            maximum_bytes=maximum,
            label=label,
            exact_mode=0o400,
            require_single_link=True,
        )
    except ASRError as error:
        raise VADError(str(error)) from error


def validate_reuse(
    run_dir: Path,
    *,
    work_order: dict[str, Any],
    work_order_sha256: str,
    result_key: str,
    recipe_id: str,
    recipe_sha256: str,
    recipe: dict[str, Any],
    input_duration_ms: int,
    catalog_context: dict[str, Any] | None,
    input_file: RetainedFile,
    engine_file: RetainedFile,
    model_file: RetainedFile,
    flac: dict[str, Any],
    engine_profile: dict[str, Any],
    model_profile: dict[str, Any],
    logical_command: list[str],
) -> dict[str, Any]:
    observed = run_dir.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o500
    ):
        raise VADError("existing VAD result directory is not immutable and private")
    expected_names = {"engine.stdout.txt", "engine.stderr.txt", "result.json"}
    if {item.name for item in run_dir.iterdir()} != expected_names:
        raise VADError("existing VAD result directory has missing or extra entries")
    stdout = immutable_file_bytes(
        run_dir / "engine.stdout.txt", "existing VAD stdout", MAX_STDOUT_BYTES
    )
    stderr = immutable_file_bytes(
        run_dir / "engine.stderr.txt", "existing VAD stderr", MAX_STDERR_BYTES
    )
    result_body = immutable_file_bytes(
        run_dir / "result.json", "existing VAD result", MAX_JSON_BYTES
    )
    result = strict_json_bytes(result_body, "existing VAD result")
    if not isinstance(result, dict):
        raise VADError("existing VAD result must be an object")
    expected_top_keys = {
        "schema_version",
        "job_id",
        "status",
        "dry_run",
        "work_order_sha256",
        "recipe_id",
        "recipe_sha256",
        "result_key",
        "processing_run",
        "input",
        "engine",
        "model",
        "parameters",
        "catalog_context",
        "timing_contract",
        "commands",
        "calibration",
        "safety",
        "recipe",
        "result_path",
        "artifacts",
        "segments",
        "counts",
    }
    if set(result) != expected_top_keys:
        raise VADError("existing VAD result has missing or extra top-level fields")
    for key, expected in (
        ("schema_version", CONTRACT_VERSION),
        ("job_id", work_order["job_id"]),
        ("status", "completed"),
        ("dry_run", False),
        ("work_order_sha256", work_order_sha256),
        ("result_key", result_key),
        ("recipe_id", recipe_id),
        ("recipe_sha256", recipe_sha256),
        ("parameters", work_order["parameters"]),
        ("catalog_context", catalog_context),
        ("recipe", recipe),
        ("result_path", str(run_dir / "result.json")),
    ):
        if result.get(key) != expected:
            raise VADError(f"existing VAD result has inconsistent {key}")
    expected_input = input_file.observation | {
        key: work_order["input"][key]
        for key in ("media_id", "artifact_id", "parent_processing_run_id")
    } | {"flac_streaminfo": flac}
    if result.get("input") != expected_input:
        raise VADError("existing VAD result input observation no longer matches")
    expected_engine = engine_file.observation | {
        "profile_id": engine_profile["profile_id"],
        "version_label": engine_profile["version_label"],
        "version_evidence": engine_profile["version_evidence"],
        "build": engine_profile["build"],
        "stdout_coordinate_unit": engine_profile["stdout_coordinate_unit"],
        "known_cli_defects": engine_profile["known_cli_defects"],
    }
    if result.get("engine") != expected_engine:
        raise VADError("existing VAD result engine observation no longer matches")
    expected_model = model_file.observation | {
        key: model_profile[key]
        for key in (
            "profile_id", "model_id", "name", "revision", "source", "license_label"
        )
    }
    if result.get("model") != expected_model:
        raise VADError("existing VAD result model observation no longer matches")
    expected_timing = {
        "engine_coordinate_unit": "centiseconds",
        "engine_coordinate_to_ms_multiplier": 10,
        "artifact_coordinate_system": "artifact_media_ms",
        "source_coordinate_system": (
            None if catalog_context is None else "rendition_media_ms"
        ),
        "maximum_explicit_tail_overrun_ms": MAX_TAIL_OVERRUN_MS,
        "tail_policy": "retain_raw_engine_coordinate_and_clip_normalized_end",
    }
    if result.get("timing_contract") != expected_timing:
        raise VADError("existing VAD timing contract changed")
    expected_calibration = {
        "state": "not_calibrated",
        "calibrated_probability": None,
        "score_available": False,
        "threshold_is_not_confidence": True,
    }
    if result.get("calibration") != expected_calibration:
        raise VADError("existing VAD calibration boundary changed")
    expected_safety = {
        "visibility": "private",
        "speech_presence_candidates_only": True,
        "speaker_count_authority": "none",
        "speaker_identity_authority": "none",
        "face_identity_authority": "none",
        "active_speaker_authority": "none",
        "action_detection_authority": "none",
        "publication_authority": "none",
        "catalog_write_authority": "none",
        "human_review_required": True,
        "network": "not_used_by_adapter; worker isolation still required",
    }
    if result.get("safety") != expected_safety:
        raise VADError("existing VAD safety boundary changed")
    processing_run = exact_keys(
        result.get("processing_run"),
        "existing VAD processing_run",
        {
            "processing_run_id", "stage", "implementation_version", "started_at",
            "completed_at", "status", "duration_ms",
        },
    )
    if (
        not re.fullmatch(r"run_vad_whispercpp_[0-9a-f]{32}", str(processing_run["processing_run_id"]))
        or processing_run["processing_run_id"]
        != f"run_vad_whispercpp_{result_key[:32]}"
        or processing_run["stage"] != STAGE
        or processing_run["implementation_version"] != IMPLEMENTATION_VERSION
        or processing_run["status"] != "completed"
        or isinstance(processing_run["duration_ms"], bool)
        or not isinstance(processing_run["duration_ms"], int)
        or processing_run["duration_ms"] < 0
    ):
        raise VADError("existing VAD processing run is inconsistent")
    started = canonical_utc_second(
        processing_run["started_at"], "existing VAD processing_run.started_at"
    )
    completed = canonical_utc_second(
        processing_run["completed_at"], "existing VAD processing_run.completed_at"
    )
    elapsed_wall_ms = round((completed - started).total_seconds() * 1_000)
    if (
        completed < started
        or abs(processing_run["duration_ms"] - elapsed_wall_ms) > 2_000
        or processing_run["duration_ms"]
        > work_order["parameters"]["timeout_seconds"] * 1_000 + 60_000
    ):
        raise VADError("existing VAD processing timing is inconsistent")
    commands = exact_keys(
        result.get("commands"),
        "existing VAD commands",
        {"logical", "child_facing", "descriptor_execution_policy", "state"},
    )
    if (
        commands["logical"] != logical_command
        or commands["descriptor_execution_policy"] != DESCRIPTOR_POLICY
        or commands["state"] != "executed"
    ):
        raise VADError("existing VAD command provenance changed")
    child = commands["child_facing"]
    if (
        not isinstance(child, list)
        or len(child) != 8
        or child[1:3] != ["--threads", str(work_order["parameters"]["threads"])]
        or child[3] != "--vad-model"
        or child[5] != "--file"
        or child[7] != "--no-prints"
        or any(
            not isinstance(child[index], str)
            or re.fullmatch(r"/proc/self/fd/[0-9]+", child[index]) is None
            for index in (0, 4, 6)
        )
    ):
        raise VADError("existing VAD child-facing descriptor command changed")
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise VADError("existing VAD result has invalid artifact descriptors")
    expected_artifacts = [
        ("engine_stdout", run_dir / "engine.stdout.txt", stdout),
        ("engine_stderr", run_dir / "engine.stderr.txt", stderr),
    ]
    if [
        artifact.get("artifact_kind") if isinstance(artifact, dict) else None
        for artifact in artifacts
    ] != [artifact_kind for artifact_kind, _path, _body in expected_artifacts]:
        raise VADError("existing VAD result has invalid artifact order")
    observed_artifact_kinds: set[str] = set()
    for artifact, (artifact_kind, expected_path, body) in zip(
        artifacts, expected_artifacts, strict=True
    ):
        if not isinstance(artifact, dict):
            raise VADError("existing VAD result has an unknown artifact")
        if artifact_kind in observed_artifact_kinds:
            raise VADError("existing VAD result duplicates an artifact kind")
        observed_artifact_kinds.add(artifact_kind)
        if artifact != {
            "artifact_kind": artifact_kind,
            "path": str(expected_path),
            "sha256": hashlib.sha256(body).hexdigest(),
            "byte_count": len(body),
            "visibility": "private",
        }:
            raise VADError("existing VAD artifact descriptor does not match exact bytes")
    if observed_artifact_kinds != {
        artifact_kind for artifact_kind, _path, _body in expected_artifacts
    }:
        raise VADError("existing VAD result omits a required artifact kind")
    parsed = parse_engine_stdout(
        stdout,
        input_duration_ms=input_duration_ms,
        result_key=result_key,
        catalog_context=catalog_context,
    )
    if result.get("segments") != parsed:
        raise VADError("existing VAD parsed segments do not match retained stdout")
    speech_duration_ms = sum(
        segment["artifact_end_ms"] - segment["artifact_start_ms"]
        for segment in parsed
    )
    expected_counts = {
        "speech_segment_count": len(parsed),
        "speech_duration_ms": speech_duration_ms,
        "input_duration_ms": input_duration_ms,
        "speech_coverage_ratio": round(speech_duration_ms / input_duration_ms, 9),
        "tail_clipped_segment_count": sum(
            segment["tail_overrun_ms"] > 0 for segment in parsed
        ),
    }
    if result.get("counts") != expected_counts:
        raise VADError("existing VAD aggregate counts do not match retained segments")
    return result


def build_result_common(
    *,
    work_order: dict[str, Any],
    work_order_sha256: str,
    recipe: dict[str, Any],
    recipe_id: str,
    recipe_sha256: str,
    result_key: str,
    processing_run_id: str,
    started_at: str,
    completed_at: str,
    duration_ms: int,
    status: str,
    dry_run: bool,
    input_file: RetainedFile,
    engine_file: RetainedFile,
    model_file: RetainedFile,
    flac: dict[str, Any],
    engine_profile: dict[str, Any],
    model_profile: dict[str, Any],
    logical_command: list[str],
    child_command: list[str],
    result_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_VERSION,
        "job_id": work_order["job_id"],
        "status": status,
        "dry_run": dry_run,
        "work_order_sha256": work_order_sha256,
        "recipe_id": recipe_id,
        "recipe_sha256": recipe_sha256,
        "result_key": result_key,
        "processing_run": {
            "processing_run_id": processing_run_id,
            "stage": STAGE,
            "implementation_version": IMPLEMENTATION_VERSION,
            "started_at": started_at,
            "completed_at": completed_at,
            "status": "queued" if dry_run else "completed",
            "duration_ms": duration_ms,
        },
        "input": input_file.observation
        | {
            key: work_order["input"][key]
            for key in ("media_id", "artifact_id", "parent_processing_run_id")
        }
        | {"flac_streaminfo": flac},
        "engine": engine_file.observation
        | {
            "profile_id": engine_profile["profile_id"],
            "version_label": engine_profile["version_label"],
            "version_evidence": engine_profile["version_evidence"],
            "build": engine_profile["build"],
            "stdout_coordinate_unit": engine_profile["stdout_coordinate_unit"],
            "known_cli_defects": engine_profile["known_cli_defects"],
        },
        "model": model_file.observation
        | {
            key: model_profile[key]
            for key in (
                "profile_id",
                "model_id",
                "name",
                "revision",
                "source",
                "license_label",
            )
        },
        "parameters": work_order["parameters"],
        "catalog_context": work_order["catalog_context"],
        "timing_contract": {
            "engine_coordinate_unit": "centiseconds",
            "engine_coordinate_to_ms_multiplier": 10,
            "artifact_coordinate_system": "artifact_media_ms",
            "source_coordinate_system": (
                None
                if work_order["catalog_context"] is None
                else "rendition_media_ms"
            ),
            "maximum_explicit_tail_overrun_ms": MAX_TAIL_OVERRUN_MS,
            "tail_policy": "retain_raw_engine_coordinate_and_clip_normalized_end",
        },
        "commands": {
            "logical": logical_command,
            "child_facing": child_command,
            "descriptor_execution_policy": DESCRIPTOR_POLICY,
            "state": "planned" if dry_run else "executed",
        },
        "calibration": {
            "state": "not_calibrated",
            "calibrated_probability": None,
            "score_available": False,
            "threshold_is_not_confidence": True,
        },
        "safety": {
            "visibility": "private",
            "speech_presence_candidates_only": True,
            "speaker_count_authority": "none",
            "speaker_identity_authority": "none",
            "face_identity_authority": "none",
            "active_speaker_authority": "none",
            "action_detection_authority": "none",
            "publication_authority": "none",
            "catalog_write_authority": "none",
            "human_review_required": True,
            "network": "not_used_by_adapter; worker isolation still required",
        },
        "recipe": recipe,
        "result_path": str(result_path),
    }


def run_vad(work_order: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    started_at = utc_now()
    started_clock = time.monotonic()
    with ExitStack() as stack:
        input_file = stack.enter_context(
            retained_verified_file(
                Path(work_order["input"]["path"]),
                work_order["input"]["expected_sha256"],
                "VAD input FLAC",
            )
        )
        engine_file = stack.enter_context(
            retained_verified_file(
                Path(work_order["engine"]["executable"]),
                work_order["engine"]["expected_sha256"],
                "VAD executable",
                executable=True,
            )
        )
        model_file = stack.enter_context(
            retained_verified_file(
                Path(work_order["model"]["path"]),
                work_order["model"]["expected_sha256"],
                "VAD model",
            )
        )
        pairs = (
            (input_file, "VAD input FLAC"),
            (engine_file, "VAD executable"),
            (model_file, "VAD model"),
        )
        try:
            for retained, requested, label in (
                (input_file, work_order["input"], "input"),
                (engine_file, work_order["engine"], "engine"),
                (model_file, work_order["model"], "model"),
            ):
                require_expected_size(retained, requested["expected_byte_count"], label)
            require_sealed_input(input_file)
            require_sealed_asset(engine_file, "VAD executable", executable=True)
            require_sealed_asset(model_file, "VAD model", executable=False)
            try:
                engine_profile = profiles.match_engine_profile(
                    engine_file.observation["sha256"],
                    engine_file.observation["byte_count"],
                )
                model_profile = profiles.match_model_profile(
                    model_file.observation["sha256"],
                    model_file.observation["byte_count"],
                )
            except profiles.VADProfileError as error:
                raise VADError(str(error)) from error
            require_profile_metadata(work_order["engine"], engine_profile, "engine")
            require_profile_metadata(work_order["model"], model_profile, "model")
            flac = parse_flac_streaminfo(input_file)
            recipe = {
                "contract_version": CONTRACT_VERSION,
                "implementation_version": IMPLEMENTATION_VERSION,
                "stage": STAGE,
                "descriptor_execution_policy": DESCRIPTOR_POLICY,
                "engine_profile": engine_profile,
                "model_profile": model_profile,
                "parameters": work_order["parameters"],
                "input_audio_contract": {
                    key: flac[key]
                    for key in (
                        "container",
                        "sample_rate_hz",
                        "channels",
                        "bits_per_sample",
                    )
                },
                "output_contract": "speech-candidate-intervals-private-v1",
            }
            recipe_sha256 = sha256_bytes(canonical_bytes(recipe))
            recipe_id = f"recipe_vad_whispercpp_{recipe_sha256[:32]}"
            work_order_sha256 = sha256_bytes(canonical_bytes(work_order))
            result_identity = {
                "work_order_sha256": work_order_sha256,
                "input_sha256": input_file.observation["sha256"],
                "input_media_id": work_order["input"]["media_id"],
                "input_artifact_id": work_order["input"]["artifact_id"],
                "parent_processing_run_id": work_order["input"]["parent_processing_run_id"],
                "recipe_id": recipe_id,
                "catalog_context": work_order["catalog_context"],
            }
            result_key = sha256_bytes(canonical_bytes(result_identity))
            output_root = Path(work_order["output"]["root"])
            run_dir = (
                output_root
                / "vad"
                / "whispercpp"
                / "sha256"
                / input_file.observation["sha256"][:2]
                / input_file.observation["sha256"]
                / "results"
                / result_key
            )
            result_path = run_dir / "result.json"
            processing_run_id = f"run_vad_whispercpp_{result_key[:32]}"
            logical_command = command_for(
                work_order,
                executable=work_order["engine"]["executable"],
                model=work_order["model"]["path"],
                input_path=work_order["input"]["path"],
            )
            engine_execution_fd, engine_execution_path = stack.enter_context(
                sealed_execution_copy(engine_file, "engine", executable=True)
            )
            model_execution_fd, model_execution_path = stack.enter_context(
                sealed_execution_copy(model_file, "model", executable=False)
            )
            child_command = command_for(
                work_order,
                executable=engine_execution_path,
                model=model_execution_path,
                input_path=input_file.proc_path,
            )
            output_observations = preflight_output_tree(output_root, run_dir)
            if dry_run:
                verify_inputs(*pairs)
                if os.path.lexists(run_dir):
                    if not result_path.exists():
                        raise VADError(
                            "existing VAD result directory is partial or has no result"
                        )
                    validate_reuse(
                        run_dir,
                        work_order=work_order,
                        work_order_sha256=work_order_sha256,
                        result_key=result_key,
                        recipe_id=recipe_id,
                        recipe_sha256=recipe_sha256,
                        recipe=recipe,
                        input_duration_ms=flac["duration_ms"],
                        catalog_context=work_order["catalog_context"],
                        input_file=input_file,
                        engine_file=engine_file,
                        model_file=model_file,
                        flac=flac,
                        engine_profile=engine_profile,
                        model_profile=model_profile,
                        logical_command=logical_command,
                    )
                common = build_result_common(
                    work_order=work_order,
                    work_order_sha256=work_order_sha256,
                    recipe=recipe,
                    recipe_id=recipe_id,
                    recipe_sha256=recipe_sha256,
                    result_key=result_key,
                    processing_run_id=processing_run_id,
                    started_at=started_at,
                    completed_at=utc_now(),
                    duration_ms=round((time.monotonic() - started_clock) * 1_000),
                    status="planned",
                    dry_run=True,
                    input_file=input_file,
                    engine_file=engine_file,
                    model_file=model_file,
                    flac=flac,
                    engine_profile=engine_profile,
                    model_profile=model_profile,
                    logical_command=logical_command,
                    child_command=child_command,
                    result_path=result_path,
                )
                verify_output_tree(output_observations)
                return common | {
                    "artifacts": [],
                    "segments": [],
                    "counts": None,
                }
            if os.path.lexists(run_dir):
                if not result_path.exists():
                    raise VADError(
                        "existing VAD result directory is partial or has no result"
                    )
                verify_inputs(*pairs)
                reused = validate_reuse(
                    run_dir,
                    work_order=work_order,
                    work_order_sha256=work_order_sha256,
                    result_key=result_key,
                    recipe_id=recipe_id,
                    recipe_sha256=recipe_sha256,
                    recipe=recipe,
                    input_duration_ms=flac["duration_ms"],
                    catalog_context=work_order["catalog_context"],
                    input_file=input_file,
                    engine_file=engine_file,
                    model_file=model_file,
                    flac=flac,
                    engine_profile=engine_profile,
                    model_profile=model_profile,
                    logical_command=logical_command,
                )
                verify_output_tree(output_observations)
                return reused
            with retained_private_path(
                output_root,
                (
                    "vad",
                    "whispercpp",
                    "sha256",
                    input_file.observation["sha256"][:2],
                    input_file.observation["sha256"],
                    "results",
                ),
                output_observations,
            ) as (parent, parent_fd):
                try:
                    os.stat(result_key, dir_fd=parent_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise VADError("VAD result directory appeared before execution")
                stage_name = (
                    f".{result_key}.tmp-{processing_run_id}-{uuid.uuid4().hex}"
                )
                os.mkdir(stage_name, mode=0o700, dir_fd=parent_fd)
                stage_flags = (
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_DIRECTORY", 0)
                )
                stage_fd = os.open(stage_name, stage_flags, dir_fd=parent_fd)
                published = False
                try:
                    stage_stat = os.fstat(stage_fd)
                    if (
                        stage_stat.st_uid != os.geteuid()
                        or stat.S_IMODE(stage_stat.st_mode) != 0o700
                    ):
                        raise VADError("VAD staging directory is not owner-private")
                    try:
                        stdout, stderr = run_child(
                            child_command,
                            pass_fds=(
                                engine_execution_fd,
                                model_execution_fd,
                                input_file.descriptor,
                            ),
                            timeout_seconds=work_order["parameters"]["timeout_seconds"],
                        )
                    finally:
                        verify_inputs(*pairs)
                    segments = parse_engine_stdout(
                        stdout,
                        input_duration_ms=flac["duration_ms"],
                        result_key=result_key,
                        catalog_context=work_order["catalog_context"],
                    )
                    immutable_write_at(stage_fd, "engine.stdout.txt", stdout)
                    immutable_write_at(stage_fd, "engine.stderr.txt", stderr)
                    artifacts = [
                        {
                            "artifact_kind": kind,
                            "path": str(run_dir / name),
                            "sha256": hashlib.sha256(body).hexdigest(),
                            "byte_count": len(body),
                            "visibility": "private",
                        }
                        for kind, name, body in (
                            ("engine_stdout", "engine.stdout.txt", stdout),
                            ("engine_stderr", "engine.stderr.txt", stderr),
                        )
                    ]
                    speech_duration_ms = sum(
                        segment["artifact_end_ms"] - segment["artifact_start_ms"]
                        for segment in segments
                    )
                    completed_at = utc_now()
                    common = build_result_common(
                        work_order=work_order,
                        work_order_sha256=work_order_sha256,
                        recipe=recipe,
                        recipe_id=recipe_id,
                        recipe_sha256=recipe_sha256,
                        result_key=result_key,
                        processing_run_id=processing_run_id,
                        started_at=started_at,
                        completed_at=completed_at,
                        duration_ms=round((time.monotonic() - started_clock) * 1_000),
                        status="completed",
                        dry_run=False,
                        input_file=input_file,
                        engine_file=engine_file,
                        model_file=model_file,
                        flac=flac,
                        engine_profile=engine_profile,
                        model_profile=model_profile,
                        logical_command=logical_command,
                        child_command=child_command,
                        result_path=result_path,
                    )
                    result = common | {
                        "artifacts": artifacts,
                        "segments": segments,
                        "counts": {
                            "speech_segment_count": len(segments),
                            "speech_duration_ms": speech_duration_ms,
                            "input_duration_ms": flac["duration_ms"],
                            "speech_coverage_ratio": round(
                                speech_duration_ms / flac["duration_ms"], 9
                            ),
                            "tail_clipped_segment_count": sum(
                                segment["tail_overrun_ms"] > 0
                                for segment in segments
                            ),
                        },
                    }
                    result_body = pretty_json(result).encode("utf-8")
                    if len(result_body) > MAX_JSON_BYTES:
                        raise VADError("normalized VAD result exceeds its JSON size limit")
                    immutable_write_at(stage_fd, "result.json", result_body)
                    os.fchmod(stage_fd, 0o500)
                    os.fsync(stage_fd)
                    try:
                        atomic_publish_directory_at(
                            parent_fd, stage_name, result_key
                        )
                    except FileExistsError as error:
                        raise VADError(
                            "VAD result appeared during execution; rerun for exact reuse"
                        ) from error
                    published = True
                    os.fsync(parent_fd)
                    linked = os.stat(
                        result_key, dir_fd=parent_fd, follow_symlinks=False
                    )
                    logical = run_dir.lstat()
                    opened = os.fstat(stage_fd)
                    if (
                        output_directory_identity(linked)
                        != output_directory_identity(opened)
                        or output_directory_identity(logical)
                        != output_directory_identity(opened)
                    ):
                        raise VADError("published VAD result path changed identity")
                    verify_output_tree(output_observations)
                    return result
                finally:
                    if not published:
                        try:
                            cleanup_staging_directory(
                                parent_fd, stage_name, stage_fd
                            )
                        except FileNotFoundError:
                            pass
                    os.close(stage_fd)
        finally:
            verify_inputs(*pairs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline hash-pinned whisper.cpp/Silero VAD adapter"
    )
    commands = parser.add_subparsers(dest="command", required=True)
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
        work_order_path = absolute_file(args.work_order, "--work-order")
        raw = strict_json_file(work_order_path, "work order")
        if (
            isinstance(raw, dict)
            and isinstance(raw.get("job_id"), str)
            and JOB_ID_RE.fullmatch(raw["job_id"])
        ):
            job_id = raw["job_id"]
        work_order = validate_work_order(raw)
        result: Any = work_order
        if args.command == "run":
            result = run_vad(work_order, dry_run=args.dry_run)
        sys.stdout.write(pretty_json(result))
        return 0
    except (VADError, ASRError, OSError, subprocess.SubprocessError) as error:
        failure = {
            "schema_version": CONTRACT_VERSION,
            "status": "failed",
            "job_id": job_id,
            "error": {"type": type(error).__name__, "message": str(error)},
            "errors": [{"type": type(error).__name__, "message": str(error)}],
        }
        sys.stderr.write(pretty_json(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
