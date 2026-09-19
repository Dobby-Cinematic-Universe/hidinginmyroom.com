#!/usr/bin/env python3
"""Fail-closed, offline faster-whisper CUDA adapter for private HIMR ASR.

This module is deliberately independent from the CPU whisper.cpp lane.  It accepts
only canonical, hash-pinned work orders, runs against a sealed local model snapshot
inside an already isolated network namespace, serializes raw and normalized
timestamped machine transcripts, and atomically publishes an owner-private result
directory without replacement.

The adapter has no catalogue, identity, wiki, biometric, or publication authority.
All probabilities are retained as uncalibrated raw model scores and every output
requires human review before any release decision.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import gc
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import secrets
import shutil
import stat
import subprocess
import sys
import sysconfig
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Any, Iterator, Sequence


CONTRACT_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
WORK_ORDER_KIND = "himr_faster_whisper_gpu_work_order"
RESULT_KIND = "himr_faster_whisper_gpu_result"
RAW_TRANSCRIPT_KIND = "himr_faster_whisper_raw_transcript"
NORMALIZED_TRANSCRIPT_KIND = "himr_machine_transcript"
STAGE = "asr_faster_whisper_gpu"

MAX_JSON_INPUT_BYTES = 4 * 1024 * 1024
MAX_AUDIO_BYTES = 16 * 1024 * 1024 * 1024
MAX_AUDIO_SECONDS = 24 * 60 * 60
MAX_WALL_SECONDS = 24 * 60 * 60
MAX_RESULT_BYTES = 512 * 1024 * 1024
MAX_SEGMENTS = 2_000_000
MAX_WORDS = 20_000_000
MAX_VRAM_BYTES = 128 * 1024 * 1024 * 1024
MAX_TIMESTAMP_OVERRUN_MS = 2_000
MAX_TEXT_CHARACTERS = 16 * 1024 * 1024
GPU_LOCK_POLICY = "linux_flock_exclusive_nonblocking_gpu_uuid_v1"
OFFLINE_POLICY = "external_network_namespace_loopback_only_local_files_v1"
OUTPUT_CONTRACT = "private-faster-whisper-raw-normalized-envelope-v1"
AT_FDCWD = -100
RENAME_NOREPLACE = 1

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
GPU_UUID_RE = re.compile(r"^GPU-[0-9a-fA-F-]{16,64}$")
LANGUAGE_RE = re.compile(r"^(?:auto|[a-z]{2,3}(?:-[a-z0-9]{2,8})*)$")

EXPECTED_PACKAGE_KEYS = {
    "av",
    "ctranslate2",
    "faster-whisper",
    "nvidia-cublas-cu12",
    "nvidia-cudnn-cu12",
    "nvidia-ml-py",
}
ALLOWED_COMPUTE_TYPES = {"float16", "int8", "int8_float16"}

POLICY = {
    "visibility": "private",
    "machine_generated": True,
    "human_review_required": True,
    "scores_calibrated": False,
    "identity_authority": "none",
    "biometric_authority": "none",
    "catalogue_mutation_authority": "none",
    "publication_authority": "none",
    "wiki_authority": "none",
    "existing_asr_rerun_authority": "none",
    "network_access": False,
}

WORK_ORDER_CONTRACT_DESCRIPTOR = {
    "kind": WORK_ORDER_KIND,
    "schema_version": CONTRACT_VERSION,
    "implementation_version": IMPLEMENTATION_VERSION,
    "required_sections": [
        "catalog_context",
        "gpu",
        "inference",
        "input",
        "model",
        "output",
        "policy",
        "runtime",
    ],
    "input": {
        "format": "16_khz_mono_s16_flac",
        "sealed_modes": ["0400", "0444"],
        "single_link_required": True,
    },
    "model": {
        "admission_kind": "himr_hf_model_admission_receipt",
        "exact_commit_required": True,
        "offline_snapshot_required": True,
    },
    "runtime": {
        "admission_kind": "himr_gpu_runtime_admission_receipt",
        "isolated_python_required": True,
        "network_namespace": "distinct_loopback_only",
    },
    "inference_invariants": {
        "condition_on_previous_text": False,
        "vad_filter": False,
        "word_timestamps": True,
    },
    "policy": POLICY,
}

RESULT_CONTRACT_DESCRIPTOR = {
    "kind": RESULT_KIND,
    "schema_version": CONTRACT_VERSION,
    "output_contract": OUTPUT_CONTRACT,
    "artifacts": [RAW_TRANSCRIPT_KIND, NORMALIZED_TRANSCRIPT_KIND],
    "visibility": "private",
    "atomic_no_replace": True,
    "sealed_file_mode": "0400",
    "sealed_directory_mode": "0500",
    "scores_calibrated": False,
    "human_review_required": True,
    "identity_authority": "none",
    "publication_authority": "none",
}


class ProductionASRError(RuntimeError):
    """A production GPU ASR contract, integrity, or execution failure."""


class GPUResourceBusy(ProductionASRError):
    """The single admitted GPU is already locked by another process."""


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def contract_document() -> dict[str, Any]:
    return {
        "kind": "himr_faster_whisper_gpu_contracts",
        "schema_version": CONTRACT_VERSION,
        "work_order": {
            "descriptor": WORK_ORDER_CONTRACT_DESCRIPTOR,
            "identity_sha256": sha256_bytes(
                canonical_bytes(WORK_ORDER_CONTRACT_DESCRIPTOR)
            ),
        },
        "result": {
            "descriptor": RESULT_CONTRACT_DESCRIPTOR,
            "identity_sha256": sha256_bytes(canonical_bytes(RESULT_CONTRACT_DESCRIPTOR)),
        },
    }


def stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(value))[:32]}"


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProductionASRError(f"JSON object contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise ProductionASRError(f"JSON contains non-finite numeric constant {value}")


def parse_json_bytes(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ProductionASRError(
            f"{label} is not strict UTF-8 at [{error.start},{error.end})"
        ) from error
    try:
        return json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as error:
        raise ProductionASRError(f"{label} is invalid JSON: {error}") from error


def load_local_gpu_module(module_name: str, filename: str) -> Any:
    path = Path(__file__).resolve().with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ProductionASRError(f"cannot load pinned local GPU module {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def stable_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def normalized_absolute_path(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ProductionASRError(f"{label} must be a non-empty path string")
    path = Path(value)
    if not path.is_absolute() or Path(os.path.normpath(value)) != path:
        raise ProductionASRError(f"{label} must be absolute and normalized")
    return path


def existing_regular_file(
    value: Any,
    label: str,
    *,
    executable: bool = False,
    exact_mode: int | None = None,
    single_link: bool = False,
) -> Path:
    path = normalized_absolute_path(value, label)
    try:
        observed = path.lstat()
    except OSError as error:
        raise ProductionASRError(f"{label} cannot be inspected: {error}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or path.resolve(strict=True) != path
    ):
        raise ProductionASRError(
            f"{label} must be an existing non-symlinked regular file"
        )
    if exact_mode is not None and stat.S_IMODE(observed.st_mode) != exact_mode:
        raise ProductionASRError(f"{label} must have mode {exact_mode:04o}")
    if single_link and observed.st_nlink != 1:
        raise ProductionASRError(f"{label} must have exactly one hard link")
    if executable and not os.access(path, os.X_OK):
        raise ProductionASRError(f"{label} is not executable")
    return path


def existing_private_directory(value: Any, label: str, *, mode: int = 0o700) -> Path:
    path = normalized_absolute_path(value, label)
    try:
        observed = path.lstat()
    except OSError as error:
        raise ProductionASRError(f"{label} cannot be inspected: {error}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or path.resolve(strict=True) != path
    ):
        raise ProductionASRError(
            f"{label} must be an existing non-symlinked directory"
        )
    if observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != mode:
        raise ProductionASRError(
            f"{label} must be owned by the current user with mode {mode:04o}"
        )
    return path


def require_exact_keys(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        missing = sorted(keys - set(value)) if isinstance(value, dict) else sorted(keys)
        extra = sorted(set(value) - keys) if isinstance(value, dict) else []
        raise ProductionASRError(
            f"{label} fields are not exact (missing={missing}, extra={extra})"
        )
    return value


def bounded_text(value: Any, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ProductionASRError(
            f"{label} must be a non-empty string of at most {maximum} characters"
        )
    if any(ord(character) < 32 for character in value):
        raise ProductionASRError(f"{label} must not contain control characters")
    return value


def identifier(value: Any, label: str) -> str:
    text = bounded_text(value, label, 256)
    if not ID_RE.fullmatch(text):
        raise ProductionASRError(f"{label} contains unsupported characters")
    return text


def sha256_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ProductionASRError(f"{label} must be a lowercase SHA-256 digest")
    return value


def integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProductionASRError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise ProductionASRError(
            f"{label} must be within [{minimum},{maximum}]"
        )
    return value


def finite_number(
    value: Any, label: str, minimum: float, maximum: float
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProductionASRError(f"{label} must be numeric")
    number = float(value)
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise ProductionASRError(
            f"{label} must be finite and within [{minimum},{maximum}]"
        )
    return number


def validate_file_reference(value: Any, label: str) -> dict[str, Any]:
    reference = require_exact_keys(value, label, {"path", "expected_sha256"})
    return {
        "path": str(existing_regular_file(reference["path"], f"{label}.path")),
        "expected_sha256": sha256_value(
            reference["expected_sha256"], f"{label}.expected_sha256"
        ),
    }


def validate_input(value: Any) -> dict[str, Any]:
    keys = {
        "path",
        "expected_sha256",
        "expected_byte_count",
        "expected_duration_ms",
        "sealed_mode",
        "media_id",
        "artifact_id",
        "parent_processing_run_id",
        "timeline_offset_ms",
        "media_format",
    }
    item = require_exact_keys(value, "input", keys)
    path = existing_regular_file(item["path"], "input.path", single_link=True)
    sealed_mode = bounded_text(item["sealed_mode"], "input.sealed_mode", 4)
    if sealed_mode not in {"0400", "0444"}:
        raise ProductionASRError("input.sealed_mode must be 0400 or 0444")
    observed_mode = stat.S_IMODE(path.lstat().st_mode)
    if observed_mode != int(sealed_mode, 8) or observed_mode & 0o222:
        raise ProductionASRError(
            "input must match its sealed, non-writable producer mode"
        )
    media_format = require_exact_keys(
        item["media_format"],
        "input.media_format",
        {"container", "codec", "sample_rate_hz", "channels", "sample_format"},
    )
    normalized_format = {
        "container": bounded_text(media_format["container"], "input.media_format.container", 32),
        "codec": bounded_text(media_format["codec"], "input.media_format.codec", 32),
        "sample_rate_hz": integer(
            media_format["sample_rate_hz"], "input.media_format.sample_rate_hz", 1, 384_000
        ),
        "channels": integer(media_format["channels"], "input.media_format.channels", 1, 32),
        "sample_format": bounded_text(
            media_format["sample_format"], "input.media_format.sample_format", 32
        ),
    }
    if normalized_format != {
        "container": "flac",
        "codec": "flac",
        "sample_rate_hz": 16_000,
        "channels": 1,
        "sample_format": "s16",
    }:
        raise ProductionASRError(
            "production GPU input must be normalized 16 kHz mono s16 FLAC"
        )
    return {
        "path": str(path),
        "expected_sha256": sha256_value(
            item["expected_sha256"], "input.expected_sha256"
        ),
        "expected_byte_count": integer(
            item["expected_byte_count"], "input.expected_byte_count", 1, MAX_AUDIO_BYTES
        ),
        "expected_duration_ms": integer(
            item["expected_duration_ms"],
            "input.expected_duration_ms",
            1,
            MAX_AUDIO_SECONDS * 1_000,
        ),
        "sealed_mode": sealed_mode,
        "media_id": identifier(item["media_id"], "input.media_id"),
        "artifact_id": identifier(item["artifact_id"], "input.artifact_id"),
        "parent_processing_run_id": identifier(
            item["parent_processing_run_id"], "input.parent_processing_run_id"
        ),
        "timeline_offset_ms": integer(
            item["timeline_offset_ms"], "input.timeline_offset_ms", 0, 10**15
        ),
        "media_format": normalized_format,
    }


def validate_model(value: Any) -> dict[str, Any]:
    keys = {
        "manifest_path",
        "expected_manifest_sha256",
        "receipt_path",
        "expected_receipt_sha256",
        "snapshot_root",
        "identity_sha256",
        "repository",
        "revision",
        "license",
    }
    item = require_exact_keys(value, "model", keys)
    manifest_path = existing_regular_file(
        item["manifest_path"],
        "model.manifest_path",
        exact_mode=0o400,
        single_link=True,
    )
    receipt_path = existing_regular_file(
        item["receipt_path"],
        "model.receipt_path",
        exact_mode=0o400,
        single_link=True,
    )
    snapshot_root = existing_private_directory(
        item["snapshot_root"], "model.snapshot_root", mode=0o500
    )
    return {
        "manifest_path": str(manifest_path),
        "expected_manifest_sha256": sha256_value(
            item["expected_manifest_sha256"], "model.expected_manifest_sha256"
        ),
        "receipt_path": str(receipt_path),
        "expected_receipt_sha256": sha256_value(
            item["expected_receipt_sha256"], "model.expected_receipt_sha256"
        ),
        "snapshot_root": str(snapshot_root),
        "identity_sha256": sha256_value(
            item["identity_sha256"], "model.identity_sha256"
        ),
        "repository": bounded_text(item["repository"], "model.repository", 1_000),
        "revision": bounded_text(item["revision"], "model.revision", 256),
        "license": bounded_text(item["license"], "model.license", 256),
    }


def validate_runtime(value: Any) -> dict[str, Any]:
    keys = {
        "root",
        "expected_device",
        "python",
        "pyproject",
        "lock",
        "runtime_manifest",
        "ffprobe",
        "adapter",
        "packages",
        "offline_policy",
    }
    item = require_exact_keys(value, "runtime", keys)
    root = existing_private_directory(item["root"], "runtime.root", mode=0o700)
    expected_device = integer(
        item["expected_device"], "runtime.expected_device", 1, 2**63 - 1
    )
    if root.stat().st_dev != expected_device:
        raise ProductionASRError("runtime.root is not on runtime.expected_device")
    python = validate_file_reference(item["python"], "runtime.python")
    pyproject = validate_file_reference(item["pyproject"], "runtime.pyproject")
    lock = validate_file_reference(item["lock"], "runtime.lock")
    runtime_manifest = validate_file_reference(
        item["runtime_manifest"], "runtime.runtime_manifest"
    )
    runtime_receipt_path = Path(runtime_manifest["path"])
    runtime_receipt_metadata = runtime_receipt_path.lstat()
    if (
        stat.S_IMODE(runtime_receipt_metadata.st_mode) != 0o400
        or runtime_receipt_metadata.st_nlink != 1
    ):
        raise ProductionASRError(
            "runtime.runtime_manifest must be a mode-0400 single-link admission receipt"
        )
    ffprobe = validate_file_reference(item["ffprobe"], "runtime.ffprobe")
    adapter = validate_file_reference(item["adapter"], "runtime.adapter")
    if not os.access(python["path"], os.X_OK):
        raise ProductionASRError("runtime.python.path is not executable")
    if not os.access(ffprobe["path"], os.X_OK):
        raise ProductionASRError("runtime.ffprobe.path is not executable")
    packages = require_exact_keys(
        item["packages"], "runtime.packages", EXPECTED_PACKAGE_KEYS
    )
    normalized_packages = {
        name: bounded_text(packages[name], f"runtime.packages.{name}", 128)
        for name in sorted(EXPECTED_PACKAGE_KEYS)
    }
    if item["offline_policy"] != OFFLINE_POLICY:
        raise ProductionASRError(
            f"runtime.offline_policy must be {OFFLINE_POLICY}"
        )
    if Path(python["path"]).stat().st_dev != expected_device:
        raise ProductionASRError(
            "runtime.python.path is not on runtime.expected_device"
        )
    if runtime_receipt_path.stat().st_dev != expected_device:
        raise ProductionASRError(
            "runtime.runtime_manifest is not on runtime.expected_device"
        )
    source_path = Path(__file__).resolve()
    if Path(adapter["path"]) != source_path:
        raise ProductionASRError("runtime.adapter.path must bind this adapter source")
    return {
        "root": str(root),
        "expected_device": expected_device,
        "python": python,
        "pyproject": pyproject,
        "lock": lock,
        "runtime_manifest": runtime_manifest,
        "ffprobe": ffprobe,
        "adapter": adapter,
        "packages": normalized_packages,
        "offline_policy": OFFLINE_POLICY,
    }


def validate_gpu(value: Any, runtime: dict[str, Any]) -> dict[str, Any]:
    keys = {
        "expected_uuid",
        "device_index",
        "compute_type",
        "lock_path",
        "lock_policy",
        "minimum_free_vram_bytes",
        "maximum_process_vram_bytes",
    }
    item = require_exact_keys(value, "gpu", keys)
    expected_uuid = bounded_text(item["expected_uuid"], "gpu.expected_uuid", 96)
    if not GPU_UUID_RE.fullmatch(expected_uuid):
        raise ProductionASRError("gpu.expected_uuid is not an NVIDIA GPU UUID")
    device_index = integer(item["device_index"], "gpu.device_index", 0, 0)
    compute_type = bounded_text(item["compute_type"], "gpu.compute_type", 32)
    if compute_type not in ALLOWED_COMPUTE_TYPES:
        raise ProductionASRError(
            f"gpu.compute_type must be one of {sorted(ALLOWED_COMPUTE_TYPES)}"
        )
    lock_path = normalized_absolute_path(item["lock_path"], "gpu.lock_path")
    expected_lock = Path(runtime["root"]) / "locks" / f"{expected_uuid}.lock"
    if lock_path != expected_lock:
        raise ProductionASRError(
            f"gpu.lock_path must be the UUID-derived path {expected_lock}"
        )
    if item["lock_policy"] != GPU_LOCK_POLICY:
        raise ProductionASRError(f"gpu.lock_policy must be {GPU_LOCK_POLICY}")
    minimum_free = integer(
        item["minimum_free_vram_bytes"],
        "gpu.minimum_free_vram_bytes",
        1,
        MAX_VRAM_BYTES,
    )
    maximum_process = integer(
        item["maximum_process_vram_bytes"],
        "gpu.maximum_process_vram_bytes",
        1,
        MAX_VRAM_BYTES,
    )
    return {
        "expected_uuid": expected_uuid,
        "device_index": device_index,
        "compute_type": compute_type,
        "lock_path": str(lock_path),
        "lock_policy": GPU_LOCK_POLICY,
        "minimum_free_vram_bytes": minimum_free,
        "maximum_process_vram_bytes": maximum_process,
    }


def validate_inference(value: Any, input_item: dict[str, Any]) -> dict[str, Any]:
    keys = {
        "language",
        "beam_size",
        "best_of",
        "temperature",
        "condition_on_previous_text",
        "word_timestamps",
        "vad_filter",
        "cpu_threads",
        "num_workers",
        "max_audio_bytes",
        "max_audio_seconds",
        "max_wall_seconds",
        "max_result_bytes",
        "max_segments",
        "max_words",
    }
    item = require_exact_keys(value, "inference", keys)
    language = bounded_text(item["language"], "inference.language", 64).lower()
    if not LANGUAGE_RE.fullmatch(language):
        raise ProductionASRError(
            "inference.language must be 'auto' or a lowercase language tag"
        )
    if item["condition_on_previous_text"] is not False:
        raise ProductionASRError(
            "inference.condition_on_previous_text must be false in contract v1"
        )
    if item["word_timestamps"] is not True:
        raise ProductionASRError(
            "inference.word_timestamps must be true in contract v1"
        )
    if item["vad_filter"] is not False:
        raise ProductionASRError(
            "inference.vad_filter must be false; VAD remains a separate revision"
        )
    max_audio_bytes = integer(
        item["max_audio_bytes"], "inference.max_audio_bytes", 1, MAX_AUDIO_BYTES
    )
    max_audio_seconds = finite_number(
        item["max_audio_seconds"],
        "inference.max_audio_seconds",
        0.001,
        MAX_AUDIO_SECONDS,
    )
    max_wall_seconds = finite_number(
        item["max_wall_seconds"],
        "inference.max_wall_seconds",
        1.0,
        MAX_WALL_SECONDS,
    )
    max_result_bytes = integer(
        item["max_result_bytes"], "inference.max_result_bytes", 1, MAX_RESULT_BYTES
    )
    if input_item["expected_byte_count"] > max_audio_bytes:
        raise ProductionASRError("input exceeds inference.max_audio_bytes")
    if input_item["expected_duration_ms"] > round(max_audio_seconds * 1_000):
        raise ProductionASRError("input exceeds inference.max_audio_seconds")
    return {
        "language": language,
        "beam_size": integer(item["beam_size"], "inference.beam_size", 1, 20),
        "best_of": integer(item["best_of"], "inference.best_of", 1, 20),
        "temperature": finite_number(
            item["temperature"], "inference.temperature", 0.0, 1.0
        ),
        "condition_on_previous_text": False,
        "word_timestamps": True,
        "vad_filter": False,
        "cpu_threads": integer(
            item["cpu_threads"], "inference.cpu_threads", 1, 32
        ),
        "num_workers": integer(item["num_workers"], "inference.num_workers", 1, 4),
        "max_audio_bytes": max_audio_bytes,
        "max_audio_seconds": max_audio_seconds,
        "max_wall_seconds": max_wall_seconds,
        "max_result_bytes": max_result_bytes,
        "max_segments": integer(
            item["max_segments"], "inference.max_segments", 1, MAX_SEGMENTS
        ),
        "max_words": integer(item["max_words"], "inference.max_words", 1, MAX_WORDS),
    }


def validate_catalog_context(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    item = require_exact_keys(
        value, "catalog_context", {"source_id", "recording_id", "rendition_id"}
    )
    normalized: dict[str, Any] = {
        "source_id": identifier(item["source_id"], "catalog_context.source_id"),
        "recording_id": identifier(
            item["recording_id"], "catalog_context.recording_id"
        ),
        "rendition_id": None,
    }
    if item["rendition_id"] is not None:
        normalized["rendition_id"] = identifier(
            item["rendition_id"], "catalog_context.rendition_id"
        )
    return normalized


def validate_output(value: Any, runtime: dict[str, Any]) -> dict[str, Any]:
    item = require_exact_keys(value, "output", {"root"})
    root = existing_private_directory(item["root"], "output.root", mode=0o700)
    if root.stat().st_dev != runtime["expected_device"]:
        raise ProductionASRError(
            "output.root must remain on runtime.expected_device (the main drive)"
        )
    for reference in (
        runtime["python"],
        runtime["runtime_manifest"],
        runtime["pyproject"],
        runtime["lock"],
        runtime["adapter"],
        runtime["ffprobe"],
    ):
        path = Path(reference["path"])
        if path == root or root in path.parents:
            raise ProductionASRError("runtime inputs must remain outside output.root")
    return {"root": str(root)}


WORK_ORDER_CORE_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "job_id",
    "input",
    "model",
    "runtime",
    "gpu",
    "inference",
    "catalog_context",
    "output",
    "policy",
}


def normalize_work_order_core(value: Any) -> dict[str, Any]:
    item = require_exact_keys(value, "work-order core", WORK_ORDER_CORE_KEYS)
    if item["kind"] != WORK_ORDER_KIND:
        raise ProductionASRError(f"kind must be {WORK_ORDER_KIND}")
    if item["schema_version"] != CONTRACT_VERSION:
        raise ProductionASRError(f"schema_version must be {CONTRACT_VERSION}")
    if item["implementation_version"] != IMPLEMENTATION_VERSION:
        raise ProductionASRError(
            f"implementation_version must be {IMPLEMENTATION_VERSION}"
        )
    job_id = item["job_id"]
    if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
        raise ProductionASRError("job_id contains unsupported characters")
    input_item = validate_input(item["input"])
    model = validate_model(item["model"])
    runtime = validate_runtime(item["runtime"])
    gpu = validate_gpu(item["gpu"], runtime)
    inference = validate_inference(item["inference"], input_item)
    output = validate_output(item["output"], runtime)
    if item["policy"] != POLICY:
        raise ProductionASRError("work-order policy must be the exact fail-closed policy")
    input_path = Path(input_item["path"])
    model_manifest_path = Path(model["manifest_path"])
    model_root = Path(model["snapshot_root"])
    output_root = Path(output["root"])
    for path in (input_path, model_manifest_path, model_root):
        if path == output_root or output_root in path.parents:
            raise ProductionASRError(
                "input and model bindings must remain outside output.root"
            )
    if model_root.stat().st_dev != runtime["expected_device"]:
        raise ProductionASRError(
            "model.snapshot_root must remain on runtime.expected_device"
        )
    return {
        "kind": WORK_ORDER_KIND,
        "schema_version": CONTRACT_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "job_id": job_id,
        "input": input_item,
        "model": model,
        "runtime": runtime,
        "gpu": gpu,
        "inference": inference,
        "catalog_context": validate_catalog_context(item["catalog_context"]),
        "output": output,
        "policy": dict(POLICY),
    }


def make_work_order(core: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_work_order_core(core)
    identity = sha256_bytes(canonical_bytes(normalized))
    return {
        **normalized,
        "identity_sha256": identity,
        "work_order_id": f"gpuasrwo_{identity[:32]}",
    }


def validate_work_order(value: Any) -> dict[str, Any]:
    keys = WORK_ORDER_CORE_KEYS | {"identity_sha256", "work_order_id"}
    item = require_exact_keys(value, "work order", keys)
    core = {key: item[key] for key in WORK_ORDER_CORE_KEYS}
    expected = make_work_order(core)
    if item != expected:
        raise ProductionASRError(
            "work order is not canonical or its immutable identity is invalid"
        )
    return expected


def stable_file_bytes(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
    exact_mode: int | None = None,
    single_link: bool = False,
) -> bytes:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ProductionASRError(f"{label} must be a non-symlinked regular file")
    if exact_mode is not None and stat.S_IMODE(before.st_mode) != exact_mode:
        raise ProductionASRError(f"{label} must have mode {exact_mode:04o}")
    if single_link and before.st_nlink != 1:
        raise ProductionASRError(f"{label} must have exactly one hard link")
    if before.st_size > maximum_bytes:
        raise ProductionASRError(f"{label} exceeds {maximum_bytes} bytes")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if stable_stat_identity(opened) != stable_stat_identity(before):
            raise ProductionASRError(f"{label} changed while being opened")
        chunks: list[bytes] = []
        remaining = maximum_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after_fd = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = path.lstat()
    if (
        len(body) != before.st_size
        or len(body) > maximum_bytes
        or stable_stat_identity(after_fd) != stable_stat_identity(before)
        or stable_stat_identity(after_path) != stable_stat_identity(before)
    ):
        raise ProductionASRError(f"{label} changed while being read")
    return body


def stable_hash_file(path: Path, label: str, maximum_bytes: int | None = None) -> dict[str, Any]:
    limit = maximum_bytes if maximum_bytes is not None else max(path.stat().st_size, 1)
    body = stable_file_bytes(path, label=label, maximum_bytes=limit)
    observed = path.stat()
    return {
        "path": str(path),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "mode": stat.S_IMODE(observed.st_mode),
        "link_count": observed.st_nlink,
    }


def verify_reference(reference: dict[str, Any], label: str) -> dict[str, Any]:
    observed = stable_hash_file(Path(reference["path"]), label)
    if observed["sha256"] != reference["expected_sha256"]:
        raise ProductionASRError(
            f"{label} SHA-256 mismatch: expected {reference['expected_sha256']}, "
            f"observed {observed['sha256']}"
        )
    return observed


@contextmanager
def retained_verified_input(input_item: dict[str, Any]) -> Iterator[tuple[int, str, dict[str, Any]]]:
    path = Path(input_item["path"])
    before = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if stable_stat_identity(opened) != stable_stat_identity(before):
            raise ProductionASRError("input changed while being opened")
        digest = hashlib.sha256()
        offset = 0
        while chunk := os.pread(descriptor, 8 * 1024 * 1024, offset):
            digest.update(chunk)
            offset += len(chunk)
        observed_sha = digest.hexdigest()
        if (
            offset != input_item["expected_byte_count"]
            or observed_sha != input_item["expected_sha256"]
        ):
            raise ProductionASRError("retained input byte count or SHA-256 mismatch")
        proc_path = Path("/proc/self/fd") / str(descriptor)
        if not proc_path.is_file():
            raise ProductionASRError("Linux /proc/self/fd transport is unavailable")
        observation = {
            "path": str(path),
            "sha256": observed_sha,
            "byte_count": offset,
            "device": opened.st_dev,
            "inode": opened.st_ino,
            "mode": stat.S_IMODE(opened.st_mode),
            "link_count": opened.st_nlink,
        }
        yield descriptor, str(proc_path), observation
        digest_after = hashlib.sha256()
        offset_after = 0
        while chunk := os.pread(descriptor, 8 * 1024 * 1024, offset_after):
            digest_after.update(chunk)
            offset_after += len(chunk)
        if (
            stable_stat_identity(os.fstat(descriptor)) != stable_stat_identity(opened)
            or stable_stat_identity(path.lstat()) != stable_stat_identity(opened)
            or offset_after != offset
            or digest_after.hexdigest() != observed_sha
        ):
            raise ProductionASRError("retained input changed during GPU ASR")
    finally:
        os.close(descriptor)


def load_work_order(path_value: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    path = existing_regular_file(
        path_value,
        "--work-order",
        exact_mode=0o400,
        single_link=True,
    )
    body = stable_file_bytes(
        path,
        label="work order",
        maximum_bytes=MAX_JSON_INPUT_BYTES,
        exact_mode=0o400,
        single_link=True,
    )
    raw = parse_json_bytes(body, "work order")
    work_order = validate_work_order(raw)
    if body != canonical_bytes(work_order):
        raise ProductionASRError("work-order file must use exact canonical JSON bytes")
    return work_order, {
        "path": str(path),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "identity_sha256": work_order["identity_sha256"],
        "work_order_id": work_order["work_order_id"],
    }


def load_creation_spec(path_value: Any) -> dict[str, Any]:
    path = existing_regular_file(path_value, "--spec")
    body = stable_file_bytes(
        path,
        label="work-order creation spec",
        maximum_bytes=MAX_JSON_INPUT_BYTES,
    )
    value = parse_json_bytes(body, "work-order creation spec")
    return normalize_work_order_core(value)


def replay_model_binding(work_order: dict[str, Any]) -> dict[str, Any]:
    model = work_order["model"]
    manifest_observation = verify_reference(
        {
            "path": model["manifest_path"],
            "expected_sha256": model["expected_manifest_sha256"],
        },
        "model manifest",
    )
    receipt_observation = verify_reference(
        {
            "path": model["receipt_path"],
            "expected_sha256": model["expected_receipt_sha256"],
        },
        "model admission receipt",
    )
    model_admission = load_local_gpu_module(
        "himr_production_model_admission", "admit_hf_model.py"
    )
    manifest_path = Path(model["manifest_path"])
    bundle = manifest_path.parent
    try:
        summary = model_admission.validate_bundle(bundle)
        manifest_body = stable_file_bytes(
            manifest_path,
            label="model admission manifest",
            maximum_bytes=MAX_JSON_INPUT_BYTES,
            exact_mode=0o400,
            single_link=True,
        )
        manifest = parse_json_bytes(manifest_body, "model admission manifest")
    except (OSError, RuntimeError, ValueError) as error:
        raise ProductionASRError(f"sealed model replay failed: {error}") from error
    root = bundle / model_admission.SNAPSHOT_DIRECTORY
    expected_receipt_path = bundle / model_admission.RECEIPT_NAME
    if Path(model["receipt_path"]) != expected_receipt_path:
        raise ProductionASRError("model.receipt_path is not the admitted bundle receipt")
    if (
        summary["manifest_sha256"] != model["expected_manifest_sha256"]
        or summary["receipt_sha256"] != model["expected_receipt_sha256"]
    ):
        raise ProductionASRError("model admission manifest or receipt digest differs")
    expected_fields = {
        "snapshot_root": str(root),
        "identity_sha256": summary["manifest_identity_sha256"],
        "repository": summary["repository"],
        "revision": summary["revision"],
        "license": manifest["expected_license"],
    }
    for key, observed in expected_fields.items():
        if model[key] != observed:
            raise ProductionASRError(f"model.{key} differs from the sealed manifest")
    return {
        "manifest": manifest_observation,
        "admission_receipt": receipt_observation,
        "snapshot_root": str(root),
        "identity_sha256": summary["manifest_identity_sha256"],
        "repository": summary["repository"],
        "revision": summary["revision"],
        "license": manifest["expected_license"],
        "license_evidence": manifest["license_evidence"],
        "admission_receipt_identity_sha256": summary[
            "receipt_identity_sha256"
        ],
        "file_count": len(manifest["files"]),
        "total_bytes": sum(item["byte_count"] for item in manifest["files"]),
    }


def replay_runtime_binding(work_order: dict[str, Any], *, require_current: bool) -> dict[str, Any]:
    runtime = work_order["runtime"]
    observations = {
        name: verify_reference(runtime[name], f"runtime {name}")
        for name in ("python", "pyproject", "lock", "runtime_manifest", "ffprobe", "adapter")
    }
    runtime_admission = load_local_gpu_module(
        "himr_production_runtime_admission", "admit_runtime.py"
    )
    try:
        admission = runtime_admission.validate_receipt(
            runtime["runtime_manifest"]["path"],
            runtime["runtime_manifest"]["expected_sha256"],
        )
    except (OSError, RuntimeError, ValueError, KeyError, ImportError) as error:
        raise ProductionASRError(
            f"production runtime admission replay failed: {error}"
        ) from error
    configuration = admission["configuration"]
    evidence = admission["evidence"]
    expected_bindings = {
        "runtime_root": runtime["root"],
        "python_path": runtime["python"]["path"],
        "pyproject_path": runtime["pyproject"]["path"],
        "pyproject_sha256": runtime["pyproject"]["expected_sha256"],
        "lock_path": runtime["lock"]["path"],
        "lock_sha256": runtime["lock"]["expected_sha256"],
        "gpu_uuid": work_order["gpu"]["expected_uuid"],
        "device_index": work_order["gpu"]["device_index"],
        "compute_type": work_order["gpu"]["compute_type"],
        "scheduler_lock_path": work_order["gpu"]["lock_path"],
    }
    observed_bindings = {
        "runtime_root": configuration["runtime_root"],
        "python_path": configuration["python_executable"]["path"],
        "pyproject_path": configuration["pyproject"]["path"],
        "pyproject_sha256": configuration["pyproject"]["expected_sha256"],
        "lock_path": configuration["lock"]["path"],
        "lock_sha256": configuration["lock"]["expected_sha256"],
        "gpu_uuid": evidence["hardware"]["uuid"],
        "device_index": evidence["hardware"]["device_index"],
        "compute_type": evidence["hardware"]["selected_compute_type"],
        "scheduler_lock_path": evidence["scheduler_lock"]["lock_path"],
    }
    if observed_bindings != expected_bindings:
        raise ProductionASRError(
            "runtime admission receipt differs from the GPU ASR work order"
        )
    admitted_sources = {
        item["requested_path"]: item["sha256"]
        for item in evidence["bindings"]["sources"]
    }
    if admitted_sources.get(runtime["adapter"]["path"]) != runtime["adapter"][
        "expected_sha256"
    ]:
        raise ProductionASRError("adapter source is absent from runtime admission")
    try:
        admitted_profile = evidence["benchmark"]["profile"]
        admitted_thresholds = evidence["benchmark"]["thresholds"]
        contracts = contract_document()
        current_inference_profile = inference_profile(work_order)
        expected_profile_bindings = {
            "model_identity_sha256": work_order["model"]["identity_sha256"],
            "work_order_contract_sha256": contracts["work_order"][
                "identity_sha256"
            ],
            "result_contract_sha256": contracts["result"]["identity_sha256"],
            "inference": current_inference_profile,
            "inference_profile_sha256": sha256_bytes(
                canonical_bytes(current_inference_profile)
            ),
        }
        observed_profile_bindings = {
            key: admitted_profile[key] for key in expected_profile_bindings
        }
        maximum_admitted_process_vram = admitted_thresholds[
            "maximum_peak_process_vram_bytes"
        ]
        minimum_admitted_vram_reserve = admitted_thresholds[
            "minimum_vram_reserve_bytes"
        ]
    except (KeyError, TypeError) as error:
        raise ProductionASRError(
            "runtime admission lacks a complete production benchmark profile"
        ) from error
    if observed_profile_bindings != expected_profile_bindings:
        raise ProductionASRError(
            "work-order model, contracts, or inference profile differs from the "
            "admitted production benchmark"
        )
    if (
        work_order["gpu"]["maximum_process_vram_bytes"]
        > maximum_admitted_process_vram
        or work_order["gpu"]["minimum_free_vram_bytes"]
        < minimum_admitted_vram_reserve
    ):
        raise ProductionASRError(
            "work-order VRAM bounds exceed the admitted benchmark policy"
        )
    packages: dict[str, str] = {}
    if require_current:
        current_python = Path(sys.executable).resolve()
        if current_python != Path(runtime["python"]["path"]):
            raise ProductionASRError(
                "current Python executable does not match the work-order runtime"
            )
        if sys.version_info[:3] != (3, 12, 14):
            raise ProductionASRError("production GPU runtime requires CPython 3.12.14")
        for name, expected in runtime["packages"].items():
            try:
                observed = package_version(name)
            except PackageNotFoundError as error:
                raise ProductionASRError(
                    f"required runtime package is missing: {name}"
                ) from error
            if observed != expected:
                raise ProductionASRError(
                    f"runtime package {name} mismatch: expected {expected}, observed {observed}"
                )
            packages[name] = observed
    return {
        "root": runtime["root"],
        "expected_device": runtime["expected_device"],
        "files": observations,
        "admission": {
            "receipt_id": admission["receipt_id"],
            "identity_sha256": admission["identity_sha256"],
            "benchmark_accepted": evidence["benchmark"]["accepted"],
            "benchmark_id": evidence["benchmark"]["benchmark_id"],
            "benchmark_profile_name": admitted_profile["name"],
            "benchmark_inference_profile_sha256": admitted_profile[
                "inference_profile_sha256"
            ],
            "scheduler_lock_accepted": evidence["scheduler_lock"]["accepted"],
            "wheelhouse_tree_sha256": evidence["wheelhouse"]["tree_sha256"],
            "runtime_tree_sha256": evidence["runtime"]["tree"]["tree_sha256"],
        },
        "packages": packages or dict(runtime["packages"]),
        "python_version": platform.python_version(),
        "offline_policy": runtime["offline_policy"],
    }


def probe_audio(
    work_order: dict[str, Any],
    input_path: str | None = None,
    *,
    pass_fds: tuple[int, ...] = (),
) -> tuple[dict[str, Any], list[str]]:
    ffprobe = work_order["runtime"]["ffprobe"]["path"]
    path = input_path or work_order["input"]["path"]
    command = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "format=duration,format_name:stream=index,codec_name,sample_fmt,sample_rate,channels,channel_layout,duration",
        "-of",
        "json",
        path,
    ]
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/nonexistent",
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
        "NO_PROXY": "*",
        "no_proxy": "*",
    }
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            env=environment,
            pass_fds=pass_fds,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ProductionASRError(f"ffprobe failed: {error}") from error
    payload = parse_json_bytes(completed.stdout.encode("utf-8"), "ffprobe output")
    streams = payload.get("streams") if isinstance(payload, dict) else None
    format_item = payload.get("format") if isinstance(payload, dict) else None
    if (
        not isinstance(streams, list)
        or len(streams) != 1
        or not isinstance(streams[0], dict)
        or not isinstance(format_item, dict)
    ):
        raise ProductionASRError("input must contain exactly one primary audio stream")
    stream = streams[0]
    try:
        duration_seconds = float(format_item.get("duration") or stream.get("duration"))
    except (TypeError, ValueError) as error:
        raise ProductionASRError("input duration is unavailable") from error
    if not math.isfinite(duration_seconds) or duration_seconds <= 0:
        raise ProductionASRError("input duration must be finite and positive")
    observed = {
        "container": format_item.get("format_name"),
        "codec": stream.get("codec_name"),
        "sample_rate_hz": int(stream.get("sample_rate", 0)),
        "channels": int(stream.get("channels", 0)),
        "sample_format": stream.get("sample_fmt"),
        "channel_layout": stream.get("channel_layout"),
        "duration_ms": round(duration_seconds * 1_000),
    }
    expected_format = work_order["input"]["media_format"]
    for key in ("container", "codec", "sample_rate_hz", "channels", "sample_format"):
        if observed[key] != expected_format[key]:
            raise ProductionASRError(
                f"input probe differs from work order: {key}={observed[key]!r}"
            )
    if observed["duration_ms"] != work_order["input"]["expected_duration_ms"]:
        raise ProductionASRError(
            "input duration differs from input.expected_duration_ms"
        )
    return observed, command


def inference_profile(work_order: dict[str, Any]) -> dict[str, Any]:
    inference = work_order["inference"]
    return {
        "compute_type": work_order["gpu"]["compute_type"],
        "language": inference["language"],
        "beam_size": inference["beam_size"],
        "best_of": inference["best_of"],
        "temperature": inference["temperature"],
        "condition_on_previous_text": inference["condition_on_previous_text"],
        "word_timestamps": inference["word_timestamps"],
        "vad_filter": inference["vad_filter"],
        "cpu_threads": inference["cpu_threads"],
        "num_workers": inference["num_workers"],
    }


def recipe(work_order: dict[str, Any]) -> dict[str, Any]:
    contracts = contract_document()
    profile = inference_profile(work_order)
    return {
        "stage": STAGE,
        "contract_version": CONTRACT_VERSION,
        "work_order_contract_sha256": contracts["work_order"]["identity_sha256"],
        "result_contract_sha256": contracts["result"]["identity_sha256"],
        "inference_profile_sha256": sha256_bytes(canonical_bytes(profile)),
        "implementation_version": IMPLEMENTATION_VERSION,
        "adapter_sha256": work_order["runtime"]["adapter"]["expected_sha256"],
        "runtime_manifest_sha256": work_order["runtime"]["runtime_manifest"][
            "expected_sha256"
        ],
        "model_manifest_sha256": work_order["model"]["expected_manifest_sha256"],
        "model_admission_receipt_sha256": work_order["model"][
            "expected_receipt_sha256"
        ],
        "model_identity_sha256": work_order["model"]["identity_sha256"],
        "model_revision": work_order["model"]["revision"],
        "gpu": {
            key: work_order["gpu"][key]
            for key in (
                "expected_uuid",
                "device_index",
                "compute_type",
                "minimum_free_vram_bytes",
                "maximum_process_vram_bytes",
            )
        },
        "inference": work_order["inference"],
        "timeline_offset_ms": work_order["input"]["timeline_offset_ms"],
        "output_contract": OUTPUT_CONTRACT,
        "confidence_contract": "raw-model-scores-uncalibrated-v1",
    }


def result_plan(work_order: dict[str, Any]) -> dict[str, Any]:
    recipe_value = recipe(work_order)
    recipe_sha = sha256_bytes(canonical_bytes(recipe_value))
    recipe_id = f"recipe_gpu_asr_{recipe_sha[:32]}"
    identity = {
        "work_order_identity_sha256": work_order["identity_sha256"],
        "input_sha256": work_order["input"]["expected_sha256"],
        "input_artifact_id": work_order["input"]["artifact_id"],
        "recipe_id": recipe_id,
        "catalog_context": work_order["catalog_context"],
    }
    result_key = sha256_bytes(canonical_bytes(identity))
    output_root = Path(work_order["output"]["root"])
    run_dir = (
        output_root
        / "asr"
        / "faster-whisper-gpu"
        / "sha256"
        / work_order["input"]["expected_sha256"][:2]
        / work_order["input"]["expected_sha256"]
        / "results"
        / result_key
    )
    return {
        "recipe": recipe_value,
        "recipe_sha256": recipe_sha,
        "recipe_id": recipe_id,
        "result_key": result_key,
        "result_dir": str(run_dir),
        "result_path": str(run_dir / "result.json"),
        "raw_transcript_path": str(run_dir / "transcript.raw.json"),
        "normalized_transcript_path": str(run_dir / "transcript.normalized.json"),
    }


def replay_static_bindings(
    work_order: dict[str, Any], *, require_current_runtime: bool
) -> dict[str, Any]:
    input_observation = stable_hash_file(
        Path(work_order["input"]["path"]),
        "input audio",
        maximum_bytes=work_order["inference"]["max_audio_bytes"],
    )
    if (
        input_observation["sha256"] != work_order["input"]["expected_sha256"]
        or input_observation["byte_count"] != work_order["input"]["expected_byte_count"]
    ):
        raise ProductionASRError("input byte count or SHA-256 binding failed")
    if (
        input_observation["mode"] != int(work_order["input"]["sealed_mode"], 8)
        or input_observation["mode"] & 0o222
        or input_observation["link_count"] != 1
    ):
        raise ProductionASRError(
            "input must remain in its bound sealed mode with one link"
        )
    runtime = replay_runtime_binding(
        work_order, require_current=require_current_runtime
    )
    model = replay_model_binding(work_order)
    probe, probe_command = probe_audio(work_order)
    return {
        "input": input_observation | {"probe": probe},
        "model": model,
        "runtime": runtime,
        "commands": [probe_command],
    }


def ensure_cuda_wheel_libraries() -> None:
    purelib = Path(sysconfig.get_paths()["purelib"])
    required = [
        purelib / "nvidia" / "cublas" / "lib",
        purelib / "nvidia" / "cudnn" / "lib",
    ]
    missing = [str(path) for path in required if not path.is_dir()]
    if missing:
        raise ProductionASRError(
            f"pinned CUDA wheel library directories are missing: {missing}"
        )
    current = [
        part for part in os.environ.get("LD_LIBRARY_PATH", "").split(":") if part
    ]
    if all(str(path) in current for path in required):
        return
    environment = os.environ.copy()
    environment.pop("PYTHONHOME", None)
    environment.pop("PYTHONPATH", None)
    environment.update(
        {
            "LD_LIBRARY_PATH": ":".join(
                [str(path) for path in required] + current
            ),
            "HF_HUB_OFFLINE": "1",
            "HF_DATASETS_OFFLINE": "1",
            "HF_HUB_DISABLE_TELEMETRY": "1",
            "DO_NOT_TRACK": "1",
        }
    )
    source = str(Path(__file__).resolve())
    os.execve(
        sys.executable,
        [sys.executable, "-B", "-I", source, *sys.argv[1:]],
        environment,
    )


def network_isolation_evidence(expected_parent_namespace: str) -> dict[str, Any]:
    if not re.fullmatch(r"net:\[[0-9]+\]", expected_parent_namespace):
        raise ProductionASRError(
            "--expected-parent-network-namespace must look like net:[INTEGER]"
        )
    try:
        process_namespace = os.readlink("/proc/self/ns/net")
        lines = Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]
    except OSError as error:
        raise ProductionASRError(f"network namespace evidence unavailable: {error}") from error
    if process_namespace == expected_parent_namespace:
        raise ProductionASRError("run did not enter a distinct network namespace")
    interfaces = sorted(
        {
            line.split(":", 1)[0].strip()
            for line in lines
            if ":" in line and line.split(":", 1)[0].strip()
        }
    )
    non_loopback = [name for name in interfaces if name != "lo"]
    if non_loopback:
        raise ProductionASRError(
            f"offline namespace exposes non-loopback interfaces: {non_loopback}"
        )
    return {
        "method": "external_network_namespace",
        "expected_parent_namespace": expected_parent_namespace,
        "process_namespace": process_namespace,
        "interfaces": interfaces,
        "non_loopback_interfaces": non_loopback,
        "verified": True,
    }


def ensure_offline_environment() -> None:
    os.environ.update(
        {
            "HF_HUB_OFFLINE": "1",
            "HF_DATASETS_OFFLINE": "1",
            "HF_HUB_DISABLE_TELEMETRY": "1",
            "DO_NOT_TRACK": "1",
            "NO_PROXY": "*",
            "no_proxy": "*",
        }
    )


def ensure_private_subdirectory(parent: Path, name: str) -> Path:
    if not name or name in {".", ".."} or "/" in name:
        raise ProductionASRError("invalid private output directory component")
    path = parent / name
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise ProductionASRError(
            f"private output directory is unsafe or not mode 0700: {path}"
        )
    return path


def ensure_result_parent(work_order: dict[str, Any]) -> Path:
    plan = result_plan(work_order)
    output_root = Path(work_order["output"]["root"])
    expected_parent = Path(plan["result_dir"]).parent
    current = output_root
    relative = expected_parent.relative_to(output_root)
    for component in relative.parts:
        current = ensure_private_subdirectory(current, component)
    return current


def lock_state(lock_path: Path) -> str:
    if not lock_path.exists():
        return "absent"
    observed = lock_path.lstat()
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise ProductionASRError("GPU lock path exists but is not a regular file")
    if observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != 0o600:
        raise ProductionASRError("GPU lock file must be owner-private mode 0600")
    descriptor = os.open(lock_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "held"
        else:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return "available"
    finally:
        os.close(descriptor)


@contextmanager
def gpu_advisory_lock(work_order: dict[str, Any]) -> Iterator[dict[str, Any]]:
    lock_path = Path(work_order["gpu"]["lock_path"])
    lock_parent = lock_path.parent
    runtime_root = Path(work_order["runtime"]["root"])
    if lock_parent.parent != runtime_root:
        raise ProductionASRError("GPU lock parent escaped runtime.root")
    ensure_private_subdirectory(runtime_root, lock_parent.name)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_dev != work_order["runtime"]["expected_device"]
        ):
            raise ProductionASRError("GPU lock inode failed ownership/device checks")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise GPUResourceBusy(
                f"GPU UUID {work_order['gpu']['expected_uuid']} is already locked"
            ) from error
        yield {
            "path": str(lock_path),
            "policy": GPU_LOCK_POLICY,
            "device": observed.st_dev,
            "inode": observed.st_ino,
            "held": True,
        }
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


class HardDeadline:
    """Kill the process without publishing if native inference exceeds its bound."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.cancel = threading.Event()
        self.thread = threading.Thread(
            target=self._watch, name="himr-production-gpu-asr-deadline", daemon=True
        )

    def _watch(self) -> None:
        if not self.cancel.wait(self.seconds):
            os._exit(124)

    def __enter__(self) -> "HardDeadline":
        self.thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.cancel.set()
        self.thread.join(timeout=2)


class NVMLSampler:
    def __init__(
        self, pynvml: Any, handle: Any, maximum_process_vram_bytes: int
    ) -> None:
        self.pynvml = pynvml
        self.handle = handle
        self.maximum_process_vram_bytes = maximum_process_vram_bytes
        self.stop_event = threading.Event()
        self.global_peak_bytes = 0
        self.process_peak_bytes = 0
        self.process_measurement_seen = False
        self.thread = threading.Thread(
            target=self._sample, name="himr-production-gpu-asr-nvml", daemon=True
        )

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            try:
                memory = self.pynvml.nvmlDeviceGetMemoryInfo(self.handle)
                self.global_peak_bytes = max(self.global_peak_bytes, int(memory.used))
                processes = self.pynvml.nvmlDeviceGetComputeRunningProcesses(self.handle)
                for process in processes:
                    used = getattr(process, "usedGpuMemory", None)
                    if process.pid != os.getpid() or not isinstance(used, int):
                        continue
                    if used < 0 or used >= 2**63:
                        continue
                    self.process_measurement_seen = True
                    self.process_peak_bytes = max(self.process_peak_bytes, used)
                    if used > self.maximum_process_vram_bytes:
                        os._exit(125)
            except self.pynvml.NVMLError:
                pass
            self.stop_event.wait(0.05)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)


def nvml_memory(pynvml: Any, handle: Any) -> dict[str, int]:
    memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
    return {
        "total_bytes": int(memory.total),
        "free_bytes": int(memory.free),
        "used_bytes": int(memory.used),
    }


def finite_optional(value: Any, label: str, minimum: float, maximum: float) -> float | None:
    if value is None:
        return None
    return finite_number(value, label, minimum, maximum)


def transcript_document(core: dict[str, Any], prefix: str) -> dict[str, Any]:
    identity = sha256_bytes(canonical_bytes(core))
    return {**core, "identity_sha256": identity, "document_id": f"{prefix}_{identity[:32]}"}


def raw_segment(segment: Any, ordinal: int) -> dict[str, Any]:
    start = finite_number(segment.start, f"segment[{ordinal}].start", 0, MAX_AUDIO_SECONDS + 10)
    end = finite_number(segment.end, f"segment[{ordinal}].end", 0, MAX_AUDIO_SECONDS + 10)
    if end < start:
        raise ProductionASRError(f"segment[{ordinal}] has inverted timestamps")
    text = str(segment.text)
    if len(text) > MAX_TEXT_CHARACTERS:
        raise ProductionASRError(f"segment[{ordinal}] text is unreasonably large")
    words: list[dict[str, Any]] = []
    for word_ordinal, word in enumerate(segment.words or []):
        word_start = finite_optional(
            getattr(word, "start", None),
            f"segment[{ordinal}].word[{word_ordinal}].start",
            0,
            MAX_AUDIO_SECONDS + 10,
        )
        word_end = finite_optional(
            getattr(word, "end", None),
            f"segment[{ordinal}].word[{word_ordinal}].end",
            0,
            MAX_AUDIO_SECONDS + 10,
        )
        if word_start is not None and word_end is not None and word_end < word_start:
            raise ProductionASRError(
                f"segment[{ordinal}].word[{word_ordinal}] has inverted timestamps"
            )
        words.append(
            {
                "ordinal": word_ordinal,
                "start_seconds": word_start,
                "end_seconds": word_end,
                "text": str(word.word),
                "probability_raw": finite_optional(
                    getattr(word, "probability", None),
                    f"segment[{ordinal}].word[{word_ordinal}].probability",
                    0,
                    1,
                ),
            }
        )
    tokens = list(getattr(segment, "tokens", []) or [])
    if any(isinstance(token, bool) or not isinstance(token, int) for token in tokens):
        raise ProductionASRError(f"segment[{ordinal}] token IDs are invalid")
    return {
        "ordinal": ordinal,
        "engine_segment_id": int(getattr(segment, "id", ordinal)),
        "seek": int(getattr(segment, "seek", 0)),
        "start_seconds": start,
        "end_seconds": end,
        "text": text,
        "token_ids": tokens,
        "temperature_raw": finite_optional(
            getattr(segment, "temperature", None),
            f"segment[{ordinal}].temperature",
            0,
            10,
        ),
        "average_log_probability_raw": finite_optional(
            getattr(segment, "avg_logprob", None),
            f"segment[{ordinal}].avg_logprob",
            -1_000,
            1_000,
        ),
        "compression_ratio_raw": finite_optional(
            getattr(segment, "compression_ratio", None),
            f"segment[{ordinal}].compression_ratio",
            0,
            1_000_000,
        ),
        "no_speech_probability_raw": finite_optional(
            getattr(segment, "no_speech_prob", None),
            f"segment[{ordinal}].no_speech_probability",
            0,
            1,
        ),
        "words": words,
    }


def build_raw_transcript(
    info: Any,
    segments: Sequence[dict[str, Any]],
    work_order: dict[str, Any],
) -> dict[str, Any]:
    language = bounded_text(str(info.language), "detected language", 64).lower()
    language_probability = finite_number(
        info.language_probability, "language_probability", 0, 1
    )
    all_language_probs: list[dict[str, Any]] = []
    for ordinal, item in enumerate(getattr(info, "all_language_probs", None) or []):
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise ProductionASRError("all_language_probs has an unexpected shape")
        all_language_probs.append(
            {
                "ordinal": ordinal,
                "language": bounded_text(str(item[0]), "language candidate", 64),
                "probability_raw": finite_number(
                    item[1], "language candidate probability", 0, 1
                ),
            }
        )
    core = {
        "kind": RAW_TRANSCRIPT_KIND,
        "schema_version": 1,
        "engine": {
            "library": "faster-whisper",
            "library_version": work_order["runtime"]["packages"]["faster-whisper"],
            "model_identity_sha256": work_order["model"]["identity_sha256"],
            "model_revision": work_order["model"]["revision"],
        },
        "input": {
            "sha256": work_order["input"]["expected_sha256"],
            "duration_ms": work_order["input"]["expected_duration_ms"],
            "timeline_offset_ms": work_order["input"]["timeline_offset_ms"],
        },
        "language": {
            "value": language,
            "probability_raw": language_probability,
            "all_probabilities_raw": all_language_probs,
        },
        "duration_seconds_raw": finite_number(
            getattr(info, "duration", work_order["input"]["expected_duration_ms"] / 1000),
            "transcription duration",
            0,
            MAX_AUDIO_SECONDS + 10,
        ),
        "duration_after_vad_seconds_raw": finite_optional(
            getattr(info, "duration_after_vad", None),
            "duration_after_vad",
            0,
            MAX_AUDIO_SECONDS + 10,
        ),
        "segments": list(segments),
        "score_semantics": "raw_model_outputs_uncalibrated",
        "policy": dict(POLICY),
    }
    return transcript_document(core, "gpuasrraw")


def _timestamp_ms(value: Any, label: str, duration_ms: int) -> tuple[int, bool]:
    seconds = finite_number(value, label, 0, MAX_AUDIO_SECONDS + 10)
    milliseconds = round(seconds * 1_000)
    if milliseconds > duration_ms + MAX_TIMESTAMP_OVERRUN_MS:
        raise ProductionASRError(f"{label} exceeds the admitted audio duration")
    clipped = milliseconds > duration_ms
    return min(milliseconds, duration_ms), clipped


def normalize_raw_transcript(
    raw: dict[str, Any], work_order: dict[str, Any]
) -> dict[str, Any]:
    duration_ms = work_order["input"]["expected_duration_ms"]
    timeline_offset = work_order["input"]["timeline_offset_ms"]
    raw_segments = raw.get("segments")
    if not isinstance(raw_segments, list):
        raise ProductionASRError("raw transcript segments must be an array")
    if len(raw_segments) > work_order["inference"]["max_segments"]:
        raise ProductionASRError("raw transcript exceeds inference.max_segments")
    segments: list[dict[str, Any]] = []
    word_count = 0
    previous_start = 0
    for ordinal, raw_segment_item in enumerate(raw_segments):
        if not isinstance(raw_segment_item, dict):
            raise ProductionASRError(f"raw segment {ordinal} must be an object")
        start_local, start_clipped = _timestamp_ms(
            raw_segment_item.get("start_seconds"),
            f"segment[{ordinal}].start_seconds",
            duration_ms,
        )
        end_local, end_clipped = _timestamp_ms(
            raw_segment_item.get("end_seconds"),
            f"segment[{ordinal}].end_seconds",
            duration_ms,
        )
        if end_local < start_local or start_local < previous_start:
            raise ProductionASRError("raw segment timing is inverted or non-monotonic")
        previous_start = start_local
        raw_words = raw_segment_item.get("words")
        if not isinstance(raw_words, list):
            raise ProductionASRError(f"raw segment {ordinal} words must be an array")
        words: list[dict[str, Any]] = []
        previous_word_start = start_local
        for word_ordinal, raw_word in enumerate(raw_words):
            if not isinstance(raw_word, dict):
                raise ProductionASRError("raw word must be an object")
            if raw_word.get("start_seconds") is None or raw_word.get("end_seconds") is None:
                local_start = None
                local_end = None
                timing_clipped = False
            else:
                local_start, word_start_clipped = _timestamp_ms(
                    raw_word["start_seconds"],
                    f"segment[{ordinal}].word[{word_ordinal}].start_seconds",
                    duration_ms,
                )
                local_end, word_end_clipped = _timestamp_ms(
                    raw_word["end_seconds"],
                    f"segment[{ordinal}].word[{word_ordinal}].end_seconds",
                    duration_ms,
                )
                if local_end < local_start or local_start < previous_word_start:
                    raise ProductionASRError("raw word timing is inverted or non-monotonic")
                previous_word_start = local_start
                timing_clipped = word_start_clipped or word_end_clipped
            probability = raw_word.get("probability_raw")
            if probability is not None:
                probability = finite_number(
                    probability,
                    f"segment[{ordinal}].word[{word_ordinal}].probability_raw",
                    0,
                    1,
                )
            words.append(
                {
                    "ordinal": word_ordinal,
                    "start_ms": None if local_start is None else timeline_offset + local_start,
                    "end_ms": None if local_end is None else timeline_offset + local_end,
                    "source_start_ms": local_start,
                    "source_end_ms": local_end,
                    "text": str(raw_word.get("text", "")),
                    "raw_probability": probability,
                    "calibrated_probability": None,
                    "timing_clipped_to_input": timing_clipped,
                }
            )
            word_count += 1
            if word_count > work_order["inference"]["max_words"]:
                raise ProductionASRError("raw transcript exceeds inference.max_words")
        raw_scores = {
            "temperature": raw_segment_item.get("temperature_raw"),
            "average_log_probability": raw_segment_item.get(
                "average_log_probability_raw"
            ),
            "compression_ratio": raw_segment_item.get("compression_ratio_raw"),
            "no_speech_probability": raw_segment_item.get(
                "no_speech_probability_raw"
            ),
        }
        for score_name, score_value in raw_scores.items():
            if score_value is not None:
                raw_scores[score_name] = finite_number(
                    score_value, f"segment[{ordinal}].{score_name}", -1_000, 1_000_000
                )
        segments.append(
            {
                "ordinal": ordinal,
                "start_ms": timeline_offset + start_local,
                "end_ms": timeline_offset + end_local,
                "source_start_ms": start_local,
                "source_end_ms": end_local,
                "text": str(raw_segment_item.get("text", "")),
                "words": words,
                "raw_scores": raw_scores,
                "calibrated_confidence": None,
                "timing_clipped_to_input": start_clipped or end_clipped,
                "speaker": None,
            }
        )
    language = raw.get("language")
    if not isinstance(language, dict):
        raise ProductionASRError("raw transcript language must be an object")
    language_probability = finite_number(
        language.get("probability_raw"), "raw language probability", 0, 1
    )
    core = {
        "kind": NORMALIZED_TRANSCRIPT_KIND,
        "schema_version": 1,
        "machine_generated": True,
        "human_reviewed": False,
        "verified_quotation": False,
        "scores_calibrated": False,
        "language": {
            "value": bounded_text(language.get("value"), "raw language value", 64),
            "raw_probability": language_probability,
            "calibrated_probability": None,
        },
        "timeline": {
            "coordinate_system": "recording_milliseconds",
            "source_duration_ms": duration_ms,
            "source_offset_ms": timeline_offset,
            "end_ms": timeline_offset + duration_ms,
        },
        "segments": segments,
        "segment_count": len(segments),
        "word_count": word_count,
        "confidence_notice": (
            "Raw faster-whisper scores are not calibrated probabilities and must not "
            "be presented as verified confidence."
        ),
        "identity_notice": "No speaker or person identity is inferred by this adapter.",
        "policy": dict(POLICY),
    }
    return transcript_document(core, "gpuasrnorm")


def serialize_transcripts(
    raw: dict[str, Any], normalized: dict[str, Any], maximum_bytes: int
) -> tuple[bytes, bytes]:
    raw_body = canonical_bytes(raw)
    normalized_body = canonical_bytes(normalized)
    if (
        len(raw_body) > maximum_bytes
        or len(normalized_body) > maximum_bytes
        or len(raw_body) + len(normalized_body) > maximum_bytes
    ):
        raise ProductionASRError(
            "raw and normalized transcripts exceed inference.max_result_bytes"
        )
    return raw_body, normalized_body


def write_new_private_file(path: Path, body: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o400)
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o400)
    finally:
        os.close(descriptor)


def sync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_directory_no_replace(staging: Path, final: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ProductionASRError(
            "atomic directory publication requires Linux renameat2(RENAME_NOREPLACE)"
        )
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        AT_FDCWD,
        os.fsencode(staging),
        AT_FDCWD,
        os.fsencode(final),
        RENAME_NOREPLACE,
    )
    if result == 0:
        sync_directory(final.parent)
        return
    error_number = ctypes.get_errno()
    if error_number in (errno.EEXIST, errno.ENOTEMPTY):
        raise FileExistsError(error_number, os.strerror(error_number), str(final))
    if error_number in (errno.ENOSYS, errno.EINVAL):
        raise ProductionASRError(
            "kernel/filesystem does not support fail-closed RENAME_NOREPLACE publication"
        )
    raise OSError(error_number, os.strerror(error_number), str(final))


def remove_staging_directory(path: Path) -> None:
    if not path.exists():
        return
    observed = path.lstat()
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise ProductionASRError("refusing to clean a non-directory staging path")
    os.chmod(path, 0o700)
    for child in path.iterdir():
        child_observed = child.lstat()
        if stat.S_ISLNK(child_observed.st_mode) or not stat.S_ISREG(child_observed.st_mode):
            raise ProductionASRError("refusing to clean an unsafe staging entry")
        os.chmod(child, 0o600)
    shutil.rmtree(path)


def result_artifact(
    kind: str, path: Path, body: bytes, processing_run_id: str
) -> dict[str, Any]:
    return {
        "artifact_id": stable_id(
            "artifact", [processing_run_id, kind, str(path), sha256_bytes(body)]
        ),
        "artifact_kind": kind,
        "processing_run_id": processing_run_id,
        "path": str(path),
        "storage_uri": path.as_uri(),
        "sha256": sha256_bytes(body),
        "byte_count": len(body),
        "mime_type": "application/json",
        "visibility": "private",
        "mode": "0400",
    }


def build_result(
    *,
    work_order: dict[str, Any],
    work_order_file: dict[str, Any],
    plan: dict[str, Any],
    static_evidence: dict[str, Any],
    raw: dict[str, Any],
    normalized: dict[str, Any],
    raw_body: bytes,
    normalized_body: bytes,
    hardware: dict[str, Any],
    runtime_evidence: dict[str, Any],
    lock_evidence: dict[str, Any],
    started_at: str,
    completed_at: str,
    duration_ms: int,
) -> dict[str, Any]:
    processing_run_id = f"run_asr_faster_whisper_gpu_{uuid.uuid4().hex}"
    raw_path = Path(plan["raw_transcript_path"])
    normalized_path = Path(plan["normalized_transcript_path"])
    artifacts = [
        result_artifact(
            "faster_whisper_raw_transcript_json", raw_path, raw_body, processing_run_id
        ),
        result_artifact(
            "transcript_normalized_json",
            normalized_path,
            normalized_body,
            processing_run_id,
        ),
    ]
    core = {
        "kind": RESULT_KIND,
        "schema_version": 1,
        "status": "completed",
        "result_key": plan["result_key"],
        "result_path": plan["result_path"],
        "work_order": work_order_file,
        "job_id": work_order["job_id"],
        "recipe_id": plan["recipe_id"],
        "recipe_sha256": plan["recipe_sha256"],
        "recipe": plan["recipe"],
        "processing_run": {
            "processing_run_id": processing_run_id,
            "stage": STAGE,
            "implementation_version": IMPLEMENTATION_VERSION,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": duration_ms,
            "status": "completed",
            "random_seed": None,
        },
        "input": static_evidence["input"],
        "model": static_evidence["model"],
        "runtime": runtime_evidence,
        "hardware": hardware,
        "gpu_lock": lock_evidence,
        "inference": {
            "library": "faster-whisper",
            "device": "cuda",
            "device_index": work_order["gpu"]["device_index"],
            "compute_type": work_order["gpu"]["compute_type"],
            "parameters": work_order["inference"],
            "model_local_files_only": True,
            "task": "transcribe",
        },
        "transcript": {
            "raw_identity_sha256": raw["identity_sha256"],
            "normalized_identity_sha256": normalized["identity_sha256"],
            "language": normalized["language"],
            "segment_count": normalized["segment_count"],
            "word_count": normalized["word_count"],
            "scores_calibrated": False,
            "human_reviewed": False,
        },
        "artifacts": artifacts,
        "catalog_context": work_order["catalog_context"],
        "commands": static_evidence["commands"],
        "errors": [],
        "policy": dict(POLICY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "identity_sha256": identity,
        "result_id": f"gpuasrresult_{identity[:32]}",
    }


def publish_result(
    work_order: dict[str, Any],
    plan: dict[str, Any],
    raw_body: bytes,
    normalized_body: bytes,
    result: dict[str, Any],
) -> dict[str, Any]:
    result_body = canonical_bytes(result)
    maximum = work_order["inference"]["max_result_bytes"]
    if len(result_body) > maximum or len(raw_body) + len(normalized_body) + len(result_body) > maximum:
        raise ProductionASRError("result envelope exceeds inference.max_result_bytes")
    parent = ensure_result_parent(work_order)
    final = Path(plan["result_dir"])
    if final.exists() or final.is_symlink():
        return validate_completed_result(work_order, plan)
    staging = parent / f".{plan['result_key']}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    staging.mkdir(mode=0o700)
    try:
        write_new_private_file(staging / "transcript.raw.json", raw_body)
        write_new_private_file(staging / "transcript.normalized.json", normalized_body)
        write_new_private_file(staging / "result.json", result_body)
        sync_directory(staging)
        os.chmod(staging, 0o500)
        try:
            publish_directory_no_replace(staging, final)
        except FileExistsError:
            remove_staging_directory(staging)
            return validate_completed_result(work_order, plan)
        return validate_completed_result(work_order, plan)
    finally:
        if staging.exists():
            remove_staging_directory(staging)


def _validate_identity_document(
    value: Any, label: str, *, id_prefix: str
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProductionASRError(f"{label} must be an object")
    identity = value.get("identity_sha256")
    document_id = value.get("document_id")
    if not isinstance(identity, str) or not SHA256_RE.fullmatch(identity):
        raise ProductionASRError(f"{label} identity_sha256 is invalid")
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", "document_id"}}
    expected = sha256_bytes(canonical_bytes(core))
    if identity != expected or document_id != f"{id_prefix}_{expected[:32]}":
        raise ProductionASRError(f"{label} immutable identity failed replay")
    return value


def validate_completed_result(
    work_order: dict[str, Any], plan: dict[str, Any]
) -> dict[str, Any]:
    run_dir = Path(plan["result_dir"])
    observed_dir = run_dir.lstat()
    if (
        stat.S_ISLNK(observed_dir.st_mode)
        or not stat.S_ISDIR(observed_dir.st_mode)
        or stat.S_IMODE(observed_dir.st_mode) != 0o500
        or observed_dir.st_uid != os.getuid()
    ):
        raise ProductionASRError("existing result directory is not sealed mode 0500")
    expected_names = {
        "result.json",
        "transcript.raw.json",
        "transcript.normalized.json",
    }
    if {child.name for child in run_dir.iterdir()} != expected_names:
        raise ProductionASRError("existing result has missing or unexpected entries")
    bodies: dict[str, bytes] = {}
    maximum = work_order["inference"]["max_result_bytes"]
    for name in sorted(expected_names):
        bodies[name] = stable_file_bytes(
            run_dir / name,
            label=f"existing {name}",
            maximum_bytes=maximum,
            exact_mode=0o400,
            single_link=True,
        )
    raw = _validate_identity_document(
        parse_json_bytes(bodies["transcript.raw.json"], "raw transcript"),
        "raw transcript",
        id_prefix="gpuasrraw",
    )
    normalized = _validate_identity_document(
        parse_json_bytes(
            bodies["transcript.normalized.json"], "normalized transcript"
        ),
        "normalized transcript",
        id_prefix="gpuasrnorm",
    )
    if bodies["transcript.raw.json"] != canonical_bytes(raw):
        raise ProductionASRError("raw transcript is not canonical JSON")
    if bodies["transcript.normalized.json"] != canonical_bytes(normalized):
        raise ProductionASRError("normalized transcript is not canonical JSON")
    regenerated = normalize_raw_transcript(raw, work_order)
    if regenerated != normalized:
        raise ProductionASRError("normalized transcript does not replay from raw transcript")
    result = parse_json_bytes(bodies["result.json"], "result envelope")
    if not isinstance(result, dict):
        raise ProductionASRError("result envelope must be an object")
    identity = result.get("identity_sha256")
    result_id = result.get("result_id")
    if not isinstance(identity, str) or not SHA256_RE.fullmatch(identity):
        raise ProductionASRError("result identity_sha256 is invalid")
    core = {key: item for key, item in result.items() if key not in {"identity_sha256", "result_id"}}
    expected_identity = sha256_bytes(canonical_bytes(core))
    if identity != expected_identity or result_id != f"gpuasrresult_{expected_identity[:32]}":
        raise ProductionASRError("result envelope immutable identity failed replay")
    if bodies["result.json"] != canonical_bytes(result):
        raise ProductionASRError("result envelope is not canonical JSON")
    if (
        result.get("kind") != RESULT_KIND
        or result.get("schema_version") != 1
        or result.get("status") != "completed"
        or result.get("result_key") != plan["result_key"]
        or result.get("result_path") != plan["result_path"]
        or result.get("recipe_id") != plan["recipe_id"]
        or result.get("recipe_sha256") != plan["recipe_sha256"]
        or result.get("recipe") != plan["recipe"]
        or result.get("job_id") != work_order["job_id"]
        or result.get("policy") != POLICY
    ):
        raise ProductionASRError("result envelope lineage or policy failed replay")
    work_order_reference = result.get("work_order")
    if (
        not isinstance(work_order_reference, dict)
        or work_order_reference.get("identity_sha256") != work_order["identity_sha256"]
        or work_order_reference.get("work_order_id") != work_order["work_order_id"]
    ):
        raise ProductionASRError("result work-order reference failed replay")
    expected_artifacts = {
        "faster_whisper_raw_transcript_json": (
            Path(plan["raw_transcript_path"]),
            bodies["transcript.raw.json"],
        ),
        "transcript_normalized_json": (
            Path(plan["normalized_transcript_path"]),
            bodies["transcript.normalized.json"],
        ),
    }
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ProductionASRError("result artifact list is invalid")
    for artifact in artifacts:
        if not isinstance(artifact, dict) or artifact.get("artifact_kind") not in expected_artifacts:
            raise ProductionASRError("result artifact entry is invalid")
        expected_path, expected_body = expected_artifacts[artifact["artifact_kind"]]
        if (
            artifact.get("path") != str(expected_path)
            or artifact.get("storage_uri") != expected_path.as_uri()
            or artifact.get("sha256") != sha256_bytes(expected_body)
            or artifact.get("byte_count") != len(expected_body)
            or artifact.get("visibility") != "private"
            or artifact.get("mode") != "0400"
        ):
            raise ProductionASRError("result artifact hash/path failed replay")
    transcript = result.get("transcript")
    if (
        not isinstance(transcript, dict)
        or transcript.get("raw_identity_sha256") != raw["identity_sha256"]
        or transcript.get("normalized_identity_sha256")
        != normalized["identity_sha256"]
        or transcript.get("segment_count") != normalized["segment_count"]
        or transcript.get("word_count") != normalized["word_count"]
        or transcript.get("scores_calibrated") is not False
        or transcript.get("human_reviewed") is not False
    ):
        raise ProductionASRError("result transcript summary failed replay")
    return result


def completed_or_pending(work_order: dict[str, Any], plan: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    run_dir = Path(plan["result_dir"])
    result_path = Path(plan["result_path"])
    if result_path.is_file():
        return "completed", validate_completed_result(work_order, plan)
    if run_dir.exists() or run_dir.is_symlink():
        raise ProductionASRError(
            "immutable result path exists without a reusable completed result"
        )
    return "pending", None


def dry_run_plan(
    work_order: dict[str, Any], work_order_file: dict[str, Any]
) -> dict[str, Any]:
    static = replay_static_bindings(work_order, require_current_runtime=False)
    plan = result_plan(work_order)
    state, result = completed_or_pending(work_order, plan)
    return {
        "kind": "himr_faster_whisper_gpu_dry_run",
        "schema_version": 1,
        "status": "completed" if state == "completed" else "planned",
        "dry_run": True,
        "work_order": work_order_file,
        "result": plan,
        "result_state": state,
        "existing_result_identity_sha256": None
        if result is None
        else result["identity_sha256"],
        "bindings": static,
        "gpu": {
            "expected_uuid": work_order["gpu"]["expected_uuid"],
            "device_index": work_order["gpu"]["device_index"],
            "compute_type": work_order["gpu"]["compute_type"],
            "lock_path": work_order["gpu"]["lock_path"],
            "lock_state": lock_state(Path(work_order["gpu"]["lock_path"])),
            "queried": False,
        },
        "inference_executed": False,
        "files_written": False,
        "policy": dict(POLICY),
    }


def run_inference(
    work_order: dict[str, Any],
    work_order_file: dict[str, Any],
    expected_parent_network_namespace: str,
) -> dict[str, Any]:
    if not sys.flags.isolated:
        raise ProductionASRError("production GPU run requires Python isolated mode (-I)")
    if os.environ.get("CUDA_VISIBLE_DEVICES") not in (None, "", "0"):
        raise ProductionASRError(
            "CUDA_VISIBLE_DEVICES must be unset, empty, or 0 for physical UUID binding"
        )
    ensure_offline_environment()
    plan = result_plan(work_order)
    state, existing = completed_or_pending(work_order, plan)
    if state == "completed" and existing is not None:
        return existing
    started_at = utc_now()
    started_clock = time.monotonic()
    with gpu_advisory_lock(work_order) as lock_evidence:
        state, existing = completed_or_pending(work_order, plan)
        if state == "completed" and existing is not None:
            return existing
        with HardDeadline(work_order["inference"]["max_wall_seconds"]):
            static = replay_static_bindings(work_order, require_current_runtime=True)
            network = network_isolation_evidence(expected_parent_network_namespace)
            with retained_verified_input(work_order["input"]) as (
                _input_descriptor,
                retained_path,
                input_observation,
            ):
                retained_probe, retained_probe_command = probe_audio(
                    work_order,
                    retained_path,
                    pass_fds=(_input_descriptor,),
                )
                if retained_probe != static["input"]["probe"]:
                    raise ProductionASRError(
                        "retained input probe differs from the static preflight"
                    )
                import ctranslate2
                import pynvml
                from faster_whisper import WhisperModel

                pynvml.nvmlInit()
                sampler: NVMLSampler | None = None
                try:
                    device_count = ctranslate2.get_cuda_device_count()
                    if device_count < 1:
                        raise ProductionASRError("CTranslate2 reports no CUDA device")
                    index = work_order["gpu"]["device_index"]
                    handle = pynvml.nvmlDeviceGetHandleByIndex(index)
                    observed_uuid = pynvml.nvmlDeviceGetUUID(handle)
                    if observed_uuid != work_order["gpu"]["expected_uuid"]:
                        raise ProductionASRError(
                            "NVML device UUID differs from gpu.expected_uuid"
                        )
                    supported = sorted(
                        ctranslate2.get_supported_compute_types("cuda", index)
                    )
                    if work_order["gpu"]["compute_type"] not in supported:
                        raise ProductionASRError(
                            "requested GPU compute type is unsupported on this device"
                        )
                    memory_before = nvml_memory(pynvml, handle)
                    if (
                        memory_before["free_bytes"]
                        < work_order["gpu"]["minimum_free_vram_bytes"]
                    ):
                        raise ProductionASRError(
                            "free VRAM is below gpu.minimum_free_vram_bytes"
                        )
                    if (
                        work_order["gpu"]["maximum_process_vram_bytes"]
                        > memory_before["total_bytes"]
                    ):
                        raise ProductionASRError(
                            "gpu.maximum_process_vram_bytes exceeds physical VRAM"
                        )
                    sampler = NVMLSampler(
                        pynvml,
                        handle,
                        work_order["gpu"]["maximum_process_vram_bytes"],
                    )
                    sampler.start()
                    model_load_start = time.monotonic()
                    model = WhisperModel(
                        work_order["model"]["snapshot_root"],
                        device="cuda",
                        device_index=index,
                        compute_type=work_order["gpu"]["compute_type"],
                        local_files_only=True,
                        cpu_threads=work_order["inference"]["cpu_threads"],
                        num_workers=work_order["inference"]["num_workers"],
                    )
                    model_load_seconds = time.monotonic() - model_load_start
                    inference_start = time.monotonic()
                    iterator, info = model.transcribe(
                        retained_path,
                        task="transcribe",
                        language=None
                        if work_order["inference"]["language"] == "auto"
                        else work_order["inference"]["language"],
                        beam_size=work_order["inference"]["beam_size"],
                        best_of=work_order["inference"]["best_of"],
                        temperature=work_order["inference"]["temperature"],
                        word_timestamps=True,
                        vad_filter=False,
                        condition_on_previous_text=False,
                        without_timestamps=False,
                    )
                    segments: list[dict[str, Any]] = []
                    word_count = 0
                    for ordinal, segment in enumerate(iterator):
                        if ordinal >= work_order["inference"]["max_segments"]:
                            raise ProductionASRError(
                                "inference exceeded inference.max_segments"
                            )
                        item = raw_segment(segment, ordinal)
                        word_count += len(item["words"])
                        if word_count > work_order["inference"]["max_words"]:
                            raise ProductionASRError(
                                "inference exceeded inference.max_words"
                            )
                        segments.append(item)
                    inference_seconds = time.monotonic() - inference_start
                    del model
                    gc.collect()
                    sampler.stop()
                    memory_after = nvml_memory(pynvml, handle)
                    if not sampler.process_measurement_seen:
                        raise ProductionASRError(
                            "NVML did not observe this process; process VRAM bound is unproven"
                        )
                    if (
                        sampler.process_peak_bytes
                        > work_order["gpu"]["maximum_process_vram_bytes"]
                    ):
                        raise ProductionASRError(
                            "sampled process VRAM exceeded its admitted maximum"
                        )
                    hardware = {
                        "device_index": index,
                        "name": pynvml.nvmlDeviceGetName(handle),
                        "uuid": observed_uuid,
                        "driver_version": pynvml.nvmlSystemGetDriverVersion(),
                        "cuda_driver_version": pynvml.nvmlSystemGetCudaDriverVersion_v2(),
                        "compute_capability": list(
                            pynvml.nvmlDeviceGetCudaComputeCapability(handle)
                        ),
                        "ctranslate2_cuda_device_count": device_count,
                        "ctranslate2_supported_compute_types": supported,
                        "memory_before": memory_before,
                        "memory_after": memory_after,
                        "global_peak_used_bytes": sampler.global_peak_bytes,
                        "process_peak_used_bytes": sampler.process_peak_bytes,
                        "process_vram_measurement_seen": True,
                    }
                finally:
                    if sampler is not None:
                        sampler.stop()
                    pynvml.nvmlShutdown()
                raw = build_raw_transcript(info, segments, work_order)
                normalized = normalize_raw_transcript(raw, work_order)
                raw_body, normalized_body = serialize_transcripts(
                    raw,
                    normalized,
                    work_order["inference"]["max_result_bytes"],
                )
                post_model = replay_model_binding(work_order)
                post_runtime = replay_runtime_binding(
                    work_order, require_current=True
                )
                if post_model != static["model"] or post_runtime != static["runtime"]:
                    raise ProductionASRError(
                        "model or runtime binding changed during GPU ASR"
                    )
                runtime_evidence = {
                    **static["runtime"],
                    "platform": platform.platform(),
                    "python": sys.version,
                    "python_isolated_mode": bool(sys.flags.isolated),
                    "network": network,
                    "offline_environment": {
                        name: os.environ.get(name)
                        for name in (
                            "HF_HUB_OFFLINE",
                            "HF_DATASETS_OFFLINE",
                            "HF_HUB_DISABLE_TELEMETRY",
                            "DO_NOT_TRACK",
                        )
                    },
                    "model_load_seconds": model_load_seconds,
                    "inference_seconds": inference_seconds,
                    "inference_real_time_factor": inference_seconds
                    / (work_order["input"]["expected_duration_ms"] / 1_000),
                }
                static["input"] = input_observation | {"probe": retained_probe}
                static["commands"] = [retained_probe_command]
                completed_at = utc_now()
                result = build_result(
                    work_order=work_order,
                    work_order_file=work_order_file,
                    plan=plan,
                    static_evidence=static,
                    raw=raw,
                    normalized=normalized,
                    raw_body=raw_body,
                    normalized_body=normalized_body,
                    hardware=hardware,
                    runtime_evidence=runtime_evidence,
                    lock_evidence=lock_evidence,
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_ms=round((time.monotonic() - started_clock) * 1_000),
                )
                return publish_result(
                    work_order,
                    plan,
                    raw_body,
                    normalized_body,
                    result,
                )


def status_document(
    work_order: dict[str, Any], work_order_file: dict[str, Any]
) -> dict[str, Any]:
    plan = result_plan(work_order)
    state, result = completed_or_pending(work_order, plan)
    return {
        "kind": "himr_faster_whisper_gpu_status",
        "schema_version": 1,
        "status": "completed",
        "work_order": work_order_file,
        "state": state,
        "result_key": plan["result_key"],
        "result_path": plan["result_path"],
        "result_identity_sha256": None
        if result is None
        else result["identity_sha256"],
        "gpu_lock": {
            "path": work_order["gpu"]["lock_path"],
            "state": lock_state(Path(work_order["gpu"]["lock_path"])),
        },
        "inference_executed": False,
        "files_written": False,
        "policy": dict(POLICY),
    }


def ensure_private_result_parent(path: Path) -> None:
    parent = existing_private_directory(str(path.parent), "--output parent", mode=0o700)
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace work order: {path}")
    if parent != path.parent:
        raise ProductionASRError("--output parent is not normalized")


def write_exclusive_canonical(path: Path, value: Any) -> None:
    if not path.is_absolute() or Path(os.path.normpath(str(path))) != path:
        raise ProductionASRError("--output must be absolute and normalized")
    ensure_private_result_parent(path)
    body = canonical_bytes(value)
    parent_descriptor = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o400,
            dir_fd=parent_descriptor,
        )
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o400)
        os.link(
            temporary_name,
            path.name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        os.unlink(temporary_name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except FileNotFoundError:
            pass
        os.close(parent_descriptor)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "contracts", help="emit canonical normative work-order/result contract identities"
    )
    create = commands.add_parser(
        "create-work-order",
        help="canonicalize an exact work-order core and add its immutable identity",
    )
    create.add_argument("--spec", required=True)
    create.add_argument("--output")
    validate = commands.add_parser(
        "validate", help="replay a canonical work order and every static binding"
    )
    validate.add_argument("--work-order", required=True)
    dry_run = commands.add_parser(
        "dry-run", help="perform a read-only production preflight without CUDA inference"
    )
    dry_run.add_argument("--work-order", required=True)
    run = commands.add_parser(
        "run", help="execute or exactly reuse one private production GPU ASR result"
    )
    run.add_argument("--work-order", required=True)
    run.add_argument("--expected-parent-network-namespace", required=True)
    status = commands.add_parser(
        "status", help="read-only exact result and GPU-lock status"
    )
    status.add_argument("--work-order", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    job_id: str | None = None
    try:
        if args.command == "contracts":
            sys.stdout.buffer.write(canonical_bytes(contract_document()))
            return 0
        if args.command == "create-work-order":
            work_order = make_work_order(load_creation_spec(args.spec))
            if args.output is None:
                sys.stdout.buffer.write(canonical_bytes(work_order))
            else:
                output = normalized_absolute_path(args.output, "--output")
                write_exclusive_canonical(output, work_order)
                response = {
                    "schema_version": 1,
                    "status": "created",
                    "path": str(output),
                    "sha256": sha256_bytes(canonical_bytes(work_order)),
                    "identity_sha256": work_order["identity_sha256"],
                    "work_order_id": work_order["work_order_id"],
                }
                sys.stdout.buffer.write(canonical_bytes(response))
            return 0

        work_order, work_order_file = load_work_order(args.work_order)
        job_id = work_order["job_id"]
        if args.command == "validate":
            bindings = replay_static_bindings(
                work_order, require_current_runtime=False
            )
            response = {
                "kind": "himr_faster_whisper_gpu_validation",
                "schema_version": 1,
                "status": "validated",
                "work_order": work_order_file,
                "result": result_plan(work_order),
                "bindings": bindings,
                "inference_executed": False,
                "files_written": False,
                "policy": dict(POLICY),
            }
        elif args.command == "dry-run":
            response = dry_run_plan(work_order, work_order_file)
        elif args.command == "status":
            response = status_document(work_order, work_order_file)
        elif args.command == "run":
            ensure_cuda_wheel_libraries()
            response = run_inference(
                work_order,
                work_order_file,
                args.expected_parent_network_namespace,
            )
        else:  # pragma: no cover - argparse is exhaustive.
            raise ProductionASRError(f"unsupported command: {args.command}")
        sys.stdout.buffer.write(canonical_bytes(response))
        return 0
    except (
        ProductionASRError,
        FileExistsError,
        OSError,
        subprocess.SubprocessError,
        ValueError,
    ) as error:
        failure = {
            "kind": "himr_faster_whisper_gpu_failure",
            "schema_version": 1,
            "status": "failed",
            "job_id": job_id,
            "command": args.command,
            "error": {"type": type(error).__name__, "message": str(error)},
            "files_published": False,
            "policy": dict(POLICY),
        }
        sys.stderr.buffer.write(canonical_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
