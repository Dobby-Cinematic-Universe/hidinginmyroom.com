#!/usr/bin/env python3
"""Canonical complete resource profile for the restart-portable GPU lane.

The historical GPU admission compared decoding parameters and two VRAM values but
did not bind the remaining per-item and batch ceilings.  This successor profile is
one immutable value shared by runtime admission, queue construction, batch
materialization, the trusted launcher, and result receipts.  No consumer may widen
an individual field while claiming this profile identity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import secrets
import stat
import sys
from pathlib import Path
from typing import Any


KIND = "himr_gpu_production_profile"
SCHEMA_VERSION = 2
IMPLEMENTATION_VERSION = "0.2.1"
PROFILE_NAME = "small_en_fp16_beam5_portable_v2"
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
GPU_UUID_RE = re.compile(r"GPU-[A-Za-z0-9-]{8,92}\Z")


class ProfileError(ValueError):
    """A production profile is malformed or outside its closed envelope."""


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


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise ProfileError(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ProfileError(f"{label} must be an integer within [{minimum}, {maximum}]")
    return value


def _number(value: Any, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProfileError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ProfileError(f"{label} must be finite within [{minimum}, {maximum}]")
    return result


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ProfileError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _clean_text(value: Any, label: str, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ProfileError(f"{label} must be bounded nonempty text")
    return value


PROFILE_CORE_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "name",
    "model",
    "hardware",
    "decoding",
    "item_limits",
    "batch_limits",
    "telemetry",
    "scheduler",
    "safety",
}


def normalize_core(value: Any) -> dict[str, Any]:
    item = _exact(value, "production profile", PROFILE_CORE_KEYS)
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["name"] != PROFILE_NAME
    ):
        raise ProfileError("production profile header is unsupported")

    model = _exact(
        item["model"],
        "model",
        {"repository", "revision", "identity_sha256", "license"},
    )
    normalized_model = {
        "repository": _clean_text(model["repository"], "model.repository", 1_000),
        "revision": _clean_text(model["revision"], "model.revision"),
        "identity_sha256": _digest(model["identity_sha256"], "model.identity_sha256"),
        "license": _clean_text(model["license"], "model.license"),
    }

    hardware = _exact(
        item["hardware"],
        "hardware",
        {
            "gpu_uuid",
            "device_index",
            "compute_type",
            "minimum_driver_version",
            "minimum_compute_capability",
        },
    )
    gpu_uuid = _clean_text(hardware["gpu_uuid"], "hardware.gpu_uuid", 96)
    if not GPU_UUID_RE.fullmatch(gpu_uuid):
        raise ProfileError("hardware.gpu_uuid is not an NVIDIA GPU UUID")
    capability = hardware["minimum_compute_capability"]
    if (
        not isinstance(capability, list)
        or len(capability) != 2
        or any(isinstance(part, bool) or not isinstance(part, int) for part in capability)
        or not 1 <= capability[0] <= 99
        or not 0 <= capability[1] <= 99
    ):
        raise ProfileError("hardware.minimum_compute_capability is invalid")
    normalized_hardware = {
        "gpu_uuid": gpu_uuid,
        "device_index": _integer(hardware["device_index"], "hardware.device_index", 0, 0),
        "compute_type": _clean_text(hardware["compute_type"], "hardware.compute_type", 32),
        "minimum_driver_version": _clean_text(
            hardware["minimum_driver_version"], "hardware.minimum_driver_version", 64
        ),
        "minimum_compute_capability": capability,
    }
    if normalized_hardware["compute_type"] != "float16":
        raise ProfileError("profile v2 admits only float16")

    decoding = _exact(
        item["decoding"],
        "decoding",
        {
            "language",
            "beam_size",
            "best_of",
            "temperature",
            "condition_on_previous_text",
            "word_timestamps",
            "vad_filter",
            "cpu_threads",
            "num_workers",
            "neural_batching",
        },
    )
    normalized_decoding = {
        "language": _clean_text(decoding["language"], "decoding.language", 32),
        "beam_size": _integer(decoding["beam_size"], "decoding.beam_size", 1, 20),
        "best_of": _integer(decoding["best_of"], "decoding.best_of", 1, 20),
        "temperature": _number(decoding["temperature"], "decoding.temperature", 0, 1),
        "condition_on_previous_text": decoding["condition_on_previous_text"],
        "word_timestamps": decoding["word_timestamps"],
        "vad_filter": decoding["vad_filter"],
        "cpu_threads": _integer(decoding["cpu_threads"], "decoding.cpu_threads", 1, 16),
        "num_workers": _integer(decoding["num_workers"], "decoding.num_workers", 1, 4),
        "neural_batching": decoding["neural_batching"],
    }
    expected_decoding = {
        "language": "en",
        "beam_size": 5,
        "best_of": 5,
        "temperature": 0.0,
        "condition_on_previous_text": False,
        "word_timestamps": True,
        "vad_filter": False,
        "cpu_threads": 4,
        "num_workers": 2,
        "neural_batching": False,
    }
    if normalized_decoding != expected_decoding:
        raise ProfileError("decoding differs from the exact v2 control profile")

    item_limits = _exact(
        item["item_limits"],
        "item_limits",
        {
            "maximum_audio_bytes",
            "maximum_audio_seconds",
            "maximum_wall_seconds",
            "maximum_result_bytes",
            "maximum_segments",
            "maximum_words",
        },
    )
    normalized_item_limits = {
        "maximum_audio_bytes": _integer(
            item_limits["maximum_audio_bytes"], "item_limits.maximum_audio_bytes", 1, 2**30
        ),
        "maximum_audio_seconds": _number(
            item_limits["maximum_audio_seconds"], "item_limits.maximum_audio_seconds", 0.001, 86_400
        ),
        "maximum_wall_seconds": _number(
            item_limits["maximum_wall_seconds"], "item_limits.maximum_wall_seconds", 1, 86_400
        ),
        "maximum_result_bytes": _integer(
            item_limits["maximum_result_bytes"], "item_limits.maximum_result_bytes", 1, 2**30
        ),
        "maximum_segments": _integer(
            item_limits["maximum_segments"], "item_limits.maximum_segments", 1, 1_000_000
        ),
        "maximum_words": _integer(
            item_limits["maximum_words"], "item_limits.maximum_words", 1, 10_000_000
        ),
    }

    batch_limits = _exact(
        item["batch_limits"],
        "batch_limits",
        {
            "maximum_items",
            "maximum_total_audio_ms",
            "preferred_total_audio_ms",
            "maximum_wall_seconds",
            "model_load_count",
            "inference_concurrency",
            "ready_batch_high_water",
        },
    )
    normalized_batch_limits = {
        "maximum_items": _integer(batch_limits["maximum_items"], "batch_limits.maximum_items", 1, 32),
        "maximum_total_audio_ms": _integer(
            batch_limits["maximum_total_audio_ms"], "batch_limits.maximum_total_audio_ms", 1, 12 * 60 * 60 * 1_000
        ),
        "preferred_total_audio_ms": _integer(
            batch_limits["preferred_total_audio_ms"], "batch_limits.preferred_total_audio_ms", 1, 12 * 60 * 60 * 1_000
        ),
        "maximum_wall_seconds": _integer(
            batch_limits["maximum_wall_seconds"], "batch_limits.maximum_wall_seconds", 1, 24 * 60 * 60
        ),
        "model_load_count": _integer(batch_limits["model_load_count"], "batch_limits.model_load_count", 1, 1),
        "inference_concurrency": _integer(
            batch_limits["inference_concurrency"], "batch_limits.inference_concurrency", 2, 2
        ),
        "ready_batch_high_water": _integer(
            batch_limits["ready_batch_high_water"], "batch_limits.ready_batch_high_water", 1, 2
        ),
    }
    if normalized_batch_limits["preferred_total_audio_ms"] > normalized_batch_limits["maximum_total_audio_ms"]:
        raise ProfileError("preferred batch audio exceeds the hard batch ceiling")

    telemetry = _exact(
        item["telemetry"],
        "telemetry",
        {
            "fast_interval_ms",
            "slow_interval_ms",
            "maximum_process_vram_bytes",
            "minimum_free_vram_bytes",
            "maximum_temperature_c",
            "maximum_heartbeat_age_ms",
        },
    )
    normalized_telemetry = {
        "fast_interval_ms": _integer(telemetry["fast_interval_ms"], "telemetry.fast_interval_ms", 20, 1_000),
        "slow_interval_ms": _integer(telemetry["slow_interval_ms"], "telemetry.slow_interval_ms", 50, 5_000),
        "maximum_process_vram_bytes": _integer(
            telemetry["maximum_process_vram_bytes"], "telemetry.maximum_process_vram_bytes", 1, 6 * 1024**3
        ),
        "minimum_free_vram_bytes": _integer(
            telemetry["minimum_free_vram_bytes"], "telemetry.minimum_free_vram_bytes", 1, 6 * 1024**3
        ),
        "maximum_temperature_c": _integer(
            telemetry["maximum_temperature_c"], "telemetry.maximum_temperature_c", 1, 100
        ),
        "maximum_heartbeat_age_ms": _integer(
            telemetry["maximum_heartbeat_age_ms"], "telemetry.maximum_heartbeat_age_ms", 50, 10_000
        ),
    }

    scheduler = _exact(
        item["scheduler"],
        "scheduler",
        {
            "gpu_processes",
            "prefetch_workers",
            "prefetch_depth",
            "other_cuda_jobs",
            "gpu_lock_policy",
            "batch_lock_policy",
        },
    )
    normalized_scheduler = {
        "gpu_processes": _integer(scheduler["gpu_processes"], "scheduler.gpu_processes", 1, 1),
        "prefetch_workers": _integer(scheduler["prefetch_workers"], "scheduler.prefetch_workers", 0, 1),
        "prefetch_depth": _integer(scheduler["prefetch_depth"], "scheduler.prefetch_depth", 0, 1),
        "other_cuda_jobs": _integer(scheduler["other_cuda_jobs"], "scheduler.other_cuda_jobs", 0, 0),
        "gpu_lock_policy": _clean_text(scheduler["gpu_lock_policy"], "scheduler.gpu_lock_policy", 128),
        "batch_lock_policy": _clean_text(scheduler["batch_lock_policy"], "scheduler.batch_lock_policy", 128),
    }
    if normalized_scheduler["gpu_lock_policy"] != "linux_flock_exclusive_nonblocking_gpu_uuid_v2":
        raise ProfileError("unsupported GPU lock policy")
    if normalized_scheduler["batch_lock_policy"] != "linux_flock_exclusive_nonblocking_batch_materialize_v2":
        raise ProfileError("unsupported batch lock policy")
    if (
        normalized_scheduler["prefetch_workers"] != 0
        or normalized_scheduler["prefetch_depth"] != 0
    ):
        raise ProfileError("profile v2 does not admit unimplemented prefetching")

    safety = _exact(
        item["safety"],
        "safety",
        {
            "visibility",
            "network_access",
            "publication_authority",
            "catalogue_mutation_authority",
            "identity_authority",
            "biometric_authority",
            "wiki_authority",
            "archive_authority",
            "deletion_authority",
        },
    )
    expected_safety = {
        "visibility": "private",
        "network_access": False,
        "publication_authority": "none",
        "catalogue_mutation_authority": "none",
        "identity_authority": "none",
        "biometric_authority": "none",
        "wiki_authority": "none",
        "archive_authority": "none",
        "deletion_authority": "none",
    }
    if safety != expected_safety:
        raise ProfileError("production safety policy is not exact")

    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "name": PROFILE_NAME,
        "model": normalized_model,
        "hardware": normalized_hardware,
        "decoding": normalized_decoding,
        "item_limits": normalized_item_limits,
        "batch_limits": normalized_batch_limits,
        "telemetry": normalized_telemetry,
        "scheduler": normalized_scheduler,
        "safety": expected_safety,
    }


def make_profile(core: Any) -> dict[str, Any]:
    normalized = normalize_core(core)
    identity = sha256_bytes(canonical_bytes(normalized))
    return {
        **normalized,
        "identity_sha256": identity,
        "profile_id": f"gpuprofile_{identity[:32]}",
    }


def validate_profile(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "production profile",
        PROFILE_CORE_KEYS | {"identity_sha256", "profile_id"},
    )
    expected = make_profile({key: item[key] for key in PROFILE_CORE_KEYS})
    if item != expected:
        raise ProfileError("production profile is noncanonical or its identity is invalid")
    return expected


def default_profile() -> dict[str, Any]:
    return make_profile(
        {
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "implementation_version": IMPLEMENTATION_VERSION,
            "name": PROFILE_NAME,
            "model": {
                "repository": "Systran/faster-whisper-small.en",
                "revision": "d1d751a5f8271d482d14ca55d9e2deeebbae577f",
                "identity_sha256": "d1fb86a9ba89db472106c9f171aaf96a29262b7bdabdc7f04804cf65c581c143",
                "license": "mit",
            },
            "hardware": {
                "gpu_uuid": "GPU-0b9d7029-b3c6-1a0b-d483-6a3febc3b557",
                "device_index": 0,
                "compute_type": "float16",
                "minimum_driver_version": "610.57.04",
                "minimum_compute_capability": [8, 6],
            },
            "decoding": {
                "language": "en",
                "beam_size": 5,
                "best_of": 5,
                "temperature": 0.0,
                "condition_on_previous_text": False,
                "word_timestamps": True,
                "vad_filter": False,
                "cpu_threads": 4,
                "num_workers": 2,
                "neural_batching": False,
            },
            "item_limits": {
                "maximum_audio_bytes": 8 * 1024**2,
                "maximum_audio_seconds": 420.0,
                "maximum_wall_seconds": 900.0,
                "maximum_result_bytes": 16 * 1024**2,
                "maximum_segments": 4_096,
                "maximum_words": 32_768,
            },
            "batch_limits": {
                "maximum_items": 32,
                "maximum_total_audio_ms": 12 * 60 * 60 * 1_000,
                "preferred_total_audio_ms": 2 * 60 * 60 * 1_000,
                "maximum_wall_seconds": 1_800,
                "model_load_count": 1,
                "inference_concurrency": 2,
                "ready_batch_high_water": 2,
            },
            "telemetry": {
                "fast_interval_ms": 50,
                "slow_interval_ms": 250,
                "maximum_process_vram_bytes": 4 * 1024**3,
                "minimum_free_vram_bytes": 2 * 1024**3,
                "maximum_temperature_c": 80,
                "maximum_heartbeat_age_ms": 750,
            },
            "scheduler": {
                "gpu_processes": 1,
                "prefetch_workers": 0,
                "prefetch_depth": 0,
                "other_cuda_jobs": 0,
                "gpu_lock_policy": "linux_flock_exclusive_nonblocking_gpu_uuid_v2",
                "batch_lock_policy": "linux_flock_exclusive_nonblocking_batch_materialize_v2",
            },
            "safety": {
                "visibility": "private",
                "network_access": False,
                "publication_authority": "none",
                "catalogue_mutation_authority": "none",
                "identity_authority": "none",
                "biometric_authority": "none",
                "wiki_authority": "none",
                "archive_authority": "none",
                "deletion_authority": "none",
            },
        }
    )


def _write_exclusive(path: Path, value: Any) -> None:
    if not path.is_absolute() or path.exists() or path.is_symlink():
        raise ProfileError("profile output must be a new absolute path")
    parent = path.parent
    info = parent.lstat()
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ProfileError(
            "profile output parent must be current-user-owned mode 0700"
        )
    body = canonical_bytes(value)
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def contract_document() -> dict[str, Any]:
    profile = default_profile()
    return {
        "kind": "himr_gpu_production_profile_v2_contract",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "default_profile_id": profile["profile_id"],
        "default_identity_sha256": profile["identity_sha256"],
        "profile": profile,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts")
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            response = contract_document()
        else:
            output = Path(args.output)
            _write_exclusive(output, default_profile())
            body = canonical_bytes(default_profile())
            response = {
                "status": "materialized",
                "path": str(output),
                "sha256": sha256_bytes(body),
                "identity_sha256": default_profile()["identity_sha256"],
                "mode": "0400",
            }
        sys.stdout.buffer.write(canonical_bytes(response))
        return 0
    except (OSError, ProfileError, ValueError) as error:
        sys.stderr.buffer.write(
            canonical_bytes(
                {
                    "status": "failed",
                    "error": {
                        "type": type(error).__name__,
                        "message": str(error),
                    },
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
