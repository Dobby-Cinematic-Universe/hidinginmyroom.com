"""One-shot setup for the reviewed all-known Archive.org campaign.

The setup surface chooses no collections, URLs, commands, or policies.  It creates
only the fixed owner-private operational roots and a no-overwrite canonical config
whose provenance and GPU controls are constants reviewed in this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from .config import (
    IMPLEMENTATION_VERSION,
    KIND,
    SAFETY,
    ControllerConfig,
    ConfigError,
    build_config,
    canonical_bytes,
    sha256_bytes,
)


REPOSITORY = Path(__file__).resolve().parents[1]
OPERATIONAL_PARENT = REPOSITORY / "research/operator-state"
OPERATIONAL_ROOT = OPERATIONAL_PARENT / "autonomous-archive-all-known-2026-08-29"
INVENTORY = {
    "kind": "known_collections_inventory",
    "path": str(
        REPOSITORY
        / "research/corpus/acquisition-planning/archive-all-known-2026-08-29/campaign-inventory.json"
    ),
    "sha256": "ddeac2d290a0c9323d20a28667bf181b6af6dfca33e0af86c394c1073bb8ad63",
}

SCHEDULE_SET = {
    "kind": "sealed_archive_campaign_background_schedule_set",
    "path": str(
        REPOSITORY
        / "research/corpus/autonomous/archive-all-known-2026-08-29/producer-control/"
        "schedule-sets/bgacqscheduleset_cc0a7298263720a9c0aad56524b078a3/manifest.json"
    ),
    "sha256": "bb107b90352bbc71bd5b1561cf0e6567c415c489c49d346a66f055fad263f0ef",
    "schedule_set_id": "bgacqscheduleset_cc0a7298263720a9c0aad56524b078a3",
}

GPU_CONTROL = REPOSITORY / (
    "research/corpus/gpu-runtime/portable-v2-local-private-20260829T211303Z/"
    "controls-20260829T211733Z"
)
GPU_BINDINGS = {
    "root_registration": str(
        REPOSITORY
        / "research/corpus/gpu-runtime/portable-v2-control/hot-root-registration.json"
    ),
    "root_registration_sha256": "9e10b6f7ce302b10d764f1640a1c2f7085efea1724803d39b33a78e86ffac41c",
    "runtime_admission": str(GPU_CONTROL / "runtime-candidate-v2.json"),
    "runtime_admission_sha256": "129f5a7013ad8213a079ff2755778cba16ebbfae00d20eafe0b40d406244bed7",
    "production_profile": str(
        REPOSITORY
        / "research/corpus/gpu-runtime/portable-v2-control/production-profile-v2.json"
    ),
    "production_profile_sha256": "bf2ce7100c554687a4fa84922822b2d488c97e3bcfdc93890f9f98e5b3838cce",
    "launcher_profile": str(GPU_CONTROL / "launcher-profile-v2.json"),
    "launcher_profile_sha256": "d669112164cbb7a0e9f34ce950ea9e9231879058140dd11e088687d6601d6d0d",
    "local_readiness": str(GPU_CONTROL / "readiness-v1.json"),
    "local_readiness_sha256": "98bc028ae2031883c1940197ef842f9dbbf6d12604cacf98c7530d190b365e7b",
    "local_launcher": str(GPU_CONTROL / "trusted-launcher-v2"),
    "local_launcher_sha256": "ba3eeba50128f20b26b86c9bd1635f4cc14d520e470e8f260e0bb83749531b04",
}

WRITABLE_NAMES = (
    "state",
    "preprocess-control",
    "preprocess-output",
    "gpu-queues",
    "gpu-work-orders",
    "gpu-materializations",
    "gpu-results",
    "gpu-batches",
    "gpu-events",
    "gpu-locks",
    "cold-staging",
    "cold-receipts",
)


class SetupError(RuntimeError):
    """Production setup inputs or local filesystem state failed closed."""


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _private_directory(path: Path, label: str) -> Path:
    try:
        observed = path.lstat()
        if path.resolve(strict=True) != path:
            raise SetupError(f"{label} may not traverse a symlink")
    except SetupError:
        raise
    except OSError as error:
        raise SetupError(f"cannot inspect {label}: {error}") from error
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise SetupError(f"{label} must be current-user-owned mode 0700")
    return path


def _create_private(path: Path, label: str) -> Path:
    if not path.exists() and not path.is_symlink():
        try:
            path.mkdir(mode=0o700)
            _fsync_directory(path.parent)
        except OSError as error:
            raise SetupError(f"cannot create {label}: {error}") from error
    return _private_directory(path, label)


def _safe_parent_directory(path: Path, label: str) -> Path:
    try:
        observed = path.lstat()
        if path.resolve(strict=True) != path:
            raise SetupError(f"{label} may not traverse a symlink")
    except SetupError:
        raise
    except OSError as error:
        raise SetupError(f"cannot inspect {label}: {error}") from error
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise SetupError(f"{label} must be current-user and not peer-writable")
    return path


def _stable_hash(path: Path, expected: str, *, mode: int, label: str) -> bytes:
    try:
        inspected = path.lstat()
        if path.resolve(strict=True) != path:
            raise SetupError(f"{label} may not traverse a symlink")
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except SetupError:
        raise
    except OSError as error:
        raise SetupError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != mode
            or (inspected.st_dev, inspected.st_ino, inspected.st_size, inspected.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
        ):
            raise SetupError(f"{label} has unsafe metadata")
        digest = hashlib.sha256()
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            block = os.pread(descriptor, min(1024 * 1024, opened.st_size - offset), offset)
            if not block:
                raise SetupError(f"{label} ended during hashing")
            digest.update(block)
            body.extend(block)
            offset += len(block)
        after = os.fstat(descriptor)
        linked = path.lstat()
        fingerprint = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
            value.st_mode,
            value.st_nlink,
            value.st_uid,
        )
        if (
            fingerprint(opened) != fingerprint(after)
            or fingerprint(after) != fingerprint(linked)
            or digest.hexdigest() != expected
        ):
            raise SetupError(f"{label} differs from its reviewed SHA-256")
        return bytes(body)
    finally:
        os.close(descriptor)


def _load_schedule_set() -> dict[str, Any]:
    body = _stable_hash(
        Path(SCHEDULE_SET["path"]),
        SCHEDULE_SET["sha256"],
        mode=0o400,
        label="campaign schedule-set manifest",
    )
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SetupError(f"campaign schedule-set manifest is not strict JSON: {error}") from error
    if (
        not isinstance(value, dict)
        or value.get("schedule_set_kind") != SCHEDULE_SET["kind"]
        or value.get("schedule_set_id") != SCHEDULE_SET["schedule_set_id"]
        or not isinstance(value.get("schedules"), list)
        or len(value["schedules"]) != 193
    ):
        raise SetupError("campaign schedule-set header or cardinality differs")
    return value


def _preflight_inputs() -> dict[str, Any]:
    _stable_hash(
        Path(INVENTORY["path"]),
        INVENTORY["sha256"],
        mode=0o400,
        label="campaign inventory",
    )
    schedule_set = _load_schedule_set()
    for path_key, digest_key, label in (
        ("root_registration", "root_registration_sha256", "GPU root registration"),
        ("runtime_admission", "runtime_admission_sha256", "GPU runtime admission"),
        ("production_profile", "production_profile_sha256", "GPU production profile"),
        ("launcher_profile", "launcher_profile_sha256", "GPU launcher profile"),
        ("local_readiness", "local_readiness_sha256", "GPU local readiness"),
    ):
        _stable_hash(
            Path(GPU_BINDINGS[path_key]),
            GPU_BINDINGS[digest_key],
            mode=0o400,
            label=label,
        )
    _stable_hash(
        Path(GPU_BINDINGS["local_launcher"]),
        GPU_BINDINGS["local_launcher_sha256"],
        mode=0o500,
        label="GPU local trusted launcher",
    )

    return schedule_set


def _initialize_roots() -> dict[str, Path]:
    if not OPERATIONAL_PARENT.exists() and not OPERATIONAL_PARENT.is_symlink():
        parent = _safe_parent_directory(REPOSITORY / "research", "research root")
        del parent
        _create_private(OPERATIONAL_PARENT, "operator-state root")
    else:
        _private_directory(OPERATIONAL_PARENT, "operator-state root")
    root = _create_private(OPERATIONAL_ROOT, "autonomous operational root")
    allowed = set(WRITABLE_NAMES)
    unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed)
    if unexpected:
        raise SetupError(f"autonomous operational root has unexpected entries: {unexpected}")
    result = {
        name: _create_private(root / name, f"autonomous {name} root")
        for name in WRITABLE_NAMES
    }
    state = result["state"]
    state_allowed = {"events", "gpu-children"}
    unexpected_state = sorted(
        path.name for path in state.iterdir() if path.name not in state_allowed
    )
    if unexpected_state:
        raise SetupError(
            "controller state root is not fresh; refusing a second production config"
        )
    _create_private(state / "events", "controller event root")
    _create_private(state / "gpu-children", "controller GPU child journal root")
    for name, path in result.items():
        if name != "state" and any(path.iterdir()):
            raise SetupError(f"autonomous {name} root is not empty")
    return result


def archive_all_known_config_core(
    roots: dict[str, Path],
    schedule_set: dict[str, Any] | None = None,
) -> dict[str, Any]:
    schedule_set = schedule_set or _load_schedule_set()
    schedules = [
        {
            "path": row["schedule_path"],
            "sha256": row["schedule_sha256"],
            "schedule_id": row["schedule_id"],
            "role": row["role"],
        }
        for row in schedule_set["schedules"]
    ]
    campaign_core = {
        "inventory": dict(INVENTORY),
        "schedule_set": dict(SCHEDULE_SET),
        "schedules": schedules,
        "global_ready_high_items": 16,
        "global_ready_high_bytes": 32 * 1024**3,
    }
    campaign_id = (
        "himrarccampaign_" + sha256_bytes(canonical_bytes(campaign_core))[:32]
    )
    gpu = {key: value for key, value in GPU_BINDINGS.items() if key != "local_launcher_sha256"}
    return {
        "kind": KIND,
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "campaign": {"campaign_id": campaign_id, **campaign_core},
        "state_root": str(roots["state"]),
        "acquisition": {
            "normal_processing": {
                "max_new_items": 8,
                "max_new_bytes": 16 * 1024**3,
                "max_run_seconds": 14_400,
                "free_space_floor_bytes": 512 * 1024**3,
            },
            "cold_acquisition_only_requires_chunking": {
                "max_new_items": 8,
                "max_new_bytes": 128 * 1024**3,
                "max_run_seconds": 14_400,
                "free_space_floor_bytes": 512 * 1024**3,
            },
        },
        "preprocess": {
            "bundle_root": str(roots["preprocess-control"]),
            "processing_output_root": str(roots["preprocess-output"]),
            "max_items": 8,
            "max_attempts_per_item": 3,
        },
        "gpu_readiness": {
            "enabled": True,
            "queue_root": str(roots["gpu-queues"]),
            **gpu,
            "working_directory": str(REPOSITORY),
            "child_journal_root": str(roots["state"] / "gpu-children"),
            "work_order_root": str(roots["gpu-work-orders"]),
            "receipt_root": str(roots["gpu-materializations"]),
            "result_root": str(roots["gpu-results"]),
            "batch_root": str(roots["gpu-batches"]),
            "event_root": str(roots["gpu-events"]),
            "lock_root": str(roots["gpu-locks"]),
            "execution_mode": "local-private-production",
            "max_items_per_batch": 32,
            "max_batches_per_cycle": 2,
            "max_attempts_per_batch": 3,
            "max_active_children": 1,
        },
        "cold_retention": {
            "enabled": False,
            "destination_root": "/mnt/archive/HIMR",
            "staging_root": str(roots["cold-staging"]),
            "receipt_root": str(roots["cold-receipts"]),
            "free_space_floor_bytes": 100 * 1024**3,
            "max_items_per_cycle": 2,
        },
        "scheduler": {
            "idle_seconds": 2.0,
            "failure_backoff_seconds": 60.0,
            "max_consecutive_failures": 10,
        },
        "safety": dict(SAFETY),
    }


def _write_sealed_no_replace(path: Path, body: bytes) -> None:
    raw = str(path)
    if (
        not path.is_absolute()
        or os.path.normpath(raw) != raw
        or raw == "/"
        or "//" in raw
        or "\\" in raw
    ):
        raise SetupError("config output must be one normalized absolute path")
    if path == OPERATIONAL_ROOT or OPERATIONAL_ROOT in path.parents:
        raise SetupError("config output must be outside writable operational roots")
    if path.exists() or path.is_symlink():
        raise SetupError("refusing to overwrite the config output")
    try:
        parent = path.parent.resolve(strict=True)
        observed = parent.lstat()
    except OSError as error:
        raise SetupError(f"cannot inspect config output parent: {error}") from error
    if (
        parent != path.parent
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise SetupError("config output parent must be current-user and not peer-writable")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".himr-autonomous-config.tmp-", dir=parent
    )
    temporary = Path(temporary_name)
    linked = False
    try:
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise SetupError("config output write made no progress")
            offset += written
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path)
            linked = True
        except FileExistsError as error:
            raise SetupError("refusing to overwrite the config output") from error
        temporary.unlink()
        _fsync_directory(parent)
        linked = False
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        if linked:
            path.unlink(missing_ok=True)
            _fsync_directory(parent)


def setup_archive_all_known(output: Path) -> dict[str, Any]:
    """Preflight reviewed inputs, initialize roots, and seal one config."""

    schedule_set = _preflight_inputs()
    prospective_roots = {
        name: OPERATIONAL_ROOT / name for name in WRITABLE_NAMES
    }
    try:
        document = build_config(
            archive_all_known_config_core(prospective_roots, schedule_set)
        )
    except ConfigError as error:
        raise SetupError(f"fixed production config is invalid: {error}") from error
    body = canonical_bytes(document)
    try:
        from .sealed_backend import SealedArchiveBackend

        preflight_config = ControllerConfig(
            document=document,
            path=output,
            physical_sha256=sha256_bytes(body),
        )
        restored = SealedArchiveBackend(preflight_config).restore(())
    except Exception as error:
        raise SetupError(
            f"production campaign deep replay failed before root creation: {error}"
        ) from error
    roots = _initialize_roots()
    if roots != prospective_roots:
        raise SetupError("initialized operational roots differ from fixed config paths")
    _write_sealed_no_replace(output, body)
    return {
        "status": "configured",
        "config": str(output),
        "config_sha256": sha256_bytes(body),
        "config_id": document["config_id"],
        "campaign_id": document["campaign"]["campaign_id"],
        "state_root": document["state_root"],
        "schedule_set_id": SCHEDULE_SET["schedule_set_id"],
        "configured_schedule_count": len(document["campaign"]["schedules"]),
        "coverage": restored["campaign_coverage"],
        "files_overwritten": False,
        "processing_started": False,
    }


__all__ = [
    "SetupError",
    "archive_all_known_config_core",
    "setup_archive_all_known",
]
