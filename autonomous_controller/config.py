"""Closed, digest-bound configuration for the autonomous Archive controller."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any


KIND = "himr_autonomous_archive_controller_config"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.2.0"
FIXED_COLD_ROOT = Path("/mnt/archive/HIMR")
MAX_CONFIG_BYTES = 256 * 1024
MAX_INTEGER = 2**63 - 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CONFIG_ID_RE = re.compile(r"^himrautocfg_[0-9a-f]{32}$")
SCHEDULE_SET_ID_PATTERNS = {
    "sealed_archive_campaign_background_schedule_set": re.compile(
        r"^bgacqscheduleset_[0-9a-f]{32}$"
    ),
    "sealed_archive_campaign_composite_schedule_set": re.compile(
        r"^bgacqcompositeset_[0-9a-f]{32}$"
    ),
}

SAFETY = {
    "catalogue_access": "none",
    "catalogue_mutation_authority": "none",
    "cold_destination": str(FIXED_COLD_ROOT),
    "cold_mount_filesystem": "xfs",
    "cold_mount_uuid": "5b5813ad-b1a4-4f52-9960-e762ceac5636",
    "credentials_allowed": False,
    "deletion_authority": "none",
    "discovery_authority": "none",
    "gpu_execution_authority": "exact_local_private_systemd_child_only",
    "identity_authority": "none",
    "publication_authority": "none",
    "source_authority": "ordered_exact_sealed_archive_campaign_schedules",
}

# Fresh campaigns may bind the reviewed replacement disk directly. Historical
# configurations retain their original bytes and use the exact migration proof.
REPLACEMENT_COLD_MOUNT_UUID = "af41b7da-a588-41cf-83f8-cd99ef425b74"
REPLACEMENT_SAFETY = {**SAFETY, "cold_mount_uuid": REPLACEMENT_COLD_MOUNT_UUID}

CORE_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "campaign",
    "state_root",
    "acquisition",
    "preprocess",
    "gpu_readiness",
    "cold_retention",
    "scheduler",
    "safety",
}


class ConfigError(RuntimeError):
    """The controller configuration is unsafe, mutable, or inconsistent."""


def canonical_bytes(value: Any) -> bytes:
    try:
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
    except (TypeError, ValueError) as error:
        raise ConfigError(f"configuration is not canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise ConfigError(f"{label} has unexpected fields: {observed}")
    return value


def _text(value: Any, label: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ConfigError(f"{label} must be bounded non-empty text")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ConfigError(f"{label} must be a lowercase SHA-256")
    return value


def _integer(
    value: Any,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_INTEGER,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ConfigError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def _number(
    value: Any,
    label: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not minimum <= float(value) <= maximum
    ):
        raise ConfigError(f"{label} must be a number in [{minimum}, {maximum}]")
    return float(value)


def _path(value: Any, label: str, *, allow_cold: bool = False) -> Path:
    raw = _text(value, label, maximum=4096)
    path = Path(raw)
    if (
        not path.is_absolute()
        or str(path) != raw
        or os.path.normpath(raw) != raw
        or "//" in raw
        or "\\" in raw
        or raw == "/"
    ):
        raise ConfigError(f"{label} must be one normalized absolute local path")
    if not allow_cold and (path == FIXED_COLD_ROOT or FIXED_COLD_ROOT in path.parents):
        raise ConfigError(f"{label} may not reference cold storage")
    return path


def _trees_intersect(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


@dataclass(frozen=True)
class ControllerConfig:
    """A normalized controller configuration and its external byte binding."""

    document: dict[str, Any]
    path: Path
    physical_sha256: str

    @property
    def config_id(self) -> str:
        return self.document["config_id"]

    @property
    def state_root(self) -> Path:
        return Path(self.document["state_root"])

    def section(self, name: str) -> dict[str, Any]:
        return self.document[name]


def normalize_config(value: Any) -> dict[str, Any]:
    item = _exact(value, "controller config", CORE_KEYS | {"identity_sha256", "config_id"})
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["safety"] not in (SAFETY, REPLACEMENT_SAFETY)
    ):
        raise ConfigError("controller header or no-authority safety policy differs")
    normalized_safety = (
        REPLACEMENT_SAFETY if item["safety"] == REPLACEMENT_SAFETY else SAFETY
    )

    campaign = _exact(
        item["campaign"],
        "campaign",
        {
            "campaign_id",
            "inventory",
            "schedule_set",
            "schedules",
            "global_ready_high_items",
            "global_ready_high_bytes",
        },
    )
    campaign_id = _text(campaign["campaign_id"], "campaign.campaign_id", maximum=96)
    if re.fullmatch(r"himrarccampaign_[0-9a-f]{32}", campaign_id) is None:
        raise ConfigError("campaign.campaign_id is invalid")
    inventory = _exact(
        campaign["inventory"], "campaign.inventory", {"kind", "path", "sha256"}
    )
    inventory_kind = _text(
        inventory["kind"], "campaign.inventory.kind", maximum=64
    )
    if inventory_kind != "known_collections_inventory":
        raise ConfigError("campaign inventory kind is unsupported")
    normalized_inventory = {
        "kind": inventory_kind,
        "path": str(_path(inventory["path"], "campaign.inventory.path")),
        "sha256": _digest(inventory["sha256"], "campaign.inventory.sha256"),
    }
    schedule_set = _exact(
        campaign["schedule_set"],
        "campaign.schedule_set",
        {"kind", "path", "sha256", "schedule_set_id"},
    )
    schedule_set_kind = _text(
        schedule_set["kind"], "campaign.schedule_set.kind", maximum=96
    )
    schedule_set_id_pattern = SCHEDULE_SET_ID_PATTERNS.get(schedule_set_kind)
    if schedule_set_id_pattern is None:
        raise ConfigError("campaign schedule-set kind is unsupported")
    schedule_set_id = _text(
        schedule_set["schedule_set_id"],
        "campaign.schedule_set.schedule_set_id",
        maximum=96,
    )
    if schedule_set_id_pattern.fullmatch(schedule_set_id) is None:
        raise ConfigError("campaign schedule-set ID is invalid")
    normalized_schedule_set = {
        "kind": schedule_set_kind,
        "path": str(_path(schedule_set["path"], "campaign.schedule_set.path")),
        "sha256": _digest(
            schedule_set["sha256"], "campaign.schedule_set.sha256"
        ),
        "schedule_set_id": schedule_set_id,
    }
    if not isinstance(campaign["schedules"], list) or not 1 <= len(campaign["schedules"]) <= 1024:
        raise ConfigError("campaign.schedules must contain 1 to 1024 exact schedules")
    normalized_schedules: list[dict[str, str]] = []
    seen_schedule_ids: set[str] = set()
    seen_schedule_paths: set[str] = set()
    for index, raw_schedule in enumerate(campaign["schedules"], 1):
        schedule = _exact(
            raw_schedule,
            f"campaign.schedules[{index - 1}]",
            {"path", "sha256", "schedule_id", "role"},
        )
        normalized_schedule = {
            "path": str(_path(schedule["path"], f"campaign.schedules[{index - 1}].path")),
            "sha256": _digest(schedule["sha256"], f"campaign.schedules[{index - 1}].sha256"),
            "schedule_id": _text(
                schedule["schedule_id"],
                f"campaign.schedules[{index - 1}].schedule_id",
                maximum=96,
            ),
            "role": _text(
                schedule["role"],
                f"campaign.schedules[{index - 1}].role",
                maximum=64,
            ),
        }
        if normalized_schedule["role"] not in {
            "normal_processing",
            "cold_acquisition_only_requires_chunking",
        }:
            raise ConfigError(f"campaign schedule {index} role is unsupported")
        if re.fullmatch(r"bgacqsched_[0-9a-f]{32}", normalized_schedule["schedule_id"]) is None:
            raise ConfigError(f"campaign schedule {index} ID is invalid")
        if (
            normalized_schedule["schedule_id"] in seen_schedule_ids
            or normalized_schedule["path"] in seen_schedule_paths
        ):
            raise ConfigError("campaign repeats a schedule ID or path")
        seen_schedule_ids.add(normalized_schedule["schedule_id"])
        seen_schedule_paths.add(normalized_schedule["path"])
        normalized_schedules.append(normalized_schedule)
    if not any(
        schedule["role"] == "normal_processing"
        for schedule in normalized_schedules
    ):
        raise ConfigError("campaign requires at least one normal-processing schedule")
    campaign_core = {
        "inventory": normalized_inventory,
        "schedule_set": normalized_schedule_set,
        "schedules": normalized_schedules,
        "global_ready_high_items": _integer(
            campaign["global_ready_high_items"],
            "campaign.global_ready_high_items",
            minimum=1,
            maximum=8192,
        ),
        "global_ready_high_bytes": _integer(
            campaign["global_ready_high_bytes"],
            "campaign.global_ready_high_bytes",
            minimum=1,
        ),
    }
    expected_campaign_id = (
        f"himrarccampaign_{sha256_bytes(canonical_bytes(campaign_core))[:32]}"
    )
    if campaign_id != expected_campaign_id:
        raise ConfigError("campaign.campaign_id differs from its exact inventory and schedules")
    normalized_campaign = {"campaign_id": campaign_id, **campaign_core}

    state_root = _path(item["state_root"], "state_root")
    acquisition = _exact(
        item["acquisition"],
        "acquisition",
        {
            "normal_processing",
            "cold_acquisition_only_requires_chunking",
        },
    )
    normalized_acquisition: dict[str, dict[str, int]] = {}
    for role in (
        "normal_processing",
        "cold_acquisition_only_requires_chunking",
    ):
        limits = _exact(
            acquisition[role],
            f"acquisition.{role}",
            {
                "max_new_items",
                "max_new_bytes",
                "max_run_seconds",
                "free_space_floor_bytes",
            },
        )
        normalized_acquisition[role] = {
            "max_new_items": _integer(
                limits["max_new_items"],
                f"acquisition.{role}.max_new_items",
                minimum=1,
                maximum=8,
            ),
            "max_new_bytes": _integer(
                limits["max_new_bytes"],
                f"acquisition.{role}.max_new_bytes",
                minimum=1,
            ),
            "max_run_seconds": _integer(
                limits["max_run_seconds"],
                f"acquisition.{role}.max_run_seconds",
                minimum=1,
                maximum=14_400,
            ),
            "free_space_floor_bytes": _integer(
                limits["free_space_floor_bytes"],
                f"acquisition.{role}.free_space_floor_bytes",
            ),
        }
    if (
        any(
            limits["free_space_floor_bytes"] < 512 * 1024**3
            for limits in normalized_acquisition.values()
        )
        or
        normalized_acquisition["cold_acquisition_only_requires_chunking"][
            "max_new_bytes"
        ]
        < 64 * 1024**3
        or normalized_acquisition["cold_acquisition_only_requires_chunking"][
            "free_space_floor_bytes"
        ]
        < 512 * 1024**3
    ):
        raise ConfigError(
            "acquisition requires the reviewed 512-GiB floor and 64-GiB cold-only dispatch"
        )

    preprocess = _exact(
        item["preprocess"],
        "preprocess",
        {
            "bundle_root",
            "processing_output_root",
            "max_items",
            "max_attempts_per_item",
        },
    )
    normalized_preprocess = {
        "bundle_root": str(_path(preprocess["bundle_root"], "preprocess.bundle_root")),
        "processing_output_root": str(_path(preprocess["processing_output_root"], "preprocess.processing_output_root")),
        "max_items": _integer(preprocess["max_items"], "preprocess.max_items", minimum=1, maximum=8),
        "max_attempts_per_item": _integer(
            preprocess["max_attempts_per_item"],
            "preprocess.max_attempts_per_item",
            minimum=3,
            maximum=3,
        ),
    }

    gpu = _exact(
        item["gpu_readiness"],
        "gpu_readiness",
        {
            "enabled",
            "queue_root",
            "root_registration",
            "root_registration_sha256",
            "runtime_admission",
            "runtime_admission_sha256",
            "production_profile",
            "production_profile_sha256",
            "launcher_profile",
            "launcher_profile_sha256",
            "local_readiness",
            "local_readiness_sha256",
            "local_launcher",
            "working_directory",
            "child_journal_root",
            "work_order_root",
            "receipt_root",
            "result_root",
            "batch_root",
            "event_root",
            "lock_root",
            "execution_mode",
            "max_items_per_batch",
            "max_batches_per_cycle",
            "max_attempts_per_batch",
            "max_active_children",
        },
    )
    if not isinstance(gpu["enabled"], bool):
        raise ConfigError("gpu_readiness.enabled must be boolean")
    execution_mode = _text(gpu["execution_mode"], "gpu_readiness.execution_mode")
    if execution_mode != "local-private-production":
        raise ConfigError(
            "autonomous GPU execution requires local-private-production"
        )
    child_journal_root = _path(
        gpu["child_journal_root"], "gpu_readiness.child_journal_root"
    )
    if child_journal_root != state_root / "gpu-children":
        raise ConfigError(
            "gpu_readiness.child_journal_root must be state_root/gpu-children"
        )
    normalized_gpu = {
        "enabled": gpu["enabled"],
        "queue_root": str(_path(gpu["queue_root"], "gpu_readiness.queue_root")),
        "root_registration": str(_path(gpu["root_registration"], "gpu_readiness.root_registration")),
        "root_registration_sha256": _digest(gpu["root_registration_sha256"], "gpu_readiness.root_registration_sha256"),
        "runtime_admission": str(_path(gpu["runtime_admission"], "gpu_readiness.runtime_admission")),
        "runtime_admission_sha256": _digest(gpu["runtime_admission_sha256"], "gpu_readiness.runtime_admission_sha256"),
        "production_profile": str(_path(gpu["production_profile"], "gpu_readiness.production_profile")),
        "production_profile_sha256": _digest(gpu["production_profile_sha256"], "gpu_readiness.production_profile_sha256"),
        "launcher_profile": str(_path(gpu["launcher_profile"], "gpu_readiness.launcher_profile")),
        "launcher_profile_sha256": _digest(gpu["launcher_profile_sha256"], "gpu_readiness.launcher_profile_sha256"),
        "local_readiness": str(_path(gpu["local_readiness"], "gpu_readiness.local_readiness")),
        "local_readiness_sha256": _digest(gpu["local_readiness_sha256"], "gpu_readiness.local_readiness_sha256"),
        "local_launcher": str(_path(gpu["local_launcher"], "gpu_readiness.local_launcher")),
        "working_directory": str(_path(gpu["working_directory"], "gpu_readiness.working_directory")),
        "child_journal_root": str(child_journal_root),
        "work_order_root": str(_path(gpu["work_order_root"], "gpu_readiness.work_order_root")),
        "receipt_root": str(_path(gpu["receipt_root"], "gpu_readiness.receipt_root")),
        "result_root": str(_path(gpu["result_root"], "gpu_readiness.result_root")),
        "batch_root": str(_path(gpu["batch_root"], "gpu_readiness.batch_root")),
        "event_root": str(_path(gpu["event_root"], "gpu_readiness.event_root")),
        "lock_root": str(_path(gpu["lock_root"], "gpu_readiness.lock_root")),
        "execution_mode": execution_mode,
        "max_items_per_batch": _integer(gpu["max_items_per_batch"], "gpu_readiness.max_items_per_batch", minimum=1, maximum=32),
        "max_batches_per_cycle": _integer(gpu["max_batches_per_cycle"], "gpu_readiness.max_batches_per_cycle", minimum=1, maximum=2),
        "max_attempts_per_batch": _integer(
            gpu["max_attempts_per_batch"],
            "gpu_readiness.max_attempts_per_batch",
            minimum=1,
            maximum=10,
        ),
        "max_active_children": _integer(
            gpu["max_active_children"],
            "gpu_readiness.max_active_children",
            minimum=1,
            maximum=1,
        ),
    }

    cold = _exact(
        item["cold_retention"],
        "cold_retention",
        {"enabled", "destination_root", "staging_root", "receipt_root", "free_space_floor_bytes", "max_items_per_cycle"},
    )
    if not isinstance(cold["enabled"], bool):
        raise ConfigError("cold_retention.enabled must be boolean")
    destination = _path(cold["destination_root"], "cold_retention.destination_root", allow_cold=True)
    if destination != FIXED_COLD_ROOT:
        raise ConfigError("cold retention destination must be exactly /mnt/archive/HIMR")
    normalized_cold = {
        "enabled": cold["enabled"],
        "destination_root": str(destination),
        "staging_root": str(_path(cold["staging_root"], "cold_retention.staging_root")),
        "receipt_root": str(_path(cold["receipt_root"], "cold_retention.receipt_root")),
        "free_space_floor_bytes": _integer(cold["free_space_floor_bytes"], "cold_retention.free_space_floor_bytes"),
        "max_items_per_cycle": _integer(cold["max_items_per_cycle"], "cold_retention.max_items_per_cycle", minimum=1, maximum=2),
    }

    scheduler = _exact(
        item["scheduler"],
        "scheduler",
        {"idle_seconds", "failure_backoff_seconds", "max_consecutive_failures"},
    )
    normalized_scheduler = {
        "idle_seconds": _number(scheduler["idle_seconds"], "scheduler.idle_seconds", minimum=0.1, maximum=3_600.0),
        "failure_backoff_seconds": _number(scheduler["failure_backoff_seconds"], "scheduler.failure_backoff_seconds", minimum=0.1, maximum=3_600.0),
        "max_consecutive_failures": _integer(scheduler["max_consecutive_failures"], "scheduler.max_consecutive_failures", minimum=1, maximum=100),
    }

    hot_roots = [
        state_root,
        Path(normalized_preprocess["bundle_root"]),
        Path(normalized_preprocess["processing_output_root"]),
        Path(normalized_gpu["queue_root"]),
        Path(normalized_gpu["work_order_root"]),
        Path(normalized_gpu["receipt_root"]),
        Path(normalized_gpu["result_root"]),
        Path(normalized_gpu["batch_root"]),
        Path(normalized_gpu["event_root"]),
        Path(normalized_gpu["lock_root"]),
        Path(normalized_cold["staging_root"]),
        Path(normalized_cold["receipt_root"]),
    ]
    for index, left in enumerate(hot_roots):
        for right in hot_roots[index + 1 :]:
            if _trees_intersect(left, right):
                raise ConfigError(f"controller writable roots must be disjoint: {left} and {right}")

    core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "campaign": normalized_campaign,
        "state_root": str(state_root),
        "acquisition": normalized_acquisition,
        "preprocess": normalized_preprocess,
        "gpu_readiness": normalized_gpu,
        "cold_retention": normalized_cold,
        "scheduler": normalized_scheduler,
        "safety": dict(normalized_safety),
    }
    identity = sha256_bytes(canonical_bytes(core))
    normalized = {
        **core,
        "identity_sha256": identity,
        "config_id": f"himrautocfg_{identity[:32]}",
    }
    if item["identity_sha256"] != identity or item["config_id"] != normalized["config_id"]:
        raise ConfigError("controller config identity is inconsistent")
    if canonical_bytes(item) != canonical_bytes(normalized):
        raise ConfigError("controller config is not normalized")
    return normalized


def build_config(core: dict[str, Any]) -> dict[str, Any]:
    """Add deterministic identity fields to an otherwise complete config core."""

    if not isinstance(core, dict) or set(core) != CORE_KEYS:
        raise ConfigError("config core fields differ from the closed contract")
    identity = sha256_bytes(canonical_bytes(core))
    return normalize_config(
        {
            **core,
            "identity_sha256": identity,
            "config_id": f"himrautocfg_{identity[:32]}",
        }
    )


def _read_sealed(path: Path) -> bytes:
    raw = os.fspath(path)
    if not path.is_absolute() or str(path) != raw or os.path.normpath(raw) != raw:
        raise ConfigError("config path must be normalized absolute")
    try:
        inspected = path.lstat()
        if path.resolve(strict=True) != path:
            raise ConfigError("config path may not traverse a symlink")
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except ConfigError:
        raise
    except OSError as error:
        raise ConfigError(f"cannot open controller config: {error}") from error
    try:
        opened = os.fstat(descriptor)
        allowed = (
            (opened.st_uid == os.geteuid() and stat.S_IMODE(opened.st_mode) == 0o400)
            or (opened.st_uid == 0 and stat.S_IMODE(opened.st_mode) == 0o444)
        )
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or not allowed
            or opened.st_size < 1
            or opened.st_size > MAX_CONFIG_BYTES
            or (inspected.st_dev, inspected.st_ino, inspected.st_size, inspected.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mode)
        ):
            raise ConfigError("config must be a sealed single-link regular file")
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, opened.st_size - offset, offset)
            if not chunk:
                raise ConfigError("controller config ended during read")
            body.extend(chunk)
            offset += len(chunk)
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
        if fingerprint(opened) != fingerprint(after) or fingerprint(after) != fingerprint(linked):
            raise ConfigError("controller config changed during read")
        return bytes(body)
    finally:
        os.close(descriptor)


def load_config(path: Path, expected_sha256: str) -> ControllerConfig:
    expected = _digest(expected_sha256, "expected config SHA-256")
    body = _read_sealed(path)
    observed = sha256_bytes(body)
    if observed != expected:
        raise ConfigError("controller config differs from its external SHA-256")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ConfigError(f"controller config is not strict JSON: {error}") from error
    normalized = normalize_config(value)
    if body != canonical_bytes(normalized):
        raise ConfigError("controller config bytes are not canonical")
    return ControllerConfig(normalized, path, observed)
