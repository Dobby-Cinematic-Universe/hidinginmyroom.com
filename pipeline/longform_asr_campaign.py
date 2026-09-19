#!/usr/bin/env python3
"""Migration-safe controller adapter for the recording-first long-form ASR lane.

The adapter deliberately remains a separate finite-stage process.  It consumes only
immutable ordinary GPU queue members whose admitted disposition is
``requires_chunking``, writes only below its own deployment root, and delegates GPU
execution to :mod:`pipeline.gpu.longform_asr_runner_v1`.  That runner acquires the
same UUID-specific lock named by the source controller configuration, so the two ASR
implementations cannot execute on the GPU at the same time.

No command in this module starts, stops, rewrites, or appends to the source
controller. ``status`` is read-only, ``prepare-once`` prepares at most one candidate
(including isolated cold-source preprocessing when needed), and ``run-once``
performs at most one recording.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator, Sequence


ROOT = Path(__file__).resolve().parents[1]
CORPUS_SRC = ROOT / "corpus" / "src"
if str(CORPUS_SRC) not in sys.path:
    sys.path.insert(0, str(CORPUS_SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_controller.config import (  # noqa: E402
    ControllerConfig,
    load_config as load_controller_config,
)
from autonomous_controller.sealed_backend import (  # noqa: E402
    BackendError as SealedBackendError,
    _install_acquisition_directory_identity_adapter,
)
from autonomous_controller.state import read_control_state  # noqa: E402
from autonomous_controller.public_status import read_public_status  # noqa: E402
from acquisition import archive_preprocess_handoff  # noqa: E402
from himr_corpus.longform_asr_planner import (  # noqa: E402
    build_longform_asr_plan,
    validate_planning_policy,
)
from pipeline import longform_asr_input, preprocess_batch  # noqa: E402


KIND = "himr_longform_asr_campaign_config"
SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
CONFIG_ID_RE = re.compile(r"^himrlongcfg_[0-9a-f]{32}$")
JOB_ID_RE = re.compile(r"^himrlongjob_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_CONFIG_BYTES = 1024 * 1024
MAX_QUEUE_MANIFESTS = 100_000
MAX_CANDIDATES = 1_000_000
FIXED_COLD_ROOT = Path("/mnt/archive/HIMR")
HOT_SCRATCH_FREE_FLOOR_BYTES = 64 * 1024**3
SAFETY = {
    "archive_authority": "none",
    "catalogue_mutation_authority": "none",
    "deletion_authority": "completed_job_derived_preprocess_scratch_only",
    "identity_authority": "none",
    "network_access": False,
    "publication_authority": "none",
    "source_controller_mutation_authority": "none",
    "source_selection": (
        "validated_requires_chunking_gpu_queue_members_or_completed_cold_schedule_results"
    ),
    "visibility": "private",
    "wiki_authority": "none",
}
CONFIG_CORE_KEYS = {
    "kind",
    "schema_version",
    "implementation_version",
    "source_controller",
    "planning_policy",
    "tools",
    "deployment",
    "execution",
    "safety",
}


class CampaignError(RuntimeError):
    """The deployment, source queue, or finite execution failed closed."""


@dataclass(frozen=True)
class CampaignConfig:
    document: dict[str, Any]
    path: Path
    physical_sha256: str

    @property
    def config_id(self) -> str:
        return self.document["config_id"]

    @property
    def root(self) -> Path:
        return Path(self.document["deployment"]["root"])


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
    except (TypeError, ValueError, RecursionError) as error:
        raise CampaignError(f"value is not canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise CampaignError(f"{label} has unexpected fields: {observed}")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise CampaignError(f"{label} must be a lowercase SHA-256")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise CampaignError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def _absolute_path(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise CampaignError(f"{label} must be a non-empty absolute path")
    path = Path(value)
    if (
        not path.is_absolute()
        or str(path) != value
        or os.path.normpath(value) != value
        or "//" in value
        or "\\" in value
        or value == "/"
    ):
        raise CampaignError(f"{label} must be one normalized absolute local path")
    return path


def _trees_intersect(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _stable_file(
    path: Path,
    label: str,
    *,
    maximum: int = MAX_CONFIG_BYTES,
    executable: bool = False,
    allowed_modes: frozenset[int] | None = None,
) -> bytes:
    try:
        inspected = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise CampaignError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        mode = stat.S_IMODE(opened.st_mode)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_size < 1
            or opened.st_size > maximum
            or opened.st_uid not in {os.geteuid(), 0}
            or mode & 0o022
            or (allowed_modes is not None and mode not in allowed_modes)
            or (inspected.st_dev, inspected.st_ino, inspected.st_mode, inspected.st_size)
            != (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_size)
        ):
            raise CampaignError(f"{label} has unsafe metadata")
        if executable and not os.access(f"/proc/self/fd/{descriptor}", os.X_OK):
            raise CampaignError(f"{label} is not executable")
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            block = os.pread(
                descriptor,
                min(1024 * 1024, opened.st_size - offset),
                offset,
            )
            if not block:
                raise CampaignError(f"{label} ended while being read")
            body.extend(block)
            offset += len(block)
        after = os.fstat(descriptor)
        linked = path.lstat()
        fingerprint = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_uid,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        if fingerprint(opened) != fingerprint(after) or fingerprint(after) != fingerprint(linked):
            raise CampaignError(f"{label} changed while being read")
        return bytes(body)
    finally:
        os.close(descriptor)


def _strict_json(body: bytes, label: str) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CampaignError(f"{label} repeats key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=unique,
            parse_constant=lambda value: (_ for _ in ()).throw(
                CampaignError(f"{label} contains non-finite number {value}")
            ),
        )
    except CampaignError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise CampaignError(f"{label} is not strict JSON: {error}") from error


def _controller_reference(controller: ControllerConfig) -> dict[str, Any]:
    gpu = controller.section("gpu_readiness")
    preprocess = controller.section("preprocess")
    cold = controller.section("cold_retention")
    writable_roots = sorted(
        {
            str(controller.state_root),
            preprocess["bundle_root"],
            preprocess["processing_output_root"],
            *(
                gpu[name]
                for name in (
                    "queue_root",
                    "work_order_root",
                    "receipt_root",
                    "result_root",
                    "batch_root",
                    "event_root",
                    "lock_root",
                )
            ),
            cold["staging_root"],
            cold["receipt_root"],
        }
    )
    return {
        "config_id": controller.config_id,
        "identity_sha256": controller.document["identity_sha256"],
        "path": str(controller.path),
        "physical_sha256": controller.physical_sha256,
        "gpu_queue_root": gpu["queue_root"],
        "gpu_lock_root": gpu["lock_root"],
        "root_registration": gpu["root_registration"],
        "root_registration_sha256": gpu["root_registration_sha256"],
        "production_profile": gpu["production_profile"],
        "production_profile_sha256": gpu["production_profile_sha256"],
        "writable_roots": writable_roots,
    }


def _normalize_config(value: Any) -> dict[str, Any]:
    item = _exact(value, "long-form campaign config", CONFIG_CORE_KEYS | {"identity_sha256", "config_id"})
    if (
        item["kind"] != KIND
        or item["schema_version"] != SCHEMA_VERSION
        or item["implementation_version"] != IMPLEMENTATION_VERSION
        or item["safety"] != SAFETY
    ):
        raise CampaignError("long-form campaign header or safety policy differs")

    source = _exact(
        item["source_controller"],
        "source_controller",
        {
            "config_id",
            "identity_sha256",
            "path",
            "physical_sha256",
            "gpu_queue_root",
            "gpu_lock_root",
            "root_registration",
            "root_registration_sha256",
            "production_profile",
            "production_profile_sha256",
            "writable_roots",
        },
    )
    if not isinstance(source["config_id"], str) or not source["config_id"].startswith("himrautocfg_"):
        raise CampaignError("source controller config ID is invalid")
    normalized_source = {
        "config_id": source["config_id"],
        "identity_sha256": _digest(source["identity_sha256"], "source controller identity"),
        "path": str(_absolute_path(source["path"], "source controller path")),
        "physical_sha256": _digest(source["physical_sha256"], "source controller physical SHA-256"),
        "gpu_queue_root": str(_absolute_path(source["gpu_queue_root"], "source GPU queue root")),
        "gpu_lock_root": str(_absolute_path(source["gpu_lock_root"], "source GPU lock root")),
        "root_registration": str(_absolute_path(source["root_registration"], "root registration")),
        "root_registration_sha256": _digest(source["root_registration_sha256"], "root registration SHA-256"),
        "production_profile": str(_absolute_path(source["production_profile"], "production profile")),
        "production_profile_sha256": _digest(source["production_profile_sha256"], "production profile SHA-256"),
        "writable_roots": [
            str(_absolute_path(path, "source writable root"))
            for path in source["writable_roots"]
        ]
        if isinstance(source["writable_roots"], list)
        else [],
    }
    if (
        not normalized_source["writable_roots"]
        or normalized_source["writable_roots"]
        != sorted(set(normalized_source["writable_roots"]))
    ):
        raise CampaignError("source writable roots must be a sorted unique array")

    policy = _exact(item["planning_policy"], "planning_policy", {"path", "sha256"})
    normalized_policy = {
        "path": str(_absolute_path(policy["path"], "planning policy path")),
        "sha256": _digest(policy["sha256"], "planning policy SHA-256"),
    }
    tools = _exact(item["tools"], "tools", {"ffmpeg", "ffprobe"})
    normalized_tools: dict[str, dict[str, str]] = {}
    for name in ("ffmpeg", "ffprobe"):
        row = _exact(tools[name], f"tools.{name}", {"path", "sha256"})
        normalized_tools[name] = {
            "path": str(_absolute_path(row["path"], f"{name} path")),
            "sha256": _digest(row["sha256"], f"{name} SHA-256"),
        }

    deployment = _exact(
        item["deployment"],
        "deployment",
        {
            "root",
            "jobs_root",
            "dispatch_lock",
            "discovery_root",
            "cold_locator_manifest",
            "cold_locator_manifest_sha256",
            "candidate_receipt_root",
            "queue_receipt_root",
            "status_path",
        },
    )
    root = _absolute_path(deployment["root"], "deployment root")
    jobs_root = _absolute_path(deployment["jobs_root"], "jobs root")
    dispatch_lock = _absolute_path(deployment["dispatch_lock"], "dispatch lock")
    discovery_root = _absolute_path(deployment["discovery_root"], "discovery root")
    cold_locator_manifest = _absolute_path(
        deployment["cold_locator_manifest"], "cold locator manifest"
    )
    candidate_receipt_root = _absolute_path(
        deployment["candidate_receipt_root"], "candidate receipt root"
    )
    queue_receipt_root = _absolute_path(
        deployment["queue_receipt_root"], "queue receipt root"
    )
    status_path = _absolute_path(deployment["status_path"], "status path")
    if (
        jobs_root != root / "jobs"
        or dispatch_lock != root / "dispatch.lock"
        or discovery_root != root / "discovery"
        or cold_locator_manifest != discovery_root / "cold-locators.json"
        or candidate_receipt_root != discovery_root / "candidate-receipts"
        or queue_receipt_root != discovery_root / "queue-receipts"
        or status_path != root / "status.json"
    ):
        raise CampaignError("deployment descendants differ from the closed layout")
    if root == FIXED_COLD_ROOT or FIXED_COLD_ROOT in root.parents:
        raise CampaignError("long-form mutable state may not use cold storage")
    source_writable_roots = [
        Path(path) for path in normalized_source["writable_roots"]
    ]
    if any(_trees_intersect(root, candidate) for candidate in source_writable_roots):
        raise CampaignError("long-form deployment root intersects source controller state")
    normalized_deployment = {
        "root": str(root),
        "jobs_root": str(jobs_root),
        "dispatch_lock": str(dispatch_lock),
        "discovery_root": str(discovery_root),
        "cold_locator_manifest": str(cold_locator_manifest),
        "cold_locator_manifest_sha256": _digest(
            deployment["cold_locator_manifest_sha256"],
            "cold locator manifest SHA-256",
        ),
        "candidate_receipt_root": str(candidate_receipt_root),
        "queue_receipt_root": str(queue_receipt_root),
        "status_path": str(status_path),
    }

    execution = _exact(
        item["execution"],
        "execution",
        {"max_recordings_per_cycle", "max_run_seconds", "initial_prompt", "hotwords"},
    )
    initial_prompt = execution["initial_prompt"]
    if initial_prompt is not None and (
        not isinstance(initial_prompt, str) or len(initial_prompt) > 16_384 or "\x00" in initial_prompt
    ):
        raise CampaignError("initial prompt must be null or bounded text")
    hotwords = execution["hotwords"]
    if (
        not isinstance(hotwords, list)
        or len(hotwords) > 4096
        or any(not isinstance(word, str) or not word or len(word) > 256 or "\x00" in word for word in hotwords)
    ):
        raise CampaignError("hotwords must be a bounded text array")
    normalized_hotwords = sorted(set(hotwords), key=str.casefold)
    if hotwords != normalized_hotwords:
        raise CampaignError("hotwords must be sorted and unique")
    normalized_execution = {
        "max_recordings_per_cycle": _integer(
            execution["max_recordings_per_cycle"], "max recordings per cycle", 1, 1
        ),
        "max_run_seconds": _integer(
            execution["max_run_seconds"], "max run seconds", 60, 7 * 24 * 60 * 60
        ),
        "initial_prompt": initial_prompt,
        "hotwords": normalized_hotwords,
    }

    core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "source_controller": normalized_source,
        "planning_policy": normalized_policy,
        "tools": normalized_tools,
        "deployment": normalized_deployment,
        "execution": normalized_execution,
        "safety": dict(SAFETY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    normalized = {
        **core,
        "identity_sha256": identity,
        "config_id": f"himrlongcfg_{identity[:32]}",
    }
    if item["identity_sha256"] != identity or item["config_id"] != normalized["config_id"]:
        raise CampaignError("long-form campaign config identity is inconsistent")
    if canonical_bytes(item) != canonical_bytes(normalized):
        raise CampaignError("long-form campaign config is not normalized")
    return normalized


def build_config(
    *,
    controller: ControllerConfig,
    planning_policy: Path,
    planning_policy_sha256: str,
    ffmpeg: Path,
    ffmpeg_sha256: str,
    ffprobe: Path,
    ffprobe_sha256: str,
    deployment_root: Path,
    max_run_seconds: int,
    cold_locator_manifest_sha256: str,
    initial_prompt: str | None = None,
    hotwords: Sequence[str] = (),
) -> dict[str, Any]:
    core = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "source_controller": _controller_reference(controller),
        "planning_policy": {
            "path": str(planning_policy),
            "sha256": planning_policy_sha256,
        },
        "tools": {
            "ffmpeg": {"path": str(ffmpeg), "sha256": ffmpeg_sha256},
            "ffprobe": {"path": str(ffprobe), "sha256": ffprobe_sha256},
        },
        "deployment": {
            "root": str(deployment_root),
            "jobs_root": str(deployment_root / "jobs"),
            "dispatch_lock": str(deployment_root / "dispatch.lock"),
            "discovery_root": str(deployment_root / "discovery"),
            "cold_locator_manifest": str(
                deployment_root / "discovery" / "cold-locators.json"
            ),
            "cold_locator_manifest_sha256": cold_locator_manifest_sha256,
            "candidate_receipt_root": str(
                deployment_root / "discovery" / "candidate-receipts"
            ),
            "queue_receipt_root": str(
                deployment_root / "discovery" / "queue-receipts"
            ),
            "status_path": str(deployment_root / "status.json"),
        },
        "execution": {
            "max_recordings_per_cycle": 1,
            "max_run_seconds": max_run_seconds,
            "initial_prompt": initial_prompt,
            "hotwords": sorted(set(hotwords), key=str.casefold),
        },
        "safety": dict(SAFETY),
    }
    identity = sha256_bytes(canonical_bytes(core))
    return _normalize_config(
        {
            **core,
            "identity_sha256": identity,
            "config_id": f"himrlongcfg_{identity[:32]}",
        }
    )


def _verify_external_inputs(value: dict[str, Any]) -> ControllerConfig:
    source = value["source_controller"]
    try:
        controller = load_controller_config(
            Path(source["path"]), source["physical_sha256"]
        )
    except Exception as error:
        raise CampaignError(f"source controller replay failed: {error}") from error
    if _controller_reference(controller) != source:
        raise CampaignError("source controller fields differ from the deployment binding")
    for row, label, executable in (
        (value["planning_policy"], "planning policy", False),
        (value["tools"]["ffmpeg"], "ffmpeg", True),
        (value["tools"]["ffprobe"], "ffprobe", True),
    ):
        body = _stable_file(Path(row["path"]), label, executable=executable)
        if sha256_bytes(body) != row["sha256"]:
            raise CampaignError(f"{label} differs from its deployment SHA-256")
        if label == "planning policy":
            policy = _strict_json(body, label)
            try:
                replayed = validate_planning_policy(policy)
            except Exception as error:
                raise CampaignError(f"planning policy replay failed: {error}") from error
            if replayed != policy:
                raise CampaignError("planning policy replay changed its semantics")
    locator_path = Path(value["deployment"]["cold_locator_manifest"])
    locator_body = _stable_file(
        locator_path,
        "cold locator manifest",
        maximum=64 * 1024 * 1024,
        allowed_modes=frozenset({0o400}),
    )
    if sha256_bytes(locator_body) != value["deployment"]["cold_locator_manifest_sha256"]:
        raise CampaignError("cold locator manifest differs from its deployment SHA-256")
    _validate_cold_locator_manifest(
        _strict_json(locator_body, "cold locator manifest"), controller
    )
    return controller


def load_campaign_config(path: Path, expected_sha256: str) -> CampaignConfig:
    expected = _digest(expected_sha256, "expected campaign config SHA-256")
    body = _stable_file(path, "long-form campaign config", allowed_modes=frozenset({0o400, 0o444}))
    if sha256_bytes(body) != expected:
        raise CampaignError("long-form campaign config differs from its external SHA-256")
    value = _strict_json(body, "long-form campaign config")
    normalized = _normalize_config(value)
    if canonical_bytes(normalized) != body:
        raise CampaignError("long-form campaign config bytes are not canonical")
    _verify_external_inputs(normalized)
    return CampaignConfig(normalized, path, expected)


def _source_stop_requested(config: CampaignConfig) -> bool:
    source = config.document["source_controller"]
    try:
        controller = load_controller_config(Path(source["path"]), source["physical_sha256"])
        control = read_control_state(controller)
    except Exception as error:
        raise CampaignError(f"source controller Stop replay failed: {error}") from error
    desired = control.get("desired_state")
    if desired not in {"running", "stopped"}:
        raise CampaignError("source controller desired state is invalid")
    return desired == "stopped"


def _ordinary_gpu_busy(config: CampaignConfig) -> bool:
    """Hold unless the running primary controller's GPU lane has no demand.

    Acquisition, preprocessing, and cold retention may continue independently.
    A second opportunity flock shared with the ordinary child supervisor closes
    the point-in-time race between this projection and actual GPU admission.
    """

    source = config.document["source_controller"]
    try:
        status = read_public_status(
            Path(source["path"]), source["physical_sha256"]
        )
    except Exception as error:
        raise CampaignError(f"source public GPU status replay failed: {error}") from error
    desired = status.get("desired_state")
    actual = status.get("actual_state")
    lifecycle = status.get("lifecycle")
    known_states = {
        "not_started",
        "starting",
        "running",
        "retrying",
        "stopping",
        "stopped",
        "completed",
        "blocked",
        "faulted",
    }
    if desired not in {"running", "stopped"}:
        raise CampaignError("source public desired state is invalid")
    if actual not in known_states or lifecycle not in known_states:
        raise CampaignError("source public actual state or lifecycle is invalid")
    if desired != "running" or actual != "running" or lifecycle != "running":
        return True
    monitor = status.get("monitor")
    if not isinstance(monitor, dict):
        return True
    gpu = monitor.get("gpu_readiness")
    if not isinstance(gpu, dict):
        return True

    def counter(field: str) -> int:
        value = gpu.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CampaignError(
                f"source public gpu_readiness.{field} counter is invalid"
            )
        return value

    known_stage_statuses = {"complete", "skipped", "held", "progressed"}
    if gpu.get("status") not in known_stage_statuses:
        raise CampaignError("source public gpu_readiness status is invalid")
    demand = [
        counter(field)
        for field in (
            "pending_batches",
            "pending_items",
            "ready_batches",
            "active_children",
            "buffered_ready_items",
        )
    ]
    current = gpu.get("current_gpu_child")
    if current is not None and not isinstance(current, dict):
        raise CampaignError("source public current GPU child is invalid")
    return any(demand) or current is not None


def _private_directory(path: Path, label: str, *, create: bool) -> Path:
    if create and not path.exists() and not path.is_symlink():
        path.mkdir(mode=0o700)
    try:
        observed = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise CampaignError(f"cannot inspect {label}: {error}") from error
    if (
        resolved != path
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise CampaignError(f"{label} must be a current-user mode-0700 directory")
    return path


def _write_new(path: Path, body: bytes, *, mode: int) -> None:
    parent = _private_directory(path.parent, "output parent", create=False)
    temporary_fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=parent)
    temporary = Path(temporary_name)
    linked = False
    try:
        offset = 0
        while offset < len(body):
            written = os.write(temporary_fd, body[offset:])
            if written <= 0:
                raise CampaignError("output write made no progress")
            offset += written
        os.fchmod(temporary_fd, mode)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
            linked = True
        except FileExistsError as error:
            raise CampaignError(f"refusing to replace existing output {path}") from error
        temporary.unlink()
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        linked = False
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        temporary.unlink(missing_ok=True)
        if linked:
            path.unlink(missing_ok=True)


def _write_mutable_status(path: Path, value: dict[str, Any]) -> None:
    body = canonical_bytes(value)
    if len(body) > 1024 * 1024:
        raise CampaignError("long-form public status exceeds its byte bound")
    parent = _private_directory(path.parent, "status parent", create=False)
    if path.exists() or path.is_symlink():
        try:
            observed = path.lstat()
        except OSError as error:
            raise CampaignError(f"cannot inspect prior long-form status: {error}") from error
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_nlink != 1
        ):
            raise CampaignError("prior long-form status has unsafe metadata")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".status.tmp-", dir=parent)
    temporary = Path(temporary_name)
    try:
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise CampaignError("status write made no progress")
            offset += written
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def materialize_config(
    *,
    controller_config: Path,
    controller_config_sha256: str,
    planning_policy: Path,
    planning_policy_sha256: str,
    ffmpeg: Path,
    ffmpeg_sha256: str,
    ffprobe: Path,
    ffprobe_sha256: str,
    deployment_root: Path,
    output: Path,
    max_run_seconds: int,
    initial_prompt: str | None = None,
    hotwords: Sequence[str] = (),
) -> CampaignConfig:
    try:
        controller = load_controller_config(controller_config, controller_config_sha256)
    except Exception as error:
        raise CampaignError(f"source controller replay failed: {error}") from error
    for path, expected, label, executable in (
        (planning_policy, planning_policy_sha256, "planning policy", False),
        (ffmpeg, ffmpeg_sha256, "ffmpeg", True),
        (ffprobe, ffprobe_sha256, "ffprobe", True),
    ):
        body = _stable_file(path, label, executable=executable)
        if sha256_bytes(body) != _digest(expected, f"{label} SHA-256"):
            raise CampaignError(f"{label} differs from its expected SHA-256")
    policy_body = _stable_file(planning_policy, "planning policy")
    policy = _strict_json(policy_body, "planning policy")
    try:
        replayed_policy = validate_planning_policy(policy)
    except Exception as error:
        raise CampaignError(f"planning policy replay failed: {error}") from error
    if replayed_policy != policy:
        raise CampaignError("planning policy replay changed its semantics")

    parent = _private_directory(deployment_root.parent, "deployment parent", create=False)
    del parent
    if deployment_root.exists() or deployment_root.is_symlink():
        raise CampaignError("refusing to reuse a long-form deployment root")
    deployment_root.mkdir(mode=0o700)
    jobs_root = deployment_root / "jobs"
    jobs_root.mkdir(mode=0o700)
    discovery_root = deployment_root / "discovery"
    discovery_root.mkdir(mode=0o700)
    candidate_receipt_root = discovery_root / "candidate-receipts"
    candidate_receipt_root.mkdir(mode=0o700)
    queue_receipt_root = discovery_root / "queue-receipts"
    queue_receipt_root.mkdir(mode=0o700)
    dispatch_lock = deployment_root / "dispatch.lock"
    lock_fd = os.open(dispatch_lock, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    os.close(lock_fd)
    locator_manifest = build_cold_locator_manifest(controller)
    locator_path = discovery_root / "cold-locators.json"
    locator_body = canonical_bytes(locator_manifest)
    _write_new(locator_path, locator_body, mode=0o400)
    document = build_config(
        controller=controller,
        planning_policy=planning_policy,
        planning_policy_sha256=planning_policy_sha256,
        ffmpeg=ffmpeg,
        ffmpeg_sha256=ffmpeg_sha256,
        ffprobe=ffprobe,
        ffprobe_sha256=ffprobe_sha256,
        deployment_root=deployment_root,
        max_run_seconds=max_run_seconds,
        initial_prompt=initial_prompt,
        hotwords=hotwords,
        cold_locator_manifest_sha256=sha256_bytes(locator_body),
    )
    _write_new(output, canonical_bytes(document), mode=0o400)
    digest_value = sha256_bytes(canonical_bytes(document))
    config = load_campaign_config(output, digest_value)
    _persist_status(config, [], lifecycle="ready")
    return config


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise CampaignError(f"cannot load required module {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


def _queue_module() -> ModuleType:
    return _load_module(
        "himr_longform_campaign_gpu_queue_v1",
        ROOT / "pipeline" / "preprocess_gpu_asr_queue_v1.py",
    )


def build_cold_locator_manifest(controller: ControllerConfig) -> dict[str, Any]:
    """Seal exact cold work-order/result locators without opening media payloads."""

    locators: list[dict[str, Any]] = []
    for reference in controller.document["campaign"]["schedules"]:
        if reference["role"] != "cold_acquisition_only_requires_chunking":
            continue
        schedule_path = Path(reference["path"])
        try:
            schedule, resolved, body = (
                archive_preprocess_handoff.background_producer.load_schedule(
                    schedule_path
                )
            )
            bundle = archive_preprocess_handoff.queue_runner._load_bundle(
                Path(schedule["queue"]["manifest_path"])
            )
        except Exception as error:
            raise CampaignError(
                f"cold schedule locator replay failed for {schedule_path}: {error}"
            ) from error
        if (
            resolved != schedule_path
            or sha256_bytes(body) != reference["sha256"]
            or schedule["schedule_id"] != reference["schedule_id"]
            or bundle["manifest"]["bundle_id"] != schedule["queue"]["bundle_id"]
        ):
            raise CampaignError("cold locator source differs from controller binding")
        for entry, order in zip(
            bundle["manifest"]["work_orders"], bundle["orders"], strict=True
        ):
            result_path = archive_preprocess_handoff.queue_runner._result_path(order)
            core = {
                "schedule": {
                    "path": str(schedule_path),
                    "sha256": reference["sha256"],
                    "schedule_id": reference["schedule_id"],
                },
                "queue": {
                    "bundle_id": bundle["manifest"]["bundle_id"],
                    "manifest_path": str(bundle["path"]),
                    "manifest_sha256": sha256_bytes(bundle["body"]),
                    "ordinal": entry["queue_ordinal"],
                    "job_id": entry["job_id"],
                },
                "work_order": copy.deepcopy(order),
                "work_order_sha256": sha256_bytes(
                    archive_preprocess_handoff.queue_runner.canonical_bytes(order)
                ),
                "result_path": str(result_path),
            }
            identity = sha256_bytes(canonical_bytes(core))
            locators.append(
                {
                    **core,
                    "locator_id": f"himrcoldloc_{identity[:32]}",
                    "identity_sha256": identity,
                }
            )
    locators.sort(
        key=lambda row: (
            row["schedule"]["schedule_id"], row["queue"]["ordinal"]
        )
    )
    if len(locators) > MAX_CANDIDATES:
        raise CampaignError("cold locator count exceeds its bound")
    core = {
        "kind": "himr_longform_cold_locator_manifest",
        "schema_version": 1,
        "source_controller": {
            "config_id": controller.config_id,
            "identity_sha256": controller.document["identity_sha256"],
            "physical_sha256": controller.physical_sha256,
        },
        "locators": locators,
        "locator_count": len(locators),
        "selection": "all_sealed_cold_acquisition_only_requires_chunking_work_orders",
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "manifest_id": f"himrcoldlocset_{identity[:32]}",
        "identity_sha256": identity,
    }


def _validate_cold_locator_manifest(
    value: Any, controller: ControllerConfig
) -> dict[str, Any]:
    item = _exact(
        value,
        "cold locator manifest",
        {
            "kind",
            "schema_version",
            "source_controller",
            "locators",
            "locator_count",
            "selection",
            "manifest_id",
            "identity_sha256",
        },
    )
    if (
        item["kind"] != "himr_longform_cold_locator_manifest"
        or item["schema_version"] != 1
        or item["selection"]
        != "all_sealed_cold_acquisition_only_requires_chunking_work_orders"
        or item["source_controller"]
        != {
            "config_id": controller.config_id,
            "identity_sha256": controller.document["identity_sha256"],
            "physical_sha256": controller.physical_sha256,
        }
        or not isinstance(item["locators"], list)
        or item["locator_count"] != len(item["locators"])
        or len(item["locators"]) > MAX_CANDIDATES
    ):
        raise CampaignError("cold locator manifest header or cardinality differs")
    seen: set[str] = set()
    ordering: list[tuple[str, int]] = []
    for locator in item["locators"]:
        row = _exact(
            locator,
            "cold locator",
            {
                "schedule",
                "queue",
                "work_order",
                "work_order_sha256",
                "result_path",
                "locator_id",
                "identity_sha256",
            },
        )
        core = {
            key: row[key]
            for key in row
            if key not in {"locator_id", "identity_sha256"}
        }
        identity = sha256_bytes(canonical_bytes(core))
        expected_path = archive_preprocess_handoff.queue_runner._result_path(
            row["work_order"]
        )
        if (
            row["identity_sha256"] != identity
            or row["locator_id"] != f"himrcoldloc_{identity[:32]}"
            or row["locator_id"] in seen
            or row["result_path"] != str(expected_path)
            or row["work_order_sha256"]
            != sha256_bytes(
                archive_preprocess_handoff.queue_runner.canonical_bytes(
                    row["work_order"]
                )
            )
            or row["queue"]["job_id"] != row["work_order"].get("job_id")
        ):
            raise CampaignError("cold locator identity or result binding differs")
        seen.add(row["locator_id"])
        ordering.append((row["schedule"]["schedule_id"], row["queue"]["ordinal"]))
    if ordering != sorted(ordering) or len(set(ordering)) != len(ordering):
        raise CampaignError("cold locators are not ordered and unique")
    core = {
        key: item[key]
        for key in item
        if key not in {"manifest_id", "identity_sha256"}
    }
    identity = sha256_bytes(canonical_bytes(core))
    if (
        item["identity_sha256"] != identity
        or item["manifest_id"] != f"himrcoldlocset_{identity[:32]}"
    ):
        raise CampaignError("cold locator manifest identity differs")
    return item


def _load_cold_locator_manifest(config: CampaignConfig) -> dict[str, Any]:
    source = config.document["source_controller"]
    controller = load_controller_config(Path(source["path"]), source["physical_sha256"])
    path = Path(config.document["deployment"]["cold_locator_manifest"])
    body = _stable_file(
        path,
        "cold locator manifest",
        maximum=64 * 1024 * 1024,
        allowed_modes=frozenset({0o400}),
    )
    if sha256_bytes(body) != config.document["deployment"]["cold_locator_manifest_sha256"]:
        raise CampaignError("cold locator manifest physical identity differs")
    value = _strict_json(body, "cold locator manifest")
    replayed = _validate_cold_locator_manifest(value, controller)
    if canonical_bytes(replayed) != body:
        raise CampaignError("cold locator manifest bytes are not canonical")
    return replayed


def _queue_manifest_paths(config: CampaignConfig) -> list[Path]:
    queues = Path(config.document["source_controller"]["gpu_queue_root"]) / "queues"
    if not queues.exists():
        return []
    _private_directory(queues, "source GPU queues directory", create=False)
    paths: list[Path] = []
    try:
        entries = sorted(os.scandir(queues), key=lambda row: row.name)
    except OSError as error:
        raise CampaignError(f"cannot enumerate source GPU queues: {error}") from error
    if len(entries) > MAX_QUEUE_MANIFESTS:
        raise CampaignError("source GPU queue count exceeds its scan bound")
    for entry in entries:
        if entry.name.startswith("."):
            if entry.name in {".writer.lock"}:
                continue
            raise CampaignError("source GPU queues directory has an unsupported hidden entry")
        if not entry.is_dir(follow_symlinks=False) or not re.fullmatch(r"gpuasrqueue_[0-9a-f]{32}", entry.name):
            raise CampaignError("source GPU queues directory has an unsupported entry")
        paths.append(queues / entry.name / "manifest.json")
    return paths


def _queue_anchor_documents(
    config: CampaignConfig, queue_module: ModuleType
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Replay only the two small external queue anchors, never media payloads."""

    source = config.document["source_controller"]
    try:
        registration, registration_path, registration_digest = (
            queue_module._load_registration(
                Path(source["root_registration"]),
                source["root_registration_sha256"],
            )
        )
        registration_reference = queue_module._registration_reference(
            registration, registration_path, registration_digest
        )
    except Exception as error:
        raise CampaignError(f"source queue root anchor replay failed: {error}") from error
    profile_path = Path(source["production_profile"])
    profile_body = _stable_file(
        profile_path,
        "source queue production profile",
        maximum=1024 * 1024,
        allowed_modes=frozenset({0o400, 0o444}),
    )
    if sha256_bytes(profile_body) != source["production_profile_sha256"]:
        raise CampaignError("source queue production profile differs from its binding")
    try:
        profile = queue_module.PROFILE_V2.validate_profile(
            _strict_json(profile_body, "source queue production profile")
        )
    except Exception as error:
        raise CampaignError(f"source queue production profile replay failed: {error}") from error
    if queue_module.PROFILE_V2.canonical_bytes(profile) != profile_body:
        raise CampaignError("source queue production profile is not canonical")
    observed = profile_path.lstat()
    try:
        relative = profile_path.relative_to(Path(registration["path"])).as_posix()
    except (KeyError, ValueError) as error:
        raise CampaignError("source queue production profile is outside its root") from error
    profile_reference = {
        "path": str(profile_path),
        "relative_path": relative,
        "physical_sha256": sha256_bytes(profile_body),
        "byte_count": len(profile_body),
        "document_uid": observed.st_uid,
        "document_mode": f"{stat.S_IMODE(observed.st_mode):04o}",
        "profile_id": profile["profile_id"],
        "identity_sha256": profile["identity_sha256"],
    }
    return registration_reference, profile, profile_reference


def _queue_file_metadata(path: Path) -> dict[str, int]:
    try:
        observed = path.lstat()
    except OSError as error:
        raise CampaignError(f"cannot inspect source GPU queue manifest: {error}") from error
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_uid not in {os.geteuid(), 0}
        or stat.S_IMODE(observed.st_mode) != 0o400
        or observed.st_nlink != 1
        or observed.st_size < 1
        or observed.st_size > 32 * 1024 * 1024
    ):
        raise CampaignError("source GPU queue manifest has unsafe metadata")
    return {
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "mode": observed.st_mode,
        "uid": observed.st_uid,
        "link_count": observed.st_nlink,
        "byte_count": observed.st_size,
        "mtime_ns": observed.st_mtime_ns,
        "ctime_ns": observed.st_ctime_ns,
    }


def _queue_candidate(
    queue: dict[str, Any], queue_body: bytes, manifest_path: Path, member: dict[str, Any]
) -> dict[str, Any]:
    disposition = member["resource_disposition"]
    if disposition["state"] != "requires_chunking":
        raise CampaignError("queue candidate is not marked requires_chunking")
    if member["private_handling"] is not None:
        raise CampaignError("requires-chunking member carries private handling")
    audio = member["audio"]
    try:
        preprocess_result = member["lineage"]["preprocess_result"]
        candidate_core = {
            "candidate_kind": "gpu_queue_requires_chunking",
            "queue": {
                "path": str(manifest_path),
                "sha256": sha256_bytes(queue_body),
                "queue_id": queue["queue_id"],
            },
            "member": {
                "ordinal": member["ordinal"],
                "member_id": member["member_id"],
                "identity_sha256": member["identity_sha256"],
            },
            "audio": {
                key: audio[key]
                for key in (
                    "artifact_id",
                    "media_id",
                    "path",
                    "sha256",
                    "byte_count",
                    "duration_ms",
                )
            },
            "preprocess_result": {
                "path": preprocess_result["path"],
                "sha256": preprocess_result["sha256"],
            },
            "disposition": copy.deepcopy(disposition),
        }
    except (KeyError, TypeError) as error:
        raise CampaignError("requires-chunking queue member is incomplete") from error
    identity = sha256_bytes(canonical_bytes(candidate_core))
    return {**candidate_core, "identity_sha256": identity}


def _shallow_queue_manifest(
    config: CampaignConfig,
    manifest_path: Path,
    queue_module: ModuleType,
    registration_reference: dict[str, Any],
    profile: dict[str, Any],
    profile_reference: dict[str, Any],
) -> tuple[dict[str, Any], bytes, list[dict[str, Any]]]:
    """Replay the sealed queue envelope without reopening normalized audio."""

    body = _stable_file(
        manifest_path,
        "source GPU queue manifest",
        maximum=queue_module.MAX_MANIFEST_BYTES,
        allowed_modes=frozenset({0o400}),
    )
    queue = _exact(
        _strict_json(body, "source GPU queue manifest"),
        "source GPU queue manifest",
        queue_module.MANIFEST_KEYS,
    )
    if canonical_bytes(queue) != body:
        raise CampaignError("source GPU queue manifest is not canonical")
    if (
        queue["kind"] != queue_module.KIND
        or queue["schema_version"] != queue_module.SCHEMA_VERSION
        or queue["implementation_version"] != queue_module.IMPLEMENTATION_VERSION
        or queue["materializer"] != queue_module.MATERIALIZER
        or queue["safety"] != queue_module.SAFETY
        or queue["portable_root_registration"] != registration_reference
        or queue["production_profile"]
        != {"document": profile, "reference": profile_reference}
    ):
        raise CampaignError("source GPU queue header or external anchors differ")
    core = {
        key: queue[key]
        for key in queue
        if key not in {"queue_id", "identity_sha256", "queue_relative_path"}
    }
    identity = sha256_bytes(canonical_bytes(core))
    queue_id = f"{queue_module.QUEUE_ID_PREFIX}{identity[:32]}"
    expected_path = (
        Path(config.document["source_controller"]["gpu_queue_root"])
        / "queues"
        / queue_id
        / "manifest.json"
    )
    if (
        queue["identity_sha256"] != identity
        or queue["queue_id"] != queue_id
        or queue["queue_relative_path"] != f"queues/{queue_id}"
        or manifest_path != expected_path
        or queue["output"]
        != {"queue_root": config.document["source_controller"]["gpu_queue_root"]}
    ):
        raise CampaignError("source GPU queue identity or path binding differs")
    members = queue["members"]
    skips = queue["explicit_skips"]
    if (
        not isinstance(members, list)
        or not isinstance(skips, list)
        or len(members) + len(skips) > queue_module.MAX_ITEMS
    ):
        raise CampaignError("source GPU queue member arrays are invalid")
    seen_members: set[str] = set()
    member_ordinals: list[int] = []
    preprocess_ordinals: list[int] = []
    candidates: list[dict[str, Any]] = []
    for member in members:
        row = _exact(
            member,
            "source GPU queue member",
            {
                "preprocess_ordinal",
                "audio",
                "resource_disposition",
                "lineage",
                "routing_hint",
                "private_handling",
                "ordinal",
                "member_id",
                "identity_sha256",
            },
        )
        member_core = {
            key: row[key]
            for key in row
            if key not in {"member_id", "identity_sha256"}
        }
        member_identity = sha256_bytes(canonical_bytes(member_core))
        expected_member_id = f"{queue_module.MEMBER_ID_PREFIX}{member_identity[:32]}"
        audio = row["audio"]
        if not isinstance(audio, dict):
            raise CampaignError("source GPU queue audio descriptor is invalid")
        try:
            expected_disposition = queue_module._resource_disposition(audio, profile)
        except Exception as error:
            raise CampaignError(f"source GPU queue disposition replay failed: {error}") from error
        if (
            row["identity_sha256"] != member_identity
            or row["member_id"] != expected_member_id
            or row["member_id"] in seen_members
            or row["resource_disposition"] != expected_disposition
        ):
            raise CampaignError("source GPU queue member identity or disposition differs")
        seen_members.add(row["member_id"])
        member_ordinals.append(
            _integer(row["ordinal"], "source queue member ordinal", 1, queue_module.MAX_ITEMS)
        )
        preprocess_ordinals.append(
            _integer(
                row["preprocess_ordinal"],
                "source queue preprocess ordinal",
                1,
                queue_module.MAX_ITEMS,
            )
        )
        if expected_disposition["state"] == "requires_chunking":
            candidates.append(_queue_candidate(queue, body, manifest_path, row))
    if member_ordinals != list(range(1, len(members) + 1)):
        raise CampaignError("source GPU queue member ordinals are not contiguous")
    for skipped in skips:
        if not isinstance(skipped, dict):
            raise CampaignError("source GPU queue explicit skip is invalid")
        skip_core = {
            key: skipped[key]
            for key in skipped
            if key not in {"skip_id", "identity_sha256"}
        }
        skip_identity = sha256_bytes(canonical_bytes(skip_core))
        if (
            skipped.get("identity_sha256") != skip_identity
            or skipped.get("skip_id")
            != f"{queue_module.SKIP_ID_PREFIX}{skip_identity[:32]}"
        ):
            raise CampaignError("source GPU queue explicit skip identity differs")
        preprocess_ordinals.append(
            _integer(
                skipped.get("preprocess_ordinal"),
                "source queue skipped preprocess ordinal",
                1,
                queue_module.MAX_ITEMS,
            )
        )
    if sorted(preprocess_ordinals) != list(range(1, len(members) + len(skips) + 1)):
        raise CampaignError("source GPU queue preprocess coverage differs")
    ready = [row for row in members if row["resource_disposition"]["state"] == "ready"]
    chunking = [
        row
        for row in members
        if row["resource_disposition"]["state"] == "requires_chunking"
    ]
    totals = {
        "receipt_count": len(members) + len(skips),
        "member_count": len(members),
        "ready_count": len(ready),
        "requires_chunking_count": len(chunking),
        "explicit_skip_count": len(skips),
        "private_handling_descriptor_count": sum(
            row.get("private_handling") is not None for row in [*members, *skips]
        ),
        "audio_byte_count": sum(row["audio"]["byte_count"] for row in members),
        "audio_duration_ms": sum(row["audio"]["duration_ms"] for row in members),
        "ready_audio_byte_count": sum(row["audio"]["byte_count"] for row in ready),
        "ready_audio_duration_ms": sum(row["audio"]["duration_ms"] for row in ready),
        "requires_chunking_audio_byte_count": sum(
            row["audio"]["byte_count"] for row in chunking
        ),
        "requires_chunking_audio_duration_ms": sum(
            row["audio"]["duration_ms"] for row in chunking
        ),
    }
    if queue["totals"] != totals:
        raise CampaignError("source GPU queue totals differ from its members")
    return queue, body, candidates


def _queue_discovery_receipt(
    manifest_path: Path,
    queue: dict[str, Any],
    body: bytes,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    core = {
        "kind": "himr_longform_gpu_queue_discovery_receipt",
        "schema_version": 1,
        "queue": {
            "path": str(manifest_path),
            "physical_sha256": sha256_bytes(body),
            "queue_id": queue["queue_id"],
            "identity_sha256": queue["identity_sha256"],
            "file_metadata": _queue_file_metadata(manifest_path),
        },
        "candidates": copy.deepcopy(candidates),
        "policy": {
            "immutable_queue_envelope_replayed": True,
            "media_payload_replay_deferred_to_recording_input": True,
            "source_controller_mutated": False,
        },
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "receipt_id": f"himrlongqueue_{identity[:32]}",
        "identity_sha256": identity,
    }


def _validate_queue_discovery_receipt(
    value: Any, manifest_path: Path
) -> dict[str, Any]:
    item = _exact(
        value,
        "GPU queue discovery receipt",
        {
            "kind",
            "schema_version",
            "queue",
            "candidates",
            "policy",
            "receipt_id",
            "identity_sha256",
        },
    )
    queue = _exact(
        item["queue"],
        "GPU queue discovery receipt queue",
        {
            "path",
            "physical_sha256",
            "queue_id",
            "identity_sha256",
            "file_metadata",
        },
    )
    if (
        item["kind"] != "himr_longform_gpu_queue_discovery_receipt"
        or item["schema_version"] != 1
        or item["policy"]
        != {
            "immutable_queue_envelope_replayed": True,
            "media_payload_replay_deferred_to_recording_input": True,
            "source_controller_mutated": False,
        }
        or queue["path"] != str(manifest_path)
        or queue["queue_id"] != manifest_path.parent.name
        or not isinstance(item["candidates"], list)
        or len(item["candidates"]) > 128
    ):
        raise CampaignError("GPU queue discovery receipt binding differs")
    _digest(queue["physical_sha256"], "receipt queue physical SHA-256")
    _digest(queue["identity_sha256"], "receipt queue identity")
    seen: set[str] = set()
    for candidate in item["candidates"]:
        if not isinstance(candidate, dict):
            raise CampaignError("GPU queue discovery receipt candidate is invalid")
        candidate_core = {
            key: candidate[key] for key in candidate if key != "identity_sha256"
        }
        identity = sha256_bytes(canonical_bytes(candidate_core))
        if (
            candidate.get("candidate_kind") != "gpu_queue_requires_chunking"
            or candidate.get("identity_sha256") != identity
            or identity in seen
            or candidate.get("queue")
            != {
                "path": str(manifest_path),
                "sha256": queue["physical_sha256"],
                "queue_id": queue["queue_id"],
            }
            or candidate.get("disposition", {}).get("state")
            != "requires_chunking"
        ):
            raise CampaignError("GPU queue discovery receipt candidate differs")
        seen.add(identity)
    core = {
        key: item[key]
        for key in item
        if key not in {"receipt_id", "identity_sha256"}
    }
    identity = sha256_bytes(canonical_bytes(core))
    if (
        item["identity_sha256"] != identity
        or item["receipt_id"] != f"himrlongqueue_{identity[:32]}"
    ):
        raise CampaignError("GPU queue discovery receipt identity differs")
    observed_metadata = _queue_file_metadata(manifest_path)
    recorded_metadata = _exact(
        queue["file_metadata"],
        "GPU queue discovery receipt file metadata",
        set(observed_metadata),
    )
    if any(type(value) is not int or value < 0 for value in recorded_metadata.values()):
        raise CampaignError("GPU queue discovery receipt file metadata is invalid")
    if recorded_metadata != observed_metadata:
        # A reboot can change st_dev, and an exact copy can change inode/times.
        # Those observations are a cache witness, not the queue's identity.
        # Revalidate only the small sealed envelope under its original digest;
        # never rewrite the historical receipt or replay normalized audio here.
        body = _stable_file(
            manifest_path,
            "source GPU queue manifest",
            maximum=32 * 1024 * 1024,
            allowed_modes=frozenset({0o400}),
        )
        if sha256_bytes(body) != queue["physical_sha256"]:
            raise CampaignError("GPU queue discovery receipt manifest SHA-256 differs")
    return item


def discover_gpu_queue_candidates(
    config: CampaignConfig, *, admit_new: bool = False
) -> list[dict[str, Any]]:
    """Replay immutable receipts; envelope-validate each queue only once."""

    manifests = {
        path.parent.name: path for path in _queue_manifest_paths(config)
    }
    receipt_root = _private_directory(
        Path(config.document["deployment"]["queue_receipt_root"]),
        "GPU queue discovery receipt root",
        create=False,
    )
    try:
        receipt_entries = sorted(os.scandir(receipt_root), key=lambda row: row.name)
    except OSError as error:
        raise CampaignError(f"cannot scan GPU queue discovery receipts: {error}") from error
    if len(receipt_entries) > MAX_QUEUE_MANIFESTS:
        raise CampaignError("GPU queue discovery receipt count exceeds its bound")
    receipts: dict[str, dict[str, Any]] = {}
    candidates: list[dict[str, Any]] = []
    for entry in receipt_entries:
        if (
            not entry.is_file(follow_symlinks=False)
            or not re.fullmatch(r"gpuasrqueue_[0-9a-f]{32}\.json", entry.name)
        ):
            raise CampaignError("GPU queue discovery receipt root has an unsupported entry")
        queue_id = entry.name[:-5]
        manifest_path = manifests.get(queue_id)
        if manifest_path is None or queue_id in receipts:
            raise CampaignError("GPU queue discovery receipt lacks a unique source queue")
        body = _stable_file(
            Path(entry.path),
            "GPU queue discovery receipt",
            maximum=16 * 1024 * 1024,
            allowed_modes=frozenset({0o400}),
        )
        receipt = _validate_queue_discovery_receipt(
            _strict_json(body, "GPU queue discovery receipt"), manifest_path
        )
        if canonical_bytes(receipt) != body:
            raise CampaignError("GPU queue discovery receipt is not canonical")
        receipts[queue_id] = receipt
        candidates.extend(receipt["candidates"])
    if admit_new and len(receipts) != len(manifests):
        queue_module = _queue_module()
        anchors = _queue_anchor_documents(config, queue_module)
        for queue_id, manifest_path in sorted(manifests.items()):
            if queue_id in receipts:
                continue
            queue, queue_body, discovered = _shallow_queue_manifest(
                config, manifest_path, queue_module, *anchors
            )
            receipt = _queue_discovery_receipt(
                manifest_path, queue, queue_body, discovered
            )
            _write_new(
                receipt_root / f"{queue_id}.json",
                canonical_bytes(receipt),
                mode=0o400,
            )
            receipts[queue_id] = receipt
            candidates.extend(discovered)
    seen_members: set[str] = set()
    for candidate in candidates:
        member_id = candidate["member"]["member_id"]
        if member_id in seen_members:
            raise CampaignError("source queues repeat a long-form member ID")
        seen_members.add(member_id)
    if len(candidates) > MAX_CANDIDATES:
        raise CampaignError("long-form candidate count exceeds its scan bound")
    candidates.sort(
        key=lambda row: (row["queue"]["queue_id"], row["member"]["ordinal"])
    )
    return candidates


def _cold_candidate(locator: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    try:
        duration_ms = state["result"]["admission"]["normalized_probe"]["format"][
            "duration_ms"
        ]
    except (KeyError, TypeError) as error:
        raise CampaignError("cold acquisition result lacks an admitted duration") from error
    duration_ms = _integer(duration_ms, "cold source duration", 1, 2**63 - 1)
    core = {
        "candidate_kind": "cold_schedule_completed_acquisition",
        "schedule": copy.deepcopy(locator["schedule"]),
        "queue": {
            "bundle_id": locator["queue"]["bundle_id"],
            "ordinal": locator["queue"]["ordinal"],
            "job_id": locator["queue"]["job_id"],
        },
        "acquisition_result": {
            "path": locator["result_path"],
            "sha256": state["result_sha256"],
        },
        "source_media": {
            "sha256": state["media_sha256"],
            "byte_count": state["byte_count"],
            "duration_ms": duration_ms,
        },
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {**core, "identity_sha256": identity}


def _cold_candidate_receipt(locator: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    core = {
        "kind": "himr_longform_cold_candidate_receipt",
        "schema_version": 1,
        "locator_id": locator["locator_id"],
        "locator_identity_sha256": locator["identity_sha256"],
        "candidate": copy.deepcopy(candidate),
        "policy": {
            "immutable_deep_validation_receipt": True,
            "source_controller_mutated": False,
        },
    }
    identity = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "receipt_id": f"himrcoldcandidate_{identity[:32]}",
        "identity_sha256": identity,
    }


def _validate_cold_candidate_receipt(
    value: Any, locator: dict[str, Any]
) -> dict[str, Any]:
    item = _exact(
        value,
        "cold candidate receipt",
        {
            "kind",
            "schema_version",
            "locator_id",
            "locator_identity_sha256",
            "candidate",
            "policy",
            "receipt_id",
            "identity_sha256",
        },
    )
    candidate = item["candidate"]
    if (
        item["kind"] != "himr_longform_cold_candidate_receipt"
        or item["schema_version"] != 1
        or item["locator_id"] != locator["locator_id"]
        or item["locator_identity_sha256"] != locator["identity_sha256"]
        or item["policy"]
        != {
            "immutable_deep_validation_receipt": True,
            "source_controller_mutated": False,
        }
        or not isinstance(candidate, dict)
        or candidate.get("candidate_kind")
        != "cold_schedule_completed_acquisition"
        or candidate.get("schedule") != locator["schedule"]
        or candidate.get("queue", {}).get("bundle_id")
        != locator["queue"]["bundle_id"]
        or candidate.get("queue", {}).get("ordinal")
        != locator["queue"]["ordinal"]
        or candidate.get("queue", {}).get("job_id")
        != locator["queue"]["job_id"]
        or candidate.get("acquisition_result", {}).get("path")
        != locator["result_path"]
    ):
        raise CampaignError("cold candidate receipt differs from its locator")
    candidate_core = {
        key: candidate[key] for key in candidate if key != "identity_sha256"
    }
    if candidate.get("identity_sha256") != sha256_bytes(canonical_bytes(candidate_core)):
        raise CampaignError("cold candidate semantic identity differs")
    core = {
        key: item[key]
        for key in item
        if key not in {"receipt_id", "identity_sha256"}
    }
    identity = sha256_bytes(canonical_bytes(core))
    if (
        item["identity_sha256"] != identity
        or item["receipt_id"] != f"himrcoldcandidate_{identity[:32]}"
    ):
        raise CampaignError("cold candidate receipt identity differs")
    return item


def _cold_scheduling_hint(locator: dict[str, Any]) -> tuple[int, str]:
    """Read only the small result envelope for ordering; never admit from it."""

    path = Path(locator["result_path"])
    body = _stable_file(
        path,
        "cold acquisition scheduling envelope",
        maximum=16 * 1024 * 1024,
    )
    value = _strict_json(body, "cold acquisition scheduling envelope")
    try:
        duration = value["admission"]["normalized_probe"]["format"]["duration_ms"]
    except (KeyError, TypeError) as error:
        raise CampaignError("cold scheduling envelope lacks duration metadata") from error
    if (
        value.get("status") != "completed"
        or value.get("dry_run") is not False
        or value.get("job_id") != locator["queue"]["job_id"]
        or value.get("result_path") != locator["result_path"]
    ):
        raise CampaignError("cold scheduling envelope differs from its locator")
    return (
        _integer(duration, "cold scheduling duration", 1, 2**63 - 1),
        locator["identity_sha256"],
    )


def _inspect_cold_result_with_admission_retry(
    work_order: dict[str, Any],
) -> dict[str, Any] | None:
    """Repeat strict replay only across the bounded atomic-publication window."""

    try:
        installed = _install_acquisition_directory_identity_adapter(
            archive_preprocess_handoff.queue_runner,
            required=True,
        )
        if installed is not True:
            raise SealedBackendError(
                "required acquisition directory identity adapter was not installed"
            )
    except SealedBackendError as error:
        raise CampaignError(
            f"cold result directory identity adapter failed: {error}"
        ) from error
    retry_limit = archive_preprocess_handoff.RESULT_ADMISSION_RETRY_LIMIT
    race_messages = archive_preprocess_handoff.RESULT_ADMISSION_RACE_MESSAGES
    for attempt in range(1, retry_limit + 1):
        try:
            return archive_preprocess_handoff.queue_runner._inspect_result(work_order)
        except archive_preprocess_handoff.queue_runner.QueueRunnerError as error:
            transient_shape = any(message in str(error) for message in race_messages)
            if not transient_shape or attempt == retry_limit:
                raise
            # Each attempt repeats the complete immutable replay.  This tolerates
            # only a recognized concurrent publication race; malformed or changed
            # result and payload leaves remain fatal.
            time.sleep(archive_preprocess_handoff.RESULT_ADMISSION_RETRY_SECONDS)
    raise CampaignError("unreachable cold result-admission retry state")


def discover_cold_schedule_candidates(
    config: CampaignConfig,
    *,
    admit_new: bool = False,
    maximum_new_receipts: int = 1,
) -> list[dict[str, Any]]:
    """Replay receipts and deeply inspect only newly appearing result paths."""

    maximum_new_receipts = _integer(
        maximum_new_receipts, "maximum new cold candidate receipts", 0, 8
    )
    manifest = _load_cold_locator_manifest(config)
    locators = {row["locator_id"]: row for row in manifest["locators"]}
    receipt_root = _private_directory(
        Path(config.document["deployment"]["candidate_receipt_root"]),
        "cold candidate receipt root",
        create=False,
    )
    try:
        entries = sorted(os.scandir(receipt_root), key=lambda row: row.name)
    except OSError as error:
        raise CampaignError(f"cannot scan cold candidate receipts: {error}") from error
    if len(entries) > len(locators):
        raise CampaignError("cold candidate receipts exceed locator cardinality")
    candidates: list[dict[str, Any]] = []
    admitted: set[str] = set()
    for entry in entries:
        if (
            not entry.is_file(follow_symlinks=False)
            or not re.fullmatch(r"himrcoldloc_[0-9a-f]{32}\.json", entry.name)
        ):
            raise CampaignError("cold candidate receipt root has an unsupported entry")
        locator_id = entry.name[:-5]
        locator = locators.get(locator_id)
        if locator is None or locator_id in admitted:
            raise CampaignError("cold candidate receipt has no unique locator")
        body = _stable_file(
            Path(entry.path),
            "cold candidate receipt",
            maximum=256 * 1024,
            allowed_modes=frozenset({0o400}),
        )
        receipt = _validate_cold_candidate_receipt(
            _strict_json(body, "cold candidate receipt"), locator
        )
        if canonical_bytes(receipt) != body:
            raise CampaignError("cold candidate receipt bytes are not canonical")
        admitted.add(locator_id)
        candidates.append(receipt["candidate"])

    remaining_budget = maximum_new_receipts if admit_new else 0
    appearing: list[tuple[tuple[int, str], dict[str, Any]]] = []
    for locator in manifest["locators"]:
        if locator["locator_id"] in admitted:
            continue
        # This is the only recurring probe for a pending acquisition: no JSON or
        # media payload is opened until the exact result leaf appears.
        try:
            observed = Path(locator["result_path"]).lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise CampaignError(f"cannot inspect pending cold result path: {error}") from error
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
            raise CampaignError("appearing cold result path is not a regular file")
        if remaining_budget:
            appearing.append((_cold_scheduling_hint(locator), locator))
    for _hint, locator in sorted(appearing, key=lambda row: row[0])[
        :remaining_budget
    ]:
        try:
            state = _inspect_cold_result_with_admission_retry(locator["work_order"])
        except Exception as error:
            raise CampaignError(
                f"new cold acquisition result failed deep replay: {error}"
            ) from error
        if state is None:
            raise CampaignError("appearing cold result disappeared during deep replay")
        candidate = _cold_candidate(locator, state)
        receipt = _cold_candidate_receipt(locator, candidate)
        _write_new(
            receipt_root / f"{locator['locator_id']}.json",
            canonical_bytes(receipt),
            mode=0o400,
        )
        admitted.add(locator["locator_id"])
        candidates.append(candidate)
    candidates.sort(key=lambda row: row["identity_sha256"])
    return candidates


def discover_candidates(
    config: CampaignConfig,
    *,
    admit_new_cold: bool = False,
    admit_new_queues: bool = False,
) -> list[dict[str, Any]]:
    """Discover both already-preprocessed and cold-primary long recordings."""

    candidates = [
        *discover_gpu_queue_candidates(config, admit_new=admit_new_queues),
        *discover_cold_schedule_candidates(
            config, admit_new=admit_new_cold, maximum_new_receipts=1
        ),
    ]
    seen: set[str] = set()
    for candidate in candidates:
        identity = candidate["identity_sha256"]
        if identity in seen:
            raise CampaignError("long-form discovery repeated a candidate identity")
        seen.add(identity)
    candidates.sort(
        key=lambda row: (
            0
            if row["candidate_kind"] == "gpu_queue_requires_chunking"
            else 1,
            row["identity_sha256"],
        )
    )
    return candidates


def _job(config: CampaignConfig, candidate: dict[str, Any]) -> dict[str, Any]:
    core = {
        "campaign_config_id": config.config_id,
        "campaign_config_identity_sha256": config.document["identity_sha256"],
        "candidate_identity_sha256": candidate["identity_sha256"],
        "candidate_kind": candidate["candidate_kind"],
    }
    identity = sha256_bytes(canonical_bytes(core))
    job_id = f"himrlongjob_{identity[:32]}"
    root = Path(config.document["deployment"]["jobs_root"]) / job_id
    return {
        "kind": "himr_longform_asr_campaign_job",
        "schema_version": 1,
        "job_id": job_id,
        "identity_sha256": identity,
        "source": copy.deepcopy(candidate),
        "paths": {
            "root": str(root),
            "recording_input": str(root / "recording-input.json"),
            "plan": str(root / "plan.json"),
            "results": str(root / "results"),
            "bindings": str(root / "span-bindings.json"),
            "transcript": str(root / "recording-transcript.json"),
            "completion": str(root / "completion.json"),
            "cleanup": str(root / "cleanup.json"),
            "preprocess_selection": str(root / "preprocess" / "selection.json"),
            "preprocess_bundle_root": str(root / "preprocess" / "bundles"),
            "preprocess_output_root": str(root / "preprocess" / "output"),
            "preprocess_state_root": str(root / "preprocess" / "state"),
        },
        "campaign": core,
    }


def _read_identical_or_write(path: Path, value: dict[str, Any], *, mode: int = 0o400) -> None:
    body = canonical_bytes(value)
    if path.exists() and not path.is_symlink():
        observed = _stable_file(path, path.name, maximum=64 * 1024 * 1024, allowed_modes=frozenset({mode}))
        if observed != body:
            raise CampaignError(f"existing {path.name} differs from deterministic replay")
        return
    _write_new(path, body, mode=mode)


def _ensure_job_root(config: CampaignConfig, job: dict[str, Any]) -> Path:
    job_root = Path(job["paths"]["root"])
    jobs_root = _private_directory(
        Path(config.document["deployment"]["jobs_root"]),
        "jobs root",
        create=False,
    )
    if job_root.parent != jobs_root or not JOB_ID_RE.fullmatch(job_root.name):
        raise CampaignError("job root escaped the deployment jobs root")
    if not job_root.exists() and not job_root.is_symlink():
        job_root.mkdir(mode=0o700)
    return _private_directory(job_root, "job root", create=False)


def _ensure_private_descendant(path: Path, root: Path, label: str) -> Path:
    if root not in path.parents:
        raise CampaignError(f"{label} escaped its job root")
    chain: list[Path] = []
    current = path
    while current != root:
        chain.append(current)
        current = current.parent
    for candidate in reversed(chain):
        if not candidate.exists() and not candidate.is_symlink():
            candidate.mkdir(mode=0o700)
        _private_directory(candidate, label, create=False)
    return path


def _cold_preprocess_source(
    config: CampaignConfig,
    candidate: dict[str, Any],
    job: dict[str, Any],
) -> dict[str, Any]:
    """Resume one isolated ASR-ready preprocessing job for a cold source."""

    job_root = _ensure_job_root(config, job)
    paths = job["paths"]
    preprocess_root = _ensure_private_descendant(
        job_root / "preprocess", job_root, "job preprocess root"
    )
    bundle_root = _ensure_private_descendant(
        Path(paths["preprocess_bundle_root"]), job_root, "job preprocess bundle root"
    )
    output_root = _ensure_private_descendant(
        Path(paths["preprocess_output_root"]), job_root, "job preprocess output root"
    )
    if shutil.disk_usage(output_root).free < HOT_SCRATCH_FREE_FLOOR_BYTES:
        return {
            "status": "held",
            "reason": "hot_scratch_free_space_below_64_gib_floor",
        }
    state_root = _ensure_private_descendant(
        Path(paths["preprocess_state_root"]), job_root, "job preprocess state root"
    )
    selection_path = Path(paths["preprocess_selection"])
    acquisition_path = Path(candidate["acquisition_result"]["path"])
    intended = preprocess_batch.build_selection([acquisition_path])
    if selection_path.exists() and not selection_path.is_symlink():
        selection, _body, replayed_path = preprocess_batch.read_selection(selection_path)
        if selection != intended or replayed_path != selection_path:
            raise CampaignError("cold preprocess selection differs from deterministic replay")
    else:
        selection = preprocess_batch.write_selection(
            [acquisition_path], selection_path
        )
        if selection != intended:
            raise CampaignError("cold preprocess selection changed during admission")
    bundle_path = preprocess_batch.materialize_bundle(
        selection_path,
        bundle_root,
        output_root,
        operation_profile="asr-ready",
    )
    manifest, selection, orders = preprocess_batch.validate_bundle(bundle_path)
    selected_acquisition = None
    if len(selection["entries"]) == 1:
        acquisition = selection["entries"][0]["acquisition_result"]
        selected_acquisition = {
            "path": acquisition["path"],
            "sha256": acquisition["sha256"],
        }
    if (
        len(orders) != 1
        or manifest["work_order_count"] != 1
        or orders[0]["operations"] != preprocess_batch.ASR_READY_OPERATIONS
        or selected_acquisition
        != {
            "path": candidate["acquisition_result"]["path"],
            "sha256": candidate["acquisition_result"]["sha256"],
        }
    ):
        raise CampaignError("cold preprocess bundle differs from its candidate")
    existing = preprocess_batch.existing_receipts(
        state_root,
        manifest=manifest,
        selection=selection,
        orders=orders,
    )
    if 1 not in existing:
        if _source_stop_requested(config):
            return {"status": "held", "reason": "durable_stop_requested"}
        outcome = _strict_subprocess(
            [
                str(ROOT / "pipeline" / "bin" / "preprocess-batch"),
                "run",
                "--bundle",
                str(bundle_path),
                "--state-root",
                str(state_root),
                "--limit",
                "1",
            ],
            timeout=config.document["execution"]["max_run_seconds"],
            label="cold ASR-ready preprocess",
            stop_config=config,
        )
        if outcome.get("status") == "held":
            return outcome
        existing = preprocess_batch.existing_receipts(
            state_root,
            manifest=manifest,
            selection=selection,
            orders=orders,
        )
    if set(existing) != {1}:
        raise CampaignError("cold preprocess receipt set is incomplete")
    # ``existing_receipts`` returns the validated receipt together with its
    # physical digest.  Keep that storage-envelope contract intact here; the
    # cold campaign only needs the receipt payload for candidate binding.
    receipt = existing[1]["receipt"]
    if (
        receipt["acquisition_result"]["path"]
        != candidate["acquisition_result"]["path"]
        or receipt["acquisition_result"]["sha256"]
        != candidate["acquisition_result"]["sha256"]
        or receipt["source_media"]["sha256"]
        != candidate["source_media"]["sha256"]
        or receipt["source_media"]["byte_count"]
        != candidate["source_media"]["byte_count"]
    ):
        raise CampaignError("cold preprocess receipt differs from its acquisition candidate")
    audio_rows = [
        row
        for row in receipt["artifacts"]
        if row.get("artifact_kind") == "audio_16khz_mono_flac"
    ]
    if len(audio_rows) != 1:
        raise CampaignError("cold preprocess receipt lacks one normalized audio artifact")
    audio = audio_rows[0]
    return {
        "status": "completed",
        "preprocess_result": copy.deepcopy(receipt["preprocess_result"]),
        "media_id": receipt["source_media"]["media_id"],
        "audio": {
            key: audio[key]
            for key in ("artifact_id", "path", "sha256", "byte_count")
        },
    }


def _prepared_source(
    config: CampaignConfig,
    candidate: dict[str, Any],
    job: dict[str, Any],
) -> dict[str, Any]:
    if candidate["candidate_kind"] == "gpu_queue_requires_chunking":
        return {
            "status": "completed",
            "preprocess_result": copy.deepcopy(candidate["preprocess_result"]),
            "media_id": candidate["audio"]["media_id"],
            "audio": copy.deepcopy(candidate["audio"]),
        }
    if candidate["candidate_kind"] == "cold_schedule_completed_acquisition":
        return _cold_preprocess_source(config, candidate, job)
    raise CampaignError("long-form candidate kind is unsupported")


def _duration_ms_matches_exact_samples(
    duration_ms: Any, manifest_audio: dict[str, Any]
) -> bool:
    """Accept either nearest millisecond only at an exact half-ms tie.

    Existing preprocess receipts use Python's ties-to-even rounding, while the
    recording-first manifest uses ties-up rounding.  The two representations can
    differ by one millisecond only when the exact FLAC duration lies precisely
    halfway between two millisecond integers.  Compare in sample space so an
    ordinary one-millisecond metadata error still fails closed.
    """

    admitted = _integer(duration_ms, "admitted audio duration", 1, 2**63 - 1)
    sample_rate = _integer(
        manifest_audio.get("sample_rate_hz"),
        "prepared audio sample rate",
        1,
        2**63 - 1,
    )
    total_samples = _integer(
        manifest_audio.get("total_samples"),
        "prepared audio sample count",
        1,
        2**63 - 1,
    )
    # duration_ms * sample_rate and total_samples * 1000 have common units.
    # A nearest-integer millisecond is at most half a millisecond away.
    error = abs(admitted * sample_rate - total_samples * 1_000)
    return error * 2 <= sample_rate


def prepare_candidate(config: CampaignConfig, candidate: dict[str, Any]) -> dict[str, Any]:
    job = _job(config, candidate)
    job_root = _ensure_job_root(config, job)
    source = _prepared_source(config, candidate, job)
    if source.get("status") == "held":
        return {
            "status": "held",
            "reason": source["reason"],
            "job_id": job["job_id"],
            "job": job,
        }

    manifest = longform_asr_input.build_manifest(
        Path(source["preprocess_result"]["path"]),
        recording_id=job["job_id"],
        media_id=source["media_id"],
        ffprobe=Path(config.document["tools"]["ffprobe"]["path"]),
        expected_ffprobe_sha256=config.document["tools"]["ffprobe"]["sha256"],
        include_routing=True,
    )
    manifest_audio = manifest["recording"]["input"]
    comparable = ("artifact_id", "path", "sha256", "byte_count")
    if {key: manifest_audio[key] for key in comparable} != {
        key: source["audio"][key] for key in comparable
    }:
        raise CampaignError("prepared recording differs from its admitted queue member")
    if "duration_ms" in source["audio"]:
        if not _duration_ms_matches_exact_samples(
            manifest_audio["duration_ms"], manifest_audio
        ):
            raise CampaignError(
                "prepared recording duration differs from its exact sample count"
            )
        if not _duration_ms_matches_exact_samples(
            source["audio"]["duration_ms"], manifest_audio
        ):
            raise CampaignError(
                "prepared recording duration differs from its admitted queue member"
            )
    policy_row = config.document["planning_policy"]
    policy_body = _stable_file(Path(policy_row["path"]), "planning policy")
    policy = _strict_json(policy_body, "planning policy")
    plan = build_longform_asr_plan(manifest, policy)
    _read_identical_or_write(Path(job["paths"]["recording_input"]), manifest)
    _read_identical_or_write(Path(job["paths"]["plan"]), plan)
    _read_identical_or_write(job_root / "job.json", job)
    return {
        "status": "prepared",
        "job_id": job["job_id"],
        "job": job,
        "plan_id": plan["plan_id"],
        "strategy": plan["strategy"],
        "span_count": len(plan["spans"]),
    }


def _job_status(job: dict[str, Any]) -> str:
    paths = job["paths"]
    if Path(paths["completion"]).exists():
        return "completed"
    if Path(paths["transcript"]).exists() or Path(paths["bindings"]).exists():
        return "incomplete"
    if Path(paths["plan"]).exists():
        return "prepared"
    state_root = Path(paths["preprocess_state_root"])
    if state_root.exists():
        return "preprocessed"
    return "unprepared"


def _candidate_duration(candidate: dict[str, Any]) -> int:
    value = (
        candidate["audio"]["duration_ms"]
        if candidate["candidate_kind"] == "gpu_queue_requires_chunking"
        else candidate["source_media"]["duration_ms"]
    )
    return _integer(value, "candidate duration", 1, 2**63 - 1)


def _select_next_candidate(
    config: CampaignConfig, candidates: Sequence[dict[str, Any]]
) -> dict[str, Any] | None:
    eligible = [
        candidate
        for candidate in candidates
        if _job_status(_job(config, candidate)) != "completed"
    ]
    if not eligible:
        return None
    return min(
        eligible,
        key=lambda row: (_candidate_duration(row), row["identity_sha256"]),
    )


def _status_projection(
    config: CampaignConfig,
    candidates: Sequence[dict[str, Any]],
    *,
    lifecycle: str,
    active_job: str | None,
    last_error: dict[str, str] | None,
) -> dict[str, Any]:
    if lifecycle not in {"ready", "running", "waiting", "stopped", "faulted"}:
        raise CampaignError("long-form lifecycle is invalid")
    manifest = _load_cold_locator_manifest(config)
    counts = {
        "unprepared": 0,
        "preprocessed": 0,
        "prepared": 0,
        "incomplete": 0,
        "completed": 0,
    }
    cold = 0
    queue = 0
    for candidate in candidates:
        counts[_job_status(_job(config, candidate))] += 1
        if candidate["candidate_kind"] == "cold_schedule_completed_acquisition":
            cold += 1
        else:
            queue += 1
    return {
        "kind": "himr_longform_asr_campaign_status",
        "schema_version": 1,
        "source_controller": {
            "config_id": config.document["source_controller"]["config_id"],
            "physical_sha256": config.document["source_controller"]["physical_sha256"],
        },
        "campaign_config": {
            "config_id": config.config_id,
            "physical_sha256": config.physical_sha256,
        },
        "lifecycle": lifecycle,
        "expected_cold_backlog": manifest["locator_count"],
        "discovered": {
            "cold_candidates": cold,
            "queue_candidates": queue,
            "total_candidates": len(candidates),
        },
        "jobs": counts,
        "active_job": active_job,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "last_error": last_error,
    }


def _persist_status(
    config: CampaignConfig,
    candidates: Sequence[dict[str, Any]],
    *,
    lifecycle: str,
    active_job: str | None = None,
    last_error: dict[str, str] | None = None,
) -> dict[str, Any]:
    value = _status_projection(
        config,
        candidates,
        lifecycle=lifecycle,
        active_job=active_job,
        last_error=last_error,
    )
    _write_mutable_status(Path(config.document["deployment"]["status_path"]), value)
    return value


def _refresh_status_heartbeat(config: CampaignConfig) -> dict[str, Any]:
    """Refresh the small waiting projection without replaying candidate receipts."""

    path = Path(config.document["deployment"]["status_path"])
    body = _stable_file(
        path,
        "long-form campaign status",
        maximum=1024 * 1024,
        allowed_modes=frozenset({0o600}),
    )
    value = _strict_json(body, "long-form campaign status")
    expected_fields = {
        "kind",
        "schema_version",
        "source_controller",
        "campaign_config",
        "lifecycle",
        "expected_cold_backlog",
        "discovered",
        "jobs",
        "active_job",
        "updated_at",
        "last_error",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise CampaignError("long-form campaign status fields differ")
    if (
        value["kind"] != "himr_longform_asr_campaign_status"
        or value["schema_version"] != 1
        or value["source_controller"]
        != {
            "config_id": config.document["source_controller"]["config_id"],
            "physical_sha256": config.document["source_controller"][
                "physical_sha256"
            ],
        }
        or value["campaign_config"]
        != {
            "config_id": config.config_id,
            "physical_sha256": config.physical_sha256,
        }
        or value["lifecycle"]
        not in {"ready", "running", "waiting", "stopped", "faulted"}
        or value["active_job"] is not None
        or value["last_error"] is not None
    ):
        raise CampaignError("long-form campaign status binding is invalid")
    discovered = value["discovered"]
    jobs = value["jobs"]
    if (
        not isinstance(discovered, dict)
        or set(discovered)
        != {"cold_candidates", "queue_candidates", "total_candidates"}
        or not isinstance(jobs, dict)
        or set(jobs)
        != {"unprepared", "preprocessed", "prepared", "incomplete", "completed"}
    ):
        raise CampaignError("long-form campaign status counters differ")
    counters = [value["expected_cold_backlog"], *discovered.values(), *jobs.values()]
    if any(
        isinstance(counter, bool) or not isinstance(counter, int) or counter < 0
        for counter in counters
    ):
        raise CampaignError("long-form campaign status counter is invalid")
    if (
        discovered["cold_candidates"] + discovered["queue_candidates"]
        != discovered["total_candidates"]
        or sum(jobs.values()) != discovered["total_candidates"]
    ):
        raise CampaignError("long-form campaign status totals differ")
    refreshed = {
        **value,
        "lifecycle": "waiting",
        "updated_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }
    _write_mutable_status(path, refreshed)
    return refreshed


def campaign_status(config: CampaignConfig) -> dict[str, Any]:
    candidates = discover_candidates(config)
    locator_manifest = _load_cold_locator_manifest(config)
    counts = {
        "unprepared": 0,
        "preprocessed": 0,
        "prepared": 0,
        "incomplete": 0,
        "completed": 0,
    }
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        job = _job(config, candidate)
        status = _job_status(job)
        counts[status] += 1
        row = {
            "job_id": job["job_id"],
            "status": status,
            "candidate_kind": candidate["candidate_kind"],
        }
        if candidate["candidate_kind"] == "gpu_queue_requires_chunking":
            row.update(
                {
                    "queue_id": candidate["queue"]["queue_id"],
                    "member_id": candidate["member"]["member_id"],
                    "duration_ms": candidate["audio"]["duration_ms"],
                }
            )
        else:
            row.update(
                {
                    "schedule_id": candidate["schedule"]["schedule_id"],
                    "queue_ordinal": candidate["queue"]["ordinal"],
                    "source_byte_count": candidate["source_media"]["byte_count"],
                }
            )
        rows.append(row)
    return {
        "status": "complete" if candidates and counts["completed"] == len(candidates) else "ready",
        "config_id": config.config_id,
        "candidate_count": len(candidates),
        "expected_cold_backlog": locator_manifest["locator_count"],
        "counts": counts,
        "jobs": rows,
        "gpu_invoked": False,
        "source_controller_mutated": False,
    }


@contextmanager
def _dispatch_lock(config: CampaignConfig) -> Iterator[None]:
    path = Path(config.document["deployment"]["dispatch_lock"])
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise CampaignError(f"cannot open long-form dispatch lock: {error}") from error
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or opened.st_size != 0
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
        ):
            raise CampaignError("long-form dispatch lock has unsafe metadata")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CampaignError("long-form dispatch lock is occupied") from error
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _hotwords_file(config: CampaignConfig, job: dict[str, Any]) -> Path | None:
    hotwords = config.document["execution"]["hotwords"]
    if not hotwords:
        return None
    path = Path(job["paths"]["root"]) / "hotwords.json"
    _read_identical_or_write(path, {"hotwords": hotwords})
    return path


def runner_argv(config: CampaignConfig, job: dict[str, Any]) -> list[str]:
    source = config.document["source_controller"]
    paths = job["paths"]
    argv = [
        str(ROOT / "pipeline" / "bin" / "longform-asr-runner-v1"),
        "run-plan",
        "--plan",
        paths["plan"],
        "--output-root",
        paths["results"],
        "--controller-config",
        source["path"],
        "--controller-config-sha256",
        source["physical_sha256"],
        "--ffmpeg",
        config.document["tools"]["ffmpeg"]["path"],
        "--ffmpeg-sha256",
        config.document["tools"]["ffmpeg"]["sha256"],
        "--bindings-output",
        paths["bindings"],
        "--honor-controller-stop",
        "--yield-to-ordinary-gpu",
    ]
    initial_prompt = config.document["execution"]["initial_prompt"]
    if initial_prompt is not None:
        argv.extend(("--initial-prompt", initial_prompt))
    hotwords = _hotwords_file(config, job)
    if hotwords is not None:
        argv.extend(("--hotwords-json", str(hotwords)))
    return argv


def assembler_argv(job: dict[str, Any]) -> list[str]:
    paths = job["paths"]
    return [
        str(ROOT / "pipeline" / "bin" / "longform-transcript-assembler"),
        "--parent-manifest",
        paths["plan"],
        "--span-results",
        paths["bindings"],
        "--output",
        paths["transcript"],
    ]


def _cleanup_completed_cold_scratch(
    config: CampaignConfig,
    job: dict[str, Any],
    completion: dict[str, Any],
) -> dict[str, Any]:
    """Remove only one completed cold job's derived preprocess output tree."""

    if job["source"]["candidate_kind"] != "cold_schedule_completed_acquisition":
        return {"status": "not_applicable"}
    cleanup_path = Path(job["paths"]["cleanup"])
    if cleanup_path.exists() and not cleanup_path.is_symlink():
        body = _stable_file(
            cleanup_path,
            "cold scratch cleanup receipt",
            maximum=1024 * 1024,
            allowed_modes=frozenset({0o400}),
        )
        value = _strict_json(body, "cold scratch cleanup receipt")
        if canonical_bytes(value) != body:
            raise CampaignError("cold scratch cleanup receipt is not canonical")
        return value
    completion_path = Path(job["paths"]["completion"])
    completion_body = _stable_file(
        completion_path,
        "long-form completion",
        maximum=64 * 1024 * 1024,
        allowed_modes=frozenset({0o400}),
    )
    if completion_body != canonical_bytes(completion):
        raise CampaignError("cleanup completion binding differs")
    job_root = _ensure_job_root(config, job)
    output = Path(job["paths"]["preprocess_output_root"])
    expected = job_root / "preprocess" / "output"
    if output != expected:
        raise CampaignError("cold scratch cleanup target differs from closed layout")
    tombstone = output.parent / "output.cleanup-pending"
    if output.exists() and tombstone.exists():
        raise CampaignError("cold scratch output and cleanup tombstone both exist")
    observed: dict[str, int] | None = None
    target = tombstone if tombstone.exists() else output
    if target.exists() or target.is_symlink():
        info = target.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise CampaignError("cold scratch cleanup target has unsafe metadata")
        file_count = 0
        byte_count = 0
        for directory, directory_names, file_names in os.walk(
            target, topdown=True, followlinks=False
        ):
            directory_path = Path(directory)
            directory_info = directory_path.lstat()
            if (
                stat.S_ISLNK(directory_info.st_mode)
                or not stat.S_ISDIR(directory_info.st_mode)
                or directory_info.st_uid != os.geteuid()
                or stat.S_IMODE(directory_info.st_mode) & 0o022
            ):
                raise CampaignError("cold scratch tree has an unsafe directory")
            for name in [*directory_names, *file_names]:
                child = directory_path / name
                child_info = child.lstat()
                if stat.S_ISLNK(child_info.st_mode) or child_info.st_uid != os.geteuid():
                    raise CampaignError("cold scratch tree has a symlink or foreign owner")
                if stat.S_ISREG(child_info.st_mode):
                    file_count += 1
                    byte_count += child_info.st_size
                elif not stat.S_ISDIR(child_info.st_mode):
                    raise CampaignError("cold scratch tree has an unsupported entry")
        observed = {"file_count": file_count, "byte_count": byte_count}
        if target == output:
            os.rename(output, tombstone)
            target = tombstone
        shutil.rmtree(target)
        directory_fd = os.open(
            output.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    receipt = {
        "kind": "himr_longform_derived_scratch_cleanup",
        "schema_version": 1,
        "job_id": job["job_id"],
        "completion_sha256": sha256_bytes(completion_body),
        "removed_path": str(output),
        "observed_before_cleanup": observed,
        "scope": "derived_asr_ready_preprocess_output_only",
        "source_raw_removed": False,
        "plan_removed": False,
        "span_results_removed": False,
        "transcript_removed": False,
        "receipts_removed": False,
        "status": "completed",
    }
    _read_identical_or_write(cleanup_path, receipt)
    return receipt


def _reconcile_completed_cleanup(
    config: CampaignConfig, candidates: Sequence[dict[str, Any]]
) -> None:
    for candidate in candidates:
        if candidate["candidate_kind"] != "cold_schedule_completed_acquisition":
            continue
        job = _job(config, candidate)
        completion_path = Path(job["paths"]["completion"])
        if not completion_path.exists() or completion_path.is_symlink():
            continue
        body = _stable_file(
            completion_path,
            "long-form completion",
            maximum=64 * 1024 * 1024,
            allowed_modes=frozenset({0o400}),
        )
        completion = _strict_json(body, "long-form completion")
        if not isinstance(completion, dict) or canonical_bytes(completion) != body:
            raise CampaignError("long-form completion is not canonical")
        _cleanup_completed_cold_scratch(config, job, completion)


def _strict_subprocess(
    argv: Sequence[str],
    *,
    timeout: int,
    label: str,
    stop_config: CampaignConfig | None = None,
) -> dict[str, Any]:
    if stop_config is not None and _source_stop_requested(stop_config):
        return {"status": "held", "reason": "durable_stop_requested"}
    try:
        completed = subprocess.run(
            list(argv),
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            env={
                "HOME": os.environ.get("HOME", "/nonexistent"),
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "PYTHONNOUSERSITE": "1",
            },
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise CampaignError(f"{label} failed to execute: {error}") from error
    output = completed.stdout if completed.stdout.strip() else completed.stderr
    try:
        value = _strict_json(output, f"{label} output")
    except CampaignError as error:
        raise CampaignError(
            f"{label} exited {completed.returncode} without one strict JSON object"
        ) from error
    if not isinstance(value, dict):
        raise CampaignError(f"{label} output is not an object")
    if completed.returncode != 0:
        message = value.get("error", {}).get("message") if isinstance(value.get("error"), dict) else None
        if label == "long-form runner" and message in {
            "GPU UUID lock is occupied",
            "GPU opportunity lock is occupied",
        }:
            return {"status": "held", "reason": "ordinary_gpu_lane_not_idle"}
        raise CampaignError(f"{label} failed: {message or value}")
    return value


def prepare_once(config: CampaignConfig) -> dict[str, Any]:
    with _dispatch_lock(config):
        candidates = discover_candidates(
            config, admit_new_cold=True, admit_new_queues=True
        )
        selected = _select_next_candidate(config, candidates)
        if selected is None:
            return {
                "status": "complete" if candidates else "held",
                "reason": "all_candidates_completed" if candidates else "no_requires_chunking_candidates",
                "gpu_invoked": False,
            }
        return {**prepare_candidate(config, selected), "gpu_invoked": False}


def run_once(config: CampaignConfig) -> dict[str, Any]:
    with _dispatch_lock(config):
        candidates = discover_candidates(
            config, admit_new_cold=True, admit_new_queues=True
        )
        _reconcile_completed_cleanup(config, candidates)

        def finish(value: dict[str, Any], *, lifecycle: str = "waiting") -> dict[str, Any]:
            _persist_status(config, candidates, lifecycle=lifecycle)
            return value

        selected = _select_next_candidate(config, candidates)
        if selected is None:
            return finish(
                {
                    "status": "complete" if candidates else "held",
                    "reason": "all_candidates_completed" if candidates else "no_requires_chunking_candidates",
                    "gpu_invoked": False,
                }
            )
        active_job = _job(config, selected)["job_id"]
        _persist_status(
            config, candidates, lifecycle="running", active_job=active_job
        )
        prepared = prepare_candidate(config, selected)
        job = prepared["job"]
        if prepared["status"] == "held":
            return finish(
                {
                    "status": "held",
                    "reason": prepared["reason"],
                    "job_id": job["job_id"],
                    "gpu_invoked": False,
                    "source_controller_mutated": False,
                }
            )
        if _source_stop_requested(config):
            return finish(
                {
                    "status": "held",
                    "reason": "durable_stop_requested",
                    "job_id": job["job_id"],
                    "gpu_invoked": False,
                    "source_controller_mutated": False,
                },
                lifecycle="stopped",
            )
        if _ordinary_gpu_busy(config):
            return finish(
                {
                    "status": "held",
                    "reason": "ordinary_gpu_lane_not_idle",
                    "job_id": job["job_id"],
                    "gpu_invoked": False,
                    "source_controller_mutated": False,
                }
            )
        runner = _strict_subprocess(
            runner_argv(config, job),
            timeout=config.document["execution"]["max_run_seconds"],
            label="long-form runner",
            stop_config=config,
        )
        if runner.get("status") in {"held", "incomplete"}:
            reason = runner.get(
                "reason", "durable_controller_stop_observed_between_spans"
            )
            stopped = (
                reason == "durable_controller_stop_observed_between_spans"
                or _source_stop_requested(config)
            )
            return finish(
                {
                    "status": "held",
                    "reason": reason,
                    "job_id": job["job_id"],
                    "gpu_invoked": runner.get("model_load_count", 0) > 0,
                    "source_controller_mutated": False,
                },
                lifecycle="stopped" if stopped else "waiting",
            )
        assembled = _strict_subprocess(
            assembler_argv(job),
            timeout=600,
            label="long-form assembler",
        )
        transcript_path = Path(job["paths"]["transcript"])
        transcript_body = _stable_file(
            transcript_path,
            "recording transcript",
            maximum=512 * 1024 * 1024,
            allowed_modes=frozenset({0o400, 0o600}),
        )
        completion = {
            "kind": "himr_longform_asr_campaign_completion",
            "schema_version": 1,
            "job_id": job["job_id"],
            "campaign_config_id": config.config_id,
            "candidate_identity_sha256": selected["identity_sha256"],
            "transcript": {
                "path": str(transcript_path),
                "sha256": sha256_bytes(transcript_body),
                "byte_count": len(transcript_body),
            },
            "runner": runner,
            "assembler": assembled,
            "policy": {
                "machine_generated": True,
                "human_review_required": True,
                "publication_authority": "none",
                "catalogue_mutation_authority": "none",
            },
        }
        _read_identical_or_write(Path(job["paths"]["completion"]), completion)
        cleanup = _cleanup_completed_cold_scratch(config, job, completion)
        return finish(
            {
                "status": "completed",
                "job_id": job["job_id"],
                "transcript": completion["transcript"],
                "gpu_invoked": runner.get("model_load_count", 0) > 0,
                "derived_scratch_cleanup": cleanup,
                "source_controller_mutated": False,
            }
        )


def run_continuous(
    config: CampaignConfig,
    *,
    sleep: Any = time.sleep,
    idle_seconds: float = 30.0,
    max_cycles: int | None = None,
) -> dict[str, Any]:
    """Run finite one-recording cycles until the source Stop intent is durable."""

    cycles = 0
    completed = 0
    held = 0
    last_outcome: dict[str, Any] | None = None

    def persist_failure(error: Exception) -> None:
        failure = {"type": type(error).__name__, "message": str(error)[:2048]}
        try:
            candidates = discover_candidates(config)
        except Exception:
            candidates = []
        _persist_status(
            config,
            candidates,
            lifecycle="faulted",
            last_error=failure,
        )

    while True:
        if _source_stop_requested(config):
            stopped_candidates = discover_candidates(config)
            _persist_status(config, stopped_candidates, lifecycle="stopped")
            return {
                "status": "stopped",
                "reason": "source_controller_desired_state_stopped",
                "cycles": cycles,
                "completed_recordings": completed,
                "held_cycles": held,
                "last_outcome": last_outcome,
                "source_controller_mutated": False,
            }
        try:
            last_outcome = run_once(config)
        except Exception as error:
            persist_failure(error)
            raise
        cycles += 1
        if last_outcome["status"] == "completed":
            completed += 1
        else:
            held += 1
        if max_cycles is not None and cycles >= max_cycles:
            return {
                "status": "bounded",
                "reason": "test_cycle_bound_reached",
                "cycles": cycles,
                "completed_recordings": completed,
                "held_cycles": held,
                "last_outcome": last_outcome,
                "source_controller_mutated": False,
            }
        if _source_stop_requested(config):
            continue
        if last_outcome.get("reason") in {
            "ordinary_gpu_lane_not_idle",
            "ordinary_gpu_work_observed_between_spans",
        }:
            # One candidate is already prepared. Replaying hundreds of immutable
            # receipts on every ordinary-GPU tick cannot change admission and can
            # consume tens of gigabytes of logical I/O. Poll only the cached source
            # projection, while refreshing this companion's small status heartbeat.
            try:
                while _ordinary_gpu_busy(config):
                    if _source_stop_requested(config):
                        break
                    _refresh_status_heartbeat(config)
                    sleep(idle_seconds)
            except Exception as error:
                persist_failure(error)
                raise
            continue
        # Ordinary GPU lock contention, no currently completed acquisition, and a
        # fully drained snapshot are all normal autonomous wait states.
        sleep(idle_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup = subparsers.add_parser("setup", help="Create an isolated sealed deployment")
    setup.add_argument("--controller-config", type=Path, required=True)
    setup.add_argument("--controller-config-sha256", required=True)
    setup.add_argument("--planning-policy", type=Path, required=True)
    setup.add_argument("--planning-policy-sha256", required=True)
    setup.add_argument("--ffmpeg", type=Path, default=Path("/usr/bin/ffmpeg"))
    setup.add_argument("--ffmpeg-sha256", required=True)
    setup.add_argument("--ffprobe", type=Path, default=Path("/usr/bin/ffprobe"))
    setup.add_argument("--ffprobe-sha256", required=True)
    setup.add_argument("--deployment-root", type=Path, required=True)
    setup.add_argument("--output", type=Path, required=True)
    setup.add_argument("--max-run-seconds", type=int, default=24 * 60 * 60)
    setup.add_argument("--initial-prompt")
    setup.add_argument("--hotword", action="append", default=[])

    for name, help_text in (
        ("status", "Read-only replay of candidates and local job state"),
        ("prepare-once", "Prepare metadata for at most one recording"),
        ("run-once", "Resume at most one recording under the shared GPU lock"),
        ("run", "Continuously run finite cycles until source Stop intent"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--expected-config-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "setup":
            config = materialize_config(
                controller_config=args.controller_config,
                controller_config_sha256=args.controller_config_sha256,
                planning_policy=args.planning_policy,
                planning_policy_sha256=args.planning_policy_sha256,
                ffmpeg=args.ffmpeg,
                ffmpeg_sha256=args.ffmpeg_sha256,
                ffprobe=args.ffprobe,
                ffprobe_sha256=args.ffprobe_sha256,
                deployment_root=args.deployment_root,
                output=args.output,
                max_run_seconds=args.max_run_seconds,
                initial_prompt=args.initial_prompt,
                hotwords=args.hotword,
            )
            result = {
                "status": "completed",
                "config_id": config.config_id,
                "config_path": str(config.path),
                "config_sha256": config.physical_sha256,
                "deployment_root": str(config.root),
                "source_controller_mutated": False,
                "gpu_invoked": False,
            }
        else:
            config = load_campaign_config(args.config, args.expected_config_sha256)
            if args.command == "status":
                result = campaign_status(config)
            elif args.command == "prepare-once":
                result = prepare_once(config)
            elif args.command == "run":
                result = run_continuous(config)
            else:
                result = run_once(config)
        sys.stdout.buffer.write(canonical_bytes(result))
        return 0
    except Exception as error:
        sys.stdout.buffer.write(
            canonical_bytes(
                {
                    "status": "failed",
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
