"""Offline admission of the 2026-08-30 unified Archive campaign.

This setup does not discover sources or start processing.  It retires the exact
stopped predecessor as a writer, proves that a sealed composite retains every
predecessor schedule byte-for-byte, reuses only the predecessor's preprocess
bundle and output roots, creates a fresh controller/GPU/cold state tree, deeply
restores the successor from an empty event journal, and finally seals its config.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import re
import stat
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterator

from .config import (
    IMPLEMENTATION_VERSION,
    KIND,
    ControllerConfig,
    ConfigError,
    build_config,
    canonical_bytes,
    load_config,
    sha256_bytes,
)
from .setup_archive_all_known import (
    SetupError,
    _create_private,
    _private_directory,
    _safe_parent_directory,
    _stable_hash,
    _write_sealed_no_replace,
)
from .state import (
    MAX_STATUS_BYTES,
    _stable_mutable_json,
    read_control_state,
)


REPOSITORY = Path(__file__).resolve().parents[1]
OPERATIONAL_PARENT = REPOSITORY / "research/operator-state"
SUCCESSOR_OPERATIONAL_ROOT = (
    OPERATIONAL_PARENT / "autonomous-archive-all-known-2026-08-30"
)
SUCCESSOR_CAMPAIGN_ROOT = (
    REPOSITORY / "research/corpus/autonomous/archive-all-known-2026-08-30"
)
SUCCESSOR_CONFIG = SUCCESSOR_CAMPAIGN_ROOT / "controller-config.json"
SUCCESSOR_INVENTORY = (
    REPOSITORY
    / "research/corpus/acquisition-planning/archive-all-known-2026-08-30/"
    "campaign-inventory.json"
)
SUCCESSOR_COMPOSITE_ROOT = SUCCESSOR_CAMPAIGN_ROOT / "producer-control"

PREDECESSOR_CONFIG = {
    "path": str(
        REPOSITORY
        / "research/corpus/autonomous/archive-all-known-2026-08-29/"
        "controller-config.json"
    ),
    "sha256": "4a1fe84f9db6dd7d1afeb177245ae9d1457d1765379266740bc4eea154668dad",
    "config_id": "himrautocfg_fa3a67856c384c767bc10c1ce869efe8",
    "state_root": str(
        OPERATIONAL_PARENT / "autonomous-archive-all-known-2026-08-29/state"
    ),
}

COMPOSITE_KIND = "sealed_archive_campaign_composite_schedule_set"
COMPOSITE_ID_RE = re.compile(r"^bgacqcompositeset_[0-9a-f]{32}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_COMPOSITE_BYTES = 32 * 1024 * 1024
MAX_COMPOSITE_SCHEDULES = 4_096

FRESH_WRITABLE_NAMES = (
    "state",
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

SCHEDULE_KEYS = {
    "schedule_ordinal",
    "component",
    "component_schedule_ordinal",
    "role",
    "schedule_path",
    "schedule_sha256",
    "schedule_byte_count",
    "schedule_id",
    "schedule_identity_sha256",
    "preprocess_state_root",
    "source_schedule_set",
    "source_campaign",
    "source_epoch",
}


def _normalized_absolute(path: Path, label: str) -> Path:
    raw = str(path)
    if (
        not path.is_absolute()
        or os.path.normpath(raw) != raw
        or raw == "/"
        or "//" in raw
        or "\\" in raw
    ):
        raise SetupError(f"{label} must be one normalized absolute path")
    return path


def _validate_sha256(value: str, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise SetupError(f"{label} must be a lowercase SHA-256")
    return value


def _load_successor_inventory(expected_sha256: str) -> None:
    _validate_sha256(expected_sha256, "successor inventory SHA-256")
    _stable_hash(
        SUCCESSOR_INVENTORY,
        expected_sha256,
        mode=0o400,
        label="successor campaign inventory",
    )


def _load_composite_schedule_set(
    manifest_path: Path, expected_sha256: str
) -> dict[str, Any]:
    """Load only the sealed header/projection needed to construct the config.

    The backend performs the authoritative deep replay of the composite and both
    component schedule sets before this setup admits a configuration.
    """

    manifest_path = _normalized_absolute(
        manifest_path, "composite schedule-set manifest"
    )
    expected_sha256 = _validate_sha256(
        expected_sha256, "composite schedule-set manifest SHA-256"
    )
    expected_parent = SUCCESSOR_COMPOSITE_ROOT / "composite-schedule-sets"
    try:
        relative = manifest_path.relative_to(expected_parent)
    except ValueError as error:
        raise SetupError(
            "composite schedule-set manifest is outside the fixed successor control root"
        ) from error
    if (
        len(relative.parts) != 2
        or relative.parts[1] != "manifest.json"
        or COMPOSITE_ID_RE.fullmatch(relative.parts[0]) is None
    ):
        raise SetupError("composite schedule-set manifest path is not canonical")
    body = _stable_hash(
        manifest_path,
        expected_sha256,
        mode=0o400,
        label="composite schedule-set manifest",
    )
    if not 1 <= len(body) <= MAX_COMPOSITE_BYTES:
        raise SetupError("composite schedule-set manifest exceeds its byte cap")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SetupError(
            f"composite schedule-set manifest is not strict JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise SetupError("composite schedule-set manifest must be an object")
    schedules = value.get("schedules")
    if (
        value.get("schema_version") != 1
        or value.get("composite_kind") != COMPOSITE_KIND
        or value.get("composite_schedule_set_id") != relative.parts[0]
        or COMPOSITE_ID_RE.fullmatch(
            str(value.get("composite_schedule_set_id", ""))
        )
        is None
        or value.get("component_order") != ["predecessor", "addendum"]
        or not isinstance(schedules, list)
        or not 4 <= len(schedules) <= MAX_COMPOSITE_SCHEDULES
    ):
        raise SetupError("composite schedule-set header or cardinality differs")
    expected_ordinals = list(range(1, len(schedules) + 1))
    if [row.get("schedule_ordinal") for row in schedules] != expected_ordinals:
        raise SetupError("composite schedule ordinals are not contiguous")
    for ordinal, row in enumerate(schedules, 1):
        if not isinstance(row, dict) or set(row) != SCHEDULE_KEYS:
            raise SetupError(
                f"composite schedule {ordinal} fields differ from the exact contract"
            )
        if (
            row["component"] not in {"predecessor", "addendum"}
            or row["role"]
            not in {
                "normal_processing",
                "cold_acquisition_only_requires_chunking",
            }
            or not isinstance(row["schedule_path"], str)
            or not isinstance(row["schedule_id"], str)
            or SHA256_RE.fullmatch(str(row["schedule_sha256"])) is None
        ):
            raise SetupError(f"composite schedule {ordinal} projection is invalid")
    return value


def _assert_predecessor_component(
    predecessor: ControllerConfig, composite: dict[str, Any]
) -> None:
    """Prove that the composite references the reviewed predecessor exactly."""

    components = composite.get("components")
    if not isinstance(components, list) or len(components) != 2:
        raise SetupError("composite component references are invalid")
    component = components[0]
    predecessor_schedule_set = predecessor.section("campaign")["schedule_set"]
    expected_component = {
        "schedule_set_id": predecessor_schedule_set["schedule_set_id"],
        "manifest_path": predecessor_schedule_set["path"],
        "manifest_sha256": predecessor_schedule_set["sha256"],
    }
    if (
        not isinstance(component, dict)
        or component.get("component") != "predecessor"
        or any(component.get(key) != value for key, value in expected_component.items())
    ):
        raise SetupError(
            "composite predecessor reference differs from the reviewed stopped campaign"
        )
    projected = [
        {
            "path": row["schedule_path"],
            "sha256": row["schedule_sha256"],
            "schedule_id": row["schedule_id"],
            "role": row["role"],
        }
        for row in composite["schedules"]
        if row["component"] == "predecessor"
    ]
    if projected != predecessor.section("campaign")["schedules"]:
        raise SetupError(
            "composite predecessor schedules are not the exact ordered predecessor set"
        )


def _successor_roots() -> dict[str, Path]:
    return {
        name: SUCCESSOR_OPERATIONAL_ROOT / name for name in FRESH_WRITABLE_NAMES
    }


def successor_config_core(
    *,
    predecessor: ControllerConfig,
    composite: dict[str, Any],
    inventory_sha256: str,
    composite_manifest_path: Path,
    composite_manifest_sha256: str,
    roots: dict[str, Path] | None = None,
) -> dict[str, Any]:
    """Build the fixed-policy successor core without creating any filesystem state."""

    roots = roots or _successor_roots()
    if set(roots) != set(FRESH_WRITABLE_NAMES):
        raise SetupError("successor fresh-root mapping differs from the closed contract")
    predecessor_document = predecessor.document
    predecessor_campaign = predecessor.section("campaign")
    predecessor_preprocess = predecessor.section("preprocess")
    predecessor_gpu = predecessor.section("gpu_readiness")
    predecessor_cold = predecessor.section("cold_retention")
    schedules = [
        {
            "path": row["schedule_path"],
            "sha256": row["schedule_sha256"],
            "schedule_id": row["schedule_id"],
            "role": row["role"],
        }
        for row in composite["schedules"]
    ]
    campaign_core = {
        "inventory": {
            "kind": "known_collections_inventory",
            "path": str(SUCCESSOR_INVENTORY),
            "sha256": inventory_sha256,
        },
        "schedule_set": {
            "kind": COMPOSITE_KIND,
            "path": str(composite_manifest_path),
            "sha256": composite_manifest_sha256,
            "schedule_set_id": composite["composite_schedule_set_id"],
        },
        "schedules": schedules,
        "global_ready_high_items": predecessor_campaign[
            "global_ready_high_items"
        ],
        "global_ready_high_bytes": predecessor_campaign[
            "global_ready_high_bytes"
        ],
    }
    campaign_id = "himrarccampaign_" + sha256_bytes(
        canonical_bytes(campaign_core)
    )[:32]
    gpu = deepcopy(predecessor_gpu)
    gpu.update(
        {
            "queue_root": str(roots["gpu-queues"]),
            "child_journal_root": str(roots["state"] / "gpu-children"),
            "work_order_root": str(roots["gpu-work-orders"]),
            "receipt_root": str(roots["gpu-materializations"]),
            "result_root": str(roots["gpu-results"]),
            "batch_root": str(roots["gpu-batches"]),
            "event_root": str(roots["gpu-events"]),
            "lock_root": str(roots["gpu-locks"]),
        }
    )
    cold = deepcopy(predecessor_cold)
    cold.update(
        {
            "staging_root": str(roots["cold-staging"]),
            "receipt_root": str(roots["cold-receipts"]),
        }
    )
    return {
        "kind": KIND,
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "campaign": {"campaign_id": campaign_id, **campaign_core},
        "state_root": str(roots["state"]),
        "acquisition": deepcopy(predecessor_document["acquisition"]),
        "preprocess": {
            "bundle_root": predecessor_preprocess["bundle_root"],
            "processing_output_root": predecessor_preprocess[
                "processing_output_root"
            ],
            "max_items": predecessor_preprocess["max_items"],
            "max_attempts_per_item": predecessor_preprocess[
                "max_attempts_per_item"
            ],
        },
        "gpu_readiness": gpu,
        "cold_retention": cold,
        "scheduler": deepcopy(predecessor_document["scheduler"]),
        "safety": deepcopy(predecessor_document["safety"]),
    }


def _validate_reused_preprocess_roots(predecessor: ControllerConfig) -> None:
    preprocess = predecessor.section("preprocess")
    for key, label in (
        ("bundle_root", "reused predecessor preprocess bundle root"),
        ("processing_output_root", "reused predecessor preprocess output root"),
    ):
        path = Path(preprocess[key])
        _private_directory(path, label)
        if path == SUCCESSOR_OPERATIONAL_ROOT or SUCCESSOR_OPERATIONAL_ROOT in path.parents:
            raise SetupError(f"{label} may not be inside the fresh successor root")


def _validate_output_target(output: Path) -> None:
    output = _normalized_absolute(output, "successor config output")
    if output != SUCCESSOR_CONFIG:
        raise SetupError("successor config output differs from the fixed production path")
    if output.exists() or output.is_symlink():
        raise SetupError("refusing to overwrite the successor config output")
    try:
        parent = output.parent.resolve(strict=True)
        observed = parent.lstat()
    except OSError as error:
        raise SetupError(f"cannot inspect successor config output parent: {error}") from error
    if (
        parent != output.parent
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise SetupError(
            "successor config output parent must be current-user and not peer-writable"
        )


@contextmanager
def _hold_existing_lock(path: Path, label: str) -> Iterator[None]:
    """Hold one existing predecessor lock without creating or replacing it."""

    descriptor = -1
    try:
        inspected = path.lstat()
        if path.resolve(strict=True) != path:
            raise SetupError(f"{label} may not traverse a symlink")
        descriptor = os.open(
            path,
            os.O_RDWR
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
    except SetupError:
        if descriptor >= 0:
            os.close(descriptor)
        raise
    except OSError as error:
        if descriptor >= 0:
            os.close(descriptor)
        raise SetupError(f"cannot open {label}: {error}") from error
    try:
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (inspected.st_dev, inspected.st_ino)
            != (opened.st_dev, opened.st_ino)
        ):
            raise SetupError(f"{label} must be an owner-private single-link lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                raise SetupError(f"{label} is held; predecessor is not quiescent") from error
            raise SetupError(f"cannot lock {label}: {error}") from error
        yield
    finally:
        try:
            if descriptor >= 0:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            if descriptor >= 0:
                os.close(descriptor)


def _validate_predecessor_stopped_documents(
    predecessor: ControllerConfig,
) -> dict[str, Any]:
    """Validate the mutable stopped projection while both locks are held."""

    state_root = predecessor.state_root
    control = read_control_state(predecessor)
    status_path = state_root / "status.json"
    if not status_path.exists() and not status_path.is_symlink():
        raise SetupError("predecessor has no terminal cached status")
    status = _stable_mutable_json(
        status_path,
        maximum=MAX_STATUS_BYTES,
        label="predecessor cached status",
        mode=0o600,
    )
    execution = status.get("execution")
    lanes = status.get("lanes")
    if (
        control.get("desired_state") != "stopped"
        or status.get("config_id") != predecessor.config_id
        or status.get("config_sha256") != predecessor.physical_sha256
        or status.get("desired_state") != "stopped"
        or status.get("actual_state") != "stopped"
        or status.get("lifecycle") != "stopped"
        or status.get("current_stage") is not None
        or status.get("current_gpu_child") is not None
        or not isinstance(execution, dict)
        or execution.get("accepting_new_work") is not False
        or execution.get("draining") is not False
        or execution.get("inflight_total") != 0
        or not isinstance(lanes, dict)
        or set(lanes)
        != {
            "acquisition",
            "preprocess",
            "gpu_readiness",
            "cold_retention",
        }
        or any(
            not isinstance(row, dict) or row.get("active") != 0
            for row in lanes.values()
        )
    ):
        raise SetupError(
            "predecessor desired/actual state or in-flight lanes are not fully stopped"
        )
    return status


@contextmanager
def _hold_stopped_predecessor(predecessor: ControllerConfig) -> Iterator[dict[str, Any]]:
    """Freeze Start and prove the old foreground/GPU lanes are fully stopped."""

    state_root = predecessor.state_root
    with _hold_existing_lock(
        state_root / "controller.lock", "predecessor controller lock"
    ):
        with _hold_existing_lock(
            state_root / "control.lock", "predecessor control lock"
        ):
            status = _validate_predecessor_stopped_documents(predecessor)
            yield status


def _initialize_fresh_roots() -> dict[str, Path]:
    if not OPERATIONAL_PARENT.exists() and not OPERATIONAL_PARENT.is_symlink():
        _safe_parent_directory(REPOSITORY / "research", "research root")
        _create_private(OPERATIONAL_PARENT, "operator-state root")
    else:
        _private_directory(OPERATIONAL_PARENT, "operator-state root")
    root = _create_private(
        SUCCESSOR_OPERATIONAL_ROOT, "successor autonomous operational root"
    )
    allowed = set(FRESH_WRITABLE_NAMES)
    unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed)
    if unexpected:
        raise SetupError(
            f"successor autonomous operational root has unexpected entries: {unexpected}"
        )
    roots = {
        name: _create_private(root / name, f"successor {name} root")
        for name in FRESH_WRITABLE_NAMES
    }
    state = roots["state"]
    state_allowed = {"events", "gpu-children"}
    unexpected_state = sorted(
        path.name for path in state.iterdir() if path.name not in state_allowed
    )
    if unexpected_state:
        raise SetupError("successor controller state root is not fresh")
    events = _create_private(state / "events", "successor controller event root")
    children = _create_private(
        state / "gpu-children", "successor GPU child journal root"
    )
    if any(events.iterdir()) or any(children.iterdir()):
        raise SetupError("successor controller event or GPU child state is not empty")
    for name, path in roots.items():
        if name != "state" and any(path.iterdir()):
            raise SetupError(f"successor {name} root is not empty")
    return roots


def _durable_predecessor_progress(status: dict[str, Any]) -> dict[str, int]:
    """Derive exact physical-progress expectations from the frozen predecessor."""

    monitor = status.get("monitor")
    progress = status.get("progress")
    telemetry = status.get("pipeline_telemetry")
    acquisition = monitor.get("acquisition") if isinstance(monitor, dict) else None
    preprocess = monitor.get("preprocess") if isinstance(monitor, dict) else None
    progress_acquisition = (
        progress.get("acquisition") if isinstance(progress, dict) else None
    )
    acquired = acquisition.get("completed") if isinstance(acquisition, dict) else None
    acquired_projection = (
        progress_acquisition.get("completed")
        if isinstance(progress_acquisition, dict)
        else None
    )
    preprocessed = (
        preprocess.get("preprocessed_items_cumulative")
        if isinstance(preprocess, dict)
        else None
    )
    preprocessed_projection = (
        telemetry.get("preprocessed_items") if isinstance(telemetry, dict) else None
    )
    values = (
        acquired,
        acquired_projection,
        preprocessed,
        preprocessed_projection,
    )
    if (
        any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values)
        or acquired != acquired_projection
        or preprocessed != preprocessed_projection
        or preprocessed > acquired
    ):
        raise SetupError(
            "predecessor durable acquisition/preprocess progress projections disagree"
        )
    return {
        "authoritative_acquisitions_replayed": acquired,
        "preprocessed_items_cumulative": preprocessed,
    }


def _composite_expected_coverage(composite: dict[str, Any]) -> dict[str, int]:
    """Derive the successor cardinalities from the externally sealed composite."""

    coverage = composite.get("coverage_proof")
    role_totals = composite.get("role_totals")
    if not isinstance(coverage, dict) or not isinstance(role_totals, list):
        raise SetupError("composite coverage proof or role totals are absent")
    by_role: dict[str, dict[str, Any]] = {}
    for row in role_totals:
        if not isinstance(row, dict) or row.get("role") in by_role:
            raise SetupError("composite role totals are malformed or repeated")
        role = row.get("role")
        if role not in {
            "normal_processing",
            "cold_acquisition_only_requires_chunking",
        }:
            raise SetupError("composite role totals contain an unsupported role")
        by_role[role] = row
    if set(by_role) != {
        "normal_processing",
        "cold_acquisition_only_requires_chunking",
    }:
        raise SetupError("composite role totals are incomplete")
    candidate_count = coverage.get("source_selected_count")
    schedule_count = coverage.get("schedule_count")
    ready_count = by_role["normal_processing"].get("selected_count")
    cold_count = by_role[
        "cold_acquisition_only_requires_chunking"
    ].get("selected_count")
    values = (candidate_count, schedule_count, ready_count, cold_count)
    if (
        any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in values)
        or candidate_count != ready_count + cold_count
        or schedule_count != len(composite["schedules"])
    ):
        raise SetupError("composite selected or schedule cardinalities disagree")
    return {
        "candidate_count": candidate_count,
        "ready_selected_count": ready_count,
        "parked_requires_chunking_count": cold_count,
        "schedule_count": schedule_count,
    }


def _assert_empty_restore(
    restored: dict[str, Any],
    *,
    predecessor_progress: dict[str, int],
    expected_coverage: dict[str, int],
) -> None:
    zero_keys = (
        "cold_retention_records_restored",
        "cold_retentions_pending_exact_replay",
        "gpu_records_replayed",
        "gpu_ready_batches",
        "gpu_pending_items_restored",
        "gpu_completed_items_restored",
        "gpu_requires_chunking_items_restored",
        "gpu_explicit_skips_restored",
        "gpu_parked_batches_restored",
        "gpu_parked_items_restored",
        "preprocess_failed_attempts_restored",
        "preprocess_retryable_failed_items_restored",
        "preprocess_parked_items_restored",
    )
    if not isinstance(restored, dict) or any(restored.get(key) != 0 for key in zero_keys):
        raise SetupError(
            "successor empty state unexpectedly restored event, GPU, cold, or failure records"
        )
    for key, expected in predecessor_progress.items():
        if restored.get(key) != expected:
            raise SetupError(
                f"successor did not preserve predecessor durable progress: {key}"
            )
    campaign = restored.get("campaign_coverage")
    schedule_set = restored.get("schedule_set_coverage")
    if not isinstance(campaign, dict) or not isinstance(schedule_set, dict):
        raise SetupError("successor restore omitted campaign or schedule-set coverage")
    expected_pairs = {
        "candidate_count": expected_coverage["candidate_count"],
        "ready_selected_count": expected_coverage["ready_selected_count"],
        "parked_requires_chunking_count": expected_coverage[
            "parked_requires_chunking_count"
        ],
        "scheduled_candidate_count": expected_coverage["candidate_count"],
        "scheduled_ready_count": expected_coverage["ready_selected_count"],
        "scheduled_cold_only_count": expected_coverage[
            "parked_requires_chunking_count"
        ],
    }
    if any(campaign.get(key) != value for key, value in expected_pairs.items()):
        raise SetupError("successor campaign coverage differs from the sealed composite")
    if (
        restored.get("campaign_schedule_count")
        != expected_coverage["schedule_count"]
        or schedule_set.get("schedule_count")
        != expected_coverage["schedule_count"]
        or schedule_set.get("selected_count")
        != expected_coverage["candidate_count"]
        or schedule_set.get("normal_selected_count")
        != expected_coverage["ready_selected_count"]
        or schedule_set.get("cold_only_selected_count")
        != expected_coverage["parked_requires_chunking_count"]
    ):
        raise SetupError(
            "successor schedule-set coverage differs from the sealed composite"
        )


def setup_archive_successor(
    *,
    composite_manifest_path: Path,
    expected_composite_manifest_sha256: str,
    expected_inventory_sha256: str,
    output: Path = SUCCESSOR_CONFIG,
) -> dict[str, Any]:
    """Admit one stopped, fresh-state successor configuration without running it."""

    _validate_output_target(output)
    _load_successor_inventory(expected_inventory_sha256)
    composite = _load_composite_schedule_set(
        composite_manifest_path, expected_composite_manifest_sha256
    )
    try:
        predecessor = load_config(
            Path(PREDECESSOR_CONFIG["path"]), PREDECESSOR_CONFIG["sha256"]
        )
    except ConfigError as error:
        raise SetupError(f"reviewed predecessor config failed replay: {error}") from error
    if (
        predecessor.config_id != PREDECESSOR_CONFIG["config_id"]
        or str(predecessor.state_root) != PREDECESSOR_CONFIG["state_root"]
    ):
        raise SetupError("reviewed predecessor config identity or state root differs")
    _assert_predecessor_component(predecessor, composite)
    expected_coverage = _composite_expected_coverage(composite)
    _validate_reused_preprocess_roots(predecessor)
    prospective_roots = _successor_roots()
    core = successor_config_core(
        predecessor=predecessor,
        composite=composite,
        inventory_sha256=expected_inventory_sha256,
        composite_manifest_path=composite_manifest_path,
        composite_manifest_sha256=expected_composite_manifest_sha256,
        roots=prospective_roots,
    )
    try:
        document = build_config(core)
    except ConfigError as error:
        raise SetupError(f"fixed successor config is invalid: {error}") from error
    body = canonical_bytes(document)
    with _hold_stopped_predecessor(predecessor) as predecessor_status:
        predecessor_progress = _durable_predecessor_progress(predecessor_status)
        roots = _initialize_fresh_roots()
        if roots != prospective_roots:
            raise SetupError("initialized successor roots differ from fixed config paths")
        try:
            from .sealed_backend import SealedArchiveBackend

            candidate = ControllerConfig(
                document=document,
                path=output,
                physical_sha256=sha256_bytes(body),
            )
            restored = SealedArchiveBackend(candidate).restore(())
        except Exception as error:
            raise SetupError(
                f"successor empty-state deep replay failed before config admission: {error}"
            ) from error
        _assert_empty_restore(
            restored,
            predecessor_progress=predecessor_progress,
            expected_coverage=expected_coverage,
        )
        # Re-check both mutable stop documents while their locks are still held.
        # This also detects a non-cooperating replacement of either document.
        _validate_predecessor_stopped_documents(predecessor)
        _write_sealed_no_replace(output, body)
    return {
        "status": "configured",
        "config": str(output),
        "config_sha256": sha256_bytes(body),
        "config_id": document["config_id"],
        "campaign_id": document["campaign"]["campaign_id"],
        "state_root": document["state_root"],
        "schedule_set_id": composite["composite_schedule_set_id"],
        "configured_schedule_count": len(document["campaign"]["schedules"]),
        "predecessor_config_id": predecessor.config_id,
        "predecessor_schedule_count": len(
            predecessor.section("campaign")["schedules"]
        ),
        "reused_preprocess_bundle_root": document["preprocess"]["bundle_root"],
        "reused_preprocess_output_root": document["preprocess"][
            "processing_output_root"
        ],
        "coverage": restored["campaign_coverage"],
        "preprocessed_items_reused": restored["preprocessed_items_cumulative"],
        "acquisitions_reused": restored["authoritative_acquisitions_replayed"],
        "fresh_gpu_records_restored": restored["gpu_records_replayed"],
        "files_overwritten": False,
        "processing_started": False,
    }


__all__ = [
    "SetupError",
    "setup_archive_successor",
    "successor_config_core",
]
