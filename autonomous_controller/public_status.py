"""Lightweight cached status reader for the local operator console.

This path deliberately does not instantiate :class:`ControlStore`, enumerate the
event journal, replay schedules, inspect media, or touch the cold mount.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import ControllerConfig, _read_sealed, load_config, sha256_bytes
from .sealed_backend import SealedArchiveBackend
from .state import (
    MAX_STATUS_BYTES,
    _lightweight_state_root,
    _stable_mutable_json,
    read_control_state,
)


class PublicStatusError(RuntimeError):
    """The cached controller status or campaign coverage cannot be trusted."""


_PIPELINE_TELEMETRY_KEYS = {
    "schema_version",
    "queued_items",
    "queued_items_basis",
    "queued_observation_sequence",
    "preprocessed_items",
    "preprocessed_items_basis",
    "asr_completed_items",
    "asr_completed_items_basis",
}
_QUEUED_BASES = {
    "latest_admitted_acquisition_ready_items",
    "latest_admitted_acquisition_raw_normal_ready_items_legacy",
    "latest_admitted_preprocess_ready_items_after",
    "latest_admitted_preprocess_raw_ready_items_legacy",
    "unavailable_invalid_ready_snapshot",
    "unavailable_no_admitted_ready_snapshot",
}
_PREPROCESSED_BASES = {
    "durable_stage_finished_preprocess_processed_items_sum",
    "latest_admitted_preprocess_receipt_total",
    "validated_backend_restore_preprocess_receipt_total",
    "unavailable_incomplete_durable_preprocess_history",
    "unavailable_invalid_preprocess_receipt_total",
    "unavailable_preprocess_receipt_total_regressed",
}
_ASR_BASES = {
    "latest_admitted_gpu_completed_items",
    "validated_backend_restore_gpu_completed_items",
    "unavailable_gpu_completed_item_count",
    "unavailable_gpu_completed_item_count_regressed",
}


def _unavailable_pipeline_telemetry() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "queued_items": None,
        "queued_items_basis": "unavailable_no_admitted_ready_snapshot",
        "queued_observation_sequence": None,
        "preprocessed_items": None,
        "preprocessed_items_basis": (
            "unavailable_incomplete_durable_preprocess_history"
        ),
        "asr_completed_items": None,
        "asr_completed_items_basis": "unavailable_gpu_completed_item_count",
    }


def _validated_pipeline_telemetry(value: Any) -> dict[str, Any]:
    """Validate the controller's bounded, journal-derived count projection.

    Older cached status documents legitimately lack this optional schema-v1
    extension.  They get explicit unavailable values; per-cycle counters are not
    relabelled as cumulative totals.
    """

    if value is None:
        return _unavailable_pipeline_telemetry()
    if not isinstance(value, dict) or set(value) != _PIPELINE_TELEMETRY_KEYS:
        raise PublicStatusError("cached pipeline telemetry has unexpected fields")
    if value.get("schema_version") != 1:
        raise PublicStatusError("cached pipeline telemetry schema is unsupported")
    for key in ("queued_items", "preprocessed_items", "asr_completed_items"):
        count = value.get(key)
        if count is not None and (
            isinstance(count, bool) or not isinstance(count, int) or count < 0
        ):
            raise PublicStatusError(f"cached pipeline telemetry {key} is invalid")
    sequence = value.get("queued_observation_sequence")
    if sequence is not None and (
        isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1
    ):
        raise PublicStatusError(
            "cached pipeline telemetry queue observation sequence is invalid"
        )
    for key in (
        "queued_items_basis",
        "preprocessed_items_basis",
        "asr_completed_items_basis",
    ):
        basis = value.get(key)
        if not isinstance(basis, str) or not basis or len(basis) > 128:
            raise PublicStatusError(f"cached pipeline telemetry {key} is invalid")
    if (
        value["queued_items_basis"] not in _QUEUED_BASES
        or value["preprocessed_items_basis"] not in _PREPROCESSED_BASES
        or value["asr_completed_items_basis"] not in _ASR_BASES
    ):
        raise PublicStatusError("cached pipeline telemetry basis is unsupported")
    if (value["queued_items"] is None) != value["queued_items_basis"].startswith(
        "unavailable_"
    ):
        raise PublicStatusError("cached queue telemetry value and basis disagree")
    if (
        value["preprocessed_items"] is None
    ) != value["preprocessed_items_basis"].startswith("unavailable_"):
        raise PublicStatusError(
            "cached preprocess telemetry value and basis disagree"
        )
    if (
        value["asr_completed_items"] is None
    ) != value["asr_completed_items_basis"].startswith("unavailable_"):
        raise PublicStatusError("cached ASR telemetry value and basis disagree")
    if value["queued_items"] is not None and sequence is None:
        raise PublicStatusError(
            "cached queue telemetry lacks its admission observation"
        )
    return dict(value)


def _campaign_status(config: ControllerConfig) -> dict[str, Any]:
    campaign = config.section("campaign")
    inventory = campaign["inventory"]
    try:
        body = _read_sealed(Path(inventory["path"]))
        if sha256_bytes(body) != inventory["sha256"]:
            raise PublicStatusError(
                "campaign inventory differs from its external SHA-256"
            )
        value = json.loads(body)
        coverage = SealedArchiveBackend._inventory_coverage(value)
    except PublicStatusError:
        raise
    except Exception as error:
        raise PublicStatusError(f"campaign inventory validation failed: {error}") from error
    collections = [
        {
            "identifier": row["identifier"],
            "candidate_count": row["candidate_count"],
            "ready_selected_count": row["ready_selected_count"],
            "parked_requires_chunking_count": row[
                "parked_requires_chunking_count"
            ],
            "estimated_selected_bytes": row["estimated_selected_bytes"],
        }
        for row in value["collections"]
    ]
    result = {
        "campaign_id": campaign["campaign_id"],
        "schedule_set_id": campaign["schedule_set"]["schedule_set_id"],
        "inventory_kind": campaign["inventory"]["kind"],
        "configured_schedule_count": len(campaign["schedules"]),
        "configured_normal_schedule_count": sum(
            schedule["role"] == "normal_processing"
            for schedule in campaign["schedules"]
        ),
        "configured_cold_only_schedule_count": sum(
            schedule["role"] == "cold_acquisition_only_requires_chunking"
            for schedule in campaign["schedules"]
        ),
        "collection_filter": "none_sealed_inventory_controls_coverage",
        **coverage,
        "collections": collections,
    }
    return result


def read_public_status(
    config_path: Path | str, expected_config_sha256: str
) -> dict[str, Any]:
    """Return validated config coverage plus only the cached ``status.json``.

    The function is safe for frequent console polling: its work is bounded by the
    256-KiB config, small sealed campaign inventory, and 256-KiB status cap.
    """

    config = load_config(Path(config_path), expected_config_sha256)
    state_root = _lightweight_state_root(config)
    status_path = state_root / "status.json"
    if status_path.exists() or status_path.is_symlink():
        status = _stable_mutable_json(
            status_path,
            maximum=MAX_STATUS_BYTES,
            label="cached controller public status",
            mode=0o600,
        )
        if (
            status.get("kind") != "himr_autonomous_controller_status"
            or status.get("schema_version") != 1
            or status.get("config_id") != config.config_id
            or status.get("config_sha256") != config.physical_sha256
        ):
            raise PublicStatusError(
                "cached controller status belongs to a different configuration"
            )
    else:
        status = {
            "kind": "himr_autonomous_controller_status",
            "schema_version": 1,
            "config_id": config.config_id,
            "config_sha256": config.physical_sha256,
            "lifecycle": "not_started",
            "actual_state": "not_started",
            "desired_state": "stopped",
            "current_stage": None,
            "pid": None,
            "started_at": None,
            "updated_at": None,
            "cycle": 0,
            "dispatch_sequence": 0,
            "scheduler_mode": "sequential_cycles",
            "completion_reason": None,
            "consecutive_failures": 0,
            "last_error": None,
            "errors": [],
            "last_event": None,
            "monitor": {},
            "stages": {
                "acquisition": None,
                "preprocess": None,
                "gpu_readiness": None,
                "cold_retention": None,
            },
            "lanes": {
                stage: {
                    "state": "idle",
                    "active": 0,
                    "limit": 1,
                    "dispatch_id": None,
                    "started_at": None,
                    "last_status": None,
                    "wait_reason": None,
                    "last_transition_at": None,
                }
                for stage in (
                    "acquisition",
                    "preprocess",
                    "gpu_readiness",
                    "cold_retention",
                )
            },
            "execution": {
                "accepting_new_work": False,
                "draining": False,
                "inflight_total": 0,
            },
            "progress": {},
            "pipeline_telemetry": _unavailable_pipeline_telemetry(),
            "throughput": {
                "last_cycle_new_acquisition_items": 0,
                "last_cycle_new_acquisition_bytes": 0,
            },
            "storage": {
                "acquisition_ready_bytes": None,
                "acquisition_ready_items": None,
                "campaign_ready_high_bytes": config.section("campaign")[
                    "global_ready_high_bytes"
                ],
                "campaign_ready_high_items": config.section("campaign")[
                    "global_ready_high_items"
                ],
                "acquisition_backpressure": None,
                "cold_only_acquisition_ready_bytes": None,
                "cold_only_acquisition_ready_items": None,
                "cold_retained_items": None,
                "hot_deletions": 0,
            },
            "recent_activity": [],
            "current_gpu_child": None,
            "safety": dict(config.document["safety"]),
        }
    status["pipeline_telemetry"] = _validated_pipeline_telemetry(
        status.get("pipeline_telemetry")
    )
    campaign = _campaign_status(config)
    gpu_monitor = status.get("monitor", {}).get("gpu_readiness") if isinstance(
        status.get("monitor"), dict
    ) else None
    acquisition_monitor = status.get("monitor", {}).get("acquisition") if isinstance(
        status.get("monitor"), dict
    ) else None
    preprocess_monitor = status.get("monitor", {}).get("preprocess") if isinstance(
        status.get("monitor"), dict
    ) else None
    gpu_chunking = (
        gpu_monitor.get("requires_chunking_items_cumulative", 0)
        if isinstance(gpu_monitor, dict)
        else 0
    )
    if isinstance(gpu_chunking, bool) or not isinstance(gpu_chunking, int):
        gpu_chunking = 0
    scheduled_quarantined = (
        acquisition_monitor.get("quarantined_items", 0)
        if isinstance(acquisition_monitor, dict)
        else 0
    )
    if isinstance(scheduled_quarantined, bool) or not isinstance(
        scheduled_quarantined, int
    ):
        scheduled_quarantined = 0
    preprocess_parked = (
        preprocess_monitor.get("parked_items", 0)
        if isinstance(preprocess_monitor, dict)
        else 0
    )
    if isinstance(preprocess_parked, bool) or not isinstance(preprocess_parked, int):
        preprocess_parked = 0
    gpu_parked = (
        gpu_monitor.get("parked_items", 0)
        if isinstance(gpu_monitor, dict)
        else 0
    )
    if isinstance(gpu_parked, bool) or not isinstance(gpu_parked, int):
        gpu_parked = 0
    inventory_chunking = campaign["parked_requires_chunking_count"]
    campaign.update(
        {
            "inventory_requires_chunking_backlog": inventory_chunking,
            "gpu_requires_chunking_backlog": gpu_chunking,
            "postprocess_backlog_count": inventory_chunking + gpu_chunking,
            "scheduled_acquisition_quarantined_count": scheduled_quarantined,
            "preprocess_parked_count": preprocess_parked,
            "gpu_parked_item_count": gpu_parked,
            "unresolved_parked_items": (
                inventory_chunking
                + gpu_chunking
                + scheduled_quarantined
                + preprocess_parked
                + gpu_parked
            ),
        }
    )
    campaign["campaign_complete"] = status.get("completion_reason") == "campaign_drained"

    # The mutable control document is the current Start/Stop authority.  Cached
    # status can legitimately lag it at both launch and graceful-stop boundaries,
    # so publish explicit control gates instead of asking the browser to infer
    # safety from an incomplete lifecycle vocabulary.
    control = read_control_state(config)
    desired = control["desired_state"]
    actual = status.get("actual_state")
    startable_states = {
        "not_started",
        "stopped",
        "completed",
        "blocked",
        "faulted",
    }
    active_states = {"starting", "running", "retrying", "stopping"}
    can_start = desired == "stopped" and actual in startable_states
    can_stop = desired == "running"
    running = desired == "running" or actual in active_states
    controls = {
        "can_start": can_start,
        "can_stop": can_stop,
        "control_generation": control["generation"],
    }
    return {
        **status,
        "desired_state": desired,
        "running": running,
        "can_start": can_start,
        "can_stop": can_stop,
        "controls": controls,
        "control_generation": control["generation"],
        "campaign": campaign,
    }
