"""Finite-stage scheduler with one durable start intent and graceful stop."""

from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence

from .config import ControllerConfig, SAFETY
from .longform_companion import (
    LongformCompanionRegistration,
    load_registration_if_present,
    read_companion_status,
)
from .state import ControlStore, StateError, utc_now


STAGES = ("acquisition", "preprocess", "gpu_readiness", "cold_retention")
OUTCOME_STATUSES = {"progressed", "held", "complete", "skipped"}
PIPELINE_TELEMETRY_SCHEMA_VERSION = 1
# Healthy independent lanes can cross running/waiting twice per poll. Persist one
# aggregate heartbeat at most this often; faults, Stop, quiesce, and material
# outcomes bypass the coalescer. A timer preserves the same bound when a stage
# remains inside one long finite call and emits no further transitions.
LANE_STATUS_MAX_STALENESS_SECONDS = 5.0
# Backend restart documents can be tens of megabytes even though the journal
# evidence between them is small.  Keep a bounded recovery tail without turning
# every material lane completion into a full checkpoint rewrite.  Lifecycle and
# retry boundaries bypass this cadence below.
CHECKPOINT_MAX_TAIL_EVENTS = 64
CHECKPOINT_MAX_STALENESS_SECONDS = 300.0
# In-process lane handoff data can be much larger than its durable evidence.  The
# coordinator delivers this one reserved artifact to peer backends, while the
# controller deliberately excludes it from the append-only event journal.
TRANSIENT_PEER_RUNTIME_ARTIFACT = "_transient_peer_runtime"
CheckpointAnchor = tuple[int, str]


class ControllerError(RuntimeError):
    """The autonomous scheduler or one of its closed stage adapters failed."""


class StartupRestoreStopRequested(Exception):
    """A durable Stop was observed at a complete startup-restore unit boundary.

    This is controller lifecycle flow, not a backend fault.  The interruptible
    restore surface raises it only after its current immutable unit has been
    completely inspected and before any partial backend checkpoint is admitted.
    """


@dataclass(frozen=True)
class StageOutcome:
    stage: str
    status: str
    progressed: bool
    monitor: dict[str, Any]
    artifacts: dict[str, Any]

    def normalized(self) -> dict[str, Any]:
        if self.stage not in STAGES:
            raise ControllerError(f"backend returned unsupported stage {self.stage!r}")
        if self.status not in OUTCOME_STATUSES:
            raise ControllerError(f"backend returned unsupported status {self.status!r}")
        if not isinstance(self.progressed, bool):
            raise ControllerError("backend progress marker must be boolean")
        if not isinstance(self.monitor, dict) or not isinstance(self.artifacts, dict):
            raise ControllerError("backend monitor and artifacts must be objects")
        return {
            "stage": self.stage,
            "status": self.status,
            "progressed": self.progressed,
            "monitor": self.monitor,
            "artifacts": self.artifacts,
        }


class PipelineBackend(Protocol):
    """The only execution surface available to the scheduler."""

    def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]: ...

    def run_stage(self, stage: str) -> StageOutcome: ...

    def run_parallel_stages(self) -> tuple[StageOutcome, StageOutcome]: ...


class CheckpointPipelineBackend(Protocol):
    """Optional fast-recovery surface discovered at runtime.

    A capable backend accepts either a validated backend checkpoint plus its
    journal tail, or ``None`` plus the full journal for the one-time metadata
    bootstrap which creates the first checkpoint.  Legacy backends need not
    implement any part of this protocol.
    """

    def restore_checkpoint(
        self,
        backend: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
    ) -> dict[str, Any]: ...

    def restore_checkpoint_interruptibly(
        self,
        backend: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
        *,
        cancellation_boundary: Callable[[], None],
    ) -> dict[str, Any]: ...

    def export_checkpoint(self) -> dict[str, Any] | None: ...

    def rebuild_from_checkpoint(
        self,
        backend: dict[str, Any] | None,
        tail_events: Sequence[dict[str, Any]],
    ) -> tuple[PipelineBackend, dict[str, Any]]: ...


def _failure(error: BaseException) -> dict[str, str]:
    return {
        "type": type(error).__name__,
        "message": str(error)[:2048],
    }


class AutonomousController:
    """Run closed, bounded stage calls until a durable stop request is observed.

    Stop is checked before and after every finite stage call.  A stage therefore
    reaches its own receipt boundary; an asynchronously admitted exact GPU child is
    then quiesced through its persisted systemd identity before the controller exits.
    """

    def __init__(
        self,
        config: ControllerConfig,
        store: ControlStore,
        backend: PipelineBackend,
        *,
        now: Callable[[], str] = utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        status_monotonic: Callable[[], float] = time.monotonic,
        checkpoint_monotonic: Callable[[], float] = time.monotonic,
    ):
        if store.config.config_id != config.config_id:
            raise ControllerError("state store belongs to a different configuration")
        self.config = config
        self.store = store
        self.backend = backend
        self.now = now
        self.monotonic = monotonic
        self.sleep = sleep
        self._status_monotonic = status_monotonic
        self._checkpoint_monotonic = checkpoint_monotonic
        self._checkpoint_cadence_initialized = False
        self._checkpoint_anchor_sequence = 0
        self._checkpoint_last_written_at: float | None = None
        self._checkpoint_pending_anchor: CheckpointAnchor | None = None
        # ``export_checkpoint() is None`` has one production meaning: a shared
        # lane committed logical queue authority before the journal-observed
        # root caught up.  Witness-only generations with an unchanged logical
        # digest are checkpointable directly.  Retain a true logical deferral's
        # distinction from an ordinary not-due poll so the independent scheduler
        # can briefly drain already-admitted calls instead of redispatching
        # forever and starving checkpoint export.
        self._checkpoint_export_deferred = False
        self._started_at: str | None = None
        self._cycle = 0
        self._consecutive_failures = 0
        # The public live counter may clear before healthy redispatch so the UI
        # does not advertise an active fault.  This private retry epoch remains
        # authoritative for the fail-closed ceiling until every lane which won a
        # primary fault has itself crossed a complete successful boundary.
        self._retry_epoch_failures = 0
        self._retry_fault_stages: set[str] = set()
        self._monitor: dict[str, Any] = {stage: None for stage in STAGES}
        self._last_error: dict[str, str] | None = None
        self._secondary_errors: list[dict[str, str]] = []
        self._completion_reason: str | None = None
        self._monitor_lock = threading.RLock()
        self._dispatch_sequence = 0
        self._scheduler_mode = "sequential_cycles"
        self._lane_monitor: dict[str, dict[str, Any]] = {
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
            for stage in STAGES
        }
        self._lane_material_status_pending: set[str] = set()
        self._lane_status_last_flush_at: float | None = None
        self._lane_status_dirty = False
        self._lane_status_timer: threading.Timer | None = None
        self._lane_status_timer_generation = 0
        self._lane_status_flush_error: BaseException | None = None
        self._quiesce_target: Any = backend
        # The adjacent registration is an explicit deployment opt-in.  Its
        # absence preserves the legacy controller exactly; when present, replay
        # it once here and use only the companion's bounded status projection at
        # terminal checks.
        self._longform_companion: LongformCompanionRegistration | None = (
            load_registration_if_present(config.path, config.physical_sha256)
        )
        self._telemetry_admission_sequence = 0
        self._queued_items: int | None = None
        self._queued_items_basis = "unavailable_no_admitted_ready_snapshot"
        self._queued_observation_sequence: int | None = None
        self._preprocessed_items = 0
        self._preprocessed_items_exact = True
        self._preprocessed_items_high_water = 0
        self._preprocessed_items_regressed = False
        self._preprocessed_items_basis = (
            "durable_stage_finished_preprocess_processed_items_sum"
        )
        self._asr_completed_items: int | None = None
        self._asr_completed_items_high_water: int | None = None
        self._asr_completed_items_regressed = False
        self._asr_completed_items_basis = (
            "unavailable_gpu_completed_item_count"
        )
        self._restore_pipeline_telemetry(store.events)

    @staticmethod
    def _telemetry_count(value: Any) -> int | None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return value

    def _restore_pipeline_telemetry(
        self, events: Sequence[dict[str, Any]]
    ) -> None:
        """Rebuild only bounded counters from the already validated journal.

        The public reader never replays the journal.  The foreground controller
        folds each admitted outcome once and persists the resulting projection in
        ``status.json`` instead.
        """

        for event in events:
            event_type = event.get("event_type")
            if event_type not in {"stage_finished", "gpu_child_quiesced"}:
                continue
            outcome = event.get("payload", {}).get("outcome")
            if not isinstance(outcome, dict):
                continue
            self._observe_pipeline_telemetry(
                outcome,
                durable_preprocess=event_type == "stage_finished",
            )

    def _observe_pipeline_telemetry(
        self,
        outcome: StageOutcome | dict[str, Any],
        *,
        durable_preprocess: bool,
    ) -> None:
        value = outcome.normalized() if isinstance(outcome, StageOutcome) else outcome
        stage = value.get("stage")
        monitor = value.get("monitor")
        progressed = value.get("progressed")
        if stage not in STAGES or not isinstance(monitor, dict):
            return
        self._telemetry_admission_sequence += 1

        ready_field: str | None = None
        ready_basis: str | None = None
        if stage == "acquisition":
            if "ready_items" in monitor:
                ready_field = "ready_items"
                ready_basis = "latest_admitted_acquisition_ready_items"
            elif "raw_normal_ready_items" in monitor:
                ready_field = "raw_normal_ready_items"
                ready_basis = (
                    "latest_admitted_acquisition_raw_normal_ready_items_legacy"
                )
        elif stage == "preprocess":
            if "ready_items_after" in monitor:
                ready_field = "ready_items_after"
                ready_basis = "latest_admitted_preprocess_ready_items_after"
            elif "raw_ready_items" in monitor:
                ready_field = "raw_ready_items"
                ready_basis = "latest_admitted_preprocess_raw_ready_items_legacy"
        if ready_field is not None and ready_basis is not None:
            self._queued_items = self._telemetry_count(monitor[ready_field])
            self._queued_items_basis = (
                ready_basis
                if self._queued_items is not None
                else "unavailable_invalid_ready_snapshot"
            )
            self._queued_observation_sequence = self._telemetry_admission_sequence
        elif stage in {"acquisition", "preprocess"}:
            # Do not retain an older stage's snapshot after a newer admission
            # whose current queue cardinality cannot be established.
            self._queued_items = None
            self._queued_items_basis = "unavailable_no_admitted_ready_snapshot"
            self._queued_observation_sequence = None

        if stage == "preprocess" and durable_preprocess:
            processed = self._telemetry_count(monitor.get("processed_items"))
            if progressed is True and processed is not None:
                self._preprocessed_items += processed
                self._preprocessed_items_high_water = max(
                    self._preprocessed_items_high_water,
                    self._preprocessed_items,
                )
                self._preprocessed_items_basis = (
                    "durable_stage_finished_preprocess_processed_items_sum"
                )
            elif progressed is True or processed not in {None, 0}:
                self._preprocessed_items_exact = False
                self._preprocessed_items_basis = (
                    "unavailable_incomplete_durable_preprocess_history"
                )

        if stage == "preprocess" and "preprocessed_items_cumulative" in monitor:
            self._observe_preprocessed_total(
                monitor["preprocessed_items_cumulative"],
                basis="latest_admitted_preprocess_receipt_total",
            )

        if stage == "gpu_readiness":
            self._observe_asr_completed_total(
                monitor.get("completed_items"),
                basis="latest_admitted_gpu_completed_items",
            )

    def _observe_preprocessed_total(self, value: Any, *, basis: str) -> None:
        count = self._telemetry_count(value)
        if self._preprocessed_items_regressed:
            self._preprocessed_items_exact = False
            self._preprocessed_items_basis = (
                "unavailable_preprocess_receipt_total_regressed"
            )
        elif count is None:
            self._preprocessed_items_exact = False
            self._preprocessed_items_basis = (
                "unavailable_invalid_preprocess_receipt_total"
            )
        elif count < max(
            self._preprocessed_items_high_water, self._preprocessed_items
        ):
            self._preprocessed_items_regressed = True
            self._preprocessed_items_exact = False
            self._preprocessed_items_basis = (
                "unavailable_preprocess_receipt_total_regressed"
            )
        else:
            self._preprocessed_items = count
            self._preprocessed_items_high_water = count
            self._preprocessed_items_exact = True
            self._preprocessed_items_basis = basis

    def _observe_asr_completed_total(self, value: Any, *, basis: str) -> None:
        completed = self._telemetry_count(value)
        if self._asr_completed_items_regressed:
            self._asr_completed_items = None
            self._asr_completed_items_basis = (
                "unavailable_gpu_completed_item_count_regressed"
            )
        elif completed is None:
            self._asr_completed_items = None
            self._asr_completed_items_basis = (
                "unavailable_gpu_completed_item_count"
            )
        elif (
            self._asr_completed_items_high_water is not None
            and completed < self._asr_completed_items_high_water
        ):
            self._asr_completed_items = None
            self._asr_completed_items_regressed = True
            self._asr_completed_items_basis = (
                "unavailable_gpu_completed_item_count_regressed"
            )
        else:
            self._asr_completed_items = completed
            self._asr_completed_items_high_water = completed
            self._asr_completed_items_basis = basis

    def _observe_recovery_pipeline_telemetry(self, recovery: dict[str, Any]) -> None:
        if "preprocessed_items_cumulative" in recovery:
            self._observe_preprocessed_total(
                recovery["preprocessed_items_cumulative"],
                basis="validated_backend_restore_preprocess_receipt_total",
            )
        if "gpu_completed_items_restored" in recovery:
            self._observe_asr_completed_total(
                recovery["gpu_completed_items_restored"],
                basis="validated_backend_restore_gpu_completed_items",
            )

    def _pipeline_telemetry(self) -> dict[str, Any]:
        return {
            "schema_version": PIPELINE_TELEMETRY_SCHEMA_VERSION,
            "queued_items": self._queued_items,
            "queued_items_basis": self._queued_items_basis,
            "queued_observation_sequence": self._queued_observation_sequence,
            "preprocessed_items": (
                self._preprocessed_items
                if self._preprocessed_items_exact
                else None
            ),
            "preprocessed_items_basis": (
                self._preprocessed_items_basis
            ),
            "asr_completed_items": self._asr_completed_items,
            "asr_completed_items_basis": self._asr_completed_items_basis,
        }

    def _desired_state(self) -> str:
        return self.store.read_control()["desired_state"]

    def _startup_restore_cancellation_boundary(self) -> None:
        """Convert a durable Stop into non-fault startup lifecycle flow."""

        if self._desired_state() != "running":
            raise StartupRestoreStopRequested

    def _restore_backend(self) -> tuple[dict[str, Any], str, int]:
        """Restore from one validated checkpoint tail when the backend opts in.

        The no-checkpoint call into ``restore_checkpoint`` is intentional.  It
        lets a production backend perform its one-time metadata bootstrap without
        routing through the legacy full-payload replay.  Simple/legacy backends
        retain their original ``restore(full_journal)`` behavior.
        """

        view = self.store.recovery_view()
        restore_checkpoint = getattr(self.backend, "restore_checkpoint", None)
        if callable(restore_checkpoint):
            backend_document = (
                None if view.checkpoint is None else view.checkpoint["backend"]
            )
            interruptible_restore = getattr(
                self.backend, "restore_checkpoint_interruptibly", None
            )
            if callable(interruptible_restore):
                # The production backend first certifies empty GPU-child
                # authority (or defers Stop until exact GPU recovery/quiesce),
                # then invokes this only between complete immutable restore
                # units. Legacy/test backends retain their original all-or-
                # nothing restore_checkpoint surface.
                restored = interruptible_restore(
                    backend_document,
                    view.tail_events,
                    cancellation_boundary=(
                        self._startup_restore_cancellation_boundary
                    ),
                )
            else:
                restored = restore_checkpoint(backend_document, view.tail_events)
            mode = (
                "checkpoint_tail"
                if view.checkpoint is not None
                else "checkpoint_metadata_bootstrap"
            )
            replayed = len(view.tail_events)
        else:
            restored = self.backend.restore(self.store.events)
            mode = "legacy_full_journal"
            replayed = len(self.store.events)
        if not isinstance(restored, dict):
            raise ControllerError("backend restore monitor must be an object")
        self._initialize_checkpoint_cadence(view.anchor_sequence)
        return restored, mode, replayed

    def _initialize_checkpoint_cadence(self, anchor_sequence: int) -> None:
        """Start one process-local cadence from a validated recovery anchor."""

        if (
            isinstance(anchor_sequence, bool)
            or not isinstance(anchor_sequence, int)
            or anchor_sequence < 0
        ):
            raise ControllerError("checkpoint recovery anchor is invalid")
        observed = self._checkpoint_monotonic()
        if (
            isinstance(observed, bool)
            or not isinstance(observed, (int, float))
            or not math.isfinite(float(observed))
        ):
            raise ControllerError("checkpoint monotonic clock is invalid")
        self._checkpoint_anchor_sequence = anchor_sequence
        self._checkpoint_last_written_at = float(observed)
        self._checkpoint_pending_anchor = None
        self._checkpoint_export_deferred = False
        self._checkpoint_cadence_initialized = True

    def _checkpoint_due(
        self,
        anchor: CheckpointAnchor,
        *,
        force: bool,
    ) -> bool:
        """Return whether one already-applied journal head should be persisted."""

        sequence, _event_sha256 = anchor
        if sequence <= self._checkpoint_anchor_sequence:
            return False
        if force or self._checkpoint_anchor_sequence == 0:
            return True
        if sequence - self._checkpoint_anchor_sequence >= CHECKPOINT_MAX_TAIL_EVENTS:
            return True
        observed = self._checkpoint_monotonic()
        if (
            isinstance(observed, bool)
            or not isinstance(observed, (int, float))
            or not math.isfinite(float(observed))
        ):
            raise ControllerError("checkpoint monotonic clock is invalid")
        last = self._checkpoint_last_written_at
        if last is None or float(observed) < last:
            raise ControllerError("checkpoint monotonic clock regressed")
        return float(observed) - last >= CHECKPOINT_MAX_STALENESS_SECONDS

    def _write_backend_checkpoint(
        self,
        anchor: CheckpointAnchor | None = None,
        force: bool = False,
    ) -> bool:
        """Persist a due backend snapshot at one journal/root boundary.

        ``anchor`` is supplied only after the backend root has applied that exact
        event.  A ``None`` anchor is a cheap cadence poll: it may flush a previously
        applied pending anchor after the elapsed-time bound, but it never invents
        authority for an event which the root has not observed.
        """

        export = getattr(self.backend, "export_checkpoint", None)
        restore_checkpoint = getattr(self.backend, "restore_checkpoint", None)
        if not callable(export) or not callable(restore_checkpoint):
            return False
        if not self._checkpoint_cadence_initialized:
            view = self.store.recovery_view()
            self._initialize_checkpoint_cadence(view.anchor_sequence)
        if anchor is None:
            anchor = self._checkpoint_pending_anchor
            if anchor is None:
                return False
        sequence, event_sha256 = anchor
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence < 1
            or not isinstance(event_sha256, str)
        ):
            raise ControllerError("backend checkpoint anchor is invalid")
        pending = self._checkpoint_pending_anchor
        if pending is not None and sequence < pending[0]:
            raise ControllerError(
                "backend checkpoint anchor regresses pending authority"
            )
        self._checkpoint_pending_anchor = anchor
        # Once a due export has reached the backend and been atomically
        # deferred, it remains due until publication succeeds.  In particular,
        # do not forget a forced lifecycle/quiesce boundary merely because its
        # drain retry uses the ordinary poll surface.
        if not self._checkpoint_due(
            anchor,
            force=force or self._checkpoint_export_deferred,
        ):
            return False
        backend_document = export()
        if backend_document is None:
            # A shared lane may have committed exact queue state just before its
            # journal/root outcome boundary. Preserve the pending anchor and try
            # again on the next serialized poll; checkpoint+tail remains valid.
            self._checkpoint_export_deferred = True
            return False
        if not isinstance(backend_document, dict):
            raise ControllerError("backend checkpoint export must be an object")
        self.store.write_checkpoint(
            backend_document,
            created_at=self.now(),
            expected_anchor_sequence=sequence,
            expected_anchor_sha256=event_sha256,
        )
        completed_at = self._checkpoint_monotonic()
        if (
            isinstance(completed_at, bool)
            or not isinstance(completed_at, (int, float))
            or not math.isfinite(float(completed_at))
            or (
                self._checkpoint_last_written_at is not None
                and float(completed_at) < self._checkpoint_last_written_at
            )
        ):
            raise ControllerError("checkpoint monotonic clock is invalid")
        self._checkpoint_anchor_sequence = sequence
        self._checkpoint_last_written_at = float(completed_at)
        self._checkpoint_pending_anchor = None
        self._checkpoint_export_deferred = False
        return True

    def _checkpoint_event(
        self,
        event: dict[str, Any] | None,
        *,
        force: bool = False,
    ) -> bool:
        if event is None:
            return False
        return self._write_backend_checkpoint(
            (event["sequence"], event["event_sha256"]),
            force=force,
        )

    def _discard_checkpoint_poll_anchor(self) -> None:
        """Fence cadence polling after an unobserved failure-journal boundary."""

        self._checkpoint_pending_anchor = None
        self._checkpoint_export_deferred = False

    def _checkpoint_export_is_deferred(self) -> bool:
        """Return whether a due snapshot awaits a shared/root drain boundary."""

        return self._checkpoint_export_deferred

    def _status(self, lifecycle: str, *, current_stage: str | None = None) -> dict[str, Any]:
        last = self.store.events[-1] if self.store.events else None
        recent = [
            {
                "sequence": event["sequence"],
                "event_type": event["event_type"],
                "occurred_at": event["occurred_at"],
                "event_sha256": event["event_sha256"],
            }
            for event in self.store.events[-16:]
        ]
        progress = {
            stage: (
                None
                if not isinstance(value, dict)
                else {
                    key: value[key]
                    for key in (
                        "status",
                        "progressed",
                        "completed",
                        "pending",
                        "processed_items",
                        "completed_items",
                        "ready_batches",
                        "retained_items",
                        "pending_items",
                        "quarantined_items",
                        "parked_items",
                        "parked_batches",
                        "requires_chunking_items_cumulative",
                        "active_children",
                    )
                    if key in value
                }
            )
            for stage, value in self._monitor.items()
            if stage in STAGES
        }
        acquisition = self._monitor.get("acquisition")
        preprocess = self._monitor.get("preprocess")
        cold = self._monitor.get("cold_retention")
        gpu = self._monitor.get("gpu_readiness")
        campaign = self.config.section("campaign")
        recovery = self._monitor.get("recovery")
        coverage = {
            "collection_count": None,
            "candidate_count": None,
            "ready_selected_count": None,
            "parked_requires_chunking_count": None,
            "estimated_selected_bytes": None,
            **(
                recovery.get("campaign_coverage", {})
                if isinstance(recovery, dict)
                and isinstance(recovery.get("campaign_coverage", {}), dict)
                else {}
            ),
        }
        inventory_chunking = coverage.get("parked_requires_chunking_count")
        if isinstance(inventory_chunking, bool) or not isinstance(inventory_chunking, int):
            inventory_chunking = 0
        gpu_chunking = (
            gpu.get("requires_chunking_items_cumulative", 0)
            if isinstance(gpu, dict)
            else 0
        )
        if isinstance(gpu_chunking, bool) or not isinstance(gpu_chunking, int):
            gpu_chunking = 0
        acquisition_quarantined = (
            acquisition.get("quarantined_items", 0)
            if isinstance(acquisition, dict)
            else 0
        )
        if isinstance(acquisition_quarantined, bool) or not isinstance(
            acquisition_quarantined, int
        ):
            acquisition_quarantined = 0
        preprocess_parked = (
            preprocess.get("parked_items", 0)
            if isinstance(preprocess, dict)
            else 0
        )
        if isinstance(preprocess_parked, bool) or not isinstance(
            preprocess_parked, int
        ):
            preprocess_parked = 0
        gpu_parked_items = (
            gpu.get("parked_items", 0) if isinstance(gpu, dict) else 0
        )
        if isinstance(gpu_parked_items, bool) or not isinstance(
            gpu_parked_items, int
        ):
            gpu_parked_items = 0
        postprocess_backlog = inventory_chunking + gpu_chunking
        unresolved_parked = (
            postprocess_backlog
            + acquisition_quarantined
            + preprocess_parked
            + gpu_parked_items
        )
        active_lanes = [
            stage
            for stage, value in self._lane_monitor.items()
            if isinstance(value, dict) and value.get("active") == 1
        ]
        if self._scheduler_mode == "independent_lanes" and current_stage is None:
            current_stage = (
                active_lanes[0]
                if len(active_lanes) == 1
                else "concurrent"
                if active_lanes
                else None
            )
        return {
            "kind": "himr_autonomous_controller_status",
            "schema_version": 1,
            "config_id": self.config.config_id,
            "config_sha256": self.config.physical_sha256,
            "lifecycle": lifecycle,
            "actual_state": lifecycle,
            "desired_state": self._desired_state(),
            "current_stage": current_stage,
            "pid": os.getpid(),
            "started_at": self._started_at,
            "updated_at": self.now(),
            "cycle": self._cycle,
            "dispatch_sequence": self._dispatch_sequence,
            "scheduler_mode": self._scheduler_mode,
            "completion_reason": self._completion_reason,
            "campaign": {
                "campaign_id": campaign["campaign_id"],
                "schedule_set_id": campaign["schedule_set"]["schedule_set_id"],
                "inventory_kind": campaign["inventory"]["kind"],
                "configured_schedule_count": len(campaign["schedules"]),
                "configured_normal_schedule_count": sum(
                    schedule["role"] == "normal_processing"
                    for schedule in campaign["schedules"]
                ),
                "configured_cold_only_schedule_count": sum(
                    schedule["role"]
                    == "cold_acquisition_only_requires_chunking"
                    for schedule in campaign["schedules"]
                ),
                "collection_filter": "none_sealed_inventory_controls_coverage",
                **coverage,
                "inventory_requires_chunking_backlog": inventory_chunking,
                "gpu_requires_chunking_backlog": gpu_chunking,
                "postprocess_backlog_count": postprocess_backlog,
                "scheduled_acquisition_quarantined_count": acquisition_quarantined,
                "preprocess_parked_count": preprocess_parked,
                "gpu_parked_item_count": gpu_parked_items,
                "unresolved_parked_items": unresolved_parked,
                "campaign_complete": self._completion_reason == "campaign_drained",
            },
            "consecutive_failures": self._consecutive_failures,
            "last_error": self._last_error,
            "errors": [
                *([] if self._last_error is None else [self._last_error]),
                *self._secondary_errors,
            ],
            "last_event": (
                None
                if last is None
                else {
                    "sequence": last["sequence"],
                    "event_type": last["event_type"],
                    "event_sha256": last["event_sha256"],
                }
            ),
            "monitor": self._monitor,
            "stages": {stage: self._monitor.get(stage) for stage in STAGES},
            "lanes": {
                stage: dict(value) for stage, value in self._lane_monitor.items()
            },
            "execution": {
                "accepting_new_work": (
                    lifecycle in {"starting", "running", "retrying"}
                    and self._desired_state() == "running"
                    and self._last_error is None
                ),
                "draining": lifecycle == "stopping",
                "inflight_total": len(active_lanes),
            },
            "progress": progress,
            "pipeline_telemetry": self._pipeline_telemetry(),
            "throughput": {
                "last_cycle_new_acquisition_items": (
                    acquisition.get("new_items", 0)
                    if isinstance(acquisition, dict)
                    else 0
                ),
                "last_cycle_new_acquisition_bytes": (
                    acquisition.get("new_bytes", 0)
                    if isinstance(acquisition, dict)
                    else 0
                ),
            },
            "storage": {
                "acquisition_ready_bytes": (
                    acquisition.get("ready_bytes")
                    if isinstance(acquisition, dict)
                    else None
                ),
                "acquisition_ready_items": (
                    acquisition.get("ready_items")
                    if isinstance(acquisition, dict)
                    else None
                ),
                "campaign_ready_high_bytes": campaign["global_ready_high_bytes"],
                "campaign_ready_high_items": campaign["global_ready_high_items"],
                "acquisition_backpressure": (
                    acquisition.get("stop_reason")
                    if isinstance(acquisition, dict)
                    and acquisition.get("status") == "held"
                    else None
                ),
                "cold_only_acquisition_ready_bytes": (
                    acquisition.get("cold_only_ready_bytes")
                    if isinstance(acquisition, dict)
                    else None
                ),
                "cold_only_acquisition_ready_items": (
                    acquisition.get("cold_only_ready_items")
                    if isinstance(acquisition, dict)
                    else None
                ),
                "cold_retained_items": (
                    cold.get("retained_items") if isinstance(cold, dict) else None
                ),
                "hot_deletions": 0,
            },
            "recent_activity": recent,
            "current_gpu_child": (
                gpu.get("current_gpu_child")
                if isinstance(gpu, dict)
                else None
            ),
            "safety": dict(SAFETY),
        }

    def _cancel_lane_status_timer_locked(self) -> None:
        self._lane_status_timer_generation += 1
        timer = self._lane_status_timer
        self._lane_status_timer = None
        if timer is not None:
            timer.cancel()

    def _write_status_locked(
        self, lifecycle: str, *, current_stage: str | None = None
    ) -> None:
        """Persist one authoritative status and supersede any idle heartbeat."""

        self._cancel_lane_status_timer_locked()
        self.store.write_status(
            self._status(lifecycle, current_stage=current_stage)
        )
        self._lane_status_last_flush_at = self._status_monotonic()
        self._lane_status_dirty = False
        self._lane_status_flush_error = None

    def _flush_coalesced_lane_status(self, generation: int) -> None:
        """Timer boundary for a dirty healthy aggregate lane projection."""

        with self._monitor_lock:
            if (
                generation != self._lane_status_timer_generation
                or not self._lane_status_dirty
            ):
                return
            self._lane_status_timer = None
            lifecycle = (
                "running" if self._desired_state() == "running" else "stopping"
            )
            try:
                self._write_status_locked(lifecycle)
            except BaseException as error:
                # A timer cannot throw into a coordinator lane. Preserve the exact
                # failure so the next lane boundary raises it and enters the normal
                # fault/quiesce path. Explicit terminal/Stop writes may meanwhile
                # retry and clear it synchronously.
                self._lane_status_flush_error = error

    def _arm_lane_status_timer_locked(self, delay: float) -> None:
        if self._lane_status_timer is not None:
            return
        self._lane_status_timer_generation += 1
        generation = self._lane_status_timer_generation
        timer = threading.Timer(
            max(0.001, delay),
            self._flush_coalesced_lane_status,
            args=(generation,),
        )
        timer.daemon = True
        self._lane_status_timer = timer
        timer.start()

    def _write_status(self, lifecycle: str, *, current_stage: str | None = None) -> None:
        with self._monitor_lock:
            self._write_status_locked(lifecycle, current_stage=current_stage)

    def _record_secondary_error(
        self, context: str, error: BaseException
    ) -> None:
        failure = _failure(error)
        failure["message"] = f"{context}: {failure['message']}"[:2048]
        with self._monitor_lock:
            if failure not in self._secondary_errors:
                # ``errors`` is a bounded current-condition diagnostic, not a
                # second journal.  Durable failure history remains in events.
                self._secondary_errors = [
                    *self._secondary_errors[-14:],
                    failure,
                ]

    def _write_status_after_failure(
        self, lifecycle: str, *, context: str
    ) -> bool:
        """Best-effort status publication which cannot replace a primary error."""

        try:
            self._write_status(lifecycle)
        except BaseException as error:
            self._record_secondary_error(context, error)
            return False
        return True

    def _clear_live_failure_state(self, *, reset_retry_epoch: bool = True) -> None:
        """Clear retry gating after a rebuilt backend survives its backoff."""

        with self._monitor_lock:
            self._consecutive_failures = 0
            self._last_error = None
            self._secondary_errors = []
            if reset_retry_epoch:
                self._retry_epoch_failures = 0
                self._retry_fault_stages.clear()

    def _record_lane_recovery(self, stage: str) -> None:
        """Close a retry epoch only when its previously faulting lane recovers."""

        if stage not in STAGES:
            raise ControllerError("independent scheduler recovered an invalid lane")
        with self._monitor_lock:
            self._retry_fault_stages.discard(stage)
            # A coordinator-construction retry has no lane identity of its own.
            # The first rebuilt lane which crosses the coordinator's full success
            # boundary proves that construction recovered.  A generic controller
            # fault after construction (especially terminal publication after
            # ``run`` returns) must *not* be erased here: it clears only when the
            # whole controller try block later completes successfully.
            self._retry_fault_stages.discard("controller_construction")
            if not self._retry_fault_stages:
                self._retry_epoch_failures = 0

    def _admit_outcome_boundary(
        self,
        outcome: StageOutcome,
        stage: str,
        *,
        dispatch_id: int | None = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Return progress and the exact newly admitted journal anchor."""

        if outcome.stage != stage:
            raise ControllerError(
                f"backend returned {outcome.stage!r} while {stage!r} was active"
            )
        normalized = outcome.normalized()
        durable_artifacts = dict(normalized["artifacts"])
        durable_artifacts.pop(TRANSIENT_PEER_RUNTIME_ARTIFACT, None)
        durable_outcome = {**normalized, "artifacts": durable_artifacts}
        event: dict[str, Any] | None = None
        with self._monitor_lock:
            previous = self._monitor.get(stage)
            self._monitor[stage] = {
                "status": outcome.status,
                "progressed": outcome.progressed,
                **outcome.monitor,
            }
            if dispatch_id is not None:
                self._dispatch_sequence = max(self._dispatch_sequence, dispatch_id)
            should_journal = outcome.progressed or (
                outcome.status in {"complete", "skipped"}
                and (
                    not isinstance(previous, dict)
                    or previous.get("status") != outcome.status
                )
            )
            if should_journal:
                payload: dict[str, Any] = {
                    "cycle": self._cycle,
                    "outcome": durable_outcome,
                }
                if dispatch_id is not None:
                    payload["dispatch_id"] = dispatch_id
                event = self.store.append_event(
                    "stage_finished", payload, occurred_at=self.now()
                )
                self._monitor[stage]["event_sha256"] = event["event_sha256"]
            self._observe_pipeline_telemetry(
                durable_outcome,
                durable_preprocess=should_journal,
            )
        return outcome.progressed, event

    def _admit_outcome(
        self,
        outcome: StageOutcome,
        stage: str,
        *,
        dispatch_id: int | None = None,
    ) -> bool:
        """Validate, publish, and durably journal one finite stage outcome."""

        progressed, _event = self._admit_outcome_boundary(
            outcome, stage, dispatch_id=dispatch_id
        )
        return progressed

    def _set_lane_state(self, stage: str, value: dict[str, Any]) -> None:
        if stage not in STAGES or not isinstance(value, dict):
            raise ControllerError("independent lane published invalid state")
        with self._monitor_lock:
            if self._lane_status_flush_error is not None:
                error = self._lane_status_flush_error
                self._lane_status_flush_error = None
                raise error
            self._lane_monitor[stage] = dict(value)
            dispatch_id = value.get("dispatch_id")
            if isinstance(dispatch_id, int) and not isinstance(dispatch_id, bool):
                self._dispatch_sequence = max(self._dispatch_sequence, dispatch_id)
                if self._scheduler_mode == "independent_lanes":
                    self._cycle = max(self._cycle, dispatch_id)
            lifecycle = (
                "running" if self._desired_state() == "running" else "stopping"
            )
            state = value.get("state")
            immediate = (
                lifecycle != "running"
                or state == "faulted"
                or value.get("wait_reason") == "gpu_child_quiesced"
                or stage in self._lane_material_status_pending
                or state not in {"idle", "running", "waiting"}
            )
            observed_at = self._status_monotonic()
            elapsed = (
                None
                if self._lane_status_last_flush_at is None
                else observed_at - self._lane_status_last_flush_at
            )
            if (
                immediate
                or elapsed is None
                or elapsed >= LANE_STATUS_MAX_STALENESS_SECONDS
            ):
                self._write_status_locked(lifecycle)
                self._lane_material_status_pending.discard(stage)
                return
            self._lane_status_dirty = True
            self._arm_lane_status_timer_locked(
                LANE_STATUS_MAX_STALENESS_SECONDS - max(0.0, elapsed)
            )

    def _set_lane_fault(self, stage: str, error: BaseException) -> None:
        """Expose a first lane fault without admitting a concurrent event.

        A worker can fail while acquisition is still inside a long finite call.
        Status is mutable and thread-safe, so publish the error immediately.  The
        append-only ``cycle_failed`` event remains owned by ``run`` after all lane
        threads join; admitting it here could put later in-flight stage outcomes
        on the wrong side of the failure event.
        """

        if stage not in STAGES or not isinstance(error, BaseException):
            raise ControllerError("independent lane published invalid fault")
        with self._monitor_lock:
            self._lane_material_status_pending.discard(stage)
            self._last_error = _failure(error)
            lifecycle = (
                "running" if self._desired_state() == "running" else "stopping"
            )
            self._write_status_locked(lifecycle)

    def _admit_lane_outcome(
        self, stage: str, outcome: StageOutcome, dispatch_id: int
    ) -> CheckpointAnchor | None:
        # The coordinator immediately publishes the lane's resulting idle/waiting
        # state after peer observation. Mark a changed/material projection so that
        # publication bypasses healthy-transition coalescing. Stable held polls are
        # already represented in memory and need only the bounded aggregate timer.
        # The material outcome itself is durable in the journal before return.
        with self._monitor_lock:
            previous = self._monitor.get(stage)
            previous_projection = (
                {
                    key: value
                    for key, value in previous.items()
                    if key != "event_sha256"
                }
                if isinstance(previous, dict)
                else previous
            )
        _progressed, event = self._admit_outcome_boundary(
            outcome, stage, dispatch_id=dispatch_id
        )
        with self._monitor_lock:
            current = self._monitor.get(stage)
            current_projection = (
                {
                    key: value
                    for key, value in current.items()
                    if key != "event_sha256"
                }
                if isinstance(current, dict)
                else current
            )
            if (
                outcome.progressed
                or previous_projection != current_projection
            ):
                self._lane_material_status_pending.add(stage)
        return (
            None
            if event is None
            else (event["sequence"], event["event_sha256"])
        )

    def _wait_interruptibly(
        self,
        seconds: float,
        *,
        lifecycle: str,
        preserve_primary: bool = False,
    ) -> bool:
        deadline = self.monotonic() + seconds
        while True:
            if self._desired_state() == "stopped":
                return False
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                return True
            if preserve_primary:
                self._write_status_after_failure(
                    lifecycle,
                    context=f"{lifecycle}_status_publication",
                )
            else:
                self._write_status(lifecycle)
            self.sleep(min(1.0, remaining))

    def run_cycle(self) -> bool:
        """Run at most one bounded call in each stage.

        Returns ``False`` as soon as stop is requested; otherwise returns ``True``
        after the complete stage sequence.
        """

        self._cycle += 1
        progressed = False
        parallel = getattr(self.backend, "run_parallel_stages", None)
        if parallel is not None:
            if self._desired_state() == "stopped":
                return False
            self._write_status(
                "running", current_stage="acquisition+preprocess"
            )
            # Flush an elapsed pending anchor before a finite call is allowed to
            # update non-journaled runtime caches on the root backend.
            self._write_backend_checkpoint(None)
            outcomes = parallel()
            if (
                not isinstance(outcomes, tuple)
                or len(outcomes) != 2
                or not all(isinstance(value, StageOutcome) for value in outcomes)
            ):
                raise ControllerError(
                    "backend parallel lanes returned an invalid outcome pair"
            )
            final_event: dict[str, Any] | None = None
            for stage, outcome in zip(STAGES[:2], outcomes, strict=True):
                stage_progressed, event = self._admit_outcome_boundary(
                    outcome, stage
                )
                progressed = stage_progressed or progressed
                if event is not None:
                    final_event = event
            # The backend has already applied both overlapping outcomes by the
            # time it returns.  Anchor only after both journal admissions so a
            # checkpoint cannot include preprocess state ahead of its journal.
            self._checkpoint_event(final_event)
            self._write_status("running")
            if self._desired_state() == "stopped":
                return False
            remaining_stages = STAGES[2:]
        else:
            remaining_stages = STAGES

        for stage in remaining_stages:
            if self._desired_state() == "stopped":
                return False
            self._write_status("running", current_stage=stage)
            self._write_backend_checkpoint(None)
            outcome = self.backend.run_stage(stage)
            stage_progressed, event = self._admit_outcome_boundary(outcome, stage)
            progressed = stage_progressed or progressed
            self._checkpoint_event(event)
            self._write_status("running")
            if self._desired_state() == "stopped":
                return False
        if progressed:
            event = self.store.append_event(
                "cycle_finished",
                {"cycle": self._cycle, "progressed": True},
                occurred_at=self.now(),
            )
            self._checkpoint_event(event)
        terminal = self._terminal_disposition()
        if terminal is not None:
            self._record_terminal_disposition(terminal)
            return False
        self._write_status("running")
        return True

    def _record_terminal_disposition(self, terminal: str) -> None:
        if terminal not in {
            "campaign_drained",
            "primary_pass_drained_with_postprocess_backlog",
            "primary_pass_drained_with_parked_items",
        }:
            raise ControllerError("independent scheduler returned invalid terminal state")
        with self._monitor_lock:
            self._completion_reason = terminal
            self.store.set_desired_state("stopped", requested_at=self.now())
            event = self.store.append_event(
                (
                    "campaign_drained"
                    if terminal == "campaign_drained"
                    else "primary_pass_drained_with_backlog"
                ),
                {
                    "cycle": self._cycle,
                    "dispatch_sequence": self._dispatch_sequence,
                    "parked_requires_chunking_count": self._parked_item_count(),
                    "gpu_requires_chunking_count": self._gpu_chunking_count(),
                    "scheduled_quarantine_count": self._scheduled_quarantine_count(),
                    "preprocess_parked_count": self._preprocess_parked_count(),
                    "gpu_parked_item_count": self._gpu_parked_item_count(),
                },
                occurred_at=self.now(),
            )
            self._checkpoint_event(event)
            self._write_status_locked(
                "completed" if terminal == "campaign_drained" else "blocked"
            )

    def _parked_item_count(self) -> int:
        recovery = self._monitor.get("recovery")
        if not isinstance(recovery, dict):
            return 0
        coverage = recovery.get("campaign_coverage")
        if not isinstance(coverage, dict):
            return 0
        value = coverage.get("parked_requires_chunking_count", 0)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    def _gpu_chunking_count(self) -> int:
        gpu = self._monitor.get("gpu_readiness")
        if not isinstance(gpu, dict):
            return 0
        value = gpu.get("requires_chunking_items_cumulative", 0)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    def _scheduled_quarantine_count(self) -> int:
        acquisition = self._monitor.get("acquisition")
        if not isinstance(acquisition, dict):
            return 0
        value = acquisition.get("quarantined_items", 0)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    def _preprocess_parked_count(self) -> int:
        preprocess = self._monitor.get("preprocess")
        if not isinstance(preprocess, dict):
            return 0
        value = preprocess.get("parked_items", 0)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    def _gpu_parked_item_count(self) -> int:
        gpu = self._monitor.get("gpu_readiness")
        if not isinstance(gpu, dict):
            return 0
        value = gpu.get("parked_items", 0)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    def _terminal_disposition(self) -> str | None:
        with self._monitor_lock:
            return self._terminal_disposition_unlocked()

    def _longform_postprocess_complete(self) -> bool:
        """Return whether the registered companion covers its entire backlog.

        A partial snapshot is a normal wait state: keeping the primary
        controller alive also keeps its durable Start intent available to the
        companion.  A malformed or faulted snapshot is not silently treated as
        completion and therefore fails the registered deployment closed.
        """

        registration = self._longform_companion
        if registration is None:
            return False
        status = read_companion_status(registration)
        if status["lifecycle"] == "faulted":
            error = status["last_error"]
            detail = (
                error["message"]
                if isinstance(error, dict) and isinstance(error.get("message"), str)
                else "unknown companion failure"
            )
            raise ControllerError(f"registered long-form companion faulted: {detail}")
        discovered = status["discovered"]
        jobs = status["jobs"]
        return bool(
            status["active_job"] is None
            and discovered["cold_candidates"] == status["expected_cold_backlog"]
            and jobs["completed"] == discovered["total_candidates"]
        )

    def _terminal_disposition_unlocked(self) -> str | None:
        acquisition = self._monitor.get("acquisition")
        preprocess = self._monitor.get("preprocess")
        gpu = self._monitor.get("gpu_readiness")
        cold = self._monitor.get("cold_retention")
        scheduled_drained = bool(
            isinstance(acquisition, dict)
            and acquisition.get("status") == "complete"
            and acquisition.get("pending") == 0
            and isinstance(preprocess, dict)
            and preprocess.get("status") == "complete"
            and preprocess.get("ready_items_after") == 0
            and isinstance(gpu, dict)
            and gpu.get("status") in {"complete", "skipped"}
            and gpu.get("pending_batches", 0) == 0
            and gpu.get("active_children", 0) == 0
            and isinstance(cold, dict)
            and cold.get("status") in {"complete", "skipped"}
            and cold.get("pending_items", 0) == 0
            and cold.get("replay_pending_items", 0) == 0
        )
        if not scheduled_drained:
            return None
        if self._parked_item_count() + self._gpu_chunking_count() > 0:
            if self._longform_companion is None:
                return "primary_pass_drained_with_postprocess_backlog"
            if not self._longform_postprocess_complete():
                return None
        if (
            self._scheduled_quarantine_count()
            + self._preprocess_parked_count()
            + self._gpu_parked_item_count()
            > 0
        ):
            return "primary_pass_drained_with_parked_items"
        return "campaign_drained"

    def _quiesce_backend(self, backend: Any | None = None) -> None:
        target = self._quiesce_target if backend is None else backend
        quiesce = getattr(target, "quiesce", None)
        if quiesce is None:
            return
        outcome = quiesce()
        if outcome is None:
            return
        self._admit_gpu_quiesce_outcome(outcome)

    def _quiesce_after_failure(self, *, context: str) -> bool:
        """Attempt exact child shutdown without replacing the primary failure."""

        try:
            self._quiesce_backend()
        except BaseException as error:
            self._record_secondary_error(context, error)
            return False
        return True

    def _admit_gpu_quiesce_outcome(
        self, outcome: StageOutcome
    ) -> CheckpointAnchor | None:
        """Journal one material owning-lane quiesce without stage dependencies."""

        if not isinstance(outcome, StageOutcome) or outcome.stage != "gpu_readiness":
            raise ControllerError("backend quiesce returned an invalid GPU outcome")
        normalized = outcome.normalized()
        event: dict[str, Any] | None = None
        with self._monitor_lock:
            self._monitor["gpu_readiness"] = {
                "status": outcome.status,
                "progressed": outcome.progressed,
                **outcome.monitor,
            }
            if outcome.progressed:
                event = self.store.append_event(
                    "gpu_child_quiesced",
                    {"cycle": self._cycle, "outcome": normalized},
                    occurred_at=self.now(),
                )
                self._monitor["gpu_readiness"]["event_sha256"] = event[
                    "event_sha256"
                ]
            self._observe_pipeline_telemetry(
                normalized,
                durable_preprocess=False,
            )
        return (
            None
            if event is None
            else (event["sequence"], event["event_sha256"])
        )

    def _rebuild_independent_backend(self) -> None:
        """Replace a partial lane root from a checkpoint and admitted tail."""

        previous = self.backend
        view = self.store.recovery_view()
        rebuild_checkpoint = getattr(previous, "rebuild_from_checkpoint", None)
        if callable(rebuild_checkpoint):
            backend_document = (
                None if view.checkpoint is None else view.checkpoint["backend"]
            )
            rebuilt = rebuild_checkpoint(backend_document, view.tail_events)
            replayed_event_count = len(view.tail_events)
            rebuild_mode = (
                "checkpoint_tail"
                if view.checkpoint is not None
                else "checkpoint_metadata_bootstrap"
            )
        else:
            rebuild = getattr(previous, "rebuild_from_events", None)
            if not callable(rebuild):
                raise ControllerError(
                    "independent backend cannot rebuild after a partial lane failure"
                )
            replay_events = tuple(self.store.events)
            rebuilt = rebuild(replay_events)
            replayed_event_count = len(replay_events)
            rebuild_mode = "legacy_full_journal"
        if (
            not isinstance(rebuilt, tuple)
            or len(rebuilt) != 2
            or rebuilt[0] is previous
            or not isinstance(rebuilt[1], dict)
            or not all(
                callable(getattr(rebuilt[0], name, None))
                for name in ("fork_lane", "observe_peer_outcome")
            )
        ):
            raise ControllerError(
                "independent backend rebuild returned an invalid fresh root"
            )
        backend, recovery = rebuilt
        self.backend = backend
        self._quiesce_target = backend
        # The fresh root is exactly checkpoint+tail authority.  Reset cadence to
        # that durable base before publishing the forced retry boundary below.
        self._initialize_checkpoint_cadence(view.anchor_sequence)
        with self._monitor_lock:
            self._monitor["recovery"] = recovery
            self._observe_recovery_pipeline_telemetry(recovery)
            event = self.store.append_event(
                "backend_rebuilt",
                {
                    "cycle": self._cycle,
                    "replayed_event_count": replayed_event_count,
                    "rebuild_mode": rebuild_mode,
                    "reason": "independent_lane_retry_after_admitted_tail",
                },
                occurred_at=self.now(),
            )
            self._checkpoint_event(event, force=True)

    def run(self, *, maximum_cycles: int | None = None) -> int:
        """Enter the foreground controller loop.

        ``maximum_cycles`` exists only for embedding and tests.  The CLI deliberately
        exposes no such knob: its ``run`` command continues until graceful stop or a
        fail-closed retry ceiling.
        """

        if maximum_cycles is not None and (
            isinstance(maximum_cycles, bool)
            or not isinstance(maximum_cycles, int)
            or maximum_cycles < 1
        ):
            raise ControllerError("maximum_cycles must be a positive integer")
        scheduler = self.config.section("scheduler")
        with self.store.run_lock():
            if self._desired_state() != "running":
                self._write_status("stopped")
                return 0
            self._started_at = self.now()
            self._write_status("starting")
            try:
                try:
                    restored, restore_mode, replayed_event_count = (
                        self._restore_backend()
                    )
                except StartupRestoreStopRequested:
                    # No controller_started event or backend checkpoint has been
                    # published, so the partially built in-process root has no
                    # durable authority.  Publish only the accepted lifecycle
                    # transition and return through the normal rc=0 CLI path.
                    self._write_status("stopping")
                    self.store.append_event(
                        "controller_stopped",
                        {
                            "cycle": self._cycle,
                            "reason": "stop_requested_during_startup_restore",
                        },
                        occurred_at=self.now(),
                    )
                    self._write_status("stopped")
                    return 0
                self._monitor["recovery"] = restored
                self._observe_recovery_pipeline_telemetry(restored)
                started_event = self.store.append_event(
                    "controller_started",
                    {
                        "pid": os.getpid(),
                        "restored_event_count": len(self.store.events),
                        "replayed_event_count": replayed_event_count,
                        "restore_mode": restore_mode,
                    },
                    occurred_at=self.now(),
                )
                # The backend is now exactly at the admitted startup boundary.
                # This also creates the first metadata checkpoint after a
                # no-checkpoint bootstrap, making every later restart tail-only.
                self._checkpoint_event(started_event)
                self._write_status("running")
                from .lane_coordinator import (
                    IndependentLaneCoordinator,
                    supports_independent_lanes,
                )

                independent = supports_independent_lanes(self.backend)
                if independent:
                    self._scheduler_mode = "independent_lanes"
                    self._write_status("running")
                while self._desired_state() == "running":
                    coordinator = None
                    try:
                        if independent:
                            coordinator = IndependentLaneCoordinator(
                                self.backend,
                                desired_state=self._desired_state,
                                on_outcome=self._admit_lane_outcome,
                                on_lane_state=self._set_lane_state,
                                terminal_disposition=self._terminal_disposition,
                                now=self.now,
                                idle_seconds=scheduler["idle_seconds"],
                                maximum_dispatches=maximum_cycles,
                                on_gpu_quiesce=self._admit_gpu_quiesce_outcome,
                                on_checkpoint=self._write_backend_checkpoint,
                                on_checkpoint_poll=lambda: self._write_backend_checkpoint(
                                    None
                                ),
                                checkpoint_deferred=(
                                    self._checkpoint_export_is_deferred
                                ),
                                on_forced_checkpoint=lambda anchor: self._write_backend_checkpoint(
                                    anchor, force=True
                                ),
                                on_lane_fault=self._set_lane_fault,
                                on_lane_recovered=self._record_lane_recovery,
                            )
                            self._quiesce_target = coordinator.gpu_backend
                            lane_result = coordinator.run()
                            keep_running = False
                            if lane_result.reason == "terminal":
                                if lane_result.terminal_disposition is None:
                                    raise ControllerError(
                                        "independent lanes reported terminal without a disposition"
                                    )
                                self._record_terminal_disposition(
                                    lane_result.terminal_disposition
                                )
                            elif lane_result.reason == "limit":
                                self._completion_reason = "test_cycle_limit"
                                self.store.set_desired_state(
                                    "stopped", requested_at=self.now()
                                )
                        else:
                            keep_running = self.run_cycle()
                    except Exception as error:
                        with self._monitor_lock:
                            fault_stage = (
                                coordinator.primary_fault_stage
                                if coordinator is not None
                                else None
                            )
                            if independent and fault_stage in STAGES:
                                self._retry_fault_stages.add(fault_stage)
                            elif independent and coordinator is None:
                                # Constructor failures can be proven recovered by
                                # the first complete lane boundary on the rebuilt
                                # coordinator.
                                self._retry_fault_stages.add(
                                    "controller_construction"
                                )
                            else:
                                # Post-construction/controller-level faults have no
                                # lane recovery boundary. A later wholly successful
                                # coordinator return plus post-run handling clears
                                # this sentinel through the normal success path.
                                self._retry_fault_stages.add("controller")
                            self._retry_epoch_failures += 1
                            self._consecutive_failures = (
                                self._retry_epoch_failures
                            )
                            self._last_error = _failure(error)
                            self._secondary_errors = []
                        if coordinator is not None:
                            for context, secondary in coordinator.secondary_failures:
                                self._record_secondary_error(context, secondary)
                        # Contain any exact child before journal or status I/O.
                        # The failure event remains controller-owned and is
                        # admitted only after every independent lane has joined.
                        quiesced = self._quiesce_after_failure(
                            context="gpu_quiesce_after_cycle_failure"
                        )
                        self.store.append_event(
                            "cycle_failed",
                            {
                                "cycle": self._cycle,
                                "scheduler_mode": self._scheduler_mode,
                                "consecutive_failures": self._retry_epoch_failures,
                                "error": self._last_error,
                            },
                            occurred_at=self.now(),
                        )
                        # The failing root may have mutated before it raised.  Its
                        # failure event is valid journal evidence for rebuild, but
                        # no cadence poll may snapshot that partial in-memory root.
                        self._discard_checkpoint_poll_anchor()
                        if (
                            not quiesced
                            or self._retry_epoch_failures
                            >= scheduler["max_consecutive_failures"]
                        ):
                            self.store.set_desired_state("stopped", requested_at=self.now())
                            self._write_status_after_failure(
                                "stopping",
                                context="stopping_status_publication",
                            )
                            self._write_status_after_failure(
                                "faulted",
                                context="faulted_status_publication",
                            )
                            return 2
                        if independent:
                            # A durable Stop accepted while a lane was failing
                            # must not be delayed by the expensive full-journal
                            # rebuild needed only for another retry.
                            if self._desired_state() != "running":
                                break
                            self._rebuild_independent_backend()
                        self._write_status_after_failure(
                            "retrying",
                            context="retrying_status_publication",
                        )
                        if not self._wait_interruptibly(
                            scheduler["failure_backoff_seconds"],
                            lifecycle="retrying",
                            preserve_primary=True,
                        ):
                            break
                        if independent:
                            # A fresh backend which survived the configured
                            # backoff is now the live authority.  The journaled
                            # failure remains, but it must no longer gate healthy
                            # work or keep ``accepting_new_work`` false.
                            self._clear_live_failure_state(
                                reset_retry_epoch=False
                            )
                            self._write_status("running")
                        continue
                    self._clear_live_failure_state()
                    if not keep_running:
                        break
                    if (
                        not independent
                        and maximum_cycles is not None
                        and self._cycle >= maximum_cycles
                    ):
                        self._completion_reason = "test_cycle_limit"
                        self.store.set_desired_state("stopped", requested_at=self.now())
                        break
                    if not self._wait_interruptibly(
                        scheduler["idle_seconds"], lifecycle="running"
                    ):
                        break
                if self._completion_reason == "campaign_drained":
                    event = self.store.append_event(
                        "controller_completed",
                        {"cycle": self._cycle, "reason": "campaign_drained"},
                        occurred_at=self.now(),
                    )
                    self._checkpoint_event(event, force=True)
                    self._write_status("completed")
                elif self._completion_reason in {
                    "primary_pass_drained_with_postprocess_backlog",
                    "primary_pass_drained_with_parked_items",
                }:
                    event = self.store.append_event(
                        "controller_blocked",
                        {
                            "cycle": self._cycle,
                            "reason": self._completion_reason,
                            "parked_requires_chunking_count": self._parked_item_count(),
                            "gpu_requires_chunking_count": self._gpu_chunking_count(),
                            "scheduled_quarantine_count": self._scheduled_quarantine_count(),
                            "preprocess_parked_count": self._preprocess_parked_count(),
                            "gpu_parked_item_count": self._gpu_parked_item_count(),
                        },
                        occurred_at=self.now(),
                    )
                    self._checkpoint_event(event, force=True)
                    self._write_status("blocked")
                else:
                    # Stop the exact child before a lifecycle status fsync can
                    # stall.  Status still records the ensuing drain boundary.
                    self._quiesce_backend()
                    self._write_status("stopping")
                    event = self.store.append_event(
                        "controller_stopped",
                        {"cycle": self._cycle, "reason": "graceful_stop_requested"},
                        occurred_at=self.now(),
                    )
                    self._checkpoint_event(event, force=True)
                    self._write_status("stopped")
                return 0
            except Exception as error:
                if self._last_error is None:
                    self._last_error = _failure(error)
                else:
                    self._record_secondary_error(
                        "controller_failure_cleanup", error
                    )
                # This attempt precedes control, event, and status publication.
                # Cleanup failures are diagnostic secondaries and cannot replace
                # the exception which entered this handler.
                self._quiesce_after_failure(
                    context="gpu_quiesce_after_controller_failure"
                )
                try:
                    self.store.set_desired_state("stopped", requested_at=self.now())
                except (StateError, OSError) as cleanup_error:
                    self._record_secondary_error(
                        "failed_control_publication", cleanup_error
                    )
                try:
                    self.store.append_event(
                        "controller_failed",
                        {"cycle": self._cycle, "error": self._last_error},
                        occurred_at=self.now(),
                    )
                    self._discard_checkpoint_poll_anchor()
                except (StateError, OSError) as cleanup_error:
                    self._record_secondary_error(
                        "controller_failed_event_publication", cleanup_error
                    )
                self._write_status_after_failure(
                    "faulted",
                    context="faulted_status_publication",
                )
                raise
