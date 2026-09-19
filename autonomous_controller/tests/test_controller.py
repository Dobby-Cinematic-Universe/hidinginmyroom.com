from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any, Sequence
from unittest import mock

from autonomous_controller.config import (
    CORE_KEYS,
    IMPLEMENTATION_VERSION,
    KIND,
    SAFETY,
    ControllerConfig,
    ConfigError,
    build_config,
    canonical_bytes,
    load_config,
    normalize_config,
    sha256_bytes,
)
from autonomous_controller.controller import (
    CHECKPOINT_MAX_STALENESS_SECONDS,
    CHECKPOINT_MAX_TAIL_EVENTS,
    LANE_STATUS_MAX_STALENESS_SECONDS,
    STAGES,
    TRANSIENT_PEER_RUNTIME_ARTIFACT,
    AutonomousController,
    ControllerError,
    StageOutcome,
)
from autonomous_controller.lane_coordinator import IndependentLaneCoordinator
from autonomous_controller.public_status import (
    PublicStatusError,
    _validated_pipeline_telemetry,
    read_public_status,
)
from autonomous_controller.state import (
    ControlStore,
    read_control_state,
    request_start,
    request_stop,
)


def config_core(root: Path, *, max_failures: int = 2) -> dict[str, Any]:
    def child(name: str) -> str:
        return str(root / name)

    inventory = {
        "kind": "known_collections_inventory",
        "path": child("broad-plan.json"),
        "sha256": "1" * 64,
    }
    schedules = [
        {
            "path": child("schedule-1.json"),
            "sha256": "2" * 64,
            "schedule_id": "bgacqsched_" + "3" * 32,
            "role": "normal_processing",
        },
        {
            "path": child("schedule-2.json"),
            "sha256": "4" * 64,
            "schedule_id": "bgacqsched_" + "5" * 32,
            "role": "normal_processing",
        },
    ]
    campaign_without_id = {
        "inventory": inventory,
        "schedule_set": {
            "kind": "sealed_archive_campaign_background_schedule_set",
            "path": child("schedule-set.json"),
            "sha256": "f" * 64,
            "schedule_set_id": "bgacqscheduleset_" + "e" * 32,
        },
        "schedules": schedules,
        "global_ready_high_items": 16,
        "global_ready_high_bytes": 32 * 1024**3,
    }
    campaign_id = (
        "himrarccampaign_"
        + sha256_bytes(canonical_bytes(campaign_without_id))[:32]
    )
    return {
        "kind": KIND,
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "campaign": {"campaign_id": campaign_id, **campaign_without_id},
        "state_root": child("state"),
        "acquisition": {
            "normal_processing": {
                "max_new_items": 1,
                "max_new_bytes": 1024**3,
                "max_run_seconds": 120,
                "free_space_floor_bytes": 512 * 1024**3,
            },
            "cold_acquisition_only_requires_chunking": {
                "max_new_items": 1,
                "max_new_bytes": 64 * 1024**3,
                "max_run_seconds": 120,
                "free_space_floor_bytes": 512 * 1024**3,
            },
        },
        "preprocess": {
            "bundle_root": child("preprocess-control"),
            "processing_output_root": child("preprocess-output"),
            "max_items": 1,
            "max_attempts_per_item": 3,
        },
        "gpu_readiness": {
            "enabled": True,
            "queue_root": child("gpu-queues"),
            "root_registration": child("hot-root.json"),
            "root_registration_sha256": "6" * 64,
            "runtime_admission": child("runtime.json"),
            "runtime_admission_sha256": "7" * 64,
            "production_profile": child("profile.json"),
            "production_profile_sha256": "8" * 64,
            "launcher_profile": child("launcher-profile.json"),
            "launcher_profile_sha256": "9" * 64,
            "local_readiness": child("local-readiness.json"),
            "local_readiness_sha256": "a" * 64,
            "local_launcher": child("trusted-launcher-v2"),
            "working_directory": str(root),
            "child_journal_root": child("state/gpu-children"),
            "work_order_root": child("gpu-work-orders"),
            "receipt_root": child("gpu-receipts"),
            "result_root": child("gpu-results"),
            "batch_root": child("gpu-batches"),
            "event_root": child("gpu-events"),
            "lock_root": child("gpu-locks"),
            "execution_mode": "local-private-production",
            "max_items_per_batch": 32,
            "max_batches_per_cycle": 2,
            "max_attempts_per_batch": 3,
            "max_active_children": 1,
        },
        "cold_retention": {
            "enabled": True,
            "destination_root": "/mnt/archive/HIMR",
            "staging_root": child("cold-stage"),
            "receipt_root": child("cold-receipts"),
            "free_space_floor_bytes": 100 * 1024**3,
            "max_items_per_cycle": 1,
        },
        "scheduler": {
            "idle_seconds": 0.1,
            "failure_backoff_seconds": 0.1,
            "max_consecutive_failures": max_failures,
        },
        "safety": dict(SAFETY),
    }


def make_config(root: Path, *, max_failures: int = 2) -> ControllerConfig:
    document = build_config(config_core(root, max_failures=max_failures))
    return ControllerConfig(
        document=document,
        path=root / "controller-config.json",
        physical_sha256=sha256_bytes(canonical_bytes(document)),
    )


class HeldBackend:
    def __init__(self) -> None:
        self.restored: Sequence[dict[str, Any]] | None = None
        self.calls: list[str] = []

    def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
        self.restored = events
        return {"restored": len(events)}

    def run_stage(self, stage: str) -> StageOutcome:
        self.calls.append(stage)
        return StageOutcome(stage, "held", False, {"observed": True}, {})


class BlockingBackend(HeldBackend):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def run_stage(self, stage: str) -> StageOutcome:
        self.calls.append(stage)
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise AssertionError("test did not release the finite stage")
        return StageOutcome(stage, "progressed", True, {"bounded": True}, {})


class CompleteBackend(HeldBackend):
    def __init__(self, *, parked: int = 0) -> None:
        super().__init__()
        self.parked = parked

    def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
        self.restored = events
        return {
            "campaign_coverage": {
                "parked_requires_chunking_count": self.parked,
            }
        }

    def run_stage(self, stage: str) -> StageOutcome:
        monitors = {
            "acquisition": {"pending": 0, "ready_items": 0, "ready_bytes": 0},
            "preprocess": {"ready_items_after": 0},
            "gpu_readiness": {"pending_batches": 0, "active_children": 0},
            "cold_retention": {"pending_items": 0, "replay_pending_items": 0},
        }
        return StageOutcome(stage, "complete", False, monitors[stage], {})


class ParallelStopBackend(HeldBackend):
    def __init__(self, store: ControlStore) -> None:
        super().__init__()
        self.store = store
        self.parallel_calls = 0

    def run_parallel_stages(self) -> tuple[StageOutcome, StageOutcome]:
        self.parallel_calls += 1
        self.store.set_desired_state("stopped")
        return (
            StageOutcome(
                "acquisition", "progressed", True, {"new_items": 1}, {}
            ),
            StageOutcome(
                "preprocess", "progressed", True, {"processed_items": 1}, {}
            ),
        )

    def run_stage(self, stage: str) -> StageOutcome:
        raise AssertionError(f"stop after finite overlap started forbidden stage {stage}")


class FailingBackend(HeldBackend):
    def __init__(self) -> None:
        super().__init__()
        self.quiesce_calls = 0

    def run_stage(self, stage: str) -> StageOutcome:
        self.calls.append(stage)
        raise RuntimeError("bounded stage failure")

    def quiesce(self) -> None:
        self.quiesce_calls += 1


class IndependentLaneBackend:
    def __init__(self, owner: "IndependentBackend", stage: str) -> None:
        self.owner = owner
        self.stage = stage
        self.calls = 0
        self.item_limits: list[int | None] = []
        self.peer_outcomes: list[str] = []
        self.quiesce_calls = 0

    def observe_peer_outcome(self, outcome: StageOutcome) -> None:
        self.peer_outcomes.append(outcome.stage)

    def run_stage(
        self, stage: str, *, item_limit: int | None = None
    ) -> StageOutcome:
        if stage != self.stage:
            raise AssertionError("lane received work for a different stage")
        self.calls += 1
        self.item_limits.append(item_limit)
        if stage == "acquisition":
            self.owner.acquisition_entered.set()
            if not self.owner.acquisition_release.wait(timeout=5):
                raise AssertionError("test did not release acquisition file boundary")
            self.owner.acquisition_finished.set()
            return StageOutcome(
                stage,
                "progressed",
                True,
                {
                    "new_items": 1,
                    "new_bytes": 100,
                    "active_schedule_id": "schedule-one",
                },
                {},
            )
        if stage == "preprocess":
            if self.calls <= 2:
                if self.calls == 2:
                    self.owner.preprocess_advanced_twice.set()
                return StageOutcome(
                    stage,
                    "progressed",
                    True,
                    {
                        "processed_items": 1,
                        "active_schedule_id": f"schedule-pre-{self.calls}",
                    },
                    {},
                )
            return StageOutcome(stage, "held", False, {"processed_items": 0}, {})
        if stage == "gpu_readiness":
            if self.calls <= 2:
                if self.calls == 2:
                    self.owner.gpu_advanced_twice.set()
                return StageOutcome(
                    stage,
                    "progressed",
                    True,
                    {
                        "active_children": 1,
                        "pending_batches": 1,
                        "ready_batches": 1,
                    },
                    {},
                )
            return StageOutcome(
                stage,
                "held",
                False,
                {"active_children": 0, "pending_batches": 1},
                {},
            )
        return StageOutcome(stage, "skipped", False, {"pending_items": 0}, {})

    def quiesce(self) -> StageOutcome:
        self.quiesce_calls += 1
        return StageOutcome(
            "gpu_readiness",
            "held",
            self.quiesce_calls == 1,
            {"active_children": 0, "pending_batches": 1},
            {},
        )


class IndependentBackend:
    def __init__(self) -> None:
        self.acquisition_entered = threading.Event()
        self.acquisition_release = threading.Event()
        self.acquisition_finished = threading.Event()
        self.preprocess_advanced_twice = threading.Event()
        self.gpu_advanced_twice = threading.Event()
        self.root_observations: list[str] = []
        self.lanes: dict[str, IndependentLaneBackend] = {}

    def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
        return {"restored": len(events)}

    def fork_lane(self, stage: str) -> IndependentLaneBackend:
        lane = IndependentLaneBackend(self, stage)
        self.lanes[stage] = lane
        return lane

    def observe_peer_outcome(self, outcome: StageOutcome) -> None:
        self.root_observations.append(outcome.stage)


class ControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        (self.root / "state").mkdir(mode=0o700)
        self.config = make_config(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_config_is_multi_schedule_and_rejects_authority_drift(self) -> None:
        self.assertEqual(2, len(self.config.document["campaign"]["schedules"]))
        forged = copy.deepcopy(self.config.document)
        forged["safety"]["publication_authority"] = "automatic"
        with self.assertRaisesRegex(ConfigError, "safety policy"):
            normalize_config(forged)

    def test_config_accepts_only_the_kind_specific_composite_schedule_set_id(self) -> None:
        def bind_campaign_id(core: dict[str, Any]) -> None:
            campaign_core = {
                key: value
                for key, value in core["campaign"].items()
                if key != "campaign_id"
            }
            core["campaign"]["campaign_id"] = (
                "himrarccampaign_"
                + sha256_bytes(canonical_bytes(campaign_core))[:32]
            )

        core = config_core(self.root)
        core["campaign"]["schedule_set"].update(
            {
                "kind": "sealed_archive_campaign_composite_schedule_set",
                "schedule_set_id": "bgacqcompositeset_" + "d" * 32,
            }
        )
        bind_campaign_id(core)
        document = build_config(core)
        self.assertEqual(
            "bgacqcompositeset_" + "d" * 32,
            document["campaign"]["schedule_set"]["schedule_set_id"],
        )

        forged = copy.deepcopy(core)
        forged["campaign"]["schedule_set"]["schedule_set_id"] = (
            "bgacqscheduleset_" + "d" * 32
        )
        bind_campaign_id(forged)
        with self.assertRaisesRegex(ConfigError, "schedule-set ID is invalid"):
            build_config(forged)

    def test_healthy_lane_status_churn_coalesces_to_one_bounded_aggregate(self) -> None:
        store = ControlStore(self.config)
        request_start(self.config)
        clock = [0.0]
        controller = AutonomousController(
            self.config,
            store,
            HeldBackend(),
            status_monotonic=lambda: clock[0],
        )
        controller._scheduler_mode = "independent_lanes"
        writes: list[dict[str, Any]] = []
        original_write = store.write_status

        def counting_write(value: dict[str, Any]) -> None:
            writes.append(copy.deepcopy(value))
            original_write(value)

        timers = []

        class FakeTimer:
            def __init__(self, delay, callback, args=()) -> None:
                self.delay = delay
                self.callback = callback
                self.args = args
                self.daemon = False
                self.cancelled = False
                self.started = False
                timers.append(self)

            def start(self) -> None:
                self.started = True

            def cancel(self) -> None:
                self.cancelled = True

            def fire(self) -> None:
                self.callback(*self.args)

        def lane_state(state: str, dispatch_id: int) -> dict[str, Any]:
            return {
                "state": state,
                "active": int(state == "running"),
                "limit": 1,
                "dispatch_id": dispatch_id,
                "started_at": "2026-08-30T00:00:00Z",
                "last_status": "held",
                "wait_reason": "stable-idle" if state == "waiting" else None,
                "last_transition_at": "2026-08-30T00:00:00Z",
            }

        with (
            mock.patch.object(store, "write_status", side_effect=counting_write),
            mock.patch(
                "autonomous_controller.controller.threading.Timer", FakeTimer
            ),
        ):
            controller._write_status("running")
            for dispatch_id in range(1, 65):
                for stage in STAGES:
                    controller._set_lane_state(
                        stage, lane_state("running", dispatch_id)
                    )
                    controller._set_lane_state(
                        stage, lane_state("waiting", dispatch_id)
                    )
            self.assertEqual(1, len(writes))
            self.assertEqual(1, len(timers))
            self.assertTrue(timers[0].started)
            self.assertEqual(
                LANE_STATUS_MAX_STALENESS_SECONDS, timers[0].delay
            )
            # Every suppressed transition is already reflected in memory. The
            # single deadline callback persists one aggregate of all four lanes.
            self.assertTrue(
                all(
                    value["state"] == "waiting"
                    for value in controller._lane_monitor.values()
                )
            )
            timers[0].fire()
            self.assertEqual(2, len(writes))
            self.assertTrue(
                all(
                    value["state"] == "waiting"
                    for value in writes[-1]["lanes"].values()
                )
            )
            self.assertFalse(controller._lane_status_dirty)
            self.assertIsNone(controller._lane_status_timer)

            stable_statuses = {
                "acquisition": "held",
                "preprocess": "complete",
                "gpu_readiness": "skipped",
                "cold_retention": "complete",
            }
            for dispatch_id, (stage, status) in enumerate(
                stable_statuses.items(), 100
            ):
                controller._admit_lane_outcome(
                    stage,
                    StageOutcome(stage, status, False, {}, {}),
                    dispatch_id,
                )
                controller._set_lane_state(
                    stage,
                    {
                        **lane_state("waiting", dispatch_id),
                        "last_status": status,
                    },
                )
            material_write_count = len(writes)

            # Stable complete/skipped lanes are idle too; they must not bypass
            # coalescing merely because of their status label. Across two public
            # freshness intervals, hundreds of polls produce exactly two writes.
            for interval in range(2):
                for poll in range(32):
                    for offset, (stage, status) in enumerate(
                        stable_statuses.items()
                    ):
                        dispatch_id = 200 + interval * 200 + poll * 4 + offset
                        controller._admit_lane_outcome(
                            stage,
                            StageOutcome(stage, status, False, {}, {}),
                            dispatch_id,
                        )
                        controller._set_lane_state(
                            stage,
                            {
                                **lane_state("waiting", dispatch_id),
                                "last_status": status,
                            },
                        )
                self.assertEqual(material_write_count + interval, len(writes))
                timers[-1].fire()
                self.assertEqual(material_write_count + interval + 1, len(writes))

    def test_material_fault_quiesce_and_stop_lane_statuses_flush_immediately(self) -> None:
        store = ControlStore(self.config)
        request_start(self.config)
        clock = [0.0]
        controller = AutonomousController(
            self.config,
            store,
            HeldBackend(),
            status_monotonic=lambda: clock[0],
        )
        controller._scheduler_mode = "independent_lanes"
        writes: list[dict[str, Any]] = []
        original_write = store.write_status

        def counting_write(value: dict[str, Any]) -> None:
            writes.append(copy.deepcopy(value))
            original_write(value)

        class FakeTimer:
            instances = []

            def __init__(self, delay, callback, args=()) -> None:
                self.delay = delay
                self.callback = callback
                self.args = args
                self.daemon = False
                self.cancelled = False
                self.__class__.instances.append(self)

            def start(self) -> None:
                return None

            def cancel(self) -> None:
                self.cancelled = True

        def lane_state(
            stage_state: str,
            *,
            dispatch_id: int,
            wait_reason: str | None = None,
        ) -> dict[str, Any]:
            return {
                "state": stage_state,
                "active": int(stage_state == "running"),
                "limit": 1,
                "dispatch_id": dispatch_id,
                "started_at": "2026-08-30T00:00:00Z",
                "last_status": "held",
                "wait_reason": wait_reason,
                "last_transition_at": "2026-08-30T00:00:00Z",
            }

        with (
            mock.patch.object(store, "write_status", side_effect=counting_write),
            mock.patch(
                "autonomous_controller.controller.threading.Timer", FakeTimer
            ),
        ):
            controller._write_status("running")
            controller._admit_lane_outcome(
                "preprocess",
                StageOutcome(
                    "preprocess",
                    "held",
                    False,
                    {"processed_items": 0, "ready_items_after": 4},
                    {},
                ),
                1,
            )
            controller._set_lane_state(
                "preprocess", lane_state("waiting", dispatch_id=1)
            )
            self.assertEqual(2, len(writes), "changed outcome must flush")

            # An identical held poll is healthy churn and only arms the deadline.
            controller._admit_lane_outcome(
                "preprocess",
                StageOutcome(
                    "preprocess",
                    "held",
                    False,
                    {"processed_items": 0, "ready_items_after": 4},
                    {},
                ),
                2,
            )
            controller._set_lane_state(
                "preprocess", lane_state("waiting", dispatch_id=2)
            )
            self.assertEqual(2, len(writes))
            pending_timer = FakeTimer.instances[-1]

            controller._set_lane_state(
                "gpu_readiness",
                lane_state(
                    "idle",
                    dispatch_id=3,
                    wait_reason="gpu_child_quiesced",
                ),
            )
            self.assertEqual(3, len(writes), "quiesce must flush")
            self.assertTrue(pending_timer.cancelled)

            controller._set_lane_fault(
                "gpu_readiness", RuntimeError("immediate lane fault")
            )
            self.assertEqual(4, len(writes), "fault must flush")
            self.assertEqual(
                "immediate lane fault", writes[-1]["last_error"]["message"]
            )

            store.set_desired_state("stopped")
            controller._set_lane_state(
                "acquisition", lane_state("running", dispatch_id=4)
            )
            self.assertEqual(5, len(writes), "Stop must flush")
            self.assertEqual("stopping", writes[-1]["actual_state"])

    def test_load_requires_canonical_sealed_bytes_and_external_digest(self) -> None:
        path = self.config.path
        body = canonical_bytes(self.config.document)
        path.write_bytes(body)
        path.chmod(0o400)
        loaded = load_config(path, sha256_bytes(body))
        self.assertEqual(self.config.config_id, loaded.config_id)
        with self.assertRaisesRegex(ConfigError, "external SHA-256"):
            load_config(path, "f" * 64)

    def test_run_requires_durable_start_intent_and_preserves_newer_stop(self) -> None:
        store = ControlStore(self.config)
        backend = HeldBackend()
        controller = AutonomousController(self.config, store, backend)
        self.assertEqual(0, controller.run())
        self.assertIsNone(backend.restored)
        self.assertEqual([], list(store.events))

        started = request_start(self.config, requested_at="2026-08-29T00:00:00Z")
        stopped = request_stop(self.config, requested_at="2026-08-29T00:00:01Z")
        self.assertGreater(stopped["generation"], started["generation"])
        self.assertEqual(0, controller.run())
        self.assertIsNone(backend.restored)
        self.assertEqual(stopped, read_control_state(self.config))

    def test_checkpoint_backend_bootstraps_without_legacy_restore(self) -> None:
        store = ControlStore(self.config)
        seed = store.append_event(
            "seed",
            {"value": 1},
            occurred_at="2026-08-29T00:00:00Z",
        )

        class Backend(HeldBackend):
            def __init__(self) -> None:
                super().__init__()
                self.checkpoint_restores: list[
                    tuple[dict[str, Any] | None, tuple[dict[str, Any], ...]]
                ] = []
                self.exports = 0

            def restore_checkpoint(self, backend, tail_events):
                self.checkpoint_restores.append(
                    (copy.deepcopy(backend), tuple(tail_events))
                )
                return {"restored": len(tail_events)}

            def export_checkpoint(self) -> dict[str, Any]:
                self.exports += 1
                return {"export": self.exports}

        backend = Backend()
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        self.assertEqual(0, controller.run(maximum_cycles=1))
        self.assertIsNone(backend.restored)
        self.assertEqual([(None, (seed,))], backend.checkpoint_restores)
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual("controller_stopped", checkpoint["anchor"]["event_type"])
        self.assertGreaterEqual(backend.exports, 2)
        started = next(
            event for event in store.events if event["event_type"] == "controller_started"
        )
        self.assertEqual(
            "checkpoint_metadata_bootstrap",
            started["payload"]["restore_mode"],
        )
        self.assertEqual(1, started["payload"]["replayed_event_count"])

    def test_checkpoint_restore_receives_only_validated_tail(self) -> None:
        store = ControlStore(self.config)
        anchor = store.append_event(
            "anchor",
            {"value": 1},
            occurred_at="2026-08-29T00:00:00Z",
        )
        checkpoint = store.write_checkpoint(
            {"generation": 7}, created_at="2026-08-29T00:00:01Z"
        )
        tail = store.append_event(
            "tail",
            {"value": 2},
            occurred_at="2026-08-29T00:00:02Z",
        )

        class Backend(HeldBackend):
            def __init__(self) -> None:
                super().__init__()
                self.checkpoint_restore = None

            def restore_checkpoint(self, backend, tail_events):
                self.checkpoint_restore = (
                    copy.deepcopy(backend),
                    tuple(tail_events),
                )
                return {"restored": len(tail_events)}

            def export_checkpoint(self) -> dict[str, Any]:
                return {"generation": 8}

        backend = Backend()
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        self.assertEqual(0, controller.run(maximum_cycles=1))
        self.assertEqual(
            ({"generation": 7}, (tail,)), backend.checkpoint_restore
        )
        self.assertIsNone(backend.restored)
        started = next(
            event for event in store.events if event["event_type"] == "controller_started"
        )
        self.assertEqual("checkpoint_tail", started["payload"]["restore_mode"])
        self.assertEqual(1, started["payload"]["replayed_event_count"])
        self.assertEqual(anchor["event_sha256"], checkpoint["anchor"]["event_sha256"])

    def test_stop_during_interruptible_startup_restore_is_clean_and_uncheckpointed(
        self,
    ) -> None:
        store = ControlStore(self.config)

        class Backend(HeldBackend):
            def __init__(self) -> None:
                super().__init__()
                self.entered = threading.Event()
                self.release = threading.Event()
                self.legacy_restore_called = False
                self.boundaries = 0

            def restore_checkpoint(self, _backend, _tail_events):
                self.legacy_restore_called = True
                raise AssertionError("interruptible restore was bypassed")

            def restore_checkpoint_interruptibly(
                self,
                _backend,
                _tail_events,
                *,
                cancellation_boundary,
            ):
                self.entered.set()
                if not self.release.wait(timeout=5):
                    raise AssertionError("startup restore test was not released")
                self.boundaries += 1
                cancellation_boundary()
                raise AssertionError("Stop boundary unexpectedly returned")

        backend = Backend()
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        result: list[int] = []
        worker = threading.Thread(target=lambda: result.append(controller.run()))
        worker.start()
        self.assertTrue(backend.entered.wait(timeout=5))
        stopped = store.set_desired_state("stopped")
        backend.release.set()
        worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual([0], result)
        self.assertEqual("stopped", stopped["desired_state"])
        self.assertEqual(stopped, store.read_control())
        self.assertEqual(1, backend.boundaries)
        self.assertFalse(backend.legacy_restore_called)
        self.assertEqual([], backend.calls)
        self.assertIsNone(store.read_checkpoint())
        self.assertEqual(
            ["controller_stopped"],
            [event["event_type"] for event in store.events],
        )
        self.assertEqual(
            "stop_requested_during_startup_restore",
            store.events[0]["payload"]["reason"],
        )
        status = store.read_status()
        assert status is not None
        self.assertEqual("stopped", status["actual_state"])
        self.assertEqual("stopped", status["desired_state"])
        self.assertIsNone(status["last_error"])

    def test_checkpoint_cadence_bounds_tail_without_exporting_every_event(self) -> None:
        store = ControlStore(self.config)
        store.append_event(
            "anchor",
            {"value": 0},
            occurred_at="2026-08-29T00:00:00Z",
        )
        store.write_checkpoint(
            {"generation": 0}, created_at="2026-08-29T00:00:01Z"
        )
        clock = [0.0]

        class Backend(HeldBackend):
            def __init__(self) -> None:
                super().__init__()
                self.exports = 0

            def restore_checkpoint(self, _backend, tail_events):
                return {"restored": len(tail_events)}

            def export_checkpoint(self) -> dict[str, Any]:
                self.exports += 1
                return {"generation": self.exports}

        backend = Backend()
        controller = AutonomousController(
            self.config,
            store,
            backend,
            checkpoint_monotonic=lambda: clock[0],
        )
        controller._restore_backend()
        latest = None
        for ordinal in range(1, CHECKPOINT_MAX_TAIL_EVENTS):
            latest = store.append_event(
                "material",
                {"ordinal": ordinal},
                occurred_at="2026-08-29T00:00:02Z",
            )
            self.assertFalse(controller._checkpoint_event(latest))
        self.assertEqual(0, backend.exports)
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual(1, checkpoint["anchor"]["sequence"])

        latest = store.append_event(
            "material",
            {"ordinal": CHECKPOINT_MAX_TAIL_EVENTS},
            occurred_at="2026-08-29T00:00:02Z",
        )
        self.assertTrue(controller._checkpoint_event(latest))
        self.assertEqual(1, backend.exports)
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual(latest["sequence"], checkpoint["anchor"]["sequence"])

    def test_checkpoint_cadence_elapsed_poll_and_forced_boundary(self) -> None:
        store = ControlStore(self.config)
        store.append_event(
            "anchor",
            {"value": 0},
            occurred_at="2026-08-29T00:00:00Z",
        )
        store.write_checkpoint(
            {"generation": 0}, created_at="2026-08-29T00:00:01Z"
        )
        clock = [0.0]

        class Backend(HeldBackend):
            def __init__(self) -> None:
                super().__init__()
                self.exports = 0

            def restore_checkpoint(self, _backend, tail_events):
                return {"restored": len(tail_events)}

            def export_checkpoint(self) -> dict[str, Any]:
                self.exports += 1
                return {"generation": self.exports}

        backend = Backend()
        controller = AutonomousController(
            self.config,
            store,
            backend,
            checkpoint_monotonic=lambda: clock[0],
        )
        controller._restore_backend()
        elapsed = store.append_event(
            "material",
            {"value": 1},
            occurred_at="2026-08-29T00:00:02Z",
        )
        self.assertFalse(controller._checkpoint_event(elapsed))
        clock[0] = CHECKPOINT_MAX_STALENESS_SECONDS
        self.assertTrue(controller._write_backend_checkpoint(None))
        self.assertEqual(1, backend.exports)

        forced = store.append_event(
            "clean_boundary",
            {"value": 2},
            occurred_at="2026-08-29T00:00:03Z",
        )
        self.assertTrue(controller._checkpoint_event(forced, force=True))
        self.assertEqual(2, backend.exports)
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual(forced["sequence"], checkpoint["anchor"]["sequence"])

    def test_checkpoint_export_deferral_preserves_pending_anchor(self) -> None:
        store = ControlStore(self.config)
        original = store.append_event(
            "anchor", {"value": 0}, occurred_at="2026-08-29T00:00:00Z"
        )
        store.write_checkpoint(
            {"generation": 0}, created_at="2026-08-29T00:00:01Z"
        )

        class Backend(HeldBackend):
            def __init__(self) -> None:
                super().__init__()
                self.exports = [None, {"generation": 1}]

            def restore_checkpoint(self, _backend, tail_events):
                return {"restored": len(tail_events)}

            def export_checkpoint(self):
                return self.exports.pop(0)

        backend = Backend()
        controller = AutonomousController(self.config, store, backend)
        controller._restore_backend()
        event = store.append_event(
            "material", {"value": 1}, occurred_at="2026-08-29T00:00:02Z"
        )
        self.assertFalse(controller._checkpoint_event(event, force=True))
        self.assertTrue(controller._checkpoint_export_is_deferred())
        self.assertEqual((event["sequence"], event["event_sha256"]), controller._checkpoint_pending_anchor)
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual(original["sequence"], checkpoint["anchor"]["sequence"])
        # A due export remains due after deferral even when the drain path uses
        # the ordinary non-forced poll surface.
        self.assertTrue(controller._write_backend_checkpoint(None))
        self.assertFalse(controller._checkpoint_export_is_deferred())
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual(event["sequence"], checkpoint["anchor"]["sequence"])

    def test_legacy_backend_ignores_valid_checkpoint_and_replays_full_journal(self) -> None:
        store = ControlStore(self.config)
        event = store.append_event(
            "anchor",
            {"value": 1},
            occurred_at="2026-08-29T00:00:00Z",
        )
        store.write_checkpoint(
            {"generation": 7}, created_at="2026-08-29T00:00:01Z"
        )
        backend = HeldBackend()
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        self.assertEqual(0, controller.run(maximum_cycles=1))
        self.assertEqual((event,), tuple(backend.restored or ()))
        started = next(
            item for item in store.events if item["event_type"] == "controller_started"
        )
        self.assertEqual("legacy_full_journal", started["payload"]["restore_mode"])

    def test_graceful_stop_waits_for_current_finite_stage_and_starts_no_next_stage(self) -> None:
        store = ControlStore(self.config)
        backend = BlockingBackend()
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        result: list[int] = []
        worker = threading.Thread(target=lambda: result.append(controller.run()))
        worker.start()
        self.assertTrue(backend.entered.wait(timeout=5))
        control = store.set_desired_state("stopped")
        self.assertEqual("stopped", control["desired_state"])
        self.assertTrue(worker.is_alive(), "stop must not cancel an in-flight stage")
        backend.release.set()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertEqual([0], result)
        self.assertEqual(["acquisition"], backend.calls)

    def test_stop_after_parallel_pair_journals_deterministically_and_starts_no_gpu(self) -> None:
        store = ControlStore(self.config)
        backend = ParallelStopBackend(store)
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        self.assertEqual(0, controller.run())
        self.assertEqual(1, backend.parallel_calls)
        stages = [
            event["payload"]["outcome"]["stage"]
            for event in store.events
            if event["event_type"] == "stage_finished"
        ]
        self.assertEqual(["acquisition", "preprocess"], stages)
        status = store.read_status()
        assert status is not None
        self.assertEqual("stopped", status["actual_state"])
        self.assertEqual("stopped", status["desired_state"])
        self.assertEqual(0, status["storage"]["hot_deletions"])

    def test_independent_lanes_advance_while_acquisition_is_blocked_and_stop_at_item_boundary(self) -> None:
        store = ControlStore(self.config)
        backend = IndependentBackend()
        controller = AutonomousController(self.config, store, backend)
        request_start(self.config)
        observations: dict[str, bool] = {}

        def stop_after_parallel_progress() -> None:
            self.assertTrue(backend.acquisition_entered.wait(timeout=5))
            self.assertTrue(backend.preprocess_advanced_twice.wait(timeout=5))
            self.assertTrue(backend.gpu_advanced_twice.wait(timeout=5))
            observations["acquisition_still_running"] = not backend.acquisition_finished.is_set()
            store.set_desired_state("stopped")
            backend.acquisition_release.set()

        stopper = threading.Thread(target=stop_after_parallel_progress)
        stopper.start()
        self.assertEqual(0, controller.run())
        stopper.join(timeout=5)
        self.assertFalse(stopper.is_alive())
        self.assertTrue(observations["acquisition_still_running"])
        self.assertEqual(1, backend.lanes["acquisition"].calls)
        self.assertGreaterEqual(backend.lanes["preprocess"].calls, 2)
        self.assertGreaterEqual(backend.lanes["gpu_readiness"].calls, 2)
        self.assertEqual([None], backend.lanes["acquisition"].item_limits)
        self.assertTrue(
            all(value is None for value in backend.lanes["preprocess"].item_limits)
        )
        self.assertTrue(
            all(value is None for value in backend.lanes["gpu_readiness"].item_limits)
        )
        self.assertEqual(
            2,
            backend.lanes["gpu_readiness"].quiesce_calls,
            [
                (event["event_type"], event["payload"])
                for event in store.events
                if event["event_type"] == "cycle_failed"
            ],
        )
        self.assertEqual(
            1,
            sum(
                event["event_type"] == "gpu_child_quiesced"
                for event in store.events
            ),
        )
        self.assertIn("preprocess", backend.root_observations)
        self.assertIn("gpu_readiness", backend.root_observations)
        status = store.read_status()
        assert status is not None
        self.assertEqual("independent_lanes", status["scheduler_mode"])
        self.assertGreaterEqual(status["dispatch_sequence"], 5)
        self.assertEqual(0, status["execution"]["inflight_total"])
        self.assertEqual(set(STAGES), set(status["lanes"]))
        self.assertEqual("stopped", status["actual_state"])

    def test_transient_peer_runtime_is_not_written_to_durable_events(self) -> None:
        store = ControlStore(self.config)
        controller = AutonomousController(self.config, store, HeldBackend())
        outcome = StageOutcome(
            "acquisition",
            "progressed",
            True,
            {"new_items": 1},
            {
                "backend_kind": "test",
                TRANSIENT_PEER_RUNTIME_ARTIFACT: {
                    "kind": "acquisition_queue_summary_v1",
                    "queue_summary": {"large": [1, 2, 3]},
                },
            },
        )
        self.assertTrue(controller._admit_outcome(outcome, "acquisition"))
        event = store.events[-1]
        durable = event["payload"]["outcome"]["artifacts"]
        self.assertEqual({"backend_kind": "test"}, durable)
        self.assertIn(TRANSIENT_PEER_RUNTIME_ARTIFACT, outcome.artifacts)

    def test_terminal_requires_a_fresh_downstream_fixed_point(self) -> None:
        class Lane:
            def __init__(self) -> None:
                self.observed: list[str] = []

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                self.observed.append(outcome.stage)

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

        backend = Backend()
        coordinator = IndependentLaneCoordinator(
            backend,  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=lambda *_args: None,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: "campaign_drained",
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.1,
        )

        def finish(stage: str, *, progressed: bool = False) -> None:
            binding = coordinator._begin(stage)
            assert binding is not None
            dispatch, started, generation = binding
            coordinator._complete(
                stage,
                StageOutcome(stage, "complete", progressed, {}, {}),
                dispatch,
                started,
                generation,
            )

        # These completions all predate the final acquisition generation.
        finish("gpu_readiness")
        finish("cold_retention")
        stale_preprocess = coordinator._begin("preprocess")
        assert stale_preprocess is not None
        finish("acquisition", progressed=True)
        dispatch, started, generation = stale_preprocess
        coordinator._complete(
            "preprocess",
            StageOutcome("preprocess", "complete", False, {}, {}),
            dispatch,
            started,
            generation,
        )
        self.assertIsNone(coordinator._reason)

        # Each dependency lane must observe acquisition and complete again.
        for stage in ("preprocess", "gpu_readiness", "cold_retention"):
            coordinator._apply_peer_outcomes(stage)
            finish(stage)
            self.assertIn("acquisition", backend.lanes[stage].observed)
        self.assertEqual("terminal", coordinator._reason)
        self.assertEqual("campaign_drained", coordinator._terminal)

    def test_lane_checkpoint_is_serialized_after_journal_and_root_observation(self) -> None:
        ordering: list[str] = []

        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                ordering.append(f"root:{outcome.stage}")

        def admit(stage: str, _outcome: StageOutcome, _dispatch: int):
            ordering.append(f"journal:{stage}")
            return (1, "a" * 64)

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=admit,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.1,
            on_checkpoint=lambda _anchor: ordering.append("checkpoint"),
            on_checkpoint_poll=lambda: ordering.append("checkpoint_poll"),
        )
        binding = coordinator._begin("preprocess")
        assert binding is not None
        dispatch, started, generation = binding
        coordinator._complete(
            "preprocess",
            StageOutcome("preprocess", "progressed", True, {}, {}),
            dispatch,
            started,
            generation,
        )
        self.assertEqual(
            ["journal:preprocess", "root:preprocess", "checkpoint"], ordering
        )

        # An unjournaled stable poll still updates the root aggregate but does
        # not rewrite the same checkpoint anchor.
        ordering.clear()
        coordinator.on_outcome = lambda stage, _outcome, _dispatch: (
            ordering.append(f"journal:{stage}") or None
        )
        binding = coordinator._begin("preprocess")
        assert binding is not None
        dispatch, started, generation = binding
        coordinator._complete(
            "preprocess",
            StageOutcome("preprocess", "held", False, {}, {}),
            dispatch,
            started,
            generation,
        )
        self.assertEqual(
            ["journal:preprocess", "checkpoint_poll", "root:preprocess"],
            ordering,
        )

    def test_deferred_checkpoint_drains_inflight_outcomes_then_resumes_dispatch(
        self,
    ) -> None:
        ordering: list[str] = []
        shared_generation = {"value": 2}
        root_generation = {"value": 0}
        deferred = {"value": False}

        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                root_generation["value"] += 1
                ordering.append(f"root:{outcome.stage}")

        def admit(
            stage: str, outcome: StageOutcome, dispatch_id: int
        ) -> tuple[int, str] | None:
            ordering.append(f"admit:{stage}")
            if not outcome.progressed:
                return None
            return dispatch_id, f"{dispatch_id:064x}"

        def checkpoint(label: str) -> bool:
            if root_generation["value"] != shared_generation["value"]:
                deferred["value"] = True
                ordering.append(f"{label}:deferred")
                return False
            deferred["value"] = False
            ordering.append(f"{label}:written")
            return True

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=admit,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
            on_checkpoint=lambda _anchor: checkpoint("event"),
            on_checkpoint_poll=lambda: checkpoint("poll"),
            checkpoint_deferred=lambda: deferred["value"],
        )
        acquisition = coordinator._begin("acquisition")
        preprocess = coordinator._begin("preprocess")
        assert acquisition is not None and preprocess is not None

        coordinator._complete(
            "acquisition",
            StageOutcome("acquisition", "progressed", True, {}, {}),
            *acquisition,
        )
        self.assertTrue(coordinator._checkpoint_drain_requested)

        entered = threading.Event()
        admitted: list[tuple[int, str, int] | None] = []

        def begin_gpu() -> None:
            entered.set()
            admitted.append(coordinator._begin("gpu_readiness"))
            ordering.append("begin:gpu_readiness")

        waiter = threading.Thread(target=begin_gpu)
        waiter.start()
        self.assertTrue(entered.wait(timeout=1))
        waiter.join(timeout=0.05)
        self.assertTrue(waiter.is_alive(), "deferred export admitted new work")

        # This already-admitted peer carries the shared generation which made
        # the first export defer.  Its pre-root poll still defers; after root
        # observation the drained retry publishes the pending anchor.
        coordinator._complete(
            "preprocess",
            StageOutcome("preprocess", "held", False, {}, {}),
            *preprocess,
        )
        waiter.join(timeout=1)
        self.assertFalse(waiter.is_alive())
        self.assertIsNotNone(admitted[0])
        self.assertFalse(coordinator._checkpoint_drain_requested)
        self.assertEqual(
            [
                "admit:acquisition",
                "root:acquisition",
                "event:deferred",
                "admit:preprocess",
                "poll:deferred",
                "root:preprocess",
                "poll:written",
                "begin:gpu_readiness",
            ],
            ordering,
        )

    def test_checkpoint_drain_wait_escapes_durable_stop_and_lane_fault(self) -> None:
        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

        for boundary in ("stop", "fault"):
            with self.subTest(boundary=boundary):
                desired = {"value": "running"}
                deferred = {"value": False}
                coordinator = IndependentLaneCoordinator(
                    Backend(),  # type: ignore[arg-type]
                    desired_state=lambda: desired["value"],
                    on_outcome=lambda *_args: None,
                    on_lane_state=lambda *_args: None,
                    terminal_disposition=lambda: None,
                    now=lambda: "2026-08-29T00:00:00Z",
                    idle_seconds=0.01,
                    checkpoint_deferred=lambda: deferred["value"],
                )
                self.assertIsNotNone(coordinator._begin("acquisition"))
                with coordinator._condition:
                    deferred["value"] = True
                    coordinator._checkpoint_drain_requested = True

                entered = threading.Event()
                results: list[tuple[int, str, int] | None] = []

                def blocked_begin() -> None:
                    entered.set()
                    results.append(coordinator._begin("preprocess"))

                waiter = threading.Thread(target=blocked_begin)
                waiter.start()
                self.assertTrue(entered.wait(timeout=1))
                waiter.join(timeout=0.05)
                self.assertTrue(waiter.is_alive())
                if boundary == "stop":
                    # No condition notification: the bounded wait itself must
                    # observe the externally written durable control state.
                    desired["value"] = "stopped"
                else:
                    with coordinator._condition:
                        coordinator._fault = RuntimeError("peer failed")
                        coordinator._condition.notify_all()
                waiter.join(timeout=1)
                self.assertFalse(waiter.is_alive())
                self.assertEqual([None], results)

    def test_checkpoint_repeated_deferral_at_drained_boundary_fails_closed(
        self,
    ) -> None:
        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

        polls: list[str] = []
        deferred = {"value": False}

        def defer(_anchor: tuple[int, str]) -> bool:
            deferred["value"] = True
            return False

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=lambda *_args: (1, "a" * 64),
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
            on_checkpoint=defer,
            on_checkpoint_poll=lambda: polls.append("poll") or False,
            checkpoint_deferred=lambda: deferred["value"],
        )
        binding = coordinator._begin("acquisition")
        assert binding is not None
        with self.assertRaisesRegex(
            ControllerError,
            "checkpoint export remained deferred at a drained lane boundary",
        ):
            coordinator._complete(
                "acquisition",
                StageOutcome("acquisition", "progressed", True, {}, {}),
                *binding,
            )
        self.assertEqual(["poll"], polls)
        self.assertTrue(coordinator._root_checkpoint_poisoned)
        self.assertFalse(coordinator._checkpoint_drain_exporting)

    def test_failed_drain_poisons_root_before_gpu_quiesce_can_observe(self) -> None:
        observations: list[str] = []

        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                observations.append(f"root:{outcome.stage}")

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=lambda *_args: None,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
            on_gpu_quiesce=lambda _outcome: (
                observations.append("admit:gpu_quiesce")
                or (2, "b" * 64)
            ),
            on_checkpoint_poll=lambda: observations.append("poll") or False,
            checkpoint_deferred=lambda: True,
            on_forced_checkpoint=lambda _anchor: observations.append(
                "checkpoint:gpu_quiesce"
            ),
        )

        main_thread = threading.get_ident()
        root_released = threading.Event()
        observer_done = threading.Event()

        class RootReleaseGate:
            def __init__(self) -> None:
                self.lock = threading.Lock()

            def __enter__(self):
                self.lock.acquire()
                return self

            def __exit__(self, _error_type, _error, _traceback) -> None:
                self.lock.release()
                if threading.get_ident() == main_thread:
                    root_released.set()
                    if not observer_done.wait(timeout=1):
                        raise RuntimeError("GPU observer did not reach the root fence")

        coordinator._root_observer_lock = RootReleaseGate()  # type: ignore[assignment]
        quiesce = StageOutcome("gpu_readiness", "held", False, {}, {})

        def observe_quiesce() -> None:
            try:
                if root_released.wait(timeout=1):
                    coordinator._observe_gpu_quiesce(quiesce)
            finally:
                observer_done.set()

        observer = threading.Thread(target=observe_quiesce)
        observer.start()
        with self.assertRaisesRegex(
            ControllerError,
            "checkpoint export remained deferred at a drained lane boundary",
        ):
            coordinator._retry_checkpoint_after_drain()
        observer.join(timeout=1)
        self.assertFalse(observer.is_alive())
        self.assertTrue(coordinator._root_checkpoint_poisoned)
        self.assertEqual(["poll", "admit:gpu_quiesce"], observations)

    def test_lane_fault_and_active_clear_are_one_checkpoint_drain_boundary(
        self,
    ) -> None:
        stage_entered = threading.Event()
        release_stage = threading.Event()
        fault_boundary_entered = threading.Event()
        release_fault_boundary = threading.Event()
        deferred = {"value": False}
        polls: list[str] = []

        class Lane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):
                if stage != "acquisition":  # pragma: no cover - never dispatched
                    raise AssertionError(stage)
                stage_entered.set()
                if not release_stage.wait(timeout=1):  # pragma: no cover
                    raise AssertionError("fixture stage was not released")
                raise RuntimeError("fixture lane failure")

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane(stage) for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=lambda *_args: None,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
            on_checkpoint_poll=lambda: polls.append("poll") or True,
            checkpoint_deferred=lambda: deferred["value"],
        )
        original_record_fault = coordinator._record_fault

        def delayed_record_fault(*args, **kwargs):
            fault_boundary_entered.set()
            if not release_fault_boundary.wait(timeout=1):  # pragma: no cover
                raise AssertionError("fixture fault boundary was not released")
            return original_record_fault(*args, **kwargs)

        coordinator._record_fault = delayed_record_fault  # type: ignore[method-assign]
        lane = threading.Thread(target=coordinator._lane_loop, args=("acquisition",))
        lane.start()
        self.assertTrue(stage_entered.wait(timeout=1))
        with coordinator._condition:
            deferred["value"] = True
            coordinator._checkpoint_drain_requested = True
        release_stage.set()
        self.assertTrue(fault_boundary_entered.wait(timeout=1))
        with coordinator._condition:
            self.assertTrue(coordinator._active["acquisition"])
            self.assertIsNone(coordinator._fault)

        admitted: list[tuple[int, str, int] | None] = []
        waiter = threading.Thread(
            target=lambda: admitted.append(coordinator._begin("preprocess"))
        )
        waiter.start()
        waiter.join(timeout=0.05)
        self.assertTrue(waiter.is_alive())
        self.assertEqual([], polls)

        release_fault_boundary.set()
        lane.join(timeout=1)
        waiter.join(timeout=1)
        self.assertFalse(lane.is_alive())
        self.assertFalse(waiter.is_alive())
        self.assertEqual([None], admitted)
        self.assertEqual([], polls)
        with coordinator._condition:
            self.assertFalse(coordinator._active["acquisition"])
            self.assertIsInstance(coordinator._fault, RuntimeError)

    def test_material_gpu_quiesce_uses_the_same_checkpoint_order(self) -> None:
        ordering: list[str] = []

        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                ordering.append(f"root:{outcome.stage}")

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=lambda *_args: False,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.1,
            on_gpu_quiesce=lambda _outcome: ordering.append("journal:quiesce")
            or (2, "b" * 64),
            on_checkpoint=lambda _anchor: ordering.append("checkpoint"),
            on_forced_checkpoint=lambda _anchor: ordering.append(
                "forced_checkpoint"
            ),
        )
        coordinator._observe_gpu_quiesce(
            StageOutcome("gpu_readiness", "progressed", True, {}, {})
        )
        self.assertEqual(
            [
                "journal:quiesce",
                "root:gpu_readiness",
                "forced_checkpoint",
            ],
            ordering,
        )

    def test_failed_root_boundary_poison_fences_later_checkpoints(self) -> None:
        ordering: list[str] = []

        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane() for stage in STAGES}
                self.observations = 0

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                self.observations += 1
                ordering.append(f"root:{outcome.stage}")
                if self.observations == 1:
                    raise RuntimeError("partial root mutation")

        backend = Backend()

        def admit(stage: str, _outcome: StageOutcome, dispatch: int):
            ordering.append(f"journal:{stage}")
            return (dispatch, f"{dispatch:064x}")

        coordinator = IndependentLaneCoordinator(
            backend,  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=admit,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.1,
            on_checkpoint=lambda _anchor: ordering.append("checkpoint"),
        )

        first = coordinator._begin("preprocess")
        assert first is not None
        with self.assertRaisesRegex(RuntimeError, "partial root mutation"):
            coordinator._complete(
                "preprocess",
                StageOutcome("preprocess", "progressed", True, {}, {}),
                *first,
            )

        second = coordinator._begin("cold_retention")
        assert second is not None
        coordinator._complete(
            "cold_retention",
            StageOutcome("cold_retention", "progressed", True, {}, {}),
            *second,
        )
        self.assertEqual(
            [
                "journal:preprocess",
                "root:preprocess",
                "journal:cold_retention",
            ],
            ordering,
        )

    def test_gpu_quiesces_before_a_blocked_acquisition_lane_joins(self) -> None:
        desired = {"state": "running"}
        acquisition_entered = threading.Event()
        acquisition_release = threading.Event()
        gpu_active = threading.Event()
        gpu_quiesced = threading.Event()

        class Lane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "acquisition":
                    acquisition_entered.set()
                    if not acquisition_release.wait(timeout=5):
                        raise AssertionError("acquisition boundary was not released")
                    return StageOutcome(stage, "held", False, {}, {})
                if stage == "gpu_readiness":
                    gpu_active.set()
                    return StageOutcome(
                        stage,
                        "held",
                        False,
                        {"active_children": 1, "pending_batches": 1},
                        {},
                    )
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                gpu_quiesced.set()
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"active_children": 0, "pending_batches": 1},
                    {},
                )

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane(stage) for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: desired["state"],
            on_outcome=lambda *_args: None,
            on_lane_state=lambda *_args: None,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
        )
        result = []
        runner = threading.Thread(target=lambda: result.append(coordinator.run()))
        runner.start()
        self.assertTrue(acquisition_entered.wait(timeout=2))
        self.assertTrue(gpu_active.wait(timeout=2))
        desired["state"] = "stopped"
        self.assertTrue(
            gpu_quiesced.wait(timeout=1),
            "GPU must quiesce while acquisition remains at its file boundary",
        )
        self.assertTrue(runner.is_alive())
        acquisition_release.set()
        runner.join(timeout=3)
        self.assertFalse(runner.is_alive())
        self.assertEqual("desired_stop", result[0].reason)

    def test_peer_fault_quiesces_returned_gpu_work_before_outcome_observers(self) -> None:
        acquisition_entered = threading.Event()
        acquisition_release = threading.Event()
        gpu_entered = threading.Event()
        fault_admitted = threading.Event()
        gpu_quiesced = threading.Event()
        gpu_returned = threading.Event()
        ordering: list[str] = []
        ordering_lock = threading.Lock()
        public_gpu_monitor: dict[str, int] = {}
        durable_events: list[str] = []

        def mark(value: str) -> None:
            with ordering_lock:
                ordering.append(value)

        class Lane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "acquisition":
                    acquisition_entered.set()
                    if not acquisition_release.wait(timeout=5):
                        raise AssertionError("test did not release acquisition")
                    return StageOutcome(stage, "held", False, {}, {})
                if stage == "preprocess":
                    if not acquisition_entered.wait(timeout=2):
                        raise AssertionError("acquisition did not enter")
                    if not gpu_entered.wait(timeout=2):
                        raise AssertionError("GPU lane did not enter")
                    mark("peer_fault")
                    raise RuntimeError("peer lane failed during GPU dispatch")
                if stage == "gpu_readiness":
                    if not acquisition_entered.wait(timeout=2):
                        raise AssertionError("acquisition did not enter")
                    gpu_entered.set()
                    if not fault_admitted.wait(timeout=2):
                        raise AssertionError("peer fault was not admitted")
                    mark("gpu_return")
                    gpu_returned.set()
                    return StageOutcome(
                        stage,
                        "progressed",
                        True,
                        {"active_children": 1, "pending_batches": 1},
                        {"claim": "returned-gpu-authority"},
                    )
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                mark("quiesce")
                gpu_quiesced.set()
                return StageOutcome(
                    "gpu_readiness",
                    "progressed",
                    True,
                    {"active_children": 0, "pending_batches": 1},
                    {},
                )

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane(stage) for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                if outcome.stage == "gpu_readiness":
                    mark("gpu_root_observer")

        def observe_outcome(
            stage: str, outcome: StageOutcome, _dispatch_id: int
        ) -> None:
            if stage == "gpu_readiness":
                public_gpu_monitor["active_children"] = outcome.monitor[
                    "active_children"
                ]
                durable_events.append("stage_finished")
                mark("gpu_outcome_observer")

        def publish_lane(stage: str, value: dict[str, Any]) -> None:
            if (
                stage == "gpu_readiness"
                and gpu_returned.is_set()
                and value.get("state") in {"idle", "waiting", "faulted"}
            ):
                mark("gpu_lane_status")

        def publish_fault(stage: str, _error: BaseException) -> None:
            if stage == "preprocess":
                mark("fault_status")
                fault_admitted.set()

        def observe_quiesce(outcome: StageOutcome) -> None:
            public_gpu_monitor["active_children"] = outcome.monitor[
                "active_children"
            ]
            durable_events.append("gpu_child_quiesced")
            mark("quiesce_observer")
            raise OSError("quiesce observer failed after admission")

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=observe_outcome,
            on_lane_state=publish_lane,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
            on_gpu_quiesce=observe_quiesce,
            on_lane_fault=publish_fault,
        )
        errors: list[BaseException] = []

        def run_coordinator() -> None:
            try:
                coordinator.run()
            except BaseException as error:
                errors.append(error)

        runner = threading.Thread(target=run_coordinator)
        runner.start()
        try:
            self.assertTrue(fault_admitted.wait(timeout=2))
            self.assertTrue(gpu_quiesced.wait(timeout=2))
        finally:
            acquisition_release.set()
            runner.join(timeout=5)
        self.assertFalse(runner.is_alive())
        self.assertEqual(1, len(errors))
        self.assertEqual("peer lane failed during GPU dispatch", str(errors[0]))

        with ordering_lock:
            observed = list(ordering)
        self.assertEqual(1, observed.count("quiesce"))
        for later in (
            "quiesce_observer",
            "gpu_outcome_observer",
            "gpu_root_observer",
            "gpu_lane_status",
        ):
            self.assertLess(observed.index("quiesce"), observed.index(later))
        self.assertLess(observed.index("peer_fault"), observed.index("gpu_return"))
        self.assertLess(observed.index("gpu_return"), observed.index("quiesce"))
        self.assertLess(
            observed.index("gpu_outcome_observer"),
            observed.index("quiesce_observer"),
        )
        self.assertLess(
            observed.index("gpu_root_observer"),
            observed.index("quiesce_observer"),
        )
        self.assertEqual(1, observed.count("gpu_outcome_observer"))
        self.assertEqual(1, observed.count("gpu_root_observer"))
        self.assertEqual(1, observed.count("quiesce_observer"))
        self.assertEqual(
            ["stage_finished", "gpu_child_quiesced"], durable_events
        )
        self.assertEqual(0, public_gpu_monitor["active_children"])
        self.assertIn(
            (
                "gpu_readiness_quiesce",
                "OSError",
                "quiesce observer failed after admission",
            ),
            {
                (context, type(error).__name__, str(error))
                for context, error in coordinator.secondary_failures
            },
        )

    def test_gpu_fault_quiesces_before_failing_status_and_preserves_primary(self) -> None:
        acquisition_entered = threading.Event()
        acquisition_release = threading.Event()
        quiesce_attempted = threading.Event()
        ordering: list[str] = []

        class Lane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "acquisition":
                    acquisition_entered.set()
                    if not acquisition_release.wait(timeout=5):
                        raise AssertionError("test did not release acquisition")
                    return StageOutcome(stage, "held", False, {}, {})
                if stage == "gpu_readiness":
                    if not acquisition_entered.wait(timeout=2):
                        raise AssertionError("acquisition did not enter first")
                    raise RuntimeError("primary GPU lane failure")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> None:
                ordering.append("quiesce")
                quiesce_attempted.set()
                raise OSError("secondary exact quiesce failure")

        class Backend:
            def __init__(self) -> None:
                self.lanes = {stage: Lane(stage) for stage in STAGES}

            def fork_lane(self, stage: str) -> Lane:
                return self.lanes[stage]

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

        def publish_lane(stage: str, value: dict[str, Any]) -> None:
            if stage == "gpu_readiness" and value.get("state") == "faulted":
                ordering.append("lane_status")
                raise OSError("secondary lane status failure")

        def publish_fault(_stage: str, _error: BaseException) -> None:
            ordering.append("fault_status")
            raise OSError("secondary fault status failure")

        coordinator = IndependentLaneCoordinator(
            Backend(),  # type: ignore[arg-type]
            desired_state=lambda: "running",
            on_outcome=lambda *_args: None,
            on_lane_state=publish_lane,
            terminal_disposition=lambda: None,
            now=lambda: "2026-08-29T00:00:00Z",
            idle_seconds=0.01,
            on_lane_fault=publish_fault,
        )
        errors: list[BaseException] = []

        def run_coordinator() -> None:
            try:
                coordinator.run()
            except BaseException as error:
                errors.append(error)

        runner = threading.Thread(target=run_coordinator)
        runner.start()
        self.assertTrue(quiesce_attempted.wait(timeout=2))
        self.assertEqual("quiesce", ordering[0])
        self.assertIn("fault_status", ordering)
        self.assertIn("lane_status", ordering)
        self.assertTrue(runner.is_alive(), "acquisition must still be at its boundary")
        acquisition_release.set()
        runner.join(timeout=3)
        self.assertFalse(runner.is_alive())
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual("primary GPU lane failure", str(errors[0]))
        secondary = {
            (context, type(error).__name__, str(error))
            for context, error in coordinator.secondary_failures
        }
        self.assertIn(
            (
                "gpu_readiness_quiesce",
                "OSError",
                "secondary exact quiesce failure",
            ),
            secondary,
        )
        self.assertIn(
            (
                "gpu_readiness_fault_status",
                "OSError",
                "secondary fault status failure",
            ),
            secondary,
        )
        self.assertIn(
            (
                "gpu_readiness_lane_status",
                "OSError",
                "secondary lane status failure",
            ),
            secondary,
        )

    def test_gpu_fault_remains_visible_while_acquisition_is_blocked(self) -> None:
        store = ControlStore(self.config)
        acquisition_entered = threading.Event()
        acquisition_release = threading.Event()
        quiesce_returned = threading.Event()
        settled_fault_status = threading.Event()

        class FaultLane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "acquisition":
                    acquisition_entered.set()
                    if not acquisition_release.wait(timeout=5):
                        raise AssertionError("test did not release acquisition")
                    return StageOutcome(stage, "held", False, {}, {})
                if stage == "gpu_readiness":
                    if not acquisition_entered.wait(timeout=2):
                        raise AssertionError("acquisition did not enter first")
                    raise RuntimeError("original GPU lane failure")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                quiesce_returned.set()
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 2, "active_children": 0},
                    {},
                )

        class FaultBackend:
            def restore(self, _events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {}

            def fork_lane(self, stage: str) -> FaultLane:
                return FaultLane(stage)

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def rebuild_from_events(self, _events):  # pragma: no cover
                raise AssertionError("accepted stop must bypass retry rebuild")

        class ObservedController(AutonomousController):
            def _set_lane_state(self, stage: str, value: dict[str, Any]) -> None:
                super()._set_lane_state(stage, value)
                if (
                    stage == "gpu_readiness"
                    and value.get("state") == "faulted"
                    and value.get("active") == 0
                    and quiesce_returned.is_set()
                ):
                    settled_fault_status.set()

        controller = ObservedController(
            self.config,
            store,
            FaultBackend(),  # type: ignore[arg-type]
        )
        request_start(self.config)
        result: list[int] = []
        errors: list[BaseException] = []

        def run_controller() -> None:
            try:
                result.append(controller.run())
            except BaseException as error:  # pragma: no cover - diagnostic guard
                errors.append(error)

        runner = threading.Thread(target=run_controller)
        runner.start()
        try:
            self.assertTrue(acquisition_entered.wait(timeout=2))
            self.assertTrue(settled_fault_status.wait(timeout=2))
            self.assertTrue(
                runner.is_alive(),
                "the acquisition boundary must still be blocking controller admission",
            )
            status = store.read_status()
            assert status is not None
            self.assertEqual(
                {
                    "type": "RuntimeError",
                    "message": "original GPU lane failure",
                },
                status["last_error"],
            )
            self.assertEqual("faulted", status["lanes"]["gpu_readiness"]["state"])
            self.assertEqual(0, status["lanes"]["gpu_readiness"]["active"])
            self.assertFalse(status["execution"]["accepting_new_work"])
            self.assertEqual(
                "original GPU lane failure",
                status["lanes"]["gpu_readiness"]["wait_reason"],
            )
            self.assertFalse(
                any(event["event_type"] == "cycle_failed" for event in store.events),
                (
                    "a worker must not reorder the durable failure ahead of an "
                    "in-flight lane"
                ),
            )
        finally:
            store.set_desired_state("stopped")
            acquisition_release.set()
            runner.join(timeout=5)
        self.assertFalse(runner.is_alive())
        self.assertEqual([], errors)
        self.assertEqual([0], result)
        self.assertEqual(
            1,
            sum(event["event_type"] == "cycle_failed" for event in store.events),
        )

    def test_independent_retry_rebuilds_fresh_root_from_admitted_tail(self) -> None:
        store = ControlStore(self.config)
        gpu_observed = threading.Event()
        shared: dict[str, Any] = {
            "gpu_emitted": False,
            "preprocess_emitted": False,
            "observer_failed": False,
            "rebuild_count": 0,
            "retry_ran": False,
        }

        class RetryLane:
            def __init__(self, root: "RetryBackend", stage: str) -> None:
                self.root = root
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if self.root.generation == 0:
                    if stage == "gpu_readiness" and not shared["gpu_emitted"]:
                        shared["gpu_emitted"] = True
                        return StageOutcome(
                            stage,
                            "progressed",
                            True,
                            {"pending_batches": 1, "active_children": 0},
                            {"claim": "gpu-claim"},
                        )
                    if stage == "preprocess" and not shared["preprocess_emitted"]:
                        if not gpu_observed.wait(timeout=2):
                            raise AssertionError("GPU claim was not observed first")
                        shared["preprocess_emitted"] = True
                        return StageOutcome(
                            stage,
                            "progressed",
                            True,
                            {"processed_items": 1},
                            {"claim": "preprocess-claim"},
                        )
                    return StageOutcome(stage, "held", False, {}, {})
                if stage == "acquisition" and not shared["retry_ran"]:
                    shared["retry_ran"] = True
                    store.set_desired_state("stopped")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 1, "active_children": 0},
                    {},
                )

        class RetryBackend:
            def __init__(self, generation: int = 0) -> None:
                self.generation = generation
                self.claims: list[str] = []

            def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                restored: list[str] = []
                for event in events:
                    if event["event_type"] != "stage_finished":
                        continue
                    claim = event["payload"]["outcome"]["artifacts"].get("claim")
                    if claim is None:
                        continue
                    if claim in restored:
                        raise AssertionError("durable authority was replayed twice")
                    restored.append(claim)
                self.claims = restored
                if self.generation:
                    shared["rebuilt_claims"] = list(restored)
                return {"restored_claims": len(restored)}

            def fork_lane(self, stage: str) -> RetryLane:
                return RetryLane(self, stage)

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                claim = outcome.artifacts.get("claim")
                if claim is None:
                    return
                if claim in self.claims:
                    raise AssertionError("live authority was applied twice")
                self.claims.append(claim)
                if claim == "gpu-claim":
                    gpu_observed.set()
                if (
                    claim == "preprocess-claim"
                    and self.generation == 0
                    and not shared["observer_failed"]
                ):
                    shared["observer_failed"] = True
                    raise RuntimeError("root observation failed after event admission")

            def rebuild_from_events(
                self, events: Sequence[dict[str, Any]]
            ) -> tuple["RetryBackend", dict[str, Any]]:
                shared["rebuild_count"] += 1
                rebuilt = RetryBackend(self.generation + 1)
                recovery = rebuilt.restore(events)
                shared["rebuilt_root"] = rebuilt
                return rebuilt, recovery

        backend = RetryBackend()
        controller = AutonomousController(self.config, store, backend)  # type: ignore[arg-type]
        request_start(self.config)
        self.assertEqual(0, controller.run())
        self.assertTrue(shared["observer_failed"])
        self.assertTrue(shared["retry_ran"])
        self.assertEqual(1, shared["rebuild_count"])
        self.assertEqual(
            ["gpu-claim", "preprocess-claim"], shared["rebuilt_claims"]
        )
        durable_claims = [
            event["payload"]["outcome"]["artifacts"]["claim"]
            for event in store.events
            if event["event_type"] == "stage_finished"
            and "claim" in event["payload"]["outcome"]["artifacts"]
        ]
        self.assertEqual(["gpu-claim", "preprocess-claim"], durable_claims)
        self.assertEqual(
            1,
            sum(event["event_type"] == "backend_rebuilt" for event in store.events),
        )

    def test_independent_retry_prefers_checkpoint_and_tail(self) -> None:
        store = ControlStore(self.config)
        store.append_event(
            "anchor",
            {"value": 1},
            occurred_at="2026-08-29T00:00:00Z",
        )
        store.write_checkpoint(
            {"generation": 4}, created_at="2026-08-29T00:00:01Z"
        )
        tail = store.append_event(
            "tail",
            {"value": 2},
            occurred_at="2026-08-29T00:00:02Z",
        )
        calls: list[tuple[dict[str, Any] | None, tuple[dict[str, Any], ...]]] = []

        class Lane:
            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None):  # pragma: no cover
                raise AssertionError(stage)

        class FreshBackend:
            def fork_lane(self, _stage: str) -> Lane:
                return Lane()

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def restore_checkpoint(self, _backend, _tail):  # pragma: no cover
                raise AssertionError("fresh backend is already restored")

            def export_checkpoint(self) -> dict[str, Any]:
                return {"generation": 5}

        fresh = FreshBackend()

        class Backend(FreshBackend):
            def rebuild_from_checkpoint(self, backend, tail_events):
                calls.append((copy.deepcopy(backend), tuple(tail_events)))
                return fresh, {"restored": len(tail_events)}

            def rebuild_from_events(self, _events):  # pragma: no cover
                raise AssertionError("legacy retry replay must not run")

        controller = AutonomousController(self.config, store, Backend())  # type: ignore[arg-type]
        controller._rebuild_independent_backend()
        self.assertEqual([({"generation": 4}, (tail,))], calls)
        self.assertIs(fresh, controller.backend)
        checkpoint = store.read_checkpoint()
        assert checkpoint is not None
        self.assertEqual({"generation": 5}, checkpoint["backend"])
        self.assertEqual("backend_rebuilt", checkpoint["anchor"]["event_type"])
        rebuilt = store.events[-1]
        self.assertEqual("checkpoint_tail", rebuilt["payload"]["rebuild_mode"])
        self.assertEqual(1, rebuilt["payload"]["replayed_event_count"])

    def test_rebuilt_backend_clears_live_failure_before_healthy_redispatch(self) -> None:
        store = ControlStore(self.config)
        healthy_dispatch_entered = threading.Event()
        healthy_dispatch_release = threading.Event()

        class RetryLane:
            def __init__(self, root: "RetryBackend", stage: str) -> None:
                self.root = root
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if self.root.generation == 0 and stage == "acquisition":
                    raise RuntimeError("first generation failed")
                if self.root.generation == 1 and stage == "acquisition":
                    healthy_dispatch_entered.set()
                    if not healthy_dispatch_release.wait(timeout=5):
                        raise AssertionError("test did not release healthy dispatch")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 0, "active_children": 0},
                    {},
                )

        class RetryBackend:
            def __init__(self, generation: int = 0) -> None:
                self.generation = generation

            def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {"restored": len(events), "generation": self.generation}

            def fork_lane(self, stage: str) -> RetryLane:
                return RetryLane(self, stage)

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def rebuild_from_events(
                self, events: Sequence[dict[str, Any]]
            ) -> tuple["RetryBackend", dict[str, Any]]:
                rebuilt = RetryBackend(self.generation + 1)
                return rebuilt, rebuilt.restore(events)

        controller = AutonomousController(
            self.config,
            store,
            RetryBackend(),  # type: ignore[arg-type]
        )
        request_start(self.config)
        results: list[int] = []
        errors: list[BaseException] = []

        def run_controller() -> None:
            try:
                results.append(controller.run())
            except BaseException as error:  # pragma: no cover - diagnostic guard
                errors.append(error)

        runner = threading.Thread(target=run_controller)
        runner.start()
        try:
            self.assertTrue(healthy_dispatch_entered.wait(timeout=5))
            status = store.read_status()
            assert status is not None
            self.assertEqual("running", status["actual_state"])
            self.assertEqual(0, status["consecutive_failures"])
            self.assertIsNone(status["last_error"])
            self.assertEqual([], status["errors"])
            self.assertTrue(status["execution"]["accepting_new_work"])
            self.assertTrue(
                any(
                    event["event_type"] == "cycle_failed"
                    and event["payload"]["error"]["message"]
                    == "first generation failed"
                    for event in store.events
                ),
                "live retry recovery must not erase durable failure history",
            )
        finally:
            store.set_desired_state("stopped")
            healthy_dispatch_release.set()
            runner.join(timeout=5)
        self.assertFalse(runner.is_alive())
        self.assertEqual([], errors)
        self.assertEqual([0], results)

    def test_same_lane_repeat_fault_reaches_ceiling_despite_peer_progress(self) -> None:
        config = make_config(self.root, max_failures=3)
        store = ControlStore(config)
        shared = {"gpu_attempts": 0, "rebuilds": 0}

        class RepeatLane:
            def __init__(self, root: "RepeatBackend", stage: str) -> None:
                self.root = root
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "acquisition":
                    self.root.acquisition_completed.set()
                    return StageOutcome(
                        stage,
                        "progressed",
                        True,
                        {"new_items": 1},
                        {"generation": self.root.generation},
                    )
                if stage == "gpu_readiness":
                    if not self.root.acquisition_completed.wait(timeout=5):
                        raise AssertionError("peer acquisition did not complete")
                    shared["gpu_attempts"] += 1
                    raise RuntimeError("deterministic GPU lane fault")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 0, "active_children": 0},
                    {},
                )

        class RepeatBackend:
            def __init__(self, generation: int = 0) -> None:
                self.generation = generation
                self.acquisition_completed = threading.Event()

            def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {"restored": len(events), "generation": self.generation}

            def fork_lane(self, stage: str) -> RepeatLane:
                return RepeatLane(self, stage)

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def rebuild_from_events(
                self, events: Sequence[dict[str, Any]]
            ) -> tuple["RepeatBackend", dict[str, Any]]:
                shared["rebuilds"] += 1
                rebuilt = RepeatBackend(self.generation + 1)
                return rebuilt, rebuilt.restore(events)

        controller = AutonomousController(
            config,
            store,
            RepeatBackend(),  # type: ignore[arg-type]
        )
        request_start(config)
        self.assertEqual(2, controller.run())
        self.assertEqual(3, shared["gpu_attempts"])
        self.assertEqual(2, shared["rebuilds"])
        failed = [
            event
            for event in store.events
            if event["event_type"] == "cycle_failed"
        ]
        self.assertEqual(
            [1, 2, 3],
            [event["payload"]["consecutive_failures"] for event in failed],
        )
        status = store.read_status()
        assert status is not None
        self.assertEqual("faulted", status["actual_state"])
        self.assertEqual(3, status["consecutive_failures"])

    def test_fault_epoch_resets_only_after_same_lane_success_boundary(self) -> None:
        config = make_config(self.root, max_failures=2)
        store = ControlStore(config)
        shared = {"gpu_attempts": 0, "rebuilds": 0}

        class RecoverLane:
            def __init__(self, root: "RecoverBackend", stage: str) -> None:
                self.root = root
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "gpu_readiness":
                    shared["gpu_attempts"] += 1
                    if self.root.generation == 0:
                        raise RuntimeError("one GPU fault")
                    self.root.gpu_returned.set()
                    return StageOutcome(stage, "held", False, {}, {})
                if stage == "acquisition" and self.root.generation == 1:
                    if not self.root.gpu_recovered.wait(timeout=5):
                        raise AssertionError("GPU recovery callback did not finish")
                    raise RuntimeError("new acquisition fault after GPU recovery")
                if stage == "acquisition" and self.root.generation >= 2:
                    store.set_desired_state("stopped")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 0, "active_children": 0},
                    {},
                )

        class RecoverBackend:
            def __init__(self, generation: int = 0) -> None:
                self.generation = generation
                self.gpu_returned = threading.Event()
                self.gpu_recovered = threading.Event()

            def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {"restored": len(events), "generation": self.generation}

            def fork_lane(self, stage: str) -> RecoverLane:
                return RecoverLane(self, stage)

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def rebuild_from_events(
                self, events: Sequence[dict[str, Any]]
            ) -> tuple["RecoverBackend", dict[str, Any]]:
                shared["rebuilds"] += 1
                rebuilt = RecoverBackend(self.generation + 1)
                return rebuilt, rebuilt.restore(events)

        class RecoveryAwareController(AutonomousController):
            def _record_lane_recovery(self, stage: str) -> None:
                super()._record_lane_recovery(stage)
                backend = self.backend
                if (
                    stage == "gpu_readiness"
                    and isinstance(backend, RecoverBackend)
                    and backend.gpu_returned.is_set()
                ):
                    backend.gpu_recovered.set()

        controller = RecoveryAwareController(
            config,
            store,
            RecoverBackend(),  # type: ignore[arg-type]
        )
        request_start(config)
        self.assertEqual(0, controller.run())
        self.assertEqual(2, shared["rebuilds"])
        failed = [
            event
            for event in store.events
            if event["event_type"] == "cycle_failed"
        ]
        self.assertEqual(2, len(failed))
        self.assertEqual(
            [1, 1],
            [event["payload"]["consecutive_failures"] for event in failed],
        )

    def test_controller_fault_epoch_resets_after_rebuilt_lane_success(self) -> None:
        config = make_config(self.root, max_failures=2)
        store = ControlStore(config)
        rebuilt_boundary = threading.Event()
        shared = {"rebuilds": 0}

        class RecoveryLane:
            def __init__(self, root: "RecoveryBackend", stage: str) -> None:
                self.root = root
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if self.root.generation == 1 and stage == "acquisition":
                    if not rebuilt_boundary.wait(timeout=5):
                        raise AssertionError(
                            "rebuilt coordinator did not cross a healthy lane boundary"
                        )
                    raise RuntimeError("lane fault after controller recovery")
                if self.root.generation >= 2 and stage == "acquisition":
                    store.set_desired_state("stopped")
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 0, "active_children": 0},
                    {},
                )

        class RecoveryBackend:
            def __init__(self, generation: int = 0) -> None:
                self.generation = generation

            def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {"restored": len(events), "generation": self.generation}

            def fork_lane(self, stage: str) -> RecoveryLane:
                if self.generation == 0:
                    # Coordinator construction fails before it can expose a
                    # primary lane, exercising the controller sentinel.
                    raise RuntimeError("controller-level lane construction fault")
                return RecoveryLane(self, stage)

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def rebuild_from_events(
                self, events: Sequence[dict[str, Any]]
            ) -> tuple["RecoveryBackend", dict[str, Any]]:
                shared["rebuilds"] += 1
                rebuilt = RecoveryBackend(self.generation + 1)
                return rebuilt, rebuilt.restore(events)

        class BoundaryAwareController(AutonomousController):
            def _record_lane_recovery(self, stage: str) -> None:
                super()._record_lane_recovery(stage)
                backend = self.backend
                if isinstance(backend, RecoveryBackend) and backend.generation == 1:
                    rebuilt_boundary.set()

        controller = BoundaryAwareController(
            config,
            store,
            RecoveryBackend(),  # type: ignore[arg-type]
        )
        request_start(config)
        self.assertEqual(0, controller.run())
        self.assertEqual(2, shared["rebuilds"])
        failed = [
            event
            for event in store.events
            if event["event_type"] == "cycle_failed"
        ]
        self.assertEqual(
            [1, 1],
            [event["payload"]["consecutive_failures"] for event in failed],
        )

    def test_post_coordinator_fault_reaches_retry_ceiling_despite_lane_success(self) -> None:
        config = make_config(self.root, max_failures=2)
        store = ControlStore(config)
        shared = {"rebuilds": 0, "terminal_attempts": 0}

        terminal_monitors = {
            "acquisition": {"pending": 0, "quarantined_items": 0},
            "preprocess": {"ready_items_after": 0, "parked_items": 0},
            "gpu_readiness": {
                "pending_batches": 0,
                "active_children": 0,
                "requires_chunking_items_cumulative": 0,
                "parked_items": 0,
            },
            "cold_retention": {"pending_items": 0, "replay_pending_items": 0},
        }

        class TerminalLane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                return StageOutcome(
                    stage,
                    "complete",
                    False,
                    terminal_monitors[stage],
                    {},
                )

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 0, "active_children": 0},
                    {},
                )

        class TerminalBackend:
            def restore(self, events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {"restored": len(events)}

            def fork_lane(self, stage: str) -> TerminalLane:
                return TerminalLane(stage)

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def rebuild_from_events(
                self, events: Sequence[dict[str, Any]]
            ) -> tuple["TerminalBackend", dict[str, Any]]:
                shared["rebuilds"] += 1
                rebuilt = TerminalBackend()
                return rebuilt, rebuilt.restore(events)

        class FailingTerminalController(AutonomousController):
            def _record_terminal_disposition(self, terminal: str) -> None:
                shared["terminal_attempts"] += 1
                raise RuntimeError(
                    "deterministic post-coordinator terminal publication failure"
                )

        controller = FailingTerminalController(
            config,
            store,
            TerminalBackend(),  # type: ignore[arg-type]
        )
        request_start(config)
        self.assertEqual(2, controller.run())
        self.assertEqual(2, shared["terminal_attempts"])
        self.assertEqual(1, shared["rebuilds"])
        failed = [
            event
            for event in store.events
            if event["event_type"] == "cycle_failed"
        ]
        self.assertEqual(
            [1, 2],
            [event["payload"]["consecutive_failures"] for event in failed],
        )
        self.assertTrue(
            all(
                event["payload"]["error"]["message"]
                == "deterministic post-coordinator terminal publication failure"
                for event in failed
            )
        )

    def test_stop_during_lane_failure_skips_expensive_retry_rebuild(self) -> None:
        store = ControlStore(self.config)
        observed = {"failed": False, "rebuilds": 0}

        class StopFailureLane:
            def __init__(self, stage: str) -> None:
                self.stage = stage

            def observe_peer_outcome(self, _outcome: StageOutcome) -> None:
                return None

            def run_stage(self, stage: str, *, item_limit=None) -> StageOutcome:
                if stage == "acquisition" and not observed["failed"]:
                    return StageOutcome(
                        stage,
                        "progressed",
                        True,
                        {"new_items": 1},
                        {"claim": "admitted-before-stop"},
                    )
                return StageOutcome(stage, "held", False, {}, {})

            def quiesce(self) -> StageOutcome:
                return StageOutcome(
                    "gpu_readiness",
                    "held",
                    False,
                    {"pending_batches": 0, "active_children": 0},
                    {},
                )

        class StopFailureBackend:
            def restore(self, _events: Sequence[dict[str, Any]]) -> dict[str, Any]:
                return {}

            def fork_lane(self, stage: str) -> StopFailureLane:
                return StopFailureLane(stage)

            def observe_peer_outcome(self, outcome: StageOutcome) -> None:
                if outcome.stage == "acquisition" and outcome.progressed:
                    observed["failed"] = True
                    store.set_desired_state("stopped")
                    raise RuntimeError("stop won after durable outcome admission")

            def rebuild_from_events(self, _events):
                observed["rebuilds"] += 1
                raise AssertionError("stop must bypass retry-only deep rebuild")

        controller = AutonomousController(
            self.config,
            store,
            StopFailureBackend(),  # type: ignore[arg-type]
        )
        request_start(self.config)
        self.assertEqual(0, controller.run())
        self.assertTrue(observed["failed"])
        self.assertEqual(0, observed["rebuilds"])
        status = store.read_status()
        assert status is not None
        self.assertEqual("stopped", status["actual_state"])

    def test_event_admission_is_thread_safe_for_independent_lane_completions(self) -> None:
        store = ControlStore(self.config)
        barrier = threading.Barrier(4)

        def append(worker: int) -> None:
            barrier.wait(timeout=5)
            for ordinal in range(3):
                store.append_event(
                    "concurrent_test",
                    {"worker": worker, "ordinal": ordinal},
                    occurred_at="2026-08-29T00:00:00Z",
                )

        workers = [threading.Thread(target=append, args=(index,)) for index in range(4)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(12, len(store.events))
        self.assertEqual(
            list(range(1, 13)), [event["sequence"] for event in store.events]
        )
        self.assertEqual(
            [None, *[event["event_sha256"] for event in store.events[:-1]]],
            [event["previous_event_sha256"] for event in store.events],
        )

    def test_retry_ceiling_quiesces_backend_before_faulted_exit(self) -> None:
        config = make_config(self.root, max_failures=1)
        store = ControlStore(config)
        backend = FailingBackend()
        controller = AutonomousController(config, store, backend)
        request_start(config)

        self.assertEqual(2, controller.run())
        self.assertEqual(["acquisition"], backend.calls)
        self.assertEqual(1, backend.quiesce_calls)
        self.assertEqual("stopped", read_control_state(config)["desired_state"])
        status = store.read_status()
        assert status is not None
        self.assertEqual("faulted", status["actual_state"])

    def test_retry_ceiling_reports_cleanup_failures_without_masking_primary(self) -> None:
        config = make_config(self.root, max_failures=1)
        store = ControlStore(config)
        ordering: list[str] = []

        class CleanupFailingBackend(FailingBackend):
            def quiesce(self) -> None:
                self.quiesce_calls += 1
                ordering.append("quiesce")
                raise OSError("exact quiesce failed")

        class StatusFailingController(AutonomousController):
            def _write_status(
                self, lifecycle: str, *, current_stage: str | None = None
            ) -> None:
                if lifecycle == "stopping":
                    ordering.append("stopping_status")
                    raise OSError("stopping status fsync failed")
                super()._write_status(lifecycle, current_stage=current_stage)

        backend = CleanupFailingBackend()
        controller = StatusFailingController(config, store, backend)
        request_start(config)

        self.assertEqual(2, controller.run())
        self.assertEqual(["quiesce", "stopping_status"], ordering)
        self.assertEqual(1, backend.quiesce_calls)
        status = store.read_status()
        assert status is not None
        self.assertEqual(
            {"type": "RuntimeError", "message": "bounded stage failure"},
            status["last_error"],
        )
        self.assertEqual(status["last_error"], status["errors"][0])
        self.assertTrue(
            any(
                error["type"] == "OSError"
                and "gpu_quiesce_after_cycle_failure: exact quiesce failed"
                in error["message"]
                for error in status["errors"][1:]
            )
        )
        self.assertTrue(
            any(
                error["type"] == "OSError"
                and "stopping_status_publication: stopping status fsync failed"
                in error["message"]
                for error in status["errors"][1:]
            )
        )
        failed = [
            event
            for event in store.events
            if event["event_type"] == "cycle_failed"
        ]
        self.assertEqual(1, len(failed))
        self.assertEqual(status["last_error"], failed[0]["payload"]["error"])

    def test_restart_replays_contiguous_journal_and_continues_sequence(self) -> None:
        first_store = ControlStore(self.config)
        first_backend = HeldBackend()
        first = AutonomousController(self.config, first_store, first_backend)
        request_start(self.config)
        self.assertEqual(0, first.run(maximum_cycles=1))
        first_count = len(first_store.events)
        self.assertEqual(2, first_count)

        second_store = ControlStore(self.config)
        second_backend = HeldBackend()
        second = AutonomousController(self.config, second_store, second_backend)
        request_start(self.config)
        self.assertEqual(0, second.run(maximum_cycles=1))
        assert second_backend.restored is not None
        self.assertEqual(first_count, len(second_backend.restored))
        self.assertGreater(len(second_store.events), first_count)
        self.assertEqual(
            list(range(1, len(second_store.events) + 1)),
            [event["sequence"] for event in second_store.events],
        )

    def test_status_exposes_basic_monitor_contract(self) -> None:
        store = ControlStore(self.config)
        controller = AutonomousController(self.config, store, HeldBackend())
        request_start(self.config)
        self.assertEqual(0, controller.run(maximum_cycles=1))
        status = store.read_status()
        assert status is not None
        for key in (
            "desired_state",
            "actual_state",
            "stages",
            "progress",
            "pipeline_telemetry",
            "throughput",
            "storage",
            "errors",
            "recent_activity",
            "current_gpu_child",
        ):
            self.assertIn(key, status)
        self.assertEqual(set(STAGES), set(status["stages"]))
        self.assertLessEqual(len(status["recent_activity"]), 16)

    def test_pipeline_telemetry_is_cumulative_and_uses_latest_runnable_backlog(self) -> None:
        store = ControlStore(self.config)
        controller = AutonomousController(self.config, store, HeldBackend())
        controller._admit_outcome(
            StageOutcome(
                "acquisition",
                "progressed",
                True,
                {
                    "new_items": 3,
                    "ready_items": 9,
                    "raw_normal_ready_items": 12,
                },
                {},
            ),
            "acquisition",
        )
        controller._admit_outcome(
            StageOutcome(
                "preprocess",
                "progressed",
                True,
                {
                    "processed_items": 2,
                    "preprocessed_items_cumulative": 2,
                    "ready_items_after": 7,
                    "raw_ready_items": 10,
                },
                {},
            ),
            "preprocess",
        )
        controller._admit_outcome(
            StageOutcome(
                "gpu_readiness",
                "progressed",
                True,
                {"completed_batches": 3, "completed_items": 5},
                {},
            ),
            "gpu_readiness",
        )

        telemetry = controller._pipeline_telemetry()
        self.assertEqual(7, telemetry["queued_items"])
        self.assertEqual(
            "latest_admitted_preprocess_ready_items_after",
            telemetry["queued_items_basis"],
        )
        self.assertEqual(2, telemetry["preprocessed_items"])
        self.assertEqual(5, telemetry["asr_completed_items"])

        restored = AutonomousController(self.config, ControlStore(self.config), HeldBackend())
        self.assertEqual(telemetry, restored._pipeline_telemetry())
        restored._admit_outcome(
            StageOutcome(
                "preprocess",
                "progressed",
                True,
                {"processed_items": 1, "ready_items_after": 6},
                {},
            ),
            "preprocess",
        )
        restored._admit_outcome(
            StageOutcome(
                "preprocess",
                "held",
                False,
                {"processed_items": 99, "ready_items_after": 6},
                {},
            ),
            "preprocess",
        )
        current = restored._pipeline_telemetry()
        self.assertEqual(3, current["preprocessed_items"])
        self.assertEqual(6, current["queued_items"])

    def test_receipt_and_asr_recovery_totals_reanchor_and_regress_fail_closed(self) -> None:
        controller = AutonomousController(
            self.config, ControlStore(self.config), HeldBackend()
        )
        controller._admit_outcome(
            StageOutcome(
                "preprocess",
                "progressed",
                True,
                {
                    "processed_items": 1,
                    "preprocessed_items_cumulative": 3,
                    "ready_items_after": 4,
                },
                {},
            ),
            "preprocess",
        )
        controller._observe_recovery_pipeline_telemetry(
            {
                "preprocessed_items_cumulative": 4,
                "gpu_completed_items_restored": 6,
            }
        )
        telemetry = controller._pipeline_telemetry()
        self.assertEqual(4, telemetry["preprocessed_items"])
        self.assertEqual(
            "validated_backend_restore_preprocess_receipt_total",
            telemetry["preprocessed_items_basis"],
        )
        self.assertEqual(6, telemetry["asr_completed_items"])

        controller._admit_outcome(
            StageOutcome(
                "preprocess",
                "held",
                False,
                {
                    "processed_items": 0,
                    "preprocessed_items_cumulative": 3,
                    "ready_items_after": 4,
                },
                {},
            ),
            "preprocess",
        )
        controller._admit_outcome(
            StageOutcome(
                "gpu_readiness",
                "held",
                False,
                {"completed_items": 5, "completed_batches": 1},
                {},
            ),
            "gpu_readiness",
        )
        regressed = controller._pipeline_telemetry()
        self.assertIsNone(regressed["preprocessed_items"])
        self.assertEqual(
            "unavailable_preprocess_receipt_total_regressed",
            regressed["preprocessed_items_basis"],
        )
        self.assertIsNone(regressed["asr_completed_items"])
        self.assertEqual(
            "unavailable_gpu_completed_item_count_regressed",
            regressed["asr_completed_items_basis"],
        )

        controller._observe_recovery_pipeline_telemetry(
            {
                "preprocessed_items_cumulative": 9,
                "gpu_completed_items_restored": 9,
            }
        )
        still_closed = controller._pipeline_telemetry()
        self.assertIsNone(still_closed["preprocessed_items"])
        self.assertIsNone(still_closed["asr_completed_items"])

    def test_pipeline_telemetry_never_relabels_batches_as_asr_items(self) -> None:
        controller = AutonomousController(
            self.config, ControlStore(self.config), HeldBackend()
        )
        controller._admit_outcome(
            StageOutcome(
                "gpu_readiness",
                "progressed",
                True,
                {"completed_batches": 4},
                {},
            ),
            "gpu_readiness",
        )
        telemetry = controller._pipeline_telemetry()
        self.assertIsNone(telemetry["asr_completed_items"])
        self.assertEqual(
            "unavailable_gpu_completed_item_count",
            telemetry["asr_completed_items_basis"],
        )

    def test_public_pipeline_telemetry_is_optional_but_malformed_counts_fail_closed(self) -> None:
        unavailable = _validated_pipeline_telemetry(None)
        self.assertIsNone(unavailable["queued_items"])
        self.assertIsNone(unavailable["preprocessed_items"])
        malformed = dict(unavailable)
        malformed["queued_items"] = True
        with self.assertRaisesRegex(PublicStatusError, "queued_items"):
            _validated_pipeline_telemetry(malformed)

    def test_held_polling_does_not_grow_the_material_event_journal(self) -> None:
        store = ControlStore(self.config)
        ticks = iter(range(10_000))
        controller = AutonomousController(
            self.config,
            store,
            HeldBackend(),
            monotonic=lambda: float(next(ticks)),
            sleep=lambda _seconds: None,
        )
        request_start(self.config)
        self.assertEqual(0, controller.run(maximum_cycles=100))
        self.assertEqual(
            ["controller_started", "controller_stopped"],
            [event["event_type"] for event in store.events],
        )

    def test_campaign_completion_distinguishes_unresolved_parked_inventory(self) -> None:
        complete_root = self.root / "complete"
        complete_root.mkdir(mode=0o700)
        (complete_root / "state").mkdir(mode=0o700)
        complete_config = make_config(complete_root)
        complete_store = ControlStore(complete_config)
        complete = AutonomousController(
            complete_config, complete_store, CompleteBackend(parked=0)
        )
        request_start(complete_config)
        self.assertEqual(0, complete.run())
        self.assertEqual("completed", complete_store.read_status()["actual_state"])

        blocked_root = self.root / "blocked"
        blocked_root.mkdir(mode=0o700)
        (blocked_root / "state").mkdir(mode=0o700)
        blocked_config = make_config(blocked_root)
        blocked_store = ControlStore(blocked_config)
        blocked = AutonomousController(
            blocked_config, blocked_store, CompleteBackend(parked=724)
        )
        request_start(blocked_config)
        self.assertEqual(0, blocked.run())
        status = blocked_store.read_status()
        self.assertEqual("blocked", status["actual_state"])
        self.assertEqual(
            "primary_pass_drained_with_postprocess_backlog",
            status["completion_reason"],
        )

    def test_registered_longform_companion_holds_then_covers_postprocess_backlog(
        self,
    ) -> None:
        controller = AutonomousController(
            self.config, ControlStore(self.config), CompleteBackend(parked=724)
        )
        controller._monitor.update(
            {
                "recovery": {
                    "campaign_coverage": {"parked_requires_chunking_count": 724}
                },
                "acquisition": {"status": "complete", "pending": 0},
                "preprocess": {
                    "status": "complete",
                    "ready_items_after": 0,
                },
                "gpu_readiness": {
                    "status": "complete",
                    "pending_batches": 0,
                    "active_children": 0,
                },
                "cold_retention": {
                    "status": "skipped",
                    "pending_items": 0,
                    "replay_pending_items": 0,
                },
            }
        )
        controller._longform_companion = mock.sentinel.registration  # type: ignore[assignment]
        base_status = {
            "lifecycle": "waiting",
            "last_error": None,
            "expected_cold_backlog": 724,
            "discovered": {
                "cold_candidates": 723,
                "queue_candidates": 0,
                "total_candidates": 723,
            },
            "jobs": {
                "unprepared": 0,
                "preprocessed": 0,
                "prepared": 0,
                "incomplete": 0,
                "completed": 723,
            },
            "active_job": None,
        }
        with mock.patch(
            "autonomous_controller.controller.read_companion_status",
            return_value=base_status,
        ):
            self.assertIsNone(controller._terminal_disposition())

        complete_status = copy.deepcopy(base_status)
        complete_status["discovered"]["cold_candidates"] = 724
        complete_status["discovered"]["total_candidates"] = 724
        complete_status["jobs"]["completed"] = 724
        with mock.patch(
            "autonomous_controller.controller.read_companion_status",
            return_value=complete_status,
        ):
            self.assertEqual("campaign_drained", controller._terminal_disposition())

    def test_registered_longform_companion_fault_fails_terminal_check_closed(
        self,
    ) -> None:
        controller = AutonomousController(
            self.config, ControlStore(self.config), CompleteBackend(parked=1)
        )
        controller._longform_companion = mock.sentinel.registration  # type: ignore[assignment]
        with mock.patch(
            "autonomous_controller.controller.read_companion_status",
            return_value={
                "lifecycle": "faulted",
                "last_error": {"type": "CampaignError", "message": "bad span"},
            },
        ):
            with self.assertRaisesRegex(
                ControllerError, "registered long-form companion faulted: bad span"
            ):
                controller._longform_postprocess_complete()

    def test_stop_and_public_status_do_not_replay_the_event_journal(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        inventory = repository / (
            "research/corpus/acquisition-planning/archive-all-known-2026-08-29/"
            "campaign-inventory.json"
        )
        core = config_core(self.root)
        core["campaign"]["inventory"] = {
            "kind": "known_collections_inventory",
            "path": str(inventory),
            "sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
        }
        campaign_core = {
            key: value
            for key, value in core["campaign"].items()
            if key != "campaign_id"
        }
        core["campaign"]["campaign_id"] = (
            "himrarccampaign_"
            + sha256_bytes(canonical_bytes(campaign_core))[:32]
        )
        document = build_config(core)
        body = canonical_bytes(document)
        path = self.root / "public-controller.json"
        path.write_bytes(body)
        path.chmod(0o400)
        config = load_config(path, hashlib.sha256(body).hexdigest())
        store = ControlStore(config)
        corrupt = store.events_root / "not-an-event"
        corrupt.write_text("not json", encoding="utf-8")
        corrupt.chmod(0o400)

        started = request_start(config)
        self.assertEqual("running", started["desired_state"])
        started_public = read_public_status(path, hashlib.sha256(body).hexdigest())
        self.assertEqual("running", started_public["desired_state"])
        self.assertTrue(started_public["running"])
        self.assertFalse(started_public["can_start"])
        self.assertTrue(started_public["can_stop"])
        self.assertEqual(started["generation"], started_public["control_generation"])
        control = request_stop(config)
        self.assertEqual("stopped", control["desired_state"])
        self.assertGreater(control["generation"], started["generation"])
        self.assertEqual(control, read_control_state(config))
        public = read_public_status(path, hashlib.sha256(body).hexdigest())
        self.assertEqual("not_started", public["actual_state"])
        self.assertEqual("stopped", public["desired_state"])
        self.assertFalse(public["running"])
        self.assertTrue(public["can_start"])
        self.assertFalse(public["can_stop"])
        self.assertEqual(control["generation"], public["control_generation"])
        self.assertEqual(
            {
                "can_start": True,
                "can_stop": False,
                "control_generation": control["generation"],
            },
            public["controls"],
        )
        self.assertEqual(7, public["campaign"]["collection_count"])
        self.assertEqual(3_923, public["campaign"]["candidate_count"])
        self.assertEqual(724, public["campaign"]["unresolved_parked_items"])


if __name__ == "__main__":
    unittest.main()
