from __future__ import annotations

import fcntl
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from autonomous_controller.config import (
    ControllerConfig,
    canonical_bytes,
    load_config,
)
from autonomous_controller.setup_archive_successor import (
    COMPOSITE_KIND,
    FRESH_WRITABLE_NAMES,
    PREDECESSOR_CONFIG,
    SetupError,
    _assert_predecessor_component,
    _assert_empty_restore,
    _composite_expected_coverage,
    _durable_predecessor_progress,
    _hold_stopped_predecessor,
    _initialize_fresh_roots,
    _validate_reused_preprocess_roots,
    successor_config_core,
)


def _write_private_json(path: Path, value: dict) -> None:
    path.write_bytes(canonical_bytes(value))
    os.chmod(path, 0o600)


class SuccessorSetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-successor-setup-")
        self.root = Path(self.temporary.name).resolve()
        os.chmod(self.root, 0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _predecessor_state_fixture(self) -> ControllerConfig:
        state = self.root / "predecessor-state"
        state.mkdir(mode=0o700)
        (state / "events").mkdir(mode=0o700)
        (state / "gpu-children").mkdir(mode=0o700)
        for name in ("controller.lock", "control.lock"):
            path = state / name
            path.touch(mode=0o600)
            os.chmod(path, 0o600)
        config_id = "himrautocfg_" + "a" * 32
        physical_sha256 = "b" * 64
        config = ControllerConfig(
            document={"config_id": config_id, "state_root": str(state)},
            path=self.root / "predecessor-config.json",
            physical_sha256=physical_sha256,
        )
        _write_private_json(
            state / "control.json",
            {
                "kind": "himr_autonomous_controller_control",
                "schema_version": 1,
                "config_id": config_id,
                "generation": 9,
                "desired_state": "stopped",
                "requested_at": "2026-08-30T00:00:00Z",
            },
        )
        _write_private_json(
            state / "status.json",
            {
                "kind": "himr_autonomous_controller_status",
                "schema_version": 1,
                "config_id": config_id,
                "config_sha256": physical_sha256,
                "desired_state": "stopped",
                "actual_state": "stopped",
                "lifecycle": "stopped",
                "current_stage": None,
                "current_gpu_child": None,
                "execution": {
                    "accepting_new_work": False,
                    "draining": False,
                    "inflight_total": 0,
                },
                "lanes": {
                    name: {"active": 0}
                    for name in (
                        "acquisition",
                        "preprocess",
                        "gpu_readiness",
                        "cold_retention",
                    )
                },
            },
        )
        return config

    def test_stopped_predecessor_holds_both_existing_locks(self) -> None:
        config = self._predecessor_state_fixture()
        with _hold_stopped_predecessor(config) as status:
            self.assertEqual("stopped", status["actual_state"])
            for name in ("controller.lock", "control.lock"):
                descriptor = os.open(config.state_root / name, os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)

    def test_running_or_inflight_predecessor_fails_closed(self) -> None:
        config = self._predecessor_state_fixture()
        status_path = config.state_root / "status.json"
        value = __import__("json").loads(status_path.read_bytes())
        value["execution"]["inflight_total"] = 1
        _write_private_json(status_path, value)
        with self.assertRaisesRegex(SetupError, "not fully stopped"):
            with _hold_stopped_predecessor(config):
                pass

    def test_held_predecessor_run_lock_fails_without_creating_state(self) -> None:
        config = self._predecessor_state_fixture()
        descriptor = os.open(config.state_root / "controller.lock", os.O_RDWR)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(SetupError, "is held"):
                with _hold_stopped_predecessor(config):
                    pass
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def test_fresh_initializer_excludes_and_does_not_touch_preprocess_roots(self) -> None:
        repository = self.root / "repository"
        research = repository / "research"
        research.mkdir(parents=True, mode=0o755)
        operational_parent = research / "operator-state"
        successor_root = operational_parent / "successor"
        reused_bundle = self.root / "reused-bundles"
        reused_output = self.root / "reused-output"
        reused_bundle.mkdir(mode=0o700)
        reused_output.mkdir(mode=0o700)
        (reused_bundle / "existing-bundle").write_text("preserved", encoding="utf-8")
        (reused_output / "existing-result").write_text("preserved", encoding="utf-8")
        predecessor = ControllerConfig(
            document={
                "config_id": "himrautocfg_" + "c" * 32,
                "state_root": str(self.root / "unused-state"),
                "preprocess": {
                    "bundle_root": str(reused_bundle),
                    "processing_output_root": str(reused_output),
                },
            },
            path=self.root / "unused.json",
            physical_sha256="d" * 64,
        )
        with (
            patch(
                "autonomous_controller.setup_archive_successor.REPOSITORY",
                repository,
            ),
            patch(
                "autonomous_controller.setup_archive_successor.OPERATIONAL_PARENT",
                operational_parent,
            ),
            patch(
                "autonomous_controller.setup_archive_successor.SUCCESSOR_OPERATIONAL_ROOT",
                successor_root,
            ),
        ):
            _validate_reused_preprocess_roots(predecessor)
            roots = _initialize_fresh_roots()
        self.assertEqual(set(FRESH_WRITABLE_NAMES), set(roots))
        self.assertNotIn("preprocess-control", roots)
        self.assertNotIn("preprocess-output", roots)
        self.assertEqual(
            "preserved", (reused_bundle / "existing-bundle").read_text(encoding="utf-8")
        )
        self.assertEqual(
            "preserved", (reused_output / "existing-result").read_text(encoding="utf-8")
        )
        self.assertFalse(any((roots["state"] / "events").iterdir()))
        self.assertFalse(any((roots["state"] / "gpu-children").iterdir()))
        for name, path in roots.items():
            self.assertEqual(0o700, path.stat().st_mode & 0o777, name)

    def test_successor_core_preserves_preprocess_and_moves_every_other_root(self) -> None:
        predecessor = load_config(
            Path(PREDECESSOR_CONFIG["path"]), PREDECESSOR_CONFIG["sha256"]
        )
        predecessor_schedules = predecessor.section("campaign")["schedules"]
        normal = [row for row in predecessor_schedules if row["role"] == "normal_processing"]
        cold = [
            row
            for row in predecessor_schedules
            if row["role"] == "cold_acquisition_only_requires_chunking"
        ]

        def composite_row(row: dict, ordinal: int, component: str) -> dict:
            return {
                "schedule_ordinal": ordinal,
                "component": component,
                "component_schedule_ordinal": ordinal,
                "role": row["role"],
                "schedule_path": row["path"],
                "schedule_sha256": row["sha256"],
                "schedule_byte_count": 1,
                "schedule_id": row["schedule_id"],
                "schedule_identity_sha256": "1" * 64,
                "preprocess_state_root": str(self.root / f"state-{ordinal}"),
                "source_schedule_set": {},
                "source_campaign": {},
                "source_epoch": {},
            }

        flattened = []
        for row in normal:
            flattened.append(composite_row(row, len(flattened) + 1, "predecessor"))
        add_normal = {
            "path": str(self.root / "add-normal.json"),
            "sha256": "2" * 64,
            "schedule_id": "bgacqsched_" + "2" * 32,
            "role": "normal_processing",
        }
        flattened.append(composite_row(add_normal, len(flattened) + 1, "addendum"))
        for row in cold:
            flattened.append(composite_row(row, len(flattened) + 1, "predecessor"))
        add_cold = {
            "path": str(self.root / "add-cold.json"),
            "sha256": "3" * 64,
            "schedule_id": "bgacqsched_" + "3" * 32,
            "role": "cold_acquisition_only_requires_chunking",
        }
        flattened.append(composite_row(add_cold, len(flattened) + 1, "addendum"))
        source_set = predecessor.section("campaign")["schedule_set"]
        composite = {
            "composite_schedule_set_id": "bgacqcompositeset_" + "4" * 32,
            "components": [
                {
                    "component": "predecessor",
                    "schedule_set_id": source_set["schedule_set_id"],
                    "manifest_path": source_set["path"],
                    "manifest_sha256": source_set["sha256"],
                },
                {"component": "addendum"},
            ],
            "schedules": flattened,
        }
        _assert_predecessor_component(predecessor, composite)
        roots = {name: self.root / name for name in FRESH_WRITABLE_NAMES}
        manifest = self.root / "composite.json"
        core = successor_config_core(
            predecessor=predecessor,
            composite=composite,
            inventory_sha256="5" * 64,
            composite_manifest_path=manifest,
            composite_manifest_sha256="6" * 64,
            roots=roots,
        )
        self.assertEqual(COMPOSITE_KIND, core["campaign"]["schedule_set"]["kind"])
        self.assertEqual(
            predecessor.section("preprocess")["bundle_root"],
            core["preprocess"]["bundle_root"],
        )
        self.assertEqual(
            predecessor.section("preprocess")["processing_output_root"],
            core["preprocess"]["processing_output_root"],
        )
        self.assertEqual(str(roots["gpu-queues"]), core["gpu_readiness"]["queue_root"])
        self.assertEqual(str(roots["state"]), core["state_root"])
        self.assertEqual(
            len(predecessor_schedules) + 2, len(core["campaign"]["schedules"])
        )

    def test_restore_must_equal_frozen_progress_and_composite_coverage(self) -> None:
        status = {
            "monitor": {
                "acquisition": {"completed": 195},
                "preprocess": {"preprocessed_items_cumulative": 176},
            },
            "progress": {"acquisition": {"completed": 195}},
            "pipeline_telemetry": {"preprocessed_items": 176},
        }
        predecessor_progress = _durable_predecessor_progress(status)
        composite = {
            "schedules": [{} for _ in range(219)],
            "coverage_proof": {
                "source_selected_count": 4484,
                "schedule_count": 219,
            },
            "role_totals": [
                {"role": "normal_processing", "selected_count": 3680},
                {
                    "role": "cold_acquisition_only_requires_chunking",
                    "selected_count": 804,
                },
            ],
        }
        expected_coverage = _composite_expected_coverage(composite)
        restored = {
            key: 0
            for key in (
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
        }
        restored.update(predecessor_progress)
        restored.update(
            {
                "campaign_schedule_count": 219,
                "campaign_coverage": {
                    "candidate_count": 4484,
                    "ready_selected_count": 3680,
                    "parked_requires_chunking_count": 804,
                    "scheduled_candidate_count": 4484,
                    "scheduled_ready_count": 3680,
                    "scheduled_cold_only_count": 804,
                },
                "schedule_set_coverage": {
                    "schedule_count": 219,
                    "selected_count": 4484,
                    "normal_selected_count": 3680,
                    "cold_only_selected_count": 804,
                },
            }
        )
        _assert_empty_restore(
            restored,
            predecessor_progress=predecessor_progress,
            expected_coverage=expected_coverage,
        )
        restored["preprocessed_items_cumulative"] = 175
        with self.assertRaisesRegex(SetupError, "durable progress"):
            _assert_empty_restore(
                restored,
                predecessor_progress=predecessor_progress,
                expected_coverage=expected_coverage,
            )


if __name__ == "__main__":
    unittest.main()
