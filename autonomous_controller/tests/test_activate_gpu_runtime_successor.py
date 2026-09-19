from __future__ import annotations

from contextlib import contextmanager, nullcontext
import copy
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from autonomous_controller import activate_gpu_runtime_successor as activation
from autonomous_controller.config import ControllerConfig
from autonomous_controller.state import RecoveryView


class ActivationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="himr-runtime-activation-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.receipts = self.root / "materializations"
        self.receipts.mkdir(mode=0o700)
        self.config = ControllerConfig({"config_id": "test-config", "state_root": str(self.root),
            "gpu_readiness": {"receipt_root": str(self.root)}}, self.root / "config.json", "a" * 64)
        self.receipt = self.receipts / "receipt.json"
        self.receipt.write_bytes(b"synthetic")
        self.receipt.chmod(0o400)
        self.record = {"batch_key": "batch-1", "record_kind": "ready_batch",
                       "batch": {"batch_id": "batch-1"},
                       "sources": [{"materialization_receipt": {"path": str(self.receipt)}}]}
        self.row = {"record": self.record, "status": "completed", "item_count": 2,
                    "claims": [], "dispositions": [], "parked": None, "files": []}
        self.backend = {"gpu": {"records": [self.row]}}
        self.view = RecoveryView(checkpoint={"backend": self.backend, "checkpoint_sha256": "b" * 64},
                                 tail_events=(), anchor_sequence=12, legacy_full_replay=False)
        validator = mock.patch.object(activation.SealedArchiveBackend,
            "_validated_backend_checkpoint_envelope", side_effect=lambda _self, value: (value, {}))
        validator.start()
        self.addCleanup(validator.stop)

    def test_drained_checkpoint_counts_and_exact_metadata_inventory(self):
        result = activation._drained_gpu(self.config, self.view)
        self.assertEqual(result["completed_batches"], 1)
        self.assertEqual(result["completed_items"], 2)
        activation._check_materialization_inventory(self.config, result["materialization_receipts"])

    def test_pending_parked_and_malformed_records_refuse(self):
        for status in ("pending", "parked", "unknown", "not_applicable"):
            self.row["status"] = status
            with self.subTest(status=status), self.assertRaises(activation.ActivationError):
                activation._drained_gpu(self.config, self.view)

    def test_not_applicable_requires_no_batch_and_zero_items(self):
        self.row.update(status="not_applicable", item_count=0,
                        record={"batch_key": "no-ready", "record_kind": "no_ready_members", "batch": None})
        result = activation._drained_gpu(self.config, self.view)
        self.assertEqual(result["not_applicable_records"], 1)
        self.assertEqual(result["completed_items"], 0)
        self.assertEqual(result["materialization_receipts"], set())

    def test_uncheckpointed_tail_or_missing_checkpoint_refuses(self):
        for view in (RecoveryView(None, (), 0, True),
                     RecoveryView(self.view.checkpoint, ({"event_type": "stage_started"},), 12, False)):
            with self.subTest(view=view), self.assertRaises(activation.ActivationError):
                activation._drained_gpu(self.config, view)

    def test_unknown_or_missing_materialization_fails_closed(self):
        with self.assertRaises(activation.ActivationError):
            activation._check_materialization_inventory(self.config, set())
        with self.assertRaises(activation.ActivationError):
            activation._check_materialization_inventory(self.config, {str(self.receipt), str(self.receipts / "missing")})

    def test_materialization_symlink_refuses(self):
        link = self.receipts / "link"
        link.symlink_to(self.receipt)
        with self.assertRaises(activation.ActivationError):
            activation._check_materialization_inventory(self.config, {str(self.receipt), str(link)})

    def _activation_context(self, control):
        held = SimpleNamespace(check_unchanged=mock.Mock())
        patches = [
            mock.patch.object(activation, "_guards", return_value=[{}]),
            mock.patch.object(activation, "hold_legacy", return_value=nullcontext(held)),
            mock.patch.object(activation, "_existing_control_lock", return_value=nullcontext()),
            mock.patch.object(activation, "_read_only_store", return_value=SimpleNamespace(recovery_view=lambda: self.view)),
            mock.patch.object(activation, "read_control_state", return_value=control),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        return held

    def test_stage_holds_guards_and_never_starts_services(self):
        held = self._activation_context({"desired_state": "stopped", "generation": 63})
        authorization = {"authorization_id": "test", "identity_sha256": "c" * 64}

        def stage(_config, _controls, *, assert_stopped):
            assert_stopped()
            return authorization

        with mock.patch.object(activation.successor, "stage_successor", side_effect=stage) as stage_mock:
            result = activation.activate(self.config, {}, expected_generation=63)
        stage_mock.assert_called_once()
        self.assertGreaterEqual(held.check_unchanged.call_count, 4)
        self.assertEqual(result["status"], "staged")
        self.assertFalse(result["services_started"])

    def test_check_only_does_not_stage(self):
        self._activation_context({"desired_state": "stopped", "generation": 63})
        with mock.patch.object(activation.successor, "stage_successor") as stage_mock, \
             mock.patch.object(activation.successor, "build_successor", return_value={"authorization_id": "test", "identity_sha256": "c" * 64}):
            result = activation.activate(self.config, {}, expected_generation=63, check_only=True)
        stage_mock.assert_not_called()
        self.assertEqual(result["status"], "validated_only")

    def test_wrong_generation_or_running_intent_never_stages(self):
        for control in ({"desired_state": "stopped", "generation": 62},
                        {"desired_state": "running", "generation": 63}):
            self._activation_context(control)
            with mock.patch.object(activation.successor, "stage_successor") as stage_mock:
                with self.assertRaises(activation.ActivationError):
                    activation.activate(self.config, {}, expected_generation=63)
            stage_mock.assert_not_called()

    def test_control_lock_is_existing_only_and_exclusive(self):
        path = self.root / "control.lock"
        with self.assertRaises(FileNotFoundError):
            with activation._existing_control_lock(self.config):
                self.fail("missing lock accepted")
        self.assertFalse(path.exists())
        path.write_bytes(b"")
        path.chmod(0o600)
        with activation._existing_control_lock(self.config):
            with self.assertRaises(BlockingIOError):
                with activation._existing_control_lock(self.config):
                    self.fail("concurrent publication lock accepted")
        self.assertEqual(path.read_bytes(), b"")

    def _inert_fault_fixture(self):
        from pipeline.tests.test_hybrid_legacy_guard import LegacyGuardTests
        fixture = LegacyGuardTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.status["lanes"]["gpu_readiness"].update(
            state="faulted", last_status="held", wait_reason=activation.INERT_BWRAP_FAILURE)
        gpu = {"active_children": 0, "current_gpu_child": None, "stop_reconciled": True,
               "pending_batches": 0, "pending_items": 0, "parked_batches": 0,
               "parked_items": 0, "buffered_ready_items": 0}
        fixture.status["stages"]["gpu_readiness"] = gpu
        fixture.status["monitor"]["gpu_readiness"] = gpu
        fixture.save()
        return fixture

    def test_inert_fault_guard_keeps_raw_status_and_strict_hybrid_guard_unchanged(self):
        fixture = self._inert_fault_fixture()
        path = Path(fixture.config["status_path"])
        before = path.read_bytes()
        self.assertFalse(fixture.inspect()["safe"])
        with activation.hold_legacy([fixture.config]) as held:
            fixture.assert_held("controller_lock")
            fixture.assert_held("companion_lock")
            held.check_unchanged()
            self.assertEqual(path.read_bytes(), before)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(fixture.status["lanes"]["gpu_readiness"]["state"], "faulted")
        self.assertFalse(fixture.inspect()["safe"])
        with self.assertRaises(activation.ActivationError):
            held.check_unchanged()

    def test_inert_fault_guard_rejects_active_unrelated_fault_and_live_pid(self):
        fixture = self._inert_fault_fixture()
        original = copy.deepcopy(fixture.status)
        changes = [lambda s: s["lanes"]["gpu_readiness"].update(active=1),
                   lambda s: s["lanes"]["gpu_readiness"].update(wait_reason="different failure"),
                   lambda s: s["lanes"]["acquisition"].update(state="faulted"),
                   lambda s: s["stages"]["gpu_readiness"].update(pending_batches=1),
                   lambda s: s.update(pid=os.getpid())]
        for index, change in enumerate(changes):
            fixture.status = copy.deepcopy(original)
            change(fixture.status)
            fixture.save()
            with self.subTest(index=index), self.assertRaises((activation.ActivationError, activation.legacy.LegacyBusy)):
                with activation.hold_legacy([fixture.config]):
                    self.fail("unsafe fault entered activation")

    def test_inert_fault_guard_retains_raw_snapshot_witness(self):
        fixture = self._inert_fault_fixture()
        with activation.hold_legacy([fixture.config]) as held:
            fixture.status["last_error"] = "changed after lock acquisition"
            fixture.save()
            with self.assertRaisesRegex(activation.ActivationError, "raw legacy state changed"):
                held.check_unchanged()


if __name__ == "__main__":
    unittest.main()
