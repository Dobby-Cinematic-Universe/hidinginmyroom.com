from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest import mock

from autonomous_controller import cli
from autonomous_controller.gpu_child import GpuChildError
from autonomous_controller.public_status import PublicStatusError


class AutonomousControllerCliTests(unittest.TestCase):
    argv = [
        "run",
        "--config",
        "/private/controller-config.json",
        "--expected-config-sha256",
        "a" * 64,
    ]

    def invoke(
        self,
        *,
        returncode: int = 0,
        lifecycle: str = "stopped",
        run_error: Exception | None = None,
        status_error: Exception | None = None,
    ) -> tuple[int, str, str]:
        config = SimpleNamespace(config_id="himrautocfg_" + "b" * 32)
        public = {
            "lifecycle": lifecycle,
            "actual_state": lifecycle,
            "desired_state": "stopped",
            "completion_reason": (
                "campaign_drained" if lifecycle == "completed" else None
            ),
            "cycle": 7,
            "last_error": (
                {"type": "FixtureError", "message": "faulted"}
                if lifecycle == "faulted"
                else None
            ),
        }
        controller = mock.Mock()
        if run_error is None:
            controller.run.return_value = returncode
        else:
            controller.run.side_effect = run_error
        status_side_effect = status_error if status_error is not None else None
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(cli, "load_config", return_value=config),
            mock.patch.object(cli, "ControlStore", return_value=mock.Mock()),
            mock.patch.object(cli, "SealedArchiveBackend", return_value=mock.Mock()),
            mock.patch.object(cli, "AutonomousController", return_value=controller),
            mock.patch.object(cli.signal, "signal"),
            mock.patch.object(
                cli,
                "read_public_status",
                return_value=public,
                side_effect=status_side_effect,
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            observed = cli.main(self.argv)
        return observed, stdout.getvalue(), stderr.getvalue()

    def test_successful_run_emits_one_strict_terminal_object(self) -> None:
        for lifecycle in ("stopped", "completed", "blocked"):
            with self.subTest(lifecycle=lifecycle):
                returncode, stdout, stderr = self.invoke(lifecycle=lifecycle)
                self.assertEqual(returncode, 0)
                self.assertEqual(stderr, "")
                value = json.loads(stdout)
                self.assertEqual(value["status"], "controller_exited")
                self.assertEqual(value["returncode"], 0)
                self.assertEqual(value["lifecycle"], lifecycle)

    def test_retry_ceiling_emits_one_strict_failure_object(self) -> None:
        returncode, stdout, stderr = self.invoke(returncode=2, lifecycle="faulted")
        self.assertEqual(returncode, 2)
        self.assertEqual(stdout, "")
        value = json.loads(stderr)
        self.assertEqual(value["status"], "controller_faulted")
        self.assertEqual(value["returncode"], 2)
        self.assertEqual(value["actual_state"], "faulted")

    def test_gpu_child_error_is_structured_without_a_traceback(self) -> None:
        returncode, stdout, stderr = self.invoke(
            run_error=GpuChildError("fixture GPU boundary failure")
        )
        self.assertEqual(returncode, 2)
        self.assertEqual(stdout, "")
        value = json.loads(stderr)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["error"]["type"], "GpuChildError")
        self.assertNotIn("Traceback", stderr)

    def test_dynamically_loaded_runtime_error_keeps_strict_json_boundary(self) -> None:
        class QueueRunnerLikeError(RuntimeError):
            pass

        returncode, stdout, stderr = self.invoke(
            run_error=QueueRunnerLikeError("fixture immutable-envelope failure")
        )
        self.assertEqual(returncode, 2)
        self.assertEqual(stdout, "")
        value = json.loads(stderr)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["error"]["type"], "QueueRunnerLikeError")
        self.assertEqual(
            value["error"]["message"], "fixture immutable-envelope failure"
        )
        self.assertNotIn("Traceback", stderr)

    def test_terminal_status_failure_emits_no_partial_success_object(self) -> None:
        returncode, stdout, stderr = self.invoke(
            status_error=PublicStatusError("terminal status unavailable")
        )
        self.assertEqual(returncode, 2)
        self.assertEqual(stdout, "")
        value = json.loads(stderr)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["error"]["type"], "PublicStatusError")

    def test_deep_audit_checkpoints_full_journal_under_run_lock(self) -> None:
        config = SimpleNamespace(config_id="himrautocfg_" + "b" * 32)
        anchor = {
            "sequence": 9,
            "event_sha256": "c" * 64,
            "event_type": "cycle_completed",
        }
        events = ({**anchor, "payload": {}},)
        lock_store = mock.MagicMock()
        audit_store = mock.Mock()
        audit_store.read_control.return_value = {"desired_state": "stopped"}
        audit_store.events = events
        final_store = mock.Mock()
        final_store.read_control.return_value = {"desired_state": "stopped"}
        final_store.events = events
        final_store.write_checkpoint.return_value = {
            "anchor": anchor,
            "checkpoint_sha256": "d" * 64,
            "created_at": "2026-08-31T12:00:00Z",
        }
        backend = mock.Mock()
        backend.restore.return_value = {"restart": {"mode": "deep_audit"}}
        backend.export_checkpoint.return_value = {"kind": "fixture_checkpoint"}
        stdout = io.StringIO()
        stderr = io.StringIO()
        argv = [
            "deep-audit-checkpoint",
            "--config",
            "/private/controller-config.json",
            "--expected-config-sha256",
            "a" * 64,
        ]
        with (
            mock.patch.object(cli, "load_config", return_value=config),
            mock.patch.object(
                cli,
                "ControlStore",
                side_effect=[lock_store, audit_store, final_store],
            ),
            mock.patch.object(cli, "SealedArchiveBackend", return_value=backend),
            mock.patch.object(cli, "AutonomousController") as controller_type,
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            returncode = cli.main(argv)

        self.assertEqual(returncode, 0)
        self.assertEqual(stderr.getvalue(), "")
        value = json.loads(stdout.getvalue())
        self.assertEqual(value["status"], "deep_audit_checkpointed")
        self.assertEqual(value["recovery_mode"], "deep_audit")
        self.assertFalse(value["stages_launched"])
        lock_store.run_lock.assert_called_once_with()
        backend.restore.assert_called_once_with(events)
        backend.export_checkpoint.assert_called_once_with()
        final_store.write_checkpoint.assert_called_once_with(
            {"kind": "fixture_checkpoint"},
            expected_anchor_sequence=9,
            expected_anchor_sha256="c" * 64,
        )
        controller_type.assert_not_called()

    def test_deep_audit_refuses_running_intent_before_backend_creation(self) -> None:
        config = SimpleNamespace(config_id="himrautocfg_" + "b" * 32)
        lock_store = mock.MagicMock()
        audit_store = mock.Mock()
        audit_store.read_control.return_value = {"desired_state": "running"}
        stdout = io.StringIO()
        stderr = io.StringIO()
        argv = [
            "deep-audit-checkpoint",
            "--config",
            "/private/controller-config.json",
            "--expected-config-sha256",
            "a" * 64,
        ]
        with (
            mock.patch.object(cli, "load_config", return_value=config),
            mock.patch.object(
                cli, "ControlStore", side_effect=[lock_store, audit_store]
            ),
            mock.patch.object(cli, "SealedArchiveBackend") as backend_type,
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            returncode = cli.main(argv)

        self.assertEqual(returncode, 2)
        self.assertEqual(stdout.getvalue(), "")
        value = json.loads(stderr.getvalue())
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["error"]["type"], "StateError")
        self.assertIn("desired_state stopped", value["error"]["message"])
        backend_type.assert_not_called()

    def test_recover_checkpoint_replays_existing_checkpoint_without_launch(self) -> None:
        for has_checkpoint, trust_copy in ((True, False), (False, False), (True, True), (False, True)):
            with self.subTest(has_checkpoint=has_checkpoint, trust_copy=trust_copy):
                config = SimpleNamespace(config_id="himrautocfg_" + "b" * 32)
                anchor = {"sequence": 9, "event_sha256": "c" * 64,
                          "event_type": "controller_stopped"}
                lock_store = mock.MagicMock()
                audit_store = mock.Mock()
                audit_store.read_control.return_value = {"desired_state": "stopped"}
                audit_store.events = (anchor,)
                audit_store.recovery_view.return_value = SimpleNamespace(
                    checkpoint={"backend": {"old": True}} if has_checkpoint else None,
                    tail_events=({"tail": True},),
                )
                final_store = mock.Mock()
                final_store.read_control.return_value = {"desired_state": "stopped"}
                final_store.events = (anchor,)
                final_store.write_checkpoint.return_value = {
                    "anchor": anchor, "checkpoint_sha256": "d" * 64,
                    "created_at": "2026-09-06T23:00:00Z",
                }
                backend = mock.Mock()
                backend.restore_checkpoint.return_value = {
                    "restart": {"mode": "checkpoint_plus_tail"}
                }
                backend.restore_rsync_copy_checkpoint.return_value = backend.restore_checkpoint.return_value
                backend.export_checkpoint.return_value = {"refreshed": True}
                stdout, stderr = io.StringIO(), io.StringIO()
                with (
                    mock.patch.object(cli, "load_config", return_value=config),
                    mock.patch.object(cli, "ControlStore",
                                      side_effect=[lock_store, audit_store, final_store]),
                    mock.patch.object(cli, "SealedArchiveBackend", return_value=backend),
                    mock.patch.object(cli, "AutonomousController") as controller_type,
                    redirect_stdout(stdout), redirect_stderr(stderr),
                ):
                    code = cli.main([
                        "recover-checkpoint", "--config", "/private/config.json",
                        "--expected-config-sha256", "a" * 64,
                    ] + (["--trust-rsync-copy"] if trust_copy else []))
                backend.restore.assert_not_called()
                controller_type.assert_not_called()
                lock_store.run_lock.assert_called_once_with()
                if has_checkpoint:
                    self.assertEqual(code, 0)
                    value = json.loads(stdout.getvalue())
                    self.assertEqual(value["status"], "recovery_checkpointed")
                    self.assertFalse(value["stages_launched"])
                    selected = (backend.restore_rsync_copy_checkpoint if trust_copy
                                else backend.restore_checkpoint)
                    other = (backend.restore_checkpoint if trust_copy
                             else backend.restore_rsync_copy_checkpoint)
                    selected.assert_called_once_with(
                        {"old": True}, ({"tail": True},)
                    )
                    other.assert_not_called()
                    final_store.write_checkpoint.assert_called_once()
                else:
                    self.assertEqual(code, 2)
                    self.assertIn("requires an existing checkpoint", stderr.getvalue())
                    backend.restore_checkpoint.assert_not_called()
                    backend.restore_rsync_copy_checkpoint.assert_not_called()
                    backend.export_checkpoint.assert_not_called()
                    final_store.write_checkpoint.assert_not_called()

    def test_deep_audit_discards_snapshot_if_stop_intent_changes(self) -> None:
        config = SimpleNamespace(config_id="himrautocfg_" + "b" * 32)
        anchor = {
            "sequence": 2,
            "event_sha256": "e" * 64,
            "event_type": "controller_stopped",
        }
        events = ({**anchor, "payload": {}},)
        lock_store = mock.MagicMock()
        audit_store = mock.Mock()
        audit_store.read_control.return_value = {"desired_state": "stopped"}
        audit_store.events = events
        final_store = mock.Mock()
        final_store.read_control.return_value = {"desired_state": "running"}
        final_store.events = events
        backend = mock.Mock()
        backend.restore.return_value = {"restart": {"mode": "deep_audit"}}
        backend.export_checkpoint.return_value = {"kind": "fixture_checkpoint"}
        stdout = io.StringIO()
        stderr = io.StringIO()
        argv = [
            "deep-audit-checkpoint",
            "--config",
            "/private/controller-config.json",
            "--expected-config-sha256",
            "a" * 64,
        ]
        with (
            mock.patch.object(cli, "load_config", return_value=config),
            mock.patch.object(
                cli,
                "ControlStore",
                side_effect=[lock_store, audit_store, final_store],
            ),
            mock.patch.object(cli, "SealedArchiveBackend", return_value=backend),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            returncode = cli.main(argv)

        self.assertEqual(returncode, 2)
        self.assertEqual(stdout.getvalue(), "")
        value = json.loads(stderr.getvalue())
        self.assertEqual(value["error"]["type"], "StateError")
        self.assertIn("changed from stopped", value["error"]["message"])
        final_store.write_checkpoint.assert_not_called()

    def test_deep_audit_discards_snapshot_if_journal_head_changes(self) -> None:
        config = SimpleNamespace(config_id="himrautocfg_" + "b" * 32)
        initial = {
            "sequence": 2,
            "event_sha256": "e" * 64,
            "event_type": "controller_stopped",
            "payload": {},
        }
        advanced = {
            "sequence": 3,
            "event_sha256": "f" * 64,
            "event_type": "controller_started",
            "payload": {},
        }
        lock_store = mock.MagicMock()
        audit_store = mock.Mock()
        audit_store.read_control.return_value = {"desired_state": "stopped"}
        audit_store.events = (initial,)
        final_store = mock.Mock()
        final_store.read_control.return_value = {"desired_state": "stopped"}
        final_store.events = (initial, advanced)
        backend = mock.Mock()
        backend.restore.return_value = {"restart": {"mode": "deep_audit"}}
        backend.export_checkpoint.return_value = {"kind": "fixture_checkpoint"}
        stdout = io.StringIO()
        stderr = io.StringIO()
        argv = [
            "deep-audit-checkpoint",
            "--config",
            "/private/controller-config.json",
            "--expected-config-sha256",
            "a" * 64,
        ]
        with (
            mock.patch.object(cli, "load_config", return_value=config),
            mock.patch.object(
                cli,
                "ControlStore",
                side_effect=[lock_store, audit_store, final_store],
            ),
            mock.patch.object(cli, "SealedArchiveBackend", return_value=backend),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            returncode = cli.main(argv)

        self.assertEqual(returncode, 2)
        self.assertEqual(stdout.getvalue(), "")
        value = json.loads(stderr.getvalue())
        self.assertEqual(value["error"]["type"], "StateError")
        self.assertIn("journal changed", value["error"]["message"])
        final_store.write_checkpoint.assert_not_called()


if __name__ == "__main__":
    unittest.main()
