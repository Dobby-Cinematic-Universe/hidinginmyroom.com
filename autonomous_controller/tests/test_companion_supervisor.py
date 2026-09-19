from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from autonomous_controller import companion_supervisor as supervisor
from autonomous_controller.gpu_child import CONTROLLER_SUPERVISOR_PID_ENV
from autonomous_controller.longform_companion import LongformCompanionRegistration


class CompanionSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config_path = self.root / "controller.json"
        self.config_path.write_text("{}\n", encoding="utf-8")
        self.controller = SimpleNamespace(
            config_id="himrautocfg_" + "a" * 32,
        )
        document = {
            "identity_sha256": "b" * 64,
            "companion": {
                "config_path": str(self.root / "companion.json"),
                "config_sha256": "c" * 64,
                "entrypoint_path": "/bin/true",
                "status_path": str(self.root / "status.json"),
            },
            "supervision": {
                "poll_interval_milliseconds": 50,
                "graceful_stop_timeout_seconds": 60,
            },
        }
        self.registration = LongformCompanionRegistration(
            document=document,
            path=self.root / "registration.json",
            physical_sha256="d" * 64,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _python_result(value: dict[str, object], returncode: int = 0) -> list[str]:
        body = json.dumps(value, separators=(",", ":"))
        return [
            sys.executable,
            "-c",
            f"import sys;sys.stdout.write({body!r});sys.exit({returncode})",
        ]

    def test_successful_pair_returns_one_composite_result(self) -> None:
        source = self._python_result({"status": "controller_exited"})
        companion = self._python_result({"status": "stopped"})
        with (
            mock.patch.object(supervisor, "load_config", return_value=self.controller),
            mock.patch.object(supervisor, "read_companion_status", return_value={}),
            mock.patch.object(supervisor, "_child_argvs", return_value=(source, companion)),
            mock.patch.object(supervisor, "request_stop") as stop,
        ):
            returncode, result = supervisor.supervise_registered_campaign(
                controller_config_path=self.config_path,
                expected_controller_sha256="e" * 64,
                registration=self.registration,
            )
        self.assertEqual(returncode, 0)
        self.assertEqual(result["status"], "supervised_campaign_exited")
        self.assertEqual(
            result["children"]["longform_companion"]["result"]["status"],
            "stopped",
        )
        # Both can finish inside one polling interval; no redundant control write
        # is then needed.
        self.assertLessEqual(stop.call_count, 1)

    def test_only_source_child_receives_exact_supervisor_delegation(self) -> None:
        script = (
            "import json,os;"
            f"key={CONTROLLER_SUPERVISOR_PID_ENV!r};"
            "print(json.dumps({'status':'stopped','delegation':os.environ.get(key)},"
            "separators=(',',':')))"
        )
        child = [sys.executable, "-c", script]
        expected_supervisor_pid = str(os.getpid())
        with (
            mock.patch.object(supervisor, "load_config", return_value=self.controller),
            mock.patch.object(supervisor, "read_companion_status", return_value={}),
            mock.patch.object(
                supervisor, "_child_argvs", return_value=(child, child)
            ),
            mock.patch.object(supervisor, "request_stop"),
            mock.patch.dict(
                os.environ,
                {CONTROLLER_SUPERVISOR_PID_ENV: "999999"},
                clear=False,
            ),
        ):
            returncode, result = supervisor.supervise_registered_campaign(
                controller_config_path=self.config_path,
                expected_controller_sha256="e" * 64,
                registration=self.registration,
            )

        self.assertEqual(returncode, 0)
        self.assertEqual(
            result["children"]["source_controller"]["result"]["delegation"],
            expected_supervisor_pid,
        )
        self.assertIsNone(
            result["children"]["longform_companion"]["result"]["delegation"]
        )

    def test_spawn_rejects_delegation_for_longform_companion(self) -> None:
        with self.assertRaisesRegex(
            supervisor.CompanionSupervisorError,
            "restricted to the source controller",
        ):
            supervisor._spawn(
                "longform_companion",
                ["/bin/true"],
                self.root,
                controller_supervisor_pid=os.getpid(),
            )

    def test_child_failure_durably_stops_surviving_peer(self) -> None:
        source_body = json.dumps({"status": "controller_exited"}, separators=(",", ":"))
        source = [
            sys.executable,
            "-c",
            f"import sys,time;time.sleep(.3);sys.stdout.write({source_body!r})",
        ]
        companion = self._python_result(
            {
                "status": "failed",
                "error": {"type": "CampaignError", "message": "fixture failure"},
            },
            returncode=2,
        )
        with (
            mock.patch.object(supervisor, "load_config", return_value=self.controller),
            mock.patch.object(supervisor, "read_companion_status", return_value={}),
            mock.patch.object(supervisor, "_child_argvs", return_value=(source, companion)),
            mock.patch.object(supervisor, "request_stop") as stop,
        ):
            returncode, result = supervisor.supervise_registered_campaign(
                controller_config_path=self.config_path,
                expected_controller_sha256="e" * 64,
                registration=self.registration,
            )
        self.assertEqual(returncode, 2)
        self.assertEqual(result["status"], "supervised_campaign_faulted")
        self.assertTrue(result["stop_coordinated"])
        self.assertEqual(
            result["stop_reason"],
            "longform_companion failed with status 2: CampaignError: fixture failure",
        )
        stop.assert_called_once()

    @unittest.skipUnless(hasattr(signal, "SIGKILL"), "requires POSIX signals")
    def test_signaled_source_without_output_reports_termination_not_bad_json(self) -> None:
        source = [
            sys.executable,
            "-c",
            "import os,signal;os.kill(os.getpid(),signal.SIGKILL)",
        ]
        companion = self._python_result({"status": "stopped"})
        with (
            mock.patch.object(supervisor, "load_config", return_value=self.controller),
            mock.patch.object(supervisor, "read_companion_status", return_value={}),
            mock.patch.object(
                supervisor, "_child_argvs", return_value=(source, companion)
            ),
            mock.patch.object(supervisor, "request_stop"),
        ):
            returncode, result = supervisor.supervise_registered_campaign(
                controller_config_path=self.config_path,
                expected_controller_sha256="e" * 64,
                registration=self.registration,
            )

        source_result = result["children"]["source_controller"]
        self.assertEqual(returncode, 2)
        self.assertEqual(source_result["returncode"], -signal.SIGKILL)
        self.assertIsNone(source_result["result"])
        self.assertIn(
            "terminated by signal 9 before emitting one result object",
            source_result["result_error"],
        )
        self.assertNotIn("not strict JSON", source_result["result_error"])
        self.assertIn(
            "source_controller failed with status -9",
            result["error"]["message"],
        )

    def test_launch_failure_clears_durable_start_intent(self) -> None:
        with (
            mock.patch.object(supervisor, "load_config", return_value=self.controller),
            mock.patch.object(supervisor, "read_companion_status", return_value={}),
            mock.patch.object(supervisor, "_child_argvs", return_value=(["one"], ["two"])),
            mock.patch.object(supervisor, "_spawn", side_effect=OSError("fixture launch")),
            mock.patch.object(supervisor, "request_stop") as stop,
        ):
            returncode, result = supervisor.supervise_registered_campaign(
                controller_config_path=self.config_path,
                expected_controller_sha256="e" * 64,
                registration=self.registration,
            )
        self.assertEqual(returncode, 2)
        self.assertTrue(result["stop_coordinated"])
        stop.assert_called_once()

    def test_request_stop_failure_cannot_orphan_the_surviving_peer(self) -> None:
        source = [sys.executable, "-c", "import time;time.sleep(20)"]
        companion = self._python_result({"status": "failed"}, returncode=2)
        admitted = []
        real_spawn = supervisor._spawn

        def capture_spawn(name, argv, repository_root, **kwargs):
            child = real_spawn(name, argv, repository_root, **kwargs)
            admitted.append(child)
            return child

        with (
            mock.patch.object(supervisor, "load_config", return_value=self.controller),
            mock.patch.object(supervisor, "read_companion_status", return_value={}),
            mock.patch.object(supervisor, "_child_argvs", return_value=(source, companion)),
            mock.patch.object(supervisor, "_spawn", side_effect=capture_spawn),
            mock.patch.object(
                supervisor, "request_stop", side_effect=OSError("fixture control failure")
            ),
            self.assertRaisesRegex(OSError, "fixture control failure"),
        ):
            supervisor.supervise_registered_campaign(
                controller_config_path=self.config_path,
                expected_controller_sha256="e" * 64,
                registration=self.registration,
            )
        self.assertEqual(len(admitted), 2)
        self.assertTrue(all(child.process.poll() is not None for child in admitted))

    def test_no_registration_is_exact_legacy_fallthrough(self) -> None:
        argv = [
            "run",
            "--config",
            str(self.config_path),
            "--expected-config-sha256",
            "e" * 64,
        ]
        with mock.patch.object(
            supervisor, "load_registration_if_present", return_value=None
        ):
            self.assertIsNone(supervisor.maybe_supervise_run(argv))


if __name__ == "__main__":
    unittest.main()
