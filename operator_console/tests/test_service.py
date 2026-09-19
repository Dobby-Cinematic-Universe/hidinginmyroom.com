from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from copy import deepcopy
from pathlib import Path
from unittest import mock

from autonomous_controller.config import ControllerConfig
from autonomous_controller.state import (
    read_control_state,
    request_start as controller_request_start,
    request_stop as controller_request_stop,
)
from operator_console import registry
from operator_console.service import (
    CANCEL_CONFIRMATION,
    DEFAULT_SERVICE_STATE_DIRECTORY,
    GLOBAL_JOB_LIMIT,
    MAX_STREAM_BYTES,
    SYSTEMD_CHILD_MAX_FILE_BYTES,
    OperatorService,
    ServiceError,
    initialize_workspace,
    validate_profiles,
)


class FakeSystemd:
    """Small deterministic systemd-run/systemctl model; it never starts a process."""

    invocation_id = "0123456789abcdef0123456789abcdef"

    def __init__(self, *, hold: bool = False, outcome: str = "success") -> None:
        self.hold = hold
        self.outcome = outcome
        self.calls: list[list[str]] = []
        self.units: dict[str, dict[str, object]] = {}

    def __call__(self, argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        self.calls.append(list(argv))
        if kwargs.get("shell") is not False or kwargs.get("stdin") is not subprocess.DEVNULL:
            raise AssertionError("systemd control invocation was not closed")
        if argv[0] == "/usr/bin/systemd-run":
            unit = next(item.split("=", 1)[1] for item in argv if item.startswith("--unit="))
            stdout = Path(
                next(
                    item.split("append:", 1)[1]
                    for item in argv
                    if item.startswith("--property=StandardOutput=append:")
                )
            )
            stderr = Path(
                next(
                    item.split("append:", 1)[1]
                    for item in argv
                    if item.startswith("--property=StandardError=append:")
                )
            )
            if self.outcome == "timeout":
                stderr.write_text('{"status":"timed_out"}\n', encoding="utf-8")
            else:
                stdout.write_text('{"status":"gpu_fixture_ok"}\n', encoding="utf-8")
            self.units[unit] = {"shows": 0, "state": "running"}
            return subprocess.CompletedProcess(argv, 0, b"", b"")

        if argv[0] != "/usr/bin/systemctl":
            raise AssertionError(f"unexpected executable: {argv!r}")
        operation = "show" if "show" in argv else "stop" if "stop" in argv else None
        unit = argv[-1]
        if operation == "stop":
            if unit in self.units:
                self.units[unit]["state"] = "stopped"
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        if operation != "show":
            raise AssertionError(f"unexpected systemctl operation: {argv!r}")
        row = self.units.get(unit)
        if row is None or row["state"] == "retired":
            return self._show(
                argv,
                load="not-found",
                active="inactive",
                sub="dead",
                result="",
                code="",
                status="",
                invocation="",
                returncode=1,
            )
        row["shows"] = int(row["shows"]) + 1
        state = str(row["state"])
        if state == "stopped":
            return self._show(
                argv,
                load="loaded",
                active="inactive",
                sub="dead",
                result="signal",
                code="2",
                status="15",
            )
        if self.hold or int(row["shows"]) == 1:
            return self._show(
                argv,
                load="loaded",
                active="active",
                sub="running",
                result="",
                code="",
                status="",
            )
        if self.outcome == "timeout":
            return self._show(
                argv,
                load="loaded",
                active="failed",
                sub="failed",
                result="timeout",
                code="2",
                status="15",
            )
        return self._show(
            argv,
            load="loaded",
            active="active",
            sub="exited",
            result="success",
            code="1",
            status="0",
        )

    def _show(
        self,
        argv: list[str],
        *,
        load: str,
        active: str,
        sub: str,
        result: str,
        code: str,
        status: str,
        invocation: str | None = None,
        returncode: int = 0,
    ) -> subprocess.CompletedProcess[bytes]:
        if invocation is None:
            invocation = self.invocation_id
        body = (
            f"LoadState={load}\n"
            f"ActiveState={active}\n"
            f"SubState={sub}\n"
            f"Result={result}\n"
            f"ExecMainCode={code}\n"
            f"ExecMainStatus={status}\n"
            f"InvocationID={invocation}\n"
        ).encode("utf-8")
        return subprocess.CompletedProcess(argv, returncode, body, b"")


class OperatorServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-operator-service-")
        self.repo = Path(self.temporary.name).resolve()
        (self.repo / "research").mkdir()
        self.workspace = self.repo / "research" / "console"
        self.workspace.mkdir(mode=0o700)
        self.profile_path = self.workspace / "profiles.json"
        self.state_root = self.workspace / "state"
        self.fake = self.repo / "research" / "fake-tool"
        self.fake.write_text(
            """#!/usr/bin/python3
import json
import os
import sys
import time

mode = sys.argv[1]
if mode == "sleep":
    time.sleep(float(sys.argv[2]))
    value = {"status": "slept", "secret_present": "HIMR_SECRET_SENTINEL" in os.environ}
elif mode == "fail":
    sys.stderr.write(json.dumps({"status": "failed_fixture"}))
    raise SystemExit(2)
elif mode == "large":
    sys.stdout.write(json.dumps({"payload": "x" * int(sys.argv[2])}))
    raise SystemExit(0)
elif mode == "umask":
    with open(sys.argv[2], "w", encoding="utf-8") as output:
        output.write("fixture")
    value = {"status": "created", "mode": oct(os.stat(sys.argv[2]).st_mode & 0o777)}
else:
    value = {"status": "ok", "secret_present": "HIMR_SECRET_SENTINEL" in os.environ}
if mode in {"sleep", "success", "umask"}:
    sys.stdout.write(json.dumps(value))
""",
            encoding="utf-8",
        )
        self.fake.chmod(0o700)
        relative = str(self.fake.relative_to(self.repo))
        self.actions = {
            "preprocess.status": registry.ActionSpec(
                action_id="preprocess.status",
                stage="Fixture preprocess",
                label="Fixture preprocess",
                description="Finite fake fixture.",
                effect="inspect",
                resource="preprocess",
                launcher="repo",
                entrypoint=relative,
                prefix=("success",),
                fields=(),
                confirmation=None,
                timeout_seconds=5,
            ),
            "acquisition.validate": registry.ActionSpec(
                action_id="acquisition.validate",
                stage="Fixture acquisition",
                label="Fixture acquisition",
                description="Finite fake fixture.",
                effect="inspect",
                resource="network",
                launcher="repo",
                entrypoint=relative,
                prefix=("sleep", "0.25"),
                fields=(),
                confirmation=None,
                timeout_seconds=5,
            ),
        }
        self.action_patch = mock.patch.dict(registry.ACTIONS, self.actions, clear=False)
        self.action_patch.start()
        self.service: OperatorService | None = None

    def tearDown(self) -> None:
        if self.service is not None:
            self.service.wait_for_jobs(timeout=3)
            self.service.close()
        self.action_patch.stop()
        self.temporary.cleanup()

    def write_profiles(self, rows: list[dict[str, object]]) -> None:
        body = (
            json.dumps(
                {"schema_version": 1, "profiles": rows},
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        if self.profile_path.exists():
            self.profile_path.chmod(0o600)
        self.profile_path.write_text(body, encoding="utf-8")
        self.profile_path.chmod(0o400)

    @staticmethod
    def profile(profile_id: str, action: str) -> dict[str, object]:
        return {
            "id": profile_id,
            "label": profile_id,
            "description": "Finite fake profile.",
            "action": action,
            "parameters": {},
        }

    def open_service(self, rows: list[dict[str, object]]) -> OperatorService:
        self.write_profiles(rows)
        self.service = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        return self.service

    def execute_profile(self, service: OperatorService, profile_id: str) -> str:
        prepared = service.prepare(
            profile_id=profile_id, expected_revision=service.revision
        )
        job = service.execute(
            preparation_token=prepared["preparation_token"],
            expected_revision=service.revision,
            confirmation=None,
        )
        return job["job_id"]

    def systemd_action(self) -> registry.ActionSpec:
        return registry.ActionSpec(
            action_id="gpu.vnext.fixture",
            stage="GPU fixture",
            label="GPU fixture",
            description="Finite fake systemd fixture.",
            effect="execute",
            resource="gpu",
            launcher="repo",
            entrypoint=str(self.fake.relative_to(self.repo)),
            prefix=("success",),
            fields=(),
            confirmation="RUN GPU FIXTURE",
            timeout_seconds=900,
            supervisor="systemd_user",
            memory_max_bytes=4 * 1024 * 1024 * 1024,
        )

    def autonomy_fixture(self) -> tuple[dict[str, object], ControllerConfig]:
        entrypoint = (
            self.repo
            / "autonomous_controller"
            / "bin"
            / "himr-autonomous-controller"
        )
        entrypoint.parent.mkdir(parents=True)
        entrypoint.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        entrypoint.chmod(0o700)
        config_path = self.repo / "research" / "autonomous-controller.json"
        config_path.write_text("{}\n", encoding="utf-8")
        config_path.chmod(0o400)
        controller_state = self.repo / "research" / "autonomous-controller-state"
        controller_state.mkdir(mode=0o700)
        digest = "ab" * 32
        controller_config = ControllerConfig(
            {
                "config_id": "himrautocfg_" + "1" * 32,
                "state_root": str(controller_state),
            },
            config_path,
            digest,
        )
        return (
            {
                "id": "autonomy.start",
                "label": "Start",
                "description": "Start the sealed autonomous campaign.",
                "action": "autonomy.run",
                "parameters": {
                    "config": str(config_path),
                    "expected_config_sha256": digest,
                },
            },
            controller_config,
        )

    @staticmethod
    def execute_systemd_profile(service: OperatorService, profile_id: str) -> str:
        prepared = service.prepare(
            profile_id=profile_id, expected_revision=service.revision
        )
        job = service.execute(
            preparation_token=prepared["preparation_token"],
            expected_revision=service.revision,
            confirmation="RUN GPU FIXTURE",
        )
        return job["job_id"]

    @staticmethod
    def wait_for_state(
        service: OperatorService, job_id: str, states: set[str], timeout: float = 3
    ) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = service.jobs[job_id]["state"]
            if state in states:
                return state
            time.sleep(0.01)
        raise AssertionError(
            f"job {job_id} did not reach {sorted(states)}; got {service.jobs[job_id]['state']}"
        )

    def test_init_is_private_and_never_overwrites_profiles(self) -> None:
        target = self.repo / "research" / "initialized"
        result = initialize_workspace(repo_root=self.repo, workspace=target)
        self.assertEqual(result["status"], "initialized")
        self.assertEqual(stat_mode(target), 0o700)
        self.assertEqual(
            result["state_root"], str(target / DEFAULT_SERVICE_STATE_DIRECTORY)
        )
        self.assertEqual(
            stat_mode(target / DEFAULT_SERVICE_STATE_DIRECTORY), 0o700
        )
        self.assertEqual(stat_mode(target / "profiles.json"), 0o400)
        with self.assertRaisesRegex(ServiceError, "never overwrites"):
            initialize_workspace(repo_root=self.repo, workspace=target)

    def test_init_keeps_pipeline_state_separate_from_console_state(self) -> None:
        target = self.repo / "research" / "separated"
        pipeline_state = target / "state" / "gpu-local"
        pipeline_state.mkdir(parents=True, mode=0o700)
        os.chmod(target, 0o700)
        os.chmod(target / "state", 0o700)
        marker = pipeline_state / "existing-asset"
        marker.write_bytes(b"pipeline asset\n")

        result = initialize_workspace(repo_root=self.repo, workspace=target)

        self.assertEqual(
            result["state_root"], str(target / DEFAULT_SERVICE_STATE_DIRECTORY)
        )
        self.assertEqual(marker.read_bytes(), b"pipeline asset\n")
        self.assertEqual(
            stat_mode(target / DEFAULT_SERVICE_STATE_DIRECTORY), 0o700
        )
        self.assertEqual(
            stat_mode(target / DEFAULT_SERVICE_STATE_DIRECTORY / "jobs"), 0o700
        )

    def test_validate_profiles_is_read_only_and_prepares_every_entrypoint(self) -> None:
        self.write_profiles([self.profile("fixture.success", "preprocess.status")])
        before = self.profile_path.read_bytes()
        result = validate_profiles(repo_root=self.repo, profile_path=self.profile_path)
        self.assertEqual(result["status"], "validated")
        self.assertEqual(result["profile_count"], 1)
        self.assertFalse(result["subprocess_executed"])
        self.assertFalse(result["files_written"])
        self.assertEqual(self.profile_path.read_bytes(), before)

    def test_finite_job_has_sanitized_environment_private_state_and_chunked_logs(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        with mock.patch.dict(
            os.environ,
            {"HIMR_SECRET_SENTINEL": "must-not-leak", "DISCORD_TOKEN": "no"},
            clear=False,
        ):
            job_id = self.execute_profile(service, "fixture.success")
            self.assertTrue(service.wait_for_jobs(timeout=3))
        state = service.public_state(csrf_token="csrf", prefix="/o/test/")
        job = next(row for row in state["jobs"] if row["job_id"] == job_id)
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["returncode"], 0)
        self.assertEqual(job["summary"], {"secret_present": False, "status": "ok"})
        self.assertFalse(job["cancellation_supported"])
        self.assertNotIn("pid", job)
        self.assertNotIn("command", job)
        self.assertTrue(job["logs"]["stdout"]["base_url"].endswith("/stdout/"))
        first = service.read_log_chunk(job_id, "stdout", 0)
        self.assertEqual(first["stream"], "stdout")
        self.assertTrue(first["eof"])
        self.assertIn('"status": "ok"', first["text"])
        self.assertEqual(first["next_offset"], len(first["text"].encode()))
        with self.assertRaisesRegex(ServiceError, "beyond captured"):
            service.read_log_chunk(job_id, "stdout", first["next_offset"] + 1)

        directory = self.state_root / "jobs" / job_id
        self.assertEqual(stat_mode(self.state_root), 0o700)
        self.assertEqual(stat_mode(directory), 0o700)
        self.assertEqual(stat_mode(directory / "record.json"), 0o600)
        self.assertEqual(stat_mode(directory / "stdout.log"), 0o600)

    def test_preparation_is_one_use_and_entrypoint_drift_blocks_launch(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        prepared = service.prepare(
            profile_id="fixture.success", expected_revision=service.revision
        )
        self.fake.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        self.fake.chmod(0o700)
        with self.assertRaisesRegex(ServiceError, "changed"):
            service.execute(
                preparation_token=prepared["preparation_token"],
                expected_revision=service.revision,
                confirmation=None,
            )
        self.assertEqual(service.jobs, {})
        with self.assertRaisesRegex(ServiceError, "already consumed"):
            service.execute(
                preparation_token=prepared["preparation_token"],
                expected_revision=service.revision,
                confirmation=None,
            )

    def test_resource_groups_allow_network_preprocess_overlap_but_not_duplicates(self) -> None:
        sleeping_preprocess = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "prefix": ("sleep", "0.25"),
            }
        )
        with mock.patch.dict(
            registry.ACTIONS,
            {"preprocess.status": sleeping_preprocess},
            clear=False,
        ):
            service = self.open_service(
                [
                    self.profile("preprocess.one", "preprocess.status"),
                    self.profile("preprocess.two", "preprocess.status"),
                    self.profile("network.one", "acquisition.validate"),
                ]
            )
            self.execute_profile(service, "preprocess.one")
            prepared = service.prepare(
                profile_id="preprocess.two", expected_revision=service.revision
            )
            with self.assertRaisesRegex(ServiceError, "already has an active job"):
                service.execute(
                    preparation_token=prepared["preparation_token"],
                    expected_revision=service.revision,
                    confirmation=None,
                )
            self.execute_profile(service, "network.one")
            state = service.public_state(csrf_token="csrf", prefix="/o/test/")
            capacity = state["capacity"]
            self.assertEqual(capacity["active_count"], 2)
            self.assertEqual(
                "operator_console_admission_reservations", capacity["kind"]
            )
            self.assertEqual(
                "launch_admission_not_runtime_utilization",
                capacity["semantics"],
            )
            self.assertEqual(
                {
                    "limit": GLOBAL_JOB_LIMIT,
                    "active_job_count": 2,
                    "available_slot_count": GLOBAL_JOB_LIMIT - 2,
                },
                capacity["global"],
            )
            archive_reservation = capacity["resources"]["archive_pipeline"]
            self.assertEqual(2, archive_reservation["active_count"])
            self.assertEqual(2, archive_reservation["conflicting_job_count"])
            self.assertTrue(archive_reservation["blocked"])
            self.assertEqual(
                ["network", "preprocess"], archive_reservation["claim_ids"]
            )
            cpu_reservation = capacity["resources"]["cpu_asr"]
            self.assertEqual(0, cpu_reservation["active_count"])
            self.assertEqual(0, cpu_reservation["conflicting_job_count"])
            self.assertFalse(cpu_reservation["blocked"])
            self.assertTrue(service.wait_for_jobs(timeout=3))

    def test_rolling_archive_resource_exclusively_claims_both_stage_lanes(self) -> None:
        sleeping_preprocess = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "prefix": ("sleep", "0.25"),
            }
        )
        rolling = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "action_id": "archive.rolling_pipeline",
                "resource": "archive_pipeline",
                "prefix": ("sleep", "0.25"),
            }
        )
        with mock.patch.dict(
            registry.ACTIONS,
            {
                "preprocess.status": sleeping_preprocess,
                "archive.rolling_pipeline": rolling,
            },
            clear=False,
        ):
            service = self.open_service(
                [
                    self.profile("rolling.one", "archive.rolling_pipeline"),
                    self.profile("network.one", "acquisition.validate"),
                    self.profile("preprocess.one", "preprocess.status"),
                ]
            )
            self.execute_profile(service, "rolling.one")
            capacity = service.public_state(
                csrf_token="csrf", prefix="/o/test/"
            )["capacity"]["resources"]
            self.assertEqual(1, capacity["archive_pipeline"]["active_count"])
            self.assertEqual(1, capacity["network"]["active_count"])
            self.assertEqual(1, capacity["preprocess"]["active_count"])
            for profile_id in ("network.one", "preprocess.one"):
                prepared = service.prepare(
                    profile_id=profile_id, expected_revision=service.revision
                )
                with self.assertRaisesRegex(
                    ServiceError, "already has an active job conflict"
                ):
                    service.execute(
                        preparation_token=prepared["preparation_token"],
                        expected_revision=service.revision,
                        confirmation=None,
                    )
            self.assertTrue(service.wait_for_jobs(timeout=3))

            self.execute_profile(service, "network.one")
            self.execute_profile(service, "preprocess.one")
            prepared = service.prepare(
                profile_id="rolling.one", expected_revision=service.revision
            )
            with self.assertRaisesRegex(
                ServiceError, "already has an active job conflict"
            ):
                service.execute(
                    preparation_token=prepared["preparation_token"],
                    expected_revision=service.revision,
                    confirmation=None,
                )
            self.assertTrue(service.wait_for_jobs(timeout=3))

    def test_cold_retention_claims_network_but_allows_preprocess(self) -> None:
        cold = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "action_id": "retention.public_acquisition",
                "resource": "cold_storage",
                "prefix": ("sleep", "0.25"),
            }
        )
        with mock.patch.dict(
            registry.ACTIONS,
            {"retention.public_acquisition": cold},
            clear=False,
        ):
            service = self.open_service(
                [
                    self.profile("cold.one", "retention.public_acquisition"),
                    self.profile("network.one", "acquisition.validate"),
                    self.profile("preprocess.one", "preprocess.status"),
                ]
            )
            self.execute_profile(service, "cold.one")
            capacity = service.public_state(
                csrf_token="csrf", prefix="/o/test/"
            )["capacity"]["resources"]
            self.assertEqual(1, capacity["cold_storage"]["active_count"])
            self.assertEqual(1, capacity["network"]["active_count"])
            self.assertEqual(0, capacity["preprocess"]["active_count"])
            prepared = service.prepare(
                profile_id="network.one", expected_revision=service.revision
            )
            with self.assertRaisesRegex(
                ServiceError, "already has an active job conflict"
            ):
                service.execute(
                    preparation_token=prepared["preparation_token"],
                    expected_revision=service.revision,
                    confirmation=None,
                )
            self.execute_profile(service, "preprocess.one")
            self.assertTrue(service.wait_for_jobs(timeout=3))

    def test_cancel_is_fail_closed_and_does_not_signal_job(self) -> None:
        service = self.open_service(
            [self.profile("network.one", "acquisition.validate")]
        )
        job_id = self.execute_profile(service, "network.one")
        with self.assertRaisesRegex(ServiceError, "Cancellation is disabled") as raised:
            service.cancel(job_id=job_id, expected_revision=service.revision)
        self.assertEqual(raised.exception.status, 409)
        self.assertTrue(service.wait_for_jobs(timeout=3))
        self.assertEqual(service.jobs[job_id]["state"], "succeeded")

    def test_systemd_job_uses_fixed_cgroup_envelope_and_sealed_logs(self) -> None:
        action = self.systemd_action()
        fake_systemd = FakeSystemd()
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            service = self.open_service(
                [self.profile("gpu.fixture", action.action_id)]
            )
            with mock.patch.dict(
                os.environ,
                {"HIMR_SECRET_SENTINEL": "never-forward", "DISCORD_TOKEN": "never"},
                clear=False,
            ):
                job_id = self.execute_systemd_profile(service, "gpu.fixture")
                self.assertTrue(service.wait_for_jobs(timeout=5))

        record = service.jobs[job_id]
        self.assertEqual(record["state"], "succeeded")
        self.assertEqual(record["summary"], {"status": "gpu_fixture_ok"})
        self.assertIsNone(record["pid"])
        self.assertEqual(record["supervisor"], "systemd_user")
        self.assertEqual(record["unit"]["invocation_id"], fake_systemd.invocation_id)
        launch = next(call for call in fake_systemd.calls if call[0] == "/usr/bin/systemd-run")
        expected_unit = f"--unit=himr-operator-job-{job_id[4:]}.service"
        self.assertIn(expected_unit, launch)
        self.assertIn("--property=Type=exec", launch)
        self.assertIn("--property=ExitType=cgroup", launch)
        self.assertIn("--property=Restart=no", launch)
        self.assertIn("--property=KillMode=control-group", launch)
        self.assertIn("--property=RuntimeMaxSec=900s", launch)
        self.assertIn("--property=TimeoutStopSec=15s", launch)
        self.assertIn("--property=TasksMax=64", launch)
        self.assertIn("--property=LimitNOFILE=1024", launch)
        self.assertIn(
            f"--property=LimitFSIZE={SYSTEMD_CHILD_MAX_FILE_BYTES}", launch
        )
        self.assertIn("--property=LimitCORE=0", launch)
        self.assertIn("--property=MemorySwapMax=0", launch)
        self.assertIn("--property=MemoryMax=4294967296", launch)
        boundary = launch.index("--")
        self.assertEqual(launch[boundary + 1 : boundary + 3], ["/usr/bin/env", "-i"])
        self.assertFalse(any("HIMR_SECRET_SENTINEL" in item for item in launch))
        self.assertFalse(any("DISCORD_TOKEN" in item for item in launch))
        self.assertTrue(all(call[0] in {"/usr/bin/systemd-run", "/usr/bin/systemctl"} for call in fake_systemd.calls))

    def test_autonomy_start_arms_before_systemd_and_later_stop_wins(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        fake_systemd = FakeSystemd()
        observed: list[tuple[str, str]] = []
        systemd_observed = threading.Event()

        def arm(config: ControllerConfig) -> dict[str, object]:
            observed.append(("arm", read_control_state(config)["desired_state"]))
            return controller_request_start(config)

        def systemd(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            if argv[0] == "/usr/bin/systemd-run":
                observed.append(
                    ("systemd", read_control_state(controller_config)["desired_state"])
                )
                systemd_observed.set()
            return fake_systemd(argv, **kwargs)

        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.request_start", side_effect=arm
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=systemd
        ):
            service = self.open_service([profile])
            prepared = service.prepare(
                profile_id="autonomy.start", expected_revision=service.revision
            )
            job = service.execute(
                preparation_token=prepared["preparation_token"],
                expected_revision=service.revision,
                confirmation=None,
            )
            self.assertTrue(systemd_observed.wait(timeout=3))
            stopped = controller_request_stop(controller_config)
            self.assertTrue(service.wait_for_jobs(timeout=5))

        self.assertEqual(observed[:2], [("arm", "stopped"), ("systemd", "running")])
        self.assertEqual(stopped["generation"], 2)
        self.assertEqual(stopped["desired_state"], "stopped")
        self.assertEqual(read_control_state(controller_config), stopped)
        self.assertEqual(service.jobs[job["job_id"]]["state"], "succeeded")

    def test_terminal_managed_autonomy_job_reconciles_stale_running_cache(self) -> None:
        profile, _controller_config = self.autonomy_fixture()
        service = self.open_service([profile])
        command = service._startup_commands["autonomy.start"]
        job_id = "job_" + "a" * 32
        record = service._new_job_record(command, job_id)
        record.update(
            state="failed",
            created_at="2026-08-31T04:00:00Z",
            started_at="2026-08-31T04:00:01Z",
            completed_at="2026-08-31T04:52:40Z",
            returncode=2,
            error="subprocess exited with status 2",
        )
        record["unit"]["invocation_id"] = FakeSystemd.invocation_id
        service.jobs[job_id] = record
        cached = {
            "lifecycle": "running",
            "actual_state": "running",
            "desired_state": "stopped",
            "running": True,
            "can_start": False,
            "can_stop": False,
            "controls": {
                "can_start": False,
                "can_stop": False,
                "control_generation": 27,
            },
            "control_generation": 27,
            "updated_at": "2026-08-31T04:26:39Z",
            "last_error": {"type": "BackendError", "message": "fixture race"},
        }

        with mock.patch(
            "autonomous_controller.public_status.read_public_status",
            return_value=cached,
        ):
            autonomy = service.public_state(
                csrf_token="csrf", prefix="/o/test/"
            )["autonomy"]

        self.assertEqual("faulted", autonomy["actual_state"])
        self.assertEqual("faulted", autonomy["lifecycle"])
        self.assertFalse(autonomy["running"])
        self.assertTrue(autonomy["can_start"])
        self.assertFalse(autonomy["can_stop"])
        self.assertTrue(autonomy["controls"]["can_start"])
        self.assertEqual(cached["last_error"], autonomy["last_error"])
        self.assertEqual(
            {
                "kind": "managed_systemd_terminal_over_stale_controller_cache",
                "job_id": job_id,
                "job_state": "failed",
                "returncode": 2,
                "completed_at": "2026-08-31T04:52:40Z",
                "cached_actual_state": "running",
                "cached_lifecycle": "running",
                "cached_updated_at": "2026-08-31T04:26:39Z",
            },
            autonomy["terminal_job_reconciliation"],
        )

    def test_terminal_autonomy_reconciliation_fails_closed_without_exact_proof(self) -> None:
        profile, _controller_config = self.autonomy_fixture()
        service = self.open_service([profile])
        command = service._startup_commands["autonomy.start"]
        job_id = "job_" + "b" * 32
        base_record = service._new_job_record(command, job_id)
        base_record.update(
            state="failed",
            created_at="2026-08-31T04:00:00Z",
            started_at="2026-08-31T04:00:01Z",
            completed_at="2026-08-31T04:52:40Z",
            returncode=2,
            error="subprocess exited with status 2",
        )
        base_record["unit"]["invocation_id"] = FakeSystemd.invocation_id
        base_status = {
            "lifecycle": "running",
            "actual_state": "running",
            "desired_state": "stopped",
            "running": True,
            "can_start": False,
            "can_stop": False,
            "controls": {
                "can_start": False,
                "can_stop": False,
                "control_generation": 9,
            },
            "updated_at": "2026-08-31T04:26:39Z",
        }
        cases = {
            "active-job": lambda record, _status: record.update(
                state="running", completed_at=None, returncode=None
            ),
            "indeterminate-job": lambda record, _status: record.update(
                state="indeterminate_after_restart", returncode=None
            ),
            "unbound-unit": lambda record, _status: record["unit"].update(
                invocation_id=None
            ),
            "status-after-job": lambda _record, status: status.update(
                updated_at="2026-08-31T04:52:41Z"
            ),
            "status-before-job": lambda _record, status: status.update(
                updated_at="2026-08-31T04:00:00Z"
            ),
            "created-after-start": lambda record, _status: record.update(
                created_at="2026-08-31T04:00:02Z"
            ),
            "command-drift": lambda record, _status: record["command"]["argv"].append(
                "--drift"
            ),
            "running-intent": lambda _record, status: status.update(
                desired_state="running"
            ),
        }
        for label, mutate in cases.items():
            with self.subTest(label=label):
                record = deepcopy(base_record)
                status = deepcopy(base_status)
                mutate(record, status)
                service.jobs = {job_id: record}
                observed = service._reconcile_terminal_autonomy_status_locked(
                    status, command
                )
                self.assertEqual("running", observed["actual_state"])
                self.assertFalse(observed["can_start"])
                self.assertNotIn("terminal_job_reconciliation", observed)

        conflict_id = "job_" + "c" * 32
        service.jobs = {
            job_id: deepcopy(base_record),
            conflict_id: {
                "action_id": "acquisition.run",
                "resource": "network",
                "state": "running",
            },
        }
        observed = service._reconcile_terminal_autonomy_status_locked(
            deepcopy(base_status), command
        )
        self.assertEqual("running", observed["actual_state"])
        self.assertFalse(observed["can_start"])
        self.assertNotIn("terminal_job_reconciliation", observed)

    def test_autonomy_stop_after_arm_before_unit_observation_remains_stopped(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        fake_systemd = FakeSystemd()
        systemd_entered = threading.Event()
        allow_systemd = threading.Event()
        observed: list[str] = []

        def systemd(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            if argv[0] == "/usr/bin/systemd-run":
                systemd_entered.set()
                self.assertTrue(allow_systemd.wait(timeout=3))
                observed.append(read_control_state(controller_config)["desired_state"])
            return fake_systemd(argv, **kwargs)

        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=systemd
        ):
            service = self.open_service([profile])
            prepared = service.prepare(
                profile_id="autonomy.start", expected_revision=service.revision
            )
            job = service.execute(
                preparation_token=prepared["preparation_token"],
                expected_revision=service.revision,
                confirmation=None,
            )
            self.assertTrue(systemd_entered.wait(timeout=3))
            stopped = controller_request_stop(controller_config)
            allow_systemd.set()
            self.assertTrue(service.wait_for_jobs(timeout=5))

        self.assertEqual(observed, ["stopped"])
        self.assertEqual(stopped["generation"], 2)
        self.assertEqual(read_control_state(controller_config), stopped)
        self.assertEqual(service.jobs[job["job_id"]]["state"], "succeeded")

    def test_autonomy_admission_rejection_restores_stop_before_job_finishes(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        observed: list[str] = []

        def arm(config: ControllerConfig) -> dict[str, object]:
            observed.append("arm")
            return controller_request_start(config)

        def disarm(config: ControllerConfig) -> dict[str, object]:
            observed.append("disarm")
            return controller_request_stop(config)

        def reject(
            argv: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[bytes]:
            self.assertEqual(argv[0], "/usr/bin/systemd-run")
            observed.append("systemd-rejected")
            return subprocess.CompletedProcess(argv, 1, b"", b"rejected")

        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.request_start", side_effect=arm
        ), mock.patch(
            "operator_console.service.request_stop", side_effect=disarm
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=reject
        ):
            service = self.open_service([profile])
            prepared = service.prepare(
                profile_id="autonomy.start", expected_revision=service.revision
            )
            job = service.execute(
                preparation_token=prepared["preparation_token"],
                expected_revision=service.revision,
                confirmation=None,
            )
            self.assertTrue(service.wait_for_jobs(timeout=5))

        self.assertEqual(observed, ["arm", "systemd-rejected", "disarm"])
        control = read_control_state(controller_config)
        self.assertEqual(control["generation"], 2)
        self.assertEqual(control["desired_state"], "stopped")
        self.assertEqual(service.jobs[job["job_id"]]["state"], "failed")

    def test_autonomy_arm_failure_launches_no_unit_and_records_failure(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        systemd_calls: list[list[str]] = []

        def forbidden_systemd(
            argv: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[bytes]:
            systemd_calls.append(argv)
            raise AssertionError("systemd must not run after a failed Start arm")

        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.request_start",
            side_effect=RuntimeError("arm failed"),
        ), mock.patch(
            "operator_console.service.request_stop",
            side_effect=controller_request_stop,
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=forbidden_systemd
        ):
            service = self.open_service([profile])
            prepared = service.prepare(
                profile_id="autonomy.start", expected_revision=service.revision
            )
            with self.assertRaisesRegex(ServiceError, "not safely committed"):
                service.execute(
                    preparation_token=prepared["preparation_token"],
                    expected_revision=service.revision,
                    confirmation=None,
                )

        self.assertEqual(systemd_calls, [])
        self.assertEqual(len(service.jobs), 1)
        record = next(iter(service.jobs.values()))
        self.assertEqual(record["state"], "failed")
        self.assertIn("not safely committed", record["error"])
        self.assertEqual(
            read_control_state(controller_config)["desired_state"], "stopped"
        )

    def test_autonomy_record_commit_failure_happens_before_start_arm(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        service = self.open_service([profile])
        prepared = service.prepare(
            profile_id="autonomy.start", expected_revision=service.revision
        )
        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.request_start"
        ) as arm, mock.patch.object(
            service, "_commit_job_locked", side_effect=OSError("commit failed")
        ):
            with self.assertRaisesRegex(OSError, "commit failed"):
                service.execute(
                    preparation_token=prepared["preparation_token"],
                    expected_revision=service.revision,
                    confirmation=None,
                )

        arm.assert_not_called()
        self.assertEqual(service.jobs, {})
        self.assertEqual(list(service.jobs_root.iterdir()), [])
        self.assertEqual(
            read_control_state(controller_config)["desired_state"], "stopped"
        )

    def test_autonomy_thread_start_failure_disarms_and_records_failure(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.threading.Thread.start",
            side_effect=RuntimeError("thread start failed"),
        ):
            service = self.open_service([profile])
            prepared = service.prepare(
                profile_id="autonomy.start", expected_revision=service.revision
            )
            with self.assertRaisesRegex(ServiceError, "thread start failed"):
                service.execute(
                    preparation_token=prepared["preparation_token"],
                    expected_revision=service.revision,
                    confirmation=None,
                )

        control = read_control_state(controller_config)
        self.assertEqual(control["generation"], 2)
        self.assertEqual(control["desired_state"], "stopped")
        self.assertEqual(len(service.jobs), 1)
        self.assertEqual(next(iter(service.jobs.values()))["state"], "failed")

    def test_autonomy_restart_with_armed_absent_unit_disarms_and_releases(self) -> None:
        profile, controller_config = self.autonomy_fixture()
        fake_systemd = FakeSystemd()
        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.threading.Thread.start", return_value=None
        ):
            first = self.open_service([profile])
            prepared = first.prepare(
                profile_id="autonomy.start", expected_revision=first.revision
            )
            job = first.execute(
                preparation_token=prepared["preparation_token"],
                expected_revision=first.revision,
                confirmation=None,
            )
        self.assertEqual(
            read_control_state(controller_config)["desired_state"], "running"
        )
        first.close()
        self.service = None

        with mock.patch(
            "operator_console.service.load_config", return_value=controller_config
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            second = OperatorService(
                repo_root=self.repo,
                profile_path=self.profile_path,
                state_root=self.state_root,
            )
            self.service = second
            self.assertTrue(second.wait_for_jobs(timeout=5))

        control = read_control_state(controller_config)
        self.assertEqual(control["generation"], 2)
        self.assertEqual(control["desired_state"], "stopped")
        self.assertEqual(
            second.jobs[job["job_id"]]["state"], "indeterminate_after_restart"
        )
        self.assertEqual(
            second.public_state(csrf_token="csrf", prefix="/o/test/")["capacity"][
                "active_count"
            ],
            0,
        )

    def test_thirty_day_autonomy_record_reloads_with_large_file_envelope(self) -> None:
        action = registry.ActionSpec(
            **{
                **self.systemd_action().__dict__,
                "action_id": "autonomy.fixture",
                "resource": "autonomous_pipeline",
                "timeout_seconds": registry.MAX_ACTION_TIMEOUT_SECONDS,
                "file_size_max_bytes": 64 * 1024**3,
                "tasks_max": 256,
                "stop_timeout_seconds": 30,
                "memory_swap_max_bytes": 0,
            }
        )
        fake_systemd = FakeSystemd(hold=True)
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            first = self.open_service(
                [self.profile("autonomy.fixture", action.action_id)]
            )
            job_id = self.execute_systemd_profile(first, "autonomy.fixture")
            self.wait_for_state(first, job_id, {"running"})
            launch = next(
                call for call in fake_systemd.calls
                if call[0] == "/usr/bin/systemd-run"
            )
            self.assertIn(
                f"--property=RuntimeMaxSec={registry.MAX_ACTION_TIMEOUT_SECONDS}s",
                launch,
            )
            self.assertIn(f"--property=LimitFSIZE={64 * 1024**3}", launch)
            self.assertIn("--property=TasksMax=256", launch)
            first.close()
            self.assertTrue(first.wait_for_jobs(timeout=3))
            self.service = None

            second = OperatorService(
                repo_root=self.repo,
                profile_path=self.profile_path,
                state_root=self.state_root,
            )
            self.service = second
            self.assertEqual(
                second.jobs[job_id]["timeout_seconds"],
                registry.MAX_ACTION_TIMEOUT_SECONDS,
            )
            self.assertIn(
                second.jobs[job_id]["state"],
                {"running", "detached_running", "reconciling"},
            )

    def test_systemd_cancellation_requires_phrase_and_stops_only_derived_unit(self) -> None:
        action = self.systemd_action()
        fake_systemd = FakeSystemd(hold=True)
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            service = self.open_service(
                [self.profile("gpu.fixture", action.action_id)]
            )
            job_id = self.execute_systemd_profile(service, "gpu.fixture")
            self.wait_for_state(service, job_id, {"running"})
            with self.assertRaisesRegex(ServiceError, "confirmation"):
                service.cancel(
                    job_id=job_id,
                    expected_revision=service.revision,
                    confirmation="cancel",
                )
            self.assertFalse(any("stop" in call for call in fake_systemd.calls))
            service.cancel(
                job_id=job_id,
                expected_revision=service.revision,
                confirmation=CANCEL_CONFIRMATION,
            )
            self.assertTrue(service.wait_for_jobs(timeout=5))

        record = service.jobs[job_id]
        self.assertEqual(record["state"], "cancelled_reconciliation_required")
        self.assertIsNotNone(record["cancellation_requested_at"])
        stop_calls = [call for call in fake_systemd.calls if "stop" in call]
        self.assertEqual(
            stop_calls[0],
            [
                "/usr/bin/systemctl",
                "--user",
                "--no-ask-password",
                "stop",
                f"himr-operator-job-{job_id[4:]}.service",
            ],
        )
        self.assertIsNone(record["pid"])

    def test_systemd_restart_reconciles_same_invocation_to_completion(self) -> None:
        action = self.systemd_action()
        fake_systemd = FakeSystemd(hold=True)
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            first = self.open_service(
                [self.profile("gpu.fixture", action.action_id)]
            )
            job_id = self.execute_systemd_profile(first, "gpu.fixture")
            self.wait_for_state(first, job_id, {"running"})
            first.close()
            self.assertTrue(first.wait_for_jobs(timeout=3))
            self.service = None

            fake_systemd.hold = False
            second = OperatorService(
                repo_root=self.repo,
                profile_path=self.profile_path,
                state_root=self.state_root,
            )
            self.service = second
            self.assertTrue(second.wait_for_jobs(timeout=5))
            self.assertEqual(second.jobs[job_id]["state"], "succeeded")
            self.assertEqual(
                second.jobs[job_id]["unit"]["invocation_id"],
                fake_systemd.invocation_id,
            )

    def test_systemd_runtime_timeout_is_failed_and_deadline_exceeded(self) -> None:
        action = self.systemd_action()
        fake_systemd = FakeSystemd(outcome="timeout")
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            service = self.open_service(
                [self.profile("gpu.fixture", action.action_id)]
            )
            job_id = self.execute_systemd_profile(service, "gpu.fixture")
            self.assertTrue(service.wait_for_jobs(timeout=5))
        self.assertEqual(service.jobs[job_id]["state"], "failed")
        self.assertTrue(service.jobs[job_id]["deadline_exceeded"])
        self.assertEqual(service.jobs[job_id]["returncode"], -15)

    def test_systemd_invocation_change_refuses_cancellation_without_stop(self) -> None:
        action = self.systemd_action()
        fake_systemd = FakeSystemd(hold=True)
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            service = self.open_service(
                [self.profile("gpu.fixture", action.action_id)]
            )
            job_id = self.execute_systemd_profile(service, "gpu.fixture")
            self.wait_for_state(service, job_id, {"running"})
            fake_systemd.invocation_id = "fedcba9876543210fedcba9876543210"
            with self.assertRaisesRegex(ServiceError, "identity changed"):
                service.cancel(
                    job_id=job_id,
                    expected_revision=service.revision,
                    confirmation=CANCEL_CONFIRMATION,
                )
            self.assertFalse(any("stop" in call for call in fake_systemd.calls))
            self.assertEqual(service.jobs[job_id]["state"], "reconciling")
            self.assertIn("InvocationID changed", service.jobs[job_id]["error"])
            service.close()
            self.assertTrue(service.wait_for_jobs(timeout=3))

    def test_systemd_restart_with_absent_unit_requires_reconciliation(self) -> None:
        action = self.systemd_action()
        fake_systemd = FakeSystemd(hold=True)
        with mock.patch.dict(
            registry.ACTIONS, {action.action_id: action}, clear=False
        ), mock.patch(
            "operator_console.service.subprocess.run", side_effect=fake_systemd
        ):
            first = self.open_service(
                [self.profile("gpu.fixture", action.action_id)]
            )
            job_id = self.execute_systemd_profile(first, "gpu.fixture")
            self.wait_for_state(first, job_id, {"running"})
            first.close()
            self.assertTrue(first.wait_for_jobs(timeout=3))
            self.service = None
            fake_systemd.units.clear()

            second = OperatorService(
                repo_root=self.repo,
                profile_path=self.profile_path,
                state_root=self.state_root,
            )
            self.service = second
            self.assertTrue(second.wait_for_jobs(timeout=5))
            self.assertEqual(
                second.jobs[job_id]["state"], "indeterminate_after_restart"
            )

    def test_stream_overflow_is_capped_and_cannot_be_success(self) -> None:
        large_action = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "prefix": ("large", "4096"),
            }
        )
        with mock.patch.dict(
            registry.ACTIONS,
            {"preprocess.status": large_action},
            clear=False,
        ), mock.patch("operator_console.service.MAX_STREAM_BYTES", 1024):
            service = self.open_service(
                [self.profile("fixture.large", "preprocess.status")]
            )
            job_id = self.execute_profile(service, "fixture.large")
            self.assertTrue(service.wait_for_jobs(timeout=3))
            record = service.jobs[job_id]
            self.assertEqual(record["state"], "failed")
            self.assertTrue(record["logs"]["stdout"]["truncated"])
            self.assertEqual(record["logs"]["stdout"]["captured_byte_count"], 1024)
            self.assertGreater(record["logs"]["stdout"]["byte_count"], 1024)

    def test_log_reads_are_bounded_to_64_kib_offsets(self) -> None:
        large_action = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "prefix": ("large", "70000"),
            }
        )
        with mock.patch.dict(
            registry.ACTIONS, {"preprocess.status": large_action}, clear=False
        ):
            service = self.open_service(
                [self.profile("fixture.large", "preprocess.status")]
            )
            job_id = self.execute_profile(service, "fixture.large")
            self.assertTrue(service.wait_for_jobs(timeout=3))
            first = service.read_log_chunk(job_id, "stdout", 0)
            self.assertEqual(first["next_offset"], 64 * 1024)
            self.assertFalse(first["eof"])
            second = service.read_log_chunk(job_id, "stdout", first["next_offset"])
            self.assertTrue(second["eof"])

    def test_child_process_uses_private_umask(self) -> None:
        output = self.repo / "research" / "child-created"
        umask_action = registry.ActionSpec(
            **{
                **self.actions["preprocess.status"].__dict__,
                "prefix": ("umask", str(output)),
            }
        )
        with mock.patch.dict(
            registry.ACTIONS, {"preprocess.status": umask_action}, clear=False
        ):
            service = self.open_service(
                [self.profile("fixture.umask", "preprocess.status")]
            )
            job_id = self.execute_profile(service, "fixture.umask")
            self.assertTrue(service.wait_for_jobs(timeout=3))
            self.assertEqual(service.jobs[job_id]["state"], "succeeded")
            self.assertEqual(stat_mode(output), 0o600)

    def test_public_history_is_bounded_but_private_records_remain(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        with mock.patch("operator_console.service.PUBLIC_JOB_LIMIT", 2):
            for _index in range(3):
                self.execute_profile(service, "fixture.success")
                self.assertTrue(service.wait_for_jobs(timeout=3))
            state = service.public_state(csrf_token="csrf", prefix="/o/test/")
        self.assertEqual(len(service.jobs), 3)
        self.assertEqual(state["job_history_count"], 3)
        self.assertTrue(state["job_history_truncated"])
        self.assertEqual(len(state["jobs"]), 2)

    def test_exception_log_metadata_reloads_consistently(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        with mock.patch.object(
            service, "_finish_job", side_effect=RuntimeError("finite fixture")
        ):
            job_id = self.execute_profile(service, "fixture.success")
            self.assertTrue(service.wait_for_jobs(timeout=3))
        metadata = service.jobs[job_id]["logs"]["stdout"]
        self.assertEqual(metadata["byte_count"], metadata["captured_byte_count"])
        service.close()
        self.service = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        self.assertEqual(self.service.jobs[job_id]["state"], "failed")

    def test_loaded_state_rejects_integer_booleans(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        job_id = self.execute_profile(service, "fixture.success")
        self.assertTrue(service.wait_for_jobs(timeout=3))
        service.close()
        self.service = None
        path = self.state_root / "jobs" / job_id / "record.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        record["logs"]["stdout"]["available"] = 1
        path.write_text(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ServiceError, "stdout metadata"):
            OperatorService(
                repo_root=self.repo,
                profile_path=self.profile_path,
                state_root=self.state_root,
            )

    def test_terminal_indeterminate_job_is_not_recommitted_on_each_restart(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        job_id = self.execute_profile(service, "fixture.success")
        self.assertTrue(service.wait_for_jobs(timeout=3))
        service.close()
        self.service = None
        path = self.state_root / "jobs" / job_id / "record.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        record["state"] = "indeterminate_after_restart"
        record["returncode"] = None
        path.write_text(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        first = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        first_revision = first.revision
        first.close()
        second = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        self.service = second
        self.assertEqual(second.revision, first_revision)

    def test_schema_v1_direct_job_history_remains_restart_compatible(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        job_id = self.execute_profile(service, "fixture.success")
        self.assertTrue(service.wait_for_jobs(timeout=3))
        service.close()
        self.service = None
        path = self.state_root / "jobs" / job_id / "record.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        record["schema_version"] = 1
        record.pop("supervisor")
        record.pop("unit")
        record.pop("cancellation_requested_at")
        path.write_text(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        self.service = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        self.assertEqual(self.service.jobs[job_id]["state"], "succeeded")
        self.assertEqual(self.service.jobs[job_id]["supervisor"], "direct")

    def test_historical_action_id_need_not_remain_in_current_registry(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        job_id = self.execute_profile(service, "fixture.success")
        self.assertTrue(service.wait_for_jobs(timeout=3))
        service.close()
        self.service = None
        path = self.state_root / "jobs" / job_id / "record.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        record["action_id"] = "historical.removed"
        path.write_text(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        self.service = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        self.assertEqual(self.service.jobs[job_id]["action_id"], "historical.removed")

    def test_second_service_cannot_share_state_root(self) -> None:
        service = self.open_service(
            [self.profile("fixture.success", "preprocess.status")]
        )
        with self.assertRaisesRegex(ServiceError, "another operator console"):
            OperatorService(
                repo_root=self.repo,
                profile_path=self.profile_path,
                state_root=self.state_root,
            )
        self.assertIsNotNone(service)


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


if __name__ == "__main__":
    unittest.main()
