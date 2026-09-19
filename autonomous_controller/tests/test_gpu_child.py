from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable
from unittest import mock

from autonomous_controller.gpu_child import (
    CHILD_UNIT_RE,
    CHILD_ENVIRONMENT,
    CONTROLLER_SUPERVISOR_PID_ENV,
    ENV,
    FILE_SIZE_MAX_BYTES,
    MEMORY_MAX_BYTES,
    OUTER_UNIT_ENV,
    RUNTIME_MAX_SECONDS,
    SYSTEMCTL,
    SYSTEMD_INVOCATION_ENV,
    SYSTEMD_RUN,
    BatchResultStatus,
    ControllerUnitContext,
    GpuChildError,
    GpuChildReconciliationRequired,
    LocalPrivateGpuLaunchSpec,
    PrivateGpuChildJournal,
    SystemdGpuChildExecutor,
)


OUTER_UNIT = "himr-operator-job-" + "1" * 32 + ".service"
OUTER_INVOCATION = "2" * 32
CHILD_INVOCATION = "3" * 32
BATCH_ID = "gpuasrbatch2_" + "4" * 32


class FakeProbe:
    def __init__(self, *statuses: str):
        self.statuses = list(statuses or ("pending",))
        self.calls = 0

    def inspect(self, spec: LocalPrivateGpuLaunchSpec) -> BatchResultStatus:
        index = min(self.calls, len(self.statuses) - 1)
        status = self.statuses[index]
        self.calls += 1
        if status == "completed":
            return BatchResultStatus(status, spec.batch_id, (1,), (), ())
        if status == "invalid":
            return BatchResultStatus(
                status,
                spec.batch_id,
                (),
                (),
                ({"ordinal": 1, "error_type": "FixtureError", "message": "bad result"},),
            )
        return BatchResultStatus(status, spec.batch_id, (), (1,), ())


class FakeSystemd:
    """Deterministic manager model. It never executes the launcher."""

    def __init__(
        self,
        *,
        child_mode: str = "hold",
        outer_invocation: str = OUTER_INVOCATION,
        run_returncode: int = 0,
        stop_returncode: int = 0,
        on_run: Callable[[str], None] | None = None,
        on_stop: Callable[[str], None] | None = None,
    ):
        self.child_mode = child_mode
        self.outer_invocation = outer_invocation
        self.run_returncode = run_returncode
        self.stop_returncode = stop_returncode
        self.on_run = on_run
        self.on_stop = on_stop
        self.calls: list[tuple[list[str], dict[str, Any]]] = []
        self.units: dict[str, dict[str, Any]] = {}

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        self.calls.append((list(argv), dict(kwargs)))
        if kwargs.get("shell") is not False:
            raise AssertionError("systemd runner enabled a shell")
        if kwargs.get("stdin") is not subprocess.DEVNULL:
            raise AssertionError("systemd runner inherited stdin")
        if kwargs.get("close_fds") is not True or kwargs.get("cwd") != "/":
            raise AssertionError("systemd runner process boundary is not closed")
        if argv[0] == SYSTEMD_RUN:
            unit = next(row.split("=", 1)[1] for row in argv if row.startswith("--unit="))
            if self.on_run is not None:
                self.on_run(unit)
            if self.run_returncode != 0:
                return subprocess.CompletedProcess(argv, self.run_returncode, b"", b"fixture rejected")
            if self.child_mode == "vanished":
                self.units.pop(unit, None)
            else:
                self.units[unit] = {
                    "mode": self.child_mode,
                    "invocation": CHILD_INVOCATION,
                }
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        if argv[0] != SYSTEMCTL:
            raise AssertionError(f"unexpected executable {argv[0]!r}")
        if "stop" in argv:
            unit = argv[-1]
            if self.on_stop is not None:
                self.on_stop(unit)
            if self.stop_returncode != 0:
                return subprocess.CompletedProcess(argv, self.stop_returncode, b"", b"stop rejected")
            row = self.units.get(unit)
            if row is not None and row["mode"] != "ignore_stop":
                row["mode"] = "stopped"
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        if "show" not in argv:
            raise AssertionError(f"unexpected systemctl operation: {argv!r}")
        return self._show(argv, argv[-1])

    def _show(self, argv: list[str], unit: str) -> subprocess.CompletedProcess[bytes]:
        if unit == OUTER_UNIT:
            return self._response(
                argv,
                load="loaded",
                active="active",
                sub="running",
                result="success",
                code="",
                status="",
                invocation=self.outer_invocation,
            )
        row = self.units.get(unit)
        if row is None:
            return self._response(
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
        mode = row["mode"]
        invocation = "" if mode == "no_invocation" else row["invocation"]
        if mode in {"hold", "no_invocation", "ignore_stop"}:
            return self._response(
                argv,
                load="loaded",
                active="active",
                sub="running",
                result="",
                code="",
                status="",
                invocation=invocation,
            )
        if mode == "success":
            return self._response(
                argv,
                load="loaded",
                active="active",
                sub="exited",
                result="success",
                code="1",
                status="0",
                invocation=invocation,
            )
        if mode == "failure":
            return self._response(
                argv,
                load="loaded",
                active="failed",
                sub="failed",
                result="exit-code",
                code="1",
                status="7",
                invocation=invocation,
            )
        if mode == "stopped":
            return self._response(
                argv,
                load="loaded",
                active="inactive",
                sub="dead",
                result="success",
                code="1",
                status="0",
                invocation=invocation,
            )
        raise AssertionError(f"unknown fake unit mode: {mode}")

    @staticmethod
    def _response(
        argv: list[str],
        *,
        load: str,
        active: str,
        sub: str,
        result: str,
        code: str,
        status: str,
        invocation: str,
        returncode: int = 0,
    ) -> subprocess.CompletedProcess[bytes]:
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

    @property
    def run_calls(self) -> list[list[str]]:
        return [argv for argv, _kwargs in self.calls if argv[0] == SYSTEMD_RUN]

    @property
    def stop_calls(self) -> list[list[str]]:
        return [argv for argv, _kwargs in self.calls if argv[0] == SYSTEMCTL and "stop" in argv]


class GpuChildExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-gpu-child-")
        self.root = Path(self.temporary.name).resolve()
        os.chmod(self.root, 0o700)
        self.controls = self.root / "controls"
        self.controls.mkdir(mode=0o700)
        self.roots: dict[str, Path] = {}
        for name in ("results", "events", "locks", "journal"):
            path = self.root / name
            path.mkdir(mode=0o700)
            self.roots[name] = path
        self.file_bindings: dict[str, tuple[Path, str]] = {}
        for name in (
            "batch",
            "runtime",
            "production-profile",
            "root-registration",
            "launcher-profile",
            "readiness",
        ):
            path = self.controls / f"{name}.json"
            body = json.dumps({"fixture": name}, sort_keys=True, separators=(",", ":")) + "\n"
            path.write_text(body, encoding="utf-8")
            path.chmod(0o400)
            self.file_bindings[name] = (path, hashlib.sha256(body.encode()).hexdigest())
        self.launcher = self.controls / "trusted-launcher-v2"
        self.launcher.write_text("#!/usr/bin/python3 -IB\n", encoding="utf-8")
        self.launcher.chmod(0o500)
        self.context = ControllerUnitContext(OUTER_UNIT, OUTER_INVOCATION)
        self.journal = PrivateGpuChildJournal(self.roots["journal"])
        self.spec = LocalPrivateGpuLaunchSpec(
            batch_id=BATCH_ID,
            batch_manifest=self.file_bindings["batch"][0],
            expected_batch_sha256=self.file_bindings["batch"][1],
            runtime_admission=self.file_bindings["runtime"][0],
            expected_runtime_admission_sha256=self.file_bindings["runtime"][1],
            production_profile=self.file_bindings["production-profile"][0],
            expected_production_profile_sha256=self.file_bindings["production-profile"][1],
            root_registration=self.file_bindings["root-registration"][0],
            expected_root_registration_sha256=self.file_bindings["root-registration"][1],
            launcher_profile=self.file_bindings["launcher-profile"][0],
            expected_launcher_profile_sha256=self.file_bindings["launcher-profile"][1],
            local_readiness=self.file_bindings["readiness"][0],
            expected_local_readiness_sha256=self.file_bindings["readiness"][1],
            local_launcher=self.launcher,
            writable_result_root=self.roots["results"],
            writable_event_root=self.roots["events"],
            writable_lock_root=self.roots["locks"],
            working_directory=self.root,
        )

    def tearDown(self) -> None:
        # Controls are intentionally sealed mode 0400; TemporaryDirectory can still
        # unlink them, but restore owner write on directories for unusual platforms.
        for path in self.root.rglob("*"):
            if path.is_dir():
                path.chmod(0o700)
        self.temporary.cleanup()

    def executor(
        self,
        fake: FakeSystemd,
        probe: FakeProbe,
        **kwargs: Any,
    ) -> SystemdGpuChildExecutor:
        return SystemdGpuChildExecutor(
            self.context,
            self.journal,
            result_probe=probe,
            runner=fake,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
            **kwargs,
        )

    def accepted_timeout_executor(
        self, *, persist_unit: bool = True
    ) -> tuple[FakeSystemd, SystemdGpuChildExecutor]:
        fake: FakeSystemd

        def accepted_timeout(unit: str) -> None:
            if persist_unit:
                fake.units[unit] = {
                    "mode": "hold",
                    "invocation": CHILD_INVOCATION,
                }
            raise subprocess.TimeoutExpired([SYSTEMD_RUN], 1)

        fake = FakeSystemd(on_run=accepted_timeout)
        return fake, self.executor(fake, FakeProbe())

    def test_context_requires_exact_operator_unit_and_systemd_invocation(self) -> None:
        context = ControllerUnitContext.from_environment(
            {OUTER_UNIT_ENV: OUTER_UNIT, SYSTEMD_INVOCATION_ENV: OUTER_INVOCATION}
        )
        self.assertEqual(context, self.context)
        for environment in (
            {},
            {OUTER_UNIT_ENV: "other.service", SYSTEMD_INVOCATION_ENV: OUTER_INVOCATION},
            {OUTER_UNIT_ENV: OUTER_UNIT, SYSTEMD_INVOCATION_ENV: "not-an-invocation"},
        ):
            with self.subTest(environment=environment), self.assertRaises(GpuChildError):
                ControllerUnitContext.from_environment(environment)

    def test_scrubbed_context_resolves_exact_current_systemd_unit(self) -> None:
        process_id = 4242
        control_group = "/user.slice/user-1000.slice/app.slice/" + OUTER_UNIT
        cgroup_path = self.root / "self.cgroup"
        cgroup_path.write_text(f"0::{control_group}\n", encoding="utf-8")
        calls: list[tuple[list[str], dict[str, Any]]] = []

        def query(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
            calls.append((list(argv), dict(kwargs)))
            body = (
                "LoadState=loaded\n"
                "ActiveState=active\n"
                "SubState=running\n"
                f"InvocationID={OUTER_INVOCATION}\n"
                f"MainPID={process_id}\n"
                f"ControlGroup={control_group}\n"
            ).encode("utf-8")
            return subprocess.CompletedProcess(argv, 0, body, b"")

        context = ControllerUnitContext.from_current_systemd_unit(
            {OUTER_UNIT_ENV: OUTER_UNIT, "AMBIENT_SECRET": "never-forward"},
            runner=query,
            process_id=process_id,
            process_cgroup_path=cgroup_path,
        )

        self.assertEqual(context, self.context)
        self.assertEqual(len(calls), 1)
        argv, kwargs = calls[0]
        self.assertEqual(argv[0], SYSTEMCTL)
        self.assertEqual(argv[-1], OUTER_UNIT)
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        self.assertIs(kwargs["stdout"], subprocess.PIPE)
        self.assertIs(kwargs["stderr"], subprocess.PIPE)
        self.assertFalse(kwargs["shell"])
        self.assertTrue(kwargs["close_fds"])
        self.assertEqual(kwargs["cwd"], "/")
        self.assertNotIn("AMBIENT_SECRET", kwargs["env"])

    def test_scrubbed_context_accepts_only_exact_direct_supervisor_delegation(self) -> None:
        controller_pid = 4243
        supervisor_pid = 4242
        control_group = "/user.slice/user-1000.slice/app.slice/" + OUTER_UNIT
        cgroup_path = self.root / "delegated.cgroup"
        cgroup_path.write_text(f"0::{control_group}\n", encoding="utf-8")

        def query(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[bytes]:
            body = (
                "LoadState=loaded\n"
                "ActiveState=active\n"
                "SubState=running\n"
                f"InvocationID={OUTER_INVOCATION}\n"
                f"MainPID={supervisor_pid}\n"
                f"ControlGroup={control_group}\n"
            ).encode("utf-8")
            return subprocess.CompletedProcess(argv, 0, body, b"")

        context = ControllerUnitContext.from_current_systemd_unit(
            {
                OUTER_UNIT_ENV: OUTER_UNIT,
                CONTROLLER_SUPERVISOR_PID_ENV: str(supervisor_pid),
            },
            runner=query,
            process_id=controller_pid,
            parent_process_id=supervisor_pid,
            process_cgroup_path=cgroup_path,
        )

        self.assertEqual(context, self.context)

        cases = (
            ("missing", {}, supervisor_pid, "not the outer unit MainPID"),
            (
                "malformed",
                {CONTROLLER_SUPERVISOR_PID_ENV: "04242"},
                supervisor_pid,
                "supervisor PID is invalid",
            ),
            (
                "manager-mismatch",
                {CONTROLLER_SUPERVISOR_PID_ENV: str(supervisor_pid + 1)},
                supervisor_pid,
                "differs from the outer unit MainPID",
            ),
            (
                "parent-mismatch",
                {CONTROLLER_SUPERVISOR_PID_ENV: str(supervisor_pid)},
                supervisor_pid + 1,
                "direct parent is not",
            ),
        )
        for label, delegated_environment, parent_pid, message in cases:
            with self.subTest(label=label), self.assertRaisesRegex(
                GpuChildError, message
            ):
                ControllerUnitContext.from_current_systemd_unit(
                    {OUTER_UNIT_ENV: OUTER_UNIT, **delegated_environment},
                    runner=query,
                    process_id=controller_pid,
                    parent_process_id=parent_pid,
                    process_cgroup_path=cgroup_path,
                )

    def test_direct_main_pid_rejects_supervisor_delegation_marker(self) -> None:
        process_id = 4242
        control_group = "/user.slice/user-1000.slice/app.slice/" + OUTER_UNIT
        cgroup_path = self.root / "direct-with-delegation.cgroup"
        cgroup_path.write_text(f"0::{control_group}\n", encoding="utf-8")

        def query(argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[bytes]:
            body = (
                "LoadState=loaded\n"
                "ActiveState=active\n"
                "SubState=running\n"
                f"InvocationID={OUTER_INVOCATION}\n"
                f"MainPID={process_id}\n"
                f"ControlGroup={control_group}\n"
            ).encode("utf-8")
            return subprocess.CompletedProcess(argv, 0, body, b"")

        with self.assertRaisesRegex(GpuChildError, "invalid for the outer unit MainPID"):
            ControllerUnitContext.from_current_systemd_unit(
                {
                    OUTER_UNIT_ENV: OUTER_UNIT,
                    CONTROLLER_SUPERVISOR_PID_ENV: str(process_id),
                },
                runner=query,
                process_id=process_id,
                process_cgroup_path=cgroup_path,
            )

    def test_current_systemd_context_rejects_manager_identity_mismatches(self) -> None:
        process_id = 4242
        control_group = "/user.slice/user-1000.slice/app.slice/" + OUTER_UNIT
        cgroup_path = self.root / "self.cgroup"
        cgroup_path.write_text(f"0::{control_group}\n", encoding="utf-8")
        baseline = {
            "LoadState": "loaded",
            "ActiveState": "active",
            "SubState": "running",
            "InvocationID": OUTER_INVOCATION,
            "MainPID": str(process_id),
            "ControlGroup": control_group,
        }
        cases = (
            ("inactive", "process-live", {"ActiveState": "inactive"}, {}, 0),
            ("invocation", "InvocationID", {"InvocationID": "invalid"}, {}, 0),
            ("main-pid", "MainPID", {"MainPID": str(process_id + 1)}, {}, 0),
            ("cgroup", "cgroup differs", {"ControlGroup": control_group + "-other"}, {}, 0),
            ("query-failed", "query failed", {}, {}, 1),
            (
                "inherited-id",
                "differs",
                {},
                {SYSTEMD_INVOCATION_ENV: "5" * 32},
                0,
            ),
        )
        for label, message, changes, extra_environment, returncode in cases:
            with self.subTest(label=label):
                fields = {**baseline, **changes}

                def query(
                    argv: list[str],
                    **_kwargs: Any,
                ) -> subprocess.CompletedProcess[bytes]:
                    body = "".join(
                        f"{key}={value}\n" for key, value in fields.items()
                    ).encode("utf-8")
                    return subprocess.CompletedProcess(argv, returncode, body, b"")

                with self.assertRaisesRegex(GpuChildError, message):
                    ControllerUnitContext.from_current_systemd_unit(
                        {OUTER_UNIT_ENV: OUTER_UNIT, **extra_environment},
                        runner=query,
                        process_id=process_id,
                        process_cgroup_path=cgroup_path,
                    )

    def test_unit_name_is_deterministic_per_outer_invocation_batch_and_attempt(self) -> None:
        executor = self.executor(FakeSystemd(), FakeProbe())
        first = executor.unit_name(self.spec)
        self.assertIsNotNone(CHILD_UNIT_RE.fullmatch(first))
        self.assertEqual(first, executor.unit_name(self.spec))
        self.assertIn(OUTER_INVOCATION, first)
        self.assertIn(self.spec.expected_batch_sha256[:32], first)
        self.assertTrue(first.endswith("-000001.service"))
        self.assertNotEqual(first, executor.unit_name(replace(self.spec, attempt_ordinal=2)))
        other = SystemdGpuChildExecutor(
            ControllerUnitContext(OUTER_UNIT, "5" * 32),
            self.journal,
            result_probe=FakeProbe(),
            runner=FakeSystemd(outer_invocation="5" * 32),
            sleep=lambda _seconds: None,
        )
        self.assertNotEqual(first, other.unit_name(self.spec))

    def test_launch_persists_intent_then_invocation_before_returning_running(self) -> None:
        observed: list[str] = []

        def on_run(unit: str) -> None:
            record = self.journal.load(unit)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.state, "launching")
            self.assertIsNone(record.child_invocation_id)
            observed.append(unit)

        fake = FakeSystemd(on_run=on_run)
        record = self.executor(fake, FakeProbe()).launch(self.spec)
        self.assertEqual(record.state, "running")
        self.assertEqual(record.child_invocation_id, CHILD_INVOCATION)
        self.assertEqual(observed, [record.unit_name])
        persisted = self.journal.load(record.unit_name)
        self.assertEqual(persisted, record)

        child = self.roots["journal"] / record.unit_name.removesuffix(".service")
        self.assertEqual(stat.S_IMODE(child.stat().st_mode), 0o700)
        for name in ("record.json", "stdout.log", "stderr.log"):
            self.assertEqual(stat.S_IMODE((child / name).stat().st_mode), 0o600)
        body = (child / "record.json").read_bytes()
        self.assertEqual(body, json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode() + b"\n")

    def test_launch_uses_exact_gpu_envelope_dependencies_and_closed_argv(self) -> None:
        fake = FakeSystemd()
        record = self.executor(fake, FakeProbe()).launch(self.spec)
        self.assertEqual(record.state, "running")
        self.assertEqual(len(fake.run_calls), 1)
        argv = fake.run_calls[0]
        expected_properties = {
            "--property=Type=exec",
            "--property=ExitType=cgroup",
            "--property=Restart=no",
            "--property=KillMode=control-group",
            f"--property=RuntimeMaxSec={RUNTIME_MAX_SECONDS}s",
            "--property=TimeoutStopSec=15s",
            "--property=SendSIGKILL=yes",
            "--property=OOMPolicy=stop",
            "--property=TasksMax=64",
            "--property=LimitNOFILE=1024",
            f"--property=LimitFSIZE={FILE_SIZE_MAX_BYTES}",
            "--property=LimitCORE=0",
            f"--property=MemoryMax={MEMORY_MAX_BYTES}",
            "--property=MemorySwapMax=0",
            "--property=UMask=0077",
            "--property=StandardInput=null",
            "--property=RemainAfterExit=yes",
            f"--property=PartOf={OUTER_UNIT}",
            f"--property=BindsTo={OUTER_UNIT}",
            f"--property=After={OUTER_UNIT}",
        }
        self.assertTrue(expected_properties <= set(argv))
        self.assertEqual(argv[:5], [SYSTEMD_RUN, "--user", "--no-block", "--quiet", "--no-ask-password"])
        separator = argv.index("--")
        self.assertEqual(argv[separator + 1 : separator + 3], [ENV, "-i"])
        launcher_index = argv.index(str(self.launcher))
        self.assertEqual(tuple(argv[launcher_index:]), self.spec.launcher_argv())
        self.assertEqual(
            set(argv[separator + 3 : launcher_index]),
            {f"{key}={value}" for key, value in CHILD_ENVIRONMENT.items()},
        )
        run_kwargs = next(kwargs for command, kwargs in fake.calls if command[0] == SYSTEMD_RUN)
        self.assertEqual(run_kwargs["env"]["HOME"], "/nonexistent")
        self.assertNotIn(OUTER_UNIT_ENV, run_kwargs["env"])

    def test_outer_invocation_mismatch_refuses_launch(self) -> None:
        fake = FakeSystemd(outer_invocation="6" * 32)
        with self.assertRaises(GpuChildError):
            self.executor(fake, FakeProbe()).launch(self.spec)
        self.assertEqual(fake.run_calls, [])

    def test_prior_invocation_active_attempt_blocks_duplicate_admission(self) -> None:
        old_invocation = "5" * 32
        old_fake = FakeSystemd(outer_invocation=old_invocation)
        old_executor = SystemdGpuChildExecutor(
            ControllerUnitContext(OUTER_UNIT, old_invocation),
            self.journal,
            result_probe=FakeProbe(),
            runner=old_fake,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
        )
        old_record = old_executor.launch(self.spec)
        self.assertEqual("running", old_record.state)

        current_fake = FakeSystemd()
        current_executor = self.executor(current_fake, FakeProbe())
        with self.assertRaisesRegex(
            GpuChildReconciliationRequired, "prior controller invocation"
        ):
            current_executor.launch(self.spec)
        self.assertEqual([], current_fake.run_calls)

        alternate_working = self.root / "alternate-working"
        alternate_working.mkdir(mode=0o700)
        changed_identity = replace(
            self.spec, working_directory=alternate_working
        )
        changed_fake = FakeSystemd()
        changed_executor = self.executor(changed_fake, FakeProbe())
        with self.assertRaisesRegex(
            GpuChildReconciliationRequired, "prior controller invocation"
        ):
            changed_executor.launch(changed_identity)
        self.assertEqual([], changed_fake.run_calls)

        self.assertEqual("stopped", old_executor.stop(self.spec).state)
        fresh_fake = FakeSystemd()
        fresh_executor = self.executor(fresh_fake, FakeProbe())
        admitted = fresh_executor.launch(self.spec)
        self.assertEqual("running", admitted.state)
        self.assertEqual(1, len(fresh_fake.run_calls))

    def test_prior_authority_cache_invalidates_when_foreign_unit_is_added(self) -> None:
        current_fake = FakeSystemd()
        current_executor = self.executor(current_fake, FakeProbe())
        current_executor._assert_no_prior_invocation_authority(self.spec)

        old_invocation = "5" * 32
        old_fake = FakeSystemd(outer_invocation=old_invocation)
        old_executor = SystemdGpuChildExecutor(
            ControllerUnitContext(OUTER_UNIT, old_invocation),
            self.journal,
            result_probe=FakeProbe(),
            runner=old_fake,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
        )
        old_record = old_executor.launch(self.spec)
        self.assertEqual("running", old_record.state)

        with self.assertRaisesRegex(
            GpuChildReconciliationRequired, "prior controller invocation"
        ):
            current_executor.launch(self.spec)
        self.assertEqual([], current_fake.run_calls)

    def test_launch_authority_closes_foreign_probe_to_intent_race(self) -> None:
        old_invocation = "5" * 32
        old_fake = FakeSystemd(outer_invocation=old_invocation)
        old_executor = SystemdGpuChildExecutor(
            ControllerUnitContext(OUTER_UNIT, old_invocation),
            self.journal,
            result_probe=FakeProbe(),
            runner=old_fake,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
        )

        test_case = self
        blocked_windows: list[str] = []

        def assert_foreign_launch_blocked(window: str) -> None:
            with test_case.assertRaisesRegex(
                GpuChildReconciliationRequired,
                "launch authority is already held",
            ):
                old_executor.launch(self.spec)
            blocked_windows.append(window)

        class RacingProbe:
            def inspect(
                self, spec: LocalPrivateGpuLaunchSpec
            ) -> BatchResultStatus:
                # The current executor has already scanned an empty journal.
                # A foreign launch in this exact window must fail before it can
                # persist a second intent or ask systemd to create a unit.
                assert_foreign_launch_blocked("after-scan-before-intent")
                return BatchResultStatus("pending", spec.batch_id, (), (1,), ())

        current_fake = FakeSystemd(
            on_run=lambda _unit: assert_foreign_launch_blocked(
                "after-intent-before-acceptance"
            )
        )
        current_executor = self.executor(current_fake, RacingProbe())

        current = current_executor.launch(self.spec)

        self.assertEqual("running", current.state)
        self.assertEqual(
            ["after-scan-before-intent", "after-intent-before-acceptance"],
            blocked_windows,
        )
        self.assertEqual(1, len(current_fake.run_calls))
        self.assertEqual([], old_fake.run_calls)
        self.assertEqual((current,), self.journal.list_records())

    def test_prelaunch_authority_scan_is_fail_closed_and_amortized(self) -> None:
        fake = FakeSystemd()
        executor = self.executor(fake, FakeProbe())
        original = self.journal.list_records
        scans = 0

        def counted_records():
            nonlocal scans
            scans += 1
            return original()

        with mock.patch.object(
            self.journal, "list_records", side_effect=counted_records
        ):
            first = executor.launch(self.spec)
            executor.stop(self.spec)
            second_spec = replace(self.spec, attempt_ordinal=2)
            executor.launch(second_spec)
        self.assertEqual("stopped", self.journal.load(first.unit_name).state)
        self.assertEqual(1, scans)

        failed_fake = FakeSystemd()
        failed_executor = self.executor(failed_fake, FakeProbe())
        with (
            mock.patch.object(
                self.journal,
                "list_records",
                side_effect=OSError("fixture journal scan failed"),
            ),
            self.assertRaisesRegex(
                GpuChildReconciliationRequired, "could not be scanned"
            ),
        ):
            failed_executor.launch(replace(self.spec, attempt_ordinal=3))
        self.assertEqual([], failed_fake.run_calls)

    def test_unjournalled_deterministic_unit_collision_is_never_adopted(self) -> None:
        fake = FakeSystemd()
        executor = self.executor(fake, FakeProbe())
        unit = executor.unit_name(self.spec)
        fake.units[unit] = {"mode": "hold", "invocation": "7" * 32}
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.launch(self.spec)
        self.assertEqual(fake.run_calls, [])
        self.assertIsNone(self.journal.load(unit))

    def test_already_completed_batch_is_persisted_without_a_child(self) -> None:
        fake = FakeSystemd()
        record = self.executor(fake, FakeProbe("completed")).launch(self.spec)
        self.assertEqual(record.state, "succeeded")
        self.assertIsNone(record.child_invocation_id)
        self.assertIsNone(record.launch_accepted_at)
        self.assertEqual(record.result["status"], "completed")
        self.assertEqual(fake.run_calls, [])

    def test_systemd_run_rejection_is_durable_and_not_reported_running(self) -> None:
        fake = FakeSystemd(run_returncode=1)
        executor = self.executor(fake, FakeProbe())
        with self.assertRaises(GpuChildError):
            executor.launch(self.spec)
        record = self.journal.load(executor.unit_name(self.spec))
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.state, "failed")
        self.assertIsNone(record.child_invocation_id)

    def test_success_requires_exact_completed_result_replay_and_retires_unit(self) -> None:
        states_seen_at_stop: list[str] = []

        def on_stop(unit: str) -> None:
            record = self.journal.load(unit)
            assert record is not None
            states_seen_at_stop.append(record.state)

        fake = FakeSystemd(child_mode="success", on_stop=on_stop)
        record = self.executor(fake, FakeProbe("pending", "completed")).launch(self.spec)
        self.assertEqual(record.state, "succeeded")
        self.assertEqual(record.returncode, 0)
        self.assertEqual(record.result["status"], "completed")
        self.assertEqual(states_seen_at_stop, ["retiring"])
        self.assertEqual(len(fake.stop_calls), 1)
        self.assertEqual(fake.stop_calls[0][-1], record.unit_name)

    def test_zero_exit_without_completed_results_requires_reconciliation(self) -> None:
        fake = FakeSystemd(child_mode="success")
        record = self.executor(fake, FakeProbe("pending")).launch(self.spec)
        self.assertEqual(record.state, "reconciliation_required")
        self.assertEqual(record.returncode, 0)
        self.assertEqual(record.result["status"], "pending")
        self.assertEqual(len(fake.stop_calls), 1)

    def test_nonzero_exit_with_replayable_partial_results_is_failed_and_retired(self) -> None:
        fake = FakeSystemd(child_mode="failure")
        record = self.executor(fake, FakeProbe("pending")).launch(self.spec)
        self.assertEqual(record.state, "failed")
        self.assertEqual(record.returncode, 7)
        self.assertEqual(record.result["status"], "pending")
        self.assertEqual(len(fake.stop_calls), 1)

    def test_running_child_reconciles_after_executor_restart(self) -> None:
        fake = FakeSystemd()
        first = self.executor(fake, FakeProbe()).launch(self.spec)
        second_executor = self.executor(fake, FakeProbe())
        second = second_executor.reconcile(self.spec)
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(second.state, "running")
        self.assertEqual(second.child_invocation_id, first.child_invocation_id)
        self.assertEqual(fake.run_calls, [fake.run_calls[0]])

    def test_accepted_timeout_reconcile_binds_acceptance_before_exact_stop(self) -> None:
        fake, executor = self.accepted_timeout_executor()
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.launch(self.spec)
        ambiguous = self.journal.load(executor.unit_name(self.spec))
        assert ambiguous is not None
        self.assertEqual("reconciliation_required", ambiguous.state)
        self.assertIsNone(ambiguous.launch_accepted_at)
        self.assertIsNone(ambiguous.child_invocation_id)

        running = executor.reconcile(self.spec)
        assert running is not None
        self.assertEqual("running", running.state)
        self.assertEqual(CHILD_INVOCATION, running.child_invocation_id)
        self.assertIsNotNone(running.launch_accepted_at)
        self.assertIsNone(running.completed_at)

        stopped = executor.stop(self.spec)
        self.assertEqual("stopped", stopped.state)
        self.assertEqual(1, len(fake.stop_calls))
        self.assertEqual("stopped", fake.units[stopped.unit_name]["mode"])

    def test_accepted_timeout_direct_stop_queries_binds_and_controls_unit(self) -> None:
        fake, executor = self.accepted_timeout_executor()
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.launch(self.spec)

        preserved = executor.stop(self.spec)
        self.assertEqual("reconciliation_required", preserved.state)
        self.assertEqual(CHILD_INVOCATION, preserved.child_invocation_id)
        self.assertIsNotNone(preserved.launch_accepted_at)
        self.assertIsNotNone(preserved.stop_requested_at)
        self.assertEqual(1, len(fake.stop_calls))
        self.assertEqual("stopped", fake.units[preserved.unit_name]["mode"])

    def test_ambiguous_launch_absence_is_queried_without_false_manager_stop(self) -> None:
        fake, executor = self.accepted_timeout_executor(persist_unit=False)
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.launch(self.spec)
        calls_before_stop = len(fake.calls)

        preserved = executor.stop(self.spec)
        self.assertEqual("reconciliation_required", preserved.state)
        self.assertIsNone(preserved.launch_accepted_at)
        self.assertEqual([], fake.stop_calls)
        self.assertGreater(len(fake.calls), calls_before_stop)

    def test_manager_identity_without_acceptance_is_rejected_at_record_boundary(self) -> None:
        fake = FakeSystemd()
        running = self.executor(fake, FakeProbe()).launch(self.spec)
        for state in ("running", "retiring", "stopping", "succeeded"):
            changes = {
                "state": state,
                "launch_accepted_at": None,
            }
            if state == "succeeded":
                changes["result"] = BatchResultStatus(
                    "completed", self.spec.batch_id, (1,), (), ()
                ).document()
            with self.subTest(state=state), self.assertRaisesRegex(
                GpuChildError, "lacks durable launch acceptance"
            ):
                replace(running, **changes).validated()
        for state in ("succeeded", "failed", "stopped"):
            changes = {
                "state": state,
                "child_invocation_id": None,
            }
            if state == "succeeded":
                changes["result"] = BatchResultStatus(
                    "completed", self.spec.batch_id, (1,), (), ()
                ).document()
            with self.subTest(inverse_state=state), self.assertRaisesRegex(
                GpuChildError, "lacks durable manager identity"
            ):
                replace(running, **changes).validated()

        for state in ("launching", "reconciliation_required"):
            with self.subTest(accepted_unbound_state=state):
                replace(
                    running,
                    state=state,
                    child_invocation_id=None,
                ).validated()

    def test_invocation_change_refuses_reconciliation_and_stop(self) -> None:
        fake = FakeSystemd()
        executor = self.executor(fake, FakeProbe())
        record = executor.launch(self.spec)
        fake.units[record.unit_name]["invocation"] = "8" * 32
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.reconcile(self.spec)
        persisted = self.journal.load(record.unit_name)
        assert persisted is not None
        self.assertEqual(persisted.state, "reconciliation_required")
        self.assertEqual(fake.stop_calls, [])
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.stop(self.spec)
        self.assertEqual(fake.stop_calls, [])

    def test_stop_persists_stopping_before_exact_stop_and_verifies_inactive(self) -> None:
        states_seen: list[str] = []

        def on_stop(unit: str) -> None:
            record = self.journal.load(unit)
            assert record is not None
            states_seen.append(record.state)

        fake = FakeSystemd(on_stop=on_stop)
        executor = self.executor(fake, FakeProbe())
        running = executor.launch(self.spec)
        stopped = executor.stop(self.spec)
        self.assertEqual(stopped.state, "stopped")
        self.assertEqual(stopped.child_invocation_id, CHILD_INVOCATION)
        self.assertEqual(states_seen, ["stopping"])
        self.assertEqual(fake.stop_calls[0][-1], running.unit_name)
        self.assertEqual(fake.units[running.unit_name]["mode"], "stopped")
        # An already inactive exact unit makes repeated Stop idempotent and does not
        # issue a second manager stop command.
        repeated = executor.stop(self.spec)
        self.assertEqual(repeated.state, "stopped")
        self.assertEqual(len(fake.stop_calls), 1)

    def test_stop_failure_keeps_reconciliation_required_and_resource_identity(self) -> None:
        fake = FakeSystemd(stop_returncode=1)
        executor = self.executor(fake, FakeProbe())
        running = executor.launch(self.spec)
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.stop(self.spec)
        record = self.journal.load(running.unit_name)
        assert record is not None
        self.assertEqual(record.state, "reconciliation_required")
        self.assertEqual(record.child_invocation_id, CHILD_INVOCATION)
        self.assertIsNotNone(record.stop_requested_at)
        ambiguity = record.error

        fake.stop_returncode = 0
        reconciled = executor.reconcile(self.spec)
        assert reconciled is not None
        self.assertEqual("stopping", reconciled.state)
        self.assertEqual(record.stop_requested_at, reconciled.stop_requested_at)
        self.assertEqual(ambiguity, reconciled.error)
        self.assertIsNone(reconciled.completed_at)

        stopped = executor.stop(self.spec)
        self.assertEqual("stopped", stopped.state)
        self.assertEqual(2, len(fake.stop_calls))
        self.assertEqual("stopped", fake.units[stopped.unit_name]["mode"])

    def test_stop_that_never_becomes_inactive_is_not_reported_stopped(self) -> None:
        fake = FakeSystemd(child_mode="ignore_stop")
        executor = self.executor(fake, FakeProbe(), stop_attempts=2)
        running = executor.launch(self.spec)
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.stop(self.spec)
        record = self.journal.load(running.unit_name)
        assert record is not None
        self.assertEqual(record.state, "reconciliation_required")

    def test_stop_preserves_proven_terminal_outcomes_when_unit_is_inactive_or_absent(self) -> None:
        fake = FakeSystemd()
        cases = (
            (1, "success", FakeProbe("pending", "completed", "completed"), "succeeded", True),
            (2, "failure", FakeProbe("pending"), "failed", False),
            (3, "success", FakeProbe("pending"), "reconciliation_required", True),
        )
        for ordinal, mode, probe, expected_state, remove_unit in cases:
            with self.subTest(state=expected_state, absent=remove_unit):
                fake.child_mode = mode
                spec = replace(self.spec, attempt_ordinal=ordinal)
                executor = self.executor(fake, probe)
                terminal = executor.launch(spec)
                self.assertEqual(expected_state, terminal.state)
                if remove_unit:
                    fake.units.pop(terminal.unit_name)
                stop_calls = len(fake.stop_calls)
                evidence = (
                    terminal.state,
                    terminal.returncode,
                    terminal.systemd_result,
                    terminal.result,
                    terminal.error,
                    terminal.completed_at,
                )

                preserved = executor.stop(spec)

                self.assertEqual(
                    evidence,
                    (
                        preserved.state,
                        preserved.returncode,
                        preserved.systemd_result,
                        preserved.result,
                        preserved.error,
                        preserved.completed_at,
                    ),
                )
                self.assertEqual(stop_calls, len(fake.stop_calls))

    def test_stop_replays_succeeded_result_before_preserving_terminal_outcome(self) -> None:
        fake = FakeSystemd(child_mode="success")
        executor = self.executor(
            fake, FakeProbe("pending", "completed", "pending")
        )
        succeeded = executor.launch(self.spec)
        self.assertEqual("succeeded", succeeded.state)
        stop_calls = len(fake.stop_calls)

        replayed = executor.stop(self.spec)

        self.assertEqual("reconciliation_required", replayed.state)
        self.assertIn("no longer establishes completion", replayed.error)
        self.assertEqual(stop_calls, len(fake.stop_calls))

    def test_stop_preflight_transport_failure_is_durable_ambiguity(self) -> None:
        fake = FakeSystemd()
        fail_child_show = False

        def runner(argv: list[str], **kwargs: Any):
            if (
                fail_child_show
                and argv[0] == SYSTEMCTL
                and "show" in argv
                and argv[-1] != OUTER_UNIT
            ):
                raise subprocess.TimeoutExpired(argv, 1)
            return fake(argv, **kwargs)

        executor = SystemdGpuChildExecutor(
            self.context,
            self.journal,
            result_probe=FakeProbe(),
            runner=runner,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
        )
        running = executor.launch(self.spec)
        fail_child_show = True

        with self.assertRaises(GpuChildReconciliationRequired):
            executor.stop(self.spec)

        persisted = self.journal.load(running.unit_name)
        assert persisted is not None
        self.assertEqual("reconciliation_required", persisted.state)
        self.assertIsNotNone(persisted.stop_requested_at)
        self.assertIn("stop preflight failed", persisted.error)
        self.assertEqual([], fake.stop_calls)

    def test_stop_control_transport_failure_is_durably_reconciliation_required(self) -> None:
        def timeout(_unit: str) -> None:
            raise subprocess.TimeoutExpired([SYSTEMCTL, "stop"], 1)

        fake = FakeSystemd(on_stop=timeout)
        executor = self.executor(fake, FakeProbe())
        running = executor.launch(self.spec)

        with self.assertRaises(GpuChildReconciliationRequired):
            executor.stop(self.spec)

        persisted = self.journal.load(running.unit_name)
        assert persisted is not None
        self.assertEqual("reconciliation_required", persisted.state)
        self.assertEqual(CHILD_INVOCATION, persisted.child_invocation_id)
        self.assertIsNotNone(persisted.stop_requested_at)
        self.assertIsNotNone(persisted.completed_at)
        self.assertIn("stop could not be proven", persisted.error)

    def test_terminal_retirement_transport_failures_are_durable(self) -> None:
        class ReplayFailureProbe:
            def __init__(self) -> None:
                self.calls = 0

            def inspect(self, spec: LocalPrivateGpuLaunchSpec) -> BatchResultStatus:
                self.calls += 1
                if self.calls == 1:
                    return BatchResultStatus("pending", spec.batch_id, (), (1,), ())
                raise RuntimeError("fixture result replay unavailable")

        def timeout(_unit: str) -> None:
            raise subprocess.TimeoutExpired([SYSTEMCTL, "stop"], 1)

        cases = (
            (1, FakeProbe("pending", "completed"), "terminal child could not be retired"),
            (2, ReplayFailureProbe(), "result replay failed and terminal child could not be retired"),
        )
        for ordinal, probe, message in cases:
            with self.subTest(ordinal=ordinal):
                fake = FakeSystemd(child_mode="success", on_stop=timeout)
                executor = SystemdGpuChildExecutor(
                    self.context,
                    self.journal,
                    result_probe=probe,
                    runner=fake,
                    sleep=lambda _seconds: None,
                    now=lambda: "2026-08-29T22:00:00Z",
                )
                spec = replace(self.spec, attempt_ordinal=ordinal)

                persisted = executor.launch(spec)

                self.assertEqual("reconciliation_required", persisted.state)
                self.assertIsNotNone(persisted.stop_requested_at)
                self.assertIsNotNone(persisted.completed_at)
                self.assertIn(message, persisted.error)

    def test_prelaunch_success_replay_exception_is_persisted(self) -> None:
        class PrelaunchProbe:
            def __init__(self) -> None:
                self.calls = 0

            def inspect(self, spec: LocalPrivateGpuLaunchSpec) -> BatchResultStatus:
                self.calls += 1
                if self.calls == 1:
                    return BatchResultStatus("completed", spec.batch_id, (1,), (), ())
                raise RuntimeError("fixture replay unavailable")

        fake = FakeSystemd()
        probe = PrelaunchProbe()
        executor = SystemdGpuChildExecutor(
            self.context,
            self.journal,
            result_probe=probe,
            runner=fake,
            sleep=lambda _seconds: None,
            now=lambda: "2026-08-29T22:00:00Z",
        )
        succeeded = executor.launch(self.spec)
        self.assertEqual("succeeded", succeeded.state)
        manager_calls = len(fake.calls)

        replayed = executor.reconcile(self.spec)

        assert replayed is not None
        self.assertEqual("reconciliation_required", replayed.state)
        self.assertIn("prelaunch GPU result replay failed", replayed.error)
        self.assertEqual(manager_calls, len(fake.calls))

    def test_no_invocation_after_manager_acceptance_is_reconciliation_required(self) -> None:
        fake = FakeSystemd(child_mode="no_invocation")
        executor = self.executor(fake, FakeProbe(), bind_attempts=2)
        with self.assertRaises(GpuChildReconciliationRequired):
            executor.launch(self.spec)
        record = self.journal.load(executor.unit_name(self.spec))
        assert record is not None
        self.assertEqual(record.state, "reconciliation_required")
        self.assertIsNone(record.child_invocation_id)

    def test_retired_succeeded_record_reconciles_from_results_after_unit_absence(self) -> None:
        fake = FakeSystemd(child_mode="success")
        probe = FakeProbe("pending", "completed", "completed")
        executor = self.executor(fake, probe)
        record = executor.launch(self.spec)
        self.assertEqual(record.state, "succeeded")
        fake.units.pop(record.unit_name)
        replayed = executor.reconcile(self.spec)
        assert replayed is not None
        self.assertEqual(replayed.state, "succeeded")
        self.assertEqual(replayed.result["status"], "completed")

    def test_binding_change_for_same_unit_is_rejected(self) -> None:
        fake = FakeSystemd()
        executor = self.executor(fake, FakeProbe())
        executor.launch(self.spec)
        alternate_root = self.root / "alternate-results"
        alternate_root.mkdir(mode=0o700)
        changed = replace(self.spec, writable_result_root=alternate_root)
        with self.assertRaises(GpuChildError):
            executor.launch(changed)
        self.assertEqual(len(fake.run_calls), 1)

    def test_hash_or_file_mode_change_fails_before_systemd_control(self) -> None:
        for changed in (
            replace(self.spec, expected_batch_sha256="a" * 64),
            replace(self.spec, local_launcher=self.file_bindings["batch"][0]),
        ):
            fake = FakeSystemd()
            with self.subTest(changed=changed), self.assertRaises(GpuChildError):
                self.executor(fake, FakeProbe()).launch(changed)
            self.assertEqual(fake.calls, [])

    def test_private_journal_reloads_records_and_rejects_unsafe_contents(self) -> None:
        fake = FakeSystemd()
        record = self.executor(fake, FakeProbe()).launch(self.spec)
        reopened = PrivateGpuChildJournal(self.roots["journal"])
        self.assertEqual(reopened.list_records(), (record,))
        record_path = (
            self.roots["journal"]
            / record.unit_name.removesuffix(".service")
            / "record.json"
        )
        record_path.chmod(0o644)
        with self.assertRaises(GpuChildError):
            PrivateGpuChildJournal(self.roots["journal"])


if __name__ == "__main__":
    unittest.main()
