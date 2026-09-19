"""Offline handoff tests: no manager changes, downloads, or live campaign paths."""

from contextlib import contextmanager, ExitStack, nullcontext, redirect_stdout
from copy import deepcopy
import json
import io
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from autonomous_controller import campaign_handoff as handoff
from pipeline import hybrid_legacy_guard as legacy
from pipeline.tests import test_hybrid_legacy_guard as guard_fixtures


UNIT = "himr-operator-job-" + "1" * 32 + ".service"
INVOCATION = "2" * 32


def unit_state(*, finished=False):
    return {
        "Id": UNIT, "LoadState": "loaded", "ActiveState": "active",
        "SubState": "exited" if finished else "running", "MainPID": "0" if finished else "123",
        "ControlPID": "0", "Result": "success", "ExecMainCode": "1" if finished else "0",
        "ExecMainStatus": "0", "InvocationID": INVOCATION,
        "ControlGroup": "" if finished else f"/user.slice/user-{os.geteuid()}.slice/user@{os.geteuid()}.service/app.slice/{UNIT}",
        "StandardOutput": "append",
    }


def result(config, registration, *, coordinated=True):
    return {
        "status": "supervised_campaign_exited", "source_config_id": config.config_id,
        "registration_identity_sha256": registration.document["identity_sha256"],
        "registration_sha256": registration.physical_sha256,
        "stop_coordinated": coordinated,
        "stop_reason": "one supervised child exited" if coordinated else None,
        "children": {
            "source_controller": {"returncode": 0, "result_error": None, "result": {
                "status": "controller_exited", "config_id": config.config_id, "returncode": 0,
                "lifecycle": "completed", "actual_state": "completed", "desired_state": "stopped",
                "completion_reason": "campaign_drained", "cycle": 4, "last_error": None,
            }},
            "longform_companion": {"returncode": 0, "result_error": None, "result": {
                "status": "stopped", "reason": "source_controller_desired_state_stopped",
                "source_controller_mutated": False,
            }},
        },
    }


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.fixture = guard_fixtures.LegacyGuardTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        fixture = self.fixture
        fixture.status.update(lifecycle="completed", actual_state="completed", completion_reason="campaign_drained")
        fixture.companion_status["jobs"] = {"unprepared": 0, "preprocessed": 0, "prepared": 0, "incomplete": 0, "completed": 4}
        fixture.save()
        self.predecessor = SimpleNamespace(
            path=fixture.root / "predecessor-config.json", state_root=fixture.controller,
            config_id=fixture.control["config_id"], physical_sha256="b" * 64,
        )
        self.successor = SimpleNamespace(
            path=fixture.root / "successor-config.json", state_root=fixture.root / "successor",
            config_id="himrautocfg_" + "e" * 32, physical_sha256="f" * 64,
        )
        self.registration = SimpleNamespace(
            status_path=fixture.companion / "status.json", physical_sha256="7" * 64,
            document={"identity_sha256": "8" * 64},
        )
        self.successor_registration = SimpleNamespace(
            status_path=fixture.root / "new-companion" / "status.json", physical_sha256="9" * 64,
            document={"identity_sha256": "0" * 64},
        )
        self.initial = {**fixture.control, "generation": 10, "desired_state": "running"}
        self.final = deepcopy(fixture.control)
        self.proof = result(self.predecessor, self.registration)

    def validate(self, value=None, *, final=None):
        handoff._validate_result(value or self.proof, self.predecessor, self.registration,
                                 self.initial, final or self.final, self.fixture.status)

    def test_result_requires_exact_automatic_generation_trajectory(self):
        self.validate()
        uncoordinated = result(self.predecessor, self.registration, coordinated=False)
        self.validate(uncoordinated, final={**self.final, "generation": 11})
        for generation in (10, 11, 13):
            with self.subTest(generation=generation), self.assertRaises(handoff.HandoffCancelled):
                self.validate(final={**self.final, "generation": generation})
        with self.assertRaises(handoff.HandoffCancelled):
            self.validate(uncoordinated)  # An extra manual Stop is not automatic.

    def test_result_rejects_wrong_config_registration_child_or_stop_reason(self):
        mutations = [
            lambda value: value.update(source_config_id="different"),
            lambda value: value.update(registration_sha256="0" * 64),
            lambda value: value.update(registration_identity_sha256="0" * 64),
            lambda value: value.update(stop_reason="supervisor received signal 15"),
            lambda value: value.update(stop_coordinated=1),
            lambda value: value["children"]["source_controller"].update(returncode=2),
            lambda value: value["children"]["source_controller"]["result"].update(completion_reason="primary_pass_drained_with_parked_items"),
            lambda value: value["children"]["source_controller"]["result"].update(cycle=3),
            lambda value: value["children"]["longform_companion"]["result"].update(reason="manual_stop"),
        ]
        for mutation in mutations:
            value = deepcopy(self.proof)
            mutation(value)
            with self.subTest(value=value), self.assertRaises(handoff.HandoffError):
                self.validate(value)

    def test_strict_json_rejects_duplicate_trailing_nonfinite_or_nonobject(self):
        for body in (b'{"a":1,"a":2}', b'{}{}', b'{"a":NaN}', b'{"a":1e999}', b'[]'):
            with self.subTest(body=body), self.assertRaises(handoff.HandoffError):
                handoff._strict_json(body)
        self.assertEqual(handoff._strict_json(b' {"status":"ok"}\n'), {"status": "ok"})

    def test_terminal_unit_requires_no_processes_and_success(self):
        initial = unit_state()
        self.assertFalse(handoff._terminal_unit(initial, initial))
        with mock.patch.object(handoff, "_cgroup_empty", return_value=True):
            self.assertTrue(handoff._terminal_unit(unit_state(finished=True), initial))
            for key, value in (("MainPID", "456"), ("ControlPID", "9"), ("Result", "signal"),
                               ("ExecMainStatus", "2"), ("ExecMainCode", "2"), ("SubState", "failed")):
                changed = {**unit_state(finished=True), key: value}
                with self.subTest(key=key), self.assertRaises(handoff.HandoffError):
                    handoff._terminal_unit(changed, initial)
        with mock.patch.object(handoff, "_cgroup_empty", return_value=False), self.assertRaises(handoff.HandoffError):
            handoff._terminal_unit(unit_state(finished=True), initial)

    def test_unit_query_exact_identity_and_closed_environment(self):
        body = "\n".join(f"{key}={value}" for key, value in unit_state().items()).encode()
        complete = subprocess.CompletedProcess([], 0, stdout=body, stderr=b"")
        with mock.patch.object(handoff, "_validate_system_tools"), mock.patch.object(handoff.subprocess, "run", return_value=complete) as runner:
            self.assertEqual(handoff._unit_state(UNIT, INVOCATION), unit_state())
            self.assertEqual(runner.call_args.kwargs["timeout"], 10)
            self.assertNotIn("SALAD_API_KEY", runner.call_args.kwargs["env"])
            complete.stdout = body + b"\nMainPID=123"
            with self.assertRaises(handoff.HandoffError):
                handoff._unit_state(UNIT, INVOCATION)
            complete.stdout = body.replace(INVOCATION.encode(), b"3" * 32)
            with self.assertRaises(handoff.HandoffError):
                handoff._unit_state(UNIT, INVOCATION)

    def test_bound_output_exact_inode_and_later_stable_json(self):
        path = self.fixture.root / "output.json"
        path.touch(mode=0o600)
        actual_stat = os.stat

        def stat_output(target, *args, **kwargs):
            if str(target) == "/proc/123/fd/1":
                return actual_stat(path)
            return actual_stat(target, *args, **kwargs)

        with mock.patch.object(handoff.os, "stat", side_effect=stat_output):
            with handoff._bound_output(path, unit_state()) as read_result:
                path.write_text(json.dumps(self.proof))
                self.assertEqual(read_result(), self.proof)
                path.unlink()
                path.write_text("{}")
                path.chmod(0o600)
                with self.assertRaises(handoff.HandoffError):
                    read_result()

    def test_bound_output_rejects_nonempty_symlink_or_different_stdout(self):
        path = self.fixture.root / "output.json"
        path.write_text("{}")
        path.chmod(0o600)
        with self.assertRaises(handoff.HandoffError):
            with handoff._bound_output(path, unit_state()):
                pass
        path.unlink()
        path.symlink_to(self.fixture.controller / "control.json")
        with self.assertRaises(OSError):
            with handoff._bound_output(path, unit_state()):
                pass
        path.unlink()
        path.touch(mode=0o600)
        actual_stat = os.stat
        with mock.patch.object(handoff.os, "stat", side_effect=lambda target, *args, **kwargs:
                actual_stat(self.fixture.controller / "control.json") if str(target) == "/proc/123/fd/1" else actual_stat(target, *args, **kwargs)):
            with self.assertRaises(handoff.HandoffError):
                with handoff._bound_output(path, unit_state()):
                    pass

    @contextmanager
    def real_guard_readers(self):
        with mock.patch.object(handoff, "read_control_state", side_effect=lambda config: legacy._document(config.state_root / "control.json")), \
                mock.patch.object(handoff, "read_companion_status", side_effect=lambda registration: legacy._document(registration.status_path)):
            yield

    def test_completed_guard_holds_both_locks_without_mutating_raw_status(self):
        before = (self.fixture.controller / "status.json").read_bytes()
        with self.real_guard_readers(), handoff._hold_completed(self.predecessor, self.registration) as check:
            self.fixture.assert_held("controller_lock")
            self.fixture.assert_held("companion_lock")
            check()
            self.assertEqual(before, (self.fixture.controller / "status.json").read_bytes())
        self.assertFalse(self.fixture.inspect()["safe"])  # Ordinary guard is unchanged.
        with self.fixture.external_lock("controller_lock"), self.fixture.external_lock("companion_lock"):
            pass

    def test_completed_guard_rejects_incomplete_longform_and_active_gpu(self):
        with self.real_guard_readers():
            self.fixture.companion_status["jobs"].update(completed=3, incomplete=1)
            self.fixture.save()
            with self.assertRaises(handoff.HandoffError):
                with handoff._hold_completed(self.predecessor, self.registration):
                    pass
            self.fixture.companion_status["jobs"].update(completed=4, incomplete=0)
            self.fixture.status["lanes"]["gpu_readiness"]["active"] = 1
            self.fixture.save()
            with self.assertRaises(legacy.LegacyBusy):
                with handoff._hold_completed(self.predecessor, self.registration):
                    pass

    def test_completed_guard_detects_repeated_stop_and_releases_on_busy(self):
        with self.real_guard_readers():
            with handoff._hold_completed(self.predecessor, self.registration) as check:
                self.fixture.control["generation"] += 1
                self.fixture.save()
                with self.assertRaises(handoff.HandoffCancelled):
                    check()
            with self.fixture.external_lock("companion_lock"), self.assertRaises(BlockingIOError):
                with handoff._hold_completed(self.predecessor, self.registration):
                    pass
            with self.fixture.external_lock("controller_lock"):
                pass

    def test_intent_stop_before_completed_status_cancels(self):
        successor_initial = {**self.initial, "desired_state": "stopped", "generation": 0}
        with mock.patch.object(handoff, "read_control_state", side_effect=[successor_initial, self.final]):
            with self.assertRaises(handoff.HandoffCancelled):
                handoff._check_intents(self.predecessor, self.successor, self.initial, successor_initial,
                                       {**self.fixture.status, "completion_reason": None})

    def test_successor_repeated_stop_cancels(self):
        successor_initial = {**self.initial, "desired_state": "stopped", "generation": 0}
        with mock.patch.object(handoff, "read_control_state", return_value={**successor_initial, "generation": 1}):
            with self.assertRaises(handoff.HandoffCancelled):
                handoff._check_intents(self.predecessor, self.successor, self.initial, successor_initial, self.fixture.status)

    def test_live_supervisor_stop_transition_allows_wait_only(self):
        successor_initial = {**self.initial, "desired_state": "stopped", "generation": 0}
        stale = {**self.fixture.status, "lifecycle": "running", "completion_reason": None}
        for generation in (11, 12):
            current = {**self.final, "generation": generation}
            with mock.patch.object(handoff, "read_control_state", side_effect=[successor_initial, current]):
                self.assertEqual(handoff._check_intents(
                    self.predecessor, self.successor, self.initial, successor_initial,
                    stale, allow_completion_transition=True), current)
            # Even the same generation cannot pass the final proof with stale or
            # manually stopped source state. No Stop by itself proves completion.
            with self.assertRaises(handoff.HandoffError):
                handoff._validate_result(self.proof, self.predecessor, self.registration,
                                         self.initial, current, stale)
        with mock.patch.object(handoff, "read_control_state", side_effect=[successor_initial, {**self.final, "generation": 13}]):
            with self.assertRaises(handoff.HandoffCancelled):
                handoff._check_intents(self.predecessor, self.successor, self.initial, successor_initial,
                                       stale, allow_completion_transition=True)

    @contextmanager
    def runtime(self, *, proof=None, manual_stop=False, wait=False):
        successor_control = {**self.initial, "config_id": self.successor.config_id, "generation": 0, "desired_state": "stopped"}
        controls = {self.predecessor.config_id: deepcopy(self.initial), self.successor.config_id: successor_control}
        events = []

        def read_control(config):
            return deepcopy(controls[config.config_id])

        def source_status(_config):
            if not wait:
                controls[self.predecessor.config_id] = deepcopy(self.final)
            status = deepcopy(self.fixture.status)
            if manual_stop:
                status["completion_reason"] = None
            return status

        def cas(config, expected, desired):
            self.assertEqual(controls[config.config_id], expected)
            events.append(("cas", desired))
            controls[config.config_id] = {**expected, "desired_state": desired, "generation": expected["generation"] + 1}
            return deepcopy(controls[config.config_id])

        @contextmanager
        def held(*_args):
            events.append("locked")
            try:
                yield lambda: events.append("checked")
            finally:
                events.append("released")

        def execute(*_args):
            self.assertIn("released", events)
            self.assertEqual(events.count("released"), 1)
            self.assertEqual(controls[self.successor.config_id]["desired_state"], "running")
            events.append("exec")
            raise OSError("synthetic exec failure")

        with ExitStack() as stack:
            self.output = io.StringIO()
            stack.enter_context(redirect_stdout(self.output))
            stack.enter_context(mock.patch.dict(os.environ, {}, clear=True))
            stack.enter_context(mock.patch.object(handoff.ControllerUnitContext, "from_current_systemd_unit", return_value=SimpleNamespace(outer_unit="himr-operator-job-" + "4" * 32 + ".service")))
            stack.enter_context(mock.patch.object(handoff, "load_config", side_effect=lambda path, sha: self.predecessor if path == self.predecessor.path else self.successor))
            stack.enter_context(mock.patch.object(handoff, "load_registration_if_present", side_effect=lambda path, sha: self.registration if path == self.predecessor.path else self.successor_registration))
            stack.enter_context(mock.patch.object(handoff, "read_companion_status", return_value=deepcopy(self.fixture.companion_status)))
            stack.enter_context(mock.patch.object(handoff, "read_control_state", side_effect=read_control))
            stack.enter_context(mock.patch.object(handoff, "_source_status", side_effect=source_status))
            stack.enter_context(mock.patch.object(handoff, "_unit_state", side_effect=[unit_state(), unit_state()] + [unit_state(finished=not wait)] * 10))
            stack.enter_context(mock.patch.object(handoff, "_cgroup_empty", return_value=True))
            stack.enter_context(mock.patch.object(handoff, "_bound_output", return_value=nullcontext(lambda: deepcopy(proof or self.proof))))
            stack.enter_context(mock.patch.object(handoff, "_hold_completed", side_effect=held))
            stack.enter_context(mock.patch.object(handoff, "_set_control_if_unchanged", side_effect=cas))
            stack.enter_context(mock.patch.object(handoff.os, "execv", side_effect=execute))
            yield controls, events

    def run_handoff(self, **overrides):
        options = {
            "predecessor_unit": UNIT, "predecessor_invocation_id": INVOCATION,
            "predecessor_generation": 10, "predecessor_output": self.fixture.root / "output.json",
            "successor_config": self.successor.path, "successor_sha256": self.successor.physical_sha256,
            "max_wait_seconds": 20, "poll_seconds": 10,
        }
        options.update(overrides)
        handoff.handoff(self.predecessor.path, self.predecessor.physical_sha256, **options)

    def test_runtime_exec_failure_restores_only_its_start_and_releases_old_locks(self):
        with self.runtime() as (controls, events), self.assertRaisesRegex(OSError, "synthetic"):
            self.run_handoff()
        self.assertEqual([item for item in events if isinstance(item, tuple)], [("cas", "running"), ("cas", "stopped")])
        self.assertLess(events.index("released"), events.index("exec"))
        self.assertEqual(controls[self.successor.config_id]["desired_state"], "stopped")
        marker = json.loads(self.output.getvalue())
        self.assertEqual(marker["status"], "waiting_for_predecessor")
        self.assertEqual(marker["predecessor_invocation_id"], INVOCATION)
        self.assertFalse(marker["campaign_started"])

    def test_runtime_manual_stop_never_arms_or_executes(self):
        with self.runtime(manual_stop=True) as (_controls, events), self.assertRaises(handoff.HandoffCancelled):
            self.run_handoff()
        self.assertEqual(events, [])

    def test_runtime_invalid_completion_never_arms(self):
        proof = deepcopy(self.proof)
        proof["children"]["source_controller"]["result"]["completion_reason"] = "primary_pass_drained_with_parked_items"
        with self.runtime(proof=proof) as (_controls, events), self.assertRaises(handoff.HandoffError):
            self.run_handoff()
        self.assertEqual(events, [])

    def test_wait_is_bounded_and_each_sleep_at_most_ten_seconds(self):
        tick = [0.0]

        def sleep(seconds):
            self.assertGreater(seconds, 0)
            self.assertLessEqual(seconds, 10)
            tick[0] += seconds

        with self.runtime(wait=True) as (_controls, events), \
                mock.patch.object(handoff.time, "monotonic", side_effect=lambda: tick[0]), \
                mock.patch.object(handoff.time, "sleep", side_effect=sleep), \
                self.assertRaisesRegex(handoff.HandoffCancelled, "expired"):
            self.run_handoff(max_wait_seconds=25)
        self.assertEqual(tick[0], 25)
        self.assertEqual(events, [])

    def test_argument_bounds_reject_boolean_or_unbounded_values(self):
        for kwargs in ({"max_wait_seconds": 86401}, {"poll_seconds": 11}, {"poll_seconds": True}, {"predecessor_generation": -1}):
            with self.subTest(kwargs=kwargs), mock.patch.object(handoff, "_handoff") as run, self.assertRaises(handoff.HandoffError):
                self.run_handoff(**kwargs)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
