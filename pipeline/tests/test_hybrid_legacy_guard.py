from __future__ import annotations

from contextlib import contextmanager
import copy
import fcntl
import json
import os
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from pipeline import hybrid_legacy_guard as guard


class LegacyGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.controller = self.root / "controller"
        self.companion = self.root / "companion"
        self.controller.mkdir(mode=0o700)
        self.companion.mkdir(mode=0o700)
        self.config = {
            "control_path": str(self.controller / "control.json"),
            "status_path": str(self.controller / "status.json"),
            "controller_lock": str(self.controller / "controller.lock"),
            "companion_status_path": str(self.companion / "status.json"),
            "companion_lock": str(self.companion / "dispatch.lock"),
        }
        self.control = {
            "kind": "himr_autonomous_controller_control", "schema_version": 1,
            "config_id": "himrautocfg_" + "a" * 32, "generation": 12,
            "desired_state": "stopped", "requested_at": "2026-09-06T23:00:00Z",
        }
        self.status = {
            "kind": "himr_autonomous_controller_status", "schema_version": 1,
            "config_id": self.control["config_id"], "config_sha256": "b" * 64,
            "lifecycle": "stopped", "actual_state": "stopped", "desired_state": "stopped",
            "current_stage": None, "pid": 2**31 - 1, "started_at": "2026-09-06T22:00:00Z",
            "updated_at": "2026-09-06T23:00:00Z", "cycle": 4, "dispatch_sequence": 6,
            "scheduler_mode": "independent_lanes", "completion_reason": None,
            "campaign": {}, "consecutive_failures": 0, "last_error": None,
            "errors": [], "last_event": None,
            "monitor": {stage: None for stage in guard.STAGES},
            "stages": {stage: None for stage in guard.STAGES},
            "lanes": {stage: {
                "state": "idle", "active": 0, "limit": 1, "dispatch_id": None,
                "started_at": None, "last_status": None, "wait_reason": None,
                "last_transition_at": None,
            } for stage in guard.STAGES},
            "execution": {"accepting_new_work": False, "draining": False, "inflight_total": 0},
            "progress": {}, "pipeline_telemetry": {}, "throughput": {}, "storage": {},
            "recent_activity": [], "current_gpu_child": None, "safety": {},
        }
        self.companion_status = {
            "kind": "himr_longform_asr_campaign_status", "schema_version": 1,
            "source_controller": {"config_id": self.control["config_id"], "physical_sha256": "b" * 64},
            "campaign_config": {"config_id": "himrlongcfg_" + "c" * 32, "physical_sha256": "d" * 64},
            "lifecycle": "stopped", "expected_cold_backlog": 3,
            "discovered": {"cold_candidates": 3, "queue_candidates": 1, "total_candidates": 4},
            "jobs": {"unprepared": 1, "preprocessed": 0, "prepared": 1, "incomplete": 0, "completed": 2},
            "active_job": None, "updated_at": "2026-09-06T23:00:00Z", "last_error": None,
        }
        for key in ("controller_lock", "companion_lock"):
            Path(self.config[key]).touch(mode=0o600)
        self.save()

    def write(self, key: str, value: dict) -> None:
        path = Path(self.config[key])
        path.write_bytes(guard._canonical(value))
        path.chmod(0o600)

    def save(self) -> None:
        self.write("control_path", self.control)
        self.write("status_path", self.status)
        self.write("companion_status_path", self.companion_status)

    def inspect(self) -> dict:
        return guard.inspect_legacy([self.config])

    @contextmanager
    def external_lock(self, key: str):
        descriptor = os.open(self.config[key], os.O_RDWR)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield descriptor
        finally:
            os.close(descriptor)

    def assert_held(self, key: str) -> None:
        descriptor = os.open(self.config[key], os.O_RDWR)
        try:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)

    def test_stopped_inspection_is_readonly_and_does_not_flock(self) -> None:
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.root.rglob("*") if path.is_file()}
        with mock.patch.object(fcntl, "flock", side_effect=AssertionError("inspection must not lock")):
            result = self.inspect()
        after = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.root.rglob("*") if path.is_file()}
        self.assertTrue(result["safe"])
        self.assertEqual(result["guard_count"], 1)
        self.assertRegex(result["witness_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(before, after)

    def test_running_refused_before_any_execution_lock_open(self) -> None:
        self.control["desired_state"] = "running"
        self.save()
        actual_open = os.open

        def checked_open(path, flags, *args, **kwargs):
            self.assertNotIn(Path(path).name, {"controller.lock", "dispatch.lock", "control.lock", "service.lock"})
            return actual_open(path, flags, *args, **kwargs)

        with mock.patch.object(os, "open", side_effect=checked_open):
            self.assertFalse(self.inspect()["safe"])
            with self.assertRaisesRegex(guard.LegacyBusy, "control_not_stopped"):
                with guard.hold_legacy([self.config]):
                    self.fail("running controller entered hybrid context")

    def test_holds_both_existing_locks_through_entire_cycle(self) -> None:
        with guard.hold_legacy([self.config]) as held:
            self.assert_held("controller_lock")
            self.assert_held("companion_lock")
            held.check_unchanged()
        with self.external_lock("controller_lock"), self.external_lock("companion_lock"):
            pass
        with self.assertRaisesRegex(guard.LegacyBusy, "not_held"):
            held.check_unchanged()

    def test_inherited_descriptors_remain_cloexec_until_explicit_handoff(self) -> None:
        with guard.hold_legacy([self.config]) as held:
            descriptors = held.inherited_fds
            self.assertIsInstance(descriptors, tuple)
            self.assertEqual(len(descriptors), 2)
            self.assertTrue(all(not os.get_inheritable(descriptor) for descriptor in descriptors))
        with self.assertRaisesRegex(guard.LegacyBusy, "not_held"):
            _ = held.inherited_fds

    def test_child_retains_legacy_locks_after_parent_context_closes(self) -> None:
        child = None
        try:
            with guard.hold_legacy([self.config]) as held:
                child = subprocess.Popen(
                    [sys.executable, "-c", "import sys; print('ready', flush=True); sys.stdin.readline()"],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, pass_fds=held.inherited_fds,
                )
                self.assertEqual(child.stdout.readline(), "ready\n")
            # Closing the parent's descriptors models the lease behavior after
            # parent death: no LOCK_UN was sent to the shared descriptions.
            self.assert_held("controller_lock")
            self.assert_held("companion_lock")
            child.communicate("finish\n", timeout=5)
            self.assertEqual(child.returncode, 0)
            with self.external_lock("controller_lock"), self.external_lock("companion_lock"):
                pass
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.communicate(timeout=5)

    def test_lock_content_never_written_or_created(self) -> None:
        actual_open = os.open

        def checked_open(path, flags, *args, **kwargs):
            self.assertFalse(flags & os.O_CREAT)
            self.assertFalse(flags & os.O_TRUNC)
            self.assertNotIn(Path(path).name, {"control.lock", "service.lock"})
            return actual_open(path, flags, *args, **kwargs)

        with mock.patch.object(os, "open", side_effect=checked_open):
            with guard.hold_legacy([self.config]):
                pass
        for key in ("controller_lock", "companion_lock"):
            self.assertEqual(Path(self.config[key]).read_bytes(), b"")

    def test_busy_controller_lock_fails_closed(self) -> None:
        with self.external_lock("controller_lock"):
            with self.assertRaisesRegex(guard.LegacyBusy, "lock_busy"):
                with guard.hold_legacy([self.config]):
                    self.fail("busy controller lock accepted")
        with self.external_lock("companion_lock"):
            pass

    def test_busy_companion_lock_fails_closed_and_releases_other_lock(self) -> None:
        with self.external_lock("companion_lock"):
            with self.assertRaisesRegex(guard.LegacyBusy, "lock_busy"):
                with guard.hold_legacy([self.config]):
                    self.fail("busy companion lock accepted")
        with self.external_lock("controller_lock"):
            pass

    def test_exception_in_cycle_releases_both_locks(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "synthetic"):
            with guard.hold_legacy([self.config]):
                raise RuntimeError("synthetic")
        with self.external_lock("controller_lock"), self.external_lock("companion_lock"):
            pass

    def test_interrupted_second_lock_acquisition_releases_first_lock(self) -> None:
        actual_flock = fcntl.flock
        calls = 0

        def interrupt_second(descriptor, operation):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt
            actual_flock(descriptor, operation)

        with mock.patch.object(fcntl, "flock", side_effect=interrupt_second):
            with self.assertRaises(KeyboardInterrupt):
                with guard.hold_legacy([self.config]):
                    self.fail("interrupted acquisition entered context")
        with self.external_lock("controller_lock"), self.external_lock("companion_lock"):
            pass

    def test_generation_change_is_detected_even_if_still_stopped(self) -> None:
        with guard.hold_legacy([self.config]) as held:
            self.control["generation"] += 1
            self.write("control_path", self.control)
            with self.assertRaisesRegex(guard.LegacyBusy, "state_changed"):
                held.check_unchanged()

    def test_start_intent_change_is_detected_without_blocking_control_write(self) -> None:
        with guard.hold_legacy([self.config]) as held:
            self.control["desired_state"] = "running"
            self.control["generation"] += 1
            self.write("control_path", self.control)
            with self.assertRaisesRegex(guard.LegacyBusy, "control_not_stopped"):
                held.check_unchanged()

    def test_state_change_between_inspection_and_lock_is_rejected(self) -> None:
        actual_flock = fcntl.flock
        changed = False

        def change_after_lock(descriptor, operation):
            nonlocal changed
            actual_flock(descriptor, operation)
            if not changed:
                self.control["generation"] += 1
                self.write("control_path", self.control)
                changed = True

        with mock.patch.object(fcntl, "flock", side_effect=change_after_lock):
            with self.assertRaisesRegex(guard.LegacyBusy, "state_changed"):
                with guard.hold_legacy([self.config]):
                    self.fail("changed state accepted")
        with self.external_lock("controller_lock"), self.external_lock("companion_lock"):
            pass

    def test_missing_state_and_locks_not_created(self) -> None:
        for key in self.config:
            with self.subTest(key=key):
                target = Path(self.config[key])
                backup = target.with_name(target.name + ".saved")
                target.rename(backup)
                self.assertFalse(self.inspect()["safe"])
                with self.assertRaises(guard.LegacyBusy):
                    with guard.hold_legacy([self.config]):
                        pass
                self.assertFalse(target.exists())
                backup.rename(target)

    def test_actual_running_stopping_failed_unknown_states_refused(self) -> None:
        for state in ("running", "starting", "stopping", "failed", "blocked", "completed", "mystery"):
            with self.subTest(state=state):
                self.status["actual_state"] = state
                self.status["lifecycle"] = state
                self.write("status_path", self.status)
                self.assertFalse(self.inspect()["safe"])

    def test_stopped_with_historical_error_is_allowed(self) -> None:
        self.status["last_error"] = {"type": "HistoricalError", "message": "private details not copied to guard output"}
        self.status["errors"] = [self.status["last_error"]]
        self.save()
        self.assertTrue(self.inspect()["safe"])

    def test_live_or_recycled_controller_pid_is_conservative_hold(self) -> None:
        self.status["pid"] = os.getpid()
        self.save()
        self.assertEqual(self.inspect()["reasons"], ["legacy_controller_process_present"])

    def test_active_execution_markers_refused(self) -> None:
        for field, value in (("accepting_new_work", True), ("draining", True), ("inflight_total", 1), ("inflight_total", False)):
            with self.subTest(field=field, value=value):
                changed = copy.deepcopy(self.status)
                changed["execution"][field] = value
                self.write("status_path", changed)
                self.assertFalse(self.inspect()["safe"])

    def test_active_lane_and_missing_lane_refused(self) -> None:
        changed = copy.deepcopy(self.status)
        changed["lanes"]["preprocess"]["active"] = 1
        self.write("status_path", changed)
        self.assertFalse(self.inspect()["safe"])
        changed["lanes"].pop("preprocess")
        self.write("status_path", changed)
        self.assertFalse(self.inspect()["safe"])

    def test_gpu_child_must_be_quiesced_in_both_status_projections(self) -> None:
        for gpu in ({"active_children": 1, "current_gpu_child": "unit"}, {}, {"active_children": 0, "current_gpu_child": "unit"}):
            with self.subTest(gpu=gpu):
                changed = copy.deepcopy(self.status)
                changed["monitor"]["gpu_readiness"] = gpu
                changed["stages"]["gpu_readiness"] = gpu
                self.write("status_path", changed)
                self.assertFalse(self.inspect()["safe"])
        gpu = {"active_children": 0, "current_gpu_child": None}
        self.status["monitor"]["gpu_readiness"] = gpu
        self.status["stages"]["gpu_readiness"] = gpu
        self.save()
        self.assertTrue(self.inspect()["safe"])

    def test_companion_ready_allowed_but_waiting_running_faulted_rejected(self) -> None:
        for state in ("ready", "waiting", "running", "faulted"):
            with self.subTest(state=state):
                self.companion_status["lifecycle"] = state
                self.write("companion_status_path", self.companion_status)
                self.assertEqual(self.inspect()["safe"], state == "ready")

    def test_companion_cross_campaign_or_active_job_refused(self) -> None:
        self.companion_status["source_controller"]["physical_sha256"] = "f" * 64
        self.save()
        self.assertFalse(self.inspect()["safe"])
        self.companion_status["source_controller"]["physical_sha256"] = "b" * 64
        self.companion_status["active_job"] = "himrlongjob_" + "a" * 32
        self.save()
        self.assertFalse(self.inspect()["safe"])

    def test_noncanonical_duplicate_and_nonfinite_documents_refused(self) -> None:
        for body in (b'{"desired_state":"stopped","desired_state":"running"}', b'{"value":NaN}', b'{"value":1e400}', json.dumps(self.control).encode()):
            with self.subTest(body=body[:40]):
                Path(self.config["control_path"]).write_bytes(body)
                self.assertFalse(self.inspect()["safe"])

    def test_no_paths_or_document_values_leak_in_failure_reasons(self) -> None:
        Path(self.config["status_path"]).write_bytes(b"SECRET_TOKEN_DO_NOT_PRINT")
        result = json.dumps(self.inspect())
        self.assertNotIn("SECRET_TOKEN", result)
        self.assertNotIn(str(self.root), result)

    def test_nonprivate_files_and_parent_refused(self) -> None:
        for key in self.config:
            with self.subTest(key=key):
                path = Path(self.config[key])
                path.chmod(0o644)
                self.assertFalse(self.inspect()["safe"])
                path.chmod(0o600)
        self.controller.chmod(0o755)
        self.assertFalse(self.inspect()["safe"])

    @contextmanager
    def private_group_accounts(self, *, supplemental=None, primary=None):
        info = self.root.stat()
        owner = SimpleNamespace(pw_uid=info.st_uid, pw_gid=info.st_gid, pw_name="fixture-owner")
        group = SimpleNamespace(gr_gid=info.st_gid, gr_mem=[] if supplemental is None else supplemental)
        accounts = [owner] if primary is None else primary
        with mock.patch.object(guard.pwd, "getpwuid", return_value=owner), mock.patch.object(guard.grp, "getgrgid", return_value=group), mock.patch.object(guard.pwd, "getpwall", return_value=accounts):
            yield owner, group

    def test_group_writable_ancestor_allowed_only_for_owner_private_primary_group(self) -> None:
        self.root.chmod(0o775)
        with self.private_group_accounts(supplemental=["fixture-owner"]):
            self.assertTrue(self.inspect()["safe"])
            with guard.hold_legacy([self.config]) as held:
                held.check_unchanged()
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o775)

    def test_group_writable_ancestor_rejects_other_supplemental_member(self) -> None:
        self.root.chmod(0o775)
        with self.private_group_accounts(supplemental=["fixture-owner", "other-user"]):
            self.assertEqual(self.inspect()["reasons"], ["legacy_directory_unsafe"])

    def test_group_writable_ancestor_rejects_other_primary_group_account(self) -> None:
        self.root.chmod(0o775)
        other = SimpleNamespace(pw_uid=os.geteuid() + 1, pw_gid=self.root.stat().st_gid, pw_name="other-user")
        with self.private_group_accounts() as (owner, _group), mock.patch.object(guard.pwd, "getpwall", return_value=[owner, other]):
            self.assertEqual(self.inspect()["reasons"], ["legacy_directory_unsafe"])

    def test_private_group_exception_rejects_missing_or_failed_account_lookup(self) -> None:
        self.root.chmod(0o775)
        for name in ("getpwuid", "getpwall"):
            with self.subTest(name=name), self.private_group_accounts(), mock.patch.object(guard.pwd, name, side_effect=KeyError("missing fixture identity")):
                self.assertFalse(self.inspect()["safe"])
        with self.private_group_accounts(), mock.patch.object(guard.grp, "getgrgid", side_effect=OSError("lookup unavailable")):
            self.assertFalse(self.inspect()["safe"])
        with self.private_group_accounts(primary=[]):
            self.assertFalse(self.inspect()["safe"])

    def test_private_group_exception_never_allows_world_write_or_nonprivate_state_parent(self) -> None:
        with self.private_group_accounts():
            self.root.chmod(0o777)
            self.assertFalse(self.inspect()["safe"])
            self.root.chmod(0o775)
            self.controller.chmod(0o770)
            self.assertFalse(self.inspect()["safe"])

    def test_group_membership_change_revokes_held_guard(self) -> None:
        self.root.chmod(0o775)
        with self.private_group_accounts() as (_owner, group), guard.hold_legacy([self.config]) as held:
            group.gr_mem.append("new-peer")
            with self.assertRaisesRegex(guard.LegacyBusy, "directory_unsafe"):
                held.check_unchanged()

    def test_symlink_leaf_and_ancestor_refused(self) -> None:
        path = Path(self.config["status_path"])
        backup = self.root / "saved-status.json"
        path.rename(backup)
        path.symlink_to(backup)
        self.assertFalse(self.inspect()["safe"])
        path.unlink()
        backup.rename(path)
        alias = self.root / "alias"
        alias.symlink_to(self.controller, target_is_directory=True)
        changed = dict(self.config)
        for key in ("control_path", "status_path", "controller_lock"):
            changed[key] = str(alias / Path(changed[key]).name)
        self.assertFalse(guard.inspect_legacy([changed])["safe"])

    def test_hardlink_lock_refused(self) -> None:
        os.link(self.config["controller_lock"], self.root / "hardlink")
        self.assertFalse(self.inspect()["safe"])

    def test_lock_replacement_during_cycle_refused(self) -> None:
        with guard.hold_legacy([self.config]) as held:
            path = Path(self.config["controller_lock"])
            path.rename(path.with_name("old.lock"))
            path.touch(mode=0o600)
            with self.assertRaisesRegex(guard.LegacyBusy, "lock_changed"):
                held.check_unchanged()

    def test_empty_repeated_and_invalid_configuration_fail_closed(self) -> None:
        for configs in ([], [self.config, self.config], [{**self.config, "extra": "no"}], [{**self.config, "control_path": "relative.json"}], "bad"):
            with self.subTest(configs=str(configs)[:40]):
                self.assertFalse(guard.inspect_legacy(configs)["safe"])

    def test_missing_controller_status_field_fails_closed(self) -> None:
        self.status.pop("current_gpu_child")
        self.save()
        self.assertFalse(self.inspect()["safe"])

    def test_malformed_companion_counts_fail_closed(self) -> None:
        self.companion_status["jobs"]["completed"] += 1
        self.save()
        self.assertFalse(self.inspect()["safe"])


if __name__ == "__main__":
    unittest.main()
