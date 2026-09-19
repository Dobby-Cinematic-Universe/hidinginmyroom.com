"""Offline recovery-coordinator tests; never use campaign, media, or network paths."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager, nullcontext
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import fcntl
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from autonomous_controller import quarantine_recovery as recovery
from pipeline import hybrid_legacy_guard


NOW = "2026-09-12T15:00:00Z"


class AcquisitionError(RuntimeError):
    pass


class FixtureRunner:
    """Small filesystem double; only receipt bytes created under TemporaryDirectory."""

    acquire = SimpleNamespace(AcquisitionError=AcquisitionError)
    canonical_bytes = staticmethod(recovery.proof.canonical_bytes)
    sha256_bytes = staticmethod(lambda value: hashlib.sha256(value).hexdigest())

    def __init__(self) -> None:
        self.results: dict[str, dict] = {}
        self.failures: dict[str, list] = {}
        self.materialize_queue = SimpleNamespace(validate_utc_timestamp=self._timestamp)
        self._replay_bundle = mock.Mock(side_effect=lambda bundle, _software: bundle)
        self._capacity_allows = mock.Mock(return_value=True)
        self._dispatch_one = mock.Mock(side_effect=self._dispatch)

    @staticmethod
    def _timestamp(value, _label):
        if datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ") != value:
            raise ValueError("invalid timestamp")

    @staticmethod
    def _result_path(order):
        return Path(order["fixture_result_path"])

    @staticmethod
    def _safe_directory(path, _label, *, required_mode=None):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or (required_mode is not None and stat.S_IMODE(info.st_mode) != required_mode):
            raise RuntimeError("unsafe fixture directory")

    @staticmethod
    def _directory_names(path, _label):
        return {entry.name for entry in path.iterdir()}

    @staticmethod
    def _stable_read(path, *, maximum, label, required_mode):
        del label
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != required_mode:
            raise RuntimeError("unsafe fixture receipt")
        body = path.read_bytes()
        if len(body) > maximum:
            raise RuntimeError("fixture receipt exceeds cap")
        return body, info

    @staticmethod
    def _strict_json(body, _label):
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate key")
                value[key] = item
            return value
        return json.loads(body, object_pairs_hook=unique)

    @staticmethod
    def _write_immutable_receipt(path, value, _label):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        body = recovery.proof.pretty_bytes(value)
        if path.exists():
            if path.read_bytes() != body:
                raise RuntimeError("immutable receipt differs")
            return
        path.write_bytes(body)
        path.chmod(0o400)

    @staticmethod
    def _software_document():
        return {"fixture": True}

    def _scan_failure_states(self, bundle):
        return self.failures[str(bundle["path"])]

    def _inspect_failure_state(self, bundle, entry, _order):
        return self.failures[str(bundle["path"])][entry["queue_ordinal"] - 1]

    def _inspect_result(self, order):
        return self.results.get(order["job_id"])

    def _dispatch(self, order, _remaining):
        state = {"job_id": order["job_id"], "status": "completed"}
        self.results[order["job_id"]] = state
        return state, "completed"


class RecoveryFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runner = FixtureRunner()
        self.entries = [{"queue_ordinal": ordinal} for ordinal in range(1, 4)]
        self.orders = [{
            "job_id": f"acq-{'a' * 32}-{ordinal:06d}",
            "adapter": "direct_http", "source": {"access_state": "public"},
            "adapter_config": {"expected_byte_count": 1000, "url": f"https://example.invalid/{ordinal}.mp4"},
            "limits": {"free_space_floor_bytes": 100},
            "fixture_result_path": str(self.root / f"result-{ordinal}.json"),
        } for ordinal in range(1, 4)]
        self.quarantines = [{"receipt_sha256": str(ordinal) * 64} for ordinal in range(1, 4)]
        self.bundle = {
            "path": self.root / "bundle.json", "body": b"fixture bundle",
            "manifest": {"bundle_id": "fixture-bundle", "work_orders": self.entries},
            "orders": self.orders,
        }
        self.runner.failures[str(self.bundle["path"])] = [
            {"quarantine": quarantine} for quarantine in self.quarantines
        ]
        self.schedule = {
            "schedule_id": "fixture-schedule", "queue": {
                "manifest_path": str(self.bundle["path"]),
                "manifest_sha256": self.runner.sha256_bytes(self.bundle["body"]),
                "bundle_id": self.bundle["manifest"]["bundle_id"],
            },
        }
        self.reference = {
            "path": str(self.root / "schedule.json"), "sha256": self.runner.sha256_bytes(b"fixture schedule"),
            "schedule_id": self.schedule["schedule_id"],
        }
        self.config = SimpleNamespace(
            path=self.root / "controller.json", physical_sha256="c" * 64,
            state_root=self.root / "state",
            config_id="himrautocfg_" + "b" * 32,
            document={"state_root": str(self.root / "state")},
            section=lambda name: {"schedules": [self.reference]} if name == "campaign" else None,
        )
        self.runner._load_bundle = mock.Mock(return_value=self.bundle)
        self.modules = SimpleNamespace(queue_runner=self.runner, background=SimpleNamespace(
            load_schedule=mock.Mock(return_value=(self.schedule, Path(self.reference["path"]), b"fixture schedule")),
        ))
        self.plan = recovery.build_retry_plan(self.config, 3, modules=self.modules)
        self.plan_path = self.root / "plan.json"
        self.plan_sha = "d" * 64
        self.control = {"desired_state": "stopped", "generation": 10}

    def start_receipt(self, number=1, *, item=None, root=None, **changes):
        item = item or self.plan["entries"][0]
        root = root or self.root / "attempts" / item["job_id"]
        core = {
            "schema_version": 1, "kind": "himr_quarantine_retry_attempt_started",
            "authorization_plan_sha256": self.plan_sha, "job_id": item["job_id"],
            "result_path": item["result_path"], "attempt_number": number, "started_at": NOW,
            **changes,
        }
        return recovery._receipt(root / f"{number:06d}.started.json", core, self.runner)

    def attempts(self, item=None):
        item = item or self.plan["entries"][0]
        return recovery._attempts(self.root / "attempts" / item["job_id"], item, self.plan_sha, 3, self.runner)

    def finish_receipt(self, start, status="transient_failure", error="temporary", *, item=None):
        item = item or self.plan["entries"][0]
        recovery._finish(self.root / "attempts" / item["job_id"], start, status, error, self.runner)

    @contextmanager
    def runtime(self, *, count=1):
        plan = deepcopy(self.plan)
        plan["entries"] = plan["entries"][:count]
        held = SimpleNamespace(check_unchanged=mock.Mock(), inherited_fds=())
        held_active = {"value": False}

        @contextmanager
        def hold(_guards):
            held_active["value"] = True
            try:
                yield held
            finally:
                held_active["value"] = False

        contexts = [
            (self.reference, self.schedule, self.bundle, entry, order, quarantine)
            for entry, order, quarantine in zip(self.entries, self.orders, self.quarantines)
        ][:count]
        with ExitStack() as stack:
            def patch(target, name, **kwargs):
                return stack.enter_context(mock.patch.object(target, name, **kwargs))
            patch(recovery.proof, "validate_plan", return_value=plan)
            seal = patch(recovery.proof, "seal_completion")
            patch(recovery.proof, "completion_path", side_effect=lambda _b, _e, order: self.root / (order["job_id"] + ".proof.json"))
            patch(recovery, "load_config", return_value=self.config)
            patch(recovery, "_load_modules", return_value=self.modules)
            patch(recovery, "_inspect_bounded", side_effect=lambda order, runner, _deadline: runner._inspect_result(order))
            patch(recovery, "_dispatch_bounded", side_effect=lambda order, runner, remaining: runner._dispatch_one(order, remaining)[0])
            patch(recovery, "_contexts", return_value=contexts)
            backend = patch(recovery, "SealedArchiveBackend").return_value
            patch(recovery, "_guards", return_value=[{"fixture": True}])
            patch(recovery, "_signals", side_effect=nullcontext)
            patch(hybrid_legacy_guard, "hold_legacy", side_effect=hold)
            patch(recovery, "utc_now", return_value=NOW)
            patch(recovery, "read_control_state", side_effect=lambda _config: deepcopy(self.control))
            patch(recovery, "_atomic_mutable_json", side_effect=lambda path, value, **_kw: path.write_bytes(recovery.proof.pretty_bytes(value)))
            backoff = patch(recovery, "_backoff")
            # Resume safeguards are verified below; never discover a live unit.
            if hasattr(recovery, "ControllerUnitContext"):
                patch(recovery.ControllerUnitContext, "from_current_systemd_unit")
            request_start = patch(recovery, "request_start", create=True)
            request_stop = patch(recovery, "request_stop", create=True)
            control_cas = patch(recovery, "_set_control_if_unchanged", create=True,
                                side_effect=lambda _config, _expected, desired: {**self.control, "generation": 11, "desired_state": desired})
            execv = patch(recovery.os, "execv", side_effect=OSError("fixture exec failure"))
            yield SimpleNamespace(plan=plan, held=held, held_active=held_active, seal=seal,
                                  backend=backend, backoff=backoff, request_start=request_start,
                                  request_stop=request_stop, control_cas=control_cas, execv=execv)


class RecoveryPlanTests(RecoveryFixture):
    def test_plan_includes_only_quarantines_and_preserves_exact_order_authority(self):
        self.runner.failures[str(self.bundle["path"])][1] = {"quarantine": None}
        plan = recovery.build_retry_plan(self.config, 2, modules=self.modules)
        self.assertEqual([entry["ordinal"] for entry in plan["entries"]], [1, 3])
        self.assertEqual(plan["entries"][0]["work_order_sha256"], self.runner.sha256_bytes(self.runner.canonical_bytes(self.orders[0])))
        self.assertEqual(plan["entries"][0]["result_path"], self.orders[0]["fixture_result_path"])
        self.assertEqual(plan["plan_sha256"], recovery._hash({key: value for key, value in plan.items() if key != "plan_sha256"}))

    def test_expected_count_is_exact_and_bounded(self):
        for count in (True, 0, 101, 2, 4):
            with self.subTest(count=count), self.assertRaises(recovery.RecoveryError):
                recovery.build_retry_plan(self.config, count, modules=self.modules)

    def test_existing_result_requires_original_plan_replay(self):
        self.runner._result_path(self.orders[0]).write_text("fixture", encoding="utf-8")
        with self.assertRaisesRegex(recovery.RecoveryError, "original recovery plan"):
            recovery.build_retry_plan(self.config, 3, modules=self.modules)

    def test_dangling_result_symlink_cannot_be_new_authority(self):
        self.runner._result_path(self.orders[0]).symlink_to(self.root / "missing")
        with self.assertRaises(recovery.RecoveryError):
            recovery.build_retry_plan(self.config, 3, modules=self.modules)

    def test_nonpublic_or_nonhttp_work_is_rejected(self):
        for key, value in (("adapter", "other"), ("source", {"access_state": "private"})):
            with self.subTest(key=key):
                original = self.orders[0][key]
                self.orders[0][key] = value
                with self.assertRaisesRegex(recovery.RecoveryError, "public HTTP"):
                    recovery.build_retry_plan(self.config, 3, modules=self.modules)
                self.orders[0][key] = original

    def test_duplicate_job_identity_is_rejected(self):
        self.orders[1]["job_id"] = self.orders[0]["job_id"]
        with self.assertRaisesRegex(recovery.RecoveryError, "repeat"):
            recovery.build_retry_plan(self.config, 3, modules=self.modules)

    def test_schedule_and_bundle_physical_pins_are_enforced(self):
        for target, key in ((self.reference, "sha256"), (self.schedule["queue"], "manifest_sha256")):
            with self.subTest(key=key):
                original = target[key]
                target[key] = "f" * 64
                with self.assertRaisesRegex(recovery.RecoveryError, "sealed"):
                    list(recovery._bundles(self.config, self.modules))
                target[key] = original

    def test_contexts_reject_modified_quarantine_url_ordinal_and_campaign(self):
        for key, value in (("quarantine_receipt_sha256", "f" * 64), ("url", "https://example.invalid/different"),
                           ("ordinal", 999), ("manifest_path", str(self.root / "unregistered.json"))):
            with self.subTest(key=key):
                plan = deepcopy(self.plan)
                plan["entries"][0][key] = value
                with self.assertRaises(recovery.RecoveryError):
                    recovery._contexts(plan, self.config, self.modules)

    def test_contexts_reject_quarantine_removed_since_plan(self):
        self.runner.failures[str(self.bundle["path"])][0]["quarantine"] = None
        with self.assertRaises(recovery.RecoveryError):
            recovery._contexts(self.plan, self.config, self.modules)


class RecoveryLedgerTests(RecoveryFixture):
    def test_missing_and_empty_attempt_directories_admit_no_attempt(self):
        self.assertEqual(self.attempts(), [])
        (self.root / "attempts" / self.orders[0]["job_id"]).mkdir(parents=True, mode=0o700)
        self.assertEqual(self.attempts(), [])

    def test_three_contiguous_finished_attempts_replay(self):
        for number in range(1, 4):
            self.finish_receipt(self.start_receipt(number))
        rows = self.attempts()
        self.assertEqual([row["start"]["attempt_number"] for row in rows], [1, 2, 3])
        self.assertTrue(all(row["finish"]["status"] == "transient_failure" for row in rows))

    def test_fourth_attempt_and_noncontiguous_attempts_rejected(self):
        self.start_receipt(2)
        with self.assertRaises(recovery.RecoveryError):
            self.attempts()
        for number in (1, 3, 4):
            self.start_receipt(number)
        with self.assertRaises(recovery.RecoveryError):
            self.attempts()

    def test_unfinished_nonfinal_attempt_rejected(self):
        self.start_receipt(1)
        self.start_receipt(2)
        with self.assertRaises(recovery.RecoveryError):
            self.attempts()

    def test_extra_unbound_file_rejected(self):
        self.start_receipt()
        (self.root / "attempts" / self.orders[0]["job_id"] / "unexpected.json").write_text("{}")
        with self.assertRaises(recovery.RecoveryError):
            self.attempts()

    def test_start_authority_changes_rejected_even_with_new_receipt_hash(self):
        for key, value in (("authorization_plan_sha256", "f" * 64), ("job_id", "other"),
                           ("result_path", str(self.root / "other.json")), ("attempt_number", 2)):
            with self.subTest(key=key):
                root = self.root / ("case-" + key)
                self.start_receipt(root=root, **{key: value})
                with self.assertRaises(recovery.RecoveryError):
                    recovery._attempts(root, self.plan["entries"][0], self.plan_sha, 3, self.runner)

    def test_receipt_byte_tamper_and_noncanonical_json_rejected(self):
        start = self.start_receipt()
        path = self.root / "attempts" / self.orders[0]["job_id"] / "000001.started.json"
        path.chmod(0o600)
        path.write_bytes(json.dumps(start).encode())
        path.chmod(0o400)
        with self.assertRaisesRegex(recovery.RecoveryError, "canonical"):
            self.attempts()
        start["job_id"] = "tampered"
        path.chmod(0o600)
        path.write_bytes(recovery.proof.pretty_bytes(start))
        path.chmod(0o400)
        with self.assertRaisesRegex(recovery.RecoveryError, "hash-bound"):
            self.attempts()

    def test_finish_must_bind_start_receipt(self):
        self.start_receipt()
        path = self.root / "attempts" / self.orders[0]["job_id"] / "000001.finished.json"
        recovery._receipt(path, {
            "schema_version": 1, "kind": "himr_quarantine_retry_attempt_finished",
            "started_receipt_sha256": "f" * 64, "finished_at": NOW,
            "status": "transient_failure", "error": "fixture",
        }, self.runner)
        with self.assertRaisesRegex(recovery.RecoveryError, "differs from its start"):
            self.attempts()


class RecoveryExecutionTests(RecoveryFixture):
    def test_success_seals_proof_before_finished_and_does_not_start_campaign(self):
        with self.runtime() as run:
            def proof_before_finished(*_args, **_kwargs):
                self.assertTrue(run.held_active["value"])
                self.assertIsNone(self.attempts()[-1]["finish"])
            run.seal.side_effect = proof_before_finished
            result = recovery.run_plan(self.plan_path, self.plan_sha)
            self.assertEqual(result["completed_items"], 1)
            self.assertEqual(result["failed_items"], 0)
            self.assertFalse(result["campaign_resumed"])
            run.request_start.assert_not_called()
            run.control_cas.assert_not_called()
            run.execv.assert_not_called()
            self.assertEqual(self.attempts()[0]["finish"]["status"], "completed")
            self.runner._dispatch_one.assert_called_once()

    def test_transient_failures_stop_at_three_attempts_with_two_backoffs(self):
        self.runner._dispatch_one.side_effect = AcquisitionError("HTTP request failed with status 503")
        with self.runtime() as run:
            result = recovery.run_plan(self.plan_path, self.plan_sha)
            self.assertEqual(self.runner._dispatch_one.call_count, 3)
            self.assertEqual(run.backoff.call_count, 2)
            self.assertTrue(all(call.args[0] == 120 for call in run.backoff.call_args_list))
            self.assertEqual(result["failed_items"], 1)
            self.assertEqual(result["completed_items"], 0)
            run.seal.assert_not_called()
        self.assertEqual(len(self.attempts()), 3)

    def test_exhausted_ledger_never_dispatches_a_fourth_retry(self):
        for number in range(1, 4):
            self.finish_receipt(self.start_receipt(number))
        with self.runtime() as run:
            result = recovery.run_plan(self.plan_path, self.plan_sha)
            self.assertEqual(result["failed_items"], 1)
            self.runner._dispatch_one.assert_not_called()
            run.backoff.assert_not_called()

    def test_resuming_interrupted_attempt_consumes_prior_attempt_and_backoff(self):
        self.start_receipt()
        with self.runtime() as run:
            recovery.run_plan(self.plan_path, self.plan_sha)
            self.runner._dispatch_one.assert_called_once()
            run.backoff.assert_called_once()
        self.assertEqual([row["finish"]["status"] for row in self.attempts()], ["interrupted", "completed"])

    def test_interrupted_published_result_is_reused_without_another_download(self):
        self.start_receipt()
        self.runner.results[self.orders[0]["job_id"]] = {"status": "completed", "job_id": self.orders[0]["job_id"]}
        with self.runtime() as run:
            result = recovery.run_plan(self.plan_path, self.plan_sha)
            self.runner._dispatch_one.assert_not_called()
            run.seal.assert_called_once()
            self.assertEqual(result["completed_items"], 1)
            self.assertEqual(self.attempts()[0]["finish"]["status"], "completed")

    def test_already_completed_retry_is_rechecked_but_not_downloaded_again(self):
        start = self.start_receipt()
        self.finish_receipt(start, "completed", None)
        self.runner.results[self.orders[0]["job_id"]] = {"status": "completed"}
        with self.runtime() as run:
            recovery.run_plan(self.plan_path, self.plan_sha)
            self.runner._dispatch_one.assert_not_called()
            run.seal.assert_called_once()
        self.assertEqual(len(self.attempts()), 1)

    def test_result_without_retry_start_is_never_adopted(self):
        self.runner.results[self.orders[0]["job_id"]] = {"status": "completed"}
        with self.runtime() as run, self.assertRaisesRegex(recovery.RecoveryError, "attempt start"):
            recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
        self.runner._dispatch_one.assert_not_called()
        run.seal.assert_not_called()
        run.execv.assert_not_called()

    def test_proof_without_result_fails_closed(self):
        (self.root / (self.orders[0]["job_id"] + ".proof.json")).write_text("fixture")
        with self.runtime() as run, self.assertRaisesRegex(recovery.RecoveryError, "without its completed result"):
            recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
        self.runner._dispatch_one.assert_not_called()
        run.execv.assert_not_called()

    def test_published_result_then_acquisition_exception_does_not_retry(self):
        def publish_then_raise(order, remaining):
            self.runner._dispatch(order, remaining)
            raise AcquisitionError("HTTP request failed with status 503")
        self.runner._dispatch_one.side_effect = publish_then_raise
        with self.runtime() as run:
            result = recovery.run_plan(self.plan_path, self.plan_sha)
            self.assertEqual(result["completed_items"], 1)
            self.runner._dispatch_one.assert_called_once()
            run.seal.assert_called_once()
            run.backoff.assert_not_called()

    def test_fatal_failure_does_not_retry_or_resume(self):
        self.runner._dispatch_one.side_effect = AcquisitionError("HTTP request failed with status 403")
        with self.runtime() as run, self.assertRaisesRegex(recovery.RecoveryError, "non-transient"):
            recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
        self.runner._dispatch_one.assert_called_once()
        run.backoff.assert_not_called()
        run.execv.assert_not_called()
        run.control_cas.assert_not_called()
        self.assertEqual(self.attempts()[0]["finish"]["status"], "fatal_failure")

    def test_prior_fatal_attempt_requires_review(self):
        self.finish_receipt(self.start_receipt(), "fatal_failure", "fixture")
        with self.runtime() as run, self.assertRaisesRegex(recovery.RecoveryError, "explicit review"):
            recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
        self.runner._dispatch_one.assert_not_called()
        run.execv.assert_not_called()

    def test_control_guard_change_prevents_dispatch_and_resume(self):
        with self.runtime() as run:
            run.held.check_unchanged.side_effect = hybrid_legacy_guard.LegacyBusy("legacy_state_changed")
            with self.assertRaises(hybrid_legacy_guard.LegacyBusy):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            self.runner._dispatch_one.assert_not_called()
            run.execv.assert_not_called()
            run.control_cas.assert_not_called()
            self.assertFalse(json.loads((self.root / "status.json").read_text())["campaign_resumed"])

    def test_control_change_after_result_leaves_resumable_start_and_no_resume(self):
        with self.runtime() as run:
            def dispatch(order, remaining):
                value = self.runner._dispatch(order, remaining)
                run.held.check_unchanged.side_effect = hybrid_legacy_guard.LegacyBusy("legacy_state_changed")
                return value
            self.runner._dispatch_one.side_effect = dispatch
            with self.assertRaises(hybrid_legacy_guard.LegacyBusy):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            run.seal.assert_not_called()
            run.execv.assert_not_called()
            self.assertIsNone(self.attempts()[0]["finish"])

    def test_proof_failure_keeps_result_for_replay_and_does_not_resume(self):
        with self.runtime() as run:
            run.seal.side_effect = recovery.RecoveryError("fixture proof failure")
            with self.assertRaisesRegex(recovery.RecoveryError, "proof failure"):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            run.execv.assert_not_called()
            self.assertIsNone(self.attempts()[0]["finish"])
        with self.runtime() as run:
            recovery.run_plan(self.plan_path, self.plan_sha)
            self.assertEqual(self.runner._dispatch_one.call_count, 1)
            run.seal.assert_called_once()

    def test_free_space_floor_prevents_attempt_and_resume(self):
        self.runner._capacity_allows.return_value = False
        with self.runtime() as run, self.assertRaisesRegex(recovery.RecoveryStopped, "free-space"):
            recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
        self.runner._dispatch_one.assert_not_called()
        run.execv.assert_not_called()
        self.assertEqual(self.attempts(), [])

    def test_plan_change_during_execution_fails_before_dispatch(self):
        with self.runtime() as run:
            recovery.proof.validate_plan.side_effect = [run.plan, run.plan, {**run.plan, "created_at": "changed"}]
            with self.assertRaisesRegex(recovery.RecoveryError, "plan changed"):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            self.runner._dispatch_one.assert_not_called()
            run.execv.assert_not_called()

    def test_successful_items_and_exhausted_items_both_are_reported(self):
        def dispatch(order, remaining):
            if order["job_id"] == self.orders[0]["job_id"]:
                raise AcquisitionError("HTTP request failed with status 503")
            return self.runner._dispatch(order, remaining)
        self.runner._dispatch_one.side_effect = dispatch
        with self.runtime(count=2) as run:
            result = recovery.run_plan(self.plan_path, self.plan_sha)
            self.assertEqual((result["failed_items"], result["completed_items"]), (1, 1))
            self.assertEqual(result["failed_jobs"], [self.orders[0]["job_id"]])
            self.assertEqual(result["completed_jobs"], [self.orders[1]["job_id"]])
            self.assertEqual(run.seal.call_count, 1)

    def test_exec_failure_restores_stopped_intent_after_releasing_execution_guards(self):
        with self.runtime() as run:
            def execute(_path, _argv):
                self.assertFalse(run.held_active["value"])
                raise OSError("fixture exec failure")
            run.execv.side_effect = execute
            with self.assertRaisesRegex(OSError, "exec failure"):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            self.assertEqual([call.args[2] for call in run.control_cas.call_args_list], ["running", "stopped"])
            self.assertEqual(run.control_cas.call_args_list[0].args[1], self.control)
            self.assertEqual(run.control_cas.call_args_list[1].args[1]["desired_state"], "running")
            self.assertEqual(run.execv.call_args.args[1][1:], [
                "run", "--config", str(self.config.path), "--expected-config-sha256", self.config.physical_sha256,
            ])
            self.assertFalse(json.loads((self.root / "status.json").read_text())["campaign_resumed"])

    def test_new_control_generation_refuses_handoff_even_when_still_stopped(self):
        with self.runtime() as run:
            run.control_cas.side_effect = recovery.RecoveryError("operator intent changed; automatic recovery handoff refused")
            with self.assertRaisesRegex(recovery.RecoveryError, "intent changed"):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            run.execv.assert_not_called()
            self.assertEqual(run.control_cas.call_count, 1)
            self.assertEqual(self.attempts()[0]["finish"]["status"], "completed")

    def test_resume_unit_admission_failure_prevents_any_download_or_control_write(self):
        with self.runtime() as run:
            recovery.ControllerUnitContext.from_current_systemd_unit.side_effect = recovery.RecoveryError("wrong outer unit")
            with self.assertRaisesRegex(recovery.RecoveryError, "outer unit"):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            self.runner._dispatch_one.assert_not_called()
            run.execv.assert_not_called()
            run.control_cas.assert_not_called()

    def test_second_recovery_invocation_cannot_overwrite_active_status(self):
        descriptor = recovery._open_lock(self.root / "recovery.lock", "fixture recovery lock")
        self.addCleanup(os.close, descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status = self.root / "status.json"
        status.write_text("fixture active status", encoding="utf-8")
        with self.runtime() as run, self.assertRaisesRegex(recovery.RecoveryError, "already running"):
            recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
        self.assertEqual(status.read_text(), "fixture active status")
        self.runner._dispatch_one.assert_not_called()
        run.control_cas.assert_not_called()

    def test_mount_change_after_download_prevents_proof_and_resume(self):
        with self.runtime() as run:
            def dispatch(order, remaining):
                state = self.runner._dispatch(order, remaining)
                run.backend._validate_cold_storage_identity.side_effect = recovery.RecoveryError("cold mount changed")
                return state
            self.runner._dispatch_one.side_effect = dispatch
            with self.assertRaisesRegex(recovery.RecoveryError, "mount changed"):
                recovery.run_plan(self.plan_path, self.plan_sha, resume_campaign=True)
            run.seal.assert_not_called()
            run.execv.assert_not_called()
            self.assertIsNone(self.attempts()[0]["finish"])


class RecoveryControlTests(RecoveryFixture):
    def test_control_handoff_is_compare_and_set_and_increments_generation(self):
        self.config.state_root.mkdir(mode=0o700)
        with mock.patch.object(recovery, "_read_control_path", return_value=deepcopy(self.control)), mock.patch.object(
            recovery, "_atomic_mutable_json"
        ) as write, mock.patch.object(recovery, "utc_now", return_value=NOW):
            updated = recovery._set_control_if_unchanged(self.config, deepcopy(self.control), "running")
        self.assertEqual(updated, {"desired_state": "running", "generation": 11, "requested_at": NOW})
        self.assertEqual(write.call_args.args[:2], (self.config.state_root / "control.json", updated))

    def test_second_stop_generation_blocks_compare_and_set(self):
        self.config.state_root.mkdir(mode=0o700)
        with mock.patch.object(recovery, "_read_control_path", return_value={**self.control, "generation": 11}), mock.patch.object(
            recovery, "_atomic_mutable_json"
        ) as write, self.assertRaisesRegex(recovery.RecoveryError, "intent changed"):
            recovery._set_control_if_unchanged(self.config, self.control, "running")
        write.assert_not_called()

    def test_exec_failure_cleanup_cannot_overwrite_newer_operator_intent(self):
        self.config.state_root.mkdir(mode=0o700)
        expected = {"desired_state": "running", "generation": 11}
        with mock.patch.object(recovery, "_read_control_path", return_value={"desired_state": "stopped", "generation": 12}), mock.patch.object(
            recovery, "_atomic_mutable_json"
        ) as write, self.assertRaises(recovery.RecoveryError):
            recovery._set_control_if_unchanged(self.config, expected, "stopped")
        write.assert_not_called()

    def test_control_generation_overflow_does_not_write(self):
        self.config.state_root.mkdir(mode=0o700)
        saturated = {**self.control, "generation": 2**63 - 1}
        with mock.patch.object(recovery, "_read_control_path", return_value=saturated), mock.patch.object(
            recovery, "_atomic_mutable_json"
        ) as write, self.assertRaises(recovery.RecoveryError):
            recovery._set_control_if_unchanged(self.config, saturated, "running")
        write.assert_not_called()


class RecoveryDeadlineTests(unittest.TestCase):
    def test_download_inspection_and_adapter_validation_share_one_deadline(self):
        inside = {"value": False}

        @contextmanager
        def deadline(seconds):
            self.assertEqual(seconds, 123)
            inside["value"] = True
            try:
                yield
            finally:
                inside["value"] = False

        def checked(value):
            def operation(*_args, **_kwargs):
                self.assertTrue(inside["value"])
                return value
            return operation

        result = {"fixture": "completed"}
        runner = SimpleNamespace(
            _hard_deadline=deadline, QueueDeadlineError=type("DeadlineError", (RuntimeError,), {}),
            acquire=SimpleNamespace(run_acquisition=mock.Mock(side_effect=checked({"returned": True}))),
            _inspect_result=mock.Mock(side_effect=checked(result)),
            _validate_adapter_return=mock.Mock(side_effect=checked(None)),
        )
        self.assertEqual(recovery._dispatch_bounded({"fixture": "order"}, runner, 123), result)
        runner.acquire.run_acquisition.assert_called_once_with({"fixture": "order"}, dry_run=False)
        runner._validate_adapter_return.assert_called_once_with({"returned": True}, result)
        self.assertFalse(inside["value"])

    def test_missing_completed_result_cannot_be_a_success(self):
        runner = SimpleNamespace(
            _hard_deadline=lambda _seconds: nullcontext(), QueueDeadlineError=type("DeadlineError", (RuntimeError,), {}),
            acquire=SimpleNamespace(run_acquisition=mock.Mock(return_value={})),
            _inspect_result=mock.Mock(return_value=None), _validate_adapter_return=mock.Mock(),
        )
        with self.assertRaisesRegex(recovery.RecoveryError, "without a completed result"):
            recovery._dispatch_bounded({}, runner, 123)
        runner._validate_adapter_return.assert_not_called()

    def test_deadline_failure_is_a_resumable_stop(self):
        error_type = type("DeadlineError", (RuntimeError,), {})
        runner = SimpleNamespace(
            _hard_deadline=lambda _seconds: nullcontext(), QueueDeadlineError=error_type,
            acquire=SimpleNamespace(run_acquisition=mock.Mock(side_effect=error_type("deadline reached"))),
            _inspect_result=mock.Mock(side_effect=error_type("deadline reached")),
        )
        with self.assertRaises(recovery.RecoveryStopped):
            recovery._dispatch_bounded({}, runner, 1)
        with self.assertRaises(recovery.RecoveryStopped):
            recovery._inspect_bounded({}, runner, recovery.time.monotonic() + 1)


class RecoveryBackoffTests(unittest.TestCase):
    def test_backoff_rechecks_stop_every_second_and_has_a_wall_deadline(self):
        clock = {"time": 0.0}
        held = SimpleNamespace(check_unchanged=mock.Mock())
        with mock.patch.object(recovery.time, "monotonic", side_effect=lambda: clock["time"]), mock.patch.object(
            recovery.time, "sleep", side_effect=lambda seconds: clock.__setitem__("time", clock["time"] + seconds)
        ) as sleep:
            recovery._backoff(3, held, 10)
            self.assertEqual(clock["time"], 3)
            self.assertEqual(held.check_unchanged.call_count, 3)
            self.assertTrue(all(call.args[0] <= 1 for call in sleep.call_args_list))
            with self.assertRaises(recovery.RecoveryStopped):
                recovery._backoff(120, held, 5)
            self.assertEqual(clock["time"], 5)

    def test_backoff_guard_change_does_not_sleep(self):
        held = SimpleNamespace(check_unchanged=mock.Mock(side_effect=hybrid_legacy_guard.LegacyBusy("changed")))
        with mock.patch.object(recovery.time, "sleep") as sleep, self.assertRaises(hybrid_legacy_guard.LegacyBusy):
            recovery._backoff(120, held, recovery.time.monotonic() + 500)
        sleep.assert_not_called()

    def test_retryable_network_errors_are_closed_and_do_not_hide_disk_io(self):
        for message in ("HTTP request failed with status 503", "HTTP request failed: temporary failure in name resolution",
                        "HTTP body transfer failed: connection reset"):
            with self.subTest(message=message):
                self.assertTrue(recovery._transient(AcquisitionError(message)))
        for message in ("HTTP request failed with status 403", "[Errno 5] Input/output error", "hash mismatch",
                        "HTTP body transfer failed: [Errno 5] Input/output error"):
            with self.subTest(message=message):
                self.assertFalse(recovery._transient(AcquisitionError(message)))


if __name__ == "__main__":
    unittest.main()
