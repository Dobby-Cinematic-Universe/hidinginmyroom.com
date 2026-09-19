"""Parallel collection partitions, child admission, spawn and failure draining."""
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from copy import deepcopy
import multiprocessing
import os
from pathlib import Path
import threading
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline import cloud_transcription_summary_parallel as parallel
from pipeline.tests import test_cloud_transcription_summary as fixtures

r, worker = parallel.r, parallel.worker
_SPAWN_BARRIER = None


def _spawn_init(barrier):
    global _SPAWN_BARRIER
    _SPAWN_BARRIER = barrier


def _spawn_probe(group):
    _SPAWN_BARRIER.wait(timeout=10)
    return {"pid": os.getpid(), "group": group, "paid_calls": 0}


class ImmediateExecutor:
    def __init__(self, **kwargs):
        self.kwargs, self.calls, self.shutdown_calls = kwargs, [], []

    def submit(self, function, group):
        self.calls.append(deepcopy(group))
        future = Future()
        try:
            future.set_result(function(group))
        except Exception as error:
            future.set_exception(error)
        return future

    def shutdown(self, **kwargs):
        self.shutdown_calls.append(kwargs)


class ChildTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.prepare()
        self.case.cycle()  # Fixture-only fake Gemini submission; network forbidden.
        self.manifest = worker.load_manifest(self.case.ref)
        self.entry = r.read(r.binding(next((self.case.worker_root / "entries").iterdir())))
        root = Path(self.entry["plan"]["path"]).parent
        wave = next((root / "waves").iterdir()).name
        self.group = {"plan": self.entry["plan"], "waves": [wave]}
        self.stop = threading.Event()
        self.child = {"worker_ref": self.case.ref, "manifest": self.manifest,
                      "env_file": str(self.case.root / ".env"), "stop_event": self.stop}
        context = patch.object(parallel, "_CHILD", self.child)
        context.start()
        self.addCleanup(context.stop)

    def test_original_get_collection_retains_global_reservation_and_never_submits(self):
        before = list(self.case.client.created)
        self.case.client.complete = True
        with patch.object(r, "api_client", return_value=self.case.client), patch.object(r, "submit_wave") as submit:
            report = parallel._poll_group(self.group)
        self.assertEqual(report["fatal_errors"], [])
        self.assertEqual(report["events"][0]["state"], "collected")
        self.assertEqual(report["new_paid_requests"], 0)
        self.assertEqual(self.case.client.created, before)
        submit.assert_not_called()
        self.assertTrue((self.case.worker_root / "reservations" / (self.group["waves"][0] + ".json")).is_file())

    def test_get_transport_errors_are_sanitized_and_not_retried_within_group(self):
        error = r.client_module.BatchClientError("PRIVATE RESPONSE AND KEY", status_code=429, retry_after_seconds=22)
        with patch.object(r, "poll_wave", side_effect=error) as poll:
            report = parallel._poll_group(self.group)
        self.assertEqual(poll.call_count, 1)
        self.assertEqual(report["fatal_errors"], [])
        event = report["events"][0]
        self.assertEqual(event["state"], "transport_error_retained")
        self.assertEqual(event["status_code"], 429)
        self.assertEqual(event["retry_after_seconds"], 22)
        self.assertFalse(event["automatic_retry"])
        self.assertNotIn("PRIVATE", repr(report))
        self.assertEqual(poll.call_args.kwargs["env_file"], self.child["env_file"])

    def test_data_or_provenance_failure_is_fatal_without_serializing_private_exception(self):
        with patch.object(r, "poll_wave", side_effect=r.Error("PRIVATE SOURCE TEXT")):
            report = parallel._poll_group(self.group)
        self.assertEqual(report["groups_completed"], 0)
        self.assertEqual(report["fatal_errors"][0]["state"], "collection_validation_failed")
        self.assertNotIn("PRIVATE", repr(report))

    def test_existing_intent_receipt_and_global_reservation_are_required_before_get(self):
        root = Path(self.entry["plan"]["path"]).parent
        folder = root / "waves" / self.group["waves"][0]
        paths = [folder / "submit-intent.json", folder / "submitted.json",
                 self.case.worker_root / "reservations" / (self.group["waves"][0] + ".json")]
        for index, path in enumerate(paths):
            retained = self.case.root / ("retained-" + str(index) + ".json")
            path.rename(retained)
            with patch.object(r, "poll_wave") as poll:
                report = parallel._poll_group(self.group)
            self.assertTrue(report["fatal_errors"])
            poll.assert_not_called()
            retained.rename(path)

    def test_record_waves_are_sequential_and_stop_prevents_the_next_wave(self):
        self.case.client.complete = True
        self.case.cycle()  # Collect initial wave and create transcript reducer fixture.
        root = Path(self.entry["plan"]["path"]).parent
        names = [path.name for path in (root / "waves").iterdir()]
        self.assertEqual(len(names), 2)
        group = {"plan": self.entry["plan"], "waves": names}
        seen = []
        def poll(_path, _sha, name, **kwargs):
            seen.append(name)
            self.stop.set()
            return {"state": "remote_pending", "wave_id": name}
        with patch.object(r, "poll_wave", side_effect=poll):
            report = parallel._poll_group(group)
        self.assertEqual(seen, names[:1])
        self.assertEqual(report["groups_completed"], 0)
        self.assertEqual(report["fatal_errors"], [])

    def test_other_provider_wave_and_unrelated_record_plan_are_rejected(self):
        bad = {"plan": {"path": str(self.case.root / "other" / "plan.json"), "sha256": "a" * 64},
               "waves": self.group["waves"]}
        with patch.object(r, "poll_wave") as poll:
            self.assertTrue(parallel._poll_group(bad)["fatal_errors"])
            poll.assert_not_called()
        wave = Path(self.entry["plan"]["path"]).parent / "waves" / self.group["waves"][0] / "wave.json"
        value = r.read(r.binding(wave))
        value["provider"] = "openai"
        wave.chmod(0o600)
        wave.write_bytes(r.canonical(value))
        with patch.object(r, "poll_wave") as poll:
            self.assertTrue(parallel._poll_group(self.group)["fatal_errors"])
            poll.assert_not_called()

    def test_initializer_explicitly_enters_release_and_cache_scopes_without_key_arguments(self):
        entered = []
        @contextmanager
        def release_scope(reference):
            entered.append(("release", reference))
            yield
            entered.append(("release_closed", reference))
        @contextmanager
        def cache_scope(reference, **options):
            entered.append(("cache", reference))
            yield
            entered.append(("cache_closed", reference))
        release_ref = {"path": "/private/release.json", "sha256": "a" * 64}
        with patch.object(parallel, "_runtime"), patch.object(parallel.release, "activate", side_effect=release_scope), \
                patch.object(parallel.job_cache, "scope", side_effect=cache_scope), patch.object(parallel.atexit, "register"):
            parallel._initialize(self.case.ref, release_ref, self.child["env_file"], "/private/runtime", self.stop)
            stack = parallel._CHILD["stack"]
            self.assertEqual(entered[:2], [("release", release_ref), ("cache", self.case.ref)])
            stack.close()
        self.assertEqual([row[0] for row in entered], ["release", "cache", "cache_closed", "release_closed"])


class PoolTests(unittest.TestCase):
    def setUp(self):
        self.runtime = Path(parallel.__file__).parent.parent
        self.worker_ref = {"path": "/private/worker/manifest.json", "sha256": "a" * 64}
        self.release_ref = {"path": "/private/release.json", "sha256": "b" * 64}
        self.created = []
        runtime_guard = patch.object(parallel, "_runtime", return_value=self.runtime)
        runtime_guard.start()
        self.addCleanup(runtime_guard.stop)
        def factory(**kwargs):
            executor = ImmediateExecutor(**kwargs)
            self.created.append(executor)
            return executor
        context = patch.object(parallel, "ProcessPoolExecutor", side_effect=factory)
        context.start()
        self.addCleanup(context.stop)

    def group(self, index):
        return {"plan": {"path": f"/private/worker/records/r{index}/plan.json", "sha256": f"{index:064x}"},
                "waves": ["summarywave_" + f"{index:032x}"]}

    def scope(self, **kwargs):
        return parallel.scope(self.worker_ref, self.release_ref, env_file="/private/.env",
                              runtime_path=str(self.runtime), **kwargs)

    def report(self, group):
        return {"events": [{"operation": "poll", "wave_id": group["waves"][0], "state": "remote_pending"}],
                "fatal_errors": [], "groups_completed": 1, "new_paid_requests": 0}

    def test_persistent_scope_uses_spawn_paths_only_and_rejects_duplicate_record_partition(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "PRIVATE_KEY_VALUE"}), \
                patch.object(parallel, "_poll_group", side_effect=self.report):
            with self.scope(max_workers=2) as pool:
                self.assertTrue(parallel.active())
                parallel.poll_groups([self.group(1), self.group(2)])
                parallel.poll_groups([self.group(1)])
                duplicate = {**self.group(1), "waves": self.group(3)["waves"]}
                with self.assertRaises(parallel.ParallelPollError):
                    parallel.poll_groups([self.group(1), duplicate])
                self.assertEqual(pool.statistics()["groups_completed"], 3)
                self.assertEqual(len(self.created), 1)
                self.assertEqual(self.created[0].kwargs["mp_context"].get_start_method(), "spawn")
                self.assertNotIn("PRIVATE_KEY_VALUE", repr(self.created[0].kwargs["initargs"]))
                self.assertNotIn("PRIVATE_KEY_VALUE", repr(self.created[0].calls))
        self.assertFalse(parallel.active())
        self.assertEqual(self.created[0].shutdown_calls, [{"wait": True, "cancel_futures": True}])

    def test_fatal_worker_result_drains_started_groups_and_does_not_dispatch_remaining(self):
        def result(group):
            if group == self.group(1):
                return {"events": [], "fatal_errors": [{"state": "collection_validation_failed"}], "groups_completed": 0}
            return self.report(group)
        with patch.object(parallel, "_poll_group", side_effect=result), self.scope(max_workers=2):
            with self.assertRaises(parallel.ParallelPollError) as caught:
                parallel.poll_groups([self.group(1), self.group(2), self.group(3)])
            self.assertEqual(len(self.created[0].calls), 2)
            self.assertEqual(caught.exception.report["groups_completed"], 1)
            self.assertEqual(self.created[0].shutdown_calls, [{"wait": True, "cancel_futures": True}])

    def test_broken_future_is_physically_shutdown_before_fatal_exception_escapes(self):
        with patch.object(parallel, "_poll_group", side_effect=BrokenProcessPool("PRIVATE FAILURE")), \
                self.scope(max_workers=2) as pool:
            with self.assertRaises(parallel.ParallelPollError) as caught:
                parallel.poll_groups([self.group(1), self.group(2)])
            self.assertTrue(pool.closed)
            self.assertTrue(pool._shutdown_complete)
            self.assertEqual(self.created[0].shutdown_calls, [{"wait": True, "cancel_futures": True}])
            self.assertNotIn("PRIVATE", repr(caught.exception.report))
            with self.assertRaises(parallel.ParallelPollError):
                parallel.poll_groups([self.group(1)])
        self.assertEqual(len(self.created[0].shutdown_calls), 1)  # Outer cleanup is idempotent.

    def test_partial_dispatch_failure_shutdowns_already_started_workers_before_raise(self):
        with patch.object(parallel, "_poll_group", side_effect=self.report), self.scope(max_workers=2) as pool:
            original_submit = pool.executor.submit
            count = [0]
            def submit(function, group):
                count[0] += 1
                if count[0] == 2:
                    raise BrokenProcessPool("dispatch failed")
                return original_submit(function, group)
            with patch.object(pool.executor, "submit", side_effect=submit):
                with self.assertRaises(BrokenProcessPool):
                    parallel.poll_groups([self.group(1), self.group(2), self.group(3)])
            self.assertTrue(pool.closed)
            self.assertTrue(pool._shutdown_complete)
            self.assertEqual(len(self.created[0].calls), 1)
            self.assertEqual(self.created[0].shutdown_calls, [{"wait": True, "cancel_futures": True}])

    def test_wait_failure_is_shutdown_before_propagation_with_lock_released(self):
        with patch.object(parallel, "_poll_group", side_effect=self.report), self.scope(max_workers=2) as pool:
            with patch.object(parallel, "wait", side_effect=RuntimeError("wait infrastructure failed")):
                with self.assertRaisesRegex(RuntimeError, "wait infrastructure"):
                    parallel.poll_groups([self.group(1)])
            self.assertTrue(pool._shutdown_complete)
            self.assertFalse(pool.lock.locked())
            self.assertEqual(self.created[0].shutdown_calls, [{"wait": True, "cancel_futures": True}])

    def test_pause_before_dispatch_and_no_active_scope_are_safe(self):
        with self.assertRaises(parallel.ParallelPollError):
            parallel.poll_groups([])
        with self.scope(), patch.object(parallel, "_poll_group") as poll:
            report = parallel.poll_groups([self.group(1)], stopping=lambda: True)
            self.assertEqual(report["groups_completed"], 0)
            self.assertEqual(self.created[0].calls, [])
            poll.assert_not_called()

    def test_nested_scope_and_bad_worker_bounds_rejected(self):
        with self.scope():
            with self.assertRaises(parallel.ParallelPollError):
                with self.scope():
                    self.fail("nested scope accepted")
        for count in (0, 5, True):
            with self.assertRaises(RuntimeError):
                with self.scope(max_workers=count):
                    self.fail("invalid worker count")

    def test_wave_duplicates_bad_ids_and_oversized_group_lists_rejected(self):
        for groups in ([self.group(1), {**self.group(2), "waves": self.group(1)["waves"]}],
                       [{**self.group(1), "waves": ["bad-wave"]}],
                       [self.group(index) for index in range(parallel.MAX_GROUPS + 1)]):
            with self.assertRaises(RuntimeError):
                parallel._groups(groups)


class SpawnTests(unittest.TestCase):
    def test_runtime_guard_rejects_other_private_directory_before_worker_creation(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(RuntimeError):
                parallel._runtime(folder)

    def test_two_real_spawn_processes_are_reused_without_pickling_clients_or_keys(self):
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(2)
        groups = [{"plan": {"path": f"/private/r{i}/plan.json", "sha256": "a" * 64},
                   "waves": ["summarywave_" + f"{i:032x}"]} for i in (1, 2)]
        with ProcessPoolExecutor(max_workers=2, mp_context=context, initializer=_spawn_init, initargs=(barrier,)) as executor:
            first = list(executor.map(_spawn_probe, groups))
            second = list(executor.map(_spawn_probe, groups))
        pids = {value["pid"] for value in first}
        self.assertEqual(len(pids), 2)
        self.assertNotIn(os.getpid(), pids)
        self.assertEqual(pids, {value["pid"] for value in second})
        self.assertTrue(all(value["paid_calls"] == 0 for value in first + second))


if __name__ == "__main__":
    unittest.main()
