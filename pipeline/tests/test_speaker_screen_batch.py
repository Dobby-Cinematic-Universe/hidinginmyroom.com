"""Synthetic batch tests: no model loads, downloads, campaign access, or ASR."""
from contextlib import contextmanager, ExitStack, redirect_stdout
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from pipeline import speaker_screen as screen
from pipeline import speaker_screen_batch as batch
from pipeline.tests import test_speaker_screen as fixtures


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SpeakerScreenTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.orders = []
        references = []
        for index in range(3):
            order = deepcopy(self.fixture.order)
            source = self.root / f"source-{index}.wav"
            source.write_bytes(self.fixture.original)
            source.chmod(0o600)
            order["recording"].update(media_id=f"test:recording-{index}", path=str(source))
            order["output_root"] = str(self.root / f"recording-output-{index}")
            path = self.root / f"order-{index}.json"
            path.write_bytes(screen.canonical(order))
            path.chmod(0o400)
            self.orders.append(order)
            references.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        self.request = {"kind": "himr_speaker_screen_batch_request", "schema_version": 1,
                        "work_orders": references, "state_root": str(self.root / "batch"), "resources": {}}
        self.request_path = self.root / "request.json"
        self.request_path.write_bytes(screen.canonical(self.request))
        self.request_path.chmod(0o400)
        self.request_sha = hashlib.sha256(self.request_path.read_bytes()).hexdigest()
        self.path = self.root / "batch" / "manifest.json"
        self.manifest = None
        self.sha = None

    def seal(self):
        self.manifest = batch.seal_manifest(self.request_path, self.request_sha, self.path)
        self.sha = hashlib.sha256(self.path.read_bytes()).hexdigest()
        return self.manifest

    def reseal_request(self):
        self.request_path.chmod(0o600)
        self.request_path.write_bytes(screen.canonical(self.request))
        self.request_path.chmod(0o400)
        self.request_sha = hashlib.sha256(self.request_path.read_bytes()).hexdigest()

    def replace_order(self, index, order):
        path = Path(self.request["work_orders"][index]["path"])
        path.chmod(0o600)
        path.write_bytes(screen.canonical(order))
        path.chmod(0o400)
        self.request["work_orders"][index]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()

    def test_plan_seals_only_explicit_orders_without_media_or_model_reads(self):
        self.fixture.source.unlink()
        for order in self.orders:
            Path(order["recording"]["path"]).unlink()
        value = self.seal()
        self.assertEqual(value["resources"], {"concurrency": 2, "max_run_seconds": 3600})
        self.assertEqual(len(value["jobs"]), 3)
        self.assertEqual(value, batch.validate_manifest(value))
        self.assertEqual(self.path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertFalse(any(Path(order["output_root"]).exists() for order in self.orders))

    def test_manifest_resealing_is_idempotent_and_never_overwrites(self):
        value = self.seal()
        before = self.path.read_bytes()
        self.assertEqual(self.seal(), value)
        self.request["resources"]["concurrency"] = 3
        self.reseal_request()
        with self.assertRaises(batch.BatchError):
            self.seal()
        self.assertEqual(before, self.path.read_bytes())

    def test_request_digest_manifest_identity_and_implementation_tamper_rejected(self):
        self.seal()
        with self.assertRaises(batch.BatchError):
            batch.status_batch(self.path, "f" * 64)
        for mutate in (
            lambda value: value["jobs"][0].update(planned_windows=1),
            lambda value: value["python"].update(sha256="0" * 64),
            lambda value: value["implementation"].update({"speaker_screen_batch.py": "0" * 64}),
            lambda value: value.update(batch_id="screenbatch_" + "0" * 32),
        ):
            changed = deepcopy(self.manifest)
            mutate(changed)
            with self.subTest(value=changed), self.assertRaises(batch.BatchError):
                batch.validate_manifest(changed)

    def test_rejects_repeated_sources_ids_orders_and_nested_output_roots(self):
        cases = [
            lambda request, orders: request["work_orders"].append(request["work_orders"][0]),
            lambda request, orders: orders[1]["recording"].update(media_id=orders[0]["recording"]["media_id"]),
            lambda request, orders: orders[1]["recording"].update(path=orders[0]["recording"]["path"]),
            lambda request, orders: orders[1].update(output_root=orders[0]["output_root"] + "/nested"),
            lambda request, orders: orders[0].update(output_root=request["state_root"] + "/nested"),
            lambda request, orders: orders[0].update(output_root=orders[1]["recording"]["path"]),
        ]
        original = deepcopy(self.request)
        for mutation in cases:
            self.request = deepcopy(original)
            orders = deepcopy(self.orders)
            mutation(self.request, orders)
            for index, order in enumerate(orders):
                self.replace_order(index, order)
            with self.subTest(mutation=mutation), self.assertRaises(batch.BatchError):
                batch.build_manifest(self.request)

    def test_resource_bounds_and_unknown_fields_rejected(self):
        for resources in ({"concurrency": 0}, {"concurrency": 5}, {"concurrency": True},
                          {"max_run_seconds": 86401}, {"max_run_seconds": 1}, {"gpu": True}):
            with self.subTest(resources=resources), self.assertRaises(batch.BatchError):
                batch.build_manifest({**self.request, "resources": resources})

    def test_nonempty_unmarked_workspace_and_wrong_manifest_location_rejected(self):
        self.path.parent.mkdir(mode=0o700)
        (self.path.parent / "unrelated.txt").write_text("preserve")
        with self.assertRaises(batch.BatchError):
            self.seal()
        self.assertEqual((self.path.parent / "unrelated.txt").read_text(), "preserve")
        with self.assertRaises(batch.BatchError):
            batch.seal_manifest(self.request_path, self.request_sha, self.root / "elsewhere.json")

    def test_status_is_readonly_and_does_not_create_recording_or_lock_files(self):
        self.seal()
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.root.rglob("*") if path.is_file()}
        value = batch.status_batch(self.path, self.sha)
        after = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in self.root.rglob("*") if path.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(value["counts"]["not_started"], 3)
        self.assertEqual(value["state"], "paused")
        self.assertNotIn('"embedding":', json.dumps(value))

    @contextmanager
    def fake_workers(self, *, fail_index=None, hang=False):
        state = {"launched": [], "active": set(), "peak": 0, "killed": []}

        class Process:
            def __init__(process, index):
                process.index, process.pid, process.returncode, process.polls = index, 100000000 + index, None, 0

            def poll(process):
                process.polls += 1
                if not hang and process.polls >= 2 and process.returncode is None:
                    process.returncode = 2 if process.index == fail_index else 0
                    state["active"].discard(process.index)
                return process.returncode

            def wait(process, timeout=None):
                if process.returncode is None:
                    process.returncode = -9
                state["active"].discard(process.index)
                return process.returncode

        def launch(manifest, job, lock_fd, deadline, attempt, *, child_signal_mask=None):
            index = job["index"]
            state["launched"].append(index)
            state["active"].add(index)
            state["peak"] = max(state["peak"], len(state["active"]))
            start = {"kind": "himr_speaker_screen_batch_attempt", "schema_version": 1,
                     "batch_id": manifest["batch_id"], "attempt": attempt,
                     "job_index": index, "plan_id": job["plan_id"], "work_order": job["work_order"]}
            screen.write_immutable(self.path.parent / f"attempt-{attempt:06d}.start.json", start)
            plan = screen.build_plan(screen.read_json(Path(job["work_order"]["path"]), job["work_order"]["sha256"]))
            with mock.patch.object(screen, "decode_window", return_value=b"\0\0" * 16000 * 10), \
                    mock.patch.object(screen, "ModelProcess", fixtures.FakeModel):
                result = screen.run_screen(plan)
            stdout, stderr = io.BytesIO(screen.canonical(result)), io.BytesIO()
            return {"process": Process(index), "stdout": stdout, "stderr": stderr, "job": job,
                    "start": start, "deadline": deadline}

        with mock.patch.object(batch, "_launch", side_effect=launch), \
                mock.patch.object(batch.os, "killpg", side_effect=lambda pid, sig: state["killed"].append(pid)), \
                mock.patch.object(batch.time, "sleep"):
            yield state

    def test_parallel_bound_single_invocation_per_job_and_explicit_resume(self):
        self.seal()
        with self.fake_workers() as workers:
            first = batch.run_batch(self.path, self.sha)
        self.assertEqual(workers["peak"], 2)
        self.assertEqual(workers["launched"], [0, 1, 2])
        self.assertEqual(first["counts"]["sampling_plans_paused"], 3)
        self.assertTrue(all(row["completed_windows"] == 1 for row in first["recordings"]))
        with self.fake_workers():
            second = batch.run_batch(self.path, self.sha)
        self.assertTrue(all(row["completed_windows"] == 2 for row in second["recordings"]))
        with self.fake_workers():
            third = batch.run_batch(self.path, self.sha)
        self.assertEqual(third["state"], "screening_complete")
        self.assertEqual(third["counts"]["sampling_plans_completed"], 3)
        with self.fake_workers() as workers:
            final = batch.run_batch(self.path, self.sha)
        self.assertEqual(workers["launched"], [])
        self.assertEqual(final["counts"], third["counts"])

    def test_worker_failure_keeps_other_recordings_progress_and_cli_returns_two(self):
        self.seal()
        with self.fake_workers(fail_index=0):
            value = batch.run_batch(self.path, self.sha)
        self.assertEqual(value["errors"], [{"job_index": 0, "reason": "worker_failed"}])
        self.assertTrue(all(row["completed_windows"] == 1 for row in value["recordings"]))
        with mock.patch.object(batch, "run_batch", return_value=value), redirect_stdout(io.StringIO()):
            self.assertEqual(batch.main(["run", "--manifest", str(self.path), "--expected-sha256", self.sha]), 2)

    def test_cancellation_kills_every_worker_and_keeps_checkpoints(self):
        self.seal()
        with self.fake_workers(hang=True) as workers, mock.patch.object(batch.time, "sleep", side_effect=KeyboardInterrupt):
            value = batch.run_batch(self.path, self.sha)
        self.assertEqual(value["invocation_state"], "cancelled")
        self.assertEqual(len(workers["killed"]), 2)
        self.assertEqual(workers["active"], set())
        self.assertEqual(value["recordings"][0]["completed_windows"], 1)
        self.assertEqual(value["recordings"][2]["sampling_state"], "not_started")

    def test_wall_clock_limit_stops_dispatch_and_preserves_partial_progress(self):
        self.request["resources"]["max_run_seconds"] = 10
        self.reseal_request()
        self.seal()
        clock = [0.0]

        def elapsed(seconds):
            self.assertLessEqual(seconds, 0.2)
            clock[0] = 10.0

        with self.fake_workers(hang=True) as workers, \
                mock.patch.object(batch.time, "monotonic", side_effect=lambda: clock[0]), \
                mock.patch.object(batch.time, "sleep", side_effect=elapsed):
            value = batch.run_batch(self.path, self.sha)
        self.assertEqual(value["invocation_state"], "time_limit")
        self.assertEqual(workers["launched"], [0, 1])
        self.assertEqual(len(workers["killed"]), 2)
        self.assertEqual(value["recordings"][2]["sampling_state"], "not_started")

    def test_plan_only_interrupted_worker_initialization_can_resume(self):
        self.seal()
        job = self.manifest["jobs"][0]
        plan = screen.build_plan(self.orders[0])
        root = screen.workspace(Path(job["output_root"]), create=True)
        directory = root / plan["plan_id"]
        directory.mkdir(mode=0o700)
        screen.write_immutable(directory / "plan.json", plan)
        self.assertEqual(batch.status_batch(self.path, self.sha)["recordings"][0]["sampling_state"], "not_started")
        with self.fake_workers():
            value = batch.run_batch(self.path, self.sha)
        self.assertEqual(value["recordings"][0]["completed_windows"], 1)

    def test_cleanup_failure_on_first_checkpoint_does_not_skip_second_worker(self):
        self.seal()
        with self.fake_workers(hang=True) as workers, \
                mock.patch.object(batch.time, "sleep", side_effect=KeyboardInterrupt), \
                mock.patch.object(batch, "_finish", side_effect=batch.BatchError("synthetic malformed checkpoint")):
            value = batch.run_batch(self.path, self.sha)
        self.assertEqual(len(workers["killed"]), 2)
        self.assertEqual(workers["active"], set())
        self.assertEqual(len(value["errors"]), 2)

    def test_batch_lock_excludes_another_runner_and_is_cloexec(self):
        self.seal()
        with batch._locked(self.path.parent) as descriptor:
            self.assertFalse(os.get_inheritable(descriptor))
            with self.assertRaises(batch.BatchError):
                with batch._locked(self.path.parent):
                    pass

    def test_attempt_tamper_rejected_and_interrupted_start_can_resume(self):
        self.seal()
        job = self.manifest["jobs"][0]
        start = {"kind": "himr_speaker_screen_batch_attempt", "schema_version": 1,
                 "batch_id": self.manifest["batch_id"], "attempt": 1, "job_index": 0,
                 "plan_id": job["plan_id"], "work_order": job["work_order"]}
        path = self.path.parent / "attempt-000001.start.json"
        screen.write_immutable(path, start)
        self.assertEqual(batch._attempt_number(self.path.parent, self.manifest), 2)
        path.write_bytes(screen.canonical({**start, "batch_id": "wrong"}))
        with self.assertRaises(batch.BatchError):
            batch._attempt_number(self.path.parent, self.manifest)

    def test_launch_uses_selected_python_offline_environment_and_inherited_lease(self):
        self.seal()
        with batch._locked(self.path.parent) as descriptor, \
                mock.patch.object(batch.subprocess, "Popen") as popen:
            worker = batch._launch(self.manifest, self.manifest["jobs"][0], descriptor, time.monotonic() + 30, 1,
                                   child_signal_mask={signal.SIGUSR1})
            self.addCleanup(worker["stdout"].close)
            self.addCleanup(worker["stderr"].close)
            args, kwargs = popen.call_args
            self.assertEqual(args[0][0], self.manifest["python"]["path"])
            self.assertEqual(kwargs["pass_fds"], (descriptor,))
            self.assertTrue(kwargs["start_new_session"])
            self.assertEqual(kwargs["env"]["CUDA_VISIBLE_DEVICES"], "")
            self.assertEqual(kwargs["env"]["HF_HUB_OFFLINE"], "1")
            self.assertNotIn("SALAD_API_KEY", kwargs["env"])
            self.assertNotIn("HOME", kwargs["env"])
            with mock.patch.object(screen, "die_with_parent") as lifetime, mock.patch.object(screen, "deny_internet") as offline, \
                    mock.patch.object(batch.resource, "setrlimit"), mock.patch.object(batch.signal, "pthread_sigmask") as mask:
                kwargs["preexec_fn"]()
                lifetime.assert_called_once_with(os.getpid())
                offline.assert_called_once()
                mask.assert_called_once_with(signal.SIG_SETMASK, {signal.SIGUSR1})

    def test_post_spawn_exception_terminates_child_and_closes_both_streams(self):
        self.seal()
        process = mock.Mock()
        streams = []
        spawned = [False]
        actual_monotonic = time.monotonic

        def popen(*_args, **_kwargs):
            spawned[0] = True
            return process

        def clock():
            if spawned[0]:
                raise KeyboardInterrupt
            return actual_monotonic()

        def temporary(*_args, **_kwargs):
            stream = io.BytesIO()
            streams.append(stream)
            return stream

        with batch._locked(self.path.parent) as descriptor, \
                mock.patch.object(batch.subprocess, "Popen", side_effect=popen), \
                mock.patch.object(batch.tempfile, "TemporaryFile", side_effect=temporary), \
                mock.patch.object(batch.time, "monotonic", side_effect=clock), \
                mock.patch.object(batch, "_terminate") as terminate:
            with self.assertRaises(KeyboardInterrupt):
                batch._launch(self.manifest, self.manifest["jobs"][0], descriptor, actual_monotonic() + 30, 1)
            terminate.assert_called_once_with({"process": process})
        self.assertEqual(len(streams), 2)
        self.assertTrue(all(stream.closed for stream in streams))

    def test_pending_signal_is_delivered_only_after_worker_registration(self):
        self.seal()
        registered = []
        actual_mask = signal.pthread_sigmask

        def mask(how, values):
            if how == signal.SIG_SETMASK and registered:
                # Model a pending Ctrl-C delivered on unmask, after append.
                actual_mask(how, values)
                raise KeyboardInterrupt
            return actual_mask(how, values)

        with self.fake_workers(hang=True) as workers:
            fake_launch = batch._launch

            def launched(*args, **kwargs):
                worker = fake_launch(*args, **kwargs)
                registered.append(worker)
                return worker

            with mock.patch.object(batch, "_launch", side_effect=launched), \
                    mock.patch.object(batch.signal, "pthread_sigmask", side_effect=mask):
                value = batch.run_batch(self.path, self.sha)
        self.assertEqual(value["invocation_state"], "cancelled")
        self.assertEqual(len(workers["killed"]), 1)
        self.assertEqual(workers["active"], set())

    def test_partial_positive_can_finish_screen_without_claiming_full_sampling(self):
        self.seal()
        job = deepcopy(self.manifest["jobs"][0])
        job["resources"]["early_stop_on_positive"] = True
        value = {"plan_id": job["plan_id"], "state": "paused", "planned_windows": 3,
                 "completed_windows": 2, "remaining_windows": 1, "screening_decision_complete": True,
                 "stop_reason": "supported_multiple_speakers", "summary": {
                     "status": "multiple_speaker_candidate", "reason_flags": ["supported_distinct_voice_groups"],
                     "coverage": {"planned_windows": 3, "inspected_windows": 2, "uninspected_windows": 1}}}
        projected = batch._projection(job, value)
        self.assertEqual(projected["sampling_state"], "paused")
        self.assertTrue(projected["screening_decision_complete"])
        status = batch._summary(self.manifest, [projected])
        self.assertEqual(status["state"], "screening_complete")
        self.assertEqual(status["counts"]["sampling_plans_completed"], 0)
        value["state"] = "completed"
        with self.assertRaises(batch.BatchError):
            batch._projection(job, value)


if __name__ == "__main__":
    unittest.main()
