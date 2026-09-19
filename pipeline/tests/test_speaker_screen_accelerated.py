"""Resident orchestration contracts; synthetic inference, bounded local decode."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest import mock
import wave

from jsonschema import Draft202012Validator

from pipeline import speaker_screen_accelerated as resident


EVENTS = []


class FakeModel:
    instances = []
    mode = "solo"
    fail_call = None

    def __init__(self, models, execution, implementation, **kwargs):
        self.models, self.execution = models, execution
        self.calls = []
        self.closed = False
        self.instances.append(self)

    def analyze_batch(self, items, timeout):
        self.calls.append([item["window"]["index"] for item in items])
        EVENTS.append(("infer", self.calls[-1]))
        if self.fail_call == len(self.calls):
            raise resident.ScreenError("synthetic inference failure")
        if self.mode == "cancel":
            raise KeyboardInterrupt
        observations = []
        for item in items:
            window = item["window"]
            vector = [0.0] * 192
            vector[window["index"] % 2 if self.mode == "multiple" else (len(self.calls) > 2)] = 1.0
            observations.append({"index": window["index"], "start_ms": window["start_ms"],
                "end_ms": window["start_ms"] + 2000, "speech_ms": 2000, "embedding": vector})
        runtime = {"models": self.models, "recipe": resident.engine.model_recipe("cpu"),
                   "threads": self.execution["threads"], "device": "cpu", "batch_size": self.execution["batch_size"],
                   "cuda_memory_fraction": None, "runtime_versions": resident.engine.CPU_RUNTIME_PINS,
                   "cuda": None, "requested_gpu_uuid": None, "nvidia_driver_version": None}
        return {"observations": observations, "runtime": runtime,
                "statistics": {"model_initializations": 1, "batches": len(self.calls)}}

    def close(self):
        self.closed = True


class FakeDecoders:
    instances = []
    fail_index = None
    close_error = False

    def __init__(self, count, implementation, **kwargs):
        self.active = None
        self.decoded_seconds = 0.0
        self.worker_starts = count
        self.closes = 0
        self.instances.append(self)

    def submit(self, order, source_witness, ffmpeg_witness, windows, timeout, deadline):
        if self.active is not None:
            raise AssertionError("overlapping unbounded prefetch")
        self.active = windows
        EVENTS.append(("submit", [window["index"] for window in windows]))

    def collect(self, deadline):
        if self.fail_index is not None and any(window["index"] == self.fail_index for window in self.active):
            raise resident.ScreenError("synthetic future decode error")
        windows, self.active = self.active, None
        self.decoded_seconds += len(windows) * 0.2
        EVENTS.append(("collect", [window["index"] for window in windows]))
        return [{"window": window, "pcm": bytes((window["end_ms"] - window["start_ms"]) * 32)} for window in windows]

    def close(self):
        self.active = None
        self.closes += 1
        if self.close_error:
            raise OSError("synthetic decoder close error")


class ResidentRunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="resident-runner-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.tool = self.root / "ffmpeg"
        self.tool.write_bytes(b"synthetic ffmpeg, never executed")
        self.tool.chmod(0o700)
        self.models = {"kind": "himr_speaker_screen_models", "schema_version": 1,
            "silero_vad": {"path": str(self.root / "vad.onnx"), "sha256": "1" * 64},
            "ecapa_embedding": {"path": str(self.root / "model.ckpt"), "sha256": "2" * 64}}
        self.orders, references = [], []
        for index in range(2):
            source = self.root / f"source-{index}.wav"
            with wave.open(str(source), "wb") as handle:
                handle.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
                handle.writeframes(bytes(64000))
            source.chmod(0o600)
            order = {"kind": "himr_cpu_speaker_screen_work_order", "schema_version": 1,
                "recording": {"media_id": f"synthetic:{index}", "path": str(source),
                    "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "byte_count": source.stat().st_size,
                    "duration_ms": 240000},
                "ffmpeg": {"path": str(self.tool), "sha256": hashlib.sha256(self.tool.read_bytes()).hexdigest()},
                "models": self.models, "policy": {}, "resources": {"threads": 1,
                    "window_timeout_seconds": 10, "max_run_seconds": 60, "max_windows_per_run": 512,
                    "early_stop_on_positive": False},
                "source_verification": "metadata_witness", "output_root": str(self.root / f"cpu-original-{index}")}
            self.orders.append(order)
            references.append({"path": str(self.root / f"order-{index}.json"), "sha256": resident.screen.digest(order)})
        self.request = {"kind": "himr_resident_speaker_screen_request", "schema_version": 1,
                        "work_orders": references, "state_root": str(self.root / "resident"),
                        "execution": {"batch_size": 2, "max_run_seconds": 60}}
        patcher = mock.patch.object(resident.engine.cpu, "runtime_versions", return_value=resident.engine.CPU_RUNTIME_PINS)
        patcher.start()
        self.addCleanup(patcher.stop)
        FakeModel.instances, FakeDecoders.instances = [], []
        FakeModel.mode, FakeModel.fail_call = "solo", None
        FakeDecoders.fail_index, FakeDecoders.close_error = None, False
        EVENTS.clear()

    def seal(self):
        for order, ref in zip(self.orders, self.request["work_orders"]):
            ref["sha256"] = resident.screen.digest(order)
            resident.screen.write_immutable(Path(ref["path"]), order)
        request_path = self.root / "request.json"
        resident.screen.write_immutable(request_path, self.request)
        self.path = Path(self.request["state_root"]) / "manifest.json"
        self.manifest = resident.seal_manifest(str(request_path), resident.screen.digest(self.request), str(self.path))
        self.sha = resident.screen.digest(self.manifest)
        return self.manifest

    def run_fake(self):
        with mock.patch.object(resident.worker_api, "ResidentModelProcess", FakeModel), \
                mock.patch.object(resident, "DecodePool", FakeDecoders):
            return resident.run_batch(str(self.path), self.sha)

    def test_plan_is_metadata_only_and_keeps_original_roots_untouched(self):
        self.seal()
        self.assertTrue(self.path.exists())
        for order in self.orders:
            self.assertFalse(Path(order["output_root"]).exists())
        self.assertEqual([len(plan["batches"]) for plan in self.manifest["plans"]], [2, 2])
        status = resident.status_batch(str(self.path), self.sha)
        self.assertEqual(status["counts"]["completed_windows"], 0)
        self.assertFalse((self.path.parent / "resident.lock").exists())

    def test_two_recordings_share_one_model_and_keep_groups_recording_local(self):
        self.seal()
        result = self.run_fake()
        self.assertEqual(result["state"], "screening_complete")
        self.assertEqual(result["counts"]["completed_windows"], 8)
        self.assertEqual(result["counts"]["no_second_voice_detected_in_sampled_audio"], 2)
        self.assertEqual(len(FakeModel.instances), 1)
        self.assertEqual(FakeModel.instances[0].calls, [[0, 1], [2, 3], [0, 1], [2, 3]])
        self.assertTrue(FakeModel.instances[0].closed)
        self.assertEqual(result["metrics"]["model_workers_started"], 1)
        self.assertNotIn('"embedding":', json.dumps(result))
        self.assertEqual(EVENTS[:4], [("submit", [0, 1]), ("collect", [0, 1]),
                                     ("submit", [2, 3]), ("infer", [0, 1])])
        self.assertEqual(resident.status_batch(str(self.path), self.sha)["counts"], result["counts"])

    def test_completed_replay_launches_nothing_and_writes_no_receipts(self):
        self.seal()
        self.run_fake()
        before = {path: path.read_bytes() for path in self.path.parent.rglob("*.json")}
        FakeModel.instances.clear()
        result = self.run_fake()
        self.assertEqual(result["metrics"]["model_workers_started"], 0)
        self.assertEqual(FakeModel.instances, [])
        self.assertEqual(before, {path: path.read_bytes() for path in self.path.parent.rglob("*.json")})

    def test_interruption_keeps_whole_batch_and_resumes_original_partition(self):
        self.seal()
        FakeModel.fail_call = 2
        failed = self.run_fake()
        self.assertEqual(failed["invocation_state"], "failed")
        self.assertEqual(failed["counts"]["completed_windows"], 2)
        first = self.path.parent / self.manifest["plans"][0]["plan_id"] / "batch-0000.json"
        original = first.read_bytes()
        FakeModel.fail_call = None
        completed = self.run_fake()
        self.assertEqual(completed["state"], "screening_complete")
        self.assertEqual(FakeModel.instances[-1].calls[0], [2, 3])
        self.assertEqual(first.read_bytes(), original)

    def test_original_per_recording_work_bound_is_respected(self):
        for order in self.orders:
            order["resources"]["max_windows_per_run"] = 1
        self.seal()
        self.assertEqual(len(self.manifest["plans"][0]["batches"]), 4)
        self.assertEqual(self.run_fake()["counts"]["completed_windows"], 2)
        self.assertEqual(self.run_fake()["counts"]["completed_windows"], 4)

    def test_positive_early_stop_discards_bad_lookahead_without_invalidating_decision(self):
        for order in self.orders:
            order["recording"]["duration_ms"] = 600000
            order["resources"]["early_stop_on_positive"] = True
        self.request["execution"]["batch_size"] = 4
        self.seal()
        FakeModel.mode = "multiple"
        FakeDecoders.fail_index = 4
        result = self.run_fake()
        self.assertEqual(result["state"], "screening_complete")
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["counts"]["completed_windows"], 8)
        self.assertEqual(result["counts"]["sampling_plans_completed"], 0)
        self.assertTrue(all(row["sampling_state"] == "paused" for row in result["recordings"]))
        self.assertEqual(FakeModel.instances[0].calls, [[0, 1, 2, 3], [0, 1, 2, 3]])

    def test_cancel_closes_both_model_and_decoder_pool(self):
        self.seal()
        FakeModel.mode = "cancel"
        result = self.run_fake()
        self.assertEqual(result["invocation_state"], "cancelled")
        self.assertTrue(FakeModel.instances[0].closed)
        self.assertGreater(FakeDecoders.instances[0].closes, 0)
        self.assertEqual(result["counts"]["completed_windows"], 0)

    def test_model_cleanup_still_runs_when_decoder_cleanup_fails(self):
        self.seal()
        FakeDecoders.close_error = True
        result = self.run_fake()
        self.assertTrue(FakeModel.instances[0].closed)
        self.assertEqual(result["invocation_state"], "failed")
        self.assertTrue(result["errors"])

    def test_mutated_source_fails_closed_without_overwriting_prior_batch(self):
        self.seal()
        FakeModel.fail_call = 2
        self.run_fake()
        source = Path(self.orders[0]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        FakeModel.fail_call = None
        result = self.run_fake()
        self.assertEqual(result["invocation_state"], "failed")
        self.assertEqual(result["counts"]["completed_windows"], 2)

    def test_missing_checkpoint_in_sealed_result_is_not_repaired(self):
        self.seal()
        self.run_fake()
        checkpoint = self.path.parent / self.manifest["plans"][0]["plan_id"] / "batch-0000.json"
        checkpoint.unlink()
        with self.assertRaises(resident.ScreenError):
            resident.status_batch(str(self.path), self.sha)
        with self.assertRaises(resident.ScreenError):
            self.run_fake()

    def test_status_during_active_writer_reports_running_without_replay(self):
        self.seal()
        with resident._locked(self.path.parent), mock.patch.object(resident, "_read_job", side_effect=AssertionError("racy read")):
            result = resident.status_batch(str(self.path), self.sha)
        self.assertEqual(result["state"], "running")
        self.assertTrue(result["snapshot_deferred"])
        self.assertIsNone(result["counts"])

    def test_request_rejects_colliding_original_roots_and_mixed_model_pairs(self):
        self.request["state_root"] = self.orders[0]["output_root"]
        with self.assertRaises(resident.ScreenError):
            self.seal()

    def test_runtime_and_request_changes_do_not_reuse_manifest(self):
        self.seal()
        with mock.patch.object(resident.engine.cpu, "runtime_versions", return_value=resident.engine.CUDA_RUNTIME_PINS):
            with self.assertRaises(resident.ScreenError):
                resident.status_batch(str(self.path), self.sha)
        with self.assertRaises(resident.ScreenError):
            resident.status_batch(str(self.path), "0" * 64)

    def test_exact_resource_bounds_and_gpu_opt_in(self):
        for key, value in (("threads", True), ("batch_size", 0), ("batch_size", 17),
                           ("decode_prefetch", 3), ("max_run_seconds", 86401),
                           ("cuda_memory_fraction", float("nan")), ("host_memory_max_bytes", 1),
                           ("gpu_uuid", "GPU-anything"), ("device", "cuda")):
            with self.subTest(key=key), self.assertRaises(resident.ScreenError):
                resident.validate_execution({key: value})
        self.assertEqual(resident.validate_execution({})["device"], "cpu")

    @unittest.skipUnless(shutil.which("ffmpeg"), "requires local ffmpeg")
    def test_real_bounded_parallel_decode_has_exact_pcm_without_models(self):
        source = Path(self.orders[0]["recording"]["path"])
        tool = Path(shutil.which("ffmpeg")).resolve()
        order = copy.deepcopy(self.orders[0])
        order["ffmpeg"]["path"] = str(tool)
        windows = [{"index": 0, "start_ms": 0, "end_ms": 1000},
                   {"index": 1, "start_ms": 1000, "end_ms": 2000}]
        pool = resident.DecodePool(2, resident._implementation(), max_run_seconds=30)
        self.addCleanup(pool.close)
        with resident.screen.opened(source) as fd, resident.screen.opened(tool, executable=True) as executable:
            pool.submit(order, resident.screen.witness(fd), resident.screen.witness(executable), windows, 10, time.monotonic() + 15)
            decoded = pool.collect(time.monotonic() + 15)
        self.assertEqual([item["window"] for item in decoded], windows)
        self.assertEqual([item["pcm"] for item in decoded], [bytes(32000), bytes(32000)])
        processes = [process for process, _ in pool.workers]
        pool.close()
        self.assertTrue(all(not process.is_alive() for process in processes))


class ResidentSchemaTests(unittest.TestCase):
    def setUp(self):
        pipeline = Path(resident.__file__).parent
        self.schema = json.loads((pipeline / "schemas/speaker-screen-accelerated-request.schema.json").read_text())
        self.example = json.loads((pipeline / "examples/speaker-screen-accelerated-request.example.json").read_text())
        self.cuda_example = json.loads((pipeline / "examples/speaker-screen-accelerated-cuda-request.example.json").read_text())
        self.validator = Draft202012Validator(self.schema)

    def test_examples_and_schema_defaults_match_runtime(self):
        Draft202012Validator.check_schema(self.schema)
        self.validator.validate(self.example)
        self.validator.validate(self.cuda_example)
        props = self.schema["properties"]["execution"]["properties"]
        self.assertEqual({key: value["default"] for key, value in props.items()}, resident.DEFAULT_EXECUTION)
        self.assertEqual(resident.validate_execution(self.cuda_example["execution"])["device"], "cuda")

    def test_paths_versions_extra_authority_and_gpu_selection_fail_closed(self):
        for value in ("/", "relative", "/tmp/../secret", "/tmp/./file", "/tmp//file", "/tmp/path/", "/tmp/a\\b", "/tmp/a\n"):
            example = copy.deepcopy(self.example)
            example["state_root"] = value
            self.assertFalse(self.validator.is_valid(example), repr(value))
        for key, value in (("schema_version", True), ("publish", True), ("discover", True)):
            self.assertFalse(self.validator.is_valid({**self.example, key: value}))
        for execution in ({"device": "cuda"}, {"device": "cuda", "gpu_uuid": None},
                          {"device": "cpu", "gpu_uuid": self.cuda_example["execution"]["gpu_uuid"]},
                          {"threads": True}, {"batch_size": 17}, {"decode_prefetch": 3},
                          {"cuda_memory_fraction": 0.8}, {"host_memory_max_bytes": 1}):
            self.assertFalse(self.validator.is_valid({**self.example, "execution": execution}), execution)


if __name__ == "__main__":
    unittest.main()
