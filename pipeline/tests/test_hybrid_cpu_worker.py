"""CPU worker tests use synthetic artifacts and never run ASR or real media."""

from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from pipeline import hybrid_cpu_worker as worker
from pipeline import asr_whispercpp as adapter
from pipeline.salad_transcription_contract import canonical_bytes


class HybridCpuWorkerTests(unittest.TestCase):
    def setUp(self):
        # The production adapter deliberately rejects /tmp output roots.
        temporary = tempfile.TemporaryDirectory(prefix=".cpu-worker-test-", dir=Path(__file__).parent)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "source"
        self.tools = self.root / "tools"
        self.source.mkdir(mode=0o700)
        self.tools.mkdir(mode=0o700)
        self.audio = self.source / "audio.flac"
        self.audio.write_bytes(b"synthetic source bytes; no decoder is invoked")
        self.audio.chmod(0o600)
        self.manifest = self.source / "recording-input.json"
        self.samples = 3 * 16000
        self.write_manifest()
        executable = self.tools / "whisper-cli"
        executable.write_bytes(b"synthetic executable; never invoked")
        executable.chmod(0o700)
        profile = next(value for value in worker.engine_profiles.ENGINE_PROFILES if value["admission"] == "current_new_batch")
        self.engine = {"executable": str(executable), **{key: copy.deepcopy(profile[key])
                       for key in ("expected_sha256", "version_label", "version_evidence", "build")}}
        model = self.tools / "model.bin"
        model.write_bytes(b"synthetic model; never loaded")
        model.chmod(0o600)
        self.model = {"path": str(model), "expected_sha256": self.sha(model.read_bytes()),
                      "model_id": "model-synthetic", "name": "Synthetic test model", "revision": "test-revision",
                      "source": "synthetic-fixture", "license_label": "test-only"}
        self.probe = self.tools / "ffprobe"
        self.ffmpeg_file = self.tools / "ffmpeg"
        for path in (self.probe, self.ffmpeg_file):
            path.write_bytes(b"synthetic tool; never invoked")
            path.chmod(0o700)
        self.ffprobe = {"path": str(self.probe), "sha256": self.sha(self.probe.read_bytes())}
        self.ffmpeg = {"path": str(self.ffmpeg_file), "sha256": self.sha(self.ffmpeg_file.read_bytes())}
        self.output = self.root / "output"
        self.invoked = []
        self.prepared = []

    @staticmethod
    def sha(body):
        return hashlib.sha256(body).hexdigest()

    def write_manifest(self):
        self.manifest.write_bytes(canonical_bytes({
            "kind": "himr_longform_recording_input_manifest", "schema_version": 1, "boundary_candidates": [],
            "recording": {"recording_id": "original-recording", "media_id": "original-media",
                          "input": {"artifact_id": "original-audio-artifact", "path": str(self.audio),
                                    "sha256": self.sha(self.audio.read_bytes()), "byte_count": self.audio.stat().st_size,
                                    "channels": 1, "sample_rate_hz": 16000, "total_samples": self.samples,
                                    "duration_ms": (self.samples * 1000 + 8000) // 16000}},
        }))
        self.manifest.chmod(0o600)

    def arguments(self, **overrides):
        result = {"recording_input": self.manifest, "expected_sha256": self.sha(self.manifest.read_bytes()),
                  "output_root": self.output, "engine": self.engine, "model": self.model,
                  "ffprobe": self.ffprobe, "ffmpeg": self.ffmpeg, "window_seconds": 1}
        result.update(overrides)
        return result

    def prepare(self, plan, window, *, lease_fds=()):
        self.prepared.append(window["ordinal"])
        directory = worker._derived_path(plan, window)
        worker._private(directory)
        audio_path = directory / "audio.flac"
        body = f"synthetic window {window['ordinal']}".encode()
        audio_path.write_bytes(body)
        audio_path.chmod(0o600)
        audio = {"path": str(audio_path), "sha256": self.sha(body), "byte_count": len(body)}
        manifest = worker._window_manifest(plan, window, audio)
        worker._write(directory / "recording-input.json", manifest)
        receipt = {"kind": "himr_hybrid_cpu_window_audio", "schema_version": 1, "plan_id": plan["plan_id"],
                   "window": copy.deepcopy(window), "source_audio": copy.deepcopy(plan["recording"]["audio"]),
                   "audio": audio, "manifest": worker._file_binding(directory / "recording-input.json")}
        worker._write(directory / "receipt.json", receipt)
        return receipt

    def invoke(self, plan_path, plan_sha, window, timeout, descriptor, *, lease_fds=()):
        self.invoked.append(window["ordinal"])
        self.assertTrue(os.fstat(descriptor))
        for inherited in lease_fds:
            self.assertTrue(os.fstat(inherited))
        plan = worker._read_json(plan_path, plan_sha)
        order = worker.build_work_order(plan, window)
        self.assertEqual(order["window"]["offset_ms"], 0)
        self.assertNotEqual(order["input"]["path"], str(self.audio))
        binding = worker._result_binding(order)
        result_path = Path(binding["path"])
        worker._private(result_path.parent)
        end = min(500, window["duration_ms"])
        raw = {"params": {"translate": False}, "result": {"language": "en"}, "transcription": [
            {"offsets": {"from": 0, "to": end}, "text": f"window {window['ordinal']}", "tokens": [
                {"offsets": {"from": 0, "to": end}, "text": "word", "id": 1, "p": 0.5, "t_dtw": 0}
            ]}
        ]}
        normalized = adapter.normalize_engine_output(raw, requested_language="en", window=binding["window"])
        raw_path = result_path.parent / "whisper.raw.json"
        normalized_path = result_path.parent / "transcript.normalized.json"
        worker._write(raw_path, raw)
        worker._write(normalized_path, normalized)
        run_id = f"run-synthetic-{window['ordinal']}"
        artifacts = [adapter.artifact_row(processing_run_id=run_id, kind=kind, final_path=path, staged_path=path)
                     for kind, path in (("whispercpp_output_json_full", raw_path), ("transcript_normalized_json", normalized_path))]
        result = {
            "status": "completed", "dry_run": False, "job_id": order["job_id"],
            "work_order_sha256": binding["work_order_sha256"], "recipe_id": binding["recipe_id"],
            "result_key": binding["result_key"], "result_path": str(result_path), "window": binding["window"],
            "input": {"sha256": order["input"]["expected_sha256"]},
            "engine": {"sha256": self.engine["expected_sha256"]}, "model": {"sha256": self.model["expected_sha256"]},
            "catalog_context": None, "processing_run": {"processing_run_id": run_id},
            "artifacts": artifacts, "transcript": normalized,
        }
        worker._write(result_path, result)

    def run_mocked(self, **kwargs):
        with mock.patch.object(worker, "_prepare_window", side_effect=self.prepare), mock.patch.object(worker, "_invoke_window", side_effect=self.invoke):
            return worker.run_cpu(**self.arguments(**kwargs))

    def test_plan_is_metadata_only_and_preserves_original_identity(self):
        original_read = worker._read_bytes
        opened = []

        def read(path):
            opened.append(path)
            return original_read(path)

        with mock.patch.object(worker, "_read_bytes", side_effect=read), mock.patch.object(worker.subprocess, "run", side_effect=AssertionError("process started")):
            plan = worker.build_cpu_plan(**self.arguments())
        self.assertEqual(plan["recording"]["recording_id"], "original-recording")
        self.assertEqual(plan["recording"]["media_id"], "original-media")
        self.assertNotIn(self.audio, opened)
        self.assertNotIn(Path(self.model["path"]), opened)
        self.assertFalse(self.output.exists())
        self.assertEqual(plan["threads"], 6)
        self.assertTrue(plan["resource_policy"]["physical_window_flac"])
        self.assertIn("adapter", plan["implementation"])
        self.assertIn("worker", plan["implementation"])

    def test_invalid_cpu_limits_rejected_before_output(self):
        for options in ({"threads": 0}, {"threads": 9}, {"threads": True}, {"timeout_seconds": 86401},
                        {"window_seconds": 1801}, {"max_windows": 0}, {"max_windows": True}):
            with self.subTest(options=options), self.assertRaises(worker.CpuWorkerError):
                worker.run_cpu(**self.arguments(**options))
        self.assertFalse(self.output.exists())

    def test_source_over_24_hours_rejected(self):
        self.samples = worker.MAX_RECORDING_SAMPLES + 1
        self.write_manifest()
        with self.assertRaisesRegex(worker.CpuWorkerError, "at most 24 hours"):
            worker.build_cpu_plan(**self.arguments())

    def test_current_reviewed_engine_required(self):
        changed = copy.deepcopy(self.engine)
        changed["build"]["configuration"] = ["GGML_CUDA=ON"]
        with self.assertRaisesRegex(worker.CpuWorkerError, "current reviewed"):
            worker.build_cpu_plan(**self.arguments(engine=changed))

    def test_tiny_submillisecond_tail_is_retained_in_physical_last_window(self):
        self.samples = 16001
        self.write_manifest()
        plan = worker.build_cpu_plan(**self.arguments())
        self.assertEqual(len(plan["windows"]), 1)
        self.assertEqual(plan["windows"][0]["end_sample"], 16001)
        self.assertEqual(plan["timing"]["submillisecond_tail_samples"], 1)
        self.assertFalse(plan["timing"]["exact_sample_coverage_claimed"])

    def test_one_window_per_cycle_with_correct_parent_segment_and_token_offsets(self):
        for ordinal in range(3):
            outcome = self.run_mocked()
            self.assertEqual(outcome["executed_windows"], 1)
            self.assertEqual(outcome["completed_windows"], ordinal + 1)
            self.assertEqual(outcome["status"], "completed" if ordinal == 2 else "progress")
        self.assertEqual(self.invoked, [0, 1, 2])
        transcript = worker._read_json(Path(outcome["transcript"]["path"]), outcome["transcript"]["sha256"])
        self.assertEqual([segment["start_ms"] for segment in transcript["segments"]], [0, 1000, 2000])
        self.assertEqual([segment["tokens"][0]["end_ms"] for segment in transcript["segments"]], [500, 1500, 2500])
        self.assertEqual(transcript["recording"]["media_id"], "original-media")
        self.assertEqual(transcript["policy"]["visibility"], "private")
        self.assertFalse(transcript["policy"]["summary"])
        self.assertFalse(transcript["policy"]["diarization"])

    def test_completed_windows_reused_without_preparation_or_inference(self):
        first = self.run_mocked(max_windows=3)
        with mock.patch.object(worker, "_prepare_window", side_effect=AssertionError("repeated media read")), mock.patch.object(worker, "_invoke_window", side_effect=AssertionError("repeated inference")):
            second = worker.run_cpu(**self.arguments())
        self.assertEqual(second["status"], "completed")
        self.assertEqual(second["executed_windows"], 0)
        self.assertEqual(second["completion"], first["completion"])

    def test_failure_leaves_completed_prefix_reusable(self):
        def fail_second(*args, **kwargs):
            if args[2]["ordinal"] == 1:
                raise worker.CpuWorkerError("synthetic failure")
            return self.invoke(*args, **kwargs)

        with mock.patch.object(worker, "_prepare_window", side_effect=self.prepare), mock.patch.object(worker, "_invoke_window", side_effect=fail_second):
            with self.assertRaisesRegex(worker.CpuWorkerError, "synthetic failure"):
                worker.run_cpu(**self.arguments(max_windows=3))
        self.assertTrue((self.output / "windows" / "00000.json").exists())
        self.run_mocked(max_windows=3)
        self.assertEqual(self.invoked, [0, 1, 2])

    def test_result_commit_before_window_receipt_is_reconciled_without_rerunning(self):
        real_write = worker._write

        def crash(path, value):
            if path.parent.name == "windows":
                raise OSError("synthetic receipt crash")
            return real_write(path, value)

        with mock.patch.object(worker, "_prepare_window", side_effect=self.prepare), mock.patch.object(worker, "_invoke_window", side_effect=self.invoke), mock.patch.object(worker, "_write", side_effect=crash):
            with self.assertRaises(worker.CpuWorkerError):
                worker.run_cpu(**self.arguments())
        self.assertEqual(self.invoked, [0])
        self.run_mocked()
        # The old result is adopted; this call's one new window is ordinal 1.
        self.assertEqual(self.invoked, [0, 1])

    def test_tampered_completed_artifact_is_rejected_not_retranscribed(self):
        self.run_mocked()
        receipt = worker._read_json(self.output / "windows" / "00000.json")
        raw = Path(receipt["result"]["path"]).parent / "whisper.raw.json"
        raw.write_bytes(b'{"changed":true}\n')
        with mock.patch.object(worker, "_invoke_window", side_effect=AssertionError("unexpected rerun")):
            with self.assertRaises(worker.CpuWorkerError):
                worker.run_cpu(**self.arguments())

    def test_changed_implementation_cannot_reuse_the_old_workspace(self):
        self.run_mocked()
        current = worker._implementation()
        current["adapter"]["sha256"] = "0" * 64
        with mock.patch.object(worker, "_implementation", return_value=current), self.assertRaisesRegex(worker.CpuWorkerError, "immutable CPU evidence differs"):
            worker.run_cpu(**self.arguments())

    def test_output_cannot_overlap_sources_or_follow_symlink_ancestors(self):
        with self.assertRaises(worker.CpuWorkerError):
            worker.build_cpu_plan(**self.arguments(output_root=self.source / "nested"))
        target = self.root / "target"
        target.mkdir(mode=0o700)
        link = self.root / "link"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(worker.CpuWorkerError):
            worker.run_cpu(**self.arguments(output_root=link / "output"))
        self.assertFalse((target / "output").exists())

    def test_lease_descriptors_are_propagated_and_not_closed_by_worker(self):
        path = self.root / "external.lock"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            with mock.patch.object(worker, "_prepare_window", side_effect=self.prepare) as prepare, mock.patch.object(worker, "_invoke_window", side_effect=self.invoke) as invoke:
                worker.run_cpu(**self.arguments(lease_fds=(descriptor,)))
            self.assertIn(descriptor, prepare.call_args.kwargs["lease_fds"])
            self.assertEqual(invoke.call_args.kwargs["lease_fds"], (descriptor,))
            self.assertTrue(os.fstat(descriptor))
        finally:
            os.close(descriptor)

    def test_child_environment_excludes_cloud_credentials(self):
        with mock.patch.dict(os.environ, {"SALAD_API_KEY": "must-never-reach-cpu", "AWS_SECRET_ACCESS_KEY": "secret", "HTTP_PROXY": "private"}):
            process = mock.Mock(returncode=0)
            process.communicate.return_value = (b'{"status":"completed"}', b"")
            with mock.patch.object(worker.subprocess, "Popen", return_value=process) as started:
                worker._invoke_window(self.manifest, "a" * 64, {"ordinal": 0}, 1, 7, lease_fds=(8,))
        environment = started.call_args.kwargs["env"]
        self.assertNotIn("SALAD_API_KEY", environment)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
        self.assertNotIn("HTTP_PROXY", environment)
        self.assertEqual(started.call_args.kwargs["pass_fds"], (7, 8))

    def test_child_timeout_terminates_its_process_group(self):
        process = mock.Mock()
        process.communicate.side_effect = subprocess.TimeoutExpired("synthetic", 1)
        with mock.patch.object(worker.subprocess, "Popen", return_value=process), mock.patch.object(worker.adapter, "terminate_group") as terminate:
            with self.assertRaises(subprocess.TimeoutExpired):
                worker._invoke_window(self.manifest, "a" * 64, {"ordinal": 0}, 1, 7)
        terminate.assert_called_once_with(process)

    def test_low_space_holds_before_window_extraction(self):
        plan = worker.build_cpu_plan(**self.arguments())
        with mock.patch.object(worker.shutil, "disk_usage", return_value=mock.Mock(free=1)), mock.patch.object(worker.subprocess, "run", side_effect=AssertionError("decoder launched")):
            with self.assertRaisesRegex(worker.CpuWorkerError, "insufficient free space"):
                worker._prepare_window(plan, plan["windows"][0])

    def test_source_manifest_hash_change_holds_before_worker_output(self):
        arguments = self.arguments()
        self.manifest.write_bytes(self.manifest.read_bytes() + b"\n")
        with self.assertRaises(worker.CpuWorkerError):
            worker.run_cpu(**arguments)
        self.assertFalse(self.output.exists())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "requires media tools, not ASR")
    def test_real_synthetic_physical_windows_have_exact_samples_without_asr(self):
        ffmpeg_path = Path(shutil.which("ffmpeg")).resolve()
        ffprobe_path = Path(shutil.which("ffprobe")).resolve()
        completed = subprocess.run([str(ffmpeg_path), "-v", "error", "-y", "-f", "lavfi", "-i",
                                    "sine=frequency=440:sample_rate=16000:duration=2.125", "-c:a", "flac", str(self.audio)],
                                   stdin=subprocess.DEVNULL, capture_output=True, check=False,
                                   env=worker._worker_environment())
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.samples = 34_000
        self.write_manifest()
        plan = worker.build_cpu_plan(**self.arguments(ffmpeg={"path": str(ffmpeg_path), "sha256": self.sha(ffmpeg_path.read_bytes())},
                                                       ffprobe={"path": str(ffprobe_path), "sha256": self.sha(ffprobe_path.read_bytes())}))
        original = self.audio.read_bytes()
        with mock.patch.object(worker.adapter, "run_asr", side_effect=AssertionError("ASR invoked")):
            for window in plan["windows"]:
                receipt = worker._prepare_window(plan, window)
                manifest = worker._read_json(Path(receipt["manifest"]["path"]))
                self.assertEqual(manifest["recording"]["input"]["total_samples"], window["end_sample"] - window["start_sample"])
                self.assertEqual(worker.build_work_order(plan, window)["window"]["offset_ms"], 0)
                self.assertNotEqual(receipt["audio"]["path"], str(self.audio))
                self.assertEqual(worker._prepare_window(plan, window), receipt)
        self.assertEqual(self.audio.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
