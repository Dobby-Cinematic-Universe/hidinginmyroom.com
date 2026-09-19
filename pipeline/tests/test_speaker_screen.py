"""Synthetic standalone orchestration tests. No archive, cloud or ML inference."""
import copy
import ctypes.util
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import wave

from pipeline import speaker_screen as screen


class FakeModel:
    calls = []

    def __init__(self, models, threads, cpu_seconds=610, implementation=None):
        self.models = models
        self.threads = threads

    def analyze(self, pcm, window, timeout):
        self.calls.append(window["index"])
        start = window["start_ms"]
        duration = min(5000, window["end_ms"] - start)
        return {"observation": {"index": window["index"], "start_ms": start,
                "end_ms": start + duration, "speech_ms": duration, "embedding": [1.0, 0.0]},
                "runtime": {"test_engine": "synthetic-v1"}}

    def close(self):
        pass


class SpeakerScreenTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="speaker-screen-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source.wav"
        with wave.open(str(self.source), "wb") as handle:
            handle.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
            handle.writeframes(b"\0\0" * 16000 * 2)
        self.source.chmod(0o600)
        self.original = self.source.read_bytes()
        self.ffmpeg = self.root / "ffmpeg"
        self.ffmpeg.write_bytes(b"synthetic executable, never invoked")
        self.ffmpeg.chmod(0o700)
        self.order = {
            "kind": "himr_cpu_speaker_screen_work_order", "schema_version": 1,
            "recording": {"media_id": "media:synthetic", "path": str(self.source),
                          "sha256": hashlib.sha256(self.original).hexdigest(),
                          "byte_count": len(self.original), "duration_ms": 130000},
            "ffmpeg": {"path": str(self.ffmpeg), "sha256": hashlib.sha256(self.ffmpeg.read_bytes()).hexdigest()},
            "models": {"kind": "himr_speaker_screen_models", "schema_version": 1,
                       "silero_vad": {"path": str(self.root / "silero.onnx"), "sha256": "1" * 64},
                       "ecapa_embedding": {"path": str(self.root / "embedding.ckpt"), "sha256": "2" * 64}},
            "policy": {}, "resources": {"threads": 1, "window_timeout_seconds": 120,
                       "max_run_seconds": 600, "max_windows_per_run": 1},
            "source_verification": "metadata_witness", "output_root": str(self.root / "screen")}
        FakeModel.calls = []

    def plan(self):
        return screen.build_plan(self.order)

    def run_fake(self, plan=None, **kwargs):
        with mock.patch.object(screen, "decode_window", return_value=b"\0\0" * 16000 * 10), \
                mock.patch.object(screen, "ModelProcess", FakeModel), mock.patch.object(screen, "hash_fd", wraps=screen.hash_fd) as hashing:
            result = screen.run_screen(plan or self.plan())
        return result, hashing

    def test_plan_status_no_source_or_model_open_and_no_workspace_write(self):
        self.source.unlink()
        with mock.patch.object(screen, "opened", side_effect=AssertionError("should not open source/model")):
            plan = self.plan()
            status = screen.read_status(plan)
        self.assertEqual(status["state"], "not_started")
        self.assertFalse(Path(self.order["output_root"]).exists())

    def test_read_only_status_resumes_only_pristine_interrupted_initialization(self):
        plan = self.plan()
        output = screen.workspace(Path(self.order["output_root"]), create=True)
        job = output / plan["plan_id"]
        job.mkdir(mode=0o700)
        self.assertEqual(screen.read_status(plan)["state"], "not_started")
        self.assertEqual(list(job.iterdir()), [])
        screen.write_immutable(job / "plan.json", plan)
        self.assertTrue(screen.read_status(plan)["initialization_pending"])
        self.assertEqual([path.name for path in job.iterdir()], ["plan.json"])
        result, _ = self.run_fake(plan)
        self.assertEqual(result["completed_windows"], 1)

    def test_missing_binding_with_checkpoint_does_not_look_unstarted(self):
        result, _ = self.run_fake()
        job = Path(self.order["output_root"]) / result["plan_id"]
        (job / "source-binding.json").unlink()
        with self.assertRaisesRegex(screen.ScreenError, "unexpected artifacts"):
            screen.read_status(self.plan())

    def test_metadata_default_does_not_hash_source_and_retains_original(self):
        result, hashing = self.run_fake()
        self.assertEqual(len(hashing.call_args_list), 1)  # FFmpeg only.
        self.assertFalse(result["source_binding"]["source_sha256_reverified"])
        self.assertEqual(result["state"], "paused")
        self.assertEqual(result["completed_windows"], 1)
        self.assertEqual(self.original, self.source.read_bytes())
        self.assertNotIn('"embedding":', json.dumps(result))
        workspace = Path(self.order["output_root"])
        self.assertEqual(workspace.stat().st_mode & 0o777, 0o700)
        for path in (workspace / result["plan_id"]).iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_resume_skips_completed_windows_and_final_replay(self):
        plan = self.plan()
        for _ in plan["windows"]:
            result, _ = self.run_fake(plan)
        self.assertEqual(result["state"], "completed")
        self.assertEqual(FakeModel.calls, [row["index"] for row in plan["windows"]])
        with mock.patch.object(screen, "decode_window", side_effect=AssertionError("no decode on reuse")), \
                mock.patch.object(screen, "ModelProcess", side_effect=AssertionError("no model on reuse")):
            self.assertEqual(screen.run_screen(plan), result)
        status = screen.read_status(plan)
        self.assertEqual(status["summary"], result["summary"])
        self.assertFalse(status["source_currently_rechecked"])

    def test_source_changed_between_invocations_rejected(self):
        self.run_fake()
        with self.source.open("r+b") as handle:
            handle.write(b"DIFF")
        with self.assertRaises(screen.ScreenError):
            self.run_fake()
        self.assertEqual(FakeModel.calls, [0])

    def test_source_replaced_same_bytes_rejected_by_witness(self):
        self.run_fake()
        changed = self.root / "replacement.wav"
        changed.write_bytes(self.original)
        changed.chmod(0o600)
        os.replace(changed, self.source)
        with self.assertRaises(screen.ScreenError):
            self.run_fake()

    def test_source_sha_opt_in_and_wrong_hash_fail_closed(self):
        self.order["source_verification"] = "sha256"
        self.order["recording"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(screen.ScreenError, "source SHA-256"):
            self.run_fake()

    def test_corrupt_checkpoint_is_not_silently_recomputed(self):
        result, _ = self.run_fake()
        path = Path(self.order["output_root"]) / result["plan_id"] / "window-0000.json"
        path.write_text('{"truncated":')
        with self.assertRaises(screen.ScreenError):
            self.run_fake()
        self.assertEqual(path.read_text(), '{"truncated":')

    def test_wrong_checkpoint_window_and_nonfinite_vector_rejected(self):
        result, _ = self.run_fake()
        path = Path(self.order["output_root"]) / result["plan_id"] / "window-0000.json"
        row = json.loads(path.read_text())
        row["window"]["end_ms"] += 1
        path.write_text(json.dumps(row))
        with self.assertRaises(screen.ScreenError):
            screen.read_status(self.plan())

    def test_swapped_checkpoint_observation_cannot_borrow_another_pcm_binding(self):
        result, _ = self.run_fake()
        path = Path(self.order["output_root"]) / result["plan_id"] / "window-0000.json"
        row = json.loads(path.read_text())
        target = self.plan()["windows"][1]
        row["observation"] = {**target, "speech_ms": 0, "embedding": None}
        path.write_text(json.dumps(row))
        with self.assertRaisesRegex(screen.ScreenError, "different probe"):
            self.run_fake()

    def test_completed_result_missing_checkpoint_fails_before_dispatch(self):
        self.order["resources"]["max_windows_per_run"] = 20
        result, _ = self.run_fake()
        job = Path(self.order["output_root"]) / result["plan_id"]
        (job / "window-0000.json").unlink()
        with mock.patch.object(screen, "decode_window", side_effect=AssertionError("no new decode")), \
                self.assertRaisesRegex(screen.ScreenError, "final result differs"):
            screen.run_screen(self.plan())

    def test_implementation_change_fails_before_dispatch(self):
        plan = self.plan()
        with mock.patch.object(screen, "implementation_hashes", return_value={"changed": "f" * 64}), \
                mock.patch.object(screen, "decode_window", side_effect=AssertionError("no decode")), \
                self.assertRaisesRegex(screen.ScreenError, "implementation changed"):
            screen.run_screen(plan)

    def test_tool_mutation_during_inference_does_not_publish_checkpoint(self):
        original = FakeModel.analyze
        def changing_tool(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            self.ffmpeg.write_bytes(b"changed executable bytes")
            return result
        plan = self.plan()
        with mock.patch.object(FakeModel, "analyze", changing_tool), self.assertRaises(screen.ScreenError):
            self.run_fake(plan)
        self.assertEqual(screen.read_status(plan)["completed_windows"], 0)

    def test_misindexed_engine_observation_does_not_publish_checkpoint(self):
        original = FakeModel.analyze
        def incorrect(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            result["observation"]["index"] = 1
            return result
        plan = self.plan()
        with mock.patch.object(FakeModel, "analyze", incorrect), self.assertRaisesRegex(screen.ScreenError, "different probe"):
            self.run_fake(plan)
        self.assertEqual(screen.read_status(plan)["completed_windows"], 0)

    def test_immutable_publication_has_one_link_and_never_overwrites(self):
        path = self.root / "immutable.json"
        screen.write_immutable(path, {"v": 1})
        self.assertEqual(path.stat().st_nlink, 1)
        screen.write_immutable(path, {"v": 1})
        with self.assertRaises(screen.ScreenError):
            screen.write_immutable(path, {"v": 2})
        self.assertEqual(screen.read_json(path), {"v": 1})

    def test_shared_ancestor_still_requires_private_output_leaf(self):
        parent = self.root / "shared"
        parent.mkdir(mode=0o777)
        parent.chmod(0o777)
        output = parent / "screen"
        output.mkdir(mode=0o755)
        self.order["output_root"] = str(output)
        with self.assertRaisesRegex(RuntimeError, "owned and private"):
            self.run_fake()
        self.assertEqual(list(output.iterdir()), [])

    def test_retained_output_cannot_be_redirected_by_ancestor_rename(self):
        output = self.root / "private"
        output.mkdir(mode=0o700)
        moved = self.root / "moved"
        with screen.paths.retained_directory(output):
            output.rename(moved)
            output.mkdir(mode=0o700)  # A replacement at the old textual path.
            screen.write_immutable(output / "result.json", {"retained": True})
            self.assertEqual(screen.read_json(output / "result.json"), {"retained": True})
        self.assertFalse((output / "result.json").exists())
        self.assertEqual(screen.read_json(moved / "result.json"), {"retained": True})

    def test_increased_minimum_speech_abstains_instead_of_failing(self):
        self.order["policy"] = {"min_speech_ms": 5000}
        original = FakeModel.analyze
        def shorter(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            row = result["observation"]
            row.update(end_ms=row["start_ms"] + 3000, speech_ms=3000)
            return result
        with mock.patch.object(FakeModel, "analyze", shorter):
            result, _ = self.run_fake()
        self.assertEqual(result["summary"]["status"], "uncertain")
        self.assertEqual(result["summary"]["coverage"]["embedded_excerpts"], 0)

    def test_supported_multiple_voices_end_to_end_without_exact_person_count(self):
        self.order["recording"]["duration_ms"] = 240000
        self.order["resources"]["max_windows_per_run"] = 20
        original = FakeModel.analyze
        def diverse(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            if result["observation"]["index"] % 2:
                result["observation"]["embedding"] = [0.0, 1.0]
            return result
        with mock.patch.object(FakeModel, "analyze", diverse):
            result, _ = self.run_fake()
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["summary"]["status"], "multiple_speaker_candidate")
        self.assertFalse(result["summary"]["semantics"]["exact_speaker_count_claimed"])

    def test_throughput_preset_preserves_coverage_and_input_metadata(self):
        original = copy.deepcopy(self.order)
        self.order["policy"] = {"stride_ms": 120000, "match_cosine_min": 0.9}
        prepared = screen.prepare_order(self.order, "throughput", self.root / "faster", threads=2)
        self.assertEqual(prepared["policy"], screen.validate_order(self.order)["policy"])
        for key in ("recording", "ffmpeg", "models", "source_verification"):
            self.assertEqual(prepared[key], original[key])
        self.assertEqual(prepared["resources"]["max_windows_per_run"], 512)
        self.assertEqual(prepared["resources"]["max_run_seconds"], 3600)
        self.assertFalse(prepared["resources"]["early_stop_on_positive"])
        self.assertEqual(self.order["resources"], original["resources"])

    def test_fast_preset_spreads_probes_and_keeps_thresholds(self):
        self.order["recording"]["duration_ms"] = 12 * 3600000
        self.order["policy"] = {"match_cosine_min": 0.9, "min_support": 3}
        prepared = screen.prepare_order(self.order, "fast-triage", self.root / "fast")
        self.assertEqual(prepared["policy"]["match_cosine_min"], 0.9)
        self.assertEqual(prepared["policy"]["min_support"], 3)
        self.assertTrue(prepared["resources"]["early_stop_on_positive"])
        windows = screen.build_plan(prepared)["windows"]
        self.assertEqual(len(windows), 64)
        self.assertEqual(windows[0]["start_ms"], 0)
        self.assertEqual(windows[-1]["end_ms"], 12 * 3600000)

    def test_prepare_cli_seals_new_order_without_starting_or_touching_old_workspace(self):
        original = self.root / "original.json"
        screen.write_immutable(original, self.order)
        output = self.root / "fast.json"
        new_root = self.root / "fast-screen"
        args = ["prepare", "--work-order", str(original), "--expected-sha256", screen.digest(self.order),
                "--preset", "fast-triage", "--output", str(output), "--output-root", str(new_root)]
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stream:
            self.assertEqual(screen.main(args), 0)
        receipt = json.loads(stream.getvalue())
        self.assertFalse(receipt["screening_started"])
        self.assertEqual(receipt["sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
        self.assertFalse(new_root.exists())
        self.assertFalse(Path(self.order["output_root"]).exists())
        self.assertEqual(original.read_bytes(), screen.canonical(self.order))

    def test_prepare_cli_rejects_order_inside_old_or_new_workspace(self):
        original = self.root / "original.json"
        screen.write_immutable(original, self.order)
        new_root = self.root / "fast-screen"
        for root in (Path(self.order["output_root"]), new_root):
            with self.subTest(root=root), mock.patch("sys.stderr", new_callable=io.StringIO):
                self.assertEqual(screen.main([
                    "prepare", "--work-order", str(original), "--expected-sha256", screen.digest(self.order),
                    "--preset", "fast-triage", "--output", str(root / "order.json"),
                    "--output-root", str(new_root)]), 2)
            self.assertFalse(root.exists())

    def test_early_positive_retains_partial_coverage_and_replays_without_inference(self):
        self.order["recording"]["duration_ms"] = 600000
        self.order["resources"].update(max_windows_per_run=512, early_stop_on_positive=True)
        original = FakeModel.analyze

        def diverse(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            if result["observation"]["index"] % 2:
                result["observation"]["embedding"] = [0.0, 1.0]
            return result

        with mock.patch.object(FakeModel, "analyze", diverse):
            result, _ = self.run_fake()
        self.assertEqual(FakeModel.calls, [0, 1, 2, 3])
        self.assertEqual(result["state"], "paused")
        self.assertTrue(result["screening_decision_complete"])
        self.assertEqual(result["stop_reason"], "supported_multiple_speakers")
        self.assertEqual(result["remaining_windows"], 6)
        self.assertIn("planned_probes_uninspected", result["summary"]["reason_flags"])
        self.assertFalse(result["summary"]["coverage"]["whole_recording_inspected"])
        self.assertNotIn('"embedding":', json.dumps(result))
        with mock.patch.object(screen, "decode_window", side_effect=AssertionError("no repeated work")):
            self.assertEqual(screen.run_screen(self.plan()), result)
        job = Path(self.order["output_root"]) / result["plan_id"]
        (job / "window-0003.json").unlink()
        with mock.patch.object(screen, "decode_window", side_effect=AssertionError("do not repair")), \
                self.assertRaisesRegex(screen.ScreenError, "final result differs"):
            screen.run_screen(self.plan())

    def test_partial_negative_never_finishes_decision(self):
        self.order["recording"]["duration_ms"] = 600000
        self.order["resources"].update(max_windows_per_run=4, early_stop_on_positive=True)
        result, _ = self.run_fake()
        self.assertEqual(result["state"], "paused")
        self.assertFalse(result["screening_decision_complete"])
        self.assertEqual(result["summary"]["status"], "uncertain")
        self.assertEqual(result["stop_reason"], "invocation_limit")
        self.assertEqual(FakeModel.calls, [0, 1, 2, 3])

    def test_early_stop_disabled_by_default_and_negative_keeps_all_probes(self):
        self.order["recording"]["duration_ms"] = 600000
        self.order["resources"].update(max_windows_per_run=512, early_stop_on_positive=True)
        result, _ = self.run_fake()
        self.assertEqual(FakeModel.calls, list(range(10)))
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["stop_reason"], "sampling_plan_completed")
        self.assertEqual(result["summary"]["status"], "no_second_voice_detected_in_sampled_audio")

    def test_early_stop_flag_is_strictly_boolean(self):
        for value in (0, 1, "true", None):
            with self.subTest(value=value), self.assertRaisesRegex(screen.ScreenError, "boolean"):
                screen.validate_order({**self.order, "resources": {
                    **self.order["resources"], "early_stop_on_positive": value}})

    def test_ipc_send_is_timed_even_when_child_never_receives(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            screen.send_message(left, {"pcm": "a" * 700000}, started + 0.05)
        self.assertLess(time.monotonic() - started, 2)

    @unittest.skipUnless(ctypes.util.find_library("seccomp"), "libseccomp unavailable")
    def test_real_spawned_cpu_worker_handles_digital_silence_without_models(self):
        worker = screen.ModelProcess(self.order["models"], 1)
        try:
            answer = worker.analyze(b"\0\0" * 32000,
                                    {"index": 0, "start_ms": 0, "end_ms": 2000}, 10)
            self.assertIsNone(answer["observation"]["embedding"])
            self.assertEqual(answer["observation"]["speech_ms"], 0)
        finally:
            worker.close()
        self.assertFalse(worker.process.is_alive())

    def test_nonempty_unmarked_workspace_rejected_without_touching_contents(self):
        output = Path(self.order["output_root"])
        output.mkdir(mode=0o700)
        sentinel = output / "existing-campaign.json"
        sentinel.write_bytes(b"existing")
        with self.assertRaisesRegex(screen.ScreenError, "nonempty unmarked"):
            self.run_fake()
        self.assertEqual(list(output.iterdir()), [sentinel])

    def test_symlink_input_and_workspace_rejected(self):
        linked = self.root / "source-link.wav"
        linked.symlink_to(self.source)
        self.order["recording"]["path"] = str(linked)
        with self.assertRaises((OSError, screen.ScreenError)):
            self.run_fake()
        self.order["recording"]["path"] = str(self.source)
        workspace = Path(self.order["output_root"])
        # The first rejected invocation may have created its dedicated workspace.
        self.order["output_root"] = str(self.root / "linked-output")
        Path(self.order["output_root"]).symlink_to(workspace, target_is_directory=True)
        with self.assertRaises((OSError, screen.ScreenError)):
            self.run_fake()

    def test_concurrent_worker_rejected(self):
        output = screen.workspace(Path(self.order["output_root"]), create=True)
        with screen.locked(output), self.assertRaisesRegex(screen.ScreenError, "another screen"):
            self.run_fake()

    def test_runtime_change_rejected_without_new_checkpoint(self):
        result, _ = self.run_fake()
        original = FakeModel.analyze

        def changed(instance, *args, **kwargs):
            value = original(instance, *args, **kwargs)
            value["runtime"] = {"test_engine": "different"}
            return value

        with mock.patch.object(FakeModel, "analyze", changed), self.assertRaisesRegex(screen.ScreenError, "runtime changed"):
            self.run_fake()
        self.assertEqual(screen.read_status(self.plan())["completed_windows"], 1)

    def test_failed_second_window_retains_first_and_retries_only_second(self):
        self.run_fake()
        with mock.patch.object(screen, "decode_window", side_effect=screen.ScreenError("disk I/O failure")), \
                self.assertRaises(screen.ScreenError):
            screen.run_screen(self.plan())
        self.assertEqual(screen.read_status(self.plan())["completed_windows"], 1)
        self.run_fake()
        self.assertEqual(FakeModel.calls, [0, 1])

    def test_invalid_work_order_fields_types_paths_and_bounds(self):
        for path, value in ((["schema_version"], True), (["resources", "threads"], 3),
                            (["recording", "duration_ms"], False), (["recording", "path"], "/tmp/../media"),
                            (["source_verification"], "trust_all")):
            invalid = copy.deepcopy(self.order)
            target = invalid
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with self.subTest(path=path), self.assertRaises((screen.ScreenError, ValueError)):
                screen.validate_order(invalid)

    def test_json_duplicate_keys_and_expected_hash(self):
        path = self.root / "order.json"
        path.write_bytes(b'{"a":1,"a":2}')
        path.chmod(0o600)
        with self.assertRaises(screen.ScreenError):
            screen.read_json(path)
        path.write_bytes(screen.canonical(self.order))
        with self.assertRaisesRegex(screen.ScreenError, "SHA-256 mismatch"):
            screen.read_json(path, "f" * 64)

    @unittest.skipUnless(shutil.which("ffmpeg") and ctypes.util.find_library("seccomp"), "FFmpeg/libseccomp unavailable")
    def test_real_synthetic_pcm_decode_cpu_only_without_models(self):
        ffmpeg = str(Path(shutil.which("ffmpeg")).resolve())
        window = {"index": 0, "start_ms": 0, "end_ms": 2000}
        with screen.opened(self.source) as source, screen.opened(ffmpeg, executable=True) as tool:
            pcm = screen.decode_window(source, tool, window, 10)
        self.assertEqual(pcm, b"\0\0" * 32000)
        self.assertEqual(self.original, self.source.read_bytes())

    @unittest.skipUnless(ctypes.util.find_library("seccomp"), "libseccomp unavailable")
    def test_offline_sandbox_denies_native_network_in_child(self):
        code = "from pipeline.speaker_screen import deny_internet; import socket; deny_internet(); socket.socket(socket.AF_INET)"
        result = subprocess.run([sys.executable, "-B", "-c", code], cwd=screen.ROOT,
                                capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Operation not permitted", result.stderr)


if __name__ == "__main__":
    unittest.main()
