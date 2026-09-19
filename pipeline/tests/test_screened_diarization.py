"""Private runner integration; synthetic checkpoints/audio, no ML or GPU work."""
from contextlib import ExitStack
import copy
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import wave

from pipeline import screened_diarization as runner
from pipeline.tests import test_speaker_screen_archive_guided as fixtures
from pipeline.tests.test_speaker_screen_accelerated import FakeModel


def request(root, source, ffmpeg):
    return {"kind": "himr_screened_diarization_request", "schema_version": 1,
        "source": {"kind": "batch", "binding": source}, "include_uncertain": False,
        "media_ids": None, "state_root": str(root / "diarization"), "model_bundle": None,
        "python": None, "ffmpeg": ffmpeg, "speaker_bounds": [],
        "execution": {**runner.DEFAULT_EXECUTION, "device": "cpu", "gpu_uuid": None},
        "limits": {**runner.DEFAULT_LIMITS, "min_free_bytes": 1024**3},
        "blocking_units": ["archive-screen-test.service"], "retain_normalized_audio": False,
        "purpose": "private_unvalidated_pilot"}


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ArchiveRunnerTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.seal()
        FakeModel.mode = "multiple"
        self.fixture.run_fake()
        self.root = self.fixture.root
        self.request = request(self.root,
            {"path": str(self.fixture.path), "sha256": self.fixture.sha}, self.fixture.orders[0]["ffmpeg"])
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.impl = {"screened_diarization_engine.py": "a" * 64}
        self.stack.enter_context(mock.patch.object(runner, "implementation", return_value=self.impl))
        self.admit = self.stack.enter_context(mock.patch.object(runner.engine, "admit_bundle", return_value={"synthetic": True}))
        self.provenance_check = self.stack.enter_context(mock.patch.object(
            runner.engine, "validate_provenance", side_effect=lambda value, *a, **kw: value, create=True))
        self.services = self.stack.enter_context(mock.patch.object(runner, "_service_blockers", return_value=[]))
        self.memory = self.stack.enter_context(mock.patch.object(runner, "verify_memory_limit",
            return_value={"memory_max_bytes": 4 * 1024**3, "memory_swap_max_bytes": 0}))
        self.stack.enter_context(mock.patch.object(runner.os, "statvfs", return_value=SimpleNamespace(
            f_bavail=64 * 1024**3, f_frsize=1)))
        self.normalize = self.stack.enter_context(mock.patch.object(runner, "normalize_audio", side_effect=self.fake_normalize))
        self.commands = self.stack.enter_context(mock.patch.object(runner, "_command", side_effect=self.fake_inference))
        self.worker_requests = []

    def ready(self):
        binary = self.root / "diarization-python"
        binary.write_bytes(b"synthetic Python; never executed")
        binary.chmod(0o700)
        self.request["python"] = runner.binding(binary)
        self.request["model_bundle"] = self.fixture.document("diarization-bundle.json", {"synthetic": True})
        self.admit.return_value = {"files": {},
            "runtime_binding": self.request["model_bundle"], "review_evidence": self.request["model_bundle"],
            "license": self.request["model_bundle"],
            "runtime": {"installed_files": [], "packages": [], "python": self.request["python"]}}

    def plan(self):
        ref = self.fixture.document("diarization-request.json", self.request)
        self.plan_value = runner.create_plan(ref["path"], ref["sha256"])
        self.plan_path = Path(self.request["state_root"]) / "plan.json"
        self.plan_sha = runner.binding(self.plan_path)["sha256"]
        return self.plan_value

    def run_plan(self):
        return runner.run_plan(self.plan_path, self.plan_sha)

    def screen_files(self):
        return {str(path): (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns)
            for path in self.fixture.path.parent.rglob("*") if path.is_file()}

    def fake_normalize(self, record, req, work, **kwargs):
        # Inference is mocked, but persisted file bindings and cleanup are real.
        bindings = {}
        for key, name in (("pcm", "normalized.pcm"), ("flac", "normalized.flac")):
            path = work / name
            path.write_bytes(b"synthetic whole-recording normalized " + key.encode())
            path.chmod(0o400)
            bindings[key] = {**runner.binding(path), "byte_count": path.stat().st_size}
        bindings["pcm"]["byte_count"] = record["recording"]["duration_ms"] * 32
        return {**bindings, "duration_ms": record["recording"]["duration_ms"],
            "sample_rate": 16000, "channels": 1, "pcm_format": "s16le",
            "source_witness": record["source_witness"], "source_sha256_reverified": False,
            "timeline": dict(runner.NORMALIZATION_TIMELINE)}

    def fake_inference(self, command, **kwargs):
        path = command[command.index("--request") + 1]
        expected = command[command.index("--expected-sha256") + 1]
        value = runner.screen.read_json(Path(path), expected)
        runner.engine.validate_request(value)
        self.worker_requests.append(value)
        return runner.screen.canonical({"kind": "himr_screened_diarization_engine_output",
            "schema_version": 1, "ordinary": [
                {"start": 0.0, "end": 1.5, "speaker": "private label A"},
                {"start": 1.0, "end": 2.0, "speaker": "private label B"}],
            "exclusive": [{"start": 0.0, "end": 1.25, "speaker": "private label A"},
                          {"start": 1.25, "end": 2.0, "speaker": "private label B"}],
            "provenance": {"synthetic": True}})

    def test_actual_checkpoint_selection_plan_run_status_and_idempotent_resume(self):
        self.ready()
        before = self.screen_files()
        plan = self.plan()
        self.assertEqual(len(plan["jobs"]), 2)
        self.assertTrue(all(job["speaker_bounds"] == {"parameters": {}, "review": None} for job in plan["jobs"]))
        outcome = self.run_plan()
        self.assertEqual(outcome["state"], "completed")
        self.assertEqual(outcome["completed_recordings"], 2)
        self.assertEqual(self.commands.call_count, 2)
        results = {}
        for job in plan["jobs"]:
            path = self.plan_path.parent / job["job_id"] / "result.json"
            results[str(path)] = path.read_bytes()
            value = runner.screen.read_json(path)
            self.assertEqual(value["diarization"]["recording"]["media_sha256"], job["screened_record"]["recording"]["sha256"])
            self.assertEqual(value["diarization"]["summary"]["overlap_ms"], 500)
            self.assertEqual(value["diarization"]["summary"]["score_state"], "unavailable")
            self.assertFalse(value["publication_authority"])
            self.assertFalse(value["production_quality_validated"])
            self.assertFalse(Path(value["normalization"]["pcm"]["path"]).exists())
            self.assertFalse(Path(value["normalization"]["flac"]["path"]).exists())
            self.assertTrue(Path(value["worker_request"]["path"]).exists())
        self.commands.reset_mock()
        self.normalize.reset_mock()
        self.assertEqual(self.run_plan()["state"], "completed")
        self.commands.assert_not_called()
        self.normalize.assert_not_called()
        self.assertEqual(self.screen_files(), before)
        for name, body in results.items():
            self.assertEqual(Path(name).read_bytes(), body)
        self.assertEqual(runner.status_plan(self.plan_path, self.plan_sha)["remaining_recordings"], 0)
        self.assertGreaterEqual(self.provenance_check.call_count, 2)

    def test_completed_source_drift_is_rejected_without_relaunch(self):
        self.ready(); self.plan(); self.run_plan(); self.commands.reset_mock()
        source = Path(self.plan_value["jobs"][0]["screened_record"]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        with self.assertRaisesRegex(runner.Error, "source metadata changed"):
            self.run_plan()
        self.commands.assert_not_called()

    def test_resealed_job_id_bounds_and_resource_tamper_is_rejected(self):
        self.ready(); self.plan()
        variants = []
        for key, changed in (("job_id", "../escape"),
                ("speaker_bounds", {"parameters": {"num_speakers": 2}, "review": None}),
                ("resource_admission", {"blockers": [], "pcm_bytes": 1, "estimated_host_memory_bytes": 1})):
            value = copy.deepcopy(self.plan_value)
            value["jobs"][0][key] = changed
            variants.append(value)
        for value in variants:
            value["plan_id"] = "screeneddiar_" + runner.screen.digest({key: item for key, item in value.items() if key != "plan_id"})[:32]
            self.plan_path.write_bytes(runner.screen.canonical(value))
            with self.subTest(value=value["jobs"][0]), self.assertRaises(runner.Error):
                runner.load_plan(self.plan_path, runner.binding(self.plan_path)["sha256"])

    def test_completed_outer_result_claims_and_engine_receipt_tamper_are_rejected(self):
        self.ready(); self.plan(); self.run_plan()
        job = self.plan_value["jobs"][0]
        path = self.plan_path.parent / job["job_id"] / "result.json"
        original = runner.screen.read_json(path)
        for key, changed in (("schema_version", True), ("publication_authority", True),
                             ("production_quality_validated", True)):
            value = {**original, key: changed}
            path.write_bytes(runner.screen.canonical(value))
            with self.subTest(key=key), self.assertRaises(runner.Error):
                runner.status_plan(self.plan_path, self.plan_sha)
        path.write_bytes(runner.screen.canonical(original))
        raw = Path(original["engine_output"]["path"])
        raw.write_bytes(b"changed output")
        with self.assertRaisesRegex(runner.Error, "SHA-256"):
            runner.status_plan(self.plan_path, self.plan_sha)

    def test_model_provenance_validator_can_block_publication(self):
        self.ready(); self.plan()
        self.provenance_check.side_effect = runner.Error("wrong model provenance")
        with self.assertRaisesRegex(runner.Error, "wrong model provenance"):
            self.run_plan()
        self.assertEqual(runner.status_plan(self.plan_path, self.plan_sha)["completed_recordings"], 0)

    def test_completed_normalization_metadata_and_retention_tamper_are_rejected(self):
        self.ready(); self.plan(); self.run_plan()
        path = self.plan_path.parent / self.plan_value["jobs"][0]["job_id"] / "result.json"
        original = runner.screen.read_json(path)
        variants = []
        for key, value in (("duration_ms", 1), ("source_witness", {}), ("sample_rate", 48000)):
            changed = copy.deepcopy(original)
            changed["normalization"][key] = value
            variants.append(changed)
        changed = copy.deepcopy(original)
        changed["normalization"]["timeline"]["source_modified"] = True
        variants.append(changed)
        variants.append({**original, "normalized_audio_retained": True})
        for value in variants:
            path.write_bytes(runner.screen.canonical(value))
            with self.subTest(value=value["normalization"]), self.assertRaises(runner.Error):
                runner.status_plan(self.plan_path, self.plan_sha)

    def test_missing_model_and_python_produce_explicit_setup_blockers_without_launch(self):
        self.plan()
        outcome = self.run_plan()
        self.assertEqual(outcome["state"], "blocked_setup")
        self.assertEqual(set(outcome["blockers"]), {
            "approved_community_1_model_bundle_not_configured", "isolated_diarization_python_not_configured"})
        self.commands.assert_not_called()
        self.normalize.assert_not_called()
        self.admit.assert_not_called()
        self.memory.assert_not_called()

    def test_running_screen_service_blocks_before_decode_inference_or_memory(self):
        self.ready(); self.plan(); self.admit.reset_mock()
        self.services.return_value = ["archive-screen-test.service:active"]
        self.assertEqual(self.run_plan()["state"], "blocked_existing_work")
        self.memory.assert_not_called(); self.normalize.assert_not_called(); self.commands.assert_not_called()
        # Read-only artifact admission during plan replay is allowed; no model
        # import, decoding, memory reservation or inference has begun.

    def test_service_start_between_jobs_stops_and_preserves_completed_result(self):
        self.ready(); self.plan()
        self.services.side_effect = [[], [], ["archive-screen-test.service:active"]]
        with self.assertRaisesRegex(runner.Error, "blocking work became active"):
            self.run_plan()
        self.assertEqual(self.commands.call_count, 1)
        self.assertEqual(runner.status_plan(self.plan_path, self.plan_sha)["completed_recordings"], 1)

    def test_source_drift_before_decode_is_not_rediarized(self):
        self.ready(); self.plan()
        source = Path(self.plan_value["jobs"][0]["screened_record"]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        with self.assertRaisesRegex(runner.Error, "source metadata changed"):
            self.run_plan()
        self.normalize.assert_not_called(); self.commands.assert_not_called()
        self.assertEqual(runner.screen.read_json(self.plan_path.parent / "status.json")["state"], "planned")

    def test_source_drift_during_inference_prevents_result_publication(self):
        self.ready(); self.plan()
        def changed(command, **kwargs):
            answer = self.fake_inference(command, **kwargs)
            source = Path(self.plan_value["jobs"][0]["screened_record"]["recording"]["path"])
            source.write_bytes(b"x" * source.stat().st_size)
            return answer
        self.commands.side_effect = changed
        with self.assertRaisesRegex(runner.Error, "source metadata changed"):
            self.run_plan()
        self.assertFalse(list(self.plan_path.parent.glob("diarjob_*/result.json")))
        self.assertFalse(list(self.plan_path.parent.rglob("normalized.pcm")))
        self.assertTrue(list(self.plan_path.parent.rglob("worker-request.json")))

    def test_finite_job_cap_resumes_without_repeating_completed_work(self):
        self.ready(); self.request["limits"]["max_jobs_per_run"] = 1; self.plan()
        self.assertEqual(self.run_plan()["completed_recordings"], 1)
        self.assertEqual(self.run_plan()["completed_recordings"], 2)
        self.assertEqual(len(self.worker_requests), 2)
        self.assertNotEqual(self.worker_requests[0]["media_sha256"], self.worker_requests[1]["media_sha256"])

    def test_recording_cap_records_deferred_selection_without_expanding_plan(self):
        self.ready(); self.request["limits"]["max_recordings"] = 1
        plan = self.plan()
        self.assertEqual(plan["deferred_selected_count"], 1)
        self.assertEqual(len(plan["selection"]["records"]), 2)
        self.assertEqual(self.run_plan()["planned_recordings"], 1)

    def test_resource_deficit_fails_without_chunking_or_decode(self):
        self.ready(); self.request["limits"]["max_duration_ms"] = 1000
        plan = self.plan()
        self.assertIn("recording_duration_limit", plan["jobs"][0]["resource_admission"]["blockers"])
        with self.assertRaisesRegex(runner.Error, "larger reviewed resource envelope"):
            self.run_plan()
        self.normalize.assert_not_called(); self.commands.assert_not_called()

    def test_explicit_reviewed_bounds_and_evidence_bind_the_worker(self):
        self.ready()
        evidence = self.fixture.document("human-review.json", {"reviewer": "Test reviewer", "direct_media": True})
        sha = self.fixture.orders[0]["recording"]["sha256"]
        self.request["speaker_bounds"] = [{"media_sha256": sha, "bounds": {
            "parameters": {"min_speakers": 1, "max_speakers": 2},
            "review": {"basis": "direct_media_human_review", "media_sha256": sha,
                "reviewer": "Test reviewer", "reviewed_at": "2026-09-12T12:00:00Z", "evidence": evidence}}}]
        self.plan(); self.run_plan()
        self.assertEqual(self.worker_requests[0]["speaker_parameters"], {"min_speakers": 1, "max_speakers": 2})
        self.assertEqual(self.worker_requests[1]["speaker_parameters"], {})

    def test_reviewed_evidence_drift_is_not_accepted(self):
        self.ready()
        evidence = self.fixture.document("human-review.json", {"direct_media": True})
        sha = self.fixture.orders[0]["recording"]["sha256"]
        self.request["speaker_bounds"] = [{"media_sha256": sha, "bounds": {
            "parameters": {"num_speakers": 2}, "review": {"basis": "direct_media_human_review",
                "media_sha256": sha, "reviewer": "Test reviewer", "reviewed_at": "2026-09-12T12:00:00Z", "evidence": evidence}}}]
        self.plan()
        Path(evidence["path"]).write_bytes(b"changed review")
        with self.assertRaisesRegex(runner.Error, "SHA-256 differs"):
            self.run_plan()
        self.normalize.assert_not_called()

    def test_invalid_turns_and_nonfinite_output_never_publish(self):
        self.ready(); self.plan()
        self.commands.side_effect = None
        self.commands.return_value = b'{"kind":"himr_screened_diarization_engine_output","schema_version":1,"ordinary":[{"start":NaN,"end":1,"speaker":"a"}],"exclusive":[],"provenance":{}}'
        with self.assertRaisesRegex(runner.Error, "nonfinite"):
            self.run_plan()
        self.assertEqual(runner.status_plan(self.plan_path, self.plan_sha)["completed_recordings"], 0)

    def test_interrupt_preserves_receipts_but_removes_unretained_generated_audio(self):
        self.ready(); self.plan(); self.commands.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.run_plan()
        self.assertEqual(runner.screen.read_json(self.plan_path.parent / "status.json")["state"], "interrupted")
        self.assertFalse(list(self.plan_path.parent.rglob("normalized.pcm")))
        self.assertFalse(list(self.plan_path.parent.rglob("normalized.flac")))
        self.assertTrue(list(self.plan_path.parent.rglob("worker-request.json")))
        self.assertEqual(runner.status_plan(self.plan_path, self.plan_sha)["completed_recordings"], 0)

    def test_explicit_retention_preserves_failed_attempt_audio(self):
        self.ready(); self.request["retain_normalized_audio"] = True; self.plan()
        self.commands.side_effect = runner.Error("synthetic failure")
        with self.assertRaisesRegex(runner.Error, "synthetic failure"):
            self.run_plan()
        self.assertEqual(len(list(self.plan_path.parent.rglob("normalized.pcm"))), 1)
        self.assertEqual(len(list(self.plan_path.parent.rglob("normalized.flac"))), 1)
        self.assertFalse(list(self.plan_path.parent.glob("diarjob_*/result.json")))

    def test_retained_normalized_audio_keeps_only_owned_outputs_and_source(self):
        self.ready(); self.request["retain_normalized_audio"] = True; self.plan(); self.run_plan()
        self.assertEqual(len(list(self.plan_path.parent.rglob("normalized.pcm"))), 2)
        for order in self.fixture.orders:
            self.assertTrue(Path(order["recording"]["path"]).exists())

    def test_request_validation_rejects_unknown_fields_unreviewed_bounds_and_overlap(self):
        cases = [{**self.request, "watch": True}, {**self.request, "purpose": "production"},
            {**self.request, "include_uncertain": 1}, {**self.request, "media_ids": ["bad/path"]},
            {**self.request, "state_root": str(self.root)},
            {**self.request, "limits": {**self.request["limits"], "max_pcm_bytes": 86400000 * 32 + 1}},
            {**self.request, "execution": {**self.request["execution"], "cuda_memory_fraction": float("nan")}}]
        for value in cases:
            with self.subTest(value=value), self.assertRaises((runner.Error, runner.core.DiarizationError)):
                runner.validate_request(value)


class NativeNormalizationTests(unittest.TestCase):
    def setUp(self):
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            self.skipTest("FFmpeg unavailable")
        self.ffmpeg = str(Path(ffmpeg).resolve())
        self.temp = tempfile.TemporaryDirectory(prefix="screened-diarization-normalize-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.wav = self.root / "source.wav"
        with wave.open(str(self.wav), "wb") as audio:
            audio.setnchannels(1); audio.setsampwidth(2); audio.setframerate(16000)
            audio.writeframes(b"\x01\x00" * 32000)
        self.wav.chmod(0o600)
        self.req = request(self.root, {"path": str(self.root / "synthetic-screen.json"), "sha256": "a" * 64},
                           runner.binding(self.ffmpeg, 256 * 1024**2))
        self.lock = os.open(self.root / "test.lock", os.O_RDWR | os.O_CREAT, 0o600)
        self.addCleanup(os.close, self.lock)

    def record(self, source, duration=2000):
        with runner.screen.opened(source) as descriptor:
            witness = runner.screen.witness(descriptor)
        return {"recording": {**runner.binding(source), "byte_count": source.stat().st_size,
            "media_id": "synthetic-audio", "duration_ms": duration}, "source_witness": witness}

    def normalize(self, source, duration=2000):
        work = self.root / "work"
        work.mkdir(mode=0o700)
        return runner.normalize_audio(self.record(source, duration), self.req, work,
                                      lock_fd=self.lock, deadline=time.monotonic() + 20)

    def test_whole_wav_exact_timeline_and_retained_source_witness(self):
        before = (self.wav.read_bytes(), self.wav.stat().st_mtime_ns)
        value = self.normalize(self.wav)
        self.assertEqual(value["pcm"]["byte_count"], 64000)
        self.assertEqual(Path(value["pcm"]["path"]).read_bytes(), b"\x01\x00" * 32000)
        self.assertEqual((self.wav.read_bytes(), self.wav.stat().st_mtime_ns), before)
        self.assertEqual(value["timeline"]["source_origin_ms"], 0)
        self.assertFalse(value["timeline"]["tail_padding"])
        self.assertFalse(value["source_sha256_reverified"])

    def test_mp4_opus_start_priming_gets_explicit_origin_without_dropping_first_100ms(self):
        source = self.root / "source-opus.mp4"
        completed = subprocess.run([self.ffmpeg, "-v", "error", "-nostdin", "-threads", "1",
            "-i", str(self.wav), "-c:a", "libopus", "-b:a", "32k", str(source)],
            stdin=subprocess.DEVNULL, capture_output=True, timeout=10, check=False)
        if completed.returncode and b"Unknown encoder" in completed.stderr:
            self.skipTest("FFmpeg Opus encoder unavailable")
        self.assertEqual(completed.returncode, 0, completed.stderr.decode(errors="replace"))
        source.chmod(0o600)
        value = self.normalize(source)
        self.assertEqual(value["pcm"]["byte_count"], 64000)
        self.assertEqual(Path(value["pcm"]["path"]).stat().st_size, 64000)
        self.assertEqual(value["duration_ms"], 2000)
        self.assertTrue(value["timeline"]["initial_encoder_priming_padding_or_trim"])

    def test_internal_one_ms_pts_gap_preserves_full_eof_without_soft_stretch_or_tail_padding(self):
        source = self.root / "source-with-timestamp-gap.mka"
        # Ten-millisecond packets make the discontinuity exactly 1 ms after
        # 1 s of PCM; media EOF is 2001 ms although samples total only 2000 ms.
        completed = subprocess.run([self.ffmpeg, "-v", "error", "-nostdin", "-threads", "1",
            "-i", str(self.wav), "-af", "asetnsamples=n=160:p=0,asetpts=PTS+gte(T\\,1)*0.001/TB",
            "-c:a", "pcm_s16le", "-f", "matroska", str(source)],
            stdin=subprocess.DEVNULL, capture_output=True, timeout=10, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr.decode(errors="replace"))
        source.chmod(0o600)
        before = (source.read_bytes(), source.stat().st_mtime_ns)
        value = self.normalize(source, duration=2001)
        pcm = Path(value["pcm"]["path"]).read_bytes()
        self.assertEqual(len(pcm), 2001 * 32)
        self.assertEqual(pcm, b"\x01\x00" * 16000 + b"\x00\x00" * 16 + b"\x01\x00" * 16000)
        self.assertEqual(value["duration_ms"], 2001)
        self.assertEqual((source.read_bytes(), source.stat().st_mtime_ns), before)
        timeline = value["timeline"]
        self.assertEqual(timeline["source_origin_ms"], 0)
        self.assertEqual(timeline["resample_async"], 1)
        self.assertTrue(timeline["timestamp_gap_silence_insertion_or_overlap_trim"])
        self.assertEqual(timeline["hard_compensation_threshold_output_samples"], 1)
        self.assertFalse(timeline["soft_time_stretching"])
        self.assertFalse(timeline["tail_padding"])
        self.assertIn("max_soft_comp=0", timeline["ffmpeg_audio_filter"])
        # Increasing the claimed EOF must not make the normalizer synthesize
        # trailing silence after the final timestamped sample.
        work = self.root / "past-eof"
        work.mkdir(mode=0o700)
        with self.assertRaisesRegex(runner.Error, "full screened timeline"):
            runner.normalize_audio(self.record(source, 2002), self.req, work,
                lock_fd=self.lock, deadline=time.monotonic() + 20)

    def test_short_decode_is_rejected_without_tail_padding(self):
        with self.assertRaisesRegex(runner.Error, "full screened timeline"):
            self.normalize(self.wav, duration=2100)

    def test_source_witness_and_decoder_digest_drift_fail_before_decode(self):
        record = self.record(self.wav)
        self.wav.write_bytes(self.wav.read_bytes())
        work = self.root / "work"; work.mkdir(mode=0o700)
        with self.assertRaisesRegex(runner.Error, "source changed after screening"):
            runner.normalize_audio(record, self.req, work, lock_fd=self.lock, deadline=time.monotonic() + 20)
        self.req["ffmpeg"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(runner.Error, "decoder changed"):
            runner.normalize_audio(self.record(self.wav), self.req, work, lock_fd=self.lock, deadline=time.monotonic() + 20)
        self.assertFalse((work / "normalized.flac").exists())


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="screened-diarization-command-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lock = os.open(self.root / "test.lock", os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, self.lock)

    def command(self, code, **kwargs):
        return runner._command([sys.executable, "-c", code], lock_fd=self.lock,
            timeout=kwargs.pop("timeout", 5), maximum=kwargs.pop("maximum", 1024**2),
            environment={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, **kwargs)

    def test_bounded_child_output_is_captured_and_eio_is_not_downgraded(self):
        self.assertEqual(self.command("print('synthetic')"), b"synthetic\n")
        with self.assertRaises(OSError) as raised:
            self.command("import sys;sys.stderr.write('[Errno 5] Input/output error');sys.exit(2)")
        self.assertEqual(raised.exception.errno, errno.EIO)

    def test_timeout_reaps_spawned_child(self):
        children = []
        original = subprocess.Popen
        def capture(*args, **kwargs):
            child = original(*args, **kwargs)
            children.append(child)
            return child
        with mock.patch.object(runner.subprocess, "Popen", side_effect=capture):
            with self.assertRaises(subprocess.TimeoutExpired):
                self.command("import time;time.sleep(10)", timeout=.05)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)
        with self.assertRaises(ProcessLookupError):
            os.kill(children[0].pid, 0)

    def test_interrupt_at_launch_handoff_reaps_spawned_child(self):
        children = []
        original = subprocess.Popen
        parent = os.getpid()
        def capture(*args, **kwargs):
            child = original(*args, **kwargs)
            # libseccomp discovery can itself spawn a local ldconfig helper in
            # the preexec child; inject only at the parent's ownership handoff.
            if os.getpid() == parent:
                children.append(child)
                os.kill(parent, signal.SIGINT)
            return child
        with runner.old_batch._cancellable(), mock.patch.object(runner.subprocess, "Popen", side_effect=capture):
            with self.assertRaises(KeyboardInterrupt):
                self.command("import time;time.sleep(10)")
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)
        with self.assertRaises(ProcessLookupError):
            os.kill(children[0].pid, 0)

    def test_output_larger_than_bound_fails(self):
        with self.assertRaises(runner.Error):
            self.command("import os;os.write(1,b'x'*4096)", maximum=128)

    def test_duplicate_worker_json_field_is_rejected(self):
        with self.assertRaisesRegex(runner.Error, "duplicate"):
            runner._parse_worker_output(b'{"schema_version":1,"schema_version":2}')


if __name__ == "__main__":
    unittest.main()
