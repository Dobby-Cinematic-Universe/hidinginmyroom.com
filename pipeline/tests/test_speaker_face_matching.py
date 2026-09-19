"""Private matching runner integration; synthetic media and mocked ML only."""
from contextlib import ExitStack
import copy
import errno
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from pipeline import screened_diarization_core as diar_core
from pipeline import speaker_face_matching as runner
from pipeline.tests import test_screened_diarization as upstream_fixtures

REAL_CHILD = runner._child


class MatchingRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.upstream_root = self.root / "diarization"
        self.upstream_root.mkdir(mode=0o700)
        ffmpeg = self.root / "ffmpeg"
        ffmpeg.write_bytes(b"synthetic decoder; never executed")
        ffmpeg.chmod(0o700)
        self.request = {"kind": "himr_speaker_face_matching_request", "schema_version": 1,
            "purpose": "private_unvalidated_pilot", "diarization_plan": self.document(self.upstream_root / "plan.json", {}),
            "job_ids": None, "state_root": str(self.root / "matching"), "ffmpeg": runner.diar.binding(ffmpeg),
            "python": None, "model_bundle": None, "policy": runner.core.validate_policy(),
            "execution": dict(runner.DEFAULT_EXECUTION), "limits": dict(runner.DEFAULT_LIMITS),
            "blocking_units": ["archive-screen-test.service"], "retain_clip_media": False}
        self.upstream = {"request_value": {"state_root": str(self.upstream_root)}, "jobs": []}
        self.completed = set()
        self.source_paths = {}
        for index in range(2):
            job_id = "diarjob_" + str(index + 1) * 32
            self.upstream["jobs"].append({"job_id": job_id})
            source_path = self.root / ("source-" + str(index) + ".bin")
            source_path.write_bytes(b"synthetic source " + str(index).encode())
            source_path.chmod(0o400)
            with runner.safe.opened(source_path) as fd:
                witness = runner.safe.witness(fd)
            media_sha = hashlib.sha256(source_path.read_bytes()).hexdigest()
            self.upstream["jobs"][-1]["screened_record"] = {"recording": {"path": str(source_path)}}
            diarized = diar_core.normalize_output(30_000,
                [{"start": 0, "end": 30, "speaker": "native label"}],
                [{"start": 0, "end": 30, "speaker": "native label"}], media_sha256=media_sha, run_id=job_id)
            folder = self.upstream_root / job_id
            folder.mkdir(mode=0o700)
            self.source_paths[job_id] = folder / "result.json"
            self.document(folder / "result.json", {"diarization": diarized,
                "screened_record": {"recording": {"path": str(source_path), "sha256": media_sha,
                    "duration_ms": 30_000}, "source_witness": witness}})
            self.completed.add(job_id)
        self.upstream_patch = mock.patch.object(runner, "_upstream", return_value=self.upstream)
        self.upstream_mock = self.upstream_patch.start()
        self.addCleanup(self.upstream_patch.stop)
        self.source_patch = mock.patch.object(runner, "_read_source", side_effect=self.fake_source)
        self.source_mock = self.source_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.verify_patch = mock.patch.object(runner.diar.selection, "verify_selected_record", side_effect=self.verify_source)
        self.verify_mock = self.verify_patch.start()
        self.addCleanup(self.verify_patch.stop)
        self.impl = {"speaker_face_matching_engine.py": "a" * 64}
        self.stack.enter_context(mock.patch.object(runner, "implementation", return_value=self.impl))
        self.admit = self.stack.enter_context(mock.patch.object(runner.engine, "admit_bundle"))
        self.provenance = self.stack.enter_context(mock.patch.object(runner.engine, "validate_provenance",
            side_effect=lambda value, *args, **kw: value))
        self.services = self.stack.enter_context(mock.patch.object(runner.diar, "_service_blockers", return_value=[]))
        self.memory = self.stack.enter_context(mock.patch.object(runner.diar, "verify_memory_limit"))
        self.statvfs = self.stack.enter_context(mock.patch.object(runner.os, "statvfs",
            return_value=SimpleNamespace(f_bavail=64 * 1024**3, f_frsize=1)))
        self.decode = self.stack.enter_context(mock.patch.object(runner, "decode_clip", side_effect=self.fake_decode))
        self.child = self.stack.enter_context(mock.patch.object(runner, "_child", side_effect=self.fake_inference))
        self.worker_requests = []

    def document(self, path, value):
        runner.safe.write_immutable(Path(path), value)
        return runner.diar.binding(path)

    def fake_source(self, upstream, job):
        path = self.source_paths[job["job_id"]]
        return (runner._json(runner.diar.binding(path)) if job["job_id"] in self.completed else None), path

    def verify_source(self, record):
        with runner.safe.opened(record["recording"]["path"]) as fd:
            if runner.safe.witness(fd) != record["source_witness"]:
                raise runner.Error("source witness changed")
        return record

    def ready(self):
        python = self.root / "matching-python"
        python.write_bytes(b"synthetic isolated Python; never executed")
        python.chmod(0o700)
        self.request["python"] = runner.diar.binding(python)
        self.request["model_bundle"] = self.document(self.root / "model-bundle.json", {"synthetic": True})
        self.admit.return_value = {"runtime": {"python": self.request["python"], "installed_files": [], "packages": []}}

    def plan(self):
        ref = self.document(self.root / "matching-request.json", self.request)
        self.plan_value = runner.create_plan(ref["path"], ref["sha256"])
        self.plan_path = Path(self.request["state_root"]) / "plan.json"
        self.plan_sha = runner.diar.binding(self.plan_path)["sha256"]
        return self.plan_value

    def run_plan(self):
        return runner.run_plan(self.plan_path, self.plan_sha)

    def status(self):
        return runner.status_plan(self.plan_path, self.plan_sha)

    def result_path(self, index=0):
        return self.plan_path.parent / self.plan_value["jobs"][index]["job_id"] / "result.json"

    def rewrite(self, path, value):
        path.chmod(0o600)
        path.write_bytes(runner.safe.canonical(value))
        path.chmod(0o400)

    def fake_decode(self, record, clip, request, work, **kwargs):
        # Fake transport files are intentionally tiny: native bytes and decoding
        # have a separate real-FFmpeg test suite. All receipts/bindings are real.
        count = (clip["end_ms"] - clip["start_ms"]) // 40
        bindings = {}
        for key, name, size in (("video_rgb", "video.rgb", count * 640 * 360 * 3),
                                ("audio_pcm", "audio.pcm", count * 1280)):
            path = work / name
            path.write_bytes(b"synthetic " + key.encode())
            path.chmod(0o400)
            bindings[key] = {**runner.diar.binding(path), "byte_count": size}
        period = Fraction(1, 25)
        start = Fraction(clip["start_ms"], 1000)
        rows = [(start + index * period, period) for index in range(count)]
        receipt = runner.visual._timeline_receipt(rows, rows, [(clip["start_ms"] * 16, count * 640)],
            start_ms=clip["start_ms"], end_ms=clip["end_ms"])
        receipt.update(video_sha256=bindings["video_rgb"]["sha256"], audio_sha256=bindings["audio_pcm"]["sha256"],
                       video_stderr_sha256="b" * 64, audio_stderr_sha256="c" * 64)
        runner.visual.validate_receipt(receipt, start_ms=clip["start_ms"], end_ms=clip["end_ms"])
        bindings["decode_receipt"] = self.document(work / "decode-receipt.json", receipt)
        return bindings

    def fake_inference(self, command, **kwargs):
        ref = {"path": command[command.index("--request") + 1],
               "sha256": command[command.index("--expected-sha256") + 1]}
        worker = runner._json(ref)
        runner.engine.validate_request(worker)
        self.worker_requests.append(worker)
        sample = worker["clip"]
        observations = {"av_sync": {"state": "verified", "offset_ms": 0}, "frames": [
            {"time_ms": when, "shot_id": "shot_" + "d" * 32, "faces": [
                {"track_id": "face_track_" + "e" * 32, "raw_logit": 2.0,
                 "face_width_px": 128, "face_height_px": 128, "visible": True, "occluded": False}]}
            for when in range(sample["start_ms"], sample["end_ms"], 40)]}
        return runner.safe.canonical({"kind": "himr_speaker_face_matching_engine_output", "schema_version": 1,
            "clip_id": sample["clip_id"], "observations": observations, "provenance": {"synthetic": True}}), b""

    def test_finite_plan_completed_only_and_late_completion_does_not_expand(self):
        pending_id = self.upstream["jobs"][1]["job_id"]
        self.completed.remove(pending_id)
        plan = self.plan()
        self.assertEqual(len(plan["records"]), 1)
        self.assertEqual(len(plan["jobs"]), 3)
        self.assertEqual(plan["pending_diarization_jobs_at_snapshot"], 1)
        self.completed.add(pending_id)
        self.source_mock.reset_mock()
        status = self.status()
        self.assertEqual(status["planned_clips"], 3)
        self.assertEqual(status["pending_diarization_jobs_at_snapshot"], 1)
        self.assertEqual({call.args[1]["job_id"] for call in self.source_mock.call_args_list},
                         {self.upstream["jobs"][0]["job_id"]})

    def test_empty_snapshot_is_explicit_and_launches_nothing(self):
        self.completed.clear()
        self.plan()
        self.assertEqual(self.run_plan()["state"], "empty_selection")
        self.decode.assert_not_called()
        self.child.assert_not_called()
        self.memory.assert_not_called()

    def test_setup_blockers_precede_memory_decode_and_inference(self):
        self.plan()
        result = self.run_plan()
        self.assertEqual(result["state"], "blocked_setup")
        self.assertEqual(set(result["blockers"]), {"approved_lrasd_yunet_bundle_not_configured",
                                                   "isolated_matching_python_not_configured"})
        self.decode.assert_not_called()
        self.child.assert_not_called()
        self.memory.assert_not_called()

    def test_running_screen_service_blocks_without_starting_matching(self):
        self.ready()
        self.plan()
        self.services.return_value = ["archive-screen-test.service:active"]
        self.assertEqual(self.run_plan()["state"], "blocked_existing_work")
        self.decode.assert_not_called()
        self.child.assert_not_called()
        self.memory.assert_not_called()

    def test_synthetic_decode_worker_association_status_resume_and_cleanup(self):
        self.ready()
        before = {str(path): path.read_bytes() for path in self.upstream_root.rglob("*") if path.is_file()}
        plan = self.plan()
        result = self.run_plan()
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["candidate_matches"], 6)
        self.assertEqual(self.child.call_count, 6)
        saved = {str(self.result_path(index)): self.result_path(index).read_bytes() for index in range(6)}
        for index, job in enumerate(plan["jobs"]):
            answer = runner._json(runner.diar.binding(self.result_path(index)))
            self.assertEqual(answer["association"]["scope"]["run_id"], job["source_job_id"])
            self.assertEqual(answer["association"]["clip"], job["clip"])
            self.assertFalse(answer["semantics"]["publication_authority"])
            self.assertTrue(Path(answer["worker_request"]["path"]).exists())
            self.assertTrue(Path(answer["engine_output"]["path"]).exists())
        self.assertFalse(list(self.plan_path.parent.rglob("audio.pcm")))
        self.assertFalse(list(self.plan_path.parent.rglob("video.rgb")))
        self.child.reset_mock()
        self.decode.reset_mock()
        self.assertEqual(self.run_plan()["remaining_clips"], 0)
        self.child.assert_not_called()
        self.decode.assert_not_called()
        self.assertEqual(before, {str(path): path.read_bytes() for path in self.upstream_root.rglob("*") if path.is_file()})
        self.assertEqual(saved, {path: Path(path).read_bytes() for path in saved})

    def test_job_cap_and_total_clip_cap_are_finite_and_resumable(self):
        self.ready()
        self.request["limits"].update(max_clips=2, max_jobs_per_run=1)
        plan = self.plan()
        self.assertEqual(plan["deferred_clips"], 4)
        self.assertEqual(self.run_plan()["remaining_clips"], 1)
        self.assertEqual(self.run_plan()["remaining_clips"], 0)
        self.assertEqual(self.child.call_count, 2)

    def test_decode_review_failure_continues_remaining_clips(self):
        self.ready()
        self.plan()
        calls = 0
        def decode(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                self.fake_decode(*args, **kwargs)
                raise runner.visual.VisualError("source video timeline contains a gap")
            return self.fake_decode(*args, **kwargs)
        self.decode.side_effect = decode
        result = self.run_plan()
        self.assertEqual(result["state"], "completed_with_reviews")
        self.assertEqual(result["needs_review_clips"], 1)
        self.assertEqual(result["candidate_matches"], 5)
        self.assertEqual(self.child.call_count, 5)
        self.assertFalse(list(self.plan_path.parent.rglob("video.rgb")))
        self.assertIn("timeline", runner._json(runner.diar.binding(self.result_path()))["review_reason"])

    def test_decoder_timeout_is_needs_review_not_silently_completed(self):
        self.ready()
        self.request["limits"]["max_clips"] = 1
        self.plan()
        self.decode.side_effect = subprocess.TimeoutExpired(["synthetic-decoder"], 1)
        result = self.run_plan()
        self.assertEqual(result["needs_review_clips"], 1)
        self.assertEqual(result["candidate_matches"], 0)
        self.child.assert_not_called()

    def test_eio_stops_campaign_and_preserves_prior_results(self):
        self.ready()
        self.plan()
        count = 0
        def decode(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                self.fake_decode(*args, **kwargs)
                raise OSError(errno.EIO, "synthetic storage failure")
            return self.fake_decode(*args, **kwargs)
        self.decode.side_effect = decode
        with self.assertRaises(OSError) as error:
            self.run_plan()
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertEqual(self.status()["processed_clips"], 1)
        self.assertEqual(self.status()["needs_review_clips"], 0)
        self.assertFalse(list(self.plan_path.parent.rglob("audio.pcm")))
        self.assertEqual(runner.safe.read_json(self.plan_path.parent / "status.json")["state"], "failed")

    def test_cancel_preserves_proofs_cleans_clip_media_and_can_resume(self):
        self.ready()
        self.plan()
        self.child.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.run_plan()
        self.assertEqual(runner.safe.read_json(self.plan_path.parent / "status.json")["state"], "interrupted")
        self.assertTrue(list(self.plan_path.parent.rglob("worker-request.json")))
        self.assertFalse(list(self.plan_path.parent.rglob("audio.pcm")))
        self.assertFalse(list(self.plan_path.parent.rglob("video.rgb")))
        self.assertEqual(self.status()["processed_clips"], 0)
        self.child.side_effect = self.fake_inference
        self.assertEqual(self.run_plan()["state"], "completed")

    def test_explicit_retention_preserves_only_owned_attempt_media(self):
        self.ready()
        self.request["retain_clip_media"] = True
        self.request["limits"]["max_clips"] = 1
        self.plan()
        self.child.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.run_plan()
        self.assertEqual(len(list(self.plan_path.parent.rglob("audio.pcm"))), 1)
        self.assertEqual(len(list(self.plan_path.parent.rglob("video.rgb"))), 1)
        self.assertTrue(all((self.root / ("source-" + str(index) + ".bin")).exists() for index in range(2)))

    def test_service_becoming_active_between_jobs_stops_and_preserves_commit(self):
        self.ready()
        self.plan()
        self.services.side_effect = [[], [], ["archive-screen-test.service:active"]]
        with self.assertRaisesRegex(runner.Error, "blocking work became active"):
            self.run_plan()
        self.assertEqual(self.status()["processed_clips"], 1)
        self.assertEqual(self.child.call_count, 1)

    def test_provenance_failure_prevents_commit_and_cleans_media(self):
        self.ready()
        self.plan()
        self.provenance.side_effect = runner.Error("model provenance changed")
        with self.assertRaisesRegex(runner.Error, "model provenance changed"):
            self.run_plan()
        self.assertEqual(self.status()["processed_clips"], 0)
        self.assertFalse(list(self.plan_path.parent.rglob("video.rgb")))

    def test_source_result_drift_after_plan_is_rejected_before_launch(self):
        self.ready()
        self.plan()
        path = self.source_paths[self.upstream["jobs"][0]["job_id"]]
        original = runner._json(runner.diar.binding(path))
        self.rewrite(path, {**original, "extra": "modified"})
        with self.assertRaisesRegex(runner.Error, "disappeared or changed"):
            self.run_plan()
        self.decode.assert_not_called()
        self.child.assert_not_called()

    def test_source_drift_during_inference_prevents_commit(self):
        self.ready()
        self.plan()
        def child(*args, **kwargs):
            answer = self.fake_inference(*args, **kwargs)
            path = self.root / "source-0.bin"
            path.chmod(0o600)
            path.write_bytes(b"changed source")
            path.chmod(0o400)
            return answer
        self.child.side_effect = child
        with self.assertRaisesRegex(runner.Error, "source witness changed"):
            self.run_plan()
        self.assertFalse(self.result_path().exists())
        self.assertFalse(list(self.plan_path.parent.rglob("audio.pcm")))

    def test_changed_result_semantics_or_association_is_rejected(self):
        self.ready()
        self.request["limits"]["max_clips"] = 1
        self.plan()
        self.run_plan()
        path = self.result_path()
        original = runner._json(runner.diar.binding(path))
        for change in (lambda v: v.update(schema_version=True),
                       lambda v: v["semantics"].update(publication_authority=True),
                       lambda v: v.update(clip_media_retained=True),
                       lambda v: v["association"].update(candidate_track_id="face_track_" + "f" * 32)):
            value = copy.deepcopy(original)
            change(value)
            self.rewrite(path, value)
            with self.assertRaises(runner.Error):
                self.status()

    def test_missing_or_changed_engine_proof_cannot_resume(self):
        self.ready()
        self.request["limits"]["max_clips"] = 1
        self.plan()
        self.run_plan()
        result = runner._json(runner.diar.binding(self.result_path()))
        raw = Path(result["engine_output"]["path"])
        self.rewrite(raw, {"changed": True})
        self.child.reset_mock()
        with self.assertRaisesRegex(runner.Error, "SHA-256"):
            self.run_plan()
        self.child.assert_not_called()

    def test_resealed_decode_receipt_hash_mismatch_is_rejected(self):
        self.ready()
        self.request["limits"]["max_clips"] = 1
        self.plan()
        self.run_plan()
        result = runner._json(runner.diar.binding(self.result_path()))
        worker_path = Path(result["worker_request"]["path"])
        worker = runner._json(result["worker_request"])
        receipt_path = Path(worker["decode_receipt"]["path"])
        receipt = runner._json(worker["decode_receipt"])
        receipt["audio_sha256"] = "f" * 64
        self.rewrite(receipt_path, receipt)
        worker["decode_receipt"] = runner.diar.binding(receipt_path)
        self.rewrite(worker_path, worker)
        result["worker_request"] = runner.diar.binding(worker_path)
        self.rewrite(self.result_path(), result)
        with self.assertRaisesRegex(runner.Error, "decode receipt.*(differs|binding)"):
            self.status()

    def test_model_output_for_wrong_clip_is_rejected_without_commit(self):
        self.ready()
        self.plan()
        def child(*args, **kwargs):
            body, stderr = self.fake_inference(*args, **kwargs)
            value = json.loads(body)
            value["clip_id"] = "avclip_" + "f" * 32
            return runner.safe.canonical(value), stderr
        self.child.side_effect = child
        with self.assertRaisesRegex(runner.Error, "another clip|scope differs"):
            self.run_plan()
        self.assertEqual(self.status()["processed_clips"], 0)

    def test_inactive_faces_count_as_unknown_not_missing_processing(self):
        self.ready()
        self.request["limits"]["max_clips"] = 1
        self.plan()
        def child(*args, **kwargs):
            body, stderr = self.fake_inference(*args, **kwargs)
            value = json.loads(body)
            for frame in value["observations"]["frames"]:
                for face in frame["faces"]:
                    face["raw_logit"] = -2.0
            return runner.safe.canonical(value), stderr
        self.child.side_effect = child
        result = self.run_plan()
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["processed_clips"], 1)
        self.assertEqual(result["unknown_associations"], 1)
        self.assertEqual(result["candidate_matches"], 0)

    def test_engine_missing_frame_is_rejected_not_treated_as_low_confidence(self):
        self.ready()
        self.plan()
        def child(*args, **kwargs):
            body, stderr = self.fake_inference(*args, **kwargs)
            value = json.loads(body)
            del value["observations"]["frames"][20]
            return runner.safe.canonical(value), stderr
        self.child.side_effect = child
        with self.assertRaisesRegex(runner.Error, "omit prepared video frames"):
            self.run_plan()
        self.assertEqual(self.status()["processed_clips"], 0)

    def test_resealed_job_traversal_or_clip_changes_are_rejected(self):
        self.ready()
        self.plan()
        for change in (lambda v: v["jobs"][0].update(job_id="../escape"),
                       lambda v: v["jobs"][0]["clip"].update(start_ms=999),
                       lambda v: v.update(pending_diarization_jobs_at_snapshot=1),
                       lambda v: v.update(readiness_blockers=["invented"]),
                       lambda v: v.update(deferred_clips=True)):
            value = copy.deepcopy(self.plan_value)
            change(value)
            value["plan_id"] = "avmatchplan_" + runner.safe.digest({k: v for k, v in value.items() if k != "plan_id"})[:32]
            self.rewrite(self.plan_path, value)
            with self.assertRaises(runner.Error):
                runner.load_plan(self.plan_path, runner.diar.binding(self.plan_path)["sha256"])

    def test_owned_attempt_proofs_cannot_reference_sibling_directory(self):
        self.ready()
        self.request["limits"]["max_clips"] = 1
        self.plan()
        self.run_plan()
        result = runner._json(runner.diar.binding(self.result_path()))
        foreign = self.root / "foreign-worker.json"
        self.document(foreign, runner._json(result["worker_request"]))
        result["worker_request"] = runner.diar.binding(foreign)
        self.rewrite(self.result_path(), result)
        with self.assertRaisesRegex(runner.Error, "proof paths escape"):
            self.status()

    def test_explicit_job_filter_cannot_admit_unknown_jobs(self):
        self.request["job_ids"] = ["diarjob_" + "f" * 32]
        with self.assertRaisesRegex(runner.Error, "outside the diarization plan"):
            self.plan()

    def test_request_fields_and_overscope_paths_fail_closed(self):
        for changed in ({**self.request, "purpose": "production"}, {**self.request, "watch": True},
                        {**self.request, "retain_clip_media": 1},
                        {**self.request, "job_ids": ["diarjob_" + "1" * 32] * 2}):
            with self.assertRaises(runner.Error):
                runner.validate_request(changed)
        for target in (self.root, self.upstream_root / "child", self.root / "source-0.bin",
                       Path(runner.__file__).parent / "new-matching-output"):
            changed = {**self.request, "state_root": str(target)}
            records, _ = runner._capture(changed, self.upstream)
            with self.subTest(target=target), self.assertRaisesRegex(runner.Error, "overlaps"):
                runner._protect(changed, self.upstream, records)

    def test_insufficient_scratch_stops_before_decode(self):
        self.ready()
        self.plan()
        self.statvfs.return_value = SimpleNamespace(f_bavail=1, f_frsize=1)
        with self.assertRaisesRegex(runner.Error, "insufficient private scratch"):
            self.run_plan()
        self.decode.assert_not_called()
        self.child.assert_not_called()

    def test_actual_screening_and_completed_diarization_upstream_are_replayed(self):
        # Exercise real immutable screening checkpoints and completed diarization
        # receipts, rather than only the lightweight source fixture above.
        self.upstream_patch.stop()
        self.source_patch.stop()
        self.verify_patch.stop()
        fixture = upstream_fixtures.RunnerTests("runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.ready()
        def inference(command, **kwargs):
            return runner.safe.canonical({"kind": "himr_screened_diarization_engine_output", "schema_version": 1,
                "ordinary": [{"start": 0, "end": 8, "speaker": "native-a"},
                             {"start": 8, "end": 16, "speaker": "native-b"}],
                "exclusive": [{"start": 0, "end": 8, "speaker": "native-a"},
                              {"start": 8, "end": 16, "speaker": "native-b"}], "provenance": {"synthetic": True}})
        fixture.commands.side_effect = inference
        fixture.plan()
        self.assertEqual(fixture.run_plan()["state"], "completed")
        self.request["diarization_plan"] = runner.diar.binding(fixture.plan_path)
        self.request["ffmpeg"] = fixture.request["ffmpeg"]
        before = {str(path): path.read_bytes() for path in fixture.plan_path.parent.rglob("*") if path.is_file()}
        self.ready()
        plan = self.plan()
        self.assertEqual(len(plan["records"]), 2)
        self.assertEqual(len(plan["jobs"]), 4)
        result = self.run_plan()
        self.assertEqual(result["candidate_matches"], 4)
        self.assertEqual(before, {str(path): path.read_bytes() for path in fixture.plan_path.parent.rglob("*") if path.is_file()})


class ChildProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.lock = (self.root / "lock").open("wb")
        self.addCleanup(self.lock.close)

    def command(self, program, **kwargs):
        return REAL_CHILD([sys.executable, "-B", "-c", program], lock_fd=self.lock.fileno(),
                          environment={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
                          timeout=10, maximum=64, **kwargs)

    def test_native_child_returns_bounded_stdout_and_stderr(self):
        body, diagnostics = self.command("import sys; print('ok'); print('diagnostic', file=sys.stderr)")
        self.assertEqual(body, b"ok\n")
        self.assertEqual(diagnostics, b"diagnostic\n")

    def test_native_child_storage_diagnostic_is_eio_not_decode_review(self):
        with self.assertRaises(OSError) as error:
            self.command("import sys; print('[Errno 5] Input/output error', file=sys.stderr); sys.exit(2)")
        self.assertEqual(error.exception.errno, errno.EIO)

    def test_native_child_output_overflow_is_rejected(self):
        with self.assertRaisesRegex(runner.ChildError, "stdout exceeded"):
            self.command("print('x' * 17)", stdout_max=16)


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg unavailable; nothing is installed")
class NativeParentDecodeTests(unittest.TestCase):
    def test_real_parent_decode_proofs_source_binding_and_attempt_cleanup(self):
        with tempfile.TemporaryDirectory(prefix="himr-av-parent-decode-test-") as directory:
            root = Path(directory)
            root.chmod(0o700)
            source = root / "synthetic.mkv"
            ffmpeg = str(Path(shutil.which("ffmpeg")).resolve())
            # A 2.4 second synthetic source leaves a genuine 2 second clean clip
            # after the planner's 200 ms guard at each edge. No human media/ML.
            subprocess.run([ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "error", "-n",
                "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=2.4",
                "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=2.4",
                "-c:v", "ffv1", "-threads:v", "1", "-c:a", "pcm_s16le", str(source)],
                check=True, capture_output=True, timeout=30)
            source.chmod(0o400)
            original = source.read_bytes()
            with runner.safe.opened(source) as fd:
                witness = runner.safe.witness(fd)
            media_sha = hashlib.sha256(original).hexdigest()
            turns = [{"start": 0, "end": 2.4, "speaker": "synthetic anonymous source"}]
            diarized = diar_core.normalize_output(2400, turns, turns, media_sha256=media_sha,
                                                  run_id="diarjob_" + "a" * 32)
            sample = runner.core.plan_clips(diarized)["clips"][0]
            self.assertEqual((sample["start_ms"], sample["end_ms"]), (200, 2200))
            record = {"screened_record": {"recording": {"path": str(source), "sha256": media_sha,
                                                        "duration_ms": 2400}, "source_witness": witness}}
            request = {"ffmpeg": runner.diar.binding(Path(ffmpeg), 256 * 1024**2),
                       "execution": dict(runner.DEFAULT_EXECUTION)}
            with runner.diar._locked(root) as lock_fd:
                with runner._attempt(root, retain=False) as work:
                    decoded = runner.decode_clip(record, sample, request, work, lock_fd=lock_fd,
                                                  deadline=time.monotonic() + 30)
                    receipt = runner._json(decoded["decode_receipt"])
                    self.assertEqual(runner.visual.validate_receipt(receipt, start_ms=200, end_ms=2200), receipt)
                    self.assertEqual(receipt["frame_count"], 50)
                    self.assertTrue(receipt["av_sync_verified"])
                    self.assertFalse(receipt["source_lip_sync_verified"])
                    self.assertFalse(receipt["audio_padding"])
                    for key, hash_key, bytes_key in (("video_rgb", "video_sha256", "video_bytes"),
                                                     ("audio_pcm", "audio_sha256", "audio_bytes")):
                        prepared = Path(decoded[key]["path"])
                        self.assertEqual(prepared.stat().st_size, decoded[key]["byte_count"])
                        self.assertEqual(decoded[key]["byte_count"], receipt[bytes_key])
                        self.assertEqual(hashlib.sha256(prepared.read_bytes()).hexdigest(), decoded[key]["sha256"])
                        self.assertEqual(decoded[key]["sha256"], receipt[hash_key])
                        self.assertEqual(prepared.stat().st_mode & 0o777, 0o400)
                    diagnostics_path = work / "decode-diagnostics.json"
                    diagnostics = runner._json(runner.diar.binding(diagnostics_path))
                    self.assertEqual(set(diagnostics), {"video", "audio"})
                    for key in diagnostics:
                        self.assertEqual(hashlib.sha256(diagnostics[key].encode()).hexdigest(),
                                         receipt[key + "_stderr_sha256"])
                    proof_bytes = {name: (work / name).read_bytes()
                                   for name in ("decode-receipt.json", "decode-diagnostics.json")}
                self.assertFalse((work / "video.rgb").exists())
                self.assertFalse((work / "audio.pcm").exists())
                self.assertEqual(proof_bytes, {name: (work / name).read_bytes() for name in proof_bytes})
                self.assertEqual(source.read_bytes(), original)
                with runner.safe.opened(source) as fd:
                    self.assertEqual(runner.safe.witness(fd), witness)


if __name__ == "__main__":
    unittest.main()
