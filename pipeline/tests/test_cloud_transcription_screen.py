"""Model-free proof replay, isolation, and conservative provider-routing tests."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import time
import unittest
from unittest import mock

from pipeline import cloud_transcription_archive as archive
from pipeline import cloud_transcription_screen as screen
from pipeline.tests import test_speaker_screen_multimodal as fixtures

mm, core, engine = screen.mm, screen.core, screen.mm.engine


class ClassificationTests(unittest.TestCase):
    def result(self):
        result = {"audio": {"state": "no_supported_diversity_in_samples", "anchor_search_capped": False},
            "visual": {"state": "no_multiple_faces_observed", "frames_needing_review": 0,
                       "multiple_face_samples": 0}, "audio_availability": "available",
            "audio_refresh_requested": True, "fresh_windows_needing_review": 0,
            "fresh_windows_planned": 4, "fresh_windows_analyzed": 4}
        receipts = [{"state": "analyzed", "excerpts": [1],
                     "window": {"index": i, "reason": "uniform_audio_baseline"}} for i in range(4)]
        return result, receipts

    def test_complete_explicit_sample_negative(self):
        result, receipts = self.result()
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_negative")

    def test_absence_of_faces_is_not_audio_negative(self):
        result, receipts = self.result()
        result["audio"]["state"] = "insufficient_audio"
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_uncertain")

    def test_faces_alone_are_uncertain_not_a_speaker_count(self):
        result, receipts = self.result()
        result["visual"].update(state="repeated_multiple_faces", multiple_face_samples=2)
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_uncertain")

    def test_supported_acoustic_diversity_is_positive_even_without_video(self):
        result, receipts = self.result()
        result["audio"]["state"] = "supported_audio_diversity"
        result["visual"]["state"] = "no_video"
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_positive")

    def test_single_visual_cue_blocks_negative(self):
        result, receipts = self.result()
        result["visual"].update(state="multiple_faces_in_one_sample", multiple_face_samples=1)
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_uncertain")

    def test_each_incomplete_or_weak_condition_requires_diarization(self):
        for key, value in (("audio_availability", "unsupported_timing"),
                           ("audio_refresh_requested", False), ("fresh_windows_needing_review", 1),
                           ("fresh_windows_analyzed", 3)):
            with self.subTest(key=key):
                result, receipts = self.result()
                result[key] = value
                self.assertEqual(screen.classify_result(result, receipts)[0], "screen_uncertain")
        result, receipts = self.result()
        result["audio"]["anchor_search_capped"] = True
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_uncertain")

    def test_four_excerpts_from_one_probe_do_not_establish_negative(self):
        result, receipts = self.result()
        for receipt in receipts:
            receipt["window"]["index"] = 0
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_uncertain")

    def test_missing_video_does_not_disable_valid_independent_audio(self):
        result, receipts = self.result()
        result["visual"]["state"] = "no_video"
        self.assertEqual(screen.classify_result(result, receipts)[0], "screen_negative")


@unittest.skipUnless(Path("/usr/bin/ffmpeg").exists(), "native tool binding unavailable")
class ScreenAdapterTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.RecoveryTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        h = self.helper
        self.base = h.base
        inventory = mm.read(h.inventory)
        inventory["records"][0]["recording"]["duration_hint_ms"] = 60000
        h.record["duration_hint_ms"] = 60000
        h.inventory = h.save("inventory-60s.json", inventory)
        preparation, self.manifest = h.prepare(audio_refresh="all", max_seconds=300)
        self.manifest_ref = preparation["manifest"]
        job = mm.load_job(self.manifest["jobs"][0])
        self.recording = {"recording_id": h.record["media_id"],
            "media": {key: h.record[key] for key in ("path", "sha256", "byte_count")},
            "duration_ms": 60000, "title": "a title is not evidence",
            "aliases": [{"acquisition_result": job["acquisition"]}]}
        self.config = screen.prepare_config(self.manifest_ref)
        self.config_ref = archive.write_inventory(self.base / "screen-config.json", self.config)
        self.folder = self.base / "cloud-job"
        self.folder.mkdir(mode=0o700)

    def complete(self, manifest=None, *, positive=False, visual_faces=False, no_audio=False):
        manifest = manifest or self.manifest
        row = manifest["jobs"][0]
        folder = Path(manifest["state_root"]) / "jobs" / row["job_id"]
        mm.mkdir(folder)
        metadata = {name: {"state": "available", "stream_index": i,
                           "span": {"start_ms": 0, "end_ms": 60000}}
                    for i, name in enumerate(("video", "audio"))}

        def decode_frame(_source, _tool, _stream, target, **kwargs):
            return str(target).encode(), target

        def detect(body, _runtime):
            value = fixtures.frame(int(body), 2 if visual_faces else 0)
            return {key: value[key] for key in ("frame_sha256", "faces", "face_count", "width", "height")}

        with mock.patch.object(engine, "probe", return_value=metadata), \
             mock.patch.object(engine, "decode_frame", side_effect=decode_frame), \
             mock.patch.object(engine, "detect_faces", side_effect=detect):
            self.assertTrue(mm.visual_job(manifest, row, lambda: None, time.monotonic()+60, 1, 2, 3))

        def decode_audio(_source, _tool, _stream, window, **kwargs):
            start, end = window["start_ms"], window["end_ms"]
            return b"pcm", {"start_ms": start, "end_ms": end,
                "requested_start_ms": start, "requested_end_ms": end,
                "source_pts_verified": True, "silence_padding": False, "short_sample": False,
                "pcm_sha256": "a"*64, "decoded_samples": (end-start)*16,
                "discarded_samples": 0, "timestamp_discontinuities": 0, "waveform_retimed": False}

        def analyze(_pcm, receipt, probe):
            index = int(probe.split("-")[-1])
            axis = index % 2 if positive else 0
            voice = fixtures.voice(index, axis, probe=probe, start=receipt["start_ms"]+500)
            voice["id"] = probe + "-0"
            return {"excerpts": [] if no_audio else [voice], "legacy_eligible_excerpts": 0,
                    "vad_positive_ms": 0 if no_audio else 2500}

        model = mock.Mock()
        model.analyze.side_effect = analyze
        with mock.patch.object(engine, "decode_audio", side_effect=decode_audio):
            self.assertTrue(mm.audio_job(manifest, row, lambda: model, time.monotonic()+60, 1, 2))
        return mm.bind(folder / "result.json")

    def test_config_preparation_is_metadata_only(self):
        with mock.patch.object(mm, "source_witness", side_effect=AssertionError("media must not be opened")), \
             mock.patch.object(engine, "face_runtime", side_effect=AssertionError("no model")):
            config = screen.prepare_config(self.manifest_ref, [self.manifest_ref])
        self.assertEqual(len(config["catalog"]), 1)
        self.assertEqual(config["max_runtime_seconds"], 300)

    def test_completed_negative_replays_without_launch_or_old_writes(self):
        self.complete()
        before = {str(p): p.stat().st_mtime_ns for p in Path(self.manifest["state_root"]).rglob("*")}
        with mock.patch.object(mm, "run", side_effect=AssertionError("completed result must be reused")):
            decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        after = {str(p): p.stat().st_mtime_ns for p in Path(self.manifest["state_root"]).rglob("*")}
        self.assertEqual(before, after)
        self.assertEqual(decision["state"], "screen_negative")
        self.assertFalse(decision["diarization"])
        self.assertTrue(decision["reused_existing_screen"])
        self.assertTrue(decision["evidence_summary"]["completed_evidence_replayed"])
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_completed_positive_requests_diarization(self):
        self.complete(positive=True)
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertEqual(decision["state"], "screen_positive")
        self.assertTrue(decision["diarization"])

    def test_completed_visual_only_cues_are_uncertain_with_diarization(self):
        self.complete(visual_faces=True)
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertEqual(decision["state"], "screen_uncertain")
        self.assertTrue(decision["diarization"])
        self.assertFalse(decision["semantics"]["faces_are_speakers"])

    def test_insufficient_audio_cannot_disable_diarization(self):
        self.complete(no_audio=True)
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertEqual(decision["state"], "screen_uncertain")
        self.assertTrue(decision["diarization"])

    def test_tampered_frame_proof_blocks_not_negative(self):
        self.complete()
        path = next(Path(self.manifest["state_root"]).glob("jobs/*/frame-*.json"))
        value = json.loads(path.read_bytes())
        value["face_count"] = 99
        path.write_text(json.dumps(value))
        with self.assertRaises(RuntimeError):
            screen.screen_one(self.recording, self.folder, self.config_ref)

    def test_wrong_raw_recording_binding_rejected(self):
        self.complete()
        changed = deepcopy(self.recording)
        changed["duration_ms"] -= 1
        with self.assertRaisesRegex(screen.ScreenError, "different media or duration"):
            screen.screen_one(changed, self.folder, self.config_ref)

    def test_changed_source_blocks_before_worker(self):
        self.complete()
        self.helper.source.write_bytes(b"changedxxx")
        with mock.patch.object(mm, "run", side_effect=AssertionError("must not launch")):
            with self.assertRaisesRegex(screen.ScreenError, "source witness changed"):
                screen.screen_one(self.recording, self.folder, self.config_ref)

    def test_isolated_one_job_screen_does_not_wait_for_archive(self):
        old = Path(self.manifest["state_root"])
        before = sorted(str(path) for path in old.rglob("*"))

        def run(path, expected):
            manifest = mm.load_manifest(path, expected)
            self.assertEqual(len(manifest["jobs"]), 1)
            self.assertEqual(manifest["audio_refresh"], "all")
            self.assertEqual(manifest["max_runtime_seconds"], 300)
            self.assertTrue(Path(manifest["state_root"]).is_relative_to(self.folder))
            self.complete(manifest)

        with mock.patch.object(mm, "run", side_effect=run) as execute:
            decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        execute.assert_called_once()
        self.assertEqual(decision["state"], "screen_negative")
        self.assertFalse(decision["reused_existing_screen"])
        self.assertEqual(before, sorted(str(path) for path in old.rglob("*")))

    def test_bounded_failure_is_receipted_uncertain_and_not_retried(self):
        with mock.patch.object(mm, "run", side_effect=core.TriageError("finite worker deadline exceeded")) as execute:
            first = screen.screen_one(self.recording, self.folder, self.config_ref)
            second = screen.screen_one(self.recording, self.folder, self.config_ref)
        execute.assert_called_once()
        self.assertEqual(first, second)
        self.assertEqual(first["state"], "screen_uncertain")
        self.assertTrue(first["diarization"])
        self.assertIsNone(first["result"])
        self.assertIsNotNone(first["diagnostic"])
        self.assertFalse(first["evidence_summary"]["completed_evidence_replayed"])

    def test_partial_deadline_is_not_reported_as_completed_evidence(self):
        with mock.patch.object(mm, "run", return_value={"state": "incomplete"}):
            decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertEqual(decision["state"], "screen_uncertain")
        self.assertTrue(decision["diarization"])
        self.assertIsNone(decision["result"])

    def test_integrity_failure_during_worker_cannot_become_uncertainty(self):
        def failed(*_):
            self.helper.source.write_bytes(b"changedxxx")
            raise core.TriageError("changed source")
        with mock.patch.object(mm, "run", side_effect=failed):
            with self.assertRaisesRegex(screen.ScreenError, "source witness changed"):
                screen.screen_one(self.recording, self.folder, self.config_ref)

    def test_one_global_cpu_slot(self):
        with screen._slot(self.config_ref):
            with self.assertRaises(screen.ScreenBusy):
                screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertFalse((self.folder / "multimodal").exists())

    def test_overlap_with_active_workspace_is_rejected(self):
        with self.assertRaisesRegex(screen.ScreenError, "overlaps"):
            screen.screen_one(self.recording, Path(self.manifest["state_root"]), self.config_ref)

    def test_uncommitted_isolated_workspace_requires_review(self):
        (self.folder / "multimodal").mkdir(mode=0o700)
        with self.assertRaisesRegex(screen.ScreenError, "uncommitted"):
            screen.screen_one(self.recording, self.folder, self.config_ref)

    def test_changed_adapter_implementation_blocks_config(self):
        with mock.patch.object(screen, "implementation", return_value={}):
            with self.assertRaisesRegex(screen.ScreenError, "implementation or policy"):
                screen.load_config(self.config_ref)

    def test_unadmitted_index_job_is_rejected(self):
        config = deepcopy(self.config)
        config["catalog"][0]["job"]["job_id"] = "not-admitted"
        reference = archive.write_inventory(self.base / "bad-config.json", config)
        with self.assertRaisesRegex(screen.ScreenError, "not admitted"):
            screen.screen_one(self.recording, self.folder, reference)

    def test_persisted_negative_decision_replays_without_model(self):
        self.complete()
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        with mock.patch.object(mm, "run", side_effect=AssertionError("no model during verification")):
            self.assertEqual(screen.validate_decision(decision, self.recording, self.config_ref), decision)

    def test_persisted_diarization_flag_cannot_be_changed(self):
        self.complete(positive=True)
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        decision["diarization"] = False
        with self.assertRaisesRegex(screen.ScreenError, "does not replay"):
            screen.validate_decision(decision, self.recording, self.config_ref)

    def test_persisted_state_and_extra_fields_cannot_be_changed(self):
        self.complete()
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        decision["extra_authority"] = True
        with self.assertRaisesRegex(screen.ScreenError, "does not replay"):
            screen.validate_decision(decision, self.recording, self.config_ref)

    def test_persisted_result_exact_hash_rechecked(self):
        self.complete()
        decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        decision["result"]["sha256"] = "f"*64
        with self.assertRaises(RuntimeError):
            screen.validate_decision(decision, self.recording, self.config_ref)

    def test_persisted_uncertain_diagnostic_replays(self):
        with mock.patch.object(mm, "run", side_effect=core.TriageError("timeout")):
            decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertEqual(screen.validate_decision(decision, self.recording, self.config_ref), decision)
        decision["diarization"] = False
        with self.assertRaisesRegex(screen.ScreenError, "does not replay"):
            screen.validate_decision(decision, self.recording, self.config_ref)

    def test_persisted_isolated_completed_decision_replays(self):
        def run(path, expected):
            self.complete(mm.load_manifest(path, expected))
        with mock.patch.object(mm, "run", side_effect=run):
            decision = screen.screen_one(self.recording, self.folder, self.config_ref)
        self.assertEqual(screen.validate_decision(decision, self.recording, self.config_ref), decision)

    def test_prepare_config_cli_is_fresh_private_and_offline(self):
        folder = self.base / "private-config"
        folder.mkdir(mode=0o700)
        output = folder / "config.json"
        args = ["prepare-config", "--template-manifest", self.manifest_ref["path"],
                "--expected-sha256", self.manifest_ref["sha256"], "--output", str(output)]
        with mock.patch.object(sys := screen.sys, "stdout", new_callable=io.StringIO) as printed, \
             mock.patch.object(mm, "run", side_effect=AssertionError("no inference during config")):
            self.assertEqual(screen.main(args), 0)
        result = json.loads(printed.getvalue())
        self.assertEqual(result["indexed_screen_jobs"], 1)
        self.assertEqual(result["api_requests"], 0)
        self.assertEqual(screen.load_config(result["configuration"])["catalog"], self.config["catalog"])
        self.assertEqual(output.stat().st_mode & 0o777, 0o400)
        with mock.patch.object(sys, "stderr", new_callable=io.StringIO):
            self.assertEqual(screen.main(args), 2)

    def test_prepare_config_cli_requires_paired_reuse_hash(self):
        with mock.patch.object(screen.sys, "stderr", new_callable=io.StringIO), \
             mock.patch.object(screen, "prepare_config", side_effect=AssertionError("must reject first")):
            result = screen.main(["prepare-config", "--template-manifest", self.manifest_ref["path"],
                "--expected-sha256", self.manifest_ref["sha256"], "--reuse-manifest", self.manifest_ref["path"],
                "--output", str(self.folder / "config.json")])
        self.assertEqual(result, 2)

    def test_write_config_rejects_active_workspace_and_public_parent(self):
        with self.assertRaises(RuntimeError):
            screen.write_config(Path(self.manifest["state_root"]) / "config.json", self.config)
        public = self.base / "public-config"
        public.mkdir(mode=0o755)
        with self.assertRaises(RuntimeError):
            screen.write_config(public / "config.json", self.config)


if __name__ == "__main__":
    unittest.main()
