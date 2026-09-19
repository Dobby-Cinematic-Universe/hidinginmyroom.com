"""Guided runner end-to-end contracts with synthetic, recording-local audio."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import speaker_screen_guided as guided
from pipeline.tests.test_speaker_screen_accelerated import EVENTS, FakeDecoders, FakeModel


class GuidedRunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="guided-runner-test-")
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
            source.write_bytes(b"synthetic media " + bytes([index]))
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
            references.append({"path": str(self.root / f"order-{index}.json"), "sha256": guided.screen.digest(order)})
        self.guidance = {"kind": "himr_speaker_screen_guidance", "schema_version": 1, "records": [], "events": []}
        self.request = {"kind": "himr_guided_speaker_screen_request", "schema_version": 1,
            "work_orders": references, "state_root": str(self.root / "guided"),
            "execution": {"batch_size": 2, "max_run_seconds": 60}, "max_target_windows": 6,
            "guidance": {"path": str(self.root / "guidance.json"), "sha256": "0" * 64}}
        runtime = {"python": guided.old_batch._python_binding(),
            "versions": guided.resident.engine.CPU_RUNTIME_PINS,
            "recipe": guided.resident.engine.model_recipe("cpu"), "nvidia_driver_version": None}
        # Patch only this runner's dependency reference, never resident globals.
        patcher = mock.patch.object(guided, "_runtime_binding", return_value=runtime)
        patcher.start()
        self.addCleanup(patcher.stop)
        FakeModel.instances, FakeDecoders.instances = [], []
        FakeModel.mode, FakeModel.fail_call = "solo", None
        FakeDecoders.fail_index, FakeDecoders.close_error = None, False
        EVENTS.clear()

    def document(self, name, value):
        path = self.root / name
        guided.screen.write_immutable(path, value)
        return {"path": str(path), "sha256": guided.screen.digest(value)}

    def add_metadata(self, index, title="life updates", *, targets=None):
        recording = self.orders[index]["recording"]
        acquisition = self.document(f"acquisition-{index}.json", {
            "schema_version": 1, "status": "completed", "dry_run": False, "errors": [],
            "catalog_records": {"media_objects": [recording]},
            "source": {"title": title, "native_id": f"2026090{index + 1}-video.mp4"}})
        cues = None
        if targets is not None:
            evidence = self.document(f"transcript-{index}.json", {"text": "synthetic transcript"})
            review = self.document(f"review-{index}.json", {
                "reviewer": "synthetic fixture", "same_media_mapping_reviewed": True})
            cues = self.document(f"cues-{index}.json", {
                "kind": "himr_speaker_screen_timed_cues", "schema_version": 1,
                "media_id": recording["media_id"], "media_sha256": recording["sha256"],
                "origin": "local_asr", "evidence": evidence,
                "alignment": {"basis": "same_media", "source_media_sha256": recording["sha256"],
                    "source_duration_ms": recording["duration_ms"], "offset_ms": 0,
                    "reviewed": True, "review_evidence": review},
                "segments": [{"cue_id": f"cue-{ordinal}", "kind": "transcript", "start_ms": start,
                    "end_ms": end, "text": "Joining me is our guest"}
                    for ordinal, (start, end) in enumerate(targets)]})
        row = {"media_id": recording["media_id"], "media_sha256": recording["sha256"],
               "acquisition_result": acquisition, "timed_cues": cues}
        self.guidance["records"].append(row)
        return row

    def seal(self):
        for order, ref in zip(self.orders, self.request["work_orders"]):
            ref["sha256"] = guided.screen.digest(order)
            guided.screen.write_immutable(Path(ref["path"]), order)
        self.request["guidance"] = self.document("guidance.json", self.guidance)
        request = self.document("request.json", self.request)
        self.path = Path(self.request["state_root"]) / "manifest.json"
        self.manifest = guided.seal_manifest(request["path"], request["sha256"], str(self.path))
        self.sha = guided.screen.digest(self.manifest)
        return self.manifest

    def run_fake(self, *, model=FakeModel):
        with mock.patch.object(guided, "ResidentModelProcess", model), \
                mock.patch.object(guided, "DecodePool", FakeDecoders):
            return guided.run_batch(str(self.path), self.sha)

    def test_neutral_metadata_keeps_original_order_and_every_baseline(self):
        self.seal()
        self.assertEqual([row["original_index"] for row in self.manifest["plans"]], [0, 1])
        for plan in self.manifest["plans"]:
            self.assertEqual(plan["windows"], guided.core.plan_windows(
                plan["order"]["recording"]["duration_ms"], plan["order"]["policy"]))
            self.assertEqual(plan["guidance"]["priority"]["score"], 0)
            self.assertFalse(Path(plan["original_output_root"]).exists())
        status = guided.status_batch(str(self.path), self.sha)
        self.assertEqual(status["counts"]["baseline_planned_windows"], 8)
        self.assertEqual(status["counts"]["targeted_planned_windows"], 0)
        self.assertFalse((self.path.parent / "guided.lock").exists())

    def test_title_priority_is_stable_queue_order_not_speaker_evidence(self):
        self.add_metadata(0, "life updates")
        self.add_metadata(1, "Interview with a guest")
        self.seal()
        self.assertEqual([row["original_index"] for row in self.manifest["plans"]], [1, 0])
        self.assertEqual([row["index"] for row in self.manifest["plans"]], [0, 1])
        status = guided.status_batch(str(self.path), self.sha)
        self.assertEqual([row["media_id"] for row in status["recordings"]], ["synthetic:1", "synthetic:0"])
        self.assertTrue(all(row["classification"] is None for row in status["recordings"]))
        result = self.run_fake()
        self.assertEqual(result["counts"]["no_second_voice_detected_in_sampled_audio"], 2)
        self.assertEqual(result["recordings"][0]["date"]["basis"], "filename_date")

    def test_equal_title_priority_retains_original_order(self):
        self.add_metadata(1, "Interview")
        self.add_metadata(0, "Interview")
        self.seal()
        self.assertEqual([row["original_index"] for row in self.manifest["plans"]], [0, 1])

    def test_targets_add_probes_without_displacing_uniform_baseline(self):
        self.add_metadata(1, targets=[(30000, 50000)])
        self.seal()
        selected = self.manifest["plans"][0]
        self.assertEqual(selected["original_index"], 1)
        self.assertTrue(selected["sampling"]["target_indices"])
        baseline = [selected["windows"][index] for index in selected["sampling"]["baseline_indices"]]
        expected = guided.core.plan_windows(240000, selected["order"]["policy"])
        self.assertEqual([(w["start_ms"], w["end_ms"]) for w in baseline],
                         [(w["start_ms"], w["end_ms"]) for w in expected])
        result = self.run_fake()
        self.assertEqual(result["state"], "screening_complete")
        self.assertEqual(result["counts"]["baseline_inspected_windows"], 8)
        self.assertGreater(result["counts"]["targeted_inspected_windows"], 0)
        self.assertEqual(result["counts"]["targeted_remaining_windows"], 0)
        self.assertNotIn('"embedding":', json.dumps(result))
        self.assertEqual(len(FakeModel.instances), 1)
        self.assertTrue(FakeModel.instances[0].closed)
        self.assertEqual(guided.status_batch(str(self.path), self.sha)["counts"], result["counts"])

    def test_zero_target_cap_preserves_priority_but_adds_no_samples(self):
        self.add_metadata(1, targets=[(30000, 50000)])
        self.request["max_target_windows"] = 0
        self.seal()
        self.assertEqual(self.manifest["plans"][0]["original_index"], 1)
        self.assertEqual([len(plan["windows"]) for plan in self.manifest["plans"]], [4, 4])

    def test_resident_and_cpu_workspaces_are_rejected(self):
        root = Path(self.request["state_root"])
        root.mkdir(mode=0o700)
        guided.screen.write_immutable(root / "workspace.json", guided.resident.MARKER)
        with self.assertRaisesRegex(guided.ScreenError, "marker differs"):
            self.seal()

    def test_guidance_cannot_overlap_old_output_root(self):
        self.orders[0]["output_root"] = str(self.root / "guidance.json")
        with self.assertRaisesRegex(guided.ScreenError, "overlaps guidance"):
            self.seal()

    def test_changed_guidance_refuses_replay_without_overwriting_evidence(self):
        self.add_metadata(1, "Interview")
        self.seal()
        FakeModel.fail_call = 2
        self.run_fake()
        checkpoint = self.path.parent / self.manifest["plans"][0]["plan_id"] / "batch-0000.json"
        original = checkpoint.read_bytes()
        metadata = Path(self.guidance["records"][0]["acquisition_result"]["path"])
        metadata.write_bytes(metadata.read_bytes().replace(b"Interview", b"Life news"))
        with self.assertRaises(guided.ScreenError):
            self.run_fake()
        self.assertEqual(checkpoint.read_bytes(), original)

    def test_guidance_change_during_inference_blocks_checkpoint_and_closes_workers(self):
        self.add_metadata(1, "Interview")
        self.seal()
        metadata = Path(self.guidance["records"][0]["acquisition_result"]["path"])

        class MutatingModel(FakeModel):
            def analyze_batch(self, items, timeout):
                answer = super().analyze_batch(items, timeout)
                metadata.write_bytes(metadata.read_bytes().replace(b"Interview", b"Life news"))
                return answer

        with self.assertRaisesRegex(guided.ScreenError, "guidance evidence changed"):
            self.run_fake(model=MutatingModel)
        self.assertTrue(FakeModel.instances[-1].closed)
        self.assertGreater(FakeDecoders.instances[-1].closes, 0)
        self.assertFalse(list(self.path.parent.rglob("batch-*.json")))

    def test_interruption_resumes_same_atomic_partition(self):
        self.add_metadata(1, targets=[(30000, 50000)])
        self.seal()
        FakeModel.fail_call = 2
        failed = self.run_fake()
        self.assertEqual(failed["invocation_state"], "failed")
        self.assertEqual(failed["counts"]["completed_windows"], 2)
        first = self.path.parent / self.manifest["plans"][0]["plan_id"] / "batch-0000.json"
        saved = first.read_bytes()
        FakeModel.fail_call = None
        self.assertEqual(self.run_fake()["state"], "screening_complete")
        self.assertEqual(FakeModel.instances[-1].calls[0], [0, 1])
        resumed = self.path.parent / self.manifest["plans"][0]["plan_id"] / "batch-0001.json"
        self.assertEqual([item["observation"]["index"] for item in guided.screen.read_json(resumed)["window_results"]], [2, 3])
        self.assertEqual(first.read_bytes(), saved)

    def test_more_than_512_probes_use_bounded_local_indices_and_resume_global_indices(self):
        self.orders = self.orders[:1]
        self.request["work_orders"] = self.request["work_orders"][:1]
        self.orders[0]["recording"]["duration_ms"] = 30720000
        self.request["execution"]["batch_size"] = 16
        self.request["max_target_windows"] = 64
        self.add_metadata(0, targets=[(index * 300000 + 20000, index * 300000 + 50000) for index in range(32)])
        self.seal()
        plan = self.manifest["plans"][0]
        self.assertEqual(len(plan["sampling"]["baseline_indices"]), 512)
        self.assertEqual(len(plan["windows"]), 576)

        class ProtocolCheckingModel(FakeModel):
            def analyze_batch(self, items, timeout):
                # Exercise the actual unchanged worker boundary, not a fake
                # with more permissive window-index validation.
                validated = guided.resident.worker_api._validate_items(items, self.execution["batch_size"])
                if [item["window"]["index"] for item in validated] != list(range(len(items))):
                    raise AssertionError("model transport indices were not batch-local")
                answer = super().analyze_batch(validated, timeout)
                for observation in answer["observations"]:
                    observation.update(embedding=None, speech_ms=0)
                return answer

        paused = self.run_fake(model=ProtocolCheckingModel)
        self.assertEqual(paused["invocation_state"], "finished")
        self.assertEqual(paused["counts"]["completed_windows"], 512)
        self.assertEqual(paused["state"], "paused")
        job = self.path.parent / plan["plan_id"]
        last_committed = (job / "batch-0031.json").read_bytes()
        complete = self.run_fake(model=ProtocolCheckingModel)
        self.assertEqual(complete["state"], "screening_complete")
        self.assertEqual(complete["counts"]["completed_windows"], len(plan["windows"]))
        self.assertEqual((job / "batch-0031.json").read_bytes(), last_committed)
        final_results = [item for index in range(32, 36)
                         for item in guided.screen.read_json(job / f"batch-{index:04d}.json")["window_results"]]
        restored = [item["observation"]["index"] for item in final_results]
        self.assertEqual(restored, list(range(512, len(plan["windows"]))))
        self.assertEqual(restored[-1], 575)
        for item in final_results:
            window, observation = item["window"], item["observation"]
            self.assertEqual(window, plan["windows"][observation["index"]])
            self.assertEqual(observation["start_ms"], window["start_ms"])
        self.assertEqual(guided.status_batch(str(self.path), self.sha)["counts"], complete["counts"])

    def test_wrong_local_model_response_index_is_not_published(self):
        self.seal()

        class WrongIndexModel(FakeModel):
            def analyze_batch(self, items, timeout):
                answer = super().analyze_batch(items, timeout)
                if wrong_index == "duplicate":
                    answer["observations"][1]["index"] = answer["observations"][0]["index"]
                else:
                    answer["observations"][0]["index"] = wrong_index
                return answer

        for wrong_index in (511, True, "0", "duplicate"):
            with self.subTest(index=wrong_index):
                result = self.run_fake(model=WrongIndexModel)
                self.assertEqual(result["invocation_state"], "failed")
                self.assertEqual(result["counts"]["completed_windows"], 0)
                self.assertFalse(list(self.path.parent.rglob("batch-*.json")))

    def test_positive_decision_cannot_skip_remaining_baseline(self):
        for order in self.orders:
            order["recording"]["duration_ms"] = 600000
            order["resources"]["early_stop_on_positive"] = True
            order["resources"]["max_windows_per_run"] = 4
        self.request["execution"]["batch_size"] = 4
        self.add_metadata(1, targets=[(30000, 50000)])
        self.seal()
        FakeModel.mode = "multiple"
        partial = self.run_fake()
        self.assertEqual(partial["counts"]["multiple_speaker_candidate"], 2)
        self.assertEqual(partial["counts"]["screening_decisions_complete"], 0)
        self.assertGreater(partial["counts"]["baseline_remaining_windows"], 0)
        for _ in range(5):
            result = self.run_fake()
            if result["state"] == "screening_complete":
                break
        self.assertEqual(result["state"], "screening_complete")
        self.assertEqual(result["counts"]["baseline_remaining_windows"], 0)
        self.assertEqual(result["counts"]["baseline_inspected_windows"], 20)

    def test_completed_replay_starts_nothing_and_does_not_write(self):
        self.seal()
        self.run_fake()
        before = {str(path): path.read_bytes() for path in self.path.parent.rglob("*.json")}
        FakeModel.instances.clear()
        result = self.run_fake()
        self.assertEqual(result["metrics"]["model_workers_started"], 0)
        self.assertFalse(FakeModel.instances)
        self.assertEqual(before, {str(path): path.read_bytes() for path in self.path.parent.rglob("*.json")})

    def test_source_change_cannot_reuse_committed_batches(self):
        self.seal()
        FakeModel.fail_call = 2
        self.run_fake()
        source = Path(self.orders[0]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        FakeModel.fail_call = None
        result = self.run_fake()
        self.assertEqual(result["invocation_state"], "failed")
        self.assertEqual(result["counts"]["completed_windows"], 2)

    def test_cancel_closes_model_and_decoder(self):
        self.seal()
        FakeModel.mode = "cancel"
        result = self.run_fake()
        self.assertEqual(result["invocation_state"], "cancelled")
        self.assertTrue(FakeModel.instances[-1].closed)
        self.assertGreater(FakeDecoders.instances[-1].closes, 0)

    def test_status_under_active_lock_does_not_replay_racy_checkpoints(self):
        self.seal()
        with guided._locked(self.path.parent), mock.patch.object(guided, "_read_job", side_effect=AssertionError("racy")):
            status = guided.status_batch(str(self.path), self.sha)
        self.assertEqual(status["state"], "running")
        self.assertIsNone(status["counts"])

    def test_worker_receives_only_unchanged_resident_implementation_subset(self):
        self.seal()
        self.assertEqual(set(self.manifest["implementation"]), set(guided.IMPLEMENTATION_NAMES))
        subset = guided._worker_implementation(self.manifest["implementation"])
        self.assertEqual(set(subset), set(guided.resident.IMPLEMENTATION_NAMES))
        guided.resident.worker_api.verify_implementation(subset)

    def test_manifest_size_limit_fails_before_workspace_creation(self):
        with mock.patch.object(guided, "MAX_MANIFEST_BYTES", 10):
            with self.assertRaisesRegex(guided.ScreenError, "submit fewer work orders"):
                self.seal()
        self.assertFalse(Path(self.request["state_root"]).exists())

    def test_missing_checkpoint_under_sealed_result_is_not_repaired(self):
        self.seal()
        self.run_fake()
        checkpoint = self.path.parent / self.manifest["plans"][0]["plan_id"] / "batch-0000.json"
        checkpoint.unlink()
        with self.assertRaises(guided.ScreenError):
            self.run_fake()
        with self.assertRaises(guided.ScreenError):
            guided.status_batch(str(self.path), self.sha)

    def test_decoder_cleanup_error_does_not_skip_model_cleanup(self):
        self.seal()
        FakeDecoders.close_error = True
        result = self.run_fake()
        self.assertTrue(FakeModel.instances[-1].closed)
        self.assertEqual(result["invocation_state"], "failed")

    def test_parent_implementation_drift_blocks_evidence_publication(self):
        self.seal()
        previous = guided._implementation

        class DriftModel(FakeModel):
            def analyze_batch(self, items, timeout):
                answer = super().analyze_batch(items, timeout)
                self.drift = mock.patch.object(guided, "_implementation", return_value={})
                self.drift.start()
                return answer

            def close(self):
                super().close()
                self.drift.stop()

        result = self.run_fake(model=DriftModel)
        self.assertEqual(result["invocation_state"], "failed")
        self.assertEqual(result["counts"]["completed_windows"], 0)
        self.assertIs(guided._implementation, previous)
        self.assertTrue(FakeModel.instances[-1].closed)

    def test_manifest_cannot_change_sealed_priority_or_sampling(self):
        self.add_metadata(1, "Interview", targets=[(30000, 50000)])
        self.seal()
        for change in ("priority", "original_index", "sampling"):
            value = copy.deepcopy(self.manifest)
            if change == "priority":
                value["plans"][0]["guidance"]["priority"]["score"] = 0
            elif change == "original_index":
                value["plans"][0]["original_index"] = 0
            else:
                value["plans"][0]["sampling"]["baseline_indices"] = []
            with self.subTest(change=change), self.assertRaises(guided.ScreenError):
                guided.validate_manifest(value)


if __name__ == "__main__":
    unittest.main()
