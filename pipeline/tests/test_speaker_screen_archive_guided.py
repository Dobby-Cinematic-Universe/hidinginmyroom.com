"""Isolated archive interval runner; existing guided policy remains unchanged."""
import copy
import json
from pathlib import Path
import unittest
from unittest import mock

from pipeline import speaker_screen_archive_guided as archive
from pipeline.tests import test_speaker_screen_guided as fixtures
from pipeline.tests.test_speaker_screen_accelerated import FakeDecoders, FakeModel


class ArchiveRunnerTests(unittest.TestCase):
    def setUp(self):
        fixtures.GuidedRunnerTests.setUp(self)
        runtime = fixtures.guided._runtime_binding({})
        patcher = mock.patch.object(archive, "_runtime_binding", return_value=runtime)
        patcher.start()
        self.addCleanup(patcher.stop)
        for order in self.orders:
            order["recording"]["duration_ms"] = 1200000
            order["policy"] = {"probe_ms": 10000, "stride_ms": 300000, "max_windows": 64}
        self.request["kind"] = "himr_archive_guided_speaker_screen_request"
        self.request["max_target_windows"] = 0
        self.request["state_root"] = str(self.root / "archive-guided")

    document = fixtures.GuidedRunnerTests.document
    add_metadata = fixtures.GuidedRunnerTests.add_metadata

    def seal(self):
        for order, reference in zip(self.orders, self.request["work_orders"]):
            reference["sha256"] = archive.screen.digest(order)
            archive.screen.write_immutable(Path(reference["path"]), order)
        self.request["guidance"] = self.document("guidance.json", self.guidance)
        request = self.document("request.json", self.request)
        self.path = Path(self.request["state_root"]) / "manifest.json"
        self.manifest = archive.seal_manifest(request["path"], request["sha256"], str(self.path))
        self.sha = archive.screen.digest(self.manifest)
        return self.manifest

    def run_fake(self, *, model=FakeModel):
        with mock.patch.object(archive, "ResidentModelProcess", model), mock.patch.object(archive, "DecodePool", FakeDecoders):
            return archive.run_batch(str(self.path), self.sha)

    def test_interval_policy_omits_leading_100ms_without_rebasing_source(self):
        self.seal()
        for plan in self.manifest["plans"]:
            self.assertEqual(plan["windows"][0]["start_ms"], 100)
            self.assertEqual(plan["windows"][-1]["end_ms"], 1200000)
            self.assertEqual(plan["order"]["recording"]["duration_ms"], 1200000)
            self.assertEqual(len(plan["windows"]), 4)
            self.assertFalse(Path(plan["original_output_root"]).exists())
        self.assertEqual(self.manifest["semantics"]["leading_source_interval_omitted_ms"], 100)
        self.assertTrue(self.manifest["semantics"]["source_time_origin_unchanged"])
        self.assertNotIn("baseline_sampling_preserved", self.manifest["semantics"])

    def test_new_namespaces_reject_original_guided_manifests_and_markers(self):
        self.seal()
        self.assertTrue(self.manifest["batch_id"].startswith("archivebatch_"))
        self.assertTrue(self.manifest["plans"][0]["plan_id"].startswith("archivescreen_"))
        with self.assertRaises(archive.ScreenError):
            fixtures.guided.validate_manifest(self.manifest)
        with self.assertRaises(archive.ScreenError):
            fixtures.guided._workspace(self.path.parent)

    def test_title_priority_remains_metadata_only_and_no_extra_probes(self):
        self.add_metadata(1, "Interview with our guest")
        self.seal()
        self.assertEqual(self.manifest["plans"][0]["original_index"], 1)
        self.assertFalse(self.manifest["plans"][0]["sampling"]["target_indices"])
        result = self.run_fake()
        self.assertEqual(result["state"], "screening_complete")
        self.assertEqual(result["counts"]["no_second_voice_detected_in_sampled_audio"], 2)
        self.assertTrue(all("opening_100ms_not_screened" in row["reason_flags"] for row in result["recordings"]))
        self.assertNotIn('"embedding":', json.dumps(result))

    def test_pause_resumes_global_indices_and_exact_timestamps(self):
        self.seal()
        FakeModel.fail_call = 2
        first = self.run_fake()
        self.assertEqual(first["invocation_state"], "failed")
        self.assertEqual(first["counts"]["completed_windows"], 2)
        plan = self.manifest["plans"][0]
        checkpoint = self.path.parent / plan["plan_id"] / "batch-0000.json"
        original = checkpoint.read_bytes()
        FakeModel.fail_call = None
        complete = self.run_fake()
        self.assertEqual(complete["state"], "screening_complete")
        self.assertEqual(checkpoint.read_bytes(), original)
        resumed = archive.screen.read_json(checkpoint.parent / "batch-0001.json")
        self.assertEqual([row["observation"]["index"] for row in resumed["window_results"]], [2, 3])
        for row in resumed["window_results"]:
            self.assertEqual(row["observation"]["start_ms"], plan["windows"][row["observation"]["index"]]["start_ms"])

    def test_completed_replay_launches_nothing(self):
        self.seal()
        self.run_fake()
        FakeModel.instances.clear()
        result = self.run_fake()
        self.assertEqual(result["metrics"]["model_workers_started"], 0)
        self.assertFalse(FakeModel.instances)

    def test_incorrect_policy_and_temporal_target_fail_closed(self):
        self.orders[0]["policy"] = {}
        with self.assertRaises(archive.ScreenError):
            self.seal()

    def test_temporal_target_is_not_admitted_into_archive_recipe(self):
        self.add_metadata(0, targets=[(30000, 50000)])
        with self.assertRaisesRegex(archive.ScreenError, "does not admit"):
            self.seal()

    def test_saved_source_drift_is_rejected_and_original_roots_untouched(self):
        self.seal()
        FakeModel.fail_call = 2
        self.run_fake()
        source = Path(self.orders[0]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        FakeModel.fail_call = None
        result = self.run_fake()
        self.assertEqual(result["invocation_state"], "failed")
        self.assertTrue(all(not Path(order["output_root"]).exists() for order in self.orders))

    def test_all_admitted_interval_baseline_probes_required_for_positive(self):
        for order in self.orders:
            order["recording"]["duration_ms"] = 3000000
            order["resources"].update(max_windows_per_run=4, early_stop_on_positive=True)
        self.request["execution"]["batch_size"] = 4
        self.seal()
        FakeModel.mode = "multiple"
        result = self.run_fake()
        self.assertEqual(result["counts"]["multiple_speaker_candidate"], 2)
        self.assertEqual(result["counts"]["screening_decisions_complete"], 0)


if __name__ == "__main__":
    unittest.main()
