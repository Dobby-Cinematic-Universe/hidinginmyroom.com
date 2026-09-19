"""Read-only completed-screen selection, snapshot and proof replay contracts."""
import copy
import errno
import json
from pathlib import Path
import unittest
from unittest import mock

from pipeline import screened_diarization_selection as selection
from pipeline.tests import test_speaker_screen_archive_guided as archive_fixtures
from pipeline.tests import test_speaker_screen_campaign as campaign_fixtures
from pipeline.tests.test_speaker_screen_accelerated import FakeModel


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = archive_fixtures.ArchiveRunnerTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.seal()
        self.binding = {"path": str(self.fixture.path), "sha256": self.fixture.sha}

    def run_screen(self, mode="multiple"):
        FakeModel.mode = mode
        return self.fixture.run_fake()

    def select(self, **options):
        return selection.select_batch(self.binding, **options)

    def validate(self, value, **options):
        return selection.validate_selection_snapshot(value, source={"kind": "batch", "binding": self.binding}, **options)

    @staticmethod
    def reseal(value):
        value["selection_id"] = "screenseldiar_" + selection.screen.digest(
            {key: item for key, item in value.items() if key != "selection_id"})[:32]
        return value

    def files(self):
        return {str(path): (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns)
                for path in self.fixture.root.rglob("*") if path.is_file()}

    def test_not_started_selection_is_explicit_incomplete_and_does_not_write(self):
        before = self.files()
        value = self.select()
        self.assertFalse(value["selection_complete"])
        self.assertEqual(value["records"], [])
        self.assertEqual(value["counts"]["pending_screening_results"], 2)
        self.assertEqual(self.files(), before)

    def test_default_selects_only_complete_multiple_candidates_with_proofs(self):
        self.run_screen()
        before = self.files()
        with mock.patch.object(selection.archive, "ResidentModelProcess", side_effect=AssertionError("model forbidden")), \
                mock.patch.object(selection.archive, "run_batch", side_effect=AssertionError("screen execution forbidden")):
            value = self.select()
        self.assertTrue(value["selection_complete"])
        self.assertEqual(value["counts"]["selected_recordings"], 2)
        self.assertEqual(self.files(), before)
        for record in value["records"]:
            self.assertEqual(record["selected_label"], "multiple_speaker_candidate")
            self.assertTrue(record["screening_evidence"]["checkpoint_replay_verified"])
            self.assertFalse(record["screening_evidence"]["human_reviewed"])
            self.assertFalse(record["screening_evidence"]["source_sha256_reverified"])
            self.assertEqual(selection.verify_selected_record(record), record)
        self.assertNotIn('"embedding":', json.dumps(value))
        self.assertFalse(value["semantics"]["exact_two_speakers_inferred"])

    def test_uncertain_requires_opt_in_and_explicit_id_is_not_an_override(self):
        class UncertainModel(FakeModel):
            def analyze_batch(self, items, timeout):
                answer = super().analyze_batch(items, timeout)
                for row in answer["observations"]:
                    row.update(embedding=None, speech_ms=0)
                return answer

        self.fixture.run_fake(model=UncertainModel)
        identity = self.fixture.orders[0]["recording"]["media_id"]
        self.assertEqual(self.select()["records"], [])
        self.assertEqual(self.select(media_ids=[identity])["records"], [])
        value = self.select(include_uncertain=True, media_ids=[identity])
        self.assertEqual(len(value["records"]), 1)
        self.assertEqual(value["records"][0]["selection_reason"], "uncertain_opt_in")
        self.assertEqual(selection.verify_selected_record(value["records"][0]), value["records"][0])

    def test_negative_screen_never_becomes_solo_or_diarization_selection(self):
        self.run_screen(mode="solo")
        value = self.select(include_uncertain=True)
        self.assertEqual(value["records"], [])
        self.assertFalse(value["semantics"]["solo_inferred"])
        self.assertEqual(value["counts"]["no_second_voice_detected_in_sampled_audio"], 2)

    def test_active_batch_uses_only_complete_results_not_the_live_status_lock(self):
        FakeModel.mode = "multiple"
        FakeModel.fail_call = 3
        self.fixture.run_fake()
        with selection.archive._locked(self.fixture.path.parent), \
                mock.patch.object(selection.archive, "status_batch", side_effect=AssertionError("live status forbidden")):
            value = self.select()
        self.assertEqual(len(value["records"]), 1)
        self.assertFalse(value["selection_complete"])
        self.assertEqual(value["counts"]["pending_screening_results"], 1)

    def test_partial_positive_checkpoint_without_final_result_is_not_selected(self):
        FakeModel.mode = "multiple"
        FakeModel.fail_call = 2
        self.fixture.run_fake()
        with mock.patch.object(selection.archive, "_read_job", side_effect=AssertionError("partial replay forbidden")):
            value = self.select()
        self.assertEqual(value["records"], [])
        self.assertFalse(value["selection_complete"])

    def test_selected_record_stays_valid_when_later_recording_completes(self):
        FakeModel.mode = "multiple"
        FakeModel.fail_call = 3
        self.fixture.run_fake()
        original = self.select()
        record = copy.deepcopy(original["records"][0])
        FakeModel.fail_call = None
        self.fixture.run_fake()
        self.assertEqual(selection.verify_selected_record(record), record)
        complete = self.select()
        self.assertTrue(complete["selection_complete"])
        self.assertNotEqual(original["selection_id"], complete["selection_id"])
        self.assertEqual(len(original["records"]), 1)

    def test_private_0400_results_supported_without_chmod_by_reader(self):
        self.run_screen()
        for plan in self.fixture.manifest["plans"]:
            (self.fixture.path.parent / plan["plan_id"] / "result.json").chmod(0o400)
        before = self.files()
        self.assertEqual(len(self.select()["records"]), 2)
        self.assertEqual(self.files(), before)

    def test_peer_readable_result_rejected_instead_of_changed(self):
        self.run_screen()
        result = self.fixture.path.parent / self.fixture.manifest["plans"][0]["plan_id"] / "result.json"
        result.chmod(0o644)
        with self.assertRaisesRegex(selection.SelectionError, "owner-private"):
            self.select()
        self.assertEqual(result.stat().st_mode & 0o777, 0o644)

    def test_checkpoint_tamper_fails_closed(self):
        self.run_screen()
        plan = self.fixture.manifest["plans"][0]
        checkpoint = self.fixture.path.parent / plan["plan_id"] / "batch-0000.json"
        value = selection.screen.read_json(checkpoint)
        value["window_results"][0]["observation"]["embedding"][0] = 0.5
        checkpoint.write_bytes(selection.screen.canonical(value))
        with self.assertRaisesRegex(selection.SelectionError, "SHA-256"):
            self.select()

    def test_result_first_spoof_cannot_hide_missing_checkpoint(self):
        self.run_screen()
        plan = self.fixture.manifest["plans"][0]
        (self.fixture.path.parent / plan["plan_id"] / "batch-0000.json").unlink()
        with self.assertRaises((selection.SelectionError, FileNotFoundError)):
            self.select()

    def test_source_replacement_fails_without_full_media_hash(self):
        self.run_screen()
        source = Path(self.fixture.orders[0]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        with mock.patch.object(selection.screen, "hash_fd", wraps=selection.screen.hash_fd) as hasher:
            with self.assertRaisesRegex(selection.SelectionError, "source metadata changed"):
                self.select()
        self.assertFalse(any(call.args[1] == 64 * 1024**3 for call in hasher.call_args_list))

    def test_eio_is_not_downgraded_to_pending(self):
        self.run_screen()
        with mock.patch.object(selection, "_source_witness", side_effect=OSError(errno.EIO, "Input/output error")):
            with self.assertRaises(OSError):
                self.select()

    def test_bad_filters_and_unknown_ids_rejected(self):
        for options in ({"include_uncertain": 1}, {"media_ids": "id"}, {"media_ids": ["bad/id"]},
                        {"media_ids": ["synthetic:0", "synthetic:0"]}, {"media_ids": ["unknown"]}):
            with self.subTest(options=options), self.assertRaises(selection.SelectionError):
                self.select(**options)
        self.assertEqual(self.select(media_ids=[])["records"], [])

    def test_explicit_selected_record_cannot_change_label_source_or_bindings(self):
        self.run_screen()
        record = self.select()["records"][0]
        for key, value in (("selected_label", "no_second_voice_detected_in_sampled_audio"),
                           ("selection_reason", "human_confirmed_two_speakers"),
                           ("screening_result", {**record["screening_result"], "sha256": "0" * 64}),
                           ("source_witness", {**record["source_witness"], "st_size": 1})):
            changed = {**record, key: value}
            with self.subTest(key=key), self.assertRaises(selection.SelectionError):
                selection.verify_selected_record(changed)

    def test_selector_does_not_require_the_historical_screen_python_runtime(self):
        self.run_screen()
        with mock.patch.object(selection.archive, "_runtime_binding", side_effect=AssertionError("ML runtime lookup forbidden")), \
                mock.patch.object(selection.archive.old_batch, "_python_binding", side_effect=AssertionError("current Python binding forbidden")):
            value = self.select()
        self.assertEqual(len(value["records"]), 2)

    def test_snapshot_validation_is_read_only_and_never_resnapshots_new_results(self):
        FakeModel.mode, FakeModel.fail_call = "multiple", 3
        self.fixture.run_fake()
        original = self.select()
        FakeModel.fail_call = None
        self.fixture.run_fake()
        before = self.files()
        with mock.patch.object(selection, "_select", side_effect=AssertionError("resnapshot forbidden")), \
                mock.patch.object(selection, "_completed_record", wraps=selection._completed_record) as replay:
            self.assertEqual(self.validate(original), original)
        self.assertEqual(replay.call_count, 1)
        self.assertFalse(original["selection_complete"])
        self.assertEqual(self.files(), before)

    def test_empty_snapshot_validation_does_not_inspect_future_results(self):
        original = self.select()
        self.run_screen()
        with mock.patch.object(selection, "_completed_record", side_effect=AssertionError("future result forbidden")):
            self.assertEqual(self.validate(original), original)

    def test_resealed_snapshot_rejects_schema_semantics_implementation_and_options_changes(self):
        self.run_screen()
        original = self.select()
        cases = [
            {**original, "schema_version": True},
            {**original, "semantics": {**original["semantics"], "screen_is_human_review": True}},
            {**original, "implementation": {}},
            {**original, "options": {"include_uncertain": True, "media_ids": None}},
            {**original, "selection_complete": 1},
            {**original, "unexpected": None},
        ]
        for changed in cases:
            with self.subTest(changed=changed.keys()), self.assertRaises(selection.SelectionError):
                self.validate(self.reseal(copy.deepcopy(changed)))

    def test_resealed_snapshot_rejects_impossible_counts_missing_records_and_record_order(self):
        self.run_screen()
        original = self.select()
        cases = [
            {**original, "counts": {**original["counts"], "completed_screening_results": 1}},
            {**original, "counts": {**original["counts"], "uncertain": 1}},
            {**original, "counts": {**original["counts"], "selected_recordings": True}},
            {**original, "records": original["records"][:1],
             "counts": {**original["counts"], "selected_recordings": 1}},
            {**original, "records": list(reversed(original["records"]))},
            {**original, "records": [original["records"][0], original["records"][0]]},
            {**original, "pending_media_ids": ["unknown"]},
        ]
        for index, changed in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(selection.SelectionError):
                self.validate(self.reseal(copy.deepcopy(changed)))

    def test_snapshot_filter_and_bound_source_membership_cannot_be_overridden(self):
        self.run_screen()
        original = self.select()
        identity = original["records"][0]["recording"]["media_id"]
        filtered = self.select(media_ids=[identity])
        self.assertEqual(self.validate(filtered, media_ids=[identity]), filtered)
        changed = copy.deepcopy(filtered)
        changed["records"] = original["records"][1:]
        with self.assertRaisesRegex(selection.SelectionError, "filter"):
            self.validate(self.reseal(changed), media_ids=[identity])
        changed = copy.deepcopy(original)
        changed["records"][0]["screening_batch"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(selection.SelectionError, "membership"):
            self.validate(self.reseal(changed))
        changed = copy.deepcopy(original)
        changed["source"]["binding"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(selection.SelectionError, "source"):
            self.validate(self.reseal(changed))

    def test_snapshot_validation_rechecks_completed_selected_source(self):
        self.run_screen()
        original = self.select()
        source = Path(original["records"][0]["recording"]["path"])
        source.write_bytes(b"x" * source.stat().st_size)
        with self.assertRaisesRegex(selection.SelectionError, "source metadata changed"):
            self.validate(original)


class CampaignSelectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = campaign_fixtures.CampaignTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.seal()
        self.binding = {"path": str(self.fixture.path), "sha256": self.fixture.sha}

    def test_finite_campaign_selection_intersects_explicit_ids_and_keeps_pending(self):
        FakeModel.mode = "multiple"
        self.fixture.fixtures[0].run_fake()
        value = selection.select_campaign(self.binding, media_ids=["campaign:0"])
        self.assertEqual(value["counts"]["source_recordings"], 2)
        self.assertEqual(value["counts"]["selected_recordings"], 1)
        self.assertFalse(value["selection_complete"])
        self.assertEqual(value["pending_media_ids"], ["campaign:1"])
        self.assertEqual(selection.verify_selected_record(value["records"][0]), value["records"][0])

    def test_campaign_model_and_service_paths_never_called(self):
        with mock.patch.object(selection.campaign, "run_campaign", side_effect=AssertionError("campaign run forbidden")), \
                mock.patch.object(selection.campaign, "status_campaign", side_effect=AssertionError("campaign status forbidden")):
            value = selection.select_campaign(self.binding)
        self.assertEqual(value["records"], [])

    def test_campaign_snapshot_validation_keeps_historical_pending_after_new_completion(self):
        FakeModel.mode = "multiple"
        self.fixture.fixtures[0].run_fake()
        original = selection.select_campaign(self.binding)
        self.fixture.fixtures[1].run_fake()
        with mock.patch.object(selection, "_completed_record", wraps=selection._completed_record) as replay:
            actual = selection.validate_selection_snapshot(original, source={"kind": "campaign", "binding": self.binding})
        self.assertEqual(actual, original)
        self.assertEqual(replay.call_count, 1)
        self.assertFalse(actual["selection_complete"])


if __name__ == "__main__":
    unittest.main()
