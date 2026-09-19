"""Focused regressions for the separately staged title-policy runtime.

The original paid runtime intentionally remains byte-identical. Run these from
the private runtime cwd so its regular pipeline package selects the staged code.
"""
from pathlib import Path
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription as cloud
from pipeline import transcript_summary as io
from pipeline.tests import test_cloud_transcription_runtime as fixtures


@unittest.skipUnless(Path(cloud.__file__).resolve().parent.parent.name == "runtime",
                     "completion-first optimization is staged, not deployed in the original runtime")
class CompletionFirstExportTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CloudRuntimeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        guard = patch("socket.socket", side_effect=AssertionError("network forbidden"))
        guard.start()
        self.addCleanup(guard.stop)

    def test_export_does_not_inspect_unfinished_cloud_screen_backlog(self):
        case = self.fixture
        case.add_recording()
        case.add_recording()
        case.prepare()
        with patch.object(cloud, "inspect_job", side_effect=AssertionError("unfinished job replay")):
            exported = cloud.export(case.ref)
        self.assertEqual(exported["records"], [])
        self.assertEqual(case.submits(), [])

    def test_export_inspects_completed_jobs_but_not_pending_or_unsubmitted_jobs(self):
        case = self.fixture
        case.add_recording()
        case.add_recording()
        case.add_recording()
        case.prepare()
        case.complete_one()
        completed_ids = {row["job_id"] for row in case.plan["recordings"]
                         if (case.state / "jobs" / row["job_id"] / "completion.json").exists()}
        self.assertEqual(len(completed_ids), 1)
        original = cloud.inspect_job
        inspected = []
        def inspect(plan, ref, row):
            self.assertIn(row["job_id"], completed_ids)
            inspected.append(row["job_id"])
            return original(plan, ref, row)
        with patch.object(cloud, "inspect_job", side_effect=inspect):
            exported = cloud.export(case.ref)
        self.assertEqual(set(inspected), completed_ids)
        self.assertEqual(len(exported["records"]), 1)
        self.assertEqual(exported["records"][0]["format"], "cloud")

    def test_completed_export_still_rejects_changed_screen_proof(self):
        case = self.fixture
        case.prepare()
        case.complete_one()
        screen = case.folder() / "screen.json"
        value = io.read(io.binding(screen))
        value["diarization"] = not value["diarization"]
        case.rewrite(screen, value)
        with self.assertRaises(RuntimeError):
            cloud.export(case.ref)

    def test_completed_export_still_rejects_changed_normalized_transcript(self):
        case = self.fixture
        case.prepare()
        case.complete_one()
        path = case.folder() / "transcript.json"
        value = io.read(io.binding(path))
        value["segments"][0]["text"] = "Unverified injected text."
        case.rewrite(path, value)
        with self.assertRaises(RuntimeError):
            cloud.export(case.ref)

    def test_existing_screen_is_not_replayed_by_the_screen_producer(self):
        case = self.fixture
        case.prepare()
        with patch.object(cloud, "_screen_decision", side_effect=AssertionError("historical gate replay")), \
                patch.object(cloud.screen, "screen_one", side_effect=AssertionError("historical inference")), \
                patch.object(cloud.screen, "validate_decision", side_effect=AssertionError("historical replay")):
            result = cloud.run_screen(case.ref)
        self.assertEqual(result["new_screens"], 0)
        self.assertEqual(result["new_paid_requests"], 0)

    def test_skipping_existing_screen_does_not_allow_tampered_first_submission(self):
        case = self.fixture
        case.prepare()
        screen = case.folder() / "screen.json"
        value = io.read(io.binding(screen))
        value["diarization"] = not value["diarization"]
        case.rewrite(screen, value)
        self.assertEqual(cloud.run_screen(case.ref)["new_screens"], 0)
        with self.assertRaises(RuntimeError):
            case.cycle()
        self.assertEqual(case.audio_calls, [])
        self.assertEqual(case.submits(), [])
        self.assertFalse((case.folder() / "intent.json").exists())

    def test_client_error_metadata_exposes_typed_http_status_not_private_details(self):
        error = cloud.clients.CloudClientError("PRIVATE MESSAGE SECRET", status_code=429, ambiguous=False,
                    retry_after_seconds=42.5, response={"body": "PRIVATE PROVIDER BODY", "key": "PRIVATE TOKEN"})
        metadata = cloud.client_error_metadata(error)
        self.assertEqual(metadata["status_code"], 429)
        self.assertIs(metadata["ambiguous"], False)
        self.assertEqual(metadata["retry_after_seconds"], 42.5)
        self.assertFalse(metadata["automatic_retry"])
        self.assertTrue(metadata["paid_evidence_retained"])
        self.assertNotIn("PRIVATE", io.canonical(metadata).decode())

    def test_client_error_metadata_rejects_untyped_or_nonfinite_diagnostics(self):
        for code, ambiguous, retry in (("PRIVATE HTTP VALUE", "PRIVATE FLAG", "PRIVATE RETRY"),
                                       (True, 1, float("nan")), (999, None, float("inf")),
                                       (-1, None, -5)):
            error = cloud.clients.CloudClientError("PRIVATE", status_code=code, ambiguous=ambiguous,
                                                   retry_after_seconds=retry)
            metadata = cloud.client_error_metadata(error)
            self.assertIsNone(metadata["status_code"])
            self.assertIsNone(metadata["ambiguous"])
            self.assertIsNone(metadata["retry_after_seconds"])
            self.assertNotIn("PRIVATE", io.canonical(metadata).decode())

    def test_cycle_discovery_defers_all_unpaid_screen_replays_without_claiming_labels(self):
        case = self.fixture
        case.add_recording()
        case.add_recording()
        case.prepare()
        with patch.object(cloud, "_screen_decision", side_effect=AssertionError("unpaid discovery replay")):
            result = case.cycle(max_new_jobs=0)
            states = cloud._states(case.plan, case.ref, preview_unsubmitted=True)
            with self.assertRaisesRegex(AssertionError, "unpaid discovery replay"):
                cloud.status(case.ref)  # Explicit status remains a full diagnostic.
        self.assertEqual(result["screen_validation_deferred_until_paid_gate"], 2)
        self.assertEqual(result["screen_decisions"], {})
        self.assertEqual(result["diarization_enabled"], 0)
        self.assertEqual(result["diarization_disabled"], 0)
        for state in states.values():
            self.assertEqual(state["state"], "ready")
            self.assertEqual(state["screen_validation"], "deferred_until_paid_gate")
            self.assertIsNone(state["diarization"])
            self.assertIsNone(state["screen_state"])
        self.assertEqual(case.submits(), [])

    def test_any_physical_paid_artifact_disables_unsubmitted_preview_even_if_json_is_falsy(self):
        case = self.fixture
        case.prepare()
        row = case.plan["recordings"][0]
        paths = [case.folder() / name for name in ('intent.json', 'submission.json', 'reconciled.json',
                    'completion.json', 'terminal-job.json', 'collection-review.json',
                    'submission-untrusted-response.json', 'provider-transcript.json')]
        paths.append(case.state / 'reservations' / (row['job_id'] + '.json'))
        for index, path in enumerate(paths):
            with self.subTest(artifact=path.name):
                io.put(path, {})
                with patch.object(cloud, "_screen_decision", side_effect=RuntimeError("full paid validation")):
                    with self.assertRaisesRegex(RuntimeError, "full paid validation"):
                        cloud.inspect_job(case.plan, case.ref, row, preview_unsubmitted=True)
                path.rename(case.root / ("retained-falsy-artifact-" + str(index) + ".json"))

    def test_previewed_corrupt_selected_screen_cannot_reach_audio_upload_or_reservation(self):
        case = self.fixture
        case.prepare()
        screen = case.folder() / "screen.json"
        value = io.read(io.binding(screen))
        value["diarization"] = not value["diarization"]
        case.rewrite(screen, value)
        preview = cloud.status(case.ref, preview_unsubmitted=True)
        self.assertEqual(preview["screen_validation_deferred_until_paid_gate"], 1)
        self.assertEqual(preview["counts"]["cloud_ready"], 1)
        with self.assertRaises(RuntimeError):
            case.cycle()
        self.assertEqual(case.audio_calls, [])
        self.assertEqual(case.clients["assemblyai"].calls, [])
        self.assertFalse((case.folder() / "intent.json").exists())
        self.assertFalse((case.state / "reservations" / (case.plan["recordings"][0]["job_id"] + ".json")).exists())


if __name__ == "__main__":
    unittest.main()
