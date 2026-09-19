"""Offline hybrid-to-Salad integration, with real contracts and state machines."""

from __future__ import annotations

import copy
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest import mock
import wave

from pipeline import hybrid_pipeline as hybrid
from pipeline import hybrid_audio_prepare, hybrid_cpu_worker
from pipeline.salad_transcription_client import CloudClientError
from pipeline.tests import test_hybrid_pipeline as hybrid_fixtures
from pipeline.tests.test_salad_transcription_runtime import FakeClient


class OrganizationFakeClient(FakeClient):
    """Reuse the protocol fake, scoped to the hybrid fixture's organization."""

    def __init__(self, organization):
        super().__init__(workspace=None)
        self.organization = organization

    def upload_file(self, *args, **kwargs):
        url = super().upload_file(*args, **kwargs)
        return url.replace("/organizations/test-org/", f"/organizations/{self.organization}/")

    def submit(self, engine, payload):
        job = super().submit(engine, payload)
        job["organization_name"] = self.organization
        self.jobs[job["id"]] = copy.deepcopy(job)
        return job


class HybridCloudCycleTests(unittest.TestCase):
    def setUp(self):
        # Compose the existing synthetic fixture without inheriting its tests.
        self.fixture = hybrid_fixtures.HybridPipelineTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.config = self.fixture.config
        self.store = self.fixture.store
        self.handoff = self.fixture.publish("archive_batch", source_key="archive:cloud-cycle")
        self.store.set_control("running")
        self.job_id = "hybridjob_" + self.handoff["source"]["sha256"][:32]
        self.job_directory = self.store.root / "jobs" / self.job_id
        self.client = OrganizationFakeClient(self.config["cloud"]["organization"])
        self.render_calls = []
        self.intent_checks = 0
        self.legacy_before = {Path(path): Path(path).read_bytes() for path in self.fixture.legacy.config.values()}
        self.source_before = Path(self.handoff["source"]["path"]).read_bytes()
        patches = (
            mock.patch.object(hybrid_audio_prepare, "prepare_audio", side_effect=self.fixture.fake_prepare_worker),
            mock.patch.object(hybrid_cpu_worker, "run_cpu", side_effect=AssertionError("cloud fixture must not run ASR")),
            mock.patch.object(hybrid.salad, "SaladClient", side_effect=self.construct_client),
            mock.patch.object(hybrid.salad.Workspace, "_render_wav", autospec=True, side_effect=self.render),
            mock.patch.object(hybrid.salad.subprocess, "run", side_effect=AssertionError("no media subprocess in this integration test")),
        )
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.client.submit_hook = self.assert_durable_before_submission

    def construct_client(self, organization):
        self.assertEqual(organization, self.config["cloud"]["organization"])
        self.fixture.legacy.assert_held("controller_lock")
        self.fixture.legacy.assert_held("companion_lock")
        return self.client

    def render(self, workspace, directory, recording, chunk):
        self.render_calls.append(chunk["chunk_id"])
        self.assertEqual(len(workspace.lease_fds), 2)
        for descriptor in workspace.lease_fds:
            self.assertEqual(os.fstat(descriptor).st_size, 0)
        self.fixture.legacy.assert_held("controller_lock")
        self.fixture.legacy.assert_held("companion_lock")
        # Only tiny generated PCM; source admission/manifest/plan/state remain real.
        path = directory / "audio.wav"
        samples = chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"]
        self.assertLessEqual(samples, 10 * 16000)
        with wave.open(str(path), "wb") as handle:
            handle.setparams((1, 2, 16000, samples, "NONE", "not compressed"))
            handle.writeframes(b"\0\0" * samples)
        path.chmod(0o600)
        return {"kind": "himr_salad_prepared_audio", "schema_version": 1,
                "plan_id": workspace.plan["plan_id"], "chunk_id": chunk["chunk_id"],
                "path": str(path), **workspace._verify_wav(path, chunk)}

    def read_job(self, store=None):
        store = store or self.store
        row = store.connection.execute("SELECT * FROM jobs WHERE job_id=?", (self.job_id,)).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def cycle(self, *, restarted=False):
        if restarted:
            self.store = hybrid.Store(copy.deepcopy(self.config))
        with self.store.database(writable=True):
            result = self.store.cycle(allow_local=True, allow_cloud=True)
            job = self.read_job()
            self.store._audit_ledger()
        return result, job

    def assert_durable_before_submission(self, engine, payload):
        self.intent_checks += 1
        observer = hybrid.Store(copy.deepcopy(self.config))
        # A second read-only connection independently audits committed evidence.
        with observer.database():
            job = self.read_job(observer)
            self.assertEqual(job["status"], "cloud_running")
            binding = json.loads(job["cloud_plan_json"])
            plan = hybrid.salad.load_plan(Path(binding["path"]), binding["sha256"])
            self.assertGreater(Decimal(job["reserved_cost"]), 0)
            self.assertEqual(job["reserved_cost"], plan["estimate"]["estimated_cost_usd"])
        self.assertEqual(hybrid.salad.read_json(self.job_directory / "admission.json"),
                         {"config_id": self.config["config_id"], "job_id": self.job_id, "handoff": self.handoff})
        self.assertEqual(hybrid.salad.read_json(self.job_directory / "cloud-reservation.json"),
                         {"config_id": self.config["config_id"], "job_id": self.job_id,
                          "plan": binding, "reserved_cost": job["reserved_cost"]})
        self.assertEqual(hybrid.salad.read_json(self.job_directory / "cloud-started.json"),
                         {"config_id": self.config["config_id"], "job_id": self.job_id, "plan": binding})
        cloud_root = Path(plan["output_root"])
        state = hybrid.salad.read_json(cloud_root / "state.json")
        chunk_id = next(iter(state["chunks"]))
        row = state["chunks"][chunk_id]
        self.assertEqual(row["status"], "submitting")
        self.assertEqual(row["intent"], payload)
        self.assertEqual(hybrid.salad.read_json(cloud_root / "chunks" / chunk_id / "submission-intent.json"),
                         {"plan_id": plan["plan_id"], "chunk_id": chunk_id,
                          "intent": payload, "intent_sha256": row["intent_sha256"]})
        self.assertEqual(engine, "transcribe")
        self.assertTrue(payload["input"]["word_level_timestamps"])
        self.assertTrue(payload["input"]["sentence_level_timestamps"])
        self.assertTrue(payload["input"]["diarization"])
        self.assertTrue(payload["input"]["sentence_diarization"])
        self.assertEqual(payload["input"]["summarize"], 200)

    def successful_output(self):
        return {"text": "Synthetic cloud transcript.", "duration": 10 / 3600, "processing_time": 0.1,
                "summary": "Synthetic ten-second overview.",
                "word_segments": [{"word": "Synthetic", "start": 0.1, "end": 0.9, "speaker": "SPEAKER_00", "score": 0.8}],
                "sentence_level_timestamps": [{"text": "Synthetic cloud transcript.", "start": 0.1, "end": 2.0,
                                               "speaker": "SPEAKER_00"}]}

    def test_full_hybrid_cycle_submits_then_restart_polls_collects_without_resubmission(self):
        first, job = self.cycle()
        self.assertEqual(first["status"], "cycle_complete")
        self.assertEqual(first["scan"]["admitted"], 1)
        self.assertEqual(first["dispatched"], [{"job_id": self.job_id, "route": "cloud"}])
        self.assertEqual(job["status"], "cloud_running")
        self.assertEqual(job["attempts"], 0)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.uploads), 1)
        self.assertEqual(self.intent_checks, 1)
        reserved = job["reserved_cost"]
        cloud_binding = json.loads(job["cloud_plan_json"])
        plan = hybrid.salad.load_plan(Path(cloud_binding["path"]), cloud_binding["sha256"])
        self.assertEqual(len(plan["recordings"][0]["chunks"]), 1)
        self.assertEqual(plan["recordings"][0]["chunks"][0]["upload_provider"], "s4")
        provider_job = next(iter(self.client.jobs.values()))
        provider_job.update(status="succeeded", output=self.successful_output())

        second, completed = self.cycle(restarted=True)
        self.assertEqual(second["scan"]["admitted"], 0)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["reserved_cost"], reserved)
        self.assertEqual(second["reserved_estimated_cloud_cost_usd"], reserved)
        self.assertFalse(second["actual_spend_cap"])
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.polls), 1)
        self.assertEqual(len(self.render_calls), 1)
        self.assertEqual(len(self.fixture.prepare_calls), 1)
        self.assertEqual(len(self.fixture.prepare_calls[0]["lease_fds"]), 2)
        self.assertEqual(json.loads(completed["result_json"])["assembled_recordings"], 1)

        with hybrid.salad.Workspace(plan).locked(create=False) as workspace:
            chunk_id = next(iter(workspace.chunks))
            row = workspace.row(chunk_id)
            self.assertEqual(row["status"], "completed")
            directory = workspace.root / "chunks" / chunk_id
            raw = hybrid.salad.read_json(directory / "provider-output.json", row["provider_output_sha256"])
            transcript = hybrid.salad.read_json(directory / "transcript.json", row["transcript_sha256"])
            self.assertEqual(raw, self.successful_output())
            self.assertEqual(transcript["raw_output_sha256"], hashlib.sha256(hybrid.canonical_bytes(raw)).hexdigest())
            self.assertEqual(transcript["summary"], "Synthetic ten-second overview.")
            self.assertEqual(transcript["words"][0]["speaker_id"], chunk_id + ":SPEAKER_00")
            recording_id = plan["recordings"][0]["recording_id"]
            assembled = hybrid.salad.read_json(workspace.root / "recordings" / (hashlib.sha256(recording_id.encode()).hexdigest() + ".json"))
        self.assertEqual(assembled["chunk_summaries"][0]["text"], "Synthetic ten-second overview.")
        self.assertTrue(assembled["summary_semantics"]["whole_recording_summary_claimed"])
        third, _ = self.cycle(restarted=True)
        self.assertEqual(third["dispatched"], [])
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.polls), 1)
        self.assertEqual({path: path.read_bytes() for path in self.legacy_before}, self.legacy_before)
        self.assertEqual(Path(self.handoff["source"]["path"]).read_bytes(), self.source_before)

    def test_ambiguous_submit_restart_preserves_reservation_and_never_reposts(self):
        def ambiguous(engine, payload):
            self.assert_durable_before_submission(engine, payload)
            raise CloudClientError("synthetic uncertain submission", ambiguous=True)
        self.client.submit_hook = ambiguous
        first, job = self.cycle()
        self.assertEqual(first["dispatched"][0]["error_type"], "CloudClientError")
        self.assertEqual(job["attempts"], 1)
        self.assertEqual(len(self.client.submissions), 1)
        reservation = (self.job_directory / "cloud-reservation.json").read_bytes()
        second, held = self.cycle(restarted=True)
        self.assertEqual(held["status"], "held")
        self.assertEqual(json.loads(held["result_json"])["states"], {"submission_unknown": 1})
        self.assertEqual(second["reserved_estimated_cloud_cost_usd"], job["reserved_cost"])
        self.assertEqual((self.job_directory / "cloud-reservation.json").read_bytes(), reservation)
        self.cycle(restarted=True)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.uploads), 1)
        self.assertEqual(len(self.client.polls), 0)
        self.assertEqual(len(self.fixture.prepare_calls), 1)

    def test_started_workspace_state_loss_is_audited_before_provider_access(self):
        self.cycle()
        state = self.job_directory / "cloud" / "state.json"
        state.rename(state.with_name("saved-state-for-test.json"))
        with mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("audit must precede provider access")):
            with self.assertRaises((hybrid.HybridError, hybrid.salad.CloudPipelineError, OSError)):
                self.cycle(restarted=True)
        self.assertFalse(state.exists())
        self.assertEqual(len(self.client.submissions), 1)

    def test_committed_reservation_drift_is_audited_before_provider_access(self):
        self.cycle()
        path = self.job_directory / "cloud-reservation.json"
        receipt = hybrid.salad.read_json(path)
        receipt["reserved_cost"] = "9.99"
        path.write_bytes(hybrid.canonical_bytes(receipt))
        with mock.patch.object(hybrid.salad, "SaladClient", side_effect=AssertionError("audit must precede provider access")):
            with self.assertRaises(hybrid.HybridError):
                self.cycle(restarted=True)
        self.assertEqual(len(self.client.submissions), 1)


if __name__ == "__main__":
    unittest.main()
