"""Offline cloud-runtime integration tests, using the real sealed plan contract."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
import uuid
import wave
from pathlib import Path
from unittest import mock

from pipeline import salad_transcription as runtime
from pipeline import salad_transcription_contract as contract
from pipeline.salad_transcription_client import CloudClientError


class FakeClient:
    """Records intended cloud operations, without HTTP or external processes."""

    def __init__(self, workspace):
        self.workspace = workspace
        self.uploads = []
        self.submissions = []
        self.polls = []
        self.endpoint_reads = []
        self.downloads = []
        self.jobs = {}
        self.submit_hook = None
        self.download_result = {}

    def endpoint(self, engine):
        self.endpoint_reads.append(engine)
        return {"name": engine, "price_description": "Synthetic test price; no live service contacted"}

    def upload_file(self, path, object_name, *, expires_seconds, expected_sha256=None, expected_byte_count=None):
        body = path.read_bytes()
        if expected_sha256 != hashlib.sha256(body).hexdigest() or expected_byte_count != len(body):
            raise AssertionError("runtime omitted or changed the prepared upload binding")
        self.uploads.append((str(path), object_name, expires_seconds))
        return f"https://storage-api.salad.com/organizations/test-org/files/{object_name}" + "?token=fixture-token"

    def submit(self, engine, payload):
        self.submissions.append((engine, copy.deepcopy(payload)))
        if self.submit_hook is not None:
            self.submit_hook(engine, payload)
        job_id = str(uuid.UUID(int=len(self.submissions)))
        job = {
            "id": job_id, "organization_name": "test-org", "inference_endpoint_name": engine,
            "input": copy.deepcopy(payload["input"]), "metadata": copy.deepcopy(payload["metadata"]),
            "status": "pending", "events": [], "create_time": "2026-09-06T00:00:00Z",
            "update_time": "2026-09-06T00:00:00Z",
        }
        self.jobs[job_id] = job
        return copy.deepcopy(job)

    def get_job(self, engine, job_id):
        self.polls.append((engine, job_id))
        return copy.deepcopy(self.jobs[job_id])

    def download_output(self, url):
        self.downloads.append(url)
        if isinstance(self.download_result, Exception):
            raise self.download_result
        return copy.deepcopy(self.download_result)


class CloudRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "sources"
        source.mkdir(mode=0o700)
        audio = source / "normalized.wav"
        self.write_wav(audio, 8 * 16000)
        manifest = source / "recording-input.json"
        manifest.write_text('{"synthetic_fixture":true}\n')
        manifest.chmod(0o600)
        tools = self.root / "fixture-tools"
        tools.mkdir(mode=0o700)
        ffmpeg = tools / "ffmpeg"
        ffmpeg.write_bytes(b"synthetic executable; never invoked")
        ffmpeg.chmod(0o700)
        self.recording = {
            "manifest": {"path": str(manifest), "sha256": self.sha(manifest.read_bytes())},
            "recording_id": "recording:fixture", "media_id": "media:fixture",
            "audio": {"path": str(audio), "sha256": self.sha(audio.read_bytes()), "byte_count": audio.stat().st_size,
                      "sample_rate_hz": 16000, "total_samples": 8 * 16000, "duration_ms": 8000},
        }
        self.plan = contract.build_plan(
            [self.recording], organization="test-org", output_root=str(self.root / "cloud"),
            ffmpeg={"path": str(ffmpeg), "sha256": self.sha(ffmpeg.read_bytes())},
            rate_usd_per_hour="0.20", max_estimated_cost_usd="1", chunk_seconds=2, overlap_seconds=0,
        )
        self.workspace = runtime.Workspace(self.plan)
        self.client = FakeClient(self.workspace)
        self.chunks = list(self.workspace.chunks)
        self.initial_source = audio.read_bytes()
        self.source_audio = audio
        self.manifest_loader = mock.patch.object(runtime, "load_recording_input", return_value=copy.deepcopy(self.recording))
        self.manifest_loader.start()
        self.addCleanup(self.manifest_loader.stop)
        self.renderer = mock.patch.object(runtime.Workspace, "_render_wav", autospec=True, side_effect=self.render)
        self.renderer.start()
        self.addCleanup(self.renderer.stop)

    @staticmethod
    def sha(body):
        return hashlib.sha256(body).hexdigest()

    @staticmethod
    def write_wav(path, sample_count):
        with wave.open(str(path), "wb") as handle:
            handle.setparams((1, 2, 16000, sample_count, "NONE", "not compressed"))
            handle.writeframes(b"\0\0" * sample_count)
        path.chmod(0o600)

    def render(self, workspace, directory, recording, chunk):
        path = directory / "audio.wav"
        self.write_wav(path, chunk["analysis"]["end_sample"] - chunk["analysis"]["start_sample"])
        return {"kind": "himr_salad_prepared_audio", "schema_version": 1,
                "plan_id": self.plan["plan_id"], "chunk_id": chunk["chunk_id"], "path": str(path),
                **workspace._verify_wav(path, chunk)}

    def output(self, word="hello"):
        return {"text": word, "duration": 2 / 3600, "processing_time": 0.1,
                "sentence_level_timestamps": [{"text": word, "start": 0.1, "end": 0.9}],
                "word_segments": [{"word": word, "start": 0.1, "end": 0.9, "score": 0.7}]}

    def row_from_disk(self, chunk_id):
        return runtime.read_json(self.workspace.root / "state.json")["chunks"][chunk_id]

    def hold_chunk(self, chunk_id=None):
        chunk_id = chunk_id or self.chunks[0]
        def ambiguous(_engine, _payload):
            raise CloudClientError("synthetic connection failure", ambiguous=True)
        self.client.submit_hook = ambiguous
        with self.workspace.locked(), self.assertRaises(CloudClientError):
            self.workspace.submit(chunk_id, self.client)
        self.client.submit_hook = None
        return chunk_id

    def reconciliation_job(self, chunk_id, *, job_id=None):
        intent = self.row_from_disk(chunk_id)["intent"]
        return {"id": job_id or str(uuid.UUID(int=91)), "organization_name": "test-org",
                "inference_endpoint_name": "transcribe", "input": copy.deepcopy(intent["input"]),
                "metadata": copy.deepcopy(intent["metadata"]), "status": "pending", "events": []}

    def use_features(self, *, engine="transcribe", diarization=False, sentence_diarization=False, summary_words=0):
        self.plan = contract.build_plan(
            [self.recording], organization="test-org", engine=engine, output_root=str(self.root / "cloud"),
            ffmpeg=self.plan["ffmpeg"], rate_usd_per_hour="0.20", max_estimated_cost_usd="1",
            chunk_seconds=2, overlap_seconds=0, diarization=diarization,
            sentence_diarization=sentence_diarization, summary_words=summary_words,
        )
        self.workspace = runtime.Workspace(self.plan)
        self.client = FakeClient(self.workspace)
        self.chunks = list(self.workspace.chunks)

    def test_real_plan_and_prepare_bind_exact_samples_without_cloud_or_source_changes(self):
        self.assertEqual(contract.validate_plan(self.plan), self.plan)
        with self.workspace.locked():
            receipt = self.workspace.prepare(self.chunks[0])
            again = self.workspace.prepare(self.chunks[0])
            self.assertEqual(receipt, again)
            self.assertEqual(self.workspace.summary()["states"], {"planned": 3, "prepared": 1})
            self.assertEqual(self.workspace.collect(), 0)
        self.assertEqual(self.source_audio.read_bytes(), self.initial_source)
        self.assertEqual(self.client.uploads + self.client.submissions + self.client.polls, [])

    def test_request_intent_is_durable_before_post_and_job_id_before_poll(self):
        observed = []
        def assert_intent(_engine, payload):
            row = self.row_from_disk(self.chunks[0])
            self.assertEqual(row["status"], "submitting")
            self.assertEqual(row["intent"], payload)
            self.assertEqual(row["intent_sha256"], self.sha(contract.canonical_bytes(payload)))
            self.assertNotIn("provider_job_id", row)
            observed.append(True)
        self.client.submit_hook = assert_intent
        with self.workspace.locked():
            self.workspace.submit(self.chunks[0], self.client)
        row = self.row_from_disk(self.chunks[0])
        self.assertEqual(observed, [True])
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["provider_job_id"], str(uuid.UUID(int=1)))
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(self.client.polls, [])

    def test_ambiguous_post_is_a_durable_global_hold_and_restart_never_resubmits(self):
        chunk_id = self.hold_chunk()
        self.assertEqual(self.row_from_disk(chunk_id)["status"], "submission_unknown")
        restarted = runtime.Workspace(self.plan)
        with restarted.locked(create=False):
            result = restarted.run_cycle(self.client, max_new_jobs=4, max_inflight=4)
            self.assertEqual(result["submitted_this_cycle"], 0)
            self.assertEqual(result["held_for_reconciliation"], [chunk_id])
            with self.assertRaises(runtime.CloudPipelineError):
                restarted.submit(chunk_id, self.client)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.uploads), 1)
        self.assertEqual(self.client.endpoint_reads, [])

    def test_process_death_after_durable_intent_remains_held_after_restart(self):
        def crash(_engine, _payload):
            raise KeyboardInterrupt()
        self.client.submit_hook = crash
        with self.workspace.locked(), self.assertRaises(KeyboardInterrupt):
            self.workspace.submit(self.chunks[0], self.client)
        self.assertEqual(self.row_from_disk(self.chunks[0])["status"], "submitting")
        restarted = runtime.Workspace(self.plan)
        with restarted.locked(create=False):
            result = restarted.run_cycle(self.client, max_new_jobs=4)
        self.assertEqual(result["submitted_this_cycle"], 0)
        self.assertEqual(len(self.client.submissions), 1)

    def test_definite_submission_rejection_also_requires_operator_review(self):
        def reject(_engine, _payload):
            raise CloudClientError("synthetic rejected request", status_code=400, ambiguous=False)
        self.client.submit_hook = reject
        with self.workspace.locked(), self.assertRaises(CloudClientError):
            self.workspace.submit(self.chunks[0], self.client)
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=4)
        self.assertEqual(result["states"], {"planned": 3, "submission_unknown": 1})
        self.assertEqual(result["submitted_this_cycle"], 0)
        self.assertEqual(len(self.client.submissions), 1)

    def test_poll_rate_limit_keeps_existing_job_for_later_read_without_reposting(self):
        with self.workspace.locked():
            self.workspace.submit(self.chunks[0], self.client)
        before = self.row_from_disk(self.chunks[0])
        error = CloudClientError("synthetic throttling", status_code=429, retry_after_seconds=12)
        with mock.patch.object(self.client, "get_job", side_effect=error):
            with runtime.Workspace(self.plan).locked(create=False) as restarted:
                with self.assertRaises(CloudClientError):
                    restarted.run_cycle(self.client, max_new_jobs=0)
        self.assertEqual(self.row_from_disk(self.chunks[0]), before)
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=0)
        self.assertEqual(result["polled_this_cycle"], 1)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.uploads), 1)

    def test_public_summary_does_not_expose_upload_urls_or_request_metadata(self):
        self.hold_chunk()
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            public = json.dumps(restarted.summary())
        self.assertNotIn("fixture-token", public)
        self.assertNotIn("upload_url", public)
        self.assertNotIn("client_request_id", public)
        self.assertNotIn(str(self.source_audio), public)

    def test_reconciliation_rejects_wrong_id_input_metadata_and_organization(self):
        chunk_id = self.hold_chunk()
        requested = str(uuid.UUID(int=91))
        for field in ["id", "input", "metadata", "organization_name", "inference_endpoint_name"]:
            with self.subTest(field=field):
                job = self.reconciliation_job(chunk_id, job_id=requested)
                if field == "id":
                    job[field] = str(uuid.UUID(int=92))
                elif field == "input":
                    job[field]["url"] += "-wrong-source"
                elif field == "metadata":
                    job[field]["client_request_id"] = "wrong-request"
                else:
                    job[field] = "wrong-value"
                self.client.jobs[requested] = job
                with self.workspace.locked(create=False), self.assertRaises(runtime.CloudPipelineError):
                    self.workspace.reconcile(chunk_id, requested, self.client)
                self.assertEqual(self.row_from_disk(chunk_id)["status"], "submission_unknown")
        self.assertEqual(len(self.client.submissions), 1)

    def test_reconciliation_adopts_exact_job_without_reposting(self):
        chunk_id = self.hold_chunk()
        job = self.reconciliation_job(chunk_id)
        self.client.jobs[job["id"]] = job
        with self.workspace.locked(create=False):
            self.workspace.reconcile(chunk_id, job["id"], self.client)
        self.assertEqual(self.row_from_disk(chunk_id)["provider_job_id"], job["id"])
        self.assertEqual(self.row_from_disk(chunk_id)["status"], "pending")
        self.assertEqual(len(self.client.submissions), 1)

    def test_cycle_limits_inflight_new_submissions_and_poll_count(self):
        with self.workspace.locked():
            result = self.workspace.run_cycle(self.client, max_new_jobs=4, max_inflight=2, max_polls=1)
            self.assertEqual(result["submitted_this_cycle"], 2)
            self.assertEqual(result["states"], {"pending": 2, "planned": 2})
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=4, max_inflight=2, max_polls=1)
        self.assertEqual(result["polled_this_cycle"], 1)
        self.assertEqual(result["submitted_this_cycle"], 0)
        self.assertEqual(len(self.client.submissions), 2)
        self.assertEqual(len(self.client.polls), 1)

    def test_cycle_limits_are_bounded_integers(self):
        invalid = [
            {"max_new_jobs": -1}, {"max_new_jobs": 101}, {"max_inflight": 0}, {"max_polls": 0},
            {"max_new_jobs": True}, {"max_new_jobs": 0.5}, {"max_inflight": 1.5}, {"max_polls": True},
        ]
        with self.workspace.locked():
            for limits in invalid:
                with self.subTest(limits=limits), self.assertRaises(runtime.CloudPipelineError):
                    self.workspace.run_cycle(self.client, **limits)
        self.assertEqual(self.client.submissions, [])

    def test_success_preserves_provider_provenance_assembles_and_does_not_reprocess(self):
        with self.workspace.locked():
            self.workspace.run_cycle(self.client, max_new_jobs=4, max_inflight=4)
        for ordinal, job in enumerate(self.client.jobs.values()):
            job.update(status="succeeded", output=self.output(f"word{ordinal}"))
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=0, max_polls=4)
            self.assertEqual(result["states"], {"completed": 4})
            self.assertEqual(result["assembled_recordings"], 1)
            for chunk_id in self.chunks:
                row = restarted.row(chunk_id)
                directory = restarted.root / "chunks" / chunk_id
                raw = runtime.read_json(directory / "provider-job.json", row["provider_job_sha256"])
                output = runtime.read_json(directory / "provider-output.json", row["provider_output_sha256"])
                transcript = runtime.read_json(directory / "transcript.json", row["transcript_sha256"])
                self.assertEqual(raw, self.client.jobs[row["provider_job_id"]])
                self.assertEqual(transcript["raw_output_sha256"], self.sha(contract.canonical_bytes(output)))
                self.assertEqual(transcript["provider_job_id"], row["provider_job_id"])
                self.assertFalse(transcript["policy"]["verified_quotation"])
                self.assertFalse(transcript["policy"]["source_controller_mutation"])
            assembled_path = restarted.root / "recordings" / (self.sha(b"recording:fixture") + ".json")
            assembled = runtime.read_json(assembled_path)
            self.assertEqual(assembled["text"], "word0 word1 word2 word3")
            self.assertEqual([row["start_ms"] for row in assembled["words"]], [100, 2100, 4100, 6100])
            original = assembled_path.read_bytes()
            self.assertEqual(restarted.collect(), 1)
            self.assertEqual(assembled_path.read_bytes(), original)
        initial_calls = (len(self.client.submissions), len(self.client.polls))
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=0)
        self.assertEqual(result["polled_this_cycle"], 0)
        self.assertEqual((len(self.client.submissions), len(self.client.polls)), initial_calls)
        self.assertEqual(self.source_audio.read_bytes(), self.initial_source)

    def test_download_failure_keeps_provider_job_and_cached_raw_for_resume(self):
        with self.workspace.locked():
            self.workspace.submit(self.chunks[0], self.client)
        job = next(iter(self.client.jobs.values()))
        job.update(status="succeeded", output={"url": "https://storage-api.salad.com/organizations/test-org/files/result.json" + "?token=fixture"})
        self.client.download_result = CloudClientError("synthetic download timeout")
        with self.workspace.locked(create=False), self.assertRaises(CloudClientError):
            self.workspace.run_cycle(self.client, max_new_jobs=0)
        row = self.row_from_disk(self.chunks[0])
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["provider_job_id"], job["id"])
        self.assertTrue((self.workspace.root / "chunks" / self.chunks[0] / "provider-job.json").exists())
        self.client.download_result = self.output()
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=0)
            self.assertEqual(restarted.row(self.chunks[0])["status"], "completed")
        self.assertEqual(len(self.client.polls), 1)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(len(self.client.downloads), 2)

    def test_collect_detects_tampered_raw_evidence(self):
        with self.workspace.locked():
            self.workspace.run_cycle(self.client, max_new_jobs=4, max_inflight=4)
        for job in self.client.jobs.values():
            job.update(status="succeeded", output=self.output())
        with self.workspace.locked(create=False):
            self.workspace.run_cycle(self.client, max_new_jobs=0, max_polls=4)
        path = self.workspace.root / "chunks" / self.chunks[0] / "provider-output.json"
        changed = runtime.read_json(path)
        changed["text"] = "tampered"
        path.write_bytes(contract.canonical_bytes(changed))
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            with self.assertRaises(runtime.CloudPipelineError):
                restarted.collect()

    def test_existing_workspace_lock_rejects_another_writer(self):
        with self.workspace.locked():
            with self.assertRaises(runtime.CloudPipelineError):
                with runtime.Workspace(self.plan).locked(create=False):
                    self.fail("second writer unexpectedly acquired lock")

    def test_primary_diarization_and_summary_submit_flags_and_collected_provenance(self):
        baseline_plan_id = self.plan["plan_id"]
        self.use_features(diarization=True, sentence_diarization=True, summary_words=200)
        self.assertNotEqual(self.plan["plan_id"], baseline_plan_id)
        with self.workspace.locked():
            self.workspace.run_cycle(self.client, max_new_jobs=4, max_inflight=4)
        for engine, request in self.client.submissions:
            self.assertEqual(engine, "transcribe")
            self.assertTrue(request["input"]["diarization"])
            self.assertTrue(request["input"]["sentence_diarization"])
            self.assertTrue(request["input"]["word_level_timestamps"])
            self.assertTrue(request["input"]["sentence_level_timestamps"])
            self.assertEqual(request["input"]["summarize"], 200)
            self.assertNotIn("llm_features", request["input"])
        for ordinal, job in enumerate(self.client.jobs.values()):
            output = self.output(f"word{ordinal}")
            output["summary"] = f"Synthetic overview for chunk {ordinal}."
            output["word_segments"][0]["speaker"] = "SPEAKER_00"
            output["sentence_level_timestamps"][0]["speaker"] = "SPEAKER_00"
            job.update(status="succeeded", output=output)
        with runtime.Workspace(self.plan).locked(create=False) as restarted:
            result = restarted.run_cycle(self.client, max_new_jobs=0, max_polls=4)
            self.assertEqual(result["assembled_recordings"], 1)
            for ordinal, chunk_id in enumerate(self.chunks):
                directory = restarted.root / "chunks" / chunk_id
                row = restarted.row(chunk_id)
                transcript = runtime.read_json(directory / "transcript.json", row["transcript_sha256"])
                self.assertEqual(transcript["summary"], f"Synthetic overview for chunk {ordinal}.")
                self.assertEqual(transcript["summary_status"], "returned")
                self.assertEqual(transcript["words"][0]["speaker"], "SPEAKER_00")
                self.assertEqual(transcript["words"][0]["speaker_id"], chunk_id + ":SPEAKER_00")
                self.assertEqual(transcript["segments"][0]["speaker_id"], chunk_id + ":SPEAKER_00")
                raw = runtime.read_json(directory / "provider-output.json", row["provider_output_sha256"])
                self.assertEqual(transcript["raw_output_sha256"], self.sha(contract.canonical_bytes(raw)))
            assembled = runtime.read_json(restarted.root / "recordings" / (self.sha(b"recording:fixture") + ".json"))
        self.assertEqual(assembled["text"], "word0 word1 word2 word3")
        self.assertEqual(len({word["speaker_id"] for word in assembled["words"]}), 4)
        self.assertEqual(len(assembled["chunk_summaries"]), 4)
        self.assertFalse(assembled["summary_semantics"]["whole_recording_summary_claimed"])
        for ordinal, summary in enumerate(assembled["chunk_summaries"]):
            chunk = self.workspace.chunks[self.chunks[ordinal]]
            self.assertEqual(summary["chunk_id"], self.chunks[ordinal])
            self.assertEqual(summary["text"], f"Synthetic overview for chunk {ordinal}.")
            self.assertEqual(summary["status"], "returned")
            self.assertEqual(summary["core"], chunk["core"])
            self.assertEqual(summary["analysis"], chunk["analysis"])
            self.assertIn(summary["provider_job_id"], self.client.jobs)
        self.assertEqual(self.source_audio.read_bytes(), self.initial_source)

    def test_lite_omits_unsupported_summary_field_but_keeps_timestamps_and_diarization(self):
        self.use_features(engine="transcription-lite", diarization=True, sentence_diarization=True)
        with self.workspace.locked():
            self.workspace.submit(self.chunks[0], self.client)
        engine, request = self.client.submissions[0]
        self.assertEqual(engine, "transcription-lite")
        self.assertNotIn("summarize", request["input"])
        self.assertNotIn("custom_prompt", request["input"])
        self.assertTrue(request["input"]["word_level_timestamps"])
        self.assertTrue(request["input"]["sentence_level_timestamps"])
        self.assertTrue(request["input"]["diarization"])
        self.assertTrue(request["input"]["sentence_diarization"])
        job = next(iter(self.client.jobs.values()))
        output = self.output()
        output["word_segments"][0]["speaker"] = "SPEAKER_00"
        output["sentence_level_timestamps"][0]["speaker"] = "SPEAKER_00"
        job.update(status="succeeded", output=output)
        with self.workspace.locked(create=False):
            self.workspace.run_cycle(self.client, max_new_jobs=0)
            transcript = runtime.read_json(self.workspace.root / "chunks" / self.chunks[0] / "transcript.json")
        self.assertIsNone(transcript["summary"])
        self.assertEqual(transcript["summary_status"], "not_requested")
        self.assertEqual(transcript["words"][0]["speaker"], "SPEAKER_00")

    def test_requested_but_missing_summary_is_explicit_without_fabrication_or_reposting(self):
        self.use_features(summary_words=200)
        with self.workspace.locked():
            self.workspace.run_cycle(self.client, max_new_jobs=4, max_inflight=4)
        for job in self.client.jobs.values():
            job.update(status="succeeded", output=self.output())
        with self.workspace.locked(create=False):
            result = self.workspace.run_cycle(self.client, max_new_jobs=0, max_polls=4)
            self.assertEqual(result["states"], {"completed": 4})
            assembled = runtime.read_json(self.workspace.root / "recordings" / (self.sha(b"recording:fixture") + ".json"))
        self.assertEqual([summary["status"] for summary in assembled["chunk_summaries"]], ["missing"] * 4)
        self.assertTrue(all(summary["text"] is None for summary in assembled["chunk_summaries"]))
        self.assertEqual(len(self.client.submissions), 4)

    def test_saved_intent_features_cannot_be_changed_even_if_hash_is_recomputed(self):
        self.use_features(diarization=True, sentence_diarization=True, summary_words=200)
        self.hold_chunk()
        state_path = self.workspace.root / "state.json"
        state = runtime.read_json(state_path)
        row = state["chunks"][self.chunks[0]]
        row["intent"]["input"]["diarization"] = False
        row["intent_sha256"] = self.sha(contract.canonical_bytes(row["intent"]))
        state_path.write_bytes(contract.canonical_bytes(state))
        with self.assertRaises(runtime.CloudPipelineError):
            with runtime.Workspace(self.plan).locked(create=False):
                self.fail("a modified feature intent was unexpectedly accepted")

    def test_media_renderer_inherits_caller_leases_and_never_closes_them(self):
        self.renderer.stop()
        leases = tuple(os.open(self.root / name, os.O_RDWR | os.O_CREAT, 0o600)
                       for name in ("controller.lock", "companion.lock"))
        for descriptor in leases:
            self.addCleanup(os.close, descriptor)
        workspace = runtime.Workspace(self.plan, lease_fds=(*leases, leases[0]))
        self.assertEqual(workspace.lease_fds, leases)
        observed = []

        def encode(command, **kwargs):
            observed.append(kwargs["pass_fds"])
            for descriptor in leases:
                self.assertIn(descriptor, kwargs["pass_fds"])
                os.fstat(descriptor)
            output_fd = int(command[-1].rsplit("/", 1)[1])
            with os.fdopen(os.dup(output_fd), "wb") as handle:
                with wave.open(handle, "wb") as wav:
                    wav.setparams((1, 2, 16000, 32000, "NONE", "not compressed"))
                    wav.writeframes(b"\0\0" * 32000)
            return mock.Mock(returncode=0)

        with mock.patch.object(runtime.subprocess, "run", side_effect=encode), workspace.locked():
            receipt = workspace.prepare(self.chunks[0])
        self.assertEqual(len(observed), 1)
        self.assertEqual(len(observed[0]), 5)
        self.assertEqual(receipt["path"], str(workspace.root / "chunks" / self.chunks[0] / "audio.wav"))
        for descriptor in leases:
            os.fstat(descriptor)
        self.assertEqual(runtime.Workspace(self.plan).lease_fds, ())

    def test_lease_validation_rejects_nonfiles_invalid_closed_and_replaced_descriptors(self):
        lease = os.open(self.root / "caller.lock", os.O_RDWR | os.O_CREAT, 0o600)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, directory)
        reader, writer = os.pipe()
        self.addCleanup(os.close, reader)
        self.addCleanup(os.close, writer)
        for invalid in ([lease], (True,), (-1,), (999999,), (directory,), (reader,)):
            with self.subTest(invalid=invalid), self.assertRaises(runtime.CloudPipelineError):
                runtime.Workspace(self.plan, lease_fds=invalid)
        workspace = runtime.Workspace(self.plan, lease_fds=(lease,))
        self.renderer.stop()
        os.close(lease)
        with mock.patch.object(runtime.subprocess, "run") as run, \
                self.assertRaises(runtime.CloudPipelineError):
            workspace._render_wav(self.root, self.recording, workspace.chunks[self.chunks[0]])
        run.assert_not_called()
        replacement = os.open(self.root / "different.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if replacement != lease:
                os.dup2(replacement, lease)
            with mock.patch.object(runtime.subprocess, "run") as run, \
                    self.assertRaises(runtime.CloudPipelineError):
                workspace._render_wav(self.root, self.recording, workspace.chunks[self.chunks[0]])
            run.assert_not_called()
        finally:
            os.close(replacement)
            if replacement != lease:
                os.close(lease)


if __name__ == "__main__":
    unittest.main()
