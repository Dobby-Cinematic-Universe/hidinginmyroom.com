"""Cloud source/projection/worker integration, with no network or media reads."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_summary as worker
from pipeline import cloud_transcription_client as provider
from pipeline import transcript_summary as r
from pipeline import transcript_summary_sources as sources
from pipeline.tests.test_cloud_transcription_client import assembly_result, rev_job, rev_result
from pipeline.tests import test_transcript_summary_sources as historical_fixtures


class Gemini:
    def __init__(self, root):
        self.root, self.created, self.polled = root, [], []
        self.batches = {}
        self.complete = self.fail_post = self.bad_output = self.missing_usage = False

    def create_batch(self, model, requests, wave_id):
        assert (self.root / "reservations" / (wave_id + ".json")).is_file()
        self.created.append((model, requests, wave_id))
        if self.fail_post:
            raise r.client_module.BatchClientError("synthetic lost POST", ambiguous=True)
        name = "batches/" + wave_id
        self.batches[name] = (model, requests, wave_id)
        return self._remote(name, done=False)

    def _remote(self, name, *, done):
        model, requests, wave = self.batches[name]
        value = {"name": name, "done": done, "metadata": {"model": "models/" + model,
                 "displayName": wave, "state": "BATCH_STATE_SUCCEEDED" if done else "BATCH_STATE_RUNNING"}}
        if done:
            responses = []
            for request in requests:
                prompt = json.loads(request["request"]["contents"][0]["parts"][0]["text"])
                payload = {"summary": [{"text": "Daniel walked outside.", "classification": "reported_statement",
                                        "evidence_ids": [prompt["evidence"][0]["evidence_id"]]}],
                           "topics": [], "events": [], "uncertainties": []}
                if self.bad_output:
                    payload["summary"][0]["evidence_ids"] = ["invented-evidence"]
                response = {"modelVersion": model, "candidates": [{"finishReason": "STOP", "content": {
                    "role": "model", "parts": [{"text": json.dumps(payload)}]}}]}
                if not self.missing_usage:
                    response["usageMetadata"] = {"promptTokenCount": 100, "candidatesTokenCount": 20,
                                                 "thoughtsTokenCount": 0, "totalTokenCount": 120}
                responses.append({"metadata": {"key": request["key"]}, "response": response})
            value["response"] = {"inlinedResponses": {"inlinedResponses": responses}}
        return value

    def get_batch(self, name):
        self.polled.append(name)
        return self._remote(name, done=self.complete)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root, self.number = Path(self.temp.name), 0
        guard = patch("socket.socket", side_effect=AssertionError("network forbidden"))
        guard.start()
        self.addCleanup(guard.stop)

    def file(self, value):
        self.number += 1
        path = self.root / f"source-{self.number}.json"
        path.write_bytes(r.canonical(value))
        path.chmod(0o600)
        return r.binding(path)

    def source(self, recording="recording-1", text="Daniel walked outside.", speaker=None):
        doc = {"kind": "himr_third_party_transcript_import", "schema_version": 1,
               "recording_id": recording, "status": "completed",
               "provenance": {"label": "Third-party fixture", "source_url": "https://example.test/source",
                              "attribution": "fixture", "rights_note": "Not assessed"},
               "segments": [{"start_ms": 1001, "end_ms": 9876, "text": text, "speaker": speaker}]}
        return {"recording_id": recording, "title": "PRIVATE_TITLE_MUST_STAY_LOCAL",
                "date_metadata": {"value": "2026-09-12", "basis": "filename_date"},
                "format": "third_party", "transcript": self.file(doc), "completion": None}


class CloudSourceTests(Fixture):
    def cloud_source(self, name="assemblyai", *, diarization=True):
        media = {"path": str(self.root / "unread-original-media"), "sha256": "a" * 64, "byte_count": 456789}
        recording = "media_sha256_" + media["sha256"]
        audio = {"path": str(self.root / "already-pruned-audio.wav"), "sha256": "b" * 64,
                 "byte_count": 320044, "duration_ms": 10000, "frames": 160000,
                 "sample_rate_hz": 16000, "channels": 1, "sample_width_bytes": 2}
        if name == "assemblyai":
            raw = assembly_result()
            raw["speaker_labels"] = diarization
            raw_ref = terminal_ref = self.file(raw)
            terminal = raw
        else:
            raw, terminal = rev_result(), rev_job()
            terminal["skip_diarization"] = not diarization
            raw_ref, terminal_ref = self.file(raw), self.file(terminal)
        screen = self.file({"kind": "himr_cloud_speaker_screen_decision", "schema_version": 1,
                            "recording_id": recording, "media": media, "diarization": diarization})
        doc = {"kind": "himr_cloud_recording_transcript", "schema_version": 1,
            "recording_id": recording, "job_id": "cloudjob_" + "c" * 32, "source_media": media,
            "status": "completed", "provider_job_id": terminal["id"], "raw_result": raw_ref,
            "provider_job": terminal_ref, "screen_decision": screen, "audio": audio,
            "whole_recording_submitted": True, "machine_generated": True,
            "full_media_coverage_verified": False, "human_reviewed": False, "verified_quotation": False,
            "speaker_identity_inferred": False, "publication_authority": False,
            "normalizer_implementation_sha256": hashlib.sha256(Path(provider.__file__).read_bytes()).hexdigest(),
            **provider.normalize_result(name, raw, expected_duration_seconds=10, job=terminal, diarization=diarization)}
        self.doc = doc
        transcript = self.file(doc)
        self.completion = {"kind": "himr_cloud_transcription_completion", "schema_version": 1,
            "job_id": doc["job_id"], "audio": audio, "raw_result": raw_ref, "provider_job": terminal_ref,
            "screen_decision": screen, "transcript": transcript}
        return {"transcript": transcript, "format": "cloud", "recording_id": recording,
                "completion": self.file(self.completion), "title": "Private title", "date": None}

    def revise(self, spec, doc):
        spec = deepcopy(spec)
        spec["transcript"] = self.file(doc)
        completion = {**self.completion, "transcript": spec["transcript"]}
        spec["completion"] = self.file(completion)
        return spec

    def test_assembly_raw_proof_replay_and_honest_provenance(self):
        source = sources.normalize_source(self.cloud_source())
        self.assertEqual(source["format"], "cloud")
        self.assertEqual(source["provenance"]["origin"], "cloud_asr")
        self.assertTrue(source["provenance"]["machine_generated"])
        self.assertFalse(source["provenance"]["full_media_coverage_verified"])
        self.assertEqual(source["segments"][0]["speaker"], "SPEAKER_0000")
        self.assertEqual(source["segments"][0]["start_ms"], 100)
        self.assertEqual(source["segments"][0]["text"], "Hello.")
        self.assertEqual(source["segments"][0]["source_ref"]["native_segment_id"], "cloud-segment-0")
        self.assertEqual(set(self.doc["segments"][0]), {"start_ms", "end_ms", "text", "speaker"})
        raw = r.read(self.doc["raw_result"])
        self.assertEqual(raw["words"][0]["confidence"], .99)
        self.assertEqual((raw["words"][0]["start"], raw["words"][0]["end"]), (100, 700))

    def test_rev_raw_punctuation_and_job_scoped_labels(self):
        source = sources.normalize_source(self.cloud_source("revai"))
        self.assertEqual(source["segments"][0]["text"], "Hello, world.")
        self.assertEqual(source["segments"][0]["end_ms"], 1200)
        self.assertEqual(source["segments"][0]["source_ref"]["speaker_scope"], rev_job()["id"])
        self.assertNotIn("words", self.doc["segments"][0])
        raw = r.read(self.doc["raw_result"])
        self.assertEqual(raw["monologues"][0]["elements"][0]["ts"], .1)
        self.assertEqual(raw["monologues"][0]["elements"][0]["end_ts"], .7)

    def test_negative_screen_has_no_invented_speaker_labels(self):
        for name in ("assemblyai", "revai"):
            with self.subTest(provider=name):
                source = sources.normalize_source(self.cloud_source(name, diarization=False))
                self.assertTrue(all(row["speaker"] is None for row in source["segments"]))

    def test_requires_explicit_completion(self):
        spec = self.cloud_source()
        spec["completion"] = None
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(spec)

    def test_normalized_segment_or_injected_word_fields_rejected(self):
        spec = self.cloud_source()
        for change in ("text", "words", "speaker", "start_ms"):
            doc = deepcopy(self.doc)
            if change == "words":
                doc["segments"][0]["words"] = [{"text": "Fabricated.", "start_ms": 100, "end_ms": 700}]
            elif change == "start_ms":
                doc["segments"][0]["start_ms"] += 1
            else:
                doc["segments"][0][change] = "Fabricated."
            with self.subTest(change=change), self.assertRaises(sources.SourceError):
                sources.normalize_source(self.revise(spec, doc))

    def test_raw_provider_bytes_changed_rejected(self):
        spec = self.cloud_source()
        path = Path(self.doc["raw_result"]["path"])
        path.chmod(0o600)
        path.write_bytes(r.canonical({"unrelated": True}))
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(spec)

    def test_wrong_provider_job_model_or_normalizer_pin_rejected(self):
        spec = self.cloud_source()
        for field, value in (("provider_job_id", "unrelated-job"), ("model", "not-the-requested-model"),
                             ("normalizer_implementation_sha256", "0" * 64)):
            doc = {**self.doc, field: value}
            with self.subTest(field=field), self.assertRaises(sources.SourceError):
                sources.normalize_source(self.revise(spec, doc))

    def test_wrong_original_media_or_screen_diarization_rejected(self):
        spec = self.cloud_source()
        doc = {**self.doc, "source_media": {**self.doc["source_media"], "sha256": "d" * 64}}
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(self.revise(spec, doc))
        screen = r.read(self.doc["screen_decision"])
        doc = {**self.doc, "screen_decision": self.file({**screen, "diarization": False})}
        changed = self.revise(spec, doc)
        # Bind the changed screen in the completion too, so the recording/screen
        # consistency check—not merely a receipt mismatch—must reject it.
        changed["completion"] = self.file({**self.completion, "transcript": changed["transcript"],
                                          "screen_decision": doc["screen_decision"]})
        with self.assertRaises(sources.SourceError):
            sources.normalize_source(changed)

    def test_cannot_claim_human_review_or_verified_full_coverage(self):
        spec = self.cloud_source()
        for field in ("human_reviewed", "full_media_coverage_verified", "publication_authority"):
            with self.subTest(field=field), self.assertRaises(sources.SourceError):
                sources.normalize_source(self.revise(spec, {**self.doc, field: True}))

    def test_cloud_gemini_input_has_labels_but_no_word_or_segment_timing(self):
        for name in ("assemblyai", "revai"):
            with self.subTest(provider=name):
                source = sources.normalize_source(self.cloud_source(name))
                job = r.core.initial_jobs([source], worker.CONFIG)[0]
                wire = json.loads(job["request"]["body"]["contents"][0]["parts"][0]["text"])
                self.assertEqual(set(wire), {"stage", "evidence"})
                self.assertEqual(set(wire["evidence"][0]), {"evidence_id", "text", "speaker"})
                self.assertEqual(wire["evidence"][0]["speaker"], "p1")
                for field in ("start_ms", "end_ms", "words", "confidence", "duration_seconds"):
                    self.assertNotIn(field, json.dumps(wire))


class ProjectionTests(Fixture):
    def jobs(self, policy=True):
        source = self.source(speaker="SPEAKER_0000", text="At 12:34 I said hello.\nKeep these actual words.")
        evidence = self.file({"kind": "himr_summary_date_evidence", "schema_version": 1,
            "recording_id": source["recording_id"], "value": "2026-09-12", "date_kind": "published",
            "basis": "direct_catalogue_metadata"})
        spec = {"recording_id": source["recording_id"], "transcript": source["transcript"], "format": "third_party",
                "title": source["title"], "completion": None,
                "date": {"value": "2026-09-12", "kind": "published", "evidence": evidence}}
        normalized = sources.normalize_source(spec)
        config = deepcopy(worker.CONFIG)
        if not policy:
            config.pop("transcript_input_policy")
        return normalized, r.core.initial_jobs([normalized], config)

    def test_stripped_input_preserves_text_anonymous_speaker_and_internal_evidence(self):
        source, jobs = self.jobs()
        job = jobs[0]
        wire = json.loads(job["request"]["body"]["contents"][0]["parts"][0]["text"])
        self.assertEqual(set(wire), {"stage", "evidence"})
        self.assertEqual(set(wire["evidence"][0]), {"evidence_id", "text", "speaker"})
        self.assertEqual(wire["evidence"][0]["speaker"], "p1")
        self.assertIn("At 12:34", wire["evidence"][0]["text"])
        serialized = json.dumps(wire)
        for private in (source["title"], "2026-09-12", source["recording_id"], "start_ms", "end_ms",
                        "source_ref", "source_ids", "sources", "period", "provenance", "1001", "9876"):
            self.assertNotIn(private, serialized)
        citation = job["evidence"][0]["citations"][0]
        self.assertEqual(citation["start_ms"], 1001)
        self.assertEqual(citation["date"]["value"], "2026-09-12")

    def test_existing_config_keeps_original_projection(self):
        _, jobs = self.jobs(policy=False)
        wire = json.loads(jobs[0]["request"]["body"]["contents"][0]["parts"][0]["text"])
        self.assertIn("sources", wire)
        self.assertEqual(wire["sources"][0]["date"]["value"], "2026-09-12")

    def test_reducer_input_is_also_stripped_and_links_survive(self):
        source, initial = self.jobs()
        config = initial[0]["config"]
        result = r.core.normalize_api_result(initial[0], {
            "summary": [{"text": "A greeting.", "classification": "reported_statement", "evidence_ids": ["e1"]}],
            "topics": [], "events": [], "uncertainties": []})
        following = r.core.next_jobs([source], initial, {result["job_id"]: result}, config, stages={"transcript"})
        self.assertTrue(following)
        self.assertEqual(set(following[0]["prompt"]["input"]), {"stage", "evidence"})
        self.assertEqual(following[0]["evidence"][0]["citations"][0]["date"]["value"], "2026-09-12")

    def test_invalid_projection_policy_rejected(self):
        for value in ("unknown", True, None):
            with self.subTest(value=value), self.assertRaises(r.core.SummaryError):
                r.core.normalize_config({**worker.CONFIG, "transcript_input_policy": value})


class WorkerTests(Fixture):
    def setUp(self):
        super().setUp()
        self.cloud_ref = self.file({"kind": "synthetic-cloud-plan"})
        self.available = [self.source()]
        for target, value in (("load_plan", {}), ("implementation", {"fixture_dependency": "a" * 64})):
            context = patch.object(worker.cloud, target, return_value=value)
            context.start()
            self.addCleanup(context.stop)
        context = patch.object(worker.cloud, "export", side_effect=lambda _ref: {
            "kind": "himr_preferred_transcript_selection", "plan": self.cloud_ref, "records": self.available})
        context.start()
        self.addCleanup(context.stop)
        # Keep worker outside the original cloud-plan directory by placing the
        # fixture plan in an explicit separate producer root.
        producer = self.root / "producer"
        producer.mkdir(mode=0o700)
        path = producer / "plan.json"
        path.write_bytes(r.canonical({"kind": "synthetic-cloud-plan"}))
        path.chmod(0o600)
        self.cloud_ref = r.binding(path)
        self.worker_root = self.root / "summary-stage"

    def prepare(self, budget=5_000_000):
        self.ref = worker.prepare(self.cloud_ref, self.worker_root,
                                 max_total_budget_microusd=budget)["manifest"]
        self.client = Gemini(self.worker_root)
        return self.ref

    def cycle(self, **kwargs):
        return worker.cycle(self.ref, allow_paid_api=True, client=self.client, **kwargs)

    def snapshot(self):
        manifest = worker.load_manifest(self.ref)
        return worker._snapshot(manifest, self.ref)

    def test_prepare_status_export_offline_no_source_mutation(self):
        source_before = r.read_bytes(self.available[0]["transcript"])
        self.prepare()
        status = worker.status(self.ref)
        self.assertEqual(status["recording_plans"], 0)
        self.assertEqual(status["preferred_transcripts_available"], 1)
        self.assertEqual(worker.export(self.ref)["transcript_summaries"], 0)
        self.assertEqual(r.read_bytes(self.available[0]["transcript"]), source_before)
        self.assertEqual(self.client.created, [])

    def test_explicit_paid_flag_required_before_network(self):
        self.prepare()
        with self.assertRaises(worker.SummaryWorkerError):
            worker.cycle(self.ref, client=self.client)
        self.assertEqual(self.client.created, [])

    def test_finite_end_to_end_chunk_reducer_export_and_restart_no_repeat(self):
        self.prepare()
        first = self.cycle()
        self.assertEqual(first["new_paid_requests"], 1)
        self.client.complete = True
        second = self.cycle()
        self.assertEqual(second["new_paid_requests"], 1)
        third = self.cycle()
        self.assertEqual(third["transcript_summaries_complete"], 1)
        self.assertEqual(third["new_paid_requests"], 0)
        self.assertEqual(worker.export(self.ref)["transcript_summaries"], 1)
        self.assertEqual(self.cycle()["new_paid_requests"], 0)
        self.assertEqual(len(self.client.created), 2)
        for _, requests, _ in self.client.created:
            for request in requests:
                prompt = json.loads(request["request"]["contents"][0]["parts"][0]["text"])
                self.assertIn(prompt["stage"], {"chunk", "transcript"})
                self.assertEqual(set(prompt), {"stage", "evidence"})
        exported = r.read(worker.export(self.ref)["artifact"])
        self.assertEqual(exported["records"][0]["source"]["date_metadata"]["value"], "2026-09-12")

    def test_global_pending_wave_cap_across_records(self):
        self.available = [self.source("recording-1"), self.source("recording-2"), self.source("recording-3")]
        self.prepare()
        result = self.cycle(max_active=2)
        self.assertEqual(result["new_paid_requests"], 2)
        self.assertEqual(result["pending_waves"], 2)
        self.assertEqual(result["recording_plans"], 2)
        self.assertEqual(self.cycle(max_active=2)["new_paid_requests"], 0)

    def test_budget_prevents_posts_across_independent_plans(self):
        self.available = [self.source("recording-1"), self.source("recording-2")]
        self.prepare(budget=140000)
        result = self.cycle(max_active=2)
        self.assertEqual(result["new_paid_requests"], 1)
        self.assertEqual(result["state"], "waiting_remote" if hasattr(worker, "admission") else "budget_paused")
        self.assertLessEqual(result["accounted_microusd"], 140000)
        self.assertEqual(self.cycle(max_active=2)["new_paid_requests"], 0)

    def test_validated_terminal_usage_releases_only_measured_hold(self):
        self.prepare()
        before = self.cycle()["accounted_microusd"]
        self.client.complete = True
        after = self.cycle(max_new_waves=0)
        self.assertLess(after["accounted_microusd"], before)
        self.assertGreater(after["usage_estimate_microusd"], 0)
        self.assertEqual(after["unsettled_hold_microusd"], 0)

    def test_missing_usage_keeps_entire_cost_hold(self):
        self.prepare()
        before = self.cycle()["accounted_microusd"]
        self.client.complete = self.client.missing_usage = True
        after = self.cycle(max_new_waves=0)
        self.assertEqual(after["accounted_microusd"], before)
        self.assertEqual(after["usage_estimate_microusd"], 0)

    def test_lost_post_keeps_reservation_and_never_retries(self):
        self.prepare()
        self.client.fail_post = True
        first = self.cycle()
        self.assertTrue(first["holds"])
        self.assertEqual(first["new_paid_requests"], 1)
        self.assertEqual(first["confirmed_new_submissions"], 0)
        self.assertGreater(first["unsettled_hold_microusd"], 0)
        self.cycle()
        self.assertEqual(len(self.client.created), 1)

    def test_unknown_posts_consume_active_cap_and_attempt_limit(self):
        self.available = [self.source("recording-" + str(i)) for i in range(6)]
        self.prepare()
        self.client.fail_post = True
        first = self.cycle(max_active=2, max_new_waves=1)
        self.assertEqual(first["new_paid_requests"], 1)
        self.assertEqual(first["potential_active_waves"], 1)
        second = self.cycle(max_active=2, max_new_waves=1)
        self.assertEqual(second["new_paid_requests"], 1)
        self.assertEqual(second["potential_active_waves"], 2)
        self.assertEqual(second["state"], "needs_review")
        self.assertEqual(self.cycle(max_active=2)["new_paid_requests"], 0)
        self.assertEqual(len(self.client.created), 2)

    def test_dotenv_path_forwarded_without_reading_keys_in_worker(self):
        self.prepare()
        env_file = str(self.root / ".env-private")
        with patch.object(r, "api_client", return_value=self.client) as factory:
            result = worker.cycle(self.ref, allow_paid_api=True, env_file=env_file)
        self.assertEqual(result["new_paid_requests"], 1)
        factory.assert_called_once_with("gemini", env_file=env_file)

    def test_ledger_without_local_intent_is_held_never_automatically_reused(self):
        self.prepare()
        with patch.object(r, "submit_wave", side_effect=RuntimeError("crash before local intent")):
            with self.assertRaises(RuntimeError):
                self.cycle()
        held = worker.status(self.ref)["unsettled_hold_microusd"]
        self.assertGreater(held, 0)
        resumed = self.cycle()
        self.assertEqual(resumed["new_paid_requests"], 0)
        self.assertEqual(resumed["state"], "needs_review")
        self.assertEqual(resumed["holds"][0]["state"], "orphan_global_reservation")
        self.assertEqual(worker.status(self.ref)["unsettled_hold_microusd"], held)
        self.assertEqual(self.client.created, [])

    def test_failed_evidence_validation_not_automatically_retried(self):
        self.prepare()
        self.cycle()
        self.client.complete = self.client.bad_output = True
        result = self.cycle()
        self.assertTrue(result["holds"])
        self.assertEqual(result["transcript_summaries_complete"], 0)
        self.cycle()
        self.assertEqual(len(self.client.created), 1)

    def test_child_paid_intent_without_global_ledger_aborts(self):
        self.prepare()
        self.cycle()
        ledger = next((self.worker_root / "reservations").glob("*.json"))
        ledger.unlink()
        with self.assertRaises(worker.SummaryWorkerError):
            worker.status(self.ref)

    def test_tampered_global_reservation_aborts(self):
        self.prepare()
        self.cycle()
        ledger = next((self.worker_root / "reservations").glob("*.json"))
        value = r.read(r.binding(ledger))
        value["maximum_cost_microusd"] = 1
        ledger.chmod(0o600)
        ledger.write_bytes(r.canonical(value))
        with self.assertRaises(worker.SummaryWorkerError):
            worker.status(self.ref)

    def test_worker_lock_does_not_lock_producer_but_blocks_second_summary_worker(self):
        self.prepare()
        producer = Path(self.cloud_ref["path"]).parent
        with r.locked(producer):
            self.assertEqual(self.cycle()["new_paid_requests"], 1)
        with r.locked(self.worker_root), self.assertRaises(r.Error):
            self.cycle()

    def test_new_completed_preferred_source_admitted_on_later_cycle(self):
        self.available = []
        self.prepare()
        self.assertEqual(self.cycle()["new_paid_requests"], 0)
        self.available.append(self.source("arrived-later"))
        self.assertEqual(self.cycle()["new_paid_requests"], 1)

    def test_wrong_revision_or_injected_source_entry_aborts(self):
        self.prepare()
        self.cycle()
        self.available[0] = self.source(text="Changed revision.")
        with self.assertRaises(worker.SummaryWorkerError):
            worker.status(self.ref)

    def test_paid_poll_only_does_not_admit_or_submit_new_sources(self):
        self.prepare()
        self.assertEqual(self.cycle(max_new_waves=0)["recording_plans"], 0)
        self.assertEqual(self.client.created, [])

    def test_stopping_before_work_makes_no_post(self):
        self.prepare()
        result = self.cycle(stopping=lambda: True)
        self.assertEqual(result["state"], "paused")
        self.assertEqual(self.client.created, [])

    def test_source_metadata_and_provider_payload_not_in_gemini_input(self):
        self.prepare()
        self.cycle()
        _, requests, _ = self.client.created[0]
        actual = requests[0]["request"]["contents"][0]["parts"][0]["text"]
        for value in ("PRIVATE_TITLE", "2026-09-12", "start_ms", "end_ms", "provenance", "source_ref"):
            self.assertNotIn(value, actual)

    def test_valid_historical_asr_and_old_canary_are_not_preferred_sources(self):
        self.prepare()
        canary = "himrlongjob_e29c548056b03029e0c225bf23afc8e1"
        for format_ in ("longform", "normalized"):
            if format_ == "longform":
                doc = historical_fixtures.SourceTests.longform(self)
                doc["recording"]["recording_id"] = canary
                doc = historical_fixtures.reseal(doc, "assembly_id", "lfassembly_")
            else:
                doc = historical_fixtures.SourceTests.normalized(self)
            spec = {"format": format_, "recording_id": canary, "transcript": self.file(doc),
                    "completion": None, "title": "Completed historical ASR canary", "date": None}
            if format_ == "normalized":
                receipt = historical_fixtures.sealed({"kind": "himr_faster_whisper_gpu_result", "schema_version": 5,
                    "status": "completed", "artifacts": [{"artifact_kind": "transcript_normalized_json",
                        **spec["transcript"], "identity_sha256": doc["identity_sha256"]}]},
                    "result_id", "gpuasrresult5_")
                spec["completion"] = self.file(receipt)
            # This is valid completed legacy ASR, not merely malformed input.
            self.assertEqual(sources.normalize_source(spec)["provenance"]["completion"], "completed")
            self.available = [{key: spec[key] for key in ("recording_id", "format", "transcript", "completion", "title")}]
            self.available[0]["date_metadata"] = {"value": None, "basis": "unknown"}
            with self.subTest(format=format_), self.assertRaises(worker.SummaryWorkerError):
                self.cycle()
        self.assertEqual(self.client.created, [])
        self.assertEqual(list((self.worker_root / "entries").iterdir()), [])

    def test_unselected_completed_local_asr_is_not_rediscovered(self):
        old_canary = historical_fixtures.SourceTests.longform(self)
        old_canary["recording"]["recording_id"] = "historical-local-canary"
        old_canary = historical_fixtures.reseal(old_canary, "assembly_id", "lfassembly_")
        self.file(old_canary)
        self.prepare()
        seen = []
        original = sources.normalize_source
        def observed(spec):
            seen.append(spec["recording_id"])
            return original(spec)
        with patch.object(sources, "normalize_source", side_effect=observed):
            self.assertEqual(self.cycle()["new_paid_requests"], 1)
        self.assertTrue(seen)
        self.assertEqual(set(seen), {"recording-1"})

    def test_even_one_third_party_anonymous_label_waits_without_client_construction(self):
        self.available = [self.source("anonymous", speaker="SPEAKER_0000")]
        self.prepare()
        with patch.object(r, "api_client", side_effect=AssertionError("anonymous source reached Gemini")):
            result = worker.cycle(self.ref, allow_paid_api=True)
        self.assertEqual(result["state"], "waiting_for_speaker_identity")
        self.assertEqual(result["speaker_identity_pending"], 1)
        self.assertEqual(result["speaker_identity_pending_recording_ids"], ["anonymous"])
        self.assertEqual(result["speaker_identity_holds"][0]["reason"], "speaker_identity_pending")
        self.assertEqual(result["speaker_identity_holds"][0]["anonymous_label_count"], 1)
        self.assertEqual(result["recording_plans"], 0)
        self.assertEqual(result["new_paid_requests"], 0)
        self.assertEqual(result["holds"], [])
        self.assertEqual(result["transcript_summaries_complete"], 0)
        self.assertEqual(worker.export(self.ref)["speaker_identity_pending"], 1)
        self.assertEqual(r.read(self.available[0]["transcript"])["segments"][0]["speaker"], "SPEAKER_0000")
        self.assertEqual(worker.load_manifest(self.ref)["policy"]["anonymous_label_handling"], "hold_for_speaker_identity")

    def test_one_labeled_segment_among_unlabeled_still_holds_whole_recording(self):
        source = self.source("mixed")
        doc = r.read(source["transcript"])
        doc["segments"].append({"start_ms": 11000, "end_ms": 12000, "text": "Guest words.", "speaker": "SPEAKER_0001"})
        source["transcript"] = self.file(doc)
        self.available = [source]
        self.prepare()
        result = self.cycle()
        self.assertEqual(result["speaker_identity_pending"], 1)
        self.assertEqual(result["speaker_identity_holds"][0]["anonymous_segment_count"], 1)
        self.assertEqual(self.client.created, [])

    def test_unlabeled_transcript_advances_while_anonymous_source_waits(self):
        self.available = [self.source("anonymous", speaker="SPEAKER_0000"), self.source("unlabeled")]
        self.prepare()
        result = self.cycle(max_active=1)
        self.assertEqual(result["speaker_identity_pending"], 1)
        self.assertEqual(result["recording_plans"], 1)
        self.assertEqual(result["new_paid_requests"], 1)
        self.assertEqual(set(self.snapshot()["records"]), {"unlabeled"})
        self.client.complete = True
        self.cycle(max_active=1)
        final = self.cycle(max_active=1)
        self.assertEqual(final["transcript_summaries_complete"], 1)
        self.assertEqual(final["state"], "waiting_for_speaker_identity")
        self.assertEqual(final["speaker_identity_pending"], 1)
        self.assertEqual(len(self.client.created), 2)

    def test_positive_and_uncertain_cloud_diarization_waits_before_any_summary_phase(self):
        for provider_name, screen_state in (("assemblyai", "screen_positive"), ("revai", "screen_uncertain")):
            with self.subTest(provider=provider_name, screen=screen_state):
                spec = CloudSourceTests.cloud_source(self, provider_name, diarization=True)
                screen = r.read(self.doc["screen_decision"])
                self.doc["screen_decision"] = self.file({**screen, "state": screen_state})
                spec["transcript"] = self.file(self.doc)
                spec["completion"] = self.file({**self.completion, "screen_decision": self.doc["screen_decision"],
                                                "transcript": spec["transcript"]})
                self.assertTrue(any(row["speaker"] for row in sources.normalize_source(spec)["segments"]))
                self.available = [{"recording_id": spec["recording_id"], "format": "cloud", "transcript": spec["transcript"],
                    "completion": spec["completion"], "title": None, "date_metadata": {"value": None, "basis": "unknown"}}]
                if not hasattr(self, "ref"):
                    self.prepare()
                with patch.object(r, "api_client", side_effect=AssertionError("anonymous cloud source reached Gemini")):
                    result = worker.cycle(self.ref, allow_paid_api=True)
                self.assertEqual(result["state"], "waiting_for_speaker_identity")
                self.assertEqual(result["speaker_identity_pending"], 1)
                self.assertEqual(result["recording_plans"], 0)
                self.assertEqual(result["new_paid_requests"], 0)

    def test_diarization_off_cloud_speaker_none_is_allowed(self):
        spec = CloudSourceTests.cloud_source(self, "assemblyai", diarization=False)
        self.available = [{"recording_id": spec["recording_id"], "format": "cloud", "transcript": spec["transcript"],
            "completion": spec["completion"], "title": None, "date_metadata": {"value": None, "basis": "unknown"}}]
        self.prepare()
        result = self.cycle()
        self.assertEqual(result["speaker_identity_pending"], 0)
        self.assertEqual(result["new_paid_requests"], 1)

    def test_previously_admitted_anonymous_source_gets_no_first_wave(self):
        self.available = [self.source("previously-admitted-anonymous", speaker="SPEAKER_0000")]
        self.prepare()
        # Simulate retained admission from an earlier worker policy. It has no
        # submitted request; the current gate must not turn it into paid work.
        worker._ensure_record(worker.load_manifest(self.ref), self.available[0])
        result = self.cycle()
        self.assertEqual(result["recording_plans"], 1)
        self.assertEqual(result["speaker_identity_pending"], 1)
        self.assertEqual(result["state"], "waiting_for_speaker_identity")
        self.assertEqual(self.client.created, [])
        self.assertEqual(self.snapshot()["waves"], {})

    def test_existing_paid_anonymous_chunk_is_collected_without_reducer_submission(self):
        self.available = [self.source("previously-paid-anonymous", speaker="SPEAKER_0000")]
        self.prepare()
        manifest = worker.load_manifest(self.ref)
        entry = worker._ensure_record(manifest, self.available[0])
        ref = entry["plan"]
        ready = r.prepare_plan(ref["path"], ref["sha256"], phase="transcripts")
        plan, selected = r.load_plan(ref["path"], ref["sha256"])
        state = r.load_state(plan, selected)
        wave = next(row for row in state["waves"] if row["wave_id"] == ready["wave_id"])
        r.put(self.worker_root / "reservations" / (wave["wave_id"] + ".json"), worker._reservation(self.ref, entry, wave))
        r.submit_wave(ref["path"], ref["sha256"], wave["wave_id"], allow_paid_api=True, client=self.client, phase="transcripts")
        self.client.complete = True
        result = self.cycle()
        self.assertEqual(result["new_paid_requests"], 0)
        self.assertEqual(result["speaker_identity_pending"], 1)
        self.assertEqual(result["state"], "waiting_for_speaker_identity")
        self.assertEqual(len(self.client.created), 1)
        self.assertEqual(len(self.client.polled), 1)
        replay = r.load_state(*r.load_plan(ref["path"], ref["sha256"]))
        self.assertEqual(len(replay["collections"]), 1)
        self.assertEqual(len(replay["results"]), 1)
        self.assertEqual(replay["results"][0]["stage"], "chunk")
        self.assertEqual(worker.export(self.ref)["transcript_summaries"], 0)
        self.assertEqual(self.cycle()["new_paid_requests"], 0)

    def test_invalid_or_claimed_named_labels_cannot_bypass_gate(self):
        for label in ("Daniel", True, {"name": "Daniel"}):
            with self.subTest(label=label):
                self.available = [self.source("invalid-speaker", speaker=label)]
                if not hasattr(self, "ref"):
                    self.prepare()
                with self.assertRaises(worker.SummaryWorkerError):
                    self.cycle()
        self.assertEqual(self.client.created, [])

    def test_anonymous_label_text_is_not_mistaken_for_diarization_metadata(self):
        self.available = [self.source(text="The subtitle literally says SPEAKER_0000, but this is spoken text.")]
        self.prepare()
        self.assertEqual(self.cycle()["new_paid_requests"], 1)

    def test_identity_gate_does_not_build_full_summary_evidence_for_held_archive(self):
        self.available = [self.source("anonymous", speaker="SPEAKER_0000")]
        self.prepare()
        with patch.object(sources, "normalize_source", side_effect=AssertionError("unnecessary full normalization")):
            result = self.cycle()
        self.assertEqual(result["speaker_identity_pending"], 1)


if __name__ == "__main__":
    unittest.main()
