"""Runner integration with real contracts and injected, network-free transports."""
from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_client as client
from pipeline import transcript_summary_sources as sources


class Response(io.BytesIO):
    def __init__(self, value=None, raw=None):
        super().__init__(runner.canonical(value) if raw is None else raw)
        self.status, self.headers = 200, {}


def payload(job, text="The source reports a short walk."):
    return {"summary": [{"text": text, "classification": "reported_statement",
                "evidence_ids": [item["evidence_id"] for item in job["prompt"]["input"]["evidence"][:24]]}],
            "topics": [], "events": [], "uncertainties": []}


def response_body(provider, value):
    text = json.dumps(value)
    if provider == "gemini":
        return {"candidates": [{"finishReason": "STOP", "content": {
            "role": "model", "parts": [{"text": text}]}}]}
    return {"id": "resp_fixture", "status": "completed", "error": None,
            "incomplete_details": None, "output": [{"type": "message", "role": "assistant",
            "status": "completed", "content": [{"type": "output_text", "text": text}]}]}


class Service:
    """Real provider client, synthetic HTTP boundary; no network fallback."""
    def __init__(self, wave, plan_id):
        self.wave, self.plan_id = wave, plan_id
        self.provider = wave["provider"]
        self.name = ("batches/" if self.provider == "gemini" else "batch_") + wave["wave_id"][-16:]
        self.file_id = "file-" + wave["wave_id"][-16:]
        self.state = "pending"
        self.calls = []
        self.fail_post = False
        self.fail_post_suffix = None
        self.remote_override = {}
        self.rows = [{"custom_id": job["job_id"], "response": response_body(self.provider, payload(job)),
                      "error": None} for job in wave["jobs"]]
        cls = client.GeminiBatchClient if self.provider == "gemini" else client.OpenAIBatchClient
        self.api = cls("test-key-not-a-real-credential", transport=self.transport)

    def remote(self):
        if self.provider == "gemini":
            value = {"name": self.name, "done": self.state != "pending", "metadata": {
                "@type": "type.googleapis.com/google.ai.generativelanguage.v1beta.GenerateContentBatch",
                "name": self.name, "model": "models/" + self.wave["model"],
                "displayName": self.wave["wave_id"], "state": {
                    "pending": "BATCH_STATE_RUNNING", "completed": "BATCH_STATE_SUCCEEDED",
                    "failed": "BATCH_STATE_FAILED"}[self.state]}}
            if self.state == "completed":
                value["response"] = {"inlinedResponses": {"inlinedResponses": [
                    {"metadata": {"key": row["custom_id"]}, "response": row["response"], "error": row["error"]}
                    for row in self.rows]}}
            elif self.state == "failed":
                value["error"] = {"code": 13, "message": "Synthetic failure"}
        else:
            value = {"id": self.name, "object": "batch", "endpoint": "/v1/responses",
                "input_file_id": self.file_id, "completion_window": "24h", "created_at": 1789228800,
                "status": "in_progress" if self.state == "pending" else self.state,
                "output_file_id": "file-output" if self.state == "completed" else None, "error_file_id": None,
                "metadata": {"himr_wave": self.wave["wave_id"], "himr_plan": self.plan_id}}
        return {**value, **copy.deepcopy(self.remote_override)}

    def transport(self, request, timeout):
        method, url = request.get_method(), request.full_url
        self.calls.append((method, url, request.data))
        if method == "POST" and (self.fail_post or (self.fail_post_suffix and url.endswith(self.fail_post_suffix))):
            raise OSError("Synthetic lost response, do not repeat")
        if method == "POST" and url.endswith("/files"):
            return Response({"id": self.file_id, "object": "file", "purpose": "batch",
                "bytes": len(runner.wire(self.wave["jobs"])), "created_at": 1789228800,
                "expires_at": 1789833600, "filename": "himr-summaries.jsonl"})
        if method == "GET" and url.endswith("/content"):
            data = b"".join(runner.canonical({"custom_id": row["custom_id"], "error": row["error"],
                "response": {"status_code": 200, "body": row["response"]} if row["response"] is not None else None})
                for row in self.rows)
            return Response(raw=data)
        if method in ("GET", "POST"):
            return Response(self.remote())
        raise AssertionError("Unexpected external operation")


class SummaryRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.counter = 0
        self.network_guard = patch.object(client, "_transport", side_effect=AssertionError("Real network forbidden"))
        self.network_guard.start()
        self.addCleanup(self.network_guard.stop)

    def file(self, value):
        self.counter += 1
        path = self.root / f"input-{self.counter}.json"
        path.write_bytes(runner.canonical(value))
        path.chmod(0o600)
        return runner.binding(path)

    def source(self, recording="rec-1", text="The speaker reports a walk.", date="2026-08-20"):
        doc = {"kind": "himr_third_party_transcript_import", "schema_version": 1,
            "recording_id": recording, "status": "completed",
            "provenance": {"label": "Synthetic third-party source", "source_url": None,
                           "attribution": None, "rights_note": "No publication approval"},
            "segments": [] if text is None else [{"start_ms": 1000, "end_ms": 3000,
                                                  "text": text, "speaker": None}]}
        spec = {"transcript": self.file(doc), "format": "third_party", "recording_id": recording,
                "title": "Untrusted source title", "date": None, "completion": None}
        if date is not None:
            evidence = self.file({"kind": "himr_summary_date_evidence", "schema_version": 1,
                "recording_id": recording, "value": date, "date_kind": "published",
                "basis": "operator_supplied_metadata"})
            spec["date"] = {"value": date, "kind": "published", "evidence": evidence}
        return spec

    def plan(self, selected=None, *, approved=True, budget=None, config=None, limits=None):
        self.counter += 1
        req = {"kind": "himr_transcript_summary_request", "schema_version": 1,
            "state_root": str(self.root / f"workspace-{self.counter}"),
            "sources": [self.source()] if selected is None else selected,
            "config": copy.deepcopy(core.DEFAULT_CONFIG) if config is None else config,
            "limits": {**runner.DEFAULT_LIMITS, **(limits or {})},
            "budget": {**runner.DEFAULT_BUDGET, **(budget or {})},
            "cloud": {"processing_approved": approved, "paid_tier_confirmed": approved}}
        ref = self.file(req)
        self.creation = runner.create_plan(ref["path"], ref["sha256"])
        self.ref = self.creation["plan"]
        self.args = (self.ref["path"], self.ref["sha256"])
        self.request = req
        return self.creation

    def prepare(self):
        prepared = runner.prepare_plan(*self.args)
        self.assertIn(prepared["state"], ("prepared", "already_prepared"))
        folder = Path(self.request["state_root"]) / "waves" / prepared["wave_id"]
        wave = runner.read(runner.binding(folder / "wave.json"))
        service = Service(wave, self.creation["plan_id"])
        return wave, service, folder

    def submit(self, wave, service):
        return runner.submit_wave(*self.args, wave["wave_id"], allow_paid_api=True, client=service.api)

    def finish(self, wave, service):
        self.submit(wave, service)
        service.state = "completed"
        return runner.poll_wave(*self.args, wave["wave_id"], client=service.api)

    def test_offline_plan_prepare_status_no_credentials_or_calls(self):
        with patch.dict(os.environ, {}, clear=True):
            creation = self.plan(approved=False)
            self.assertEqual(creation["network_calls"], 0)
            self.assertFalse(creation["cloud_enabled"])
            self.assertEqual(runner.status_plan(*self.args)["state"], "ready")
            wave, service, folder = self.prepare()
            self.assertEqual(runner.status_plan(*self.args)["state"], "prepared")
            self.assertEqual(service.calls, [])
            self.assertNotIn(str(self.root).encode(), (folder / "requests.bin").read_bytes())
            self.assertEqual(runner.prepare_plan(*self.args)["wave_id"], wave["wave_id"])

    def test_end_to_end_gemini_chunks_transcript_then_openai_timeline(self):
        self.plan()
        wave, service, _ = self.prepare()
        self.assertEqual(wave["jobs"][0]["stage"], "chunk")
        self.assertEqual(self.submit(wave, service)["state"], "submitted")
        self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"], client=service.api)["state"], "remote_pending")
        service.state = "completed"
        self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"], client=service.api)["completed"], 1)
        self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"], client=service.api)["state"], "already_collected")
        transcript, transcript_api, _ = self.prepare()
        self.assertEqual(transcript["jobs"][0]["stage"], "transcript")
        self.assertEqual(transcript["provider"], "gemini")
        self.assertEqual(self.finish(transcript, transcript_api)["completed"], 1)
        timeline, timeline_api, _ = self.prepare()
        self.assertEqual(timeline["provider"], "openai")
        self.assertEqual(timeline["jobs"][0]["scope"]["period"], "2026-08")
        self.assertEqual(self.finish(timeline, timeline_api)["completed"], 1)
        status = runner.status_plan(*self.args)
        self.assertEqual(status["state"], "completed")
        self.assertEqual(status["completed_jobs"], 3)
        self.assertEqual(status["transcript_summaries_complete"], 1)
        self.assertEqual(status["timeline_summaries_complete"], 1)
        self.assertEqual(runner.prepare_plan(*self.args)["state"], "no_ready_jobs")
        exported = runner.export_plan(*self.args)
        document = runner.read(exported["artifact"])
        self.assertTrue(document["complete"])
        self.assertEqual(len(document["results"]), 2)
        self.assertFalse(document["semantics"]["publication_authority"])
        for result in document["results"]:
            self.assertEqual(result["sections"]["summary"][0]["citations"][0]["date"]["kind"], "published")

    def test_cloud_approval_and_explicit_paid_flag_are_both_required(self):
        self.plan(approved=False)
        wave, service, folder = self.prepare()
        with self.assertRaisesRegex(runner.Error, "paid submission requires"):
            self.submit(wave, service)
        self.assertFalse((folder / "submit-intent.json").exists())
        self.assertEqual(service.calls, [])
        self.plan()
        wave, service, folder = self.prepare()
        with self.assertRaisesRegex(runner.Error, "paid submission requires"):
            runner.submit_wave(*self.args, wave["wave_id"], client=service.api)
        self.assertEqual(service.calls, [])

    def test_budget_gate_precedes_intent_and_network(self):
        self.plan(budget={"max_reserved_microusd": 1})
        wave, service, folder = self.prepare()
        with self.assertRaisesRegex(runner.Error, "budget"):
            self.submit(wave, service)
        self.assertFalse((folder / "submit-intent.json").exists())
        self.assertEqual(service.calls, [])

    def test_ambiguous_submission_is_reserved_and_never_reposted(self):
        self.plan()
        wave, service, _ = self.prepare()
        service.fail_post = True
        with self.assertRaises(client.BatchClientError):
            self.submit(wave, service)
        call_count = len(service.calls)
        result = self.submit(wave, service)
        self.assertEqual(result["state"], "needs_reconciliation")
        self.assertFalse(result["automatic_resubmission"])
        self.assertEqual(len(service.calls), call_count)
        status = runner.status_plan(*self.args)
        self.assertEqual(status["state"], "needs_reconciliation")
        self.assertEqual(status["reserved_microusd"], wave["maximum_cost_microusd"])

    def test_reconcile_adopts_exact_remote_wave_without_reposting(self):
        self.plan()
        wave, service, _ = self.prepare()
        service.fail_post = True
        with self.assertRaises(client.BatchClientError):
            self.submit(wave, service)
        service.fail_post = False
        result = runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertEqual(result["state"], "reconciled")
        self.assertEqual(self.submit(wave, service)["state"], "already_submitted")
        self.assertEqual(sum(method == "POST" for method, _, _ in service.calls), 1)
        service.state = "completed"
        self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"], client=service.api)["completed"], 1)

    def test_reconcile_rejects_foreign_wave_identity(self):
        self.plan()
        wave, service, _ = self.prepare()
        service.fail_post = True
        with self.assertRaises(client.BatchClientError):
            self.submit(wave, service)
        service.fail_post = False
        metadata = service.remote()["metadata"]
        metadata["displayName"] = "summarywave_" + "0" * 32
        service.remote_override = {"metadata": metadata}
        with self.assertRaisesRegex(runner.Error, "wave identity"):
            runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)

    def test_openai_reconcile_requires_the_exact_uploaded_input(self):
        self.plan(config={**core.DEFAULT_CONFIG, "transcript_profile": "openai_mini_batch"})
        wave, service, folder = self.prepare()
        service.fail_post_suffix = "/batches"
        with self.assertRaises(client.BatchClientError):
            self.submit(wave, service)
        self.assertTrue((folder / "uploaded-input.json").exists())
        service.fail_post_suffix = None
        service.remote_override = {"input_file_id": "file-other"}
        with self.assertRaisesRegex(runner.Error, "uploaded input"):
            runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        service.remote_override = {}
        self.assertEqual(runner.reconcile_wave(*self.args, wave["wave_id"], service.name,
                                              client=service.api)["state"], "reconciled")
        self.assertEqual(sum(method == "POST" for method, _, _ in service.calls), 2)
        service.state = "completed"
        self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"], client=service.api)["completed"], 1)

    def test_partial_initial_waves_do_not_lose_unprepared_sources(self):
        self.plan([self.source("rec-a"), self.source("rec-b")], limits={"max_jobs_per_wave": 1})
        first, first_api, _ = self.prepare()
        self.submit(first, first_api)
        second, second_api, _ = self.prepare()
        self.assertNotEqual(first["jobs"][0]["job_id"], second["jobs"][0]["job_id"])
        self.assertEqual(second["jobs"][0]["stage"], "chunk")
        self.finish(second, second_api)
        first_api.state = "completed"
        runner.poll_wave(*self.args, first["wave_id"], client=first_api.api)
        status = runner.status_plan(*self.args)
        self.assertEqual(status["completed_jobs"], 2)
        self.assertEqual(status["ready_jobs"], 2)

    def test_gemini_metadata_output_wrapper_supported(self):
        self.plan()
        wave, service, _ = self.prepare()
        self.submit(wave, service)
        service.state = "completed"
        remote = service.remote()
        metadata = {**remote["metadata"], "output": remote["response"]}
        # The protocol also exposes completed inline rows in metadata.output.
        service.remote_override = {"metadata": metadata, "response": {}}
        result = runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
        self.assertEqual(result["completed"], 1)

    def test_gemini_conflicting_output_wrappers_are_rejected(self):
        self.plan()
        wave, service, folder = self.prepare()
        self.submit(wave, service)
        service.state = "completed"
        remote = service.remote()
        metadata = {**remote["metadata"], "output": {"inlinedResponses": {"inlinedResponses": []}}}
        service.remote_override = {"metadata": metadata}
        with self.assertRaises(runner.Error):
            runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
        self.assertFalse((folder / "collection.json").exists())

    def test_gemini_success_state_requires_done_without_operation_error(self):
        self.plan()
        wave, service, _ = self.prepare()
        remote = service.remote()
        remote["metadata"]["state"] = "BATCH_STATE_SUCCEEDED"
        remote["done"] = False
        with self.assertRaises(runner.Error):
            runner.check_remote(wave, remote)
        remote["done"] = True
        remote["error"] = {"code": 13}
        self.assertEqual(runner.check_remote(wave, remote)["status"], "failed")

    def test_successful_submission_is_idempotent(self):
        self.plan()
        wave, service, _ = self.prepare()
        self.submit(wave, service)
        count = len(service.calls)
        self.assertEqual(self.submit(wave, service)["state"], "already_submitted")
        self.assertEqual(len(service.calls), count)

    def test_outputs_are_collected_by_id_not_position(self):
        self.plan([self.source("rec-a"), self.source("rec-b", text="The speaker reports visiting a library.")])
        wave, service, _ = self.prepare()
        service.rows.reverse()
        self.assertEqual(self.finish(wave, service)["completed"], 2)
        plan, selected = runner.load_plan(*self.args)
        state = runner.load_state(plan, selected)
        self.assertEqual([r["job_id"] for r in state["results"]], [j["job_id"] for j in wave["jobs"]])

    def test_unknown_and_duplicate_output_ids_reject_whole_capture(self):
        for mode in ("unknown", "duplicate"):
            with self.subTest(mode=mode):
                self.plan()
                wave, service, folder = self.prepare()
                if mode == "unknown":
                    service.rows[0]["custom_id"] = "summaryjob_" + "f" * 32
                else:
                    service.rows.append(copy.deepcopy(service.rows[0]))
                self.submit(wave, service)
                service.state = "completed"
                with self.assertRaisesRegex(runner.Error, "unknown or duplicate"):
                    runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
                self.assertFalse((folder / "capture.json").exists())
                self.assertFalse((folder / "collection.json").exists())

    def test_refusal_and_foreign_citations_are_review_not_success(self):
        for mode in ("refusal", "foreign_citation", "truncated"):
            with self.subTest(mode=mode):
                self.plan()
                wave, service, _ = self.prepare()
                if mode == "foreign_citation":
                    value = payload(wave["jobs"][0])
                    value["summary"][0]["evidence_ids"] = ["summaryevidence_" + "f" * 32]
                    service.rows[0]["response"] = response_body("gemini", value)
                elif mode == "refusal":
                    service.rows[0]["response"] = {"promptFeedback": {"blockReason": "SAFETY"}, "candidates": []}
                else:
                    service.rows[0]["response"]["candidates"][0]["finishReason"] = "MAX_TOKENS"
                result = self.finish(wave, service)
                self.assertEqual(result["completed"], 0)
                self.assertEqual(result["needs_review"], 1)
                self.assertEqual(runner.status_plan(*self.args)["state"], "needs_review")
                self.assertEqual(runner.prepare_plan(*self.args)["state"], "no_ready_jobs")

    def test_explicit_retry_includes_only_failed_jobs_and_preserves_success(self):
        self.plan([self.source("rec-a"), self.source("rec-b")])
        wave, service, folder = self.prepare()
        service.rows[1].update(response=None, error={"code": 13})
        result = self.finish(wave, service)
        self.assertEqual((result["completed"], result["needs_review"]), (1, 1))
        before = (folder / "collection.json").read_bytes()
        retry = runner.prepare_plan(*self.args, retry_wave=wave["wave_id"])
        retry_folder = Path(self.request["state_root"]) / "waves" / retry["wave_id"]
        retry_wave = runner.read(runner.binding(retry_folder / "wave.json"))
        self.assertEqual([j["job_id"] for j in retry_wave["jobs"]], [wave["jobs"][1]["job_id"]])
        retry_service = Service(retry_wave, self.creation["plan_id"])
        self.assertEqual(self.finish(retry_wave, retry_service)["completed"], 1)
        self.assertEqual((folder / "collection.json").read_bytes(), before)
        status = runner.status_plan(*self.args)
        self.assertEqual(status["completed_jobs"], 2)
        self.assertEqual(status["failed_jobs"], 0)
        self.assertEqual(status["reserved_microusd"], wave["maximum_cost_microusd"] + retry_wave["maximum_cost_microusd"])

    def test_retry_attempt_ceiling_is_enforced(self):
        self.plan(budget={"max_attempts_per_job": 1})
        wave, service, _ = self.prepare()
        service.rows[0].update(response=None, error={"code": 13})
        self.finish(wave, service)
        with self.assertRaisesRegex(runner.Error, "attempt"):
            runner.prepare_plan(*self.args, retry_wave=wave["wave_id"])

    def test_failed_terminal_batch_preserves_missing_results_for_review(self):
        self.plan()
        wave, service, _ = self.prepare()
        self.submit(wave, service)
        service.state = "failed"
        result = runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
        self.assertEqual(result["needs_review"], 1)
        self.assertEqual(runner.status_plan(*self.args)["failed_jobs"], 1)

    def test_source_drift_prevents_submission_before_network(self):
        self.plan()
        wave, service, _ = self.prepare()
        Path(self.request["sources"][0]["transcript"]["path"]).write_text('{"modified":true}')
        with self.assertRaisesRegex(sources.SourceError, "SHA-256"):
            self.submit(wave, service)
        self.assertEqual(service.calls, [])

    def test_implementation_drift_is_explicit(self):
        self.plan()
        with patch.object(runner, "implementation", return_value={"modified.py": "a" * 64}):
            with self.assertRaisesRegex(runner.Error, "implementation changed"):
                runner.status_plan(*self.args)

    def test_immutable_batch_input_tamper_blocks_status(self):
        self.plan()
        _, _, folder = self.prepare()
        path = folder / "requests.bin"
        path.chmod(0o600)
        path.write_bytes(b"tampered")
        with self.assertRaisesRegex(runner.Error, "digest differs"):
            runner.status_plan(*self.args)

    def test_provider_capture_tamper_blocks_result_replay(self):
        self.plan()
        wave, service, folder = self.prepare()
        self.finish(wave, service)
        path = folder / "capture.json"
        path.chmod(0o600)
        path.write_bytes(b'{"tampered":true}\n')
        with self.assertRaisesRegex(runner.Error, "digest differs"):
            runner.status_plan(*self.args)

    def test_submission_receipt_tamper_blocks_status(self):
        self.plan()
        wave, service, folder = self.prepare()
        self.submit(wave, service)
        path = folder / "submission-response.json"
        path.chmod(0o600)
        path.write_bytes(b'{"tampered":true}\n')
        with self.assertRaises(runner.Error):
            runner.status_plan(*self.args)

    def test_submitted_remote_id_must_match_bound_response(self):
        self.plan()
        wave, service, folder = self.prepare()
        self.submit(wave, service)
        path = folder / "submitted.json"
        receipt = runner.read(runner.binding(path))
        receipt["remote_id"] = "batches/foreign"
        path.chmod(0o600)
        path.write_bytes(runner.canonical(receipt))
        with self.assertRaises(runner.Error):
            runner.status_plan(*self.args)

    def test_private_export_before_completion_is_explicitly_incomplete(self):
        self.plan()
        exported = runner.export_plan(*self.args)
        doc = runner.read(exported["artifact"])
        self.assertFalse(exported["complete"])
        self.assertFalse(doc["complete"])
        self.assertEqual(doc["results"], [])
        path = Path(exported["artifact"]["path"])
        self.assertTrue(path.is_relative_to(self.request["state_root"]))
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o400)
        self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
        self.assertEqual(runner.export_plan(*self.args)["artifact"], exported["artifact"])

    def test_empty_selection_and_empty_transcript_never_need_api(self):
        for selected in ([], [self.source(text=None)], [self.source(text=" \n\t")]):
            with self.subTest(selected=bool(selected)):
                self.plan(selected)
                self.assertEqual(self.creation["initial_jobs"], 0)
                self.assertEqual(runner.status_plan(*self.args)["state"], "completed")
                self.assertEqual(runner.prepare_plan(*self.args)["state"], "no_ready_jobs")
                self.assertTrue(runner.export_plan(*self.args)["complete"])
                if selected:
                    self.assertEqual(runner.status_plan(*self.args)["empty_transcripts"], 1)

    def test_reject_duplicate_recording_revisions(self):
        with self.assertRaisesRegex(runner.Error, "one transcript revision"):
            self.plan([self.source("rec-a"), self.source("rec-a", text="An alternate source.")])

    def test_reject_nonempty_unmarked_workspace(self):
        self.plan()
        root = Path(self.request["state_root"])
        marker = root / "workspace.json"
        marker.chmod(0o600)
        marker.unlink()
        # Replaying the original explicit request may not bless a populated tree.
        plan = runner.read(self.ref)
        with self.assertRaisesRegex(runner.Error, "nonempty unmarked"):
            runner.create_plan(plan["request"]["path"], plan["request"]["sha256"])

    def test_lock_prevents_concurrent_mutation(self):
        self.plan()
        with runner.locked(Path(self.request["state_root"])):
            with self.assertRaisesRegex(runner.Error, "another summary command"):
                runner.prepare_plan(*self.args)

    def test_cli_errors_do_not_echo_private_source_text(self):
        with patch("sys.stderr", new_callable=io.StringIO) as stderr:
            rc = runner.main(["status", "--manifest", str(self.root / "missing.json"),
                              "--expected-sha256", "a" * 64])
        self.assertEqual(rc, 2)
        self.assertIn("TranscriptSummaryError", stderr.getvalue())
        self.assertNotIn(str(self.root), stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
