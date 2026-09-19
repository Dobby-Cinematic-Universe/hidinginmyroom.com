"""Sonnet synthesis integration tests; every HTTP request stays in memory."""
from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_anthropic as anthropic
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_sources as sources
from pipeline.tests import test_transcript_summary as existing


MODEL = "claude-sonnet-5"
PROFILE = "anthropic_sonnet_batch"


def message(job):
    return {"id": "msg_synthetic", "type": "message", "role": "assistant",
            "model": MODEL, "content": [{"type": "text", "text": json.dumps(existing.payload(job))}],
            "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 100, "output_tokens": 100}}


class AnthropicService:
    """Exercise the real transport client with an exact synthetic HTTP boundary."""
    def __init__(self, wave):
        self.wave = wave
        self.name = "msgbatch_" + wave["wave_id"][-24:]
        self.state = "pending"
        self.calls = []
        self.fail_post = False
        self.http_status = None
        self.remote_override = {}
        requests = runner.anthropic_requests(wave)
        self.rows = [{"custom_id": request["custom_id"],
                      "result": {"type": "succeeded", "message": message(job)}}
                     for request, job in zip(requests, wave["jobs"])]
        self.api = anthropic.AnthropicBatchClient("synthetic-noncredential", transport=self.transport)

    def remote(self):
        ended = self.state == "completed"
        failures = {kind: sum(row["result"]["type"] == kind for row in self.rows)
                    for kind in ("errored", "canceled", "expired")}
        size = len(self.wave["jobs"])
        result = {"id": self.name, "type": "message_batch",
                  "processing_status": "ended" if ended else "in_progress",
                  "request_counts": {"processing": 0 if ended else size,
                                     "succeeded": size - sum(failures.values()) if ended else 0,
                                     **{kind: count if ended else 0 for kind, count in failures.items()}},
                  "created_at": "2026-09-12T00:00:00Z",
                  "expires_at": "2026-09-13T00:00:00Z",
                  "ended_at": "2026-09-12T01:00:00Z" if ended else None,
                  "cancel_initiated_at": None,
                  "results_url": self.url + "/results" if ended else None}
        return {**result, **copy.deepcopy(self.remote_override)}

    @property
    def url(self):
        return "https://api.anthropic.com/v1/messages/batches/" + self.name

    def transport(self, request, timeout):
        method, url = request.get_method(), request.full_url
        self.calls.append((method, url, request.data))
        if method == "POST" and url == "https://api.anthropic.com/v1/messages/batches":
            if self.http_status is not None:
                raise urllib.error.HTTPError(url, self.http_status, "Synthetic private provider detail",
                    {"Retry-After": "1"}, io.BytesIO(b"synthetic-secret-provider-response"))
            if self.fail_post:
                raise OSError("Synthetic lost POST response")
            if json.loads(request.data) != {"requests": runner.anthropic_requests(self.wave)}:
                raise AssertionError("Submitted body differs from the sealed wave")
            return existing.Response(self.remote())
        if method == "GET" and url == self.url:
            return existing.Response(self.remote())
        if method == "GET" and url == self.url + "/results":
            return existing.Response(raw=b"".join(runner.canonical(row) for row in self.rows))
        raise AssertionError("Unexpected HTTP operation: " + method + " " + url)


class UnavailableAnthropic:
    """A local proof must work after provider results or credentials disappear."""
    def get_batch(self, batch_id):
        raise AssertionError("Remote batch must not be requested")

    def download_results(self, batch_id):
        raise AssertionError("Remote results must not be requested")


class SonnetRunnerTests(unittest.TestCase):
    file = existing.SummaryRunnerTests.file
    source = existing.SummaryRunnerTests.source
    submit = existing.SummaryRunnerTests.submit
    finish = existing.SummaryRunnerTests.finish

    def setUp(self):
        existing.SummaryRunnerTests.setUp(self)
        guard = patch("urllib.request.urlopen", side_effect=AssertionError("Real network forbidden"))
        guard.start()
        self.addCleanup(guard.stop)

    def plan(self, selected=None, *, approved=True, budget=None, config=None, broader=True, limits=None):
        self.counter += 1
        settings = {**core.DEFAULT_CONFIG, "timeline_profile": PROFILE, **(config or {})}
        if broader:
            settings["broader_synthesis"] = {"profile": PROFILE, "yearly": True,
                "archive": True, "topics": [{"id": "walks", "title": "Walks",
                                             "recording_ids": ["rec-1"]}]}
        req = {"kind": "himr_transcript_summary_request", "schema_version": 1,
               "state_root": str(self.root / f"sonnet-workspace-{self.counter}"),
               "sources": [self.source()] if selected is None else selected,
               "config": settings, "limits": {**runner.DEFAULT_LIMITS, **(limits or {})},
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
        service = (AnthropicService(wave) if wave["provider"] == "anthropic"
                   else existing.Service(wave, self.creation["plan_id"]))
        return wave, service, folder

    def sonnet_plan(self, selected=None, **kwargs):
        return self.plan(selected, config={"transcript_profile": PROFILE}, broader=False, **kwargs)

    def test_gemini_transcripts_then_sonnet_month_year_archive_and_topic(self):
        selected = [self.source("rec-1", date="2025-12-20"),
                    self.source("rec-2", date="2026-08-20"),
                    self.source("rec-undated", date=None)]
        self.plan(selected)
        completed = []
        for _ in range(20):
            if runner.status_plan(*self.args)["state"] == "completed":
                break
            wave, service, _ = self.prepare()
            for job in wave["jobs"]:
                self.assertEqual(job["provider"], "gemini" if job["stage"] in
                                 ("chunk", "transcript") else "anthropic")
            completed.extend(wave["jobs"])
            self.assertEqual(self.finish(wave, service)["completed"], len(wave["jobs"]))
        status = runner.status_plan(*self.args)
        self.assertEqual(status["state"], "completed")
        self.assertEqual(status["transcript_summaries_complete"], 3)
        self.assertEqual(status["timeline_summaries_complete"], 3)
        self.assertEqual(status["yearly_summaries_complete"], 2)
        self.assertEqual(status["archive_summaries_complete"], 1)
        self.assertEqual(status["topic_summaries_complete"], 1)
        exported = runner.read(runner.export_plan(*self.args)["artifact"])
        self.assertTrue(exported["complete"])
        final = exported["results"]
        self.assertEqual({result["scope"]["period"] for result in final
                          if result["stage"] == "yearly"}, {"2025", "2026"})
        archive = next(result for result in final if result["stage"] == "archive")
        plan, normalized = runner.load_plan(*self.args)
        all_ids = {source["source_id"] for source in normalized}
        self.assertEqual(set(archive["scope"]["source_ids"]), all_ids)
        self.assertEqual({citation["source_id"] for item in archive["sections"]["summary"]
                          for citation in item["citations"]}, all_ids)
        undated = next(source["source_id"] for source in normalized
                       if source["recording_id"] == "rec-undated")
        for result in final:
            self.assertFalse(result["semantics"]["publication_authority"])
            if result["stage"] == "yearly":
                self.assertNotIn(undated, result["scope"]["source_ids"])
        topic = next(result for result in final if result["stage"] == "topic")
        self.assertEqual(topic["scope"]["period"], "walks")
        self.assertEqual(topic["scope"]["source_ids"], [next(source["source_id"]
                         for source in normalized if source["recording_id"] == "rec-1")])

    def test_offline_preparation_and_wave_specific_wire_ids(self):
        with patch.dict(os.environ, {}, clear=True):
            self.sonnet_plan(approved=False)
            wave, service, folder = self.prepare()
            self.assertEqual(service.calls, [])
            requests = runner.anthropic_requests(wave)
            for job, request in zip(wave["jobs"], requests):
                expected = "summaryreq_" + runner.digest({"wave_id": wave["wave_id"],
                                                          "job_id": job["job_id"]})[:32]
                self.assertEqual(request["custom_id"], expected)
                self.assertNotEqual(request["custom_id"], job["job_id"])
                self.assertEqual(request["params"]["model"], MODEL)
            self.assertEqual(json.loads((folder / "provider-requests.bin").read_bytes()),
                             {"requests": requests})
            self.assertNotIn(str(self.root).encode(), (folder / "provider-requests.bin").read_bytes())

    def test_partial_results_retry_preserves_success_and_changes_attempt_ids(self):
        self.sonnet_plan([self.source("rec-1"), self.source("rec-2")])
        wave, service, folder = self.prepare()
        old_ids = {row["custom_id"] for row in service.rows}
        service.rows[1]["result"] = {"type": "errored", "error": {
            "type": "error", "error": {"type": "api_error", "message": "Synthetic failure"}}}
        result = self.finish(wave, service)
        self.assertEqual((result["completed"], result["needs_review"]), (1, 1))
        before = (folder / "collection.json").read_bytes()
        retry = runner.prepare_plan(*self.args, retry_wave=wave["wave_id"])
        retry_folder = Path(self.request["state_root"]) / "waves" / retry["wave_id"]
        retry_wave = runner.read(runner.binding(retry_folder / "wave.json"))
        self.assertEqual([job["job_id"] for job in retry_wave["jobs"]], [wave["jobs"][1]["job_id"]])
        retry_service = AnthropicService(retry_wave)
        self.assertTrue(old_ids.isdisjoint(row["custom_id"] for row in retry_service.rows))
        self.assertEqual(self.finish(retry_wave, retry_service)["completed"], 1)
        self.assertEqual((folder / "collection.json").read_bytes(), before)
        status = runner.status_plan(*self.args)
        self.assertEqual(status["completed_jobs"], 2)
        self.assertEqual(status["failed_jobs"], 0)
        self.assertEqual(status["reserved_microusd"], wave["maximum_cost_microusd"] +
                         retry_wave["maximum_cost_microusd"])

    def test_missing_result_becomes_review_and_keeps_successful_result(self):
        self.sonnet_plan([self.source("rec-1"), self.source("rec-2")])
        wave, service, _ = self.prepare()
        service.rows.pop()
        result = self.finish(wave, service)
        self.assertEqual((result["completed"], result["needs_review"]), (1, 1))

    def test_ambiguous_post_waits_for_ended_exact_identity_and_never_reposts(self):
        self.sonnet_plan()
        wave, service, folder = self.prepare()
        service.fail_post = True
        with self.assertRaises(anthropic.AnthropicClientError):
            self.submit(wave, service)
        service.fail_post = False
        self.assertEqual(self.submit(wave, service)["state"], "needs_reconciliation")
        pending = runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertEqual(pending["state"], "needs_reconciliation")
        self.assertFalse((folder / "submitted.json").exists())
        service.state = "completed"
        original = service.rows[0]["custom_id"]
        service.rows[0]["custom_id"] = "summaryreq_" + "f" * 32
        with self.assertRaises(runner.Error):
            runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertFalse((folder / "submitted.json").exists())
        service.rows[0]["custom_id"] = original
        rows = service.rows
        service.rows = []
        with self.assertRaises(runner.Error):
            runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertFalse((folder / "submitted.json").exists())
        service.rows = rows
        self.assertEqual(runner.reconcile_wave(*self.args, wave["wave_id"], service.name,
                         client=service.api)["state"], "reconciled")
        self.assertTrue((folder / "reconciliation-results.json").exists())
        self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"], client=service.api)["completed"], 1)
        self.assertEqual(sum(method == "POST" for method, _, _ in service.calls), 1)

    def test_reconciliation_rejects_old_retry_ids_and_foreign_model(self):
        self.sonnet_plan()
        first, first_service, _ = self.prepare()
        original_row = copy.deepcopy(first_service.rows[0])
        first_service.rows[0]["result"] = {"type": "expired"}
        self.finish(first, first_service)
        retry = runner.prepare_plan(*self.args, retry_wave=first["wave_id"])
        folder = Path(self.request["state_root"]) / "waves" / retry["wave_id"]
        wave = runner.read(runner.binding(folder / "wave.json"))
        service = AnthropicService(wave)
        expected_row = copy.deepcopy(service.rows[0])
        service.fail_post = True
        with self.assertRaises(anthropic.AnthropicClientError):
            self.submit(wave, service)
        service.fail_post = False
        service.state = "completed"
        service.rows = [original_row]
        with self.assertRaises(runner.Error):
            runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        service.rows = [expected_row]
        service.rows[0]["result"]["message"]["model"] = "claude-unrelated"
        with self.assertRaises(runner.Error):
            runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertFalse((folder / "submitted.json").exists())

    def test_reconciled_results_collect_offline_without_provider_or_credentials(self):
        for supply_client in (False, True):
            with self.subTest(supply_client=supply_client):
                self.sonnet_plan()
                wave, service, folder = self.prepare()
                service.fail_post = True
                with self.assertRaises(anthropic.AnthropicClientError):
                    self.submit(wave, service)
                service.fail_post = False
                service.state = "completed"
                self.assertEqual(runner.reconcile_wave(*self.args, wave["wave_id"], service.name,
                                 client=service.api)["state"], "reconciled")
                proof = (folder / "reconciliation-results.json").read_bytes()
                remote_calls = len(service.calls)
                with patch.dict(os.environ, {}, clear=True), patch.object(
                        runner, "api_client", side_effect=AssertionError("No provider client or key needed")):
                    kwargs = {"client": UnavailableAnthropic()} if supply_client else {}
                    result = runner.poll_wave(*self.args, wave["wave_id"], **kwargs)
                self.assertEqual((result["completed"], result["needs_review"]), (1, 0))
                self.assertEqual((folder / "capture.json").read_bytes(), proof)
                self.assertEqual(len(service.calls), remote_calls)

    def test_reconcile_crash_after_saved_proof_resumes_offline_and_rejects_wrong_id(self):
        self.sonnet_plan()
        wave, service, folder = self.prepare()
        service.fail_post = True
        with self.assertRaises(anthropic.AnthropicClientError):
            self.submit(wave, service)
        service.fail_post = False
        service.state = "completed"
        original_put = runner.put
        interrupted = False

        def crash_before_receipt(path, value):
            nonlocal interrupted
            if Path(path).name == "reconciliation-response.json" and not interrupted:
                interrupted = True
                raise OSError("Synthetic crash after complete result proof")
            return original_put(path, value)

        with patch.object(runner, "put", side_effect=crash_before_receipt):
            with self.assertRaisesRegex(OSError, "Synthetic crash"):
                runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertTrue(interrupted)
        proof = (folder / "reconciliation-results.json").read_bytes()
        self.assertFalse((folder / "reconciliation-response.json").exists())
        self.assertFalse((folder / "submitted.json").exists())
        remote_calls = len(service.calls)
        with patch.dict(os.environ, {}, clear=True), patch.object(
                runner, "api_client", side_effect=AssertionError("No provider client or key needed")):
            with self.assertRaises((runner.Error, anthropic.AnthropicClientError)):
                runner.reconcile_wave(*self.args, wave["wave_id"], "msgbatch_wrong",
                                      client=UnavailableAnthropic())
            self.assertFalse((folder / "submitted.json").exists())
            self.assertEqual(runner.reconcile_wave(*self.args, wave["wave_id"], service.name)["state"],
                             "reconciled")
            result = runner.poll_wave(*self.args, wave["wave_id"], client=UnavailableAnthropic())
        self.assertEqual((result["completed"], result["needs_review"]), (1, 0))
        self.assertEqual((folder / "capture.json").read_bytes(), proof)
        self.assertEqual((folder / "reconciliation-results.json").read_bytes(), proof)
        self.assertEqual(len(service.calls), remote_calls)

    def test_oversized_proposal_splits_wave_without_discarding_fitting_requests(self):
        selected = [self.source("rec-1", text="The speaker reports a walk. " * 700),
                    self.source("rec-2", text="The speaker reports a walk. " * 700)]
        limit = 32768
        with patch.object(anthropic, "MAX_ANTHROPIC_BATCH_BYTES", limit):
            self.sonnet_plan(selected, limits={"max_wave_bytes": limit})
            first, service, folder = self.prepare()
            self.assertEqual(len(first["jobs"]), 1)
            self.assertLessEqual((folder / "provider-requests.bin").stat().st_size, limit)
            self.submit(first, service)
            second, _, second_folder = self.prepare()
            self.assertEqual(len(second["jobs"]), 1)
            self.assertNotEqual(first["jobs"][0]["job_id"], second["jobs"][0]["job_id"])
            self.assertLessEqual((second_folder / "provider-requests.bin").stat().st_size, limit)
            combined = {**first, "jobs": first["jobs"] + second["jobs"]}
            requests = runner.anthropic_requests(combined)
            self.assertGreater(anthropic.anthropic_batch_size(requests), limit)
            with self.assertRaises(anthropic.AnthropicClientError):
                anthropic.anthropic_batch_bytes(requests)

    def test_unknown_duplicate_and_naked_job_ids_reject_capture(self):
        for mode in ("unknown", "duplicate", "naked_job_id"):
            with self.subTest(mode=mode):
                self.sonnet_plan()
                wave, service, folder = self.prepare()
                if mode == "duplicate":
                    service.rows.append(copy.deepcopy(service.rows[0]))
                else:
                    service.rows[0]["custom_id"] = (wave["jobs"][0]["job_id"] if mode == "naked_job_id"
                                                       else "summaryreq_" + "f" * 32)
                self.submit(wave, service)
                service.state = "completed"
                with self.assertRaises(runner.Error):
                    runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
                self.assertFalse((folder / "capture.json").exists())

    def test_thinking_and_redacted_thinking_are_not_interpreted_as_output(self):
        self.sonnet_plan()
        wave, service, _ = self.prepare()
        content = service.rows[0]["result"]["message"]["content"]
        content[:0] = [{"type": "thinking", "thinking": "Synthetic thought", "signature": "test"},
                      {"type": "redacted_thinking", "data": "synthetic"}]
        self.assertEqual(self.finish(wave, service)["completed"], 1)

    def test_refusals_truncation_tools_foreign_models_and_invalid_json_need_review(self):
        for mode in ("refusal", "truncated", "tool", "foreign_model", "invalid_json",
                     "multiple_text", "foreign_citation", "wrong_role"):
            with self.subTest(mode=mode):
                self.sonnet_plan()
                wave, service, _ = self.prepare()
                body = service.rows[0]["result"]["message"]
                if mode == "refusal":
                    body["stop_reason"] = "refusal"
                elif mode == "truncated":
                    body["stop_reason"] = "max_tokens"
                elif mode == "tool":
                    body["content"].append({"type": "tool_use", "id": "tool_1", "name": "lookup", "input": {}})
                elif mode == "foreign_model":
                    body["model"] = "claude-unrelated"
                elif mode == "invalid_json":
                    body["content"][0]["text"] = "This is not JSON."
                elif mode == "multiple_text":
                    body["content"].append(copy.deepcopy(body["content"][0]))
                elif mode == "foreign_citation":
                    value = existing.payload(wave["jobs"][0])
                    value["summary"][0]["evidence_ids"] = ["summaryevidence_" + "f" * 32]
                    body["content"][0]["text"] = json.dumps(value)
                else:
                    body["role"] = "user"
                result = self.finish(wave, service)
                self.assertEqual((result["completed"], result["needs_review"]), (0, 1))

    def test_consent_explicit_flag_and_budget_gate_precede_anthropic_http(self):
        for mode in ("consent", "flag", "budget"):
            with self.subTest(mode=mode):
                self.sonnet_plan(approved=mode != "consent",
                                 budget={"max_reserved_microusd": 1} if mode == "budget" else None)
                wave, service, folder = self.prepare()
                with self.assertRaises(runner.Error):
                    runner.submit_wave(*self.args, wave["wave_id"], allow_paid_api=mode != "flag",
                                       client=service.api)
                self.assertEqual(service.calls, [])
                self.assertFalse((folder / "submit-intent.json").exists())

    def test_batch_reservation_includes_full_thinking_output_budget(self):
        self.sonnet_plan()
        wave, _, _ = self.prepare()
        for job in wave["jobs"]:
            budget = job["budget"]
            self.assertEqual(budget["output_token_allowance"], job["request"]["body"]["max_tokens"])
            self.assertEqual(budget["maximum_cost_microusd"], budget["input_token_allowance"] +
                             5 * budget["output_token_allowance"])

    def test_invalid_create_count_leaves_ambiguous_intent_without_repost(self):
        self.sonnet_plan()
        wave, service, folder = self.prepare()
        service.remote_override = {"request_counts": {"processing": 2, "succeeded": 0,
                                  "errored": 0, "canceled": 0, "expired": 0}}
        with self.assertRaises(anthropic.AnthropicClientError):
            self.submit(wave, service)
        self.assertTrue((folder / "submit-intent.json").exists())
        self.assertFalse((folder / "submitted.json").exists())
        self.assertEqual(self.submit(wave, service)["state"], "needs_reconciliation")
        self.assertEqual(sum(method == "POST" for method, _, _ in service.calls), 1)

    def test_confirmed_http_rejections_are_redacted_and_never_reposted(self):
        for status_code in (400, 401, 402, 403, 404, 413, 422, 429):
            with self.subTest(status_code=status_code):
                self.sonnet_plan()
                wave, service, folder = self.prepare()
                service.http_status = status_code
                result = self.submit(wave, service)
                self.assertEqual(result["state"], "submission_rejected")
                self.assertEqual(result["status_code"], status_code)
                self.assertFalse(result["automatic_resubmission"])
                proof = (folder / "submission-rejected.json").read_bytes()
                self.assertNotIn(b"synthetic-secret-provider-response", proof)
                self.assertNotIn("Synthetic private provider detail", json.dumps(result))
                self.assertFalse((folder / "submitted.json").exists())
                self.assertEqual(self.submit(wave, service), result)
                with self.assertRaisesRegex(runner.Error, "no remote batch"):
                    runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
                self.assertEqual(len(service.calls), 1)
                status = runner.status_plan(*self.args)
                self.assertEqual(status["state"], "needs_review")
                self.assertEqual(status["rejected_waves"], [wave["wave_id"]])
                self.assertEqual(status["reserved_microusd"], wave["maximum_cost_microusd"])

    def test_confirmed_rejection_retry_uses_new_wave_and_requires_paid_flag(self):
        self.sonnet_plan()
        first, service, folder = self.prepare()
        service.http_status = 401
        self.assertEqual(self.submit(first, service)["state"], "submission_rejected")
        proof = (folder / "submission-rejected.json").read_bytes()
        retry = runner.prepare_plan(*self.args, retry_wave=first["wave_id"])
        retry_folder = Path(self.request["state_root"]) / "waves" / retry["wave_id"]
        wave = runner.read(runner.binding(retry_folder / "wave.json"))
        self.assertNotEqual(wave["wave_id"], first["wave_id"])
        self.assertEqual(wave["retry_of"], first["wave_id"])
        self.assertEqual(wave["jobs"], first["jobs"])
        corrected = AnthropicService(wave)
        self.assertNotEqual(corrected.rows[0]["custom_id"], service.rows[0]["custom_id"])
        with self.assertRaisesRegex(runner.Error, "paid submission requires"):
            runner.submit_wave(*self.args, wave["wave_id"], client=corrected.api)
        self.assertEqual(corrected.calls, [])
        self.assertEqual(self.finish(wave, corrected)["completed"], 1)
        status = runner.status_plan(*self.args)
        self.assertEqual(status["completed_jobs"], 1)
        self.assertEqual(status["failed_jobs"], 0)
        self.assertEqual(status["reserved_microusd"], first["maximum_cost_microusd"] +
                         wave["maximum_cost_microusd"])
        self.assertEqual((folder / "submission-rejected.json").read_bytes(), proof)

    def test_confirmed_rejection_retry_cannot_bypass_budget_or_attempt_ceiling(self):
        for mode in ("budget", "attempt"):
            with self.subTest(mode=mode):
                selected = [self.source()]
                config = {**core.DEFAULT_CONFIG, "timeline_profile": PROFILE, "transcript_profile": PROFILE}
                amount = sum(job["budget"]["maximum_cost_microusd"] for job in
                             core.initial_jobs([sources.normalize_source(selected[0])], config))
                budget = ({"max_reserved_microusd": amount} if mode == "budget"
                          else {"max_attempts_per_job": 1})
                self.sonnet_plan(selected, budget=budget)
                first, service, _ = self.prepare()
                service.http_status = 429
                self.assertEqual(self.submit(first, service)["state"], "submission_rejected")
                if mode == "attempt":
                    with self.assertRaisesRegex(runner.Error, "attempt"):
                        runner.prepare_plan(*self.args, retry_wave=first["wave_id"])
                else:
                    retry = runner.prepare_plan(*self.args, retry_wave=first["wave_id"])
                    folder = Path(self.request["state_root"]) / "waves" / retry["wave_id"]
                    wave = runner.read(runner.binding(folder / "wave.json"))
                    corrected = AnthropicService(wave)
                    with self.assertRaisesRegex(runner.Error, "budget"):
                        self.submit(wave, corrected)
                    self.assertFalse((folder / "submit-intent.json").exists())
                    self.assertEqual(corrected.calls, [])
                self.assertEqual(runner.status_plan(*self.args)["reserved_microusd"], amount)
                self.assertEqual(len(service.calls), 1)

    def test_ambiguous_http_failures_remain_reconciliation_only(self):
        for status_code in (408, 409, 500, 529):
            with self.subTest(status_code=status_code):
                self.sonnet_plan()
                wave, service, folder = self.prepare()
                service.http_status = status_code
                with self.assertRaises(anthropic.AnthropicClientError) as caught:
                    self.submit(wave, service)
                self.assertNotIn("synthetic-secret-provider-response", str(caught.exception))
                self.assertNotIn("Synthetic private provider detail", str(caught.exception))
                self.assertFalse((folder / "submission-rejected.json").exists())
                self.assertEqual(self.submit(wave, service)["state"], "needs_reconciliation")
                status = runner.status_plan(*self.args)
                self.assertEqual(status["state"], "needs_reconciliation")
                self.assertEqual(status["rejected_waves"], [])
                with self.assertRaisesRegex(runner.Error, "terminal collected or definitively rejected"):
                    runner.prepare_plan(*self.args, retry_wave=wave["wave_id"])
                self.assertEqual(len(service.calls), 1)

    def test_confirmed_rejection_proof_must_match_wave_and_input(self):
        for field in ("wave_id", "input_sha256", "status_code"):
            with self.subTest(field=field):
                self.sonnet_plan()
                wave, service, folder = self.prepare()
                service.http_status = 403
                self.submit(wave, service)
                path = folder / "submission-rejected.json"
                proof = runner.read(runner.binding(path))
                proof[field] = {"wave_id": "summarywave_" + "f" * 32,
                                "input_sha256": "f" * 64, "status_code": 500}[field]
                path.chmod(0o600)
                path.write_bytes(runner.canonical(proof))
                with self.assertRaisesRegex(runner.Error, "rejection proof differs"):
                    runner.status_plan(*self.args)
                with self.assertRaisesRegex(runner.Error, "rejection proof differs"):
                    runner.prepare_plan(*self.args, retry_wave=wave["wave_id"])
                self.assertEqual(len(service.calls), 1)

    def test_malformed_remote_counts_do_not_create_a_collection(self):
        self.sonnet_plan()
        wave, service, folder = self.prepare()
        self.submit(wave, service)
        service.state = "completed"
        service.remote_override = {"request_counts": {"processing": 0, "succeeded": True,
                                  "errored": 0, "canceled": 0, "expired": 0}}
        with self.assertRaises((runner.Error, anthropic.AnthropicClientError)):
            runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
        self.assertFalse((folder / "collection.json").exists())

    def test_successful_rows_cannot_contradict_reported_error_counts(self):
        self.sonnet_plan()
        wave, service, folder = self.prepare()
        self.submit(wave, service)
        service.state = "completed"
        service.remote_override = {"request_counts": {"processing": 0, "succeeded": 0,
                                  "errored": 1, "canceled": 0, "expired": 0}}
        with self.assertRaisesRegex(runner.Error, "outcomes contradict"):
            runner.poll_wave(*self.args, wave["wave_id"], client=service.api)
        self.assertFalse((folder / "capture.json").exists())
        self.assertFalse((folder / "collection.json").exists())

    def test_original_excerpt_is_hydrated_and_forgery_rejected_by_dependency_replay(self):
        self.plan()
        for _ in range(2):
            wave, service, _ = self.prepare()
            self.finish(wave, service)
        wave, _, _ = self.prepare()
        self.assertEqual(wave["provider"], "anthropic")
        job = wave["jobs"][0]
        self.assertEqual(job["evidence"][0]["excerpts"][0]["text"], "The speaker reports a walk.")
        source_excerpts = job["prompt"]["input"]["source_excerpts"]
        self.assertEqual(source_excerpts[0]["text"], "The speaker reports a walk.")
        self.assertNotIn(str(self.root), json.dumps(source_excerpts))
        plan, selected = runner.load_plan(*self.args)
        state = runner.load_state(plan, selected)
        evidence = copy.deepcopy(job["evidence"])
        # Preserve the length and all IDs, then rebuild the job hash: source replay,
        # not merely a stale hash, must identify the invented original passage.
        excerpt = evidence[0]["excerpts"][0]
        excerpt["text"] = "X" * len(excerpt["text"])
        forged = core.make_job(job["stage"], job["scope"], evidence, job["dependencies"], job["config"])
        self.assertEqual(core.validate_job(forged), forged)
        jobs = [forged if item["job_id"] == job["job_id"] else item for item in state["jobs"]]
        with self.assertRaises(core.SummaryError):
            core.next_jobs(selected, jobs, {item["job_id"]: item for item in state["results"]},
                           self.request["config"])

    def test_source_change_blocks_sonnet_submission_before_http(self):
        self.sonnet_plan()
        wave, service, _ = self.prepare()
        Path(self.request["sources"][0]["transcript"]["path"]).write_text('{"modified":true}')
        with self.assertRaisesRegex(sources.SourceError, "SHA-256"):
            self.submit(wave, service)
        self.assertEqual(service.calls, [])

    def test_sealed_provider_request_tamper_blocks_replay(self):
        self.sonnet_plan()
        _, _, folder = self.prepare()
        path = folder / "provider-requests.bin"
        path.chmod(0o600)
        path.write_bytes(b"tampered")
        with self.assertRaises(runner.Error):
            runner.status_plan(*self.args)

    def test_sonnet_capture_tamper_blocks_replay(self):
        self.sonnet_plan()
        wave, service, folder = self.prepare()
        self.finish(wave, service)
        path = folder / "capture.json"
        path.chmod(0o600)
        path.write_bytes(b'{"tampered":true}\n')
        with self.assertRaises(runner.Error):
            runner.status_plan(*self.args)

    def test_example_is_an_offline_request_with_explicit_topic_selection(self):
        path = Path(__file__).resolve().parents[1] / "examples" / "sonnet-synthesis-request.example.json"
        request = runner.validate_request(runner.read(runner.binding(path)))
        self.assertEqual(request["cloud"], {"processing_approved": False, "paid_tier_confirmed": False})
        self.assertEqual(request["config"]["transcript_profile"], "gemini_flash_batch")
        self.assertEqual(request["config"]["timeline_profile"], PROFILE)
        self.assertEqual(request["config"]["broader_synthesis"]["topics"][0]["recording_ids"],
                         [request["sources"][0]["recording_id"]])


if __name__ == "__main__":
    unittest.main()
