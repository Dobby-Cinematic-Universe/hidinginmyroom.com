"""Offline REST-envelope and durable mutation-boundary integration checks."""
from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
import tempfile
from pathlib import Path
import unittest
from unittest import mock

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_client as transport
from pipeline import transcript_summary_core as core
from pipeline.tests.test_transcript_summary_client import Response
from pipeline.tests.test_transcript_summary_core import source, output


def gemini_operation(wave, *, state="BATCH_STATE_PENDING", done=False, rows=None):
    metadata = {"model": "models/" + wave["model"], "displayName": wave["wave_id"], "state": state}
    if rows is not None:
        metadata["output"] = {"inlinedResponses": {"inlinedResponses": rows}}
    return {"name": "batches/protocol_fixture", "metadata": metadata, "done": done}


class ProviderProtocolTests(unittest.TestCase):
    def setUp(self):
        self.job = core.initial_jobs([source()])[0]
        self.wave = {"wave_id": "summarywave_" + "a" * 32, "provider": "gemini",
                     "model": self.job["model"], "jobs": [self.job]}
        api_output = output(self.job)
        api_output["summary"][0]["evidence_ids"] = ["e1"]
        self.item = {"metadata": {"key": self.job["job_id"]}, "response": {
            "candidates": [{"finishReason": "STOP", "content": {"parts": [
                {"text": runner.canonical(api_output).decode()}]}}]}}

    def test_gemini_compatible_schema_is_bound_before_transport(self):
        local = self.job["prompt"]["response_schema"]
        body = self.job["request"]["body"]
        schema = body["generationConfig"]["responseSchema"]
        item_schema = {"type": "OBJECT", "properties": {
            "text": {"type": "STRING"},
            "classification": {"type": "STRING", "format": "enum",
                               "enum": list(core.CLASSIFICATIONS)},
            "evidence_ids": {"type": "ARRAY", "minItems": "1", "items": {"type": "STRING"}}},
            "required": ["text", "classification", "evidence_ids"],
            "propertyOrdering": ["text", "classification", "evidence_ids"]}
        expected = {"type": "OBJECT", "properties": {
            section: {"type": "ARRAY", "minItems": "1" if section == "summary" else "0",
                      "items": deepcopy(item_schema)} for section in core.SECTIONS},
            "required": list(core.SECTIONS), "propertyOrdering": list(core.SECTIONS)}
        self.assertEqual(schema, expected)
        self.assertNotIn("responseJsonSchema", body["generationConfig"])
        self.assertEqual(local, core.response_schema(self.job["config"]))
        for keyword in (b'"minLength"', b'"maxLength"', b'"maxItems"', b'"additionalProperties"'):
            self.assertIn(keyword, core.canonical(local))
            self.assertNotIn(keyword, core.canonical(schema))
        self.assertEqual(self.job["budget"]["input_utf8_bytes"], len(core.canonical(body)))

        restored = runner.parse(core.canonical(self.job))
        self.assertEqual(core.validate_job(restored), self.job)
        rows = runner.parse(runner.wire([restored]))
        http = mock.Mock(return_value=Response(gemini_operation(self.wave)))
        api = transport.GeminiBatchClient("offline-fixture-key", transport=http)
        before = deepcopy(rows)
        api.create_batch(self.job["model"], rows, self.wave["wave_id"])
        request = http.call_args.args[0]
        sent = runner.parse(request.data)["batch"]["input_config"]["requests"]["requests"][0]["request"]
        self.assertEqual(sent, body)
        self.assertEqual(rows, before)
        self.assertEqual(request.data, transport.gemini_batch_bytes(
            self.job["model"], rows, self.wave["wave_id"]))
        self.assertEqual(len(request.data), runner.submission_size([self.job]))
        http.assert_called_once()

    def test_reinstating_unadapted_wire_schema_cannot_replay_a_sealed_job(self):
        forged = deepcopy(self.job)
        forged["request"]["body"]["generationConfig"].pop("responseSchema")
        forged["request"]["body"]["generationConfig"]["responseJsonSchema"] = deepcopy(
            forged["prompt"]["response_schema"])
        with self.assertRaisesRegex(core.SummaryError, "job replay"):
            core.validate_job(forged)

    def test_stripped_gemini_wire_bounds_are_still_enforced_on_collection(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        oversized = output(self.job, text="x" * (self.job["config"]["max_item_chars"] + 1))
        blank = output(self.job, text=" ")
        excessive = output(self.job)
        excessive["summary"] *= self.job["config"]["max_items_per_section"] + 1
        for payload in (oversized, blank, excessive):
            for item in payload["summary"]:
                item["evidence_ids"] = ["e1"]
            with self.subTest(payload_size=len(core.canonical(payload))):
                response = deepcopy(self.item["response"])
                response["candidates"][0]["content"]["parts"][0]["text"] = core.canonical(payload).decode()
                row = {"custom_id": self.job["job_id"], "response": response, "error": None}
                outcome = runner.collect_result(self.wave, {"batch": remote, "items": [row]})["outcomes"][0]
                self.assertEqual(outcome["state"], "needs_review")
                self.assertEqual(outcome["failure"], "output_needs_review")
                self.assertIsNone(outcome["result"])

    def test_complete_gemini_malformed_arrays_remain_reviewable_not_fabricated_objects(self):
        # Regression for the real canaries: HTTP success, STOP, and valid JSON do
        # not prove the required classified, evidence-linked object shape.
        payloads = (
            {"summary": ["The speaker reports a walk."], "topics": ["Walking"],
             "events": ["A visit to a park."], "uncertainties": []},
            {"summary": [None], "topics": [None], "events": [None], "uncertainties": []},
            {"summary": ["The speaker reports a walk.", "reported_statement", ["e1"]],
             "topics": [], "events": [], "uncertainties": []})
        for payload in payloads:
            with self.subTest(summary=payload["summary"]):
                response = deepcopy(self.item["response"])
                response["modelVersion"] = self.job["model"]
                response["candidates"][0]["content"]["parts"][0]["text"] = core.canonical(payload).decode()
                remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True,
                    rows=[{"metadata": {"key": self.job["job_id"]}, "response": response}])
                rows = runner.remote_items("gemini", remote, mock.Mock())
                capture = {"batch": remote, "items": rows}
                before = deepcopy(capture)
                self.assertEqual(runner.response_payload("gemini", response), payload)
                with self.assertRaisesRegex(core.SummaryError, "summary item fields differ"):
                    core.normalize_api_result(self.job, payload)
                outcome = runner.collect_result(self.wave, capture)["outcomes"][0]
                self.assertEqual(outcome["state"], "needs_review")
                self.assertEqual(outcome["failure"], "output_needs_review")
                self.assertIsNone(outcome["result"])
                self.assertEqual(capture, before)

    def test_gemini_actual_metadata_output_path_collects_success(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True, rows=[self.item])
        rows = runner.remote_items("gemini", remote, mock.Mock())
        self.assertEqual(rows, [{"custom_id": self.job["job_id"], "response": self.item["response"], "error": None}])
        result = runner.collect_result(self.wave, {"batch": remote, "items": rows})
        self.assertEqual(result["outcomes"][0]["state"], "completed")
        retained = result["outcomes"][0]["result"]
        self.assertEqual(retained, core.normalize_result(self.job, output(self.job)))
        self.assertEqual(core.validate_result(self.job, retained), retained)

    def test_provider_cannot_cite_full_local_ids_or_non_evidence_aliases(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        for refs in (["s1"], ["x1"], ["e0"], ["e2"], ["e1", "e1"],
                     [self.job["evidence"][0]["evidence_id"]]):
            with self.subTest(refs=refs):
                api_output = output(self.job)
                api_output["summary"][0]["evidence_ids"] = refs
                response = deepcopy(self.item["response"])
                response["candidates"][0]["content"]["parts"][0]["text"] = runner.canonical(api_output).decode()
                rows = [{"custom_id": self.job["job_id"], "response": response, "error": None}]
                collected = runner.collect_result(self.wave, {"batch": remote, "items": rows})
                outcome = collected["outcomes"][0]
                self.assertEqual(outcome["state"], "needs_review")
                self.assertEqual(outcome["failure"], "output_needs_review")
                self.assertIsNone(outcome["result"])

    def test_reported_different_model_requires_review(self):
        self.item["response"]["modelVersion"] = "gemini-unrequested-model"
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True, rows=[self.item])
        rows = runner.remote_items("gemini", remote, mock.Mock())
        result = runner.collect_result(self.wave, {"batch": remote, "items": rows})
        self.assertEqual(result["outcomes"][0]["state"], "needs_review")

    def test_thought_flag_cannot_hide_tool_output(self):
        response = deepcopy(self.item["response"])
        response["candidates"][0]["content"]["parts"].insert(0,
            {"thought": True, "functionCall": {"name": "unrequested"}})
        with self.assertRaises(runner.Error):
            runner.response_payload("gemini", response)

    def test_gemini_response_output_path_if_used_is_still_supported(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        remote["response"] = {"inlinedResponses": {"inlinedResponses": [self.item]}}
        rows = runner.remote_items("gemini", remote, mock.Mock())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["custom_id"], self.job["job_id"])

    def test_gemini_response_resource_wrapper_exposes_only_nested_output(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        remote["response"] = {"@type": "type.googleapis.com/example.GenerateContentBatch",
            "name": remote["name"], "model": "models/" + self.wave["model"],
            "output": {"inlinedResponses": {"inlinedResponses": [self.item]}}}
        rows = runner.remote_items("gemini", remote, mock.Mock())
        self.assertEqual(rows, [{"custom_id": self.job["job_id"], "response": self.item["response"], "error": None}])

    def test_gemini_response_resource_metadata_is_not_a_competing_output(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True, rows=[self.item])
        remote["response"] = {"@type": "type.googleapis.com/example.GenerateContentBatch",
            "name": remote["name"], "model": "models/" + self.wave["model"],
            "displayName": self.wave["wave_id"], "state": "BATCH_STATE_SUCCEEDED"}
        rows = runner.remote_items("gemini", remote, mock.Mock())
        self.assertEqual(rows, [{"custom_id": self.job["job_id"], "response": self.item["response"], "error": None}])

    def test_conflicting_gemini_output_containers_fail_closed(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True, rows=[self.item])
        remote["response"] = {"inlinedResponses": {"inlinedResponses": []}}
        with self.assertRaises(runner.Error):
            runner.remote_items("gemini", remote, mock.Mock())

    def test_file_result_cannot_masquerade_as_missing_inline_rows(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        remote["metadata"]["output"] = {"responsesFile": "files/unexpected"}
        with self.assertRaises(runner.Error):
            runner.remote_items("gemini", remote, mock.Mock())

    def test_gemini_done_false_never_opens_retry_boundary(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=False)
        try:
            view = runner.check_remote(self.wave, remote)
        except runner.Error:
            return  # Rejecting inconsistent provider state is also safe.
        self.assertNotIn(view["status"], runner.TERMINAL)

    def test_gemini_operation_error_terminates_even_if_metadata_lags(self):
        for code, expected in ((1, "cancelled"), (13, "failed")):
            remote = gemini_operation(self.wave, state="BATCH_STATE_RUNNING", done=True)
            remote["error"] = {"code": code, "message": "private provider message"}
            with self.subTest(code=code):
                self.assertEqual(runner.check_remote(self.wave, remote)["status"], expected)

    def test_output_order_is_not_used_as_an_identifier(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        item = deepcopy(self.item)
        item.pop("metadata")
        remote["response"] = {"inlinedResponses": {"inlinedResponses": [item]}}
        with self.assertRaises(runner.Error):
            runner.collect_result(self.wave, {"batch": remote, "items": runner.remote_items("gemini", remote, mock.Mock())})

    def test_duplicate_output_ids_are_rejected(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        remote["response"] = {"inlinedResponses": {"inlinedResponses": [self.item, self.item]}}
        with self.assertRaises(runner.Error):
            runner.collect_result(self.wave, {"batch": remote, "items": runner.remote_items("gemini", remote, mock.Mock())})

    def test_gemini_function_call_parts_never_become_summary_text(self):
        response = deepcopy(self.item["response"])
        response["candidates"][0]["content"]["parts"][0]["functionCall"] = {"name": "unexpected_tool", "args": {}}
        with self.assertRaises(runner.Error):
            runner.response_payload("gemini", response)

    def test_openai_tool_outputs_are_not_ignored(self):
        response = {"status": "completed", "output": [
            {"type": "function_call", "name": "unexpected_tool", "arguments": "{}"},
            {"type": "message", "status": "completed", "role": "assistant", "content": [
                {"type": "output_text", "text": runner.canonical(output(self.job)).decode()}]}]}
        with self.assertRaises(runner.Error):
            runner.response_payload("openai", response)

    def test_malformed_single_item_is_reviewable_not_batch_crashing(self):
        remote = gemini_operation(self.wave, state="BATCH_STATE_SUCCEEDED", done=True)
        for malformed in ({"candidates": [None]}, {"candidates": None},
                          {"promptFeedback": None}, {"candidates": [{"finishReason": "STOP", "content": {"parts": [None]}}]}):
            row = {"custom_id": self.job["job_id"], "response": malformed, "error": None}
            with self.subTest(malformed=malformed):
                result = runner.collect_result(self.wave, {"batch": remote, "items": [row]})
                self.assertEqual(result["outcomes"][0]["state"], "needs_review")


class MutationBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.wave_id = "summarywave_" + "c" * 32
        self.folder = self.root / "waves" / self.wave_id
        self.folder.mkdir(parents=True, mode=0o700)
        self.job = core.initial_jobs([source()])[0]
        self.wave = {"wave_id": self.wave_id, "provider": "gemini", "model": self.job["model"],
                     "jobs": [self.job], "input_sha256": "a" * 64, "maximum_cost_microusd": 1000}
        self.plan = {"plan_id": "summaryplan_" + "b" * 32, "request_value": {
            "state_root": str(self.root), "cloud": {"processing_approved": True, "paid_tier_confirmed": True},
            "limits": {"max_wave_bytes": runner.MAX_WAVE_BYTES},
            "budget": {"max_reserved_microusd": 1_000_000}}}
        self.state = {"waves": [self.wave], "reserved_microusd": 0, "rejections": {}}
        self.stack = mock.patch.multiple(runner,
            load_plan=mock.Mock(return_value=(self.plan, [])),
            load_state=mock.Mock(return_value=self.state),
            locked=mock.Mock(side_effect=lambda _: nullcontext()))
        self.stack.start()
        self.addCleanup(self.stack.stop)

    def submit(self, api):
        return runner.submit_wave("unused-plan", "a" * 64, self.wave_id, allow_paid_api=True, client=api)

    def test_interrupt_after_post_intent_never_resubmits(self):
        api = mock.Mock()
        api.create_batch.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.submit(api)
        self.assertTrue((self.folder / "submit-intent.json").is_file())
        self.assertFalse((self.folder / "submitted.json").exists())
        self.assertEqual(self.submit(api)["state"], "needs_reconciliation")
        api.create_batch.assert_called_once()

    def test_gemini_incomplete_metadata_receipt_is_retained_before_binding_checks(self):
        remote = {"name": "batches/protocol_fixture"}
        self.assertEqual(transport._gemini_operation(remote), remote)
        api = mock.Mock()
        api.create_batch.return_value = remote
        with self.assertRaisesRegex(runner.Error, "exact wave identity"):
            self.submit(api)
        self.assertTrue((self.folder / "submit-intent.json").is_file())
        saved = self.folder / "submission-response.json"
        self.assertEqual(runner.read(runner.binding(saved)), remote)
        self.assertFalse((self.folder / "submitted.json").exists())
        self.assertEqual(self.submit(api)["state"], "needs_reconciliation")
        api.create_batch.assert_called_once()

        api.get_batch.return_value = gemini_operation(self.wave)
        result = runner.reconcile_wave("unused-plan", "a" * 64, self.wave_id,
                                       remote["name"], client=api)
        self.assertEqual(result["state"], "reconciled")
        receipt = runner.read(runner.binding(self.folder / "submitted.json"))
        self.assertEqual(receipt["remote_id"], remote["name"])
        self.assertTrue(receipt["reconciled"])
        self.assertEqual(runner.read(runner.binding(saved)), remote)
        api.get_batch.assert_called_once_with(remote["name"])
        api.create_batch.assert_called_once()

    def test_paid_approval_is_required_before_intent_or_api(self):
        api = mock.Mock()
        with self.assertRaises(runner.Error):
            runner.submit_wave("unused-plan", "a" * 64, self.wave_id, allow_paid_api=False, client=api)
        self.assertFalse((self.folder / "submit-intent.json").exists())
        api.create_batch.assert_not_called()

    def test_budget_denial_precedes_intent_or_api(self):
        self.plan["request_value"]["budget"]["max_reserved_microusd"] = 1
        api = mock.Mock()
        with self.assertRaises(runner.Error):
            self.submit(api)
        self.assertFalse((self.folder / "submit-intent.json").exists())
        api.create_batch.assert_not_called()

    def test_local_gemini_preflight_failure_leaves_no_ambiguous_intent(self):
        self.job["request"]["body"]["tools"] = []
        api = mock.Mock()
        with self.assertRaises((runner.Error, transport.BatchClientError)):
            self.submit(api)
        self.assertFalse((self.folder / "submit-intent.json").exists())
        api.create_batch.assert_not_called()

    def test_openai_upload_receipt_survives_interrupted_create(self):
        config = {**core.DEFAULT_CONFIG, "transcript_profile": "openai_mini_batch"}
        self.job = core.initial_jobs([source()], config)[0]
        self.wave.update(provider="openai", model=self.job["model"], jobs=[self.job])
        api = mock.Mock()
        api.upload_batch.return_value = {"id": "file-known"}
        api.create_batch.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.submit(api)
        self.assertTrue((self.folder / "uploaded-input.json").exists())
        self.assertEqual(self.submit(api)["state"], "needs_reconciliation")
        api.upload_batch.assert_called_once()
        api.create_batch.assert_called_once()


if __name__ == "__main__":
    unittest.main()
