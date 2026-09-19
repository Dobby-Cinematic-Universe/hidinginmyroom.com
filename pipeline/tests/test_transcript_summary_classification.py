"""Conservative tags preserve words/links; paid canaries migrate without resubmission."""
from copy import deepcopy
from pathlib import Path
import unittest
from unittest import mock

from pipeline import transcript_summary as r
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary_recovery as recovery
from pipeline import transcript_summary_classification as classification
from pipeline.tests import test_transcript_summary as fixtures
from pipeline.tests.test_transcript_summary import Service, payload, response_body
from pipeline.tests.test_transcript_summary_core import source
from pipeline.tests import test_transcript_summary_recovery as recovery_fixtures


class InheritanceTests(unittest.TestCase):
    def job(self, tag):
        old = core.initial_jobs([source()])[0]
        evidence = deepcopy(old["evidence"])
        evidence[0]["classification"] = tag
        return core.make_job("chunk", old["scope"], evidence, [], old["config"])

    def test_cited_uncertainty_is_inherited_without_changing_text_or_links(self):
        job = self.job("uncertainty")
        value = payload(job, "The timing remains unclear.")
        original = deepcopy(value)
        with self.assertRaisesRegex(core.SummaryError, "cannot upgrade uncertain"):
            core.normalize_api_result(job, value)
        result, changes = classification.normalize(job, value)
        item = result["sections"]["summary"][0]
        self.assertEqual(value, original)
        self.assertEqual(item["text"], original["summary"][0]["text"])
        self.assertEqual(item["classification"], "uncertainty")
        self.assertEqual(len(item["citations"]), 1)
        self.assertEqual(changes, [{"section": "summary", "item_index": 0,
            "model_classification": "reported_statement", "inherited_classification": "uncertainty"}])
        core.validate_result(job, result)

    def test_allegations_and_more_cautious_model_tags_never_become_more_certain(self):
        for source_tag in core.CLASSIFICATIONS:
            for model_tag in core.CLASSIFICATIONS:
                job = self.job(source_tag)
                value = payload(job)
                value["summary"][0]["classification"] = model_tag
                result, _ = classification.normalize(job, value)
                rank = {"reported_statement": 0, "reported_allegation": 1, "uncertainty": 2}
                self.assertGreaterEqual(rank[result["sections"]["summary"][0]["classification"]], rank[source_tag])
                self.assertGreaterEqual(rank[result["sections"]["summary"][0]["classification"]], rank[model_tag])

    def test_raw_transcript_evidence_has_no_inherited_tag(self):
        job = core.initial_jobs([source()])[0]
        self.assertTrue(all(item["classification"] is None for item in job["evidence"]))
        for tag in core.CLASSIFICATIONS:
            value = payload(job)
            value["summary"][0]["classification"] = tag
            result, changes = classification.normalize(job, value)
            self.assertEqual(result, core.normalize_api_result(job, value))
            self.assertEqual(changes, [])

    def test_invalid_foreign_duplicate_oversized_text_and_wrong_shape_still_fail(self):
        job = self.job("uncertainty")
        cases = []
        for refs in ([], ["e99999"], ["e1", "e1"], ["e1"] * 257):
            value = payload(job)
            value["summary"][0]["evidence_ids"] = refs
            cases.append(value)
        cases += [payload(job, "x" * 1915), {"summary": []}, payload(job)]
        cases[-1]["summary"][0]["classification"] = "verified_fact"
        for value in cases:
            with self.subTest(value=str(value)[:60]), self.assertRaises(RuntimeError):
                classification.normalize(job, value)


class ContinuationTests(unittest.TestCase):
    setUp = fixtures.SummaryRunnerTests.setUp
    file = fixtures.SummaryRunnerTests.file
    source = fixtures.SummaryRunnerTests.source
    producer = recovery_fixtures.RecoveryTests.producer
    finish_ready = recovery_fixtures.RecoveryTests.finish_ready

    def setup_canary(self, *, pending=False, invalid_text=False):
        self.producer(completed=True, uncertain=True)
        with mock.patch("builtins.print"):
            prepared = recovery.prepare_campaign(self.parent, self.code, self.root / "base-admissions",
                self.root / "canary.json", self.root / "canary")
        self.canary_ref = prepared["manifest"]
        self.canary = r.read(self.canary_ref)
        root = Path(self.canary["state_root"])
        r.mkdir(root); r.mkdir(root / "requests")
        r.put(root / "workspace.json", campaign.MARKER)
        r.put(root / "manifest-binding.json", self.canary_ref)
        self.plan_ref = campaign.ensure_plan(self.canary, 0)
        args = self.plan_ref["path"], self.plan_ref["sha256"]
        wave = r.read(r.prepare_plan(*args, phase="transcripts")["manifest"])
        self.assertTrue(all(job["stage"] == "transcript" for job in wave["jobs"]))
        service = Service(wave, r.read(self.plan_ref)["plan_id"])
        for i, (job, row) in enumerate(zip(wave["jobs"], service.rows)):
            value = payload(job, "The timing remains unclear.")
            if i == 0:
                value["summary"][0]["classification"] = "uncertainty"
            if invalid_text and i:
                value["summary"][0]["text"] = "x" * 1915
            row["response"] = response_body("gemini", value)
            row["response"]["modelVersion"] = job["model"]
        r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api)
        if not pending:
            service.state = "completed"
            r.poll_wave(*args, wave["wave_id"], client=service.api)
        folder = self.root / "canary-code"
        r.mkdir(folder)
        self.canary_code = self.file({"producer_manifest": self.canary_ref, "files": {
            name: r.put_bytes(folder / name, (Path(r.__file__).parent / name).read_bytes())
            for name in self.canary["implementation"]}})

    def prepare(self):
        with mock.patch.object(r, "submit_wave", side_effect=AssertionError("offline only")), mock.patch("builtins.print"):
            return classification.prepare(self.canary_ref, self.canary_code, self.root / "continuation-audit",
                self.root / "continued.json", self.root / "continued")

    def test_partial_canary_is_imported_with_exact_words_and_no_new_paid_requests(self):
        self.setup_canary()
        old_status = r.status_plan(self.plan_ref["path"], self.plan_ref["sha256"], phase="transcripts")
        self.assertEqual(old_status["failed_jobs"], 1)
        prepared = self.prepare()
        self.assertEqual(prepared["new_paid_requests"], 0)
        self.assertEqual(prepared["imported_final_summaries"], 2)
        self.assertEqual(prepared["classification_adjustments"], 1)
        value = r.read(prepared["manifest"])
        recovery.validate_campaign(value)
        r.mkdir(Path(value["state_root"])); r.mkdir(Path(value["state_root"]) / "requests")
        ref = campaign.ensure_plan(value, 0)
        args = ref["path"], ref["sha256"]
        status = r.status_plan(*args, phase="transcripts")
        self.assertTrue(status["transcript_phase_complete"])
        self.assertEqual(status["imported_chunk_jobs"], 2)
        self.assertEqual(status["imported_transcript_jobs"], 2)
        self.assertEqual(status["transcript_summaries_complete"], 2)
        self.assertEqual(status["reserved_microusd"], 0)
        self.assertEqual(r.prepare_plan(*args, phase="transcripts")["state"], "phase_complete")
        self.assertEqual(r.export_plan(*args, phase="transcripts")["final_summaries"], 2)
        self.assertEqual(r.status_plan(self.plan_ref["path"], self.plan_ref["sha256"], phase="transcripts")["failed_jobs"], 1)
        self.assertLess(value["budget_microusd"], self.canary["budget_microusd"])

    def test_pending_and_non_metadata_failures_are_not_repaired(self):
        self.setup_canary(pending=True)
        with self.assertRaisesRegex(r.Error, "collect/reconcile"):
            self.prepare()

    def test_long_text_is_not_clipped_to_make_a_canary_pass(self):
        self.setup_canary(invalid_text=True)
        with self.assertRaisesRegex(core.SummaryError, "summary item text"):
            self.prepare()

    def test_mutated_text_budget_or_import_proofs_are_rejected(self):
        self.setup_canary()
        prepared = self.prepare()
        value = r.read(prepared["manifest"])
        recovery.validate_campaign(value)
        admission = r.read(value["recovery"]["imports"]["0"])
        admission["recovered"][-1]["result"]["sections"]["summary"][0]["text"] = "Changed words."
        bad = self.file(admission)
        selected = [r.sources_module.normalize_source(spec) for spec in value["shards"][0]]
        with self.assertRaisesRegex(r.Error, "does not replay"):
            classification.load(bad, selected, value["config"])
        value["recovery"]["prior_unsettled_hold_microusd"] -= 1
        value["budget_microusd"] += 1
        with self.assertRaises(r.Error):
            recovery.validate_campaign(value)


class WavePolicyTests(unittest.TestCase):
    setUp = fixtures.SummaryRunnerTests.setUp
    file = fixtures.SummaryRunnerTests.file
    source = fixtures.SummaryRunnerTests.source

    def test_policy_is_sealed_into_each_wave_and_adjustment_is_audited(self):
        spec = self.source("tag-inheritance")
        value = {"kind": "himr_transcript_summary_request", "schema_version": 1,
            "state_root": str(self.root / "policy-plan"), "sources": [spec], "config": core.DEFAULT_CONFIG,
            "limits": r.DEFAULT_LIMITS, "budget": r.DEFAULT_BUDGET,
            "cloud": {"processing_approved": True, "paid_tier_confirmed": True},
            "classification_policy": classification.POLICY}
        request = self.file(value)
        ref = r.create_plan(request["path"], request["sha256"])["plan"]
        args = ref["path"], ref["sha256"]
        for stage in ("chunk", "transcript"):
            wave = r.read(r.prepare_plan(*args, phase="transcripts")["manifest"])
            self.assertEqual(wave["classification_policy"], classification.POLICY)
            service = Service(wave, r.read(ref)["plan_id"])
            for job, row in zip(wave["jobs"], service.rows):
                content = payload(job, "The timing remains unclear.")
                if stage == "chunk":
                    content["summary"][0]["classification"] = "uncertainty"
                row["response"] = response_body("gemini", content)
            r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api)
            service.state = "completed"
            result = r.poll_wave(*args, wave["wave_id"], client=service.api)
            self.assertEqual(result["needs_review"], 0)
        collection = r.read(r.binding(Path(ref["path"]).parent / "waves" / wave["wave_id"] / "collection.json"))
        self.assertEqual(collection["outcomes"][0]["classification_adjustments"][0]["inherited_classification"], "uncertainty")
        self.assertTrue(r.status_plan(*args, phase="transcripts")["transcript_phase_complete"])
        # A separately requested non-Gemini stage keeps its own contract.
        later = r.read(r.prepare_plan(*args)["manifest"])
        self.assertEqual(later["provider"], "openai")
        self.assertNotIn("classification_policy", later)
        r.status_plan(*args)


if __name__ == "__main__":
    unittest.main()
