"""Paid-output adoption, evidence limits and restart guarantees, all offline."""
from copy import deepcopy
from pathlib import Path
import unittest
from unittest import mock

from pipeline import transcript_summary as r
from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_recovery as recovery
from pipeline.tests import test_transcript_summary as fixtures
from pipeline.tests.test_transcript_summary import Service, payload, response_body
from pipeline.tests.test_transcript_summary_core import source
from pipeline.tests.test_transcript_summary_campaign import manifest, response


class EvidenceContractTests(unittest.TestCase):
    def setUp(self):
        self.sources = [source(["Daniel describes part of his walk."] * 40)]
        self.old = core.initial_jobs(self.sources)[0]
        self.config = {**core.DEFAULT_CONFIG, "max_evidence_refs_per_item": 256}
        self.new = core.make_job("chunk", self.old["scope"], self.old["evidence"], [], self.config)

    def test_old_wire_is_unchanged_and_new_wire_constrains_arrays(self):
        for job, expected in ((self.old, None), (self.new, "256")):
            schema = job["request"]["body"]["generationConfig"]["responseSchema"]
            refs = schema["properties"]["summary"]["items"]["properties"]["evidence_ids"]
            self.assertEqual(refs.get("maxItems"), expected)
            self.assertEqual(schema["propertyOrdering"], list(core.SECTIONS))
        self.assertNotEqual(self.old["job_id"], self.new["job_id"])

    def test_compatible_wire_omits_nested_bounds_but_local_validation_keeps_them(self):
        config = {**self.config, "gemini_schema_policy": "local_array_bounds_v2"}
        repaired = core.make_job("chunk", self.old["scope"], self.old["evidence"], [], config)
        schema = repaired["request"]["body"]["generationConfig"]["responseSchema"]
        self.assertNotIn(b'"maxItems"', core.canonical(schema))
        self.assertEqual(schema["properties"]["summary"]["minItems"], "1")
        self.assertEqual(schema["propertyOrdering"], list(core.SECTIONS))
        self.assertEqual(repaired["prompt"]["response_schema"]["properties"]["summary"]["maxItems"], 24)
        self.assertIn("1..256 unique evidence_ids", repaired["prompt"]["instructions"])
        value = payload(repaired)
        value["summary"][0]["evidence_ids"] = ["e" + str(i) for i in range(1, 41)]
        self.assertEqual(len(core.normalize_api_result(repaired, value)["sections"]["summary"][0]["citations"]), 40)
        value["summary"][0]["evidence_ids"] = ["e1"] * 257
        with self.assertRaisesRegex(core.SummaryError, "reference count"):
            core.normalize_api_result(repaired, value)
        for value in (True, False, "", "unbounded", None):
            with self.subTest(value=value), self.assertRaises(core.SummaryError):
                core.normalize_config({**config, "gemini_schema_policy": value})

    def test_schema_policy_does_not_change_other_provider_wire_requests(self):
        for profile in ("openai_mini_batch", "anthropic_sonnet_batch"):
            config = {**self.config, "transcript_profile": profile}
            old = core.make_job("chunk", self.old["scope"], self.old["evidence"], [], config)
            new = core.make_job("chunk", self.old["scope"], self.old["evidence"], [],
                               {**config, "gemini_schema_policy": "local_array_bounds_v2"})
            self.assertEqual(old["request"]["body"], new["request"]["body"])

    def test_valid_long_reference_lists_are_preserved_without_truncation(self):
        value = payload(self.new)
        value["summary"][0]["evidence_ids"] = ["e" + str(i) for i in range(1, 41)]
        with self.assertRaisesRegex(core.SummaryError, "missing or foreign"):
            core.normalize_api_result(self.old, value)
        result = core.normalize_api_result(self.new, value)
        item = result["sections"]["summary"][0]
        self.assertEqual(len(item["evidence_ids"]), 40)
        self.assertEqual(item["text"], value["summary"][0]["text"])
        self.assertEqual(core.validate_result(self.new, result), result)

    def test_foreign_duplicate_excessive_refs_and_long_text_still_fail(self):
        for refs, reason in ((["e1", "e999"], "missing or foreign"),
                             (["e1", "e1"], "repeats"),
                             (["e1"] * 257, "reference count"), ([], "reference count")):
            value = payload(self.new)
            value["summary"][0]["evidence_ids"] = refs
            with self.subTest(refs=len(refs)), self.assertRaisesRegex(core.SummaryError, reason):
                core.normalize_api_result(self.new, value)
        with self.assertRaisesRegex(core.SummaryError, "summary item text"):
            core.normalize_api_result(self.new, payload(self.new, "x" * 1915))

    def test_imported_boundary_override_requires_exact_source_coverage(self):
        core.next_jobs(self.sources, [self.new], {}, self.config,
                       initial_jobs_override=[self.new], stages={"transcript"})
        with self.assertRaises(core.SummaryError):
            core.next_jobs(self.sources, [], {}, self.config,
                           initial_jobs_override=[], stages={"transcript"})
        with self.assertRaises(core.SummaryError):
            core.next_jobs(self.sources, [self.new], {}, self.config,
                           initial_jobs_override=[self.old], stages={"transcript"})


class RecoveryTests(unittest.TestCase):
    setUp = fixtures.SummaryRunnerTests.setUp
    file = fixtures.SummaryRunnerTests.file
    source = fixtures.SummaryRunnerTests.source

    def producer(self, *, pending=False, unknown_usage=False, completed=False, uncertain=False):
        specs = []
        for index in range(3):
            spec = self.source("recording-" + str(index))
            doc = r.read(spec["transcript"])
            doc["segments"] = [{"text": "Daniel describes a walk and its surroundings.",
                                 "start_ms": i * 1000, "end_ms": (i + 1) * 1000,
                                 "speaker": None} for i in range(30)]
            spec["transcript"] = self.file(doc)
            specs.append(spec)
        value = manifest(self.root / "producer", budget=120_000_000)
        value["shards"] = [specs]
        if completed:
            value["shards"] = [[specs[0]], specs[1:]]
        value["selection"] = self.file({"kind": "himr_private_transcript_archive_selection",
            "schema_version": 1, "sources": specs, "shards": value["shards"],
            "semantics": {"third_party_transcripts_included": True}})
        self.parent = self.file(value)
        folder = self.root / "producer-code"
        r.mkdir(folder)
        files = {name: r.put_bytes(folder / name, (Path(r.__file__).parent / name).read_bytes())
                 for name in value["implementation"]}
        self.code = self.file({"producer_manifest": self.parent, "files": files})
        r.mkdir(Path(value["state_root"]))
        r.mkdir(Path(value["state_root"]) / "requests")
        for index in range(len(value["shards"])):
            ref = campaign.ensure_plan(value, index)
            self.old_ref = ref
            args = ref["path"], ref["sha256"]
            prepared = r.prepare_plan(*args, phase="transcripts")
            wave = r.read(prepared["manifest"])
            service = Service(wave, r.read(ref)["plan_id"])
            for number, (job, row) in enumerate(zip(wave["jobs"], service.rows)):
                content = payload(job)
                if uncertain and (not completed or index != 0):
                    content["summary"][0]["classification"] = "uncertainty"
                if not completed or index != 0:
                    if number >= 1:
                        content["summary"][0]["evidence_ids"] = ["e" + str(i) for i in range(1, 31)]
                    if number == 2:
                        content["summary"][0]["text"] = "x" * 1915
                row["response"] = {**response_body("gemini", content), **response(job)}
                if unknown_usage:
                    row["response"].pop("usageMetadata")
            r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api)
            if not pending:
                service.state = "completed"
                r.poll_wave(*args, wave["wave_id"], client=service.api)
            if completed and index == 0:
                self.finish_ready(ref)
        return value

    def finish_ready(self, ref):
        args = ref["path"], ref["sha256"]
        prepared = r.prepare_plan(*args, phase="transcripts")
        wave = r.read(prepared["manifest"])
        service = Service(wave, r.read(ref)["plan_id"])
        for job, row in zip(wave["jobs"], service.rows):
            row["response"].update(response(job))
        r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api, phase="transcripts")
        service.state = "completed"
        r.poll_wave(*args, wave["wave_id"], client=service.api)
        return wave

    def prepare(self):
        with mock.patch.object(r, "api_client", side_effect=AssertionError("Recovery is offline")), \
                mock.patch.object(r, "submit_wave", side_effect=AssertionError("Recovery cannot submit")), \
                mock.patch("builtins.print"):
            result = recovery.prepare_campaign(self.parent, self.code, self.root / "admissions",
                                                self.root / "recovered.json", self.root / "recovered")
        self.new_manifest = r.read(result["manifest"])
        return result

    def new_plan(self):
        r.mkdir(Path(self.new_manifest["state_root"]))
        r.mkdir(Path(self.new_manifest["state_root"]) / "requests")
        return campaign.ensure_plan(self.new_manifest, 0)

    def test_end_to_end_recovers_paid_chunks_and_only_pays_for_missing_jobs(self):
        self.producer()
        result = self.prepare()
        self.assertEqual(result["new_paid_requests"], 0)
        recovery.validate_campaign(self.new_manifest)
        admission_ref = self.new_manifest["recovery"]["imports"]["0"]
        admission = r.read(admission_ref)
        self.assertEqual([len(admission[key]) for key in ("retained", "recovered", "retry")], [1, 1, 1])
        self.assertIn("summary item text", admission["retry"][0]["reason"])
        ref = self.new_plan()
        status = r.status_plan(ref["path"], ref["sha256"], phase="transcripts")
        self.assertEqual(status["imported_chunk_jobs"], 2)
        self.assertEqual(status["recovered_chunk_jobs"], 1)
        self.assertEqual(status["reserved_microusd"], 0)
        self.assertEqual(status["ready_jobs"], 3)
        paid = self.finish_ready(ref)
        paid_chunks = [job["job_id"] for job in paid["jobs"] if job["stage"] == "chunk"]
        self.assertEqual(paid_chunks, [admission["retry"][0]["job_id"]])
        self.finish_ready(ref)
        status = r.status_plan(ref["path"], ref["sha256"], phase="transcripts")
        self.assertTrue(status["transcript_phase_complete"])
        self.assertEqual(status["transcript_summaries_complete"], 3)
        self.assertEqual(r.prepare_plan(ref["path"], ref["sha256"], phase="transcripts")["state"], "phase_complete")

    def test_pending_original_submission_blocks_migration(self):
        self.producer(pending=True)
        with self.assertRaisesRegex(r.Error, "collect/reconcile"):
            self.prepare()
        self.assertFalse((self.root / "recovered.json").exists())

    def test_unknown_usage_holds_are_not_released_by_recovery(self):
        self.producer(unknown_usage=True)
        result = self.prepare()
        self.assertEqual(result["prior_usage_estimate_microusd"], 0)
        self.assertGreater(result["prior_unsettled_hold_microusd"], 0)
        self.assertEqual(result["remaining_budget_microusd"],
                         120_000_000 - result["prior_unsettled_hold_microusd"])
        recovery.validate_campaign(self.new_manifest)

    def test_completed_original_transcripts_are_preserved_and_excluded(self):
        self.producer(completed=True)
        result = self.prepare()
        self.assertEqual(result["previous_completed_summaries"], 1)
        self.assertEqual(result["selected_transcripts"], 2)
        self.assertEqual(len(self.new_manifest["shards"]), 1)
        recovery.validate_campaign(self.new_manifest)
        changed = deepcopy(self.new_manifest)
        changed["recovery"]["previous_completed"] = []
        with self.assertRaisesRegex(r.Error, "partition or prior paid accounting"):
            recovery.validate_campaign(changed)

    def test_tampered_import_budget_selection_and_producer_are_rejected(self):
        self.producer()
        self.prepare()
        recovery.validate_campaign(self.new_manifest)
        for change in ("budget", "shards", "imports"):
            value = deepcopy(self.new_manifest)
            if change == "budget":
                value["recovery"]["prior_usage_estimate_microusd"] -= 1
                value["budget_microusd"] += 1
            elif change == "shards":
                value["shards"][0] = value["shards"][0][1:]
            else:
                value["recovery"]["imports"] = {}
            with self.subTest(change=change), self.assertRaises(r.Error):
                recovery.validate_campaign(value)
        admission = r.read(self.new_manifest["recovery"]["imports"]["0"])
        admission["retained"][0]["result"]["sections"]["summary"][0]["text"] = "Forged result."
        bad_ref = self.file(admission)
        sources = [r.sources_module.normalize_source(spec) for spec in self.new_manifest["shards"][0]]
        with self.assertRaisesRegex(r.Error, "does not replay"):
            recovery.load(bad_ref, sources, self.new_manifest["config"])
        original = r.read(self.old_ref)
        replacement = self.file({**original, "source_bytes": original["source_bytes"] + 1})
        Path(replacement["path"]).replace(self.old_ref["path"])
        with self.assertRaisesRegex(r.Error, "digest differs"):
            recovery.load(self.new_manifest["recovery"]["imports"]["0"], sources, self.new_manifest["config"])

    def test_new_failed_collection_has_actionable_reason_and_no_automatic_retry(self):
        self.producer()
        self.prepare()
        ref = self.new_plan()
        args = ref["path"], ref["sha256"]
        prepared = r.prepare_plan(*args, phase="transcripts")
        wave = r.read(prepared["manifest"])
        service = Service(wave, r.read(ref)["plan_id"])
        for job, row in zip(wave["jobs"], service.rows):
            content = payload(job, "x" * 1915) if job["stage"] == "chunk" else payload(job)
            row["response"] = {**response_body("gemini", content), **response(job)}
        r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api)
        service.state = "completed"
        r.poll_wave(*args, wave["wave_id"], client=service.api)
        status = r.status_plan(*args, phase="transcripts")
        self.assertEqual(status["failed_jobs"], 1)
        self.assertEqual(status["transcript_summaries_complete"], 2)
        self.assertTrue(any("summary item text" in reason for reason in status["failure_reasons"]))
        self.assertEqual(status["ready_jobs"], 0)
        with self.assertRaisesRegex(r.Error, "attempt limits"):
            r.prepare_plan(*args, retry_wave=wave["wave_id"], phase="transcripts")

    def test_provider_error_code_is_visible_without_echoing_remote_message(self):
        self.producer()
        self.prepare()
        ref = self.new_plan()
        args = ref["path"], ref["sha256"]
        wave = r.read(r.prepare_plan(*args, phase="transcripts")["manifest"])
        service = Service(wave, r.read(ref)["plan_id"])
        for row in service.rows:
            row["response"] = None
            row["error"] = {"code": 3, "message": "PRIVATE INPUT MUST NOT ENTER STATUS"}
        r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api)
        service.state = "completed"
        r.poll_wave(*args, wave["wave_id"], client=service.api)
        status = r.status_plan(*args, phase="transcripts")
        self.assertEqual(status["failure_reasons"], {"provider_request_failed: INVALID_ARGUMENT (3)": len(wave["jobs"])})
        self.assertNotIn("PRIVATE INPUT", r.canonical(status).decode())
        self.assertEqual(status["pending_waves"], [])


class SchemaRepairTests(unittest.TestCase):
    setUp = fixtures.SummaryRunnerTests.setUp
    file = fixtures.SummaryRunnerTests.file
    source = fixtures.SummaryRunnerTests.source
    producer = RecoveryTests.producer
    finish_ready = RecoveryTests.finish_ready

    def failed_canary(self, *, pending=False, mixed=False):
        parent = self.producer(completed=True)
        old_config = {**parent["config"], "max_evidence_refs_per_item": 256, "max_chunk_input_bytes": 26000}
        with mock.patch.object(recovery, "_new_config", return_value=old_config), mock.patch("builtins.print"):
            prepared = recovery.prepare_campaign(self.parent, self.code, self.root / "old-admissions",
                self.root / "old-recovery.json", self.root / "old-recovery")
        self.failed_ref = prepared["manifest"]
        value = r.read(self.failed_ref)
        root = Path(value["state_root"])
        r.mkdir(root)
        r.mkdir(root / "requests")
        r.put(root / "workspace.json", campaign.MARKER)
        r.put(root / "manifest-binding.json", self.failed_ref)
        ref = campaign.ensure_plan(value, 0)
        args = ref["path"], ref["sha256"]
        wave = r.read(r.prepare_plan(*args, phase="transcripts")["manifest"])
        self.assertTrue(all(j["stage"] == "transcript" for j in wave["jobs"]))
        service = Service(wave, r.read(ref)["plan_id"])
        for index, row in enumerate(service.rows):
            if not mixed or index:
                row["response"] = None
                row["error"] = {"code": 3, "message": "Request contains an invalid argument."}
        r.submit_wave(*args, wave["wave_id"], allow_paid_api=True, client=service.api)
        if not pending:
            service.state = "completed"
            r.poll_wave(*args, wave["wave_id"], client=service.api)
        r.mkdir(self.root / "failed-code")
        code = self.file({"producer_manifest": self.failed_ref, "files": {
            name: r.put_bytes(self.root / "failed-code" / name, (Path(r.__file__).parent / name).read_bytes())
            for name in value["implementation"]}})
        self.repair = {"producer_manifest": self.failed_ref, "producer_code": code, "diagnostic": None}
        self.failed_wave = wave
        return value

    def prepare(self):
        with mock.patch.object(r, "submit_wave", side_effect=AssertionError("offline only")), mock.patch("builtins.print"):
            return recovery.prepare_campaign(self.parent, self.code, self.root / "new-admissions",
                self.root / "new-recovery.json", self.root / "new-recovery", schema_repair=self.repair)

    def test_failed_canary_is_replaced_with_all_imports_and_holds_preserved(self):
        failed = self.failed_canary()
        result = self.prepare()
        value = r.read(result["manifest"])
        recovery.validate_campaign(value)
        self.assertEqual(value["config"]["gemini_schema_policy"], "local_array_bounds_v2")
        self.assertEqual(result["previous_completed_summaries"], 1)
        self.assertEqual(value["budget_microusd"], failed["budget_microusd"] - self.failed_wave["maximum_cost_microusd"])
        r.mkdir(Path(value["state_root"]))
        r.mkdir(Path(value["state_root"]) / "requests")
        ref = campaign.ensure_plan(value, 0)
        status = r.status_plan(ref["path"], ref["sha256"], phase="transcripts")
        self.assertEqual(status["imported_chunk_jobs"], 2)
        self.assertEqual(status["reserved_microusd"], 0)
        wave = self.finish_ready(ref)
        self.assertTrue(all(j["stage"] == "transcript" for j in wave["jobs"]))
        self.assertTrue(r.status_plan(ref["path"], ref["sha256"], phase="transcripts")["transcript_phase_complete"])
        for job in wave["jobs"]:
            self.assertNotIn(b'"maxItems"', core.canonical(job["request"]["body"]["generationConfig"]["responseSchema"]))

    def test_pending_canary_cannot_be_resubmitted(self):
        self.failed_canary(pending=True)
        with self.assertRaisesRegex(r.Error, "terminal reducers"):
            self.prepare()
        self.assertFalse((self.root / "new-recovery.json").exists())

    def test_successful_canary_outputs_cannot_be_discarded(self):
        self.failed_canary(mixed=True)
        with self.assertRaisesRegex(r.Error, "cannot discard"):
            self.prepare()

    def test_later_shards_or_changed_budget_block_repair(self):
        failed = self.failed_canary()
        result = self.prepare()
        value = r.read(result["manifest"])
        value["recovery"]["prior_unsettled_hold_microusd"] -= 1
        value["budget_microusd"] += 1
        with self.assertRaisesRegex(r.Error, "partition or prior paid accounting"):
            recovery.validate_campaign(value)
        r.mkdir(Path(failed["state_root"]) / "shard-0001")
        with self.assertRaisesRegex(r.Error, "isolated first-shard"):
            recovery.schema_failure_accounting(self.repair, self.parent, self.code)


if __name__ == "__main__":
    unittest.main()
