"""Offline admission, accounting and finite Gemini campaign orchestration."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary as runner
from pipeline import transcript_summary_core as core
from pipeline.tests.test_transcript_summary_core import source


def specification(recording):
    return {"transcript": {"path": "/offline-fixtures/" + recording + ".json", "sha256": "a" * 64},
            "format": "third_party", "recording_id": recording, "title": "Fixture",
            "date": None, "completion": None}


def manifest(root, *, count=1, budget=1000, active=2):
    return {"kind": campaign.KIND, "schema_version": 1, "state_root": str(root),
            "shards": [[specification("recording-" + str(index))] for index in range(count)],
            "config": {**core.DEFAULT_CONFIG, "timeline_profile": "gemini_flash_batch"},
            "budget_microusd": budget, "max_active_shards": active,
            "poll_seconds": 15, "max_runtime_seconds": 60,
            "cloud": {"processing_approved": True, "paid_tier_confirmed": True},
            "implementation": campaign.implementation()}


def response(job, *, prompt=100, candidates=20, thoughts=30, total=150):
    return {"modelVersion": job["model"], "usageMetadata": {
        "promptTokenCount": prompt, "candidatesTokenCount": candidates,
        "thoughtsTokenCount": thoughts, "totalTokenCount": total}}


class CampaignManifestTests(unittest.TestCase):
    def setUp(self):
        self.value = manifest(Path("/offline-fixtures/campaign"))

    def test_manifest_is_explicit_finite_and_gemini_transcript_only(self):
        self.assertEqual(campaign.validate_manifest(self.value), self.value)
        for field, value in (("budget_microusd", 0), ("budget_microusd", 120_000_001),
                             ("budget_microusd", True), ("max_active_shards", 5),
                             ("poll_seconds", 0), ("max_runtime_seconds", 14 * 86400 + 1),
                             ("shards", []), ("shards", [[]]), ("implementation", {})):
            changed = deepcopy(self.value)
            changed[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(runner.Error):
                campaign.validate_manifest(changed)
        for profile in ("openai_mini_batch", "anthropic_sonnet_batch"):
            for key in ("transcript_profile", "timeline_profile"):
                changed = deepcopy(self.value)
                changed["config"][key] = profile
                with self.subTest(profile=profile, key=key), self.assertRaises(runner.Error):
                    campaign.validate_manifest(changed)
        changed = deepcopy(self.value)
        changed["config"]["broader_synthesis"] = {
            "profile": "anthropic_sonnet_batch", "yearly": False, "archive": False, "topics": []}
        with self.assertRaises(runner.Error):
            campaign.validate_manifest(changed)

    def test_duplicate_recordings_and_oversized_shards_are_rejected(self):
        for shards in ([self.value["shards"][0], self.value["shards"][0]],
                       [[specification(str(index)) for index in range(33)]]):
            with self.subTest(count=len(shards)), self.assertRaises(runner.Error):
                campaign.validate_manifest({**self.value, "shards": shards})

    def test_cloud_consent_requires_literal_true_values(self):
        for bad in (False, None, "true", 1):
            for key in ("processing_approved", "paid_tier_confirmed"):
                changed = deepcopy(self.value)
                changed["cloud"][key] = bad
                with self.subTest(key=key, bad=bad), self.assertRaises(runner.Error):
                    campaign.validate_manifest(changed)

    def test_shards_disable_paid_retries_and_keep_provider_selection(self):
        request = campaign.request_for(self.value, 0)
        self.assertEqual(request["budget"]["max_attempts_per_job"], 1)
        self.assertEqual(request["config"]["transcript_profile"], "gemini_flash_batch")
        self.assertEqual(request["config"]["timeline_profile"], "gemini_flash_batch")
        self.assertNotIn("broader_synthesis", request["config"])
        self.assertEqual(request["sources"], self.value["shards"][0])


class CampaignPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "campaign"
        self.output = self.base / "manifest.json"
        spec = {**specification("one"), "format": "longform"}
        self.selection = {"kind": "himr_private_transcript_archive_selection", "schema_version": 1,
                          "sources": [spec], "shards": [[spec]],
                          "semantics": {"third_party_transcripts_included": False}}
        self.api = self.enterContext(mock.patch.object(runner, "api_client"))
        self.http = self.enterContext(mock.patch.object(runner.client_module, "_transport"))
        self.submit = self.enterContext(mock.patch.object(runner, "submit_wave"))
        self.enterContext(mock.patch("builtins.print"))

    def prepare(self, value=None, *, suffix=""):
        ref = runner.put(self.base / ("selection" + suffix + ".json"),
                         self.selection if value is None else value)
        return campaign.prepare_manifest(ref["path"], ref["sha256"], self.output, self.root)

    def test_prepare_seals_selection_offline_without_keys_or_source_reads(self):
        with mock.patch.object(runner.sources_module, "normalize_source") as normalize:
            result = self.prepare()
        self.assertEqual(result["state"], "prepared_offline_campaign")
        self.assertEqual(result["paid_requests_started"], 0)
        self.assertEqual(result["selected_transcripts"], 1)
        sealed = runner.read(result["manifest"])
        self.assertEqual(runner.read(sealed["selection"]), self.selection)
        self.assertEqual(sealed["shards"], self.selection["shards"])
        self.assertEqual(sealed["config"]["max_chunk_input_bytes"], 24000)
        self.assertFalse(self.root.exists())
        normalize.assert_not_called()
        self.api.assert_not_called()
        self.http.assert_not_called()
        self.submit.assert_not_called()

    def test_empty_inconsistent_or_malformed_selection_is_rejected(self):
        cases = [{**self.selection, "sources": []},
                 {**self.selection, "sources": [], "shards": []},
                 {**self.selection, "schema_version": True},
                 {**self.selection, "shards": None},
                 {**self.selection, "semantics": None}]
        third_party = deepcopy(self.selection)
        third_party["sources"][0]["format"] = "third_party"
        cases.append(third_party)
        for index, value in enumerate(cases):
            self.output = self.base / ("invalid-manifest-" + str(index) + ".json")
            with self.subTest(index=index), self.assertRaises(runner.Error):
                self.prepare(value, suffix=str(index))
            self.assertFalse(self.output.exists())
        self.api.assert_not_called()
        self.http.assert_not_called()
        self.submit.assert_not_called()

    def test_prepare_cli_disallows_paid_flag_without_calling_preparation(self):
        ref = runner.put(self.base / "selection.json", self.selection)
        with mock.patch.object(campaign, "prepare_manifest") as prepare:
            code = campaign.main(["--selection", ref["path"], "--expected-sha256", ref["sha256"],
                                  "--output", str(self.output), "--state-root", str(self.root),
                                  "--allow-paid-api"])
        self.assertEqual(code, 2)
        prepare.assert_not_called()
        self.api.assert_not_called()
        self.http.assert_not_called()
        self.submit.assert_not_called()

    def test_changed_selection_binding_blocks_run_before_paid_or_workspace_actions(self):
        prepared = self.prepare()
        changed = {**self.selection, "review_note": "Changed after sealing"}
        replacement = self.base / "changed-selection.json"
        runner.put(replacement, changed)
        replacement.replace(self.base / "selection.json")
        ref = prepared["manifest"]
        with self.assertRaisesRegex(runner.Error, "digest differs"):
            campaign.run(ref["path"], ref["sha256"], allow_paid_api=True)
        self.assertFalse(self.root.exists())
        self.api.assert_not_called()
        self.http.assert_not_called()
        self.submit.assert_not_called()


class CampaignAccountingTests(unittest.TestCase):
    def setUp(self):
        self.job = core.initial_jobs([source()])[0]
        self.hold = self.job["budget"]["maximum_cost_microusd"]

    def test_usage_counts_thinking_and_unknown_extra_output_tokens(self):
        for total in (150, 160):
            measured, valid = campaign.usage_cost(self.job, response(self.job, total=total))
            self.assertTrue(valid)
            self.assertEqual(measured, (100 * 3 + 7) // 8 + ((total - 100) * 15 + 7) // 8)
        value = response(self.job, thoughts=0, total=120)
        value["usageMetadata"].pop("thoughtsTokenCount")
        self.assertTrue(campaign.usage_cost(self.job, value)[1])

    def test_missing_malformed_inconsistent_or_unexpected_usage_keeps_full_hold(self):
        cases = [None, {}, {"modelVersion": self.job["model"]},
                 response(self.job, prompt=0), response(self.job, prompt=True),
                 response(self.job, total=149), response(self.job, total=10**9),
                 response(self.job, thoughts=-1)]
        for key, value in (("modelVersion", "gemini-other"), ("usageMetadata", [])):
            item = response(self.job)
            item[key] = value
            cases.append(item)
        for key, value in (("toolUsePromptTokenCount", 1), ("cachedContentTokenCount", 10),
                           ("totalTokenCount", "150"), ("candidatesTokenCount", None)):
            item = response(self.job)
            item["usageMetadata"][key] = value
            cases.append(item)
        for value in cases:
            with self.subTest(value=value):
                self.assertEqual(campaign.usage_cost(self.job, value), (self.hold, False))

    def test_usage_over_reservation_stops_instead_of_releasing_hold(self):
        for value in (response(self.job, prompt=self.job["budget"]["input_token_allowance"] + 1,
                               candidates=0, thoughts=0, total=self.job["budget"]["input_token_allowance"] + 1),
                      response(self.job, candidates=65537, thoughts=0, total=65637)):
            with self.subTest(value=value), self.assertRaisesRegex(runner.Error, "exceeds conservative reservation"):
                campaign.usage_cost(self.job, value)
        foreign = {**self.job, "provider": "anthropic"}
        with self.assertRaises(runner.Error):
            campaign.usage_cost(foreign, response(self.job))

    def test_accounting_holds_all_unsettled_intents_and_missing_rows(self):
        waves = [{"wave_id": "summarywave_" + str(index) * 32, "jobs": [self.job],
                  "maximum_cost_microusd": self.hold} for index in range(4)]
        state = {"waves": waves, "collections": {
            waves[0]["wave_id"]: {"capture": "measured"},
            waves[1]["wave_id"]: {"capture": "missing"}}}
        captures = {"measured": {"items": [{"custom_id": self.job["job_id"],
                                              "response": response(self.job), "error": None}]},
                    "missing": {"items": []}}
        # The fourth wave is only prepared: no intent, so no paid reservation.
        def exists(path):
            return waves[3]["wave_id"] not in str(path)
        plan = {"request_value": {"state_root": "/offline-fixtures/campaign/shard-0000"}}
        with mock.patch.object(runner.safe, "exists", side_effect=exists), \
                mock.patch.object(runner, "read", side_effect=lambda ref: captures[ref]):
            totals = campaign.accounted_state(plan, state)
        charge = campaign.usage_cost(self.job, response(self.job))[0]
        self.assertEqual(totals, {"usage_estimate_microusd": charge,
                                 "unsettled_hold_microusd": self.hold * 2,
                                 "accounted_microusd": charge + self.hold * 2})


class CampaignRunTests(unittest.TestCase):
    """Real private controller files, synthetic shard runner, never HTTP or keys."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "campaign"
        self.events, self.rows, self.wave_costs = [], {}, {}
        self.fail_poll = set()
        self.provider, self.stage = "gemini", "chunk"
        self.peak_active = 0
        patches = [
            mock.patch.object(campaign, "ensure_plan", side_effect=self.ensure),
            mock.patch.object(campaign, "inspect_shard", side_effect=self.inspect),
            mock.patch.object(runner, "prepare_plan", side_effect=self.prepare),
            mock.patch.object(runner, "submit_wave", side_effect=self.submit),
            mock.patch.object(runner, "poll_wave", side_effect=self.poll),
            mock.patch.object(runner, "export_plan", side_effect=lambda *args, **kw: self.events.append(("export", self.index(args[0])))),
            mock.patch.object(campaign.reader, "export_reader", side_effect=lambda *args, **kw: self.events.append(("reader", self.index(args[0])))),
            mock.patch.object(campaign.time, "sleep"),
            mock.patch.object(runner, "api_client", side_effect=AssertionError("offline test must not load credentials")),
            mock.patch("builtins.print"),
        ]
        self.mocks = [patch.start() for patch in patches]
        for patch in patches:
            self.addCleanup(patch.stop)

    @staticmethod
    def index(path):
        return int(Path(path).parent.name.removeprefix("shard-"))

    def row(self, index, *, complete=False, settled=0, held=0):
        return {"plan": {"path": str(self.root / f"shard-{index:04d}" / "plan.json"), "sha256": "a" * 64},
                "status": {"transcript_phase_complete": complete, "failed_jobs": 0,
                           "ambiguous_waves": [], "rejected_waves": [], "pending_waves": [],
                           "transcript_summaries_complete": int(complete)},
                "usage_estimate_microusd": settled, "unsettled_hold_microusd": held,
                "accounted_microusd": settled + held}

    def ensure(self, value, index):
        self.events.append(("ensure", index))
        self.rows.setdefault(index, self.row(index))
        folder = self.root / f"shard-{index:04d}"
        runner.mkdir(folder)
        runner.put(folder / "plan.json", {"offline_shard": index})
        return self.rows[index]["plan"]

    def inspect(self, ref):
        return deepcopy(self.rows[self.index(ref["path"])])

    def prepare(self, path, expected, *, phase):
        self.assertEqual(phase, "transcripts")
        index = self.index(path)
        self.events.append(("prepare", index))
        wave_id = "summarywave_" + f"{index:032x}"
        folder = Path(path).parent / "waves"
        runner.mkdir(folder)
        runner.mkdir(folder / wave_id)
        runner.put(folder / wave_id / "wave.json", {
            "provider": self.provider, "jobs": [{"stage": self.stage}],
            "maximum_cost_microusd": self.wave_costs.get(index, 100)})
        return {"state": "prepared", "wave_id": wave_id}

    def submit(self, path, expected, wave_id, *, allow_paid_api, phase):
        self.assertIs(allow_paid_api, True)
        self.assertEqual(phase, "transcripts")
        index = self.index(path)
        self.events.append(("submit", index))
        self.assertFalse(self.rows[index]["status"]["pending_waves"])
        self.rows[index]["status"]["pending_waves"] = [wave_id]
        self.rows[index]["unsettled_hold_microusd"] += self.wave_costs.get(index, 100)
        self.rows[index]["accounted_microusd"] += self.wave_costs.get(index, 100)
        self.peak_active = max(self.peak_active, sum(bool(row["status"]["pending_waves"]) for row in self.rows.values()))
        return {"state": "submitted"}

    def poll(self, path, expected, wave_id):
        index = self.index(path)
        self.events.append(("poll", index))
        row = self.rows[index]
        row["status"]["pending_waves"] = []
        if index in self.fail_poll:
            row["status"]["failed_jobs"] = 1
        else:
            row["status"]["transcript_phase_complete"] = True
            row["status"]["transcript_summaries_complete"] = 1
            row["usage_estimate_microusd"] += 10
            row["unsettled_hold_microusd"] = 0
            row["accounted_microusd"] = row["usage_estimate_microusd"]
        return {"state": "completed"}

    def execute(self, *, count=1, budget=1000, active=2, allow_paid=True):
        value = manifest(self.root, count=count, budget=budget, active=active)
        ref = runner.put(self.base / "campaign.json", value)
        return campaign.run(ref["path"], ref["sha256"], allow_paid_api=allow_paid)

    def test_no_paid_flag_stops_before_workspace_creation_or_runner_calls(self):
        with self.assertRaisesRegex(runner.Error, "allow-paid-api"):
            self.execute(allow_paid=False)
        self.assertFalse(self.root.exists())
        self.assertEqual(self.events, [])

    def test_canary_completes_before_fanout_and_concurrency_is_bounded(self):
        result = self.execute(count=6, active=2)
        self.assertEqual(result["state"], "completed")
        self.assertLess(self.events.index(("poll", 0)), self.events.index(("ensure", 1)))
        self.assertLessEqual(self.peak_active, 2)
        self.assertEqual([index for name, index in self.events if name == "submit"], list(range(6)))
        self.assertEqual([index for name, index in self.events if name == "reader"], list(range(6)))
        self.assertFalse(result["synthesis_started"])
        self.assertFalse(result["automatic_paid_retries"])

    def test_global_gate_includes_settled_usage_and_unreleased_holds(self):
        self.rows[0] = self.row(0, complete=True, settled=30, held=60)
        self.wave_costs[1] = 11
        result = self.execute(count=2, budget=100)
        self.assertEqual(result["state"], "budget_paused")
        self.assertFalse(any(name == "submit" for name, _ in self.events))
        self.assertEqual(result["usage_estimate_microusd"], 30)
        self.assertEqual(result["unsettled_hold_microusd"], 60)

    def test_global_gate_allows_exact_ceiling_but_not_one_more(self):
        self.rows[0] = self.row(0, complete=True, settled=90)
        self.wave_costs[1] = 10
        result = self.execute(count=2, budget=100)
        self.assertEqual(result["state"], "completed")
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 1)])

    def test_canary_failure_stops_without_new_shards_or_paid_retry(self):
        self.fail_poll.add(0)
        result = self.execute(count=3)
        self.assertEqual(result["state"], "needs_review")
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 0)])
        self.assertEqual([event for event in self.events if event[0] == "prepare"], [("prepare", 0)])
        self.assertFalse(any(index > 0 for _, index in self.events))

    def test_resume_polls_pending_wave_without_duplicate_post(self):
        self.mocks[7].side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.execute()
        self.mocks[7].side_effect = None
        result = self.execute()
        self.assertEqual(result["state"], "completed")
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 0)])
        self.assertEqual([event for event in self.events if event[0] == "poll"], [("poll", 0)])

    def test_failed_chunk_does_not_block_ready_reducers_or_unrelated_shards(self):
        visits = []
        def poll(path, expected, wave_id):
            index = self.index(path)
            if index != 1:
                return self.poll(path, expected, wave_id)
            visits.append(index)
            self.events.append(("poll", index))
            row = self.rows[index]
            row["status"].update(pending_waves=[], failed_jobs=1,
                                  ready_jobs=1 if len(visits) == 1 else 0,
                                  transcript_summaries_complete=0 if len(visits) == 1 else 1)
            row.update(usage_estimate_microusd=20, unsettled_hold_microusd=0, accounted_microusd=20)
        self.mocks[4].side_effect = poll
        result = self.execute(count=3)
        self.assertEqual(result["state"], "needs_review")
        self.assertEqual(self.events.count(("submit", 1)), 2)
        self.assertEqual(result["transcript_summaries_complete"], 3)
        self.assertIn(("submit", 2), self.events)

    def test_sigterm_during_post_finishes_receipt_and_prevents_next_submission(self):
        def submit(*args, **kwargs):
            campaign.signal.raise_signal(campaign.signal.SIGTERM)
            return self.submit(*args, **kwargs)
        self.mocks[3].side_effect = submit
        result = self.execute(count=3)
        self.assertEqual(result["state"], "paused")
        self.assertEqual(result["pending_waves"], 1)
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 0)])

    def test_failed_shard_resumes_an_unsubmitted_prepared_healthy_wave(self):
        self.rows[0] = self.row(0, complete=True, settled=10)
        self.rows[1] = self.row(1, settled=10)
        self.rows[1]["status"].update(failed_jobs=1, ready_jobs=0,
                                       prepared_waves=["summarywave_" + "1" * 32])
        self.fail_poll.add(1)
        def poll(*args):
            result = self.poll(*args)
            self.rows[1]["status"]["prepared_waves"] = []
            self.rows[1]["status"]["transcript_summaries_complete"] = 1
            return result
        self.mocks[4].side_effect = poll
        result = self.execute(count=2)
        self.assertEqual(result["state"], "needs_review")
        self.assertEqual(self.events.count(("submit", 1)), 1)
        self.assertEqual(result["transcript_summaries_complete"], 2)

    def test_resume_exports_already_completed_shard_without_paid_work(self):
        self.rows[0] = self.row(0, complete=True, settled=10)
        result = self.execute()
        self.assertEqual(result["state"], "completed")
        self.assertIn(("export", 0), self.events)
        self.assertIn(("reader", 0), self.events)
        self.assertFalse(any(name == "submit" for name, _ in self.events))

    def test_ambiguous_or_rejected_wave_is_not_automatically_resubmitted(self):
        for field in ("ambiguous_waves", "rejected_waves"):
            self.rows[0] = self.row(0, held=100)
            self.rows[0]["status"][field] = ["summarywave_" + "0" * 32]
            with self.subTest(field=field), self.assertRaisesRegex(runner.Error, "reconciliation"):
                self.execute()
            self.assertFalse(any(name in {"prepare", "submit"} for name, _ in self.events))

    def test_wave_provider_and_stage_are_checked_again_before_paid_call(self):
        self.provider = "anthropic"
        with self.assertRaisesRegex(runner.Error, "escaped Gemini transcript phase"):
            self.execute()
        self.assertFalse(any(name == "submit" for name, _ in self.events))

    def test_synthesis_wave_is_rejected_before_paid_call(self):
        self.stage = "timeline"
        with self.assertRaisesRegex(runner.Error, "escaped Gemini transcript phase"):
            self.execute()
        self.assertFalse(any(name == "submit" for name, _ in self.events))

    def test_finite_runtime_stops_with_pending_work_without_resubmitting(self):
        clock = {"now": 0}
        self.mocks[7].side_effect = lambda _: clock.update(now=60)
        with mock.patch.object(campaign.time, "monotonic", side_effect=lambda: clock["now"]):
            with self.assertRaisesRegex(runner.Error, "finite campaign time limit"):
                self.execute()
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 0)])
        self.assertTrue(self.rows[0]["status"]["pending_waves"])

    def test_cli_marks_stale_counts_after_a_failed_submission(self):
        value = manifest(self.root)
        ref = runner.put(self.base / "campaign.json", value)
        self.mocks[3].side_effect = runner.client_module.BatchClientError(
            "Gemini request failed with an HTTP status", status_code=400, ambiguous=False)
        result = campaign.main(["--manifest", ref["path"], "--expected-sha256", ref["sha256"],
                                "--allow-paid-api"])
        self.assertEqual(result, 2)
        self.mocks[3].assert_called_once()
        saved = runner.read(runner.binding(self.root / "status.json"))
        self.assertEqual(saved["state"], "stopped_for_review")
        self.assertTrue(saved["counts_may_be_stale"])
        self.assertIs(saved["automatic_paid_retries"], False)


if __name__ == "__main__":
    unittest.main()
