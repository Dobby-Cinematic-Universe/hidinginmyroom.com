"""Offline recovery controls: deliberate canary pause and sealed remaining budget."""
import unittest
from unittest import mock

from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary as runner
from pipeline.tests import test_transcript_summary_campaign as fixtures


class CampaignRecoveryBudgetTests(unittest.TestCase):
    setUp = fixtures.CampaignPreparationTests.setUp

    def prepare(self, *, budget=120_000_000):
        ref = runner.put(self.base / "selection.json", self.selection)
        return campaign.prepare_manifest(ref["path"], ref["sha256"], self.output,
                                         self.root, budget_microusd=budget)

    def test_remaining_allowance_is_sealed_and_cannot_overwrite_an_existing_manifest(self):
        result = self.prepare(budget=119_992_915)
        self.assertEqual(result["budget_microusd"], 119_992_915)
        sealed = runner.read(result["manifest"])
        self.assertEqual(sealed["budget_microusd"], 119_992_915)
        self.assertEqual(campaign.request_for(sealed, 0)["budget"]["max_reserved_microusd"], 119_992_915)
        with self.assertRaisesRegex(runner.Error, "existing immutable summary artifact differs"):
            self.prepare(budget=120_000_000)
        self.assertEqual(runner.read(result["manifest"]), sealed)
        self.api.assert_not_called()
        self.submit.assert_not_called()

    def test_default_and_integer_budget_bounds_remain_explicit(self):
        for index, budget in enumerate((1, 120_000_000, 0, 120_000_001, True, 1.5)):
            self.output = self.base / f"manifest-{index}.json"
            with self.subTest(budget=budget):
                if type(budget) is int and 1 <= budget <= 120_000_000:
                    self.assertEqual(self.prepare(budget=budget)["budget_microusd"], budget)
                else:
                    with self.assertRaises(runner.Error):
                        self.prepare(budget=budget)
                    self.assertFalse(self.output.exists())
        self.output = self.base / "default.json"
        ref = runner.put(self.base / "selection.json", self.selection)
        result = campaign.prepare_manifest(ref["path"], ref["sha256"], self.output, self.root)
        self.assertEqual(result["budget_microusd"], 120_000_000)
        self.api.assert_not_called()

    def test_cli_budget_is_preparation_only_and_canary_is_run_only(self):
        ref = runner.put(self.base / "selection.json", self.selection)
        arguments = ["--selection", ref["path"], "--expected-sha256", ref["sha256"],
                     "--output", str(self.output), "--state-root", str(self.root)]
        self.assertEqual(campaign.main(arguments + ["--budget-microusd", "119992915"]), 0)
        self.assertEqual(runner.read(runner.binding(self.output))["budget_microusd"], 119_992_915)
        with mock.patch.object(campaign, "prepare_manifest") as prepare:
            self.assertEqual(campaign.main(arguments + ["--canary-only"]), 2)
            prepare.assert_not_called()
        with mock.patch.object(campaign, "run") as run:
            self.assertEqual(campaign.main(["--manifest", str(self.output), "--expected-sha256",
                runner.binding(self.output)["sha256"], "--budget-microusd", "120000000"]), 2)
            run.assert_not_called()
        self.api.assert_not_called()
        self.submit.assert_not_called()


class CampaignCanaryRecoveryTests(unittest.TestCase):
    setUp = fixtures.CampaignRunTests.setUp
    index = staticmethod(fixtures.CampaignRunTests.index)
    row = fixtures.CampaignRunTests.row
    ensure = fixtures.CampaignRunTests.ensure
    inspect = fixtures.CampaignRunTests.inspect
    prepare = fixtures.CampaignRunTests.prepare
    submit = fixtures.CampaignRunTests.submit
    poll = fixtures.CampaignRunTests.poll

    def execute(self, *, canary_only=True, count=4, budget=1000):
        value = fixtures.manifest(self.root, count=count, budget=budget)
        self.ref = runner.put(self.base / "campaign.json", value)
        return campaign.run(self.ref["path"], self.ref["sha256"],
                            allow_paid_api=True, canary_only=canary_only)

    def test_canary_pauses_after_exports_then_same_manifest_resumes_without_duplicate_submission(self):
        paused = self.execute()
        self.assertEqual(paused["state"], "awaiting_canary_review")
        self.assertTrue(paused["canary_only"])
        self.assertEqual(paused["started_shards"], 1)
        self.assertEqual(paused["completed_shards"], 1)
        self.assertEqual(paused["total_shards"], 4)
        self.assertEqual(paused["pending_waves"], 0)
        self.assertTrue(all(index == 0 for _, index in self.events))
        self.assertIn(("export", 0), self.events)
        self.assertIn(("reader", 0), self.events)
        self.assertFalse((self.root / "shard-0001").exists())
        sealed = dict(self.ref)
        resumed = self.execute(canary_only=False)
        self.assertEqual(self.ref, sealed)
        self.assertEqual(resumed["state"], "completed")
        self.assertFalse(resumed["canary_only"])
        self.assertEqual([index for name, index in self.events if name == "submit"], [0, 1, 2, 3])

    def test_failed_or_budget_paused_canary_never_fans_out(self):
        self.fail_poll.add(0)
        result = self.execute()
        self.assertEqual(result["state"], "needs_review")
        self.assertTrue(all(index == 0 for _, index in self.events))
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 0)])

    def test_pending_canary_resume_does_not_repeat_a_paid_submission(self):
        self.mocks[7].side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.execute()
        self.mocks[7].side_effect = None
        result = self.execute()
        self.assertEqual(result["state"], "awaiting_canary_review")
        self.assertEqual([event for event in self.events if event[0] == "submit"], [("submit", 0)])
        self.assertTrue(all(index == 0 for _, index in self.events))

    def test_canary_mode_refuses_already_prepared_later_shards(self):
        self.execute(canary_only=False)
        self.events.clear()
        with self.assertRaisesRegex(runner.Error, "previously prepared later shards"):
            self.execute()
        self.assertEqual(self.events, [])

    def test_canary_mode_still_requires_paid_consent_and_obeys_budget(self):
        value = fixtures.manifest(self.root, count=4, budget=99)
        ref = runner.put(self.base / "campaign.json", value)
        with self.assertRaisesRegex(runner.Error, "allow-paid-api"):
            campaign.run(ref["path"], ref["sha256"], canary_only=True)
        self.assertEqual(self.events, [])
        result = campaign.run(ref["path"], ref["sha256"], allow_paid_api=True, canary_only=True)
        self.assertEqual(result["state"], "budget_paused")
        self.assertFalse(any(name == "submit" for name, _ in self.events))
        self.assertTrue(all(index == 0 for _, index in self.events))

    def test_cli_deliberate_canary_review_pause_exits_successfully(self):
        with mock.patch.object(campaign, "run", return_value={"state": "awaiting_canary_review"}) as run:
            code = campaign.main(["--manifest", str(self.base / "campaign.json"),
                                  "--expected-sha256", "a" * 64, "--allow-paid-api", "--canary-only"])
        self.assertEqual(code, 0)
        run.assert_called_once_with(str(self.base / "campaign.json"), "a" * 64,
                                    allow_paid_api=True, canary_only=True)


if __name__ == "__main__":
    unittest.main()
