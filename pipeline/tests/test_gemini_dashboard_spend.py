"""Opt-in dollar admission removal; receipts and every non-dollar guard remain."""
from copy import deepcopy
import pickle
import unittest
from unittest.mock import patch

from pipeline import gemini_dashboard_spend as spend
from pipeline.tests import test_cloud_transcription_summary as fixtures

worker, r = fixtures.worker, fixtures.r


@unittest.skipUnless(hasattr(worker, '_record_snapshot'), 'requires retained worker')
class SpendTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.prepare(budget=1)
        self.original = r.read_bytes(self.case.ref)
        self.addCleanup(spend.install(worker, self.case.ref))

    def test_small_historical_cap_does_not_block_or_rewrite_manifest(self):
        result = self.case.cycle()
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertGreater(result['accounted_microusd'], 1)
        self.assertIsNone(result['max_total_budget_microusd'])
        self.assertFalse(result['local_spend_limit_enforced'])
        self.assertEqual(result['historical_worker_budget_microusd'], 1)
        self.assertEqual(r.read_bytes(self.case.ref), self.original)
        self.case.client.complete = True
        self.case.cycle()
        result = self.case.cycle()
        self.assertEqual(result['transcript_summaries_complete'], 1)
        self.assertEqual(self.case.cycle()['new_paid_requests'], 0)

    def test_unknown_post_keeps_reservation_and_cannot_repost(self):
        self.case.client.fail_post = True
        first = self.case.cycle()
        second = self.case.cycle()
        self.assertEqual(len(self.case.client.created), 1)
        self.assertGreater(first['unsettled_hold_microusd'], 1)
        self.assertEqual(second['unsettled_hold_microusd'], first['unsettled_hold_microusd'])
        self.assertEqual(second['new_paid_requests'], 0)

    def test_admission_keeps_queue_and_cooldown_guards(self):
        args = dict(active_waves=[dict(wave_id='unknown', state='ambiguous', input_tokens=100)],
            candidate_input_tokens=100, candidate_cost_microusd=10**9,
            settled_microusd=10**9, held_microusd=10**9, budget_limit_microusd=1,
            target=1, token_limit=100, now_seconds=1, cooldown_until_seconds=2)
        decision = worker.admission.assess_admission(**args)
        self.assertEqual(decision['reasons'], ['rate_limit_cooldown', 'slot_limit', 'token_limit'])
        self.assertEqual(decision['accounted_microusd'], 2*10**9)
        self.assertIsNone(decision['budget_remaining_microusd'])
        with self.assertRaises(worker.admission.AdmissionError):
            worker.admission.assess_admission(**{**args, 'held_microusd': -1})

    def test_job_limits_and_other_workspaces_still_have_guards(self):
        self.case.cycle()
        entry = next(iter(self.case.snapshot()['records'].values()))['entry']
        plan, sources = r.load_plan(entry['plan']['path'], entry['plan']['sha256'])
        state = r.load_state(plan, sources)
        self.assertGreater(state['reserved_microusd'], 1)
        invalid = deepcopy(plan)
        invalid['request_value']['limits']['max_jobs'] = 0
        with self.assertRaisesRegex(r.Error, 'job or reservation ceiling'):
            r.load_state(invalid, sources)
        other = deepcopy(plan)
        other['request_value']['config']['timeline_profile'] = 'anthropic_sonnet_batch'
        self.assertFalse(r._dashboard_spend_plan(other))
        other['request_value']['config'] = plan['request_value']['config']
        other['request_value']['state_root'] = str(self.case.root / 'unrelated')
        self.assertFalse(r._dashboard_spend_plan(other))

    def test_missing_paid_consent_not_bypassed(self):
        with self.assertRaises(worker.SummaryWorkerError):
            worker.cycle(self.case.ref, client=self.case.client)
        self.assertEqual(self.case.client.created, [])

    def test_source_adaptation_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'guard changed'):
            spend.variant(worker._snapshot, [('not present in this function', 'pass')])

    def test_collector_initializer_is_picklable(self):
        original = worker.parallel_collection._initialize
        self.addCleanup(setattr, worker.parallel_collection, '_initialize', original)
        spend.install_collectors(worker, self.case.ref)
        restored = pickle.loads(pickle.dumps(worker.parallel_collection._initialize))
        self.assertEqual(restored.args, worker.parallel_collection._initialize.args)
