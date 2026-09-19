"""High-ceiling admission and collection tests, without real provider calls."""
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_summary as worker
from pipeline import cloud_transcription_summary_admission as admission
from pipeline import cloud_transcription_summary_parallel as parallel
from pipeline.tests import test_cloud_transcription_summary as fixtures


@unittest.skipUnless(admission.MAX_ACTIVE == 100, 'requires the 100-slot runtime')
class HundredSlotTests(unittest.TestCase):
    def assess(self, waves, **options):
        values = dict(active_waves=waves, candidate_input_tokens=100,
                      candidate_cost_microusd=100, settled_microusd=1000,
                      held_microusd=1000, budget_limit_microusd=100000, target=100)
        values.update(options)
        return admission.assess_admission(**values)

    def test_hundred_slot_boundary_includes_unknown_holds(self):
        waves = [{'wave_id': str(i), 'input_tokens': 100,
                  'state': 'orphan' if i == 0 else 'ambiguous' if i == 1 else 'pending'} for i in range(99)]
        self.assertTrue(self.assess(waves)['allowed'])
        waves.append({'wave_id': '99', 'input_tokens': 100, 'state': 'pending'})
        blocked = self.assess(waves)
        self.assertEqual(blocked['unknown_held_waves'], 2)
        self.assertFalse(blocked['allowed'])
        self.assertIn('slot_limit', blocked['reasons'])
        with self.assertRaises(admission.AdmissionError):
            self.assess(waves, target=101)

    def test_budget_and_tokens_can_stop_far_below_hundred(self):
        waves = [{'wave_id': 'existing', 'state': 'pending', 'input_tokens': 3_000_000}]
        self.assertIn('token_limit', self.assess(waves)['reasons'])
        self.assertIn('budget_limit', self.assess([], held_microusd=99000)['reasons'])

    def test_slow_start_reaches_hundred_then_429_recovers_additively(self):
        state = admission.initial_state(start=16, max_active=100)
        values = []
        for now in range(6):
            state = admission.advance(state, now_seconds=now, successful_cycle=True)
            values.append(state['target'])
        self.assertEqual(values, [16, 32, 32, 64, 64, 100])
        state = admission.advance(state, now_seconds=10, rate_limited=True, retry_after_seconds=120)
        self.assertEqual(state['target'], 50)
        state = admission.advance(state, now_seconds=129, successful_cycle=True)
        self.assertEqual(state['target'], 50)
        state = admission.advance(state, now_seconds=130, successful_cycle=True)
        state = admission.advance(state, now_seconds=131, successful_cycle=True)
        self.assertEqual(state['target'], 52)

    def test_collector_accepts_hundred_distinct_records_not_101(self):
        groups = [{'plan': {'path': '/private/record-' + str(i) + '/plan.json', 'sha256': 'a' * 64},
                   'waves': ['summarywave_' + format(i, '032x')]} for i in range(101)]
        self.assertEqual(len(parallel._groups(groups[:100])), 100)
        with self.assertRaises(parallel.ParallelPollError):
            parallel._groups(groups)

    def test_real_lifecycle_stops_at_hundred_and_can_collect_hundred_groups(self):
        case = fixtures.WorkerTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.available[:] = [case.source('recording-' + str(i)) for i in range(101)]
        case.prepare(budget=50_000_000)
        with worker.job_cache.scope(case.ref):
            first = case.cycle(max_active=100, max_new_waves=100)
            self.assertEqual(first['new_paid_requests'], 100)
            self.assertEqual(first['potential_active_waves'], 100)
            self.assertEqual(len(case.client.created), 100)
            def collect(groups, **kwargs):
                self.assertEqual(len(parallel._groups(groups)), 100)
                return {'events': [{'state': 'remote_pending', 'operation': 'poll', 'wave_id': g['waves'][0]}
                                   for g in groups], 'transport_events': [], 'groups_completed': 100, 'workers': 4}
            with patch.object(parallel, 'active', return_value=True), \
                    patch.object(parallel, 'poll_groups', side_effect=collect):
                result = worker.cycle(case.ref, allow_paid_api=True, max_active=100, max_new_waves=100)
            self.assertEqual(result['new_paid_requests'], 0)
            self.assertEqual(result['collection']['groups_completed'], 100)
        self.assertEqual(len(case.client.created), 100)


if __name__ == '__main__':
    unittest.main()
