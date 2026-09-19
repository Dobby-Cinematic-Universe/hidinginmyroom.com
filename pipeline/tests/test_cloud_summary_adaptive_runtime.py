"""No-network lifecycle integration for the staged adaptive Gemini worker."""
import fcntl
import os
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_summary as worker
from pipeline.tests import test_cloud_transcription_summary as fixtures


@unittest.skipUnless(hasattr(worker, 'admission'), 'requires staged adaptive worker')
class AdaptiveWorkerTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def sources(self, count):
        self.case.available[:] = [self.case.source('recording-' + str(i)) for i in range(count)]
        self.case.prepare()

    def test_ramps_to_sixteen_without_exceeding_slot_limit(self):
        self.sources(20)
        state = worker.admission.initial_state(max_active=16)
        with worker.job_cache.scope(self.case.ref):
            for _ in range(10):
                result = self.case.cycle(max_active=16, max_new_waves=16, adaptive_state=state)
                self.assertLessEqual(result['potential_active_waves'], 16)
        self.assertEqual(state['target'], 16)
        self.assertEqual(len(self.case.client.created), 16)

    def test_enqueued_token_ceiling_precedes_new_reservation(self):
        self.sources(2)
        first = self.case.cycle(max_active=1)
        ceiling = first['enqueued_input_token_allowance']
        result = self.case.cycle(max_active=2, max_enqueued_tokens=ceiling)
        self.assertEqual(len(self.case.client.created), 1)
        self.assertEqual(len(list((self.case.worker_root / 'reservations').glob('*.json'))), 1)
        self.assertEqual(result['state'], 'waiting_remote')
        self.assertTrue(any('token_limit' in hold['reasons'] for hold in result['admission_holds']))

    def test_temporary_budget_pressure_keeps_collection_running(self):
        self.sources(2)
        manifest = worker.load_manifest(self.case.ref)
        source = self.case.available[0]
        entry = worker._ensure_record(manifest, source)
        plan, normalized = worker.r.load_plan(entry['plan']['path'], entry['plan']['sha256'])
        initial = worker.r.plan_initial(plan['request_value'], normalized)
        limit = sum(job['budget']['maximum_cost_microusd'] for job in initial)
        # Prepare a distinct bounded fixture worker; no existing budget is resealed.
        self.case.worker_root = self.case.root / 'bounded-worker'
        self.case.prepare(budget=limit)
        result = self.case.cycle(max_active=2)
        self.assertEqual(result['state'], 'waiting_remote')
        self.assertEqual(len(self.case.client.created), 1)
        self.assertTrue(any(hold.get('budget_state') == 'temporary_reservation_pressure'
                            for hold in result['admission_holds']))
        self.case.client.complete = True
        result = self.case.cycle(max_active=2)
        self.assertGreater(result['usage_estimate_microusd'], 0)
        self.assertGreater(len(self.case.client.polled), 0)

    def test_429_reduces_target_without_reposting_the_held_request(self):
        self.sources(4)
        state = worker.admission.initial_state(max_active=16)
        calls = []
        def limited(*args, **kwargs):
            calls.append(args)
            raise worker.r.client_module.BatchClientError('quota', status_code=429,
                                                          retry_after_seconds=1.5, ambiguous=False)
        with patch.object(self.case.client, 'create_batch', side_effect=limited):
            first = self.case.cycle(max_active=16, adaptive_state=state)
            second = self.case.cycle(max_active=16, adaptive_state=state)
        self.assertEqual(len(calls), 1)
        self.assertEqual(state['target'], 4)
        self.assertEqual(first['state'], 'waiting_rate_limit')
        self.assertEqual(second['new_paid_requests'], 0)
        self.assertEqual(second['potential_active_waves'], 1)
        self.assertGreater(second['unsettled_hold_microusd'], 0)

    def test_parallel_collection_drains_under_global_lock_before_paid_work(self):
        self.sources(2)
        self.case.cycle(max_active=2)
        self.case.client.complete = True
        collected = []
        def collect(groups, **kwargs):
            fd = os.open(self.case.worker_root / 'execution.lock', os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd)
            events = []
            for group in groups:
                for wave in group['waves']:
                    value = worker.r.poll_wave(group['plan']['path'], group['plan']['sha256'],
                                               wave, client=self.case.client)
                    events.append({'operation': 'poll', **value})
            collected.append(True)
            return {'events': events, 'transport_events': [], 'groups_completed': len(groups), 'workers': 4}
        original = worker.r.submit_wave
        def submit(*args, **kwargs):
            self.assertTrue(collected)
            return original(*args, **kwargs)
        with patch.object(worker.parallel_collection, 'active', return_value=True), \
                patch.object(worker.parallel_collection, 'poll_groups', side_effect=collect), \
                patch.object(worker.r, 'api_client', return_value=self.case.client), \
                patch.object(worker.r, 'submit_wave', side_effect=submit):
            result = worker.cycle(self.case.ref, allow_paid_api=True, max_active=16)
        self.assertEqual(result['collection']['workers'], 4)
        self.assertEqual(result['collection']['groups_completed'], 2)

    def test_fatal_collection_error_prevents_admission(self):
        self.sources(2)
        self.case.cycle(max_active=1)
        before = len(self.case.client.created)
        with patch.object(worker.parallel_collection, 'active', return_value=True), \
                patch.object(worker.parallel_collection, 'poll_groups',
                             side_effect=worker.parallel_collection.ParallelPollError('synthetic failure')), \
                patch.object(worker.r, 'submit_wave', side_effect=AssertionError('paid work after collection failure')):
            with self.assertRaises(worker.parallel_collection.ParallelPollError):
                worker.cycle(self.case.ref, allow_paid_api=True, max_active=16)
        self.assertEqual(len(self.case.client.created), before)

    def test_oversized_record_does_not_starve_smaller_unadmitted_record(self):
        case = self.case
        big = case.source('large-recording', text='A' * 8000)
        small = case.source('small-recording')
        case.available[:] = [big, small]
        case.prepare()
        manifest = worker.load_manifest(case.ref)
        request = worker._request(manifest, small)
        source = worker.r.sources_module.normalize_source(request['sources'][0])
        jobs = worker.fast_initial.initial_jobs([source], manifest['config'])
        ceiling = worker.admission.wave_input_tokens(jobs)
        first = case.cycle(max_active=1, max_enqueued_tokens=ceiling)
        self.assertEqual(first['state'], 'ready')
        self.assertEqual(case.client.created, [])
        second = case.cycle(max_active=1, max_enqueued_tokens=ceiling)
        self.assertEqual(len(case.client.created), 1)
        self.assertIn('large-recording', second['deferred_oversized_recordings'])
        self.assertEqual(second['state'], 'waiting_remote')

    def test_saturated_unknown_hold_wins_over_oversized_lookahead(self):
        case = self.case
        big = case.source('large-recording', text='A' * 8000)
        small = case.source('small-recording')
        case.available[:] = [big, small, case.source('still-unadmitted')]
        case.prepare()
        manifest = worker.load_manifest(case.ref)
        request = worker._request(manifest, small)
        source = worker.r.sources_module.normalize_source(request['sources'][0])
        ceiling = worker.admission.wave_input_tokens(worker.fast_initial.initial_jobs([source], manifest['config']))
        case.client.fail_post = True
        first = case.cycle(max_active=2, max_new_waves=1, max_enqueued_tokens=ceiling)
        self.assertEqual(first['potential_active_waves'], 1)
        second = case.cycle(max_active=1, max_enqueued_tokens=ceiling)
        self.assertEqual(second['state'], 'needs_review')
        self.assertGreater(second['waiting_for_summary_admission'], 0)
        self.assertEqual(len(case.client.created), 1)


if __name__ == '__main__':
    unittest.main()
