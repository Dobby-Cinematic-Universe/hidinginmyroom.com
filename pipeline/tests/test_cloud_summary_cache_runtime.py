"""Paid-lifecycle regressions for the separately staged cached Gemini worker."""
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_summary as worker
from pipeline.tests import test_cloud_transcription_summary as fixtures


@unittest.skipUnless(hasattr(worker, 'job_cache'), 'requires the cached summary runtime')
class CachedWorkerTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.prepare()

    def test_completed_jobs_survive_cold_restart_without_rebuilding_or_rebuying(self):
        case = self.case
        with worker.job_cache.scope(case.ref):
            self.assertEqual(case.cycle()['new_paid_requests'], 1)
            case.client.complete = True
            self.assertEqual(case.cycle()['new_paid_requests'], 1)
            self.assertEqual(case.cycle()['transcript_summaries_complete'], 1)
        with patch.object(worker.fast_initial, 'initial_jobs', side_effect=AssertionError('retained chunks rebuilt')):
            with worker.job_cache.scope(case.ref):
                result = case.cycle()
                self.assertEqual(result['new_paid_requests'], 0)
                self.assertEqual(result['transcript_summaries_complete'], 1)
        self.assertEqual(len(case.client.created), 2)

    def test_unknown_paid_post_is_not_repeated_with_cache_enabled(self):
        case = self.case
        case.client.fail_post = True
        with worker.job_cache.scope(case.ref):
            case.cycle()
            result = case.cycle()
        self.assertEqual(len(case.client.created), 1)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertGreater(result['unsettled_hold_microusd'], 0)

    def test_anonymous_transcript_stays_held(self):
        case = self.case
        case.available[:] = [case.source('anonymous-recording', speaker='SPEAKER_0000')]
        with worker.job_cache.scope(case.ref):
            result = case.cycle()
        self.assertEqual(result['speaker_identity_pending'], 1)
        self.assertEqual(case.client.created, [])

    def test_unchanged_record_snapshot_reused_and_changed_receipts_rechecked(self):
        case = self.case
        with worker.job_cache.scope(case.ref):
            case.cycle()
        original = worker._record_snapshot
        with worker.job_cache.scope(case.ref):
            with patch.object(worker, '_record_snapshot', wraps=original) as inspect:
                case.snapshot()
                self.assertEqual(inspect.call_count, 1)
                case.snapshot()
                self.assertEqual(inspect.call_count, 1)
                case.client.complete = True
                case.cycle()
                self.assertGreater(inspect.call_count, 1)

    def test_root_reservation_loss_still_fails_after_record_cache_hit(self):
        case = self.case
        with worker.job_cache.scope(case.ref):
            case.cycle()
            case.snapshot()
            reservation = next((case.worker_root / 'reservations').glob('*.json'))
            reservation.rename(reservation.with_suffix('.saved'))
            with self.assertRaises(RuntimeError):
                case.snapshot()
        self.assertEqual(len(case.client.created), 1)

    def test_unchanged_completed_export_does_not_repeat_full_replay(self):
        case = self.case
        with worker.job_cache.scope(case.ref):
            case.cycle()
            case.client.complete = True
            case.cycle()
            case.cycle()
            worker.export(case.ref)
            with patch.object(worker.r, 'export_plan', side_effect=AssertionError('completed export replayed')):
                result = case.cycle()
                exported = worker.export(case.ref)
            self.assertEqual(result['new_paid_requests'], 0)
            self.assertEqual(exported['transcript_summaries'], 1)
        self.assertEqual(len(case.client.created), 2)

    def test_dispatch_validates_release_before_entering_cache_scope(self):
        events = []
        args = SimpleNamespace(command='status', manifest=self.case.ref['path'],
                               expected_sha256=self.case.ref['sha256'])
        @contextmanager
        def scoped(reference, **options):
            self.assertEqual(reference, self.case.ref)
            events.append('cache')
            yield
        with (patch.object(worker, 'load_manifest', side_effect=lambda ref: events.append('validate')),
                patch.object(worker.job_cache, 'scope', side_effect=scoped),
                patch.object(worker, 'dispatch', side_effect=lambda args: events.append('dispatch'))):
            worker.dispatch_cached(args)
        self.assertEqual(events, ['validate', 'cache', 'dispatch'])


if __name__ == '__main__':
    unittest.main()
