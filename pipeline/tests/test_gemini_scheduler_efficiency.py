from copy import deepcopy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from pipeline import cloud_transcription_summary_admission as admission
from pipeline import gemini_scheduler_efficiency as efficiency
from pipeline import transcript_summary as io
from pipeline.tests.test_gemini_queue_tokens import job


class SchedulerTests(unittest.TestCase):
    queue_policy = efficiency.queue.POLICY
    queue_tier = 'tier1'

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'reservations').mkdir(mode=0o700)
        self.record = self.root / 'records' / 'record'
        self.record.mkdir(parents=True, mode=0o700)
        self.entry = dict(plan=dict(path=str(self.record / 'plan.json'), sha256='a'*64),
            source=dict(recording_id='recording', format='third_party'))
        item = job(); item.update(stage='chunk', job_id='test-job')
        item['budget']['maximum_cost_microusd'] = 50
        body = dict(provider='gemini', retry_of=None, jobs=[item], input_sha256='b'*64, maximum_cost_microusd=50)
        self.wave = dict(body, wave_id='summarywave_' + io.digest(body)[:32])
        self.wave_id = self.wave['wave_id']
        folder = self.record / 'waves' / self.wave_id
        folder.mkdir(parents=True, mode=0o700)
        self.wave_path = folder / 'wave.json'
        io.put(self.wave_path, self.wave)
        self.ledger = self.root / 'reservations' / (self.wave_id + '.json')
        self.submitted = False
        self.complete = False
        self.cap = 100
        self.policy_holds = {}
        self.ambiguous = False
        self.available = {'recording': self.entry['source']}
        self.r = SimpleNamespace(**{name: getattr(io, name) for name in
            ('safe', 'locked', 'read', 'binding', 'digest', 'put', 'mkdir', 'canonical', 'client_module')})
        self.r.prepare_plan = Mock(side_effect=AssertionError('already-prepared/pending work must not be rebuilt'))
        self.r.load_plan = Mock(side_effect=AssertionError('candidate admission must not rebuild the plan'))
        self.r.load_state = Mock(side_effect=AssertionError('candidate admission must not rebuild the graph'))
        self.r.poll_wave = Mock(return_value=dict(state='remote_pending'))
        self.r.export_plan = Mock()
        self.r.submit_wave = Mock(side_effect=self.submit)
        self.worker = SimpleNamespace(r=self.r, admission=admission, SummaryWorkerError=RuntimeError,
            _available=Mock(side_effect=lambda _: deepcopy(self.available)),
            _identity_holds=Mock(side_effect=lambda _: deepcopy(self.policy_holds)),
            _snapshot=self.snapshot, _active_waves=self.active, _public=self.public,
            _permanent_candidate_holds=lambda *_: {}, _ensure_record=Mock(),
            _reservation=lambda ref, entry, wave: dict(wave_id=wave['wave_id'], maximum_cost_microusd=wave['maximum_cost_microusd']),
            load_manifest=lambda _: dict(state_root=str(self.root), max_total_budget_microusd=self.cap),
            _transport_event=lambda error, wave, op: dict(status_code=error.status_code, wave_id=wave, operation=op),
            job_cache=SimpleNamespace(cached_export=Mock(), statistics=lambda: {}),
            parallel_collection=SimpleNamespace(active=lambda: False, statistics=lambda: {}))
        self.old_available, self.old_gate = self.worker._available, self.worker._identity_holds
        self.addCleanup(setattr, admission, 'DEFAULT_TOKEN_LIMIT', admission.DEFAULT_TOKEN_LIMIT)
        self.counter = efficiency.install(self.worker, self.root / 'scheduler',
            queue_policy=self.queue_policy, queue_tier=self.queue_tier)
        self.counter.api = Mock()
        self.counter.api.count.return_value = dict(totalTokens=100)

    def submit(self, *args, **kwargs):
        self.assertTrue(self.ledger.exists())
        self.assertEqual(io.read(io.binding(self.ledger))['maximum_cost_microusd'], 50)
        self.assertTrue(kwargs['allow_paid_api'])
        self.submitted = True
        return dict(state='submitted')

    def snapshot(self, manifest, ref):
        available = self.worker._available(manifest)
        held = self.worker._identity_holds(available)
        orphan = self.ledger.exists() and not self.submitted
        status = dict(state='completed' if self.complete else 'needs_reconciliation' if orphan else 'waiting_remote' if self.submitted else 'prepared',
            transcript_phase_complete=self.complete, pending_waves=[self.wave_id] if self.submitted and not self.complete else [],
            ambiguous_waves=[], prepared_waves=[] if self.submitted or self.complete else [self.wave_id],
            ready_jobs=0, failed_jobs=0)
        return dict(available=available, identity_holds=held,
            records={'recording': dict(entry=self.entry, status=status)},
            waves={self.wave_id: (self.entry, self.wave_path.parent,
                dict(wave_id=self.wave_id, input_sha256=self.wave['input_sha256'], maximum_cost_microusd=50, input_tokens=25000))},
            reservations={self.wave_id: {}} if self.ledger.exists() else {},
            orphan_reservations=[dict(recording_id='recording', ambiguous_waves=[self.wave_id])] if orphan else [],
            accounted_microusd=50 if self.ledger.exists() else 0,
            usage_estimate_microusd=50 if self.complete else 0,
            unsettled_hold_microusd=50 if self.ledger.exists() and not self.complete else 0)

    def active(self, view):
        status = view['records']['recording']['status']
        names = {name: 'pending' for name in status['pending_waves']}
        names.update({name: 'orphan' for hold in view['orphan_reservations'] for name in hold['ambiguous_waves']})
        return [dict(wave_id=name, state=state, input_tokens=view['waves'][name][2]['input_tokens']) for name, state in names.items()]

    def public(self, manifest, view):
        active = self.active(view)
        return dict(pending_waves=sum(v['state'] == 'pending' for v in active), potential_active_waves=len(active),
            holds=view['orphan_reservations'], waiting_for_summary_admission=0,
            speaker_identity_pending=len(view['identity_holds']), enqueued_input_token_allowance=sum(v['input_tokens'] for v in active),
            accounted_microusd=view['accounted_microusd'])

    def cycle(self, **options):
        return self.worker.cycle({}, allow_paid_api=True, max_active=4, max_new_waves=4, client=Mock(), **options)

    def test_counted_queue_admission_preserves_financial_reservation(self):
        result = self.cycle(max_enqueued_tokens=1000)
        self.assertEqual(result['confirmed_new_submissions'], 1)
        self.assertEqual(result['enqueued_input_token_estimate'], self.expected_tokens)
        self.assertEqual(result['queue_token_accounting']['financial_input_token_allowance'], 25000)
        self.assertEqual(result['accounted_microusd'], 50)
        self.assertFalse(result['queue_token_accounting']['financial_reservations_changed'])
        self.r.prepare_plan.assert_not_called()
        self.r.load_plan.assert_not_called()
        self.r.load_state.assert_not_called()

    def test_pending_replay_skips_preparation_and_does_not_resubmit(self):
        self.cycle()
        result = self.cycle()
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(self.r.submit_wave.call_count, 1)
        self.assertEqual(result['local_processing']['pending_preparations_skipped'], 1)
        self.assertEqual(result['local_processing']['snapshot_calls'], 2)
        self.assertEqual(result['local_processing']['source_selection_calls'], 1)
        self.assertEqual(result['local_processing']['identity_gate_calls'], 1)

    def test_collection_refills_freed_slot_in_same_cycle(self):
        # Simulate a completed predecessor exposing a ready wave. No second
        # scheduler cycle or sleep should be needed to use the freed slot.
        self.submitted = True
        def collect(*args, **kwargs):
            self.submitted = False
            return dict(state='collected')
        self.r.poll_wave.side_effect = collect
        result = self.worker.cycle({}, allow_paid_api=True, max_active=1,
            max_new_waves=100, client=Mock())
        self.assertEqual(result['confirmed_new_submissions'], 1)
        self.r.poll_wave.assert_called_once()
        self.r.submit_wave.assert_called_once()
        self.assertEqual(result['potential_active_waves'], 1)

    def test_hundred_wave_refill_preserves_slot_limit(self):
        self.submitted = True
        result = self.worker.cycle({}, allow_paid_api=True, max_active=1,
            max_new_waves=100, client=Mock())
        self.assertEqual(result['new_paid_requests'], 0)
        self.r.submit_wave.assert_not_called()
        self.assertEqual(result['potential_active_waves'], 1)

    def test_refill_limit_above_hundred_is_rejected(self):
        with self.assertRaises(io.Error):
            self.worker.cycle({}, allow_paid_api=True, max_active=100,
                max_new_waves=101, client=Mock())
        self.r.submit_wave.assert_not_called()

    def test_stop_after_reservation_finishes_submission_boundary(self):
        self.cycle(stopping=lambda: self.ledger.exists())
        self.assertTrue(self.submitted)
        self.assertTrue(self.ledger.exists())

    def test_stop_before_reservation_leaves_no_orphan(self):
        self.cycle(stopping=lambda: True)
        self.assertFalse(self.ledger.exists())
        self.r.submit_wave.assert_not_called()

    def test_sources_and_identity_policy_refresh_on_each_cycle(self):
        self.cycle()
        self.policy_holds = {'recording': dict(reason='not_reviewed')}
        self.cycle()
        self.assertEqual(self.old_available.call_count, 2)
        self.assertEqual(self.old_gate.call_count, 2)
        self.assertEqual(self.r.submit_wave.call_count, 1)

    def test_review_hold_blocks_new_paid_requests(self):
        self.policy_holds = {'recording': dict(reason='not_reviewed')}
        self.cycle()
        self.r.submit_wave.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_financial_limit_not_relaxed_by_lower_token_count(self):
        self.cap = 49
        result = self.cycle(max_enqueued_tokens=1000)
        self.assertEqual(result['new_paid_requests'], 0)
        self.r.submit_wave.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_count_failure_uses_old_ceiling_and_blocks_small_window(self):
        self.counter.api.count.side_effect = io.client_module.BatchClientError('unavailable', status_code=503)
        result = self.cycle(max_enqueued_tokens=1000)
        self.assertEqual(result['new_paid_requests'], 0)
        self.r.submit_wave.assert_not_called()

    def test_unknown_reservation_keeps_slot_cost_and_queue_tokens(self):
        io.put(self.ledger, dict(wave_id=self.wave_id, maximum_cost_microusd=50))
        result = self.cycle()
        self.assertEqual(result['accounted_microusd'], 50)
        self.assertEqual(result['potential_active_waves'], 1)
        self.assertEqual(result['enqueued_input_token_estimate'], self.expected_tokens)
        self.r.submit_wave.assert_not_called()

    def test_ambiguous_submit_retains_reservation_and_never_repeats(self):
        self.r.submit_wave.side_effect = io.client_module.BatchClientError('transport', ambiguous=True)
        first = self.cycle()
        self.assertEqual(first['new_paid_requests'], 1)
        self.assertTrue(self.ledger.exists())
        self.cycle()
        self.assertEqual(self.r.submit_wave.call_count, 1)

    def test_changed_prepared_wave_fails_before_paid_reservation(self):
        self.wave_path.chmod(0o600)
        altered = deepcopy(self.wave); altered['jobs'][0]['request']['body']['contents'][0]['parts'][0]['text'] = 'changed'
        self.wave_path.write_bytes(io.canonical(altered))
        with self.assertRaises(RuntimeError):
            self.cycle()
        self.r.submit_wave.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_stopping_prevents_new_reservation(self):
        self.assertEqual(self.cycle(stopping=lambda: True)['state'], 'paused')
        self.assertFalse(self.ledger.exists())

    @property
    def expected_tokens(self):
        return 100 if self.queue_policy == efficiency.queue.COUNTED_POLICY else 238


class CountedSchedulerTests(SchedulerTests):
    queue_policy = efficiency.queue.COUNTED_POLICY

    def test_default_ceiling_leaves_five_percent_and_reports_honest_metrics(self):
        result = self.cycle()
        account = result['queue_token_accounting']
        self.assertEqual(result['operator_enqueued_token_ceiling'], 2850000)
        self.assertEqual(account['operator_reserve_tokens'], 150000)
        self.assertEqual(account['tier1_batch_token_limit'], 3000000)
        self.assertEqual(account['counted_input_tokens'], 100)
        self.assertEqual(account['per_request_headroom_tokens'], 0)
        self.assertEqual(account['confirmed_pending_input_token_estimate'], 100)
        self.assertEqual(account['unresolved_submission_token_estimate'], 0)
        self.assertFalse(account['provider_reported_occupancy'])
        self.assertFalse(result['account_quota_verified'])

    def test_ceiling_above_approved_target_rejected_before_submission(self):
        for value in (2850001, 3000000):
            with self.subTest(value=value), self.assertRaises(io.Error):
                self.cycle(max_enqueued_tokens=value)
        self.r.submit_wave.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_exact_count_fits_where_old_per_request_padding_did_not(self):
        result = self.cycle(max_enqueued_tokens=100)
        self.assertEqual(result['confirmed_new_submissions'], 1)
        self.assertEqual(result['enqueued_input_token_estimate'], 100)
        self.assertEqual(result['accounted_microusd'], 50)

    def test_count_above_available_space_stays_blocked(self):
        result = self.cycle(max_enqueued_tokens=99)
        self.assertEqual(result['new_paid_requests'], 0)
        self.r.submit_wave.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_unresolved_queue_count_is_reported_and_retained(self):
        io.put(self.ledger, dict(wave_id=self.wave_id, maximum_cost_microusd=50))
        result = self.cycle()
        account = result['queue_token_accounting']
        self.assertEqual(account['confirmed_pending_input_token_estimate'], 0)
        self.assertEqual(account['unresolved_submission_token_estimate'], 100)
        self.assertEqual(result['accounted_microusd'], 50)
        self.r.submit_wave.assert_not_called()


class Tier2SchedulerTests(SchedulerTests):
    queue_policy = efficiency.queue.COUNTED_POLICY
    queue_tier = 'tier2'

    def test_tier2_ceiling_and_twenty_million_reserve(self):
        result = self.cycle()
        self.assertEqual(result['operator_enqueued_token_ceiling'], 380000000)
        self.assertEqual(result['queue_token_accounting']['provider_batch_token_limit'], 400000000)
        self.assertEqual(result['queue_token_accounting']['operator_reserve_tokens'], 20000000)
        self.assertFalse(result['account_quota_verified'])

    def test_tier2_still_rejects_above_safe_ceiling(self):
        with self.assertRaises(io.Error):
            self.cycle(max_enqueued_tokens=380000001)
        self.r.submit_wave.assert_not_called()

    def test_tier2_admission_can_exceed_old_tier1_ceiling(self):
        result = admission.assess_admission(active_waves=[], candidate_input_tokens=4000000,
            candidate_cost_microusd=1, settled_microusd=0, held_microusd=0,
            budget_limit_microusd=100, target=1, token_limit=380000000)
        self.assertTrue(result['allowed'])


if __name__ == '__main__':
    unittest.main()
