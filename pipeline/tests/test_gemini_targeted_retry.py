"""Targeted partial-batch recovery, with fake Gemini and network forbidden."""
from copy import deepcopy
import json
from pathlib import Path
from contextlib import ExitStack
import pickle
import unittest
from unittest.mock import Mock, patch

from pipeline import gemini_scheduler_efficiency as efficiency
from pipeline import gemini_targeted_retry as retry
from pipeline.tests import test_cloud_transcription_summary as fixtures

worker, r = fixtures.worker, fixtures.r


class InternalGemini(fixtures.Gemini):
    errors = None

    def get_batch(self, name):
        value = super().get_batch(name)
        if self.complete:
            rows = value['response']['inlinedResponses']['inlinedResponses']
            for row in rows:
                code = (self.errors or {}).get(row['metadata']['key'])
                if code is not None:
                    row.pop('response')
                    row['error'] = {'code': code, 'message': 'Synthetic provider failure'}
        return value


@unittest.skipUnless(hasattr(worker, '_record_snapshot'), 'requires the retained efficient worker runtime')
class TargetedTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.available = [self.case.source(text='Daniel walked outside and discussed his day. ' * 650)]
        self.case.prepare()
        self.client = self.case.client = InternalGemini(self.case.worker_root)
        self.case.cycle()
        self.entry_path = next((self.case.worker_root / 'entries').iterdir())
        self.entry = r.read(r.binding(self.entry_path))
        self.plan_ref = self.entry['plan']
        self.plan_root = Path(self.plan_ref['path']).parent
        self.original_path = next((self.plan_root / 'waves').glob('*/wave.json'))
        self.original = r.read(r.binding(self.original_path))
        self.assertGreater(len(self.original['jobs']), 1)
        self.failed = self.original['jobs'][0]['job_id']
        self.succeeded = self.original['jobs'][1]['job_id']
        self.client.errors = {self.failed: 13}
        self.client.complete = True
        result = self.case.cycle(max_new_waves=0)
        self.assertEqual(result['counts']['needs_review'], 1)
        collection = r.binding(self.original_path.parent / 'collection.json')
        job = self.original['jobs'][0]
        self.value = dict(kind=retry.KIND, schema_version=1, worker=self.case.ref,
            approval='Explicit fixture approval for one confirmed failure',
            selection_cutoff_utc='2026-09-16T02:52:15+00:00',
            max_attempts_per_selected_job=2, provider_error_code=13,
            selected_job_count=1, maximum_additional_cost_microusd=job['budget']['maximum_cost_microusd'],
            grants=[dict(recording_id=self.entry['source']['recording_id'], entry=r.binding(self.entry_path),
                plan=self.plan_ref, wave=r.binding(self.original_path), collection=collection,
                jobs=[dict(job_id=self.failed, job_sha256=r.digest(job),
                    maximum_cost_microusd=job['budget']['maximum_cost_microusd'])])])
        self.authority_ref = self.case.file(self.value)

    def activate(self):
        authority, restore = retry.install(worker, self.authority_ref)
        self.addCleanup(restore)
        for name in ('_available', '_identity_holds', '_snapshot', 'cycle'):
            saved = getattr(worker, name)
            self.addCleanup(setattr, worker, name, saved)
        counter = efficiency.install(worker, self.case.root / 'scheduler', queue_policy=efficiency.queue.COUNTED_POLICY)
        counter.api = Mock(count=Mock(return_value={'totalTokens': 100}))
        self.authority = authority
        return authority

    def state(self):
        plan, sources = r.load_plan(self.plan_ref['path'], self.plan_ref['sha256'])
        return plan, sources, r.load_state(plan, sources)

    def test_retry_remains_forbidden_without_explicit_authority(self):
        with self.assertRaisesRegex(r.Error, 'attempt limits'):
            r.prepare_plan(self.plan_ref['path'], self.plan_ref['sha256'], retry_wave=self.original['wave_id'])
        self.assertEqual(len(self.client.created), 1)

    def test_successful_chunk_preserved_and_only_failed_job_is_submitted(self):
        saved = {path: path.read_bytes() for path in (self.original_path.parent).iterdir() if path.is_file()}
        self.activate()
        result = self.case.cycle()
        self.assertEqual(result['confirmed_new_submissions'], 1)
        self.assertEqual([v['key'] for v in self.client.created[-1][1]], [self.failed])
        self.assertEqual(result['targeted_recovery']['pending_jobs'], 1)
        self.assertFalse(result['targeted_recovery']['automatic_retry_discovery'])
        for path, content in saved.items():
            self.assertEqual(path.read_bytes(), content)
        self.client.errors = {}
        self.case.cycle()  # Recovery completes; only the normal reducer is new.
        result = self.case.cycle()
        self.assertEqual(result['transcript_summaries_complete'], 1)
        self.assertEqual(result['targeted_recovery']['completed_jobs'], 1)
        plan, _, state = self.state()
        self.assertEqual(state['attempts'][self.failed], 2)
        self.assertEqual(state['attempts'][self.succeeded], 1)
        self.assertEqual(plan['request_value']['budget']['max_attempts_per_job'], 1)
        self.assertEqual(self.case.cycle()['new_paid_requests'], 0)

    def test_failed_retry_cannot_be_retried_again_even_after_restart(self):
        self.activate()
        self.case.cycle()
        self.case.cycle()
        result = self.case.cycle()
        self.assertEqual(result['targeted_recovery']['failed_again_jobs'], 1)
        self.assertEqual(len(self.client.created), 2)
        self.authority.prepared.clear()  # Same finite authority after restart.
        self.assertEqual(self.case.cycle()['new_paid_requests'], 0)
        self.assertEqual(len(self.client.created), 2)

    def test_ambiguous_retry_preserves_hold_and_never_reposts(self):
        self.activate()
        self.client.fail_post = True
        result = self.case.cycle()
        self.assertEqual(result['targeted_recovery']['ambiguous_jobs'], 1)
        cost = result['accounted_microusd']
        self.authority.prepared.clear()
        again = self.case.cycle()
        self.assertEqual(len(self.client.created), 2)
        self.assertEqual(again['accounted_microusd'], cost)
        self.assertEqual(again['potential_active_waves'], 1)

    def test_authority_rejects_successful_jobs_or_other_error_codes(self):
        for key in (self.succeeded, 'not-a-real-job'):
            value = deepcopy(self.value)
            value['grants'][0]['jobs'][0]['job_id'] = key
            with self.assertRaisesRegex(r.Error, 'confirmed INTERNAL'):
                retry.Authority(worker, self.case.file(value))
        value = deepcopy(self.value)
        value['provider_error_code'] = 14
        with self.assertRaises(r.Error):
            retry.Authority(worker, self.case.file(value))

    def test_authority_hash_mutation_and_wrong_worker_are_rejected(self):
        bad_ref = {**self.authority_ref, 'sha256': 'a' * 64}
        with self.assertRaises(r.Error):
            retry.Authority(worker, bad_ref)
        authority = self.activate()
        with self.assertRaisesRegex(r.Error, 'different worker'):
            authority.prepare_pending({}, {'path': '/wrong', 'sha256': 'b' * 64})

    def test_new_unapproved_failure_does_not_gain_retry_authority(self):
        self.activate()
        self.case.cycle()
        self.client.errors = {}
        self.case.cycle()  # Ordinary downstream reducer is submitted.
        reducer = self.client.created[-1][1][0]['key']
        self.client.errors = {reducer: 13}
        result = self.case.cycle()
        self.assertEqual(result['targeted_recovery']['completed_jobs'], 1)
        self.assertEqual(result['counts']['needs_review'], 1)
        self.assertEqual(self.case.cycle()['new_paid_requests'], 0)
        self.assertEqual(len(self.client.created), 3)

    def test_retry_uses_existing_token_and_budget_admission_limits(self):
        self.activate()
        result = self.case.cycle(max_enqueued_tokens=99)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(result['targeted_recovery']['prepared_jobs'], 1)
        self.assertEqual(len(self.client.created), 1)
        original = worker.admission.assess_admission
        def no_budget(**kwargs):
            kwargs['budget_limit_microusd'] = kwargs['settled_microusd'] + kwargs['held_microusd']
            return original(**kwargs)
        with patch.object(worker.admission, 'assess_admission', side_effect=no_budget):
            self.assertEqual(self.case.cycle()['new_paid_requests'], 0)
        self.assertEqual(len(self.client.created), 1)

    def test_retained_cache_handles_retry_without_rebuilding_initial_chunks(self):
        self.activate()
        self.case.cycle()
        with worker.job_cache.scope(self.case.ref) as cache:
            with patch.object(cache, 'builder', side_effect=AssertionError('must reuse original chunks')):
                _, _, state = self.state()
                self.assertEqual(state['attempts'][self.failed], 2)
                self.assertEqual(cache.statistics()['retained_seed_hits'], 1)

    def test_unapproved_retry_wave_is_rejected_by_scheduler_and_full_replay(self):
        self.activate()
        self.case.cycle()
        _, _, state = self.state()
        wave = deepcopy(state['waves'][-1])
        wave['jobs'] = [deepcopy(self.original['jobs'][1])]
        self.assertFalse(self.authority.permits(self.entry, wave))
        with self.assertRaisesRegex(r.Error, 'unapproved'):
            self.authority.check_state(self.authority.entry_record(self.entry), {**state, 'waves': [wave]})
        with patch.object(r, 'read', return_value=wave), self.assertRaises(worker.SummaryWorkerError):
            efficiency.candidate_wave(worker, self.entry, wave['wave_id'])

    def test_spawn_initializer_is_picklable_and_restores_core_on_child_exit(self):
        self.activate()
        initializer = pickle.loads(pickle.dumps(worker.parallel_collection._initialize))
        self.assertEqual(initializer.func, retry.initialize_collector)
        before = r.load_state
        with ExitStack() as stack:
            child = {'worker_ref': self.case.ref, 'stack': stack}
            with patch.object(worker.parallel_collection, '_CHILD', child):
                previous = Mock()
                retry.initialize_collector(r.binding(retry.__file__), self.authority_ref, previous, 'sentinel')
                previous.assert_called_once_with('sentinel')
                self.assertIsNot(r.load_state, before)
        self.assertIs(r.load_state, before)

    def test_parallel_collector_admits_approved_retry_and_never_posts(self):
        self.activate()
        self.case.cycle()
        _, _, state = self.state()
        wave_id = state['waves'][-1]['wave_id']
        self.client.errors = {}
        import threading
        parallel = worker.parallel_collection
        child = dict(worker_ref=self.case.ref, manifest=worker.load_manifest(self.case.ref),
            env_file=str(self.case.root / '.env'), stop_event=threading.Event())
        before = list(self.client.created)
        with patch.object(parallel, '_CHILD', child), patch.object(r, 'api_client', return_value=self.client):
            report = parallel._poll_group(dict(plan=self.plan_ref, waves=[wave_id]))
        self.assertEqual(report['fatal_errors'], [])
        self.assertEqual(report['events'][0]['state'], 'collected')
        self.assertEqual(self.client.created, before)

    def test_original_success_and_failure_receipts_remain_budgeted_after_recovery(self):
        before = self.case.snapshot()['accounted_microusd']
        self.activate()
        self.case.cycle()
        self.client.errors = {}
        self.case.cycle(max_new_waves=0)
        view = self.case.snapshot()
        self.assertGreater(view['accounted_microusd'], before)
        self.assertEqual(len(view['reservations']), 2)
        self.assertEqual(view['unsettled_hold_microusd'], self.value['maximum_additional_cost_microusd'])


class ExtendedTests(TargetedTests):
    def setUp(self):
        super().setUp()
        self.value.update(schema_version=2, max_attempts_per_selected_job=3,
            repairs=[], repaired_job_count=0)
        for grant in self.value['grants']:
            for job in grant['jobs']:
                job.update(max_attempts=2, previous_failure=None)
        self.authority_ref = self.case.file(self.value)

    def test_third_attempt_requires_specific_failed_retry_proof(self):
        self.value['grants'][0]['jobs'][0]['max_attempts'] = 3
        with self.assertRaisesRegex(r.Error, 'third attempt requires'):
            retry.Authority(worker, self.case.file(self.value))

    def test_retry_output_can_be_repaired_without_another_paid_attempt(self):
        authority, restore = retry.install(worker, self.authority_ref)
        original_get = self.client.get_batch
        def response(name):
            value = original_get(name)
            for row in value.get('response', {}).get('inlinedResponses', {}).get('inlinedResponses', []):
                if row['metadata']['key'] == self.failed:
                    content = row['response']['candidates'][0]['content']['parts'][0]
                    payload = json.loads(content['text'])
                    payload['uncertainties'] = [dict(text='The weather is unclear.',
                        classification='reported_statement', evidence_ids=['e1'])]
                    content['text'] = json.dumps(payload)
            return value
        self.client.errors = {}
        self.client.get_batch = response
        try:
            prepared = r.prepare_plan(self.plan_ref['path'], self.plan_ref['sha256'],
                retry_wave=self.original['wave_id'], phase='transcripts')
            folder = self.plan_root / 'waves' / prepared['wave_id']
            wave = r.read(r.binding(folder / 'wave.json'))
            r.put(self.case.worker_root / 'reservations' / (wave['wave_id'] + '.json'),
                worker._reservation(self.case.ref, self.entry, wave))
            r.submit_wave(self.plan_ref['path'], self.plan_ref['sha256'], wave['wave_id'],
                allow_paid_api=True, client=self.client)
            r.poll_wave(self.plan_ref['path'], self.plan_ref['sha256'], wave['wave_id'], client=self.client)
        finally:
            restore()
        from pipeline import gemini_dashboard_recovery_prepare as preparation
        report = preparation.prepare(worker, self.case.ref, self.authority_ref,
            self.case.file(dict(state='paused', holds=[dict(plan=self.plan_ref, failed_jobs=1)])),
            self.case.root / 'repair-successor.json', approval='Repair an exact failed retry output')
        self.assertEqual(report['repair_jobs'], 1)
        self.assertEqual(report['newly_recoverable_failed_jobs'], 0)
        self.authority_ref = report['authority']
        self.activate()
        _, _, state = self.state()
        result = next(x for x in state['results'] if x['job_id'] == self.failed)
        self.assertEqual(result['sections']['uncertainties'][0]['classification'], 'uncertainty')
        self.assertEqual(state['attempts'][self.failed], 2)
        self.assertEqual(len(self.client.created), 2)

    def test_bound_third_attempt_is_finite(self):
        authority, restore = retry.install(worker, self.authority_ref)
        try:
            prepared = r.prepare_plan(self.plan_ref['path'], self.plan_ref['sha256'],
                retry_wave=self.original['wave_id'], phase='transcripts')
            wave_id = prepared['wave_id']
            wave = r.read(r.binding(self.plan_root / 'waves' / wave_id / 'wave.json'))
            r.put(self.case.worker_root / 'reservations' / (wave_id + '.json'),
                worker._reservation(self.case.ref, self.entry, wave))
            r.submit_wave(self.plan_ref['path'], self.plan_ref['sha256'], wave_id,
                allow_paid_api=True, client=self.client)
            r.poll_wave(self.plan_ref['path'], self.plan_ref['sha256'], wave_id, client=self.client)
        finally:
            restore()
        job = self.value['grants'][0]['jobs'][0]
        job.update(max_attempts=3, previous_failure=r.binding(self.plan_root / 'waves' / wave_id / 'collection.json'))
        self.authority_ref = self.case.file(self.value)
        # A successor authority must retain third-attempt proofs exactly.
        from pipeline import gemini_dashboard_recovery_prepare as preparation
        status = self.case.file(dict(state='paused', holds=[]))
        successor = preparation.prepare(worker, self.case.ref, self.authority_ref,
            status, self.case.root / 'successor.json', approval='Preserve previous grants')
        successor_value = r.read(successor['authority'])
        self.assertEqual(successor_value['grants'], self.value['grants'])
        self.authority_ref = successor['authority']
        self.activate()
        self.case.cycle()
        self.case.cycle()
        _, _, state = self.state()
        self.assertEqual(state['attempts'][self.failed], 3)
        self.authority.prepared.clear()
        self.assertEqual(self.case.cycle()['new_paid_requests'], 0)
        self.assertEqual(len(self.client.created), 3)


@unittest.skipUnless(hasattr(worker, '_record_snapshot'), 'requires retained worker')
class LocalRepairTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.available = [self.case.source(text='Daniel walked outside and discussed his day. ' * 650)]
        self.case.prepare()
        client = self.case.client = InternalGemini(self.case.worker_root)
        self.case.cycle()
        entry_path = next((self.case.worker_root / 'entries').iterdir())
        self.entry = r.read(r.binding(entry_path))
        self.plan_ref = self.entry['plan']
        self.plan_root = Path(self.plan_ref['path']).parent
        self.wave_path = next((self.plan_root / 'waves').glob('*/wave.json'))
        self.wave = r.read(r.binding(self.wave_path))
        self.failed, self.repaired = [j['job_id'] for j in self.wave['jobs'][:2]]
        original_get = client.get_batch

        def get_batch(name):
            value = original_get(name)
            for row in value.get('response', {}).get('inlinedResponses', {}).get('inlinedResponses', []):
                if row['metadata']['key'] == self.repaired:
                    content = row['response']['candidates'][0]['content']['parts'][0]
                    payload = json.loads(content['text'])
                    payload['uncertainties'] = [dict(text='The weather is unclear.',
                        classification='reported_statement', evidence_ids=['e1'])]
                    content['text'] = json.dumps(payload)
            return value

        client.get_batch = get_batch
        client.errors = {self.failed: 13}
        client.complete = True
        self.case.cycle(max_new_waves=0)
        grant = dict(recording_id=self.entry['source']['recording_id'], entry=r.binding(entry_path),
            plan=self.plan_ref, wave=r.binding(self.wave_path),
            collection=r.binding(self.wave_path.parent / 'collection.json'))
        first, second = self.wave['jobs'][:2]
        self.value = dict(kind=retry.KIND, schema_version=2, worker=self.case.ref,
            approval='Fixture approval', selection_cutoff_utc='2026-09-16T15:00:00Z',
            max_attempts_per_selected_job=3, provider_error_code=13, selected_job_count=1,
            maximum_additional_cost_microusd=first['budget']['maximum_cost_microusd'],
            repaired_job_count=1,
            grants=[dict(grant, jobs=[dict(job_id=self.failed, job_sha256=r.digest(first),
                maximum_cost_microusd=first['budget']['maximum_cost_microusd'], max_attempts=2, previous_failure=None)])],
            repairs=[dict(grant, jobs=[dict(job_id=self.repaired, job_sha256=r.digest(second))])])

    def activate(self):
        from pipeline import gemini_dashboard_spend as spend
        self.addCleanup(spend.install(worker, self.case.ref))
        authority, restore = retry.install(worker, self.case.file(self.value))
        self.addCleanup(restore)
        return authority

    def test_repairs_only_classification_and_preserves_receipts_and_dependency_replay(self):
        before = {p: p.read_bytes() for p in self.wave_path.parent.iterdir() if p.is_file()}
        authority = self.activate()
        plan, sources = r.load_plan(self.plan_ref['path'], self.plan_ref['sha256'])
        state = r.load_state(plan, sources)
        result = next(row for row in state['results'] if row['job_id'] == self.repaired)
        item = result['sections']['uncertainties'][0]
        self.assertEqual(item['classification'], 'uncertainty')
        self.assertEqual(item['text'], 'The weather is unclear.')
        self.assertTrue(item['citations'])
        self.assertEqual(authority.progress(authority.record(plan), state)['locally_repaired_jobs'], 1)
        self.assertEqual(state['attempts'][self.repaired], 1)
        for path, content in before.items():
            self.assertEqual(path.read_bytes(), content)
        self.assertEqual(len(self.case.client.created), 1)

    def test_repair_cannot_reclassify_provider_errors_or_valid_outputs(self):
        job = self.wave['jobs'][0]
        self.value['repairs'][0]['jobs'] = [dict(job_id=self.failed, job_sha256=r.digest(job))]
        with self.assertRaisesRegex(r.Error, 'not an exact uncertainty'):
            retry.Authority(worker, self.case.file(self.value))

    def test_changed_repair_proof_rejected(self):
        self.value['repairs'][0]['jobs'][0]['job_sha256'] = 'f'*64
        with self.assertRaisesRegex(r.Error, 'not an exact uncertainty'):
            retry.Authority(worker, self.case.file(self.value))

    def test_successor_preserves_previous_repairs(self):
        from pipeline import gemini_dashboard_recovery_prepare as preparation
        report = preparation.prepare(worker, self.case.ref, self.case.file(self.value),
            self.case.file(dict(state='paused', holds=[])), self.case.root / 'successor.json',
            approval='Preserve prior repair authority')
        successor = r.read(report['authority'])
        self.assertEqual(successor['repairs'], self.value['repairs'])
        self.assertEqual(successor['repaired_job_count'], 1)


class FailureSelectionTests(unittest.TestCase):
    def test_missing_result_requires_terminal_internal_batch_error(self):
        row = dict(state='needs_review', failure='missing_terminal_result')
        batch = dict(done=True, error={'code': 13}, metadata={'state': 'BATCH_STATE_FAILED'})
        self.assertTrue(retry.Authority.retryable(row, True, batch=batch))
        self.assertFalse(retry.Authority.retryable(row, False, batch=batch))
        self.assertFalse(retry.Authority.retryable(row, True))
        for altered in ({**batch, 'done': False}, {**batch, 'error': {'code': 7}},
                {**batch, 'metadata': {'state': 'BATCH_STATE_RUNNING'}},
                {**batch, 'error': {'code': '13'}}):
            self.assertFalse(retry.Authority.retryable(row, True, batch=altered))
        self.assertFalse(retry.Authority.retryable({**row, 'state': 'completed'}, True, batch=batch))

    def test_invalid_text_retry_does_not_relax_output_validation(self):
        self.assertTrue(retry.Authority.retryable(dict(state='needs_review',
            failure='output_needs_review', validation_error='invalid summary item text'), True))
        self.assertFalse(retry.Authority.retryable(dict(state='needs_review',
            failure='output_needs_review', validation_error='blocked or incomplete Gemini response'), True))


if __name__ == '__main__':
    unittest.main()
