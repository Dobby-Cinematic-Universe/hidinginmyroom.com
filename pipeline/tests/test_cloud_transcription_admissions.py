"""Integration of recovered paid ASR with fresh plans, budgets and Gemini gates.

Uses original immutable paid/screen/audio proof fixtures. All provider calls are
fake or prohibited; no production artifacts, model jobs or APIs are touched.
"""
from copy import deepcopy
from pathlib import Path
import unittest
from unittest import mock

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_recovery as recovery
from pipeline import cloud_transcription_screen as screen
from pipeline import cloud_transcription_summary as summaries
from pipeline import transcript_summary as io
from pipeline.tests import test_cloud_transcription_recovery as paid_fixtures
from pipeline.tests import test_cloud_transcription_runtime as runtime_fixtures


class CloudAdmissionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.paid = paid_fixtures.PaidRecoveryTests()
        self.paid.setUp()
        self.addCleanup(self.paid.doCleanups)
        self.base = self.paid.base
        self.catalog_ref = self.paid.recover()
        self.catalog = recovery.load_catalog(self.catalog_ref)
        self.prior = self.catalog['prior_reserved_microusd']
        self.state = self.base / 'new-cloud-stage'
        self.third_party = self.base / 'preferred-third-party'
        self.third_party.mkdir(mode=0o700)
        self.new = None

    def prepare(self, *, new_recording=False, third_party=False):
        recordings = [deepcopy(self.paid.recording)]
        config_ref = None
        if new_recording:
            self.new = runtime_fixtures.CloudRuntimeTests()
            self.new.setUp()
            self.addCleanup(self.new.doCleanups)
            self.new.add_recording()
            self.new.screen_fixture()
            recordings.extend(self.new.recordings)
            config_ref = self.new.screen_config_ref
        if third_party:
            path = self.third_party / '2026-09-13 - Existing preferred [xKuOtWjOCaA].txt'
            path.write_bytes(b'1\n00:00:01,000 --> 00:00:58,000\nPreferred third-party speech.\n')
            path.chmod(0o400)
        inventory = io.put(self.base / 'new-cloud-inventory.json', {
            'kind': 'himr_cloud_transcription_archive_inventory', 'schema_version': 1,
            'recordings': recordings})
        result = cloud.prepare(inventory, str(self.third_party), str(self.state),
            screen_config_ref=config_ref, cloud_admissions_ref=self.catalog_ref)
        self.ref, self.plan = result['plan'], cloud.load_plan(result['plan'])
        if self.new:
            row = self.plan['recordings'][1]
            folder = self.state / 'jobs' / row['job_id']
            io.mkdir(folder)
            io.put(folder / 'job.json', {'plan': self.ref, 'recording': row})
            decision = screen.screen_one(row['recording'], folder, config_ref)
            io.put(folder / 'screen.json', decision)
            self.new.state, self.new.ref, self.new.plan = self.state, self.ref, self.plan
        return result

    def forbidden_client(self, _provider):
        self.fail('recovered paid record must not construct another provider client')

    def test_recovered_result_is_exported_as_cloud_without_new_job_or_paid_request(self):
        before = {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                  for path in self.paid.root.rglob('*') if path.is_file()}
        result = self.prepare()
        row = self.plan['recordings'][0]
        self.assertEqual(row['disposition'], 'cloud_import')
        self.assertEqual(result['counts']['cloud_import'], 1)
        self.assertEqual(result['reserved_microusd'], self.prior)
        self.assertIsNone(row['provider'])
        with mock.patch.object(cloud.media, 'prepare', side_effect=AssertionError('no audio rebuild')):
            cycle = cloud.cycle(self.ref, allow_paid_api=True, budget_microusd=150_000_000,
                                client_factory=self.forbidden_client)
        self.assertEqual(cycle['new_paid_requests'], 0)
        self.assertEqual(cycle['reserved_microusd'], self.prior)
        self.assertFalse(list((self.state / 'jobs').iterdir()))
        self.assertFalse(list((self.state / 'reservations').iterdir()))
        exported = cloud.export(self.ref)['records']
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0]['format'], 'cloud')
        self.assertEqual(exported[0]['transcript'], row['import']['transcript'])
        self.assertEqual(exported[0]['completion'], row['import']['completion'])
        transcript = io.read(exported[0]['transcript'])
        self.assertEqual(transcript['raw_result']['path'], str(self.paid.folder / 'terminal-job.json'))
        self.assertEqual(transcript['text'], 'Hello.')
        self.assertEqual(before, {str(path): (path.stat().st_size, path.stat().st_mtime_ns)
                                  for path in self.paid.root.rglob('*') if path.is_file()})

    def test_total_cap_includes_prior_hold_before_any_new_post_or_audio(self):
        self.prepare(new_recording=True)
        remaining_cost = cloud.cost_bound('assemblyai', 60000, False)
        cap = self.prior + remaining_cost - 1
        result = self.new.cycle(budget_microusd=cap)
        self.assertEqual(result['state'], 'budget_paused')
        self.assertEqual(result['reserved_microusd'], self.prior)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(self.new.audio_calls, [])
        self.assertEqual(self.new.submits(), [])

    def test_exact_total_cap_allows_only_remaining_new_reservation(self):
        self.prepare(new_recording=True)
        cost = cloud.cost_bound('assemblyai', 60000, False)
        cap = self.prior + cost
        result = self.new.cycle(budget_microusd=cap)
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertEqual(result['reserved_microusd'], cap)
        self.assertEqual(result['prior_cloud_reservation_microusd'], self.prior)
        self.assertEqual(len(self.new.submits()), 1)
        self.assertEqual(len(list((self.state / 'reservations').glob('*.json'))), 1)

    def test_cap_below_prior_cost_rejected_before_spending_limit_is_written(self):
        self.prepare()
        with self.assertRaisesRegex(cloud.CloudError, 'previously reserved'):
            cloud.cycle(self.ref, allow_paid_api=True, budget_microusd=self.prior - 1,
                        client_factory=self.forbidden_client)
        self.assertFalse((self.state / 'spending-limit.json').exists())
        self.assertEqual(cloud.status(self.ref)['reserved_microusd'], self.prior)

    def test_preferred_third_party_supersedes_paid_cloud_but_not_its_cost(self):
        self.prepare(third_party=True)
        row = self.plan['recordings'][0]
        self.assertEqual(row['disposition'], 'third_party')
        status = cloud.status(self.ref)
        self.assertEqual(status['prior_cloud_reservation_microusd'], self.prior)
        self.assertEqual(status['reserved_microusd'], self.prior)
        with mock.patch.object(cloud.media, 'prepare', side_effect=AssertionError('no audio rebuild')):
            result = cloud.cycle(self.ref, allow_paid_api=True, budget_microusd=150_000_000,
                                 client_factory=self.forbidden_client)
        self.assertEqual(result['new_paid_requests'], 0)
        selected = cloud.export(self.ref)['records'][0]
        self.assertEqual(selected['format'], 'third_party')
        self.assertEqual(io.read(selected['transcript'])['segments'][0]['text'], 'Preferred third-party speech.')

    def test_changed_admission_blocks_import_before_fresh_workspace(self):
        path = Path(self.catalog['admissions'][0]['admission']['path'])
        value = io.read(io.binding(path))
        value['prior_reserved_microusd'] = 0
        self.paid.replace(path, value)
        with self.assertRaises(RuntimeError):
            self.prepare()
        self.assertFalse(self.state.exists())

    def test_forged_plan_prior_total_cannot_discard_original_paid_cost(self):
        self.prepare()
        value = deepcopy(self.plan)
        value['prior_reserved_microusd'] = 0
        changed = self.paid.replace(Path(self.ref['path']), value)
        with self.assertRaisesRegex(cloud.CloudError, 'accounting differs'):
            cloud.load_plan(changed)

    def test_recovered_record_cannot_be_reclassified_as_a_new_paid_cloud_job(self):
        self.prepare()
        value = deepcopy(self.plan)
        row = value['recordings'][0]
        provider, reason = cloud.route(row['recording'])
        row.update(disposition='cloud', import_=None, provider=provider, reason=reason,
                   maximum_cost_microusd=cloud.cost_bound(provider, row['recording']['duration_ms']))
        row['import'] = row.pop('import_')
        changed = self.paid.replace(Path(self.ref['path']), value)
        with self.assertRaises(cloud.CloudError):
            cloud.load_plan(changed)

    def test_imported_anonymous_speaker_is_held_by_gemini_without_submission(self):
        self.prepare()
        worker_ref = summaries.prepare(self.ref, self.base / 'new-summary-stage',
                                       max_total_budget_microusd=119_040_085)['manifest']
        manifest = summaries.load_manifest(worker_ref)
        available = summaries._available(manifest)
        self.assertEqual(available[self.paid.recording['recording_id']]['format'], 'cloud')
        status = summaries.status(worker_ref)
        self.assertEqual(status['preferred_transcripts_available'], 1)
        self.assertEqual(status['speaker_identity_pending'], 1)
        self.assertEqual(status['waiting_for_summary_admission'], 0)
        with mock.patch.object(summaries, '_ensure_record', side_effect=AssertionError('anonymous input must remain held')), \
             mock.patch.object(summaries.r, 'submit_wave', side_effect=AssertionError('no Gemini POST')):
            result = summaries.cycle(worker_ref, allow_paid_api=True)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(result['speaker_identity_pending'], 1)
        self.assertEqual(result['recording_plans'], 0)
        self.assertEqual(summaries.export(worker_ref)['transcript_summaries'], 0)


if __name__ == '__main__':
    unittest.main()
