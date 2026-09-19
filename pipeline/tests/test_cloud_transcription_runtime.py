"""Offline integration tests for cloud orchestration and crash recovery.

Provider network calls and audio decoding are replaced; immutable workspace
artifacts, hashes, receipt replay, client result validation and pruning are real.
"""
from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import shutil
import signal
import tempfile
import unittest
import wave
from unittest import mock

from pipeline import cloud_transcription as runtime
from pipeline import cloud_transcription_client as clients
from pipeline import cloud_transcription_media as media
from pipeline import cloud_transcription_screen as screening
from pipeline import transcript_summary as io
from pipeline.tests import test_cloud_transcription_screen as screen_fixtures


class FakeProvider:
    def __init__(self, case, provider):
        self.case, self.provider = case, provider
        self.calls = []
        self.jobs = {}
        self.poll_status = 'processing' if provider == 'assemblyai' else 'in_progress'
        self.post_status = 'queued' if provider == 'assemblyai' else 'in_progress'
        self.post_error = None
        self.after_post = lambda: None
        self.duration_seconds = 60

    def upload(self, path, *, expected_sha256):
        self.calls.append(('upload', path, expected_sha256))
        return {'upload_url': 'https://cdn.assemblyai.com/upload/' + expected_sha256}

    def _submit(self, *, upload_url=None, path=None, metadata=None, diarization=True):
        intents = list((self.case.state / 'jobs').glob('*/intent.json'))
        self.case.assertTrue(intents, 'paid POST must have a durable intent')
        self.case.assertTrue((self.case.state / 'spending-limit.json').exists())
        identifier = self.provider + '-job-' + str(len(self.jobs) + 1)
        raw = {'id': identifier, 'status': self.post_status}
        if self.provider == 'assemblyai':
            raw.update(audio_url=upload_url, speech_models=[clients.ASSEMBLYAI_MODEL],
                       speaker_labels=diarization, language_code='en')
        else:
            raw.update(type='async', transcriber='machine', language='en',
                       metadata=metadata, skip_diarization=not diarization)
            clients.revai_options(metadata, diarization=diarization)
        self.jobs[identifier] = raw
        self.calls.append(('submit', identifier, diarization))
        self.after_post()
        if self.post_error:
            raise self.post_error
        return raw

    def submit(self, upload_url, *, diarization=True):
        return self._submit(upload_url=upload_url, diarization=diarization)

    def submit_file(self, path, *, expected_sha256, metadata=None, diarization=True):
        self.case.assertEqual(hashlib.sha256(Path(path).read_bytes()).hexdigest(), expected_sha256)
        return self._submit(path=path, metadata=metadata, diarization=diarization)

    def poll(self, identifier):
        self.calls.append(('poll', identifier))
        raw = {**self.jobs[identifier], 'status': self.poll_status}
        if self.provider == 'assemblyai' and self.poll_status == 'completed':
            word = {'text': 'Hello.', 'start': 100, 'end': 700, 'confidence': .99}
            if raw['speaker_labels']:
                word['speaker'] = 'A'
            raw.update(audio_duration=self.duration_seconds, speech_model_used=clients.ASSEMBLYAI_MODEL,
                       text='Hello.', words=[word], utterances=None)
            if raw['speaker_labels']:
                raw['utterances'] = [{'speaker': 'A', 'start': 100, 'end': 700,
                                      'text': 'Hello.', 'words': [word]}]
        elif self.provider == 'revai' and self.poll_status == 'transcribed':
            raw['duration_seconds'] = self.duration_seconds
        return raw

    def transcript(self, identifier):
        self.calls.append(('transcript', identifier))
        return {'monologues': [{'speaker': 0, 'elements': [
            {'type': 'text', 'value': 'Hello', 'ts': .1, 'end_ts': .7, 'confidence': .99},
            {'type': 'punct', 'value': '.'}]}]}


class CloudRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / 'cloud-workspace'
        self.third_party = self.root / 'third-party'
        self.third_party.mkdir(mode=0o700)
        self.recordings = []
        self.sequence = 0
        self.clients = {provider: FakeProvider(self, provider) for provider in ('assemblyai', 'revai')}
        self.audio_calls = []
        self.stopped = False
        self.screen_config_ref = None
        self.screen_positive = False
        self.screen_uncertain = False
        self.screen_helpers = []

    @staticmethod
    def rewrite(path, value):
        path.chmod(0o600)
        path.write_bytes(io.canonical(value))
        path.chmod(0o400)
        return io.binding(path)

    def add_recording(self, *, duration_ms=60000, state='ready'):
        self.sequence += 1
        path = self.root / ('source-' + str(self.sequence) + '.wav')
        with wave.open(str(path), 'wb') as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(16000)
            output.writeframes(self.sequence.to_bytes(2, 'little') * 16000 * 60)
        path.chmod(0o400)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        row = {'recording_id': 'media_sha256_' + digest,
               'media': {'path': str(path), 'sha256': digest, 'byte_count': path.stat().st_size},
               'duration_ms': duration_ms, 'state': state, 'reasons': [], 'aliases': [],
               'source_ids': {'youtube': [], 'archive_native': []}, 'title': 'Synthetic test',
               'date': {'value': '2026-09-13', 'basis': 'test'}, 'audio_state': 'audio_present',
               'source_witness': None}
        self.recordings.append(row)
        return row

    def screen_fixture(self):
        """Real screened proof tree; only decoders/model inference are mocked."""
        refs = []
        for recording in self.recordings:
            helper = screen_fixtures.fixtures.RecoveryTests()
            helper.setUp()
            self.addCleanup(helper.doCleanups)
            helper.source = Path(recording['media']['path'])
            helper.record = {**recording['media'], 'media_id': recording['recording_id'],
                             'duration_hint_ms': recording['duration_ms']}
            acquisition = helper.save('raw/runtime-acquisition.json', {'status': 'completed', 'errors': [],
                'admission': {key: helper.record[key] for key in ('path', 'sha256', 'byte_count', 'media_id')}})
            helper.inventory = helper.save('runtime-inventory.json', {
                'kind': 'himr_private_speaker_screen_archive_inventory', 'schema_version': 1,
                'records': [{'recording': helper.record, 'aliases': [{'title': 'synthetic'}],
                             'acquisition_result': acquisition}]})
            recording['aliases'] = [{'acquisition_result': acquisition}]
            prepared, manifest = helper.prepare(audio_refresh='all', max_seconds=300)
            fixture = screen_fixtures.ScreenAdapterTests()
            fixture.manifest = manifest
            fixture.complete(positive=self.screen_positive, visual_faces=self.screen_uncertain)
            self.screen_helpers.append(fixture)
            refs.append(prepared['manifest'])
        config = screening.prepare_config(refs[0], refs[1:])
        self.screen_config_ref = io.put(self.root / 'screen-config.json', config)

    def prepare(self, *, screened=True, publish_decisions=True):
        if not self.recordings:
            self.add_recording()
        if screened:
            self.screen_fixture()
        source = self.root / 'inventory.json'
        source_ref = io.put(source, {'kind': 'himr_cloud_transcription_archive_inventory',
                                    'schema_version': 1, 'recordings': self.recordings})
        ffmpeg = shutil.which('ffmpeg')
        if ffmpeg is None:
            self.skipTest('FFmpeg binary needed only for implementation binding')
        kwargs = {'ffmpeg_path': str(Path(ffmpeg).resolve())}
        if self.screen_config_ref is not None:
            kwargs['screen_config_ref'] = self.screen_config_ref
        with mock.patch.object(runtime.imports, 'inventory', return_value=[]), mock.patch.object(
                runtime.imports, 'match_recordings', side_effect=lambda recordings, entries: [
                    {'status': 'missing', 'selected': None} for _ in recordings]):
            result = runtime.prepare(source_ref, str(self.third_party), str(self.state), **kwargs)
        self.ref = result['plan']
        self.plan = runtime.load_plan(self.ref)
        if screened and publish_decisions:
            for row in self.plan['recordings']:
                if row['disposition'] != 'cloud':
                    continue
                folder = self.state / 'jobs' / row['job_id']
                io.mkdir(folder)
                io.put(folder / 'job.json', {'plan': self.ref, 'recording': row})
                decision = screening.screen_one(row['recording'], folder, self.screen_config_ref)
                io.put(folder / 'screen.json', decision)
        return result

    def audio_builder(self, recording, folder, ffmpeg):
        self.audio_calls.append(recording['recording_id'])
        audio = folder / 'audio.wav'
        receipt = folder / 'audio.json'
        if receipt.exists():
            return io.read(io.binding(receipt))['audio']
        shutil.copyfile(recording['media']['path'], audio)
        audio.chmod(0o600)
        value = media.inspect_wav(audio, 60000)
        value['duration_ms'] = recording['duration_ms']
        io.put(receipt, {'kind': 'himr_cloud_prepared_audio', 'schema_version': 1,
                        'source': recording['media'], 'ffmpeg': ffmpeg, 'audio': value,
                        'whole_recording': True, 'speech_filter': False, 'cuts': False})
        return value

    def cycle(self, **kwargs):
        options = {'allow_paid_api': True, 'budget_microusd': 10_000_000,
                   'client_factory': lambda provider: self.clients[provider],
                   'prepare_audio': self.audio_builder, 'stopping': lambda: self.stopped}
        options.update(kwargs)
        return runtime.cycle(self.ref, **options)

    def folder(self, index=0):
        return self.state / 'jobs' / self.plan['recordings'][index]['job_id']

    def submits(self):
        return [call for provider in self.clients.values() for call in provider.calls if call[0] == 'submit']

    def complete_one(self):
        self.cycle()
        self.clients['assemblyai'].poll_status = 'completed'
        return self.cycle(allow_paid_api=False, budget_microusd=None)

    def test_prepare_status_export_never_construct_provider_or_open_media(self):
        self.add_recording()
        with mock.patch.object(runtime, 'client_for') as factory, mock.patch.object(runtime.media, 'prepare') as decode:
            prepared = self.prepare()
            self.assertEqual(prepared['state'], 'prepared_offline')
            self.assertEqual(runtime.status(self.ref)['reserved_microusd'], 0)
            self.assertEqual(runtime.export(self.ref)['records'], [])
        factory.assert_not_called()
        decode.assert_not_called()

    def test_route_and_reservation_bounds(self):
        for duration, provider in [(36_000_000, 'assemblyai'), (36_000_001, 'revai'), (61_200_000, 'revai'),
                                   (61_200_001, None), (None, None), (True, None)]:
            self.assertEqual(runtime.route({'duration_ms': duration})[0], provider)
        self.assertGreater(runtime.cost_bound('assemblyai', 1000), 0)
        self.assertGreater(runtime.cost_bound('assemblyai', 36_000_000), 2_300_000)

    def test_no_authorization_means_no_audio_upload_or_submission(self):
        self.prepare()
        result = self.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])

    def test_no_screen_configuration_cannot_authorize_any_paid_work(self):
        self.prepare(screened=False)
        result = self.cycle()
        self.assertEqual(result['counts']['cloud_waiting_screen'], 1)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])

    def test_missing_screen_decision_waits_before_media_preparation(self):
        self.prepare()
        (self.folder() / 'screen.json').rename(self.root / 'retained-screen.json')
        result = self.cycle()
        self.assertEqual(result['counts']['cloud_waiting_screen'], 1)
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])

    def test_verified_screen_negative_disables_diarization_preserving_segment_bounds(self):
        self.prepare()
        self.complete_one()
        document = io.read(io.binding(self.folder() / 'transcript.json'))
        self.assertIs(self.submits()[0][2], False)
        self.assertIs(document['diarization_requested'], False)
        self.assertIsNone(document['segments'][0]['speaker'])
        self.assertEqual(document['segments'][0]['start_ms'], 100)
        self.assertEqual(set(document['segments'][0]), {'start_ms', 'end_ms', 'text', 'speaker'})
        raw = io.read(io.binding(self.folder() / 'terminal-job.json'))
        self.assertEqual(raw['words'][0]['start'], 100)
        intent = io.read(io.binding(self.folder() / 'intent.json'))
        self.assertEqual(intent['maximum_cost_microusd'], runtime.cost_bound('assemblyai', 60000, False))

    def test_verified_positive_requests_diarization_without_forcing_speaker_count(self):
        self.screen_positive = True
        self.prepare()
        self.complete_one()
        document = io.read(io.binding(self.folder() / 'transcript.json'))
        self.assertIs(self.submits()[0][2], True)
        self.assertIs(document['diarization_requested'], True)
        self.assertEqual(document['segments'][0]['speaker'], 'SPEAKER_0000')

    def test_local_screen_stage_runs_while_cloud_http_lock_is_owned(self):
        self.prepare(publish_decisions=False)
        with io.locked(self.state), mock.patch.object(runtime, 'client_for') as provider, mock.patch.object(runtime.media, 'prepare') as decoder:
            result = runtime.run_screen(self.ref)
        self.assertEqual(result['new_screens'], 1)
        self.assertEqual(result['decisions'], {'screen_negative': 1})
        self.assertEqual(result['new_paid_requests'], 0)
        provider.assert_not_called()
        decoder.assert_not_called()
        self.assertEqual(runtime.status(self.ref)['counts']['cloud_ready'], 1)

    def test_two_local_screen_workers_share_a_separate_exclusive_stage_lock(self):
        self.prepare(publish_decisions=False)
        with io.locked(self.state / 'screen-worker'):
            with self.assertRaises(io.Error):
                runtime.run_screen(self.ref)
        self.assertFalse((self.folder() / 'screen.json').exists())

    def test_screen_stage_creates_private_work_folder_before_fresh_screen(self):
        self.prepare(publish_decisions=False)
        original = screening.screen_one
        def checked(recording, folder, reference):
            self.assertTrue(folder.is_dir(), 'fresh isolated screening requires its own existing private directory')
            self.assertEqual(folder.stat().st_mode & 0o077, 0)
            return original(recording, folder, reference)
        with mock.patch.object(screening, 'screen_one', side_effect=checked):
            result = runtime.run_screen(self.ref)
        self.assertEqual(result['new_screens'], 1)

    def test_screen_stage_can_create_a_fresh_isolated_proof_tree_without_mutating_old_campaign(self):
        self.prepare(publish_decisions=False)
        fixture = self.screen_helpers[0]
        old_root = Path(fixture.manifest['state_root'])
        original_result = next(old_root.glob('jobs/*/result.json'))
        original_result.rename(original_result.with_name('retained-result.json'))
        before = {str(path): path.stat().st_mtime_ns for path in old_root.rglob('*')}
        def isolated_run(path, expected_sha256):
            manifest = screening.mm.load_manifest(path, expected_sha256)
            self.assertTrue(Path(manifest['state_root']).is_relative_to(self.folder() / 'screen-work'))
            self.assertEqual(len(manifest['jobs']), 1)
            fixture.complete(manifest)
        with mock.patch.object(screening.mm, 'run', side_effect=isolated_run) as worker:
            result = runtime.run_screen(self.ref)
        worker.assert_called_once()
        self.assertEqual(result['decisions'], {'screen_negative': 1})
        after = {str(path): path.stat().st_mtime_ns for path in old_root.rglob('*')}
        self.assertEqual(before, after)
        self.assertEqual(runtime.status(self.ref)['counts']['cloud_ready'], 1)

    def test_uncertain_screen_count_is_not_a_positive_count_but_requests_diarization(self):
        self.screen_uncertain = True
        self.prepare(publish_decisions=False)
        result = runtime.run_screen(self.ref)
        self.assertEqual(result['decisions'], {'screen_uncertain': 1})
        self.assertNotIn('screen_positive', result['decisions'])
        decision = io.read(io.binding(self.folder() / 'screen.json'))
        self.assertIs(decision['diarization'], True)
        self.cycle()
        self.assertIs(self.submits()[0][2], True)

    def test_screen_stage_does_not_repeat_completed_local_decision(self):
        self.prepare()
        with mock.patch.object(screening, 'screen_one', side_effect=AssertionError('do not rerun verified screen')):
            result = runtime.run_screen(self.ref)
        self.assertEqual(result['new_screens'], 0)

    def test_wrong_screen_configuration_hash_blocks_before_any_provider(self):
        self.prepare()
        path = Path(self.screen_config_ref['path'])
        value = io.read(self.screen_config_ref)
        value['max_runtime_seconds'] += 1
        self.rewrite(path, value)
        with self.assertRaises(io.Error):
            self.cycle()
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])

    def test_alias_provenance_cannot_overlap_cloud_output_workspace(self):
        self.add_recording()
        self.recordings[0]['aliases'] = [{'acquisition_result': {'path': str(self.state / 'jobs' / 'source.json'),
                                                             'sha256': '0' * 64}}]
        with self.assertRaises(io.Error):
            self.prepare(screened=False)
        self.assertFalse((self.state / 'plan.json').exists())

    def test_larger_whole_stream_routes_to_rev_only_and_collects_machine_result(self):
        self.add_recording(duration_ms=36_001_000)
        self.prepare()
        self.assertEqual(self.plan['recordings'][0]['provider'], 'revai')
        provider = self.clients['revai']
        provider.duration_seconds = 36_001
        self.cycle()
        self.assertEqual(len(self.submits()), 1)
        self.assertEqual(self.clients['assemblyai'].calls, [])
        provider.poll_status = 'transcribed'
        result = self.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(result['counts']['cloud_completed'], 1)
        document = io.read(io.binding(self.folder() / 'transcript.json'))
        self.assertEqual(document['provider'], 'revai')
        self.assertEqual(document['model'], 'machine')
        self.assertEqual(document['text'], 'Hello.')
        self.assertTrue((self.folder() / 'provider-transcript.json').exists())
        self.assertEqual([call[0] for call in provider.calls], ['submit', 'poll', 'transcript'])

    def test_screen_decision_tampering_rejected_before_media_or_paid_work(self):
        self.prepare()
        decision = self.folder() / 'screen.json'
        value = io.read(io.binding(decision))
        value['diarization'] = True
        self.rewrite(decision, value)
        with self.assertRaises(RuntimeError):
            self.cycle()
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])

    def test_paid_authorization_without_budget_is_rejected_before_work(self):
        self.prepare()
        with self.assertRaises(runtime.CloudError):
            self.cycle(budget_microusd=None)
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])

    def test_cycle_intent_and_receipt_persist_then_restart_never_resubmits(self):
        self.prepare()
        result = self.cycle()
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertTrue((self.folder() / 'intent.json').exists())
        self.assertTrue((self.folder() / 'submission.json').exists())
        self.assertEqual(runtime.status(self.ref)['reserved_microusd'], runtime.cost_bound('assemblyai', 60000, False))
        restarted = self.cycle()
        self.assertEqual(restarted['new_paid_requests'], 0)
        self.assertEqual(len(self.submits()), 1)
        self.assertEqual(len(self.audio_calls), 1)

    def test_complete_collects_retains_proofs_and_prunes_only_upload_cache(self):
        self.prepare()
        original = Path(self.recordings[0]['media']['path']).read_bytes()
        result = self.complete_one()
        self.assertEqual(result['counts']['cloud_completed'], 1)
        self.assertGreater(result['pruned_upload_audio_bytes'], 0)
        self.assertFalse((self.folder() / 'audio.wav').exists())
        for name in ('audio.json', 'intent.json', 'submission.json', 'terminal-job.json', 'transcript.json', 'completion.json'):
            self.assertTrue((self.folder() / name).exists())
        self.assertEqual(Path(self.recordings[0]['media']['path']).read_bytes(), original)
        self.assertEqual(len(runtime.export(self.ref)['records']), 1)
        provider_calls = copy.deepcopy(self.clients['assemblyai'].calls)
        self.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(self.clients['assemblyai'].calls, provider_calls)

    def test_historical_local_asr_is_never_a_cloud_or_summary_source(self):
        from pipeline import cloud_transcription_summary as summaries
        recording = self.add_recording()
        historical = io.put(self.root / 'historical-local-asr.json', {
            'kind': 'himr_longform_recording_transcript', 'recording_id': recording['recording_id'],
            'text': 'Historical glossary-contaminated local ASR must remain separate.'})
        recording['historical_local_asr'] = historical
        original = Path(historical['path']).read_bytes()
        actual_read = io.read
        def exclude_historical(reference):
            self.assertNotEqual(reference['path'], historical['path'], 'historical ASR must not be consumed')
            return actual_read(reference)
        with mock.patch.object(io, 'read', side_effect=exclude_historical):
            self.prepare()
            self.assertEqual(runtime.export(self.ref)['records'], [])
            worker_ref = summaries.prepare(self.ref, self.root / 'summary-worker',
                                           max_total_budget_microusd=1_000_000)['manifest']
            manifest = summaries.load_manifest(worker_ref)
            self.assertEqual(summaries._available(manifest), {})
            self.complete_one()
            available = summaries._available(manifest)
            self.assertEqual(set(available), {recording['recording_id']})
            preferred = available[recording['recording_id']]
            self.assertEqual(preferred['format'], 'cloud')
            self.assertEqual(preferred['transcript']['path'], str(self.folder() / 'transcript.json'))
            self.assertEqual(actual_read(preferred['transcript'])['text'], 'Hello.')
        self.assertEqual(Path(historical['path']).read_bytes(), original)

    def test_third_party_current_transcript_is_not_rebought_even_with_positive_screen(self):
        from pipeline import cloud_transcription_summary as summaries
        recording = self.add_recording()
        recording['source_ids']['youtube'] = ['xKuOtWjOCaA']
        third_party = self.third_party / '2026-09-13 - Synthetic [xKuOtWjOCaA].txt'
        third_party.write_bytes(b'1\n00:00:01,000 --> 00:00:58,000\nThird-party transcript kept.\n')
        original = third_party.read_bytes()
        self.screen_positive = True
        self.screen_fixture()
        inventory = io.put(self.root / 'inventory.json', {'kind': 'himr_cloud_transcription_archive_inventory',
                          'schema_version': 1, 'recordings': self.recordings})
        result = runtime.prepare(inventory, str(self.third_party), str(self.state),
            ffmpeg_path=str(Path(shutil.which('ffmpeg')).resolve()), screen_config_ref=self.screen_config_ref)
        self.ref, self.plan = result['plan'], runtime.load_plan(result['plan'])
        self.assertEqual(self.plan['recordings'][0]['disposition'], 'third_party')
        self.assertEqual(runtime.run_screen(self.ref)['new_screens'], 0)
        self.assertEqual(self.cycle()['new_paid_requests'], 0)
        self.assertEqual(self.audio_calls, [])
        self.assertEqual(self.submits(), [])
        preferred = runtime.export(self.ref)['records']
        self.assertEqual(len(preferred), 1)
        self.assertEqual(preferred[0]['format'], 'third_party')
        worker_ref = summaries.prepare(self.ref, self.root / 'summary-worker',
                                       max_total_budget_microusd=1_000_000)['manifest']
        self.assertEqual(summaries._available(summaries.load_manifest(worker_ref))[recording['recording_id']]['format'], 'third_party')
        self.assertEqual(third_party.read_bytes(), original)

    def test_ambiguous_post_holds_reservation_and_blocks_all_new_submissions(self):
        self.add_recording()
        self.add_recording()
        self.prepare()
        self.clients['assemblyai'].post_error = clients.CloudClientError('transport failed', ambiguous=True)
        with self.assertRaises(clients.CloudClientError):
            self.cycle(max_new_jobs=2)
        self.assertEqual(len(self.submits()), 1)
        held = runtime.status(self.ref)['reserved_microusd']
        self.assertGreater(held, 0)
        self.clients['assemblyai'].post_error = None
        result = self.cycle(max_new_jobs=2)
        self.assertEqual(result['state'], 'reconciliation_required')
        self.assertEqual(result['reserved_microusd'], held)
        self.assertEqual(len(self.submits()), 1)

    def test_malformed_success_receipt_retained_before_semantic_validation(self):
        self.prepare()
        provider = self.clients['assemblyai']
        with mock.patch.object(provider, 'submit', return_value={'unexpected': 'raw proof'}), self.assertRaises(clients.CloudClientError):
            self.cycle()
        self.assertEqual(io.read(io.binding(self.folder() / 'submission.json')), {'unexpected': 'raw proof'})
        self.assertTrue((self.folder() / 'intent.json').exists())

    def test_budget_and_active_limit_prevent_extra_audio_or_posts(self):
        self.add_recording()
        self.add_recording()
        self.prepare()
        bound = self.plan['recordings'][0]['maximum_cost_microusd']
        result = self.cycle(budget_microusd=bound, max_new_jobs=2, max_active=2)
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertEqual(result['state'], 'budget_paused')
        self.assertEqual(len(self.audio_calls), 1)
        with self.assertRaises(io.Error):
            self.cycle(budget_microusd=bound * 2, max_new_jobs=2)
        self.assertEqual(len(self.submits()), 1)

    def test_max_active_one_does_not_prepare_second_recording(self):
        self.add_recording()
        self.add_recording()
        self.prepare()
        result = self.cycle(max_new_jobs=2, max_active=1)
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertEqual(len(self.audio_calls), 1)

    def test_failed_poll_retains_hold_while_next_healthy_recording_continues(self):
        self.add_recording()
        self.add_recording()
        self.prepare()
        self.cycle()
        self.clients['assemblyai'].poll_status = 'error'
        result = self.cycle(max_new_jobs=2)
        self.assertEqual(result['state'], 'running')
        self.assertEqual(result['new_paid_requests'], 1)
        self.assertEqual(result['counts']['cloud_failed'], 1)
        self.assertEqual(result['reserved_microusd'], 2 * runtime.cost_bound('assemblyai', 60000, False))
        self.assertEqual(len(self.submits()), 2)
        self.clients['assemblyai'].poll_status = 'completed'
        terminal = self.cycle()
        self.assertEqual(terminal['state'], 'cloud_complete_with_review_holds')
        self.assertEqual(terminal['counts']['cloud_failed'], 1)
        self.assertEqual(terminal['counts']['cloud_completed'], 1)
        self.assertEqual(terminal['reserved_microusd'], result['reserved_microusd'])
        self.assertEqual(len(self.submits()), 2)

    def test_provider_immediate_failed_post_holds_recording_but_allows_second_post(self):
        self.add_recording()
        self.add_recording()
        self.prepare()
        provider = self.clients['assemblyai']
        provider.post_status = 'error'
        provider.after_post = lambda: setattr(provider, 'post_status', 'queued')
        result = self.cycle(max_new_jobs=2)
        self.assertEqual(len(self.submits()), 2)
        self.assertEqual(result['state'], 'running')
        self.assertEqual(result['counts']['cloud_failed'], 1)
        self.assertEqual(result['counts']['cloud_pending'], 1)

    def normalization_review(self, *, other_recording=False):
        self.add_recording()
        if other_recording:
            self.add_recording()
        self.prepare()
        self.cycle()
        provider = self.clients['assemblyai']
        first_id = next(iter(provider.jobs))
        provider.poll_status = 'completed'
        original = provider.poll
        def poll(identifier):
            result = original(identifier)
            if identifier == first_id:
                result['text'] = 'Inconsistent provider full text.'
            return result
        with mock.patch.object(provider, 'poll', side_effect=poll):
            return self.cycle()

    def test_normalization_review_preserves_evidence_and_budget_while_next_recording_continues(self):
        result = self.normalization_review(other_recording=True)
        self.assertEqual(result['state'], 'running')
        self.assertEqual(result['counts']['cloud_needs_review'], 1)
        self.assertEqual(result['counts']['cloud_pending'], 1)
        review = io.read(io.binding(self.folder() / 'collection-review.json'))
        self.assertEqual(review['plan'], self.ref)
        self.assertEqual(review['source_media'], self.recordings[0]['media'])
        self.assertEqual(review['intent'], io.binding(self.folder() / 'intent.json'))
        self.assertEqual(review['raw_result'], io.binding(self.folder() / 'terminal-job.json'))
        self.assertFalse(review['automatic_paid_retry'])
        self.assertTrue((self.folder() / 'audio.wav').exists())
        self.assertFalse((self.folder() / 'completion.json').exists())
        before = list(self.clients['assemblyai'].calls)
        completed = self.cycle()
        self.assertEqual(completed['state'], 'cloud_complete_with_review_holds')
        self.assertEqual(completed['reserved_microusd'], result['reserved_microusd'])
        self.assertEqual(completed['counts']['cloud_completed'], 1)
        self.assertEqual(completed['counts']['cloud_needs_review'], 1)
        self.assertEqual(len(self.submits()), 2)
        additional_polls = [call for call in self.clients['assemblyai'].calls[len(before):] if call[0] == 'poll']
        self.assertEqual(additional_polls, [('poll', 'assemblyai-job-2')])
        exported = runtime.export(self.ref)['records']
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0]['recording_id'], self.recordings[1]['recording_id'])

    def test_only_normalization_review_finishes_with_hold_not_retry(self):
        result = self.normalization_review()
        self.assertEqual(result['state'], 'cloud_complete_with_review_holds')
        calls = list(self.clients['assemblyai'].calls)
        again = self.cycle()
        self.assertEqual(again['state'], 'cloud_complete_with_review_holds')
        self.assertEqual(again['reserved_microusd'], result['reserved_microusd'])
        self.assertEqual(self.clients['assemblyai'].calls, calls)

    def test_tampered_collection_review_bindings_stop_before_other_paid_work(self):
        self.normalization_review()
        path = self.folder() / 'collection-review.json'
        review = io.read(io.binding(path))
        review['normalizer_implementation_sha256'] = '0' * 64
        self.rewrite(path, review)
        calls = list(self.clients['assemblyai'].calls)
        with self.assertRaises(runtime.CloudError):
            self.cycle()
        self.assertEqual(self.clients['assemblyai'].calls, calls)

    def test_changed_raw_review_evidence_is_not_accepted_or_reposted(self):
        self.normalization_review()
        path = self.folder() / 'terminal-job.json'
        result = io.read(io.binding(path))
        result['text'] = 'Different provider text.'
        self.rewrite(path, result)
        with self.assertRaises(runtime.CloudError):
            self.cycle()
        self.assertEqual(len(self.submits()), 1)

    def test_collection_review_must_replay_a_real_normalization_rejection(self):
        self.normalization_review()
        terminal_path = self.folder() / 'terminal-job.json'
        raw = io.read(io.binding(terminal_path))
        raw['text'] = 'Hello.'
        terminal_ref = self.rewrite(terminal_path, raw)
        review_path = self.folder() / 'collection-review.json'
        review = io.read(io.binding(review_path))
        review['raw_result'] = terminal_ref
        review['provider_job'] = terminal_ref
        self.rewrite(review_path, review)
        with self.assertRaisesRegex(runtime.CloudError, 'no longer replays'):
            runtime.status(self.ref)
        self.assertEqual(len(self.submits()), 1)

    def test_storage_errors_during_normalization_are_global_not_collection_review(self):
        self.prepare()
        self.cycle()
        self.clients['assemblyai'].poll_status = 'completed'
        with mock.patch.object(runtime, '_transcript_document', side_effect=OSError('unreadable proof')):
            with self.assertRaises(OSError):
                self.cycle()
        self.assertFalse((self.folder() / 'collection-review.json').exists())
        self.assertFalse((self.folder() / 'completion.json').exists())
        self.assertEqual(len(self.submits()), 1)

    def test_sigterm_after_post_still_persists_receipt_then_stops(self):
        self.add_recording()
        self.add_recording()
        self.prepare()
        self.clients['assemblyai'].after_post = lambda: setattr(self, 'stopped', True)
        result = self.cycle(max_new_jobs=2)
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(len(self.submits()), 1)
        self.assertTrue((self.folder() / 'submission.json').exists())

    def test_pause_signal_restores_previous_handler(self):
        before = signal.getsignal(signal.SIGTERM)
        with runtime.pause_signal() as stopped:
            self.assertFalse(stopped())
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            self.assertTrue(stopped())
        self.assertIs(signal.getsignal(signal.SIGTERM), before)

    def test_removed_submitted_job_folder_cannot_reset_reservation_and_rebill(self):
        self.prepare()
        self.cycle()
        self.folder().rename(self.root / 'retained-interrupted-job')
        with self.assertRaises((runtime.CloudError, io.Error)):
            self.cycle()
        self.assertEqual(len(self.submits()), 1)

    def test_removed_root_reservation_cannot_erase_existing_paid_intent(self):
        self.prepare()
        self.cycle()
        reservation = self.state / 'reservations' / (self.plan['recordings'][0]['job_id'] + '.json')
        reservation.rename(self.root / 'retained-reservation.json')
        with self.assertRaises((runtime.CloudError, io.Error)):
            self.cycle()
        self.assertEqual(len(self.submits()), 1)

    def test_root_reservation_without_job_intent_is_held_not_resubmitted(self):
        self.prepare()
        self.cycle()
        (self.folder() / 'intent.json').rename(self.root / 'retained-intent.json')
        (self.folder() / 'submission.json').rename(self.root / 'retained-submission.json')
        result = self.cycle()
        self.assertEqual(result['state'], 'reconciliation_required')
        self.assertGreater(result['reserved_microusd'], 0)
        self.assertEqual(len(self.submits()), 1)

    def test_paid_reservation_cannot_recreate_missing_spending_authority(self):
        self.prepare()
        self.cycle()
        (self.state / 'spending-limit.json').rename(self.root / 'retained-spending-limit.json')
        with self.assertRaises((runtime.CloudError, io.Error)):
            self.cycle(budget_microusd=20_000_000)
        self.assertEqual(len(self.submits()), 1)

    def test_changed_screen_proof_after_submission_blocks_poll_and_new_work(self):
        self.prepare()
        self.cycle()
        before = copy.deepcopy(self.clients['assemblyai'].calls)
        (self.folder() / 'screen.json').rename(self.root / 'retained-screen.json')
        with self.assertRaises((runtime.CloudError, io.Error)):
            self.cycle()
        self.assertEqual(self.clients['assemblyai'].calls, before)

    def test_intent_metadata_is_valid_for_rev_client_without_changing_fingerprint(self):
        self.prepare()
        self.cycle()
        intent = io.read(io.binding(self.folder() / 'intent.json'))
        options = clients.revai_options(intent['request_metadata'])
        self.assertEqual(options['metadata'], intent['request_metadata'])

    def test_poller_cannot_claim_a_different_remote_job_completed(self):
        self.prepare()
        self.cycle()
        provider = self.clients['assemblyai']
        provider.poll_status = 'completed'
        original = provider.poll
        with mock.patch.object(provider, 'poll', side_effect=lambda identifier: {**original(identifier), 'id': 'wrong-job'}):
            with self.assertRaises(clients.CloudClientError):
                self.cycle()
        self.assertFalse((self.folder() / 'completion.json').exists())
        self.assertTrue((self.folder() / 'audio.wav').exists())

    def test_completion_forged_text_with_resealed_local_links_is_rejected(self):
        self.prepare()
        self.complete_one()
        transcript = self.folder() / 'transcript.json'
        document = io.read(io.binding(transcript))
        document['text'] = 'Fabricated transcript content.'
        changed = self.rewrite(transcript, document)
        completion = self.folder() / 'completion.json'
        record = io.read(io.binding(completion))
        record['transcript'] = changed
        self.rewrite(completion, record)
        with self.assertRaises((runtime.CloudError, clients.CloudClientError, io.Error)):
            runtime.status(self.ref)
        with self.assertRaises((runtime.CloudError, clients.CloudClientError, io.Error)):
            runtime.export(self.ref)

    def test_completion_cannot_override_failed_terminal_evidence(self):
        self.prepare()
        self.complete_one()
        terminal = self.folder() / 'terminal-job.json'
        raw = io.read(io.binding(terminal))
        raw['status'] = 'error'
        changed = self.rewrite(terminal, raw)
        completion = self.folder() / 'completion.json'
        record = io.read(io.binding(completion))
        record['raw_result'] = changed
        self.rewrite(completion, record)
        with self.assertRaises((runtime.CloudError, clients.CloudClientError, io.Error)):
            runtime.status(self.ref)

    def test_reconcile_uses_get_only_and_matches_uploaded_audio(self):
        self.prepare()
        provider = self.clients['assemblyai']
        provider.post_error = clients.CloudClientError('lost receipt', ambiguous=True)
        with self.assertRaises(clients.CloudClientError):
            self.cycle()
        identifier = next(iter(provider.jobs))
        provider.post_error = None
        result = runtime.reconcile(self.ref, self.plan['recordings'][0]['job_id'], identifier, client=provider)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertTrue((self.folder() / 'reconciled.json').exists())
        self.assertEqual(len(self.submits()), 1)
        self.assertEqual(provider.calls[-1], ('poll', identifier))

    def test_paid_post_error_with_private_response_preserves_receipt_for_reconciliation(self):
        self.prepare()
        proof = {'id': 'existing-job', 'status': 'queued'}
        self.clients['assemblyai'].post_error = clients.CloudClientError('witness changed', ambiguous=True, response=proof)
        with self.assertRaises(clients.CloudClientError):
            self.cycle()
        self.assertEqual(io.read(io.binding(self.folder() / 'submission-untrusted-response.json')), proof)
        self.assertEqual(runtime.status(self.ref)['counts']['cloud_reconciliation_required'], 1)


if __name__ == '__main__':
    unittest.main()
