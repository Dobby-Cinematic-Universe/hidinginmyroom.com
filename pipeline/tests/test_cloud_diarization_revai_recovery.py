from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline import cloud_diarization_batch as batch
from pipeline import cloud_diarization_revai_recovery as recovery
from pipeline import cloud_transcription_client as clients
from pipeline import reviewed_transcript_feed as feed
from pipeline import transcript_summary as io
from pipeline.tests.test_cloud_transcription_client import Response, rev_job, rev_result


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        recovery.STOP = False
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.folder = self.root / 'jobs' / recovery.JOB
        self.folder.mkdir(parents=True, mode=0o700)
        (self.root / 'reservations').mkdir(mode=0o700)
        self.recovery_root = self.root / 'recovery'
        self.recovery_root.mkdir(mode=0o700)
        self.media = dict(path='/retained/source', sha256=recovery.MEDIA_SHA, byte_count=1000)
        self.row = dict(job_id=recovery.JOB, provider='assemblyai', language='auto', diarization=True,
            maximum_cost_microusd=10_000, retained_third_party=dict(transcript=dict(path='/original/transcript', sha256='b'*64)),
            analyst_candidate=dict(id='G07oXmwgq3s'), recording=dict(recording_id='physical-recording',
                duration_ms=10000, title='dinner and owl cafe with bloodbucket', media=self.media))
        self.plan = dict(state_root=str(self.root), recordings=[self.row], prior_plans=[],
            ffmpeg=dict(path='/usr/bin/ffmpeg', sha256='a'*64), maximum_cost_microusd=10_000,
            budget=dict(combined_ceiling_microusd=92_315_445))
        self.plan_ref = io.put(self.root / 'plan.json', self.plan)
        self.original_hold = dict(plan=self.plan_ref, job_id=recovery.JOB,
            reason='cloud request transport failed', state='needs_review')
        io.put(self.folder / 'hold.json', self.original_hold)
        audio = dict(path=str(self.folder / 'audio.wav'), sha256='c'*64,
            duration_ms=10000, channels=1, sample_rate_hz=16000, sample_width_bytes=2)
        payload = b'fLaC a small stable test payload'
        self.flac = self.folder / 'audio.flac'
        self.flac.write_bytes(payload)
        self.flac.chmod(0o600)
        self.transport = dict(path=str(self.flac), sha256=hashlib.sha256(payload).hexdigest(), byte_count=len(payload))
        self.manifest = dict(state_root=str(self.recovery_root), plan=self.plan_ref,
            job=io.put(self.folder / 'job.json', dict(plan=self.plan_ref, recording=self.row)),
            screen=io.put(self.folder / 'screen.json', dict(plan=self.plan_ref,
                recording_id='physical-recording', diarization=True)),
            audio_receipt=io.put(self.folder / 'audio.json', dict(audio=audio, source=self.media, ffmpeg=self.plan['ffmpeg'])),
            flac_receipt=io.put(self.folder / 'flac-transport.json', dict(audio=audio, transport=self.transport,
                ffmpeg=self.plan['ffmpeg'], lossless_verified=True)),
            original_hold=io.put(self.recovery_root / 'assemblyai-upload-hold.json', self.original_hold),
            maximum_cost_microusd=1000)
        self.ref = io.put(self.recovery_root / 'manifest.json', self.manifest)
        self.api = Mock()
        self.terminal = rev_job()
        self.raw = rev_result()

        def submit(path, *, expected_sha256, metadata, diarization):
            self.assertEqual(path, str(self.flac))
            self.assertEqual(expected_sha256, self.transport['sha256'])
            self.assertTrue(diarization)
            self.assertTrue((self.folder / 'intent.json').exists())
            self.assertTrue((self.root / 'reservations' / (recovery.JOB + '.json')).exists())
            self.terminal['metadata'] = metadata
            return {**self.terminal, 'status': 'in_progress'}
        self.api.submit_file.side_effect = submit
        self.api.poll.side_effect = lambda _: deepcopy(self.terminal)
        self.api.transcript.side_effect = lambda _: deepcopy(self.raw)
        for patcher in (
                patch.object(recovery, 'bootstrap', return_value=(self.manifest, self.plan, self.row, batch)),
                patch.object(recovery, 'FLAC_SHA', self.transport['sha256']),
                patch.object(recovery, 'observed_client', return_value=self.api),
                patch.object(batch.env, 'api_key', return_value='test-only-key'),
                patch.object(recovery, 'sleep')):
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch('builtins.print')
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_recovery(self, allow_paid=True):
        return recovery.run(self.ref, '/unused/.env', allow_paid)

    def test_success_preserves_sources_and_collection_replay_is_free(self):
        self.assertEqual(self.run_recovery(), 'completed')
        doc = io.read(io.binding(self.folder / 'transcript.json'))
        self.assertEqual(doc['provider'], 'revai')
        self.assertEqual(doc['model'], 'machine')
        self.assertEqual(doc['provider_fallback'], self.ref)
        self.assertEqual(doc['source_media'], self.media)
        self.assertEqual(doc['source_third_party_preserved'], self.row['retained_third_party']['transcript'])
        self.assertTrue(doc['whole_recording_submitted'])
        self.assertFalse(doc['human_reviewed'])
        self.assertEqual(doc['segments'][0]['start_ms'], 100)
        self.assertEqual(doc['segments'][0]['end_ms'], 1200)
        self.assertTrue(all('words' not in s for s in doc['segments']))
        self.assertTrue(all(s['speaker'] is None for s in doc['segments']))
        self.assertEqual(doc['provider_speaker_labels'], {})
        self.assertEqual(io.read(io.binding(self.folder / 'provider-transcript.json')), self.raw)
        self.assertEqual(io.read(io.binding(self.folder / 'hold.json')), self.original_hold)
        self.assertTrue(self.flac.exists())
        self.assertEqual(self.run_recovery(False), 'completed')
        self.assertEqual(self.api.submit_file.call_count, 1)
        aggregate = io.read(io.binding(self.root / 'status.json'))
        self.assertEqual(aggregate['states'], {'completed': 1})
        self.assertEqual(aggregate['records'][0]['provider'], 'revai')

    def test_multiple_labels_enter_existing_review_feed_but_not_summaries(self):
        self.raw = dict(monologues=[dict(speaker=0, elements=[dict(type='text', value='Hello ' * 30, ts=.1, end_ts=4)]),
            dict(speaker=1, elements=[dict(type='text', value='Reply ' * 30, ts=5, end_ts=9)])])
        self.assertEqual(self.run_recovery(), 'completed')
        report = io.put(self.root / 'review.json', dict(reports=[]))
        confirmations = io.put(self.root / 'confirmations.json', dict(records=[]))
        decisions = self.root / 'decisions'
        decisions.mkdir(mode=0o700)
        output = self.root / 'feed'
        worker = feed.Feed(report['path'], confirmations['path'], decisions, [self.plan_ref['path']], output)
        value = worker.scan()
        self.assertEqual(value['recordings'], 1)
        self.assertEqual(value['review_complete'], 0)
        self.assertEqual(value['summary_eligible'], 0)

    def test_interrupted_paid_post_cannot_be_repeated(self):
        self.api.submit_file.side_effect = clients.CloudClientError('transport failed', ambiguous=True)
        self.assertEqual(self.run_recovery(), 'reconciliation_required')
        self.assertEqual(self.run_recovery(), 'held')
        self.assertEqual(self.api.submit_file.call_count, 1)
        self.assertFalse((self.folder / 'submission.json').exists())
        self.assertTrue((self.folder / 'intent.json').exists())

    def test_saved_receipt_resumes_without_another_post(self):
        submit = self.api.submit_file.side_effect
        def pause_after_submit(*args, **kwargs):
            result = submit(*args, **kwargs)
            recovery.STOP = True
            return result
        self.api.submit_file.side_effect = pause_after_submit
        self.assertEqual(self.run_recovery(), 'paused')
        recovery.STOP = False
        self.assertEqual(self.run_recovery(False), 'completed')
        self.assertEqual(self.api.submit_file.call_count, 1)

    def test_mismatched_receipt_is_retained_without_repurchase(self):
        submit = self.api.submit_file.side_effect
        def wrong_metadata(*args, **kwargs):
            return {**submit(*args, **kwargs), 'metadata': 'another-recording'}
        self.api.submit_file.side_effect = wrong_metadata
        self.assertEqual(self.run_recovery(), 'needs_review')
        self.assertTrue((self.folder / 'submission.json').exists())
        self.assertEqual(self.run_recovery(), 'held')
        self.assertEqual(self.api.submit_file.call_count, 1)
        self.api.transcript.assert_not_called()

    def test_transient_get_failure_does_not_repeat_paid_submission(self):
        calls = []
        def poll(_):
            calls.append(1)
            if len(calls) == 1:
                raise clients.CloudClientError('cloud request failed with an HTTP status', status_code=503)
            return deepcopy(self.terminal)
        self.api.poll.side_effect = poll
        self.assertEqual(self.run_recovery(), 'completed')
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.api.submit_file.call_count, 1)

    def test_every_paid_marker_and_reservation_blocks_first_post(self):
        for name in batch.PAID_FILES:
            with self.subTest(name=name):
                path = self.folder / name
                io.put(path, {'evidence': True})
                with self.assertRaises(recovery.RecoveryError):
                    recovery.submission_state(self.folder, self.root / 'reservations', {}, batch)
                path.unlink()
        io.put(self.root / 'reservations' / (recovery.JOB + '.json'), {'evidence': True})
        with self.assertRaises(recovery.RecoveryError):
            recovery.submission_state(self.folder, self.root / 'reservations', {}, batch)

    def test_changed_flac_fails_before_reservation_and_paid_post(self):
        self.flac.write_bytes(b'changed')
        self.assertEqual(self.run_recovery(), 'needs_review')
        self.api.submit_file.assert_not_called()
        self.assertFalse((self.folder / 'intent.json').exists())

    def test_missing_approval_cannot_submit(self):
        self.assertEqual(self.run_recovery(False), 'needs_review')
        self.api.submit_file.assert_not_called()

    def test_batch_budget_still_blocks_an_overallocation(self):
        io.put(self.root / 'reservations' / 'existing-job.json', dict(maximum_cost_microusd=batch.ALLOCATION))
        self.assertEqual(self.run_recovery(), 'needs_review')
        self.api.submit_file.assert_not_called()
        self.assertFalse((self.folder / 'intent.json').exists())

    def test_changed_original_hold_cannot_be_bypassed(self):
        (self.folder / 'hold.json').unlink()
        io.put(self.folder / 'hold.json', dict(state='needs_review', reason='unrelated new failure'))
        self.assertEqual(self.run_recovery(), 'needs_review')
        self.api.submit_file.assert_not_called()

    def test_foreign_provider_receipt_cannot_be_collected_as_revai(self):
        io.put(self.folder / 'intent.json', dict(provider='assemblyai'))
        io.put(self.folder / 'submission.json', dict(id='old-job'))
        io.put(self.root / 'reservations' / (recovery.JOB + '.json'), dict(provider='assemblyai', maximum_cost_microusd=1000))
        self.assertEqual(self.run_recovery(), 'needs_review')
        self.api.submit_file.assert_not_called()
        self.api.poll.assert_not_called()

    def test_selection_provider_and_budget_are_bounded(self):
        _, cost = recovery.selection(self.plan, batch)
        self.assertLess(cost, self.row['maximum_cost_microusd'])
        for field, value in [('provider', 'revai'), ('diarization', False), ('maximum_cost_microusd', 0)]:
            altered = deepcopy(self.plan)
            altered['recordings'][0][field] = value
            with self.subTest(field=field), self.assertRaises(recovery.RecoveryError):
                recovery.selection(altered, batch)


class StreamingTests(unittest.TestCase):
    def test_observer_preserves_multipart_options_and_body_verification(self):
        recovery.STOP = False
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'audio.flac'
            payload = b'fLaC retained whole recording'
            path.write_bytes(payload)
            path.chmod(0o600)
            progress = Mock()
            api = recovery.observed_client(clients, 'test-key', progress)
            requests = []
            def transport(request, timeout):
                body = b''.join(request.data)
                requests.append(body)
                self.assertEqual(int(request.headers['Content-length']), len(body))
                self.assertEqual(timeout, 3600)
                self.assertIn(b'"transcriber":"machine"', body)
                self.assertIn(b'"skip_diarization":false', body)
                self.assertIn(b'"metadata":"test-metadata"', body)
                self.assertIn(payload, body)
                return Response(rev_job())
            with patch.object(clients, '_transport', side_effect=transport):
                response = api.submit_file(str(path), expected_sha256=hashlib.sha256(payload).hexdigest(),
                    metadata='test-metadata', diarization=True)
            self.assertEqual(response, rev_job())
            self.assertEqual(len(requests), 1)
            self.assertFalse(progress.call_args.kwargs['provider_acceptance_confirmed'])
            self.assertEqual(progress.call_args.kwargs['bytes_sent'], len(requests[0]))


if __name__ == '__main__':
    unittest.main()
