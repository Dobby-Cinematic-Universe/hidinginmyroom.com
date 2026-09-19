"""Real lossless encoding and isolated upload failures, with fake providers."""
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

from pipeline import cloud_transcription as cloud
from pipeline.tests import test_cloud_transcription_runtime as fixtures


@unittest.skipUnless(hasattr(cloud, 'upload_flow'), 'requires FLAC runtime')
class UploadTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.CloudRuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.flow = cloud.upload_flow

    def test_transient_upload_defers_only_that_record_and_second_submits(self):
        case = self.case
        case.add_recording(); case.add_recording(); case.prepare()
        provider = case.clients['assemblyai']
        original = provider.upload
        attempted = []
        def upload(path, **options):
            attempted.append(path)
            self.assertEqual(Path(path).suffix, '.flac')
            if len(attempted) == 1:
                raise cloud.clients.CloudClientError('cloud request transport failed', ambiguous=True)
            return original(path, **options)
        with patch.object(provider, 'upload', side_effect=upload):
            result = case.cycle(max_new_jobs=2, max_active=4)
            self.assertEqual(result['new_paid_requests'], 1)
            self.assertEqual(len(result['upload_deferrals']), 1)
            failed = Path(attempted[0]).parent
            self.assertFalse((failed/'intent.json').exists())
            self.assertFalse((case.state/'reservations'/(failed.name+'.json')).exists())
            self.assertFalse(self.flow.ready(failed))
            case.cycle(max_new_jobs=2, max_active=4)
            self.assertEqual(len(attempted), 2)

    def test_auth_failure_is_not_hidden_by_backoff(self):
        self.case.add_recording(); self.case.prepare()
        error = cloud.clients.CloudClientError('cloud request failed with an HTTP status', status_code=401)
        with patch.object(self.case.clients['assemblyai'], 'upload', side_effect=error):
            with self.assertRaises(cloud.clients.CloudClientError):
                self.case.cycle()
        self.assertFalse((self.case.folder()/'upload-backoff').exists())

    def test_backoff_survives_new_reader_and_honors_retry_after(self):
        self.case.add_recording(); self.case.prepare()
        folder = self.case.folder()
        error = cloud.clients.CloudClientError('cloud request failed with an HTTP status', status_code=429, retry_after_seconds=1800)
        self.flow.defer(folder, error, now=100)
        self.assertFalse(self.flow.ready(folder, now=1899))
        self.assertTrue(self.flow.ready(folder, now=1900))
        self.flow.defer(folder, error, now=1900)
        self.assertEqual(len(self.flow.failures(folder)), 2)

    def test_upload_attempts_per_cycle_are_bounded_even_when_all_fail(self):
        for _ in range(3): self.case.add_recording()
        self.case.prepare()
        error = cloud.clients.CloudClientError('cloud request transport failed', ambiguous=True)
        with patch.object(self.case.clients['assemblyai'], 'upload', side_effect=error) as upload:
            result = self.case.cycle(max_new_jobs=2, max_active=4)
        self.assertEqual(upload.call_count, 2)
        self.assertEqual(result['new_paid_requests'], 0)
        self.assertEqual(len(result['upload_deferrals']), 2)

    def test_real_flac_verified_reused_and_changed_copy_rejected(self):
        row = self.case.add_recording(); self.case.prepare()
        folder = self.case.folder()
        audio = self.case.audio_builder(row, folder, self.case.plan['ffmpeg'])
        payload = self.flow.prepare(audio, folder, self.case.plan['ffmpeg'])
        self.assertLess(payload['byte_count'], audio['byte_count'])
        with patch.object(self.flow.subprocess, 'run', side_effect=AssertionError('reencoded')):
            self.assertEqual(self.flow.prepare(audio, folder, self.case.plan['ffmpeg']), payload)
        path = Path(payload['path'])
        path.write_bytes(path.read_bytes()+b'changed')
        with self.assertRaises(RuntimeError):
            self.flow.prepare(audio, folder, self.case.plan['ffmpeg'])

    def test_complete_orphan_flac_adopted_only_after_lossless_check(self):
        row = self.case.add_recording(); self.case.prepare()
        folder = self.case.folder()
        audio = self.case.audio_builder(row, folder, self.case.plan['ffmpeg'])
        payload = self.flow.prepare(audio, folder, self.case.plan['ffmpeg'])
        (folder/'flac-transport.json').rename(folder/'old-proof.json')
        self.assertEqual(self.flow.prepare(audio, folder, self.case.plan['ffmpeg']), payload)

    def test_completion_prunes_only_regenerable_upload_copies(self):
        row = self.case.add_recording(); self.case.prepare()
        self.case.cycle()
        self.assertTrue((self.case.folder()/'audio.flac').exists())
        self.case.clients['assemblyai'].poll_status = 'completed'
        self.case.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertFalse((self.case.folder()/'audio.flac').exists())
        self.assertFalse((self.case.folder()/'audio.wav').exists())
        self.assertTrue(Path(row['media']['path']).exists())
        self.assertTrue((self.case.folder()/'flac-transport.json').exists())

    def test_runner_does_not_repeat_upload_or_paid_post_inline(self):
        from pipeline import cloud_transcription_resilient as runner
        client = Mock()
        error = cloud.clients.CloudClientError('cloud request transport failed', ambiguous=True)
        client.upload.side_effect = error
        client.submit.side_effect = error
        proxy = runner.RetryingClient('assemblyai', client, deadline=10**12)
        with self.assertRaises(cloud.clients.CloudClientError):
            proxy.upload('/private/audio.flac', expected_sha256='a'*64)
        with self.assertRaises(cloud.clients.CloudClientError):
            proxy.submit('https://cdn.assemblyai.com/upload/test')
        client.upload.assert_called_once()
        client.submit.assert_called_once()

    def test_runner_retains_safe_get_retry(self):
        from pipeline import cloud_transcription_resilient as runner
        client = Mock()
        client.poll.side_effect = [cloud.clients.CloudClientError('cloud request transport failed'), {'state':'okay'}]
        proxy = runner.RetryingClient('assemblyai', client, deadline=10**12)
        with patch.object(runner, '_wait'):
            self.assertEqual(proxy.poll('id'), {'state':'okay'})
        self.assertEqual(client.poll.call_count, 2)


if __name__ == '__main__':
    unittest.main()
