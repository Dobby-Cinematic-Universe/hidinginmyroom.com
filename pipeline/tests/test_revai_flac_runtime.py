"""Rev AI FLAC transport preserves pre-POST protection and PCM result contracts."""
from pathlib import Path
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription as cloud
from pipeline.tests import test_cloud_transcription_runtime as fixtures
from pipeline.tests.test_cloud_transcription_client import Response, rev_job


@unittest.skipUnless(hasattr(cloud, 'upload_flow') and 'both_providers' in cloud.release.SEMANTICS.get('cloud_upload_transport',''), 'requires both-provider FLAC runtime')
class RevFlacTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.CloudRuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.add_recording(duration_ms=36_001_000)
        self.case.prepare()

    def test_flac_multipart_preserves_metadata_diarization_and_pcm_completion(self):
        provider = self.case.clients['revai']
        provider.duration_seconds = 36_001
        original = provider.submit_file
        def submit(path, **options):
            self.assertEqual(Path(path).suffix, '.flac')
            self.assertIn('metadata', options)
            self.assertIn('diarization', options)
            self.assertTrue((self.case.folder()/'intent.json').exists())
            return original(path, **options)
        with patch.object(provider, 'submit_file', side_effect=submit) as call:
            self.case.cycle()
        call.assert_called_once()
        provider.poll_status = 'transcribed'
        result = self.case.cycle(allow_paid_api=False, budget_microusd=None)
        self.assertEqual(result['counts']['cloud_completed'], 1)
        completion = cloud.io.read(cloud.io.binding(self.case.folder()/'completion.json'))
        self.assertTrue(completion['audio']['path'].endswith('.wav'))
        self.assertFalse((self.case.folder()/'audio.flac').exists())

    def test_ambiguous_rev_post_is_not_upload_backoff_or_repeated(self):
        provider = self.case.clients['revai']
        provider.post_error = cloud.clients.CloudClientError('cloud request transport failed', ambiguous=True)
        with self.assertRaises(cloud.clients.CloudClientError):
            self.case.cycle()
        result = self.case.cycle()
        self.assertEqual(result['state'], 'reconciliation_required')
        self.assertEqual(len(self.case.submits()), 1)
        self.assertFalse((self.case.folder()/'upload-backoff').exists())

    def test_size_rejected_before_paid_intent(self):
        with patch.object(cloud.upload_flow, 'prepare', return_value={'path':'/private/test.flac','sha256':'a'*64,'byte_count':cloud.clients.REVAI_MAX_UPLOAD_BYTES+1}):
            with self.assertRaisesRegex(RuntimeError, 'FLAC exceeds Rev AI'):
                self.case.cycle()
        self.assertFalse((self.case.folder()/'intent.json').exists())
        self.assertFalse(self.case.submits())

    def test_real_flac_bytes_pass_through_streaming_multipart_encoder(self):
        row = self.case.recordings[0]
        audio = self.case.audio_builder(row, self.case.folder(), self.case.plan['ffmpeg'])
        flac = cloud.upload_flow.prepare(audio, self.case.folder(), self.case.plan['ffmpeg'])
        captured = []
        def transport(request, timeout):
            body = b''.join(request.data)
            self.assertIn(b'filename="audio.flac"', body)
            self.assertIn(Path(flac['path']).read_bytes(), body)
            self.assertIn(b'"skip_diarization":false', body)
            self.assertEqual(int(request.get_header('Content-length')), len(body))
            captured.append(True)
            return Response(rev_job())
        with patch.object(cloud.clients, '_transport', side_effect=transport):
            cloud.clients.RevAIClient('test-key').submit_file(flac['path'], expected_sha256=flac['sha256'],
                                                          metadata='test_fingerprint', diarization=True)
        self.assertEqual(captured, [True])


if __name__ == '__main__':
    unittest.main()
