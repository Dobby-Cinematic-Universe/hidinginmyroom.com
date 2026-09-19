"""Large-input arithmetic and multipart framing without allocating GBs or calling APIs."""
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_client as client
from pipeline import cloud_transcription as cloud


class RevLargePreflightTests(unittest.TestCase):
    def test_seventeen_hour_pcm_fits_direct_upload_including_margin(self):
        maximum = client.REVAI_MAX_SECONDS
        pcm_bytes = maximum * 16000 * 2 + 65536
        self.assertLess(pcm_bytes, client.REVAI_MAX_UPLOAD_BYTES)
        self.assertEqual(cloud.route({'duration_ms': maximum * 1000})[0], 'revai')
        self.assertIsNone(cloud.route({'duration_ms': maximum * 1000 + 1})[0])

    def test_ten_hour_boundary_routes_to_rev_without_cutting(self):
        self.assertEqual(cloud.route({'duration_ms': 36_000_000})[0], 'assemblyai')
        self.assertEqual(cloud.route({'duration_ms': 36_000_001})[0], 'revai')

    def test_multipart_checks_total_request_not_just_audio(self):
        @contextmanager
        def descriptor(*args):
            yield 42, SimpleNamespace(st_size=client.REVAI_MAX_REQUEST_BYTES)
        with patch.object(client, '_upload_file', descriptor), patch.object(client, '_transport') as transport:
            with self.assertRaisesRegex(client.CloudClientError, 'multipart request exceeds'):
                client.RevAIClient('test-key').submit_file('/private/audio.wav', expected_sha256='a' * 64)
            transport.assert_not_called()

    def test_maximum_admitted_audio_has_room_for_longest_metadata(self):
        @contextmanager
        def descriptor(*args):
            yield 42, SimpleNamespace(st_size=client.REVAI_MAX_UPLOAD_BYTES)
        captured = []
        def request(self, method, path, **kwargs):
            captured.append(kwargs['data'].length)
            return {'id': 'offline-only', 'status': 'in_progress'}
        with patch.object(client, '_upload_file', descriptor), patch.object(client.RevAIClient, '_request', request):
            client.RevAIClient('test-key').submit_file('/private/audio.wav', expected_sha256='a' * 64,
                                                   metadata='x' * 128, diarization=True)
        self.assertGreater(captured[0], client.REVAI_MAX_UPLOAD_BYTES)
        self.assertLessEqual(captured[0], client.REVAI_MAX_REQUEST_BYTES)


if __name__ == '__main__':
    unittest.main()
