"""Pilot restart protection, tested with no network or real credentials."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline import revai_pilot as pilot
from pipeline.tests.test_cloud_transcription_client import rev_job, rev_result


class PilotTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.enterContext(patch.object(pilot, 'ROOT', self.root))
        plan = pilot.io.put(self.root / 'plan.json', {'test_only': True})
        self.enterContext(patch.object(pilot, 'PLAN', plan))
        self.enterContext(patch.object(pilot.env, 'api_key', return_value='test-key'))
        self.client = Mock()
        self.enterContext(patch.object(pilot.client_module, 'RevAIClient', return_value=self.client))
        pilot.io.put(self.root / 'manifest.json', {
            'client_sha256': pilot.digest_file(pilot.client_module.__file__), 'cloud_plan': plan,
            'maximum_pilot_microusd': pilot.PILOT_ALLOWANCE, 'metadata': 'test_pilot',
            'duration_seconds': 10, 'audio': {'path': str(self.root / 'audio.wav'), 'sha256': 'a' * 64}})

    def test_paid_flag_required(self):
        with self.assertRaisesRegex(RuntimeError, 'requires --allow-paid-api'):
            pilot.run('submit', '/unused.env', False)
        self.client.submit_file.assert_not_called()

    def test_success_receipt_makes_second_submit_noop(self):
        self.client.submit_file.return_value = rev_job()
        self.assertEqual(pilot.run('submit', '/unused.env', True)['new_paid_requests'], 1)
        self.assertEqual(pilot.run('submit', '/unused.env', True)['new_paid_requests'], 0)
        self.assertTrue((self.root / 'reservation.json').exists())
        self.client.submit_file.assert_called_once()

    def test_ambiguous_post_keeps_intent_and_blocks_resubmission(self):
        self.client.submit_file.side_effect = pilot.client_module.CloudClientError('transport failure', ambiguous=True)
        with self.assertRaises(pilot.client_module.CloudClientError):
            pilot.run('submit', '/unused.env', True)
        with self.assertRaisesRegex(RuntimeError, 'reconcile'):
            pilot.run('submit', '/unused.env', True)
        self.assertTrue((self.root / 'submission-error.json').exists())
        self.client.submit_file.assert_called_once()

    def test_collection_replays_saved_result_without_paid_or_get_calls(self):
        pilot.io.put(self.root / 'submission.json', rev_job())
        self.client.poll.return_value = rev_job()
        self.client.transcript.return_value = rev_result()
        first = pilot.run('collect', '/unused.env', False)
        second = pilot.run('collect', '/unused.env', False)
        self.assertEqual(first, second)
        self.assertEqual(first['state'], 'completed')
        self.assertFalse(first['normalized_word_timestamps'])
        self.client.poll.assert_called_once()
        self.client.transcript.assert_called_once()
        self.client.submit_file.assert_not_called()


if __name__ == '__main__':
    unittest.main()
