from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from pipeline import claude_non_graphic_recovery as recovery
from pipeline.tests.test_transcript_summary_core import source
from pipeline.tests.test_transcript_summary_anthropic import batch


class RecoveryTests(unittest.TestCase):
    def test_projection_preserves_evidence_and_omits_metadata(self):
        core = recovery.r.core
        job = core.initial_jobs([source(['A neutral statement with evidence.'])],
            {**core.DEFAULT_CONFIG, 'transcript_input_policy':'text_and_speaker_evidence_v1'})[0]
        before = deepcopy(job)
        config = deepcopy(job['config'])
        config.pop('transcript_input_policy')
        config['transcript_profile'] = 'anthropic_sonnet_batch'
        original = core.compact_input
        with recovery.projection():
            derived = core.make_job('chunk', job['scope'], job['evidence'], [], config)
            core.validate_job(derived)
        self.assertIs(core.compact_input, original)
        self.assertEqual(job, before)
        self.assertEqual(derived['evidence'], job['evidence'])
        self.assertEqual(derived['prompt']['input'], job['prompt']['input'])
        self.assertEqual(set(derived['prompt']['input']), {'stage','evidence'})
        self.assertEqual(derived['provider'], 'anthropic')

    def fixture(self, root):
        core = recovery.r.core
        with recovery.projection():
            job = core.initial_jobs([source(['A neutral statement.'])],
                {**core.DEFAULT_CONFIG, 'transcript_profile':'anthropic_sonnet_batch'})[0]
        return dict(state_root=str(root), maximum_cost_microusd=100000,
            wave=dict(provider='anthropic', model=job['model'],
                wave_id='summarywave_'+'a'*32, jobs=[job]))

    def test_submitted_never_posts_again(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            m = self.fixture(root)
            client = Mock()
            client.create_batch.return_value = batch()
            with patch.object(recovery, 'load', return_value=m):
                self.assertEqual(recovery.submit({}, client=client)['state'], 'submitted')
                self.assertEqual(recovery.submit({}, client=client)['state'], 'already_submitted')
            self.assertEqual(client.create_batch.call_count, 1)
            self.assertTrue((root/'requests.bin').exists())

    def test_ambiguous_post_preserves_intent_and_never_retries(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            client = Mock()
            client.create_batch.side_effect = RuntimeError('lost response')
            with patch.object(recovery, 'load', return_value=self.fixture(root)):
                with self.assertRaises(RuntimeError):
                    recovery.submit({}, client=client)
                self.assertEqual(recovery.submit({}, client=client)['state'], 'needs_reconciliation')
            self.assertEqual(client.create_batch.call_count, 1)
            self.assertTrue((root/'submit-intent.json').exists())


if __name__ == '__main__':
    unittest.main()
