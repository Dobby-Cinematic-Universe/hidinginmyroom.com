from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from pipeline import gemini_unknown_replacement as recovery


class ReplacementTests(unittest.TestCase):
    def test_intent_prevents_second_post_after_ambiguous_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = root/'original'
            original.mkdir(mode=0o700)
            wave = dict(wave_id='summarywave_'+'a'*32, provider='gemini', model='gemini-3.8-flash',
                jobs=[dict(job_id='summaryjob_'+'b'*32, request=dict(body={}))],
                explicit_unknown_replacement=dict(original_wave=dict(path=str(original/'wave.json'))))
            folder = root/wave['wave_id']
            folder.mkdir(mode=0o700)
            manifest = dict(state_root=str(root), rows=[dict(wave=wave)])
            api = Mock()
            api.batch_bytes.return_value = b'{}'
            api.create_batch.side_effect = RuntimeError('ambiguous response')
            with patch.object(recovery, 'load', return_value=manifest):
                with self.assertRaises(RuntimeError):
                    recovery.submit({}, client=api)
                result = recovery.submit({}, client=api)
            self.assertEqual(result['outcomes'][0]['state'], 'needs_reconciliation')
            self.assertEqual(api.create_batch.call_count, 1)
            self.assertTrue((folder/'submit-intent.json').exists())
            self.assertFalse(list(original.iterdir()))

    def test_late_original_receipt_prevents_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = root/'original'
            original.mkdir(mode=0o700)
            recovery.r.put(original/'submitted.json', dict(remote_id='batches/original'))
            wave = dict(wave_id='summarywave_'+'a'*32,
                explicit_unknown_replacement=dict(original_wave=dict(path=str(original/'wave.json'))))
            (root/wave['wave_id']).mkdir(mode=0o700)
            api = Mock()
            with patch.object(recovery, 'load', return_value=dict(state_root=str(root), rows=[dict(wave=wave)])):
                with self.assertRaisesRegex(RuntimeError, 'original now has a receipt'):
                    recovery.submit({}, client=api)
            api.create_batch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
