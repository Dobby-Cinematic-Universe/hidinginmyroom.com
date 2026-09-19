import copy
import unittest
from pipeline.cloud_timing_recovery import repair


class TimingRecoveryTests(unittest.TestCase):
    def test_only_end_changes_and_original_preserved(self):
        raw = {'text': 'Hello', 'utterances': [dict(start=10, end=20,
            text='Hello', speaker='A', words=[dict(start=10, end=40, text='Hello')])]}
        before = copy.deepcopy(raw)
        fixed, changes = repair(raw)
        self.assertEqual(raw, before)
        self.assertEqual(changes[0]['expansion_ms'], 20)
        fixed['utterances'][0]['end'] = 20
        self.assertEqual(fixed, raw)

    def test_large_expansion_rejected(self):
        with self.assertRaises(ValueError):
            repair({'utterances': [dict(end=1, words=[dict(end=2000)])]})

    def test_no_change_rejected(self):
        with self.assertRaises(ValueError):
            repair({'utterances': [dict(end=10, words=[dict(end=10)])]})
