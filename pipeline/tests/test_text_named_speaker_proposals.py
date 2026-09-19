import unittest
from pipeline.text_named_speaker_proposals import stats


class TextProposalTests(unittest.TestCase):
    def test_overlap_not_double_counted(self):
        s=[dict(speaker='A',start_ms=0,end_ms=20000)]*5
        result=stats(s)['A']
        self.assertEqual(result['labeled_duration_ms'],20000)
        self.assertFalse(result['substantial'])

    def test_both_duration_and_turns_required(self):
        self.assertFalse(stats([dict(speaker='A',start_ms=0,end_ms=60000)])['A']['substantial'])
        s=[dict(speaker='A',start_ms=i*12000,end_ms=(i+1)*12000) for i in range(5)]
        self.assertTrue(stats(s)['A']['substantial'])
