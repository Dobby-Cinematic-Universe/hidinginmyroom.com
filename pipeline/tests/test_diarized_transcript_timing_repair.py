from copy import deepcopy
import unittest
from pipeline.diarized_transcript_timing_repair import repair


class TimingTests(unittest.TestCase):
    def test_retained_word_end_repairs_without_changing_words_text_or_label(self):
        raw=dict(text='unaltered',utterances=[dict(start=10,end=1000,speaker='A',
            text='unchanged',words=[dict(start=10,end=2247,text='unchanged')])])
        before=deepcopy(raw)
        adjusted,changes=repair(raw)
        self.assertEqual(raw,before)
        self.assertEqual(changes[0]['expansion_ms'],1247)
        adjusted['utterances'][0]['end']=1000
        self.assertEqual(adjusted,raw)

    def test_large_or_absent_repair_is_not_accepted(self):
        for end in (1000,5000):
            with self.assertRaises(ValueError):
                repair(dict(utterances=[dict(end=1000,words=[dict(end=end)])]))
