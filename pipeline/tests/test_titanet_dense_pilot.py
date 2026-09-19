import unittest
from pipeline.titanet_embedding_dense_pilot import select


class DensePilotTests(unittest.TestCase):
    def fixture(self, confidence=.95):
        segments=[dict(speaker='A',start_ms=i*20000,end_ms=i*20000+10000) for i in range(4)]
        utterances=[dict(words=[dict(start=s['start_ms']+j*500,end=s['start_ms']+j*500+450,confidence=confidence) for j in range(20)]) for s in segments]
        return segments,utterances

    def test_confident_speech_and_guards(self):
        segments,utterances=self.fixture()
        clips=select(segments,utterances)
        self.assertEqual(len(clips),3)
        for c in clips:
            s=segments[c['segment_index']]
            self.assertGreaterEqual(c['start_ms'],s['start_ms']+250)
            self.assertLessEqual(c['end_ms'],s['end_ms']-250)
            self.assertGreaterEqual(c['word_coverage_fraction'],.6)
        self.assertTrue(all(a['end_ms']<=b['start_ms'] or b['end_ms']<=a['start_ms'] for i,a in enumerate(clips) for b in clips[i+1:]))

    def test_low_confidence_not_enrollment_material(self):
        self.assertEqual(select(*self.fixture(.4)),[])

    def test_missing_words_not_silently_center_sampled(self):
        segments,_=self.fixture()
        self.assertEqual(select(segments,[dict(words=[]) for _ in segments]),[])

    def test_overlap_not_eligible(self):
        segments,utterances=self.fixture()
        duplicate={**segments[0],'speaker':'B'}
        clips=select(segments+[duplicate],utterances+[utterances[0]])
        self.assertTrue(all(c['segment_index'] not in {0,4} for c in clips))


if __name__=='__main__':unittest.main()
