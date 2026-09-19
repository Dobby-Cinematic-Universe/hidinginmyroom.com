import unittest
from pipeline.reviewed_voice_gallery import enroll,evaluate


class GalleryTests(unittest.TestCase):
    def fixture(self):
        rows=[dict(recording_id=str(i//2),clip={'path':f'/clip-{i}.wav','sha256':str(i)*64},
                   start_ms=i*5000,end_ms=(i+1)*5000,embedding=[1.0]*192) for i in range(6)]
        annotations=[dict(probe=i,clip=r['clip'],kind='single_speaker',speakers=['Daniel']) for i,r in enumerate(rows)]
        return {'rows':rows},annotations

    def test_mixed_tts_and_laugh_not_enrolled(self):
        data,a=self.fixture();a[1].update(kind='mixed_speakers',speakers=['Sunny','Daniel'],note='Sunny laugh only')
        a[2].update(kind='tts',speakers=[])
        g=enroll(data,a)
        self.assertEqual({s['probe'] for s in g['samples']},{0,3,4,5})
        self.assertEqual(len(g['challenge_examples']),2)
        self.assertFalse(g['automatic_labeling_enabled'])

    def test_holdout_excludes_whole_recording(self):
        data,a=self.fixture();g=enroll(data,a);d=evaluate(g)
        for t in d['trials']:
            self.assertTrue(all(data['rows'][i]['recording_id']!=data['rows'][t['probe']]['recording_id'] for i in t['reference_probes']))
            self.assertIsNone(t['identity'])

    def test_duplicate_and_changed_clip_rejected(self):
        data,a=self.fixture()
        with self.assertRaises(ValueError):enroll(data,a+[a[0]])
        a[0]['clip']={'path':'/wrong','sha256':'a'*64}
        with self.assertRaises(ValueError):enroll(data,a)

    def test_invalid_single_label_mixed_rejected(self):
        data,a=self.fixture();a[0]['speakers']=['Daniel','Sunny']
        with self.assertRaises(ValueError):enroll(data,a)


if __name__=='__main__':unittest.main()
