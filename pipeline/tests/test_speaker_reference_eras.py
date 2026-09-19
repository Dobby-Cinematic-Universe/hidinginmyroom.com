import unittest
from pipeline.speaker_reference_eras import coverage, era, profile, windows


class EraTests(unittest.TestCase):
    def test_eras(self):
        self.assertEqual(era('2015-12-06'),'2015-2018')
        self.assertEqual(era('2020-11-06'),'2019-2022')
        self.assertEqual(era('2026-09-10'),'2023-2026')
        with self.assertRaises(ValueError):era('2014-01-01')

    def test_overlaps_not_double_counted(self):
        self.assertAlmostEqual(coverage([(0,3000),(2000,4000)],0,5000),.8)

    def test_bounded_disjoint_selection(self):
        clips=windows([(i,i+9000) for i in range(0,1000000,10000)],1000000)
        self.assertEqual(len(clips),3)
        self.assertTrue(all(c['end_ms']<=900000 for c in clips))
        self.assertTrue(all(b['start_ms']-a['start_ms']>=10000 for a,b in zip(clips,clips[1:])))

    def test_holdout_excludes_whole_video_and_never_confirms_identity(self):
        sources=[{'recording_id':r,'era':e,'title':r} for r,e in [('a','2015-2018'),('b','2015-2018'),('c','2023-2026')]]
        rows=[{'recording_id':s['recording_id'],'embedding':[1.0]*192} for s in sources for _ in range(3)]
        result=profile(rows,sources)
        self.assertFalse(result['identity_verified'])
        self.assertFalse(result['automatic_labeling_enabled'])
        self.assertIsNone(result['calibration']['threshold'])
        for h in result['held_out_recording_scores']:
            self.assertNotIn(h['query_recording_id'],h['reference_recording_ids'])
            self.assertIsNone(h['identity'])
            self.assertAlmostEqual(h['median_cosine'],1)

    def test_empty_embeddings_do_not_create_profiles(self):
        self.assertEqual(profile([],[])['era_profiles'],[])


if __name__=='__main__':unittest.main()
