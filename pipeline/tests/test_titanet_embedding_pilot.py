import unittest
from pipeline.titanet_embedding_pilot import select, unit, cosine, comparisons


class PilotTests(unittest.TestCase):
    def test_three_disjoint_windows_inside_guards(self):
        segments=[{'speaker':'A','start_ms':i*10000,'end_ms':i*10000+8000} for i in range(8)]
        clips=select(segments)
        self.assertEqual(len(clips),3)
        for clip in clips:
            s=segments[clip['segment_index']]
            self.assertGreaterEqual(clip['start_ms'],s['start_ms']+250)
            self.assertLessEqual(clip['end_ms'],s['end_ms']-250)
            self.assertEqual(clip['end_ms']-clip['start_ms'],5000)

    def test_short_and_overlapping_turns_excluded(self):
        self.assertEqual(select([dict(speaker='A',start_ms=0,end_ms=5000)]),[])
        self.assertEqual(select([dict(speaker='A',start_ms=0,end_ms=7000),dict(speaker='B',start_ms=1000,end_ms=9000)]),[])

    def test_finite_unit_vectors(self):
        a=[1.0]*192
        self.assertAlmostEqual(cosine(a,a),1)
        self.assertAlmostEqual(sum(x*x for x in unit(a)),1)
        for bad in ([0.0]*192,[float('nan')]*192,[1.0]*191):
            with self.assertRaises(ValueError):unit(bad)

    def test_comparisons_never_cross_recordings_or_assign_identity(self):
        rows=[dict(recording_id=r,provider_label=l,embedding=[1.0]*192) for r,l in [('one','A'),('one','A'),('one','B'),('two','A')]]
        result=comparisons(rows)
        self.assertEqual(len(result),2)
        self.assertTrue(all(r['recording_id']=='one' for r in result))
        self.assertTrue(all('identity' not in r for r in result))


if __name__=='__main__':unittest.main()
