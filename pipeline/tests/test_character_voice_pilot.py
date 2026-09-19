import unittest
from pipeline.character_voice_pilot import select, score, cluster


class CharacterVoiceTests(unittest.TestCase):
    def row(self, vector, recording='one', name='Mila'):
        return {'embedding':vector,'recording_id':recording,'title_name_lead':name,
                'clip':{'path':'/test.wav','sha256':'a'*64},'start_ms':0,'end_ms':5000}

    def test_bounded_disjoint_sampling(self):
        values=select([],600000)
        self.assertEqual(len(values),12)
        self.assertTrue(all(v['start_ms']>=250 and v['end_ms']<=599750 for v in values))
        self.assertTrue(all(b['start_ms']>=a['end_ms'] for a,b in zip(values,values[1:])))

    def test_subtitle_guidance_not_named_identity(self):
        values=select([(0,600000)],600000)
        self.assertTrue(all('identity' not in v for v in values))

    def test_even_exact_reference_match_stays_unconfirmed(self):
        v=[1.0]*192
        result=score([self.row(v)],[{'era':'early','embedding':v},{'era':'recent','embedding':v}])[0]
        self.assertEqual(result['reference_similarity_hint'],'strong')
        self.assertIsNone(result['identity'])
        self.assertEqual(result['decision'],'unknown_unverified_references')

    def test_nonmatch_is_not_assigned_title_name(self):
        result=score([self.row([-1.0]*192)],[{'era':'early','embedding':[1.0]*192}])[0]
        self.assertIsNone(result['identity'])
        self.assertEqual(result['reference_similarity_hint'],'weak_or_conflicting')

    def test_groups_are_anonymous_even_across_same_named_titles(self):
        groups=cluster([self.row([1.0]*192),self.row([1.0]*192,'two')])
        self.assertEqual(len(groups),1)
        self.assertEqual(groups[0]['recording_count'],2)
        self.assertIsNone(groups[0]['identity'])

    def test_missing_embeddings_excluded(self):
        self.assertEqual(cluster([self.row(None)]),[])
        self.assertEqual(score([self.row(None)],[{'era':'early','embedding':[1.0]*192}]),[])


if __name__=='__main__':unittest.main()
