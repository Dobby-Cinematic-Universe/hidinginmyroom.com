import unittest
from pathlib import Path
import tempfile
from pipeline.event_embedding_groups import required_similarity,cluster


def row(i,dates=()):return dict(id=str(i),text='Example',entities=['mila'],dates=list(dates),recordings=[str(i)])


class GroupingTests(unittest.TestCase):
    def test_year_conflict_not_recording_date(self):
        self.assertGreater(required_similarity(row(0,['2016']),row(1,['2020']),{'mila'}),1)
        a,b=row(0),row(1);a['recording_date']='2016';b['recording_date']='2026'
        self.assertEqual(required_similarity(a,b,{'mila'}),.82)

    def test_generic_daniel_alone_requires_stronger_similarity(self):
        self.assertEqual(required_similarity(row(0),row(1),set()),.90)

    def test_distinct_transitions_not_conflated_but_disagreements_preserved(self):
        a,b=row(0),row(1);a['text']='Daniel married Chihiro.';b['text']='Daniel divorced Chihiro.'
        self.assertGreater(required_similarity(a,b,{'mila'}),1)
        a['text']='Daniel did not divorce Chihiro.'
        self.assertEqual(required_similarity(a,b,{'mila'}),.82)

    def test_complete_link_prevents_transitive_topic_chain(self):
        import numpy as np
        vectors=np.array([[1,0],[.8660254,.5],[.5,.8660254]],dtype=np.float32)
        result=cluster([row(i) for i in range(3)],vectors,[dict(id='mila',label='Mila',type='person')])
        self.assertEqual(len(result),1);self.assertEqual(len(result[0]['members']),2)

    def test_group_size_bounded_and_ids_deterministic(self):
        import numpy as np
        rows=[row(i) for i in range(5)];vectors=np.ones((5,1),dtype=np.float32)
        groups=cluster(rows,vectors,[],max_group=3)
        self.assertTrue(all(len(g['members'])<=3 for g in groups))
        self.assertEqual(groups,cluster(rows,vectors,[],max_group=3))

    @unittest.skipUnless(Path('research/corpus/event-grouping-runtime-20260917/model.onnx').exists(),'local model not installed')
    def test_real_model_ignores_tokenizer_padding_and_reuses_exact_vectors(self):
        import numpy as np
        from pipeline.event_embedding_groups import embeddings
        runtime=Path('research/corpus/event-grouping-runtime-20260917').resolve()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name in ('model.onnx','tokenizer.json'):(root/name).symlink_to(runtime/name)
            rows=[dict(text=t) for t in ('Daniel cut his hair.','Daniel divorced his wife.','The weather is warm and sunny.')]
            first,info=embeddings(rows,root);second,_=embeddings(rows,root)
            self.assertTrue(np.array_equal(first,second))
            self.assertLess(float(first[0]@first[1]),.8)
            self.assertLess(float(first[0]@first[2]),.3)
            self.assertTrue(np.allclose(np.linalg.norm(first,axis=1),1))


if __name__=='__main__':unittest.main()
