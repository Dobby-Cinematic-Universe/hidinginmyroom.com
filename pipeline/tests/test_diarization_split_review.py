import unittest
from pipeline.diarization_split_review import analyze


class SplitReviewTests(unittest.TestCase):
    def test_brief_reply_flagged_not_merged(self):
        doc={'segments':[
            dict(speaker='A',start_ms=0,end_ms=2000,text='I was going to'),
            dict(speaker='B',start_ms=2100,end_ms=2500,text='yes'),
            dict(speaker='A',start_ms=2600,end_ms=5000,text='continue.') ]}
        r=analyze(doc)
        c=next(c for c in r['candidates'] if c['segment_index']==1)
        self.assertEqual(c['labels_to_compare'],['A','B'])
        self.assertFalse(c['merge_approved'])
        self.assertIsNone(c['identity'])
        self.assertEqual(doc['segments'][1]['speaker'],'B')

    def test_overlap_not_brief_nonoverlap_sequence(self):
        doc={'segments':[
            dict(speaker='A',start_ms=0,end_ms=3000,text='Hello.'),
            dict(speaker='B',start_ms=2000,end_ms=2500,text='Yes.'),
            dict(speaker='A',start_ms=2600,end_ms=5000,text='Okay.') ]}
        c=next(c for c in analyze(doc)['candidates'] if c['segment_index']==1)
        self.assertIsNone(c['labels_to_compare'])

    def test_missing_labels_not_assigned(self):
        self.assertEqual(analyze({'segments':[dict(speaker=None,start_ms=0,end_ms=10,text='Hi') ]})['candidates'],[])
