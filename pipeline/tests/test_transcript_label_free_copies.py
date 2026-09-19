import copy
import unittest
from pipeline.transcript_label_free_copies import project


class LabelFreeCopyTests(unittest.TestCase):
    def fixture(self):
        return dict(kind='himr_cloud_recording_transcript', text='Hello. Yes.',
            diarization_requested=True, provider_speaker_labels={'SPEAKER_0000':'A','SPEAKER_0001':'B'},
            segments=[dict(start_ms=0,end_ms=1000,text='Hello.',speaker='SPEAKER_0000'),
                      dict(start_ms=900,end_ms=1300,text='Yes.',speaker='SPEAKER_0001')])

    def test_preserves_original_text_and_overlapping_times(self):
        doc=self.fixture(); before=copy.deepcopy(doc)
        result=project(doc, {'path':'/original','sha256':'a'*64})
        self.assertEqual(doc,before)
        self.assertEqual(result['text'],doc['text'])
        for old,new in zip(doc['segments'],result['segments']):
            self.assertEqual(new,{**old,'speaker':None})
        self.assertEqual(result['provider_speaker_labels'],{})
        self.assertTrue(result['diarization_requested'])
        self.assertFalse(result['projection']['absence_of_labels_proves_single_speaker'])

    def test_unlabeled_remains_unlabeled(self):
        doc=self.fixture()
        for s in doc['segments']:s['speaker']=None
        result=project(doc, {})
        self.assertEqual(result['projection']['original_label_count'],0)
        self.assertEqual(result['segments'],doc['segments'])
