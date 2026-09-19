import unittest
from unittest.mock import patch
from pipeline import targeted_retranscription as t


class TargetedTests(unittest.TestCase):
    def test_duplicate_requires_source_identity_and_compatible_duration(self):
        a=dict(recording_id='a',duration_ms=100000,source_ids={'youtube':['abcdefghijk']})
        b=dict(a,recording_id='b',duration_ms=100001)
        self.assertTrue(t.related(a,b))
        self.assertFalse(t.related(a,dict(b,duration_ms=300000)))
        self.assertFalse(t.related(a,dict(b,source_ids={'youtube':['other']})))

    def test_long_probes_only_cover_missing_interval(self):
        ranges=t.probe_ranges(7200000,6000000)
        self.assertEqual(len(ranges),8)
        self.assertTrue(all(start>=6000000 and start+length<=7200000 for start,length in ranges))
        self.assertEqual(t.probe_ranges(30000,0),[(0,30000)])
        with self.assertRaises(ValueError):t.probe_ranges(100,101)

    def test_paid_post_never_enters_retry_wrapper(self):
        def submit():self.fail('must not call paid method')
        with self.assertRaisesRegex(ValueError,'cannot be retried'):t.safe_call(submit)

    def test_one_speaker_normalized_without_word_timestamps(self):
        normalized=dict(provider_speaker_labels={'SPEAKER_0000':'A'},segments=[
            dict(start_ms=1,end_ms=100,text='Hello',speaker='SPEAKER_0000')])
        row=dict(provider='assemblyai',diarization=True,language='auto')
        with patch.object(t.client,'normalize_result',return_value=normalized):
            doc,count=t.normalize(row,{}, {},{'duration_ms':1000})
        self.assertEqual(count,1)
        self.assertIsNone(doc['segments'][0]['speaker'])
        self.assertEqual(doc['single_speaker_normalization']['original_provider_speaker_labels'],{'SPEAKER_0000':'A'})

    def test_invalid_turn_is_held_not_fabricated(self):
        row=dict(provider='assemblyai',diarization=False,language='auto')
        with patch.object(t.client,'normalize_result',return_value=dict(segments=[dict(start_ms=1,end_ms=1)])):
            with self.assertRaisesRegex(ValueError,'local recovery'):t.normalize(row,{}, {},{'duration_ms':1000})


if __name__=='__main__':unittest.main()
