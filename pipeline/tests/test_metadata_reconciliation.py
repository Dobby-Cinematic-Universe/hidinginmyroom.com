import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from pipeline import metadata_reconciliation as m

class ReconciliationTests(unittest.TestCase):
    def setup_match(self,root):
        text=b'1\n00:00:01,000 --> 00:00:03,000\nUnchanged words.\n'
        source=Path(root)/'source.txt';source.write_bytes(text)
        record=dict(recording_id='one',duration_ms=600000)
        match=dict(status='ambiguous',matching_basis=['exact_youtube_id'],matched_keys=['youtube:abcdefghijk'],
            issues=['identity_key_matches_multiple_recordings','possible_missing_tail'],related_physical_recordings=['one','two'],
            candidates=[dict(path=str(source),sha256=hashlib.sha256(text).hexdigest())])
        return record,match,{'one':record,'two':dict(duration_ms=600010)}

    def test_exact_variant_mapping_accepts_marked_partial_without_editing_words(self):
        with tempfile.TemporaryDirectory() as root:
            args=self.setup_match(root);doc=m.third_party_copy(*args)
            self.assertTrue(doc['reconciliation']['partial'])
            self.assertFalse(doc['reconciliation']['media_alignment_verified'])
            self.assertEqual(doc['segments'][0]['text'],'Unchanged words.')
            self.assertEqual(doc['segments'][0]['start_ms'],1000)

    def test_different_duration_variants_are_not_blindly_mapped(self):
        with tempfile.TemporaryDirectory() as root:
            args=self.setup_match(root);args[2]['two']['duration_ms']=900000
            with self.assertRaisesRegex(ValueError,'timeline alignment'):m.third_party_copy(*args)

    def test_no_title_only_or_modified_source_admission(self):
        with tempfile.TemporaryDirectory() as root:
            args=self.setup_match(root);args[1]['matching_basis']=['fuzzy_title']
            with self.assertRaisesRegex(ValueError,'not exact'):m.third_party_copy(*args)
            args=self.setup_match(root);args[1]['candidates'][0]['sha256']='0'*64
            with self.assertRaisesRegex(ValueError,'changed'):m.third_party_copy(*args)

    def test_overrun_and_empty_sources_remain_held(self):
        with tempfile.TemporaryDirectory() as root:
            args=self.setup_match(root);args[1]['issues']=['timestamps_exceed_media_duration']
            with self.assertRaises(ValueError):m.third_party_copy(*args)
            args[1]['issues']=['empty_transcript']
            with self.assertRaises(ValueError):m.third_party_copy(*args)

    def test_source_duration_selects_only_closest_compatible_variant(self):
        with tempfile.TemporaryDirectory() as root:
            args=self.setup_match(root)
            args[0]['duration_ms']=3000;args[2]['two']['duration_ms']=600000
            args[1]['candidates'][0]['last_end_ms']=3000
            result=m.third_party_copy(*args)
            self.assertTrue(result['reconciliation']['duration_disambiguated'])
            args[0]['duration_ms']=600000;args[2]['two']['duration_ms']=3000
            with self.assertRaisesRegex(ValueError,'timeline alignment'):m.third_party_copy(*args)

    def test_cloud_recovery_preserves_turns_but_rejects_text_or_time_mismatch(self):
        raw=dict(status='completed',text='Hello there.',utterances=[
            dict(start=100,end=900,speaker='A',text='Hello there.',words=[dict(start=-1)])])
        documents={'terminal-job.json':raw,'audio.json':{'audio':{'duration_ms':1000}}}
        row=dict(job_id='test',recording=dict(recording_id='one',media={}))
        with patch.object(m.io,'binding',side_effect=lambda p:p.name), patch.object(m.io,'read',side_effect=lambda ref:documents[ref]):
            doc=m.cloud_copy(row,Path('/unused'))
            self.assertEqual(doc['segments'],[dict(start_ms=100,end_ms=900,text='Hello there.',speaker='SPEAKER_0000')])
            raw['text']='Different words.'
            with self.assertRaisesRegex(ValueError,'text differs'):m.cloud_copy(row,Path('/unused'))
            raw['text']='Hello there.';raw['utterances'][0]['end']=100
            with self.assertRaisesRegex(ValueError,'timestamp'):m.cloud_copy(row,Path('/unused'))

if __name__=='__main__':unittest.main()
