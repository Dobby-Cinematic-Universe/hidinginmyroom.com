from copy import deepcopy
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from pipeline import cloud_diarization_batch as batch
from pipeline import cloud_transcription_client as clients
from pipeline.tests.test_cloud_transcription_client import assembly_result, rev_result, rev_job


class BatchTests(unittest.TestCase):
    def fixture(self):
        originals=[];recommendations=[]
        for i in range(63):
            recording=dict(recording_id='media_'+str(i),media=dict(path='/media/'+str(i),
                sha256=str(i).zfill(64),byte_count=100),duration_ms=600000,state='ready',reasons=[],title=str(i))
            job=batch.job_id(recording);ref=dict(path='/transcripts/'+str(i),sha256='a'*64)
            originals.append(dict(job_id=job,recording=recording,disposition='third_party',
                maximum_cost_microusd=0,import_=dict(transcript=ref)))
            originals[-1]['import']=originals[-1].pop('import_')
            recommendations.append(dict(id=str(i),original_job_id=job,recording_id=recording['recording_id'],
                media=recording['media'],duration_ms=recording['duration_ms'],transcript_ref=ref,tier='additional_conversation'))
        return dict(prior_reserved_microusd=0,recordings=originals),dict(kind='himr_analyst_diarization_recommendations',records=recommendations)

    def test_exact_selection_and_no_source_mutation(self):
        base,audit=self.fixture();base['recordings'][0]['recording'].update(state='review',reasons=['source_id_maps_to_multiple_physical_recordings'])
        before=deepcopy(base);rows=batch.select(base,audit)
        self.assertEqual(base,before);self.assertEqual(len(rows),63)
        self.assertTrue(all(row['diarization'] is True and row['language']=='auto' for row in rows))
        self.assertEqual(rows[0]['recording']['state'],'ready')

    def test_rejects_admission_failures_tampering_and_unapproved_count(self):
        for change in ['media','state','count','tier']:
            base,audit=self.fixture()
            if change=='media':audit['records'][0]['media']={'path':'/different'}
            if change=='state':base['recordings'][0]['recording'].update(state='review',reasons=['damaged'])
            if change=='count':audit['records'].pop()
            if change=='tier':audit['records'][0]['tier']='reuse_before_repurchase'
            with self.subTest(change=change),self.assertRaises(batch.BatchError):batch.select(base,audit)

    def test_budget_guard_and_long_recording_route(self):
        base,_=self.fixture();previous=dict(allocation_microusd=5_000_000,maximum_cost_microusd=3_500_000)
        self.assertEqual(batch.allocation_check(base,previous)['combined_ceiling_microusd'],25_000_000)
        base['prior_reserved_microusd']=126_000_000
        with self.assertRaises(batch.BatchError):batch.allocation_check(base,previous)
        provider,cost=batch.bound_cost(dict(duration_ms=11*3600_000))
        self.assertEqual(provider,'revai');self.assertGreater(cost,2_200_000)

    def test_default_language_unchanged_explicit_auto_has_no_english_lock(self):
        url='https://cdn.assemblyai.com/upload/test-id'
        self.assertEqual(clients.assemblyai_options(url)['language_code'],'en')
        options=clients.assemblyai_options(url,language='auto')
        self.assertNotIn('language_code',options);self.assertTrue(options['language_detection'])
        self.assertEqual(options['speech_models'],['universal-3-5-pro'])
        self.assertTrue(options['speaker_labels']);self.assertNotIn('speakers_expected',options)
        self.assertNotIn('prompt',options);self.assertNotIn('keyterms_prompt',options)
        for invalid in [None,True,[],{},'ja','universal-2']:
            with self.subTest(invalid=invalid),self.assertRaises(clients.CloudClientError):clients.assemblyai_options(url,language=invalid)

    def test_auto_accepts_japanese_but_does_not_disable_model_timing_checks(self):
        raw=assembly_result();raw['language_code']='ja'
        with self.assertRaises(clients.CloudClientError):clients.normalize_result('assemblyai',raw,expected_duration_seconds=10)
        normalized=clients.normalize_result('assemblyai',raw,expected_duration_seconds=10,language='auto')
        self.assertEqual(normalized['text'],raw['text']);self.assertNotIn('words',normalized['segments'][0])
        for key,value in [('language_code','th'),('speech_model_used','universal-2'),('speaker_labels',False)]:
            bad=deepcopy(raw);bad[key]=value
            with self.subTest(key=key),self.assertRaises(clients.CloudClientError):clients.normalize_result('assemblyai',bad,expected_duration_seconds=10,language='auto')
        with self.assertRaises(clients.CloudClientError):clients.normalize_result('revai',rev_result(),job=rev_job(),expected_duration_seconds=10,language='auto')

    def test_narrow_utterance_end_recovery_preserves_original(self):
        raw=assembly_result();raw['utterances'][0]['end']=650;before=deepcopy(raw)
        row=dict(provider='assemblyai',language='auto')
        normalized,adjusted,changes=batch.normalize(row,raw,raw,dict(duration_ms=10000))
        self.assertEqual(raw,before);self.assertEqual(normalized['segments'][0]['end_ms'],700)
        self.assertEqual(changes,[dict(utterance_index=0,old_end_ms=650,new_end_ms=700)])
        bad=deepcopy(raw);bad['utterances'][0]['words'][0]['start']=20
        with self.assertRaises(clients.CloudClientError):batch.normalize(row,bad,bad,dict(duration_ms=10000))

    def test_cjk_spacing_is_not_a_content_change(self):
        raw=assembly_result();raw['language_code']='ja';raw['text']='これは本です。'
        raw['utterances'][0]['text']='これ は 本 です。';before=deepcopy(raw)
        normalized=clients.normalize_result('assemblyai',raw,expected_duration_seconds=10,language='auto')
        self.assertTrue(normalized['cjk_spacing_only_difference']);self.assertEqual(raw,before)
        self.assertEqual(normalized['text'],raw['text'])
        self.assertEqual(normalized['segments'][0]['text'],raw['utterances'][0]['text'])
        for language,text in [('en','これは本です。'),('ja','これは紙です。')]:
            bad=deepcopy(raw);bad['language_code']=language;bad['text']=text
            with self.assertRaises(clients.CloudClientError):
                clients.normalize_result('assemblyai',bad,expected_duration_seconds=10,language='auto')

    def test_collector_release_preserves_plan_and_limits_changes(self):
        ref=dict(path='/plan.json',sha256='a'*64)
        old={'cloud_transcription_client.py':dict(path='/v1/client',sha256='b'*64),
             'cloud_transcription_media.py':dict(path='/v1/media',sha256='c'*64)}
        new={'cloud_transcription_client.py':dict(path='/v2/client',sha256='d'*64),
             'cloud_transcription_media.py':dict(path='/v2/media',sha256='c'*64)}
        release=dict(kind=batch.KIND+'_runtime_release',plan=ref,previous_implementation=old,
                     implementation=new,paid_selection_changed=False,paid_retries_authorized=False)
        batch.check_runtime_release(ref,old,new,release)
        with self.assertRaises(batch.BatchError):batch.check_runtime_release(ref,old,new,None)
        bad=deepcopy(release);bad['plan']['sha256']='f'*64
        with self.assertRaises(batch.BatchError):batch.check_runtime_release(ref,old,new,bad)
        bad=deepcopy(release);bad['implementation']['cloud_transcription_media.py']['sha256']='e'*64
        with self.assertRaises(batch.BatchError):batch.check_runtime_release(ref,old,bad['implementation'],bad)

    def test_safe_retry_refuses_paid_posts(self):
        paid=Mock();paid.__name__='submit'
        with self.assertRaises(batch.BatchError):batch.safe_call(paid)
        paid.assert_not_called()

    def test_reservation_never_reissued(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'reservations').mkdir(mode=0o700);(root/'jobs'/'job').mkdir(parents=True,mode=0o700)
            plan=dict(state_root=str(root));row=dict(job_id='job');intent=dict(maximum_cost_microusd=100)
            batch.reserve(plan,{},row,intent)
            with self.assertRaises(batch.BatchError):batch.reserve(plan,{},row,intent)
            self.assertTrue((root/'jobs'/'job'/'intent.json').exists())


if __name__=='__main__':unittest.main()
