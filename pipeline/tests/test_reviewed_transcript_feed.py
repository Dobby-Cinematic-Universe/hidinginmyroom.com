from copy import deepcopy
import unittest
from pipeline.reviewed_transcript_feed import project


class ProjectionTests(unittest.TestCase):
    def setUp(self):
        self.ref = dict(path='/test/transcript.json', sha256='a' * 64)
        self.doc = dict(job_id='job1', text='original', segments=[
            dict(start_ms=i*1000, end_ms=(i+1)*1000, text='word ' * 25, speaker=label)
            for i, label in enumerate(['A', 'B', 'C'])])

    def decision(self, label, name='', source='participant', scope='label', index=0):
        return dict(transcript=self.ref, decision=dict(job_id='job1', label=label,
            name=name, source=source, scope=scope, segment_index=index))

    def test_merge_manual_names_keep_text_and_time_exclude_playback_only_from_model(self):
        original = deepcopy(self.doc)
        reviews=[self.decision('A','Daniel'),self.decision('B','daniel'),self.decision('C',source='playback')]
        projected, model, evidence = project(self.doc,self.ref,reviews)
        self.assertEqual(projected['projection']['named_participants'],['Daniel'])
        self.assertTrue(projected['projection']['summary_eligible'])
        self.assertEqual(len(model['segments']),2)
        self.assertTrue(all(set(s)=={'speaker','text','evidence_id'} for s in model['segments']))
        self.assertEqual(self.doc,original)
        self.assertEqual([(s['text'],s['start_ms'],s['end_ms']) for s in projected['segments']],
                         [(s['text'],s['start_ms'],s['end_ms']) for s in original['segments']])
        self.assertEqual(len(evidence),3)

    def test_uncertain_is_reviewed_but_never_assigned_to_daniel(self):
        reviews=[self.decision('A','Daniel'),self.decision('B',source='uncertain'),self.decision('C','Daniel / Game Audio')]
        projected, model, _ = project(self.doc,self.ref,reviews)
        self.assertTrue(projected['projection']['review_complete'])
        self.assertEqual([s['speaker'] for s in model['segments']],['Daniel','Uncertain speaker','Uncertain speaker'])
        self.assertTrue(projected['projection']['summary_eligible'])

    def test_tentative_names_and_unnamed_participants_not_promoted(self):
        reviews=[self.decision('A','Cammie?'),self.decision('B'),self.decision('C',source='tts')]
        projected, model, _ = project(self.doc,self.ref,reviews)
        self.assertTrue(projected['projection']['review_complete'])
        self.assertEqual(projected['projection']['named_participants'],[])
        self.assertEqual([s['speaker'] for s in model['segments']],['Uncertain speaker','Unidentified participant'])

    def test_unreviewed_stays_held_and_short_rule_reapplied_after_exclusion(self):
        projected, _, _=project(self.doc,self.ref,[])
        self.assertFalse(projected['projection']['summary_eligible'])
        reviews=[self.decision('A','Daniel'),self.decision('B',source='tts'),self.decision('C',source='playback')]
        projected, _, _=project(self.doc,self.ref,reviews)
        self.assertTrue(projected['projection']['review_complete'])
        self.assertEqual(projected['projection']['exclusion_reason'],'under_50_participant_words')

    def test_segment_uncertainty_overrides_whole_label(self):
        reviews=[self.decision('A',source='uncertain',scope='segment'),self.decision('A','Daniel'),
                 self.decision('B','Mila'),self.decision('C','Mila')]
        projected, model, _=project(self.doc,self.ref,reviews)
        self.assertIsNone(projected['segments'][0]['speaker_name'])
        self.assertEqual(model['segments'][0]['speaker'],'Uncertain speaker')

    def test_confirmation_for_another_transcript_cannot_name_this_revision(self):
        confirmed=dict(transcript=dict(path='/other',sha256='b'*64),
                       confirmed_mappings=[dict(label='A',name='Daniel')])
        projected, model, _=project(self.doc,self.ref,[],confirmed)
        self.assertFalse(projected['projection']['review_complete'])
        self.assertEqual(model['segments'][0]['speaker'],'Uncertain speaker')
