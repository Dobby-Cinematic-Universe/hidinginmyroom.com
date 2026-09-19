from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from pipeline import summary_recovery_integration as integration
from pipeline import transcript_summary_reader as reader
from pipeline.tests.test_transcript_summary_core import source, complete
from pipeline.tests.test_transcript_summary import Service

r = integration.r


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.source = source(['Original statement with linked evidence.'])
        self.config = {**r.core.DEFAULT_CONFIG,'transcript_input_policy':'text_and_speaker_evidence_v1'}
        self.job = r.core.initial_jobs([self.source],self.config)[0]
        self.snapshot = dict(initial=[self.job],jobs=[self.job],results=[])

    def replacement(self, claude=False):
        edited = integration.pilot.sanitize(self.job,[dict(first=1,last=1,
            text='A neutral editorial description.',classification='reported_statement')])
        if claude:
            config = {**self.config,'transcript_profile':'anthropic_sonnet_batch'}
            config.pop('transcript_input_policy')
            with integration.claude.projection():
                edited = r.core.make_job('chunk',self.job['scope'],edited['evidence'],[],config)
                result = complete(edited)
        else:
            result = complete(edited)
        return {self.job['job_id']:dict(original_job=self.job,job=edited,result=result,
            editorial_copy=True,classification_adjustments=[])}

    def test_mixed_provider_parent_keeps_identity_and_final_depends_on_it(self):
        recovery = self.replacement(claude=True)
        before = deepcopy(self.snapshot)
        parents,reducers,results,applied = integration.replace_inputs(self.snapshot,recovery)
        self.assertEqual(self.snapshot,before)
        self.assertEqual(parents[0]['provider'],'anthropic')
        self.assertEqual(results[parents[0]['job_id']]['model'],'claude-sonnet-5')
        ready,final = r.core._advance('transcript',[self.source['source_id']],None,
            parents,reducers,results,self.config)
        self.assertIsNone(final)
        self.assertEqual(ready[0]['dependencies'],[parents[0]['job_id']])
        self.assertTrue(ready[0]['scope']['final'])
        self.assertEqual(set(ready[0]['prompt']['input']),{'stage','evidence'})
        self.assertEqual(applied,[self.job['job_id']])

    def test_successful_original_cannot_be_overwritten(self):
        self.snapshot['results']=[complete(self.job)]
        with self.assertRaisesRegex(RuntimeError,'successful'):
            integration.replace_inputs(self.snapshot,self.replacement())

    def test_missing_chunk_prevents_reduction(self):
        with self.assertRaisesRegex(RuntimeError,'unrecovered chunk'):
            integration.replace_inputs(self.snapshot,{})

    def test_foreign_scope_rejected(self):
        mapping = self.replacement()
        mapping[self.job['job_id']]['job']['scope']['index']+=1
        with self.assertRaisesRegex(RuntimeError,'scope'):
            integration.replace_inputs(self.snapshot,mapping)

    def test_retained_success_needs_no_replacement(self):
        result = complete(self.job)
        self.snapshot['results']=[result]
        parents,_,results,applied = integration.replace_inputs(self.snapshot,{})
        self.assertEqual(parents,[self.job])
        self.assertEqual(results,{self.job['job_id']:result})
        self.assertEqual(applied,[])

    def test_final_replacement_is_reused_without_new_reducer(self):
        chunk = complete(self.job)
        ready,_ = r.core._advance('transcript',[self.source['source_id']],None,
            [self.job],[],{self.job['job_id']:chunk},self.config)
        job = ready[0]
        result = complete(job)
        self.snapshot['jobs'].append(job)
        self.snapshot['results']=[chunk]
        parents,reducers,results,_ = integration.replace_inputs(self.snapshot,
            {job['job_id']:dict(original_job=job,job=job,result=result)})
        ready,final = r.core._advance('transcript',[self.source['source_id']],None,
            parents,reducers,results,self.config)
        self.assertEqual(ready,[])
        self.assertEqual(final,job)

    def test_reader_prefers_complete_explicit_recovery(self):
        expected=dict(state='exported_private_reader',phase_complete=True)
        with (patch.object(integration,'preferred_reader',return_value=expected) as preferred,
                patch.object(reader.runner,'load_plan',side_effect=AssertionError('must not reopen old failure'))):
            self.assertEqual(reader.export_reader('/private/plan.json','a'*64),expected)
        preferred.assert_called_once()

    def test_paid_reduction_collects_and_publishes_once(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            folder=root/'record'
            folder.mkdir(mode=0o700)
            (folder/'waves').mkdir(mode=0o700)
            (folder/'exports').mkdir(mode=0o700)
            (root/'scheduler-efficiency-v1').mkdir(mode=0o700)
            r.put(root/'scheduler-efficiency-v1/status.json',dict(state='waiting_remote',
                transcript_summaries_complete=4,holds=[dict(recording_id=self.source['recording_id'])]))
            doc=dict(source=self.source,parents=[self.job],retained_reducers=[],
                results={self.job['job_id']:complete(self.job)},config=self.config,applied=[],substitutions={},
                original_plan=dict(path='/original/plan.json',sha256='a'*64),
                original_snapshot=dict(path='/snapshot.json',sha256='b'*64))
            ref=r.put(folder/'record.json',doc)
            manifest=dict(state_root=str(root),base=str(root),max_new_waves_per_record=12)
            api=Mock()
            api.batch_bytes.side_effect=r.client_module.gemini_batch_bytes
            services=[]
            def create(model,requests,name):
                wave=r.read(r.binding(folder/'waves'/name/'wave.json'))
                service=Service(wave,'unused')
                services.append(service)
                return service.remote()
            api.create_batch.side_effect=create
            def get(name):
                service=services[0]
                self.assertEqual(name,service.name)
                service.state='completed'
                return service.remote()
            api.get_batch.side_effect=get
            with (patch.object(integration,'load',return_value=manifest),
                    patch.object(r,'api_client',return_value=api)):
                first=integration.cycle({},[(ref,doc)],env_file=None,allow_paid_api=True)
                self.assertEqual(first['completed'],0)
                second=integration.cycle({},[(ref,doc)],env_file=None,allow_paid_api=True)
                self.assertEqual(second['completed'],1)
                third=integration.cycle({},[(ref,doc)],env_file=None,allow_paid_api=True)
                self.assertEqual(third['completed'],1)
            self.assertEqual(api.create_batch.call_count,1)
            overall=r.read(r.binding(root/'overall-status.json'))
            self.assertEqual(overall['preferred_transcript_summaries_complete'],5)
            self.assertTrue((folder/'exports/reader.json').exists())


if __name__=='__main__':
    unittest.main()
