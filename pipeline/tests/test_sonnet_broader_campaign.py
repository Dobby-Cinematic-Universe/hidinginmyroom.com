import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from pipeline import sonnet_broader_campaign as c
from pipeline import sonnet_broader_selection as selection
from pipeline.tests.test_transcript_summary_anthropic import batch


def fixture():
    sections={s:[] for s in c.SECTIONS}
    sections['summary']=[dict(item_id='original-item',text='A move was discussed.',
        classification='reported_statement',transcript_ids=['t1'])]
    parents=[dict(parent_id='t1',transcript_ids=['t1'],sections=sections)]
    metadata={'t1':dict(title='A recording',date=dict(value='2020-01-02',basis='metadata'))}
    return parents,metadata


def response(evidence=None,classification='reported_statement'):
    payload={s:[] for s in c.SECTIONS}
    payload['summary']=[dict(text='A move was discussed.',classification=classification,
        evidence_ids=['e1'] if evidence is None else evidence)]
    return dict(type='message',role='assistant',model=c.MODEL,stop_reason='end_turn',
        content=[dict(type='text',text=json.dumps(payload))])


class SynthesisTests(unittest.TestCase):
    def setUp(self):
        c._PACK_CACHE.clear()
        self.parents,self.metadata=fixture()
        self.job=c.make_job('monthly','2020-01',0,0,self.parents,self.metadata,True)

    def test_scope_and_undated_separation(self):
        records=[dict(transcript_id='a',date={'value':'2020-01-01'}),
                 dict(transcript_id='b',date={'value':None})]
        scopes=c.scope_specs(records,'monthly-yearly-archive')
        self.assertEqual(len(scopes),4)
        year=next(s for s in scopes if s['stage']=='yearly')
        self.assertEqual(year['transcript_ids'],['a'])
        self.assertEqual(scopes[-1]['dependencies'],['yearly:2020','monthly:unknown'])

    def test_date_conflicts_are_not_guessed(self):
        result=selection.date_for({'date_metadata':{'value':'2020-01-01','basis':'title'}},
            {'date':{'value':'2020-02-01','basis':'archive'}})
        self.assertIsNone(result['value'])
        self.assertEqual(selection.physical('file:reviewed:123'),'file')

    def test_normalization_and_evidence_chain(self):
        result=c.normalize(self.job,response())
        item=result['sections']['summary'][0]
        self.assertEqual(item['evidence_ids'],['original-item'])
        self.assertEqual(item['transcript_ids'],['t1'])

    def test_invalid_evidence_and_refusal_rejected(self):
        for value in (response(['foreign']),response(['e1','e1']),
                      {**response(),'stop_reason':'refusal'}, {**response(),'model':'different'}):
            with self.assertRaises(c.r.Error):c.normalize(self.job,value)

    def test_uncertainty_inherited_without_rewriting_text(self):
        self.job['evidence'][0]['classification']='uncertainty'
        result=c.normalize(self.job,response())
        self.assertEqual(result['sections']['summary'][0]['classification'],'uncertainty')
        self.assertEqual(result['sections']['summary'][0]['text'],'A move was discussed.')

    def test_upper_levels_keep_local_links_without_expanding_source_table(self):
        result=c.normalize(self.job,response())
        job=c.make_job('yearly','2020',0,0,[result],self.metadata,True)
        wire=json.loads(job['params']['messages'][0]['content'])
        self.assertEqual(wire['sources'],[])
        self.assertNotIn('t1',json.dumps(wire))
        self.assertEqual(job['evidence'][0]['transcript_ids'],['t1'])
        c.r.anthropic_module.anthropic_batch_bytes([dict(custom_id=job['job_id'],params=job['params'])])

    def test_pack_rejects_oversize_without_truncation(self):
        with mock.patch.object(c,'MAX_INPUT',1):
            with self.assertRaises(c.r.Error):c.pack('monthly','2020-01',0,self.parents,self.metadata)

    def test_dispatch_is_idempotent_and_budgeted(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); folder=root/'batches'/'batch';folder.mkdir(parents=True,mode=0o700)
            b=dict(batch_id='batch',job_ids=[self.job['job_id']]); jobs={self.job['job_id']:self.job}
            api=mock.Mock();api.create_batch.return_value=batch()
            self.assertEqual(c.dispatch(root,b,jobs,api,0),'budget_hold')
            api.create_batch.assert_not_called()
            self.assertEqual(c.dispatch(root,b,jobs,api,1_000_000),'submitted')
            self.assertEqual(c.dispatch(root,b,jobs,api,1_000_000),'already_submitted')
            api.create_batch.assert_called_once()

    def test_ambiguous_post_is_never_repeated(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'batches'/'batch').mkdir(parents=True,mode=0o700)
            b=dict(batch_id='batch',job_ids=[self.job['job_id']]);jobs={self.job['job_id']:self.job}
            api=mock.Mock();api.create_batch.side_effect=RuntimeError('connection lost')
            with self.assertRaises(RuntimeError):c.dispatch(root,b,jobs,api,1_000_000)
            self.assertEqual(c.dispatch(root,b,jobs,api,1_000_000),'needs_reconciliation')
            api.create_batch.assert_called_once()

    def test_failed_job_does_not_hide_successful_result(self):
        b=dict(job_ids=[self.job['job_id'],'failed'])
        rows=c.outcomes(b,{self.job['job_id']:self.job,'failed':self.job},
            [dict(custom_id=self.job['job_id'],response=response(),error=None)])
        self.assertEqual([x['state'] for x in rows],['completed','needs_review'])

    def test_publish_links_to_timestamp_free_transcript(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for d in ('reader','reader/transcripts','exports'):(root/d).mkdir(parents=True,mode=0o700)
            ref=c.r.put(root/'transcript.json',dict(segments=[dict(text='Original speech.',start=12)]))
            leaves={'t1':dict(title='A recording',date={'value':None},transcript=ref)}
            c.publish(root,{'monthly:2020-01':c.normalize(self.job,response())},leaves,{'path':'manifest','sha256':'test'})
            reader=json.loads((root/'reader/index.json').read_text())
            link=reader['summaries'][0]['sections']['summary'][0]['sources'][0]['href']
            self.assertEqual((root/'reader'/link).read_text(),'Original speech.')

    def test_captured_batch_replay_unlocks_yearly_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name in ('jobs','batches'):c.r.mkdir(root/name)
            key=self.job['job_id'];c.r.mkdir(root/'jobs'/key)
            c.r.put(root/'jobs'/key/'job.json',self.job)
            body=dict(manifest={'path':'manifest','sha256':'test'},job_ids=[key])
            b=dict(batch_id='sonnetbatch_'+c.r.digest(body)[:32],**body)
            folder=root/'batches'/b['batch_id'];c.r.mkdir(folder)
            c.r.put(folder/'batch.json',b)
            api=mock.Mock();api.create_batch.return_value=batch()
            c.dispatch(root,b,{key:self.job},api,1_000_000)
            api.get_batch.return_value=batch(ended=True)
            with mock.patch.object(c.r,'remote_items',return_value=[dict(custom_id=key,response=response(),error=None)]):
                c.collect(root,b,{key:self.job},api)
            current=c.state(root)
            self.assertEqual(set(current['results']),{key})
            self.assertEqual(current['pending'],[])
            records=[dict(transcript_id='t1',date={'value':'2020-01-02'})]
            manifest=dict(specs=c.scope_specs(records,'monthly-yearly-archive'),max_levels=12)
            leaves={'t1':dict(transcript_id='t1',sections=self.parents[0]['sections'])}
            ready,finals,_=c.frontier(manifest,leaves,self.metadata,current)
            self.assertEqual(set(finals),{'monthly:2020-01'})
            self.assertEqual([j['stage'] for j in ready],['yearly'])


if __name__=='__main__':unittest.main()
