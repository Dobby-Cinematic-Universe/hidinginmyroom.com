"""Receipt-preserving summary recovery for the September 17 livestream."""
from pathlib import Path
from pipeline import new_archive_recording as original
from pipeline import gemini_standard_tail as standard
from pipeline import transcript_summary_classification as classification

r=original.r
ROOT=original.ROOT/'summary-recovery-v1'


def main():
    r.mkdir(ROOT)
    with r.locked(ROOT):
        old=original.ROOT/'summary'
        doc=r.read(r.binding(old/'transcript.json'))
        # Canonical import identity only; original provider receipts are untouched.
        old_id=doc['job_id']
        doc['job_id']='cloudjob_'+r.digest(dict(original_job=old_id,transcript=r.binding(old/'transcript.json')))[:32]
        transcript=r.put(ROOT/'transcript.json',doc)
        completion=r.read(r.binding(old/'completion.json'))
        completion.update(job_id=doc['job_id'],transcript=transcript)
        cref=r.put(ROOT/'completion.json',completion)
        r.put(ROOT/'identity-adapter.json',dict(original=r.binding(old/'transcript.json'),adapted=transcript,original_job_id=old_id,canonical_job_id=doc['job_id'],provider_resubmitted=False))
        spec=dict(transcript=transcript,completion=cref,format='cloud',recording_id=doc['recording_id'],title='Hiding in my room is live!',date=dict(value='2026-09-17',kind='recorded',evidence=r.binding(old/'date.json')))
        source=r.sources_module.normalize_source(spec)
        r.put(ROOT/'source.json',source)
        config={**r.core.DEFAULT_CONFIG,'timeline_profile':'gemini_flash_batch','max_chunk_input_bytes':24000,'max_evidence_refs_per_item':256,'gemini_schema_policy':'local_array_bounds_v2','transcript_input_policy':'text_and_speaker_evidence_v1'}
        parents=r.core.initial_jobs([source],config)
        r.mkdir(ROOT/'requests')
        batch=r.api_client('gemini',env_file=str(Path('.env').resolve()))
        api=standard.StandardClient(batch._api_key,timeout_seconds=180)
        results={};reductions=[]
        def execute(job):
            folder=ROOT/'requests'/job['job_id'];r.mkdir(folder)
            r.put(folder/'job.json',job)
            if not (folder/'response.json').exists():
                if (folder/'intent.json').exists():raise ValueError('Uncertain request: inspect before resubmitting')
                spent=sum(r.read(r.binding(p))['maximum_cost_microusd'] for p in (ROOT/'requests').glob('*/intent.json'))
                cost=2*job['budget']['maximum_cost_microusd']
                if spent+cost>2_000_000:raise ValueError('Two-dollar summary bound exceeded')
                r.put(folder/'intent.json',dict(maximum_cost_microusd=cost,job=r.binding(folder/'job.json')))
                response=api._request('POST','/v1beta/models/'+job['model']+':generateContent',data=r.canonical(job['request']['body']),content_type='application/json',timeout_seconds=180)
                r.put(folder/'response.json',response)
            result,changes=classification.normalize(job,r.response_payload('gemini',r.read(r.binding(folder/'response.json'))))
            r.put(folder/'result.json',dict(result=result,classification_adjustments=changes))
            results[job['job_id']]=result
            print('Completed '+job['stage']+' '+job['job_id'],flush=True)
        for job in parents:execute(job)
        for _ in range(10):
            ready,final=r.core._advance('transcript',[source['source_id']],None,parents,reductions,results,config)
            if final is not None:break
            if not ready:raise ValueError('No reduction progress')
            for job in ready:execute(job);reductions.append(job)
        else:raise ValueError('Reduction depth exceeded')
        final_ref=r.put(ROOT/'final.json',results[final['job_id']])
        reader=dict(kind='himr_private_summary_reader_export',phase='transcripts',phase_complete=True,
            records=[dict(recording_id=source['recording_id'],title=source['title'],date=source['date'],sections=results[final['job_id']]['sections'])],canonical=final_ref)
        r.put(ROOT/'reader.json',reader)
        r.put(ROOT/'completed.json',dict(state='completed',reader=r.binding(ROOT/'reader.json'),transcript=transcript,source=r.binding(ROOT/'source.json')))
        print('Summary completed',flush=True)


if __name__=='__main__':main()
