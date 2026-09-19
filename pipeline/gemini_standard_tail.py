"""Explicit standard-API continuation of one stalled batch, no automatic retries."""
import argparse
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import re
from pipeline import summary_recovery_integration as integration
from pipeline import cloud_transcription_summary as worker
from pipeline import gemini_dashboard_spend as spend, gemini_targeted_retry as retry
from pipeline import gemini_recovery_extension as recovery, reviewed_summary_adapter as reviewed
from pipeline import short_summary_admission_runner as short

r=integration.r


class StandardClient(r.client_module.GeminiBatchClient):
    def _destination(self, method, path):
        if method=='POST' and re.fullmatch(r'/v1beta/models/gemini-[a-z0-9.-]+:generateContent',path):
            return 'https://generativelanguage.googleapis.com'+path
        return super()._destination(method,path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base',type=Path,required=True);p.add_argument('--record',required=True)
    p.add_argument('--wave',required=True);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--env-file',required=True);p.add_argument('--allow-paid-api',action='store_true')
    a=p.parse_args()
    if not a.allow_paid_api: p.error('explicit paid API authorization required')
    if not re.fullmatch(r'summaryrecord_[a-f0-9]{32}',a.record) or not re.fullmatch(r'summarywave_[a-f0-9]{32}',a.wave):
        raise r.Error('Invalid bounded target')
    base=a.base.resolve();output=a.root.resolve();folder=base/'records'/a.record
    wref=r.binding(folder/'waves'/a.wave/'wave.json');wave=r.read(wref)
    pref=r.binding(folder/'plan.json');worker_ref=r.binding(base/'manifest.json')
    release_ref=r.binding(base.parent/'conservative-diarization-v1/execution-release/release.json')
    spend.install(worker,worker_ref);short.install(worker);recovery.install(worker)
    reviewed.install(worker,base.parent/'reviewed-transcript-feed-v1/index.json')
    r.mkdir(output)
    with r.locked(output),ExitStack() as stack:
        stack.enter_context(worker.release.activate(release_ref))
        stack.enter_context(worker.normalization_scope(worker_ref))
        stack.enter_context(worker.job_cache.scope(worker_ref))
        retry.install(worker,r.binding(base/'tier2-recovery-20260916/authority-v2.json'))
        snapshot_path=output/'snapshot.json'
        if snapshot_path.exists(): snapshot=r.read(r.binding(snapshot_path))
        else:
            with r.locked(folder):
                plan,sources=r.load_plan(pref['path'],pref['sha256']);state=r.load_state(plan,sources)
                initial=state.get('initial_jobs_override') or r.core.initial_jobs(sources,plan['request_value']['config'])
                r.core.source_coverage(sources,initial)
                snapshot=dict(plan=plan,sources=sources,initial=initial,jobs=state['jobs'],results=state['results'])
                r.put(snapshot_path,snapshot)
        if len(snapshot['sources'])!=1 or any(j['stage']!='chunk' for j in wave['jobs']):raise r.Error('Expected one recording with stalled chunks')
        results={x['job_id']:x for x in snapshot['results']};parents=snapshot['initial']
        reductions=[j for j in snapshot['jobs'] if j['stage']=='transcript']
        missing={j['job_id'] for j in parents if j['job_id'] not in results}
        if missing-{j['job_id'] for j in wave['jobs']}:raise r.Error('Other chunks are missing; do not broaden fallback')
        r.put(output/'authority.json',dict(original_plan=pref,original_wave=wref,implementation=r.binding(__file__),
            approval='User authorized standard API for stalled final Gemini wave and missing reductions, accepting duplicate-charge risk.',
            standard_input_multiplier=2,maximum_new_requests=12,original_artifacts_unchanged=True))
        batch_api=r.api_client('gemini',env_file=a.env_file)
        api=StandardClient(batch_api._api_key,timeout_seconds=180)
        def execute(job):
            r.core.validate_job(job)
            target=output/'requests'/job['job_id'];r.mkdir(target)
            r.put(target/'job.json',job)
            response_path=target/'response.json'
            if not response_path.exists():
                if (target/'intent.json').exists():raise r.Error('Standard request uncertain; no automatic resubmission')
                r.put(target/'intent.json',dict(job=r.binding(target/'job.json'),api='standard_generateContent',
                    maximum_cost_microusd=2*job['budget']['maximum_cost_microusd']))
                response=api._request('POST','/v1beta/models/'+job['model']+':generateContent',
                    data=r.canonical(job['request']['body']),content_type='application/json',timeout_seconds=180)
                r.put(response_path,response)
            response=r.read(r.binding(response_path))
            value,changes=integration.classification.normalize(job,r.response_payload('gemini',response))
            r.put(target/'result.json',dict(result=value,classification_adjustments=changes,response=r.binding(response_path)))
            results[job['job_id']]=value
            print(r.canonical(dict(state='completed',job_id=job['job_id'],stage=job['stage'])).decode(),flush=True)
        for job in wave['jobs']:
            if job['job_id'] in missing:execute(job)
        config=snapshot['plan']['request_value']['config'];source=snapshot['sources'][0]
        for level in range(10):
            ready,final=r.core._advance('transcript',[source['source_id']],None,parents,reductions,results,config)
            if final is not None:break
            if not ready:raise r.Error('No reduction progress')
            for job in ready:
                if len(list((output/'requests').glob('*/intent.json')))>=12 and not (output/'requests'/job['job_id']/'intent.json').exists():raise r.Error('Request bound reached')
                execute(job);reductions.append(job)
        else:raise r.Error('Reduction bound reached')
        doc=dict(original_plan=pref,source=source,parents=parents,applied=sorted(missing),substitutions={},original_snapshot=r.binding(snapshot_path))
        r.put(output/'record.json',doc)
        export=integration.publish(doc,output,results[final['job_id']],r.binding(output/'authority.json'))
        r.put(output/'completed.json',dict(state='completed',export=export,retained_results=len(snapshot['results']),
            standard_requests=len(list((output/'requests').glob('*/intent.json'))),original_batch_preserved=True))
        print(r.canonical(dict(state='completed',title=source['title'],export=export['reader'])).decode(),flush=True)


if __name__=='__main__':main()
