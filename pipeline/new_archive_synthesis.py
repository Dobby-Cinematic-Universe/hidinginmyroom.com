"""Incrementally refresh three broader scopes with receipt-safe standard Claude."""
from pathlib import Path
from pipeline import sonnet_summary_delta as delta
from pipeline import sonnet_broader_strict_recovery as strict
from pipeline import sonnet_broader_recovery as pricing

c=delta.c
r=c.r
ROOT=Path('research/private-summaries/new-archive-synthesis-20260918').resolve()
PREVIEW=Path('research/corpus/site-previews/release-20260918-v10').resolve()


class StandardClient(r.anthropic_module.AnthropicBatchClient):
    def _destination(self,method,path):
        if method=='POST' and path=='/v1/messages':return r.anthropic_module.ANTHROPIC_API_ORIGIN+path
        return super()._destination(method,path)


def main():
    ref=r.binding(ROOT/'manifest.json') if (ROOT/'manifest.json').exists() else delta.prepare(
        Path('research/private-summaries/sonnet-summary-delta-20260917-v3').resolve(),PREVIEW,ROOT)
    m,selection,leaves,metadata=c.load(ref)
    if m['delta_implementation']!=r.binding(delta.__file__):raise ValueError('Delta implementation changed')
    api=StandardClient(r.api_client('anthropic',env_file=str(Path('.env').resolve()))._api_key,timeout_seconds=300)
    with r.locked(ROOT):
        jobs={};results={};reserved=0
        for p in (ROOT/'jobs').glob('*/job.json'):
            job=r.read(r.binding(p));jobs[job['job_id']]=job
            if (p.parent/'intent.json').exists():reserved+=r.read(r.binding(p.parent/'intent.json'))['maximum_cost_microusd']
            if (p.parent/'response.json').exists():results[job['job_id']]=c.normalize(job,r.read(r.binding(p.parent/'response.json')))
        for _ in range(30):
            ready,finals,progress=delta.frontier(m,leaves,metadata,dict(jobs=jobs,results=results,assigned=set(results)))
            c.publish(ROOT,finals,leaves,ref)
            state=dict(complete=len(finals)==len(m['specs']),scopes=len(m['specs']),completed_scopes=len(finals),
                selected_recordings=len(leaves),reserved_maximum_usd=reserved/1e6,scope_progress=progress,standard_api=True)
            c.files.atomic(ROOT/'status.json',state)
            print(r.canonical(state).decode(),flush=True)
            if state['complete']:return
            if not ready:raise ValueError('No ready synthesis jobs')
            for job in ready:
                folder=ROOT/'jobs'/job['job_id'];r.mkdir(folder)
                r.put(folder/'job.json',job);jobs[job['job_id']]=job
                params=strict.strict_params(job)
                cost=2*pricing.retry_cost(params)
                if (folder/'intent.json').exists():raise ValueError('Uncertain Claude request; inspect before retry')
                if reserved+cost>min(m['budget_microusd'],20_000_000):raise ValueError('Incremental synthesis budget reached')
                r.put(folder/'intent.json',dict(params=params,maximum_cost_microusd=cost,original_job=r.binding(folder/'job.json')))
                reserved+=cost
                response=api._request('POST','/v1/messages',data=r.canonical(params),content_type='application/json',timeout_seconds=300)
                r.put(folder/'response.json',response)
                result=c.normalize(job,response)
                r.put(folder/'result.json',result);results[job['job_id']]=result
                print('Completed '+job['stage']+' '+job['period'],flush=True)
        raise ValueError('Synthesis depth exceeded')


if __name__=='__main__':main()
