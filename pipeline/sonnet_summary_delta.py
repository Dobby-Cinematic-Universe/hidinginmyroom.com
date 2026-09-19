"""Incremental synthesis for newly admitted summaries; retain unaffected scopes."""
import argparse
from copy import deepcopy
from pathlib import Path
import os
import hashlib
import json
import shutil
import time
from pipeline import sonnet_broader_campaign as c
r=c.r
original_frontier=c.frontier


def prepare(oldroot,preview,root):
    oldref=r.binding(oldroot/'manifest.json');old,selection,leaves,_=c.load(oldref)
    prior_status=r.read(r.binding(oldroot.parent/'sonnet-broader-evidence-recovery-20260917/status.json'))
    if not prior_status['complete']:raise r.Error('Previous synthesis must be complete')
    r.mkdir(root)
    for name in ('records','jobs','batches','reader','exports'):r.mkdir(root/name)
    r.mkdir(root/'reader'/'transcripts')
    # Retain private reader transcript files without rereading the archive.
    for file in (oldroot/'reader'/'transcripts').glob('*.txt'):
        target=root/'reader'/'transcripts'/file.name
        if not target.exists():shutil.copyfile(file,target)
    summaries=r.read(r.binding(preview/'summaries/refreshed.json'))
    summary_ref=r.put(root/'summary-snapshot.json',summaries)
    mapping=r.read(r.binding(preview/'identity-map.json'))
    identities={x['public_recording_id']:x for x in mapping['records']}
    corpus=r.read(r.binding(preview/'corpus'/'manifest.json'));catalog={}
    for shard in corpus['catalog_shards']:
        catalog_root=(preview/'corpus'/'releases'/corpus['release_id']).resolve()
        shard_path=(catalog_root/shard['path']).resolve()
        if not shard_path.is_relative_to(catalog_root):raise r.Error('Unsafe catalog shard')
        data=shard_path.read_bytes()
        if hashlib.sha256(data).hexdigest()!=shard['sha256']:raise r.Error('Catalog shard changed')
        rows=json.loads(data)
        catalog.update({x['recording_id']:x for x in rows['recordings']})
    records=deepcopy(selection['records']);known={x['recording_id'] for x in records};added=[]
    for summary in summaries['summaries']:
        if summary['kind']!='transcript':continue
        identity=identities[summary['recording_id']];pid=identity['recording_id']
        if pid in known:continue
        tid='transcript_'+r.digest(pid)[:24]
        # Preserve unknown dates; never fabricate the first day of a month.
        value=catalog[summary['recording_id']].get('date_label') if summary['period'] else None
        if value and not value.startswith(summary['period']+'-'):raise r.Error('Summary and recording date differ')
        date=dict(value=value,basis='retained_catalog_date' if value else 'unknown',event_date_verified=False)
        sections={section:[dict(item_id='summaryitem_'+r.digest(dict(recording=pid,section=section,index=i,item=item))[:32],
            text=item['text'],classification=item['classification'],transcript_ids=[tid],evidence=dict(summary_export=summary_ref,summary_id=summary['id'],index=i))
            for i,item in enumerate(items)] for section,items in summary['sections'].items()}
        doc=dict(kind='himr_synthesis_transcript_summary_input',transcript_id=tid,recording_id=pid,title=summary['title'],date=date,
            sections=sections,transcript=identity['transcript'],source_export=summary_ref)
        ref=r.put(root/'records'/(tid+'.json'),doc)
        records.append(dict(transcript_id=tid,recording_id=pid,title=doc['title'],date=date,input=ref));added.append(tid)
    if not added:raise r.Error('No new summaries')
    selected={**selection,'records':records,'physical_recordings':len(records)}
    selected_ref=r.put(root/'selection.json',selected)
    specs=c.scope_specs(records,old['scope']);seeds={}
    reader=r.read(r.binding(oldroot/'reader'/'index.json'))
    for row in reader['summaries']:
        result=r.read(row['canonical'])['result'];key=result['stage']+':'+result['period']
        spec=next(x for x in specs if x['stage']+':'+x['period']==key)
        if not set(spec['transcript_ids'])&set(added):seeds[key]=row['canonical']
    body={**old,'selection':selected_ref,'specs':specs,'state_root':str(root),
        'budget_microusd':old['budget_microusd']-round(prior_status['reserved_maximum_usd']*1e6),
        'delta_implementation':r.binding(__file__),'original_campaign':oldref,'retained_scopes':seeds,
        'added_transcripts':added,'approval':'User requested all summary dependents include newly completed summaries; incremental scopes only, shared $150 cap.'}
    ref=r.put(root/'manifest.json',body)
    print(r.canonical(dict(added=len(added),retained_scopes=len(seeds),refresh_scopes=[x['stage']+':'+x['period'] for x in specs if x['stage']+':'+x['period'] not in seeds],manifest=ref)).decode(),flush=True)
    return ref


def frontier(m,leaves,metadata,s):
    ready=[];finals={};progress=[]
    for spec in m['specs']:
        key=spec['stage']+':'+spec['period']
        if key in m['retained_scopes']:
            result=r.read(m['retained_scopes'][key])['result']
            if sorted(result['transcript_ids'])!=sorted(spec['transcript_ids']):raise r.Error('Retained scope membership changed')
            finals[key]=result;progress.append(dict(scope=key,state='retained'));continue
        if not all(dep in finals for dep in spec['dependencies']):
            progress.append(dict(scope=key,state='waiting_for_dependencies'));continue
        parents=[finals[dep] for dep in spec['dependencies']] if spec['dependencies'] else [c.leaf(leaves[tid]) for tid in spec['transcript_ids']]
        for level in range(m['max_levels']):
            expected=c.pack(spec['stage'],spec['period'],level,parents,metadata)
            for job in expected:
                if job['job_id'] in s['jobs'] and s['jobs'][job['job_id']]!=job:raise r.Error('Delta job replay differs')
            if not all(j['job_id'] in s['results'] for j in expected):
                ready.extend(j for j in expected if j['job_id'] not in s['assigned']);progress.append(dict(scope=key,state='processing'));break
            if expected[0]['final']:
                finals[key]=s['results'][expected[0]['job_id']];progress.append(dict(scope=key,state='completed'));break
            parents=[s['results'][j['job_id']] for j in expected]
        else:raise r.Error('Delta hierarchy limit')
    return ready,finals,progress


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);p.add_argument('--preview',type=Path,required=True)
    p.add_argument('--original',type=Path,required=True);p.add_argument('--env-file',required=True);p.add_argument('--allow-paid-api',action='store_true')
    a=p.parse_args();root=a.root.resolve()
    ref=r.binding(root/'manifest.json') if (root/'manifest.json').exists() else prepare(a.original.resolve(),a.preview.resolve(),root)
    m,selection,leaves,metadata=c.load(ref)
    if m['delta_implementation']!=r.binding(__file__):raise r.Error('Delta implementation changed')
    c.frontier=frontier
    api=r.api_client('anthropic',env_file=a.env_file)
    deadline=time.monotonic()+86400
    while time.monotonic()<deadline:
        status=c.cycle(ref,m,selection,leaves,metadata,api,allow_paid=a.allow_paid_api)
        print(r.canonical({k:v for k,v in status.items() if k not in ('scope_progress','source_exclusions')}).decode(),flush=True)
        if status['complete'] or not a.allow_paid_api or not status['active_batches']:break
        time.sleep(30)


if __name__=='__main__':main()
