"""Explicit mixed-provider recovery continuations and preferred final exports.

Original plans, failed collections, unknown intents and successful chunks remain
immutable. Only missing transcript reductions are purchased. Reader publication
is separate from historical worker accounting and retains original source links.
"""
import argparse
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import time
from pipeline import gemini_non_graphic_pilot as pilot
from pipeline import claude_non_graphic_recovery as claude
from pipeline import gemini_unknown_replacement as unknown
from pipeline import transcript_summary_classification as classification
from pipeline import reviewed_transcript_feed as feed

r = pilot.r
KIND = 'himr_summary_recovery_integration'


def checked_collection(wave, ref, *, anthropic=False):
    collection = r.read(ref)
    capture = r.read(collection['capture'])
    with claude.projection() if anthropic else ExitStack():
        expected = r.collect_result(wave, capture)
    if collection != {**expected, 'capture':collection['capture']}:
        raise r.Error('recovery collection does not replay')
    return collection, capture


def recoveries(base):
    """Replay exactly the approved recovery sets; never discover new retries."""
    mapping, plans, proofs = {}, {}, []
    cmref = r.binding(base/'claude-non-graphic-20260916/run/manifest.json')
    cm = claude.load(cmref)
    ccref = r.binding(Path(cm['state_root'])/'collection.json')
    cc, _ = checked_collection(cm['wave'], ccref, anthropic=True)
    cout = {o['job_id']:o for o in cc['outcomes']}
    proofs += [cmref, ccref, cc['capture']]
    for name in ('non-graphic-pilot-20260916','non-graphic-remaining-20260916'):
        mref = r.binding(base/name/'run/manifest.json')
        m = pilot.load(mref)
        cref = r.binding(Path(m['state_root'])/'collection.json')
        collection, _ = checked_collection(m['wave'], cref)
        selection = r.read(m['selection'])
        outcomes = {o['job_id']:o for o in collection['outcomes']}
        proofs += [mref, m['selection'], cref, collection['capture']]
        for row, job in zip(selection['records'], m['wave']['jobs']):
            original = pilot.original(row)
            if pilot.sanitize(original, row['edits']) != job:
                raise r.Error('editorial job differs from approved edits')
            pref = r.binding(Path(row['wave']['path']).parents[2]/'plan.json')
            plans[pref['path']] = pref
            result = outcomes[job['job_id']]['result']
            producer = cref
            if result is None:
                matches = [j for j in cm['wave']['jobs'] if j['scope']==job['scope'] and j['evidence']==job['evidence']]
                if len(matches) != 1 or cout[matches[0]['job_id']]['state'] != 'completed':
                    raise r.Error('editorial chunk still lacks a validated result')
                job = matches[0]
                result = cout[job['job_id']]['result']
                producer = ccref
            mapping[original['job_id']] = dict(original_job=original, job=job,
                result=result, producer=producer, editorial_copy=True, classification_adjustments=[])
            proofs += [pref, row['wave'], row['collection']]
    uref = r.binding(base/'reconciliation-20260916/replacements/manifest.json')
    um = unknown.load(uref)
    proofs.append(uref)
    for row in um['rows']:
        wave = row['wave']
        cref = r.binding(Path(um['state_root'])/wave['wave_id']/'collection.json')
        collection, capture = checked_collection(wave, cref)
        outcomes = {o['job_id']:o for o in collection['outcomes']}
        items = {i['custom_id']:i for i in capture['items']}
        pref = row['original_plan']
        plans[pref['path']] = pref
        proofs += [pref, cref, collection['capture'], wave['explicit_unknown_replacement']['original_wave'],
            wave['explicit_unknown_replacement']['original_intent']]
        for job in wave['jobs']:
            out = outcomes[job['job_id']]
            result, changes = out['result'], []
            if result is None:
                # Metadata-only conservative correction, never a content refusal
                # or evidence/text rewrite. Strict normalization must then pass.
                if out.get('validation_error') not in {'summary cannot upgrade an allegation to a statement',
                        'uncertainties require uncertainty classification'}:
                    raise r.Error('replacement still requires review')
                item = items[job['job_id']]
                if item['error'] is not None:
                    raise r.Error('cannot repair provider failure')
                result, changes = classification.normalize(job, r.response_payload('gemini', item['response']))
            if job['job_id'] in mapping:
                raise r.Error('conflicting recovery mappings')
            mapping[job['job_id']] = dict(original_job=job, job=job, result=result,
                producer=cref, editorial_copy=False, classification_adjustments=changes)
    return mapping, plans, proofs


def implementation():
    return dict(runtime=r.implementation(), integration=r.binding(__file__),
        pilot=pilot.implementation(), claude=claude.implementation(), unknown=unknown.implementation(),
        classification=r.binding(classification.__file__), feed=r.binding(feed.__file__))


def replace_inputs(snapshot, mapping):
    """Keep exact successful jobs; substitute only explicitly recovered failures."""
    known = {j['job_id']:j for j in snapshot['jobs']}
    initial = snapshot['initial']
    for job in initial:
        known.setdefault(job['job_id'],job)
    results = {v['job_id']:v for v in snapshot['results']}
    parents, applied = [], []
    for job in initial:
        key = job['job_id']
        replacement = mapping.get(key)
        if replacement:
            if replacement['original_job'] != job or key in results:
                raise r.Error('recovery would replace a successful or different original job')
            derived = replacement['job']
            old_links = r.core._unique_citations([c for e in job['evidence'] for c in e['citations']])
            new_links = r.core._unique_citations([c for e in derived['evidence'] for c in e['citations']])
            if derived['scope'] != job['scope'] or old_links != new_links:
                raise r.Error('recovery changed source scope or original evidence links')
            parents.append(replacement['job'])
            results[replacement['job']['job_id']] = replacement['result']
            applied.append(key)
        else:
            if key not in results:
                raise r.Error('recording still has an unrecovered chunk')
            parents.append(job)
    reductions = [j for j in snapshot['jobs'] if j['stage']=='transcript']
    for job in reductions:
        key = job['job_id']
        if key in mapping:
            recovery = mapping[key]
            if recovery['original_job'] != job or recovery['job'] != job or key in results:
                raise r.Error('recovered reducer must be the exact unpaid-result original')
            results[key] = recovery['result']
            applied.append(key)
    for job in parents + reductions:
        if job['job_id'] in results:
            with claude.projection() if job['provider']=='anthropic' else ExitStack():
                r.core.validate_result(job, results[job['job_id']])
    return parents, reductions, results, applied


def prepare(base, root):
    mapping, plans, proofs = recoveries(base)
    from pipeline import cloud_transcription_summary as worker
    from pipeline import gemini_dashboard_spend as spend, gemini_targeted_retry as retry
    from pipeline import gemini_recovery_extension as recovery, reviewed_summary_adapter as reviewed
    from pipeline import short_summary_admission_runner as short
    worker_ref = r.binding(base/'manifest.json')
    release_ref = r.binding(base.parent/'conservative-diarization-v1/execution-release/release.json')
    authority_ref = r.binding(base/'tier2-recovery-20260916/authority-v2.json')
    spend.install(worker, worker_ref)
    short.install(worker)
    recovery.install(worker)
    reviewed.install(worker, base.parent/'reviewed-transcript-feed-v1/index.json')
    records = []
    r.mkdir(root)
    r.mkdir(root/'records')
    with ExitStack() as stack:
        stack.enter_context(worker.release.activate(release_ref))
        stack.enter_context(worker.normalization_scope(worker_ref))
        stack.enter_context(worker.job_cache.scope(worker_ref))
        retry.install(worker, authority_ref)
        for pref in plans.values():
            folder = Path(pref['path']).parent
            with r.locked(folder):
                plan, sources = r.load_plan(pref['path'],pref['sha256'])
                state = r.load_state(plan,sources)
                initial = state.get('initial_jobs_override') or r.core.initial_jobs(sources,plan['request_value']['config'])
                r.core.source_coverage(sources,initial)
                snapshot = dict(plan=plan, sources=sources, initial=initial, jobs=state['jobs'], results=state['results'])
                parents, reductions, results, applied = replace_inputs(snapshot,mapping)
                if not applied or len(sources)!=1:
                    raise r.Error('recovery must apply to exactly one physical recording')
                config = plan['request_value']['config']
                if config.get('transcript_input_policy')!='text_and_speaker_evidence_v1':
                    raise r.Error('continuation requires metadata-free transcript input policy')
                # Replay any existing reducer before admitting a continuation.
                r.core._advance('transcript',plan['source_ids'],None,parents,reductions,results,config)
                key = folder.name
                target = root/'records'/key
                r.mkdir(target)
                r.mkdir(target/'waves')
                r.mkdir(target/'exports')
                source_proofs = [pref,plan['request']]
                source_proofs += [r.binding(p) for p in (folder/'sources').glob('*.json')]
                source_proofs += [r.binding(p) for p in (folder/'waves').glob('*/*') if p.is_file() and p.name!='execution.lock']
                document = dict(kind=KIND+'_record',original_plan=pref,source=sources[0],
                    config=config,parents=parents,retained_reducers=reductions,results=results,
                    applied=applied,substitutions={k:mapping[k] for k in applied},
                    original_snapshot=r.put(target/'original-snapshot.json',snapshot), proofs=source_proofs)
                record_ref = r.put(target/'record.json',document)
                records.append(record_ref)
                print(r.canonical(dict(event='integrated_inputs',record=key,chunks=len(parents),recovered=len(applied))).decode(),flush=True)
    return r.put(root/'manifest.json',dict(kind=KIND,implementation=implementation(),
        approval='User: Yes, integrate recovered chunks. Reuse successful results and purchase only missing transcript reductions.',
        state_root=str(root.resolve()),base=str(base.resolve()),records=records,proofs=proofs,
        original_worker=worker_ref,release=release_ref,retry_authority=authority_ref,
        no_automatic_retries=True,max_new_waves_per_record=12))


def load(ref):
    m = r.read(ref)
    if m['kind']!=KIND or m['implementation']!=implementation() or Path(ref['path'])!=Path(m['state_root'])/'manifest.json':
        raise r.Error('continuation implementation or identity changed')
    return m


def inspect_record(ref):
    doc = r.read(ref)
    snapshot = r.read(doc['original_snapshot'])
    for proof in doc['proofs']:
        r.read_bytes(proof)
    parents, retained, results, applied = replace_inputs(snapshot,doc['substitutions'])
    if (parents!=doc['parents'] or retained!=doc['retained_reducers'] or results!=doc['results']
            or applied!=doc['applied'] or doc['source']!=snapshot['sources'][0]
            or doc['config']!=snapshot['plan']['request_value']['config']):
        raise r.Error('continuation record differs from exact admitted source')
    return doc


def frontier(doc, folder):
    jobs = list(doc['retained_reducers'])
    results = deepcopy(doc['results'])
    pending, held, waves = [], [], []
    paths = sorted((folder/'waves').glob('*/wave.json'),key=lambda p:r.read(r.binding(p))['ordinal'])
    for ordinal,path in enumerate(paths):
        wave = r.read(r.binding(path))
        if wave['ordinal']!=ordinal or wave['wave_id']!='summarywave_'+r.digest({k:v for k,v in wave.items() if k!='wave_id'})[:32]:
            raise r.Error('continuation wave order or identity differs')
        ready,_ = r.core._advance('transcript',[doc['source']['source_id']],None,doc['parents'],jobs,results,doc['config'])
        if wave['jobs']!=ready:
            raise r.Error('continuation wave differs from complete dependency frontier')
        jobs.extend(wave['jobs'])
        waves.append(wave)
        cref = path.parent/'collection.json'
        if cref.exists():
            coll,_ = checked_collection(wave,r.binding(cref))
            receipt = r.read(r.binding(path.parent/'submitted.json'))
            r.check_remote(wave,r.read(coll['capture'])['batch'],receipt['remote_id'])
            for outcome in coll['outcomes']:
                if outcome['state']=='completed':
                    results[outcome['job_id']]=outcome['result']
                else:
                    held.append(outcome)
        else:
            pending.append(wave)
    ready,final = r.core._advance('transcript',[doc['source']['source_id']],None,doc['parents'],jobs,results,doc['config'])
    return dict(jobs=jobs,results=results,pending=pending,held=held,waves=waves,ready=ready,final=final)


def publish(doc, folder, result, manifest_ref):
    source = doc['source']
    body = dict(kind='himr_recovered_transcript_summary_export',schema_version=1,
        original_plan=doc['original_plan'],continuation=manifest_ref,record=r.binding(folder/'record.json'),
        source_id=source['source_id'],recording_id=source['recording_id'],title=source['title'],
        phase_complete=True,results=[result],recovered_job_ids=doc['applied'],
        editorial_input_used=any(x['editorial_copy'] for x in doc['substitutions'].values()),
        provider_provenance=[dict(job_id=j['job_id'],provider=j['provider'],model=j['model']) for j in doc['parents']],
        original_artifacts_unchanged=True)
    artifact = r.put(folder/'exports'/('summaries-'+r.digest(body)[:32]+'.json'),body)
    # A directly usable reader copy, retaining canonical internal evidence above.
    raw = '\n'.join(x['text'] for x in source['segments']).encode()
    transcript = r.put_bytes(folder/'exports'/'transcript.txt',raw)
    reader = dict(kind='himr_private_summary_reader_export',schema_version=1,
        plan_id='summarycontinuation_'+doc['original_snapshot']['sha256'][:32],phase='transcripts',phase_complete=True,complete=False,
        selected_source_ids=[source['source_id']],canonical_recovery=artifact,
        records=[dict(source_id=source['source_id'],recording_id=source['recording_id'],title=source['title'],
            date={k:source['date'][k] for k in ('value','kind')},
            sections={k:[dict(text=i['text'],classification=i['classification']) for i in v]
                for k,v in result['sections'].items()})],synthesis=[],
        transcript_files=[dict(source_id=source['source_id'],href='transcript.txt',sha256=transcript['sha256'])],
        semantics=r.SEMANTICS)
    return dict(recording_id=source['recording_id'],source_id=source['source_id'],title=source['title'],
        original_plan=doc['original_plan'],canonical=artifact,reader=r.put(folder/'exports'/'reader.json',reader))


def cycle(ref, docs, *, env_file, allow_paid_api):
    m = load(ref)
    root = Path(m['state_root'])
    api = r.api_client('gemini',env_file=env_file)
    records, exports = [], []
    with r.locked(root):
        for record_ref,doc in docs:
            folder = Path(record_ref['path']).parent
            state = frontier(doc,folder)
            for wave in state['pending']:
                wf = folder/'waves'/wave['wave_id']
                receipt_path = wf/'submitted.json'
                if not receipt_path.exists():
                    continue
                receipt = r.read(r.binding(receipt_path))
                try:
                    remote = api.get_batch(receipt['remote_id'])
                except r.client_module.BatchClientError:
                    continue  # Retain the receipt; poll again, never POST again.
                view = r.check_remote(wave,remote,receipt['remote_id'])
                if view['status'] in r.TERMINAL:
                    capture = dict(batch=remote,items=r.remote_items('gemini',remote,api))
                    capref = r.put(wf/'capture.json',capture)
                    r.put(wf/'collection.json',{**r.collect_result(wave,capture),'capture':capref})
            state = frontier(doc,folder)
            if state['final'] is not None:
                exports.append(publish(doc,folder,state['results'][state['final']['job_id']],ref))
                records.append(dict(title=doc['source']['title'],state='completed'))
                continue
            if state['held']:
                records.append(dict(title=doc['source']['title'],state='needs_review',failures=state['held']))
                continue
            wave = state['pending'][0] if state['pending'] else None
            if wave is None and state['ready'] and allow_paid_api:
                if len(state['waves'])>=m['max_new_waves_per_record']:
                    raise r.Error('continuation exceeds finite wave bound')
                body = dict(provider='gemini',model=state['ready'][0]['model'],jobs=state['ready'],
                    ordinal=len(state['waves']),record=record_ref,
                    classification_policy=classification.POLICY)
                wave = dict(wave_id='summarywave_'+r.digest(body)[:32],**body)
                wf = folder/'waves'/wave['wave_id']
                r.mkdir(wf)
                r.put(wf/'wave.json',wave)
            if wave is None:
                records.append(dict(title=doc['source']['title'],state='not_submitted'))
                continue
            wf = folder/'waves'/wave['wave_id']
            if not (wf/'submitted.json').exists() and not (wf/'submit-intent.json').exists() and allow_paid_api:
                requests = [dict(key=j['job_id'],request=j['request']['body']) for j in wave['jobs']]
                data = api.batch_bytes(wave['model'],requests,wave['wave_id'])
                payload = r.put_bytes(wf/'requests.bin',data)
                r.put(wf/'submit-intent.json',dict(input=payload,manifest=ref,
                    maximum_cost_microusd=sum(j['budget']['maximum_cost_microusd'] for j in wave['jobs'])))
                try:
                    remote = api.create_batch(wave['model'],requests,wave['wave_id'])
                except r.client_module.BatchClientError:
                    records.append(dict(title=doc['source']['title'],state='needs_reconciliation'))
                    continue  # One ambiguous POST cannot block other recordings.
                response = r.put(wf/'submission-response.json',remote)
                view = r.check_remote(wave,remote)
                r.put(wf/'submitted.json',dict(remote_id=view['id'],response=response))
            records.append(dict(title=doc['source']['title'],state='waiting_remote' if (wf/'submitted.json').exists() else 'needs_reconciliation'))
        # Published last; readers prefer these complete revisions over old holds.
        index = dict(kind=KIND+'_preferred_exports',continuation=ref,records=exports,
            preference='Use these final summaries for matching recording_id; preserve other existing summaries.')
        feed.atomic(root/'preferred-exports.json',index)
        status = dict(kind=KIND+'_status',recordings=len(records),completed=len(exports),records=records,
            original_worker_unchanged=True,automatic_retries=False)
        feed.atomic(root/'status.json',status)
        original = r.read(r.binding(Path(m['base'])/'scheduler-efficiency-v1/status.json'))
        completed_ids = {x['recording_id'] for x in exports}
        superseded = [x for x in original['holds'] if x['recording_id'] in completed_ids]
        overall = dict(kind=KIND+'_overall_status',original_worker_status=original['state'],
            preferred_transcript_summaries_complete=original['transcript_summaries_complete']+len(superseded),
            recovered_final_summaries=len(exports),historical_holds_superseded=len(superseded),
            remaining_original_holds=[x for x in original['holds'] if x['recording_id'] not in completed_ids],
            continuation_pending=len(records)-len(exports),preferred_exports=r.binding(root/'preferred-exports.json'))
        feed.atomic(root/'overall-status.json',overall)
        return status


def preferred_reader(plan_ref):
    """Read-only reader handoff for an exact original plan, never a title match."""
    base = Path(plan_ref['path']).parent.parent.parent
    root = base/'integrated-recovery-20260916'
    index_path = root/'preferred-exports.json'
    if not index_path.exists():
        return None
    index = r.read(r.binding(index_path))
    matches = [x for x in index['records'] if x['original_plan']==plan_ref]
    if not matches:
        return None
    if len(matches)!=1:
        raise r.Error('conflicting preferred recovery exports')
    m = load(index['continuation'])
    match = matches[0]
    body = r.read(match['canonical'])
    if body['record'] not in m['records'] or body['continuation']!=index['continuation']:
        raise r.Error('preferred export escaped approved continuation')
    doc = inspect_record(body['record'])
    state = frontier(doc,Path(body['record']['path']).parent)
    if (state['final'] is None or body['results']!=[state['results'][state['final']['job_id']]]
            or body['original_plan']!=plan_ref):
        raise r.Error('preferred recovery is not a complete source reduction')
    reader = r.read(match['reader'])
    if reader['canonical_recovery']!=match['canonical']:
        raise r.Error('reader canonical evidence differs')
    return dict(state='exported_private_reader',artifact=match['reader'],transcript_summaries=1,
        synthesis_summaries=0,phase='transcripts',phase_complete=True,complete=False,
        recovered_continuation=index['continuation'])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=('prepare','run','cycle'))
    p.add_argument('--base')
    p.add_argument('--path',required=True)
    p.add_argument('--sha256')
    p.add_argument('--env-file')
    p.add_argument('--allow-paid-api',action='store_true')
    a = p.parse_args()
    if a.command=='prepare':
        print(r.canonical(prepare(Path(a.base),Path(a.path))).decode(),flush=True)
        return
    ref = dict(path=str(Path(a.path).resolve()),sha256=a.sha256)
    m = load(ref)
    recoveries(Path(m['base']))  # Replay retained provider captures once at startup.
    docs = [(record,inspect_record(record)) for record in m['records']]
    deadline = time.monotonic() + 86400
    while True:
        result = cycle(ref,docs,env_file=a.env_file,allow_paid_api=a.allow_paid_api)
        print(r.canonical(dict(recordings=result['recordings'],completed=result['completed'],
            states=[x['state'] for x in result['records']])).decode(),flush=True)
        if (a.command=='cycle' or time.monotonic()>=deadline
                or all(x['state'] in {'completed','needs_review','needs_reconciliation'} for x in result['records'])):
            return
        time.sleep(30)


if __name__=='__main__':
    main()
