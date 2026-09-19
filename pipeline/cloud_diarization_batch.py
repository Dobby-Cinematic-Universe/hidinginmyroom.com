"""Explicit audited replacements in an independent, resumable paid workspace.

The audit is selection authority after user approval, not a fabricated positive
acoustic screen. Existing campaigns and third-party originals are never edited.
Four isolated processes prepare/upload/collect independently. Paid POSTs are
issued at most once per durable intent; only uploads and GETs can be retried.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import signal
import time

from pipeline import cloud_transcription_client as clients
from pipeline import cloud_transcription_env as env
from pipeline import cloud_transcription_media as media
from pipeline import cloud_transcription_upload as upload
from pipeline import reviewed_transcript_feed as feed
from pipeline import transcript_summary as io

KIND = 'himr_audited_diarization_batch'
TIERS = frozenset({'priority_long_form', 'additional_conversation', 'multilingual_conversation'})
ALLOCATION = 20_000_000
TOTAL_CAP = 150_000_000
PAID_FILES = ('intent.json', 'submission.json', 'reconciled.json', 'completion.json',
              'terminal-job.json', 'provider-transcript.json', 'submission-untrusted-response.json')
STOP = False


class BatchError(RuntimeError):
    pass


def _stop(*_):
    global STOP
    STOP = True


def _sleep(seconds):
    end = time.monotonic() + seconds
    while not STOP and time.monotonic() < end:
        time.sleep(min(1, end-time.monotonic()))


def _optional(path):
    return io.read(io.binding(path)) if Path(path).exists() else None


def job_id(recording):
    return 'cloudjob_' + io.digest({key: recording[key] for key in ('media', 'recording_id', 'duration_ms')})[:32]


def bound_cost(recording):
    duration = recording['duration_ms']
    media.tolerance_ms(duration)
    provider = 'assemblyai' if duration <= clients.ASSEMBLYAI_MAX_SECONDS*1000 else 'revai'
    clients.validate_duration(provider, duration/1000)
    seconds = max(15, math.ceil((duration + media.tolerance_ms(duration))/1000))
    rate = 230000 if provider == 'assemblyai' else 200000
    return provider, (seconds*rate+3599)//3600


def select(base, audit):
    if audit.get('kind') != 'himr_analyst_diarization_recommendations':
        raise BatchError('expected the explicit analyst recommendation list')
    wanted = [r for r in audit['records'] if r['tier'] in TIERS]
    if len(wanted) != 63 or len({r['recording_id'] for r in wanted}) != 63:
        raise BatchError('approval is scoped to exactly 63 distinct recommendations')
    original = {r['job_id']: r for r in base['recordings']}
    result = []
    for candidate in wanted:
        row = original[candidate['original_job_id']]
        recording = row['recording']
        if (row['disposition'] != 'third_party' or row['import']['transcript'] != candidate['transcript_ref']
                or recording['recording_id'] != candidate['recording_id'] or recording['media'] != candidate['media']
                or recording['duration_ms'] != candidate['duration_ms'] or row['job_id'] != job_id(recording)):
            raise BatchError('candidate differs from exact original third-party selection')
        if recording['state'] != 'ready' and not (recording['state'] == 'review'
                and recording['reasons'] == ['source_id_maps_to_multiple_physical_recordings']):
            raise BatchError('cannot bypass an unrelated admission/media failure')
        provider, cost = bound_cost(recording)
        selected = deepcopy(recording)
        selected.update(state='ready', reasons=[])
        result.append(dict(job_id=row['job_id'], recording=selected, provider=provider,
            maximum_cost_microusd=cost, diarization=True,
            language='auto' if provider == 'assemblyai' else 'en',
            retained_third_party=row['import'], original_recording=recording,
            analyst_candidate=candidate, disposition='cloud'))
    # A whole short Japanese and English recording are canaries, not extra paid
    # samples. Other jobs remain independently resumable if a result is rejected.
    canaries = {'dDls26hZTpY': 0, 'xekcJ6hsjYU': 1}
    result.sort(key=lambda r: (canaries.get(r['analyst_candidate']['id'], 2),
        0 if r['analyst_candidate']['tier']=='priority_long_form' else 1,
        r['recording']['duration_ms']))
    if sum(r['maximum_cost_microusd'] for r in result) > ALLOCATION:
        raise BatchError('selected jobs exceed the batch allocation')
    return result


def allocation_check(base, previous):
    maximum = base['prior_reserved_microusd'] + sum(r['maximum_cost_microusd'] for r in base['recordings'])
    previous_allocation = previous['allocation_microusd']
    if not isinstance(previous_allocation, int) or previous_allocation < previous['maximum_cost_microusd']:
        raise BatchError('invalid previous lane allocation')
    if maximum+previous_allocation+ALLOCATION > TOTAL_CAP:
        raise BatchError('combined transcription allocations exceed the user cap')
    return dict(original_maximum_microusd=maximum, previous_lane_allocation_microusd=previous_allocation,
                this_allocation_microusd=ALLOCATION, combined_ceiling_microusd=maximum+previous_allocation+ALLOCATION,
                total_cap_microusd=TOTAL_CAP)


def assert_no_prior_paid(rows, prior_refs):
    identifiers = {r['recording']['recording_id'] for r in rows}
    for ref in prior_refs:
        prior = io.read(ref)
        for row in prior['recordings']:
            if row['recording']['recording_id'] not in identifiers:
                continue
            folder = Path(prior['state_root'])/'jobs'/row['job_id']
            paths = [folder/name for name in PAID_FILES]
            paths.append(Path(prior['state_root'])/'reservations'/(row['job_id']+'.json'))
            if any(path.exists() for path in paths):
                raise BatchError('selected physical recording already has paid evidence; no duplicate submission')


def runtime_bindings():
    root = Path(__file__).resolve().parent
    return {p.name: io.binding(p) for p in sorted(root.glob('*.py'))}


def check_runtime_release(ref, expected, actual, release):
    """A collector repair must not replace the plan or any existing paid intent."""
    if expected == actual:
        return
    if (not isinstance(release, dict) or release.get('kind') != KIND+'_runtime_release'
            or release.get('plan') != ref or release.get('previous_implementation') != expected
            or release.get('implementation') != actual
            or release.get('paid_selection_changed') is not False
            or release.get('paid_retries_authorized') is not False
            or set(expected) != set(actual)):
        raise BatchError('batch runtime differs without a bound collector release')
    changed = {name for name in expected if expected[name]['sha256'] != actual[name]['sha256']}
    if not changed <= {'cloud_diarization_batch.py', 'cloud_transcription_client.py'}:
        raise BatchError('collector release changes unrelated implementation')


def prepare(base_ref, audit_ref, previous_ref, prior_refs, output):
    base, audit, previous = map(io.read, (base_ref, audit_ref, previous_ref))
    rows = select(base, audit)
    budget = allocation_check(base, previous)
    assert_no_prior_paid(rows, prior_refs)
    root = Path(output).resolve()
    io.protect(root, {'original_plan':base_ref, 'audit':audit_ref, 'previous':previous_ref})
    io.mkdir(root)
    with io.locked(root):
        if (root/'plan.json').exists():
            existing=io.read(io.binding(root/'plan.json'))
            if (existing['audit']!=audit_ref or existing['original_plan']!=base_ref
                    or existing['previous_lane']!=previous_ref or existing['prior_plans']!=prior_refs):
                raise BatchError('existing batch belongs to different authority')
            return io.binding(root/'plan.json')
        if any(p.name != 'execution.lock' for p in root.iterdir()):
            raise BatchError('refusing nonempty unmarked batch workspace')
        for name in ('jobs','reservations'):
            io.mkdir(root/name)
        approval = io.put(root/'authorization.json', dict(kind=KIND+'_authorization',schema_version=1,
            approval='User: Yes, submit those with diarization. Applies to the 63 positive audit recommendations only.',
            audit=audit_ref, original_plan=base_ref, previous_lane=previous_ref, prior_plans=prior_refs,
            budget=budget, excluded_tiers=sorted({r['tier'] for r in audit['records']} - TIERS),
            third_party_originals_preserved=True, local_asr_used=False, existing_reviews_modified=False,
            automatic_paid_retries=False, speaker_identity_inferred=False))
        plan = dict(kind=KIND, schema_version=1,state_root=str(root), authorization=approval,
            audit=audit_ref, original_plan=base_ref, previous_lane=previous_ref, prior_plans=prior_refs,
            ffmpeg=io.binding('/usr/bin/ffmpeg'), implementation=runtime_bindings(),
            budget=budget, maximum_cost_microusd=sum(r['maximum_cost_microusd'] for r in rows),
            policy=dict(whole_recordings=True, flac_transport=True, diarization=True,
                fixed_speaker_count=None, automatic_paid_retries=False, automatic_model_fallback=False,
                word_timestamps_in_canonical=False, new_gemini_requests=False,
                native_model_languages=sorted(clients.ASSEMBLYAI_NATIVE_LANGUAGES),
                minority_thai_coverage_not_guaranteed=True),recordings=rows)
        ref = io.put(root/'plan.json',plan)
        io.put(root/'spending-limit.json',dict(plan=ref,maximum_microusd=ALLOCATION,
            scope='this_batch_with_separate_combined_allocation_guard', automatic_hold_release=False))
        for row in rows:
            folder=root/'jobs'/row['job_id']; io.mkdir(folder)
            io.put(folder/'job.json',dict(plan=ref,recording=row))
            # Preserve screening limitations verbatim. Explicit approval and
            # inspected transcript dialogue authorize this override, not faces.
            evidence=[io.binding(path) for path in row['analyst_candidate']['existing_screen_paths']]
            io.put(folder/'screen.json',dict(kind=KIND+'_approved_diarization',plan=ref,
                recording_id=row['recording']['recording_id'],media=row['recording']['media'],
                authorization=approval,audit=audit_ref, candidate_id=row['analyst_candidate']['id'],
                base_audio_states=row['analyst_candidate']['existing_audio_states'],
                retained_screen_results=evidence,diarization=True,
                acoustic_positive_inferred=False,speaker_identity_inferred=False))
        return ref


def load_plan(ref):
    plan=io.read(ref)
    if plan['kind']!=KIND or Path(ref['path'])!=Path(plan['state_root'])/'plan.json':
        raise BatchError('batch plan identity differs')
    actual = runtime_bindings()
    release = None
    if plan['implementation'] != actual:
        release = _optional(Path(__file__).resolve().parent.parent/'runtime-release.json')
    check_runtime_release(ref, plan['implementation'], actual, release)
    base,audit,previous=map(io.read,(plan['original_plan'],plan['audit'],plan['previous_lane']))
    if plan['recordings']!=select(base,audit) or plan['budget']!=allocation_check(base,previous):
        raise BatchError('batch selection or budget differs')
    io.read(plan['authorization'])
    return plan


def safe_call(method, *args, **kwargs):
    """Only upload and GET operations may enter this retry wrapper."""
    if method.__name__ not in {'upload','poll','transcript'}:
        raise BatchError('paid method cannot use safe retries')
    for attempt in range(4):
        if STOP: raise BatchError('paused')
        try:
            return method(*args,**kwargs)
        except clients.CloudClientError as error:
            if not upload.transient(error) or attempt==3: raise
            _sleep(min(120,max(5*2**attempt,error.retry_after_seconds or 0)))


def reserve(plan, ref, row, intent):
    root=Path(plan['state_root'])
    for attempt in range(50):
        try:
            return _reserve_locked(root,row,intent)
        except io.Error as error:
            if str(error)!='another summary command owns this workspace' or attempt==49:
                raise
            _sleep(.1)


def _reserve_locked(root,row,intent):
    with io.locked(root/'reservations'):
        path=root/'reservations'/(row['job_id']+'.json')
        if path.exists():
            raise BatchError('existing paid reservation requires reconciliation, never another POST')
        reserved=sum(io.read(io.binding(p))['maximum_cost_microusd'] for p in (root/'reservations').glob('*.json'))
        if reserved+intent['maximum_cost_microusd']>ALLOCATION:
            raise BatchError('batch budget exceeded')
        io.put(path,intent)
        io.put(root/'jobs'/row['job_id']/'intent.json',intent)


def normalize(row, raw, terminal, audio):
    """Narrow existing timing repair, with all original provider bytes retained."""
    options=dict(expected_duration_seconds=audio['duration_ms']/1000,job=terminal,
                 diarization=True,language=row['language'])
    try:
        return clients.normalize_result(row['provider'],raw,**options),None,None
    except clients.CloudClientError as error:
        if row['provider']!='assemblyai' or str(error)!='utterance/word timing mismatch': raise
    adjusted=deepcopy(raw);changes=[]
    for index,turn in enumerate(adjusted['utterances']):
        end=max(word['end'] for word in turn['words'])
        delta=end-turn['end']
        if delta>2000: raise BatchError('utterance timing defect exceeds bounded repair')
        if delta>0:
            changes.append(dict(utterance_index=index,old_end_ms=turn['end'],new_end_ms=end))
            turn['end']=end
    if not 1<=len(changes)<=10: raise BatchError('no bounded utterance-end repair available')
    normalized=clients.normalize_result(row['provider'],adjusted,**options)
    if normalized['text']!=raw['text']: raise BatchError('timing repair changed provider text')
    return normalized,adjusted,changes


def _record(ref, job, env_file):
    plan=load_plan(ref);root=Path(plan['state_root'])
    row=next(r for r in plan['recordings'] if r['job_id']==job);folder=root/'jobs'/job
    def progress(state, **extra):
        feed.atomic(folder/'status.json',dict(job_id=job,state=state,updated_unix=time.time(),**extra))
    with io.locked(folder):
        if (folder/'completion.json').exists():
            completed=io.read(io.binding(folder/'completion.json'));io.read(completed['transcript'])
            progress('completed');return 'completed'
        if (folder/'hold.json').exists():
            return io.read(io.binding(folder/'hold.json'))['state']
        if io.read(io.binding(folder/'job.json')) != dict(plan=ref,recording=row):
            raise BatchError('job binding differs')
        provider=row['provider'];key=env.api_key(provider,env_file=env_file)
        client=(clients.AssemblyAIClient if provider=='assemblyai' else clients.RevAIClient)(key,timeout_seconds=900)
        intent=_optional(folder/'intent.json');receipt=_optional(folder/'submission.json')
        reservation=_optional(root/'reservations'/(job+'.json'))
        if receipt is None and (intent is not None or reservation is not None):
            raise BatchError('paid request requires reconciliation; no repeat')
        if receipt is not None and (intent is None or intent!=reservation or intent['plan']!=ref
                or intent['diarization'] is not True or intent['language']!=row['language']):
            raise BatchError('paid intent/receipt binding differs')
        screen_ref=io.binding(folder/'screen.json');decision=io.read(screen_ref)
        if decision['plan']!=ref or decision['diarization'] is not True:
            raise BatchError('explicit diarization approval differs')
        if receipt is None:
            assert_no_prior_paid([row],plan['prior_plans'])
            progress('preparing_audio')
            audio=media.prepare(row['recording'],folder,plan['ffmpeg'])
            clients.validate_duration(provider,audio['duration_ms']/1000)
            progress('preparing_flac')
            transport=upload.prepare(audio,folder,plan['ffmpeg'])
            if transport['byte_count']>(clients.ASSEMBLYAI_MAX_UPLOAD_BYTES if provider=='assemblyai' else clients.REVAI_MAX_UPLOAD_BYTES):
                raise BatchError('whole-file FLAC exceeds selected provider limit')
            uploaded=_optional(folder/'upload.json')
            if provider=='assemblyai':
                if uploaded is None:
                    progress('uploading')
                    uploaded=safe_call(client.upload,transport['path'],expected_sha256=transport['sha256'])
                    io.put(folder/'upload.json',uploaded)
                clients.validate_upload_url(uploaded['upload_url'])
            if STOP: progress('paused');return 'paused'
            metadata=job+'_'+io.digest(dict(audio=audio,screen=screen_ref,language=row['language']))[:24]
            intent=dict(kind=KIND+'_paid_intent',plan=ref,job_id=job,provider=provider,
                recording_id=row['recording']['recording_id'],audio=audio,transport=transport,
                screen_decision=screen_ref,diarization=True,language=row['language'],
                request_metadata=metadata,maximum_cost_microusd=row['maximum_cost_microusd'])
            reserve(plan,ref,row,intent)
            progress('submitting')
            try:
                if provider=='assemblyai':
                    receipt=client.submit(uploaded['upload_url'],diarization=True,language=row['language'])
                else:
                    receipt=client.submit_file(transport['path'],expected_sha256=transport['sha256'],
                                               metadata=metadata,diarization=True)
            except clients.CloudClientError as error:
                if error.response is not None: io.put(folder/'submission-untrusted-response.json',error.response)
                raise
            io.put(folder/'submission.json',receipt)
        else:
            audio=intent['audio']
        remote=clients.validate_job(provider,receipt)
        progress('pending',provider=provider,provider_job_id=remote['job_id'])
        while not STOP:
            terminal=_optional(folder/'terminal-job.json')
            if terminal is None:
                try: terminal=safe_call(client.poll,remote['job_id'])
                except clients.CloudClientError as error:
                    if upload.transient(error): _sleep(60);continue
                    raise
                checked=clients.validate_job(provider,terminal,expected_job_id=remote['job_id'])
                if checked['status'] not in {'completed','transcribed','error','failed'}:
                    _sleep(20);continue
                io.put(folder/'terminal-job.json',terminal)
            clients.validate_job(provider,terminal,expected_job_id=remote['job_id'])
            if terminal['status'] in {'error','failed'}:
                raise BatchError('provider reported a terminal failure; original response retained')
            terminal_ref=io.binding(folder/'terminal-job.json')
            progress('collecting',provider=provider,provider_job_id=remote['job_id'])
            if provider=='assemblyai': raw,raw_ref=terminal,terminal_ref
            else:
                raw=_optional(folder/'provider-transcript.json')
                if raw is None:
                    raw=safe_call(client.transcript,remote['job_id'])
                    io.put(folder/'provider-transcript.json',raw)
                raw_ref=io.binding(folder/'provider-transcript.json')
            normalized,adjusted,changes=normalize(row,raw,terminal,audio)
            doc=dict(kind='himr_cloud_recording_transcript',schema_version=1,job_id=job,
                recording_id=row['recording']['recording_id'],source_media=row['recording']['media'],
                status='completed',provider_job_id=remote['job_id'],raw_result=raw_ref,provider_job=terminal_ref,
                screen_decision=screen_ref,audio=audio,whole_recording_submitted=True,
                normalizer_implementation_sha256=io.binding(clients.__file__)['sha256'],
                machine_generated=True,full_media_coverage_verified=False,human_reviewed=False,
                verified_quotation=False,speaker_identity_inferred=False,publication_authority=False,
                language_policy=row['language'],detected_language=terminal.get('language_code',terminal.get('language')),
                source_third_party_preserved=row['retained_third_party']['transcript'],**normalized)
            if changes:
                adjusted_ref=io.put(folder/'timing-adjusted-result.json',adjusted)
                doc['recovery']=dict(kind='bounded_utterance_end_from_retained_words',
                    original_raw_result=raw_ref,adjusted_result=adjusted_ref,changes=changes,
                    text_changed=False,paid_retry=False,human_review_required=True)
            labels={s['speaker'] for s in doc['segments'] if s.get('speaker')}
            if len(labels)==1:
                doc['single_speaker_normalization']=dict(original_provider_speaker_labels=doc['provider_speaker_labels'])
                doc['provider_speaker_labels']={}
                for segment in doc['segments']: segment['speaker']=None
            transcript_ref=io.put(folder/'transcript.json',doc)
            completion=dict(kind='himr_cloud_transcription_completion',schema_version=1,job_id=job,
                audio=audio,raw_result=raw_ref,provider_job=terminal_ref,transcript=transcript_ref,
                screen_decision=screen_ref,batch_plan=ref)
            io.put(folder/'completion.json',completion)
            # Only regenerable files made by this batch are pruned, after the
            # immutable provider response and canonical transcript are durable.
            pruned=upload.prune(folder,completion)+media.prune_completed(folder,completion)
            progress('completed',provider=provider,provider_job_id=remote['job_id'],
                speaker_labels=len(labels),requires_speaker_review=len(labels)>1,pruned_bytes=pruned)
            return 'completed'
        progress('paused');return 'paused'


def record(ref,job,env_file):
    os.umask(0o077)
    signal.signal(signal.SIGTERM,_stop);signal.signal(signal.SIGINT,_stop)
    try:
        return _record(ref,job,env_file)
    except Exception as error:
        plan=io.read(ref);folder=Path(plan['state_root'])/'jobs'/job
        uncertain=(folder/'intent.json').exists() and not (folder/'submission.json').exists()
        if STOP and not uncertain:
            feed.atomic(folder/'status.json',dict(job_id=job,state='paused',updated_unix=time.time()))
            return 'paused'
        state='reconciliation_required' if uncertain else 'needs_review'
        hold=dict(kind=KIND+'_hold',plan=ref,job_id=job,state=state,
            error_type=type(error).__name__,reason=str(error) if isinstance(error,(BatchError,clients.CloudClientError,media.MediaError)) else 'local operation failed; retained state requires inspection',
            status_code=getattr(error,'status_code',None),automatic_paid_retry=False,
            provider_receipts_preserved=True)
        io.put(folder/'hold.json',hold)
        feed.atomic(folder/'status.json',dict(job_id=job,state=state,error_type=hold['error_type'],updated_unix=time.time()))
        return state


def status(ref):
    plan=io.read(ref);root=Path(plan['state_root']);states=Counter();reserved=0;submitted=0;records=[]
    for row in plan['recordings']:
        folder=root/'jobs'/row['job_id'];state=_optional(folder/'status.json') or {'state':'queued'}
        hold=_optional(folder/'hold.json')
        if (folder/'completion.json').exists(): state={**state,'state':'completed'}
        elif hold: state={**state,'state':hold['state']}
        states[state['state']]+=1
        receipt=_optional(folder/'submission.json');submitted+=receipt is not None
        reserve_=_optional(root/'reservations'/(row['job_id']+'.json'))
        reserved+=reserve_['maximum_cost_microusd'] if reserve_ else 0
        records.append(dict(job_id=row['job_id'],title=row['recording']['title'],
            provider=row['provider'],candidate_id=row['analyst_candidate']['id'],state=state['state'],
            provider_job_id=receipt.get('id') if receipt else None))
    result=dict(kind=KIND+'_status',plan=ref,selected_recordings=len(records),states=dict(states),
        provider_submissions=submitted,reserved_microusd=reserved,allocation_microusd=ALLOCATION,
        estimated_maximum_microusd=plan['maximum_cost_microusd'],records=records,
        automatic_paid_retries=False,new_gemini_requests=0)
    return result


def run(ref,env_file,workers=4):
    if not 1<=workers<=4: raise BatchError('at most four isolated workers')
    plan=load_plan(ref);root=Path(plan['state_root']);deadline=time.monotonic()+86400
    for provider in {r['provider'] for r in plan['recordings']}:
        if not env.api_key(provider,env_file=env_file): raise BatchError('required API key is missing')
    signal.signal(signal.SIGTERM,_stop);signal.signal(signal.SIGINT,_stop)
    with io.locked(root),ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context('spawn')) as pool:
        pending=list(plan['recordings']);active={}
        while (pending or active) and not STOP and time.monotonic()<deadline:
            while pending and len(active)<workers:
                row=pending.pop(0);future=pool.submit(record,ref,row['job_id'],env_file);active[future]=row['job_id']
            for future in list(active):
                if future.done():
                    result=future.result()
                    print(json.dumps(dict(event='record_finished',job_id=active.pop(future),state=result)),flush=True)
            result=status(ref);feed.atomic(root/'status.json',result)
            print(json.dumps({k:v for k,v in result.items() if k!='records'}),flush=True)
            if pending or active: _sleep(20)
        result=status(ref);feed.atomic(root/'status.json',result)
        return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['prepare','run','status'])
    for option in ('base-plan','audit','previous-lane','output','plan','expected-sha256','env-file'):
        p.add_argument('--'+option)
    p.add_argument('--prior-plan',action='append',default=[])
    p.add_argument('--allow-paid-api',action='store_true');p.add_argument('--workers',type=int,default=4)
    a=p.parse_args();os.umask(0o077)
    if a.command=='prepare':
        print(json.dumps(prepare(io.binding(a.base_plan),io.binding(a.audit),io.binding(a.previous_lane),
              [io.binding(path) for path in a.prior_plan],a.output)));return
    ref=dict(path=a.plan,sha256=a.expected_sha256)
    if a.command=='run':
        if not a.allow_paid_api or not a.env_file: raise BatchError('explicit paid approval and dotenv path required')
        value=run(ref,a.env_file,a.workers)
    else:value=status(ref)
    print(json.dumps({k:v for k,v in value.items() if k!='records'}))


if __name__=='__main__':main()
