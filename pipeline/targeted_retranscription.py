"""Separate, speech-gated recovery lane authorized on 2026-09-17.

No source replacement, automatic paid retries, or automatic summary admission.
Whole-file FLAC is submitted; screening samples never become ASR inputs.
"""
import argparse
from concurrent.futures import ProcessPoolExecutor
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import time

from pipeline import transcript_summary as io
from pipeline import cloud_transcription_client as client
from pipeline import cloud_transcription_env as env
from pipeline import cloud_transcription_media as media
from pipeline import cloud_transcription_upload as upload
from pipeline import cloud_transcription_title_policy as titles
from pipeline import reviewed_transcript_feed as feed

KIND = 'himr_targeted_retranscription_20260917'
STOP = False
PRIORITY = ('Live Q&A with my Sister', 'Staying at my viewers house',
            'Staying at the cheapest hotel in Osaka with my girlfriend', 'Self Isolation - Day 1-2')
PAID = ('intent.json','submission-intent.json','submission.json','completion.json',
        'terminal-job.json','provider-transcript.json','submission-untrusted-response.json')


def read(path):
    return io.read(io.binding(Path(path).resolve()))


def implementations():
    return {m.__name__: io.binding(m.__file__) for m in (io,client,env,media,upload,feed,titles)} | {'runner':io.binding(__file__)}


def related(a,b):
    if a['recording_id']==b['recording_id']: return True
    if abs(a['duration_ms']-b['duration_ms'])>max(10000,min(a['duration_ms'],b['duration_ms'])*.01): return False
    return bool(set(a['source_ids'].get('youtube',[])) & set(b['source_ids'].get('youtube',[])))


def prior_evidence(base, recording):
    result=[]
    for relative in ('transcription-v5','diarized-third-party-v1/transcription','diarized-candidates-v1/transcription'):
        root=base/relative
        for row in read(root/'plan.json')['recordings']:
            if related(recording,row['recording']):
                folder=root/'jobs'/row['job_id']
                paths=[folder/name for name in PAID]+[root/'reservations'/(row['job_id']+'.json')]
                evidence=[io.binding(p) for p in paths if p.exists()]
                if evidence:
                    terminal=read(folder/'terminal-job.json') if (folder/'terminal-job.json').exists() else None
                    # Explicitly empty terminal results are the only prior paid
                    # jobs this approval retries. Pending/uncertain work is held.
                    retry_empty=bool(terminal and terminal.get('status')=='completed' and not terminal.get('text'))
                    result.append(dict(recording_id=row['recording']['recording_id'],evidence=evidence,retry_empty=retry_empty))
    return result


def prepare(base, preview, output):
    base,preview,output=base.resolve(),preview.resolve(),output.resolve()
    if output.exists(): raise ValueError('use a fresh recovery workspace')
    plan=read(base/'transcription-v5/plan.json')
    rows={r['recording']['recording_id']:r for r in plan['recordings']}
    recon=read(base/'metadata-reconciliation-v3/index.json')
    matches=read(base/'transcription-v5/third-party-matches.json')
    matches=dict(zip(matches['recording_ids'],matches['matches']))
    candidates=[];excluded=[]
    for r in recon['records']:
        original=rows[r['recording_id']];meta=original['recording']
        if r['reconciliation'].get('partial') and any(t in meta['title'] for t in PRIORITY):
            doc=read(r['transcript']['path'])
            candidates.append((original,'priority_missing_tail',max(s['end_ms'] for s in doc['segments'])))
    for h in recon['held']:
        original=rows[h['recording_id']];issues=matches[h['recording_id']]['issues']
        if original['disposition']=='no_audio' or 'timestamps_exceed_media_duration' in issues or h['reason']=='invalid segment timing; no invented timestamps':
            excluded.append(dict(recording_id=h['recording_id'],title=h['title'],reason='no_audio_or_local_timing_recovery_first'));continue
        start=0
        if h['reason']=='different-duration variants need timeline alignment':
            start=matches[h['recording_id']]['candidates'][0]['last_end_ms']
        candidates.append((original,h['reason'],start))
    selected=[]
    for original,reason,start in sorted(candidates,key=lambda v:(v[1]!='priority_missing_tail',v[0]['recording']['media']['byte_count'])):
        meta=original['recording']
        if meta['state']!='ready' and set(meta['reasons'])-{'source_id_maps_to_multiple_physical_recordings'}:
            excluded.append(dict(recording_id=meta['recording_id'],title=meta['title'],reason='unresolved_source_admission',details=meta['reasons']));continue
        duplicate=next((r for r in selected if related(meta,r['recording'])),None)
        if duplicate:
            duplicate['alternate_recordings'].append(meta['recording_id']);continue
        prior=prior_evidence(base,meta)
        if any(not r['retry_empty'] for r in prior):
            excluded.append(dict(recording_id=meta['recording_id'],title=meta['title'],reason='existing_paid_work_needs_reconciliation_or_reuse',prior=prior));continue
        oldscreen=base/'transcription-v5/jobs'/original['job_id']/'selective-screen.json'
        diarization=bool(read(oldscreen).get('diarization')) if oldscreen.exists() else titles.match_titles(meta)['matched']
        if reason=='priority_missing_tail' and 'Self Isolation' not in meta['title']:diarization=True
        provider='assemblyai' if meta['duration_ms']<=client.ASSEMBLYAI_MAX_SECONDS*1000 else 'revai'
        seconds=math.ceil((meta['duration_ms']+media.tolerance_ms(meta['duration_ms']))/1000)
        cost=(max(15,seconds)*(230000 if provider=='assemblyai' else 200000)+3599)//3600
        selected.append(dict(job_id=original['job_id'],recording=meta,reason=reason,probe_start_ms=start,
            alternate_recordings=[],prior_paid=prior,diarization=diarization,provider=provider,
            language='auto' if provider=='assemblyai' else 'en',maximum_cost_microusd=cost))
    ceiling=sum(r['maximum_cost_microusd'] for r in selected)
    if ceiling>10_000_000:raise ValueError('targeted selection exceeds conservative $10 allocation')
    previous_budget=read(base/'diarized-candidates-v1/transcription/plan.json')['budget']
    if previous_budget['combined_ceiling_microusd']+10_000_000>150_000_000:
        raise ValueError('combined transcription allocations exceed $150')
    io.mkdir(output);io.mkdir(output/'jobs');io.mkdir(output/'reservations')
    models=read(Path('research/corpus/speaker-screen-runtime-20260912/models.json'))
    result=dict(kind=KIND,state_root=str(output.resolve()),approval='User: Yes, retranscribe them. Targeted missing-speech recovery, not full archive.',
        base=str(base.resolve()),source_plan=io.binding(base/'transcription-v5/plan.json'),
        reconciliation=io.binding(base/'metadata-reconciliation-v3/index.json'),preview=io.binding(preview/'preparation.json'),
        implementation=implementations(),ffmpeg=io.binding('/usr/bin/ffmpeg'),vad=models['silero_vad'],
        maximum_cost_microusd=ceiling,allocation_microusd=10_000_000,previous_budget=previous_budget,recordings=selected,excluded=excluded,
        original_transcripts_modified=False,automatic_paid_retries=False,new_summary_requests=False)
    ref=io.put(output/'plan.json',result)
    for row in selected:io.mkdir(output/'jobs'/row['job_id'])
    return dict(plan=ref,candidates=len(selected),excluded=len(excluded),maximum_cost_microusd=ceiling)


def probe_ranges(duration,start):
    if not 0<=start<duration:raise ValueError('invalid missing-speech interval')
    if duration<=20*60000:return [(0,duration)]
    span=duration-start;width=min(30000,span)
    return sorted(set((round(start+(span-width)*i/7),width) for i in range(8)))


def speech_screen(plan,row):
    import numpy as np
    import onnxruntime as ort
    data=Path(plan['vad']['path']).read_bytes()
    if hashlib.sha256(data).hexdigest()!=plan['vad']['sha256']:raise ValueError('VAD model changed')
    options=ort.SessionOptions();options.intra_op_num_threads=1;options.inter_op_num_threads=1
    session=ort.InferenceSession(data,sess_options=options,providers=['CPUExecutionProvider'])
    probes=[]
    for start,length in probe_ranges(row['recording']['duration_ms'],row['probe_start_ms']):
        if STOP:raise ValueError('paused')
        result=subprocess.run([plan['ffmpeg']['path'],'-nostdin','-v','error','-threads','1',
            '-protocol_whitelist','file,pipe','-format_whitelist',media.FORMATS,'-ss',str(start/1000),
            '-i',row['recording']['media']['path'],'-t',str(length/1000),'-map','0:a:0','-vn','-ac','1','-ar','16000',
            '-f','s16le','pipe:1'],capture_output=True,timeout=180,check=True)
        if len(result.stdout)>length*32+65536:raise ValueError('oversized probe')
        wave=np.frombuffer(result.stdout,dtype='<i2').astype(np.float32)/32768
        state=np.zeros((2,1,128),np.float32);context=np.zeros((1,64),np.float32);speech=0;run=0;longest=0
        for pos in range(0,len(wave),512):
            frame=np.pad(wave[pos:pos+512],(0,max(0,512-len(wave[pos:pos+512]))))
            inputs=np.concatenate((context,frame.reshape(1,512)),axis=1)
            out,state=session.run(['output','stateN'],{'input':inputs,'state':state,'sr':np.array(16000,dtype=np.int64)})
            if not np.isfinite(out).all():raise ValueError('invalid VAD output')
            context=inputs[:,-64:].copy()
            positive=float(out[0,0])>=.5
            speech+=32 if positive else 0;run=run+32 if positive else 0;longest=max(longest,run)
        probes.append(dict(start_ms=start,duration_ms=len(wave)//16,speech_ms=speech,longest_run_ms=longest))
    positive=any(p['speech_ms']>=1500 and p['longest_run_ms']>=256 for p in probes)
    return dict(kind=KIND+'_speech_screen',positive=positive,probes=probes,model=plan['vad'],
        speech_is_not_verified_live_dialogue=True,negative_sampling_does_not_prove_silence=True,
        whole_recording_submitted_if_positive=True)


def normalize(row,raw,terminal,audio):
    # Reuse strict normalizer; retain failures for local repair, never repurchase.
    doc=client.normalize_result(row['provider'],raw,expected_duration_seconds=audio['duration_ms']/1000,
        job=terminal,diarization=row['diarization'],language=row['language'])
    if any(s['end_ms']<=s['start_ms'] for s in doc['segments']):raise ValueError('nonpositive turn interval needs local recovery')
    labels={s.get('speaker') for s in doc['segments'] if s.get('speaker')}
    if len(labels)==1:
        doc['single_speaker_normalization']=dict(original_provider_speaker_labels=doc['provider_speaker_labels'])
        doc['provider_speaker_labels']={}
        for s in doc['segments']:s['speaker']=None
    return doc,len(labels)


def stop(*args):
    global STOP
    STOP=True


def sleep(seconds):
    end=time.monotonic()+seconds
    while not STOP and time.monotonic()<end:time.sleep(min(1,end-time.monotonic()))


def safe_call(method,*args,**kwargs):
    if method.__name__ not in {'upload','poll','transcript'}:raise ValueError('paid POST cannot be retried')
    for attempt in range(4):
        if STOP:raise ValueError('paused')
        try:return method(*args,**kwargs)
        except client.CloudClientError as error:
            if not upload.transient(error) or attempt==3:raise
            sleep(min(60,max(5*2**attempt,error.retry_after_seconds or 0)))


def process(ref,job,env_file):
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop);os.umask(0o077)
    plan=io.read(ref);root=Path(plan['state_root']);folder=root/'jobs'/job
    row=next(r for r in plan['recordings'] if r['job_id']==job)
    def status(state,**kw):feed.atomic(folder/'status.json',dict(job_id=job,title=row['recording']['title'],state=state,updated_unix=time.time(),**kw))
    try:
        with io.locked(folder):
            if plan['implementation']!=implementations():raise ValueError('runtime changed; explicit recovery required')
            if (folder/'completion.json').exists():return 'completed'
            if (folder/'hold.json').exists():return 'held'
            if STOP:return 'paused'
            receipt=read(folder/'submission.json') if (folder/'submission.json').exists() else None
            intent=read(folder/'intent.json') if (folder/'intent.json').exists() else None
            reservation=root/'reservations'/(job+'.json')
            if receipt is None and (intent or reservation.exists()):raise ValueError('uncertain paid POST; reconcile before retry')
            if receipt and (not intent or read(reservation)!=intent or intent['plan']!=ref):raise ValueError('paid receipt binding mismatch')
            key=env.api_key(row['provider'],env_file=env_file)
            if not key:raise ValueError('provider key missing')
            api=(client.AssemblyAIClient if row['provider']=='assemblyai' else client.RevAIClient)(key,timeout_seconds=900)
            if receipt is None:
                if prior_evidence(Path(plan['base']),row['recording'])!=row['prior_paid']:raise ValueError('prior paid evidence changed')
                if io.binding(plan['ffmpeg']['path'])!=plan['ffmpeg']:raise ValueError('decoder changed')
                status('screening_speech')
                screen=read(folder/'speech-screen.json') if (folder/'speech-screen.json').exists() else speech_screen(plan,row)
                io.put(folder/'speech-screen.json',screen)
                if not screen['positive']:status('no_speech_detected_in_probes');return 'no_speech_detected_in_probes'
                status('preparing_audio');audio=media.prepare(row['recording'],folder,plan['ffmpeg'])
                status('preparing_flac');transport=upload.prepare(audio,folder,plan['ffmpeg'])
                limit=client.ASSEMBLYAI_MAX_UPLOAD_BYTES if row['provider']=='assemblyai' else client.REVAI_MAX_UPLOAD_BYTES
                if transport['byte_count']>limit:raise ValueError('FLAC exceeds provider limit')
                uploaded=None
                if row['provider']=='assemblyai':
                    status('uploading');uploaded=read(folder/'upload.json') if (folder/'upload.json').exists() else safe_call(api.upload,transport['path'],expected_sha256=transport['sha256'])
                    io.put(folder/'upload.json',uploaded)
                if STOP:status('paused');return 'paused'
                intent=dict(plan=ref,job_id=job,recording_id=row['recording']['recording_id'],provider=row['provider'],
                    audio=audio,transport=transport,diarization=row['diarization'],language=row['language'],maximum_cost_microusd=row['maximum_cost_microusd'])
                # Exclusive job lock prevents duplicates; preflight plan sum is the
                # batch budget bound, so reservations need no shared contention.
                io.put(reservation,intent);io.put(folder/'intent.json',intent);status('submitting')
                try:
                    receipt=api.submit(uploaded['upload_url'],diarization=row['diarization'],language=row['language']) if row['provider']=='assemblyai' else api.submit_file(transport['path'],expected_sha256=transport['sha256'],metadata=job+'_recovery_20260917',diarization=row['diarization'])
                except client.CloudClientError as error:
                    if error.response is not None:io.put(folder/'submission-untrusted-response.json',error.response)
                    raise
                io.put(folder/'submission.json',receipt)
            audio=intent['audio'];remote=client.validate_job(row['provider'],receipt)
            status('pending',provider=row['provider'],provider_job_id=remote['job_id'])
            deadline=time.monotonic()+24*3600
            while not STOP and time.monotonic()<deadline:
                terminal=read(folder/'terminal-job.json') if (folder/'terminal-job.json').exists() else safe_call(api.poll,remote['job_id'])
                checked=client.validate_job(row['provider'],terminal,expected_job_id=remote['job_id'])
                if checked['status'] not in {'completed','transcribed','error','failed'}:sleep(20);continue
                terminal_ref=io.put(folder/'terminal-job.json',terminal)
                if checked['status'] in {'error','failed'}:raise ValueError('provider failure retained; no automatic paid retry')
                raw=terminal if row['provider']=='assemblyai' else (read(folder/'provider-transcript.json') if (folder/'provider-transcript.json').exists() else safe_call(api.transcript,remote['job_id']))
                raw_ref=terminal_ref if row['provider']=='assemblyai' else io.put(folder/'provider-transcript.json',raw)
                normalized,labels=normalize(row,raw,terminal,audio)
                doc=dict(kind='himr_cloud_recording_transcript',schema_version=1,job_id=job,recording_id=row['recording']['recording_id'],
                    source_media=row['recording']['media'],status='completed',audio=audio,provider_job_id=remote['job_id'],
                    raw_result=raw_ref,provider_job=terminal_ref,whole_recording_submitted=True,machine_generated=True,
                    human_reviewed=False,full_media_coverage_verified=False,publication_authority=False,
                    speaker_identity_inferred=False,recovery_plan=ref,**normalized)
                transcript=io.put(folder/'transcript.json',doc)
                io.put(folder/'completion.json',dict(transcript=transcript,audio=audio,raw_result=raw_ref,plan=ref))
                status('completed',requires_speaker_review=labels>1,provider=row['provider'],provider_job_id=remote['job_id'])
                return 'completed'
            status('paused');return 'paused'
    except Exception as error:
        uncertain=(folder/'intent.json').exists() and not (folder/'submission.json').exists()
        if STOP and not uncertain:
            status('paused');return 'paused'
        state='reconciliation_required' if uncertain else 'needs_review'
        # Never print provider URLs, responses, credentials or source transcript text.
        reason=str(error) if isinstance(error,(ValueError,client.CloudClientError,media.MediaError)) else type(error).__name__
        io.put(folder/'hold.json',dict(plan=ref,state=state,reason=reason,automatic_paid_retry=False))
        status(state);return state


def run(ref,env_file,workers):
    plan=io.read(ref)
    if plan['kind']!=KIND or plan['implementation']!=implementations():raise ValueError('plan/runtime mismatch')
    if sum(r['maximum_cost_microusd'] for r in plan['recordings'])>plan['allocation_microusd']:raise ValueError('allocation exceeded')
    io.mkdir(Path(plan['state_root'])/'runner')
    with io.locked(Path(plan['state_root'])/'runner'):
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures=[pool.submit(process,ref,r['job_id'],env_file) for r in plan['recordings']]
            counts=Counter()
            for f in futures:
                counts[f.result()]+=1
                feed.atomic(Path(plan['state_root'])/'status.json',dict(counts=dict(counts),total=len(futures),updated_unix=time.time()))
        return dict(counts)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('command',choices=['prepare','run'])
    parser.add_argument('--base',type=Path);parser.add_argument('--preview',type=Path);parser.add_argument('--output',type=Path)
    parser.add_argument('--plan');parser.add_argument('--expected-sha256');parser.add_argument('--env-file')
    parser.add_argument('--allow-paid-api',action='store_true');parser.add_argument('--workers',type=int,default=4)
    a=parser.parse_args();os.umask(0o077)
    if a.command=='prepare':result=prepare(a.base,a.preview,a.output)
    else:
        if not a.allow_paid_api or not a.expected_sha256 or not 1<=a.workers<=4:parser.error('explicit paid opt-in, plan hash and 1–4 workers required')
        result=run(dict(path=a.plan,sha256=a.expected_sha256),a.env_file,a.workers)
    print(json.dumps(result))


if __name__=='__main__':main()
