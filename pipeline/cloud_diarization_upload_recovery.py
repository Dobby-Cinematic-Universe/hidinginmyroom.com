"""Sequential, observable blob-upload recovery for three explicitly held jobs.

Uses the unchanged pinned batch to reserve/submit/collect. Only unbillable
AssemblyAI uploads are retried here. No completed job or paid intent is reset.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

KIND = 'himr_targeted_diarization_upload_recovery'
JOBS = frozenset({
    'cloudjob_ce5fc02f115ba410007ffca6f3e86fd7',
    'cloudjob_04ccdf056594dc64c92acc5816863208',
    'cloudjob_002dcd70e78a489b1d3bdd0fc126ffff',
})
STOP = False


def bound_read(ref):
    raw = Path(ref['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != ref['sha256']:
        raise RuntimeError('recovery binding changed')
    return json.loads(raw)


def bootstrap(ref):
    manifest = bound_read(ref)
    if (manifest['kind'] != KIND or {r['job_id'] for r in manifest['jobs']} != JOBS
            or len(manifest['jobs']) != 3 or manifest['paid_retries_authorized'] is not False):
        raise RuntimeError('recovery is restricted to the three failed uploads')
    implementation = manifest['implementation']
    if (Path(implementation['path']).resolve() != Path(__file__).resolve()
            or hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != implementation['sha256']):
        raise RuntimeError('recovery implementation changed')
    bound_read(manifest['runtime_release'])
    sys.path.insert(0, manifest['runtime'])
    from pipeline import cloud_diarization_batch as batch
    if Path(batch.__file__).resolve().parent.parent != Path(manifest['runtime']).resolve():
        raise RuntimeError('wrong batch runtime imported')
    plan = batch.load_plan(manifest['plan'])
    return manifest, plan, batch


def eligible_for_blob_retry(folder, reservations, job):
    evidence = ('intent.json', 'submission.json', 'reconciled.json',
                'completion.json', 'terminal-job.json', 'provider-transcript.json',
                'submission-untrusted-response.json')
    return (not any((folder/name).exists() for name in evidence)
            and not (reservations/(job+'.json')).exists())


def stop(*_):
    global STOP
    STOP = True


def release_upload_hold(folder, expected, io):
    """Retain the exact original hold; never clear a later paid/validation hold."""
    hold, retained = folder/'hold.json', folder/'hold-resolved-upload-recovery-20260915.json'
    if retained.exists():
        if io.read(io.binding(retained)) != expected:
            raise RuntimeError('retained upload hold differs')
        return
    if not hold.exists() or io.read(io.binding(hold)) != expected:
        raise RuntimeError('only the approved original upload hold may be released')
    os.rename(hold, retained)
    with io.paths.retained_directory(folder) as fd:
        os.fsync(fd)


def upload_one(manifest, plan, selected, batch, env_file):
    io, client, feed = batch.io, batch.clients, batch.feed
    job = selected['job_id']; folder=Path(plan['state_root'])/'jobs'/job
    root=Path(manifest['state_root']); report=root/(job+'-progress.json')
    def progress(state, **extra):
        value=dict(job_id=job,state=state,updated_unix=time.time(),**extra)
        feed.atomic(report,value);print(json.dumps(value),flush=True)
    with io.locked(folder):
        if (folder/'completion.json').exists():return 'completed'
        if not eligible_for_blob_retry(folder,Path(plan['state_root'])/'reservations',job):
            # Existing paid receipts can be collected by the unchanged runner;
            # an ambiguous paid request remains reconciliation-only.
            return 'collect_existing'
        row=next(r for r in plan['recordings'] if r['job_id']==job)
        batch.assert_no_prior_paid([row],plan['prior_plans'])
        original=selected['original_hold']
        if (original['plan']!=manifest['plan'] or original['job_id']!=job
                or original['reason']!='cloud request transport failed'):
            raise RuntimeError('wrong original upload failure')
        if not (folder/'upload.json').exists():
            saved=io.read(selected['flac_receipt']); audio=io.read(selected['audio_receipt'])
            if (saved['audio']!=audio['audio'] or audio['source']!=row['recording']['media']
                    or saved['ffmpeg']!=plan['ffmpeg'] or saved['lossless_verified'] is not True):
                raise RuntimeError('retained whole-file FLAC binding differs')
            transport=saved['transport']
            if Path(transport['path'])!=folder/'audio.flac':
                raise RuntimeError('wrong retained upload path')
            attempts=sorted(root.glob(job+'-upload-attempt-*.json'))
            if len(attempts)>=2:return 'upload_attempts_exhausted'
            for attempt in range(len(attempts)+1,3):
                if STOP:return 'paused'
                started=time.monotonic(); last=[0.0]
                def uploaded(sent):
                    now=time.monotonic()
                    if now-last[0]>=20 or sent==transport['byte_count']:
                        last[0]=now
                        progress('uploading',attempt=attempt,bytes_sent=sent,
                            total_bytes=transport['byte_count'],elapsed_seconds=round(now-started,1),
                            bytes_are_transport_progress_not_provider_acceptance=True)
                class Body(client._StreamingBody):
                    def __iter__(self):
                        sent=0
                        for block in super().__iter__():
                            if STOP:raise RuntimeError('recovery paused during unbillable upload')
                            yield block
                            sent+=len(block);uploaded(sent)
                api=client.AssemblyAIClient(batch.env.api_key('assemblyai',env_file=env_file),timeout_seconds=3600)
                progress('upload_starting',attempt=attempt,total_bytes=transport['byte_count'])
                try:
                    with client._upload_file(transport['path'],transport['sha256'],client.ASSEMBLYAI_MAX_UPLOAD_BYTES) as (fd,before):
                        body=Body(fd,before,transport['sha256'],path=transport['path'])
                        receipt=api._request('POST','/v2/upload',data=body,content_type='application/octet-stream')
                    # Preserve successful response before semantic validation.
                    io.put(folder/'upload.json',receipt)
                    client.validate_upload_url(receipt['upload_url'])
                    io.put(root/(job+f'-upload-attempt-{attempt:02d}.json'),
                        dict(state='uploaded',elapsed_seconds=round(time.monotonic()-started,1),
                             paid_request=False,receipt=io.binding(folder/'upload.json')))
                    progress('uploaded',attempt=attempt,total_bytes=transport['byte_count'])
                    break
                except client.CloudClientError as error:
                    if error.response is not None:
                        io.put(root/(job+f'-untrusted-upload-response-{attempt:02d}.json'),error.response)
                    event=dict(state='upload_failed',attempt=attempt,reason=str(error),
                        status_code=error.status_code,elapsed_seconds=round(time.monotonic()-started,1),
                        underlying_error_type=type(error.__context__).__name__ if error.__context__ else None,
                        paid_request=False)
                    io.put(root/(job+f'-upload-attempt-{attempt:02d}.json'),event);progress(**event)
                    if not batch.upload.transient(error) or attempt==2: return 'upload_failed'
                    for _ in range(30):
                        if STOP:return 'paused'
                        time.sleep(1)
            if not (folder/'upload.json').exists():return 'upload_failed'
        client.validate_upload_url(io.read(io.binding(folder/'upload.json'))['upload_url'])
        release_upload_hold(folder,original,io)
        progress('ready_for_first_paid_submission')
        return 'ready'


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=('run','collect'))
    for flag in ('manifest','expected-sha256','env-file'):p.add_argument('--'+flag,required=True)
    p.add_argument('--job');p.add_argument('--allow-paid-api',action='store_true')
    a=p.parse_args();os.umask(0o077)
    if not a.allow_paid_api:raise RuntimeError('explicit paid approval is required')
    ref=dict(path=a.manifest,sha256=a.expected_sha256);manifest,plan,batch=bootstrap(ref)
    if a.command=='collect':
        if a.job not in JOBS:raise RuntimeError('job outside targeted recovery')
        print(json.dumps(dict(event='collection_finished',job_id=a.job,
            state=batch.record(manifest['plan'],a.job,a.env_file))),flush=True);return
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    children=[];results={}
    with batch.io.locked(manifest['state_root']):
        for selected in manifest['jobs']:
            if STOP:break
            try:state=upload_one(manifest,plan,selected,batch,a.env_file)
            except Exception as error:
                state='local_recovery_error'
                print(json.dumps(dict(job_id=selected['job_id'],state=state,error_type=type(error).__name__)),flush=True)
            results[selected['job_id']]=state
            if state in {'ready','collect_existing'}:
                children.append(subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve()),
                    'collect','--manifest',a.manifest,'--expected-sha256',a.expected_sha256,
                    '--env-file',a.env_file,'--allow-paid-api','--job',selected['job_id']]))
        while any(child.poll() is None for child in children):
            if STOP:
                for child in children:
                    if child.poll() is None:child.terminate()
            time.sleep(1)
        final=batch.status(manifest['plan'])
        batch.feed.atomic(Path(plan['state_root'])/'status.json',final)
        batch.feed.atomic(Path(manifest['state_root'])/'result.json',dict(results=results,
            batch_states=final['states'],new_paid_retry_requests=0,child_exit_codes=[c.returncode for c in children]))
        print(json.dumps(dict(event='recovery_finished',states=final['states'])),flush=True)


if __name__=='__main__':main()
