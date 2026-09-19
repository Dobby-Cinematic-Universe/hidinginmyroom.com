"""Prepare isolated, timestamp-preserving audio repairs for three unpaid holds."""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import time
from pipeline import targeted_retranscription as lane
from pipeline import transcript_summary as io
from pipeline import cloud_transcription_media as media

TARGETS={'cloudjob_005541298d49c9c37bbdc7043b3c8032',
         'cloudjob_2b62407493b0a525b34e2b066b7405fc',
         'cloudjob_0a886e4aef62dd15ab5b79f153274c93'}


def prepare(source,output):
    source=source.resolve();output=output.resolve();old=lane.read(source)
    if output.exists():raise ValueError('fresh repair workspace required')
    if old['implementation']!=lane.implementations():raise ValueError('source runtime changed')
    rows=[];oldroot=Path(old['state_root']);base=Path(old['base'])
    originals=lane.read(base/'transcription-v5/plan.json')['recordings']
    for row in old['recordings']:
        if row['job_id'] not in TARGETS:continue
        folder=oldroot/'jobs'/row['job_id']
        if any((folder/f).exists() for f in lane.PAID) or (oldroot/'reservations'/(row['job_id']+'.json')).exists():
            raise ValueError('repair selection already has paid evidence')
        hold=lane.read(folder/'hold.json')
        if hold['state']!='needs_review' or 'audio' not in hold['reason']:raise ValueError('unexpected hold')
        fixed=deepcopy(row);fixed['prior_media_hold']=io.binding(folder/'hold.json')
        if row['job_id']=='cloudjob_0a886e4aef62dd15ab5b79f153274c93':
            alternate=next(r for r in originals if r['job_id']=='cloudjob_cd4bb1dcf87a8038dbd28931c0d14811')
            if not lane.related(row['recording'],alternate['recording']):raise ValueError('alternate source identity differs')
            fixed['replaces_failed_media_recording']=row['recording']['recording_id']
            fixed['recording']=alternate['recording'];fixed['job_id']=alternate['job_id']
            fixed['alternate_recordings']=[row['recording']['recording_id']]
            fixed['maximum_cost_microusd']=max(row['maximum_cost_microusd'],alternate['maximum_cost_microusd'])
        fixed['prior_paid']=lane.prior_evidence(base,fixed['recording'])
        if any(not p['retry_empty'] for p in fixed['prior_paid']):raise ValueError('prior paid work exists')
        rows.append(fixed)
    if len(rows)!=3:raise ValueError('expected exactly three unpaid holds')
    io.mkdir(output);io.mkdir(output/'jobs');io.mkdir(output/'reservations')
    plan=deepcopy(old);plan.update(state_root=str(output),recordings=rows,excluded=[],
        maximum_cost_microusd=sum(r['maximum_cost_microusd'] for r in rows),
        media_recovery_of=io.binding(source),media_recovery_implementation=io.binding(__file__))
    ref=io.put(output/'plan.json',plan)
    for row in rows:
        folder=output/'jobs'/row['job_id'];io.mkdir(folder);recording=row['recording']
        # Decode against actual packet timestamps, preserve gaps with zeros, and
        # retain a silent container tail. No text or speaker labels are inferred.
        filters=f"aresample=16000:async=1:first_pts=0,apad=whole_dur={recording['duration_ms']/1000}"
        try:
            with io.safe.opened(recording['media']['path']) as fd:
                before=io.safe.witness(fd)
                if before['st_size']!=recording['media']['byte_count'] or io.safe.hash_fd(fd,before['st_size'],time.monotonic()+900)!=recording['media']['sha256']:
                    raise ValueError('alternate/source media changed')
                args=[plan['ffmpeg']['path'],'-nostdin','-hide_banner','-loglevel','error','-xerror',
                    '-protocol_whitelist','file,pipe','-format_whitelist',media.FORMATS,'-threads','2',
                    '-i',f'/proc/self/fd/{fd}','-map','0:a:0','-vn','-sn','-dn','-map_metadata','-1',
                    '-filter_threads','1','-af',filters,'-t',str(recording['duration_ms']/1000),
                    '-ac','1','-ar','16000','-c:a','pcm_s16le','-f','wav','-n',str(folder/'audio.wav')]
                subprocess.run(args,pass_fds=(fd,),stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,timeout=300,check=True)
                if io.safe.witness(fd)!=before:raise ValueError('media changed during decode')
            audio=media.inspect_wav(folder/'audio.wav',recording['duration_ms'])
            recovery=dict(kind='timestamp_preserving_audio_gap_recovery',source_hold=row['prior_media_hold'],
                implementation=io.binding(__file__),filter=filters,silence_padding=True,
                timeline_gap_repair=True,original_media_modified=False,original_failed_decode_preserved=True)
            io.put(folder/'audio.json',dict(kind='himr_cloud_prepared_audio',schema_version=1,
                source=recording['media'],ffmpeg=plan['ffmpeg'],audio=audio,whole_recording=True,
                speech_filter=False,cuts=False,timeline_recovery=recovery))
        except Exception as error:
            io.put(folder/'hold.json',dict(plan=ref,state='needs_review',reason=type(error).__name__,automatic_paid_retry=False))
    return ref


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();os.umask(0o077);print(json.dumps(prepare(a.source,a.output)))
