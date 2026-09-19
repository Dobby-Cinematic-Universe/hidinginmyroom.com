"""Repair one unsubmitted clip whose valid audio begins after video time zero."""
import argparse
from decimal import Decimal, ROUND_HALF_UP
import json
import os
from pathlib import Path
import subprocess
import wave
from pipeline import transcript_summary as io
from pipeline import cloud_transcription_media as media
from pipeline.transcript_audio_review import binding, read_bound, write_json

JOB='cloudjob_e6d83121a81787752c670c1d4ad91f93'
SOURCE_SHA='99576cc6e056685292af4f12dc102da54d83a0efd062aebe4b74019a1eba6d5b'


def silence_samples(offset):
    offset=Decimal(offset)
    if not offset.is_finite() or not 0<offset<=10:
        raise ValueError('unsupported start offset')
    return int((offset*16000).to_integral_value(rounding=ROUND_HALF_UP))


def run(plan_path):
    plan_ref=binding(plan_path);plan=read_bound(plan_ref);root=Path(plan['state_root'])
    row=next(r for r in plan['recordings'] if r['job_id']==JOB);rec=row['recording']
    folder=root/'jobs'/JOB
    with io.locked(root):
        for name in ('audio.json','intent.json','submission.json','upload.json','completion.json','reconciled.json'):
            if (folder/name).exists():raise ValueError('target already prepared or submitted')
        if (root/'reservations'/(JOB+'.json')).exists():raise ValueError('target has paid reservation')
        source=rec['media'];ffmpeg=plan['ffmpeg']
        if source['sha256']!=SOURCE_SHA or binding(source['path'])['sha256']!=SOURCE_SHA:
            raise ValueError('target media changed')
        if binding(ffmpeg['path'])!=ffmpeg:raise ValueError('decoder changed')
        if Path(source['path']).stat().st_size!=source['byte_count']:raise ValueError('source size changed')
        probe=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_entries',
            'format=duration:stream=codec_type,start_time,duration','-of','json',source['path']],timeout=30))
        streams=[s for s in probe['streams'] if s['codec_type']=='audio']
        if len(streams)!=1:raise ValueError('ambiguous audio stream')
        audio_stream=streams[0];padding=silence_samples(audio_stream['start_time'])
        original=folder/'audio.wav';original_ref=binding(original)
        with wave.open(str(original)) as wav:
            if (wav.getnchannels(),wav.getsampwidth(),wav.getframerate())!=(1,2,16000):raise ValueError('invalid decoded format')
            pcm=wav.readframes(wav.getnframes())
        decoded=subprocess.check_output([ffmpeg['path'],'-nostdin','-v','error','-xerror',
            '-protocol_whitelist','file,pipe','-threads','2','-i',source['path'],'-map','0:a:0',
            '-vn','-sn','-dn','-filter_threads','1','-ac','1','-ar','16000','-c:a','pcm_s16le',
            '-f','s16le','pipe:1'],timeout=60)
        if pcm!=decoded:raise ValueError('retained audio differs from complete fresh decode')
        if abs(len(pcm)/32000-float(audio_stream['duration']))>.01:raise ValueError('audio duration differs')
        if abs((len(pcm)//2+padding)/16-rec['duration_ms'])>2:raise ValueError('offset does not explain duration')
        repair=folder/'start-offset-repair-v1';repair.mkdir(mode=0o700,exist_ok=False)
        aligned=repair/'aligned.wav'
        with wave.open(str(aligned),'wb') as wav:
            wav.setnchannels(1);wav.setsampwidth(2);wav.setframerate(16000)
            wav.writeframes(b'\x00\x00'*padding+pcm)
        media.inspect_wav(aligned,rec['duration_ms'])
        with wave.open(str(aligned)) as wav:
            if wav.readframes(padding)!=b'\x00\x00'*padding or wav.readframes(len(pcm)//2)!=pcm:
                raise ValueError('aligned PCM verification failed')
        if binding(original)!=original_ref:raise ValueError('original decode changed')
        write_json(repair/'repair.json',dict(kind='himr_audio_start_offset_repair',plan=plan_ref,
            job_id=JOB,source_media=source,ffmpeg=ffmpeg,probe=probe,
            original_decoded_audio=original_ref,preserved_original_path=str(repair/'original-decoded.wav'),
            padding_samples=padding,padding_ms=padding/16,
            operation='restore_silent_video_timeline_prefix',speech_samples_changed=False,
            new_paid_requests=0,implementation=binding(__file__)))
        original.rename(repair/'original-decoded.wav');aligned.rename(original)
        audio=media.inspect_wav(original,rec['duration_ms'])
        io.put(folder/'audio.json',dict(kind='himr_cloud_prepared_audio',schema_version=1,
            source=source,ffmpeg=ffmpeg,audio=audio,whole_recording=True,speech_filter=False,cuts=False))
        if media.prepare(rec,folder,ffmpeg)!=audio:raise ValueError('normal preparation receipt replay failed')
        return dict(job_id=JOB,padding_ms=padding/16,prepared_duration_ms=audio['duration_ms'],
            original_audio_preserved=True,speech_pcm_unchanged=True,new_paid_requests=0)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--plan',required=True)
    a=p.parse_args();os.umask(0o077);print(json.dumps(run(a.plan)))
