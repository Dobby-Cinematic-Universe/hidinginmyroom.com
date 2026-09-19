"""Private, provisional multi-era voice-reference pilot. No named enrollment.

Titles and transcript self-introductions nominate candidates, never verify
identity. Frames are evidence for scene review, never face identification.
"""
import argparse
from collections import defaultdict
import itertools
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import wave

from pipeline import titanet_embedding_dense_pilot as embedding
from pipeline.transcript_audio_review import binding, read_bound, write_json


def era(date):
    year = int(date[:4])
    if 2015 <= year <= 2018: return '2015-2018'
    if 2019 <= year <= 2022: return '2019-2022'
    if 2023 <= year <= 2026: return '2023-2026'
    raise ValueError('date outside pilot eras')


def coverage(intervals, start, end):
    total, last = 0, start
    for a,b in sorted(intervals):
        left,right = max(a,start,last),min(b,end)
        if right > left:
            total += right-left
            last = right
    return total/(end-start)


def windows(intervals, duration_ms):
    # The first 15 minutes only: bounded reference search, not a solo claim.
    limit=min(duration_ms,900000)
    candidates=[]
    for a,_ in intervals:
        start=max(250,int(a))
        if start+5250>limit:continue
        score=coverage(intervals,start,start+5000)
        if score>=.7:candidates.append((start,score))
    chosen=[]
    for fraction in (.15,.5,.85):
        eligible=[x for x in candidates if all(abs(x[0]-p['start_ms'])>=10000 for p in chosen)]
        if not eligible:break
        target=fraction*limit
        start,score=min(eligible,key=lambda x:(abs(x[0]-target),-x[1]))
        chosen.append({'start_ms':start,'end_ms':start+5000,'timed_text_coverage':score})
    return sorted(chosen,key=lambda x:x['start_ms'])


def prepare(campaign_ref, job_ids, output, model_path, vad_ref):
    if not 2 <= len(set(job_ids)) == len(job_ids) <= 8:
        raise ValueError('need two to eight distinct explicit recordings')
    campaign=read_bound(campaign_ref)
    selected={r['job_id']:r for r in campaign['recordings'] if r['job_id'] in job_ids}
    if set(selected)!=set(job_ids):raise ValueError('unknown recording')
    model=binding(model_path)
    if model['sha256']!=embedding.MODEL_SHA:raise ValueError('wrong model')
    output=Path(output).resolve();output.mkdir(mode=0o700,parents=True,exist_ok=False)
    ffmpeg=binding(Path(shutil.which('ffmpeg')).resolve())
    rows,sources=[],[]
    for job_id in job_ids:
        row=selected[job_id];rec=row['recording'];date=rec['date']['value'];period=era(date)
        if row['disposition']=='third_party':
            ref=row['import']['transcript'];doc=read_bound(ref)
            intervals=[(s['start_ms'],s['end_ms']) for s in doc['segments']]
            basis='third_party_subtitle_intervals_not_word_confidence'
            raw_ref=None
        elif row['disposition']=='cloud':
            ref=binding(Path(campaign['state_root'])/'jobs'/job_id/'transcript.json');doc=read_bound(ref)
            raw_ref=doc['raw_result'];raw=read_bound(raw_ref)
            intervals=[(w['start'],w['end']) for w in raw['words'] if w.get('confidence',0)>=.8]
            basis='raw_word_intervals_confidence_at_least_08'
        else:raise ValueError('local ASR and review-only inputs excluded')
        if doc['recording_id']!=rec['recording_id']:raise ValueError('recording mismatch')
        media=rec['media'];source=Path(media['path']);before=source.stat()
        if before.st_size!=media['byte_count']:raise ValueError('media size changed')
        self_introductions=[{k:s[k] for k in ('start_ms','end_ms','text')} for s in doc['segments']
                            if 'my name is daniel' in s['text'].lower()]
        source_entry={'recording_id':rec['recording_id'],'job_id':job_id,'title':rec['title'],
                      'date':rec['date'],'era':period,'media':media,'transcript':ref,
                      'raw_result':raw_ref,'selection_basis':basis,
                      'self_introduction_transcript_leads':self_introductions,
                      'identity_verified':False,'requested_name':'Daniel'}
        chosen=windows(intervals,rec['duration_ms'])
        source_entry['sample_count']=len(chosen)
        for ordinal,c in enumerate(chosen):
            path=output/f'clip-{len(rows):03d}.wav'
            args=[ffmpeg['path'],'-nostdin','-v','error','-threads','1','-protocol_whitelist','file,pipe',
                  '-ss',str(c['start_ms']/1000),'-i',str(source)]
            subprocess.run(args+['-t','5','-vn','-ac','1','-ar','16000','-af',
                'aresample=16000,atrim=end_sample=80000','-c:a','pcm_s16le','-n',str(path)],
                check=True,timeout=60,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            with wave.open(str(path)) as wav:
                frames=wav.getnframes()
                if (wav.getnchannels(),wav.getsampwidth(),wav.getframerate())!=(1,2,16000) or not 79000<=frames<=80000:
                    raise ValueError('clip outside decode tolerance')
            rows.append({**c,'segment_index':None,'provider_label':'UNVERIFIED_REFERENCE_CANDIDATE',
                         'recording_id':rec['recording_id'],'era':period,'title':rec['title'],
                         'transcript':ref,'clip':binding(path),'decoded_duration_ms':frames/16})
            if ordinal==0:
                frame=output/f'frame-{job_id}.jpg'
                subprocess.run(args+['-frames:v','1','-vf','scale=640:-2','-n',str(frame)],
                               check=True,timeout=60,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
                if frame.exists():source_entry['scene_frame']=binding(frame)
        after=source.stat()
        witness=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        if witness(before)!=witness(after):raise ValueError('media changed during read')
        read_bound(ref);sources.append(source_entry)
    plan={'kind':'himr_titanet_embedding_pilot_plan','implementation':binding(embedding.__file__),
          'selection_implementation':binding(__file__),'campaign':campaign_ref,
          'model':model,'model_revision':embedding.MODEL_REVISION,'vad':vad_ref,'ffmpeg':ffmpeg,
          'sources':sources,'clips':rows,'skipped_labels':[],'requested_name':'Daniel',
          'identity_verified':False,'production_eligible':False,'full_media_rehash':False}
    write_json(output/'plan.json',plan)
    return binding(output/'plan.json')


def profile(rows, sources):
    """Equal recording weights per era; every holdout excludes its entire video."""
    by_record=defaultdict(list)
    for r in rows:
        if r.get('embedding') is not None:by_record[r['recording_id']].append(embedding.unit(r['embedding']))
    info={s['recording_id']:s for s in sources}
    average=lambda vectors:embedding.unit([sum(v[i] for v in vectors)/len(vectors) for i in range(192)])
    recordings=[];periods=defaultdict(list)
    for recording_id,vectors in by_record.items():
        scores=[embedding.cosine(a,b) for a,b in itertools.combinations(vectors,2)]
        entry={'recording_id':recording_id,'era':info[recording_id]['era'],'title':info[recording_id]['title'],
               'sample_count':len(vectors),'embedding':average(vectors),
               'within_recording_minimum':min(scores) if scores else None,
               'within_recording_median':statistics.median(scores) if scores else None}
        recordings.append(entry);periods[entry['era']].append(entry['embedding'])
    eras=[{'era':e,'recording_count':len(v),'embedding':average(v)} for e,v in sorted(periods.items())]
    heldout=[]
    for query in recordings:
        for period in sorted(periods):
            reference=[r for r in recordings if r['era']==period and r['recording_id']!=query['recording_id']]
            if not reference:continue
            scores=[embedding.cosine(query['embedding'],r['embedding']) for r in reference]
            heldout.append({'query_recording_id':query['recording_id'],'query_era':query['era'],
                            'reference_era':period,'reference_recording_ids':[r['recording_id'] for r in reference],
                            'median_cosine':statistics.median(scores),'minimum_cosine':min(scores),
                            'identity':None,'decision':'unknown_unverified_references'})
    return {'requested_name':'Daniel','status':'provisional_not_enrolled','identity_verified':False,
            'production_eligible':False,'automatic_labeling_enabled':False,'recording_profiles':recordings,
            'era_profiles':eras,'held_out_recording_scores':heldout,
            'calibration':{'verified_positive_examples':0,'verified_negative_examples':0,
                           'threshold':None,'false_accept_rate_established':False}}


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='mode',required=True)
    s=sub.add_parser('prepare');s.add_argument('--campaign',required=True);s.add_argument('--job',action='append',required=True)
    s.add_argument('--output',required=True);s.add_argument('--model',required=True);s.add_argument('--vad-config',required=True)
    s=sub.add_parser('profile');s.add_argument('--embeddings',required=True);s.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    if a.mode=='prepare':
        result=prepare(binding(a.campaign),a.job,a.output,a.model,read_bound(binding(a.vad_config))['silero_vad'])
    else:
        ref=binding(a.embeddings);data=read_bound(ref);plan=read_bound(data['plan'])
        result=profile(data['rows'],plan['sources']);result.update(embeddings=ref,plan=data['plan'],implementation=binding(__file__))
        write_json(a.output,result);result=binding(a.output)
    print(json.dumps(result),flush=True)


if __name__=='__main__':main()
