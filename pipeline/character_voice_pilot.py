"""Title-led anonymous voice candidates, compared with provisional Daniel audio.

Names in titles are search leads only. This module never assigns a named
identity, performs face recognition, changes ASR or submits paid requests.
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
from pipeline.speaker_reference_eras import coverage
from pipeline.transcript_audio_review import binding, read_bound, write_json


def select(intervals, duration_ms, count=12):
    if not 1 <= count <= 12 or duration_ms < 5500:raise ValueError('invalid probe bound')
    chosen=[]
    for i in range(count):
        target=int((i+.5)*duration_ms/count)
        candidates=[int(a) for a,b in intervals if abs(a-target)<=30000 and b>a]
        candidates=[x for x in candidates if 250<=x and x+5250<=duration_ms
                    and all(abs(x-p['start_ms'])>=5000 for p in chosen)]
        candidates=[(x,coverage(intervals,x,x+5000)) for x in candidates]
        candidates=[(x,c) for x,c in candidates if c>=.65]
        if candidates:
            start,score=min(candidates,key=lambda p:abs(p[0]-target))
            basis='nearby_dense_subtitle_intervals_not_word_confidence'
        else:
            start=max(250,min(duration_ms-5250,target-2500));score=None
            basis='uniform_audio_only_probe_no_transcript_claim'
        if any(abs(start-p['start_ms'])<5000 for p in chosen):continue
        chosen.append({'start_ms':start,'end_ms':start+5000,'selection_basis':basis,'subtitle_coverage':score})
    return chosen


def prepare(campaign_ref, selections, output, model_path, vad_ref):
    if not 1<=len(selections)<=4:raise ValueError('one to four bounded title leads required')
    campaign=read_bound(campaign_ref);mapping={r['job_id']:r for r in campaign['recordings']}
    root=Path(output).resolve();root.mkdir(mode=0o700,parents=True,exist_ok=False)
    model=binding(model_path)
    if model['sha256']!=embedding.MODEL_SHA:raise ValueError('wrong TitaNet model')
    ffmpeg=binding(Path(shutil.which('ffmpeg')).resolve());sources=[];clips=[]
    for job_id,name in selections:
        row=mapping[job_id];rec=row['recording'];media=rec['media'];source=Path(media['path'])
        before=source.stat()
        if before.st_size!=media['byte_count'] or rec['audio_state']!='audio_present':raise ValueError('media not admitted for sampling')
        ref=row['import']['transcript'] if row['disposition']=='third_party' else None
        doc=read_bound(ref) if ref else None
        if doc and doc['recording_id']!=rec['recording_id']:raise ValueError('transcript identity mismatch')
        intervals=[(s['start_ms'],s['end_ms']) for s in doc['segments']] if doc else []
        sources.append({'job_id':job_id,'recording_id':rec['recording_id'],'title':rec['title'],
                        'title_name_lead':name,'media':media,'transcript':ref,
                        'original_disposition':row['disposition'],'original_reason':row['reason'],
                        'identity_verified':False})
        for c in select(intervals,rec['duration_ms']):
            path=root/f'clip-{len(clips):03d}.wav'
            subprocess.run([ffmpeg['path'],'-nostdin','-v','error','-threads','1','-protocol_whitelist','file,pipe',
                '-ss',str(c['start_ms']/1000),'-i',str(source),'-t','5','-vn','-ac','1','-ar','16000',
                '-af','aresample=16000,atrim=end_sample=80000','-c:a','pcm_s16le','-n',str(path)],
                check=True,timeout=60,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            with wave.open(str(path)) as wav:
                n=wav.getnframes()
                if (wav.getnchannels(),wav.getsampwidth(),wav.getframerate())!=(1,2,16000) or not 79000<=n<=80000:
                    raise ValueError('probe exceeds decode tolerance')
            clips.append({**c,'recording_id':rec['recording_id'],'title':rec['title'],
                          'title_name_lead':name,'transcript':ref,'clip':binding(path),
                          'decoded_duration_ms':n/16,'provider_label':'UNDIARIZED_PROBE',
                          'segment_index':None,'identity':None})
        after=source.stat()
        witness=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        if witness(before)!=witness(after):raise ValueError('media changed during sampling')
    plan={'kind':'himr_titanet_embedding_pilot_plan','implementation':binding(embedding.__file__),
          'selection_implementation':binding(__file__),'campaign':campaign_ref,'model':model,
          'model_revision':embedding.MODEL_REVISION,'vad':vad_ref,'ffmpeg':ffmpeg,'sources':sources,
          'clips':clips,'skipped_labels':[],'production_eligible':False,'full_media_rehash':False,
          'single_voice_per_clip_verified':False}
    write_json(root/'plan.json',plan);return binding(root/'plan.json')


def score(rows, profiles):
    if not profiles or any('embedding' not in p for p in profiles):raise ValueError('missing reference profiles')
    results=[]
    for i,r in enumerate(rows):
        if r.get('embedding') is None:continue
        scores=[{'era':p['era'],'cosine':embedding.cosine(r['embedding'],p['embedding'])} for p in profiles]
        maximum=max(s['cosine'] for s in scores);median=statistics.median(s['cosine'] for s in scores)
        # Routing hint only; this is NOT a calibrated identity decision.
        strong=maximum>=.70 and median>=.55
        results.append({'row_index':i,'recording_id':r['recording_id'],'title_name_lead':r['title_name_lead'],
                        'clip':r['clip'],'start_ms':r['start_ms'],'end_ms':r['end_ms'],
                        'daniel_reference_scores':scores,'maximum_cosine':maximum,'median_cosine':median,
                        'reference_similarity_hint':'strong' if strong else 'weak_or_conflicting',
                        'identity':None,'decision':'unknown_unverified_references'})
    return results


def cluster(rows, threshold=.65):
    """Complete-link provisional acoustic groups, never named identities."""
    groups=[]
    for i,r in enumerate(rows):
        if r.get('embedding') is None:continue
        compatible=[g for g in groups if all(embedding.cosine(r['embedding'],rows[j]['embedding'])>=threshold for j in g)]
        if compatible:compatible[0].append(i)
        else:groups.append([i])
    return [{'anonymous_group':f'candidate-{i+1:03d}','row_indices':g,
             'recording_count':len({rows[j]['recording_id'] for j in g}),
             'title_name_leads':sorted({rows[j]['title_name_lead'] for j in g}),
             'identity':None,'validated_single_speaker':False} for i,g in enumerate(groups)]


def compare(result_ref, reference_ref, output):
    result=read_bound(result_ref);reference=read_bound(reference_ref)
    reference_result=read_bound(reference['embeddings'])
    plan=read_bound(result['plan']);reference_plan=read_bound(reference_result['plan'])
    if plan['model']!=reference_plan['model'] or plan['model_revision']!=reference_plan['model_revision']:
        raise ValueError('embedding models do not match')
    value={'kind':'himr_title_voice_candidate_review','embeddings':result_ref,'daniel_reference':reference_ref,
           'plan':result['plan'],'implementation':binding(__file__),'matches':score(result['rows'],reference['era_profiles']),
           'anonymous_groups':cluster(result['rows']), 'similarity_thresholds_calibrated':False,
           'name_from_title_assignment':False,'face_identity_matching':False,
           'production_eligible':False,'automatic_labeling_enabled':False,
           'negative_match_does_not_identify_title_character':True}
    write_json(output,value);return binding(output)


def frames(review_ref, output):
    review=read_bound(review_ref);plan=read_bound(review['plan']);sources={s['recording_id']:s for s in plan['sources']}
    grouped=defaultdict(list)
    for r in review['matches']:grouped[r['recording_id']].append(r)
    root=Path(output);root.mkdir(mode=0o700,parents=True,exist_ok=False);frames=[]
    for recording,rows in grouped.items():
        # Inspect high and low reference-similarity examples after audio scoring.
        choices=[max(rows,key=lambda r:r['maximum_cosine']),min(rows,key=lambda r:r['maximum_cosine'])]
        seen=set()
        for row in choices:
            if row['row_index'] in seen:continue
            seen.add(row['row_index']);source=sources[recording]['media'];before=Path(source['path']).stat()
            if before.st_size!=source['byte_count']:raise ValueError('frame source size changed')
            path=root/f'probe-{row["row_index"]:03d}.jpg'
            subprocess.run([plan['ffmpeg']['path'],'-nostdin','-v','error','-threads','1','-protocol_whitelist','file,pipe',
                '-ss',str((row['start_ms']+2500)/1000),'-i',source['path'],'-frames:v','1','-vf','scale=640:-2',
                '-n',str(path)],check=True,timeout=60,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            after=Path(source['path']).stat()
            if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
                raise ValueError('frame source changed')
            if path.exists():frames.append({'row_index':row['row_index'],'recording_id':recording,
                'title':sources[recording]['title'],'frame':binding(path),'maximum_cosine':row['maximum_cosine'],
                'identity':None,'visible_person_count':None,'scene_review_pending':True})
    value={'kind':'himr_post_audio_scene_review','review':review_ref,'frames':frames,'face_identification':False}
    write_json(root/'frames.json',value);return binding(root/'frames.json')


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='mode',required=True)
    s=sub.add_parser('prepare');s.add_argument('--campaign',required=True);s.add_argument('--selection',action='append',required=True)
    s.add_argument('--output',required=True);s.add_argument('--model',required=True);s.add_argument('--vad-config',required=True)
    s=sub.add_parser('compare');s.add_argument('--embeddings',required=True);s.add_argument('--reference',required=True);s.add_argument('--output',required=True)
    s=sub.add_parser('frames');s.add_argument('--review',required=True);s.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    if a.mode=='prepare':
        result=prepare(binding(a.campaign),[v.split(':',1) for v in a.selection],a.output,a.model,read_bound(binding(a.vad_config))['silero_vad'])
    elif a.mode=='compare':result=compare(binding(a.embeddings),binding(a.reference),a.output)
    else:result=frames(binding(a.review),a.output)
    print(json.dumps(result),flush=True)


if __name__=='__main__':main()
