"""User-reviewed audio enrollment and recording-held-out diagnostics only.

No face recognition, automatic production labeling, or promotion of earlier
unverified references. Mixed speech, laughter in mixed turns, and TTS are kept
as challenge examples, never averaged into named single-speaker profiles.
"""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import os
from pipeline.transcript_audio_review import binding, read_bound, write_json
from pipeline.titanet_embedding_pilot import cosine, unit


def enroll(data, annotations):
    seen=set();samples=[];challenges=[]
    for a in annotations:
        i=a['probe']
        if type(i) is not int or i in seen or not 0<=i<len(data['rows']):raise ValueError('duplicate/invalid probe')
        seen.add(i);row=data['rows'][i]
        if a['clip']!=row['clip']:raise ValueError('annotation clip binding mismatch')
        names=a['speakers'];kind=a['kind']
        if kind not in {'single_speaker','mixed_speakers','tts'}:raise ValueError('invalid annotation kind')
        if not isinstance(names,list) or len(names)!=len(set(names)) or any(not isinstance(n,str) or not n for n in names):raise ValueError('invalid speaker names')
        if ((kind=='single_speaker' and len(names)!=1) or (kind=='mixed_speakers' and len(names)<2)
                or (kind=='tts' and names)):raise ValueError('annotation kind and speakers disagree')
        item={'probe':i,'recording_id':row['recording_id'],'clip':row['clip'],
              'start_ms':row['start_ms'],'end_ms':row['end_ms'],'annotation':a,
              'embedding':unit(row['embedding']) if row.get('embedding') is not None else None}
        if kind=='single_speaker' and item['embedding'] is not None:
            samples.append({**item,'speaker':names[0]})
        else:challenges.append(item)
    by_name=defaultdict(list)
    for s in samples:by_name[s['speaker']].append(s)
    profiles=[]
    for name,rows in sorted(by_name.items()):
        by_record=defaultdict(list)
        for r in rows:by_record[r['recording_id']].append(r['embedding'])
        mean=lambda vectors:unit([sum(v[i] for v in vectors)/len(vectors) for i in range(192)])
        record_vectors=[mean(v) for v in by_record.values()]
        profiles.append({'speaker':name,'sample_count':len(rows),'recording_count':len(by_record),
                         'embedding':mean(record_vectors),'reference_probes':[r['probe'] for r in rows]})
    return {'kind':'himr_user_reviewed_voice_gallery','identity_authority':'user_labels_for_exact_audio_probes',
            'samples':samples,'profiles':profiles,'challenge_examples':challenges,
            'unreviewed_probes':sorted(set(range(len(data['rows'])))-seen),
            'old_provisional_references_promoted':False,'face_identity_matching':False,
            'production_eligible':False,'automatic_labeling_enabled':False}


def rank(vector, references):
    scores=defaultdict(list)
    for r in references:scores[r['speaker']].append(cosine(vector,r['embedding']))
    return sorted([{'speaker':name,'maximum_cosine':max(v)} for name,v in scores.items()],
                  key=lambda r:(-r['maximum_cosine'],r['speaker']))


def evaluate(gallery):
    trials=[]
    for query in gallery['samples']:
        refs=[s for s in gallery['samples'] if s['recording_id']!=query['recording_id']]
        ranked=rank(query['embedding'],refs)
        if query['speaker'] not in {r['speaker'] for r in ranked}:
            trials.append({'probe':query['probe'],'state':'no_other_recording_reference'});continue
        trials.append({'probe':query['probe'],'expected':query['speaker'],'ranking':ranked,
                       'top1_correct':ranked[0]['speaker']==query['speaker'],
                       'margin':ranked[0]['maximum_cosine']-ranked[1]['maximum_cosine'] if len(ranked)>1 else None,
                       'reference_probes':[s['probe'] for s in refs],
                       'identity':None,'state':'diagnostic_only_no_calibrated_threshold'})
    challenges=[]
    for q in gallery['challenge_examples']:
        if q['embedding'] is None:continue
        refs=[s for s in gallery['samples'] if s['recording_id']!=q['recording_id']]
        challenges.append({'probe':q['probe'],'annotation':q['annotation'],
                           'ranking':rank(q['embedding'],refs),'identity':None,
                           'decision':'withhold_mixed_or_tts','reference_probes':[s['probe'] for s in refs]})
    summary={}
    for name in sorted({s['speaker'] for s in gallery['samples']}):
        rows=[r for r in trials if r.get('expected')==name]
        summary[name]={'held_out_probes':len(rows),'nearest_label_correct':sum(r['top1_correct'] for r in rows)}
    return {'kind':'himr_reviewed_voice_holdout_diagnostics','per_speaker':summary,'trials':trials,
            'challenge_trials':challenges,'whole_query_recording_excluded':True,
            'threshold_calibrated':False,'production_accuracy_established':False,
            'automatic_labeling_enabled':False}


def build(embeddings_ref, annotations_ref, output):
    data=read_bound(embeddings_ref);review=read_bound(annotations_ref)
    if review['embeddings']!=embeddings_ref or review['authority']!='user_probe_labels':raise ValueError('review source mismatch')
    for a in review['annotations']:
        if binding(a['clip']['path'])!=a['clip']:raise ValueError('reviewed audio changed')
    root=Path(output);root.mkdir(mode=0o700,parents=True,exist_ok=False)
    gallery=enroll(data,review['annotations'])
    plan=read_bound(data['plan'])
    gallery.update(embeddings=embeddings_ref,annotations=annotations_ref,model=plan['model'],
                   model_revision=plan['model_revision'],implementation=binding(__file__))
    write_json(root/'gallery.json',gallery)
    diagnostics=evaluate(gallery);diagnostics['gallery']=binding(root/'gallery.json')
    write_json(root/'diagnostics.json',diagnostics)
    return {'gallery':binding(root/'gallery.json'),'diagnostics':binding(root/'diagnostics.json'),
            'profiles':[{k:p[k] for k in ('speaker','sample_count','recording_count')} for p in gallery['profiles']],
            'diagnostic_summary':diagnostics['per_speaker']}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--embeddings',required=True)
    p.add_argument('--annotations',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    print(json.dumps(build(binding(a.embeddings),binding(a.annotations),a.output)))
