"""Separate bounded local TitaNet pilot. Never assigns identities or edits ASR.

The official NVIDIA model is pinned to a reviewed repository revision and file
digest. Comparisons are raw cosine similarities, not calibrated probabilities.
"""
import argparse
from collections import defaultdict
import hashlib
from importlib import metadata
import itertools
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time
import wave

from pipeline.transcript_audio_review import binding, read_bound, write_json, analyze

MODEL_SHA = 'e838520693f269e7984f55bc8eb3c2d60ccf246bf4b896d4be9bcabe3e4b0fe3'
MODEL_REVISION = '0dc382f40121a5fbd34db10a2bb04d826c2be6a8'


def select(segments, utterances):
    """Three spread 5s excerpts per label, guided by dense confident raw words."""
    labels = list(dict.fromkeys(s['speaker'] for s in segments))
    if len(labels) > 12:
        raise ValueError('pilot supports at most twelve provider labels')
    result = []
    for label in labels:
        candidates = []
        for i, s in enumerate(segments):
            if s['speaker'] != label or s['end_ms'] - s['start_ms'] < 5500:
                continue
            words = utterances[i].get('words') or []
            # At most 40 positions per utterance. Do not write word timestamps
            # into transcript projections or Gemini inputs.
            indices = sorted({round(k*(len(words)-1)/39) for k in range(40)}) if words else []
            for index in indices:
                start = max(s['start_ms']+250, min(s['end_ms']-5250, words[index]['start']))
                end = start+5000
                if any(j != i and other['start_ms'] < end and start < other['end_ms']
                       for j, other in enumerate(segments)):
                    continue
                covered, confidence_sum, last = 0, 0, start
                for word in words:
                    left=max(start,last,word['start']);right=min(end,word['end'])
                    if right > left:
                        covered+=right-left
                        confidence_sum+=(right-left)*word['confidence']
                        last=right
                if covered < 3000 or confidence_sum/covered < .8:
                    continue
                candidates.append({'segment_index':i,'provider_label':label,
                                   'start_ms':start,'end_ms':end,
                                   'word_coverage_fraction':covered/5000,
                                   'duration_weighted_word_confidence':confidence_sum/covered})
        candidates.sort(key=lambda c: c['start_ms'])
        indices = sorted({round(i*(len(candidates)-1)/2) for i in range(3)}) if candidates else []
        for i in indices:
            c = candidates[i]
            if not any(c['start_ms'] < p['end_ms'] and p['start_ms'] < c['end_ms'] for p in result):
                result.append(c)
    return result


def unit(vector):
    if len(vector) != 192 or any(type(v) not in (int, float) or not math.isfinite(v) for v in vector):
        raise ValueError('expected a finite 192D embedding')
    norm = math.hypot(*vector)
    if norm < 1e-12:
        raise ValueError('zero embedding')
    return [v/norm for v in vector]


def cosine(a, b):
    return max(-1.0, min(1.0, sum(x*y for x,y in zip(unit(a), unit(b)))))


def comparisons(rows):
    groups = defaultdict(list)
    for r in rows:
        if r.get('embedding') is not None:
            groups[(r['recording_id'], r['provider_label'])].append(r)
    result = []
    for a, b in itertools.combinations_with_replacement(sorted(groups), 2):
        if a[0] != b[0]:
            continue
        pairs = list(itertools.combinations(groups[a], 2)) if a == b else list(itertools.product(groups[a], groups[b]))
        scores = [cosine(x['embedding'],y['embedding']) for x,y in pairs]
        if scores:
            result.append({'recording_id': a[0], 'labels': [a[1],b[1]],
                           'same_provider_label': a == b, 'pair_count': len(scores),
                           'minimum': min(scores), 'median': statistics.median(scores), 'maximum': max(scores)})
    return result


def prepare(transcripts, output, model_path, vad_ref):
    if not 1 <= len(transcripts) <= 4:
        raise ValueError('pilot requires one to four explicit transcripts')
    model = binding(model_path)
    if model['sha256'] != MODEL_SHA:
        raise ValueError('not the pinned NVIDIA TitaNet checkpoint')
    output = Path(output).resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    ffmpeg = binding(Path(shutil.which('ffmpeg')).resolve())
    rows, sources, skipped = [], [], []
    for transcript in transcripts:
        ref = binding(transcript)
        doc = read_bound(ref)
        if doc.get('status') != 'completed' or not doc.get('diarization_requested'):
            raise ValueError('pilot needs completed diarized cloud input')
        raw = read_bound(doc['raw_result'])
        analyze(doc, raw)  # exact raw/normalized alignment and label mapping
        selected = select(doc['segments'], raw['utterances'])
        media = doc['source_media']; source = Path(media['path'])
        before = source.stat()
        if before.st_size != media['byte_count']:
            raise ValueError('source size changed')
        sources.append({'transcript': ref, 'source_media': media, 'raw_result': doc['raw_result']})
        for label in dict.fromkeys(s['speaker'] for s in doc['segments']):
            if not any(c['provider_label'] == label for c in selected):
                skipped.append({'recording_id': doc['recording_id'], 'provider_label': label,
                                'reason': 'no_dense_confident_nonoverlapping_5s_excerpt_with_turn_guards'})
        for c in selected:
            if len(rows) >= 48:
                raise ValueError('pilot exceeds 48 clip bound')
            path = output/f'clip-{len(rows):03d}.wav'
            subprocess.run([ffmpeg['path'], '-nostdin', '-v', 'error', '-threads', '1',
                            '-protocol_whitelist', 'file,pipe', '-ss', str(c['start_ms']/1000),
                            '-i', str(source), '-t', '5', '-vn', '-ac', '1', '-ar', '16000',
                            '-af', 'aresample=16000,atrim=end_sample=80000',
                            '-c:a', 'pcm_s16le', '-n', str(path)], check=True, timeout=60,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            with wave.open(str(path)) as wav:
                frames = wav.getnframes()
                if (wav.getnchannels(),wav.getsampwidth(),wav.getframerate()) != (1,2,16000) or not 79000 <= frames <= 80000:
                    raise ValueError('clip exceeds five-second PCM decode tolerance')
            rows.append({**c, 'recording_id': doc['recording_id'], 'title': doc.get('title'),
                         'transcript': ref, 'clip': binding(path), 'decoded_duration_ms': frames/16})
        after = source.stat()
        witness = lambda s: (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        if witness(before) != witness(after):
            raise ValueError('source changed during extraction')
        read_bound(ref)
    value = {'kind': 'himr_titanet_embedding_pilot_plan', 'implementation': binding(__file__),
             'model': model, 'model_revision': MODEL_REVISION, 'vad': vad_ref, 'ffmpeg': ffmpeg,
             'sources': sources, 'clips': rows, 'skipped_labels': skipped,
             'selection': 'raw_word_dense_confidence_08_coverage_06_v1',
             'full_media_rehash': False, 'production_eligible': False}
    write_json(output/'plan.json', value)
    return binding(output/'plan.json')


def infer(plan_ref):
    started = time.monotonic()
    plan = read_bound(plan_ref)
    if plan['kind'] != 'himr_titanet_embedding_pilot_plan' or not 1 <= len(plan['clips']) <= 48:
        raise ValueError('invalid pilot plan')
    if plan['implementation'] != binding(__file__) or plan['model']['sha256'] != MODEL_SHA:
        raise ValueError('pilot implementation/model differs')
    if binding(plan['model']['path']) != plan['model'] or binding(plan['vad']['path']) != plan['vad']:
        raise ValueError('model digest mismatch')
    for k,v in {'CUDA_VISIBLE_DEVICES':'', 'HF_HUB_OFFLINE':'1', 'HF_DATASETS_OFFLINE':'1',
                'WANDB_MODE':'disabled', 'HF_HUB_DISABLE_TELEMETRY':'1', 'DO_NOT_TRACK':'1',
                'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}.items():
        os.environ[k] = v
    from pipeline.speaker_screen import deny_internet
    deny_internet()
    import numpy as np
    import torch
    import onnxruntime as ort
    from nemo.collections.asr.models import EncDecSpeakerLabelModel
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    model = EncDecSpeakerLabelModel.restore_from(plan['model']['path'], map_location=torch.device('cpu'))
    model.eval().requires_grad_(False)
    model.preprocessor.featurizer.dither = 0.0
    options = ort.SessionOptions(); options.intra_op_num_threads=1; options.inter_op_num_threads=1
    vad = ort.InferenceSession(plan['vad']['path'], sess_options=options, providers=['CPUExecutionProvider'])
    load_seconds = time.monotonic()-started
    rows = []
    for row in plan['clips']:
        if binding(row['clip']['path']) != row['clip']:
            raise ValueError('clip changed')
        with wave.open(row['clip']['path']) as wav:
            if ((wav.getnchannels(),wav.getsampwidth(),wav.getframerate()) != (1,2,16000)
                    or not 79000 <= wav.getnframes() <= 80000
                    or wav.getnframes()/16 != row['decoded_duration_ms']):
                raise ValueError('unexpected clip shape')
            samples = np.frombuffer(wav.readframes(80000),dtype='<i2').astype(np.float32)/32768
        state=np.zeros((2,1,128),dtype=np.float32); context=np.zeros((1,64),dtype=np.float32)
        speech=0
        for start in range(0,len(samples),512):
            frame=samples[start:start+512]; length=len(frame)
            inputs=np.concatenate((context,np.pad(frame,(0,512-length)).reshape(1,512)),axis=1)
            probability,state=vad.run(['output','stateN'],{'input':inputs,'state':state,'sr':np.array(16000,dtype=np.int64)})
            if not np.isfinite(probability).all() or not 0 <= float(probability[0,0]) <= 1:
                raise ValueError('invalid VAD output')
            if probability[0,0] >= .5: speech+=length
            context=inputs[:,-64:].copy()
        fraction=speech/len(samples)
        result={**row,'vad_speech_fraction':fraction,'embedding':None,'identity':None}
        if fraction < .6:
            result['state']='insufficient_speech_for_pilot'
        else:
            with torch.inference_mode():
                vector=model.get_embedding(row['clip']['path']).detach().cpu().reshape(-1).tolist()
            result.update(state='embedded', embedding=unit(vector))
        rows.append(result)
        print(json.dumps({'clip':len(rows),'total':len(plan['clips']),'state':result['state']}),flush=True)
    value={'kind':'himr_titanet_embedding_pilot_result','plan':plan_ref,'rows':rows,
           'comparisons':comparisons(rows),'production_eligible':False,
           'identity_assignments':0,'automatic_label_merges':0,'new_paid_requests':0,
           'similarities_are_probabilities':False,'network':'IPv4/IPv6 sockets kernel denied',
           'model_load_seconds':load_seconds,'total_seconds':time.monotonic()-started,
           'runtime_versions':{d:metadata.version(d) for d in ['nemo_toolkit','torch','torchaudio','numpy','onnxruntime']}}
    target=Path(plan_ref['path']).parent/'embeddings.json'
    write_json(target,value)
    return binding(target)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('prepare');p.add_argument('--transcript',action='append',required=True)
    p.add_argument('--output',required=True);p.add_argument('--model',required=True);p.add_argument('--vad-config',required=True)
    p=sub.add_parser('infer');p.add_argument('--plan',required=True);p.add_argument('--sha256',required=True)
    args=parser.parse_args();os.umask(0o077)
    if args.command=='prepare':
        vad=read_bound(binding(args.vad_config))['silero_vad']
        value=prepare(args.transcript,args.output,args.model,vad)
    else:
        value=infer({'path':args.plan,'sha256':args.sha256})
    print(json.dumps(value),flush=True)


if __name__=='__main__':main()
