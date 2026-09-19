"""Offline model/cropping ablation on exact user-reviewed voice clips.

No label changes, additional named audio, model training, paid calls, or
production activation. Selection uses VAD only, never the expected identity.
"""
import argparse
from copy import deepcopy
import json
import math
import os
from pathlib import Path
import time
import wave
from pipeline.transcript_audio_review import binding,read_bound,write_json
from pipeline.titanet_embedding_pilot import unit,MODEL_SHA
from pipeline.reviewed_voice_gallery import enroll,evaluate


def longest_speech(probabilities, samples):
    runs=[];begin=None
    for i,p in enumerate(probabilities):
        if not math.isfinite(p) or not 0<=p<=1:raise ValueError('invalid VAD probability')
        if p>=.5 and begin is None:begin=i*512
        if p<.5 and begin is not None:
            runs.append((begin,min(i*512,samples)));begin=None
    if begin is not None:runs.append((begin,samples))
    if not runs:return None
    start,end=max(runs,key=lambda r:(r[1]-r[0],-r[0]))
    return (start,end) if end-start>=32000 else None


def ecapa(embeddings_ref,annotations_ref,config_ref,output):
    started=time.monotonic();data=read_bound(embeddings_ref);annotations=read_bound(annotations_ref)
    if annotations['embeddings']!=embeddings_ref:raise ValueError('annotations differ')
    root=Path(output);root.mkdir(mode=0o700,parents=True,exist_ok=False)
    (root/'crops').mkdir(mode=0o700)
    from pipeline.speaker_screen import deny_internet
    deny_internet()
    from pipeline import speaker_screen_engine as engine
    model=engine.CpuScreenEngine(read_bound(config_ref),threads=1);backend=model._load()
    import numpy as np
    rows=[]
    for a in annotations['annotations']:
        i=a['probe'];row=data['rows'][i]
        if row['clip']!=a['clip'] or binding(a['clip']['path'])!=a['clip']:raise ValueError('reviewed clip changed')
        with wave.open(a['clip']['path']) as wav:
            if (wav.getnchannels(),wav.getsampwidth(),wav.getframerate())!=(1,2,16000):raise ValueError('invalid audio')
            if not 32000<=wav.getnframes()<=80000:raise ValueError('unbounded audio')
            pcm=wav.readframes(wav.getnframes())
        x=np.frombuffer(pcm,dtype='<i2').astype(np.float32)/32768
        probabilities=backend.probabilities(pcm);span=longest_speech(probabilities,len(x))
        rms=float(np.sqrt(np.mean(x*x)))
        entry={'probe':i,'source_clip':a['clip'],'annotation_kind':a['kind'],
               'rms_dbfs':20*math.log10(rms) if rms else None,
               'near_clipped_fraction':float(np.mean(np.abs(x)>=.999)),
               'full_ecapa':unit(backend.encode(pcm)),'speech_crop_ecapa':None,'crop':None}
        if span:
            start,end=span;selected=pcm[start*2:end*2];path=root/'crops'/f'probe-{i:03d}.wav'
            with wave.open(str(path),'wb') as wav:
                wav.setnchannels(1);wav.setsampwidth(2);wav.setframerate(16000);wav.writeframes(selected)
            entry.update(crop=binding(path),crop_start_sample=start,crop_end_sample=end,
                         speech_crop_ecapa=unit(backend.encode(selected)))
        rows.append(entry)
    result={'kind':'himr_mila_embedding_ablation','embeddings':embeddings_ref,'annotations':annotations_ref,
            'models':config_ref,'engine':binding(engine.__file__),'implementation':binding(__file__),
            'rows':rows,'seconds':time.monotonic()-started,'model_provenance':model.provenance(),
            'production_eligible':False,'labels_modified':False,'network':'kernel_denied_ipv4_ipv6'}
    write_json(root/'ecapa.json',result);return binding(root/'ecapa.json')


def titanet_crops(ecapa_ref,model_ref,output):
    experiment=read_bound(ecapa_ref)
    if model_ref['sha256']!=MODEL_SHA or binding(model_ref['path'])!=model_ref:raise ValueError('model mismatch')
    for k,v in {'CUDA_VISIBLE_DEVICES':'','HF_HUB_OFFLINE':'1','HF_HUB_DISABLE_TELEMETRY':'1','WANDB_MODE':'disabled',
                'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}.items():os.environ[k]=v
    from pipeline.speaker_screen import deny_internet
    deny_internet()
    import torch
    from nemo.collections.asr.models import EncDecSpeakerLabelModel
    torch.set_num_threads(1);torch.set_num_interop_threads(1)
    model=EncDecSpeakerLabelModel.restore_from(model_ref['path'],map_location=torch.device('cpu'))
    model.eval().requires_grad_(False);model.preprocessor.featurizer.dither=0
    rows=[]
    for row in experiment['rows']:
        vector=None
        if row['crop']:
            if binding(row['crop']['path'])!=row['crop']:raise ValueError('crop changed')
            with torch.inference_mode():vector=unit(model.get_embedding(row['crop']['path']).detach().cpu().reshape(-1).tolist())
        rows.append({'probe':row['probe'],'speech_crop_titanet':vector})
    value={'kind':'himr_titanet_crop_ablation','ecapa_experiment':ecapa_ref,'model':model_ref,
           'rows':rows,'implementation':binding(__file__),'production_eligible':False}
    write_json(output,value);return binding(output)


def diagnostics(ecapa_ref,cropped_ref,output):
    experiment=read_bound(ecapa_ref);cropped=read_bound(cropped_ref)
    if cropped['ecapa_experiment']!=ecapa_ref:raise ValueError('experiments differ')
    original=read_bound(experiment['embeddings']);annotations=read_bound(experiment['annotations'])
    values={r['probe']:r for r in experiment['rows']};values2={r['probe']:r for r in cropped['rows']}
    results={}
    for variant in ['baseline_titanet','full_ecapa','speech_crop_ecapa','speech_crop_titanet']:
        data=deepcopy(original)
        if variant!='baseline_titanet':
            for a in annotations['annotations']:
                i=a['probe'];v=values2[i][variant] if variant=='speech_crop_titanet' else values[i][variant]
                data['rows'][i]['embedding']=v
        gallery=enroll(data,annotations['annotations']);d=evaluate(gallery)
        d['eligible_single_speaker_probes']=[s['probe'] for s in gallery['samples']]
        d['missing_single_speaker_probes']=[a['probe'] for a in annotations['annotations']
            if a['kind']=='single_speaker' and data['rows'][a['probe']]['embedding'] is None]
        results[variant]=d
    result={'kind':'himr_mila_voice_ablation_diagnostics','ecapa':ecapa_ref,'cropped_titanet':cropped_ref,
            'variants':results,'selected_for_production':None,'same_small_reviewed_set_reused':True,
            'independent_confirmation_required':True,'automatic_labeling_enabled':False,
            'implementation':binding(__file__)}
    write_json(output,result);return binding(output)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);s=p.add_subparsers(dest='mode',required=True)
    a=s.add_parser('ecapa');a.add_argument('--embeddings',required=True);a.add_argument('--annotations',required=True);a.add_argument('--models',required=True);a.add_argument('--output',required=True)
    a=s.add_parser('titanet');a.add_argument('--ecapa',required=True);a.add_argument('--model',required=True);a.add_argument('--output',required=True)
    a=s.add_parser('compare');a.add_argument('--ecapa',required=True);a.add_argument('--cropped',required=True);a.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    if a.mode=='ecapa':r=ecapa(binding(a.embeddings),binding(a.annotations),binding(a.models),a.output)
    elif a.mode=='titanet':r=titanet_crops(binding(a.ecapa),binding(a.model),a.output)
    else:r=diagnostics(binding(a.ecapa),binding(a.cropped),a.output)
    print(json.dumps(r),flush=True)
