"""Small listening packet around user-labeled Mila anchors; no inherited names."""
import html
import os
from pathlib import Path
import subprocess
from pipeline.transcript_audio_review import binding,read_bound,write_json


def prepare(embeddings_ref,annotations_ref,output):
    data=read_bound(embeddings_ref);review=read_bound(annotations_ref);plan=read_bound(data['plan'])
    if review['embeddings']!=embeddings_ref:raise ValueError('review binding mismatch')
    annotations={a['probe']:a for a in review['annotations']}
    root=Path(output);root.mkdir(mode=0o700,parents=True,exist_ok=False)
    ffmpeg=plan['ffmpeg']
    if binding(ffmpeg['path'])!=ffmpeg:raise ValueError('decoder changed')
    sources={s['recording_id']:s['media'] for s in plan['sources']}
    entries=[];parts=['<!doctype html><meta charset="utf-8"><title>Mila reference follow-up</title>',
        '<h1>Mila reference follow-up</h1><p>Original labels are preserved. Adjacent clips are unconfirmed and may contain another voice. Context clips include the labeled original at seconds 5–10; labels apply only to that original interval.</p>']
    for probe in (1,4,20):
        a=annotations[probe];row=data['rows'][probe]
        if a['kind']!='single_speaker' or a['speakers']!=['Mila'] or a['clip']!=row['clip']:
            raise ValueError('expected exact reviewed Mila anchor')
        source=sources[row['recording_id']];before=Path(source['path']).stat()
        if before.st_size!=source['byte_count']:raise ValueError('media changed')
        start,end=row['start_ms'],row['end_ms']
        for name,left,right in [('before',start-5000,start),('after',end,end+5000),('context',start-5000,end+5000)]:
            if left<0:continue
            identifier=f'M{probe}-{name}';target=root/(identifier+'.wav')
            subprocess.run([ffmpeg['path'],'-nostdin','-v','error','-threads','1','-protocol_whitelist','file,pipe',
                '-ss',str(left/1000),'-i',source['path'],'-t',str((right-left)/1000),'-vn','-ac','1','-ar','16000',
                '-c:a','pcm_s16le','-n',str(target)],check=True,timeout=60,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            entries.append({'id':identifier,'original_probe':probe,'source_media':source,'start_ms':left,'end_ms':right,
                            'clip':binding(target),'identity':None,'needs_user_review':True,
                            'confirmed_subinterval_ms':[start,end] if name=='context' else None})
            parts.append(f'<h2>{html.escape(identifier)}</h2><p>{html.escape(row["title"])} — source {left/1000:.3f} to {right/1000:.3f} seconds</p><audio controls preload="none" src="{target.name}"></audio>')
        after=Path(source['path']).stat()
        witness=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        if witness(before)!=witness(after):raise ValueError('media changed while extracting')
    value={'kind':'himr_mila_reference_followup','embeddings':embeddings_ref,'annotations':annotations_ref,
           'clips':entries,'implementation':binding(__file__),'names_inherited':False,'production_eligible':False}
    write_json(root/'clips.json',value)
    with (root/'review.html').open('x') as f:f.write('\n'.join(parts))
    return binding(root/'review.html')


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--embeddings',required=True)
    p.add_argument('--annotations',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    print(prepare(binding(a.embeddings),binding(a.annotations),a.output))
