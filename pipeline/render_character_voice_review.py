"""Render a private, static listening worksheet from bound pilot artifacts."""
import argparse
import html
import os
from pathlib import Path
from urllib.parse import quote
from pipeline.transcript_audio_review import binding, read_bound


def render(review_ref, frames_ref, output):
    review=read_bound(review_ref);frames=read_bound(frames_ref)
    if frames['review']!=review_ref:raise ValueError('frames belong to a different audio review')
    target=Path(output).resolve()
    def link(ref):
        if binding(ref['path'])!=ref:raise ValueError('review asset changed')
        return html.escape(quote(os.path.relpath(ref['path'],target.parent)),quote=True)
    data=read_bound(review['embeddings'])
    frame_by_row={f['row_index']:f for f in frames['frames']}
    group_by_row={i:g['anonymous_group'] for g in review['anonymous_groups'] for i in g['row_indices']}
    text=['<!doctype html><meta charset="utf-8"><title>HIMR voice candidate review</title>',
          '<style>body{max-width:1000px;margin:32px auto;font:16px system-ui;padding:0 20px}article{border-top:1px solid #aaa;padding:20px 0}img{max-width:480px;max-height:420px;display:block}audio{display:block;margin:12px 0}</style>',
          '<h1>Voice candidates—not verified identities</h1>',
          '<p>Names in video titles are search leads only. Scores compare audio with provisional Daniel era profiles; they are not probabilities. A low score does not identify Mila or Sunny. Static frames show scene context, not who is speaking or who a person is.</p>',
          '<p>No named enrollment, face recognition, production relabeling or paid requests occurred. Listen for mixed speech, playback, TTS and poor-quality sound before considering a reference.</p>']
    for m in review['matches']:
        r=data['rows'][m['row_index']];i=m['row_index'];f=frame_by_row.get(i)
        text.append(f'<article><h2>Probe {i}: {html.escape(r["title"])}</h2>')
        text.append(f'<p>Time: {r["start_ms"]/1000:.2f}s. Anonymous acoustic group: {html.escape(group_by_row[i])}. Identity: unknown.</p>')
        text.append(f'<p>Daniel-reference cosine: max {m["maximum_cosine"]:.3f}; median {m["median_cosine"]:.3f}. Hint: {html.escape(m["reference_similarity_hint"])}.</p>')
        text.append(f'<audio controls preload="none" src="{link(r["clip"])}"></audio>')
        if f:text.append(f'<img alt="Scene context; no identity assignment" src="{link(f["frame"])}">')
        text.append('</article>')
    with target.open('x') as f:f.write('\n'.join(text))
    return binding(target)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--review',required=True)
    p.add_argument('--frames',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    print(render(binding(a.review),binding(a.frames),a.output))
