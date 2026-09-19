"""Human-reviewed text evidence proposals; no biometric matching or label edits."""
import argparse
import html
import json
import os
from pathlib import Path
from collections import defaultdict
from pipeline.transcript_audio_review import binding, read_bound, write_json

# Evidence selected after reading transcript context, not inferred from audio.
PROPOSALS = {
    'cloudjob_be1be1649cdca10612dba1b391f29299': [
        ('SPEAKER_0002', 'Daniel', 'explicit_self_introduction', [(23, "Nice to meet you. I'm Daniel.")]),
        ('SPEAKER_0003', 'Kimberly', 'explicit_self_introduction', [(22, 'Hi, my name is Kimberly. Nice to meet you.')]),
    ],
    'cloudjob_dab421170852dda7fde91a8833e2c983': [
        ('SPEAKER_0000', 'Daniel', 'name_addressed_first_person_response',
         [(681, "Why is Daniel a leech? I'm not! I make my own money."),
          (956, "I'm gonna name it Daniel Lord ASMR.")]),
        ('SPEAKER_0002', 'Sabrina', 'repeated_name_addressed_first_person_responses',
         [(531, 'Sabrina, what do you think of living in London? I really like it.'),
          (537, 'Sabrina, are you a witch? I wish I was a witch.'),
          (543, "Sabrina, have you ever considered modelling? No, because I'm really awkward in front of the camera")]),
    ],
}


def stats(segments):
    grouped = defaultdict(list)
    for s in segments:
        if s.get('speaker'):
            grouped[s['speaker']].append((s['start_ms'], s['end_ms']))
    result = {}
    for label, intervals in grouped.items():
        total = 0
        end = -1
        for a,b in sorted(intervals):
            total += max(0, b-max(a,end))
            end = max(end,b)
        result[label] = dict(turns=len(intervals), labeled_duration_ms=total,
                             substantial=len(intervals)>=5 and total>=60000)
    return result


def run(manifest_path, output):
    manifest_ref = binding(manifest_path)
    manifest = read_bound(manifest_ref)
    records = []
    matched = set()
    for entry in manifest['transcripts']:
        doc = read_bound(entry['original'])
        measurements = stats(doc['segments'])
        if sum(v['substantial'] for v in measurements.values())<2:
            continue
        proposals = []
        for label, name, basis, selections in PROPOSALS.get(entry['job_id'], []):
            if not measurements[label]['substantial']:
                raise ValueError('proposed participant lacks sufficient labeled speech')
            evidence = []
            for index, quote in selections:
                s = doc['segments'][index]
                if s['speaker'] != label or quote not in s['text']:
                    raise ValueError('reviewed text evidence no longer matches')
                evidence.append(dict(transcript=entry['original'], segment_index=index,
                    start_ms=s['start_ms'], end_ms=s['end_ms'], quote=quote,
                    char_start=s['text'].index(quote), char_end=s['text'].index(quote)+len(quote)))
            proposals.append(dict(label=label, proposed_name=name, basis=basis,
                status='pending_human_review', approved=False, evidence=evidence,
                caveat='Text supports a name association; it does not establish identity or label purity across all turns.'))
        if proposals:
            matched.add(entry['job_id'])
        records.append(dict(job_id=entry['job_id'], title=entry['title'],
            transcript=entry['original'], label_free_copy=entry['copy'],
            speech_statistics=measurements, proposals=proposals,
            unmapped_labels=sorted(set(measurements)-{p['label'] for p in proposals}),
            two_named_candidates_pending_review=len(proposals)>=2,
            approved_diarization_exception=False))
    if matched != set(PROPOSALS):
        raise ValueError('expected reviewed recording absent')
    root = Path(output).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    result = dict(kind='himr_text_evidence_name_proposals', source_snapshot=manifest_ref,
        implementation=binding(__file__), records=records,
        minimum_labeled_duration_ms=60000, minimum_turns=5,
        duration_caveat='Union of provider-labeled intervals, not measured clean speech or proof of human participants.',
        voice_embeddings_used=False, face_matching_used=False, title_only_assignments=False,
        originals_modified=False, label_free_copies_modified=False, production_modified=False,
        automatic_merges=False, approved_exceptions=0,
        search_limit='Bounded text review of the existing 294-copy snapshot; unproposed labels remain unknown, not proven unidentifiable.')
    write_json(root/'proposals.json',result)
    parts=['<!doctype html><meta charset="utf-8"><title>Named speaker proposals</title>',
        '<h1>Text-evidence name proposals — pending review</h1><p>No embeddings, automatic merges, '
        'or production edits. Duration counts provider-labeled intervals, not verified clean speech. '
        'A name in one turn does not guarantee every turn with that label belongs to that person.</p>']
    for r in records:
        if not r['proposals']:continue
        parts.append(f'<h2>{html.escape(r["title"])}</h2>')
        for p in r['proposals']:
            st=r['speech_statistics'][p['label']]
            parts.append(f'<h3>{html.escape(p["label"])} → {html.escape(p["proposed_name"])}</h3>'
                f'<p>{st["turns"]} turns; {st["labeled_duration_ms"]/60000:.1f} labeled minutes. Pending review.</p>')
            for e in p['evidence']:
                parts.append(f'<p>Segment {e["segment_index"]}, {e["start_ms"]/1000:.1f}s: '
                             f'{html.escape(e["quote"])}</p>')
        parts.append(f'<p>Other labels remain unknown: {html.escape(", ".join(r["unmapped_labels"]))}</p>')
    with (root/'review.html').open('x') as f:f.write('\n'.join(parts))
    return dict(eligible_recordings=len(records), proposed_recordings=len(matched),
        named_label_proposals=sum(len(r['proposals']) for r in records), review=binding(root/'review.html'))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();os.umask(0o077)
    print(json.dumps(run(a.manifest,a.output)))
