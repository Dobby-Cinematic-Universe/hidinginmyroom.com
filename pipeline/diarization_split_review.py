"""Text/timing-only diarization triage; candidates require human confirmation."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import html
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from pipeline.transcript_audio_review import binding, read_bound, write_json


def analyze(doc):
    segments = doc['segments']
    counts = Counter(s.get('speaker') for s in segments if s.get('speaker'))
    candidates = []
    for i, s in enumerate(segments):
        label = s.get('speaker')
        if label is None:
            continue
        flags = []
        proposed_pair = None
        duration = s['end_ms'] - s['start_ms']
        if counts[label] <= 2:
            flags.append('label_has_at_most_two_turns')
        if duration < 1000:
            flags.append('subsecond_turn')
        if re.search(r'\b(tts|text.to.speech|donat\w*)\b', s['text'], re.I):
            flags.append('tts_or_donation_mentioned_not_source_proof')
        if i and s['start_ms'] < segments[i-1]['end_ms']:
            flags.append('overlapping_turns_not_necessarily_error')
        if 0 < i < len(segments)-1:
            before, after = segments[i-1], segments[i+1]
            gaps = (s['start_ms']-before['end_ms'], after['start_ms']-s['end_ms'])
            if (before.get('speaker') == after.get('speaker') != label
                    and before.get('speaker') and all(0 <= g <= 2000 for g in gaps)
                    and 0 < duration <= 5000):
                flags.append('brief_A_B_A_sequence_not_same_speaker_proof')
                proposed_pair = [before['speaker'], label]
                if not re.search(r'[.!?][\"\u201d\u2019\x27]*$', before['text'].strip()):
                    flags.append('preceding_text_has_no_sentence_end')
        if not flags:
            continue
        score = (4 * bool(proposed_pair) + 2 * ('label_has_at_most_two_turns' in flags)
                 + 2 * ('preceding_text_has_no_sentence_end' in flags)
                 + int('subsecond_turn' in flags))
        candidates.append(dict(segment_index=i, label=label, flags=flags, priority=score,
            labels_to_compare=proposed_pair, identity=None, merge_approved=False,
            context=[dict(segment_index=j, **segments[j])
                     for j in range(max(0, i-1), min(len(segments), i+2))]))
    return dict(label_turn_counts=dict(counts), candidates=sorted(candidates,
                key=lambda c: (-c['priority'], c['segment_index'])))


def run(campaign_path, recovery_path, output):
    plan_ref = binding(campaign_path)
    plan = read_bound(plan_ref)
    titles = {r['job_id']: r['recording']['title'] for r in plan['recordings']}
    inputs = sorted((Path(plan['state_root']) / 'jobs').glob('*/transcript.json'))
    if recovery_path:
        recovery = read_bound(binding(recovery_path))
        inputs.extend(Path(r['transcript']['path']) for r in recovery['recordings'])
    reports, docs = [], {}
    for path in inputs:
        ref = binding(path)
        doc = read_bound(ref)
        report = analyze(doc)
        if len(report['label_turn_counts']) <= 1:
            continue
        job = doc['job_id']
        docs[job] = doc
        reports.append(dict(job_id=job, title=titles[job], transcript=ref,
            recovered_copy='recovery' in doc, **report))
    root = Path(output).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    ffmpeg = binding(Path(shutil.which('ffmpeg')).resolve())
    # Up to two high-priority excerpts in each of six recordings.
    ranked = sorted([r for r in reports if r['candidates']],
                    key=lambda r: -r['candidates'][0]['priority'])
    selections = []
    for report in ranked[:6]:
        for candidate in report['candidates'][:2]:
            selections.append((report, candidate))

    def extract(item):
        index, (report, candidate) = item
        doc = docs[report['job_id']]
        s = doc['segments'][candidate['segment_index']]
        source = doc['source_media']
        before = Path(source['path']).stat()
        if before.st_size != source['byte_count']:
            raise ValueError('media size changed')
        start = max(0, s['start_ms'] - 5000)
        end = min(doc['duration_seconds']*1000, s['end_ms']+5000, start+20000)
        target = root / f'R{index+1:02d}.wav'
        subprocess.run([ffmpeg['path'], '-nostdin', '-v', 'error', '-threads', '1',
            '-protocol_whitelist', 'file,pipe', '-ss', str(start/1000), '-i', source['path'],
            '-t', str((end-start)/1000), '-vn', '-ac', '1', '-ar', '16000',
            '-c:a', 'pcm_s16le', '-n', str(target)], check=True, timeout=60,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        after = Path(source['path']).stat()
        witness = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns, x.st_ctime_ns)
        if witness(before) != witness(after):
            raise ValueError('media changed during extraction')
        return dict(id=target.stem, title=report['title'], job_id=report['job_id'],
            transcript=report['transcript'], candidate=candidate, clip=binding(target),
            start_ms=start, end_ms=end, focus_in_clip_ms=[s['start_ms']-start,s['end_ms']-start])

    with ThreadPoolExecutor(max_workers=2) as pool:
        clips = list(pool.map(extract, enumerate(selections)))
    result = dict(kind='himr_text_timing_diarization_review', plan=plan_ref,
        reports=reports, clips=clips, implementation=binding(__file__), ffmpeg=ffmpeg,
        voice_identity_matching=False, production_modified=False, automatic_merges=False,
        selection_note='Heuristic triage, not measured diarization accuracy; genuine replies can trigger flags.')
    write_json(root/'review.json', result)
    parts = ['<!doctype html><meta charset="utf-8"><title>Diarization split review</title>',
        '<h1>Diarization split review</h1><p>Text/timing candidates only. No voices identified or labels merged. '
        'A short reply can legitimately have another label. Please indicate whether the highlighted interval '
        'is the same speaker as its neighbors, another speaker, playback/TTS, or uncertain. '
        'Decisions apply to this interval, not every occurrence of the label.</p>']
    for clip in clips:
        c = clip['candidate']
        parts.append(f'<h2>{clip["id"]}: {html.escape(clip["title"])}</h2>'
            f'<p>Source {clip["start_ms"]/1000:.1f}s; focus '
            f'{clip["focus_in_clip_ms"][0]/1000:.1f}–{clip["focus_in_clip_ms"][1]/1000:.1f}s in clip. '
            f'Flags: {html.escape(", ".join(c["flags"]))}</p>'
            f'<audio controls preload="none" src="{clip["id"]}.wav"></audio>')
        for s in c['context']:
            focus = ' [FOCUS]' if s['segment_index']==c['segment_index'] else ''
            parts.append(f'<p>{html.escape(str(s["speaker"]))}{focus}: {html.escape(s["text"])}</p>')
    with (root/'review.html').open('x') as f:
        f.write('\n'.join(parts))
    return dict(recordings=len(reports), clips=len(clips), review=binding(root/'review.html'),
        candidate_intervals=sum(len(r['candidates']) for r in reports),
        brief_A_B_A_intervals=sum(bool(c['labels_to_compare']) for r in reports for c in r['candidates']))


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--campaign', required=True)
    p.add_argument('--recovery')
    p.add_argument('--output', required=True)
    a=p.parse_args()
    os.umask(0o077)
    print(json.dumps(run(a.campaign, a.recovery, a.output)))
