"""Bounded title/date-led listening packet; no voice or face identity matching."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import html
import json
import os
from pathlib import Path
import shutil
import subprocess
import wave

from pipeline.transcript_audio_review import binding, read_bound, write_json

JOBS = [
    'cloudjob_6210543d2bc1d4a0feb9b7c94126d18c',
    'cloudjob_c172770cb7c01003f804e02280734a57',
    'cloudjob_15297252aeae7e012da2dbae70941055',
    'cloudjob_74e27d406b855b50f28194a20fe49686',
]


def prepare(campaign_path, output):
    campaign_ref = binding(campaign_path)
    campaign = read_bound(campaign_ref)
    root = Path(output).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    ffmpeg = binding(Path(shutil.which('ffmpeg')).resolve())
    mapping = {r['job_id']: r for r in campaign['recordings']}
    tasks = []
    for job in JOBS:
        rec = mapping[job]['recording']
        if rec['audio_state'] != 'audio_present':
            raise ValueError('source audio unavailable')
        for i in range(6):
            start = max(0, int(rec['duration_ms'] * (i + .5) / 6) - 6000)
            tasks.append((job, rec, start))

    def extract(item):
        index, (job, rec, start) = item
        source = rec['media']
        before = Path(source['path']).stat()
        if before.st_size != source['byte_count']:
            raise ValueError('source size changed')
        path = root / f'C{index + 1:02d}.wav'
        subprocess.run([ffmpeg['path'], '-nostdin', '-v', 'error', '-threads', '1',
                        '-protocol_whitelist', 'file,pipe', '-ss', str(start / 1000),
                        '-i', source['path'], '-t', '12', '-vn', '-ac', '1', '-ar',
                        '16000', '-c:a', 'pcm_s16le', '-n', str(path)],
                       check=True, timeout=60, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        after = Path(source['path']).stat()
        witness = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if witness(before) != witness(after):
            raise ValueError('source changed during extraction')
        with wave.open(str(path)) as wav:
            if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (1, 2, 16000):
                raise ValueError('invalid audio format')
            if not 190000 <= wav.getnframes() <= 192000:
                raise ValueError('unexpected clip duration')
            duration = wav.getnframes() / 16
        return dict(id=path.stem, job_id=job, recording_id=rec['recording_id'],
                    title=rec['title'], date=rec['date'], source_media=source,
                    start_ms=start, end_ms=start + duration, clip=binding(path),
                    identity=None, single_speaker_verified=False,
                    selection_basis='title_and_2020_date_lead_uniform_sampling_not_identity')

    with ThreadPoolExecutor(max_workers=2) as pool:
        rows = list(pool.map(extract, enumerate(tasks)))
    write_json(root / 'clips.json', dict(kind='himr_text_led_listening_candidates',
               campaign=campaign_ref, clips=rows, ffmpeg=ffmpeg,
               implementation=binding(__file__), production_eligible=False,
               voice_matching=False, face_matching=False))
    return root


def quality(root, config):
    # Reuse the existing speech detector only. Never invoke speaker encoding.
    from pipeline.speaker_screen import deny_internet
    deny_internet()
    from pipeline.speaker_screen_engine import CpuScreenEngine
    backend = CpuScreenEngine(read_bound(binding(config)), threads=1)._load()
    root = Path(root)
    packet = read_bound(binding(root / 'clips.json'))
    rows = []
    for row in packet['clips']:
        if binding(row['clip']['path']) != row['clip']:
            raise ValueError('clip changed')
        with wave.open(row['clip']['path']) as wav:
            pcm = wav.readframes(wav.getnframes())
        probabilities = backend.probabilities(pcm)
        fraction = sum(p >= .5 for p in probabilities) / len(probabilities)
        rows.append({**row, 'speech_fraction': fraction})
    write_json(root / 'quality.json', dict(clips=rows, model_config=binding(config),
               voice_matching=False, production_eligible=False,
               caveat='VAD is not music rejection, speaker counting, or identity verification'))
    parts = ['<!doctype html><meta charset="utf-8"><title>Other video listening candidates</title>',
             '<h1>Other video listening candidates</h1><p>Titles and dates are leads only. '
             'No speaker has been identified. Speech detector scores do not prove clean or single-speaker audio. '
             'Please report clip IDs and the intervals containing only Mila, if any.</p>']
    for row in sorted(rows, key=lambda r: -r['speech_fraction']):
        parts.append(f'<h2>{row["id"]}: {html.escape(row["title"])}</h2>'
                     f'<p>Source {row["start_ms"]/1000:.1f}–{row["end_ms"]/1000:.1f}s; '
                     f'speech detector fraction {row["speech_fraction"]:.0%}</p>'
                     f'<audio controls preload="none" src="{row["id"]}.wav"></audio>')
    with (root / 'review.html').open('x') as f:
        f.write('\n'.join(parts))
    return dict(review=binding(root / 'review.html'), clips=len(rows),
                speech_fraction_at_least_half=sum(r['speech_fraction'] >= .5 for r in rows))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=['prepare', 'quality'])
    p.add_argument('--output', required=True)
    p.add_argument('--campaign')
    p.add_argument('--config')
    a = p.parse_args()
    os.umask(0o077)
    if a.mode == 'prepare':
        print(prepare(a.campaign, a.output))
    else:
        print(json.dumps(quality(a.output, a.config)))
