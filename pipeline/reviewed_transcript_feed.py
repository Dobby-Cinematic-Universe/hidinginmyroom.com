"""Reversible, continuously refreshed transcript copies from explicit reviews."""
import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import signal
import time


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()


def bound(path):
    path = Path(path).resolve()
    raw = path.read_bytes()
    return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())


def read(ref):
    raw = Path(ref['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != ref['sha256']:
        raise ValueError('review input binding changed')
    return json.loads(raw)


def atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    raw = encode(value)
    if path.exists() and path.read_bytes() == raw:
        return bound(path)
    temp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    with temp.open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    os.replace(temp, path)
    return bound(path)


def immutable(root, stem, value):
    digest = hashlib.sha256(encode(value)).hexdigest()
    path = Path(root) / (stem + '-' + digest + '.json')
    if path.exists():
        if path.read_bytes() != encode(value):
            raise ValueError('content-addressed reviewed copy changed')
        return bound(path)
    return atomic(path, value)


def project(doc, source_ref, reviews, confirmed=None):
    """Never infer a person from voice, frames, a title, or an anonymous label."""
    confirmed = confirmed or {}
    if confirmed.get('transcript') != source_ref:
        confirmed = {}
    whole = {m['label']: dict(source='participant', name=m['name'])
             for m in confirmed.get('confirmed_mappings', [])}
    individual = {}
    accepted = []
    for entry in reviews:
        if entry['transcript'] != source_ref:
            continue
        decision = entry['decision']
        if decision['job_id'] != doc['job_id']:
            continue
        if decision['source'] not in {'participant', 'playback', 'tts', 'uncertain'}:
            raise ValueError('unsupported review source')
        if decision['scope'] == 'label':
            whole[decision['label']] = decision
        elif decision['scope'] == 'segment':
            index = decision['segment_index']
            if type(index) is not int or not 0 <= index < len(doc['segments']) or doc['segments'][index]['speaker'] != decision['label']:
                raise ValueError('review turn binding differs')
            individual[(index, decision['label'])] = decision
        else:
            raise ValueError('unsupported review scope')
        accepted.append(entry)
    output = deepcopy(doc)
    output['kind'] = 'himr_reviewed_transcript_copy'
    names, rows, payload, evidence = {}, [], [], []
    counts = Counter()
    for index, original in enumerate(doc['segments']):
        label = original.get('speaker')
        decision = individual.get((index, label), whole.get(label))
        source = decision['source'] if decision else 'unreviewed'
        saved_name = ' '.join((decision.get('name') or '').split()) if decision else ''
        name = saved_name if source == 'participant' else ''
        reason = None
        # A mixed label or a tentative name is not a second established person.
        if name and ('/' in name or '?' in name):
            source, name, reason = 'uncertain', '', 'mixed_or_provisional_manual_assignment'
        elif source == 'uncertain':
            reason = 'explicit_manual_uncertainty'
        key = name.casefold() if name else None
        if key is not None:
            names.setdefault(key, name)
        counts[source] += 1
        speaker_id = 'SPEAKER_' + str(list(names).index(key)).zfill(4) if key is not None else None
        rows.append({**original, 'original_speaker': label, 'speaker': speaker_id,
            'speaker_name': names.get(key), 'audio_source': source,
            'manually_reviewed': decision is not None, 'saved_name': saved_name or None,
            'attribution_uncertainty': reason})
        evidence_id = 'segment_' + str(index)
        evidence.append(dict(evidence_id=evidence_id, original_transcript=source_ref,
            segment_index=index, original_speaker=label, start_ms=original['start_ms'], end_ms=original['end_ms']))
        if source in {'playback', 'tts'}:
            continue  # Retained above, excluded only from participant summaries.
        display = names.get(key) or ('Unidentified participant' if source == 'participant' else 'Uncertain speaker')
        payload.append(dict(evidence_id=evidence_id, speaker=display, text=original['text']))
    # Keep per-turn names, but do not present anonymous provider clusters as
    # confirmed diarization where fewer than two named people were reviewed.
    retain = len(names) >= 2
    if not retain:
        for row in rows:
            row['speaker'] = None
    output['segments'] = rows
    output['provider_speaker_labels'] = {}
    words = sum(len(row['text'].split()) for row in payload)
    output['projection'] = dict(original_transcript=source_ref,
        review_complete=counts['unreviewed'] == 0, review_source_counts=dict(counts),
        named_participants=list(names.values()), named_diarization_retained=retain,
        original_text_changed=False, segment_timestamps_changed=False,
        speaker_identity_inferred=False, uncertainty_retained=True,
        word_count_for_summary=words,
        summary_eligible=counts['unreviewed'] == 0 and words >= 50,
        exclusion_reason='unreviewed_turns' if counts['unreviewed'] else 'under_50_participant_words' if words < 50 else None)
    model = dict(kind='himr_reviewed_summary_input', instructions=(
        'Summarize only the supplied transcript evidence. Names are saved reviewer assignments. '
        'Uncertain speaker and Unidentified participant are not Daniel by default. '
        'Do not resolve mixed speech, guess names, or attribute uncertain statements to named people. '
        'Preserve uncertainty and internal evidence IDs. Playback, game/background audio and TTS '
        'have been excluded from these participant-summary inputs.'), segments=payload)
    return output, model, evidence


class Feed:
    def __init__(self, report, confirmations, decisions, plans, output):
        self.report, self.confirmations = Path(report), Path(confirmations)
        self.decisions, self.plans, self.output = Path(decisions), list(map(Path, plans)), Path(output)

    def scan(self):
        report_ref, confirmed_ref = bound(self.report), bound(self.confirmations)
        report, confirmed = read(report_ref), read(confirmed_ref)
        records = {r['job_id']: r for r in report['reports']}
        for path in self.plans:
            if not path.exists():
                continue
            plan = read(bound(path))
            for row in plan['recordings']:
                job = row['job_id']
                completion = Path(plan['state_root']) / 'jobs' / job / 'completion.json'
                if job in records or not completion.exists():
                    continue
                receipt = read(bound(completion))
                ref = receipt['transcript']
                doc = read(ref)
                if doc['job_id'] != job or doc['recording_id'] != row['recording']['recording_id']:
                    raise ValueError('review discovery identity differs')
                labels = Counter(s.get('speaker') for s in doc['segments'] if s.get('speaker'))
                if len(labels) < 2 or sum(len(s['text'].split()) for s in doc['segments']) < 50:
                    continue
                records[job] = dict(job_id=job, title=row['recording']['title'], transcript=ref,
                    recovered_copy=False, label_turn_counts=dict(labels), candidates=[])
        decisions = {}
        for path in sorted(self.decisions.glob('*.json')):
            reference = bound(path)
            entry = read(reference)
            decisions.setdefault(entry['decision']['job_id'], []).append((reference, entry))
        confirmations = {r['job_id']: r for r in confirmed['records']}
        items = []
        for job, record in records.items():
            ref = record['transcript']
            doc = read(ref)
            valid = [(r, e) for r, e in decisions.get(job, []) if e['transcript'] == ref]
            projected, model, evidence = project(doc, ref, [e for _, e in valid], confirmations.get(job))
            projected['projection'].update(review_decisions=[r for r, _ in valid],
                confirmations=confirmed_ref, implementation=bound(__file__))
            folder = self.output / 'records' / job
            projection_ref = immutable(folder, 'transcript', projected)
            model_ref = immutable(folder, 'summary-input', model)
            evidence_ref = immutable(folder, 'evidence', dict(transcript=projection_ref, segments=evidence))
            items.append(dict(job_id=job, recording_id=doc['recording_id'], title=record['title'],
                transcript=projection_ref, model_input=model_ref,
                evidence=evidence_ref, **projected['projection']))
        atomic(self.output / 'review-index.json', {**report, 'reports': list(records.values()),
            'derived_from': report_ref, 'dynamic_receipt_discovery': True})
        index = dict(kind='himr_reviewed_transcript_feed', records=items,
            original_transcripts_modified=False, new_paid_requests=0,
            summary_input_policy='speaker_text_evidence_only_without_timestamps',
            review_complete=sum(r['review_complete'] for r in items),
            summary_eligible=sum(r['summary_eligible'] for r in items))
        atomic(self.output / 'index.json', index)
        return dict(recordings=len(items), review_complete=index['review_complete'],
            summary_eligible=index['summary_eligible'], new_paid_requests=0)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for flag in ('report', 'confirmations', 'decisions', 'output'):
        p.add_argument('--' + flag, required=True)
    p.add_argument('--plan', action='append', default=[])
    p.add_argument('--watch', action='store_true')
    a = p.parse_args()
    os.umask(0o077)
    active = [True]
    signal.signal(signal.SIGTERM, lambda *_: active.__setitem__(0, False))
    worker = Feed(a.report, a.confirmations, a.decisions, a.plan, a.output)
    while active[0]:
        print(json.dumps(worker.scan()), flush=True)
        if not a.watch:
            break
        deadline = time.monotonic() + 60
        while active[0] and time.monotonic() < deadline:
            time.sleep(1)


if __name__ == '__main__':
    main()
