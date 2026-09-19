"""Offline, bounded recovery exports; never modifies running campaign state."""
import argparse
import copy
import json
import os
from pathlib import Path

from pipeline import cloud_transcription_client as client
from pipeline.cloud_transcription import _transcript_document
from pipeline.transcript_audio_review import binding, read_bound, write_json

TARGETS = {
    'cloudjob_6915775e9077d5e7ee9bf4d180a5add4':
        'c51f9770a0a76db78cd7ab0e6e0d2edd94456a6074aae61aa90fe26d76ef6cc7',
    'cloudjob_dab421170852dda7fde91a8833e2c983':
        '7f282d9de93054768961ffd47fad351f5f7de3c74d557888b219eeb39cf68c84',
}


def repair(raw):
    result = copy.deepcopy(raw)
    changes = []
    for index, utterance in enumerate(result['utterances']):
        end = max(w['end'] for w in utterance['words'])
        delta = end - utterance['end']
        if delta > 1100:
            raise ValueError('end expansion exceeds bounded recovery')
        if delta > 0:
            changes.append(dict(utterance_index=index, old_end_ms=utterance['end'],
                                new_end_ms=end, expansion_ms=delta))
            utterance['end'] = end
    if not 1 <= len(changes) <= 2:
        raise ValueError('unexpected number of timing corrections')
    return result, changes


def recover(campaign_path, output):
    plan_ref = binding(campaign_path)
    plan = read_bound(plan_ref)
    mapping = {r['job_id']: r for r in plan['recordings']}
    root = Path(output).resolve()
    if root.exists():
        raise ValueError('recovery output already exists')
    prepared = []
    for job, expected_sha in TARGETS.items():
        folder = Path(plan['state_root']) / 'jobs' / job
        review_ref = binding(folder / 'collection-review.json')
        review = read_bound(review_ref)
        if (review['job_id'] != job or review['plan'] != plan_ref
                or review['raw_result']['sha256'] != expected_sha
                or review['provider'] != 'assemblyai'
                or review['normalizer_implementation_sha256'] != binding(client.__file__)['sha256']):
            raise ValueError('review contract differs')
        raw = read_bound(review['raw_result'])
        terminal = read_bound(review['provider_job'])
        audio = read_bound(review['audio_receipt'])['audio']
        screen = read_bound(review['screen_decision'])
        repaired, changes = repair(raw)
        normalized = client.normalize_result('assemblyai', repaired,
            expected_duration_seconds=audio['duration_ms'] / 1000,
            job=terminal, diarization=screen['diarization'])
        if normalized['text'] != raw['text']:
            raise ValueError('text changed')
        prepared.append((job, review_ref, review, raw, repaired, changes, terminal, audio, screen))
    root.mkdir(mode=0o700, parents=True)
    recovered = []
    for job, review_ref, review, raw, repaired, changes, terminal, audio, screen in prepared:
        folder = root / job
        folder.mkdir(mode=0o700)
        # Explicitly marked derivative, never represented as an original API response.
        derivative_path = folder / 'timing-adjusted-result.json'
        write_json(derivative_path, repaired)
        doc = _transcript_document(mapping[job], audio, binding(derivative_path),
            review['provider_job'], terminal, repaired, review['screen_decision'], screen)
        doc['recovery'] = dict(kind='utterance_end_expansion_from_retained_word_bounds',
            original_raw_result=review['raw_result'], original_review=review_ref,
            implementation=binding(__file__), changes=changes,
            raw_result_is_local_derivative=True, production_admitted=False)
        write_json(folder / 'transcript.json', doc)
        if binding(review['raw_result']['path']) != review['raw_result']:
            raise ValueError('original result changed')
        recovered.append(dict(job_id=job, title=mapping[job]['recording']['title'],
            transcript=binding(folder / 'transcript.json'), segments=len(doc['segments']),
            changes=changes, text_unchanged=doc['text'] == raw['text']))
    manifest = dict(kind='himr_offline_cloud_timing_recovery', plan=plan_ref,
        recordings=recovered, new_paid_requests=0, live_campaign_modified=False,
        production_admitted=False, remaining_step='versioned live-reader integration',
        original_provider_results_preserved=True)
    write_json(root / 'recovery.json', manifest)
    return manifest


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--campaign', required=True)
    p.add_argument('--output', required=True)
    args = p.parse_args()
    os.umask(0o077)
    print(json.dumps(recover(args.campaign, args.output)))
