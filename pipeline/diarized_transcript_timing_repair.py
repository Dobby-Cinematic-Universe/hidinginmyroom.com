"""Recover a bounded utterance-end defect into explicit review-only copies.

Never changes provider receipts or retries transcription. Only an utterance's
end can expand to its retained final word end, by at most two seconds. All other
provider text, labels, word data, timing checks and duration bounds remain.
"""
import argparse
from copy import deepcopy
from pathlib import Path

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_client as client
from pipeline import reviewed_transcript_feed as feed


def repair(raw):
    adjusted, changes = deepcopy(raw), []
    for index, utterance in enumerate(adjusted['utterances']):
        words = utterance['words']
        if not words:
            continue
        end = max(word['end'] for word in words)
        delta = end - utterance['end']
        if delta > 2000:
            raise ValueError('utterance end exceeds bounded timing repair')
        if delta > 0:
            changes.append(dict(utterance_index=index, old_end_ms=utterance['end'],
                                new_end_ms=end, expansion_ms=delta))
            utterance['end'] = end
    if not 1 <= len(changes) <= 10:
        raise ValueError('no bounded utterance-end repair available')
    return adjusted, changes


def recover(plan_ref, jobs, base_report, output):
    plan = feed.read(plan_ref)
    rows = {r['job_id']:r for r in plan['recordings']}
    report = feed.read(base_report)
    reports = {r['job_id']:r for r in report['reports']}
    for job in jobs:
        row = rows[job]
        folder = Path(plan['state_root']) / 'jobs' / job
        review_ref = feed.bound(folder / 'collection-review.json')
        review = feed.read(review_ref)
        if (review['plan'] != plan_ref or review['job_id'] != job
                or review['provider'] != 'assemblyai'
                or review['recording_id'] != row['recording']['recording_id']
                or review['normalizer_implementation_sha256'] != feed.bound(client.__file__)['sha256']
                or review['raw_result'] != review['provider_job']):
            raise ValueError('timing repair review binding differs')
        raw = feed.read(review['raw_result'])
        audio = feed.read(review['audio_receipt'])['audio']
        decision = feed.read(review['screen_decision'])
        if decision['diarization'] is not True:
            raise ValueError('repair requires an explicitly diarized result')
        client.validate_job('assemblyai', raw, expected_job_id=raw['id'])
        adjusted, changes = repair(raw)
        # Finish provider normalization before publishing any review copy.
        client.normalize_result('assemblyai', adjusted, job=raw, diarization=True,
                                expected_duration_seconds=audio['duration_ms']/1000)
        target = Path(output) / job
        adjusted_ref = feed.immutable(target, 'timing-adjusted-result', adjusted)
        doc = cloud._transcript_document(row, audio, adjusted_ref, review['provider_job'],
            raw, adjusted, review['screen_decision'], decision)
        if doc['text'] != raw['text']:
            raise ValueError('timing recovery cannot change transcript text')
        doc['recovery'] = dict(kind='bounded_utterance_end_from_retained_word_bounds',
            original_raw_result=review['raw_result'], original_review=review_ref,
            implementation=feed.bound(__file__), changes=changes,
            raw_result_is_local_derivative=True, production_admitted=False)
        ref = feed.immutable(target, 'transcript', doc)
        reports[job] = dict(job_id=job, title=row['recording']['title'], transcript=ref,
                            recovered_copy=True, candidates=[])
    return feed.atomic(Path(output) / 'review.json', {**report, 'reports':list(reports.values()),
        'recovery_base_report':base_report, 'recovery_plan':plan_ref,
        'original_provider_results_preserved':True, 'new_paid_requests':0})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for flag in ('plan', 'base-report', 'output'):
        p.add_argument('--' + flag, required=True)
    p.add_argument('--job', action='append', required=True)
    a = p.parse_args()
    print(feed.encode(recover(feed.bound(a.plan), a.job, feed.bound(a.base_report), a.output)).decode().strip())


if __name__ == '__main__':
    main()
