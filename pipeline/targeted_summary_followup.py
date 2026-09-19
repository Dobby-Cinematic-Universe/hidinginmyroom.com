"""Prepare missing targeted summaries and a non-destructive corpus input overlay."""
import argparse
from pathlib import Path
import re
from pipeline import transcript_summary as r
from pipeline import transcript_summary_campaign as campaign
from pipeline import cloud_transcription_client as cloud


def read(path): return r.read(r.binding(path))


def prepare(base, preview, root):
    r.mkdir(root); r.mkdir(root / 'imports')
    selection = read(preview / 'inputs/selection.json')
    existing = {x['recording_id'] for x in selection['records']}
    reconciliation = read(preview / 'inputs/reconciliation.json')
    records = {x['recording_id']: x for x in reconciliation['records']}
    admitted, held, sources = [], [], []
    for lane in ('targeted-retranscription-20260917-v3', 'targeted-retranscription-media-20260917'):
        plan = read(base / lane / 'plan.json')
        for row in plan['recordings']:
            folder = base / lane / 'jobs' / row['job_id']
            status = read(folder / 'status.json')
            if status['state'] != 'completed': continue
            doc = read(folder / 'transcript.json'); pid = doc['recording_id']
            original = r.binding(folder / 'transcript.json')
            if read(folder / 'completion.json')['transcript'] != original:
                raise r.Error('targeted completion differs')
            if status['requires_speaker_review']:
                held.append(dict(recording_id=pid, title=row['recording']['title'],
                                 reason='anonymous_multiple_speakers', transcript=original))
                continue
            records[pid] = dict(recording_id=pid, transcript=original, needs_speaker_review=False,
                                reconciliation=dict(reason='completed_targeted_recovery', partial=False))
            admitted.append(dict(recording_id=pid, title=row['recording']['title'], transcript=original))
            if pid in existing:
                held.append(dict(recording_id=pid, reason='summary_already_exists')); continue
            text = '\n'.join(s['text'] for s in doc['segments'])
            # Auto-language CJK spacing can inflate the whitespace word count.
            if len(text.split()) < 50 or (re.search('[\u3040-\u30ff\u4e00-\u9fff]', text)
                                           and len(re.sub(r'\s', '', text)) < 200):
                held.append(dict(recording_id=pid, title=row['recording']['title'],
                                 reason='extremely_short_transcript', transcript=original)); continue
            target = root / 'imports' / row['job_id']; r.mkdir(target)
            normalized = cloud.normalize_result(doc['provider'], r.read(doc['raw_result']),
                expected_duration_seconds=doc['audio']['duration_ms']/1000,
                job=r.read(doc['provider_job']), diarization=doc['diarization_requested'])
            if normalized['segments'] != doc['segments'] or normalized['text'] != doc['text']:
                raise r.Error('targeted text/timing differs from canonical provider replay')
            screen = r.put(target / 'screen.json', dict(kind='himr_cloud_speaker_screen_decision',
                schema_version=1, recording_id=pid, media=doc['source_media'],
                diarization=doc['diarization_requested'], source=r.binding(folder / 'speech-screen.json')))
            canonical = {k:v for k,v in doc.items() if k != 'recovery_plan'}
            canonical.update(screen_decision=screen, verified_quotation=False,
                             normalizer_implementation_sha256=r.binding(cloud.__file__)['sha256'])
            transcript = r.put(target / 'transcript.json', canonical)
            completion = r.put(target / 'completion.json', dict(kind='himr_cloud_transcription_completion',
                schema_version=1, job_id=doc['job_id'], audio=doc['audio'], raw_result=doc['raw_result'],
                provider_job=doc['provider_job'], screen_decision=screen, transcript=transcript))
            spec = dict(transcript=transcript, completion=completion, format='cloud', recording_id=pid,
                        title=row['recording']['title'], date=None)
            r.sources_module.normalize_source(spec)
            sources.append(spec)
    reconciliation['records'] = list(records.values())
    reconciliation['targeted_recovery'] = dict(admitted=admitted, held=held)
    r.put(root / 'reconciliation.json', reconciliation)
    config = {**r.core.DEFAULT_CONFIG, 'timeline_profile':'gemini_flash_batch',
        'max_chunk_input_bytes':24000, 'max_evidence_refs_per_item':256,
        'gemini_schema_policy':'local_array_bounds_v2', 'transcript_input_policy':'text_and_speaker_evidence_v1'}
    manifest = campaign.validate_manifest(dict(kind=campaign.KIND, schema_version=1,
        state_root=str(root / 'run'), shards=[[x] for x in sources], config=config,
        budget_microusd=120000000, max_active_shards=4, poll_seconds=30, max_runtime_seconds=1209600,
        cloud=dict(processing_approved=True, paid_tier_confirmed=True),
        classification_policy='conservative_evidence_inheritance_v1', implementation=campaign.implementation()))
    binding = r.put(root / 'manifest.json', manifest)
    report = dict(admitted=admitted, held=held, new_summary_recordings=len(sources), manifest=binding,
                  original_transcripts_modified=False, media_read=False)
    r.put(root / 'preparation.json', report)
    print(r.canonical(report).decode(), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True); p.add_argument('--preview', type=Path, required=True)
    p.add_argument('--root', type=Path, required=True)
    a = p.parse_args(); prepare(a.base.resolve(), a.preview.resolve(), a.root.resolve())
