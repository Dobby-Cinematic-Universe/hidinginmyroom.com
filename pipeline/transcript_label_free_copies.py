"""Reversible label-free cloud transcript snapshots, not paid-state mutations."""
import argparse
import copy
import json
import os
from pathlib import Path

from pipeline.transcript_audio_review import binding, read_bound, write_json


def project(doc, source_ref):
    result = copy.deepcopy(doc)
    labels = {s.get('speaker') for s in doc['segments'] if s.get('speaker') is not None}
    for segment in result['segments']:
        segment['speaker'] = None
    result['provider_speaker_labels'] = {}
    # The request flag is historical: do not falsely claim the API ran without diarization.
    result['kind'] = 'himr_label_free_transcript_copy'
    result['projection'] = {
        'kind': 'omit_unconfirmed_speaker_labels',
        'original_transcript': source_ref,
        'original_kind': doc['kind'],
        'original_label_count': len(labels),
        'text_changed': False,
        'segment_timestamps_changed': False,
        'segments_merged': False,
        'speaker_identity_inferred': False,
        'absence_of_labels_proves_single_speaker': False,
        'confirmed_named_participant_mapping': None,
        'live_pipeline_admitted': False,
    }
    return result


def run(campaign_path, recovery_path, output):
    plan_ref = binding(campaign_path)
    plan = read_bound(plan_ref)
    jobs = {r['job_id']: r for r in plan['recordings']}
    sources = sorted((Path(plan['state_root'])/'jobs').glob('*/transcript.json'))
    if recovery_path:
        recovery = read_bound(binding(recovery_path))
        sources.extend(Path(r['transcript']['path']) for r in recovery['recordings'])
    root = Path(output).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    entries = []
    seen = set()
    for source in sources:
        source_ref = binding(source)
        doc = read_bound(source_ref)
        job = doc['job_id']
        if job in seen or job not in jobs or doc['recording_id'] != jobs[job]['recording']['recording_id']:
            raise ValueError('duplicate or mismatched recording')
        seen.add(job)
        projected = project(doc, source_ref)
        folder = root/job
        folder.mkdir(mode=0o700)
        target = folder/'transcript.json'
        write_json(target, projected)
        # Check full text and every segment field other than the omitted label.
        reread = read_bound(binding(target))
        if reread['text'] != doc['text'] or len(reread['segments']) != len(doc['segments']):
            raise ValueError('transcript text or segmentation changed')
        for old, new in zip(doc['segments'], reread['segments']):
            if {k:v for k,v in old.items() if k!='speaker'} != {k:v for k,v in new.items() if k!='speaker'}:
                raise ValueError('segment content changed')
            if new['speaker'] is not None:
                raise ValueError('speaker label not removed')
        if binding(source) != source_ref:
            raise ValueError('source changed during copy')
        entries.append(dict(job_id=job, title=jobs[job]['recording']['title'],
            original=source_ref, copy=binding(target),
            original_label_count=projected['projection']['original_label_count'],
            recovered_copy='recovery' in doc))
    manifest = dict(kind='himr_label_free_transcript_snapshot', campaign=plan_ref,
        implementation=binding(__file__), transcripts=entries,
        copies=len(entries), labels_removed_from=sum(e['original_label_count']>0 for e in entries),
        multi_label_transcripts=sum(e['original_label_count']>1 for e in entries),
        two_confirmed_named_participant_exceptions=0,
        exception_reason='No full-transcript confirmed named participant mappings supplied.',
        new_paid_requests=0, originals_modified=False, live_pipeline_modified=False,
        future_results_included=False)
    write_json(root/'manifest.json', manifest)
    return {k:v for k,v in manifest.items() if k not in {'transcripts','implementation','campaign'}}


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--campaign', required=True)
    p.add_argument('--recovery')
    p.add_argument('--output', required=True)
    a=p.parse_args()
    os.umask(0o077)
    print(json.dumps(run(a.campaign, a.recovery, a.output)))
