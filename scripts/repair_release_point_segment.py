"""Targeted, audited release-only point-timestamp normalization; no provider calls."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline import corpus_summary_preview as preview
from himr_corpus import sharded_release as shards


def normalize_point(doc, index):
    result = copy.deepcopy(doc)
    segment = result['segments'][index]
    start = segment['start_ms']
    if type(start) is not int or start < 0 or segment['end_ms'] != start:
        raise ValueError('Expected an exact nonnegative point timestamp')
    if index + 1 < len(result['segments']) and result['segments'][index+1]['start_ms'] <= start:
        raise ValueError('No room for a one-millisecond display interval')
    segment['end_ms'] = start + 1
    return result


def main():
    base = Path('research/corpus/site-previews').resolve()
    source = base / 'release-20260917-v8'
    target = base / 'release-20260917-v9'
    if target.exists(): raise ValueError('Target already exists; do not overwrite snapshots')
    target.mkdir(mode=0o700)
    read = lambda p: json.loads(p.read_text())
    def put(p, value):
        p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        p.write_bytes(shards._encoded(value)); p.chmod(0o600)
    leaf = read(Path('research/private-summaries/sonnet-broader-20260916-selection/records/transcript_91d3eb22aaf252ebb2923276.json'))
    ref = leaf['transcript']; original = Path(ref['path']).read_bytes()
    if hashlib.sha256(original).hexdigest() != ref['sha256']: raise ValueError('Changed original transcript')
    doc = read(Path(ref['path'])); repaired = normalize_point(doc, 275)
    if any(s['start_ms'] < 0 or s['end_ms'] <= s['start_ms'] for s in repaired['segments']): raise ValueError('Additional invalid timing')
    put(target/'inputs/release-only-timing-copy.json', repaired)
    corrected_ref = preview.io.binding(target/'inputs/release-only-timing-copy.json')
    manifest = read(source/'corpus/manifest.json')
    oldroot = source/'corpus/releases'/manifest['release_id']
    rid = 'rec_229d10f17e73cec62198b361c1462ada'
    changed = None
    for descriptor in manifest['catalog_shards']:
        catalog = read(oldroot/descriptor['path'])
        for index, row in enumerate(catalog['recordings']):
            if row['recording_id'] == rid:
                changed = (descriptor, catalog, index, read(oldroot/row['detail']['path']))
    if changed is None: raise ValueError('Target recording absent')
    descriptor, catalog, index, envelope = changed
    record = envelope['recording']
    if record['transcript_revisions']: raise ValueError('Refuse replacing an admitted transcript')
    revision = 'rev_'+preview.io.digest(dict(recording_id=rid,transcript=corrected_ref['sha256']))[:32]
    segments = [dict(segment_id=revision+'_'+str(n),start_ms=s['start_ms'],end_ms=s['end_ms'],text=s['text'],speaker_label=preview.speaker(s),confidence_band=None,calibrated_probability=None) for n,s in enumerate(repaired['segments']) if s['text'].strip()]
    record['review_state']='unreviewed'
    record['transcript_revisions']=[dict(revision_id=revision,revision_kind='raw_asr',language='en',review_state='machine',machine_generated=True,unreviewed=True,verified_quotation=False,disclaimer_code='machine_generated_unreviewed_not_verified_quotation_v1',lifecycle_state='active',lifecycle_history=[],segments=segments)]
    content=shards._encoded(envelope); digest=hashlib.sha256(content).hexdigest()
    detail=dict(path=f'recordings/{rid}-{digest[:16]}.json',sha256=digest,bytes=len(content))
    catalog['recordings'][index]=shards._summary(record,detail)
    content=shards._encoded(catalog); digest=hashlib.sha256(content).hexdigest()
    descriptor.update(path=f"catalog/catalog-{catalog['ordinal']:05d}-{digest[:16]}.json",sha256=digest,bytes=len(content))
    descriptor['transcript_revision_count']+=1; descriptor['segment_count']+=len(segments)
    manifest['counts']['transcript_revisions']+=1;manifest['counts']['segments']+=len(segments)
    manifest['stats']['transcript_revisions']+=1;manifest['stats']['searchable_transcript_segments']+=len(segments)
    manifest['release_id']=shards._manifest_release_id(manifest)
    newroot=target/'corpus/releases'/manifest['release_id']
    # Link only immutable shards. New content-addressed names never overwrite them.
    shutil.copytree(oldroot,newroot,copy_function=os.link)
    put(newroot/detail['path'],envelope);put(newroot/descriptor['path'],catalog)
    put(target/'corpus/manifest.json',manifest)
    annotations=read(source/'corpus/annotations.json')
    annotations['recordings'][rid]=dict(origin='cloud',attribution=None,model=doc.get('model'),speaker_review_complete=False,coverage_verified=False,coverage_note='One provider point timestamp retains its start and uses a 1 ms release-display interval; actual speech duration is unknown. Original transcript retained unchanged.')
    put(target/'corpus/annotations.json',annotations)
    mapping=read(source/'identity-map.json')
    mapping['records'].append(dict(recording_id=leaf['recording_id'],public_recording_id=rid,transcript=corrected_ref,selected_summary=leaf['source_export']))
    put(target/'identity-map.json',mapping)
    for name in ('release.json','refreshed.json'):
        summary=read(source/'summaries'/name)
        summary['summaries'].append(preview.summary_row(leaf,rid))
        summary['release_id']='summaries_'+preview.io.digest(summary['summaries'])[:24]
        put(target/'summaries'/name,summary)
    report=read(source/'preparation.json')
    report['excluded']=[r for r in report['excluded'] if r['recording_id']!=leaf['recording_id']]
    report['counts'].update(manifest['counts'])
    report['counts']['transcript_summaries']+=1
    report['corpus_manifest']=preview.io.binding(target/'corpus/manifest.json')
    report['summary_release']=preview.io.binding(target/'summaries/release.json')
    report['release_only_timing_repair']=dict(original=ref,corrected=corrected_ref,segment_index=275,original_start_ms=4020120,original_end_ms=4020120,display_end_ms=4020121,actual_duration_known=False,text_changed=False,paid_requests=0,approval='User requested unblocking this transcript for release.')
    put(target/'preparation.json',report)
    print(json.dumps(dict(snapshot=str(target),recording_id=rid,segments=len(segments),original_unchanged=hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256'],repair=report['release_only_timing_repair'])))

if __name__ == '__main__': main()
