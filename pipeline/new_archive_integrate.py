"""Add one completed recording without rereading historical transcript shards."""
import datetime as dt
import os
import json
import hashlib
from pathlib import Path
from pipeline import new_archive_recovery as recovery
from pipeline import corpus_summary_preview as preview
from himr_corpus import sharded_release as shards

r=recovery.r
BASE=Path('research/corpus/site-previews/release-20260917-v9').resolve()
ROOT=BASE.parent/'release-20260918-v10'


def public_read(path,sha256=None):
    raw=path.read_bytes()
    if sha256 and hashlib.sha256(raw).hexdigest()!=sha256:raise ValueError('Changed public catalog')
    return json.loads(raw)


def main():
    completed=r.read(r.binding(recovery.ROOT/'completed.json'))
    doc=r.read(completed['transcript'])
    meta=r.read(r.binding(recovery.original.ROOT/'recording.json'))
    record=preview.project_record(meta,doc,completed['transcript'],dict(value='2026-09-17',basis='archive_filename'))
    generated=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    # Validate the new detail independently; retain old immutable details as-is.
    single=shards._single_record_release(record,generated)
    shards.validate_release_shape(single)
    manifest=r.read(r.binding(BASE/'corpus/manifest.json'))
    oldroot=BASE/'corpus/releases'/manifest['release_id']
    catalog=[]
    for ref in manifest['catalog_shards']:
        catalog.extend(public_read(oldroot/ref['path'],ref['sha256'])['recordings'])
    if any(x['recording_id']==record['recording_id'] for x in catalog):raise ValueError('Recording already included')
    r.mkdir(ROOT);r.mkdir(ROOT/'corpus');r.mkdir(ROOT/'summaries');r.mkdir(ROOT/'corpus/releases')
    work=ROOT/'corpus/new-item-staging';r.mkdir(work)
    envelope=dict(schema_version=2,kind='recording',recording=record)
    detail_path='recordings/'+record['recording_id']+'-'+shards._sha256(shards._encoded(envelope))[:16]+'.json'
    detail=dict(path=detail_path,**shards._write_file(work/detail_path,envelope))
    catalog.append(shards._summary(record,detail));catalog.sort(key=shards._recording_sort_key)
    descriptors=[]
    size=manifest['catalog_shard_size']
    for ordinal,start in enumerate(range(0,len(catalog),size)):
        batch=catalog[start:start+size]
        payload=dict(schema_version=2,kind='catalog',ordinal=ordinal,recordings=batch)
        relative=f'catalog/catalog-{ordinal:05d}-{shards._sha256(shards._encoded(payload))[:16]}.json'
        descriptors.append(dict(path=relative,**shards._write_file(work/relative,payload),recording_count=len(batch),source_count=sum(x['source_count'] for x in batch),transcript_revision_count=sum(x['transcript_revision_count'] for x in batch),segment_count=sum(x['segment_count'] for x in batch),first_recording_id=batch[0]['recording_id'],last_recording_id=batch[-1]['recording_id']))
    for item in catalog:
        ref=item['detail']
        if item['recording_id']==record['recording_id']:continue
        target=work/ref['path']
        if not target.exists():os.link(oldroot/ref['path'],target)
    stats,facets=shards._derived_release_metadata([record])
    counts={k:manifest['counts'][k]+single['counts'][k] for k in manifest['counts']}
    updated={**manifest,'generated_at':generated,'counts':counts,'catalog_shards':descriptors,
        'stats':{k:manifest['stats'][k]+stats[k] for k in stats},
        'facets':{k:sorted(set(manifest['facets'][k])|set(facets[k]),reverse=k=='years') for k in facets}}
    updated['release_id']=shards._manifest_release_id(updated)
    target=ROOT/'corpus/releases'/updated['release_id']
    if not target.exists():work.rename(target)
    r.put(ROOT/'corpus/manifest.json',updated)
    annotations=r.read(r.binding(BASE/'corpus/annotations.json'))
    annotations['recordings'][record['recording_id']]=dict(origin='cloud',attribution=None,model=doc.get('model'),speaker_review_complete=False,coverage_verified=False,coverage_note=None)
    r.put(ROOT/'corpus/annotations.json',annotations)
    mapping=r.read(r.binding(BASE/'identity-map.json'))
    mapping['records'].append(dict(recording_id=meta['recording_id'],public_recording_id=record['recording_id'],transcript=completed['transcript'],selected_summary=completed['reader']))
    r.put(ROOT/'identity-map.json',mapping)
    summary_path=BASE/'summaries/refreshed.json'
    if not summary_path.exists():summary_path=BASE/'summaries/release.json'
    summaries=r.read(r.binding(summary_path))
    leaf=r.read(completed['reader'])['records'][0]
    summaries['summaries'].append(preview.summary_row(leaf,record['recording_id']))
    summaries.update(generated_at=generated,release_id='summaries_'+r.digest(summaries['summaries'])[:24])
    r.put(ROOT/'summaries/release.json',summaries)
    r.put(ROOT/'summaries/refreshed.json',summaries)
    report=r.read(r.binding(BASE/'preparation.json'))
    report.update(generated_at=generated,corpus_manifest=r.binding(ROOT/'corpus/manifest.json'),summary_release=r.binding(ROOT/'summaries/release.json'),incremental_source=completed,publication_approved=False)
    report['counts'].update(counts,transcript_summaries=sum(x['kind']=='transcript' for x in summaries['summaries']),broader_summaries=sum(x['kind']!='transcript' for x in summaries['summaries']))
    report['counts']['cloud']+=1
    r.put(ROOT/'preparation.json',report)
    print(r.canonical(dict(root=str(ROOT),recording_id=record['recording_id'],counts=report['counts'])).decode())


if __name__=='__main__':main()
