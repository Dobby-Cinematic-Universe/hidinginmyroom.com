"""Prepare a private, development-only corpus/summary projection. No paid calls.

Never writes publication decisions or installs files under src/data or public.
Original machine text and segment timestamps are retained without claiming a
verbatim human review. Speaker review is separate from transcription accuracy.
"""
import argparse
from collections import Counter
import datetime as dt
import hashlib
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit
from pipeline import transcript_summary as io
from pipeline import sonnet_broader_selection as selection
from pipeline import reviewed_transcript_feed as files

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'corpus/src'))
from himr_corpus.sharded_release import export_release_v2_from_release
from himr_corpus.exporter import _release_payload
from himr_corpus.importers import canonical_json


def public_id(physical):
    return 'rec_'+io.digest(dict(namespace='cloud-third-party-preview-v1',physical=physical))[:32]


def speaker(segment):
    source=segment.get('audio_source')
    if source and source!='participant':
        return {'playback':'Playback / game audio','background_noise':'Background noise',
            'tts':'Text to speech','uncertain':'Uncertain audio source'}.get(source,source.replace('_',' ').capitalize())
    name=segment.get('speaker_name') or segment.get('saved_name') or segment.get('speaker')
    if segment.get('attribution_uncertainty'):
        return (str(name)+' (uncertain)') if name else 'Uncertain participant'
    return name


def project_record(meta,doc,ref,date):
    rid=public_id(meta['recording_id'])
    if doc.get('status')!='completed' or doc.get('kind') not in {
        'himr_third_party_transcript_import','himr_cloud_recording_transcript','himr_reviewed_transcript_copy'}:
        raise ValueError('not a completed cloud or third-party transcript')
    if selection.physical(doc['recording_id'])!=meta['recording_id']:
        raise ValueError('transcript physical identity differs')
    sources={}
    for alias in meta['aliases']:
        url=alias.get('canonical_url','');parsed=urlsplit(url)
        if (parsed.scheme!='https' or parsed.hostname not in {'archive.org','www.youtube.com','youtube.com','youtu.be'}
                or parsed.query or parsed.username or parsed.password):
            # YouTube watch IDs may be in the query, but only use the explicit retained ID.
            if alias.get('platform')=='youtube' and re.fullmatch(r'[A-Za-z0-9_-]{11}',alias.get('youtube_id','')):
                url='https://www.youtube.com/watch?v='+alias['youtube_id']
            else:continue
        sid='src_'+io.digest(dict(platform=alias['platform'],url=url))[:32]
        sources[sid]=dict(source_id=sid,platform=alias['platform'],url=url,
            native_id=alias['source_native_id'],access_state='public')
    if not sources:raise ValueError('no retained public source URL')
    revision='rev_'+io.digest(dict(recording_id=rid,transcript=ref['sha256']))[:32]
    segments=[]
    for n,seg in enumerate(doc['segments']):
        if not isinstance(seg.get('text'),str):raise ValueError('invalid transcript text')
        if not seg['text'].strip():continue
        start,end=seg.get('start_ms'),seg.get('end_ms')
        if type(start)!=int or type(end)!=int or start<0 or end<=start:
            raise ValueError('invalid segment timing; no invented timestamps')
        segments.append(dict(segment_id=revision+'_'+str(n),start_ms=start,end_ms=end,text=seg['text'],
            speaker_label=speaker(seg),confidence_band=None,calibrated_probability=None))
    if not segments:raise ValueError('empty transcript')
    if any(a['start_ms']>b['start_ms'] for a,b in zip(segments,segments[1:])):raise ValueError('out-of-order transcript timing')
    rev=dict(revision_id=revision,revision_kind='raw_asr',language='en',review_state='machine',
        machine_generated=True,unreviewed=True,verified_quotation=False,
        disclaimer_code='machine_generated_unreviewed_not_verified_quotation_v1',
        lifecycle_state='active',lifecycle_history=[],segments=segments)
    return dict(recording_id=rid,slug=rid.replace('_','-'),title=meta['title'],
        date_label=date['value'],date_year=int(date['value'][:4]) if date['value'] else None,
        date_basis=date['basis'],duration_ms=meta.get('duration_ms'),recording_type='unknown',
        review_state='unreviewed',sources=list(sources.values()),transcript_revisions=[rev])


def metadata_record(meta,date):
    # Reuse the same URL allowlist and public identity without inventing transcript data.
    sources={}
    for alias in meta['aliases']:
        url=alias.get('canonical_url','');parsed=urlsplit(url)
        if (parsed.scheme!='https' or parsed.hostname not in {'archive.org','www.youtube.com','youtube.com','youtu.be'}
                or parsed.query or parsed.username or parsed.password):
            if alias.get('platform')=='youtube' and re.fullmatch(r'[A-Za-z0-9_-]{11}',alias.get('youtube_id','')):
                url='https://www.youtube.com/watch?v='+alias['youtube_id']
            else:continue
        sid='src_'+io.digest(dict(platform=alias['platform'],url=url))[:32]
        sources[sid]=dict(source_id=sid,platform=alias['platform'],url=url,native_id=alias['source_native_id'],access_state='public')
    if not sources:raise ValueError('no retained public source URL')
    rid=public_id(meta['recording_id'])
    return dict(recording_id=rid,slug=rid.replace('_','-'),title=meta['title'],date_label=date['value'],
        date_year=int(date['value'][:4]) if date['value'] else None,date_basis=date['basis'],
        duration_ms=meta.get('duration_ms'),recording_type='unknown',review_state='metadata_only',
        sources=list(sources.values()),transcript_revisions=[])


def summary_row(doc,rid):
    return dict(id='recording-'+rid[4:],kind='transcript',period=(doc['date']['value'] or '')[:7] or None,
        title=doc['title'],recording_id=rid,publication='prepared',
        sections={section:[dict(text=i['text'],classification=i['classification'],source_recording_ids=[rid])
            for i in items] for section,items in doc['sections'].items()})


def prepare(base,selection_path,broader,root,reconciled=None,reviewed_feed=None):
    if root.resolve().parent!= (ROOT/'research/corpus/site-previews').resolve():
        raise ValueError('preview must be a fresh child of the private site-previews directory')
    if root.exists():raise ValueError('use a fresh snapshot directory')
    root.parent.mkdir(mode=0o700,exist_ok=True)
    io.mkdir(root)
    for part in ('inputs','corpus','summaries'):io.mkdir(root/part)
    snapshots={}
    def snapshot(name,path):
        value=io.read(io.binding(path));snapshots[name]=io.put(root/'inputs'/(name+'.json'),value);return value
    plan=snapshot('plan',base/'transcription-v5/plan.json')
    reviewed=snapshot('reviewed',base/'reviewed-transcript-feed-v1/index.json')
    selected=snapshot('selection',selection_path)
    inventory={x['recording']['recording_id']:x['recording'] for x in plan['recordings']}
    reviews={selection.physical(x['recording_id']):x for x in reviewed['records']}
    if reviewed_feed:
        extra_reviews=snapshot('additional-review',reviewed_feed)
        reviews.update({selection.physical(x['recording_id']):x for x in extra_reviews['records']})
    leaves={}
    candidates={}
    for row in plan['recordings']:
        pid=row['recording']['recording_id'];imported=row.get('import')
        if imported and imported.get('transcript'):candidates[pid]=imported['transcript']
        p=base/'transcription-v5/jobs'/row['job_id']/'transcript.json'
        if p.exists():candidates[pid]=io.binding(p)
    for row in selected['records']:
        leaf=io.read(row['input']);leaves[leaf['recording_id']]=leaf
        candidates[leaf['recording_id']]=leaf['transcript']
    if reconciled:
        extra=snapshot('reconciliation',reconciled)
        if extra['kind']!='himr_metadata_reconciliation':raise ValueError('invalid reconciliation index')
        for row in extra['records']:
            if not row['needs_speaker_review']:candidates[row['recording_id']]=row['transcript']
    for pid,row in reviews.items():
        if row['review_complete']:candidates[pid]=row['transcript']
        else:candidates.pop(pid,None)
    records=[];summaries=[];mapping=[];excluded=[];annotations={};retained=set();tids={};counts=Counter()
    for pid in sorted(inventory):
        meta=inventory[pid];ref=candidates.get(pid);leaf=leaves.get(pid);review=reviews.get(pid)
        if ref is None:
            excluded.append(dict(recording_id=pid,reason='speaker_review_incomplete' if review else 'no_completed_preferred_transcript'));continue
        try:
            doc=io.read(ref)
            if doc.get('diarization_requested') and len(set(s.get('speaker') for s in doc['segments'] if s.get('speaker')))>1 and not review:
                raise ValueError('anonymous multi-speaker transcript needs review')
            date=leaf['date'] if leaf else selection.date_for({},meta)
            record=project_record(meta,doc,ref,date)
        except (ValueError,RuntimeError,KeyError) as error:
            excluded.append(dict(recording_id=pid,reason=str(error)[:250]));continue
        rid=record['recording_id'];records.append(record);retained.add(pid)
        origin='third_party' if doc['kind']=='himr_third_party_transcript_import' else 'cloud'
        counts[origin]+=1
        annotations[rid]=dict(origin=origin,
            attribution=doc.get('provenance',{}).get('attribution'),
            model=doc.get('model') or doc.get('provenance',{}).get('label'),
            speaker_review_complete=bool(review and review['review_complete']),
            coverage_verified=False,coverage_note=doc.get('reconciliation',{}).get('coverage_note'))
        mapping.append(dict(recording_id=pid,public_recording_id=rid,transcript=ref,
            selected_summary=leaf['source_export'] if leaf else None))
        if leaf:
            summaries.append(summary_row(leaf,rid));tids[leaf['transcript_id']]=rid
        else:counts['transcripts_without_summary']+=1
    metadata_excluded=[]
    for exclusion in excluded:
        pid=exclusion['recording_id'];meta=inventory[pid]
        try:record=metadata_record(meta,selection.date_for({},meta))
        except ValueError as error:
            metadata_excluded.append(dict(recording_id=pid,reason=str(error)));continue
        records.append(record);counts['metadata_only']+=1
    broader_excluded=[]
    if (broader/'reader/index.json').exists():
        index=snapshot('broader',broader/'reader/index.json')
        for row in index['summaries']:
            result=io.read(row['canonical'])['result']
            missing=sorted({tid for items in result['sections'].values() for item in items for tid in item['transcript_ids'] if tid not in tids})
            if missing:
                broader_excluded.append(dict(stage=row['stage'],period=row['period'],missing_transcript_ids=missing));continue
            period=row['period'] if row['period'] not in {'unknown','selected-archive'} else None
            summaries.append(dict(id=row['stage']+'-'+(period or 'undated' if row['stage']!='archive' else 'overview'),
                kind=row['stage'],period=period,title='Archive overview' if row['stage']=='archive' else (period or 'Undated recordings')+' summary',
                recording_id=None,publication='prepared',sections={section:[dict(text=i['text'],classification=i['classification'],
                    source_recording_ids=sorted({tids[t] for t in i['transcript_ids']})) for i in items] for section,items in result['sections'].items()}))
    generated=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    totals=dict(recordings=len(records),sources=sum(len(x['sources']) for x in records),
        transcript_revisions=sum(len(x['transcript_revisions']) for x in records),
        segments=sum(len(rev['segments']) for x in records for rev in x['transcript_revisions']))
    release=dict(schema_version=1,release_id='release_'+io.digest(dict(mapping=mapping,generated=generated))[:24],generated_at=generated,counts=totals,recordings=records)
    release['release_id']='release_'+hashlib.sha256(canonical_json(_release_payload(release)).encode()).hexdigest()[:24]
    exported=export_release_v2_from_release(release,root/'corpus')
    io.put(root/'corpus/annotations.json',dict(schema_version=1,recordings=annotations))
    summary_release=dict(schema_version=1,release_id='summaries_'+io.digest(summaries)[:24],generated_at=generated,summaries=summaries)
    io.put(root/'summaries/release.json',summary_release)
    io.put(root/'identity-map.json',dict(records=mapping))
    report=dict(kind='himr_private_site_preview',generated_at=generated,inputs=snapshots,
        counts={**totals,**counts,'transcript_summaries':sum(x['kind']=='transcript' for x in summaries),
            'broader_summaries':sum(x['kind']!='transcript' for x in summaries)},excluded=excluded,
        broader_excluded=broader_excluded,metadata_excluded=metadata_excluded,publication_approved=False,paid_requests=0,
        remaining_publication_gates=['rights','privacy','sensitivity','public_source_availability'],
        corpus_manifest=io.binding(root/'corpus/manifest.json'),summary_release=io.binding(root/'summaries/release.json'))
    proof=io.put(root/'preparation.json',report)
    return dict(report=proof,counts=report['counts'],excluded=len(excluded),root=str(root))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base',type=Path,required=True);p.add_argument('--selection',type=Path,required=True)
    p.add_argument('--broader',type=Path,required=True);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--reconciled',type=Path);p.add_argument('--reviewed-feed',type=Path)
    a=p.parse_args();print(io.canonical(prepare(a.base,a.selection,a.broader,a.root,a.reconciled,a.reviewed_feed)).decode())

if __name__=='__main__':main()
