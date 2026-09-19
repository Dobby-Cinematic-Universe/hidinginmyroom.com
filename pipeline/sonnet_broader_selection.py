"""Select existing preferred summaries without rebuilding transcripts or ASR."""
from collections import Counter
from pathlib import Path
import datetime as dt
from pipeline import transcript_summary as r


def physical(recording):
    return recording.split(':reviewed:',1)[0]


def date_for(entry, inventory):
    candidates = []
    for value in [entry.get('date_metadata'), inventory.get('date')]+[x.get('date') for x in inventory.get('aliases',[])]:
        if value and value.get('value'):
            dt.date.fromisoformat(value['value'])
            candidates.append(value)
    values = sorted({x['value'] for x in candidates})
    if len(values)==1:
        return dict(value=values[0],basis='retained_archive_metadata',
            source_bases=sorted({x['basis'] for x in candidates}),event_date_verified=False)
    return dict(value=None,basis='conflicting_metadata' if values else 'unknown',
        candidate_dates=values,event_date_verified=False)


def select(base, root):
    base,root = Path(base),Path(root)
    r.mkdir(root)
    r.mkdir(root/'records')
    refs = dict(length_index=r.binding(base.parent/'length-filtered-summaries-v1/index.json'),
        reviewed=r.binding(base.parent/'reviewed-transcript-feed-v1/index.json'),
        recovered=r.binding(base/'integrated-recovery-20260916/preferred-exports.json'),
        inventory=r.binding(base.parent/'inventory.json'),
        status=r.binding(base/'scheduler-efficiency-v1/status.json'))
    docs={k:r.read(v) for k,v in refs.items()}
    # Snapshot mutable aggregate indexes; the exact source export refs below are immutable.
    for key,value in docs.items():
        refs[key]=r.put(root/(key+'-snapshot.json'),value)
    inventory={x['recording_id']:x for x in docs['inventory']['recordings']}
    reviews={physical(x['recording_id']):x for x in docs['reviewed']['records']}
    short={x['recording_id'] for x in docs['status']['short_transcript_holds']}
    entries={}
    for path in (base/'entries').glob('*.json'):
        value=r.read(r.binding(path))
        entries[value['source']['recording_id']]=(r.binding(path),value)
    candidates={}
    omitted=[]
    for row in docs['length_index']['eligible_summaries']:
        rid=row['recording_id']
        if rid in short:
            continue
        if len(row['summary_exports'])!=1:
            raise r.Error('ambiguous final export selection')
        candidates[rid]=dict(export=row['summary_exports'][0],recovered=False)
    for row in docs['recovered']['records']:
        candidates[row['recording_id']]=dict(export=row['canonical'],recovered=True)
    chosen={}
    duplicates=0
    for rid,candidate in candidates.items():
        pid=physical(rid)
        review=reviews.get(pid)
        if review and not review['summary_eligible']:
            omitted.append(dict(recording_id=rid,reason='latest_review_not_summary_eligible'))
            continue
        entry_ref,entry=entries[rid]
        if entry['source']['format'] not in {'cloud','third_party'}:
            raise r.Error('historical local ASR must not enter synthesis')
        rank=1 if ':reviewed:' in rid else 0
        if pid in chosen:
            duplicates+=1
            if rank==chosen[pid]['rank']:
                raise r.Error('conflicting preferred summary revisions')
            if rank<chosen[pid]['rank']:
                continue
        chosen[pid]=dict(**candidate,entry=entry,entry_ref=entry_ref,rank=rank)
    records=[]
    for pid,value in sorted(chosen.items()):
        entry=value['entry'];source=entry['source']
        export=r.read(value['export'])
        plan=r.read(entry['plan'])
        if (export.get('phase_complete') is not True or (value['recovered'] and export['original_plan']!=entry['plan'])
                or (not value['recovered'] and export['plan_id']!=plan['plan_id'])):
            raise r.Error('selected final export does not belong to exact original plan')
        finals=[x for x in export['results'] if x['stage']=='transcript' and x['scope']['final']]
        if len(finals)!=1 or finals[0]['scope']['source_ids']!=plan['source_ids'] or len(plan['source_ids'])!=1:
            raise r.Error('selection needs one complete recording summary')
        result=finals[0]
        tid='transcript_'+r.digest(pid)[:24]
        sections={}
        for section,items in result['sections'].items():
            sections[section]=[]
            for item in items:
                if not item['citations'] or any(c['source_id']!=plan['source_ids'][0] for c in item['citations']):
                    raise r.Error('summary evidence escaped selected transcript')
                sections[section].append(dict(item_id=item['item_id'],text=item['text'],
                    classification=item['classification'],transcript_ids=[tid],
                    evidence=dict(summary_export=value['export'],item_id=item['item_id'])))
        document=dict(kind='himr_synthesis_transcript_summary_input',transcript_id=tid,
            recording_id=pid,summary_recording_id=source['recording_id'],source_id=plan['source_ids'][0],
            title=source.get('title') or inventory.get(pid,{}).get('title') or pid,
            date=date_for(source,inventory.get(pid,{})),sections=sections,source_export=value['export'],
            entry=value['entry_ref'],transcript=source['transcript'],original_plan=entry['plan'],
            summary_result_id=result['result_id'],recovered=value['recovered'])
        ref=r.put(root/'records'/(tid+'.json'),document)
        records.append(dict(transcript_id=tid,recording_id=pid,title=document['title'],date=document['date'],input=ref))
    counts=Counter((x['date']['value'] or 'unknown')[:7] for x in records)
    admitted={x['recording_id'] for x in records}
    pending=[dict(recording_id=rid,reason='no_completed_eligible_summary') for rid in entries
        if physical(rid) not in admitted and rid not in short
        and not (reviews.get(physical(rid)) and not reviews[physical(rid)]['summary_eligible'])]
    selection=dict(kind='himr_sonnet_broader_selection',records=records,metadata_snapshots=refs,
        physical_recordings=len(records),duplicate_summary_revisions_excluded=duplicates,
        omitted=omitted,pending=pending,groups=dict(sorted(counts.items())),
        source_policy='completed_third_party_or_cloud_summaries_prefer_reviewed_and_recovered',
        chronology='recording/publication metadata, not verified dates of narrated events',
        new_paid_requests=0,implementation=r.binding(__file__))
    ref=r.put(root/'selection.json',selection)
    print(r.canonical(dict(selection=ref,recordings=len(records),duplicate_revisions_excluded=duplicates,
        dated=sum(n for k,n in counts.items() if k!='unknown'),undated=counts.get('unknown',0),
        dated_months=len(counts)-('unknown' in counts),pending=len(pending))).decode(),flush=True)
    return ref


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base',required=True)
    p.add_argument('--root',required=True)
    a=p.parse_args()
    select(a.base,a.root)
