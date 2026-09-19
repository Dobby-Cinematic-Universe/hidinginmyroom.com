import {createHash} from 'node:crypto';
import {eventHref} from './event-links.mjs';
const digest=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
export function groupingInput(graph) {
  return {schema_version:1,events:graph.events.map(e=>({id:e.event_id,text:e.description,
    entities:e.mentionedEntities||[],dates:e.dateMentions||[],recordings:e.sourceRecordings.map(r=>r.recording_id)})).sort((a,b)=>a.id.localeCompare(b.id)),
    entities:graph.entities.map(e=>({id:e.entity_id,label:e.label,type:e.entity_type})).sort((a,b)=>a.id.localeCompare(b.id))};
}
export function groupingDigest(input) {return digest(input.events);}

export function attachEventGroups(graph,artifact) {
  if(artifact.schema_version!==1||artifact.kind!=='himr_local_related_accounts'||!Array.isArray(artifact.groups))throw new Error('Invalid event grouping artifact');
  const input=groupingInput(graph);
  if(artifact.input_sha256!==groupingDigest(input))return {...graph,groupingStale:true};
  const events=new Map(graph.events.map(e=>[e.event_id,e])),seen=new Set();
  const groups=artifact.groups.map(g=>{
    if(!/^accounts_[a-f0-9]{24}$/.test(g.id)||!Array.isArray(g.members)||g.members.length<2||!g.members.includes(g.representative))throw new Error('Invalid related-account group');
    if(g.id!=='accounts_'+digest([...g.members].sort()).slice(0,24))throw new Error('Group membership identity mismatch');
    const members=g.members.map(id=>{if(!events.has(id)||seen.has(id))throw new Error('Unknown or overlapping event group member');seen.add(id);return events.get(id);});
    const sources=[...new Map(members.flatMap(e=>e.sourceRecordings.map(r=>[r.recording_id,r]))).values()];
    const representative=events.get(g.representative);
    return {id:g.id,slug:g.id.replace('_','-'),label:representative.label,representativeId:g.representative,
      members:members.map(e=>e.event_id),sourceRecordings:sources,
      mentionedEntities:[...new Set(members.flatMap(e=>e.mentionedEntities||[]))],
      sourceSummaryIds:[...new Set(members.flatMap(e=>e.sourceSummaryIds||[]))]};
  }).sort((a,b)=>Number(b.sourceRecordings.length>1)-Number(a.sourceRecordings.length>1)||b.sourceRecordings.length-a.sourceRecordings.length||b.members.length-a.members.length||a.id.localeCompare(b.id));
  return {...graph,eventGroups:groups,groupingModel:artifact.model,groupingStale:false};
}

export function eventDirectoryItems(graph,entityId='') {
  const groups=graph.eventGroups||[],grouped=new Set(groups.flatMap(g=>g.members));
  return [
    ...groups.filter(g=>!entityId||g.mentionedEntities.includes(entityId)).map(g=>({sourceCount:g.sourceRecordings.length,related:true,href:`/corpus/event-groups/${g.slug}/`,title:g.label,detail:`Related accounts · ${g.sourceRecordings.length} source recordings · ${g.members.length} original descriptions`})),
    ...graph.events.filter(e=>!grouped.has(e.event_id)&&(!entityId||e.mentionedEntities?.includes(entityId))).map(e=>({sourceCount:e.sourceRecordings.length,related:false,href:eventHref(e.slug),title:e.label,detail:`${e.sourceRecordings?.[0]?.recording_date||'Recording date unknown'} · ${e.sourceRecordings.length} source recordings · Ungrouped description`}))
  ].sort((a,b)=>Number(b.sourceCount>1)-Number(a.sourceCount>1)||Number(b.related)-Number(a.related)||b.sourceCount-a.sourceCount);
}
