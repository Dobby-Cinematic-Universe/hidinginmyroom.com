import { createHash } from 'node:crypto';
import { knownSpeakerLabel } from './presentation.mjs';

const hash = (s) => createHash('sha256').update(s).digest('hex').slice(0,24);
const key = (s) => s.normalize('NFKC').trim().replace(/\s+/g,' ').toLowerCase();
const escape = (s) => s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
const nonPeople = /^(?:background noise|playback(?: \/ game audio)?|game audio|text to speech|tts|music|uncertain audio source)$/i;
const places = ['Japan','Thailand','England','United Kingdom','South Korea','Korea','China','Taiwan','Hong Kong','Germany','Canada','Australia','Tokyo','Osaka','Kyoto','Bangkok','London'];
const platforms = ['YouTube','Twitch','Patreon','Discord','Reddit','TikTok','Instagram'];

export function personLabel(value) {
  const label=knownSpeakerLabel(value);
  return label && !nonPeople.test(label) ? label.replace(/\s*\(uncertain\)$/i,'') : null;
}

// These are literal date mentions, not an inferred date of the described event.
export function dateMentions(text) {
  return [...new Set(text.match(/\b(?:19|20)\d{2}(?:-\d{2}(?:-\d{2})?)?\b|\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,?\s+(?:19|20)\d{2})?\b/g)||[])];
}

/** Pure derived layer: no review decisions, source edits, or identity inference. */
export function createDerivedGraph(catalog,summaries) {
  const entities=new Map(), events=new Map();
  const recordings=new Map(catalog.recordings.map(r=>[r.recording_id,r]));
  function addEntity(label,type) {
    label=label.trim();const k=key(label);
    if (!entities.has(k)) entities.set(k,{entity_id:'entity_'+hash(k),slug:'entity-'+hash(k),label,
      entity_type:type,mentions:new Map(),speakerRecordings:new Map()});
  }
  for(const label of catalog.facets.speakers) {const name=personLabel(label);if(name)addEntity(name,'person');}
  // Daniel remains a mention candidate even for intentionally unlabeled transcripts.
  addEntity('Daniel','person');
  for(const label of places)addEntity(label,'place');
  for(const label of platforms)addEntity(label,'platform');
  const sourceSummaries=summaries.filter(s=>s.kind==='transcript');
  const discovered=new Map();
  // Repeated capitalized name-like phrases are candidates, not asserted identities.
  // Avoid guessing aliases or merging people with similar names.
  for(const summary of sourceSummaries) {
    const seen=new Set();
    for(const item of Object.values(summary.sections).flat()) {
      for(const match of item.text.matchAll(/\b[A-Z][a-z]{2,}(?:[ '-][A-Z][a-z]{2,}){1,2}\b/g)) {
        const label=match[0];
        if (/^(?:The|This|That|His|Her|Their|When|After|Before|During|According|Last|Next|One|Two|Three|First|Second|Whether|Why|How|What|Which|Who|Where|If|While|Despite|Both|Did|Does|Will|Would|Could|Should|Can|Some|Several|Many|Most|More|Other|Only|Any|All|Still|Also|About|Around|As|At|By|For|From|In|Into|Is|It|Its|On|Since|To|Until|With|Without|No|Not)\b/.test(label))continue;
        if (/\b(?:January|February|March|April|May|June|July|August|September|October|November|December|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b/.test(label))continue;
        seen.add(label);
      }
      for(const match of item.text.matchAll(/\b(?:named|called|friend|advisor|instructor|coworker)\s+([A-Z][a-z]{2,})\b/g))seen.add(match[1]);
    }
    for(const label of seen) {
      if(!discovered.has(label))discovered.set(label,new Set());
      discovered.get(label).add(summary.recording_id);
    }
  }
  for(const [label,ids] of discovered)if(ids.size>=2)addEntity(label,'unknown');
  const matcher=new RegExp(`(?<![\\p{L}\\p{N}_])(?:${[...entities.values()].map(e=>e.label).sort((a,b)=>b.length-a.length).map(escape).join('|')})(?![\\p{L}\\p{N}_])`,'gu');
  function mentions(text) {return new Set([...text.matchAll(matcher)].map(m=>key(m[0])));}
  function link(id) {
    const r=recordings.get(id);if(!r)return null;
    return {recording_id:id,slug:r.slug,title:r.title,recording_date:r.date_label,date_basis:r.date_basis};
  }
  function addMention(k,id,basis) {
    const ref=link(id);if(!ref)return;
    const e=entities.get(k),old=e.mentions.get(id);
    e.mentions.set(id,{...ref,basis:[...new Set([...(old?.basis||[]),basis])]});
  }
  for(const summary of sourceSummaries) {
    for(const item of Object.values(summary.sections).flat()) {
      for(const k of mentions(item.text))for(const id of item.source_recording_ids)addMention(k,id,'summary');
    }
    for(const item of summary.sections.events) {
      const ids=item.source_recording_ids.filter(id=>recordings.has(id));if(!ids.length)continue;
      const id='event_'+hash(key(item.text)+'|'+item.classification);
      if(!events.has(id))events.set(id,{event_id:id,slug:id.replace('_','-'),label:item.text.length>170?item.text.slice(0,167)+'…':item.text,
        description:item.text,classification:item.classification,sourceRecordings:new Map(),
        sourceSummaryIds:new Set(),mentionedEntities:[...mentions(item.text)].map(k=>entities.get(k).entity_id),
        dateMentions:dateMentions(item.text)});
      const event=events.get(id);event.sourceSummaryIds.add(summary.id);
      for(const id of ids)event.sourceRecordings.set(id,link(id));
    }
  }
  return {
    addRecording(record) {
      const text=record.transcript_revisions.flatMap(r=>r.segments.map(s=>s.text)).join('\n');
      for(const k of mentions(text))addMention(k,record.recording_id,'transcript');
      const labels=new Set(record.transcript_revisions.flatMap(r=>r.segments.map(s=>personLabel(s.speaker_label)).filter(Boolean)));
      for(const label of labels) {
        const e=entities.get(key(label)),ref=link(record.recording_id);
        if(e&&ref)e.speakerRecordings.set(record.recording_id,ref);
      }
    },
    finish() {
      const sortLinks=xs=>[...xs.values()].sort((a,b)=>(b.recording_date||'').localeCompare(a.recording_date||'')||a.title.localeCompare(b.title));
      const es=[...entities.values()].filter(e=>e.mentions.size||e.speakerRecordings.size)
        .map(e=>({...e,mentions:sortLinks(e.mentions),speakerRecordings:sortLinks(e.speakerRecordings)}))
        .sort((a,b)=>b.mentions.length+b.speakerRecordings.length-a.mentions.length-a.speakerRecordings.length||a.label.localeCompare(b.label));
      const vs=[...events.values()].map(e=>({...e,sourceRecordings:sortLinks(e.sourceRecordings),sourceSummaryIds:[...e.sourceSummaryIds]}))
        .sort((a,b)=>(b.sourceRecordings[0]?.recording_date||'').localeCompare(a.sourceRecordings[0]?.recording_date||'')||a.event_id.localeCompare(b.event_id));
      return {schemaVersion:1,releaseId:'derived_'+hash(catalog.releaseId+JSON.stringify(sourceSummaries)),generatedAt:catalog.generatedAt,
        derived:true,counts:{entities:es.length,events:vs.length,appearances:es.reduce((n,e)=>n+e.speakerRecordings.length,0),
          event_participants:0,event_dates:0,event_relations:0,event_evidence:vs.reduce((n,e)=>n+e.sourceRecordings.length,0)},
        entities:es,events:vs,appearances:[],eventParticipants:[],eventDates:[],eventRelations:[],eventEvidence:[]};
    },
  };
}
