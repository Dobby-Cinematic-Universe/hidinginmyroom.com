// Three small reader sidecars beside the catalog. Each is machine-derived and says so where it is shown.
//   search aliases      other spellings of a game, product or place name found in the machine transcripts
//   related recordings  a recording that names an earlier recording ("refers back to")
//   chapters            machine-detected topic shifts in long recordings, with a few frequent words each
const FORM=/^[a-z0-9à-ÿ][a-z0-9à-ÿ '\-]{1,38}$/,WORD=/^[a-z][a-z'\-]{2,24}$/,ID=/^rec_[a-f0-9]{32}$/;
const only=(o,allowed,what)=>{for(const k of Object.keys(o))if(!allowed.includes(k))throw Error(`Unexpected ${what} field ${k}`);};

export function projectSearchAliases(input){
  const sets=[];const seen=new Set();
  for(const row of input?.sets||[]){
    only(row,['canonical','category','forms'],'alias set');
    if(typeof row.canonical!=='string'||!row.canonical||row.canonical.length>60||!['game','product','place'].includes(row.category))throw Error('Invalid alias set');
    if(!Array.isArray(row.forms)||row.forms.length<2||row.forms.length>12||!row.forms.every(f=>typeof f==='string'&&FORM.test(f)))throw Error('Invalid alias forms');
    for(const f of row.forms){if(seen.has(f))throw Error('Alias form appears in two sets');seen.add(f);}
    sets.push({canonical:row.canonical,forms:[...row.forms]});
  }
  return {schema_version:1,sets};
}
// Other queries to run for a query that contains a known spelling. At most `limit` are returned.
export function expandQuery(query,sets,limit=6){
  const q=` ${String(query||'').toLowerCase().normalize('NFC').replace(/[^\p{L}\p{N}'\-]+/gu,' ').trim()} `;
  if(q.trim().length<3)return [];
  const out=[];
  for(const set of sets||[]){
    const hit=set.forms.filter(f=>q.includes(` ${f} `)).sort((a,b)=>b.length-a.length)[0];
    if(!hit)continue;
    for(const f of set.forms){
      if(f===hit||out.length>=limit)continue;
      out.push({query:q.replace(` ${hit} `,` ${f} `).trim(),form:f,canonical:set.canonical});
    }
  }
  return out;
}

export function projectRelatedRecordings(input,catalog){
  const ids=new Set(catalog.map(r=>r.recording_id));const recordings={};
  for(const [id,rows] of Object.entries(input?.recordings||{})){
    if(!ids.has(id))continue;
    if(!Array.isArray(rows)||rows.length>12)throw Error('Invalid related recordings');
    const kept=[];
    for(const row of rows){
      only(row,['target','position_ms'],'related recording');
      if(!ID.test(row.target||'')||row.target===id)throw Error('Invalid related recording target');
      if(row.position_ms!==null&&!(Number.isInteger(row.position_ms)&&row.position_ms>=0))throw Error('Invalid related recording position');
      if(ids.has(row.target))kept.push({target:row.target,position_ms:row.position_ms});
    }
    if(kept.length)recordings[id]=kept;
  }
  return {schema_version:1,recordings};
}

export function projectChapters(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));const recordings={};
  for(const [id,rows] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);if(!target||!target.searchable_segment_count)continue;
    if(!Array.isArray(rows)||rows.length<2||rows.length>80)throw Error('Invalid chapter list');
    let previous=0;const kept=[];
    for(const row of rows){
      only(row,['start_ms','end_ms','terms'],'chapter');
      if(!Number.isInteger(row.start_ms)||!Number.isInteger(row.end_ms)||row.start_ms<previous||row.end_ms<=row.start_ms)throw Error('Invalid chapter times');
      if(!Array.isArray(row.terms)||row.terms.length>4||!row.terms.every(t=>typeof t==='string'&&WORD.test(t)))throw Error('Invalid chapter terms');
      previous=row.end_ms;kept.push({start_ms:row.start_ms,end_ms:row.end_ms,terms:[...row.terms]});
    }
    recordings[id]=kept;
  }
  return {schema_version:1,recordings};
}

// Stretches of months in which the transcripts name a game, product or platform unusually often.
export function projectMentionBursts(input){
  const bursts=[];
  for(const row of input?.bursts||[]){
    only(row,['subject','type','start','end','months','ratio','recordings','end_known','both_transcript_kinds'],'burst');
    const month=v=>typeof v==='string'&&/^\d{4}-(0[1-9]|1[0-2])$/.test(v);
    if(typeof row.subject!=='string'||!row.subject||row.subject.length>60||!['game','product','platform'].includes(row.type)||!month(row.start)||!month(row.end)||row.start>row.end)throw Error('Invalid burst');
    if(!Number.isInteger(row.months)||row.months<1||!(row.ratio===null||(typeof row.ratio==='number'&&row.ratio>0))||!Number.isInteger(row.recordings)||row.recordings<1||typeof row.end_known!=='boolean'||typeof row.both_transcript_kinds!=='boolean')throw Error('Invalid burst numbers');
    bursts.push({subject:row.subject,type:row.type,start:row.start,end:row.end,months:row.months,ratio:row.ratio,recordings:row.recordings,end_known:row.end_known,both_transcript_kinds:row.both_transcript_kinds});
  }
  return {schema_version:1,bursts};
}
