// Date estimates for undated recordings: a sidecar beside the catalog, never a catalog date.
// The catalog's date_label, date_basis and slug stay untouched, so estimates cannot enter
// analysis trends or URLs. Every public string that describes one says it is an estimate, circa,
// or a date read from an archived page.
const DAY=/^\d{4}-\d{2}(?:-\d{2})?$/,MONTH=/^\d{4}-\d{2}$/;
const validMonth=v=>typeof v==='string'&&MONTH.test(v)&&Number(v.slice(5,7))>=1&&Number(v.slice(5,7))<=12;
const validDate=v=>typeof v==='string'&&DAY.test(v)&&(validMonth(v)||(/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString().slice(0,10)===v));
const PAGE_SOURCE=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https?:\/\/(?:www\.|m\.)?youtube\.com\/watch\?[\w\-=&%.;]+$/;
// What narrowed a date, in the words shown to readers.
export const EVIDENCE_SOURCES={stream_metadata:'an archived stream record',legacy_index_label:'a date label in an earlier transcript index',archived_listing_unattributed:'an archived channel listing',
  calendar_clue:'a weekday or date said in the recording',release_anchor:'the release date of something mentioned',back_reference:'a reference to an earlier recording',
  event_reference:'a statement of how long ago something happened',platform_window:'the period covered by the platform\'s other recordings',
  excerpt:'a dated excerpt that was cut from this recording',replay:'older dated material played back in it'};
const EVIDENCE_SHORT={stream_metadata:'archived stream record',legacy_index_label:'earlier index label',archived_listing_unattributed:'archived listing',calendar_clue:'date said in recording',
  release_anchor:'release dates',back_reference:'earlier recording',event_reference:'elapsed-time statement',platform_window:'platform period',
  excerpt:'dated excerpt',replay:'replayed older material'};
const validSources=v=>Array.isArray(v)&&v.length>0&&v.length<=10&&new Set(v).size===v.length&&v.every(s=>Object.hasOwn(EVIDENCE_SOURCES,s));
const listSources=v=>{const t=v.map(s=>EVIDENCE_SOURCES[s]);return t.length>1?`${t.slice(0,-1).join(', ')} and ${t.at(-1)}`:t[0];};
const COPY_KINDS={exact:'exact_copy',full:'full_copy',partial:'text_estimate'};
const keys=(o,allowed,what)=>{for(const k of Object.keys(o))if(!allowed.includes(k))throw Error(`Unexpected ${what} field ${k}`);};

export function projectDateEstimates(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));
  const recordings={};
  for(const [id,row] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);
    if(!target||target.date_label)continue; // only undated recordings of this release
    keys(row,['basis','date','copy','estimate','listing','page','evidence','narrowed_by','period','source'],'date estimate');
    if(!['exact_copy','full_copy','text_estimate','title_date','archived_listing','archived_page','evidence_date','collection_period','source_recording','source_estimate','series_period'].includes(row.basis))throw Error('Invalid date estimate basis');
    let estimate=null,copy=null,listing=null,page=null;
    if(row.page!=null){
      // Upload date displayed on an archived copy of the video's own YouTube page.
      const g=row.page;keys(g,['date','captured','source'],'page');
      if(![g.date,g.captured].every(v=>validDate(v)&&v.length===10)||g.date>g.captured||!PAGE_SOURCE.test(g.source||''))throw Error('Invalid archived page date');
      if(!['archived_page','exact_copy','full_copy'].includes(row.basis))throw Error('Archived page date does not fit this basis');
      page={date:g.date,captured:g.captured,source:g.source};
    }
    if(row.listing!=null){
      // Publication window inferred from archived channel listings; full dates only.
      const l=row.listing;keys(l,['from','to'],'listing');
      if(![l.from,l.to].every(v=>validDate(v)&&v.length===10)||l.from>l.to)throw Error('Invalid archived listing window');
      if(!['archived_listing','exact_copy','full_copy'].includes(row.basis))throw Error('Listing window does not fit this basis');
      listing={from:l.from,to:l.to};
    }
    let evidence=null,narrowedBy=null;
    if(row.evidence!=null){
      // A date range of a week or less from combined evidence (labels, spoken dates, archived stream records).
      const e=row.evidence;keys(e,['from','to','sources'],'evidence');
      if(row.basis!=='evidence_date'||![e.from,e.to].every(v=>validDate(v)&&v.length===10)||e.from>e.to||(Date.parse(e.to)-Date.parse(e.from))/864e5>7||!validSources(e.sources))throw Error('Invalid evidence date');
      evidence={from:e.from,to:e.to,sources:[...e.sources]};
    }
    if(row.narrowed_by!=null){
      if(!['text_estimate','source_estimate'].includes(row.basis)||!validSources(row.narrowed_by))throw Error('Invalid narrowing sources');
      narrowedBy=[...row.narrowed_by];
    }
    let period=null;
    if(row.period!=null&&row.basis==='series_period'){
      // A clip with no match of its own: the day of another file of the same clip, or the range between its numbered neighbours.
      const p=row.period;keys(p,['from','to','rule'],'period');
      if(![p.from,p.to].every(v=>validDate(v)&&v.length===10)||p.from>p.to||!['same clip name','between numbered neighbours'].includes(p.rule)||(p.rule==='same clip name')!==(p.from===p.to))throw Error('Invalid series period');
      period={from:p.from,to:p.to,rule:p.rule};
    }else if(row.period!=null){
      // The span of the dated members of an archived clip collection that this undated clip belongs to.
      const p=row.period;keys(p,['from','to','collection','dated_members'],'period');
      if(row.basis!=='collection_period'||![p.from,p.to].every(v=>validDate(v)&&v.length===10)||p.from>p.to||p.collection!=='twitch_clips'||!Number.isInteger(p.dated_members)||p.dated_members<20)throw Error('Invalid collection period');
      period={from:p.from,to:p.to,collection:p.collection,dated_members:p.dated_members};
    }
    if(row.estimate!=null){
      const e=row.estimate;keys(e,['month','low','high','confidence_band'],'estimate');
      if(![e.month,e.low,e.high].every(validMonth)||!(e.low<=e.month&&e.month<=e.high))throw Error('Invalid estimate range');
      if(!Number.isInteger(e.confidence_band)||e.confidence_band<0||e.confidence_band>4)throw Error('Invalid estimate confidence band');
      estimate={month:e.month,low:e.low,high:e.high,confidence_band:e.confidence_band};
    }
    if(row.copy!=null){
      const c=row.copy;keys(c,['kind','recording_id','date'],'copy');
      const other=byId.get(c.recording_id);
      if(!(c.kind in COPY_KINDS)||!other||c.recording_id===id||other.date_label!==c.date||!validDate(c.date))throw Error('Invalid date estimate copy');
      if(row.basis==='archived_page'?c.kind==='partial':COPY_KINDS[c.kind]!==row.basis)throw Error('Copy kind does not match date estimate basis');
      copy={kind:c.kind,recording_id:c.recording_id,date:c.date};
    }
    let source=null;
    if(row.source!=null){
      // Another recording whose transcript holds this recording's words.
      const s=row.source,other=byId.get(s.recording_id);
      if(!other||s.recording_id===id)throw Error('Invalid source recording');
      if(row.basis==='source_estimate'){
        // The containing recording is undated too; its own wording estimate is passed on unchanged.
        keys(s,['recording_id','basis','relation'],'source');
        const from=input.recordings[s.recording_id],same=(a,b)=>['month','low','high','confidence_band'].every(k=>a?.[k]===b?.[k]);
        if(other.date_label||s.relation!=='inside_undated_recording'||s.basis!=='text_estimate'||from?.basis!=='text_estimate'||!same(from.estimate,row.estimate)||JSON.stringify(from.narrowed_by??null)!==JSON.stringify(row.narrowed_by??null))throw Error('Invalid source estimate');
        source={recording_id:s.recording_id,relation:s.relation};
      }else{
        // The containing recording is dated; its catalog date is the only date this can carry.
        keys(s,['recording_id','date','tier','relation'],'source');
        if(!['source_recording','title_date','archived_listing','archived_page'].includes(row.basis)||!['A','B'].includes(s.tier)||!['same_recording','not_after'].includes(s.relation)||!validDate(s.date)||s.date.length!==10||other.date_label!==s.date)throw Error('Invalid source recording');
        source={recording_id:s.recording_id,date:s.date,tier:s.tier,relation:s.relation};
      }
    }
    if(row.basis==='title_date'){
      const match=target.title?.match(/^\[?((?:19|20)\d{2})-?(\d{2})-?(\d{2})(?:\]|\b)/)||target.title?.match(/\bcirca\s+((?:19|20)\d{2})-?(\d{2})-?(\d{2})\b/i);
      const christmas=target.title?.match(/\bChristmas\b[^\n]*\b((?:19|20)\d{2})\b/i);
      const titleDate=match?match.slice(1).join('-'):christmas?`${christmas[1]}-12`:null;
      if(!validDate(row.date)||titleDate!==row.date||copy||estimate)throw Error('Invalid title date');
    }
    else if(row.basis==='collection_period'){if(!period||row.date!==period.from.slice(0,7)||copy||estimate||listing||page||evidence||source)throw Error('Invalid collection period basis');}
    else if(row.basis==='source_recording'){if(!source?.date||row.date!==source.date||copy||estimate||listing||page||evidence||period)throw Error('Invalid source recording basis');}
    else if(row.basis==='source_estimate'){if(!source||source.date||!estimate||row.date!==estimate.month||copy||listing||page||evidence||period)throw Error('Invalid source estimate basis');}
    else if(row.basis==='series_period'){if(!period||row.date!==period.from||copy||estimate||listing||page||evidence||source)throw Error('Invalid series period basis');}
    else if(row.basis==='evidence_date'){if(!evidence||!validDate(row.date)||row.date.length!==10||row.date<evidence.from||row.date>evidence.to||copy||estimate||listing||page)throw Error('Invalid evidence date basis');}
    else if(row.basis==='archived_page'){if(!page||row.date!==page.date||estimate||listing||(copy&&Math.abs(Date.parse(copy.date)-Date.parse(page.date))>864e5))throw Error('Invalid archived page basis');}
    else if(row.basis==='archived_listing'){if(!listing||row.date!==listing.from||copy||estimate)throw Error('Invalid archived listing date');}
    else if(row.basis==='text_estimate'){if(!estimate||row.date!==estimate.month)throw Error('Text estimate needs its month');}
    else if(!copy||row.date!==copy.date)throw Error('Copy-based date must be the copy date');
    recordings[id]={basis:row.basis,date:row.date,copy,estimate};
    if(listing)recordings[id].listing=listing;
    if(evidence)recordings[id].evidence=evidence;
    if(period)recordings[id].period=period;
    if(narrowedBy)recordings[id].narrowed_by=narrowedBy;
    if(source)recordings[id].source=source;
    if(page){if(Math.abs(Date.parse(row.date)-Date.parse(page.date))>864e5)throw Error('Archived page date disagrees with the copy date');recordings[id].page=page;}
  }
  return {schema_version:1,recordings};
}

const MONTHS=['January','February','March','April','May','June','July','August','September','October','November','December'];
const month=v=>`${MONTHS[Number(v.slice(5,7))-1]} ${v.slice(0,4)}`;

// An archived listing window of at most CIRCA_DAYS is narrow enough to place the recording in
// date order and year filters as an approximate ("circa") date. Wider windows and model
// estimates never are. The catalog date itself is still not changed.
export const CIRCA_DAYS=31;
const days=l=>Math.round((Date.parse(l.to)-Date.parse(l.from))/864e5);
// A copy date that the listing confirms keeps the dated copy's own (exact) date.
// So does a date read from an archived copy of the video's own page.
// So does the day of a dated recording that holds a clip's words, unless that day is only the latest possible one.
export const circaDate=row=>row?.basis==='evidence_date'?row.date:row?.basis==='source_recording'?(row.source.relation==='same_recording'?row.date:undefined):row?.page?row.date:row?.listing&&days(row.listing)<=CIRCA_DAYS?(row.basis==='archived_listing'?row.listing.from:row.date):undefined;
// An identical transcript whose date an archived listing confirms is another copy of that recording.
export const duplicateOf=row=>(row?.basis==='exact_copy'&&(row.listing||row.page)&&row.copy)||(row?.basis==='archived_page'&&row.copy?.kind==='exact')?row.copy.recording_id:undefined;

// Display strings. `card` is for list cards, `short` goes beside the title on the recording
// page, `detail` explains the basis. All three say the date is estimated.
export function describeDateEstimate(row){
  if(!row)return undefined;
  const range=e=>e.low===e.high?`the range lies within ${month(e.low)}`:`approximate range ${month(e.low)}–${month(e.high)}`;
  // A dated recording that also holds these words, mentioned beside a date that rests on something else.
  const alsoIn=row.source?.date&&row.basis!=='source_recording'?` The words of this recording are also found inside a recording dated ${row.source.date}${row.source.relation==='not_after'?' (a latest possible day)':''}${
    row.basis==='title_date'&&Math.abs(Date.parse(row.source.date)-Date.parse(row.date.length===10?row.date:`${row.date}-15`))>31*864e5?', which differs from the date in the title; neither is verified':''}.`:'';
  const sourceCopy=row.source?{recording_id:row.source.recording_id}:undefined;
  if(row.basis==='title_date'){
    const display=validMonth(row.date)?month(row.date):row.date;
    return {label:'Date in title',card:`Estimated ${display} (title)`,short:`Estimated ${display} · date in title`,detail:`The title points to ${display}. This is a date clue, not a verified recording or upload date.${alsoIn}`,copy:sourceCopy};
  }
  if(row.basis==='source_recording'){
    const s=row.source,probable=s.tier==='B'?' (a probable match)':'';
    if(s.relation==='not_after')return {label:'Latest possible date',card:`${row.date} or earlier (part of a dated recording)`,short:`${row.date} or earlier · part of a dated recording`,
      detail:`The words of this recording are found inside a recording dated ${row.date}${probable}. Recordings that hold the same words carry dates more than a week apart, or the recording found plays older clips back, `+
        `so ${row.date} is the latest possible day and not an estimate of the day itself. Not a verified recording or upload date.`,copy:sourceCopy};
    return {label:'Date from the recording it is part of',card:`circa ${row.date} (part of a dated recording)`,short:`circa ${row.date} · part of a dated recording`,circa:row.date,
      detail:`The words of this recording are found inside a longer recording dated ${row.date}${probable}, so it is taken to be an excerpt of that recording. The date is that recording's upload or stream day, used here as an estimate. `+
        `It is not the day this clip was made or uploaded. If the archive holds only a later replay and not the original, the true day is earlier. Lists and year filters place this recording at ${row.date}.`,copy:sourceCopy};
  }
  if(row.basis==='source_estimate'){
    const e=row.estimate;
    return {label:'Estimate from the recording it is part of',card:`Estimated ${month(e.month)} (part of an undated recording)`,short:`Estimated ${month(e.month)} · from the recording it is part of`,
      detail:`The words of this recording are found inside a longer recording that has no catalog date either. That recording's estimate is passed on: ${month(e.month)}; ${range(e)}.`+
        `${row.narrowed_by?` Its range was narrowed using ${listSources(row.narrowed_by)}.`:''} The estimate comes from the words used and can miss the true date. Not a verified recording or upload date.`,copy:sourceCopy};
  }
  if(row.basis==='series_period'){
    const p=row.period;
    if(p.from===p.to)return {label:'Date from clips with the same name',card:`Estimated ${p.from} (another file of the same clip)`,short:`Estimated ${p.from} · another file of the same clip`,
      detail:`No recording that holds these words was found. Another file with the same clip name was matched to a recording dated ${p.from}, and this file is given the same day as an estimate. Not a verified recording or upload date.`};
    return {label:'Date from clips with the same name',card:`Estimated ${p.from} to ${p.to} (numbered clip series)`,short:`Estimated ${p.from} to ${p.to} · numbered clip series`,
      detail:`No recording that holds these words was found. Clips with this name are numbered, and where the numbers could be tested they follow the dates of the recordings the clips were cut from. `+
        `The neighbouring numbers place this clip between ${p.from} and ${p.to}. Not a verified recording or upload date.`};
  }
  if(row.basis==='collection_period'){
    const p=row.period,a=month(p.from.slice(0,7)),b=month(p.to.slice(0,7));
    return {label:'Estimated period',card:`Estimated ${p.from.slice(0,4)}–${p.to.slice(0,4)} (clip collection)`,short:`Estimated ${a} to ${b} · period of its clip collection`,
      detail:`This clip belongs to an archived collection of Twitch clips. The ${p.dated_members} dated clips in that collection run from ${p.from} to ${p.to}, so this one is probably from that period. A clip's date is when the clip was made, which can be months after the stream it shows. Not a verified date.`};
  }
  if(row.basis==='evidence_date'){
    const e=row.evidence,span=e.from===e.to?`on ${e.from}`:`between ${e.from} and ${e.to}`;
    return {label:'Date from combined evidence',card:`circa ${row.date} (${EVIDENCE_SHORT[e.sources[0]]})`,short:`circa ${row.date} · ${EVIDENCE_SHORT[e.sources[0]]}`,circa:row.date,
      detail:`Placed ${span} using ${listSources(e.sources)}. This is an estimate from combined evidence, not a verified recording or upload date. Lists and year filters place this recording at ${row.date}.`};
  }
  if(row.basis==='archived_page'){
    return {label:'Date from archived video page',card:`${row.date} (archived video page)`,short:`${row.date} · upload date on archived video page`,circa:row.date,
      detail:`An archived copy of this video's YouTube page, saved on ${row.page.captured}, shows it as published on ${row.date}. `+
        'The page shows the date in YouTube\'s own time zone, so it can differ by one day from dates elsewhere in the corpus. The corpus has not verified it against the original upload.'+
        (row.copy?` A recording dated ${row.copy.date} has ${row.copy.kind==='exact'?'the same':'a matching'} transcript.`:'')+alsoIn,
      source:row.page.source,copyNote:undefined,copy:row.copy??sourceCopy,duplicateOf:duplicateOf(row)};
  }
  if(row.basis==='archived_listing'){
    const l=row.listing,span=l.from===l.to?l.from:`${l.from} to ${l.to}`;
    const word=circaDate(row)?'circa':'Estimated';
    return {label:'Date from archived listing',card:`${word} ${span} (archived listing)`,short:`${word} ${span} · archived channel listing`,circa:circaDate(row),
      detail:`An archived copy of the channel's video listing shows this video as published ${l.from===l.to?`on ${l.from}`:`between ${l.from} and ${l.to}`}. `+
        'The dates are worked out from the listing\'s relative age display (such as "3 weeks ago"), so they are approximate. Not a verified recording or upload date.'+
        (circaDate(row)?' Lists and year filters place this recording at the start of that window.':'')+alsoIn,copy:sourceCopy};
  }
  if(row.basis==='text_estimate'){
    const e=row.estimate;
    return {label:'Estimated date',card:`Estimated ${month(e.month)}${row.copy?' (partial copy)':''}`,short:`Estimated ${month(e.month)} (not a verified date)`,
      detail:`Estimated ${month(e.month)}; ${range(e)}.${row.narrowed_by?` The range was narrowed using ${listSources(row.narrowed_by)}.`:''} This range reflects the model's measured errors, including a check against archived listing dates, and can miss the true date. Estimates are less reliable near the earliest and latest years of the archive. Not a verified recording or upload date.`,
      copyNote:row.copy?`Partial text match with a recording dated ${row.copy.date}; this may be reused or replayed material and does not establish this recording's date.`:undefined,copy:row.copy??undefined};
  }
  const kind=row.basis==='exact_copy'?'Identical transcript':'Matching transcript';
  const how=row.basis==='exact_copy'?'has the same transcript text as':'shares most of its transcript with';
  return {label:'Date from matching text',card:`Estimated ${row.date} (${row.basis==='exact_copy'?'identical':'matching'} transcript)`,short:`Estimated ${row.date} · ${row.basis==='exact_copy'?'identical':'matching'} transcript`,
    detail:`${kind}: this recording ${how} a recording dated ${row.date}. That date is used here as an estimate; it is not a verified date for this recording.`+
      ' Matching text can come from a re-upload or reused material.'+
      (row.page?` An archived copy of the video's own page shows it as published on ${row.page.date}.`:'')+
      (row.listing?` An archived channel listing is consistent with this date (published ${row.listing.from===row.listing.to?`on ${row.listing.from}`:`between ${row.listing.from} and ${row.listing.to}`}).`:''),
    copyNote:undefined,copy:row.copy,circa:circaDate(row),duplicateOf:duplicateOf(row)};
}
