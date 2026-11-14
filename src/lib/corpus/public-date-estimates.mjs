// Date estimates for undated recordings: a sidecar beside the catalog, never a catalog date.
// The catalog's date_label, date_basis and slug stay untouched, so estimates cannot enter
// analysis trends or URLs. Every public string that describes one says it is an estimate, circa,
// or a date read from an archived page.
const DAY=/^\d{4}-\d{2}(?:-\d{2})?$/,MONTH=/^\d{4}-\d{2}$/;
const validMonth=v=>typeof v==='string'&&MONTH.test(v)&&Number(v.slice(5,7))>=1&&Number(v.slice(5,7))<=12;
const validDate=v=>typeof v==='string'&&DAY.test(v)&&(validMonth(v)||(/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString().slice(0,10)===v));
const PAGE_SOURCE=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https?:\/\/(?:www\.|m\.)?youtube\.com\/watch\?[\w\-=&%.;]+$/;
const COPY_KINDS={exact:'exact_copy',full:'full_copy',partial:'text_estimate'};
const keys=(o,allowed,what)=>{for(const k of Object.keys(o))if(!allowed.includes(k))throw Error(`Unexpected ${what} field ${k}`);};

export function projectDateEstimates(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));
  const recordings={};
  for(const [id,row] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);
    if(!target||target.date_label)continue; // only undated recordings of this release
    keys(row,['basis','date','copy','estimate','listing','page'],'date estimate');
    if(!['exact_copy','full_copy','text_estimate','title_date','archived_listing','archived_page'].includes(row.basis))throw Error('Invalid date estimate basis');
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
    if(row.basis==='title_date'){
      const match=target.title?.match(/^\[?((?:19|20)\d{2})-?(\d{2})-?(\d{2})(?:\]|\b)/)||target.title?.match(/\bcirca\s+((?:19|20)\d{2})-?(\d{2})-?(\d{2})\b/i);
      const christmas=target.title?.match(/\bChristmas\b[^\n]*\b((?:19|20)\d{2})\b/i);
      const titleDate=match?match.slice(1).join('-'):christmas?`${christmas[1]}-12`:null;
      if(!validDate(row.date)||titleDate!==row.date||copy||estimate)throw Error('Invalid title date');
    }
    else if(row.basis==='archived_page'){if(!page||row.date!==page.date||estimate||listing||(copy&&Math.abs(Date.parse(copy.date)-Date.parse(page.date))>864e5))throw Error('Invalid archived page basis');}
    else if(row.basis==='archived_listing'){if(!listing||row.date!==listing.from||copy||estimate)throw Error('Invalid archived listing date');}
    else if(row.basis==='text_estimate'){if(!estimate||row.date!==estimate.month)throw Error('Text estimate needs its month');}
    else if(!copy||row.date!==copy.date)throw Error('Copy-based date must be the copy date');
    recordings[id]={basis:row.basis,date:row.date,copy,estimate};
    if(listing)recordings[id].listing=listing;
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
export const circaDate=row=>row?.page?row.date:row?.listing&&days(row.listing)<=CIRCA_DAYS?(row.basis==='archived_listing'?row.listing.from:row.date):undefined;
// An identical transcript whose date an archived listing confirms is another copy of that recording.
export const duplicateOf=row=>(row?.basis==='exact_copy'&&(row.listing||row.page)&&row.copy)||(row?.basis==='archived_page'&&row.copy?.kind==='exact')?row.copy.recording_id:undefined;

// Display strings. `card` is for list cards, `short` goes beside the title on the recording
// page, `detail` explains the basis. All three say the date is estimated.
export function describeDateEstimate(row){
  if(!row)return undefined;
  const range=e=>`approximate range ${month(e.low)}–${month(e.high)}`;
  if(row.basis==='title_date'){
    const display=validMonth(row.date)?month(row.date):row.date;
    return {label:'Date in title',card:`Estimated ${display} (title)`,short:`Estimated ${display} · date in title`,detail:`The title points to ${display}. This is a date clue, not a verified recording or upload date.`};
  }
  if(row.basis==='archived_page'){
    return {label:'Date from archived video page',card:`${row.date} (archived video page)`,short:`${row.date} · upload date on archived video page`,circa:row.date,
      detail:`An archived copy of this video's YouTube page, saved on ${row.page.captured}, shows it as published on ${row.date}. `+
        'The page shows the date in YouTube\'s own time zone, so it can differ by one day from dates elsewhere in the corpus. The corpus has not verified it against the original upload.'+
        (row.copy?` A recording dated ${row.copy.date} has ${row.copy.kind==='exact'?'the same':'a matching'} transcript.`:''),
      source:row.page.source,copyNote:undefined,copy:row.copy??undefined,duplicateOf:duplicateOf(row)};
  }
  if(row.basis==='archived_listing'){
    const l=row.listing,span=l.from===l.to?l.from:`${l.from} to ${l.to}`;
    const word=circaDate(row)?'circa':'Estimated';
    return {label:'Date from archived listing',card:`${word} ${span} (archived listing)`,short:`${word} ${span} · archived channel listing`,circa:circaDate(row),
      detail:`An archived copy of the channel's video listing shows this video as published ${l.from===l.to?`on ${l.from}`:`between ${l.from} and ${l.to}`}. `+
        'The dates are worked out from the listing\'s relative age display (such as "3 weeks ago"), so they are approximate. Not a verified recording or upload date.'+
        (circaDate(row)?' Lists and year filters place this recording at the start of that window.':'')};
  }
  if(row.basis==='text_estimate'){
    const e=row.estimate;
    return {label:'Estimated date',card:`Estimated ${month(e.month)}${row.copy?' (partial copy)':''}`,short:`Estimated ${month(e.month)} (not a verified date)`,
      detail:`Estimated ${month(e.month)}; ${range(e)}. This range reflects the model's measured errors, including a check against archived listing dates, and can miss the true date. Estimates are less reliable near the earliest and latest years of the archive. Not a verified recording or upload date.`,
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
