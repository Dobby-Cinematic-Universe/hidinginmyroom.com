// Date estimates for undated recordings: a sidecar beside the catalog, never a catalog date.
// The catalog's date_label, date_basis and slug stay untouched, so estimates cannot enter
// chronologies, trends or URLs. Every public string that describes one says it is an estimate.
const DAY=/^\d{4}-\d{2}(?:-\d{2})?$/,MONTH=/^\d{4}-\d{2}$/;
const validMonth=v=>typeof v==='string'&&MONTH.test(v)&&Number(v.slice(5,7))>=1&&Number(v.slice(5,7))<=12;
const validDate=v=>typeof v==='string'&&DAY.test(v)&&(validMonth(v)||(/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString().slice(0,10)===v));
const COPY_KINDS={exact:'exact_copy',full:'full_copy',partial:'text_estimate'};
const keys=(o,allowed,what)=>{for(const k of Object.keys(o))if(!allowed.includes(k))throw Error(`Unexpected ${what} field ${k}`);};

export function projectDateEstimates(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));
  const recordings={};
  for(const [id,row] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);
    if(!target||target.date_label)continue; // only undated recordings of this release
    keys(row,['basis','date','copy','estimate'],'date estimate');
    if(!['exact_copy','full_copy','text_estimate','title_date'].includes(row.basis))throw Error('Invalid date estimate basis');
    let estimate=null,copy=null;
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
      if(COPY_KINDS[c.kind]!==row.basis)throw Error('Copy kind does not match date estimate basis');
      copy={kind:c.kind,recording_id:c.recording_id,date:c.date};
    }
    if(row.basis==='title_date'){
      const match=target.title?.match(/^\[?((?:19|20)\d{2})-?(\d{2})-?(\d{2})(?:\]|\b)/)||target.title?.match(/\bcirca\s+((?:19|20)\d{2})-?(\d{2})-?(\d{2})\b/i);
      const christmas=target.title?.match(/\bChristmas\b[^\n]*\b((?:19|20)\d{2})\b/i);
      const titleDate=match?match.slice(1).join('-'):christmas?`${christmas[1]}-12`:null;
      if(!validDate(row.date)||titleDate!==row.date||copy||estimate)throw Error('Invalid title date');
    }
    else if(row.basis==='text_estimate'){if(!estimate||row.date!==estimate.month)throw Error('Text estimate needs its month');}
    else if(!copy||row.date!==copy.date)throw Error('Copy-based date must be the copy date');
    recordings[id]={basis:row.basis,date:row.date,copy,estimate};
  }
  return {schema_version:1,recordings};
}

const MONTHS=['January','February','March','April','May','June','July','August','September','October','November','December'];
const month=v=>`${MONTHS[Number(v.slice(5,7))-1]} ${v.slice(0,4)}`;

// Display strings. `card` is for list cards, `short` goes beside the title on the recording
// page, `detail` explains the basis. All three say the date is estimated.
export function describeDateEstimate(row){
  if(!row)return undefined;
  const range=e=>`approximate range ${month(e.low)}–${month(e.high)}`;
  if(row.basis==='title_date'){
    const display=validMonth(row.date)?month(row.date):row.date;
    return {label:'Date in title',card:`Estimated ${display} (title)`,short:`Estimated ${display} · date in title`,detail:`The title points to ${display}. This is a date clue, not a verified recording or upload date.`};
  }
  if(row.basis==='text_estimate'){
    const e=row.estimate;
    return {label:'Estimated date',card:`Estimated ${month(e.month)}${row.copy?' (partial copy)':''}`,short:`Estimated ${month(e.month)} (not a verified date)`,
      detail:`Estimated ${month(e.month)}; ${range(e)}. This range reflects the model's errors on dated recordings and can miss the true date. Estimates are less reliable near the earliest and latest years of the archive. Not a verified recording or upload date.`,
      copyNote:row.copy?`Partial text match with a recording dated ${row.copy.date}; this may be reused or replayed material and does not establish this recording's date.`:undefined,copy:row.copy??undefined};
  }
  const kind=row.basis==='exact_copy'?'Identical transcript':'Matching transcript';
  const how=row.basis==='exact_copy'?'has the same transcript text as':'shares most of its transcript with';
  return {label:'Date from matching text',card:`Estimated ${row.date} (${row.basis==='exact_copy'?'identical':'matching'} transcript)`,short:`Estimated ${row.date} · ${row.basis==='exact_copy'?'identical':'matching'} transcript`,
    detail:`${kind}: this recording ${how} a recording dated ${row.date}. That date is used here as an estimate; it is not a verified date for this recording.`+
      ' Matching text can come from a re-upload or reused material.',
    copyNote:undefined,copy:row.copy};
}
