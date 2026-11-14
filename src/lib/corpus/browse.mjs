import {recordingLength} from './duration.mjs';

/** Metadata-only discovery: no transcript shards or media are loaded. */
export function filterRecordings(rows, {q='', year='', availability='', length='', sort='newest'}={}) {
  const words=q.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  // `circa` is an approximate date from a narrow archived listing window; it orders and filters
  // like a date. Rows folded into another recording (`duplicateOf`) are not listed; their titles
  // and any earlier titles are searchable through the kept row's `aliases`.
  const when=r=>r.date||r.circa||'', yearOf=r=>r.year||(r.circa?Number(r.circa.slice(0,4)):null);
  const text=r=>[r.title,...(r.aliases||[])].join('\n').toLocaleLowerCase();
  return rows.filter(r=>!r.duplicateOf && words.every(w=>text(r).includes(w)) &&
    (!year || (year==='undated' ? !yearOf(r) : String(yearOf(r))===year)) &&
    (!length || recordingLength(r.durationMs)===length) &&
    (!availability || (availability==='transcript' ? r.hasTranscript : availability==='summary' ? !!r.summaryId : !r.hasTranscript)))
    .sort((a,b)=> {
      if(sort==='title') return a.title.localeCompare(b.title)||a.href.localeCompare(b.href);
      if(!when(a)!==!when(b)) return when(a) ? -1 : 1;
      const date=when(a).localeCompare(when(b));
      return (sort==='oldest'?date:-date)||a.title.localeCompare(b.title)||a.href.localeCompare(b.href);
    });
}
