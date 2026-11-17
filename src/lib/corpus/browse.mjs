import {recordingLength} from './duration.mjs';
import {matchesAll,searchText,searchWords} from './title-search.mjs';

/** Metadata-only discovery: no transcript shards or media are loaded. */
export function filterRecordings(rows, {q='', year='', availability='', length='', sort='newest'}={}) {
  const words=searchWords(q);
  // `circa` is an approximate date from a narrow archived listing window and `estimated` any other
  // estimate of an undated recording; both order and filter like a date, the circa date first. Rows folded into another recording (`duplicateOf`) are not listed; their titles
  // and any earlier titles are searchable through the kept row's `aliases`.
  const when=r=>r.date||r.circa||r.estimated||'', yearOf=r=>r.year||(r.circa||r.estimated?Number((r.circa||r.estimated).slice(0,4)):null);
  return rows.filter(r=>!r.duplicateOf && matchesAll(searchText(r.title,r.aliases),words) &&
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
