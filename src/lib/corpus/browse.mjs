/** Metadata-only discovery: no transcript shards or media are loaded. */
export function filterRecordings(rows, {q='', year='', availability='', sort='newest'}={}) {
  const words=q.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  return rows.filter(r=>words.every(w=>r.title.toLocaleLowerCase().includes(w)) &&
    (!year || (year==='undated' ? !r.year : String(r.year)===year)) &&
    (!availability || (availability==='transcript' ? r.hasTranscript : availability==='summary' ? !!r.summaryId : !r.hasTranscript)))
    .sort((a,b)=> {
      if(sort==='title') return a.title.localeCompare(b.title)||a.href.localeCompare(b.href);
      if(!a.date!==!b.date) return a.date ? -1 : 1;
      const date=(a.date||'').localeCompare(b.date||'');
      return (sort==='oldest'?date:-date)||a.title.localeCompare(b.title)||a.href.localeCompare(b.href);
    });
}
