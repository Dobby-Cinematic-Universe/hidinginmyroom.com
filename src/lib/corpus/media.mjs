// Only retained public HTTPS sources may become player URLs. Never infer files.
export function playableSources(sources, context = {}) {
  const choices = [];
  const replacedSources = new Set();
  const manifest = context.accessCopies;
  if (/^release_[a-f0-9]{24}$/.test(context.releaseId || '') && /^rec_[a-f0-9]{32}$/.test(context.recordingId || '') && manifest?.schema_version === 1 && manifest.corpus_release === context.releaseId && Array.isArray(manifest.copies)) {
    for (const copy of manifest.copies) {
      if (!copy || copy.recording_id !== context.recordingId || !/^[a-f0-9]{64}$/.test(copy.source_sha256 || '')) continue;
      const original = sources.find(source => source.source_id === copy.source_id && source.url === copy.original_url && source.access_state === 'public');
      if (!original) continue;
      let url; try { url = new URL(copy.playback_url); } catch { continue; }
      if (url.protocol !== 'https:' || url.hostname !== 'archive.org' || url.port || url.username || url.password || url.search || url.hash || !/^\/download\/[^/]+\/.+\.mp4$/i.test(url.pathname)) continue;
      choices.push({kind:'video', url:url.href, label:'Browser-compatible copy'});
      replacedSources.add(original);
    }
  }
  for (const source of sources) {
    if (replacedSources.has(source)) continue;
    let url; try { url = new URL(source.url); } catch { continue; }
    if (url.protocol !== 'https:' || url.username || url.password || source.access_state !== 'public') continue;
    if (url.hostname === 'archive.org' && url.pathname.startsWith('/download/') && /\.(mp4|m4v|webm|ogv)$/i.test(url.pathname)) {
      choices.push({kind:'video', url:url.href, label:'Internet Archive'});
    } else if (['youtube.com','www.youtube.com','youtu.be'].includes(url.hostname)) {
      const id = url.hostname === 'youtu.be' ? url.pathname.slice(1) : url.pathname === '/watch' ? url.searchParams.get('v') : null;
      if (/^[\w-]{11}$/.test(id || '')) choices.push({kind:'youtube', url:`https://www.youtube-nocookie.com/embed/${id}`, label:'YouTube'});
    }
  }
  return choices.filter((v,i,a)=>a.findIndex(x=>x.url===v.url)===i);
}
