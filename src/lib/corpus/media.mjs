// Only retained public HTTPS sources may become player URLs. Never infer files.
export function playableSources(sources) {
  const choices = [];
  for (const source of sources) {
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
