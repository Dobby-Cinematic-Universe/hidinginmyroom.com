import type { PublicSummary } from './release';
const labels = {summary:'Overview',topics:'Themes',events:'Developments',uncertainties:'Uncertainties & gaps'};
const cache = new Map<string, Promise<Pick<PublicSummary,'id'|'sections'>>>();
async function expand(details: HTMLDetailsElement) {
  if (!details.open || details.dataset.loaded || details.dataset.loading) return;
  const id = details.dataset.summaryId!;
  if (!/^[a-z0-9-]+$/.test(id)) return;
  const body = details.querySelector<HTMLElement>('[data-summary-body]')!;
  details.dataset.loading = 'true'; body.setAttribute('aria-busy','true'); body.textContent = 'Loading summary…';
  try {
    if (!cache.has(id)) cache.set(id, fetch(`/corpus/summaries/data/${id}.json`).then(async r => {
      if (!r.ok) throw Error('Summary unavailable'); return r.json();
    }));
    const value = await cache.get(id);
    if (!value) throw Error('Summary unavailable');
    const content = document.createDocumentFragment();
    const notice = document.createElement('p'); notice.className='summary-caution';
    notice.textContent='AI-generated and unreviewed. Check the recording for context.'; content.append(notice);
    for (const [key,label] of Object.entries(labels) as [keyof typeof labels,string][]) {
      const items = value.sections[key]; if (!items?.length) continue;
      const h = document.createElement('h4'); h.textContent=label; content.append(h);
      for (const item of items) {
        const p = document.createElement('p'); p.textContent=item.text;
        if (item.classification==='reported_allegation') {
          const tag=document.createElement('strong'); tag.textContent='Reported allegation: ';p.prepend(tag);
        }
        content.append(p);
      }
    }
    body.replaceChildren(content); details.dataset.loaded='true';
  } catch {
    cache.delete(id); body.textContent='Could not load the summary. Close and reopen to retry, or use the summary-page link below.';
  } finally { delete details.dataset.loading; body.removeAttribute('aria-busy'); }
}
document.addEventListener('toggle', event => {
  if (event.target instanceof HTMLDetailsElement && event.target.matches('[data-summary-id]')) void expand(event.target);
}, true);
