import type { PublicSummary } from './release';
import {validateSummaryShardIndex,validateSummaryShardBytes,type SummaryShardDescriptor,type InlineSummary} from './shards.mjs';
const labels = {summary:'Overview',topics:'Themes',events:'Developments',uncertainties:'Uncertainties & gaps'};
const cache = new Map<string, Promise<Pick<PublicSummary,'id'|'sections'>>>();
let indexPromise:Promise<{release:string;entries:Map<string,SummaryShardDescriptor>}>|undefined;
const shardCache=new Map<string,Promise<Map<string,InlineSummary>>>();
async function loadInlineSummary(id:string):Promise<InlineSummary>{
  if(!indexPromise)indexPromise=fetch('/corpus/summaries/data/index.json',{cache:'no-cache'}).then(async response=>{
    if(!response.ok)throw Error('Summary index unavailable');
    const index=await response.json();
    return {release:index.release_id,entries:validateSummaryShardIndex(index)};
  }).catch(error=>{indexPromise=undefined;throw error;});
  const index=await indexPromise,descriptor=index.entries.get(id);
  if(!descriptor)throw Error('Summary unavailable');
  if(!shardCache.has(descriptor.id))shardCache.set(descriptor.id,fetch(descriptor.url).then(async response=>{
    if(!response.ok)throw Error('Summary shard unavailable');
    return validateSummaryShardBytes(new Uint8Array(await response.arrayBuffer()),descriptor,index.release);
  }).catch(error=>{shardCache.delete(descriptor.id);indexPromise=undefined;throw error;}));
  const value=(await shardCache.get(descriptor.id))?.get(id);
  if(!value)throw Error('Summary unavailable');
  return value;
}
async function expand(details: HTMLDetailsElement) {
  if (!details.open || details.dataset.loaded || details.dataset.loading) return;
  const id = details.dataset.summaryId!;
  if (!/^[a-z0-9-]+$/.test(id)) return;
  const body = details.querySelector<HTMLElement>('[data-summary-body]')!;
  details.dataset.loading = 'true'; body.setAttribute('aria-busy','true'); body.textContent = 'Loading summary…';
  try {
    if (!cache.has(id)) cache.set(id, loadInlineSummary(id));
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
