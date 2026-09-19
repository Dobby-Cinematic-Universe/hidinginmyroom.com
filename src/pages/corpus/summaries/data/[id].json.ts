import type { APIRoute } from 'astro';
import { loadSummaryRelease } from '../../../../lib/summaries/release';
export async function getStaticPaths() {
  return (await loadSummaryRelease()).summaries.filter(s=>s.kind==='transcript').map(summary=>({
    params:{id:summary.id},props:{summary},
  }));
}
export const GET: APIRoute = ({props}) => new Response(JSON.stringify({
  id:props.summary.id, sections:props.summary.sections,
}), {headers:{'Content-Type':'application/json; charset=utf-8','X-Content-Type-Options':'nosniff'}});
