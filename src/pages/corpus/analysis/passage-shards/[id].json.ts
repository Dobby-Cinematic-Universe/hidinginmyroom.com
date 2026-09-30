import type { APIRoute, GetStaticPaths } from 'astro';
import { loadPassageShardPlan, loadPassageShardText } from '../../../../lib/analysis/passage-shard-server';

export const getStaticPaths: GetStaticPaths = async () => (await loadPassageShardPlan()).map(shard => ({params:{id:shard.id}}));
export const GET: APIRoute = async ({params}) => {
  if (!/^[a-f0-9]{64}$/.test(params.id ?? '')) return new Response('Not found', {status:404});
  const text = await loadPassageShardText(params.id!);
  if (text === null) return new Response('Not found', {status:404});
  return new Response(text, {headers:{'Content-Type':'application/json; charset=utf-8','Cache-Control':'public, max-age=31536000, immutable'}});
};
