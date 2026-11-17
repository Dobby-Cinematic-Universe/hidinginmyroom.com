import { loadAnalysisRelease } from '../../../lib/analysis/release';
import { analysisShards } from '../../../lib/analysis/shards.mjs';
import { loadPassageShardPlan } from '../../../lib/analysis/passage-shard-server';
export async function GET() {
  const release = await loadAnalysisRelease();
  const cacheControl = import.meta.env.DEV ? 'no-store' : 'public, max-age=300';
  if (!release) return new Response(JSON.stringify({ available: false }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': cacheControl },
  });
  const { videos, ...metadata } = release;
  const video_shards = analysisShards(videos).map(({id,count}) => ({id,url:`/corpus/analysis/shards/${id}.json`,count}));
  const passageIndex = import.meta.env.DEV ? {} : { passage_shards: await loadPassageShardPlan() };
  return new Response(JSON.stringify({ available: true, release: metadata, video_shards, ...passageIndex }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': cacheControl },
  });
}
