import { loadAnalysisRelease } from '../../../lib/analysis/release';
import { analysisShardId } from '../../../lib/analysis/shards.mjs';
import { loadPassageShardPlan } from '../../../lib/analysis/passage-shard-server';
const shardSize = 100;
export async function GET() {
  const release = await loadAnalysisRelease();
  const cacheControl = import.meta.env.DEV ? 'no-store' : 'public, max-age=300';
  if (!release) return new Response(JSON.stringify({ available: false }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': cacheControl },
  });
  const { videos, ...metadata } = release;
  const video_shards = [];
  for (let start=0,index=0;start<videos.length;start+=shardSize,index++) {
    const id=analysisShardId(index,videos.slice(start,start+shardSize));
    video_shards.push({id,url:`/corpus/analysis/shards/${id}.json`,count:Math.min(shardSize,videos.length-start)});
  }
  const passageIndex = import.meta.env.DEV ? {} : { passage_shards: await loadPassageShardPlan() };
  return new Response(JSON.stringify({ available: true, release: metadata, video_shards, ...passageIndex }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': cacheControl },
  });
}
