import type { APIRoute, GetStaticPaths } from 'astro';
import { loadAnalysisRelease } from '../../../../lib/analysis/release';
import { ANALYSIS_SHARD_SIZE, analysisShardId, analysisShards } from '../../../../lib/analysis/shards.mjs';

const idPattern = /^(\d{3,})-([a-f0-9]{16})$/;

// One shard of score rows in the compact lossless encoding (src/lib/analysis/shard-codec.mjs). The body is
// {schema_version, shard_id, corpus_release_id, videos} in this order: the explorer hashes the bytes of `videos`
// where they lie (src/lib/analysis/shard-verify.mjs).
export const getStaticPaths: GetStaticPaths = async () => {
  const release = await loadAnalysisRelease();
  if (!release) return [];
  return analysisShards(release.videos).map(({id,count,encoded}) => ({params:{id},props:{corpusReleaseId:release.corpus_release_id,id,count,encoded}}));
};

interface Props { corpusReleaseId:string; id:string; count:number; encoded:unknown[] }
export const GET: APIRoute = ({props}) => {
  const {corpusReleaseId,id,count,encoded}=props as Props;
  const match=id.match(idPattern);
  if (!match || count>ANALYSIS_SHARD_SIZE || analysisShardId(Number(match[1]),encoded)!==id) return new Response('Not found',{status:404});
  return new Response(JSON.stringify({schema_version:2,shard_id:id,corpus_release_id:corpusReleaseId,videos:encoded}),{
    headers:{'Content-Type':'application/json; charset=utf-8','Cache-Control':'public, max-age=31536000, immutable'},
  });
};
