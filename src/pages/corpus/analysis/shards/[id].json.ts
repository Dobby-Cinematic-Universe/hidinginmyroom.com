import type { APIRoute, GetStaticPaths } from 'astro';
import { loadAnalysisRelease } from '../../../../lib/analysis/release';
import { analysisShardId } from '../../../../lib/analysis/shards.mjs';
import type { AnalysisVideo } from '../../../../lib/analysis/explorer.mjs';

const shardSize = 100;
const idPattern = /^(\d{3,})-([a-f0-9]{16})$/;

export const getStaticPaths: GetStaticPaths = async () => {
  const release = await loadAnalysisRelease();
  if (!release) return [];
  const paths = [];
  for (let start=0,index=0;start<release.videos.length;start+=shardSize,index++) {
    const videos=release.videos.slice(start,start+shardSize),id=analysisShardId(index,videos);
    paths.push({params:{id},props:{corpusReleaseId:release.corpus_release_id,id,videos}});
  }
  return paths;
};

interface Props { corpusReleaseId:string; id:string; videos:AnalysisVideo[] }
export const GET: APIRoute = ({props}) => {
  const {corpusReleaseId,id,videos}=props as Props;
  const match=id.match(idPattern);
  if (!match || videos.length>shardSize || analysisShardId(Number(match[1]),videos)!==id) return new Response('Not found',{status:404});
  return new Response(JSON.stringify({schema_version:1,shard_id:id,corpus_release_id:corpusReleaseId,videos}),{
    headers:{'Content-Type':'application/json; charset=utf-8','Cache-Control':'public, max-age=31536000, immutable'},
  });
};
