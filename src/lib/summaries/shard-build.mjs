import {createHash} from 'node:crypto';
import {validateSummaryShardIndex,validateSummaryShard} from './shards.mjs';

export function buildSummaryShards(release) {
  const rows=release.summaries.filter(row=>row.kind==='transcript')
    .map(({id,sections})=>({id,sections})).sort((a,b)=>a.id.localeCompare(b.id));
  const assets=[],shards=[];
  const encode=summaries=>JSON.stringify({schema_version:1,release_id:release.release_id,summaries});
  const add=summaries=>{
    const text=encode(summaries),id=createHash('sha256').update(text).digest('hex');
    const descriptor={id,url:`/corpus/summaries/data/shards/${id}.json`,sha256:id,count:summaries.length,summary_ids:summaries.map(row=>row.id)};
    validateSummaryShard(JSON.parse(text),descriptor,release.release_id);
    assets.push({id,text});shards.push(descriptor);
  };
  let pending=[];
  for(const row of rows){
    if(pending.length&&(pending.length===64||Buffer.byteLength(encode([...pending,row]))>4*1024*1024)){
      add(pending);pending=[];
    }
    pending.push(row);
    if(Buffer.byteLength(encode(pending))>4*1024*1024)throw Error('One summary exceeds shard bound');
  }
  if(pending.length)add(pending);
  const index={schema_version:1,release_id:release.release_id,shards};
  validateSummaryShardIndex(index);
  return {index,assets};
}
