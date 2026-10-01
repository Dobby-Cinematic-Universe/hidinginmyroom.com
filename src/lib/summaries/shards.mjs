const idPattern=/^[a-z0-9][a-z0-9-]{0,95}$/;
const shardPattern=/^[a-f0-9]{64}$/;
const sections=['summary','topics','events','uncertainties'];
const classifications=['reported_statement','reported_allegation','uncertainty'];
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)
  &&Object.keys(value).sort().join('|')===[...keys].sort().join('|');
const releaseId=value=>typeof value==='string'&&/^summaries_[a-z0-9_-]{1,80}$/.test(value);

export function validateSummaryShardIndex(value) {
  if(!exact(value,['schema_version','release_id','shards'])||value.schema_version!==1
    ||!releaseId(value.release_id)||!Array.isArray(value.shards)||value.shards.length>1000)throw Error('Invalid summary index');
  const entries=new Map(),shards=new Set();
  for(const shard of value.shards){
    if(!exact(shard,['id','url','sha256','count','summary_ids'])||!shardPattern.test(shard.id)
      ||shard.sha256!==shard.id||shard.url!==`/corpus/summaries/data/shards/${shard.id}.json`
      ||shards.has(shard.id)||!Number.isInteger(shard.count)||shard.count<1||shard.count>64
      ||!Array.isArray(shard.summary_ids)||shard.summary_ids.length!==shard.count)throw Error('Invalid summary shard descriptor');
    shards.add(shard.id);
    for(const id of shard.summary_ids){
      if(typeof id!=='string'||!idPattern.test(id)||entries.has(id))throw Error('Invalid or duplicate summary ID');
      entries.set(id,shard);
    }
  }
  if(entries.size>20000)throw Error('Summary inventory exceeds bound');
  return entries;
}

export function validateSummaryShard(value,descriptor,expectedRelease) {
  if(!exact(value,['schema_version','release_id','summaries'])||value.schema_version!==1
    ||value.release_id!==expectedRelease||!Array.isArray(value.summaries)
    ||value.summaries.length!==descriptor.count)throw Error('Summary shard release/count mismatch');
  const entries=new Map();
  for(const [ordinal,row] of value.summaries.entries()){
    if(!exact(row,['id','sections'])||row.id!==descriptor.summary_ids[ordinal]
      ||entries.has(row.id)||!exact(row.sections,sections))throw Error('Summary shard entry mismatch');
    for(const section of sections){
      const items=row.sections[section];
      if(!Array.isArray(items)||items.length>100||(section==='summary'&&!items.length))throw Error('Invalid summary section');
      for(const item of items){
        if(!exact(item,['text','classification','source_recording_ids'])||typeof item.text!=='string'
          ||!item.text.trim()||item.text.length>4000||!classifications.includes(item.classification)
          ||(section==='uncertainties'&&item.classification!=='uncertainty')
          ||!Array.isArray(item.source_recording_ids)||!item.source_recording_ids.length
          ||item.source_recording_ids.some(id=>typeof id!=='string'||!/^rec_[a-f0-9]{32}$/.test(id))
          ||new Set(item.source_recording_ids).size!==item.source_recording_ids.length)throw Error('Invalid summary item');
      }
    }
    entries.set(row.id,row);
  }
  return entries;
}

export async function validateSummaryShardBytes(bytes,descriptor,expectedRelease) {
  if(!(bytes instanceof Uint8Array)||bytes.byteLength>4*1024*1024)throw Error('Summary shard exceeds bound');
  const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(n=>n.toString(16).padStart(2,'0')).join('');
  if(hash!==descriptor.sha256)throw Error('Summary shard checksum mismatch');
  return validateSummaryShard(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)),descriptor,expectedRelease);
}
