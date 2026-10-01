import test from 'node:test';
import assert from 'node:assert/strict';
import {buildSummaryShards} from '../../src/lib/summaries/shard-build.mjs';
import {validateSummaryShardIndex,validateSummaryShardBytes} from '../../src/lib/summaries/shards.mjs';
const row=n=>({id:`recording-${String(n).padStart(3,'0')}`,kind:'transcript',sections:{
  summary:[{text:'Daniel reports a conversation.',classification:'reported_statement',source_recording_ids:['rec_'+'a'.repeat(32)]}],topics:[],events:[],uncertainties:[],
}});
test('summary shards preserve every entry, deterministic IDs, and bounded file count',async()=>{
  const release={release_id:'summaries_test',summaries:Array.from({length:129},(_,n)=>row(n))};
  const built=buildSummaryShards(release);
  assert.equal(built.assets.length,3);
  assert.deepEqual(built,buildSummaryShards({...release,summaries:[...release.summaries].reverse()}));
  const index=validateSummaryShardIndex(built.index);assert.equal(index.size,129);
  for(const asset of built.assets){
    const descriptor=built.index.shards.find(shard=>shard.id===asset.id);
    const entries=await validateSummaryShardBytes(new TextEncoder().encode(asset.text),descriptor,release.release_id);
    for(const [id,value] of entries)assert.deepEqual(value.sections,release.summaries.find(row=>row.id===id).sections);
  }
});
test('summary shard index rejects duplicate mappings and foreign URLs',()=>{
  const {index}=buildSummaryShards({release_id:'summaries_test',summaries:[row(1)]});
  const duplicate=structuredClone(index);duplicate.shards.push(duplicate.shards[0]);
  assert.throws(()=>validateSummaryShardIndex(duplicate));
  const foreign=structuredClone(index);foreign.shards[0].url='https://example.com/data.json';
  assert.throws(()=>validateSummaryShardIndex(foreign));
  assert.throws(()=>buildSummaryShards({release_id:'summaries_test',summaries:[row(1),row(1)]}));
});
test('summary shard verification refuses corrupted bytes and cross-release data',async()=>{
  const built=buildSummaryShards({release_id:'summaries_test',summaries:[row(1)]});
  const bytes=new TextEncoder().encode(built.assets[0].text),descriptor=built.index.shards[0];
  await assert.rejects(validateSummaryShardBytes(new TextEncoder().encode('{}'),descriptor,'summaries_test'),/checksum/);
  await assert.rejects(validateSummaryShardBytes(bytes,descriptor,'summaries_other'),/release/);
  await assert.rejects(validateSummaryShardBytes(new Uint8Array(4*1024*1024+1),descriptor,'summaries_test'),/bound/);
});
