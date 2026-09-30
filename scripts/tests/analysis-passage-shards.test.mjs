import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, webcrypto } from 'node:crypto';
import { planPassageShards, renderPassageShard } from '../../src/lib/analysis/passage-shard-plan.mjs';
import { validatePassageShardIndex, validatePassageShardBytes, PASSAGE_SHARD_MAX_BYTES } from '../../src/lib/analysis/passage-shards.mjs';

if (!globalThis.crypto) globalThis.crypto = webcrypto;
function fixture(count=1, segmentCount=1) {
  const release={schema_version:1,corpus_release_id:'corpus-a',questionnaire_version:'bank-a',model:'model-a',generated_at:'now',questions:[{id:'joy',type:'score'}],videos:[]};
  const data=new Map();
  for(let i=0;i<count;i++){
    const video={recording_id:`rec_${String(i).padStart(32,'0')}`,revision_id:`rev_${i}`,href:`/corpus/videos/rec-${i}/`};release.videos.push(video);
    const segments=Array.from({length:segmentCount},(_,j)=>`seg_${j}_${'a'.repeat(140)}`);
    data.set(video.recording_id,{schema_version:1,corpus_release_id:'corpus-a',questionnaire_version:'bank-a',model:'model-a',source_generated_at:'now',recording:{recording_id:video.recording_id,revision_id:video.revision_id},passages:[{passage_id:`${video.revision_id}:0`,revision_id:video.revision_id,chunk_index:0,start_ms:0,end_ms:1000,source:{href:`${video.href}?t=0#segment-${video.revision_id}-${segments[0]}`,segment_ids:segments},scores:{joy:{value:2,low:1,high:3,coverage:1,applicable:true}}}]});
  }
  return {release,data,read:async id=>data.get(id)};
}
const descriptorFor=(text,recording_ids)=>{const id=createHash('sha256').update(text).digest('hex');return {id,url:`/corpus/analysis/passage-shards/${id}.json`,bytes:Buffer.byteLength(text),count:recording_ids.length,recording_ids};};

test('passage groups enforce inventory, byte and recording bounds and verify on browser read',async()=>{
  const {release,read}=fixture(65);
  const plan=await planPassageShards(release,read);
  assert.deepEqual(plan.map(shard=>shard.count),[32,32,1]);
  assert.equal(validatePassageShardIndex(plan,release).size,65);
  for(const shard of plan){
    const text=await renderPassageShard(shard,release,read);
    const result=await validatePassageShardBytes(new TextEncoder().encode(text),shard,release);
    assert.deepEqual([...result.keys()],shard.recording_ids);
    assert.ok(Buffer.byteLength(text)<=PASSAGE_SHARD_MAX_BYTES);
  }
  const large=fixture(5,10000),largePlan=await planPassageShards(large.release,large.read);
  assert.deepEqual(largePlan.map(shard=>shard.count),[2,2,1]);
  for(const shard of largePlan)assert.ok(shard.bytes<=PASSAGE_SHARD_MAX_BYTES);
});
test('missing source and changed source fail the production plan or rendering',async()=>{
  const {release,data,read}=fixture(2);
  const plan=await planPassageShards(release,read);
  data.get(release.videos[0].recording_id).passages[0].scores.joy.value=3;
  await assert.rejects(()=>renderPassageShard(plan[0],release,read),/changed after planning/);
  data.delete(release.videos[0].recording_id);
  await assert.rejects(()=>planPassageShards(release,read),/Invalid passage data/);
});
test('index rejects duplicates, omissions, unknown recordings, extra fields and foreign URLs',async()=>{
  const {release,read}=fixture(2),plan=await planPassageShards(release,read);
  const mutations=[
    shards=>{shards[0].recording_ids[1]=shards[0].recording_ids[0];},
    shards=>{shards[0].recording_ids.pop();shards[0].count=1;},
    shards=>{shards[0].recording_ids[0]='rec_unknown';},
    shards=>{shards[0].url='https://example.test/payload.json';},
    shards=>{shards[0].raw_receipt={};},
    shards=>{shards[0].bytes=PASSAGE_SHARD_MAX_BYTES+1;},
  ];
  for(const mutate of mutations){const copy=structuredClone(plan);mutate(copy);assert.throws(()=>validatePassageShardIndex(copy,release));}
  assert.equal(validatePassageShardIndex(undefined,release),null,'development uses per-recording endpoint');
  assert.throws(()=>validatePassageShardIndex([],release),'empty published index does not downgrade to development');
});
test('browser rejects corrupt bytes and correctly hashed but stale or malformed payloads',async()=>{
  const {release,read}=fixture(2),plan=await planPassageShards(release,read),shard=plan[0];
  const text=await renderPassageShard(shard,release,read);
  await assert.rejects(()=>validatePassageShardBytes(new TextEncoder().encode(`${text} `),shard,release),/payload size/);
  const changed=text.replace('corpus-a','corpus-b');
  await assert.rejects(()=>validatePassageShardBytes(new TextEncoder().encode(changed),shard,release),/payload digest/);
  for(const mutate of [
    value=>{value.source_generated_at='old';},
    value=>{value.recordings.reverse();},
    value=>{value.recordings[0].passages[0].source.href='https://example.test';},
    value=>{value.raw_receipt={};},
  ]){
    const value=JSON.parse(text);mutate(value);const encoded=JSON.stringify(value),descriptor=descriptorFor(encoded,shard.recording_ids);
    await assert.rejects(()=>validatePassageShardBytes(new TextEncoder().encode(encoded),descriptor,release));
  }
});
