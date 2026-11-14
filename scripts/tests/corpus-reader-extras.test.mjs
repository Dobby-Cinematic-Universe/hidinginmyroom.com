import test from 'node:test';
import assert from 'node:assert/strict';
import {projectSearchAliases,expandQuery,projectRelatedRecordings,projectChapters} from '../../src/lib/corpus/public-reader-extras.mjs';

const a='rec_'+'a'.repeat(32),b='rec_'+'b'.repeat(32),c='rec_'+'c'.repeat(32);
const catalog=[{recording_id:a,searchable_segment_count:10},{recording_id:b,searchable_segment_count:0}];

test('alias sets expand a query to the other spellings and nothing else',()=>{
  const {sets}=projectSearchAliases({sets:[{canonical:'Streamlabs',category:'product',forms:['streamlabs','stream labs']},{canonical:'Kick',category:'product',forms:['kick','kik']}]});
  assert.deepEqual(expandQuery('My Stream Labs settings',sets).map(x=>x.query),['my streamlabs settings']);
  assert.deepEqual(expandQuery('kick ban',sets).map(x=>x.form),['kik']);
  assert.deepEqual(expandQuery('kicking',sets),[]);
  assert.deepEqual(expandQuery('ki',sets),[]);
  assert.throws(()=>projectSearchAliases({sets:[{canonical:'X',category:'person',forms:['aa','bb']}]}),/Invalid alias set/);
  assert.throws(()=>projectSearchAliases({sets:[{canonical:'X',category:'place',forms:['only']}]}),/Invalid alias forms/);
  assert.throws(()=>projectSearchAliases({sets:[{canonical:'X',category:'place',forms:['aa','bb']},{canonical:'Y',category:'place',forms:['bb','cc']}]}),/two sets/);
});

test('related recordings keep only links between recordings of this release',()=>{
  const out=projectRelatedRecordings({recordings:{[a]:[{target:b,position_ms:1200},{target:c,position_ms:null}],[c]:[{target:a,position_ms:0}]}},catalog);
  assert.deepEqual(out.recordings,{[a]:[{target:b,position_ms:1200}]});
  assert.throws(()=>projectRelatedRecordings({recordings:{[a]:[{target:a,position_ms:0}]}},catalog),/Invalid related recording target/);
  assert.throws(()=>projectRelatedRecordings({recordings:{[a]:[{target:b,position_ms:-1}]}},catalog),/position/);
});

test('chapters need ordered times and plain lower-case words, and a transcript',()=>{
  const rows=[{start_ms:0,end_ms:1000,terms:['visa','tax']},{start_ms:1000,end_ms:5000,terms:[]}];
  assert.deepEqual(Object.keys(projectChapters({recordings:{[a]:rows,[b]:rows,[c]:rows}},catalog).recordings),[a]);
  assert.throws(()=>projectChapters({recordings:{[a]:[rows[1],rows[0]]}},catalog),/Invalid chapter times/);
  assert.throws(()=>projectChapters({recordings:{[a]:[{...rows[0],terms:['Someone']},rows[1]]}},catalog),/Invalid chapter terms/);
  assert.throws(()=>projectChapters({recordings:{[a]:[rows[0]]}},catalog),/Invalid chapter list/);
});

test('mention bursts accept only games, products and platforms with ordered months',async()=>{
  const {projectMentionBursts}=await import('../../src/lib/corpus/public-reader-extras.mjs');
  const row={subject:'Elden Ring',type:'game',start:'2022-02',end:'2023-02',months:13,ratio:5.7,recordings:140,end_known:true,both_transcript_kinds:true};
  assert.equal(projectMentionBursts({bursts:[row]}).bursts.length,1);
  assert.throws(()=>projectMentionBursts({bursts:[{...row,type:'person'}]}),/Invalid burst/);
  assert.throws(()=>projectMentionBursts({bursts:[{...row,start:'2023-05'}]}),/Invalid burst/);
  assert.throws(()=>projectMentionBursts({bursts:[{...row,recordings:0}]}),/Invalid burst numbers/);
});
