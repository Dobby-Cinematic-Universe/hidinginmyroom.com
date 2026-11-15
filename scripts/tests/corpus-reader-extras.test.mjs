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

test('version phrases are reworded for readers without changing the stored forms',async()=>{
  const {versionPhraseText}=await import('../../src/lib/corpus/public-reader-extras.mjs');
  assert.equal(versionPhraseText('same video id, 4 minutes shorter (4 minutes less at the start)'),'same video id, 4 minutes shorter at the start');
  assert.equal(versionPhraseText('earlier upload, 9 days before, 100 seconds longer (100 seconds more inside)'),'earlier upload, 9 days before, 100 seconds longer in the middle');
  assert.equal(versionPhraseText('same broadcast on Twitch, 3 minutes longer (68 seconds more inside; 24 seconds more inside; 3 smaller differences)'),
    'same broadcast on Twitch, 3 minutes longer (68 seconds extra in the middle; 24 seconds extra in the middle; 3 smaller differences)');
  assert.equal(versionPhraseText('same video id, 37 minutes longer (40 minutes more at the end; 2 minutes less at the start)'),'same video id, 37 minutes longer (40 minutes extra at the end; 2 minutes missing at the start)');
  assert.equal(versionPhraseText('re-upload 1 days later, 10 minutes longer (10 minutes more at the end)'),'re-upload 1 day later, 10 minutes longer at the end');
  for(const p of ['same video id, same length','same upload from another archive (separate machine transcript)','another copy with the same transcript, 7 seconds longer (the transcript ends where the shorter copy ends)'])
    assert.equal(versionPhraseText(p),p);
});

test('version links pass only fixed phrase forms and never point at the recording itself',async()=>{
  const {projectVersionLinks,VERSION_PHRASE}=await import('../../src/lib/corpus/public-reader-extras.mjs');
  const a='rec_'+'a'.repeat(32),b='rec_'+'b'.repeat(32),gone='rec_'+'c'.repeat(32);
  const cat=[{recording_id:a},{recording_id:b}];
  const out=projectVersionLinks({recordings:{[a]:[{target:b,phrase:'re-upload 64 days later, 11 seconds longer (11 seconds more at the start)'},{target:gone,phrase:'same upload from another archive (separate machine transcript)'}],[gone]:[{target:a,phrase:'same video id, same length'}]}},cat);
  assert.deepEqual(out.recordings,{[a]:[{target:b,phrase:'re-upload 64 days later, 11 seconds longer (11 seconds more at the start)'}]});
  for(const phrase of ['another copy of the same upload (identical transcript)','same broadcast on Twitch, about the same length (9 seconds more at the end; 7 seconds less at the start; 1 smaller difference)',
    'same video id, 28 minutes shorter (28 minutes less at the end)','same broadcast on YouTube, 5 h 14 min longer (2 h 31 min more at the start; 2 h 43 min more at the end)','earlier upload, 1 day before, same length',
    'another copy with the same transcript, 19 minutes longer (the transcript ends where the shorter copy ends)','same video id from another archive, 42 minutes longer (one copy has no transcript)'])assert.match(phrase,VERSION_PHRASE);
  for(const phrase of ['re-upload 64 days later, 11 seconds longer because a passage was removed','same video id, shorter','see /mnt/archive','another version (not compared directly)',''])
    assert.throws(()=>projectVersionLinks({recordings:{[a]:[{target:b,phrase}]}},cat),/Invalid version link/);
  assert.throws(()=>projectVersionLinks({recordings:{[a]:[{target:a,phrase:'same video id, same length'}]}},cat),/Invalid version link/);
  assert.throws(()=>projectVersionLinks({recordings:{[a]:[{target:b,phrase:'same video id, same length',title:'x'}]}},cat),/Unexpected version link field/);
});
