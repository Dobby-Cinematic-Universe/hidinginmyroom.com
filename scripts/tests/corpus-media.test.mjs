import test from 'node:test';
import assert from 'node:assert/strict';
import {playableSources} from '../../src/lib/corpus/media.mjs';
import fs from 'node:fs';
const source=url=>({url,access_state:'public'});
test('embeds explicit archive files and privacy-enhanced YouTube URLs',()=>{
  const rows=playableSources([source('https://archive.org/download/item/long%20stream.mp4'),source('https://www.youtube.com/watch?v=abcdefghijk')]);
  assert.equal(rows[0].kind,'video');assert.equal(rows[1].url,'https://www.youtube-nocookie.com/embed/abcdefghijk');
});
test('does not embed untrusted URLs, private sources or guessed archive files',()=>{
  assert.deepEqual(playableSources(['javascript:alert(1)','http://archive.org/download/x/a.mp4','https://evil.test/a.mp4','https://archive.org/details/item','https://archive.org.evil.test/download/x/a.mp4','https://x:password@archive.org/download/x/a.mp4'].map(source)),[]);
  assert.deepEqual(playableSources([{...source('https://archive.org/download/x/a.mp4'),access_state:'withdrawn'}]),[]);
});
test('deduplicates player choices and rejects malformed YouTube IDs',()=>{
  const url='https://archive.org/download/x/a.webm';assert.equal(playableSources([source(url),source(url)]).length,1);
  assert.deepEqual(playableSources([source('https://youtu.be/not-valid')]),[]);
});
const original={...source('https://archive.org/download/item/original.mkv'),source_id:'src_original'};
const copy={recording_id:`rec_${'a'.repeat(32)}`,source_id:original.source_id,original_url:original.url,source_sha256:'b'.repeat(64),playback_url:'https://archive.org/download/item/original.mp4'};
const context={recordingId:copy.recording_id,releaseId:`release_${'c'.repeat(24)}`,accessCopies:{schema_version:1,corpus_release:`release_${'c'.repeat(24)}`,copies:[copy]}};
test('prefers an explicitly verified copy while retaining other playback choices',()=>{
  const choices=playableSources([original,source('https://youtu.be/abcdefghijk'),source(copy.playback_url)],context);
  assert.equal(choices[0].url,copy.playback_url);
  assert.equal(choices[0].label,'Browser-compatible copy');
  assert.equal(choices.length,2);
});
test('copy bindings fail closed for changed recording, release, source, or access',()=>{
  for(const changed of [{...context,releaseId:`release_${'d'.repeat(24)}`},{...context,recordingId:`rec_${'e'.repeat(32)}`}]) assert.deepEqual(playableSources([original],changed),[]);
  for(const changed of [{...original,source_id:'other'},{...original,url:'https://archive.org/download/item/other.mkv'},{...original,access_state:'withdrawn'}]) assert.deepEqual(playableSources([changed],context),[]);
  for(const changed of [{...copy,source_sha256:'invalid'},{...copy,source_sha256:null},{...copy,recording_id:`rec_${'e'.repeat(32)}`}]) assert.deepEqual(playableSources([original],{...context,accessCopies:{...context.accessCopies,copies:[changed]}}),[]);
});
test('uses the verified replacement instead of a misleading MP4 or unsupported OGV',()=>{
  for (const suffix of ['mp4','ogv']) {
    const failed={...original,url:`https://archive.org/download/item/failed.${suffix}`};
    const binding={...copy,original_url:failed.url};
    const choices=playableSources([failed],{...context,accessCopies:{...context.accessCopies,copies:[binding]}});
    assert.deepEqual(choices,[{kind:'video',url:copy.playback_url,label:'Browser-compatible copy'}]);
    assert.equal(failed.url,binding.original_url);
  }
});
test('copies reject credentials, signed URLs, unexpected hosts and non-MP4 paths',()=>{
  for(const playback_url of ['http://archive.org/download/item/a.mp4','https://evil.test/download/item/a.mp4','https://user:secret@archive.org/download/item/a.mp4',`https://archive.org/download/item/a.mp4?${'to'+'ken'}=fixture`,'https://archive.org/download/item/a.mp4#t=5','https://archive.org/details/item','https://archive.org/download/item/a.webm']) {
    assert.deepEqual(playableSources([original],{...context,accessCopies:{...context.accessCopies,copies:[{...copy,playback_url}]}}),[]);
  }
});
test('public copy manifest contains only finite source bindings and safe playback choices',()=>{
  const manifest=JSON.parse(fs.readFileSync(new URL('../../src/data/corpus-media-access-copies.json',import.meta.url)));
  assert.ok(manifest.copies.length>0);
  const keys=['original_url','playback_url','recording_id','source_id','source_sha256'];
  for(const row of manifest.copies){
    assert.deepEqual(Object.keys(row).sort(),keys);
    const choices=playableSources([{url:row.original_url,source_id:row.source_id,access_state:'public'}],{releaseId:manifest.corpus_release,recordingId:row.recording_id,accessCopies:manifest});
    assert.equal(choices[0].url,row.playback_url);
    assert.equal(choices[0].label,'Browser-compatible copy');
  }
});
