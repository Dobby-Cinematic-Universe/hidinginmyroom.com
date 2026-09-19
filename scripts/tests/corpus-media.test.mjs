import test from 'node:test';
import assert from 'node:assert/strict';
import {playableSources} from '../../src/lib/corpus/media.mjs';
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
