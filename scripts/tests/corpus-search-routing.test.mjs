import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

test('root-relative custom search records use a root result base before initialization',async()=>{
  const ui=await readFile(new URL('../../src/components/corpus/CorpusSearch.astro',import.meta.url),'utf8');
  const options=ui.indexOf("await pagefind.options({ baseUrl: '/' })");
  const init=ui.indexOf('await pagefind.init()');
  assert.ok(options>=0 && options<init);
  const builder=await readFile(new URL('../build-corpus-index.mjs',import.meta.url),'utf8');
  assert.ok(builder.includes('const url = `/corpus/videos/${slug}/?t='));
});

test('speaker filtering supports whole recordings and retired controls stay hidden', async () => {
  const ui = await readFile(new URL('../../src/components/corpus/CorpusSearch.astro', import.meta.url), 'utf8');
  assert.ok(ui.includes('name="speaker" disabled={recordCount === 0}'));
  assert.ok(!ui.includes('name="review_state"'));
  assert.ok(!ui.includes('name="recording_type"'));
  assert.ok(ui.includes('matching words may belong to another speaker'));
  const builder = await readFile(new URL('../build-corpus-index.mjs', import.meta.url), 'utf8');
  assert.ok(builder.includes('revision.segments.map((s) => knownSpeakerLabel(s.speaker_label)).filter(Boolean)'));
});
