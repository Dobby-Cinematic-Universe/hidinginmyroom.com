import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {knownSpeakerLabel,showSpeakerLabels} from '../../src/lib/corpus/presentation.mjs';
const segments=(...labels)=>labels.map(speaker_label=>({speaker_label}));

test('unknown speaker placeholders are hidden',()=>{
  for(const value of [null,undefined,'','  ','Unknown speaker','unknown_speaker','Unknown participant','Unknown speaker (uncertain)','Uncertain participant','SPEAKER_0000','SPEAKER_0001','Speaker 2','SPEAKER_0000 (uncertain)']) assert.equal(knownSpeakerLabel(value),null);
  assert.equal(knownSpeakerLabel(' Daniel '),'Daniel');
});
test('zero and single speaker transcripts have no speaker labels',()=>{
  for(const labels of [[],[null],['Daniel'],['Daniel','Daniel',null,'Unknown speaker'],['Daniel','Daniel (uncertain)'],['Daniel','Background noise','Playback / game audio']]) assert.equal(showSpeakerLabels(segments(...labels)),false);
});
test('multiple named speakers retain labels, raw anonymous placeholders do not',()=>{
  assert.equal(showSpeakerLabels(segments('Daniel','Mila',null)),true);
  assert.equal(showSpeakerLabels(segments('SPEAKER_0000','SPEAKER_0001')),false);
});
test('confidence UI and unknown fallbacks are removed',async()=>{
  const recording=await readFile(new URL('../../src/pages/corpus/videos/[id].astro',import.meta.url),'utf8');
  const search=await readFile(new URL('../../src/components/corpus/CorpusSearch.astro',import.meta.url),'utf8');
  for(const text of [recording,search]) {
    assert.ok(!text.includes('calibrated_probability'));
    assert.ok(!text.includes('confidence_band'));
    assert.ok(!text.includes('Unknown speaker'));
  }
  assert.ok(!search.includes('name="confidence"'));
});
