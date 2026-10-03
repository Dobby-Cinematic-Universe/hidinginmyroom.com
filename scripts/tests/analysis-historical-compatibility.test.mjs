import test from 'node:test';
import assert from 'node:assert/strict';
import { analysisMatchesCorpus, historicalPassageHref } from '../../src/lib/analysis/historical-compatibility.mjs';
import { passageExtremeHref } from '../../src/lib/analysis/passage-insights.mjs';

test('historical scores require explicit original and current release binding', () => {
  const old='release_'+'a'.repeat(24),current='release_'+'b'.repeat(24),other='release_'+'c'.repeat(24);
  const pin={corpus_release:old};
  assert.equal(analysisMatchesCorpus(pin,old,old),true);
  assert.equal(analysisMatchesCorpus(pin,old,current),false);
  Object.assign(pin,{compatible_corpus_release:current,compatibility_basis:'reviewed_transcript_update_historical_scores'});
  assert.equal(analysisMatchesCorpus(pin,old,current),true);
  assert.equal(analysisMatchesCorpus(pin,old,other),false);
  assert.equal(analysisMatchesCorpus(pin,other,current),false);
  assert.equal(analysisMatchesCorpus({...pin,compatibility_basis:'anything'},old,current),false);
  assert.equal(analysisMatchesCorpus({...pin,compatible_corpus_release:old},old,old),false);
});

test('historical passage links preserve time and omit obsolete segment IDs', () => {
  const href='/corpus/videos/rec-abc/?t=3#segment-old-s1';
  assert.equal(historicalPassageHref(href,45123),'/corpus/videos/rec-abc/?t=45');
  const video={href:'/corpus/videos/rec-abc/',revision_id:'old'};
  const data={locators:[[[2,45123,'s1']]]},row=[0,3,1,1,0,1,0,null,45123,45123,2,2];
  assert.equal(passageExtremeHref(data,video,row,'low',true),'/corpus/videos/rec-abc/?t=45');
  assert.match(passageExtremeHref(data,video,row),/#segment-old-s1$/);
});
