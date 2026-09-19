import test from 'node:test';
import assert from 'node:assert/strict';
import {validatePagesCorpus} from '../check-pages-corpus.mjs';
test('Pages rejects placeholders and missing transcript summaries',()=>{
  const corpus={counts:{recordings:4064,transcript_revisions:3991}};
  assert.throws(()=>validatePagesCorpus({counts:{recordings:0}}, {summaries:[]}));
  assert.throws(()=>validatePagesCorpus(corpus,{summaries:[]}));
  assert.throws(()=>validatePagesCorpus(corpus,{summaries:[{kind:'archive'}]}));
  assert.doesNotThrow(()=>validatePagesCorpus(corpus,{summaries:[{kind:'transcript'}]}));
});
