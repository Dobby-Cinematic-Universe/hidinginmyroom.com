import test from 'node:test';
import assert from 'node:assert/strict';
import {summaryQualityFindings} from '../summary-quality.mjs';

test('release audit finds placeholder-only prose in every section without changing it', () => {
  const release={summaries:[{id:'monthly-2020-01',sections:{summary:[{text:' PLACEHOLDER. '}],events:[{text:'placeholder'}],topics:[{text:'Daniel calls the image a placeholder.'}],uncertainties:[]}}]};
  const before=JSON.stringify(release);
  assert.deepEqual(summaryQualityFindings(release),[
    {id:'monthly-2020-01',section:'summary',index:0,reason:'placeholder or repair-marker text'},
    {id:'monthly-2020-01',section:'events',index:0,reason:'placeholder or repair-marker text'}
  ]);
  assert.equal(JSON.stringify(release),before);
});

test('numbered placeholders and internal repair markers also block release', () => {
  for(const text of ['placeholder2','Placeholder?', 'skip','x','TODO','…','Valid opening.120,e120.text_dup_removed']) {
    assert.equal(summaryQualityFindings({summaries:[{id:'monthly-2020-01',sections:{summary:[{text}]}}]}).length,1);
  }
});

test('empty and substantive releases have no placeholder findings', () => {
  assert.deepEqual(summaryQualityFindings({summaries:[]}),[]);
  assert.deepEqual(summaryQualityFindings({summaries:[{id:'monthly-2020-01',sections:{summary:[{text:'Camera reviews and lens comparisons.'}]}}]}),[]);
});
