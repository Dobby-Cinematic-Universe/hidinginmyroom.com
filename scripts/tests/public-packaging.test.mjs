import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {eventHref} from '../../src/lib/corpus/event-links.mjs';
import {projectAttribution} from '../../src/lib/corpus/public-attribution.mjs';
test('content-addressed descriptions use bounded bucket routes',()=>{
  assert.equal(eventHref('event-ab'+'1'.repeat(22)),'/corpus/event-descriptions/ab/#event-ab'+'1'.repeat(22));
  assert.equal(eventHref('reviewed-event'),'/corpus/events/reviewed-event/');
});
test('public attribution excludes review and identity artifacts',()=>{
  const result=projectAttribution({recordings:{rec_a:{origin:'cloud',model:'model',speaker_review_complete:true,secret:'hidden',coverage_note:'Display timing correction.'},rec_other:{origin:'cloud'}}},new Set(['rec_a']));
  assert.deepEqual(Object.keys(result.recordings),['rec_a']);
  assert.equal(result.recordings.rec_a.secret,undefined);
  assert.equal(result.recordings.rec_a.speaker_review_complete,undefined);
  const privateFixture=path.posix.join('/','home','test-fixture','private');
  assert.throws(()=>projectAttribution({recordings:{rec_a:{origin:'cloud',attribution:privateFixture}}},new Set(['rec_a'])));
});
