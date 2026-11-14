import test from 'node:test';
import assert from 'node:assert/strict';
import {projectTitleHistory} from '../../src/lib/corpus/public-title-history.mjs';

const catalog=[{recording_id:'a',title:'Current title'}];
test('earlier titles are kept for catalog recordings, deduplicated against the catalog title',()=>{
  const out=projectTitleHistory({recordings:{a:[{title:'Old title',seen:'2017-05-06'},{title:'current TITLE',seen:'2018-01-01'},{title:'Old title',seen:'2019-01-01'}],gone:[{title:'x',seen:'2017-05-06'}]}},catalog);
  assert.deepEqual(out.recordings,{a:[{title:'Old title',seen:'2017-05-06'}]});
});
test('malformed rows and unexpected fields are rejected',()=>{
  assert.throws(()=>projectTitleHistory({recordings:{a:[{title:'Old',seen:'2017-05'}]}},catalog),/Invalid title history row/);
  assert.throws(()=>projectTitleHistory({recordings:{a:[{title:'',seen:'2017-05-06'}]}},catalog),/Invalid title history row/);
  assert.throws(()=>projectTitleHistory({recordings:{a:[{title:'Old',seen:'2017-05-06',path:'/mnt/archive'}]}},catalog),/Unexpected/);
});
