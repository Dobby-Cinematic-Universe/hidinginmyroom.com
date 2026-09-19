import test from 'node:test';
import assert from 'node:assert/strict';
import {projectAttribution} from '../../src/lib/corpus/public-attribution.mjs';

test('silent videos retain catalog-only attribution without claiming a transcript',()=>{
  const result=projectAttribution({recordings:{silent:{origin:'catalog',media_status:'no_audio',coverage_note:'Video has no audio stream.'}}},new Set(['silent']));
  assert.deepEqual(result.recordings.silent,{origin:'catalog',attribution:null,model:null,coverage_note:'Video has no audio stream.',media_status:'no_audio'});
});

test('public attribution still rejects private values and unknown media states',()=>{
  const project=row=>projectAttribution({recordings:{r:row}},new Set(['r']));
  assert.throws(()=>project({origin:'catalog',coverage_note:'/mnt/archive/private'}),/Unsafe/);
  assert.throws(()=>project({origin:'catalog',media_status:'inferred_silent'}),/Invalid/);
  assert.deepEqual(project({origin:'local_asr'}).recordings,{});
});
