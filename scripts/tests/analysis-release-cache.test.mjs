import test from 'node:test';
import assert from 'node:assert/strict';
import {analysisReleaseCacheKey,analysisReleaseIdentity,createAnalysisReleaseCache} from '../../src/lib/analysis/release-cache.mjs';

const stat=(overrides={})=>({dev:1n,ino:2n,size:100n,mtimeNs:300n,ctimeNs:400n,...overrides});

test('analysis cache key changes for pointer, file identity/mtime/size and corpus release changes',()=>{
  const file=analysisReleaseIdentity(stat()),key=analysisReleaseCacheKey('pointer-a',file,'corpus-a');
  assert.equal(analysisReleaseCacheKey('pointer-a',file,'corpus-a'),key);
  assert.notEqual(analysisReleaseCacheKey('pointer-b',file,'corpus-a'),key);
  assert.notEqual(analysisReleaseCacheKey('pointer-a',analysisReleaseIdentity(stat({ino:3n})),'corpus-a'),key);
  assert.notEqual(analysisReleaseCacheKey('pointer-a',analysisReleaseIdentity(stat({mtimeNs:301n})),'corpus-a'),key);
  assert.notEqual(analysisReleaseCacheKey('pointer-a',analysisReleaseIdentity(stat({size:101n})),'corpus-a'),key);
  assert.notEqual(analysisReleaseCacheKey('pointer-a',file,'corpus-b'),key);
});

test('identical requests share one in-flight load and reuse only its successful result',async()=>{
  const cache=createAnalysisReleaseCache();let resolve,calls=0;
  const load=()=>{calls++;return new Promise(r=>{resolve=r;});};
  const first=cache('same-key',load),second=cache('same-key',load);
  assert.equal(first,second);assert.equal(calls,0);
  await Promise.resolve();assert.equal(calls,1);resolve({release:'one'});
  assert.deepEqual(await first,{release:'one'});
  assert.deepEqual(await cache('same-key',async()=>{throw new Error('cache should be hit');}),{release:'one'});
  assert.equal(calls,1);
});

test('failed DEV loads are removed so a corrected file can be retried',async()=>{
  const cache=createAnalysisReleaseCache();let calls=0;
  await assert.rejects(cache('key',async()=>{calls++;throw new Error('temporary parse failure');}),/temporary parse failure/);
  assert.deepEqual(await cache('key',async()=>{calls++;return 'corrected';}),'corrected');
  assert.equal(calls,2);
});
