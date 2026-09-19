import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../../workers/release-assets/worker.mjs';

test('release downloads expose only hashed bundles and never allow writes or listing',async()=>{
  let reads=0;
  const env={RELEASES:{async get(){reads++;return {body:'abc',size:3,httpEtag:'"test"'};}}};
  for(const path of ['/','/releases/','/private.json','/releases/latest.tar.gz']){
    assert.equal((await worker.fetch(new Request('https://example.com'+path),env)).status,404);
  }
  const url='https://example.com/releases/'+'a'.repeat(64)+'.tar.gz';
  assert.equal((await worker.fetch(new Request(url,{method:'PUT'}),env)).status,404);
  assert.equal(reads,0);
  const response=await worker.fetch(new Request(url),env);
  assert.equal(await response.text(),'abc');
  assert.equal(response.headers.get('Content-Length'),'3');
  assert.match(response.headers.get('Cache-Control'),/immutable/);
  assert.equal(await (await worker.fetch(new Request(url,{method:'HEAD'}),env)).text(),'');
  assert.equal((await worker.fetch(new Request(url),{RELEASES:{get:async()=>null}})).status,404);
});
