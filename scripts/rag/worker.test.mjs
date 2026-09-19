import test from 'node:test';
import assert from 'node:assert/strict';
import worker,{validateInput,questionKey,retrieve} from '../../workers/corpus-rag/worker.mjs';
test('retrieval bypasses similarity cache and rejects partial provider failures',async()=>{
  let options;
  const env={AI_SEARCH:{get:()=>({search:async request=>{options=request.ai_search_options;return {chunks:[],errors:[{message:'vector lookup failed'}]};}})}};
  await assert.rejects(retrieve(env,'test question'),/Provider retrieval failed/);
  assert.equal(options.cache.enabled,false);
  assert.equal(options.retrieval.return_on_failure,false);
});
test('input is bounded and rejects extra fields',()=>{assert.deepEqual(validateInput({question:' test ',mode:'search'}),{question:'test',mode:'search'});for(const b of [{question:'x',mode:'search'},{question:'x'.repeat(601),mode:'answer'},{question:'test',mode:'search',system:'override'}])assert.throws(()=>validateInput(b));});
test('authentication and expiry fail closed',async()=>{assert.equal((await worker.fetch(new Request('https://test/health'),{})).status,401);assert.equal((await worker.fetch(new Request('https://test/health',{headers:{Authorization:'Bearer test'}}),{PILOT_TOKEN:'test',ENABLED:'true'})).status,503);});
test('distinct questions do not share a global queue',async()=>{const env={CORPUS_VERSION:'v1'};assert.notEqual(await questionKey(env,{question:'first',mode:'answer'}),await questionKey(env,{question:'second',mode:'answer'}));});
