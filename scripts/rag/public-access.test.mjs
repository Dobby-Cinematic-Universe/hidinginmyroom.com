import test from 'node:test';
import assert from 'node:assert/strict';
import publicSources from '../../workers/corpus-rag/public-sources.json' with {type:'json'};
import worker,{PilotBudget,readBody} from '../../workers/corpus-rag/worker.mjs';
import {publicReady,publicAdmission,Admission,clientKey} from '../../workers/corpus-rag/public-access.mjs';
const state=()=>{const data=new Map();let alarm=null;return {data,storage:{get:async k=>data.get(k),put:async(k,v)=>data.set(k,structuredClone(v)),getAlarm:async()=>alarm,setAlarm:async a=>{alarm=a;},list:async()=>new Map([...data].filter(([k])=>k.startsWith('cache:'))),delete:async keys=>keys.forEach(k=>data.delete(k)),deleteAll:async()=>data.clear()}};};
const publicEnv={PUBLIC_RELEASE_APPROVED:'true',INSTANCE:'himr-public-test',CORPUS_VERSION:'release',ALLOWED_ORIGIN:'https://hidinginmyroom.com',TURNSTILE_SECRET:'test',CLIENT_HASH_SECRET:'x'.repeat(64)};
const sources={release:'release',documents:{a:{title:'Allowed'}}};
test('production has no expiry but retains verification and manual disable',async()=>{
  const env={...publicEnv,ACCESS_MODE:'public',ENABLED:'true',CORPUS_VERSION:publicSources.release};
  const request=()=>new Request('https://test/query',{method:'POST',headers:{Origin:env.ALLOWED_ORIGIN,'Content-Type':'application/json'},body:JSON.stringify({question:'Archive question',mode:'search'})});
  for(const expiry of [undefined,'2000-01-01T00:00:00Z','invalid']){
    const response=await worker.fetch(request(),{...env,FREE_REVIEW_BEFORE:expiry});
    assert.equal(response.status,403);
    assert.equal((await response.json()).error,'Verification required.');
  }
  assert.equal((await worker.fetch(request(),{...env,ENABLED:'false'})).status,503);
});
test('body reader rejects oversized or malformed input',async()=>{
  await assert.rejects(readBody(new Request('https://test',{method:'POST',body:'x'.repeat(6145)})));
  await assert.rejects(readBody(new Request('https://test',{method:'POST',body:'not json'})));
  assert.deepEqual(await readBody(new Request('https://test',{method:'POST',body:'{"ok":true}'})),{ok:true});
});
test('publication gate rejects private index, absent approval and release mismatch',()=>{
  assert.equal(publicReady(publicEnv,sources),true);
  for(const changes of [{PUBLIC_RELEASE_APPROVED:'false'},{INSTANCE:'himr-private-pilot-v1'},{CORPUS_VERSION:'wrong'},{ALLOWED_ORIGIN:'https://hidinginmyroom.com/path'},{CLIENT_HASH_SECRET:''}])assert.equal(publicReady({...publicEnv,...changes},sources),false);
});
test('public worker is closed with missing configuration even for private bearer',async()=>{assert.equal((await worker.fetch(new Request('https://test/query',{headers:{Authorization:'Bearer secret'}}),{ACCESS_MODE:'public',PILOT_TOKEN:'secret'})).status,503);});
test('daily client pseudonym is deterministic without storing raw address',async()=>{const a=await clientKey('127.0.0.1',publicEnv.CLIENT_HASH_SECRET);assert.match(a,/^[a-f0-9]{64}$/);assert.equal(a,await clientKey('127.0.0.1',publicEnv.CLIENT_HASH_SECRET));assert.notEqual(a,await clientKey('127.0.0.2',publicEnv.CLIENT_HASH_SECRET));});
test('Turnstile requires success, correct hostname and action; failures never admit',async()=>{
  const env={...publicEnv,ADMISSION:{idFromName:x=>x,get:()=>({fetch:async()=>Response.json({allowed:true})})}};
  const req=new Request('https://test/query',{headers:{'CF-Connecting-IP':'127.0.0.1'}});
  const valid={success:true,hostname:'hidinginmyroom.com',action:'corpus-query'};
  for(const change of [{success:false},{hostname:'evil.example'},{action:'login'}])assert.equal((await publicAdmission(req,{turnstileToken:'token'},env,async()=>Response.json({...valid,...change}))).status,403);
  assert.equal(await publicAdmission(req,{turnstileToken:'token'},env,async()=>Response.json(valid)),null);
  assert.equal((await publicAdmission(req,{turnstileToken:'token'},env,async()=>{throw Error('offline');})).status,503);
});
test('admission rejects excess attempts and concurrent calls',async()=>{
  const s=state(),gate=new Admission(s),now=Date.now();s.data.set('usage',{day:new Date().toISOString().slice(0,10),count:50,hour:Math.floor(now/3600000),hourCount:20,last:0});
  assert.equal((await gate.fetch(new Request('https://test',{method:'POST',body:'{"global":false}'}))).status,429);
  gate.busy=true;assert.equal((await gate.fetch(new Request('https://test'))).status,429);
  await gate.alarm();assert.equal(s.data.size,0);
});
test('provider failure opens a per-question cooldown without a retry',async()=>{
  const s=state();let calls=0;const b=new PilotBudget(s,{AI_SEARCH:{get:()=>({search:async()=>{calls++;throw Error('offline');}})}});
  const req=()=>new Request('https://test',{method:'POST',body:JSON.stringify({question:'test question',mode:'search'})});
  assert.equal((await b.fetch(req())).status,503);
  assert.equal((await b.fetch(req())).status,503);assert.equal(calls,1);
});
test('successful retrieval is cached without another provider call',async()=>{
  const s=state();let calls=0;const b=new PilotBudget(s,{AI_SEARCH:{get:()=>({search:async()=>{calls++;return {chunks:[{text:'evidence',item:{key:'source'}}]};}})}});
  const req=()=>new Request('https://test',{method:'POST',body:JSON.stringify({question:'test question',mode:'search'})});
  assert.equal((await b.fetch(req())).status,200);
  const cached=await (await b.fetch(req())).json();assert.equal(cached.cached,true);assert.equal(calls,1);
});
test('old global counters cannot block retrieval',async()=>{
  const s=state();s.data.set('usage',{day:new Date().toISOString().slice(0,10),search:999999,answer:999999,last:Date.now()});
  const b=new PilotBudget(s,{AI_SEARCH:{get:()=>({search:async()=>({chunks:[]})})}});
  const r=await b.fetch(new Request('https://test',{method:'POST',body:JSON.stringify({question:'a valid question',mode:'search'})}));
  assert.equal(r.status,200);assert.equal('remaining' in await r.json(),false);
});
test('empty retrieval does not retry; manual resubmission can find newly indexed sources',async()=>{
  let searches=0,answers=0;const s=state();
  const b=new PilotBudget(s,{AI_SEARCH:{get:()=>({search:async()=>++searches===1?{chunks:[]}:{chunks:[{text:'Supported passage',item:{key:'source'}}]}})},AI:{run:async()=>{answers++;return {response:'Supported answer [1]'};}}});
  const req=()=>new Request('https://test',{method:'POST',body:JSON.stringify({question:'test question',mode:'answer'})});
  const empty=await (await b.fetch(req())).json();assert.deepEqual(empty.chunks,[]);assert.equal(searches,1);assert.equal(answers,0);
  const result=await (await b.fetch(req())).json();assert.equal(result.answer,'Supported answer [1]');assert.equal(searches,2);assert.equal(answers,1);
  assert.equal((await (await b.fetch(req())).json()).cached,true);assert.equal(searches,2);
});
test('persistent empty retrieval is bounded and never cached',async()=>{
  let calls=0;const s=state();const b=new PilotBudget(s,{AI_SEARCH:{get:()=>({search:async()=>{calls++;return {chunks:[]};}})}});
  const req=()=>new Request('https://test',{method:'POST',body:JSON.stringify({question:'test question',mode:'search'})});
  assert.deepEqual((await (await b.fetch(req())).json()).chunks,[]);assert.equal(calls,1);
  await b.fetch(req());assert.equal(calls,2);assert.equal([...s.data.keys()].some(k=>k.startsWith('cache:')),false);
});
test('malformed retrieval is unavailable, not no matches',async()=>{
  let calls=0;const b=new PilotBudget(state(),{AI_SEARCH:{get:()=>({search:async()=>{calls++;return {};}})}});
  const result=await b.fetch(new Request('https://test',{method:'POST',body:JSON.stringify({question:'test question',mode:'search'})}));
  assert.equal(result.status,503);assert.equal(calls,1);
});
test('empty retrieval never exposes unapproved public sources',async()=>{
  let calls=0;const b=new PilotBudget(state(),{ACCESS_MODE:'public',AI_SEARCH:{get:()=>({search:async()=>{calls++;return {chunks:[{text:'Private',item:{key:'not-approved'}}]};}})}});
  const result=await (await b.fetch(new Request('https://test',{method:'POST',body:JSON.stringify({question:'test question',mode:'search'})}))).json();
  assert.deepEqual(result.chunks,[]);assert.equal(calls,1);
});
