// Private packaging rehearsal. Does not change live data, publish, or call paid APIs.
import {cp,mkdir,readFile,writeFile,symlink,readdir,stat} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
import path from 'node:path';
import {validateSummaryRelease} from '../src/lib/summaries/schema.mjs';
import {projectAttribution} from '../src/lib/corpus/public-attribution.mjs';
import {normalizeSummaryUnicode} from '../src/lib/summaries/unicode.mjs';
import {createDerivedGraph} from '../src/lib/corpus/derived-graph.mjs';
import {groupingInput,groupingDigest,attachEventGroups} from '../src/lib/corpus/event-groups.mjs';
const project=process.cwd();
const productionChat=process.argv.includes('--production-chat');
let chatEnv={PUBLIC_RAG_ENABLED:'false'};
let chatBackendEnabled=false;
if(productionChat){
  const client=JSON.parse(await readFile('research/cloudflare-rag/public-v1/public-client.json','utf8'));
  const index=JSON.parse(await readFile('research/cloudflare-rag/public-v1/manifest.json','utf8'));
  if(!index.publicationApproved||index.public_origin!=='https://hidinginmyroom.com'||!/^https:\/\/[^/]+\/query$/.test(client.PUBLIC_RAG_ENDPOINT)||!client.PUBLIC_TURNSTILE_SITE_KEY)throw Error('Production chat configuration unavailable');
  chatEnv={PUBLIC_RAG_ENABLED:'true',PUBLIC_RAG_ENDPOINT:client.PUBLIC_RAG_ENDPOINT,PUBLIC_TURNSTILE_SITE_KEY:client.PUBLIC_TURNSTILE_SITE_KEY};
  chatBackendEnabled=client.PUBLIC_RAG_ENABLED===true;
}
const name=process.argv[2];
if(!/^candidate-[a-z0-9-]+$/.test(name||''))throw Error('Pass a fresh candidate-NAME');
const root=path.join(project,'research/site-release-candidates',name);
await mkdir(path.dirname(root),{recursive:true,mode:0o700});
await mkdir(root,{mode:0o700});
const pointer=JSON.parse(await readFile('research/corpus/site-previews/current.json','utf8'));
if(process.argv[3])pointer.directory=process.argv[3];
if(!/^release-[a-z0-9-]+$/.test(pointer.directory))throw Error('Unsafe preview');
const source=path.join(project,'research/corpus/site-previews',pointer.directory);
const manifest=JSON.parse(await readFile(source+'/corpus/manifest.json','utf8'));
const records=[];
for(const ref of manifest.catalog_shards){
  const bytes=await readFile(path.join(source,'corpus/releases',manifest.release_id,ref.path));
  if(createHash('sha256').update(bytes).digest('hex')!==ref.sha256)throw Error('Changed catalog');
  records.push(...JSON.parse(bytes).recordings);
}
const available=new Set(records.filter(r=>r.searchable_segment_count>0).map(r=>r.recording_id));
const summaries=JSON.parse(await readFile(source+'/summaries/refreshed.json','utf8'));
normalizeSummaryUnicode(summaries);
const held=[];
summaries.summaries=summaries.summaries.filter(s=>{
  const missing=[...new Set(Object.values(s.sections).flatMap(a=>a.flatMap(i=>i.source_recording_ids)).filter(id=>!available.has(id)))];
  if(missing.length){held.push({id:s.id,missing});return false;}return true;
});
// Simulate approved schema ONLY in this private rehearsal; deployment approval
// remains false in the candidate report. Never install this in the live repository.
for(const s of summaries.summaries)s.publication='approved';
summaries.release_id='summaries_'+createHash('sha256').update(JSON.stringify(summaries.summaries)).digest('hex').slice(0,24);
validateSummaryRelease(summaries,available);
for(const item of ['src','public','scripts','package.json','package-lock.json','tsconfig.json','astro.config.mjs'])await cp(path.join(project,item),path.join(root,item),{recursive:true});
await symlink(path.join(project,'node_modules'),path.join(root,'node_modules'),'dir');
await cp(source+'/corpus/manifest.json',root+'/src/data/corpus/manifest.json');
await writeFile(root+'/src/data/corpus/search-config.json',JSON.stringify({release_id:manifest.release_id,mode:'recording'}));
await cp(source+'/corpus/releases',root+'/src/data/corpus/releases',{recursive:true});
await writeFile(root+'/src/data/summaries/release.json',JSON.stringify(summaries));
const annotations=projectAttribution(JSON.parse(await readFile(source+'/corpus/annotations.json','utf8')),new Set(records.map(r=>r.recording_id)));
await writeFile(root+'/src/data/corpus/attribution.json',JSON.stringify(annotations));
await writeFile(root+'/src/data/corpus/graph/derived-config.json',JSON.stringify({corpus_release:manifest.release_id,summary_release:summaries.release_id}));
// Preserve existing similarity groups only for exact content-addressed event IDs.
// New descriptions remain ungrouped; do not fabricate similarity decisions.
const builder=createDerivedGraph({recordings:records,releaseId:manifest.release_id,generatedAt:manifest.generated_at,facets:manifest.facets},summaries.summaries);
const graph=builder.finish();
const groupSource=path.join(project,'research/corpus/site-previews',JSON.parse(await readFile('research/corpus/site-previews/current.json','utf8')).directory,'event-groups.json');
let grouped=graph;
try{
  const artifact=JSON.parse(await readFile(groupSource,'utf8'));
  const ids=new Set(graph.events.map(e=>e.event_id));
  artifact.groups=artifact.groups.filter(g=>g.members.every(id=>ids.has(id)));
  artifact.input_sha256=groupingDigest(groupingInput(graph));
  grouped=attachEventGroups(graph,artifact);
  await writeFile(root+'/src/data/corpus/graph/event-groups.json',JSON.stringify(artifact));
}catch(error){if(error.code!=='ENOENT')throw error;}
await writeFile(root+'/candidate.config.mjs',`import base from './astro.config.mjs';\nexport default {...base,cacheDir:'./.candidate-astro',vite:{...base.vite,cacheDir:'./.candidate-vite'},};\n`);
const report={publication_approved:false,deployment_performed:false,approval_schema_simulation:true,source:pointer.directory,corpus_release:manifest.release_id,recordings:records.length,transcripts:available.size,summaries:summaries.summaries.length,held_summaries:held,build:'pending',release_blockers:['Archive overview remains pending; refresh final summaries before release.','Catalog-only source with invalid segment timing holds two broader summaries.','Derived Entities & Events currently exists only in local preview; production graph is empty.','Public attribution projection and production chat configuration still need packaging.','Cloud indexing and real-domain launch checks remain outstanding.'],free_pages_limits:{files:20000,bytes_per_file:25*1024*1024}};
const save=()=>writeFile(root+'/candidate-report.json',JSON.stringify(report,null,2)+'\n');await save();
report.production_chat={included:productionChat,backend_enabled_at_build:chatBackendEnabled,origin:productionChat?'https://hidinginmyroom.com':null};
if(summaries.summaries.some(s=>s.kind==='archive'))report.release_blockers=report.release_blockers.filter(s=>!s.startsWith('Archive overview'));
if(!held.length){report.release_blockers=report.release_blockers.filter(s=>!s.startsWith('Catalog-only source'));await save();}
report.graph={events:graph.events.length,groups:grouped.eventGroups?.length||0,compact_event_pages:true};
report.public_attribution_records=Object.keys(annotations.recordings).length;
report.release_blockers=report.release_blockers.filter(s=>!s.startsWith('Derived Entities')&&!s.startsWith('Public attribution'));
await save();
const run=(args)=>new Promise((resolve,reject)=>{const child=spawn(process.execPath,args,{cwd:root,env:{...process.env,ASTRO_TELEMETRY_DISABLED:'1',...chatEnv},stdio:'inherit'});child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(Error('Candidate command exited '+code)));});
try{
  await run([path.join(project,'node_modules/astro/bin/astro.mjs'),'build','--config','candidate.config.mjs']);
  const search=JSON.parse(await readFile(source+'/search/search-manifest.json','utf8'));
  if(search.release_id!==manifest.release_id||search.recording_count!==available.size)throw Error('Cached search does not match corpus');
  await cp(source+'/search',root+'/dist/corpus',{recursive:true});
  await run(['scripts/check-search-isolation.mjs']);
  const files=[];async function walk(dir){for(const entry of await readdir(dir,{withFileTypes:true})){const file=path.join(dir,entry.name);if(entry.isDirectory())await walk(file);else{const s=await stat(file);files.push({path:path.relative(root+'/dist',file),bytes:s.size});}}}await walk(root+'/dist');
  report.output={files:files.length,bytes:files.reduce((n,f)=>n+f.bytes,0),oversized:files.filter(f=>f.bytes>25*1024*1024),largest:files.sort((a,b)=>b.bytes-a.bytes).slice(0,10)};
  report.free_pages_fit=files.length<=20000&&!report.output.oversized.length;
  report.build='passed';
}catch(error){report.build='failed';report.error=error.message;process.exitCode=1;}finally{await save();console.log(JSON.stringify(report));}
