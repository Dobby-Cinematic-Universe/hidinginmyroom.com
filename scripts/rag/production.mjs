import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import {createHash,randomBytes} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {api,instance,root,read,save,credentials} from './cloudflare.mjs';
if(process.env.RAG_PROFILE!=='public')throw Error('Set RAG_PROFILE=public');
const configPath='workers/corpus-rag/wrangler.production.jsonc';
const config=JSON.parse(await readFile(configPath,'utf8'));
const command=process.argv[2];
if(command==='prepare'){
  if(!process.argv.includes('--publication-approved'))throw Error('Explicit publication approval is required');
  const full=process.argv.includes('--full');
  const pilot=full?root:'research/cloudflare-rag/pilot-v1';
  const m=JSON.parse(await readFile(pilot+'/manifest.json','utf8'));
  if(!m.freeOnlyConfirmed||!m.uploadAuthorized||(!full&&m.selected_recordings.length>100)||(full&&(m.scope!=='full'||!m.publicationApproved)))throw Error('Invalid export scope');
  await mkdir(root+'/documents',{recursive:true,mode:0o700});
  const documents={};
  for(const d of m.documents){
    if(!/^(summary|transcript|metadata|synthesis)-[a-f0-9]{32}\.txt$/.test(d.key)||!/^\/corpus\/(videos\/rec-[a-f0-9]+\/(#[-\w]+)?|summaries\/[a-z0-9_-]+\/)$/.test(d.href))throw Error('Unsafe source mapping');
    const bytes=await readFile(pilot+'/documents/'+d.key);
    if(createHash('sha256').update(bytes).digest('hex')!==d.sha256)throw Error('Changed pilot content');
    if(!full)await copyFile(pilot+'/documents/'+d.key,root+'/documents/'+d.key);
    documents[d.key]={title:d.title,href:d.href,kind:d.kind};
  }
  const release=createHash('sha256').update(JSON.stringify(m.documents)).digest('hex');
  await save('manifest.json',{...m,private:false,publicationApproved:true,public_origin:config.vars.ALLOWED_ORIGIN,release});
  await writeFile('workers/corpus-rag/public-sources.json',JSON.stringify({release,documents})+'\n');
  console.log(JSON.stringify({release,documents:m.documents.length,publicationApproved:true}));
}else if(command==='provision'){
  const m=await read('manifest.json');if(!m.publicationApproved)throw Error('Publication not approved');
  const instances=await api('ai-search/instances');const list=Array.isArray(instances)?instances:instances.data||instances.items||[];
  if(!list.some(x=>x.id===instance))await api('ai-search/instances',{method:'POST',body:{id:instance,embedding_model:'@cf/qwen/qwen3-embedding-0.6b',chunk:true,chunk_size:400,chunk_overlap:10,index_method:{keyword:true,vector:true},public_endpoint_params:{enabled:false}}});
  const widgets=await api('challenges/widgets');
  const existing=(Array.isArray(widgets)?widgets:widgets.result||[]).find(w=>w.name==='HIMR corpus search');
  const widget=existing?await api('challenges/widgets/'+existing.sitekey):await api('challenges/widgets',{method:'POST',body:{name:'HIMR corpus search',domains:[new URL(config.vars.ALLOWED_ORIGIN).hostname],mode:'managed'}});
  if(!widget.secret)throw Error('Turnstile secret missing');
  await save('turnstile.json',widget);
  console.log('Public index and hostname-restricted Turnstile provisioned. No public endpoint enabled.');
}else if(command==='stage'||command==='activate'){
  const m=await read('manifest.json');if(!m.publicationApproved||m.public_origin!==config.vars.ALLOWED_ORIGIN)throw Error('Publication approval mismatch');
  const sources=JSON.parse(await readFile('workers/corpus-rag/public-sources.json','utf8'));if(sources.release!==m.release)throw Error('Release mismatch');
  const remote=await api(`ai-search/instances/${instance}`);if(remote.public_endpoint_params?.enabled!==false)throw Error('Raw search endpoint must remain private');
  if(command==='activate'){
    const receipts=await read('uploads.json');if(!m.documents.every(d=>receipts[d.key]?.accepted))throw Error('Uploads incomplete');
    const s=await api(`ai-search/instances/${instance}/stats`);
    if(s.completed!==m.documents.length||s.error||s.queued||s.running||s.skipped)throw Error('Indexing not complete/clean');
    if(!process.argv.includes('--publish'))throw Error('Explicit --publish required');
  }
  const widget=await read('turnstile.json'),e=await credentials();
  let secrets;try{secrets=await read('production-secrets.json');}catch(err){if(err.code!=='ENOENT')throw err;secrets={CLIENT_HASH_SECRET:randomBytes(32).toString('hex')};await save('production-secrets.json',secrets);}
  const env={...process.env,CLOUDFLARE_API_TOKEN:e.CLOUDFLARE_API_TOKEN,CLOUDFLARE_ACCOUNT_ID:e.CLOUDFLARE_ACCOUNT_ID,WRANGLER_SEND_METRICS:'false'};
  const run=(args,input)=>{const r=spawnSync('node_modules/.bin/wrangler',args,{env,input,encoding:'utf8'});if(r.status!==0)throw Error('Wrangler command failed; inspect deployment configuration (secrets withheld).');return r;};
  // Stage is disabled even with all bindings/secrets present. No site/DNS changes.
  const args=['deploy','--config',configPath,'--var',`INSTANCE:${instance}`,'--var',`CORPUS_VERSION:${m.release}`,'--var','PUBLIC_RELEASE_APPROVED:true','--var',`ENABLED:${command==='activate'}`];
  run(args);
  for(const [name,value] of Object.entries({...secrets,TURNSTILE_SECRET:widget.secret}))run(['secret','put',name,'--config',configPath],value);
  const domain=await api('workers/subdomain');
  await save('public-client.json',{PUBLIC_RAG_ENABLED:command==='activate',PUBLIC_RAG_ENDPOINT:`https://${config.name}.${domain.subdomain}.workers.dev/query`,PUBLIC_TURNSTILE_SITE_KEY:widget.sitekey});
  console.log(JSON.stringify({state:command==='activate'?'enabled':'staged_disabled',origin:config.vars.ALLOWED_ORIGIN}));
}else throw Error('Use prepare --publication-approved, provision, stage, or activate --publish');
