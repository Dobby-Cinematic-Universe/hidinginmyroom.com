import {randomBytes} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {api,instance,save,read,credentials} from './cloudflare.mjs';
const command=process.argv[2];
if(command==='configure'){
  const config={embedding_model:'@cf/qwen/qwen3-embedding-0.6b',index_method:{keyword:true,vector:true},fusion_method:'rrf',chunk:true,chunk_size:400,chunk_overlap:10,rewrite_query:false,reranking:false,max_num_results:6,public_endpoint_params:{enabled:false,mcp:{disabled:true},search_endpoint:{disabled:true},chat_completions_endpoint:{disabled:true}}};
  const r=await api(`ai-search/instances/${instance}`,{method:'PUT',body:config});
  if(r.public_endpoint_params?.enabled!==false)throw Error('Instance privacy not confirmed');
  await save('instance.json',{id:r.id,embedding_model:r.embedding_model,index_method:r.index_method,private:true});console.log('Private hybrid index configured.');
}else if(command==='deploy'){
  if(process.env.RAG_PROFILE==='public')throw Error('Use production.mjs for guarded public deployment');
  const manifest=await read('manifest.json');if(!manifest.freeOnlyConfirmed||!manifest.uploadAuthorized)throw Error('Missing authorization');
  let token;try{token=(await read('connection.json')).token;}catch{token=randomBytes(32).toString('hex');}
  const e=await credentials();
  // The management API token is used only by the deploy CLI, never uploaded as a Worker secret.
  const env={...process.env,CLOUDFLARE_API_TOKEN:e.CLOUDFLARE_API_TOKEN,CLOUDFLARE_ACCOUNT_ID:e.CLOUDFLARE_ACCOUNT_ID,WRANGLER_SEND_METRICS:'false'};
  const args=['--no','--','wrangler'];
  const deployed=spawnSync('npm',['exec',...args,'deploy','--config','workers/corpus-rag/wrangler.jsonc'],{env,stdio:'inherit'});if(deployed.status!==0)throw Error('Worker deployment failed');
  const secret=spawnSync('npm',['exec',...args,'secret','put','PILOT_TOKEN','--config','workers/corpus-rag/wrangler.jsonc'],{env,input:token,encoding:'utf8'});if(secret.status!==0)throw Error('Worker secret configuration failed');
  const domain=await api('workers/subdomain');const url=`https://himr-private-rag-pilot.${domain.subdomain}.workers.dev`;
  await save('connection.json',{url,token,instance});console.log(JSON.stringify({url,private:true}));
}else if(command==='status'){
  const r=await api(`ai-search/instances/${instance}`);const stats=await api(`ai-search/instances/${instance}/stats`);
  console.log(JSON.stringify({id:r.id,private:r.public_endpoint_params?.enabled===false,embedding_model:r.embedding_model,stats}));
}else throw Error('Use configure, deploy or status');
