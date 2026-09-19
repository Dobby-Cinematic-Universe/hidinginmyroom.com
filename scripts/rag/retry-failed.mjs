// Historical PATCH runner retained for interpreting its audit receipts.
// Use requeue-failed.mjs: PATCH acknowledged requests without requeueing items.
import {mkdir,rmdir} from 'node:fs/promises';
import {api,instance,root,read,save} from './cloudflare.mjs';
throw Error('Use scripts/rag/requeue-failed.mjs; the item-ID PATCH endpoint did not requeue built-in items.');
if(!['public','private'].includes(process.env.RAG_PROFILE))throw Error('Explicit profile required');
const auditName='failed-retry-paid-20260917.json';
const inspect=async(route)=>{
 for(let attempt=0;attempt<3;attempt++){
  try{return await api(route);}catch(error){
   if(attempt===2||![401,429,500,502,503,504].includes(error.status))throw error;
   await new Promise(resolve=>setTimeout(resolve,1000*(attempt+1)));
  }
 }
};
await mkdir(root+'/upload.lock');
try{
 const manifest=await read('manifest.json'),receipts=await read('uploads.json');
 const keys=new Set(manifest.documents.map(d=>d.key));
 let audit;try{audit=await read(auditName);}catch(e){if(e.code!=='ENOENT')throw e;}
 if(!audit){
  const found=new Map();
  for(let page=1;page<=500;page++){
   const rows=await api(`ai-search/instances/${instance}/items?status=error&per_page=50&page=${page}`);
   if(!Array.isArray(rows)||rows.some(x=>x.status!=='error'))throw Error('Invalid failed-item listing');
   const prior=found.size;
   for(const x of rows){if(!keys.has(x.key)||!receipts[x.key]?.accepted||receipts[x.key].id!==x.id||x.source_id!=='builtin')throw Error('Failed item not bound to current upload');found.set(x.id,x);}
   if(rows.length<50)break;
   if(found.size===prior||page===500)throw Error('Pagination failed');
  }
  audit={instance,started_at:new Date().toISOString(),approval:'User authorized retry of failed indexing after temporary Workers Paid upgrade.',items:[...found.values()],attempts:{}};
  await save(auditName,audit);
 }
 let cursor=0,done=0,writing=Promise.resolve();
 const persist=()=>{const snapshot=structuredClone(audit);writing=writing.then(()=>save(auditName,snapshot));return writing;};
 await Promise.all(Array.from({length:4},async()=>{
  while(cursor<audit.items.length){
   const item=audit.items[cursor++];if(audit.attempts[item.id])continue;
   const current=await inspect(`ai-search/instances/${instance}/items/${item.id}`);
   if(current.id!==item.id||current.key!==item.key)throw Error('Item identity changed');
   if(current.status!=='error'){audit.attempts[item.id]={state:'already_progressed',status:current.status};await persist();continue;}
   audit.attempts[item.id]={state:'intent',at:new Date().toISOString()};await persist();
   try{
    const response=await api(`ai-search/instances/${instance}/items/${item.id}`,{method:'PATCH',body:{next_action:'INDEX'}});
    const after=await api(`ai-search/instances/${instance}/items/${item.id}`);
    audit.attempts[item.id]={state:'request_accepted',response,status:after.status,next_action:after.next_action,at:new Date().toISOString()};
   }catch(error){audit.attempts[item.id]={state:'needs_inspection',error:error.message};}
   await persist();done++;if(done%100===0)console.log(JSON.stringify({attempted:done,total:audit.items.length}));
  }
 }));
 audit.finished_at=new Date().toISOString();audit.stats=await api(`ai-search/instances/${instance}/stats`);await persist();
 const counts={};for(const x of Object.values(audit.attempts))counts[x.state]=(counts[x.state]||0)+1;
 console.log(JSON.stringify({instance,failed_snapshot:audit.items.length,outcomes:counts,stats:audit.stats}));
}finally{await rmdir(root+'/upload.lock');}
