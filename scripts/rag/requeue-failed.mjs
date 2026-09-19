// Built-in items: key-based PUT actually starts indexing; PATCH may acknowledge
// without changing state. Preserve content, IDs, and an audit of each attempt.
import {mkdir,rmdir} from 'node:fs/promises';
import {api,instance,root,read,save} from './cloudflare.mjs';
if(process.env.RAG_PROFILE!=='public')throw Error('Public corpus profile required');
const run=process.argv[2]||'20260917';
if(!/^[a-z0-9-]+$/.test(run))throw Error('Invalid retry run name');
const name=`failed-key-requeue-${run}.json`;
const route=`ai-search/namespaces/default/instances/${instance}/items`;
await mkdir(root+'/upload.lock');
try{
 const manifest=await read('manifest.json'),receipts=await read('uploads.json');
 const current=new Set(manifest.documents.map(d=>d.key));
 let audit;try{audit=await read(name);}catch(e){if(e.code!=='ENOENT')throw e;}
 if(!audit){
  const items=new Map();
  for(let page=1;page<=500;page++){
   const rows=await api(`${route}?status=error&per_page=50&page=${page}`);
   if(!Array.isArray(rows))throw Error('Invalid listing');
   const previous=items.size;
   for(const i of rows){
    if(i.status!=='error'||i.source_id!=='builtin'||!current.has(i.key)||!receipts[i.key]?.accepted||receipts[i.key].id!==i.id)throw Error('Unbound failed item');
    items.set(i.id,i);
   }
   if(rows.length<50)break;
   if(items.size===previous||page===500)throw Error('Pagination failed');
  }
  audit={instance,started_at:new Date().toISOString(),items:[...items.values()],attempts:{}};
  await save(name,audit);
 }
 let cursor=0,writes=Promise.resolve(),count=0;
 const persist=()=>{const snapshot=structuredClone(audit);writes=writes.then(()=>save(name,snapshot));return writes;};
 await Promise.all(Array.from({length:4},async()=>{
  while(cursor<audit.items.length){
   const i=audit.items[cursor++];if(audit.attempts[i.id])continue;
   try{
    const before=await api(`${route}/${i.id}`);
    if(before.id!==i.id||before.key!==i.key)throw Error('Identity changed');
    if(before.status!=='error'){
     audit.attempts[i.id]={state:'already_progressed',status:before.status};await persist();continue;
    }
    audit.attempts[i.id]={state:'intent',at:new Date().toISOString()};await persist();
    const response=await api(route,{method:'PUT',body:{key:i.key,next_action:'INDEX'}});
    if(!response||response.id!==i.id||response.key!==i.key)throw Error('Requeue response identity missing or changed');
    audit.attempts[i.id]={state:['queued','running','completed'].includes(response.status)?'requeued':'needs_inspection',response,at:new Date().toISOString()};
   }catch(error){audit.attempts[i.id]={...audit.attempts[i.id],state:'needs_inspection',error:error.message};}
   await persist();count++;if(count%100===0)console.log(JSON.stringify({processed:count,total:audit.items.length}));
  }
 }));
 audit.finished_at=new Date().toISOString();audit.stats=await api(`ai-search/instances/${instance}/stats`);await persist();
 const outcomes={};for(const x of Object.values(audit.attempts))outcomes[x.state]=(outcomes[x.state]||0)+1;
 console.log(JSON.stringify({total:audit.items.length,outcomes,stats:audit.stats}));
}finally{await rmdir(root+'/upload.lock');}
