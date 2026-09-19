// Remove only superseded, locally recoverable objects from the private raw index.
// API contract: https://developers.cloudflare.com/ai-search/api/items/rest-api/
import {mkdir,rmdir,readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {api,instance,root,read,save} from './cloudflare.mjs';
if(!['public','private'].includes(process.env.RAG_PROFILE)||!process.argv.includes('--apply'))throw Error('Requires explicit profile and --apply');
await mkdir(root+'/upload.lock');
try{
  const m=await read('manifest.json'), receipts=await read('uploads.json'), stale=await read('stale-index-items.json');
  const remote=await api(`ai-search/instances/${instance}`);
  const client=process.env.RAG_PROFILE==='public'?await read('public-client.json'):null;
  if(remote.public_endpoint_params?.enabled!==false||client?.PUBLIC_RAG_ENABLED)throw Error('Index must remain private and public Worker disabled');
  if(!m.documents.every(d=>receipts[d.key]?.accepted))throw Error('Current uploads incomplete');
  const current=new Set(m.documents.map(d=>d.key));
  let audit;try{audit=await read('stale-removal-audit.json');}catch(e){if(e.code!=='ENOENT')throw e;audit={items:{}};}
  for(const key of stale.keys){
    if(!/^(summary|transcript|metadata|synthesis)-[a-f0-9]{32}\.txt$/.test(key)||current.has(key))throw Error('Unsafe or current stale key');
    const bytes=await readFile(root+'/documents/'+key),sha256=createHash('sha256').update(bytes).digest('hex');
    const receipt=receipts[key]||audit.items[key]?.receipt;
    if(!receipt?.accepted||(receipt.sha256&&receipt.sha256!==sha256))throw Error('Missing receipt or changed recovery copy');
    const listing=await api(`ai-search/instances/${instance}/items?key=${encodeURIComponent(key)}&source=builtin`);
    const matches=(Array.isArray(listing)?listing:listing.items||listing.data||[]).filter(i=>i.key===key);
    if(matches.length>1||(matches.length&&matches[0].id!==receipt.id))throw Error('Stale object identity differs');
    audit.items[key]={...audit.items[key],receipt,sha256,local_backup:'documents/'+key,state:'removal_intent'};
    await save('stale-removal-audit.json',audit);
    if(matches.length){const result=await api(`ai-search/instances/${instance}/items/${encodeURIComponent(receipt.id)}`,{method:'DELETE'});if(result.key!==key)throw Error('Deletion response key differs');}
    audit.items[key].state='removed';audit.items[key].removed_at=new Date().toISOString();
    await save('stale-removal-audit.json',audit);
    delete receipts[key];await save('uploads.json',receipts);
  }
  await save('stale-index-items.json',{...stale,deleted:true,activation_blocked:false});
  console.log(JSON.stringify({superseded_index_objects_removed:stale.keys.length,local_backups_retained:true}));
}finally{await rmdir(root+'/upload.lock');}
