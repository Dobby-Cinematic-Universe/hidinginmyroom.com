import {readFile,mkdir,rmdir} from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {api,instance,root,read,save} from './cloudflare.mjs';
const lock=path.join(root,'upload.lock');await mkdir(lock);
try{
  const m=await read('manifest.json');if(!m.freeOnlyConfirmed||!m.uploadAuthorized)throw Error('Missing pilot authorization');
  const remote=await api(`ai-search/instances/${instance}`);if(remote.public_endpoint_params?.enabled!==false||remote.embedding_model!=='@cf/qwen/qwen3-embedding-0.6b')throw Error('Expected private low-cost model configuration');
  let receipts;try{receipts=await read('uploads.json');}catch(e){if(e.code!=='ENOENT')throw e;receipts={};}
  let pendingSave=Promise.resolve();
  const saveReceipts=()=>{const snapshot=structuredClone(receipts);pendingSave=pendingSave.then(()=>save('uploads.json',snapshot));return pendingSave;};
  let finished=m.documents.filter(d=>receipts[d.key]?.accepted).length;
  let cursor=0,failure=null;
  const concurrency=process.env.RAG_PROFILE==='public'?4:1;
  // Bounded upload lanes; receipt writes serialize and uncertain POSTs never auto-repeat.
  async function upload(d){
    if(receipts[d.key]?.accepted)return;
    if(receipts[d.key]){
      const listed=await api(`ai-search/instances/${instance}/items?key=${encodeURIComponent(d.key)}&source=builtin`);
      const items=Array.isArray(listed)?listed:listed.items||listed.data||[];const item=items.find(i=>i.key===d.key);
      if(!item)throw Error(`Uncertain upload needs reconciliation: ${d.key}`);
      receipts[d.key]={accepted:true,id:item.id,status:item.status};await saveReceipts();finished++;return;
    }
    const bytes=await readFile(path.join(root,'documents',d.key));if(createHash('sha256').update(bytes).digest('hex')!==d.sha256||bytes.length>3900000)throw Error('Input integrity/size failure');
    receipts[d.key]={intent_at:new Date().toISOString(),sha256:d.sha256};await saveReceipts();
    const form=new FormData();form.set('file',new Blob([bytes],{type:'text/plain'}),d.key);
    const item=await api(`ai-search/instances/${instance}/items`,{method:'POST',body:form});
    receipts[d.key]={accepted:true,id:item.id,status:item.status,sha256:d.sha256};await saveReceipts();finished++;
    if(finished%50===0)console.log(JSON.stringify({uploaded:finished,total:m.documents.length}));
  }
  await Promise.all(Array.from({length:concurrency},async()=>{while(!failure&&cursor<m.documents.length){const d=m.documents[cursor++];try{await upload(d);}catch(e){failure=e;}}}));
  if(failure)throw failure;
  await save('upload-status.json',{state:'uploads_accepted',uploaded:finished,total:m.documents.length,updated_at:new Date().toISOString()});console.log(JSON.stringify({uploaded:finished,total:m.documents.length}));
}catch(e){await save('upload-status.json',{state:'paused',error:e.message,updated_at:new Date().toISOString()});throw e;}finally{await rmdir(lock);}
