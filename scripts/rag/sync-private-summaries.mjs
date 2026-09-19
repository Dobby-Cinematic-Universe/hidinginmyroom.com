// Refresh summary documents only; retain the pilot's exact original transcript set.
import {mkdir,rmdir,readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {root,read,save} from './cloudflare.mjs';
if(process.env.RAG_PROFILE!=='private')throw Error('Explicit private profile required');
await mkdir(root+'/upload.lock');
try {
  const before=await read('manifest.json');
  const publicRoot='research/cloudflare-rag/public-v1';
  const source=JSON.parse(await readFile(publicRoot+'/manifest.json','utf8'));
  if(!source.publicationApproved||!before.uploadAuthorized||!before.freeOnlyConfirmed)throw Error('Missing text approval');
  const summaries=source.documents.filter(d=>['summary','synthesis'].includes(d.kind));
  for(const d of summaries){
    if(!/^(summary|synthesis)-[a-f0-9]{32}\.txt$/.test(d.key))throw Error('Unsafe document key');
    const bytes=await readFile(publicRoot+'/documents/'+d.key);
    if(createHash('sha256').update(bytes).digest('hex')!==d.sha256)throw Error('Changed summary document');
    await writeFile(root+'/documents/'+d.key,bytes,{mode:0o600});
  }
  const documents=[...before.documents.filter(d=>!['summary','synthesis'].includes(d.kind)),...summaries];
  const current=new Set(documents.map(d=>d.key));
  let previous=[];try{previous=(await read('stale-index-items.json')).keys;}catch(e){if(e.code!=='ENOENT')throw e;}
  const stale=[...new Set([...previous,...before.documents.map(d=>d.key)])].filter(key=>!current.has(key));
  await save('manifest.previous.json',before);
  await save('manifest.json',{...before,summary_release:source.summary_release,documents,updated_at:new Date().toISOString()});
  await save('stale-index-items.json',{keys:stale,deleted:false,activation_blocked:stale.length>0});
  console.log(JSON.stringify({summary_documents:summaries.length,retained_other_documents:documents.length-summaries.length,stale:stale.length}));
} finally { await rmdir(root+'/upload.lock'); }
