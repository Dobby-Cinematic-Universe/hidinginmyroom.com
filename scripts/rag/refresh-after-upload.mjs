// One finite, explicitly authorized dependent job. No deployment activation.
import {access} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {root,read,save} from './cloudflare.mjs';
if(process.env.RAG_PROFILE!=='public')throw Error('Public profile required');
const preview=process.argv[2];if(!/^release-[a-z0-9-]+$/.test(preview||''))throw Error('Expected preview snapshot');
const summaryReader=process.argv[3]||'research/private-summaries/sonnet-broader-20260916/reader/index.json';
const run=(args)=>new Promise((resolve,reject)=>{const child=spawn(process.execPath,args,{stdio:'inherit',env:process.env});child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(Error('Refresh command exited '+code)));});
const deadline=Date.now()+6*3600000;
await save('followup-status.json',{state:'waiting_for_bulk_upload',preview,deadline:new Date(deadline).toISOString()});
try{
  while(true){
    let busy=false;try{await access(root+'/upload.lock');busy=true;}catch(error){if(error.code!=='ENOENT')throw error;}
    if(!busy)break;
    if(Date.now()>deadline)throw Error('Bulk upload did not drain within six hours; resume after inspection');
    await new Promise(resolve=>setTimeout(resolve,30000));
  }
  const before=await read('manifest.json'),receipts=await read('uploads.json');
  if(!before.documents.every(d=>receipts[d.key]?.accepted))throw Error('Bulk upload stopped incomplete; no manifest changed');
  await run(['scripts/refresh-preview-summaries.mjs',preview,summaryReader,'research/private-summaries/targeted-followup-20260917/run']);
  await run(['scripts/rag/prepare.mjs','--full','--publication-approved',`--preview=${preview}`]);
  await run(['scripts/rag/production.mjs','prepare','--full','--publication-approved']);
  const after=await read('manifest.json');const keys=new Set(after.documents.map(d=>d.key));
  let previousStale=[];try{previousStale=(await read('stale-index-items.json')).keys||[];}catch(error){if(error.code!=='ENOENT')throw error;}
  const stale=[...new Set([...previousStale,...before.documents.filter(d=>!keys.has(d.key)).map(d=>d.key)])].filter(key=>!keys.has(key));
  await save('stale-index-items.json',{keys:stale,deleted:false,activation_blocked:stale.length>0});
  await run(['scripts/rag/upload.mjs']);
  await run(['scripts/rag/production.mjs','stage']);
  await save('followup-status.json',{state:'uploaded_staged_disabled',preview,documents:after.documents.length,stale_items:stale.length,completed_at:new Date().toISOString()});
}catch(error){await save('followup-status.json',{state:'paused',preview,error:error.message,updated_at:new Date().toISOString()});throw error;}
