// Finite dependency runner: refresh prepared outputs, never publish or activate.
import {readFile,writeFile,rename} from 'node:fs/promises';
import {spawn} from 'node:child_process';
const delta='research/private-summaries/sonnet-summary-delta-20260917-v3';
const receipt=delta+'/dependents-status.json';
const skipCloud=process.argv.includes('--skip-cloud');
const status=async value=>{await writeFile(receipt+'.tmp',JSON.stringify({...value,updated_at:new Date().toISOString()},null,2)+'\n',{mode:0o600});await rename(receipt+'.tmp',receipt);};
const run=(args,profile)=>new Promise((resolve,reject)=>{
  const child=spawn(process.execPath,args,{stdio:'inherit',env:{...process.env,...(profile?{RAG_PROFILE:profile}:{})}});
  child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(Error(args[0]+' exited '+code)));
});
try{
  await status({state:'waiting_for_incremental_synthesis'});
  const deadline=Date.now()+25*3600000;
  while(true){
    const s=JSON.parse(await readFile(delta+'/status.json','utf8'));
    if(s.complete===true && s.completed_scopes===s.scopes)break;
    // A separate bounded recovery owner may be processing held jobs.
    if(Date.now()>deadline)throw Error('Incremental synthesis wait expired');
    await new Promise(resolve=>setTimeout(resolve,30000));
  }
  await status({state:'refreshing_dependents'});
  for(const preview of ['release-20260917-v8','release-20260917-v9'])await run(['scripts/refresh-preview-summaries.mjs',preview,delta+'/reader/index.json','research/private-summaries/targeted-followup-20260917/run']);
  await run(['scripts/group-corpus-events.mjs']);
  if(!skipCloud){
  await run(['scripts/rag/refresh-after-upload.mjs','release-20260917-v9'],'public');
  await run(['scripts/rag/reconcile-stale.mjs','--apply'],'public');
  await run(['scripts/rag/sync-private-summaries.mjs'],'private');
  await run(['scripts/rag/upload.mjs'],'private');
  await run(['scripts/rag/reconcile-stale.mjs','--apply'],'private');
  }
  const candidate='candidate-20260917-v7';
  await run(['scripts/prepare-release-candidate.mjs',candidate,'release-20260917-v9']);
  await run(['scripts/audit-release-candidate.mjs',candidate]);
  await status({state:'prepared',candidate,indexing:skipCloud?'Cloud refresh deferred: free daily Neuron limit reached':'uploads accepted; remote indexing may still be pending',published:false,public_ai_enabled:false});
}catch(error){await status({state:'needs_attention',error:error.message});throw error;}
