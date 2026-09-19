// Finite dependent preparation job; never activates AI or publishes a website.
import {readFile} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {save,read,api,instance} from './cloudflare.mjs';
if(process.env.RAG_PROFILE!=='public')throw Error('Public profile required');
const [preview,candidate]=process.argv.slice(2);
if(!/^release-[a-z0-9-]+$/.test(preview||'')||!/^candidate-[a-z0-9-]+$/.test(candidate||''))throw Error('Expected fresh preview and candidate names');
const run=args=>new Promise((resolve,reject)=>{const p=spawn(process.execPath,args,{stdio:'inherit',env:process.env});p.on('error',reject);p.on('exit',code=>code===0?resolve():reject(Error('Preparation command failed: '+args[0])));});
const deadline=Date.now()+24*3600000;
await save('finalization-status.json',{state:'waiting_for_summary_and_bulk_delta',preview,candidate,deadline:new Date(deadline).toISOString()});
try{
  while(true){
    const summary=JSON.parse(await readFile('research/private-summaries/sonnet-broader-evidence-recovery-20260917/status.json','utf8'));
    const upload=await read('followup-status.json');
    if(upload.state==='paused')throw Error('Initial upload follow-up is paused; inspect before continuing');
    if(summary.held_jobs&&!summary.active_batches)throw Error('Archive synthesis needs review');
    if(summary.complete&&upload.state==='uploaded_staged_disabled')break;
    if(Date.now()>deadline)throw Error('Dependent work did not complete within 24 hours');
    await new Promise(resolve=>setTimeout(resolve,30000));
  }
  await save('finalization-status.json',{state:'preparing_final_delta',preview,candidate});
  await run(['scripts/rag/refresh-after-upload.mjs',preview]);
  await run(['scripts/rag/reconcile-stale.mjs','--apply']);
  await run(['scripts/prepare-release-candidate.mjs',candidate,preview]);
  await run(['scripts/audit-release-candidate.mjs',candidate]);
  while(true){
    const stats=await api(`ai-search/instances/${instance}/stats`),manifest=await read('manifest.json');
    if(stats.error||stats.skipped)throw Error('Remote index contains failed or skipped items; inspect before activation');
    if(stats.completed===manifest.documents.length&&!stats.queued&&!stats.running)break;
    await save('finalization-status.json',{state:'candidate_ready_waiting_for_index',preview,candidate,stats,expected:manifest.documents.length});
    if(Date.now()>deadline)throw Error('Remote indexing did not complete within 24 hours');
    await new Promise(resolve=>setTimeout(resolve,60000));
  }
  await save('finalization-status.json',{state:'candidate_and_index_ready_public_disabled',preview,candidate,remaining:['Pages access and domain launch checks','explicit website publication approval'],completed_at:new Date().toISOString()});
}catch(error){await save('finalization-status.json',{state:'paused',preview,candidate,error:error.message,updated_at:new Date().toISOString()});throw error;}
