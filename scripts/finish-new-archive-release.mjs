// One bounded continuation: integrate completed synthesis and build, never deploy.
import {readFile,writeFile,rename} from 'node:fs/promises';
import {spawn,execFileSync} from 'node:child_process';
const root='research/private-summaries/new-archive-synthesis-20260918';
const reader=root+'/reader/index.json';
const preview='release-20260918-v10';
const candidate='candidate-20260918-v9';
const run=(args,env={})=>new Promise((resolve,reject)=>{
  const p=spawn(process.execPath,args,{stdio:'inherit',env:{...process.env,...env}});
  p.on('error',reject);p.on('exit',code=>code===0?resolve():reject(Error(args[0]+' failed: '+code)));
});
const status=async(value)=>{const file=root+'/integration-status.json';await writeFile(file+'.tmp',JSON.stringify(value)+'\n',{mode:0o600});await rename(file+'.tmp',file);};
try{
  const deadline=Date.now()+2*3600000;
  await status({state:'waiting_for_synthesis'});
  while(true){
    const s=JSON.parse(await readFile(root+'/status.json','utf8'));
    if(s.complete&&s.completed_scopes===s.scopes)break;
    const active=execFileSync('systemctl',['--user','show','himr-new-archive-synthesis-20260918.service','--property=ActiveState','--value'],{encoding:'utf8'}).trim();
    if(active!=='active'||Date.now()>deadline)throw Error('Synthesis incomplete or stopped; no partial release');
    await new Promise(resolve=>setTimeout(resolve,30000));
  }
  await status({state:'refreshing_dependents'});
  await run(['scripts/rag/refresh-after-upload.mjs',preview,reader],{RAG_PROFILE:'public'});
  await run(['scripts/rag/reconcile-stale.mjs','--apply'],{RAG_PROFILE:'public'});
  await run(['scripts/group-corpus-events.mjs']);
  execFileSync('systemctl',['--user','restart','himr-local-site-20260917.service']);
  await status({state:'building_candidate',candidate});
  await run(['scripts/prepare-release-candidate.mjs',candidate,preview,'--production-chat']);
  await run(['scripts/audit-release-candidate.mjs',candidate]);
  await status({state:'completed',candidate,preview,cloud_uploads_accepted:true,cloud_indexing_complete:false,deployment_performed:false,production_chat_enabled:false});
}catch(error){await status({state:'needs_attention',error:error.message});throw error;}
