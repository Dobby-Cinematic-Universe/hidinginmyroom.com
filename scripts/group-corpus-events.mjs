import {spawnSync} from 'node:child_process';
import {existsSync} from 'node:fs';
import path from 'node:path';
const runtime=path.resolve('research/corpus/event-grouping-runtime-20260917');
const python=path.join(runtime,'venv/bin/python');
if(!existsSync(python)||!existsSync(path.join(runtime,'model.onnx')))throw new Error('Local embedding runtime missing. See docs/LOCAL_EVENT_GROUPING.md.');
const prepared=spawnSync(process.execPath,['scripts/prepare-event-grouping.mjs'],{encoding:'utf8'});
if(prepared.status!==0)throw new Error(prepared.stderr||'Cannot prepare event descriptions');
const {input,output,events}=JSON.parse(prepared.stdout);
console.log(`Grouping ${events.toLocaleString()} descriptions locally; unchanged embeddings are reused.`);
const result=spawnSync(python,['-B','-m','pipeline.event_embedding_groups','--input',input,'--output',output,'--runtime',runtime],
  {stdio:'inherit',env:{...process.env,OPENBLAS_NUM_THREADS:'4',OMP_NUM_THREADS:'4'}});
if(result.status!==0)process.exit(result.status||1);
console.log('Grouping saved. Restart the local Astro site to load the new grouping artifact.');
