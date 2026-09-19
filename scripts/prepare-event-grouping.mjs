import {readFile,writeFile,rename,lstat} from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {createDerivedGraph} from '../src/lib/corpus/derived-graph.mjs';
import {groupingInput,groupingDigest} from '../src/lib/corpus/event-groups.mjs';

const base=path.resolve('research/corpus/site-previews');
const {directory}=JSON.parse(await readFile(path.join(base,'current.json'),'utf8'));
if(!/^release-[a-z0-9-]+$/.test(directory))throw new Error('Invalid preview');
const root=path.join(base,directory),stat=await lstat(root);
if(!stat.isDirectory()||stat.isSymbolicLink()||(stat.mode&0o077))throw new Error('Private preview required');
const corpus=path.join(root,'corpus'),manifest=JSON.parse(await readFile(path.join(corpus,'manifest.json'),'utf8'));
const recordings=[];
for(const ref of manifest.catalog_shards){
  if(!/^catalog\/catalog-\d+-[a-f0-9]+\.json$/.test(ref.path)||!/^release_[a-f0-9]+$/.test(manifest.release_id))throw new Error('Unsafe catalog path');
  const bytes=await readFile(path.join(corpus,'releases',manifest.release_id,ref.path));
  if(createHash('sha256').update(bytes).digest('hex')!==ref.sha256)throw new Error('Catalog changed');
  recordings.push(...JSON.parse(bytes).recordings);
}
let file=path.join(root,'summaries/refreshed.json');try{await lstat(file);}catch(e){if(e.code!=='ENOENT')throw e;file=path.join(root,'summaries/release.json');}
const summaries=JSON.parse(await readFile(file,'utf8'));
const graph=createDerivedGraph({releaseId:manifest.release_id,generatedAt:manifest.generated_at,recordings,facets:{speakers:manifest.facets.speakers}},summaries.summaries).finish();
const input=groupingInput(graph);input.input_sha256=groupingDigest(input);
const target=path.join(root,'event-grouping-input.json'),temp=target+'.tmp';
await writeFile(temp,JSON.stringify(input)+'\n',{mode:0o600});await rename(temp,target);
console.log(JSON.stringify({input:target,output:path.join(root,'event-groups.json'),events:input.events.length,input_sha256:input.input_sha256}));
