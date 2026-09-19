// Refresh only the local broader-summary overlay; never regenerate transcripts.
import { createHash } from 'node:crypto';
import { readFile, writeFile, rename, readdir } from 'node:fs/promises';
import path from 'node:path';
import { validateSummaryRelease } from '../src/lib/summaries/schema.mjs';
import { normalizeSummaryUnicode } from '../src/lib/summaries/unicode.mjs';
import { resolveCatalogSource } from './summary-source-mapping.mjs';

const [directory,readerPath,targetedRun] = process.argv.slice(2);
if (!directory || !/^release-[a-z0-9-]+$/.test(directory) || !readerPath) throw new Error('Usage: refresh-preview-summaries.mjs release-NAME PRIVATE_READER_INDEX');
const root=path.resolve('research/corpus/site-previews',directory);
const mapping=JSON.parse(await readFile(path.join(root,'identity-map.json'),'utf8'));
const original=JSON.parse(await readFile(path.join(root,'summaries/release.json'),'utf8'));
let effectiveReader=readerPath;
const deltaRoot='research/private-summaries/sonnet-summary-delta-20260917-v3';
try {
  const status=JSON.parse(await readFile(deltaRoot+'/status.json','utf8'));
  // Never replace a complete reader with a partially refreshed hierarchy.
  if(readerPath==='research/private-summaries/sonnet-broader-20260916/reader/index.json' && status.complete===true && status.completed_scopes===status.scopes)effectiveReader=deltaRoot+'/reader/index.json';
} catch(error) { if(error.code!=='ENOENT')throw error; }
// A completed newer continuation supersedes the historical default only.
// Explicit reader arguments always remain authoritative.
if(readerPath==='research/private-summaries/sonnet-broader-20260916/reader/index.json'){
  const latestRoot='research/private-summaries/new-archive-synthesis-20260918';
  try{
    const status=JSON.parse(await readFile(latestRoot+'/status.json','utf8'));
    if(status.complete===true&&status.completed_scopes===status.scopes)effectiveReader=latestRoot+'/reader/index.json';
  }catch(error){if(error.code!=='ENOENT')throw error;}
}
const reader=JSON.parse(await readFile(effectiveReader,'utf8'));
if(reader.kind !== 'himr_sonnet_broader_campaign_reader') throw new Error('Expected completed Claude reader exports.');
// Pipeline canonical JSON includes a trailing newline, including scalar IDs.
const ids=new Map(mapping.records.map((r)=>['transcript_'+createHash('sha256').update(JSON.stringify(r.recording_id)+'\n').digest('hex').slice(0,24),r.public_recording_id]));
const catalogManifest=JSON.parse(await readFile(path.join(root,'corpus/manifest.json'),'utf8'));
if(!/^release_[a-f0-9]+$/.test(catalogManifest.release_id))throw new Error('Invalid corpus release');
const catalogRoot=path.join(root,'corpus/releases',catalogManifest.release_id);
const catalog=[];
for(const ref of catalogManifest.catalog_shards){
  const file=path.resolve(catalogRoot,ref.path);
  if(!file.startsWith(catalogRoot+'/'))throw new Error('Unsafe catalog shard');
  const bytes=await readFile(file);
  if(createHash('sha256').update(bytes).digest('hex')!==ref.sha256)throw new Error('Changed catalog shard');
  catalog.push(...JSON.parse(bytes).recordings);
}
const catalogIds=new Set(catalog.map(r=>r.recording_id));
let resolvedCatalogSources=0;
for(const row of reader.summaries)for(const items of Object.values(row.sections))for(const item of items)for(const source of item.sources){
  if(ids.has(source.transcript_id))continue;
  const id=resolveCatalogSource(source,catalog);
  if(id){ids.set(source.transcript_id,id);resolvedCatalogSources++;}
}
const broader=[];const held=[];
for(const row of reader.summaries) {
  if(!['monthly','yearly','archive'].includes(row.stage)) throw new Error('Invalid broader stage.');
  const missing=[...new Set(Object.values(row.sections).flatMap((items)=>items.flatMap((item)=>item.sources.map((s)=>s.transcript_id))).filter((id)=>!ids.has(id)))];
  if(missing.length){held.push({stage:row.stage,period:row.period,missing_sources:missing.length});continue;}
  const period=['unknown','selected-archive'].includes(row.period)?null:row.period;
  broader.push({id:row.stage+'-'+(row.stage==='archive'?'overview':period||'undated'),kind:row.stage,period,
    title:row.stage==='archive'?'Archive overview':(period||'Undated recordings')+' summary',recording_id:null,publication:'prepared',
    sections:Object.fromEntries(Object.entries(row.sections).map(([section,items])=>[section,items.map((item)=>({text:item.text,classification:item.classification,
      source_recording_ids:[...new Set(item.sources.map((s)=>ids.get(s.transcript_id)))].sort()}))]))});
}
const transcripts=new Map(original.summaries.filter((r)=>r.kind==='transcript').map((r)=>[r.recording_id,r]));
let addedTranscripts=0;
if(targetedRun){
  const physicalIds=new Map(mapping.records.map((r)=>[r.recording_id,r.public_recording_id]));
  const manifest=JSON.parse(await readFile(path.join(targetedRun,'../manifest.json'),'utf8'));
  if(path.resolve(manifest.state_root)!==path.resolve(targetedRun))throw new Error('Targeted summary root differs');
  for(let n=0;n<manifest.shards.length;n++){
    const folder=path.join(targetedRun,`shard-${String(n).padStart(4,'0')}`,'reader-exports');
    let exports;try{exports=await readdir(folder);}catch(e){if(e.code==='ENOENT')continue;throw e;}
    for(const name of exports){
      if(!/^reader-[a-f0-9]{32}$/.test(name))continue;
      const output=JSON.parse(await readFile(path.join(folder,name,'index.json'),'utf8'));
      if(output.kind!=='himr_private_summary_reader_export'||output.phase!=='transcripts'||!output.phase_complete)continue;
      for(const row of output.records){
        const rid=physicalIds.get(row.recording_id);
        if(!rid||!manifest.shards[n].some((s)=>s.recording_id===row.recording_id))throw new Error('Unmapped targeted summary');
        if(transcripts.has(rid))continue;
        transcripts.set(rid,{id:'recording-'+rid.slice(4),kind:'transcript',period:(row.date.value||'').slice(0,7)||null,
          title:row.title,recording_id:rid,publication:'prepared',sections:Object.fromEntries(Object.entries(row.sections).map(([section,items])=>
            [section,items.map((item)=>({text:item.text,classification:item.classification,source_recording_ids:[rid]}))]))});
        addedTranscripts++;
      }
    }
  }
}
const summaries=[...transcripts.values(),...broader];
// A bounded standard-API tail recovery, separate from the stalled batch receipts.
// Only admit a completed, content-bound reader and keep any existing summary.
try {
  const receipt=JSON.parse(await readFile('research/private-summaries/gemini-standard-tail-20260917/completed.json','utf8'));
  if(receipt.state!=='completed')throw new Error('Standard tail is not complete');
  const ref=receipt.export.reader;
  const expected=path.resolve('research/private-summaries/gemini-standard-tail-20260917/exports/reader.json');
  if(path.resolve(ref.path)!==expected)throw new Error('Unexpected standard tail reader');
  const bytes=await readFile(expected);
  if(createHash('sha256').update(bytes).digest('hex')!==ref.sha256)throw new Error('Changed standard tail reader');
  const reader=JSON.parse(bytes);
  if(!reader.phase_complete||reader.phase!=='transcripts'||reader.records.length!==1)throw new Error('Incomplete standard tail reader');
  const row=reader.records[0],mapped=mapping.records.find(r=>r.recording_id===row.recording_id);
  const record=catalog.find(r=>r.recording_id===mapped?.public_recording_id);
  if(!record)throw new Error('Unmapped standard tail summary');
  if(!transcripts.has(record.recording_id))summaries.push({id:'recording-'+record.recording_id.slice(4),kind:'transcript',period:(record.date_label||'').slice(0,7)||null,
    title:record.title,recording_id:record.recording_id,publication:'prepared',sections:Object.fromEntries(Object.entries(row.sections).map(([section,items])=>
      [section,items.map(item=>({text:item.text,classification:item.classification,source_recording_ids:[record.recording_id]}))]))});
} catch(error) { if(error.code!=='ENOENT')throw error; }
normalizeSummaryUnicode({summaries});
const value={...original,release_id:'summaries_'+createHash('sha256').update(JSON.stringify(summaries)).digest('hex').slice(0,24),generated_at:new Date().toISOString().replace(/\.\d{3}Z$/,'Z'),summaries};
validateSummaryRelease(value,catalogIds,{allowPrepared:true});
const target=path.join(root,'summaries/refreshed.json');const temporary=target+`.${process.pid}.tmp`;
await writeFile(temporary,JSON.stringify(value)+'\n',{mode:0o600,flag:'wx'});await rename(temporary,target);
console.log(JSON.stringify({transcript_summaries:summaries.length-broader.length,added_targeted_transcripts:addedTranscripts,broader_summaries:broader.length,resolvedCatalogSources,held,restart_local_dev_to_reload:true,paid_requests:0}));
