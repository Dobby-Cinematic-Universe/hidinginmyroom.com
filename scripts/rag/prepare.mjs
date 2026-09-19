// Explicit private-pilot export. No network, media, local ASR or review artifacts.
import {readFile,mkdir,writeFile,rmdir} from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {root,save} from './cloudflare.mjs';
import {validateSummaryRelease} from '../../src/lib/summaries/schema.mjs';
const hash=b=>createHash('sha256').update(b).digest('hex');
const full=process.argv.includes('--full');
if(process.env.RAG_PROFILE==='public'&&(!full||!process.argv.includes('--publication-approved')))throw Error('Public export requires --full --publication-approved');
if(full&&process.env.RAG_PROFILE!=='public')throw Error('Full export requires RAG_PROFILE=public');
await mkdir(root,{recursive:true,mode:0o700});
await mkdir(path.join(root,'upload.lock'));
try {
const pointer=JSON.parse(await readFile('research/corpus/site-previews/current.json','utf8'));
const requestedPreview=process.argv.find(arg=>arg.startsWith('--preview='));
if(requestedPreview)pointer.directory=requestedPreview.slice('--preview='.length);
if(!/^release-[a-z0-9-]+$/.test(pointer.directory))throw Error('Invalid private preview');
const base=path.resolve('research/corpus/site-previews',pointer.directory);
const manifest=JSON.parse(await readFile(path.join(base,'corpus/manifest.json'),'utf8'));
if(!/^release_[a-f0-9]+$/.test(manifest.release_id))throw Error('Invalid release');
const release=path.join(base,'corpus/releases',manifest.release_id);
async function bound(ref){const p=path.resolve(release,ref.path);if(!p.startsWith(release+'/'))throw Error('Unsafe shard path');const bytes=await readFile(p);if(hash(bytes)!==ref.sha256)throw Error('Changed input shard');return JSON.parse(bytes);}
const records=(await Promise.all(manifest.catalog_shards.map(bound))).flatMap(s=>s.recordings);
const annotations=JSON.parse(await readFile(path.join(base,'corpus/annotations.json'),'utf8')).recordings;
const summaries=JSON.parse(await readFile(path.join(base,'summaries/refreshed.json'),'utf8'));
validateSummaryRelease(summaries,new Set(records.map(r=>r.recording_id)),{allowPrepared:true});
const byId=new Map(records.map(r=>[r.recording_id,r]));
const docs=[];await mkdir(path.join(root,'documents'),{recursive:true,mode:0o700});
async function document(text,meta){const sha=hash(text),key=`${meta.kind}-${sha.slice(0,32)}.txt`;await writeFile(path.join(root,'documents',key),text,{mode:0o600});docs.push({key,sha256:sha,bytes:Buffer.byteLength(text),...meta});}
for(const s of summaries.summaries.filter(s=>s.kind==='transcript')){
  const r=byId.get(s.recording_id);if(!r||!['cloud','third_party'].includes(annotations[r.recording_id]?.origin))continue;
  const href=`/corpus/videos/${r.slug}/`;
  await document(`TITLE: ${r.title}\nRECORDING DATE: ${r.date_label||'Unknown'} (not necessarily event date)\nSOURCE: ${href}\nTYPE: Machine-generated summary, not verified facts.\n\n`+Object.entries(s.sections).map(([name,items])=>`${name.toUpperCase()}\n${items.map(i=>`[${i.classification}] ${i.text}`).join('\n')}`).join('\n\n'),{kind:'summary',title:r.title,href,recording_id:r.recording_id,date:r.date_label});
}
// Stratify over dates and origins. Whole retained transcript, not hand-picked excerpts.
const eligible=records.filter(r=>r.transcript_revision_count>0&&['third_party','cloud'].includes(annotations[r.recording_id]?.origin)).sort((a,b)=>(a.date_label||'').localeCompare(b.date_label||'')||a.recording_id.localeCompare(b.recording_id));
const selected=full?eligible:[...new Map(Array.from({length:Math.min(100,eligible.length)},(_,i)=>{const r=eligible[Math.floor(i*eligible.length/Math.min(100,eligible.length))];return [r.recording_id,r];})).values()];
if(full){
  for(const r of records){
    const href=`/corpus/videos/${r.slug}/`;
    await document(`TITLE: ${r.title}\nRECORDING DATE: ${r.date_label||'Unknown'} (not necessarily event date)\nSOURCE: ${href}\nTYPE: Catalog metadata only; this entry is not evidence of what was said.\n`,{kind:'metadata',title:r.title,href,recording_id:r.recording_id,date:r.date_label});
  }
  for(const s of summaries.summaries.filter(s=>s.kind!=='transcript')){
    if(!/^[a-z0-9_-]+$/.test(s.id))throw Error('Unsafe summary ID');
    const href=`/corpus/summaries/${s.id}/`;
    await document(`TITLE: ${s.title}\nPERIOD: ${s.period||'Archive overview'}\nSOURCE: ${href}\nTYPE: Machine-generated ${s.kind} synthesis, not verified facts.\n\n`+Object.entries(s.sections).map(([name,items])=>`${name.toUpperCase()}\n${items.map(i=>`[${i.classification}] ${i.text}\nSources: ${(i.source_recording_ids||[]).map(id=>byId.get(id)).filter(Boolean).map(r=>`/corpus/videos/${r.slug}/`).join(' ')}`).join('\n')}`).join('\n\n'),{kind:'synthesis',title:s.title,href,summary_id:s.id});
  }
}
for(const r of selected){
  const raw=await bound(r.detail);const recording=raw.recording||raw;
  const revision=recording.transcript_revisions.filter(t=>t.lifecycle_state!=='retracted')[0];if(!revision)continue;
  let pieces=[],size=0,start=null;
  async function flush(){if(!pieces.length)return;const href=`/corpus/videos/${r.slug}/#segment-${revision.revision_id}-${start.segment_id}`;await document(`TITLE: ${r.title}\nRECORDING DATE: ${r.date_label||'Unknown'} (not necessarily event date)\nSOURCE: ${href}\nTYPE: Unreviewed machine transcript; statements are not established facts.\n\n${pieces.join('\n')}`,{kind:'transcript',title:r.title,href,recording_id:r.recording_id,date:r.date_label,start_ms:start.start_ms,revision:revision.revision_id});pieces=[];size=0;start=null;}
  for(const seg of revision.segments){const line=`[${seg.start_ms}ms] ${seg.speaker_label?seg.speaker_label+': ':''}${seg.text}`;if(size+line.length>12000)await flush();start??=seg;pieces.push(line);size+=line.length;}
  await flush();
}
// Keep a recoverable copy before replacing an earlier approved export.
try{await save('manifest.previous.json',JSON.parse(await readFile(path.join(root,'manifest.json'),'utf8')));}catch(e){if(e.code!=='ENOENT')throw e;}
await save('manifest.json',{version:1,scope:full?'full':'pilot',private:!full,publicationApproved:full,freeOnlyConfirmed:true,uploadAuthorized:true,created_at:new Date().toISOString(),corpus_release:manifest.release_id,summary_release:summaries.release_id,selected_recordings:selected.map(r=>r.recording_id),documents:docs});
console.log(JSON.stringify({documents:docs.length,summaries:docs.filter(d=>d.kind==='summary').length,transcript_files:docs.filter(d=>d.kind==='transcript').length,recordings:selected.length,bytes:docs.reduce((n,d)=>n+d.bytes,0)}));
}finally{await rmdir(path.join(root,'upload.lock'));}
