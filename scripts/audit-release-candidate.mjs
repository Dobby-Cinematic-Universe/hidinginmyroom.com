import {readFile,writeFile,readdir} from 'node:fs/promises';
import path from 'node:path';
const name=process.argv[2];
if(!/^candidate-[a-z0-9-]+$/.test(name||''))throw Error('Expected candidate-NAME');
const root=path.resolve('research/site-release-candidates',name),dist=path.join(root,'dist');
const report=JSON.parse(await readFile(root+'/candidate-report.json','utf8'));
if(report.build!=='passed')throw Error('Candidate build must finish first');
const files=[];async function walk(dir){for(const e of await readdir(dir,{withFileTypes:true})){const f=path.join(dir,e.name);if(e.isDirectory())await walk(f);else files.push(path.relative(dist,f));}}await walk(dist);
const names=new Set(files),links=new Map(),leaks=[];
const searchManifest=JSON.parse(await readFile(path.join(dist,'corpus/search-manifest.json'),'utf8'));
const searchHtml=await readFile(path.join(dist,'corpus/index.html'),'utf8');
const searchCount=Number(searchHtml.match(/data-record-count="(\d+)"/)?.[1]);
if(searchCount!==searchManifest.record_count)throw Error('Search UI count does not match packaged index');
let pages=0;
for(const f of files){
  if(!/\.(html|json|js|css|txt)$/.test(f))continue;
  const text=await readFile(path.join(dist,f),'utf8');
  if(/\/home\/user\/|\/mnt\/archive\/|research\/private-|CLOUDFLARE_API_TOKEN|CLIENT_HASH_SECRET/.test(text))leaks.push(f);
  if(!f.endsWith('.html'))continue;pages++;
  for(const m of text.matchAll(/\bhref="(\/[^"#?]*)[^\"]*"/g)){
    const href=m[1];if(href.startsWith('//'))continue;
    let target;try{target=decodeURIComponent(href).replace(/^\//,'');}catch{continue;}
    if(!target||target.endsWith('/'))target+='index.html';
    if(!names.has(target)&&!names.has(target+'/index.html')){
      if(!links.has(href))links.set(href,f);
    }
  }
}
const graph=JSON.parse(await readFile(root+'/src/data/corpus/graph/manifest.json','utf8'));
const value={publication_approved:false,deployable:false,pages_checked:pages,broken_links:[...links].map(([href,source])=>({href,source})),private_path_or_credential_name_findings:leaks,production_graph_counts:graph.counts,free_pages_fit:report.free_pages_fit,held_summaries:report.held_summaries,remaining:['Archive overview pending; refresh summaries after completion.','Missing transcript has one zero-duration segment; retain source and resolve timing before admitting it.','Derived entity/event graph is local-only and needs explicit release packaging.','Attribution must be projected publicly without review artifacts.','Public RAG indexing and real-domain launch checks remain outstanding.']};
if(!report.held_summaries.length)value.remaining=value.remaining.filter(s=>!s.startsWith('Missing transcript'));
if(!report.release_blockers.some(s=>s.startsWith('Archive overview')))value.remaining=value.remaining.filter(s=>!s.startsWith('Archive overview'));
if(report.graph){
  value.production_graph_counts={entities:files.filter(f=>/^corpus\/entities\/[^/]+\/index.html$/.test(f)).length,events:report.graph.events,event_groups:report.graph.groups,description_pages:files.filter(f=>/^corpus\/event-descriptions\/[^/]+\/index.html$/.test(f)).length};
  value.remaining=value.remaining.filter(s=>!s.startsWith('Derived entity'));
}
if(report.public_attribution_records){value.public_attribution_records=report.public_attribution_records;value.remaining=value.remaining.filter(s=>!s.startsWith('Attribution'));}
await writeFile(root+'/release-audit.json',JSON.stringify(value,null,2)+'\n',{mode:0o600});
console.log(JSON.stringify(value));
if(links.size||leaks.length||!report.free_pages_fit)process.exitCode=1;
