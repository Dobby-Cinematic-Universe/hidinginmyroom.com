import { createHash } from 'node:crypto';
import { lstat, readFile, writeFile, rename } from 'node:fs/promises';
import path from 'node:path';
import { validateSummaryRelease } from '../src/lib/summaries/schema.mjs';

const directory = process.argv[2];
if (!directory || !/^release-[a-z0-9-]+$/.test(directory)) throw new Error('Pass a prepared snapshot directory name.');
const base = path.resolve('research/corpus/site-previews');
const root = path.join(base,directory);
const stat = await lstat(root);
if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o077)) throw new Error('Snapshot is not private.');
const report=JSON.parse(await readFile(path.join(root,'preparation.json'),'utf8'));
if (report.kind !== 'himr_private_site_preview' || report.publication_approved !== false) throw new Error('Not a prepared private preview.');
for (const [key,relative] of [['corpus_manifest','corpus/manifest.json'],['summary_release','summaries/release.json']]) {
  const data=await readFile(path.join(root,relative));
  if (report[key].path !== path.join(root,relative) || createHash('sha256').update(data).digest('hex') !== report[key].sha256) throw new Error('Preview artifact changed.');
}
const summary=JSON.parse(await readFile(path.join(root,'summaries/release.json'),'utf8'));
const mapping=JSON.parse(await readFile(path.join(root,'identity-map.json'),'utf8'));
validateSummaryRelease(summary,new Set(mapping.records.map((r)=>r.public_recording_id)),{allowPrepared:true});
const temporary=path.join(base,`current-${process.pid}.tmp`);
await writeFile(temporary,JSON.stringify({directory})+'\n',{mode:0o600,flag:'wx'});
await rename(temporary,path.join(base,'current.json'));
console.log(JSON.stringify({active_local_preview:directory,counts:report.counts,production_release_changed:false}));
