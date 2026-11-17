// Validate the complete approved scoring projection without private tooling.
import {readFile,readdir,lstat} from 'node:fs/promises';
import path from 'node:path';
import {validateAnalysisRelease} from '../src/lib/analysis/explorer.mjs';
import {analysisLoadingHash,validateSupplementPart} from '../src/lib/analysis/supplement.mjs';
import {validatePassageData} from '../src/lib/analysis/passage-data.mjs';
import {validateDiscovery} from '../src/lib/analysis/discovery.mjs';
import {validateRecordingUmap} from '../src/lib/analysis/recording-umap.mjs';
import {PASSAGE_TABLE_FILE,validatePassageTable} from '../src/lib/analysis/passage-table.mjs';

const directory=path.resolve(process.argv[2]??'src/data/analysis');
const read=async name=>JSON.parse(await readFile(path.join(directory,name),'utf8'));
const release=validateAnalysisRelease(await read('release.json'));
const supplements={robustness:'extended-robustness.json',joint_trends:'joint-trends.json',map_diagnostics:'map-diagnostics.json',discovery:'recording-discovery.json',recording_umap:'recording-umap.json',passage_insights:'passage-insights.json',factor_diagnostics:'factor-diagnostics.json',temporal_comparisons:'temporal-comparisons.json'};
const values={};
for(const[k,file]of Object.entries(supplements))values[k]=await validateSupplementPart(await read(file),release,k);
const discovery=await validateDiscovery(release,values.discovery);
if(!discovery||!await validateRecordingUmap(release,discovery,values.recording_umap))throw Error('Recording map validation failed');
const files=await readdir(path.join(directory,'passages'));
const expected=new Set(release.videos.map(v=>`${v.recording_id}.json`));
if(files.length!==expected.size||files.some(file=>!expected.has(file)))throw Error('Passage inventory mismatch');
let passages=0;
for(const video of release.videos){
  const name=`passages/${video.recording_id}.json`,stat=await lstat(path.join(directory,name));
  if(!stat.isFile()||stat.isSymbolicLink()||stat.size>8*1024*1024)throw Error('Unsafe passage file');
  passages+=validatePassageData(await read(name),release,video).passages.length;
}
if(passages!==release.coverage.scored_chunks||release.videos.length!==release.coverage.scored_recordings)throw Error('Analysis coverage mismatch');
// The passage table is optional; when present it must belong to this release and this factor solution.
let table=null;
try{const stat=await lstat(path.join(directory,PASSAGE_TABLE_FILE));if(!stat.isFile()||stat.isSymbolicLink()||stat.size>8*1024*1024)throw Error('Unsafe passage table');table=validatePassageTable(await read(PASSAGE_TABLE_FILE),release);}
catch(error){if(error.code!=='ENOENT')throw error;}
if(table&&table.loadings_sha256!==await analysisLoadingHash(release))throw Error('Passage table belongs to another factor solution');
console.log(JSON.stringify({validated:true,recordings:release.videos.length,passages,loadings_sha256:await analysisLoadingHash(release),...(table?{passage_table_passages:table.passages}:{})}));
