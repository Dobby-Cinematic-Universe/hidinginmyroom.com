import {readFile,lstat} from 'node:fs/promises';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';
import {loadCorpusCatalog} from './release';
import {loadSummaryRelease} from '../summaries/release';
import {createHash} from 'node:crypto';
import {validateNetworkProjection} from './network-dynamics.mjs';

/** Keep the server loader distinct from the pure .mjs projection module. */
export async function loadNetworkDynamics(){
  const root=localPreviewRoot();
  const corpusRoot=root?path.join(root,'corpus'):process.env.HIMR_CORPUS_DATA_ROOT?path.resolve(process.env.HIMR_CORPUS_DATA_ROOT):path.resolve('src/data/corpus');
  const file=path.join(corpusRoot,'network-dynamics.json');
  try{
    const stat=await lstat(file);
    if(!stat.isFile()||stat.isSymbolicLink()||stat.size>2_000_000)throw new Error('Invalid network projection file');
    const [catalog,summaries,manifest,raw]=await Promise.all([loadCorpusCatalog(),loadSummaryRelease(),readFile(path.join(corpusRoot,'manifest.json')),readFile(file,'utf8')]);
    const data=validateNetworkProjection(JSON.parse(raw),{corpus_release:catalog.releaseId,summary_release:summaries.release_id,corpus_manifest_sha256:createHash('sha256').update(manifest).digest('hex')});
    return {status:'ready' as const,projection:data};
  }catch(error){
    if((error as NodeJS.ErrnoException).code==='ENOENT')return {status:'missing' as const};
    // A stale or malformed research artifact must never be shown as current research.
    console.warn('Mention dynamics withheld: projection validation or release binding failed.');
    return {status:'withheld' as const};
  }
}
