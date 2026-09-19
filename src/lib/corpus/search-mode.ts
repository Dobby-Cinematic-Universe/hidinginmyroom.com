import {readFile} from 'node:fs/promises';
import {localPreviewRoot} from '../local-preview';
export async function recordingSearch(releaseId:string):Promise<boolean>{
  if(localPreviewRoot())return true;
  try{
    const config=JSON.parse(await readFile('src/data/corpus/search-config.json','utf8'));
    if(config.release_id!==releaseId||config.mode!=='recording')throw new Error('Search configuration release mismatch');
    return true;
  }catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return false;throw error;}
}
