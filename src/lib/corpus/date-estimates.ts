import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';
import {describeDateEstimate} from './public-date-estimates.mjs';

export interface DateEstimate {
  basis:'exact_copy'|'full_copy'|'text_estimate'|'title_date';
  date:string;
  copy:{kind:'exact'|'full'|'partial';recording_id:string;date:string}|null;
  estimate:{month:string;low:string;high:string;confidence_band:number}|null;
}
type Release={recordings:Record<string,DateEstimate>};
let cached:Promise<Release>|undefined;

function load():Promise<Release>{
  const root=localPreviewRoot();
  if(root){
    try{return Promise.resolve(JSON.parse(readFileSync(path.join(root,'corpus/date-estimates.json'),'utf8')));}
    catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;return Promise.resolve({recordings:{}});}
  }
  return readFile('src/data/corpus/date-estimates.json','utf8').then(JSON.parse).catch(error=>{if(error.code==='ENOENT')return {recordings:{}};throw error;});
}

/** Estimate for an undated recording, with display text; undefined when none exists. */
export async function recordingDateEstimate(id:string){
  cached??=load();
  return describeDateEstimate((await cached).recordings[id]);
}

/** Card text ("Estimated …") for an undated recording in lists; undefined when none exists. */
export async function recordingDateEstimateCard(id:string):Promise<string|undefined>{
  return (await recordingDateEstimate(id))?.card;
}
