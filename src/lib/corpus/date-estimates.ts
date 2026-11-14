import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';
import {describeDateEstimate,circaDate,duplicateOf} from './public-date-estimates.mjs';

export interface DateEstimate {
  basis:'exact_copy'|'full_copy'|'text_estimate'|'title_date'|'archived_listing'|'archived_page';
  date:string;
  copy:{kind:'exact'|'full'|'partial';recording_id:string;date:string}|null;
  estimate:{month:string;low:string;high:string;confidence_band:number}|null;
  listing?:{from:string;to:string};
  page?:{date:string;captured:string;source:string};
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

/** Reader overlay for lists: circa date (narrow archived listing window or archived video page) and the dated
 *  recording this one is another copy of. Catalog dates are not changed. */
export async function recordingOverlay():Promise<{circa:Map<string,string>;duplicateOf:Map<string,string>;copies:Map<string,string[]>}>{
  cached??=load();
  const circa=new Map<string,string>(),dup=new Map<string,string>(),copies=new Map<string,string[]>();
  for(const [id,row] of Object.entries((await cached).recordings)){
    const c=circaDate(row),d=duplicateOf(row);
    if(c)circa.set(id,c);
    if(d){dup.set(id,d);copies.set(d,[...(copies.get(d)??[]),id]);}
  }
  return {circa,duplicateOf:dup,copies};
}

/** Catalog order for static lists: newest first by catalog date or circa date; folded copies removed. */
export async function browseOrder<T extends {recording_id:string;date_label:string|null;title:string}>(recordings:T[]):Promise<T[]>{
  const {circa,duplicateOf:dup}=await recordingOverlay();
  const date=(r:T)=>r.date_label||circa.get(r.recording_id)||'';
  return recordings.filter(r=>!dup.has(r.recording_id)).sort((a,b)=>date(b).localeCompare(date(a))||a.title.localeCompare(b.title));
}
