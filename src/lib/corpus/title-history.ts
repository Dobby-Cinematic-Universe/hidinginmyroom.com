import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';
import {recordingOverlay} from './date-estimates';

export interface EarlierTitle {title:string;seen:string}
type Release={recordings:Record<string,EarlierTitle[]>};
let cached:Promise<Release>|undefined;

function load():Promise<Release>{
  const root=localPreviewRoot();
  if(root){
    try{return Promise.resolve(JSON.parse(readFileSync(path.join(root,'corpus/title-history.json'),'utf8')));}
    catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;return Promise.resolve({recordings:{}});}
  }
  return readFile('src/data/corpus/title-history.json','utf8').then(JSON.parse).catch(error=>{if(error.code==='ENOENT')return {recordings:{}};throw error;});
}

/** Other titles a recording can be found by: its earlier titles, and the titles (present and earlier) of identical
 *  copies folded into it. Only recordings that have any are in the map. */
export async function titleAliases(recordings:{recording_id:string;title:string}[]):Promise<Map<string,string[]>>{
  const [overlay,history]=await Promise.all([recordingOverlay(),titleHistory()]);
  const byId=new Map(recordings.map(r=>[r.recording_id,r])),out=new Map<string,string[]>();
  for(const r of recordings){
    const titles=[...new Set([...(history[r.recording_id]||[]).map(t=>t.title),
      ...(overlay.copies.get(r.recording_id)||[]).flatMap(id=>byId.has(id)?[byId.get(id)!.title,...(history[id]||[]).map(t=>t.title)]:[])])].filter(t=>t!==r.title);
    if(titles.length)out.set(r.recording_id,titles);
  }
  return out;
}

/** Earlier titles of every recording that has any, from archived channel listings. */
export async function titleHistory():Promise<Record<string,EarlierTitle[]>>{
  cached??=load();
  return (await cached).recordings;
}
