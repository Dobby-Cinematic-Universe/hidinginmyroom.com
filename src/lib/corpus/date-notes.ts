import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';
import {describeDateBasis as describe} from './public-date-notes.mjs';

export interface DateNote { basis:'archived_video_page'|'torrent_filename_utc'; captured?:string; source?:string; file_date?:string }
let cached:Promise<{recordings:Record<string,DateNote>}>|undefined;
function load(){
  const root=localPreviewRoot();
  if(root){
    try{return Promise.resolve(JSON.parse(readFileSync(path.join(root,'corpus/date-notes.json'),'utf8')));}
    catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;return Promise.resolve({recordings:{}});}
  }
  return readFile('src/data/corpus/date-notes.json','utf8').then(JSON.parse).catch(error=>{if(error.code==='ENOENT')return {recordings:{}};throw error;});
}
/** How a recording's catalog date was established, where that needs a sentence; undefined otherwise. */
export async function recordingDateNote(id:string):Promise<DateNote|undefined>{
  cached??=load();
  return (await cached).recordings[id];
}
export const describeDateBasis=(basis:string|null|undefined,note?:DateNote):string=>describe(basis,note);
