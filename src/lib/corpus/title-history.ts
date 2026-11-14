import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';

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

/** Earlier titles of every recording that has any, from archived channel listings. */
export async function titleHistory():Promise<Record<string,EarlierTitle[]>>{
  cached??=load();
  return (await cached).recordings;
}
