import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';

async function sidecar<T>(name:string,empty:T):Promise<T>{
  const root=localPreviewRoot();
  if(root){
    try{return JSON.parse(readFileSync(path.join(root,'corpus',name),'utf8'));}
    catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;return empty;}
  }
  try{return JSON.parse(await readFile(path.join('src/data/corpus',name),'utf8'));}
  catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return empty;throw error;}
}
export interface Chapter {start_ms:number;end_ms:number;terms:string[]}
export interface RelatedLink {target:string;position_ms:number|null}
let related:Promise<{forward:Map<string,RelatedLink[]>;backward:Map<string,{source:string;position_ms:number|null}[]>}>|undefined;
let chapters:Promise<Record<string,Chapter[]>>|undefined;

/** Recordings this one names ("refers back to") and later recordings that name it. Machine-detected. */
export function relatedRecordings(){
  related??=sidecar<{recordings:Record<string,RelatedLink[]>}>('related-recordings.json',{recordings:{}}).then(value=>{
    const forward=new Map(Object.entries(value.recordings));const backward=new Map<string,{source:string;position_ms:number|null}[]>();
    for(const [source,rows] of forward)for(const row of rows)backward.set(row.target,[...(backward.get(row.target)??[]),{source,position_ms:row.position_ms}]);
    return {forward,backward};
  });
  return related;
}
/** Machine-detected topic shifts of a long recording; empty when none were proposed. */
export async function recordingChapters(id:string):Promise<Chapter[]>{
  chapters??=sidecar<{recordings:Record<string,Chapter[]>}>('chapters.json',{recordings:{}}).then(v=>v.recordings);
  return (await chapters)[id]??[];
}
export const searchAliases=()=>sidecar<{sets:{canonical:string;forms:string[]}[]}>('search-aliases.json',{sets:[]});
export interface MentionBurst {subject:string;type:'game'|'product'|'platform';start:string;end:string;months:number;ratio:number|null;recordings:number;end_known:boolean;both_transcript_kinds:boolean}
/** Stretches in which a game, product or platform is named unusually often; empty when the release has none. */
export const mentionBursts=async()=>(await sidecar<{bursts:MentionBurst[]}>('mention-bursts.json',{bursts:[]})).bursts;
