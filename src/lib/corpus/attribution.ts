import {readFile} from 'node:fs/promises';
import {previewAnnotation} from '../local-preview';
interface Attribution {origin:string;attribution:string|null;model:string|null;coverage_note?:string|null;speaker_review_complete?:boolean;}
let cached:Promise<{recordings:Record<string,Attribution>}>|undefined;
export async function recordingAttribution(id:string):Promise<Attribution|undefined>{
  const local=previewAnnotation(id);if(local)return local;
  cached??=readFile('src/data/corpus/attribution.json','utf8').then(JSON.parse).catch(error=>{if(error.code==='ENOENT')return {recordings:{}};throw error;});
  return (await cached).recordings[id];
}
