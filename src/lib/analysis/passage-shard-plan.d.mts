import type { AnalysisRelease } from './explorer.mjs';
import type { PassageShardDescriptor } from './passage-shards.mjs';
export function planPassageShards(release:AnalysisRelease, readRecording:(id:string)=>Promise<unknown>):Promise<PassageShardDescriptor[]>;
export function renderPassageShard(descriptor:PassageShardDescriptor, release:AnalysisRelease, readRecording:(id:string)=>Promise<unknown>):Promise<string>;
