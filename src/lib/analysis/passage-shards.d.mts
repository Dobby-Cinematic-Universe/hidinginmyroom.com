import type { AnalysisRelease } from './explorer.mjs';
import type { AnalysisPassageData } from './passage-data.mjs';
export interface PassageShardDescriptor { id:string; url:string; bytes:number; count:number; recording_ids:string[] }
export const PASSAGE_SHARD_MAX_BYTES:number;
export const PASSAGE_SHARD_MAX_RECORDINGS:number;
export function passageShardEnvelope(release:AnalysisRelease, recordings:AnalysisPassageData[]):unknown;
export function validatePassageShardIndex(value:unknown, release:AnalysisRelease):Map<string,PassageShardDescriptor>|null;
export function validatePassageShardBytes(bytes:Uint8Array, descriptor:PassageShardDescriptor, release:AnalysisRelease):Promise<Map<string,AnalysisPassageData>>;
