import type { PublicSummary } from './release';
export type InlineSummary=Pick<PublicSummary,'id'|'sections'>;
export type SummaryShardDescriptor={id:string;url:string;sha256:string;count:number;summary_ids:string[]};
export type SummaryShardIndex={schema_version:1;release_id:string;shards:SummaryShardDescriptor[]};
export function validateSummaryShardIndex(value:unknown):Map<string,SummaryShardDescriptor>;
export function validateSummaryShard(value:unknown,descriptor:SummaryShardDescriptor,expectedRelease:string):Map<string,InlineSummary>;
export function validateSummaryShardBytes(bytes:Uint8Array,descriptor:SummaryShardDescriptor,expectedRelease:string):Promise<Map<string,InlineSummary>>;
