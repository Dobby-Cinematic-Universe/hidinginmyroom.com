import type { PublicSummary } from './release';
import type { SummaryShardIndex } from './shards.mjs';
export function buildSummaryShards(release:{release_id:string;summaries:PublicSummary[]}):{index:SummaryShardIndex;assets:{id:string;text:string}[]};
