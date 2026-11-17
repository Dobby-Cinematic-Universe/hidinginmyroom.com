import type { AnalysisVideo } from './explorer.mjs';
export function shardPrefix(id: string, corpusReleaseId: string): string;
export function readVerifiedShard(bytes: Uint8Array, shard: { id: string; count: number }, corpusReleaseId: string): Promise<AnalysisVideo[]>;
