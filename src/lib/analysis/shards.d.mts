export const ANALYSIS_SHARD_SIZE: number;
export function analysisShardId(index: number, videos: unknown[]): string;
export function analysisShards(videos: unknown[]): { id: string; count: number; encoded: unknown[] }[];
