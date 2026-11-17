export function encodeShard(videos: unknown[]): unknown[];
export function decodeShard(encoded: unknown): unknown[];
/** The rows of a served shard body: decoded for schema version 2, as they are for version 1. */
export function shardRows(body: { schema_version?: number; videos?: unknown }): unknown[];
