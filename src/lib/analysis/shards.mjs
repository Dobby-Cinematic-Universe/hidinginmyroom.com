import { createHash } from 'node:crypto';
import { encodeShard } from './shard-codec.mjs';

export const ANALYSIS_SHARD_SIZE = 100;

/** Identity of a shard: its place and the first 16 hex digits of SHA-256 over the JSON text of what it serves. */
export function analysisShardId(index, videos) {
  if (!Number.isInteger(index) || index < 0 || !Array.isArray(videos)) throw new TypeError('Invalid analysis shard identity input.');
  const digest = createHash('sha256').update(JSON.stringify(videos)).digest('hex').slice(0, 16);
  return `${String(index).padStart(3, '0')}-${digest}`;
}

const cache = new WeakMap();
/** The score rows of a release in shards of 100, each in the compact lossless encoding (shard-codec.mjs) and
 *  named by the encoded rows it serves. Encoded once per release. */
export function analysisShards(videos) {
  if (!Array.isArray(videos)) throw new TypeError('Score rows must be a list.');
  let shards = cache.get(videos);
  if (!shards) {
    shards = [];
    for (let start = 0, index = 0; start < videos.length; start += ANALYSIS_SHARD_SIZE, index++) {
      const rows = videos.slice(start, start + ANALYSIS_SHARD_SIZE), encoded = encodeShard(rows);
      shards.push({ id: analysisShardId(index, encoded), count: rows.length, encoded });
    }
    cache.set(videos, shards);
  }
  return shards;
}
