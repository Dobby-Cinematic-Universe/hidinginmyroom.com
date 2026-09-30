import { createHash } from 'node:crypto';

export function analysisShardId(index, videos) {
  if (!Number.isInteger(index) || index < 0 || !Array.isArray(videos)) throw new TypeError('Invalid analysis shard identity input.');
  const digest = createHash('sha256').update(JSON.stringify(videos)).digest('hex').slice(0, 16);
  return `${String(index).padStart(3, '0')}-${digest}`;
}
