import { validatePassageData } from './passage-data.mjs';
import { matchesAnalysisIdentity } from './supplement.mjs';

export const PASSAGE_SHARD_MAX_BYTES = 4 * 1024 * 1024;
export const PASSAGE_SHARD_MAX_RECORDINGS = 32;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const keys = (value, allowed) => object(value) && Object.keys(value).length === allowed.length && Object.keys(value).every(key => allowed.includes(key));
const assert = (value, field) => { if (!value) throw new Error(`Invalid passage shard: ${field}`); };

export function passageShardEnvelope(release, recordings) {
  return { schema_version: 1, corpus_release_id: release.corpus_release_id,
    questionnaire_version: release.questionnaire_version, model: release.model,
    source_generated_at: release.generated_at, recordings };
}

// An undefined index is the development endpoint contract. A published index
// must cover every recording exactly once, even when passage data is opened later.
export function validatePassageShardIndex(value, release) {
  if (value === undefined) return null;
  assert(Array.isArray(value) && value.length > 0 && value.length <= release.videos.length, 'index count');
  const known = new Set(release.videos.map(video => video.recording_id));
  const seen = new Set(), ids = new Set(), result = new Map();
  for (const shard of value) {
    assert(keys(shard, ['id','url','bytes','count','recording_ids']), 'index fields');
    assert(typeof shard.id === 'string' && /^[a-f0-9]{64}$/.test(shard.id) && !ids.has(shard.id), 'index identity');
    ids.add(shard.id);
    assert(shard.url === `/corpus/analysis/passage-shards/${shard.id}.json`, 'canonical URL');
    assert(Number.isInteger(shard.bytes) && shard.bytes > 0 && shard.bytes <= PASSAGE_SHARD_MAX_BYTES, 'byte limit');
    assert(Number.isInteger(shard.count) && shard.count > 0 && shard.count <= PASSAGE_SHARD_MAX_RECORDINGS && Array.isArray(shard.recording_ids) && shard.recording_ids.length === shard.count, 'recording count');
    for (const id of shard.recording_ids) {
      assert(known.has(id) && !seen.has(id), 'recording coverage');
      seen.add(id); result.set(id, shard);
    }
  }
  assert(seen.size === known.size, 'complete recording coverage');
  return result;
}

export async function validatePassageShardBytes(bytes, descriptor, release) {
  assert(bytes instanceof Uint8Array && bytes.byteLength === descriptor.bytes && bytes.byteLength <= PASSAGE_SHARD_MAX_BYTES, 'payload size');
  const hash = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  const digest = [...new Uint8Array(hash)].map(byte => byte.toString(16).padStart(2, '0')).join('');
  assert(digest === descriptor.id, 'payload digest');
  const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  assert(keys(value, ['schema_version','corpus_release_id','questionnaire_version','model','source_generated_at','recordings']) && matchesAnalysisIdentity(value, release), 'release identity');
  assert(Array.isArray(value.recordings) && value.recordings.length === descriptor.count, 'payload count');
  const videos = new Map(release.videos.map(video => [video.recording_id, video]));
  const result = new Map();
  for (const [index, data] of value.recordings.entries()) {
    const id = descriptor.recording_ids[index];
    assert(data?.recording?.recording_id === id && !result.has(id) && videos.has(id), 'payload recording inventory');
    result.set(id, validatePassageData(data, release, videos.get(id)));
  }
  return result;
}
