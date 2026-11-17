import { decodeShard } from './shard-codec.mjs';

// Reads one score shard and checks it against its identity before the explorer uses it.
//
// A shard serves its rows in a compact lossless encoding (shard-codec.mjs). Its id ends in the first 16 hex digits
// of SHA-256 over the JSON text of the encoded rows, and the response body is
// JSON.stringify({schema_version, shard_id, corpus_release_id, videos}), so the bytes of `videos` are in the body as
// they were hashed. They are hashed where they lie and parsed from the same bytes: what is checked is what is used,
// and nothing is serialized a second time. A body laid out in any other way takes the slower path that parses it
// whole and hashes a fresh serialization, with the same result.
const decoder = new TextDecoder('utf-8', { fatal: true }), encoder = new TextEncoder();
const hex16 = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)).subarray(0, 8)].map(byte => byte.toString(16).padStart(2, '0')).join('');
const fail = () => { throw new Error('invalid or stale score shard'); };

export const shardPrefix = (id, corpusReleaseId) => `{"schema_version":2,"shard_id":${JSON.stringify(id)},"corpus_release_id":${JSON.stringify(corpusReleaseId)},"videos":`;

export async function readVerifiedShard(bytes, shard, corpusReleaseId) {
  if (!(bytes instanceof Uint8Array) || typeof shard?.id !== 'string' || !/^\d{3,}-[a-f0-9]{16}$/.test(shard.id)) fail();
  const expected = shard.id.slice(shard.id.indexOf('-') + 1), prefix = encoder.encode(shardPrefix(shard.id, corpusReleaseId));
  let encoded;
  if (bytes.length > prefix.length + 1 && bytes[bytes.length - 1] === 0x7d && prefix.every((byte, i) => bytes[i] === byte)) {
    const body = bytes.subarray(prefix.length, bytes.length - 1);
    if (await hex16(body) === expected) { try { encoded = JSON.parse(decoder.decode(body)); } catch { fail(); } }
  }
  if (encoded === undefined) {
    let data; try { data = JSON.parse(decoder.decode(bytes)); } catch { fail(); }
    if (data?.schema_version !== 2 || data.shard_id !== shard.id || data.corpus_release_id !== corpusReleaseId || !Array.isArray(data.videos) || await hex16(encoder.encode(JSON.stringify(data.videos))) !== expected) fail();
    encoded = data.videos;
  }
  let videos; try { videos = decodeShard(encoded); } catch { fail(); }
  if (videos.length !== shard.count) fail();
  return videos;
}
