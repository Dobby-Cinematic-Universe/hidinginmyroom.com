import assert from 'node:assert/strict';
import test from 'node:test';
import { analysisShardId, analysisShards } from '../../src/lib/analysis/shards.mjs';
import { decodeShard, encodeShard, shardRows } from '../../src/lib/analysis/shard-codec.mjs';
import { readVerifiedShard } from '../../src/lib/analysis/shard-verify.mjs';

const score = (value, low = 0, high = 4) => ({ confidence: 0.7, coverage: 1, heterogeneity: 1.059363, high, low, uncertainty: 'model_rating_range', value });
const videos = [
  { recording_id: 'rec_a', title: 'Café "one"', date: '2020-01-01', category: 'vlog', question_scores: { q1: score(1.59), q2: score(0.25, 0, 1) }, classifications: { c: { choice: 'viewers', probabilities: { viewers: 0.9, no_one: 0.1 } } }, design: { kind: 'video', version_of: null } },
  { recording_id: 'rec_b', title: '}{ "videos": []', date: null, category: 'vlog', question_scores: { q1: score(2), q2: null }, classifications: { c: { choice: 'no_one', probabilities: { viewers: 0.2, no_one: 0.8 } } }, design: { kind: 'stream', version_of: 'rec_a' } },
  { recording_id: 'rec_c', title: 'Third', date: '2021-05', category: null, question_scores: { q2: score(3), q1: 1.5 }, classifications: {}, second: { F1: 0.5 } },
];
const [shard] = analysisShards(videos), release = 'release_x';
const body = (encoded = shard.encoded, extra = {}) => new TextEncoder().encode(JSON.stringify({ schema_version: 2, shard_id: shard.id, corpus_release_id: release, videos: encoded, ...extra }));

test('the compact encoding gives back exactly what was encoded, key order included', () => {
  const cases = [videos, [], [1, 2, 3], [null, null, null], ['a', 'a', 'b', null, 'a', 'b'], [true, 1, 'x', null], [{}, {}, {}], [{ a: 1 }, { a: 2, b: 3 }, null, [1, 2], { a: 4 }, 7], [{ b: 1, a: 2 }, { a: 2, b: 1 }],
    [{ 10: 'x', 2: 'y', k: [{ deep: 1 }, { deep: 2 }] }, { 10: 'x', 2: 'z', k: [] }], [JSON.parse('{"__proto__":1,"a":2}'), JSON.parse('{"__proto__":3,"a":4}')], [{ a: { b: { c: [1, { d: null }] } } }, { a: { b: { c: [] } } }], [{ a: 1e-7, b: -0.5, c: 123456789.123 }, { a: 0, b: 1e21, c: -1 }]];
  for (const rows of cases) {
    const encoded = encodeShard(rows), served = JSON.parse(JSON.stringify(encoded));
    assert.equal(JSON.stringify(decodeShard(served)), JSON.stringify(rows));
    assert.deepEqual(decodeShard(served), rows);
  }
  assert.equal(shard.encoded[0], 'g');                                    // rows of several shapes are grouped, not carried as they are
  assert.deepEqual(shardRows({ schema_version: 2, videos: shard.encoded }), videos); assert.equal(shardRows({ schema_version: 1, videos }), videos);
  assert.throws(() => shardRows({ schema_version: 3, videos }), /Invalid score shard encoding/);
});

test('a malformed encoding is refused rather than guessed at', () => {
  const bad = [null, 'p', [], ['x', []], ['p'], ['p', 1], ['c', 1], ['c', {}, 3], ['c', 1, 1], ['d', ['a'], [1]], ['d', [1], [0]], ['o', ['a'], [], 1], ['o', ['a', 'a'], [['p', [1]], ['p', [2]]], 1], ['o', ['a'], [['p', [1, 2]]], 1],
    ['o', ['__proto__'], [['p', [1]]], 1], ['g', [0, 1, 1], [['p', [1]], ['p', [2]]]], ['g', [0, 0], [['p', [1]], ['p', [2]]]], ['g', [0], [['p', [1]]]], ['g', [2], [['p', [1]], ['p', [2]]]]];
  for (const node of bad) assert.throws(() => decodeShard(node), /Invalid score shard encoding/, JSON.stringify(node));
});

test('a score shard is accepted as served, and laid out otherwise', async () => {
  assert.equal(shard.id, analysisShardId(0, shard.encoded)); assert.equal(shard.count, 3);
  assert.deepEqual(await readVerifiedShard(body(), shard, release), videos);
  const pretty = new TextEncoder().encode(JSON.stringify({ corpus_release_id: release, videos: shard.encoded, shard_id: shard.id, schema_version: 2 }, null, 1));
  assert.deepEqual(await readVerifiedShard(pretty, shard, release), videos);
  assert.deepEqual(await readVerifiedShard(body(shard.encoded, { later: true }), shard, release), videos);
});

test('a changed, stale, truncated, mislabelled or old-format score shard is refused', async () => {
  const refused = (bytes, s = shard, r = release) => assert.rejects(readVerifiedShard(bytes, s, r), /invalid or stale score shard/);
  await refused(body(encodeShard([{ ...videos[0], title: 'changed' }, videos[1], videos[2]])));
  await refused(body(encodeShard(videos.slice(0, 2))));
  await refused(body().subarray(0, 40));
  await refused(new TextEncoder().encode('not json'));
  await refused(new TextEncoder().encode(JSON.stringify({ schema_version: 1, shard_id: shard.id, corpus_release_id: release, videos })));
  await refused(body(), shard, 'release_y');
  await refused(body(), { ...shard, count: 4 });
  await refused(body(), { id: analysisShardId(0, []), count: 3 });
  await refused(body(), { id: 'bad', count: 3 });
  const forged = ['c', 1, 3], id = analysisShardId(0, forged);                // a valid hash over an encoding that does not hold three rows of the right kind still decodes to a list; the release validation that follows rejects such rows
  assert.deepEqual(await readVerifiedShard(new TextEncoder().encode(JSON.stringify({ schema_version: 2, shard_id: id, corpus_release_id: release, videos: forged })), { id, count: 3 }, release), [1, 1, 1]);
});
