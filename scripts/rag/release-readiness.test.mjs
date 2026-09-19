import test from 'node:test';
import assert from 'node:assert/strict';
import {indexingAdvisory, requireAcceptedUploads} from './release-readiness.mjs';

const manifest = {documents: [{key: 'first'}, {key: 'second'}]};
const receipts = {first: {accepted: true}, second: {accepted: true}};
const completed = {completed: 2, queued: 0, running: 0, error: 0, skipped: 0, outdated: 0};

test('accepted uploads allow release while most documents are requeued', async () => {
  assert.doesNotThrow(() => requireAcceptedUploads(manifest, receipts));
  const status = await indexingAdvisory(24269, async () => ({completed: 4436, queued: 19792, running: 34, error: 0, skipped: 0, outdated: 7}));
  assert.equal(status.blocking, false);
  assert.equal(status.state, 'incomplete');
  assert.equal(status.queued, 19792);
});

test('every manifest document must have an accepted upload receipt', () => {
  for (const second of [undefined, {}, {accepted: false}, {accepted: 'true'}]) {
    assert.throws(() => requireAcceptedUploads(manifest, {first: receipts.first, second}), /Uploads incomplete/);
  }
  assert.throws(() => requireAcceptedUploads(manifest, undefined), /Uploads incomplete/);
  assert.doesNotThrow(() => requireAcceptedUploads(manifest, {...receipts, stale: {accepted: false}}));
});

test('complete and mismatched counts are reported, not used as release gates', async () => {
  assert.equal((await indexingAdvisory(2, async () => completed)).state, 'complete');
  for (const stats of [{...completed, completed: 1}, {...completed, completed: 3}, {...completed, outdated: 1}, {}]) {
    const status = await indexingAdvisory(2, async () => stats);
    assert.equal(status.blocking, false);
    assert.equal(status.state, 'incomplete');
  }
});

test('failed and skipped index items remain visible as non-blocking warnings', async () => {
  for (const key of ['error', 'skipped']) {
    const status = await indexingAdvisory(2, async () => ({...completed, [key]: 1}));
    assert.equal(status.state, 'needs_attention');
    assert.equal(status[key], 1);
    assert.equal(status.blocking, false);
  }
});

test('unavailable provider stats do not block release or expose provider errors', async () => {
  const status = await indexingAdvisory(2, async () => {throw Error('private provider details');});
  assert.equal(status.state, 'unavailable');
  assert.equal(status.blocking, false);
  assert.doesNotMatch(JSON.stringify(status), /private provider details/);
});
