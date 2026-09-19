import test from 'node:test';
import assert from 'node:assert/strict';
import {configureIndex} from './index-configuration.mjs';

const config = {embedding_model: 'embedding-model', chunk_size: 400, index_method: {keyword: true, vector: true}, public_endpoint_params: {enabled: false}};
function fakeApi(current) {
  const calls = [];
  return {calls, api: async (route, options) => {calls.push({route, options}); return options?.method === 'PUT' ? {...current, ...options.body} : current;}};
}

test('unchanged configuration makes no write even with rebuild override', async () => {
  for (const allowIndexRebuild of [false, true]) {
    const {api, calls} = fakeApi({...config, id: 'existing', stats: {queued: 20000}, public_endpoint_params: {enabled: false, other_server_field: 1}});
    const result = await configureIndex({api, instance: 'existing', config, allowIndexRebuild});
    assert.equal(result.changed, false);
    assert.deepEqual(calls, [{route: 'ai-search/instances/existing', options: undefined}]);
  }
});

test('different or unknown configuration cannot implicitly reindex', async () => {
  for (const current of [{...config, chunk_size: 800}, {...config, embedding_model: 'different'}, {...config, index_method: {keyword: false, vector: true}}, {}]) {
    const {api, calls} = fakeApi(current);
    await assert.rejects(configureIndex({api, instance: 'existing', config}), /--allow-index-rebuild/);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].options, undefined);
  }
});

test('explicit override permits only the requested configuration write', async () => {
  const {api, calls} = fakeApi({...config, chunk_size: 800});
  const result = await configureIndex({api, instance: 'existing', config, allowIndexRebuild: true});
  assert.equal(result.changed, true);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1], {route: 'ai-search/instances/existing', options: {method: 'PUT', body: config}});
});

test('failed configuration read never falls back to a write', async () => {
  let calls = 0;
  const api = async () => {calls++; throw Error('Unavailable');};
  await assert.rejects(configureIndex({api, instance: 'existing', config, allowIndexRebuild: true}), /Unavailable/);
  assert.equal(calls, 1);
});
