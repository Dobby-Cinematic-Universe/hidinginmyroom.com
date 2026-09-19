import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { createDevWatchIgnore } from '../dev-watch-ignore.mjs';

const root = path.resolve('example-workspace');
const ignored = createDevWatchIgnore(root);

test('prunes private workspaces at the directory boundary', () => {
  for (const directory of ['research','vidsprivate','vidspublic','corpus','pipeline','acquisition','operator_console',"Hidinginmyroom - Daniel's Patreon"]) {
    assert.equal(ignored(path.join(root,directory)),true);
    assert.equal(ignored(path.join(root,directory,'jobs','frame.json')),true);
    assert.equal(ignored(`${directory}/jobs/frame.json`),true);
  }
});
test('keeps public corpus, summary data, content and assets watched', () => {
  for (const entry of ['', 'src', 'src/data/corpus', 'src/data/summaries/release.json',
    'src/content/docs/wiki', 'public', 'public/images', 'astro.config.mjs', 'scripts/dev-watch-ignore.mjs',
    'research-notes']) assert.equal(ignored(path.join(root,entry)),false,entry);
});
test('does not accidentally ignore ancestors or sibling projects', () => {
  assert.equal(ignored(path.dirname(root)),false);
  assert.equal(ignored(path.resolve(root,'../research')),false);
});
