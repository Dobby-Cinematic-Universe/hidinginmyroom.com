import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

test('wiki evidence metadata wraps long identifiers and locators without widening the page',async()=>{
  const source=await readFile(new URL('../../src/components/wiki/SourceCitation.astro',import.meta.url),'utf8');
  assert.match(source,/\.source-citation\s*\{[^}]*min-width: 0;[^}]*overflow-wrap: anywhere/);
  assert.match(source,/\.source-citation__header\s*\{[^}]*flex-wrap: wrap/);
  assert.match(source,/dd\s*\{[^}]*min-width: 0;[^}]*overflow-wrap: anywhere/);
  assert.match(source,/dd code\s*\{[^}]*white-space: normal;[^}]*overflow-wrap: anywhere/);
  assert.match(source,/@media \(max-width: 30rem\)[\s\S]*grid-template-columns: 4\.5rem minmax\(0, 1fr\)/);
  for (const field of ['{sourceId}','{locator}','{claimId}','{evidence}','{reviewedAt}']) assert.ok(source.includes(field));
});
