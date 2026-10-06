import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
const files=['src/components/corpus/DerivedDirectory.astro','src/pages/corpus/entities/[slug].astro'];

test('entity heading shares a restrained responsive size with safe long-label wrapping',async()=>{
  const source=await readFile(new URL('../../'+files[1],import.meta.url),'utf8');
  assert.match(source,/\.graph-detail h1\s*\{[^}]*font-size: clamp\(1\.8rem, 4vw, 3rem\);[^}]*line-height: 1\.2;[^}]*overflow-wrap: anywhere/);
});

test('derived directory controls have a wrapping row, full-width search and consistent touch size',async()=>{
  const source=await readFile(new URL('../../'+files[0],import.meta.url),'utf8');
  assert.match(source,/\.derived-directory\s*\{[^}]*display: flex;[^}]*flex-wrap: wrap;[^}]*gap: \.65rem \.75rem/);
  assert.match(source,/input\[type=search\]\s*\{[^}]*width: 100%;[^}]*min-width: 0;[^}]*height: 2\.75rem;[^}]*border-radius: \.4rem/);
  assert.match(source,/button\s*\{[^}]*min-height: 2\.75rem;[^}]*border-radius: \.4rem/);
  assert.match(source,/@media \(max-width: 30rem\)\s*\{ label \{ flex-basis: 100%/);
});

test('entity and directory retain evidence notices, filter semantics and progressive pagination',async()=>{
  const directory=await readFile(new URL('../../'+files[0],import.meta.url),'utf8');
  const entity=await readFile(new URL('../../'+files[1],import.meta.url),'utf8');
  for(const value of ['data-clear>Clear filters','data-more hidden={items.length<=40}','visible+=40;render();','words.every(w=>','if(input)input.value=\'\';if(subset)subset.checked=false;filter();','history.replaceState(null,\'\',url)']) assert.ok(directory.includes(value),value);
  for(const value of ['<DerivedNotice />','A label does not establish an on-camera appearance.','identities inferred from mentions','entity.speakerRecordings','appearance.anchor.source.url','edge.label']) assert.ok(entity.includes(value),value);
});
