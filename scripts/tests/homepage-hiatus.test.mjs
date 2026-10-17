import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

const read=file=>readFile(new URL('../../'+file,import.meta.url),'utf8');

test('homepage dates the break and qualifies the reported Discord departure',async()=>{
  const source=await read('src/pages/index.astro');
  assert.match(source,/announced another YouTube break/);
  assert.match(source,/<time datetime="2026-09-27">27 September 2026<\/time>/);
  assert.match(source,/Discord departure\s+is reported as <time datetime="2026-10-05">5 October 2026<\/time>/);
  assert.ok(source.includes('/wiki/events/2026-return-and-car-camping/#september-27october-5-another-departure'));
  assert.match(source,/signal-panel__live--paused/);
  assert.match(source,/on hiatus/);
  assert.doesNotMatch(source,/>\s*online\s*</);
});

test('hiatus indicators do not imply live activity or pulse',async()=>{
  const css=await read('src/styles/global.css');
  assert.match(css,/\.status-dot--paused\s*\{[^}]*box-shadow: none;[^}]*animation: none;/);
  assert.match(css,/\.signal-panel__live--paused\s*\{[^}]*color: var\(--muted\)/);
});

test('community and Instagram links distinguish prior participation from current activity',async()=>{
  const links=await read('src/data/links.ts');
  assert.match(links,/Daniel previously posted/);
  assert.doesNotMatch(links,/Daniel is a member/);
  assert.match(links,/uninstalled the app; account deletion is not established/);
  assert.match(links,/https:\/\/discord\.gg\/hgBhC5gNPU/);
});

test('Discord source map distinguishes a full export from incremental additions and server events',async()=>{
  const source=await read('src/content/docs/wiki/archive/discord-source-map.mdx');
  for(const text of ['6,060','890','no missing IDs or changed','67 attachments','not another incremental'])assert.ok(source.includes(text),text);
  assert.match(source,/final preserved post is dated \*\*4 October UTC\*\*/);
  assert.match(source,/5 October Discord departure/);
  assert.match(source,/remaining 66 new attachments were not reviewed/);
  assert.doesNotMatch(source,/cdn\.discordapp\.com|media\.discordapp\.net/);
});

test('last-image presentation uses the local image and preserves the export boundary',async()=>{
  const home=await read('src/pages/index.astro');
  const article=await read('src/content/docs/wiki/events/2026-return-and-car-camping.mdx');
  for(const source of [home,article])assert.ok(source.includes('/images/wiki/discord-last-image-2026-10-04.jpg'));
  assert.match(home,/Later text posts followed it/);
  assert.match(article,/15:33:10\.578 UTC/);
  assert.match(article,/not captioned as a farewell/);
  assert.doesNotMatch(article,/cdn\.discordapp\.com|media\.discordapp\.net/);
});
