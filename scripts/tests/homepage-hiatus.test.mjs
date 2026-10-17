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

test('return article follows the dated sequence and ends with the departure',async()=>{
  const article=await read('src/content/docs/wiki/events/2026-return-and-car-camping.mdx');
  const sequence=[
    '## Earlier 2026 return',
    '## The visible return sequence',
    '### Earlier July-labelled archive streams',
    '### July 25: an additional archive-labelled entry',
    '## July 30: why I quit YouTube',
    '## August 4: life update and garden ritual',
    '### Stargazing dome: title promise versus visible record',
    '### The complete August 7 vehicle tour',
    '### August 8: homeless in my car',
    '### August 10: feeling sad and lonely',
    '### August 20–23: dark car streams',
    '## August 26: a later retained and machine-indexed stream',
    '## September 4: life updates',
    '## September 10: life updates',
    '## September 13: claimed house-viewing enquiry',
    '## September 17 public livestream',
    '## September 24: dissatisfaction with the format',
    '## September 24–28: further housing plans, not a confirmed move',
    '## September 27–October 5: another departure',
    '### Last preserved image'
  ];
  let previous=-1;
  for(const heading of sequence){
    const position=article.indexOf('\n'+heading+'\n');
    assert.ok(position>previous,'Missing or out-of-order section: '+heading);
    previous=position;
  }
  const sections=article.match(/^## .+$/gm);
  assert.equal(sections.at(-1),'## September 27–October 5: another departure');
});
