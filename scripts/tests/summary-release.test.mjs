import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { validateSummaryRelease, filterSummaries, periodLabel } from '../../src/lib/summaries/schema.mjs';

function fixture() {
  return { schema_version:1, release_id:'summaries_test', generated_at:'2026-09-17T00:00:00Z', summaries:[{
    id:'year-2020',kind:'yearly',period:'2020',title:'2020 in review',recording_id:null,publication:'approved',
    sections:{summary:[{text:'A fictional move was discussed.',classification:'reported_statement',source_recording_ids:['rec_test']}],topics:[],events:[],uncertainties:[]},
  }] };
}
const released = new Set(['rec_test']);

test('prepared summaries require an explicit local-preview option', () => {
  const value=fixture();value.summaries[0].publication='prepared';
  assert.throws(() => validateSummaryRelease(value,released));
  assert.equal(validateSummaryRelease(value,released,{allowPrepared:true}).summaries.length,1);
});

test('accepts approved summary and empty initial release', async () => {
  assert.equal(validateSummaryRelease(fixture(),released).summaries.length,1);
  const empty=JSON.parse(await readFile(new URL('../../src/data/summaries/release.json',import.meta.url),'utf8'));
  assert.equal(validateSummaryRelease(empty).summaries.length,0);
});
test('rejects private fields, unapproved output and unresolved sources', () => {
  for (const mutate of [
    (v) => { v.summaries[0].publication='pending'; },
    (v) => { v.summaries[0].canonical={path:'private-file'}; },
    (v) => { v.summaries[0].sections.summary[0].evidence={private:'internal'}; },
    (v) => { v.summaries[0].sections.summary[0].source_recording_ids=['missing']; },
    (v) => { v.summaries[0].sections.summary[0].source_recording_ids=[]; },
    (v) => { v.summaries[0].sections.summary=[]; },
    (v) => { v.summaries[0].id='../private'; },
    (v) => { v.summaries.push(structuredClone(v.summaries[0])); },
    (v) => { const row=structuredClone(v.summaries[0]);row.id='another';v.summaries.push(row); },
    (v) => { v.summaries[0].period='2020-13'; },
    (v) => { v.generated_at='2026-02-30T00:00:00Z'; },
  ]) { const value=fixture();mutate(value);assert.throws(() => validateSummaryRelease(value,released)); }
});
test('transcript summaries cannot cite a different recording', () => {
  const value=fixture(); Object.assign(value.summaries[0],{kind:'transcript',period:'2020-01',recording_id:'rec_other'});
  assert.throws(() => validateSummaryRelease(value,new Set(['rec_test','rec_other'])));
  value.summaries[0].recording_id='rec_test';
  assert.doesNotThrow(() => validateSummaryRelease(value,released));
});
test('uncertainties stay explicitly uncertain', () => {
  const value=fixture();value.summaries[0].sections.uncertainties=[structuredClone(value.summaries[0].sections.summary[0])];
  assert.throws(() => validateSummaryRelease(value,released));
  value.summaries[0].sections.uncertainties[0].classification='uncertainty';
  assert.doesNotThrow(() => validateSummaryRelease(value,released));
});
test('filter combinations and undated handling do not invent archive dates', () => {
  const cards=[{title:'Daniel travels',excerpt:'A visit to Japan',kind:'transcript',period:'2020-01'},
    {title:'Undated conversation',excerpt:'Travel plans',kind:'monthly',period:null},
    {title:'Archive overview',excerpt:'Travel over time',kind:'archive',period:null}];
  assert.equal(filterSummaries(cards,'JAPAN daniel','transcript','2020').length,1);
  assert.equal(filterSummaries(cards,'','monthly','2021').length,0);
  assert.deepEqual(filterSummaries(cards,'','','undated'),[cards[1]]);
  assert.equal(periodLabel(null),'Undated');assert.equal(periodLabel('2020-01'),'January 2020');
});
test('reading UI renders text rather than trusting model HTML', async () => {
  const page=await readFile(new URL('../../src/pages/corpus/summaries/[id].astro',import.meta.url),'utf8');
  assert.ok(page.includes('{item.text}'));assert.ok(!page.includes('set:html'));
  const library=await readFile(new URL('../../src/pages/corpus/summaries/index.astro',import.meta.url),'utf8');
  assert.ok(library.includes('p.textContent = card.excerpt'));assert.ok(!library.includes('innerHTML'));
});

test('summary readers omit repetitive uncertainty labels but retain allegation notices', async () => {
  const page=await readFile(new URL('../../src/pages/corpus/summaries/[id].astro',import.meta.url),'utf8');
  const inline=await readFile(new URL('../../src/lib/summaries/inline-summary.ts',import.meta.url),'utf8');
  for(const source of [page,inline]) {
    assert.ok(!source.includes("'Uncertain'")&&!source.includes("'Uncertain: '"));
    assert.match(source,/classification\s*===\s*'reported_allegation'/);
    assert.ok(source.includes('Reported allegation'));
    assert.ok(source.includes('Uncertainties & gaps'));
  }
});
