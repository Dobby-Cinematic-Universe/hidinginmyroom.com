import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {knownSpeakerLabel,showSpeakerLabels} from '../../src/lib/corpus/presentation.mjs';
const segments=(...labels)=>labels.map(speaker_label=>({speaker_label}));

test('unknown speaker placeholders are hidden',()=>{
  for(const value of [null,undefined,'','  ','Unknown speaker','unknown_speaker','Unknown participant','Unknown speaker (uncertain)','Uncertain participant','SPEAKER_0000','SPEAKER_0001','Speaker 2','SPEAKER_0000 (uncertain)']) assert.equal(knownSpeakerLabel(value),null);
  assert.equal(knownSpeakerLabel(' Daniel '),'Daniel');
});
test('zero and single speaker transcripts have no speaker labels',()=>{
  for(const labels of [[],[null],['Daniel'],['Daniel','Daniel',null,'Unknown speaker'],['Daniel','Daniel (uncertain)'],['Daniel','Background noise','Playback / game audio']]) assert.equal(showSpeakerLabels(segments(...labels)),false);
});
test('multiple named speakers retain labels, raw anonymous placeholders do not',()=>{
  assert.equal(showSpeakerLabels(segments('Daniel','Mila',null)),true);
  assert.equal(showSpeakerLabels(segments('SPEAKER_0000','SPEAKER_0001')),false);
});
test('confidence UI and unknown fallbacks are removed',async()=>{
  const recording=await readFile(new URL('../../src/pages/corpus/videos/[id].astro',import.meta.url),'utf8');
  const search=await readFile(new URL('../../src/components/corpus/CorpusSearch.astro',import.meta.url),'utf8');
  for(const text of [recording,search]) {
    assert.ok(!text.includes('calibrated_probability'));
    assert.ok(!text.includes('confidence_band'));
    assert.ok(!text.includes('Unknown speaker'));
  }
  assert.ok(!search.includes('name="confidence"'));
});

test('corpus home release metadata stays top-right in the hero with a stacked narrow layout',async()=>{
  const home=await readFile(new URL('../../src/pages/corpus/index.astro',import.meta.url),'utf8');
  const heroEnd=home.indexOf('</section>');
  const detailsStart=home.indexOf('<details class="corpus-release-details">');
  const detailsEnd=home.indexOf('</details>',detailsStart);
  const searchStart=home.indexOf('<CorpusSearch');
  assert.ok(detailsStart<detailsEnd && detailsEnd<heroEnd && heroEnd<searchStart);
  assert.equal((home.match(/<summary>Dataset release details<\/summary>/g)||[]).length,1);
  assert.ok(!home.slice(detailsStart,detailsEnd).includes(' open'));
  for(const field of ['catalog.releaseId','catalog.generatedAt','catalog.schemaVersion']) {
    assert.ok(home.slice(detailsStart,detailsEnd).includes(field));
  }
  assert.match(home,/\.corpus-release-details summary\s*\{[^}]*max-width: 100%/);
  assert.match(home,/\.corpus-release-details summary\s*\{[^}]*margin-inline-start: auto/);
  assert.match(home,/\.corpus-release-details\s*\{[^}]*margin: 2rem 0 0/);
  assert.match(home,/\.corpus-hero\s*\{[^}]*align-items: start/);
  assert.match(home,/@media \(max-width: 52rem\)[\s\S]*\.corpus-hero\s*\{[^}]*grid-template-columns: 1fr/);
  assert.match(home,/\.corpus-release dd\s*\{[^}]*overflow-wrap: anywhere/);
});

test('browse controls share a compact touch height and summaries expand to full width',async()=>{
  const browser=await readFile(new URL('../../src/components/corpus/RecordingBrowser.astro',import.meta.url),'utf8');
  const summary=await readFile(new URL('../../src/components/summaries/InlineSummary.astro',import.meta.url),'utf8');
  assert.match(browser,/input,select,button\{[^}]*box-sizing:border-box;[^}]*font-size:\.85rem;[^}]*height:2\.75rem/);
  assert.match(browser,/nav:not\(\[hidden\]\)\{[^}]*flex-wrap:wrap/);
  assert.match(summary,/\.inline-summary\s*\{[^}]*width: fit-content;[^}]*max-width: 100%/);
  assert.match(summary,/\.inline-summary\[open\]\s*\{[^}]*width: auto/);
  assert.match(summary,/\.inline-summary > summary\s*\{[^}]*min-height: 44px/);
  assert.ok(summary.includes('<summary>Read summary</summary>'));
  assert.ok(summary.includes('data-summary-body'));
  assert.ok(summary.includes('summary-caution'));
});
