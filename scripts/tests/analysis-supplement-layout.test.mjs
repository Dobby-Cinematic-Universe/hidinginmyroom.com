import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const component = name => readFile(new URL(`../../src/components/corpus/${name}.astro`, import.meta.url), 'utf8');

test('supplement control rows keep wrapped captions above aligned, width-safe inputs', async () => {
  for (const [name, controlClass] of [
    ['AnalysisDiscovery', 'discovery-controls'],
    ['AnalysisPassageInsights', 'insights-controls'],
    ['AnalysisTemporalComparisons', 'temporal-controls'],
  ]) {
    const source = await component(name);
    const rule = source.match(new RegExp(`\\.${controlClass}\\s*\\{([^}]+)\\}`))?.[1];
    assert.ok(rule, `${name}: dedicated control-row style is present`);
    assert.match(rule, /display:grid/, `${name}: captions and controls use aligned columns`);
    assert.match(rule, /grid-template-columns:repeat\(auto-fit,minmax\(min\(100%,15rem\),1fr\)\)/, `${name}: columns can shrink to the container width`);
    assert.match(rule, /align-items:end/, `${name}: controls align after caption wrapping`);
    assert.match(source, /label\s*\{display:grid;[^}]*min-width:0/, `${name}: labels can shrink without an inline caption displacing the control`);
  }
});

test('temporal cohort controls do not inherit the main recording-form layout', async () => {
  const source = await component('AnalysisTemporalComparisons');
  assert.match(source, /class="temporal-controls"/);
  assert.doesNotMatch(source, /class="controls"|(?:^|[}\s])\.controls\s*\{/);
});
