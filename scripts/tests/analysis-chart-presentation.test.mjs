import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

test('chart frame contains narrow-screen overflow, supports keyboard scrolling and a reversible fit option', async () => {
  const source = await readFile(new URL('../../src/components/corpus/AnalysisChartFrame.astro', import.meta.url), 'utf8');
  assert.match(source, /class="analysis-chart-frame" role="region" aria-label=\{label\} tabindex="0"/);
  assert.match(source, /overflow-x:auto/);
  assert.match(source, /min-width:var\(--analysis-chart-min\)/);
  assert.match(source, /\.is-fit \.analysis-chart-frame svg\{min-width:0\}/);
  assert.match(source, /classList\.toggle\('is-fit'\)/);
  assert.match(source, /setAttribute\('aria-pressed', String\(fit\)\)/);
  assert.match(source, /:has\(svg\[hidden\]\)\{display:none\}/);
});

test('all broad chart sections use readable-size frames without replacing existing data targets', async () => {
  const cases = [
    ['AnalysisExplorer.astro', ['data-trend', 'data-mds']],
    ['AnalysisDiscovery.astro', ['data-discovery-map']],
    ['AnalysisPassageInsights.astro', ['data-insights-plot']],
    ['AnalysisTemporalComparisons.astro', ['data-trend', 'data-distribution', 'data-change-chart']],
    ['AnalysisFactorDiagnostics.astro', ['data-held-out-chart']],
  ];
  for (const [file, attributes] of cases) {
    const source = await readFile(new URL(`../../src/components/corpus/${file}`, import.meta.url), 'utf8');
    for (const attribute of attributes) assert.match(source, new RegExp(`<AnalysisChartFrame[^>]*><(?:div|svg)[^>]* ${attribute}(?:[ >])`), `${file}: ${attribute}`);
  }
});
