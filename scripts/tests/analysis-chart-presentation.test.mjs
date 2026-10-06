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
    ['AnalysisExplorer.astro', ['data-trend']],
    ['AnalysisDiscovery.astro', ['data-discovery-map']],
    ['AnalysisPassageInsights.astro', ['data-insights-plot']],
    ['AnalysisTemporalComparisons.astro', ['data-trend', 'data-distribution', 'data-change-chart']],
    ['AnalysisFactorDiagnostics.astro', ['data-held-out-chart']],
  ];
  for (const [file, attributes] of cases) {
    const source = await readFile(new URL(`../../src/components/corpus/${file}`, import.meta.url), 'utf8');
    for (const attribute of attributes) assert.match(source, new RegExp(`<AnalysisChartFrame[^>]*><(?:div|svg)[^>]* ${attribute}(?:[ >])`), `${file}: ${attribute}`);
  }
  const explorer = await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro', import.meta.url), 'utf8');
  assert.match(explorer, /class="question-map-frame"><svg data-mds viewBox="0 0 760 600" role="group" tabindex="0"/);
  assert.match(explorer, /aria-describedby="question-map-help"/);
  assert.match(explorer, /\.question-map-frame svg\[data-mds\]\{overflow:hidden;max-height:none/);
  assert.match(explorer, /attachMapViewport\(\{svg,layer:cameraLayer/);
  assert.match(explorer, /data-map-action="fit"/);
  assert.match(explorer, /data-map-action="reset"/);
});
