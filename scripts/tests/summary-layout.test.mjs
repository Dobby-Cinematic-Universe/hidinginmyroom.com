import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = (path) => readFile(new URL(`../../${path}`, import.meta.url), 'utf8');

test('summary filters have consistent controls and wrapping pagination', async () => {
  const page = await source('src/pages/corpus/summaries/index.astro');
  assert.match(page, /input, select, button\s*\{[^}]*height: 2\.75rem/);
  assert.match(page, /input, select, button\s*\{[^}]*max-width: 100%/);
  assert.match(page, /\.summary-pagination:not\(\[hidden\]\)\s*\{[^}]*flex-wrap: wrap/);
  assert.match(page, /<button type="reset">Reset<\/button>/);
});

test('static summary pagination has usable controls and explicit page position', async () => {
  const page = await source('src/pages/corpus/summaries/browse/[page].astro');
  assert.match(page, /class="library-pagination" aria-label="Library pages"/);
  assert.match(page, /rel="prev"/);
  assert.match(page, /rel="next"/);
  assert.match(page, /<strong>Page \{page\} of \{pages\}<\/strong>/);
  assert.match(page, /min-height: 2\.75rem/);
  assert.match(page, /@media \(max-width: 30rem\)/);
});

test('summary readers wrap long titles, source lists and release identities', async () => {
  const page = await source('src/pages/corpus/summaries/[id].astro');
  for (const selector of ['h1', 'nav[aria-label="Breadcrumb"]', 'details ul', '.summary-release']) {
    const position = page.lastIndexOf(selector);
    assert.notEqual(position, -1);
    const rule = page.slice(position, page.indexOf('}', position));
    assert.ok(rule.includes('overflow-wrap: anywhere'), `${selector} must wrap long values`);
  }
});
