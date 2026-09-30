import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { factorDisplayLabel, factorLoadingSignature, resolveFactorDescriptions } from '../../src/lib/analysis/factor-descriptions.mjs';

function fixture() {
  const factors = [
    { id: 'F1', label: 'F1', loadings: { joy: 0.8, sadness: -0.5 } },
    { id: 'F2', label: 'F2', loadings: { joy: -0.2, sadness: 0.6 } },
  ];
  const identity = { corpus_release_id: 'corpus-a', questionnaire_version: 'questions-a', model: 'model-a', generated_at: '2026-09-30T00:00:00Z' };
  const note = { label: 'Positive tone', description: 'Tentative text pattern.', higher: 'More positive cues.', lower: 'Fewer positive cues.' };
  return {
    release: { ...identity, analysis: { factors } },
    annotations: { ...identity, schema_version: 1, loadings_sha256: createHash('sha256').update(factorLoadingSignature(factors)).digest('hex'), descriptions: { F1: note } },
  };
}

test('loading signature is independent of factor and item ordering', () => {
  const { release } = fixture();
  const reordered = release.analysis.factors.toReversed().map(factor => ({ ...factor, loadings: Object.fromEntries(Object.entries(factor.loadings).reverse()) }));
  assert.equal(factorLoadingSignature(reordered), factorLoadingSignature(release.analysis.factors));
});

test('matching fitted solution gets provisional descriptions and preserves factor IDs', async () => {
  const { release, annotations } = fixture();
  const resolved = await resolveFactorDescriptions(release, annotations);
  assert.deepEqual(resolved, annotations.descriptions);
  assert.equal(factorDisplayLabel(release.analysis.factors[0], resolved), 'F1 · Positive tone (provisional)');
  assert.equal(factorDisplayLabel(release.analysis.factors[1], resolved), 'F2');
  assert.equal(factorDisplayLabel({ id: 'F3', label: 'Generic label' }, resolved), 'F3: Generic label');
});

test('descriptions fail closed for any release identity or schema change', async () => {
  for (const key of ['corpus_release_id', 'questionnaire_version', 'model', 'generated_at', 'schema_version']) {
    const { release, annotations } = fixture();
    annotations[key] = 'changed';
    assert.deepEqual(await resolveFactorDescriptions(release, annotations), {}, key);
  }
  assert.deepEqual(await resolveFactorDescriptions(fixture().release, null), {});
});

test('refits, sign reversals and factor-set changes cannot inherit old descriptions', async () => {
  const mutations = [
    factors => { factors[0].loadings.joy = 0.81; },
    factors => { factors[0].loadings.joy *= -1; },
    factors => { factors.push({ id: 'F3', label: 'F3', loadings: { joy: 0.1 } }); },
    factors => { factors.pop(); },
  ];
  for (const mutate of mutations) {
    const { release, annotations } = fixture();
    mutate(release.analysis.factors);
    assert.deepEqual(await resolveFactorDescriptions(release, annotations), {});
  }
});

test('unknown factors and incomplete annotations are excluded', async () => {
  const { release, annotations } = fixture();
  annotations.descriptions.F2 = { ...annotations.descriptions.F1, higher: ' ' };
  annotations.descriptions.F99 = annotations.descriptions.F1;
  assert.deepEqual(Object.keys(await resolveFactorDescriptions(release, annotations)), ['F1']);
});

test('authored annotations cover nine factors with explicit saved-orientation caveats', async () => {
  const annotations = JSON.parse(await readFile(new URL('../../src/data/analysis/factor-descriptions.json', import.meta.url), 'utf8'));
  assert.deepEqual(Object.keys(annotations.descriptions), Array.from({ length: 9 }, (_, index) => `F${index + 1}`));
  assert.match(annotations.descriptions.F5.higher, /^Less expressed care/);
  assert.match(annotations.descriptions.F5.lower, /^More expressed care/);
  assert.match(annotations.descriptions.F6.higher, /^Calmer/);
  assert.match(annotations.descriptions.F9.higher, /^Less rebuttal/);
  assert.match(annotations.descriptions.F7.description, /not a diagnosis/);
  assert.match(annotations.descriptions.F9.description, /not additional live speakers/);
});
