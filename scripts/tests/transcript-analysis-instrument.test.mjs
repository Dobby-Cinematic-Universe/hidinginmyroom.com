import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

const bank = JSON.parse(await readFile(new URL('../../src/data/analysis/questions.json', import.meta.url), 'utf8'));

test('question bank is versioned and all 78 items are in the single runner-facing array', () => {
  assert.match(bank.version, /^himr-transcript-analysis-questions-v\d+$/);
  assert.equal(bank.questions.length, 78);
  assert.equal('shared_applicability' in bank, false);
  assert.deepEqual(Object.fromEntries([...new Set(bank.questions.map(q => q.domain))].map(domain => [domain, bank.questions.filter(q => q.domain === domain).length])), {
    applicability: 5,
    expressed_style: 18,
    expressed_affect: 15,
    rhetoric_social: 20,
    hypothetical_reception: 11,
    classification: 9
  });
  assert.ok(bank.references.length >= 15);
});

test('every item has a unique identifier, short name, standalone rubric, and explicit factor eligibility', () => {
  const ids = bank.questions.map(q => q.id);
  assert.equal(new Set(ids).size, ids.length);
  const byId = new Map(bank.questions.map(q => [q.id, q]));
  for (const item of bank.questions) {
    assert.ok(item.short_name && item.domain && item.instructions);
    assert.equal(typeof item.factor_eligible, 'boolean');
    if (item.type === 'score') assert.equal(item.criteria.length, 5, item.id);
    if (item.type === 'choice') assert.ok(Object.keys(item.criteria).length >= 2, item.id);
    if (item.type === 'noul') assert.deepEqual(Object.keys(item.criteria).sort(), ['false', 'true']);
    if (item.applicability) assert.equal(byId.get(item.applicability)?.domain, 'applicability', item.id);
  }
  for (const id of ['can_rate_style', 'can_rate_affect', 'can_rate_reception', 'has_quoted_or_reported_voice', 'has_asr_uncertainty']) assert.ok(byId.has(id), id);
});

test('factor-analysis eligibility excludes predictions and classifications', () => {
  for (const q of bank.questions) {
    if (['hypothetical_reception', 'classification'].includes(q.domain)) assert.equal(q.factor_eligible, false, q.id);
    if (q.factor_eligible) assert.equal(q.type, 'score', q.id);
  }
  assert.deepEqual(bank.factor_analysis.eligible_domains, ['expressed_style', 'expressed_affect', 'rhetoric_social']);
  assert.deepEqual(bank.factor_analysis.exclude_domains, ['hypothetical_reception', 'classification', 'applicability']);
});

test('shared instructions constrain scope and text-only classifiers', () => {
  const instructions = bank.instructions.join(' ').toLowerCase();
  assert.match(bank.scope, /all voices/i);
  assert.match(instructions, /not.*truth|truth.*not/i);
  assert.match(instructions, /text-to-speech/i);
  assert.match(bank.questions.find(q => q.id === 'class_tts_evidence').instructions, /text-only/i);
  assert.match(bank.questions.find(q => q.id === 'class_multiple_speakers').instructions, /speaker labels/i);
  assert.match(bank.scope, /as one unit/i);
  assert.match(bank.scope, /do not assign enduring traits to Daniel or any named person/i);
});

test('reception predictions do not assume video context or creator familiarity', () => {
  const q = bank.questions.find(x => x.id === 'reception_interest');
  assert.match(q.instructions, /seeing only this transcript excerpt/i);
  assert.match(q.instructions, /no prior knowledge of the creator/i);
  assert.doesNotMatch(q.instructions, /original video context/i);
});

test('text classifiers explicitly preserve uncertainty and avoid audio-level claims', () => {
  const tts = bank.questions.find(x => x.id === 'class_tts_evidence');
  const voices = bank.questions.find(x => x.id === 'class_multiple_speakers');
  assert.match(tts.instructions, /text-only evidence/i);
  assert.ok(tts.criteria.unknown);
  assert.ok(tts.criteria.no_text_evidence);
  assert.match(voices.instructions, /do not infer acoustic speakers/i);
  assert.ok(voices.criteria.uncertain);
  assert.ok(voices.criteria.unknown);
});
