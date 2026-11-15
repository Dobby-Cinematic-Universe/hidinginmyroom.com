import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';

const bank = JSON.parse(await readFile(new URL('../../src/data/analysis/questions.json', import.meta.url), 'utf8'));

test('question bank is version 2 and all 60 items are in the single runner-facing array', () => {
  assert.equal(bank.version, 'himr-transcript-analysis-questions-v2');
  assert.equal(bank.questions.length, 60);
  assert.equal('shared_applicability' in bank, false);
  assert.deepEqual(Object.fromEntries([...new Set(bank.questions.map(q => q.domain))].map(domain => [domain, bank.questions.filter(q => q.domain === domain).length])), {
    applicability: 1,
    content: 2,
    expressed_style: 18,
    expressed_affect: 10,
    rhetoric_social: 17,
    hypothetical_reception: 3,
    classification: 9
  });
  assert.ok(bank.references.length >= 15);
});

test('every item has a unique identifier, a readable name, a standalone rubric, and explicit factor eligibility', () => {
  const ids = bank.questions.map(q => q.id);
  assert.equal(new Set(ids).size, ids.length);
  const byId = new Map(bank.questions.map(q => [q.id, q]));
  for (const item of bank.questions) {
    assert.ok(item.short_name && item.domain && item.instructions);
    assert.match(item.short_name, /^[A-Z][A-Za-z ()-]{2,31}$/, item.id);
    assert.equal(typeof item.factor_eligible, 'boolean');
    if (item.type === 'score') { assert.equal(item.criteria.length, 5, item.id); assert.equal(item.applicability, 'can_rate', item.id); }
    if (item.type === 'choice') assert.ok(Object.keys(item.criteria).length >= 2, item.id);
    if (item.type === 'noul') assert.deepEqual(Object.keys(item.criteria).sort(), ['false', 'true']);
    if (item.applicability) assert.equal(byId.get(item.applicability)?.domain, 'applicability', item.id);
  }
  assert.equal(new Set(bank.questions.map(q => q.short_name)).size, bank.questions.length);
});

test('the bank records what changed against version 1', () => {
  const byId = new Map(bank.questions.map(q => [q.id, q]));
  assert.equal(bank.derived_from.version, 'himr-transcript-analysis-questions-v1');
  for (const id of Object.keys(bank.derived_from.dropped)) assert.equal(byId.has(id), false, id);
  for (const id of bank.derived_from.added) assert.ok(byId.has(id), id);
  assert.equal(78 - Object.keys(bank.derived_from.dropped).length + bank.derived_from.added.length, bank.questions.length);
  for (const id of ['class_subject', 'class_addressee', 'reads_messages_aloud', 'complaint_named_service', 'style_scripted', 'class_transcript_quality']) assert.ok(byId.has(id), id);
});

test('factor-analysis eligibility excludes predictions, classifications and content flags', () => {
  for (const q of bank.questions) {
    if (['hypothetical_reception', 'classification', 'content', 'applicability'].includes(q.domain)) assert.equal(q.factor_eligible, false, q.id);
    if (q.factor_eligible) assert.equal(q.type, 'score', q.id);
  }
  assert.equal(bank.questions.filter(q => q.factor_eligible).length, 45);
  assert.deepEqual(bank.factor_analysis.eligible_domains, ['expressed_style', 'expressed_affect', 'rhetoric_social']);
  assert.deepEqual(bank.factor_analysis.exclude_domains, ['hypothetical_reception', 'classification', 'applicability', 'content']);
});

test('shared instructions constrain scope: wording of one fixed-length excerpt, not a person', () => {
  const instructions = bank.instructions.join(' ').toLowerCase();
  assert.match(bank.scope, /all voices/i);
  assert.match(instructions, /not.*truth|truth.*not/i);
  assert.match(bank.scope, /as one unit/i);
  assert.match(bank.scope, /do not assign enduring traits to Daniel or any named person/i);
  assert.match(bank.scope, /exactly 500 words/i);
  assert.match(bank.scope, /without speaker labels/i);
});

test('reception estimates are few and do not claim measured engagement', () => {
  const reception = bank.questions.filter(q => q.domain === 'hypothetical_reception');
  assert.deepEqual(reception.map(q => q.id).sort(), ['reception_comment', 'reception_disagreement', 'reception_polarized']);
  assert.match(bank.questions.find(q => q.id === 'reception_comment').instructions, /not measured engagement/i);
});

test('text classifiers rest on wording only and keep an unknown option', () => {
  const voices = bank.questions.find(x => x.id === 'class_multiple_speakers');
  assert.match(voices.instructions, /only on the wording/i);
  assert.doesNotMatch(voices.instructions, /speaker labels/i);
  assert.ok(voices.criteria.uncertain);
  assert.ok(voices.criteria.unknown);
  for (const q of bank.questions.filter(x => x.type === 'choice')) assert.ok(q.criteria.unknown, q.id);
  const subject = bank.questions.find(x => x.id === 'class_subject');
  assert.ok(Object.keys(subject.criteria).length >= 10);
  assert.doesNotMatch(JSON.stringify(subject.criteria), /health condition|diagnos/i);
});
