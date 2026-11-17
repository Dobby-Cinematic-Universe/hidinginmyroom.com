import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const data = JSON.parse(read('src/lib/analysis/validation-studies.json'));
const verdict = (reliability, best, rule) =>
  reliability < rule.unclear_below ? 'unclear rubric'
  : reliability >= rule.reliability && best >= rule.correlation ? 'usable'
  : reliability >= rule.reliability && best < rule.misread_below ? 'misread'
  : 'borderline';

test('human rating studies: verdicts follow the stated rule', () => {
  assert.equal(data.dimensions.length, 8);
  assert.equal(data.first_study_questions.length, 12);
  for (const row of [...data.dimensions, ...data.first_study_questions]) {
    assert.equal(row.verdict, verdict(row.reliability, Math.max(row.jev, row.drex), data.rule), row.label ?? row.name);
    for (const key of ['reliability', 'jev', 'drex']) {
      const [low, high] = row[`${key}_interval`];
      assert.ok(low <= row[key] && row[key] <= high, `${row.label ?? row.name}: ${key} lies in its interval`);
    }
    if (row.settled && row.verdict === 'usable') assert.ok(row.reliability_interval[0] >= data.rule.reliability);
  }
});

test('human rating studies: the counts stated on other pages match the data', () => {
  const supported = data.dimensions.filter((d) => d.verdict === 'usable' && d.settled).length;
  const unclear = data.dimensions.filter((d) => d.verdict === 'unclear rubric').length;
  const misread = data.dimensions.filter((d) => d.verdict === 'misread').length;
  assert.deepEqual([supported, data.dimensions.length - supported - unclear - misread, unclear, misread], [4, 3, 1, 0]);
  const statement = 'support four of the eight dimensions, leave three at the line';
  assert.ok(read('src/pages/corpus/analysis/methods.astro').includes(statement));
  assert.ok(read('src/components/corpus/AnalysisExplorer.astro').includes(statement));
  assert.equal(data.studies.first.excerpts + data.studies.second.excerpts, 320);
  assert.equal(data.studies.second.questions, 16);
});

test('human rating studies: the data file holds summaries only', () => {
  const text = read('src/lib/analysis/validation-studies.json');
  assert.doesNotMatch(text, /rater_|participant|[A-Fa-f0-9]{24,}/);
});
