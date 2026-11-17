import assert from 'node:assert/strict';
import test from 'node:test';
import { matchedAlias, matchesAll, searchText, searchWords } from '../../src/lib/corpus/title-search.mjs';
import { filterRecordings } from '../../src/lib/corpus/browse.mjs';

test('title search: words, searched text and matching', () => {
  assert.deepEqual(searchWords('  Japan   TRIP '), ['japan', 'trip']); assert.deepEqual(searchWords(''), []); assert.deepEqual(searchWords(null), []);
  const text = searchText('A Trip to Japan', ['Old Diary', null], undefined, 'personal_vlog');
  assert.equal(text, 'a trip to japan\nold diary\npersonal_vlog');
  assert.ok(matchesAll(text, ['japan', 'trip'])); assert.ok(matchesAll(text, ['diary', 'japan'])); assert.ok(!matchesAll(text, ['japan', 'winter'])); assert.ok(matchesAll(text, []));
  assert.ok(!matchesAll(searchText('ab', 'cd'), ['bc']), 'a word does not match across two fields');
});

test('title search: the earlier title that explains a match', () => {
  assert.equal(matchedAlias('Japan again', ['My old trip diary'], ['japan', 'trip']), 'My old trip diary');
  assert.equal(matchedAlias('A trip to Japan', ['My old trip diary'], ['japan', 'trip']), undefined);
  assert.equal(matchedAlias('Japan again', undefined, ['trip']), undefined); assert.equal(matchedAlias('Japan again', ['Diary'], []), undefined);
});

test('title search: the browse page uses the same rule', () => {
  const rows = [{ title: 'A trip to Japan', href: '/a', date: '2020-01-01', year: 2020 }, { title: 'Japan again', href: '/b', date: '2021-01-01', year: 2021, aliases: ['My old trip diary'] }, { title: 'Thailand', href: '/c', date: '2022-01-01', year: 2022 }];
  assert.deepEqual(filterRecordings(rows, { q: 'trip japan', sort: 'oldest' }).map(r => r.href), ['/a', '/b']);
  assert.deepEqual(filterRecordings(rows, { q: 'diary' }).map(r => r.href), ['/b']);
});
