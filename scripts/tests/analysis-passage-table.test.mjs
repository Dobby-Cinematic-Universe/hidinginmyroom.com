import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { buildPassageFrame, findPassages, heldConstantTrend, passageYears, recordingArc, subjectProfile, subjectShares, topicContrast, topicMask, validatePassageTable } from '../../src/lib/analysis/passage-table.mjs';

const b64 = array => Buffer.from(array.buffer, array.byteOffset, array.byteLength).toString('base64');
const release = { schema_version: 1, corpus_release_id: 'release_a', questionnaire_version: 'q', model: 'm', generated_at: 'now', design: { window_words: 500 }, analysis: { factors: [{ id: 'F1' }, { id: 'F2' }] },
  videos: [{ recording_id: 'rec_' + 'a'.repeat(32), design: { kind: 'video' } }, { recording_id: 'rec_' + 'b'.repeat(32), design: { kind: 'stream' } }, { recording_id: 'rec_' + 'c'.repeat(32), design: { kind: 'clip' } }] };
// Six passages: three of a video (2016), three of a stream (2017). -128 marks a passage without a score.
const make = (change = {}) => ({ schema_version: 1, corpus_release_id: 'release_a', questionnaire_version: 'q', model: 'm', source_generated_at: 'now', loadings_sha256: 'c'.repeat(64), window_words: 500, steps_per_sd: 20, passages: 6,
  factor_ids: ['F1', 'F2'], subjects: ['games', 'family', 'unknown'], addressees: ['viewers', 'no_one'],
  recordings: [['rec_' + 'a'.repeat(32), 0, 3, 1], ['rec_' + 'b'.repeat(32), 3, 3, 1]],
  dims: b64(new Int8Array([20, 0, 40, 0, -20, 0, 0, 20, -128, -128, 60, -20])), subject: b64(new Uint8Array([0, 0, 1, 1, 255, 1])), addressee: b64(new Uint8Array([0, 0, 1, 0, 0, 1])),
  start_s: b64(new Uint32Array([0, 180, 360, 0, 200, 70000])), topics: [{ name: 'Minecraft', type: 'game', passages: [1, 3] }], ...change });
const info = [{ id: 'rec_' + 'a'.repeat(32), title: 'A', date: '2016-05-01', kind: 'video', href: '/corpus/videos/a/' }, { id: 'rec_' + 'b'.repeat(32), title: 'B', date: '2017', kind: 'stream', href: '/corpus/videos/b/' }];

test('passage table: a valid table passes and decodes', () => {
  const frame = buildPassageFrame(validatePassageTable(make(), release), info);
  assert.equal(frame.n, 6); assert.equal(frame.dim(1, 0), 2); assert.ok(Number.isNaN(frame.dim(4, 0)));
  assert.deepEqual([...frame.kind], [0, 0, 0, 1, 1, 1]); assert.deepEqual([...frame.year], [2016, 2016, 2016, 2017, 2017, 2017]); assert.equal(frame.startS[5], 70000);
});

test('passage table: tables of another release, with gaps, unknown recordings or bad columns are refused', () => {
  const bad = [{ model: 'other' }, { source_generated_at: 'earlier' }, { loadings_sha256: 'x' }, { window_words: 250 }, { factor_ids: ['F1'] }, { passages: 7 },
    { recordings: [['rec_' + 'a'.repeat(32), 0, 3, 1], ['rec_' + 'b'.repeat(32), 4, 2, 1]] }, { recordings: [['rec_' + 'a'.repeat(32), 0, 3, 1], ['rec_' + 'c'.repeat(32), 3, 3, 1]] }, { recordings: [['rec_' + 'a'.repeat(32), 0, 3, 1], ['rec_' + 'd'.repeat(32), 3, 3, 1]] },
    { recordings: [['rec_' + 'a'.repeat(32), 0, 3], ['rec_' + 'b'.repeat(32), 3, 3]] }, { recordings: [['rec_' + 'a'.repeat(32), 0, 3, 2], ['rec_' + 'b'.repeat(32), 3, 3, 1]] },
    { subject: b64(new Uint8Array([0, 0, 1, 1, 9, 1])) }, { dims: b64(new Int8Array(11)) }, { start_s: 'not base64!' }, { subjects: ['Games'] },
    { topics: [{ name: 'Minecraft', type: 'person', passages: [1] }] }, { topics: [{ name: 'Minecraft', type: 'game', passages: [3, 1] }] }, { topics: [{ name: 'Minecraft', type: 'game', passages: [6] }] }, { topics: [{ name: '<b>', type: 'game', passages: [1] }] }, { text: 'a transcript' }];
  for (const change of bad) assert.throws(() => validatePassageTable(make(change), release), /Invalid passage table/, JSON.stringify(change).slice(0, 80));
  assert.throws(() => buildPassageFrame(make(), info.slice(0, 1)), /recording details/);
});

test('passage table: the aggregations of the four views', () => {
  const frame = buildPassageFrame(make(), info), years = [2016, 2017];
  const shares = subjectShares(frame, years, 2);
  assert.deepEqual(shares[0].share, [[2 / 3, NaN], [NaN, 0]]);                                  // games: two of three video passages in 2016
  assert.deepEqual(shares[1].share[1], [NaN, 1]);                                               // family: both classified stream passages of 2017
  const profile = subjectProfile(frame, 0, 1);
  assert.ok(Math.abs(profile[0].difference[0] - (1.5 - 2 / 3)) < 1e-9 && Math.abs(profile[1].difference[0] - (-1 - 2 / 3)) < 1e-9);
  const trend = heldConstantTrend(frame, 0, 0, years, 2);
  assert.ok(Math.abs(trend[0].observed - 2 / 3) < 1e-9 && Number.isNaN(trend[1].observed) && trend[0].passages === 3);
  const arc = recordingArc(frame, 1, 0);
  assert.deepEqual(arc.values.map(v => Number.isNaN(v) ? null : v), [0, null, 3]); assert.deepEqual(arc.ranked.map(item => item.passage), [5, 3]);
  const contrast = topicContrast(frame, topicMask(frame, frame.topics[0]));
  assert.equal(contrast[0].recordings, 2);                                                      // both recordings hold passages with and without the name
  assert.ok(Math.abs(contrast[0].within - ((2 - 0) + (0 - 3)) / 2) < 1e-9);
  assert.deepEqual(findPassages(frame, { sort: 0 }), [5, 1, 0, 3, 2]);                          // passage 4 has no score
  assert.deepEqual(findPassages(frame, { kind: 0, levels: { 0: 1 }, sort: 0 }), [1, 0]);
  assert.deepEqual(findPassages(frame, { subject: 1, from: 2017, to: 2017, sort: 0, descending: false }), [3, 5]);
  assert.deepEqual(findPassages(frame, { mask: topicMask(frame, frame.topics[0]), sort: 1 }), [3, 1]);
  assert.deepEqual(passageYears(frame), [2016, 2017]);
});

test('passage table: another version of a recording can be opened and searched but is not added up', () => {
  const frame = buildPassageFrame(validatePassageTable(make({ recordings: [['rec_' + 'a'.repeat(32), 0, 3, 1], ['rec_' + 'b'.repeat(32), 3, 3, 0]] }), release), info);
  assert.deepEqual(passageYears(frame), [2016]); assert.equal(frame.recordings[1].counted, false);
  assert.deepEqual(subjectShares(frame, [2016, 2017], 1)[1].share[1], [NaN, NaN]);              // the stream's passages are not counted
  assert.equal(topicContrast(frame, topicMask(frame, frame.topics[0]))[0].recordings, 1);
  assert.deepEqual(findPassages(frame, { sort: 0 }), [1, 0, 2]); assert.deepEqual(findPassages(frame, { sort: 0, versions: true }), [5, 1, 0, 3, 2]);
  assert.deepEqual(recordingArc(frame, 1, 0).ranked.map(item => item.passage), [5, 3]);
});

test('passage table: a recording without a catalog date is placed by its estimated date and says so', () => {
  const undated = [info[0], { ...info[1], date: null, estimated: '2019-06' }], frame = buildPassageFrame(make(), undated);
  assert.deepEqual([...frame.year], [2016, 2016, 2016, 2019, 2019, 2019]); assert.equal(frame.recordings[1].dateText, 'estimated 2019-06'); assert.equal(frame.recordings[0].dateText, '2016-05-01');
  assert.deepEqual(passageYears(frame), [2016, 2017, 2018, 2019]);
  const none = buildPassageFrame(make(), [info[0], { ...info[1], date: null, estimated: null }]);
  assert.ok(Number.isNaN(none.year[3])); assert.equal(none.recordings[1].dateText, 'undated');
  assert.equal(buildPassageFrame(make(), [info[0], { ...info[1], estimated: '2019-06' }]).year[3], 2017);       // a catalog date wins
});

test('analysis bundle: the passage table is an optional member', () => {
  const code = String.raw`
import importlib.util,json,tempfile
from pathlib import Path
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('bundle','scripts/analysis-release-bundle.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
assert b.allowed('src/data/analysis/passage-table.json') and not b.allowed('src/data/analysis/passage-table.json.bak')
with tempfile.TemporaryDirectory() as folder:
    root=Path(folder);source=root/'source';source.mkdir();(source/'passages').mkdir()
    rec='rec_'+'a'*32;rev='rev_'+'b'*32
    r=dict(corpus_release_id='corpus-a',questionnaire_version='q',model='m',generated_at='now',coverage=dict(scored_recordings=1.0,scored_chunks=1.0),videos=[dict(recording_id=rec,revision_id=rev)])
    (source/'public-release.json').write_text(json.dumps(r))
    for name in b.SUPPLEMENTS:(source/name).write_text(json.dumps(dict(loadings_sha256='c'*64)))
    p=dict(corpus_release_id='corpus-a',questionnaire_version='q',model='m',source_generated_at='now',recording=dict(recording_id=rec,revision_id=rev),passages=[{}])
    (source/'passages'/(rec+'.json')).write_text(json.dumps(p))
    pin=root/'corpus.json';pin.write_text(json.dumps(dict(corpus_release='corpus-a')))
    check=type('Result',(),{'stdout':json.dumps(dict(loadings_sha256='c'*64))})()
    with patch.object(b.subprocess,'run',return_value=check):
        b.pack(source,root/'without',pin);assert b.load(root/'without/manifest.json')['files']==10
        (source/'passage-table.json').write_text(json.dumps(dict(passages=1)))
        b.pack(source,root/'with',pin);m=b.load(root/'with/manifest.json');assert m['files']==11
        dest=root/'dest';(dest/'src/data/analysis').mkdir(parents=True)
        b.restore(root/'with/manifest.json',dest,pin,root/'with/analysis.tar.gz')
        assert (dest/'src/data/analysis/passage-table.json').is_file()
        other=root/'other';(other/'src/data/analysis').mkdir(parents=True)
        b.restore(root/'without/manifest.json',other,pin,root/'without/analysis.tar.gz')
        assert not (other/'src/data/analysis/passage-table.json').exists()
print('ok')
`;
  const result = spawnSync('python3', ['-B', '-c', code], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr); assert.match(result.stdout, /ok/);
});
