import { matchesAnalysisIdentity } from './supplement.mjs';

// The passage table is an optional member of the analysis release: one row per full passage of every video and
// stream, with dimension scores, two classifications and a start time, and the passages that name each topic. A
// recording row is [id, first passage, passages, 1 if it is the version the analysis counts, else 0]: the views
// that add passages up count one version of each recording. It holds numbers, identifiers of the public corpus and
// topic names, never transcript text.
export const PASSAGE_TABLE_FILE = 'passage-table.json';
export const TOPIC_TYPES = ['place', 'platform', 'product', 'game'];
const ROOT = ['schema_version', 'corpus_release_id', 'questionnaire_version', 'model', 'source_generated_at', 'loadings_sha256', 'window_words', 'steps_per_sd', 'passages', 'factor_ids', 'subjects', 'addressees', 'recordings', 'dims', 'subject', 'addressee', 'start_s', 'topics'];
const MISSING = -128, NONE = 255;
const assert = (condition, field) => { if (!condition) throw new Error(`Invalid passage table: ${field}`); };
const integer = (value, low, high) => Number.isInteger(value) && value >= low && value <= high;
const options = value => Array.isArray(value) && value.length > 0 && value.length <= 40 && new Set(value).size === value.length && value.every(item => typeof item === 'string' && /^[a-z][a-z_]{0,59}$/.test(item));

function bytes(text, field) {
  assert(typeof text === 'string' && text.length <= 12_000_000 && /^[A-Za-z0-9+/]*={0,2}$/.test(text), `${field} encoding`);
  if (typeof Buffer !== 'undefined') return new Uint8Array(Buffer.from(text, 'base64'));
  const raw = atob(text), out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
  return out;
}

/** Typed arrays of the table. Throws if a field has the wrong length. */
export function decodePassageTable(value) {
  const n = value.passages, nd = value.factor_ids.length, raw = bytes(value.dims, 'dims');
  assert(raw.length === n * nd, 'dims length');
  const subject = bytes(value.subject, 'subject'), addressee = bytes(value.addressee, 'addressee'), start = bytes(value.start_s, 'start_s');
  assert(subject.length === n && addressee.length === n && start.length === 4 * n, 'column length');
  const view = new DataView(start.buffer, start.byteOffset, start.byteLength), startS = new Uint32Array(n);
  for (let i = 0; i < n; i++) startS[i] = view.getUint32(4 * i, true);
  return { n, nd, dims: new Int8Array(raw.buffer, raw.byteOffset, raw.length), subject, addressee, startS };
}

export function validatePassageTable(value, release) {
  assert(value !== null && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === ROOT.length && ROOT.every(key => key in value), 'root fields');
  assert(matchesAnalysisIdentity(value, release), 'release identity');
  assert(typeof value.loadings_sha256 === 'string' && /^[a-f0-9]{64}$/.test(value.loadings_sha256), 'factor solution');
  assert(integer(value.window_words, 1, 100000) && (!release.design || value.window_words === release.design.window_words), 'passage length');
  assert(integer(value.steps_per_sd, 1, 50) && integer(value.passages, 1, 500000), 'scale and size');
  const factors = release.analysis.factors.map(factor => factor.id);
  assert(Array.isArray(value.factor_ids) && value.factor_ids.length === factors.length && factors.length > 0 && value.factor_ids.every((id, k) => id === factors[k]), 'dimensions');
  assert(options(value.subjects) && options(value.addressees), 'classification options');
  const videos = new Map(release.videos.map(video => [video.recording_id, video])), seen = new Set();
  assert(Array.isArray(value.recordings) && value.recordings.length > 0 && value.recordings.length <= videos.size, 'recording count');
  let next = 0;
  for (const row of value.recordings) {
    assert(Array.isArray(row) && row.length === 4 && typeof row[0] === 'string' && integer(row[1], 0, value.passages) && integer(row[2], 1, 2000) && (row[3] === 0 || row[3] === 1), 'recording row');
    const video = videos.get(row[0]);
    assert(video && !seen.has(row[0]) && (video.design?.kind === 'video' || video.design?.kind === 'stream'), 'recording identity');
    assert(row[1] === next, 'recording order'); next += row[2]; seen.add(row[0]);
  }
  assert(next === value.passages, 'passage count');
  const table = decodePassageTable(value);
  for (let i = 0; i < table.n; i++) assert((table.subject[i] < value.subjects.length || table.subject[i] === NONE) && (table.addressee[i] < value.addressees.length || table.addressee[i] === NONE), 'classification value');
  assert(Array.isArray(value.topics) && value.topics.length <= 500, 'topics');
  const names = new Set();
  for (const topic of value.topics) {
    assert(topic !== null && typeof topic === 'object' && Object.keys(topic).length === 3 && typeof topic.name === 'string' && /^[A-Za-z0-9][A-Za-z0-9' .-]{0,59}$/.test(topic.name) && !names.has(topic.name) && TOPIC_TYPES.includes(topic.type), 'topic fields');
    names.add(topic.name);
    assert(Array.isArray(topic.passages) && topic.passages.length > 0 && topic.passages.every((i, k) => integer(i, 0, table.n - 1) && (k === 0 || i > topic.passages[k - 1])), 'topic passages');
  }
  return value;
}

/** The table joined with what the release says of each recording: one frame for every view. */
export function buildPassageFrame(value, recordings) {
  const table = decodePassageTable(value), n = table.n, rec = new Uint32Array(n), kind = new Uint8Array(n), year = new Float32Array(n), counted = new Uint8Array(n);
  if (!Array.isArray(recordings) || recordings.length !== value.recordings.length) throw new Error('Invalid passage table: recording details');
  const rows = value.recordings.map(([id, start, count, primary], r) => {
    const info = recordings[r];
    if (!info || info.id !== id) throw new Error('Invalid passage table: recording details');
    // A recording without a catalog date is placed by its estimated date, and says so wherever its date is shown.
    const estimated = !info.date && typeof info.estimated === 'string' && /^\d{4}/.test(info.estimated) ? info.estimated : null, dated = info.date ?? estimated;
    const y = typeof dated === 'string' && /^\d{4}/.test(dated) ? Number(dated.slice(0, 4)) : NaN;
    for (let i = start; i < start + count; i++) { rec[i] = r; kind[i] = info.kind === 'stream' ? 1 : 0; year[i] = y; counted[i] = primary; }
    return { id, title: info.title, date: info.date ?? null, estimated, dateText: info.date ?? (estimated ? `estimated ${estimated}` : 'undated'), aliases: Array.isArray(info.aliases) ? info.aliases.filter(item => typeof item === 'string') : [], kind: info.kind, href: info.href, start, count, counted: primary === 1 };
  });
  const scale = value.steps_per_sd, nd = table.nd, dims = table.dims;
  return { ...table, rec, kind, year, counted, recordings: rows, factorIds: value.factor_ids, subjects: value.subjects, addressees: value.addressees, topics: value.topics, windowWords: value.window_words,
    loadingsSha256: value.loadings_sha256, dim: (i, k) => { const v = dims[i * nd + k]; return v === MISSING ? NaN : v / scale; } };
}

const mean = values => values.reduce((a, b) => a + b, 0) / values.length;

/** First to last year that holds a dated passage of a counted recording. */
export function passageYears(frame) {
  let low = Infinity, high = -Infinity;
  for (let i = 0; i < frame.n; i++) if (frame.counted[i] && Number.isFinite(frame.year[i])) { low = Math.min(low, frame.year[i]); high = Math.max(high, frame.year[i]); }
  return Number.isFinite(low) ? Array.from({ length: high - low + 1 }, (_, k) => low + k) : [];
}

/** Share of the passages of a kind and year that fall under each subject; a year needs `minimum` passages. */
export function subjectShares(frame, years, minimum = 30) {
  const S = frame.subjects.length, count = [0, 1].map(() => years.map(() => new Float64Array(S))), total = [0, 1].map(() => new Float64Array(years.length)), first = years[0];
  for (let i = 0; i < frame.n; i++) {
    const y = frame.year[i] - first, s = frame.subject[i];
    if (!frame.counted[i] || !(y >= 0 && y < years.length) || s === NONE) continue;
    count[frame.kind[i]][y][s]++; total[frame.kind[i]][y]++;
  }
  const all = total[0].reduce((a, b) => a + b, 0) + total[1].reduce((a, b) => a + b, 0);
  return frame.subjects.map((id, s) => ({ id, overall: all ? (count[0].reduce((a, c) => a + c[s], 0) + count[1].reduce((a, c) => a + c[s], 0)) / all : NaN,
    share: [0, 1].map(k => years.map((_, y) => total[k][y] >= minimum ? count[k][y][s] / total[k][y] : NaN)), passages: [0, 1].map(k => years.map((_, y) => count[k][y][s])) }));
}

/** Mean dimension scores of the passages on each subject, minus the mean of all passages of the same kind. */
export function subjectProfile(frame, kind, minimum = 30) {
  const S = frame.subjects.length, nd = frame.nd, sum = Array.from({ length: S }, () => new Float64Array(nd)), n = Array.from({ length: S }, () => new Float64Array(nd)), all = new Float64Array(nd), count = new Float64Array(nd);
  for (let i = 0; i < frame.n; i++) {
    if (frame.kind[i] !== kind || !frame.counted[i]) continue;
    const s = frame.subject[i];
    for (let k = 0; k < nd; k++) { const v = frame.dim(i, k); if (!Number.isFinite(v)) continue; all[k] += v; count[k]++; if (s !== NONE) { sum[s][k] += v; n[s][k]++; } }
  }
  return frame.subjects.map((id, s) => ({ id, passages: n[s][0], difference: Array.from({ length: nd }, (_, k) => n[s][k] >= minimum && count[k] ? sum[s][k] / n[s][k] - all[k] / count[k] : NaN) }));
}

/** Yearly mean of one dimension as observed, and with every year given the subject mix of the whole period. */
export function heldConstantTrend(frame, kind, k, years, minimum = 30) {
  const S = frame.subjects.length, sum = years.map(() => new Float64Array(S)), n = years.map(() => new Float64Array(S)), mix = new Float64Array(S), first = years[0];
  let total = 0;
  for (let i = 0; i < frame.n; i++) {
    const y = frame.year[i] - first, s = frame.subject[i], v = frame.dim(i, k);
    if (frame.kind[i] !== kind || !frame.counted[i] || !(y >= 0 && y < years.length) || s === NONE || !Number.isFinite(v)) continue;
    sum[y][s] += v; n[y][s]++; mix[s]++; total++;
  }
  return years.map((year, y) => {
    const passages = n[y].reduce((a, b) => a + b, 0), observed = passages >= minimum ? sum[y].reduce((a, b) => a + b, 0) / passages : NaN;
    let held = 0, weight = 0;
    for (let s = 0; s < S; s++) if (n[y][s] >= 5) { held += mix[s] / total * sum[y][s] / n[y][s]; weight += mix[s] / total; }
    return { year, passages, observed, held: Number.isFinite(observed) && weight >= 0.5 ? held / weight : NaN, covered: weight };
  });
}

/** One dimension through the passages of one recording, with a centred average of five passages. */
export function recordingArc(frame, r, k) {
  const { start, count } = frame.recordings[r], values = Array.from({ length: count }, (_, j) => frame.dim(start + j, k));
  const smooth = values.map((_, j) => { const near = values.slice(Math.max(0, j - 2), j + 3).filter(Number.isFinite); return near.length >= 3 ? mean(near) : NaN; });
  const ranked = values.map((value, j) => ({ value, passage: start + j })).filter(item => Number.isFinite(item.value)).sort((a, b) => b.value - a.value);
  const m = ranked.length ? mean(ranked.map(item => item.value)) : NaN;
  return { values, smooth, mean: m, sd: ranked.length > 1 ? Math.sqrt(ranked.reduce((a, item) => a + (item.value - m) ** 2, 0) / (ranked.length - 1)) : NaN, ranked };
}

export function topicMask(frame, topic) {
  const mask = new Uint8Array(frame.n);
  for (const i of topic.passages) mask[i] = 1;
  return mask;
}

/** Passages that name a topic against passages that do not: within the same recording (mean of the recordings'
 *  differences, with a 95% interval over recordings) and against all other passages. `kind` -1 takes both kinds. */
export function topicContrast(frame, mask, kind = -1) {
  return frame.factorIds.map((id, k) => {
    const differences = [];
    let a = 0, an = 0, b = 0, bn = 0;
    for (const row of frame.recordings) {
      if (!row.counted || (kind >= 0 && (row.kind === 'stream' ? 1 : 0) !== kind)) continue;
      let s1 = 0, n1 = 0, s0 = 0, n0 = 0;
      for (let i = row.start; i < row.start + row.count; i++) { const v = frame.dim(i, k); if (!Number.isFinite(v)) continue; if (mask[i]) { s1 += v; n1++; } else { s0 += v; n0++; } }
      a += s1; an += n1; b += s0; bn += n0;
      if (n1 && n0) differences.push(s1 / n1 - s0 / n0);
    }
    const within = differences.length ? mean(differences) : NaN, se = differences.length > 1 ? Math.sqrt(differences.reduce((p, c) => p + (c - within) ** 2, 0) / (differences.length - 1) / differences.length) : NaN;
    return { id, recordings: differences.length, within, low: within - 1.96 * se, high: within + 1.96 * se, across: an && bn ? a / an - b / bn : NaN };
  });
}

/** Passages that meet every condition, ordered by one dimension. `levels` maps a dimension index to a threshold:
 *  positive means that much or more, negative that much or less. Other versions of a recording are left out unless
 *  `versions` is true. */
export function findPassages(frame, { kind = -1, subject = -1, addressee = -1, from = -Infinity, to = Infinity, mask = null, levels = {}, sort = 0, descending = true, versions = false } = {}) {
  const hits = [], limits = Object.entries(levels).map(([k, level]) => [Number(k), Number(level)]).filter(([, level]) => level);
  for (let i = 0; i < frame.n; i++) {
    if (!versions && !frame.counted[i]) continue;
    if (kind >= 0 && frame.kind[i] !== kind) continue;
    if (subject >= 0 && frame.subject[i] !== subject) continue;
    if (addressee >= 0 && frame.addressee[i] !== addressee) continue;
    if (mask && !mask[i]) continue;
    if ((Number.isFinite(from) || Number.isFinite(to)) && !(frame.year[i] >= from && frame.year[i] <= to)) continue;
    if (!Number.isFinite(frame.dim(i, sort))) continue;
    if (limits.some(([k, level]) => { const v = frame.dim(i, k); return !Number.isFinite(v) || (level > 0 ? v < level : v > level); })) continue;
    hits.push(i);
  }
  return hits.sort((x, y) => descending ? frame.dim(y, sort) - frame.dim(x, sort) : frame.dim(x, sort) - frame.dim(y, sort));
}
