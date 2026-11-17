// A lossless, compact encoding of the score rows that the explorer loads.
//
// A row of the release repeats its key names for every one of dozens of scores, so most of a shard is key names.
// The encoding turns a list of like-shaped values into columns: key names are written once and the values of each
// key side by side. It is defined for any JSON value and decodes to exactly what was encoded: the same values, the
// same key order, the same JSON text when serialized again. `encodeShard` checks that for the rows it is given and
// falls back to carrying them as they are, so no release can lose information to it.
//
// A node is an array whose first item names its kind:
//   ['p', values]                    values as they are (numbers, strings, booleans, nulls)
//   ['c', value, n]                  the same primitive value n times
//   ['d', dictionary, indexes]       strings or nulls drawn from a short dictionary
//   ['o', keys, columns, n]          n objects with these keys in this order; columns[k] is a node of n values
//   ['g', kinds, nodes]              rows of several shapes: kinds[i] says which node holds row i, in order
//   ['r', values]                    anything else, carried as it is
const plain = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const primitive = value => value === null || typeof value === 'string' || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value));

function shapeOf(value) {
  if (primitive(value)) return 'p';
  if (!plain(value)) return 'r';
  const keys = Object.keys(value);
  return keys.includes('__proto__') ? 'r' : 'o' + JSON.stringify(keys);
}

function encodeColumn(values) {
  const n = values.length;
  if (n === 0) return ['r', []];
  const shapes = values.map(shapeOf), first = shapes[0];
  if (shapes.every(shape => shape === first)) {
    if (first === 'r') return ['r', values];
    if (first === 'p') {
      if (n > 1 && values.every(value => value === values[0])) return ['c', values[0], n];
      if (values.every(value => value === null || typeof value === 'string')) {
        const dictionary = [...new Set(values)];
        if (dictionary.length <= n / 2) { const at = new Map(dictionary.map((value, i) => [value, i])); return ['d', dictionary, values.map(value => at.get(value))]; }
      }
      return ['p', values];
    }
    const keys = Object.keys(values[0]);
    return ['o', keys, keys.map(key => encodeColumn(values.map(value => value[key]))), n];
  }
  const kinds = [...new Set(shapes)], at = new Map(kinds.map((shape, i) => [shape, i]));
  return ['g', shapes.map(shape => at.get(shape)), kinds.map(shape => encodeColumn(values.filter((_, i) => shapes[i] === shape)))];
}

function decodeColumn(node) {
  if (!Array.isArray(node)) throw new Error('Invalid score shard encoding.');
  const [kind, a, b, c] = node;
  if (kind === 'p' || kind === 'r') { if (!Array.isArray(a) || node.length !== 2) throw new Error('Invalid score shard encoding.'); return a; }
  if (kind === 'c') { if (!primitive(a) || !Number.isInteger(b) || b < 2 || b > 1e6 || node.length !== 3) throw new Error('Invalid score shard encoding.'); return new Array(b).fill(a); }
  if (kind === 'd') {
    if (!Array.isArray(a) || !Array.isArray(b) || node.length !== 3 || !b.every(i => Number.isInteger(i) && i >= 0 && i < a.length) || !a.every(value => value === null || typeof value === 'string')) throw new Error('Invalid score shard encoding.');
    return b.map(i => a[i]);
  }
  if (kind === 'o') {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length || !Number.isInteger(c) || c < 1 || c > 1e6 || node.length !== 4 || !a.every(key => typeof key === 'string' && key !== '__proto__') || new Set(a).size !== a.length) throw new Error('Invalid score shard encoding.');
    const columns = b.map(decodeColumn);
    if (columns.some(column => column.length !== c)) throw new Error('Invalid score shard encoding.');
    const rows = new Array(c);
    for (let i = 0; i < c; i++) { const row = {}; for (let k = 0; k < a.length; k++) row[a[k]] = columns[k][i]; rows[i] = row; }
    return rows;
  }
  if (kind === 'g') {
    if (!Array.isArray(a) || !Array.isArray(b) || node.length !== 3 || b.length < 2) throw new Error('Invalid score shard encoding.');
    const parts = b.map(decodeColumn), next = new Array(parts.length).fill(0);
    const rows = a.map(i => { if (!Number.isInteger(i) || i < 0 || i >= parts.length || next[i] >= parts[i].length) throw new Error('Invalid score shard encoding.'); return parts[i][next[i]++]; });
    if (next.some((used, i) => used !== parts[i].length)) throw new Error('Invalid score shard encoding.');
    return rows;
  }
  throw new Error('Invalid score shard encoding.');
}

/** The rows of a shard in the compact encoding, or carried as they are if that would not give them back exactly. */
export function encodeShard(videos) {
  if (!Array.isArray(videos)) throw new TypeError('Score rows must be a list.');
  const encoded = encodeColumn(videos);
  try { if (JSON.stringify(decodeColumn(JSON.parse(JSON.stringify(encoded)))) === JSON.stringify(videos)) return encoded; } catch { /* carried as they are */ }
  return ['r', videos];
}

/** The rows of a served shard body: decoded for schema version 2, as they are for version 1 (the format before
 *  the compact encoding, which a live site can still serve while a release is being checked). */
export function shardRows(body) {
  if (body?.schema_version === 2) return decodeShard(body.videos);
  if (body?.schema_version === 1 && Array.isArray(body.videos)) return body.videos;
  throw new Error('Invalid score shard encoding.');
}

/** The rows that an encoded shard holds. */
export function decodeShard(encoded) {
  const rows = decodeColumn(encoded);
  if (!Array.isArray(rows)) throw new Error('Invalid score shard encoding.');
  return rows;
}
