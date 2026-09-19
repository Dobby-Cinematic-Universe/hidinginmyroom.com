import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distRoot = path.join(projectRoot, 'dist');
const corpusRoot = path.join(distRoot, 'corpus');
const mainEntryPath = path.join(distRoot, 'pagefind', 'pagefind-entry.json');
const searchManifestPath = path.join(corpusRoot, 'search-manifest.json');
const failures = [];

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const filePath = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(filePath)));
    else if (entry.isFile()) files.push(filePath);
  }
  return files;
}

async function readJson(filePath, label) {
  try {
    return JSON.parse(await readFile(filePath, 'utf8'));
  } catch (error) {
    failures.push(`${label} is missing or invalid at ${path.relative(projectRoot, filePath)}.`);
    return undefined;
  }
}

function pageCount(entry, label) {
  if (!entry || typeof entry !== 'object' || !entry.languages || typeof entry.languages !== 'object') {
    failures.push(`${label} does not contain a Pagefind languages object.`);
    return undefined;
  }
  let total = 0;
  for (const language of Object.values(entry.languages)) {
    if (
      !language ||
      typeof language !== 'object' ||
      typeof language.page_count !== 'number' ||
      !Number.isInteger(language.page_count) ||
      language.page_count < 0
    ) {
      failures.push(`${label} contains an invalid page_count.`);
      return undefined;
    }
    total += language.page_count;
  }
  return total;
}

let htmlFiles = [];
try {
  htmlFiles = (await walk(distRoot)).filter((filePath) => filePath.endsWith('.html'));
} catch {
  failures.push('dist/ is missing. Build the static site before checking search isolation.');
}

let markerPageCount = 0;
let corpusHtmlCount = 0;
for (const filePath of htmlFiles) {
  const html = await readFile(filePath, 'utf8');
  const relativePath = path.relative(distRoot, filePath);
  const hasMainMarker = /\bdata-pagefind-body(?:\s|=|>)/i.test(html);
  if (hasMainMarker) markerPageCount += 1;

  const isCorpusPage = relativePath === 'corpus.html' || relativePath.startsWith(`corpus${path.sep}`);
  if (!isCorpusPage) continue;
  corpusHtmlCount += 1;
  if (hasMainMarker) {
    failures.push(`Corpus HTML contains the main Pagefind marker: ${relativePath}`);
  }
  if (!/\bdata-pagefind-ignore=(?:"all"|'all')/i.test(html)) {
    failures.push(`Corpus HTML lacks data-pagefind-ignore="all": ${relativePath}`);
  }
}

if (corpusHtmlCount === 0) failures.push('No static corpus HTML pages were generated.');

const mainEntry = await readJson(mainEntryPath, 'Main Pagefind entry');
const searchManifest = await readJson(searchManifestPath, 'Corpus search manifest');
const mainCount = pageCount(mainEntry, 'Main Pagefind entry');
let corpusCount = 0;
const corpusEntryPaths = [];

if (searchManifest) {
  if (searchManifest.schema_version === 2) {
    const expectedKeys = [
      'schema_version',
      'release_id',
      'release_schema_version',
      'release_generated_at',
      'index_batch_size',
      'indexes',
      'recording_count',
      'record_count',
      'skipped_empty_segments',
    ].sort();
    const actualKeys = Object.keys(searchManifest).sort();
    if (
      expectedKeys.length !== actualKeys.length ||
      expectedKeys.some((key, index) => key !== actualKeys[index])
    ) {
      failures.push('Corpus search manifest v2 has unexpected or missing fields.');
    }
    if (
      !Number.isInteger(searchManifest.index_batch_size) ||
      searchManifest.index_batch_size < 1_000 ||
      searchManifest.index_batch_size > 100_000
    ) {
      failures.push('Corpus search manifest has an invalid index_batch_size.');
    }
  }
  const indexes = Array.isArray(searchManifest.indexes)
    ? searchManifest.indexes
    : [{ path: searchManifest.index_path, record_count: searchManifest.record_count }];
  if (indexes.length === 0) failures.push('Corpus search manifest declares no index shards.');
  let declaredShardCount = 0;
  const seenIndexPaths = new Set();
  for (const [ordinal, descriptor] of indexes.entries()) {
    if (
      !descriptor || typeof descriptor !== 'object' ||
      Object.keys(descriptor).sort().join(',') !== 'path,record_count' ||
      typeof descriptor.path !== 'string' ||
      !/^\/corpus\/pagefind\/(?:[0-9]{5}\/)?$/.test(descriptor.path) ||
      !Number.isInteger(descriptor.record_count) || descriptor.record_count < 0
    ) {
      failures.push(`Corpus search index descriptor ${ordinal} is invalid.`);
      continue;
    }
    if (seenIndexPaths.has(descriptor.path)) {
      failures.push(`Corpus search index descriptor ${ordinal} repeats a path.`);
      continue;
    }
    seenIndexPaths.add(descriptor.path);
    const relativeDirectory = descriptor.path.replace(/^\//, '').replace(/\/$/, '');
    const entryPath = path.join(distRoot, relativeDirectory, 'pagefind-entry.json');
    const bundlePath = path.join(distRoot, relativeDirectory, 'pagefind.js');
    corpusEntryPaths.push(entryPath);
    const entry = await readJson(entryPath, `Corpus Pagefind entry ${ordinal}`);
    const observed = pageCount(entry, `Corpus Pagefind entry ${ordinal}`);
    if (typeof observed === 'number') {
      corpusCount += observed;
      if (observed !== descriptor.record_count) {
        failures.push(
          `Corpus Pagefind shard ${ordinal} contains ${observed} records, but declares ` +
            `${descriptor.record_count}.`,
        );
      }
    }
    declaredShardCount += descriptor.record_count;
    try {
      await readFile(bundlePath);
    } catch {
      failures.push(`Corpus Pagefind JavaScript bundle ${ordinal} is missing.`);
    }
  }
  if (declaredShardCount !== searchManifest.record_count) {
    failures.push(
      `Corpus search shard descriptors total ${declaredShardCount}, but the manifest declares ` +
        `${searchManifest.record_count}.`,
    );
  }
}

if (typeof mainCount === 'number' && mainCount !== markerPageCount) {
  failures.push(
    `Main Pagefind contains ${mainCount} pages, but ${markerPageCount} rendered pages carry ` +
      'data-pagefind-body.',
  );
}

if (
  searchManifest &&
  (typeof searchManifest.record_count !== 'number' ||
    !Number.isInteger(searchManifest.record_count) ||
    searchManifest.record_count < 0)
) {
  failures.push('Corpus search manifest has an invalid record_count.');
}

if (
  typeof corpusCount === 'number' &&
  typeof searchManifest?.record_count === 'number' &&
  corpusCount !== searchManifest.record_count
) {
  failures.push(
    `Corpus Pagefind contains ${corpusCount} records, but its build manifest declares ` +
      `${searchManifest.record_count}.`,
  );
}

if (corpusEntryPaths.some((entryPath) => path.dirname(mainEntryPath) === path.dirname(entryPath))) {
  failures.push('Main and corpus Pagefind entries resolve to the same output directory.');
}

if (failures.length > 0) {
  console.error('Search-isolation checks failed:');
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(
    `Search isolation passed (${markerPageCount} main-index pages; ` +
      `${corpusCount} corpus records; ${corpusHtmlCount} excluded corpus HTML pages).`,
  );
}
