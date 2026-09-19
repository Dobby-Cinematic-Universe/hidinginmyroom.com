import { createHash } from 'node:crypto';
import {
  chmod,
  lstat,
  mkdir,
  open,
  readFile,
  readdir,
  realpath,
} from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
const wikiRoot = path.join(projectRoot, 'src', 'content', 'docs', 'wiki');
const outputRoot = path.join(
  projectRoot,
  'research',
  'corpus',
  'graph-seed-inventories',
);

function fail(message) {
  throw new Error(message);
}

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

export function canonicalJson(value) {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    typeof value === 'number'
  ) {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return '[' + value.map(canonicalJson).join(',') + ']';
  }
  if (typeof value === 'object') {
    return (
      '{' +
      Object.keys(value)
        .sort()
        .map((key) => JSON.stringify(key) + ':' + canonicalJson(value[key]))
        .join(',') +
      '}'
    );
  }
  fail('Unsupported canonical JSON value.');
}

function parseOutputArgument(argv) {
  if (argv.length !== 2 || argv[0] !== '--output') {
    fail(
      'Usage: node scripts/build-wiki-graph-seed-inventory.mjs ' +
        '--output research/corpus/graph-seed-inventories/<name>.json',
    );
  }
  const candidate = path.resolve(projectRoot, argv[1]);
  if (
    candidate === outputRoot ||
    !candidate.startsWith(outputRoot + path.sep) ||
    path.extname(candidate) !== '.json'
  ) {
    fail('Output must be a JSON file beneath research/corpus/graph-seed-inventories/.');
  }
  return candidate;
}

async function stableRead(filePath) {
  const before = await lstat(filePath);
  if (!before.isFile() || before.isSymbolicLink()) {
    fail('Wiki input is not a regular non-symlink file: ' + filePath);
  }
  const body = await readFile(filePath);
  const after = await lstat(filePath);
  for (const key of ['dev', 'ino', 'size', 'mtimeMs']) {
    if (before[key] !== after[key]) {
      fail('Wiki input changed while being read: ' + filePath);
    }
  }
  return body;
}

function unquoteYamlScalar(value) {
  const trimmed = value.trim();
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
      return trimmed.slice(1, -1);
    }
  }
  return trimmed;
}

export function frontmatterTitle(text, relativePath) {
  const match = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/.exec(text);
  if (!match) fail('Wiki page lacks closed frontmatter: ' + relativePath);
  const titleMatch = /^title:\s*(.+?)\s*$/m.exec(match[1]);
  if (!titleMatch) fail('Wiki page lacks a frontmatter title: ' + relativePath);
  const title = unquoteYamlScalar(titleMatch[1]);
  if (!title) fail('Wiki page has an empty frontmatter title: ' + relativePath);
  return title;
}

function oneQuotedAttribute(body, name, relativePath, ordinal, required = false) {
  const expression = new RegExp('\\b' + name + '="([^"]*)"', 'g');
  const matches = [...body.matchAll(expression)];
  if (matches.length > 1) {
    fail(
      'SourceCitation ' + ordinal + ' repeats ' + name + ' in ' + relativePath,
    );
  }
  if (required && matches.length !== 1) {
    fail(
      'SourceCitation ' + ordinal + ' lacks ' + name + ' in ' + relativePath,
    );
  }
  return matches.length === 1 ? matches[0][1] : null;
}

export function sourceCitations(text, relativePath) {
  const blocks = [
    ...text.matchAll(
      /<SourceCitation\b((?:"[^"]*"|'[^']*'|[^'"<>])*)\/?>/g,
    ),
  ];
  return blocks.map((match, index) => {
    const ordinal = index + 1;
    const body = match[1];
    const unquotedBody = body.replace(/"[^"]*"|'[^']*'/g, ' ');
    const sourceId = oneQuotedAttribute(
      body,
      'sourceId',
      relativePath,
      ordinal,
      true,
    );
    const claimId = oneQuotedAttribute(
      body,
      'claimId',
      relativePath,
      ordinal,
    );
    const href = oneQuotedAttribute(body, 'href', relativePath, ordinal);
    const reviewState = oneQuotedAttribute(
      body,
      'reviewState',
      relativePath,
      ordinal,
    );
    return {
      ordinal,
      source_id: sourceId,
      claim_id: claimId,
      href,
      review_state: reviewState,
      checked_attribute_present: /\bchecked\s*=/.test(unquotedBody),
    };
  });
}

async function wikiPages() {
  const pages = [];
  const treeHasher = createHash('sha256');
  for (const domain of ['characters', 'events']) {
    const directory = path.join(wikiRoot, domain);
    const entries = (await readdir(directory, { withFileTypes: true }))
      .filter((entry) => entry.name.endsWith('.mdx'))
      .sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      if (!entry.isFile() || entry.isSymbolicLink()) {
        fail('Wiki directory contains a non-regular MDX entry: ' + entry.name);
      }
      const filePath = path.join(directory, entry.name);
      const relativePath = path.relative(projectRoot, filePath).split(path.sep).join('/');
      const body = await stableRead(filePath);
      const text = body.toString('utf8');
      const citations = sourceCitations(text, relativePath);
      treeHasher.update(relativePath, 'utf8');
      treeHasher.update('\0');
      treeHasher.update(body);
      treeHasher.update('\0');
      pages.push({
        domain,
        slug: entry.name.slice(0, -4),
        title: frontmatterTitle(text, relativePath),
        source_path: relativePath,
        source_sha256: sha256(body),
        source_citations: citations,
        claim_ids: [...new Set(citations.map((item) => item.claim_id).filter(Boolean))].sort(),
      });
    }
  }
  return { pages, sourceTreeSha256: treeHasher.digest('hex') };
}

export function makeInventory(pages, sourceTreeSha256) {
  const citations = pages.flatMap((page) => page.source_citations);
  const claimIds = new Set(citations.map((item) => item.claim_id).filter(Boolean));
  const identityBody = {
    schema_version: 1,
    kind: 'wiki_graph_seed_inventory',
    source_scope: [
      'src/content/docs/wiki/characters/*.mdx',
      'src/content/docs/wiki/events/*.mdx',
    ],
    source_tree_sha256: sourceTreeSha256,
    authority: {
      semantic_mapping: false,
      identity_assertion: false,
      event_truth_assertion: false,
      catalog_import: false,
      publication: false,
    },
    counts: {
      pages: pages.length,
      character_pages: pages.filter((page) => page.domain === 'characters').length,
      event_pages: pages.filter((page) => page.domain === 'events').length,
      source_citations: citations.length,
      unique_claim_ids: claimIds.size,
      citations_without_claim_id: citations.filter((item) => item.claim_id === null)
        .length,
    },
    pages,
  };
  return {
    ...identityBody,
    inventory_id: 'wgsi_' + sha256(canonicalJson(identityBody)).slice(0, 32),
  };
}

async function writeOwnerPrivateExact(outputPath, bytes) {
  await mkdir(outputRoot, { recursive: true, mode: 0o700 });
  await chmod(outputRoot, 0o700);
  const realRoot = await realpath(outputRoot);
  const parent = path.dirname(outputPath);
  await mkdir(parent, { recursive: true, mode: 0o700 });
  const realParent = await realpath(parent);
  if (realParent !== realRoot && !realParent.startsWith(realRoot + path.sep)) {
    fail('Resolved output parent escapes the private inventory root.');
  }
  try {
    const handle = await open(outputPath, 'wx', 0o600);
    try {
      await handle.writeFile(bytes);
      await handle.sync();
    } finally {
      await handle.close();
    }
    return false;
  } catch (error) {
    if (error?.code !== 'EEXIST') throw error;
    const existing = await stableRead(outputPath);
    if (!existing.equals(bytes)) {
      fail('Refusing to overwrite a different existing inventory.');
    }
    return true;
  }
}

async function main() {
  const outputPath = parseOutputArgument(process.argv.slice(2));
  const { pages, sourceTreeSha256 } = await wikiPages();
  const inventory = makeInventory(pages, sourceTreeSha256);
  const bytes = Buffer.from(JSON.stringify(inventory, null, 2) + '\n', 'utf8');
  const reused = await writeOwnerPrivateExact(outputPath, bytes);
  process.stdout.write(
    JSON.stringify({
      inventory_id: inventory.inventory_id,
      output_sha256: sha256(bytes),
      byte_count: bytes.length,
      counts: inventory.counts,
      reused,
      semantic_authority: false,
      publication_authority: false,
    }) + '\n',
  );
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))
) {
  await main();
}
