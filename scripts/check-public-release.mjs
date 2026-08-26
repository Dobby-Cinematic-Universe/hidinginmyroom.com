import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const failures = [];

const requiredPaths = [
  '.node-version',
  'astro.config.mjs',
  'package.json',
  'public',
  'src',
];

const textExtensions = new Set([
  '.astro',
  '.css',
  '.html',
  '.js',
  '.json',
  '.md',
  '.mdx',
  '.mjs',
  '.svg',
  '.ts',
  '.tsx',
  '.yml',
  '.yaml',
]);

const prohibitedPublicExtensions = new Set([
  '.7z',
  '.avi',
  '.bz2',
  '.flac',
  '.gz',
  '.iso',
  '.m4a',
  '.m4v',
  '.mkv',
  '.mov',
  '.mp3',
  '.mp4',
  '.p12',
  '.pem',
  '.pfx',
  '.rar',
  '.tar',
  '.tgz',
  '.wav',
  '.webm',
  '.xz',
  '.zip',
  '.zst',
]);

async function pathExists(relativePath) {
  try {
    await readFile(path.join(projectRoot, relativePath));
    return true;
  } catch (error) {
    if (error?.code !== 'EISDIR') return false;
    return true;
  }
}

async function walk(relativeDirectory) {
  const entries = await readdir(path.join(projectRoot, relativeDirectory), {
    withFileTypes: true,
  });
  const files = [];

  for (const entry of entries) {
    const relativePath = path.posix.join(relativeDirectory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(relativePath)));
    else if (entry.isFile()) files.push(relativePath);
  }

  return files;
}

for (const requiredPath of requiredPaths) {
  if (!(await pathExists(requiredPath))) {
    failures.push(`Required public-build input is missing: ${requiredPath}`);
  }
}

const packageJson = JSON.parse(
  await readFile(path.join(projectRoot, 'package.json'), 'utf8'),
);

for (const scriptName of ['build', 'check']) {
  const command = packageJson.scripts?.[scriptName];
  if (typeof command !== 'string' || command.trim() === '') {
    failures.push(`package.json is missing the ${scriptName} script.`);
    continue;
  }

  if (/research(?::|\/)|snapshot-current|snapshot-reddit/i.test(command)) {
    failures.push(
      `The ${scriptName} script depends on private research tooling: ${command}`,
    );
  }
}

const publicFiles = await walk('public');
for (const relativePath of publicFiles) {
  const extension = path.extname(relativePath).toLowerCase();
  if (prohibitedPublicExtensions.has(extension)) {
    failures.push(`Non-site media or archive found in public/: ${relativePath}`);
  }

  if (/^\.env(?:\.|$)/i.test(path.basename(relativePath))) {
    failures.push(`Environment file found in public/: ${relativePath}`);
  }
}

const sourceFiles = [
  ...(await walk('src')),
  ...(await walk('.github')),
  ...(await walk('docs')),
  'CONTRIBUTING.md',
  'EDITORIAL_POLICY.md',
  'README.md',
  'RIGHTS.md',
  'SECURITY.md',
  'astro.config.mjs',
  'package.json',
];
const assetReferences = new Set();
const assetPattern = /\/images\/[A-Za-z0-9_./%+-]+\.(?:avif|gif|jpe?g|png|svg|webp)/gi;
const absoluteLocalPathPattern = /(?:file:\/\/|\/home\/[A-Za-z0-9._-]+\/|[A-Za-z]:\\Users\\)/i;
const signedDiscordUrlPattern = /https?:\/\/(?:cdn\.|media\.)discordapp\.(?:com|net)\/[^\s"')>]+[?&](?:ex|is|hm)=/i;
const secretValuePatterns = [
  /DISCORD_(?:BOT|USER)_TOKEN\s*=\s*["']?[^\s"']{16,}/i,
  /(?:authorization\s*[:=]\s*["']?bearer|bearer\s+)[A-Za-z0-9._~-]{20,}/i,
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /(?<![A-Za-z0-9])gh[opsu]_[A-Za-z0-9_]{20,}/,
  /(?<![A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}/,
];

for (const relativePath of sourceFiles) {
  if (!textExtensions.has(path.extname(relativePath).toLowerCase())) continue;
  const source = await readFile(path.join(projectRoot, relativePath), 'utf8');

  if (absoluteLocalPathPattern.test(source)) {
    failures.push(`Local filesystem path found in public source: ${relativePath}`);
  }

  if (signedDiscordUrlPattern.test(source)) {
    failures.push(`Access-bearing Discord URL found in public source: ${relativePath}`);
  }

  if (secretValuePatterns.some((pattern) => pattern.test(source))) {
    failures.push(`Possible credential value found in public source: ${relativePath}`);
  }

  for (const match of source.matchAll(assetPattern)) {
    assetReferences.add(decodeURI(match[0]).replace(/^\//, ''));
  }
}

for (const assetReference of assetReferences) {
  if (!(await pathExists(path.posix.join('public', assetReference)))) {
    failures.push(`Referenced public asset is missing: /${assetReference}`);
  }
}

if (failures.length > 0) {
  console.error('Public-release checks failed:');
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(
    `Public-release checks passed (${publicFiles.length} public files; ${assetReferences.size} local image references).`,
  );
}
