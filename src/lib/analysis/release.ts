import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { validateAnalysisRelease } from './explorer.mjs';
import type { AnalysisRelease } from './explorer.mjs';
import { analysisReleaseCacheKey, analysisReleaseIdentity, createAnalysisReleaseCache } from './release-cache.mjs';
import { loadCorpusCatalog } from '../corpus/release';
import { analysisMatchesCorpus } from './historical-compatibility.mjs';

const publicReleasePath = path.resolve('src/data/analysis/release.json');
let cached: Promise<AnalysisRelease | null> | undefined;
const devCache = createAnalysisReleaseCache();

interface AnalysisFileSource { file: string; pointerIdentity: string }

async function analysisFilePath(): Promise<AnalysisFileSource> {
  if (!import.meta.env.DEV) return { file: publicReleasePath, pointerIdentity: 'production' };
  const base = path.resolve('research/typesafe');
  const pointerPath = path.join(base, 'current.json');
  let raw: string;
  let pointerIdentity: string;
  try {
    const pointerStat = await lstat(pointerPath, { bigint: true });
    if (!pointerStat.isFile() || pointerStat.isSymbolicLink() || (pointerStat.mode & 0o077n) !== 0n || pointerStat.size > 4096n) throw new Error('Typesafe analysis pointer must be a small private regular file.');
    raw = await readFile(pointerPath, 'utf8');
    pointerIdentity = `${analysisReleaseIdentity(pointerStat)}:${raw}`;
  }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { file: publicReleasePath, pointerIdentity: 'no-private-pointer' };
    throw error;
  }
  const pointer = JSON.parse(raw);
  if (!pointer || Object.keys(pointer).length !== 1 || Object.keys(pointer)[0] !== 'directory' || typeof pointer.directory !== 'string' || !/^run-[a-z0-9-]+$/.test(pointer.directory)) {
    throw new Error('Invalid Typesafe analysis preview pointer.');
  }
  const root = path.join(base, pointer.directory);
  const rootStat = await lstat(root, { bigint: true });
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink() || (rootStat.mode & 0o077n) !== 0n) {
    throw new Error('Typesafe analysis preview must remain a private directory.');
  }
  return { file: path.join(root, 'public-release.json'), pointerIdentity };
}

async function releaseStat(file: string) {
  try { return await lstat(file, { bigint: true }); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw error;
  }
}

function validateReleaseFile(stat: Awaited<ReturnType<typeof releaseStat>>) {
  if (!stat) return;
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 80n * 1024n * 1024n) throw new Error('Invalid public transcript analysis release file: exceeds the 80 MiB source bound.');
}

async function readAndValidateAnalysis(file: string, corpusReleaseId: string): Promise<AnalysisRelease> {
    const parsed: unknown = JSON.parse(await readFile(file, 'utf8'));
    const release = validateAnalysisRelease(parsed);
    if (release.corpus_release_id !== corpusReleaseId) {
      const pin = JSON.parse(await readFile(path.resolve('analysis-release.json'), 'utf8'));
      if (!analysisMatchesCorpus(pin, release.corpus_release_id, corpusReleaseId)) throw new Error('Transcript analysis release is based on an unapproved corpus release.');
      release.historical_input = { active_corpus_release_id: corpusReleaseId, basis: 'reviewed_transcript_update_historical_scores' };
    }
    return release;
}

async function readDevelopmentAnalysis(): Promise<AnalysisRelease | null> {
  const source = await analysisFilePath();
  const stat = await releaseStat(source.file);
  validateReleaseFile(stat);
  if (!stat) return null;
  const corpus = await loadCorpusCatalog();
  const key = analysisReleaseCacheKey(source.pointerIdentity, analysisReleaseIdentity(stat), corpus.releaseId);
  return devCache(key, () => readAndValidateAnalysis(source.file, corpus.releaseId));
}

async function readProductionAnalysis(): Promise<AnalysisRelease | null> {
  const stat = await releaseStat(publicReleasePath);
  validateReleaseFile(stat);
  if (!stat) return null;
  const corpus = await loadCorpusCatalog();
  return readAndValidateAnalysis(publicReleasePath, corpus.releaseId);
}

export function loadAnalysisRelease(): Promise<AnalysisRelease | null> {
  if (import.meta.env.DEV) return readDevelopmentAnalysis();
  // Cache a successful production read, but let failures be retried by the
  // caller instead of pinning a rejected promise for the process lifetime.
  return cached ??= readProductionAnalysis().catch((error) => { cached = undefined; throw error; });
}

/** Additional artifacts follow the same preview-only/public-release boundary. */
export async function analysisAssetDirectory(): Promise<string> {
  return path.dirname((await analysisFilePath()).file);
}
