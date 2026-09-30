import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { analysisAssetDirectory, loadAnalysisRelease } from './release';
import { validateSupplementPart } from './supplement.mjs';
import type { AnalysisSupplement } from './supplement.mjs';

const assets = {
  robustness: 'extended-robustness.json', joint_trends: 'joint-trends.json', map_diagnostics: 'map-diagnostics.json',
  discovery: 'recording-discovery.json', recording_umap: 'recording-umap.json', passage_insights: 'passage-insights.json',
  factor_diagnostics: 'factor-diagnostics.json', temporal_comparisons: 'temporal-comparisons.json',
} as const;

export async function readAnalysisAsset(relative: string): Promise<unknown | null> {
  if (!Object.values(assets).includes(relative as typeof assets[keyof typeof assets]) && !/^passages\/rec_[a-f0-9]{32}\.json$/.test(relative)) throw new Error('Unrecognized analysis asset.');
  const base = await analysisAssetDirectory();
  if (relative.startsWith('passages/')) {
    try {
      const directory = await lstat(path.join(base, 'passages'));
      if (!directory.isDirectory() || directory.isSymbolicLink() || (import.meta.env.DEV && (directory.mode & 0o077) !== 0)) throw new Error('Invalid passage directory.');
    } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null; throw error; }
  }
  const file = path.join(base, relative);
  let stat;
  try { stat = await lstat(file); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null; throw error; }
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 8 * 1024 * 1024 || (import.meta.env.DEV && (stat.mode & 0o077) !== 0)) throw new Error('Invalid supplemental analysis file.');
  return JSON.parse(await readFile(file, 'utf8'));
}

export async function loadSupplement(): Promise<AnalysisSupplement | null> {
  const release = await loadAnalysisRelease();
  if (!release) return null;
  const result: AnalysisSupplement = {};
  for (const [key, file] of Object.entries(assets)) {
    try {
      const value = await readAnalysisAsset(file);
      if (value) result[key as keyof AnalysisSupplement] = await validateSupplementPart(value, release, key);
    } catch (error) { if (import.meta.env.DEV) console.warn(`Optional analysis ${key} unavailable:`, error); }
  }
  return Object.keys(result).length ? result : null;
}
