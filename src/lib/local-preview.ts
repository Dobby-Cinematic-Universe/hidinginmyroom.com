import { readFileSync, lstatSync } from 'node:fs';
import path from 'node:path';

// This pointer is private, ignored by Git, and never read in a production build.
export function localPreviewRoot(): string | undefined {
  if (!import.meta.env.DEV) return undefined;
  const base = path.resolve('research/corpus/site-previews');
  const pointer = path.join(base, 'current.json');
  let raw: string;
  try { raw = readFileSync(pointer, 'utf8'); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined; throw error; }
  const value = JSON.parse(raw);
  if (Object.keys(value).join() !== 'directory' || typeof value.directory !== 'string'
      || !/^release-[a-z0-9-]+$/.test(value.directory)) throw new Error('Invalid local preview pointer.');
  const root = path.join(base, value.directory);
  const stat = lstatSync(root);
  if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o077)) throw new Error('Preview must remain a private directory.');
  return root;
}

export function previewAnnotation(recordingId: string): {
  origin: string; attribution: string | null; model: string | null;
  speaker_review_complete: boolean; coverage_verified: boolean;
  coverage_note?: string | null;
} | undefined {
  const root = localPreviewRoot();
  if (!root) return undefined;
  return JSON.parse(readFileSync(path.join(root,'corpus/annotations.json'),'utf8')).recordings[recordingId];
}
