import { readFile, lstat } from 'node:fs/promises';
import path from 'node:path';
import { loadCorpusCatalog, loadCorpusRecording, searchableTranscripts } from '../corpus/release';
import { validateSummaryRelease } from './schema.mjs';
import { normalizeSummaryUnicode } from './unicode.mjs';
import { localPreviewRoot } from '../local-preview';

export type SummaryKind = 'archive' | 'yearly' | 'monthly' | 'transcript';
export interface SummaryItem {
  text: string;
  classification: 'reported_statement' | 'reported_allegation' | 'uncertainty';
  source_recording_ids: string[];
}
export interface PublicSummary {
  id: string; kind: SummaryKind; period: string | null; title: string;
  recording_id: string | null; publication: 'approved' | 'prepared';
  sections: Record<'summary' | 'topics' | 'events' | 'uncertainties', SummaryItem[]>;
}
interface Release { schema_version: 1; release_id: string; generated_at: string; summaries: PublicSummary[] }
let cached: Promise<Release> | undefined;
export function loadSummaryRelease(): Promise<Release> {
  return cached ??= (async () => {
    const previewRoot = localPreviewRoot();
    let file = previewRoot ? path.join(previewRoot,'summaries/release.json') : path.resolve('src/data/summaries/release.json');
    if (previewRoot) {
      const refreshed = path.join(previewRoot,'summaries/refreshed.json');
      try { await lstat(refreshed); file = refreshed; }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    }
    const stat = await lstat(file);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 64 * 1024 * 1024) throw new Error('Invalid public summary release file.');
    const value = JSON.parse(await readFile(file, 'utf8'));
    const catalog = await loadCorpusCatalog();
    const publicIds = new Set(catalog.recordings.map((r) => r.recording_id));
    validateSummaryRelease(value, publicIds, {allowPrepared: !!previewRoot});
    // Check only referenced public recordings, never private pipeline artifacts.
    const required = new Set<string>((value as Release).summaries.flatMap((s) => Object.values(s.sections).flatMap((items) => items.flatMap((i) => i.source_recording_ids))));
    const transcriptSummaryIds = new Set((value as Release).summaries.filter(s => s.kind === 'transcript').map(s => s.recording_id));
    for (const id of required) {
      const record = await loadCorpusRecording(catalog.recordings.find((r) => r.recording_id === id)!);
      // Local broader-summary previews may link to catalog-only recordings. Never
      // permit this for production, transcript summaries, or withdrawn revisions.
      if (previewRoot && !transcriptSummaryIds.has(id) && record.transcript_revisions.length === 0) continue;
      if (!searchableTranscripts(record).some((t) => t.segments.length > 0)) throw new Error('Summary points to a withdrawn or missing public transcript.');
    }
    return normalizeSummaryUnicode(value) as Release;
  })();
}
export function summaryCard(row: PublicSummary) {
  return { id: row.id, kind: row.kind, period: row.period, title: row.title,
    excerpt: row.sections.summary.map((i) => i.text).join(' ').slice(0, 240),
    href: `/corpus/summaries/${row.id}/` };
}
