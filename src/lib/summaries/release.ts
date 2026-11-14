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
// Card previews end on a word boundary with an ellipsis instead of mid-word.
function previewText(text: string, max = 240) {
  if (text.length <= max) return text;
  const cut = text.slice(0, max + 1);
  const space = cut.lastIndexOf(' ');
  return `${(space > max * 0.6 ? cut.slice(0, space) : cut.slice(0, max)).replace(/[\s,;:.–—-]+$/u, '')}…`;
}
/** Cards for a list of summaries; transcript summaries of undated recordings carry the labelled date estimate. */
export async function summaryCards(rows: PublicSummary[]) {
  const { recordingDateTexts } = await import('../corpus/date-estimates');
  const text = await recordingDateTexts();
  return rows.map((row) => summaryCard(row, row.kind === 'transcript' && !row.period && row.recording_id ? text(row.recording_id, null, '') || undefined : undefined));
}
/** `dateText` is the labelled date estimate of an undated recording's summary; display only. */
export function summaryCard(row: PublicSummary, dateText?: string) {
  return { id: row.id, kind: row.kind, period: row.period, title: row.title, ...(dateText ? { dateText } : {}),
    excerpt: previewText(row.sections.summary.map((i) => i.text).join(' ')),
    href: `/corpus/summaries/${row.id}/` };
}
