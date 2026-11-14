import { loadCorpusCatalog } from './release';
import { recordingDateEstimate } from './date-estimates';
import { describeDateBasis } from './date-notes';

export interface DatingOverview {
  total: number;
  dated: number;
  byBasis: [string, number][];
  estimates: [string, number][];
}

const order = ['Date from matching text', 'Date from archived listing', 'Date from combined evidence', 'Date in title', 'Estimated date', 'Estimated period', 'No date or estimate'];
let overview: Promise<DatingOverview> | undefined;

/** Corpus-wide date-source counts, computed once per build and shared by every recording page. */
export function datingOverview(): Promise<DatingOverview> {
  overview ??= (async () => {
    const catalog = await loadCorpusCatalog();
    const byBasis = new Map<string, number>();
    const kinds = new Map<string, number>();
    let dated = 0;
    for (const r of catalog.recordings) {
      if (r.date_label) {
        dated++;
        const label = describeDateBasis(r.date_basis);
        byBasis.set(label, (byBasis.get(label) ?? 0) + 1);
      } else {
        const label = (await recordingDateEstimate(r.recording_id))?.label ?? 'No date or estimate';
        kinds.set(label, (kinds.get(label) ?? 0) + 1);
      }
    }
    return {
      total: catalog.recordings.length,
      dated,
      byBasis: [...byBasis].sort((a, b) => b[1] - a[1]),
      estimates: [...kinds].sort((a, b) => order.indexOf(a[0]) - order.indexOf(b[0])),
    };
  })();
  return overview;
}
