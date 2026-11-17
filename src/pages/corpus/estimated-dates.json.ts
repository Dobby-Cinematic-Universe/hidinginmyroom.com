import type { APIRoute } from 'astro';
import { loadCorpusCatalog } from '../../lib/corpus/release';
import { estimatedDates } from '../../lib/corpus/date-estimates';

// The estimated date (a day or a month) of each recording without a catalog date, keyed by recording id.
// Client-rendered views place such a recording by it in date order, year filters and periods; wherever the
// date is shown it reads as an estimate (see date-texts.json). Estimates are not verified dates.
export const GET: APIRoute = async () => {
  const [catalog, estimates] = await Promise.all([loadCorpusCatalog(), estimatedDates()]);
  const out: Record<string, string> = {};
  for (const r of catalog.recordings) { const value = r.date_label ? undefined : estimates.get(r.recording_id); if (value) out[r.recording_id] = value; }
  return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' } });
};
