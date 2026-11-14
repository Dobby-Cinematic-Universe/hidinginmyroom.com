import type { APIRoute } from 'astro';
import { loadCorpusCatalog } from '../../lib/corpus/release';
import { recordingDateTexts } from '../../lib/corpus/date-estimates';

// Labelled date estimates for recordings without a catalog date, keyed by recording id.
// Client-rendered views (analysis explorer, recording map) use it where they would print "Undated".
export const GET: APIRoute = async () => {
  const catalog = await loadCorpusCatalog();
  const text = await recordingDateTexts();
  const out: Record<string, string> = {};
  for (const r of catalog.recordings) {
    if (r.date_label) continue;
    const value = text(r.recording_id, null, '');
    if (value) out[r.recording_id] = value;
  }
  return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' } });
};
