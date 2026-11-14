import { loadCorpusCatalog, formatDuration } from '../../lib/corpus/release';
import { loadSummaryRelease } from '../../lib/summaries/release';
import { recordingDateEstimateCard, recordingOverlay } from '../../lib/corpus/date-estimates';
import { titleHistory } from '../../lib/corpus/title-history';
export async function GET() {
  const [catalog, summaries] = await Promise.all([loadCorpusCatalog(), loadSummaryRelease()]);
  const ids = new Map(summaries.summaries.filter(s=>s.kind==='transcript').map(s=>[s.recording_id,s.id]));
  // `date` and `year` stay the catalog values. `circa` (narrow archived listing window) orders and
  // filters like a date; `duplicateOf` folds an identical copy into its dated recording; `aliases`
  // carries earlier titles and the titles of folded copies for title search.
  const [overlay, history] = await Promise.all([recordingOverlay(), titleHistory()]);
  const byId = new Map(catalog.recordings.map(r=>[r.recording_id,r]));
  const aliases = (r:{recording_id:string;title:string}) => [...new Set([...(history[r.recording_id]||[]).map(t=>t.title),
    ...(overlay.copies.get(r.recording_id)||[]).flatMap(id=>[byId.get(id)!.title,...(history[id]||[]).map(t=>t.title)])])].filter(t=>t!==r.title);
  const estimates = new Map(await Promise.all(catalog.recordings.filter(r=>!r.date_label).map(async r=>[r.recording_id,await recordingDateEstimateCard(r.recording_id)] as const)));
  return new Response(JSON.stringify(catalog.recordings.map(r=>({
    title:r.title, href:`/corpus/videos/${r.slug}/`, date:r.date_label, year:r.date_year,
    dateEstimate:estimates.get(r.recording_id)||null,
    ...(overlay.circa.has(r.recording_id)?{circa:overlay.circa.get(r.recording_id)}:{}),
    ...(overlay.duplicateOf.has(r.recording_id)?{duplicateOf:`/corpus/videos/${byId.get(overlay.duplicateOf.get(r.recording_id)!)!.slug}/`}:{}),
    ...(aliases(r).length?{aliases:aliases(r)}:{}),
    duration:formatDuration(r.duration_ms), durationMs:r.duration_ms, hasTranscript:r.transcript_revision_count>0,
    summaryId:ids.get(r.recording_id)||null,
  }))), {headers:{'Content-Type':'application/json'}});
}
