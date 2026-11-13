import { loadCorpusCatalog, formatDuration } from '../../lib/corpus/release';
import { loadSummaryRelease } from '../../lib/summaries/release';
import { recordingDateEstimateCard } from '../../lib/corpus/date-estimates';
export async function GET() {
  const [catalog, summaries] = await Promise.all([loadCorpusCatalog(), loadSummaryRelease()]);
  const ids = new Map(summaries.summaries.filter(s=>s.kind==='transcript').map(s=>[s.recording_id,s.id]));
  // `date` and `year` stay the catalog values, so filters and sorting still treat these as undated.
  const estimates = new Map(await Promise.all(catalog.recordings.filter(r=>!r.date_label).map(async r=>[r.recording_id,await recordingDateEstimateCard(r.recording_id)] as const)));
  return new Response(JSON.stringify(catalog.recordings.map(r=>({
    title:r.title, href:`/corpus/videos/${r.slug}/`, date:r.date_label, year:r.date_year,
    dateEstimate:estimates.get(r.recording_id)||null,
    duration:formatDuration(r.duration_ms), durationMs:r.duration_ms, hasTranscript:r.transcript_revision_count>0,
    summaryId:ids.get(r.recording_id)||null,
  }))), {headers:{'Content-Type':'application/json'}});
}
