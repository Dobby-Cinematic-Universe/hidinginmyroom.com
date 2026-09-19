import { loadCorpusCatalog, formatDuration } from '../../lib/corpus/release';
import { loadSummaryRelease } from '../../lib/summaries/release';
export async function GET() {
  const [catalog, summaries] = await Promise.all([loadCorpusCatalog(), loadSummaryRelease()]);
  const ids = new Map(summaries.summaries.filter(s=>s.kind==='transcript').map(s=>[s.recording_id,s.id]));
  return new Response(JSON.stringify(catalog.recordings.map(r=>({
    title:r.title, href:`/corpus/videos/${r.slug}/`, date:r.date_label, year:r.date_year,
    duration:formatDuration(r.duration_ms), hasTranscript:r.transcript_revision_count>0,
    summaryId:ids.get(r.recording_id)||null,
  }))), {headers:{'Content-Type':'application/json'}});
}
