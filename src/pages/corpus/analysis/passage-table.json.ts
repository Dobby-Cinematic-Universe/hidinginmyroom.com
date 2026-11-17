import { loadAnalysisRelease } from '../../../lib/analysis/release';
import { loadPassageTable } from '../../../lib/analysis/assets';
import { estimatedDates } from '../../../lib/corpus/date-estimates';

// The passage table of the release with what the release says of each recording (title, date, kind, link) and,
// for a recording without a catalog date, its estimated date.
export async function GET() {
  const headers = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': import.meta.env.DEV ? 'no-store' : 'public, max-age=300' };
  const release = await loadAnalysisRelease(), table = release ? await loadPassageTable() : null;
  if (!release || !table) return new Response(JSON.stringify({ available: false }), { headers });
  const videos = new Map(release.videos.map(video => [video.recording_id, video])), estimates = await estimatedDates();
  const recordings = table.recordings.map(([id]) => { const video = videos.get(id)!; return { id, title: video.title, date: video.date, estimated: video.date ? null : estimates.get(id) ?? null, kind: video.design!.kind, href: video.href }; });
  return new Response(JSON.stringify({ available: true, table, recordings }), { headers });
}
