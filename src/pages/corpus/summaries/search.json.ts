import { loadSummaryRelease, summaryCard } from '../../../lib/summaries/release';
export async function GET() {
  const release = await loadSummaryRelease();
  const rows = [...release.summaries].sort((a,b) => (b.period ?? '').localeCompare(a.period ?? ''));
  return new Response(JSON.stringify(rows.map(summaryCard)), {headers: {'Content-Type':'application/json'}});
}
