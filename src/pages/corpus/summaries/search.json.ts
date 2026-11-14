import { loadSummaryRelease, summaryCards } from '../../../lib/summaries/release';
export async function GET() {
  const release = await loadSummaryRelease();
  const rows = [...release.summaries].sort((a,b) => (b.period ?? '').localeCompare(a.period ?? ''));
  const cards = await summaryCards(rows);
  return new Response(JSON.stringify(cards), {headers: {'Content-Type':'application/json'}});
}
