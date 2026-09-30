import { loadSupplement } from '../../../lib/analysis/assets';

export async function GET() {
  const supplement = await loadSupplement();
  return new Response(JSON.stringify({ available: Boolean(supplement), supplement }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': import.meta.env.DEV ? 'no-store' : 'public, max-age=300' },
  });
}
