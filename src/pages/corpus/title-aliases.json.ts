import type { APIRoute } from 'astro';
import { loadCorpusCatalog } from '../../lib/corpus/release';
import { titleAliases } from '../../lib/corpus/title-history';

// Earlier titles of a recording (and the titles of identical copies folded into it), keyed by recording id, for
// the title searches of client-rendered views. Only recordings that have any are listed.
export const GET: APIRoute = async () => {
  const catalog = await loadCorpusCatalog();
  return new Response(JSON.stringify(Object.fromEntries(await titleAliases(catalog.recordings))), { headers: { 'Content-Type': 'application/json' } });
};
