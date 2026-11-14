import { searchAliases } from '../../lib/corpus/reader-extras';
// Other spellings of game, product and place names in the machine transcripts; used by transcript search.
export async function GET() {
  return new Response(JSON.stringify(await searchAliases()), { headers: { 'Content-Type': 'application/json' } });
}
