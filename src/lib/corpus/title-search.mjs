// How a title search matches, wherever recordings are found by title: the browse page, the analysis explorer,
// the recording map's selector and the passage explorer. Every word of the query must occur somewhere in the
// searched text, in any order and not necessarily next to each other; case does not matter. The searched text is
// the title, any earlier titles of the recording, and whatever else a view adds (a date, a category).
export const searchWords = query => String(query ?? '').trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
export const searchText = (...parts) => parts.flat().filter(part => typeof part === 'string' && part).join('\n').toLocaleLowerCase();
export const matchesAll = (text, words) => words.every(word => text.includes(word));
/** The earlier title that explains a match the present title does not: shown as "Also titled". */
export function matchedAlias(title, aliases, words) {
  if (!words.length || !Array.isArray(aliases) || matchesAll(String(title).toLocaleLowerCase(), words)) return undefined;
  return aliases.find(alias => words.some(word => alias.toLocaleLowerCase().includes(word)));
}
