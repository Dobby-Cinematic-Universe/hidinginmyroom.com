// Resolve only exact archive titles containing a platform ID and matching dates.
// Never guess from names, dates alone, or fuzzy titles.
export function resolveCatalogSource(source, records) {
  if (!/\[[A-Za-z0-9_-]{11}\]$/.test(source.title || '') || !source.date?.value) return null;
  const matches = records.filter(r => r.title === source.title && r.date_label === source.date.value);
  return matches.length === 1 ? matches[0].recording_id : null;
}
