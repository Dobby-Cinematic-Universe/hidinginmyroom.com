// Public release contract: intentionally incompatible with private pipeline exports.
export const kinds = ['archive', 'yearly', 'monthly', 'transcript'];
export const sections = ['summary', 'topics', 'events', 'uncertainties'];
const tags = ['reported_statement', 'reported_allegation', 'uncertainty'];
const object = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
function exact(value, keys) {
  if (!object(value) || Object.keys(value).sort().join('|') !== [...keys].sort().join('|')) {
    throw new Error('Summary release has missing or unexpected fields.');
  }
}
function text(value, max = 1200) {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error('Invalid summary text.');
}

export function validateSummaryRelease(value, publishedTranscriptIds = new Set(), { allowPrepared = false } = {}) {
  exact(value, ['schema_version', 'release_id', 'generated_at', 'summaries']);
  if (value.schema_version !== 1 || !/^summaries_[a-z0-9_-]{1,80}$/.test(value.release_id)) throw new Error('Invalid summary release identity.');
  if (typeof value.generated_at !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(value.generated_at)
      || !Number.isFinite(Date.parse(value.generated_at)) || new Date(value.generated_at).toISOString() !== value.generated_at.replace('Z', '.000Z')) throw new Error('Invalid release date.');
  if (!Array.isArray(value.summaries) || value.summaries.length > 20000) throw new Error('Invalid summary collection.');
  const ids = new Set(); const scopes = new Set();
  for (const row of value.summaries) {
    exact(row, ['id', 'kind', 'period', 'title', 'recording_id', 'publication', 'sections']);
    if (typeof row.id !== 'string' || !/^[a-z0-9][a-z0-9-]{0,95}$/.test(row.id) || ['browse','index','search'].includes(row.id) || ids.has(row.id)) throw new Error('Invalid or duplicate summary ID.');
    ids.add(row.id);
    if (!kinds.includes(row.kind) || !(row.publication === 'approved' || (allowPrepared && row.publication === 'prepared'))) throw new Error('Summary is not approved for publication.');
    text(row.title, 500);
    if (!(row.period === null || (typeof row.period === 'string' && /^(19|20)\d{2}(-(0[1-9]|1[0-2]))?$/.test(row.period)))) throw new Error('Invalid summary period.');
    if ((row.kind === 'archive' && row.period !== null) || (row.kind === 'yearly' && !/^\d{4}$/.test(row.period ?? ''))
        || (['monthly','transcript'].includes(row.kind) && row.period !== null && !/^\d{4}-\d{2}$/.test(row.period))) throw new Error('Summary kind and period differ.');
    if (row.kind === 'transcript' ? !publishedTranscriptIds.has(row.recording_id) : row.recording_id !== null) throw new Error('Summary requires a released transcript.');
    const scope = row.kind + ':' + (row.kind === 'transcript' ? row.recording_id : row.period);
    if (scopes.has(scope)) throw new Error('Duplicate preferred summary scope.');
    scopes.add(scope);
    exact(row.sections, sections);
    for (const section of sections) {
      const items = row.sections[section];
      if (!Array.isArray(items) || items.length > 100 || (section === 'summary' && !items.length)) throw new Error('Invalid summary section.');
      for (const item of items) {
        exact(item, ['text', 'classification', 'source_recording_ids']);
        text(item.text, 4000);
        if (!tags.includes(item.classification) || (section === 'uncertainties' && item.classification !== 'uncertainty')) throw new Error('Invalid evidence classification.');
        if (!Array.isArray(item.source_recording_ids) || !item.source_recording_ids.length
            || new Set(item.source_recording_ids).size !== item.source_recording_ids.length
            || item.source_recording_ids.some((id) => !publishedTranscriptIds.has(id))
            || (row.kind === 'transcript' && item.source_recording_ids.some((id) => id !== row.recording_id))) throw new Error('Summary source is not a released transcript.');
      }
    }
  }
  return value;
}

export function periodLabel(period) {
  if (!period) return 'Undated';
  if (period.length === 4) return period;
  return new Intl.DateTimeFormat('en', {month: 'long', year: 'numeric', timeZone: 'UTC'}).format(new Date(period + '-01T00:00:00Z'));
}

export function filterSummaries(rows, query = '', kind = '', year = '') {
  const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return rows.filter((row) => (!kind || row.kind === kind)
    && (!year || (year === 'undated' ? row.period === null && row.kind !== 'archive' : row.period?.startsWith(year)))
    && words.every((word) => (row.title + ' ' + row.excerpt).toLowerCase().includes(word)));
}
