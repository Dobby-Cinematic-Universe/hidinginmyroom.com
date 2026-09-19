// Upload acceptance is a release prerequisite; managed indexing is advisory.
export function requireAcceptedUploads(manifest, receipts) {
  if (!manifest.documents.every(document => receipts?.[document.key]?.accepted === true)) {
    throw Error('Uploads incomplete');
  }
}

export async function indexingAdvisory(expected, getStats) {
  try {
    const stats = await getStats();
    const counts = Object.fromEntries(['completed', 'queued', 'running', 'error', 'skipped', 'outdated']
      .map(key => [key, Number.isFinite(stats?.[key]) ? stats[key] : null]));
    const issues = (counts.error ?? 0) > 0 || (counts.skipped ?? 0) > 0;
    const complete = counts.completed === expected
      && ['queued', 'running', 'error', 'skipped'].every(key => counts[key] === 0)
      && (counts.outdated ?? 0) === 0;
    return {
      blocking: false, expected, ...counts,
      state: issues ? 'needs_attention' : complete ? 'complete' : 'incomplete',
      message: issues
        ? 'Index has failed or skipped items. Investigate search coverage; release may proceed.'
        : complete ? 'Index reports complete coverage.'
          : 'Indexing may still be running or requeued. Search coverage may be incomplete; release may proceed.',
    };
  } catch {
    // Do not leak provider errors or fail a release on an advisory stats request.
    return {blocking: false, expected, state: 'unavailable', message: 'Index status unavailable. Check search coverage separately; release may proceed.'};
  }
}
