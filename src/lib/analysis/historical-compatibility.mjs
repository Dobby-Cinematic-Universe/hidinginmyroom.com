/** Explicit pin-bound reuse preserves the original scored inputs and scores. */
export function analysisMatchesCorpus(pin, scoredCorpus, activeCorpus) {
  if (pin.corpus_release !== scoredCorpus) return false;
  const hasCompatibility = 'compatible_corpus_release' in pin || 'compatibility_basis' in pin;
  if (hasCompatibility && (pin.compatibility_basis !== 'reviewed_transcript_update_historical_scores'
    || !/^release_[a-f0-9]{24}$/.test(pin.compatible_corpus_release ?? '')
    || pin.compatible_corpus_release === scoredCorpus)) return false;
  return scoredCorpus === activeCorpus || (hasCompatibility && pin.compatible_corpus_release === activeCorpus);
}

export function historicalPassageHref(href, milliseconds) {
  const url = new URL(href, 'https://local.invalid');
  url.searchParams.set('t', String(Math.floor(milliseconds / 1000)));
  url.hash = '';
  return `${url.pathname}${url.search}`;
}
