// Pagefind partitions custom records by its ISO 639-1 language parameter. This
// is an indexing choice, not a replacement for the transcript's source metadata.
export function corpusIndexLanguage(language) {
  const tag = typeof language === 'string'
    ? language.trim().toLowerCase().replaceAll('_', '-')
    : '';
  const primary = tag.split('-')[0];
  // Unknown (BCP 47 "und") has no language partition the English site can select.
  // Use its default tokenizer while retaining "und" in metadata and filters.
  return /^[a-z]{2}$/.test(primary) ? primary : 'en';
}

export function transcriptLanguage(language) {
  return typeof language === 'string' && language.trim() ? language.trim() : 'und';
}
