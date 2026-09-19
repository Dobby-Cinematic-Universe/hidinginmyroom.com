// Release-only checks: do not alter provider receipts or hide source material.
export function summaryQualityFindings(release) {
  const findings = [];
  for (const row of release.summaries) {
    for (const [section, items] of Object.entries(row.sections)) {
      for (const [index, item] of items.entries()) {
        const filler=/^(?:placeholder\d*|skip|todo|tbd|n\/?a|null|undefined|\.{2,}|…)[.!?]?$/i.test(item.text.trim());
        if (filler || (section==='summary' && /^x$/i.test(item.text.trim())) || item.text.includes('text_dup_removed')) {
          findings.push({id: row.id, section, index, reason: 'placeholder or repair-marker text'});
        }
      }
    }
  }
  return findings;
}
