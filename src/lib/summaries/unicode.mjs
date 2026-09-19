// Decode accidentally double-escaped printable Unicode in summary prose only.
// Do not interpret HTML, newlines, arbitrary escapes or invisible control codes.
export function summaryUnicode(text) {
  return text.replace(/\\u([dD][89aAbB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})|\\u([0-9a-fA-F]{4})/g,
    (original, high, low, unit) => {
      const value = high ? String.fromCodePoint(0x10000 + ((parseInt(high,16)-0xd800)<<10) + parseInt(low,16)-0xdc00) : String.fromCharCode(parseInt(unit,16));
      return /[\p{C}\p{Zl}\p{Zp}]/u.test(value) || value.codePointAt(0)<0xa0 ? original : value;
    });
}

export function normalizeSummaryUnicode(release) {
  for (const summary of release.summaries) {
    summary.title = summaryUnicode(summary.title);
    for (const items of Object.values(summary.sections)) for (const item of items) item.text = summaryUnicode(item.text);
  }
  return release;
}
