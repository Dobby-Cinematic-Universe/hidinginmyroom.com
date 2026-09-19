export function knownSpeakerLabel(value) {
  if (typeof value !== 'string') return null;
  const label = value.trim();
  const normalized = label.replace(/\s*\(uncertain\)$/i, '').replaceAll('_',' ');
  if (/^speaker[\s-]*\d+$/i.test(normalized)) return null;
  if (!label || /^(?:unknown(?: speaker| participant)?|unidentified(?: speaker| participant)?|uncertain participant|unspecified|n\/a|none|null)$/i.test(normalized)) return null;
  return label;
}

export function showSpeakerLabels(segments) {
  const people = new Set();
  for (const segment of segments) {
    const label = knownSpeakerLabel(segment.speaker_label);
    if (!label || /^(?:background noise|playback(?: \/ game audio)?|game audio|text to speech|tts|music|uncertain audio source)$/i.test(label)) continue;
    people.add(label.replace(/\s*\(uncertain\)$/i, '').toLowerCase());
  }
  return people.size > 1;
}
