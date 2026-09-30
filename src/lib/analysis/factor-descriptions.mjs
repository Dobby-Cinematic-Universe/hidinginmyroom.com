/** Canonical saved orientations: descriptions must not silently follow a refit. */
export function factorLoadingSignature(factors) {
  return JSON.stringify(factors
    .map(factor => [factor.id, Object.entries(factor.loadings).sort(([a], [b]) => a.localeCompare(b))])
    .sort(([a], [b]) => a.localeCompare(b)));
}

export async function resolveFactorDescriptions(release, annotations) {
  if (annotations?.schema_version !== 1) return {};
  for (const key of ['corpus_release_id', 'questionnaire_version', 'model', 'generated_at']) {
    if (release[key] !== annotations[key]) return {};
  }
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(factorLoadingSignature(release.analysis.factors)));
  const hash = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
  if (hash !== annotations.loadings_sha256) return {};
  const factors = new Set(release.analysis.factors.map(factor => factor.id));
  return Object.fromEntries(Object.entries(annotations.descriptions ?? {}).filter(([id, note]) =>
    factors.has(id) && note && ['label', 'description', 'higher', 'lower'].every(key => typeof note[key] === 'string' && note[key].trim().length > 0)
  ));
}

export function factorDisplayLabel(factor, descriptions) {
  const description = descriptions[factor.id];
  if (description) return `${factor.id} · ${description.label} (provisional)`;
  return factor.label === factor.id ? factor.id : `${factor.id}: ${factor.label}`;
}
