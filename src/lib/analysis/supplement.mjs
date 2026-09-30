import { factorLoadingSignature } from './factor-descriptions.mjs';

const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const identityKeys = ['corpus_release_id', 'questionnaire_version', 'model'];
export const supplementKinds = ['robustness', 'joint_trends', 'map_diagnostics', 'discovery', 'recording_umap', 'passage_insights', 'factor_diagnostics', 'temporal_comparisons'];
export function matchesAnalysisIdentity(value, release) {
  return isObject(value) && value.schema_version === 1 && identityKeys.every(key => value[key] === release[key]) && value.source_generated_at === release.generated_at;
}

// Supplementary artifacts contain aggregates, model coefficients and dates,
// never input text, credentials, raw receipts or filesystem locations.
export function validateSafeSupplementTree(value, depth = 0) {
  if (depth > 20) throw new Error('Supplement nesting exceeds limit.');
  if (value === null || typeof value === 'boolean') return;
  if (typeof value === 'number' && Number.isFinite(value)) return;
  if (typeof value === 'string') {
    if (value.length > 6000 || /(?:\/home\/|\/mnt\/|file:\/\/|https?:\/\/|Bearer\s+[\w.-]|sk-[A-Za-z0-9]{15,})/i.test(value)) throw new Error('Supplement contains non-public material.');
    return;
  }
  if (Array.isArray(value)) {
    if (value.length > 10000) throw new Error('Supplement array exceeds limit.');
    for (const item of value) validateSafeSupplementTree(item, depth + 1);
    return;
  }
  if (!isObject(value)) throw new Error('Invalid supplement value.');
  for (const [key, item] of Object.entries(value)) {
    if (!/^[A-Za-z0-9_. -]{1,140}$/.test(key) || /^(?:api_key|authorization|secret|password|access_token|input_text|transcript_text|raw_receipt|detail_path)$/i.test(key)) throw new Error('Unsafe supplement field.');
    validateSafeSupplementTree(item, depth + 1);
  }
}

export async function analysisLoadingHash(release) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(factorLoadingSignature(release.analysis.factors)));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

export async function validateSupplementPart(value, release, kind) {
  if (!supplementKinds.includes(kind)) throw new Error('Unrecognized supplement kind.');
  if (!matchesAnalysisIdentity(value, release)) throw new Error('Supplement does not match the current scoring release.');
  validateSafeSupplementTree(value);
  if (kind !== 'map_diagnostics') {
    if (value.loadings_sha256 !== await analysisLoadingHash(release)) throw new Error('Supplement belongs to a different factor solution.');
  }
  if (kind === 'robustness' && !isObject(value.retention)) throw new Error('Missing robustness results.');
  if (kind === 'joint_trends' && !isObject(value.methods)) throw new Error('Missing joint trend results.');
  if (kind === 'discovery') {
    if (!isObject(value.pca) || !Array.isArray(value.points) || !Array.isArray(value.neighbors)) throw new Error('Missing recording discovery results.');
    const known = new Set((release.videos ?? []).map(video => video.recording_id));
    const seen = new Set();
    for (const point of value.points) {
      if (!known.has(point.id) || seen.has(point.id) || !Number.isFinite(point.x) || !Number.isFinite(point.y)) throw new Error('Invalid recording map point.');
      seen.add(point.id);
    }
    const neighborRows = new Set();
    for (const row of value.neighbors) {
      if (!seen.has(row.id) || neighborRows.has(row.id) || !Array.isArray(row.items)) throw new Error('Invalid recording neighbor list.');
      neighborRows.add(row.id);
      const neighbors = new Set();
      for (const item of row.items) {
        if (!seen.has(item.id) || item.id === row.id || neighbors.has(item.id) || !Number.isFinite(item.distance) || item.distance < 0) throw new Error('Invalid recording neighbor.');
        neighbors.add(item.id);
      }
    }
    if (neighborRows.size !== seen.size) throw new Error('Recording neighbor coverage differs from map.');
  }
  if (kind === 'recording_umap') {
    if (!isObject(value.method) || !isObject(value.standardization) || !Array.isArray(value.factor_ids) || !Array.isArray(value.points)) throw new Error('Missing recording UMAP results.');
    const ids = release.analysis.factors.map(factor => factor.id);
    if (JSON.stringify(value.factor_ids) !== JSON.stringify(ids)) throw new Error('UMAP factor order differs from the release.');
    const eligible = new Set((release.videos ?? []).filter(video => ids.every(id => Number.isFinite(video.factor_scores?.[id]?.value))).map(video => video.recording_id));
    if (value.cohort_n !== eligible.size || value.points.length !== eligible.size) throw new Error('UMAP cohort differs from complete recording profiles.');
    const seen = new Set();
    for (const point of value.points) {
      if (!eligible.has(point.id) || seen.has(point.id) || !Number.isFinite(point.x) || !Number.isFinite(point.y)) throw new Error('Invalid recording UMAP point.');
      seen.add(point.id);
    }
  }
  if (kind === 'passage_insights') {
    if (!Array.isArray(value.recording_ids) || !Array.isArray(value.question_ids) || !Array.isArray(value.rankings) || !Array.isArray(value.groups)) throw new Error('Missing passage insight results.');
    const known = new Set((release.videos ?? []).map(video => video.recording_id));
    if (new Set(value.recording_ids).size !== value.recording_ids.length || value.recording_ids.some(id => !known.has(id))) throw new Error('Unknown passage insight recording.');
    const questions = new Set(release.questions.filter(question => question.type === 'score').map(question => question.id));
    if (new Set(value.question_ids).size !== value.question_ids.length || value.question_ids.some(id => !questions.has(id)) || value.rankings.length !== value.question_ids.length) throw new Error('Unknown passage insight question.');
  }
  if (kind === 'factor_diagnostics') {
    if (!isObject(value.residual) || !isObject(value.held_out) || !isObject(value.grouping)) throw new Error('Missing factor diagnostic results.');
    const ids = value.residual.question_ids;
    if (!Array.isArray(ids) || new Set(ids).size !== ids.length || ids.some(id => !release.analysis.factor_question_ids.includes(id))) throw new Error('Residual questions differ from fitted factor bank.');
    for (const key of ['observed', 'fitted', 'residuals']) {
      if (!Array.isArray(value.residual[key]) || value.residual[key].length !== ids.length || value.residual[key].some(row => !Array.isArray(row) || row.length !== ids.length || row.some(cell => !Number.isFinite(cell)))) throw new Error('Malformed factor residual matrix.');
    }
  }
  if (kind === 'temporal_comparisons' && (!isObject(value.cohort) || !isObject(value.standardization) || !isObject(value.trajectories) || !isObject(value.distributions) || !isObject(value.changes) || typeof value.change_method !== 'string')) throw new Error('Missing temporal comparison results.');
  return value;
}

export async function loadAnalysisSupplement(release) {
  try {
    const response = await fetch('/corpus/analysis/supplement.json', { cache: 'no-cache' });
    if (!response.ok) return null;
    const payload = await response.json();
    if (!payload.available || !isObject(payload.supplement)) return null;
    const result = {};
    for (const key of supplementKinds) {
      if (!payload.supplement[key]) continue;
      try { result[key] = await validateSupplementPart(payload.supplement[key], release, key); }
      catch { /* A stale optional panel must not hide the primary release. */ }
    }
    return Object.keys(result).length ? result : null;
  } catch { return null; }
}
