import { analysisLoadingHash, matchesAnalysisIdentity, validateSupplementPart } from './supplement.mjs';

async function sha256(bytes) {
  const hash = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(hash)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

// This binary convention matches the Python exporter without decimal-rounding ambiguity.
export async function standardizedProfileHash(release, discovery) {
  const videos = new Map(release.videos.map(video => [video.recording_id, video]));
  const ids = discovery.points.map(point => new TextEncoder().encode(point.id));
  const count = discovery.factor_ids.length;
  const bytes = new Uint8Array(ids.reduce((n, id) => n + id.length + 1 + 8 * count, 0));
  const view = new DataView(bytes.buffer);
  let offset = 0;
  discovery.points.forEach((point, row) => {
    bytes.set(ids[row], offset); offset += ids[row].length + 1;
    discovery.factor_ids.forEach((id, column) => {
      const score = videos.get(point.id).factor_scores[id].value;
      view.setFloat64(offset, (score - discovery.pca.mean[column]) / discovery.pca.scale[column], true);
      offset += 8;
    });
  });
  return sha256(bytes);
}

export async function projectionCoordinateHash(points) {
  const bytes = new Uint8Array(points.length * 16), view = new DataView(bytes.buffer);
  points.forEach((point, row) => { view.setFloat64(row * 16, point.x, true); view.setFloat64(row * 16 + 8, point.y, true); });
  return sha256(bytes);
}

/** Optional UMAP may fail closed without disabling the independently validated PCA view. */
export async function validateRecordingUmap(release, discovery, value) {
  if (!matchesAnalysisIdentity(value, release) || value.loadings_sha256 !== await analysisLoadingHash(release)) return null;
  try { await validateSupplementPart(value, release, 'recording_umap'); } catch { return null; }
  if (value.cohort_n !== discovery.cohort_n || value.points.length !== discovery.points.length) return null;
  if (value.points.some((point, i) => point.id !== discovery.points[i].id)) return null;
  for (const key of ['mean', 'scale']) {
    const values = value.standardization[key], expected = discovery.pca[key];
    if (!Array.isArray(values) || values.length !== expected.length || values.some((v, i) => !Number.isFinite(v) || v !== expected[i])) return null;
  }
  const method = value.method, parameters = method.parameters;
  if (method.algorithm !== 'UMAP' || method.package !== 'umap-learn' || typeof method.package_version !== 'string' || !/^\d+\.\d+\.\d+(?:[a-z0-9.+-]*)$/i.test(method.package_version)) return null;
  if (!parameters || parameters.n_components !== 2 || parameters.metric !== 'euclidean' || !Number.isInteger(parameters.n_neighbors) || parameters.n_neighbors < 2 || parameters.n_neighbors >= value.cohort_n || !Number.isFinite(parameters.min_dist) || parameters.min_dist < 0 || parameters.min_dist > 1 || !Number.isInteger(parameters.random_state) || parameters.n_jobs !== 1 || parameters.low_memory !== true) return null;
  if (!/^[a-f0-9]{64}$/.test(value.standardized_input_sha256) || !/^[a-f0-9]{64}$/.test(value.coordinates_sha256)) return null;
  if (value.standardized_input_sha256 !== await standardizedProfileHash(release, discovery) || value.coordinates_sha256 !== await projectionCoordinateHash(value.points)) return null;
  return value;
}

export function recordingProjection(discovery, umap, requested) {
  const nonlinear = requested === 'umap' && !!umap;
  return {
    id: nonlinear ? 'umap' : 'pca',
    name: nonlinear ? 'UMAP' : 'PCA',
    points: nonlinear ? umap.points : discovery.points,
    axes: nonlinear ? ['UMAP 1', 'UMAP 2'] : ['Principal component 1', 'Principal component 2'],
    summary: nonlinear
      ? 'UMAP emphasizes local neighborhoods in the same nine-factor profiles. Its axes are arbitrary; gaps, cluster sizes and long-range distances are not calibrated, and it has no explained-variance percentage.'
      : `PCA retains ${(100 * discovery.pca.cumulative_variance_ratio[1]).toFixed(1)}% of standardized factor-score variation in two axes (PC1 ${(100 * discovery.pca.explained_variance_ratio[0]).toFixed(1)}%, PC2 ${(100 * discovery.pca.explained_variance_ratio[1]).toFixed(1)}%).`,
  };
}
