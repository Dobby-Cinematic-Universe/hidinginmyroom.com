const isRecord = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const finite = value => typeof value === 'number' && Number.isFinite(value);
const display = value => finite(value) ? value.toFixed(3) : '—';

function numericArray(values) {
  return Array.isArray(values) ? values.map(value => finite(value) ? value : null) : [];
}

function summarizeRefit(key, value) {
  if (!isRecord(value)) return null;
  const congruence = numericArray(value.loading_congruence_to_primary);
  const scoreRank = numericArray(value.aligned_factor_score_spearman_to_primary);
  return {
    key,
    status: typeof value.status === 'string' ? value.status : 'not reported',
    n: finite(value.n) ? value.n : null,
    definition: typeof value.definition === 'string' ? value.definition : '',
    congruence,
    scoreRank,
  };
}

/** Convert the private offline supplement to a small, safely-renderable UI model. */
export function summarizeRobustness(supplement) {
  if (!isRecord(supplement) || supplement.schema_version !== 1) return null;
  const retention = isRecord(supplement.retention) ? supplement.retention : {};
  const fits = Array.isArray(retention.fits) ? retention.fits.filter(isRecord).map(fit => ({
    factorCount: finite(fit.factor_count) ? fit.factor_count : null,
    status: typeof fit.status === 'string' ? fit.status : 'not reported',
    n: finite(fit.n) ? fit.n : null,
    converged: fit.converged === true,
    iterations: finite(fit.iterations) ? fit.iterations : finite(fit.extraction_diagnostics?.iterations) ? fit.extraction_diagnostics.iterations : null,
    heywoodCases: finite(fit.heywood_cases) ? fit.heywood_cases : null,
    residualRms: finite(fit.off_diagonal_residual_RMS) ? fit.off_diagonal_residual_RMS : null,
    crossK: isRecord(fit.cross_k_loading_similarity) ? fit.cross_k_loading_similarity : null,
  })) : [];
  const sameK = [];
  if (isRecord(supplement.same_k_refits)) {
    for (const [key, value] of Object.entries(supplement.same_k_refits)) {
      if (key === 'leave_major_genre_out' && isRecord(value) && Array.isArray(value.groups)) {
        for (const group of value.groups.filter(isRecord)) {
          const label = typeof group.excluded_category === 'string' ? `Leave out · ${group.excluded_category}` : 'Leave one genre out';
          const refit = summarizeRefit(label, group);
          if (refit) { refit.excluded = finite(group.excluded_recordings) ? group.excluded_recordings : null; sameK.push(refit); }
        }
      } else {
        const refit = summarizeRefit(key, value);
        if (refit) sameK.push(refit);
      }
    }
  }
  const duplicates = isRecord(supplement.near_duplicate_candidates) ? supplement.near_duplicate_candidates : null;
  const associations = isRecord(supplement.descriptive_associations) ? supplement.descriptive_associations : null;
  const joint = isRecord(supplement.joint_trends) ? supplement.joint_trends : null;
  return {
    corpusReleaseId: typeof supplement.corpus_release_id === 'string' ? supplement.corpus_release_id : null,
    factorCount: finite(retention.primary_factor_count) ? retention.primary_factor_count : null,
    fits,
    sameK,
    duplicates: duplicates ? {
      method: typeof duplicates.method === 'string' ? duplicates.method : 'Method not reported',
      thresholds: duplicates.thresholds,
      eligibleRecordings: finite(duplicates.eligible_recordings) ? duplicates.eligible_recordings : null,
      pairCount: finite(duplicates.candidate_pair_count) ? duplicates.candidate_pair_count : null,
      clusterCount: finite(duplicates.candidate_cluster_count) ? duplicates.candidate_cluster_count : finite(duplicates.cluster_count) ? duplicates.cluster_count : null,
      largestCluster: finite(duplicates.largest_candidate_cluster_size) ? duplicates.largest_candidate_cluster_size : finite(duplicates.largest_cluster_size) ? duplicates.largest_cluster_size : null,
      recordingsInClusters: finite(duplicates.recordings_in_candidate_clusters) ? duplicates.recordings_in_candidate_clusters : null,
      definition: typeof duplicates.representative_rule === 'string' ? duplicates.representative_rule : '',
      medianContainment: finite(duplicates.median_candidate_containment) ? duplicates.median_candidate_containment : null,
      interpretation: typeof duplicates.interpretation === 'string' ? duplicates.interpretation : '',
      limitations: [duplicates.transitive_cluster_caveat, duplicates.limitations, duplicates.screening_recall_caveat].filter(value => typeof value === 'string' && value).join(' '),
    } : null,
    associations: associations ? {
      method: typeof associations.method === 'string' ? associations.method : typeof associations.adjustment === 'string' ? associations.adjustment : 'Method not reported',
      caveat: typeof associations.caveat === 'string' ? associations.caveat : '',
      length: isRecord(associations.length) ? associations.length : null,
      dateYear: isRecord(associations.date_year) ? associations.date_year : null,
      genre: associations.genre_unadjusted,
      byFactor: isRecord(associations.factor_associations) ? associations.factor_associations : null,
      genreMeans: associations.genre_group_means,
    } : null,
    joint,
  };
}

export function formatRobustnessNumber(value) { return display(value); }

export function factorNames(ids, prefix = 'F') {
  return Array.isArray(ids) ? ids.slice(0, 12).map((id, index) => typeof id === 'string' ? id : `${prefix}${index + 1}`) : [];
}
