import test from 'node:test';
import assert from 'node:assert/strict';
import { factorNames, formatRobustnessNumber, summarizeRobustness } from '../../src/lib/analysis/robustness-ui.mjs';

const sample = {
  schema_version: 1,
  corpus_release_id: 'release-demo',
  retention: {
    primary_factor_count: 9,
    fits: [
      { factor_count: 9, status: 'primary', n: 3984, converged: true, iterations: 50, heywood_cases: 0, off_diagonal_residual_RMS: 0.02 },
      { factor_count: 12, status: 'exploratory', n: 3984, converged: true, iterations: 58, heywood_cases: 0, off_diagonal_residual_RMS: 0.01423,
        cross_k_loading_similarity: { matches: [{ reference_factor: 'F1', candidate_axis: 'A2', absolute_loading_cosine: 0.91 }] } },
      { factor_count: 28, status: 'failed_to_converge', n: 3984, converged: false, iterations: 500, heywood_cases: 0, off_diagonal_residual_RMS: 0.00236 },
    ],
  },
  same_k_refits: {
    higher_transcript_quality: { status: 'proper', n: 2400, definition: 'Higher-quality subset', loading_congruence_to_primary: [0.95, null], aligned_factor_score_spearman_to_primary: [0.9] },
  },
  near_duplicate_candidates: {
    method: 'MinHash candidate screen', eligible_recordings: 100, candidate_pair_count: 3,
    cluster_count: 2, largest_cluster_size: 3, recordings_in_candidate_clusters: 4,
    similarity_score_definition: 'Estimated text overlap', limitations: 'Candidates require review.', thresholds: { jaccard: 0.9 },
  },
  descriptive_associations: { method: 'Descriptive models', caveat: 'Not causal.', length: { pearson: { F1: 0.3 } }, date_year: { F1: -0.1 }, genre_unadjusted: [{ genre: 'vlog', F1: 0.2 }] },
};

test('summarizes the offline supplement without inventing or changing fit status', () => {
  const summary = summarizeRobustness(sample);
  assert.equal(summary.corpusReleaseId, 'release-demo');
  assert.equal(summary.factorCount, 9);
  assert.deepEqual(summary.fits.map(fit => [fit.factorCount, fit.converged]), [[9, true], [12, true], [28, false]]);
  assert.equal(summary.fits[2].residualRms, 0.00236);
  assert.equal(summary.fits[1].crossK.matches[0].candidate_axis, 'A2');
});

test('retains refit sample sizes and missing factor estimates as missing', () => {
  const summary = summarizeRobustness(sample);
  assert.equal(summary.sameK[0].key, 'higher_transcript_quality');
  assert.equal(summary.sameK[0].n, 2400);
  assert.deepEqual(summary.sameK[0].congruence, [0.95, null]);
  assert.deepEqual(summary.sameK[0].scoreRank, [0.9]);
});

test('preserves candidate wording and descriptive-association caveat', () => {
  const summary = summarizeRobustness(sample);
  assert.equal(summary.duplicates.pairCount, 3);
  assert.equal(summary.duplicates.limitations, 'Candidates require review.');
  assert.equal(summary.associations.caveat, 'Not causal.');
  assert.equal(summary.associations.length.pearson.F1, 0.3);
});

test('rejects absent, wrong-version, and malformed supplements safely', () => {
  assert.equal(summarizeRobustness(null), null);
  assert.equal(summarizeRobustness({ ...sample, schema_version: 2 }), null);
  assert.deepEqual(summarizeRobustness({ ...sample, retention: {} }).fits, []);
});

test('formats only finite estimates and caps display factor identifiers', () => {
  assert.equal(formatRobustnessNumber(0.12345), '0.123');
  assert.equal(formatRobustnessNumber(Number.NaN), '—');
  assert.deepEqual(factorNames(['F1', 'F2']), ['F1', 'F2']);
  assert.equal(factorNames(Array.from({ length: 20 }, (_, i) => `F${i + 1}`)).length, 12);
});

test('reads generated artifact diagnostics and preserves approximate-screen limitations', () => {
  const summary = summarizeRobustness({
    ...sample,
    retention: { fits: [{ extraction_diagnostics: { iterations: 41 } }] },
    near_duplicate_candidates: { candidate_cluster_count: 680, largest_candidate_cluster_size: 5, screening_recall_caveat: 'Not exhaustive; asymmetric overlaps may be missed.' },
    descriptive_associations: { adjustment: 'OLS with length, year, and genre controls.' },
  });
  assert.equal(summary.fits[0].iterations, 41);
  assert.equal(summary.duplicates.clusterCount, 680);
  assert.equal(summary.duplicates.largestCluster, 5);
  assert.match(summary.duplicates.limitations, /Not exhaustive/);
  assert.match(summary.associations.method, /OLS/);
});
