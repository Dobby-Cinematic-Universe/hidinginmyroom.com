export interface RobustnessFit {
  factor_count?: number;
  status?: string;
  n?: number;
  converged?: boolean;
  iterations?: number;
  extraction_diagnostics?: { iterations?: number };
  heywood_cases?: number;
  off_diagonal_residual_RMS?: number;
  cross_k_loading_similarity?: Record<string, unknown>;
}

export interface RobustnessRefit {
  status?: string;
  n?: number;
  definition?: string;
  loading_congruence_to_primary?: Array<number | null>;
  aligned_factor_score_spearman_to_primary?: Array<number | null>;
}

export interface RobustnessSupplement {
  schema_version: 1;
  corpus_release_id: string;
  questionnaire_version?: string;
  model?: string;
  source_generated_at?: string;
  loadings_sha256?: string;
  retention?: { primary_factor_count?: number; fits?: RobustnessFit[] };
  same_k_refits?: Record<string, RobustnessRefit>;
  near_duplicate_candidates?: Record<string, unknown>;
  descriptive_associations?: Record<string, unknown>;
  joint_trends?: Record<string, unknown>;
}

export interface RobustnessRefitSummary {
  key: string;
  status: string;
  n: number | null;
  definition: string;
  congruence: Array<number | null>;
  scoreRank: Array<number | null>;
  excluded?: number | null;
}

export function summarizeRobustness(supplement: unknown): {
  corpusReleaseId: string | null;
  factorCount: number | null;
  fits: Array<{ factorCount: number | null; status: string; n: number | null; converged: boolean; iterations: number | null; heywoodCases: number | null; residualRms: number | null; crossK: Record<string, unknown> | null }>;
  sameK: RobustnessRefitSummary[];
  duplicates: { method: string; thresholds: unknown; eligibleRecordings: number | null; pairCount: number | null; clusterCount: number | null; largestCluster: number | null; recordingsInClusters: number | null; definition: string; medianContainment: number | null; interpretation: string; limitations: string } | null;
  associations: { method: string; caveat: string; length: Record<string, unknown> | null; dateYear: Record<string, unknown> | null; genre: unknown; byFactor: Record<string, unknown> | null; genreMeans: unknown } | null;
  joint: Record<string, unknown> | null;
} | null;
export function formatRobustnessNumber(value: number): string;
export function factorNames(ids: unknown, prefix?: string): string[];
