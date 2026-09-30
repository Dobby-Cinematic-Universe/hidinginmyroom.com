import type { AnalysisRelease } from './explorer.mjs';

export interface MapDiagnosticPair {
  a: string; b: string; correlation: number; chord_distance: number; map_distance: number; n: number;
}
export interface MapDiagnostics {
  schema_version: 1;
  corpus_release_id: string;
  questionnaire_version: string;
  model: string;
  source_generated_at: string;
  generated_at: string;
  question_ids: string[];
  correlation_signature: string;
  cohort_n: number;
  clustering: {method: string; leaf_order: string[]; note: string};
  stress_curve: {method: string; points: {dimensions: number; normalized_rms_error: number}[]; note: string};
  shepard: {method: string; points: MapDiagnosticPair[]};
}
export function correlationSignature(release: AnalysisRelease): string;
export function validateMapDiagnostics(release: AnalysisRelease, candidate: unknown): Promise<MapDiagnostics | null>;
export function rankedRelationships(release: AnalysisRelease, questionId: string): {id: string; correlation: number; n: number}[];
export function correlationColor(value: number | null): string;
export function renderQuestionRelationships(root: HTMLElement, release: AnalysisRelease, diagnostics?: MapDiagnostics | null): void;
