import type { AnalysisRelease } from './explorer.mjs';
export type AnalysisSupplementPart = Record<string, any>;
export const supplementKinds: string[];
export interface AnalysisSupplement {
  robustness?: AnalysisSupplementPart;
  joint_trends?: AnalysisSupplementPart;
  map_diagnostics?: AnalysisSupplementPart;
  discovery?: AnalysisSupplementPart;
  recording_umap?: AnalysisSupplementPart;
  passage_insights?: AnalysisSupplementPart;
  factor_diagnostics?: AnalysisSupplementPart;
  temporal_comparisons?: AnalysisSupplementPart;
}
export function matchesAnalysisIdentity(value: unknown, release: AnalysisRelease): boolean;
export function validateSafeSupplementTree(value: unknown, depth?: number): void;
export function analysisLoadingHash(release: AnalysisRelease): Promise<string>;
export function validateSupplementPart(value: unknown, release: AnalysisRelease, kind: string): Promise<AnalysisSupplementPart>;
export function loadAnalysisSupplement(release: AnalysisRelease): Promise<AnalysisSupplement | null>;
