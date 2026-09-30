import type { AnalysisRelease } from './explorer.mjs';
export interface FactorDescription { label: string; description: string; higher: string; lower: string }
export interface FactorAnnotations { schema_version: number; corpus_release_id: string; questionnaire_version: string; model: string; generated_at: string; loadings_sha256: string; descriptions: Record<string, FactorDescription> }
export function factorLoadingSignature(factors: AnalysisRelease['analysis']['factors']): string;
export function resolveFactorDescriptions(release: AnalysisRelease, annotations: FactorAnnotations): Promise<Record<string, FactorDescription>>;
export function factorDisplayLabel(factor: AnalysisRelease['analysis']['factors'][number], descriptions: Record<string, FactorDescription>): string;
