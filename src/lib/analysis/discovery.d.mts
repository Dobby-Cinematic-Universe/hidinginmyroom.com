import type { AnalysisRelease, AnalysisVideo } from './explorer.mjs';
import type { RecordingUmap } from './recording-umap.mjs';
export interface DiscoveryData {
  schema_version:1; corpus_release_id:string; questionnaire_version:string; model:string; source_generated_at:string; generated_at:string; loadings_sha256:string;
  factor_ids:string[]; cohort_n:number; total_recordings:number; excluded_recordings:number;
  pca:{method:string;mean:number[];scale:number[];components:number[][];explained_variance_ratio:number[];cumulative_variance_ratio:number[]};
  points:{id:string;x:number;y:number;radius:number}[];
  neighbors:{id:string;items:{id:string;distance:number}[]}[];
}
export function completeFactorProfile(video:AnalysisVideo,ids:string[]):number[]|null;
export function standardizedFactorDistance(a:number[],b:number[],scales:number[]):number;
export function validateDiscovery(release:AnalysisRelease,value:unknown):Promise<DiscoveryData|null>;
export function renderDiscovery(root:HTMLElement,release:AnalysisRelease,data:DiscoveryData,umap?:RecordingUmap|null):void;
