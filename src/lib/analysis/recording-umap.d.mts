import type { AnalysisRelease } from './explorer.mjs';
import type { DiscoveryData } from './discovery.mjs';
export interface RecordingUmap {
  schema_version:1; corpus_release_id:string; questionnaire_version:string; model:string; source_generated_at:string; loadings_sha256:string;
  factor_ids:string[]; cohort_n:number; points:{id:string;x:number;y:number}[];
  standardization:{mean:number[];scale:number[]};
  standardized_input_sha256:string; coordinates_sha256:string;
  method:{algorithm:'UMAP';package:'umap-learn';package_version:string;input:string;note:string;parameters:{n_components:2;n_neighbors:number;min_dist:number;metric:'euclidean';random_state:number;n_jobs:1;low_memory:true}};
}
export function validateRecordingUmap(release:AnalysisRelease,discovery:DiscoveryData,value:unknown):Promise<RecordingUmap|null>;
export function standardizedProfileHash(release:AnalysisRelease,discovery:DiscoveryData):Promise<string>;
export function projectionCoordinateHash(points:{x:number;y:number}[]):Promise<string>;
export function recordingProjection(discovery:DiscoveryData,umap:RecordingUmap|null,requested:string):{id:string;name:string;points:{id:string;x:number;y:number}[];axes:string[];summary:string};
