import type { AnalysisRelease, AnalysisVideo } from './explorer.mjs';
export interface PassageScore { value:number|null; low:number|null; high:number|null; coverage:number; applicable:boolean }
export interface AnalysisPassage { passage_id:string; revision_id:string; chunk_index:number; start_ms:number; end_ms:number; source:{href:string;segment_ids:string[]}; scores:Record<string,PassageScore> }
export interface AnalysisPassageData { schema_version:1; corpus_release_id:string; questionnaire_version:string; model:string; source_generated_at:string; recording:{recording_id:string;revision_id:string}; passages:AnalysisPassage[] }
export function validatePassageData(value:unknown, release:AnalysisRelease, video:AnalysisVideo):AnalysisPassageData;
