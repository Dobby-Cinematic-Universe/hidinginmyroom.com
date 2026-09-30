import type {AnalysisRelease} from './explorer.mjs';
export interface ResidualPair {a:string;b:string;observed:number;fitted:number;residual:number}
export interface FactorDiagnostics {schema_version:1;loadings_sha256:string;residual:{question_ids:string[];cohort_n:number;off_diagonal_rms:number;maximum_absolute_residual:number;pairs:ResidualPair[]};held_out:{summary:Record<string,number|null>[];folds:Record<string,unknown>[];method:string;note:string};grouping:{note:string}}
export function validateFactorDiagnostics(release:AnalysisRelease,candidate:unknown):Promise<FactorDiagnostics|null>;
export function residualPairsForQuestion(data:FactorDiagnostics,id:string):ResidualPair[];
export function renderFactorDiagnostics(root:HTMLElement,release:AnalysisRelease,data:FactorDiagnostics):void;
