import type { AnalysisSupplement } from './supplement.mjs';
export interface JointTrendRow { period:string; x:number; mean:number; low:number|null; high:number|null; n:number; coverage:number; genre_counts:Record<string,number>; aligned_refits:number|null }
export function selectJointTrendSeries(supplement:AnalysisSupplement|null, metric:string, interval:'year'|'month', filters:Record<string,string>, method?:string):{series:JointTrendRow[];method:string;requested_refits:number;successful_refits:number;note:string}|null;
