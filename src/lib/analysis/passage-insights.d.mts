import type { AnalysisRelease } from './explorer.mjs';
export interface PassageInsights { schema_version:number;question_ids:string[];recording_ids:string[];eligible_recordings:number[];locators:(number|string)[][][];rankings:(number|null)[][][];groups:{dimension:string;label:string;values:(number|null)[][]}[] }
export function validatePassageInsights(data:unknown,release:AnalysisRelease):Promise<PassageInsights|null>;
export function rankPassageVariation(data:PassageInsights,questionId:string,metric?:string):(number|null)[][];
export function coverageSummary(cell:(number|null)[]):{total:number;scored:number;insufficient:number;missing:number;coverage:number|null;entropy:number|null;modelVariance:number|null;betweenVariance:number|null;recordings:number};
export function passageExtremeHref(data:PassageInsights,video:{href:string;revision_id:string},row:(number|null)[],kind?:string,historical?:boolean):string|null;
export function renderPassageInsights(root:HTMLElement,release:AnalysisRelease,data:PassageInsights):void;
