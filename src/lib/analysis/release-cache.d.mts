export function analysisReleaseIdentity(stat: {dev:bigint;ino:bigint;size:bigint;mtimeNs:bigint;ctimeNs:bigint}): string;
export function analysisReleaseCacheKey(pointerIdentity:string,fileIdentity:string,corpusReleaseId:string):string;
export function createAnalysisReleaseCache(): <T>(key:string,load:()=>Promise<T>)=>Promise<T>;
