import {readFile,writeFile,mkdir,rename} from 'node:fs/promises';
import {parseEnv} from 'node:util';
import path from 'node:path';
if(process.env.RAG_PROFILE&&!['public','private'].includes(process.env.RAG_PROFILE))throw Error('Invalid RAG_PROFILE');
export const root=path.resolve(process.env.RAG_PROFILE==='public'?'research/cloudflare-rag/public-v1':'research/cloudflare-rag/pilot-v1');
export const instance=process.env.RAG_PROFILE==='public'?'himr-public-corpus-v1':'himr-private-pilot-v1';
export async function credentials(){const e=parseEnv(await readFile('.env','utf8'));if(!e.CLOUDFLARE_API_TOKEN||!e.CLOUDFLARE_ACCOUNT_ID)throw Error('Missing Cloudflare credentials in .env');return e;}
export async function api(route,{method='GET',body}={}){
  const e=await credentials();const response=await fetch(`https://api.cloudflare.com/client/v4/accounts/${e.CLOUDFLARE_ACCOUNT_ID}/${route}`,{method,headers:{Authorization:`Bearer ${e.CLOUDFLARE_API_TOKEN}`,...(body&&!(body instanceof FormData)?{'Content-Type':'application/json'}:{})},body:body instanceof FormData?body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(60000)});
  const value=await response.json();if(!response.ok||value.success===false){const error=new Error(`Cloudflare ${response.status}: ${JSON.stringify(value.errors||[]).slice(0,1000)}`);error.status=response.status;throw error;}return value.result;
}
export async function save(name,value){await mkdir(root,{recursive:true,mode:0o700});const target=path.join(root,name),tmp=target+'.tmp';await writeFile(tmp,JSON.stringify(value,null,2)+'\n',{mode:0o600});await rename(tmp,target);}
export async function read(name){return JSON.parse(await readFile(path.join(root,name),'utf8'));}
