import type { APIRoute } from 'astro';
import {loadSummaryRelease} from '../../../../../lib/summaries/release';
import {buildSummaryShards} from '../../../../../lib/summaries/shard-build.mjs';
export async function getStaticPaths(){
  return buildSummaryShards(await loadSummaryRelease()).assets.map(({id,text})=>({params:{id},props:{text}}));
}
export const GET:APIRoute=({props})=>new Response(props.text,{
  headers:{'Content-Type':'application/json; charset=utf-8','X-Content-Type-Options':'nosniff'},
});
