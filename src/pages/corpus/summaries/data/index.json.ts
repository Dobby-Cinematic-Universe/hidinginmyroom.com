import type { APIRoute } from 'astro';
import {loadSummaryRelease} from '../../../../lib/summaries/release';
import {buildSummaryShards} from '../../../../lib/summaries/shard-build.mjs';
export const GET:APIRoute=async()=>new Response(JSON.stringify(buildSummaryShards(await loadSummaryRelease()).index),{
  headers:{'Content-Type':'application/json; charset=utf-8','X-Content-Type-Options':'nosniff'},
});
