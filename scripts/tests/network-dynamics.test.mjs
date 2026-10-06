import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createNetworkProjection,validateNetworkProjection,networkDigest} from '../../src/lib/corpus/network-dynamics.mjs';

const binding={corpus_release:`release_${'a'.repeat(24)}`,summary_release:`summaries_${'b'.repeat(24)}`,corpus_archive_sha256:'c'.repeat(64),corpus_manifest_sha256:'d'.repeat(64),transcript_input_sha256:'e'.repeat(64),registry_sha256:'f'.repeat(64),builder_sha256:'a'.repeat(64),method_version:'mention-dynamics-public-v1'};
const privateTestPath=['','home','example-user','research'].join('/');
const result=()=>({
  monte_carlo_draws:200,counts:{recordings:12,kept_after_dedupe:11,duplicates_removed:1,undated_recordings:2,expected_mentions_main:20.3,paired_recordings:0},
  entities:['Sunny','Ice Poseidon'],kinds:['partner','public_creator'],years:[2024,2025],months:['2024-01','2024-02'],
  yearly:{exposure_words:[1000,2000],H:[.4,.5],H_lo:[.3,.4],H_hi:[.5,.6],top:[.8,.7],active:[1,2],circle_rate:[20,30]},
  monthly:{n_recordings:[4,0],rate:[[2,null],[3,null]]},
  edges:[{a:'Sunny',b:'Ice Poseidon',weight:5,null:2,lift:2.5}],
  // These private/rejected fields must never enter the public projection.
  tone:{Sunny:{diagnosis:'not for publication'}},copresence:{Sunny:3},review_sample:[{text:'SECRET EXCERPT'}],release_dir:privateTestPath,
});
const fixture=()=>createNetworkProjection(result(),binding,'2026-10-06T12:00:00Z');
const redigest=p=>{p.payload_sha256=networkDigest(p.data);return p;};

test('allowlisted projection preserves aggregate values and excludes private research fields',()=>{
  const p=fixture();assert.equal(p.data.monthly[1].rates.Sunny,null);assert.equal(p.data.names[0].group,'on_channel');assert.equal(p.data.edges[0].lift,2.5);
  assert.deepEqual(p.data.agreement,[]);assert.deepEqual(p.data.bursts,[]);
  const json=JSON.stringify(p);for(const forbidden of ['SECRET EXCERPT','review_sample','diagnosis','copresence',privateTestPath,'release_dir','tone'])assert.ok(!json.includes(forbidden));
  assert.equal(validateNetworkProjection(p,binding),p);
});
test('exact release bindings and payload digest fail closed',()=>{
  for(const key of Object.keys(binding))assert.throws(()=>validateNetworkProjection(fixture(),{...binding,[key]:'different'}),/Stale/);
  const p=fixture();p.data.counts.weighted_matches++;assert.throws(()=>validateNetworkProjection(p,binding),/digest/);
});
test('unexpected keys, unapproved identities and unsafe text are rejected even with recomputed digest',()=>{
  let p=fixture();p.data.review_sample=['secret'];assert.throws(()=>validateNetworkProjection(redigest(p),binding),/fields/);
  p=fixture();p.data.names[0].name='New private identity';assert.throws(()=>validateNetworkProjection(redigest(p),binding),/Unapproved/);
  p=fixture();p.data.names[0].name='<script>alert(1)</script>';assert.throws(()=>validateNetworkProjection(redigest(p),binding),/Invalid/);
  p=fixture();p.binding.private_path=privateTestPath;assert.throws(()=>validateNetworkProjection(p,binding),/fields/);
});
test('finite numbers, interval order, duplicate endpoints and inconsistent counts are rejected',()=>{
  let p=fixture();p.data.annual[0].spread=[.7,.4,.5];assert.throws(()=>validateNetworkProjection(redigest(p),binding),/interval order/);
  p=fixture();p.data.edges[0].lift=Infinity;assert.throws(()=>validateNetworkProjection(redigest(p),binding),/Invalid/);
  p=fixture();p.data.edges.push({...p.data.edges[0],a:'Ice Poseidon',b:'Sunny'});assert.throws(()=>validateNetworkProjection(redigest(p),binding),/Duplicate/);
  p=fixture();p.data.edges[0].b='Unapproved';assert.throws(()=>validateNetworkProjection(redigest(p),binding),/endpoints/);
  p=fixture();p.data.counts.retained_recordings=20;assert.throws(()=>validateNetworkProjection(redigest(p),binding),/Inconsistent/);
});
test('calendar order is enforced and missing-month rates may be null',()=>{
  const p=fixture();assert.equal(validateNetworkProjection(p,binding).data.monthly[1].rates.Sunny,null);
  p.data.monthly.reverse();assert.throws(()=>validateNetworkProjection(redigest(p),binding),/month order/);
});
test('a weighted point estimate may lie outside its conditional resampling band',()=>{
  const p=fixture();p.data.annual[0].spread=[.3,.8,.6];assert.equal(validateNetworkProjection(redigest(p),binding).data.annual[0].spread[1],.8);
});
test('only corrected heuristic calendar episodes project; p-value metadata never exports',()=>{
  const r=result();r.burst_episodes=[{entity:'Sunny',start:'2024-01',end:'2024-02',months:2,p_value:.001}];
  assert.deepEqual(createNetworkProjection(r,binding).data.bursts,[]);
  r.methods={bursts:'Corrected calendar windows. No Poisson p-values for fractional weights.'};
  const p=createNetworkProjection(r,binding);assert.equal(p.data.bursts[0].months,2);assert.ok(!JSON.stringify(p).includes('p_value'));
});
test('UI preserves directory-first ordering and required interpretation/privacy notices',()=>{
  const page=readFileSync(new URL('../../src/pages/corpus/graph/index.astro',import.meta.url),'utf8');
  assert.ok(page.indexOf('<section id="entities">')<page.indexOf('<NetworkDynamics projection='));
  const ui=readFileSync(new URL('../../src/components/corpus/NetworkDynamics.astro',import.meta.url),'utf8');
  for(const text of ['not a social network','unlabelled speech is not attributed to Daniel','not measured probabilities','Machine-generated and unreviewed; may be wrong; not a verified quotation.','accessible alternative','Unknown recording dates','not a significance test','private review records'])assert.ok(ui.includes(text),text);
  assert.ok(!ui.includes('innerHTML'));assert.ok(!ui.includes('https://'));assert.ok(ui.includes('aria-live="polite"'));
});
test('graph imports a distinct server loader, not the same-basename pure projection module',()=>{
  const page=readFileSync(new URL('../../src/pages/corpus/graph/index.astro',import.meta.url),'utf8');
  assert.match(page,/import \{loadNetworkDynamics\} from ['"]\.\.\/\.\.\/\.\.\/lib\/corpus\/network-dynamics-release['"]/);
  const loader=readFileSync(new URL('../../src/lib/corpus/network-dynamics-release.ts',import.meta.url),'utf8');
  assert.match(loader,/export async function loadNetworkDynamics\(/);
  assert.match(loader,/from ['"]\.\/network-dynamics\.mjs['"]/);
});
