import assert from 'node:assert/strict';
import { test } from 'node:test';
import { completeFactorProfile, standardizedFactorDistance, validateDiscovery } from '../../src/lib/analysis/discovery.mjs';
import { analysisLoadingHash } from '../../src/lib/analysis/supplement.mjs';

const release = {schema_version:1,corpus_release_id:'r',questionnaire_version:'q',model:'m',generated_at:'t',analysis:{factors:[{id:'F1',loadings:{q:1}},{id:'F2',loadings:{q:0}}]},videos:[{recording_id:'a',factor_scores:{F1:{value:-1},F2:{value:0}}},{recording_id:'b',factor_scores:{F1:{value:1},F2:{value:0}}},{recording_id:'excluded',factor_scores:{}}]};
async function fixture(){return {schema_version:1,corpus_release_id:'r',questionnaire_version:'q',model:'m',source_generated_at:'t',loadings_sha256:await analysisLoadingHash(release),factor_ids:['F1','F2'],cohort_n:2,total_recordings:3,excluded_recordings:1,pca:{mean:[0,0],scale:[Math.sqrt(2),1],components:[[1,0],[0,1]],explained_variance_ratio:[1,0],cumulative_variance_ratio:[1,1]},points:[{id:'a',x:-1/Math.sqrt(2),y:0,radius:1/Math.sqrt(2)},{id:'b',x:1/Math.sqrt(2),y:0,radius:1/Math.sqrt(2)}],neighbors:[{id:'a',items:[{id:'b',distance:Math.sqrt(2)}]},{id:'b',items:[{id:'a',distance:Math.sqrt(2)}]}]};}
test('complete profiles exclude missing and nonfinite values',()=>{assert.equal(completeFactorProfile(release.videos[2],['F1','F2']),null);assert.equal(completeFactorProfile({factor_scores:{F1:{value:NaN}}},['F1']),null);});
test('distances use all standardized dimensions',()=>assert.equal(standardizedFactorDistance([0,0],[3,8],[1,2]),5));
test('artifact matches source projections and neighbor distances',async()=>assert.ok(await validateDiscovery(release,await fixture())));
test('rejects stale identity, changed projection, self-neighbor and distance',async()=>{for(const change of [d=>d.source_generated_at='old',d=>d.points[0].x=99,d=>d.neighbors[0].items[0].id='a',d=>d.neighbors[0].items[0].distance=99,d=>d.loadings_sha256='wrong']){const d=await fixture();change(d);assert.equal(await validateDiscovery(release,d),null);}});
