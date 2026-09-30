import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { analysisLoadingHash, validateSupplementPart } from '../../src/lib/analysis/supplement.mjs';
import { projectionCoordinateHash, recordingProjection, standardizedProfileHash, validateRecordingUmap } from '../../src/lib/analysis/recording-umap.mjs';

const release={corpus_release_id:'r',questionnaire_version:'q',model:'m',generated_at:'t',analysis:{factors:[{id:'F1',loadings:{a:1}},{id:'F2',loadings:{b:1}}]},videos:[0,1,2,3].map(i=>({recording_id:`rec_${i}`,factor_scores:{F1:{value:i},F2:{value:3-i}}}))};
const discovery={factor_ids:['F1','F2'],cohort_n:4,pca:{mean:[1.5,1.5],scale:[1,1],explained_variance_ratio:[.7,.3],cumulative_variance_ratio:[.7,1]},points:release.videos.map((v,i)=>({id:v.recording_id,x:i,y:-i}))};
async function fixture(){
  const points=discovery.points.map(p=>({...p,x:p.x+.25}));
  return {schema_version:1,corpus_release_id:'r',questionnaire_version:'q',model:'m',source_generated_at:'t',loadings_sha256:await analysisLoadingHash(release),factor_ids:discovery.factor_ids,cohort_n:4,standardization:{mean:[1.5,1.5],scale:[1,1]},points,
    standardized_input_sha256:await standardizedProfileHash(release,discovery),coordinates_sha256:await projectionCoordinateHash(points),
    method:{algorithm:'UMAP',package:'umap-learn',package_version:'0.5.9.post2',parameters:{n_components:2,n_neighbors:3,min_dist:.2,metric:'euclidean',random_state:20260930,n_jobs:1,low_memory:true}}};
}
test('UMAP binds the full source cohort, standardized inputs and coordinates',async()=>{
  const value=await fixture();assert.equal(await validateRecordingUmap(release,discovery,value),value);
  assert.equal(await validateSupplementPart(value,release,'recording_umap'),value);
});
test('UMAP rejects stale, partial, reordered, foreign, nonfinite and corrupt artifacts',async()=>{
  for(const mutate of [d=>d.source_generated_at='old',d=>d.points.pop(),d=>d.points.reverse(),d=>d.points[0].id='unknown',d=>d.points[0].x=Infinity,d=>d.points[0].y=17,d=>d.standardization.scale[0]=2,d=>d.method.parameters.random_state=null,d=>d.method.parameters.n_jobs=4,d=>d.standardized_input_sha256='0'.repeat(64)]){
    const value=await fixture();mutate(value);assert.equal(await validateRecordingUmap(release,discovery,value),null);
  }
  const changed=structuredClone(release);changed.videos[0].factor_scores.F1.value=.1;
  assert.equal(await validateRecordingUmap(changed,discovery,await fixture()),null);
});
test('projection selection defaults and falls back to PCA without substituting variance claims',async()=>{
  const value=await fixture();const before=structuredClone(discovery);
  assert.equal(recordingProjection(discovery,value,'pca').points,discovery.points);
  assert.equal(recordingProjection(discovery,null,'umap').id,'pca');
  const nonlinear=recordingProjection(discovery,value,'umap');assert.equal(nonlinear.points,value.points);assert.deepEqual(nonlinear.axes,['UMAP 1','UMAP 2']);assert.match(nonlinear.summary,/no explained-variance percentage/);
  assert.deepEqual(discovery,before);
});
test('map switch redraws only the projection and preserves full-profile neighbors',async()=>{
  const source=await readFile(new URL('../../src/lib/analysis/discovery.mjs',import.meta.url),'utf8');
  assert.match(source,/view\.addEventListener\('change',draw\)/);
  assert.match(source,/chart\.dataset\.projection===projection\.id/);
  assert.match(source,/const nearby=neighbors\.get\(selected\)/);
  assert.match(source,/const point=projectionPoints\.get\(selected\)/);
});
