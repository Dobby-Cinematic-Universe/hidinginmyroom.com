import test from 'node:test';
import assert from 'node:assert/strict';
import { analysisLoadingHash } from '../../src/lib/analysis/supplement.mjs';
import { validatePassageInsights,rankPassageVariation,coverageSummary,passageExtremeHref } from '../../src/lib/analysis/passage-insights.mjs';

async function fixture(){
  const release={corpus_release_id:'release-a',questionnaire_version:'q-a',model:'model-a',generated_at:'today',questions:[{id:'energy',type:'score'}],videos:[{recording_id:'rec-a'},{recording_id:'rec-b'}],analysis:{factors:[]}};
  const data={schema_version:1,corpus_release_id:release.corpus_release_id,questionnaire_version:release.questionnaire_version,model:release.model,source_generated_at:release.generated_at,loadings_sha256:await analysisLoadingHash(release),question_ids:['energy'],recording_ids:['rec-a','rec-b'],locators:[[[0,0,'seg_a'],[1,100,'seg_b']],[[0,1000,'seg_a'],[1,2000,'seg_b']]],eligible_recordings:[2],rankings:[[[0,3,0,0,0,0,0,0,0,100,0,1],[1,4,2,1,1,4,-2,null,1000,2000,0,1]]],groups:[{dimension:'all',label:'all',values:[[10,9,8,1,1,8,.4,.5,1,2]]}]};
  return {release,data};
}
test('passage diagnostics preserve zero values and distinguish missing coverage',()=>{
  const s=coverageSummary([10,9,8,1,1,8,0,0,0,2]);assert.equal(s.entropy,0);assert.equal(s.modelVariance,0);assert.equal(s.missing,1);assert.equal(s.insufficient,1);assert.equal(s.coverage,.8);
});
test('rank metrics order by absolute shift and omit absent adjacency',async()=>{
  const {data}=await fixture();assert.equal(rankPassageVariation(data,'energy','delta')[0][0],1);assert.deepEqual(rankPassageVariation(data,'energy','adjacent').map(r=>r[0]),[0]);assert.deepEqual(rankPassageVariation(data,'missing'),[]);
});
test('extreme transcript links include canonical segment anchors including time zero',async()=>{
  const {data}=await fixture();const video={href:'/corpus/videos/rec-a/',revision_id:'rev-a'};
  assert.equal(passageExtremeHref(data,video,data.rankings[0][0]),'/corpus/videos/rec-a/?t=0#segment-rev-a-seg_a');
  assert.equal(passageExtremeHref(data,video,data.rankings[0][0],'high'),'/corpus/videos/rec-a/?t=0#segment-rev-a-seg_b');
});
test('release binding rejects altered source, cohort, entropy, counters and ranking ids',async()=>{
  const {release,data}=await fixture();assert.equal(await validatePassageInsights(data,release),data);
  for(const change of [d=>d.source_generated_at='old',d=>d.loadings_sha256='wrong',d=>d.recording_ids.reverse(),d=>d.groups[0].values[0][6]=1.1,d=>d.groups[0].values[0][4]=4,d=>d.rankings[0][0][0]=10,d=>d.locators[0][0][1]=2,d=>d.eligible_recordings[0]=8,d=>d.rankings[0].push(d.rankings[0][0])]){const copy=structuredClone(data);change(copy);assert.equal(await validatePassageInsights(copy,release),null);}
});
