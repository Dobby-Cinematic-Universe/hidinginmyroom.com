import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { analysisLoadingHash, matchesAnalysisIdentity, validateSafeSupplementTree, validateSupplementPart } from '../../src/lib/analysis/supplement.mjs';

const release = { corpus_release_id:'release-a', questionnaire_version:'bank-a', model:'model-a', generated_at:'2026-09-30', analysis:{factors:[{id:'F1',loadings:{joy:.8}}]} };
const identity = {schema_version:1,corpus_release_id:release.corpus_release_id,questionnaire_version:release.questionnaire_version,model:release.model,source_generated_at:release.generated_at};

const followupRelease = {...release, videos:[{recording_id:'rec-a'},{recording_id:'rec-b'}], questions:[{id:'joy',type:'score'}], analysis:{...release.analysis, factor_question_ids:['joy']}};

test('supplement identity binds corpus, questionnaire, model and scoring version', () => {
  assert.equal(matchesAnalysisIdentity(identity,release),true);
  for (const key of Object.keys(identity)) assert.equal(matchesAnalysisIdentity({...identity,[key]:'changed'},release),false,key);
});

test('factor supplements are bound to the complete loading matrix', async () => {
  const value = {...identity, loadings_sha256:await analysisLoadingHash(release), retention:{fits:[]}};
  assert.equal(await validateSupplementPart(value,release,'robustness'),value);
  await assert.rejects(validateSupplementPart({...value,loadings_sha256:'wrong'},release,'robustness'),/different factor solution/);
  await assert.rejects(validateSupplementPart({...value,retention:null},release,'robustness'),/Missing robustness/);
  await assert.rejects(validateSupplementPart(value,release,'joint_trends'),/Missing joint trend/);
});

test('supplement tree excludes secrets, raw text, paths, nonfinite data and oversized payloads', () => {
  validateSafeSupplementTree({method:'Descriptive transcript length sensitivity',values:[.4,null,true],F1:{mean:-.7}});
  for (const value of [{api_key:'fixture'},{raw_receipt:{}},{input_text:'private text'}, {path:['','home','user','private'].join('/')}, {url:'https://example.test'}, {value:Infinity},{value:'x'.repeat(6001)}, new Array(10001).fill(0)]) {
    assert.throws(()=>validateSafeSupplementTree(value));
  }
});

test('additional assets preserve the existing publication boundary and bounded paths', async () => {
  const assets=await readFile(new URL('../../src/lib/analysis/assets.ts',import.meta.url),'utf8');
  const releaseSource=await readFile(new URL('../../src/lib/analysis/release.ts',import.meta.url),'utf8');
  assert.match(assets,/analysisAssetDirectory/);
  assert.match(assets,/isSymbolicLink/);
  assert.match(assets,/8 \* 1024 \* 1024/);
  assert.match(releaseSource,/if \(!import\.meta\.env\.DEV\) return \{ file: publicReleasePath/);
  assert.match(releaseSource,/path\.dirname\(\(await analysisFilePath\(\)\)\.file\)/);
});

test('new analyses preserve release and factor bindings and fail closed on missing payloads', async () => {
  const base = {...identity, loadings_sha256:await analysisLoadingHash(followupRelease)};
  for (const kind of ['discovery','passage_insights','factor_diagnostics','temporal_comparisons']) {
    await assert.rejects(validateSupplementPart({...base,loadings_sha256:'stale'},followupRelease,kind),/different factor solution/);
    await assert.rejects(validateSupplementPart(base,followupRelease,kind),/Missing/);
  }
  await assert.rejects(validateSupplementPart(base,followupRelease,'private_groups'),/Unrecognized/);
});

test('recording map refuses unknown IDs, duplicate points, and self or nonfinite neighbors', async () => {
  const base={...identity,loadings_sha256:await analysisLoadingHash(followupRelease),pca:{},points:[{id:'rec-a',x:0,y:1},{id:'rec-b',x:2,y:3}],neighbors:[{id:'rec-a',items:[{id:'rec-b',distance:2}]},{id:'rec-b',items:[{id:'rec-a',distance:2}]}]};
  assert.equal(await validateSupplementPart(base,followupRelease,'discovery'),base);
  await assert.rejects(validateSupplementPart({...base,points:[...base.points,base.points[0]]},followupRelease,'discovery'),/Invalid recording map/);
  for(const id of ['unknown','rec-a']) await assert.rejects(validateSupplementPart({...base,neighbors:[{id:'rec-a',items:[{id,distance:2}]}]},followupRelease,'discovery'),/Invalid recording neighbor/);
});

test('passage insights are confined to scored questions and known recordings', async () => {
  const base={...identity,loadings_sha256:await analysisLoadingHash(followupRelease),recording_ids:['rec-a'],question_ids:['joy'],rankings:[[]],groups:[]};
  assert.equal(await validateSupplementPart(base,followupRelease,'passage_insights'),base);
  await assert.rejects(validateSupplementPart({...base,recording_ids:['unknown']},followupRelease,'passage_insights'),/Unknown passage insight recording/);
  await assert.rejects(validateSupplementPart({...base,question_ids:['unknown']},followupRelease,'passage_insights'),/Unknown passage insight question/);
});

test('residual matrices must use the fitted question bank and consistent dimensions', async () => {
  const base={...identity,loadings_sha256:await analysisLoadingHash(followupRelease),residual:{question_ids:['joy'],observed:[[1]],fitted:[[.8]],residuals:[[0]]},held_out:{},grouping:{}};
  assert.equal(await validateSupplementPart(base,followupRelease,'factor_diagnostics'),base);
  await assert.rejects(validateSupplementPart({...base,residual:{...base.residual,question_ids:['unknown']}},followupRelease,'factor_diagnostics'),/Residual questions/);
  await assert.rejects(validateSupplementPart({...base,residual:{...base.residual,fitted:[[.8,.2]]}},followupRelease,'factor_diagnostics'),/Malformed factor residual/);
});
