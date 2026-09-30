import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {analysisLoadingHash} from '../../src/lib/analysis/supplement.mjs';
import {validateFactorDiagnostics,residualPairsForQuestion,residualTableRows} from '../../src/lib/analysis/factor-diagnostics.mjs';

const release={corpus_release_id:'r',questionnaire_version:'q',model:'m',generated_at:'t',analysis:{primary_factor_n:30,factor_question_ids:['a','b','c'],factors:[{id:'F1',loadings:{a:.5,b:.4,c:.2}}]}};
async function fixture(){return {schema_version:1,corpus_release_id:'r',questionnaire_version:'q',model:'m',source_generated_at:'t',loadings_sha256:await analysisLoadingHash(release),
  residual:{cohort_n:30,question_ids:['a','b','c'],observed:[[1,.3,.1],[.3,1,.2],[.1,.2,1]],fitted:[[.25,.2,.1],[.2,.16,.05],[.1,.05,.04]],residuals:[[0,.1,0],[.1,0,.15],[0,.15,0]],off_diagonal_rms:Math.sqrt((.1**2+.15**2)/3),maximum_absolute_residual:.15,
    pairs:[{a:'b',b:'c',observed:.2,fitted:.05,residual:.15},{a:'a',b:'b',observed:.3,fitted:.2,residual:.1},{a:'a',b:'c',observed:.1,fitted:.1,residual:0}]},held_out:{summary:[8,9,10,12].map(k=>({factor_count:k,valid_folds:0,evaluated_recordings:0,mse:null,standardized_mse:null,baseline_mse_same_folds:null})),folds:[]}};}

test('accepts release-matched algebraically consistent residual diagnostics',async()=>{const data=await fixture();assert.equal(await validateFactorDiagnostics(release,data),data);assert.deepEqual(residualPairsForQuestion(data,'b').map(x=>x.residual),[.15,.1]);});
test('rejects stale cohort or loadings',async()=>{const data=await fixture();data.residual.cohort_n=31;assert.equal(await validateFactorDiagnostics(release,data),null);data.residual.cohort_n=30;data.loadings_sha256='stale';assert.equal(await validateFactorDiagnostics(release,data),null);});
test('rejects inconsistent, asymmetric or duplicate residual pairs',async()=>{let data=await fixture();data.residual.residuals[0][1]=.9;assert.equal(await validateFactorDiagnostics(release,data),null);data=await fixture();data.residual.pairs[1]={...data.residual.pairs[0]};assert.equal(await validateFactorDiagnostics(release,data),null);});
test('rejects aggregate factor comparisons on unequal held-out folds',async()=>{const data=await fixture();data.held_out.summary[0].valid_folds=1;assert.equal(await validateFactorDiagnostics(release,data),null);});
test('factor diagnostics constrains grid children and explicitly labels the residual selector',()=>{
  const component=readFileSync(new URL('../../src/components/corpus/AnalysisFactorDiagnostics.astro',import.meta.url),'utf8');
  assert.match(component,/\.analysis-factor-diagnostics > \*\{min-width:0;max-width:100%\}/);
  assert.match(component,/\[data-held-out-folds\] > section\{min-width:0;max-width:100%\}/);
  assert.match(component,/<label for="factor-residual-question">Inspect residual relationships<\/label>/);
  assert.match(component,/<select id="factor-residual-question" data-residual-question>/);
});
test('selected residual questions expose every relationship while the overview stays compact',()=>{
  const data={residual:{pairs:Array.from({length:52},(_,i)=>({a:'selected',b:`q${i}`,residual:i/100}))}};
  assert.equal(residualTableRows(data,'').length,15);
  assert.equal(residualTableRows(data,'selected').length,52);
  assert.equal(residualTableRows(data,'selected')[0].b,'q51');
});
test('reconstruction chart precedes disclosed numeric tables and residual inspection',()=>{
  const component=readFileSync(new URL('../../src/components/corpus/AnalysisFactorDiagnostics.astro',import.meta.url),'utf8');
  assert.ok(component.indexOf('data-held-out-chart')<component.indexOf('data-held-out-summary'));
  assert.ok(component.indexOf('data-held-out-chart')<component.indexOf('data-residual-question'));
  assert.match(component,/<details><summary>Expand reconstruction errors and baseline values<\/summary><div data-held-out-summary>/);
  assert.match(component,/<details><summary data-residual-table-summary>/);
});
