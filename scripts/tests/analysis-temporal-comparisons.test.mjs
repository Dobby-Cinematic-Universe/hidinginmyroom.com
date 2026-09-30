import test from 'node:test';
import assert from 'node:assert/strict';
import {compareDistributions,changeAgreement,calendarX,validateTemporalComparisons} from '../../src/lib/analysis/temporal-comparisons.mjs';
test('cohort contrasts have direction and undefined effects for degenerate groups',()=>{
  assert.equal(compareDistributions({n:10,mean:1,sd:2},{n:10,mean:3,sd:2}).pooledSdEffect,1);
  assert.equal(compareDistributions({n:1,mean:1,sd:null},{n:1,mean:2,sd:null}).pooledSdEffect,null);
  assert.equal(compareDistributions(null,{}),null);
});
test('calendar spacing keeps absent months visible',()=>{
  assert.equal(calendarX('2021-02','2021-01','2021-04',0,300),100);
  assert.equal(calendarX('2021-04','2021-01','2021-04',0,300),300);
});
test('agreement counts settings not duplicate candidates',()=>{
  assert.equal(changeAgreement({sensitivity:[{candidates:[{period:'2020-02'},{period:'2020-02'}]},{candidates:[]}]},'2020-02'),1);
  assert.equal(validateTemporalComparisons({}),null);
});
test('optional payload rejects wrong axes, nonfinite statistics and malformed modes',()=>{
  const distribution={n:10,mean:1,sd:.2,p10:0,q25:.5,median:1,q75:1.5,p90:2};
  const release={analysis:{factors:[{id:'F1'}]}};
  const fixture={cohort:{n:10,dated_n:10,undated_n:0},standardization:{weights:{a:1},rare_genre_rule:'Fixed'},change_method:'Descriptive',trajectories:{F1:[{period:'2020',raw:distribution,genre_counts:{a:10},standardized:{value:1,weight_coverage:1,unsupported_genres:[]}}]},distributions:{F1:{year:[{group:'2020',distribution}],genre:[{group:'a',distribution}]}},changes:{F1:{eligible_months:1,calendar_runs:1,monthly:[{period:'2020-01',n:10,mean:1,genre_counts:{a:10}}],sensitivity:[{minimum_months:3,penalty_multiplier:2,averaging_months:1,candidates:[]}]}}};
  assert.equal(validateTemporalComparisons(fixture,release),fixture);
  assert.equal(validateTemporalComparisons(fixture,{analysis:{factors:[{id:'F2'}]}}),null);
  const broken=structuredClone(fixture);broken.changes.F1.sensitivity[0].candidates=[null];assert.equal(validateTemporalComparisons(broken,release),null);
  const unsupported=structuredClone(fixture);unsupported.trajectories.F1[0].standardized.weight_coverage=.5;assert.equal(validateTemporalComparisons(unsupported,release),null);
  const nonfinite=structuredClone(fixture);nonfinite.distributions.F1.genre[0].distribution.mean=Infinity;assert.equal(validateTemporalComparisons(nonfinite,release),null);
});
