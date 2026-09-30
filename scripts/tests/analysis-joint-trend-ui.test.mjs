import test from 'node:test';
import assert from 'node:assert/strict';
import { selectJointTrendSeries } from '../../src/lib/analysis/joint-trend-ui.mjs';

const row=(period,n=10,status='available')=>({period,n,genre_counts:{storytime:n},factors:{F1:{mean:.3,low:.1,high:.5,status,aligned_refits:500}}});
const supplement={joint_trends:{methods:{month_cluster:{method:'month_cluster',requested_refits:500,successful_proper_converged_refits:500,interval_interpretation:'fixed-cohort model-fit stability',periods:{yearly:[row('2024'),row('2025')],monthly:[row('2024-06')]}}}}};
test('joint intervals preserve period sample sizes, actual time and composition',()=>{
  const result=selectJointTrendSeries(supplement,'F1','month',{});
  assert.equal(result.series[0].x,2024+5/12);assert.equal(result.series[0].low,.1);assert.deepEqual(result.series[0].genre_counts,{storytime:10});
});
test('year filtering may select complete periods but category/text subsets cannot inherit cohort bounds',()=>{
  assert.equal(selectJointTrendSeries(supplement,'F1','year',{year:'2025'}).series.length,1);
  for(const filters of [{category:'storytime'},{text:'one video'},{year:'undated'}]) assert.equal(selectJointTrendSeries(supplement,'F1','year',filters),null);
});
test('missing supplement/method and suppressed intervals remain visibly unavailable',()=>{
  assert.equal(selectJointTrendSeries(null,'F1','year',{}),null);
  assert.equal(selectJointTrendSeries(supplement,'F1','year',{},'missing'),null);
  const other=structuredClone(supplement);other.joint_trends.methods.month_cluster.periods.yearly[0].factors.F1.status='suppressed';
  assert.equal(selectJointTrendSeries(other,'F1','year',{}).series[0].low,null);
});
