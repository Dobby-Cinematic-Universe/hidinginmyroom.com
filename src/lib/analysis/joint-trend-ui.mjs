/** Choose precomputed fixed-cohort stability ranges without applying them to a different subset. */
export function selectJointTrendSeries(supplement, metric, interval, filters, method = 'month_cluster') {
  if (filters.category || filters.text?.trim() || filters.year === 'undated') return null;
  const result = supplement?.joint_trends?.methods?.[method];
  const rows = result?.periods?.[interval === 'month' ? 'monthly' : 'yearly'];
  if (!Array.isArray(rows)) return null;
  const series = rows.filter(row => !filters.year || row.period.slice(0,4) === filters.year).flatMap(row => {
    const score = row.factors?.[metric];
    if (!score || !Number.isFinite(score.mean) || !Number.isInteger(row.n) || row.n < 1) return [];
    const boundsAvailable = score.status === 'available' && Number.isFinite(score.low) && Number.isFinite(score.high) && score.low <= score.high;
    return [{period:row.period,x:interval==='month'?Number(row.period.slice(0,4))+(Number(row.period.slice(5,7))-1)/12:Number(row.period.slice(0,4)),mean:score.mean,low:boundsAvailable?score.low:null,high:boundsAvailable?score.high:null,n:row.n,coverage:1,genre_counts:row.genre_counts??{},aligned_refits:score.aligned_refits??null}];
  });
  return {series, method:result.method, requested_refits:result.requested_refits, successful_refits:result.successful_proper_converged_refits, note:result.interval_interpretation};
}
