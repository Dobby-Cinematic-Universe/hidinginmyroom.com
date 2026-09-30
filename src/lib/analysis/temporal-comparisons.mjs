export function compareDistributions(first, second) {
  if (!first || !second || !Number.isFinite(first.mean) || !Number.isFinite(second.mean)) return null;
  const difference = second.mean - first.mean;
  const df = first.n + second.n - 2;
  const variance = df > 0 && Number.isFinite(first.sd) && Number.isFinite(second.sd)
    ? ((first.n - 1) * first.sd ** 2 + (second.n - 1) * second.sd ** 2) / df : null;
  return { difference, pooledSdEffect: variance !== null && variance > 1e-12 ? difference / Math.sqrt(variance) : null };
}

export function validateTemporalComparisons(value, release) {
  const object = v => v !== null && typeof v === 'object' && !Array.isArray(v);
  const finite = v => typeof v === 'number' && Number.isFinite(v);
  const count = v => Number.isSafeInteger(v) && v >= 0;
  const month = v => typeof v === 'string' && /^(?:19|20|21)\d{2}-(?:0[1-9]|1[0-2])$/.test(v);
  const year = v => typeof v === 'string' && /^(?:19|20|21)\d{2}$/.test(v);
  const counts = v => object(v) && Object.entries(v).every(([k,n])=>k.length>0&&k.length<100&&count(n));
  const distribution = v => object(v) && count(v.n) && v.n>0 && finite(v.mean) && (v.sd === null || finite(v.sd)&&v.sd>=0)
    && ['p10','q25','median','q75','p90'].every(k=>finite(v[k])) && v.p10<=v.q25&&v.q25<=v.median&&v.median<=v.q75&&v.q75<=v.p90;
  if (!object(value) || !object(value.trajectories) || !object(value.distributions) || !object(value.changes) || !object(value.standardization) || !object(value.cohort)) return null;
  const ids=release?.analysis?.factors?.map(f=>f.id);
  if (!Array.isArray(ids)||!ids.length||ids.some(id=>typeof id!=='string')) return null;
  if ([value.trajectories,value.distributions,value.changes].some(v=>Object.keys(v).length!==ids.length||ids.some(id=>!Object.hasOwn(v,id)))) return null;
  const c=value.cohort;
  if (![c.n,c.dated_n,c.undated_n].every(count)||c.dated_n+c.undated_n!==c.n) return null;
  const weights=value.standardization.weights;
  if (!object(weights)||Object.values(weights).some(v=>!finite(v)||v<0||v>1)||Object.keys(weights).length>100) return null;
  if (c.dated_n&&Math.abs(Object.values(weights).reduce((a,b)=>a+b,0)-1)>1e-6) return null;
  if (typeof value.change_method!=='string'||typeof value.standardization.rare_genre_rule!=='string') return null;
  for (const id of ids) {
    const rows=value.trajectories[id],d=value.distributions[id],ch=value.changes[id];
    if (!Array.isArray(rows)||rows.length>200||!object(d)||!object(ch)) return null;
    if (rows.some((r,i)=>!object(r)||!year(r.period)||i&&r.period<=rows[i-1].period||!distribution(r.raw)||!counts(r.genre_counts)||!object(r.standardized)||r.standardized.value!==null&&!finite(r.standardized.value)||!finite(r.standardized.weight_coverage)||r.standardized.weight_coverage<0||r.standardized.weight_coverage>1.000001||!Array.isArray(r.standardized.unsupported_genres)||r.standardized.unsupported_genres.some(g=>typeof g!=='string'||!Object.hasOwn(weights,g))||r.standardized.value!==null&&r.standardized.weight_coverage<.999999)) return null;
    if (['year','genre'].some(k=>!Array.isArray(d[k])||d[k].length>200||d[k].some(g=>!object(g)||typeof g.group!=='string'||g.group.length>100||!distribution(g.distribution)))) return null;
    if(d.year.length!==rows.length||d.year.some((g,i)=>g.group!==rows[i].period)||new Set(d.genre.map(g=>g.group)).size!==d.genre.length)return null;
    if (!count(ch.eligible_months)||!count(ch.calendar_runs)||!Array.isArray(ch.monthly)||ch.monthly.length>2500||ch.eligible_months!==ch.monthly.length||!Array.isArray(ch.sensitivity)||!ch.sensitivity.length||ch.sensitivity.length>20) return null;
    if(ch.monthly.some((p,i)=>!object(p)||!month(p.period)||i&&p.period<=ch.monthly[i-1].period||!count(p.n)||p.n<10||!finite(p.mean)||!counts(p.genre_counts))) return null;
    for(const mode of ch.sensitivity){
      if(!object(mode)||!count(mode.minimum_months)||mode.minimum_months<1||!finite(mode.penalty_multiplier)||mode.penalty_multiplier<=0||![1,2].includes(mode.averaging_months)||!Array.isArray(mode.candidates)||mode.candidates.length>2500)return null;
      if(mode.candidates.some(p=>!object(p)||!month(p.period)||!month(p.before_start)||!month(p.after_end)||p.before_start>=p.period||p.after_end<p.period||!distribution(p.before)||!distribution(p.after)||p.pooled_sd_effect!==null&&!finite(p.pooled_sd_effect)||!counts(p.before_genre_counts)||!counts(p.after_genre_counts)))return null;
    }
  }
  return value;
}

export function changeAgreement(changes, period) {
  return changes.sensitivity.filter(mode => mode.candidates.some(candidate => candidate.period === period)).length;
}

export function calendarX(period, start, end, left = 45, width = 610) {
  const ordinal = value => Number(value.slice(0, 4)) * 12 + Number(value.slice(5, 7) || 1) - 1;
  return left + width * (ordinal(period) - ordinal(start)) / Math.max(1, ordinal(end) - ordinal(start));
}
