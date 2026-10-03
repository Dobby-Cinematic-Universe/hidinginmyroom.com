const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const finite = (value) => typeof value === 'number' && Number.isFinite(value);
const str = (value) => typeof value === 'string' && value.length > 0;

function requireValue(condition, field) {
  if (!condition) throw new Error(`Invalid transcript analysis release field: ${field}`);
}

/** Validate the deliberately small, public-facing projection before the browser sees it. */
export function validateAnalysisRelease(value) {
  requireValue(isObject(value), 'root');
  exactKeys(value, ['schema_version','status','corpus_release_id','questionnaire_version','model','generated_at','coverage','questions','videos','analysis','historical_input'], 'root', ['schema_version','status','corpus_release_id','questionnaire_version','model','generated_at','coverage','questions','videos','analysis']);
  if (value.historical_input !== undefined) {
    exactKeys(value.historical_input, ['active_corpus_release_id','basis'], 'historical_input');
    requireValue(/^release_[a-f0-9]{24}$/.test(value.historical_input.active_corpus_release_id) && value.historical_input.active_corpus_release_id !== value.corpus_release_id && value.historical_input.basis === 'reviewed_transcript_update_historical_scores', 'historical_input');
  }
  requireValue(value.schema_version === 1, 'schema_version');
  requireValue(['pilot', 'complete'].includes(value.status), 'status');
  for (const key of ['corpus_release_id', 'questionnaire_version', 'model', 'generated_at']) requireValue(str(value[key]), key);
  requireValue(isObject(value.coverage), 'coverage');
  for (const key of ['total_recordings', 'scored_recordings', 'total_chunks', 'scored_chunks']) requireValue(Number.isInteger(value.coverage[key]) && value.coverage[key] >= 0, `coverage.${key}`);
  exactKeys(value.coverage, ['total_recordings','scored_recordings','total_chunks','scored_chunks'], 'coverage');
  requireValue(value.coverage.scored_recordings <= value.coverage.total_recordings && value.coverage.scored_chunks <= value.coverage.total_chunks, 'coverage.bounds');
  if(value.status==='complete') requireValue(value.coverage.scored_recordings===value.coverage.total_recordings&&value.coverage.scored_chunks===value.coverage.total_chunks,'coverage.complete');
  requireValue(Array.isArray(value.questions), 'questions');
  const questionIds = new Set();
  for (const [i, q] of value.questions.entries()) {
    requireValue(isObject(q), `questions.${i}`);
    exactKeys(q, ['id','short_name','domain','type','instructions','criteria','factor_eligible','applicability','sources'], `questions.${i}`, ['id','short_name','domain','type','instructions','criteria','factor_eligible']);
    for (const key of ['id', 'short_name', 'domain', 'instructions']) requireValue(str(q[key]), `questions.${i}.${key}`);
    requireValue(['score','choice','noul'].includes(q.type), `questions.${i}.type`);
    requireValue((Array.isArray(q.criteria) && q.criteria.length > 0 && q.criteria.every(str)) || (isObject(q.criteria) && Object.keys(q.criteria).length > 0 && Object.values(q.criteria).every(str)), `questions.${i}.criteria`);
    requireValue(typeof q.factor_eligible === 'boolean', `questions.${i}.factor_eligible`);
    if(q.type==='score') requireValue(Array.isArray(q.criteria) && q.criteria.length===5, `questions.${i}.score_anchors`);
    else requireValue(!q.factor_eligible, `questions.${i}.factor_eligible_non_score`);
    if(q.applicability!==undefined) requireValue(str(q.applicability), `questions.${i}.applicability`);
    if(q.sources!==undefined) requireValue(Array.isArray(q.sources)&&q.sources.every(str), `questions.${i}.sources`);
    requireValue(!questionIds.has(q.id), `questions.${i}.duplicate_id`); questionIds.add(q.id);
  }
  requireValue(Array.isArray(value.videos), 'videos');
  const videoIds = new Set();
  const factorIds = new Set();
  for (const factor of value.analysis?.factors ?? []) if (isObject(factor) && str(factor.id)) factorIds.add(factor.id);
  for (const [i, video] of value.videos.entries()) {
    requireValue(isObject(video), `videos.${i}`);
    exactKeys(video, ['recording_id','revision_id','title','href','date','date_basis','category','coverage','question_scores','factor_scores','classifications','noul_scores','scored_passage'], `videos.${i}`, ['recording_id','revision_id','title','href','date','date_basis','category','coverage','question_scores','factor_scores']);
    for (const key of ['recording_id', 'revision_id', 'title', 'href', 'date_basis']) requireValue(str(video[key]), `videos.${i}.${key}`);
    requireValue(video.href.startsWith('/corpus/videos/'), `videos.${i}.href`);
    requireValue(video.date === null || str(video.date), `videos.${i}.date`);
    requireValue(str(video.date_basis), `videos.${i}.date_basis`);
    requireValue(video.category === null || str(video.category), `videos.${i}.category`);
    requireValue(finite(video.coverage) && video.coverage >= 0 && video.coverage <= 1, `videos.${i}.coverage`);
    if (video.scored_passage !== undefined && video.scored_passage !== null) {
      validateEvidence(video.scored_passage, `videos.${i}.scored_passage`);
      requireValue(new URL(video.scored_passage.href, 'https://local.invalid').pathname === new URL(video.href, 'https://local.invalid').pathname, `videos.${i}.scored_passage.wrong_recording`);
    }
    requireValue(isObject(video.question_scores), `videos.${i}.question_scores`);
    requireValue(isObject(video.factor_scores), `videos.${i}.factor_scores`);
    requireValue(!videoIds.has(video.recording_id), `videos.${i}.duplicate_recording_id`); videoIds.add(video.recording_id);
    for (const [id, score] of Object.entries(video.question_scores)) {
      requireValue(value.questions.some(q=>q.id===id&&q.type==='score'), `videos.${i}.question_scores.${id}.unknown_or_non_numeric_question`);
      validateScore(score, `videos.${i}.question_scores.${id}`, true);
    }
    for (const [id, score] of Object.entries(video.factor_scores)) {
      requireValue(factorIds.has(id), `videos.${i}.factor_scores.${id}.unknown_factor`);
      if (score === null) continue;
      exactKeys(score, ['value','low','high','uncertainty'], `videos.${i}.factor_scores.${id}`);
      requireValue(finite(score.value) && (score.low===null||finite(score.low)) && (score.high===null||finite(score.high)) && ['bootstrap_stability','bootstrap_stability_unavailable'].includes(score.uncertainty), `videos.${i}.factor_scores.${id}`);
      const hasBounds=finite(score.low)&&finite(score.high);
      requireValue(score.uncertainty==='bootstrap_stability'?hasBounds:!hasBounds,`videos.${i}.factor_scores.${id}.uncertainty_bounds`);
      requireValue(!hasBounds||score.low<=score.high,`videos.${i}.factor_scores.${id}.interval_order`);
    }
    for(const [id,classification] of Object.entries(video.classifications??{})) {
      const q=value.questions.find(item=>item.id===id);
      requireValue(q&&q.type==='choice'&&isObject(classification), `videos.${i}.classifications.${id}`);
      exactKeys(classification,['choice','probabilities','confidence','coverage'],`videos.${i}.classifications.${id}`);
      requireValue(classification.choice===null||Object.hasOwn(q.criteria,classification.choice),`videos.${i}.classifications.${id}.choice`);
      requireValue(isObject(classification.probabilities)&&Object.entries(classification.probabilities).every(([key,n])=>Object.hasOwn(q.criteria,key)&&finite(n)&&n>=0&&n<=1),`videos.${i}.classifications.${id}.probabilities`);
      requireValue(classification.confidence===null||finite(classification.confidence)&&classification.confidence>=0&&classification.confidence<=1,`videos.${i}.classifications.${id}.confidence`);
      requireValue(classification.coverage===null||finite(classification.coverage)&&classification.coverage>=0&&classification.coverage<=1,`videos.${i}.classifications.${id}.coverage`);
    }
    for(const [id,probability] of Object.entries(video.noul_scores??{})) requireValue(value.questions.some(q=>q.id===id&&q.type==='noul')&&(probability===null||finite(probability)&&probability>=0&&probability<=1),`videos.${i}.noul_scores.${id}`);
  }
  requireValue(value.videos.length===value.coverage.scored_recordings,'coverage.video_count');
  requireValue(isObject(value.analysis), 'analysis');
  const analysis = value.analysis;
  exactKeys(analysis,['status','n','question_ids','factor_question_ids','primary_factor_n','correlations','pair_counts','mds','mds_stress','mds_method','factors','diagnostics'],'analysis',['status','n','question_ids','factor_question_ids','correlations','pair_counts','mds','mds_stress','mds_method','factors','diagnostics']);
  requireValue(['insufficient_data', 'exploratory'].includes(analysis.status), 'analysis.status');
  requireValue(Number.isInteger(analysis.n) && analysis.n >= 0, 'analysis.n');
  requireValue(Array.isArray(analysis.question_ids) && analysis.question_ids.every((id) => value.questions.some(q=>q.id===id&&q.type==='score')), 'analysis.question_ids');
  requireValue(Array.isArray(analysis.factor_question_ids) && analysis.factor_question_ids.every((id) => value.questions.some(q=>q.id===id&&q.type==='score'&&q.factor_eligible)), 'analysis.factor_question_ids');
  requireValue(analysis.primary_factor_n===undefined||(Number.isInteger(analysis.primary_factor_n)&&analysis.primary_factor_n>=0), 'analysis.primary_factor_n');
  const n = analysis.question_ids.length;
  requireValue(Array.isArray(analysis.correlations) && analysis.correlations.length === n && analysis.correlations.every((row) => Array.isArray(row) && row.length === n && row.every((x) => x === null || (finite(x) && x >= -1 && x <= 1))), 'analysis.correlations');
  requireValue(Array.isArray(analysis.pair_counts) && analysis.pair_counts.length === n && analysis.pair_counts.every((row) => Array.isArray(row) && row.length === n && row.every((x) => Number.isInteger(x) && x >= 0)), 'analysis.pair_counts');
  requireValue(Array.isArray(analysis.mds) && analysis.mds.every((point) => isObject(point) && analysis.question_ids.includes(point.id) && finite(point.x) && finite(point.y)), 'analysis.mds');
  requireValue(analysis.mds_stress === null || finite(analysis.mds_stress), 'analysis.mds_stress');
  requireValue(str(analysis.mds_method), 'analysis.mds_method');
  requireValue(Array.isArray(analysis.factors), 'analysis.factors');
  for (const [i, factor] of analysis.factors.entries()) {
    requireValue(isObject(factor) && str(factor.id) && str(factor.label) && isObject(factor.loadings) && isObject(factor.loading_intervals), `analysis.factors.${i}`);
    for (const [id, loading] of Object.entries(factor.loadings)) requireValue(analysis.factor_question_ids.includes(id) && (loading === null || finite(loading)), `analysis.factors.${i}.loadings.${id}`);
    for (const [id, interval] of Object.entries(factor.loading_intervals)) requireValue(analysis.factor_question_ids.includes(id) && (interval === null || (Array.isArray(interval) && interval.length === 2 && interval.every(finite) && interval[0] <= interval[1])), `analysis.factors.${i}.loading_intervals.${id}`);
  }
  requireValue(isObject(analysis.diagnostics), 'analysis.diagnostics');
  requireValue(Object.entries(analysis.diagnostics).every(([k,v])=>/^[a-z][a-z0-9_]{0,63}$/.test(k)&&((v===null)||(typeof v==='boolean')||(finite(v))||(typeof v==='string'&&v.length<=160&&!/(?:\/home\/|\/mnt\/|research\/|api[_-]?key|bearer\s|transcript|token)/i.test(v)))), 'analysis.diagnostics.safe_projection');
  return value;
}

function exactKeys(value, keys, field, required = keys) {
  requireValue(isObject(value)&&Object.keys(value).every(key=>keys.includes(key))&&required.every(key=>Object.hasOwn(value,key)),field+'.keys');
}

function validateScore(score, field, isQuestion) {
  requireValue(score === null || finite(score) || isObject(score), field);
  if (score === null) return;
  if (typeof score === 'number') { requireValue(score>=0&&score<=4,`${field}.value_range`);return; }
  exactKeys(score,['value','low','high','uncertainty','coverage','confidence','heterogeneity','probabilities','evidence'],field,['value','uncertainty']);
  requireValue((score.value===null||finite(score.value)&&score.value>=0&&score.value<=4) && (score.low===undefined||score.low===null||finite(score.low)&&score.low>=0&&score.low<=4) && (score.high===undefined||score.high===null||finite(score.high)&&score.high>=0&&score.high<=4), `${field}.value_range`);
  requireValue(score.low===null||score.high===null||score.low<=score.high,`${field}.interval_order`);
  for (const key of ['coverage','confidence']) requireValue(score[key]===undefined||score[key]===null||finite(score[key])&&score[key]>=0&&score[key]<=1,`${field}.${key}`);
  requireValue(score.heterogeneity===undefined||score.heterogeneity===null||finite(score.heterogeneity)&&score.heterogeneity>=0,`${field}.heterogeneity`);
  requireValue(['model_rating_range','insufficient_or_inapplicable','passage_distribution_not_video_probability'].includes(score.uncertainty), `${field}.uncertainty`);
  requireValue(score.probabilities===undefined||score.probabilities===null||(isObject(score.probabilities)&&Object.values(score.probabilities).every(n=>finite(n)&&n>=0&&n<=1)),`${field}.probabilities`);
  if (isQuestion && score.evidence!==undefined) {
    requireValue(Array.isArray(score.evidence), `${field}.evidence`);
    requireValue(score.evidence.length===0,`${field}.question_evidence_removed`);
  }
}

function validateEvidence(evidence, field) {
  requireValue(isObject(evidence) && str(evidence.href) && evidence.href.startsWith('/corpus/videos/') && Number.isInteger(evidence.start_ms) && evidence.start_ms >= 0 && Number.isInteger(evidence.end_ms) && evidence.end_ms >= evidence.start_ms && str(evidence.segment_id) && str(evidence.label), field);
}

export function scoreFor(video, id, kind) {
  if (kind === 'classification') {
    const choice = video.classifications?.[id];
    if (choice) return choice;
    const probability = video.noul_scores?.[id];
    return probability === undefined || probability === null ? null : { value: probability };
  }
  const score = (kind === 'factor' ? video.factor_scores : video.question_scores)?.[id] ?? null;
  return typeof score === 'number' ? { value: score } : score;
}

function compareMaybe(a, b, direction = 1) {
  if (a === null || a === undefined) return b === null || b === undefined ? 0 : 1;
  if (b === null || b === undefined) return -1;
  return (a < b ? -1 : a > b ? 1 : 0) * direction;
}

export function filterAndSortVideos(videos, filters, kind, metric) {
  const needle = (filters.text ?? '').trim().toLocaleLowerCase();
  const selected = videos.filter((video) =>
    (!filters.year || (filters.year === 'undated' ? !video.date : video.date?.slice(0, 4) === filters.year)) &&
    (!filters.category || video.category === filters.category) &&
    (!needle || `${video.title} ${video.category ?? ''}`.toLocaleLowerCase().includes(needle))
  );
  const direction = filters.direction === 'asc' ? 1 : -1;
  selected.sort((a, b) => {
    const av=scoreFor(a,metric,kind),bv=scoreFor(b,metric,kind);
    const avalue=kind==='classification'?(av?.choice??av?.value??null):(av?.value??null);
    const bvalue=kind==='classification'?(bv?.choice??bv?.value??null):(bv?.value??null);
    const result = filters.sort === 'date'
      ? compareMaybe(a.date, b.date, direction)
      : filters.sort === 'title'
        ? a.title.localeCompare(b.title) * (filters.direction === 'desc' ? -1 : 1)
        : compareMaybe(avalue,bvalue,direction);
    return result || a.title.localeCompare(b.title);
  });
  return selected;
}

export function groupTimeSeries(videos, metric, kind, interval = 'year') {
  const groups = new Map();
  for (const video of videos) {
    if (!validTrendDate(video.date, video.date_basis, interval)) continue;
    const key = interval === 'month' ? video.date.slice(0, 7) : video.date.slice(0, 4);
    const score = scoreFor(video, metric, kind);
    if (!score || !finite(score.value)) continue;
    const row = groups.get(key) ?? { period: key, values: [], lows: [], highs: [], coverages: [] };
    row.values.push(score.value);
    row.lows.push(finite(score.low) ? score.low : null);
    row.highs.push(finite(score.high) ? score.high : null);
    row.coverages.push(finite(score.coverage) ? score.coverage : video.coverage);
    groups.set(key, row);
  }
  return [...groups.values()].sort((a, b) => a.period.localeCompare(b.period)).map((row) => ({
    period: row.period,
    x: interval === 'month' ? Number(row.period.slice(0,4)) + (Number(row.period.slice(5,7))-1)/12 : Number(row.period.slice(0,4)),
    mean: row.values.reduce((sum, n) => sum + n, 0) / row.values.length,
    low: row.lows.length && row.lows.every(finite) ? row.lows.reduce((sum, n) => sum + n, 0) / row.lows.length : null,
    high: row.highs.length && row.highs.every(finite) ? row.highs.reduce((sum, n) => sum + n, 0) / row.highs.length : null,
    n: row.values.length,
    coverage: row.coverages.length ? row.coverages.reduce((sum, n) => sum + n, 0) / row.coverages.length : null,
  }));
}

export function trendInterpretation(kind, questionType) {
  if (kind === 'classification' && questionType === 'choice') return 'Nominal categories have no meaningful numeric time trend. Use the table to compare the predicted labels; this chart is not shown for this question.';
  if (kind === 'classification' && questionType === 'noul') return 'Each point is the mean model-estimated yes/no probability among scored recordings in that period. These are model probabilities, not observed event rates; no period confidence interval is shown.';
  if (kind === 'factor') return 'Each point is the mean factor score among scored recordings in that period. Any band averages per-recording bootstrap-stability endpoints; it is not a jointly bootstrapped interval for the period mean or a statistical confidence interval.';
  return 'Each point is the mean rubric score among scored recordings in that period. Any band averages the reported lower and upper model-rating range endpoints across recordings; it is not an interval for the period mean or a statistical confidence interval.';
}

export function largestFactorLoadings(factor, questions, limit = 6) {
  const names = new Map(questions.map((question) => [question.id, question.short_name]));
  return Object.entries(factor.loadings)
    .filter(([id, value]) => str(names.get(id)) && finite(value))
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]) || a[0].localeCompare(b[0]))
    .slice(0, limit)
    .map(([question_id, loading]) => ({ question_id, short_name: names.get(question_id), loading }));
}

function validTrendDate(date, basis, interval) {
  if (typeof date !== 'string' || !/^\d{4}(?:-\d{2}(?:-\d{2})?)?$/.test(date) || /^(?:unknown|unresolved|not_stated|undated)$/i.test(basis ?? '')) return false;
  const parts=date.split('-').map(Number);
  if(parts.length>=2&&(parts[1]<1||parts[1]>12))return false;
  if(parts.length===3){const d=new Date(Date.UTC(parts[0],parts[1]-1,parts[2]));if(d.getUTCFullYear()!==parts[0]||d.getUTCMonth()!==parts[1]-1||d.getUTCDate()!==parts[2])return false;}
  return interval!=='month'||parts.length>=2;
}

export function hrefAtTime(href, milliseconds, revisionId, segmentId) {
  const url = new URL(href, 'https://local.invalid');
  if (!url.pathname.startsWith('/corpus/videos/')) throw new Error('Invalid transcript evidence URL.');
  url.searchParams.set('t', String(Math.floor(milliseconds / 1000)));
  if (revisionId && segmentId) url.hash = `segment-${revisionId}-${segmentId}`;
  return `${url.pathname}${url.search}${url.hash}`;
}
