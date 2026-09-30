import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {filterAndSortVideos,groupTimeSeries,hrefAtTime,largestFactorLoadings,scoreFor,trendInterpretation,validateAnalysisRelease} from '../../src/lib/analysis/explorer.mjs';
import {analysisShardId} from '../../src/lib/analysis/shards.mjs';

const bank=JSON.parse(await readFile(new URL('../../src/data/analysis/questions.json',import.meta.url),'utf8'));
const numeric=bank.questions.filter(q=>q.type==='score');
const q1=numeric[0],q2=numeric[1],choiceQuestion=bank.questions.find(q=>q.type==='choice'),noulQuestion=bank.questions.find(q=>q.type==='noul');
const optionKeys=Object.keys(choiceQuestion.criteria),choice=optionKeys[0],otherChoice=optionKeys[1];

function fixture(){
  const questions=[q1,q2,choiceQuestion,noulQuestion];
  const score=(value,low,high,coverage,evidence=[])=>({value,low,high,uncertainty:'model_rating_range',coverage,confidence:.8,heterogeneity:.2,probabilities:null,evidence});
  return {
    schema_version:1,status:'pilot',corpus_release_id:'release_test',questionnaire_version:bank.version,model:'fixture-model',generated_at:'2026-09-30T00:00:00Z',
    coverage:{total_recordings:3,scored_recordings:3,total_chunks:5,scored_chunks:4},questions,
    videos:[
      {recording_id:'rec_fixture_1',revision_id:'rev1',title:'Fictional A',href:'/corpus/videos/fictional-a/',date:'2020-01-01',date_basis:'fixture_date',category:'vlog',coverage:.9,scored_passage:{href:'/corpus/videos/fictional-a/',start_ms:1500,end_ms:2500,segment_id:'s1',label:'scored passage'},question_scores:{[q1.id]:score(.8,.7,.9,.9),[q2.id]:score(.5,.4,.6,.9)},factor_scores:{F1:{value:.4,low:.2,high:.6,uncertainty:'bootstrap_stability'}},classifications:{[choiceQuestion.id]:{choice,probabilities:{[choice]:.7,[otherChoice]:.3},confidence:.7,coverage:.9}},noul_scores:{[noulQuestion.id]:.8}},
      {recording_id:'rec_fixture_2',revision_id:'rev2',title:'Fictional B',href:'/corpus/videos/fictional-b/',date:'2020-12-31',date_basis:'fixture_date',category:'stream',coverage:.5,question_scores:{[q1.id]:score(.2,.1,.3,.5),[q2.id]:score(.3,.2,.4,.5)},factor_scores:{F1:{value:.2,low:.1,high:.3,uncertainty:'bootstrap_stability'}},classifications:{},noul_scores:{}},
      {recording_id:'rec_fixture_3',revision_id:'rev3',title:'Fictional C',href:'/corpus/videos/fictional-c/',date:null,date_basis:'unresolved',category:null,coverage:0,question_scores:{[q1.id]:null,[q2.id]:null},factor_scores:{F1:null},classifications:{},noul_scores:{}},
    ],
    analysis:{status:'exploratory',n:2,question_ids:[q1.id,q2.id],factor_question_ids:[q1.id,q2.id].filter(id=>questions.find(q=>q.id===id).factor_eligible),primary_factor_n:2,correlations:[[1,.4],[.4,1]],pair_counts:[[2,2],[2,2]],mds:[{id:q1.id,x:0,y:0},{id:q2.id,x:1,y:1}],mds_stress:.2,mds_method:'fixture MDS',factors:[{id:'F1',label:'Fixture dimension',loadings:{[q1.id]:.5,[q2.id]:.2},loading_intervals:{[q1.id]:[.1,.8],[q2.id]:[0,.5]}}],diagnostics:{case:'fixture',sample_n:2}},
  };
}

test('validates actual question-bank rubric shapes and separates numeric, choice and noul outcomes',()=>{
  const data=validateAnalysisRelease(fixture());
  assert.equal(data.questions.find(q=>q.id===q1.id).criteria.length,5);
  assert.equal(data.questions.find(q=>q.id===choiceQuestion.id).criteria[choice],choiceQuestion.criteria[choice]);
  assert.equal(data.videos[0].classifications[choiceQuestion.id].probabilities[choice],.7);
  assert.equal(scoreFor(data.videos[0],q1.id,'question').uncertainty,'model_rating_range');
});

test('requires finite factor bounds for bootstrap range labels and accepts unavailable bounds explicitly',()=>{
  const unavailable=fixture();unavailable.videos[0].factor_scores.F1={value:.4,low:null,high:null,uncertainty:'bootstrap_stability_unavailable'};
  assert.doesNotThrow(()=>validateAnalysisRelease(unavailable));
  const mislabeled=fixture();mislabeled.videos[0].factor_scores.F1={value:.4,low:null,high:null,uncertainty:'bootstrap_stability'};
  assert.throws(()=>validateAnalysisRelease(mislabeled));
});

test('fails closed on foreign source passages, question-level evidence, undeclared factors, invalid matrix size and unapproved public keys',()=>{
  const foreign=fixture();foreign.videos[0].scored_passage.href='/corpus/videos/fictional-b/';assert.throws(()=>validateAnalysisRelease(foreign));
  const questionEvidence=fixture();questionEvidence.videos[0].question_scores[q1.id].evidence=[foreign.videos[0].scored_passage];assert.throws(()=>validateAnalysisRelease(questionEvidence));
  const factor=fixture();factor.videos[0].private_artifact='/mnt/secret';assert.throws(()=>validateAnalysisRelease(factor));
  const badMatrix=fixture();badMatrix.analysis.correlations.pop();assert.throws(()=>validateAnalysisRelease(badMatrix));
  const badLoading=fixture();badLoading.analysis.factors[0].loadings[choiceQuestion.id]=.8;assert.throws(()=>validateAnalysisRelease(badLoading));
  const outsideMatrix=fixture(),eligible=bank.questions.find(q=>q.type==='score'&&q.factor_eligible&&!outsideMatrix.analysis.question_ids.includes(q.id));outsideMatrix.questions.push(eligible);outsideMatrix.analysis.question_ids=[q2.id];outsideMatrix.analysis.correlations=[[1]];outsideMatrix.analysis.pair_counts=[[2]];outsideMatrix.analysis.mds=[{id:q2.id,x:0,y:0}];outsideMatrix.analysis.factor_question_ids=[eligible.id];outsideMatrix.analysis.factors[0].loadings={[eligible.id]:.5};outsideMatrix.analysis.factors[0].loading_intervals={[eligible.id]:[.1,.8]};assert.doesNotThrow(()=>validateAnalysisRelease(outsideMatrix));
});

test('sorts unknown scores last while filtering by date, category and title',()=>{
  const data=fixture();
  assert.deepEqual(filterAndSortVideos(data.videos,{sort:'score',direction:'desc',year:'2020',category:'vlog',text:''},'question',q1.id).map(v=>v.title),['Fictional A']);
  assert.deepEqual(filterAndSortVideos(data.videos,{sort:'score',direction:'desc',year:'',category:'',text:''},'question',q1.id).map(v=>v.title),['Fictional A','Fictional B','Fictional C']);
  assert.deepEqual(filterAndSortVideos(data.videos,{sort:'score',direction:'asc',year:'undated',category:'',text:'fictional'},'question',q1.id).map(v=>v.title),['Fictional C']);
});

test('uses the same records for a time-series mean and score ranges, and omits incomplete bands',()=>{
  const data=fixture(),series=groupTimeSeries(data.videos,q1.id,'question','year');
  assert.deepEqual({...series[0],low:Number(series[0].low.toFixed(6))},{period:'2020',x:2020,mean:.5,low:.4,high:.6,n:2,coverage:.7});
  data.videos[1].question_scores[q1.id].low=null;
  assert.equal(groupTimeSeries(data.videos,q1.id,'question','year')[0].low,null);
});

test('uses chronological month positions and ignores unresolved dates',()=>{
  const data=fixture();data.videos[1].date='2022-03';
  assert.deepEqual(groupTimeSeries(data.videos,q1.id,'question','month').map(s=>[s.period,s.x]),[['2020-01',2020],['2022-03',2022+2/12]]);
});

test('converts transcript milliseconds to safe revision and segment links',()=>{
  assert.equal(hrefAtTime('/corpus/videos/fictional-a/',12500,'rev1','s1'),'/corpus/videos/fictional-a/?t=12#segment-rev1-s1');
  assert.throws(()=>hrefAtTime('https://example.invalid/video',1000));
});

test('fingerprints shard contents so changed analysis cannot reuse immutable cached shard URLs',()=>{
  const data=fixture(),rows=data.videos.slice(0,2),original=analysisShardId(0,rows),changed=structuredClone(rows);
  changed[0].question_scores[q1.id].value=.81;
  assert.match(original,/^000-[a-f0-9]{16}$/);
  assert.notEqual(analysisShardId(0,changed),original);
  assert.notEqual(analysisShardId(1,rows),original);
});

test('explains trend bands according to score, factor, yes/no and nominal metric semantics',()=>{
  const score=trendInterpretation('question','score'),factor=trendInterpretation('factor','score'),noul=trendInterpretation('classification','noul'),choice=trendInterpretation('classification','choice');
  assert.match(score,/model-rating range endpoints/);assert.match(score,/not an interval for the period mean/);
  assert.match(factor,/bootstrap-stability endpoints/);assert.match(factor,/not a jointly bootstrapped interval/);
  assert.match(noul,/model-estimated yes\/no probability/);assert.match(noul,/not observed event rates/);
  assert.match(choice,/Nominal categories/);assert.match(choice,/not shown/);
});

test('UI names passage-mixture and unavailable factor bounds without implying a mean interval',async()=>{
  const source=await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro',import.meta.url),'utf8');
  assert.match(source,/Passage-mixture rating range \(not a mean interval\)/);
  assert.match(source,/bootstrap_stability_unavailable:'Stability range unavailable'/);
  assert.match(source,/Text-quality caution:/);
  assert.match(source,/Transcription problems may affect every score, including hypothetical audience confusion/);
});

test('explorer starts with the ranking table and moves broad views ahead of model diagnostics',async()=>{
  const source=(await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro',import.meta.url),'utf8')).split('<script>')[0];
  const content=source.slice(source.indexOf('<section class="analysis-explorer"'));
  const sections=[...content.matchAll(/<section\s+id="([^"]+)"|<(AnalysisDiscovery|AnalysisPassageInsights|AnalysisTemporalComparisons|AnalysisRelationships|AnalysisFactorDiagnostics|AnalysisRobustness)\s*\/>/g)].map(match=>match[1]??match[2]);
  assert.deepEqual(sections,['analysis-videos','analysis-trend','AnalysisDiscovery','AnalysisRelationships','analysis-map','AnalysisPassageInsights','AnalysisTemporalComparisons','analysis-correlations','analysis-factors','AnalysisFactorDiagnostics','AnalysisRobustness']);
  const links=[...content.slice(content.indexOf('<nav class="explorer-nav"'),content.indexOf('</nav>')).matchAll(/href="#([^"]+)"/g)].map(match=>match[1]);
  assert.deepEqual(links,['analysis-videos','analysis-trend','analysis-discovery','analysis-relationships','analysis-map','analysis-passage-insights','analysis-temporal-comparisons','analysis-correlations','analysis-factors','analysis-factor-diagnostics','analysis-robustness']);
});

test('sort controls are visible next to the table and time interval belongs to the trend view',async()=>{
  const source=(await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro',import.meta.url),'utf8')).split('<script>')[0];
  const ranking=source.slice(source.indexOf('<section id="analysis-videos"'),source.indexOf('<section id="analysis-trend"'));
  const form=ranking.slice(ranking.indexOf('<form'),ranking.indexOf('</form>'));
  const toolbar=form.slice(form.indexOf('<div class="table-toolbar"'));
  assert.match(toolbar,/name="sort"/);assert.match(toolbar,/name="direction"/);assert.match(toolbar,/data-page-size/);
  assert.doesNotMatch(toolbar,/<details|<\/details/);
  assert.ok(form.lastIndexOf('</details>')<form.indexOf('<div class="table-toolbar"'));
  assert.doesNotMatch(ranking,/data-interval/);
  const trend=source.slice(source.indexOf('<section id="analysis-trend"'),source.indexOf('<AnalysisRelationships />'));
  assert.match(trend,/data-interval/);
  assert.ok(ranking.indexOf('class="table-toolbar"')<ranking.indexOf('data-video-rows'));
});

test('order labels distinguish alphabetical, chronological, and numeric sorting',async()=>{
  const source=await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro',import.meta.url),'utf8');
  assert.match(source,/values\.sort==='title'\|\|\(values\.sort==='score'&&nominal\)\?\['Z–A','A–Z'\]/);
  assert.match(source,/textContent=nominal\?'Outcome':'Score'/);
  assert.match(source,/values\.sort==='date'\?\['Newest first','Oldest first'\]/);
  assert.match(source,/\['Highest score first','Lowest score first'\]/);
  assert.match(source,/choice\.textContent=directionLabels\[choice\.value==='desc'\?0:1\]/);
});

test('trend controls have shared alignment and context, with technical methods after the chart',async()=>{
  const source=await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro',import.meta.url),'utf8');
  assert.doesNotMatch(source,/\.trend-method\{/);
  assert.match(source,/\.trend-controls\{display:grid;[^}]*align-items:end/);
  assert.match(source,/\.trend-controls label\{[^}]*margin:0/);
  assert.match(source,/min-height:2\.75rem/);
  assert.match(source,/data-trend-context aria-live="polite"/);
  assert.match(source,/intervalSelect\.disabled=nominal/);
  assert.match(source,/dateWarning\.textContent=''/);
  assert.match(source,/trendMethodSelect\.disabled=!joint\|\|!series\.length/);
  const markup=source.split('<script>')[0];
  assert.ok(markup.indexOf('<svg data-trend')<markup.indexOf('data-trend-note'));
  assert.match(source,/status\.hidden=true;content\.hidden=false/);
  assert.match(source,/\.analysis-explorer thead th\{position:sticky/);
  assert.doesNotMatch(source,/\.analysis-explorer th\{[^}]*position:sticky/);
});

test('factor selector leaders are the six largest absolute finite pattern coefficients with signs intact',()=>{
  const questions=numeric.slice(0,8),values=[.1,-.95,.8,-.7,.6,.5,.4,null],factor={loadings:Object.fromEntries(questions.map((q,i)=>[q.id,values[i]]))};
  factor.loadings.unrecognized_question=.99;
  const leaders=largestFactorLoadings(factor,questions);
  assert.equal(leaders.length,6);
  assert.deepEqual(leaders.map(item=>item.loading),[-.95,.8,-.7,.6,.5,.4]);
  assert.equal(leaders[0].short_name,questions[1].short_name);
});
