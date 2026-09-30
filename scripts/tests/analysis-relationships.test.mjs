import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {correlationSignature, correlationColor, rankedRelationships, validateMapDiagnostics} from '../../src/lib/analysis/relationships.mjs';

const fixture = () => ({corpus_release_id:'r1',questionnaire_version:'v1',model:'m1',generated_at:'date1',analysis:{n:3,
  question_ids:['a','b','c'], correlations:[[1,.8,-.4],[.8,1,.1],[-.4,.1,1]], pair_counts:[[3,3,2],[3,3,2],[2,2,2]],
  mds:[{id:'a',x:0,y:0},{id:'b',x:1,y:0},{id:'c',x:0,y:1}],mds_stress:null,mds_method:'chord'}});
const digest = (text) => createHash('sha256').update(text).digest('hex');

function annotations(release) {
  return {schema_version:1,corpus_release_id:release.corpus_release_id,questionnaire_version:release.questionnaire_version,
    model:release.model,source_generated_at:release.generated_at,generated_at:'analysis-date',cohort_n:release.analysis.n,
    question_ids:[...release.analysis.question_ids],correlation_signature:digest(correlationSignature(release)),
    clustering:{method:'average',leaf_order:['b','a','c'],note:'display'},
    stress_curve:{method:'classical',points:[{dimensions:1,normalized_rms_error:.3},{dimensions:2,normalized_rms_error:.1}],note:'fit'},
    shepard:{method:'pairwise',points:[{a:'a',b:'b',correlation:.8,chord_distance:Math.sqrt(.4),map_distance:1,n:3},
      {a:'a',b:'c',correlation:-.4,chord_distance:Math.sqrt(2.8),map_distance:1,n:2},
      {a:'b',b:'c',correlation:.1,chord_distance:Math.sqrt(1.8),map_distance:Math.sqrt(2),n:2}]}};
}

test('validates supplemental map diagnostics against the exact release and correlation matrix', async () => {
  const release=fixture(),diagnostics=annotations(release);
  assert.equal(await validateMapDiagnostics(release,diagnostics),diagnostics);
  const changed=fixture();changed.analysis.correlations[0][1]=.7;
  assert.equal(await validateMapDiagnostics(changed,diagnostics),null);
  assert.equal(await validateMapDiagnostics({...release,generated_at:'another-date'},diagnostics),null);
  const badPair=annotations(release);badPair.shepard.points.push({...badPair.shepard.points[0]});
  assert.equal(await validateMapDiagnostics(release,badPair),null);
});

test('rankings split positive and negative relationships using signed correlations', () => {
  const ranks=rankedRelationships(fixture(),'a');
  assert.deepEqual(ranks.map((x)=>x.id),['b','c']);
  assert.equal(ranks[0].n,3);
  assert.equal(ranks[1].correlation,-.4);
  assert.deepEqual(rankedRelationships(fixture(),'unknown'),[]);
});

test('heatmap color retains the sign and null estimates have a neutral color', () => {
  assert.notEqual(correlationColor(.8),correlationColor(-.8));
  assert.match(correlationColor(null),/^#/);
});
