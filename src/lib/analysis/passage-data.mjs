import { matchesAnalysisIdentity } from './supplement.mjs';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const assert = (condition, field) => { if (!condition) throw new Error(`Invalid passage data: ${field}`); };
const keys = (value, allowed) => object(value) && Object.keys(value).length === allowed.length && Object.keys(value).every(key => allowed.includes(key));
const number = value => typeof value === 'number' && Number.isFinite(value);

export function validatePassageData(value, release, video) {
  assert(keys(value, ['schema_version','corpus_release_id','questionnaire_version','model','source_generated_at','recording','passages']), 'root fields');
  assert(matchesAnalysisIdentity(value, release), 'release identity');
  assert(keys(value.recording,['recording_id','revision_id']) && value.recording.recording_id === video.recording_id && value.recording.revision_id === video.revision_id, 'recording identity');
  assert(Array.isArray(value.passages) && value.passages.length > 0 && value.passages.length <= 2000, 'passage count');
  const questions = new Map(release.questions.filter(q => q.type === 'score' || q.type === 'noul').map(q => [q.id,q]));
  let lastIndex = -1;
  for (const passage of value.passages) {
    assert(keys(passage,['passage_id','revision_id','chunk_index','start_ms','end_ms','source','scores']), 'passage fields');
    assert(passage.revision_id === video.revision_id && Number.isInteger(passage.chunk_index) && passage.chunk_index > lastIndex && passage.passage_id === `${video.revision_id}:${passage.chunk_index}`, 'passage identity/order');
    lastIndex = passage.chunk_index;
    assert(Number.isInteger(passage.start_ms) && passage.start_ms >= 0 && Number.isInteger(passage.end_ms) && passage.end_ms >= passage.start_ms, 'timestamps');
    assert(keys(passage.source,['href','segment_ids']) && Array.isArray(passage.source.segment_ids) && passage.source.segment_ids.length > 0 && passage.source.segment_ids.length <= 10000 && passage.source.segment_ids.every(id => typeof id === 'string' && /^[a-zA-Z0-9_-]{1,160}$/.test(id)), 'source segments');
    const expectedHref = `${video.href}?t=${Math.floor(passage.start_ms / 1000)}#segment-${video.revision_id}-${passage.source.segment_ids[0]}`;
    assert(passage.source.href === expectedHref, 'canonical internal source link');
    assert(object(passage.scores) && Object.keys(passage.scores).length === questions.size, 'score count');
    for (const [id, score] of Object.entries(passage.scores)) {
      const question = questions.get(id);
      assert(question && keys(score,['value','low','high','coverage','applicable']), 'score fields');
      const max = question.type === 'noul' ? 1 : 4;
      assert([score.value,score.low,score.high].every(v => v === null || number(v) && v >= 0 && v <= max), 'score range');
      assert((score.low === null && score.high === null) || (number(score.low) && number(score.high) && score.low <= score.high), 'range order');
      assert(typeof score.applicable === 'boolean' && (score.coverage === 0 || score.coverage === 1), 'applicability');
      assert(score.coverage === 1 ? score.applicable && score.value !== null : score.value === null, 'coverage semantics');
      assert(question.type !== 'noul' || (score.low === null && score.high === null), 'probability is not an interval');
    }
  }
  return value;
}
