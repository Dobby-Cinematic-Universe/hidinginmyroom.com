import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { validatePassageData } from '../../src/lib/analysis/passage-data.mjs';

function fixture() {
  const release={corpus_release_id:'corpus-a',questionnaire_version:'bank-a',model:'model-a',generated_at:'now',questions:[{id:'joy',type:'score'},{id:'gate',type:'noul'}]};
  const video={recording_id:'rec_a',revision_id:'rev_a',href:'/corpus/videos/rec-a/'};
  const score={value:2,low:1,high:3,coverage:1,applicable:true};
  const passage={passage_id:'rev_a:0',revision_id:'rev_a',chunk_index:0,start_ms:1234,end_ms:8000,source:{href:'/corpus/videos/rec-a/?t=1#segment-rev_a-seg_a',segment_ids:['seg_a']},scores:{joy:score,gate:{value:.9,low:null,high:null,coverage:1,applicable:true}}};
  return {release,video,data:{schema_version:1,corpus_release_id:'corpus-a',questionnaire_version:'bank-a',model:'model-a',source_generated_at:'now',recording:{recording_id:'rec_a',revision_id:'rev_a'},passages:[passage]}};
}
test('passage data binds source revision, internal timestamp link and valid scores',()=>{
  const {release,video,data}=fixture();assert.equal(validatePassageData(data,release,video),data);
});
test('passage data rejects stale identities, injection, offsite links and malformed scores',()=>{
  const mutations=[
    d=>{d.source_generated_at='old';}, d=>{d.recording.revision_id='wrong';},
    d=>{d.raw_receipt={};}, d=>{d.passages[0].source.href='https://example.test';},
    d=>{d.passages[0].source.href=d.passages[0].source.href.replace('t=1','t=5');},
    d=>{d.passages[0].scores.joy.value=7;}, d=>{d.passages[0].scores.gate.low=.9;},
    d=>{d.passages[0].scores.joy.coverage=0;},d=>{d.passages.push(d.passages[0]);},
  ];
  for(const mutate of mutations){const {release,video,data}=fixture();mutate(data);assert.throws(()=>validatePassageData(data,release,video));}
});

test('passage selector and toolbar can shrink inside narrow evidence disclosures',async()=>{
  const source=await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro',import.meta.url),'utf8');
  for(const selector of ['passage-toolbar','passage-question-label','passage-question-label select']) {
    const rule=source.split(`.${selector}{`)[1]?.split('}')[0];
    assert.ok(rule?.includes('min-width:0'),`${selector} must be shrinkable`);
    assert.ok(rule?.includes('max-width:100%'),`${selector} must stay contained`);
  }
});
