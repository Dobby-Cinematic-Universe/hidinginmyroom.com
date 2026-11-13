import test from 'node:test';
import assert from 'node:assert/strict';
import {filterRecordings} from '../../src/lib/corpus/browse.mjs';
import {recordingLength} from '../../src/lib/corpus/duration.mjs';
const rows=[{title:'Later dinner',date:'2024-09-02',year:2024,href:'/b',hasTranscript:true,summaryId:'b'}, {title:'Early dinner',date:'2024-01-01',year:2024,href:'/a',hasTranscript:true}, {title:'Unknown',date:null,year:null,href:'/c',hasTranscript:false}];
test('full-date sorting, undated always last, input unchanged',()=>{assert.deepEqual(filterRecordings(rows).map(r=>r.href),['/b','/a','/c']);assert.deepEqual(filterRecordings(rows,{sort:'oldest'}).map(r=>r.href),['/a','/b','/c']);assert.equal(rows[0].href,'/b');});
test('title tokens and year combine',()=>assert.deepEqual(filterRecordings(rows,{q:'DINNER later',year:'2024'}).map(r=>r.href),['/b']));
test('availability and undated filters',()=>{assert.equal(filterRecordings(rows,{availability:'summary'}).length,1);assert.equal(filterRecordings(rows,{availability:'transcript'}).length,2);assert.equal(filterRecordings(rows,{availability:'metadata',year:'undated'})[0].href,'/c');});

test('length ranges use full duration with exact nonoverlapping boundaries',()=>{
  assert.deepEqual([1,299999,300000,1199999,1200000,3599999,3600000,10799999,10800000].map(recordingLength),
    ['under-5','under-5','5-20','5-20','20-60','20-60','60-180','60-180','180-plus']);
  for(const value of [null,undefined,0,-1,NaN,Infinity,'60000'])assert.equal(recordingLength(value),'unknown');
});

test('optional length combines with title and availability; clearing it restores unknowns',()=>{
  const durations=rows.map((row,i)=>({...row,durationMs:[10*60000,3*3600000,null][i]}));
  assert.deepEqual(filterRecordings(durations,{q:'dinner',availability:'transcript',length:'180-plus'}).map(r=>r.href),['/a']);
  assert.deepEqual(filterRecordings(durations,{length:'unknown'}).map(r=>r.href),['/c']);
  assert.deepEqual(filterRecordings(durations,{length:'5-20'}).map(r=>r.href),['/b']);
  assert.equal(filterRecordings(durations,{length:''}).length,3);
});
