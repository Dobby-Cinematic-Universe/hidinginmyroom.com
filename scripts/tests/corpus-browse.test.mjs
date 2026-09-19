import test from 'node:test';
import assert from 'node:assert/strict';
import {filterRecordings} from '../../src/lib/corpus/browse.mjs';
const rows=[{title:'Later dinner',date:'2024-09-02',year:2024,href:'/b',hasTranscript:true,summaryId:'b'}, {title:'Early dinner',date:'2024-01-01',year:2024,href:'/a',hasTranscript:true}, {title:'Unknown',date:null,year:null,href:'/c',hasTranscript:false}];
test('full-date sorting, undated always last, input unchanged',()=>{assert.deepEqual(filterRecordings(rows).map(r=>r.href),['/b','/a','/c']);assert.deepEqual(filterRecordings(rows,{sort:'oldest'}).map(r=>r.href),['/a','/b','/c']);assert.equal(rows[0].href,'/b');});
test('title tokens and year combine',()=>assert.deepEqual(filterRecordings(rows,{q:'DINNER later',year:'2024'}).map(r=>r.href),['/b']));
test('availability and undated filters',()=>{assert.equal(filterRecordings(rows,{availability:'summary'}).length,1);assert.equal(filterRecordings(rows,{availability:'transcript'}).length,2);assert.equal(filterRecordings(rows,{availability:'metadata',year:'undated'})[0].href,'/c');});
