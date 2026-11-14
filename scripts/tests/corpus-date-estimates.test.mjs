import test from 'node:test';
import assert from 'node:assert/strict';
import {projectDateEstimates,describeDateEstimate,circaDate,duplicateOf} from '../../src/lib/corpus/public-date-estimates.mjs';

const catalog=[{recording_id:'u',date_label:null},{recording_id:'d',date_label:'2018-02-14'},{recording_id:'u2',date_label:null}];
const est={month:'2023-07',low:'2022-10',high:'2024-08',confidence_band:2};
const project=row=>projectDateEstimates({recordings:{u:row}},catalog).recordings.u;

test('estimates are kept only for undated recordings and always read as estimates',()=>{
  const out=projectDateEstimates({recordings:{u:{basis:'text_estimate',date:'2023-07',copy:null,estimate:est},d:{basis:'text_estimate',date:'2023-07',copy:null,estimate:est},gone:{basis:'text_estimate',date:'2023-07',copy:null,estimate:est}}},catalog);
  assert.deepEqual(Object.keys(out.recordings),['u']);
  const text=describeDateEstimate(out.recordings.u);
  assert.match(text.short,/^Estimated July 2023 \(not a verified date\)$/);
  assert.equal(text.card,'Estimated July 2023');
  assert.match(text.detail,/range October 2022–August 2024.*Not a verified recording or upload date/);
});

test('a copy date must be the dated copy’s catalog date and carries its copy label',()=>{
  const full=project({basis:'full_copy',date:'2018-02-14',copy:{kind:'full',recording_id:'d',date:'2018-02-14'},estimate:est});
  const text=describeDateEstimate(full);
  assert.equal(text.short,'Estimated 2018-02-14 · matching transcript');
  assert.equal(text.card,'Estimated 2018-02-14 (matching transcript)');
  assert.match(text.detail,/^Matching transcript:.*not a verified date.*re-upload/);
  assert.match(describeDateEstimate(project({basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null})).detail,/^Identical transcript:/);
  assert.throws(()=>project({basis:'full_copy',date:'2018-02-15',copy:{kind:'full',recording_id:'d',date:'2018-02-15'},estimate:null}),/Invalid date estimate copy/);
  assert.throws(()=>project({basis:'full_copy',date:'2018-02-14',copy:{kind:'full',recording_id:'u2',date:'2018-02-14'},estimate:null}),/Invalid date estimate copy/);
});

test('a partial copy is a label only and never sets the date',()=>{
  const row=project({basis:'text_estimate',date:'2023-07',copy:{kind:'partial',recording_id:'d',date:'2018-02-14'},estimate:est});
  assert.equal(row.date,'2023-07');
  assert.match(describeDateEstimate(row).copyNote,/^Partial text match.*2018-02-14/);
  assert.equal(describeDateEstimate(row).card,'Estimated July 2023 (partial copy)');
  assert.throws(()=>project({basis:'full_copy',date:'2018-02-14',copy:{kind:'partial',recording_id:'d',date:'2018-02-14'},estimate:est}),/does not match/);
});

test('malformed or unexpected fields are rejected',()=>{
  assert.throws(()=>project({basis:'text_estimate',date:'2023-07',copy:null,estimate:{...est,low:'2024-01'}}),/range/);
  assert.throws(()=>project({basis:'text_estimate',date:'2023-07',copy:null,estimate:est,note:'/mnt/archive'}),/Unexpected/);
  assert.throws(()=>project({basis:'guess',date:'2023-07',copy:null,estimate:est}),/basis/);
  assert.throws(()=>project({basis:'text_estimate',date:'2023-13',copy:null,estimate:{...est,month:'2023-13'}}),/range/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{basis:'full_copy',date:'2023-02-30',copy:{kind:'full',recording_id:'d',date:'2023-02-30'},estimate:null}}},[{recording_id:'u',date_label:null},{recording_id:'d',date_label:'2023-02-30'}]),/Invalid date estimate copy/);
});

test('a title date must match the title and remain an estimate',()=>{
  const row={basis:'title_date',date:'2017-06-08',copy:null,estimate:null};
  const rows=[{recording_id:'u',title:'20170608 - Example',date_label:null}];
  assert.equal(describeDateEstimate(projectDateEstimates({recordings:{u:row}},rows).recordings.u).card,'Estimated 2017-06-08 (title)');
  assert.equal(rows[0].date_label,null);
  assert.equal(projectDateEstimates({recordings:{u:row}},[{...rows[0],title:'Family photos (circa 20170608)'}]).recordings.u.date,'2017-06-08');
  const christmas=projectDateEstimates({recordings:{u:{...row,date:'2023-12'}}},[{...rows[0],title:'Christmas in THAILAND 2023 [example]'}]).recordings.u;
  assert.equal(describeDateEstimate(christmas).card,'Estimated December 2023 (title)');
  assert.throws(()=>projectDateEstimates({recordings:{u:{...row,date:'2019-03-01'}}},rows),/Invalid title date/);
});

test('an archived listing window is shown as a circa or estimated range and can back a copy date',()=>{
  const row=project({basis:'archived_listing',date:'2022-03-01',copy:null,estimate:null,listing:{from:'2022-03-01',to:'2022-03-09'}});
  const text=describeDateEstimate(row);
  assert.equal(text.card,'circa 2022-03-01 to 2022-03-09 (archived listing)');
  assert.equal(text.short,'circa 2022-03-01 to 2022-03-09 · archived channel listing');
  assert.match(text.detail,/published between 2022-03-01 and 2022-03-09.*approximate.*Not a verified recording or upload date/);
  assert.equal(describeDateEstimate(project({basis:'archived_listing',date:'2018-01-22',copy:null,estimate:null,listing:{from:'2018-01-22',to:'2018-01-22'}})).card,'circa 2018-01-22 (archived listing)');
  const copy=project({basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null,listing:{from:'2018-02-10',to:'2018-02-17'}});
  assert.match(describeDateEstimate(copy).detail,/archived channel listing is consistent.*between 2018-02-10 and 2018-02-17/);
  assert.throws(()=>project({basis:'archived_listing',date:'2022-03-02',copy:null,estimate:null,listing:{from:'2022-03-01',to:'2022-03-09'}}),/Invalid archived listing date/);
  assert.throws(()=>project({basis:'archived_listing',date:'2022-03-09',copy:null,estimate:null,listing:{from:'2022-03-09',to:'2022-03-01'}}),/Invalid archived listing window/);
  assert.throws(()=>project({basis:'archived_listing',date:'2022-03',copy:null,estimate:null,listing:{from:'2022-03',to:'2022-04'}}),/Invalid archived listing window/);
  assert.throws(()=>project({basis:'text_estimate',date:'2023-07',copy:null,estimate:est,listing:{from:'2022-03-01',to:'2022-03-09'}}),/does not fit/);
});

test('only a narrow listing window gives a circa date; a confirmed identical copy is folded',()=>{
  const narrow=project({basis:'archived_listing',date:'2022-03-01',copy:null,estimate:null,listing:{from:'2022-03-01',to:'2022-04-01'}});
  assert.equal(circaDate(narrow),'2022-03-01');assert.equal(describeDateEstimate(narrow).circa,'2022-03-01');assert.equal(duplicateOf(narrow),undefined);
  const wide=project({basis:'archived_listing',date:'2022-03-01',copy:null,estimate:null,listing:{from:'2022-03-01',to:'2022-04-02'}});
  assert.equal(circaDate(wide),undefined);assert.equal(describeDateEstimate(wide).card,'Estimated 2022-03-01 to 2022-04-02 (archived listing)');
  const copy={basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null};
  assert.equal(duplicateOf(project(copy)),undefined);assert.equal(circaDate(project(copy)),undefined);
  const confirmed=project({...copy,listing:{from:'2018-02-10',to:'2018-02-17'}});
  assert.equal(duplicateOf(confirmed),'d');assert.equal(circaDate(confirmed),'2018-02-14');
  assert.equal(duplicateOf(project({...copy,basis:'full_copy',copy:{...copy.copy,kind:'full'},listing:{from:'2018-02-10',to:'2018-02-17'}})),undefined);
  assert.equal(circaDate(project({basis:'text_estimate',date:'2023-07',copy:null,estimate:est})),undefined);
});

test('an archived video page gives a dated, ordered entry and can confirm or fold a copy',()=>{
  const page={date:'2018-02-13',captured:'2019-03-01',source:'https://web.archive.org/web/20190301000000/https://www.youtube.com/watch?v=abcdefghijk'};
  const own=project({basis:'archived_page',date:'2018-02-13',copy:null,estimate:null,page});
  const text=describeDateEstimate(own);
  assert.equal(text.card,'2018-02-13 (archived video page)');
  assert.match(text.detail,/saved on 2019-03-01, shows it as published on 2018-02-13.*can differ by one day.*not verified/);
  assert.equal(text.source,page.source);assert.equal(circaDate(own),'2018-02-13');assert.equal(duplicateOf(own),undefined);
  const withCopy=project({basis:'archived_page',date:'2018-02-13',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null,page});
  assert.equal(duplicateOf(withCopy),'d');
  const confirmed=project({basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null,page});
  assert.equal(duplicateOf(confirmed),'d');assert.equal(circaDate(confirmed),'2018-02-14');
  assert.match(describeDateEstimate(confirmed).detail,/own page shows it as published on 2018-02-13/);
  assert.throws(()=>project({basis:'archived_page',date:'2018-02-13',copy:null,estimate:null,page:{...page,source:'https://example.com/watch?v=x'}}),/Invalid archived page date/);
  assert.throws(()=>project({basis:'archived_page',date:'2018-02-12',copy:null,estimate:null,page}),/Invalid archived page basis/);
  assert.throws(()=>project({basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null,page:{...page,date:'2018-01-02'}}),/disagrees/);
  assert.throws(()=>project({basis:'text_estimate',date:'2023-07',copy:null,estimate:est,page}),/does not fit/);
});
