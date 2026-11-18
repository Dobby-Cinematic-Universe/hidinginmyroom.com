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

test('combined evidence gives a circa date of at most a week, and a wording range can say what narrowed it',()=>{
  const ev=project({basis:'evidence_date',date:'2024-04-09',copy:null,estimate:null,evidence:{from:'2024-04-08',to:'2024-04-10',sources:['legacy_index_label','calendar_clue']}});
  const text=describeDateEstimate(ev);
  assert.equal(text.card,'circa 2024-04-09 (earlier index label)');
  assert.match(text.detail,/between 2024-04-08 and 2024-04-10 using a date label in an earlier transcript index and a weekday or date said in the recording.*not a verified/);
  assert.equal(circaDate(ev),'2024-04-09');assert.equal(duplicateOf(ev),undefined);
  const post=project({basis:'evidence_date',date:'2025-11-04',copy:null,estimate:null,evidence:{from:'2025-11-04',to:'2025-11-04',sources:['community_post','calendar_clue','stream_calendar']}});
  assert.equal(describeDateEstimate(post).card,'circa 2025-11-04 (community post)');
  assert.match(describeDateEstimate(post).detail,/a community post that linked, quoted or summarised the stream at the time, a weekday or date said in the recording and the days left free by the platform's dated streams/);
  assert.throws(()=>project({basis:'evidence_date',date:'2024-04-09',copy:null,estimate:null,evidence:{from:'2024-04-01',to:'2024-04-20',sources:['calendar_clue']}}),/Invalid evidence date/);
  assert.throws(()=>project({basis:'evidence_date',date:'2024-04-12',copy:null,estimate:null,evidence:{from:'2024-04-08',to:'2024-04-10',sources:['calendar_clue']}}),/Invalid evidence date basis/);
  assert.throws(()=>project({basis:'evidence_date',date:'2024-04-09',copy:null,estimate:null,evidence:{from:'2024-04-08',to:'2024-04-10',sources:['guess']}}),/Invalid evidence date/);
  const narrowed=project({basis:'text_estimate',date:'2023-07',copy:null,estimate:est,narrowed_by:['release_anchor']});
  assert.match(describeDateEstimate(narrowed).detail,/narrowed using the release date of something mentioned\./);
  assert.throws(()=>project({basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null,narrowed_by:['release_anchor']}),/Invalid narrowing/);
});

test('an undated clip of a dated collection gets the collection period, never a circa date',()=>{
  const row=project({basis:'collection_period',date:'2017-06',copy:null,estimate:null,period:{from:'2017-06-03',to:'2019-01-27',collection:'twitch_clips',dated_members:389}});
  const text=describeDateEstimate(row);
  assert.equal(text.card,'Estimated 2017–2019 (clip collection)');
  assert.match(text.detail,/389 dated clips.*2017-06-03 to 2019-01-27.*Not a verified date/);
  assert.equal(circaDate(row),undefined);
  assert.throws(()=>project({basis:'collection_period',date:'2018-01',copy:null,estimate:null,period:{from:'2017-06-03',to:'2019-01-27',collection:'twitch_clips',dated_members:389}}),/Invalid collection period basis/);
  assert.throws(()=>project({basis:'collection_period',date:'2017-06',copy:null,estimate:null,period:{from:'2017-06-03',to:'2019-01-27',collection:'other',dated_members:389}}),/Invalid collection period/);
});

test('a clip whose words lie inside a dated recording takes that recording’s date as a circa date',()=>{
  const row=project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'d',date:'2018-02-14',tier:'A',relation:'same_recording'}});
  const text=describeDateEstimate(row);
  assert.equal(text.card,'circa 2018-02-14 (part of a dated recording)');
  assert.match(text.detail,/found inside a longer recording dated 2018-02-14, so it is taken to be an excerpt.*used here as an estimate.*not the day this clip was made/);
  assert.equal(circaDate(row),'2018-02-14');assert.equal(duplicateOf(row),undefined);assert.equal(text.copy.recording_id,'d');
  assert.match(describeDateEstimate(project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'d',date:'2018-02-14',tier:'B',relation:'same_recording'}})).detail,/\(a probable match\)/);
  // The source must be another, dated recording, and its catalog date is the only date the estimate can carry.
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-15',copy:null,estimate:null,source:{recording_id:'d',date:'2018-02-15',tier:'A',relation:'same_recording'}}),/Invalid source recording/);
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'u2',date:'2018-02-14',tier:'A',relation:'same_recording'}}),/Invalid source recording/);
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'u',date:'2018-02-14',tier:'A',relation:'same_recording'}}),/Invalid source recording/);
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'d',date:'2018-02-14',tier:'C',relation:'same_recording'}}),/Invalid source recording/);
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null}),/Invalid source recording basis/);
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:est,source:{recording_id:'d',date:'2018-02-14',tier:'A',relation:'same_recording'}}),/Invalid source recording basis/);
  assert.throws(()=>project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'d',date:'2018-02-14',tier:'A',relation:'same_recording',path:'/mnt/archive'}}),/Unexpected source field/);
});

test('a source that may only be a later replay gives a latest possible day, never a circa date',()=>{
  const row=project({basis:'source_recording',date:'2018-02-14',copy:null,estimate:null,source:{recording_id:'d',date:'2018-02-14',tier:'A',relation:'not_after'}});
  const text=describeDateEstimate(row);
  assert.equal(text.label,'Latest possible date');
  assert.equal(text.card,'2018-02-14 or earlier (part of a dated recording)');
  assert.match(text.detail,/latest possible day and not an estimate of the day itself\. Not a verified/);
  assert.equal(circaDate(row),undefined);assert.equal(text.circa,undefined);
});

test('a clip inside an undated recording is given that recording’s wording estimate, unchanged',()=>{
  const stream={basis:'text_estimate',date:'2023-07',copy:null,estimate:est,narrowed_by:['calendar_clue']};
  const clip={basis:'source_estimate',date:'2023-07',copy:null,estimate:est,narrowed_by:['calendar_clue'],source:{recording_id:'u2',basis:'text_estimate',relation:'inside_undated_recording'}};
  const out=projectDateEstimates({recordings:{u:clip,u2:stream}},catalog).recordings.u;
  const text=describeDateEstimate(out);
  assert.equal(text.card,'Estimated July 2023 (part of an undated recording)');
  assert.match(text.detail,/no catalog date either\. That recording's estimate is passed on: July 2023; approximate range October 2022–August 2024\. Its range was narrowed using a weekday or date said in the recording\..*Not a verified/);
  assert.equal(circaDate(out),undefined);assert.equal(text.copy.recording_id,'u2');assert.deepEqual(out.source,{recording_id:'u2',relation:'inside_undated_recording'});
  // The passed-on estimate must be the containing recording's own, and that recording must be undated.
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,estimate:{...est,month:'2023-08'},date:'2023-08'},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,narrowed_by:['release_anchor']},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:clip}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,source:{recording_id:'d',basis:'text_estimate',relation:'inside_undated_recording'}},u2:stream}},catalog),/Invalid source estimate/);
});

test('a clip with no match of its own can take a day or a range from clips with the same name',()=>{
  const day=project({basis:'series_period',date:'2025-04-25',copy:null,estimate:null,period:{from:'2025-04-25',to:'2025-04-25',rule:'same clip name'}});
  assert.equal(describeDateEstimate(day).card,'Estimated 2025-04-25 (another file of the same clip)');
  const range=project({basis:'series_period',date:'2025-07-21',copy:null,estimate:null,period:{from:'2025-07-21',to:'2025-08-14',rule:'between numbered neighbours'}});
  const text=describeDateEstimate(range);
  assert.equal(text.card,'Estimated 2025-07-21 to 2025-08-14 (numbered clip series)');
  assert.match(text.detail,/No recording that holds these words was found\..*between 2025-07-21 and 2025-08-14\. Not a verified/);
  assert.equal(circaDate(day),undefined);assert.equal(circaDate(range),undefined);
  assert.throws(()=>project({basis:'series_period',date:'2025-07-21',copy:null,estimate:null,period:{from:'2025-07-21',to:'2025-08-14',rule:'same clip name'}}),/Invalid series period/);
  assert.throws(()=>project({basis:'series_period',date:'2025-07-22',copy:null,estimate:null,period:{from:'2025-07-21',to:'2025-08-14',rule:'between numbered neighbours'}}),/Invalid series period basis/);
  assert.throws(()=>project({basis:'series_period',date:'2025-07-21',copy:null,estimate:null,period:{from:'2025-07-21',to:'2025-08-14',rule:'guess'}}),/Invalid series period/);
});

test('a dated source can be mentioned beside a title or listing date, and a conflict with the title is stated',()=>{
  const cat2=[{recording_id:'t',date_label:null,title:'Photos stream (circa 20181210)'},{recording_id:'d',date_label:'2018-10-27'}];
  const row=projectDateEstimates({recordings:{t:{basis:'title_date',date:'2018-12-10',copy:null,estimate:null,source:{recording_id:'d',date:'2018-10-27',tier:'A',relation:'same_recording'}}}},cat2).recordings.t;
  const text=describeDateEstimate(row);
  assert.equal(text.card,'Estimated 2018-12-10 (title)');
  assert.match(text.detail,/also found inside a recording dated 2018-10-27, which differs from the date in the title; neither is verified\.$/);
  assert.equal(text.copy.recording_id,'d');
  const listed=project({basis:'archived_listing',date:'2018-02-10',copy:null,estimate:null,listing:{from:'2018-02-10',to:'2018-02-12'},source:{recording_id:'d',date:'2018-02-14',tier:'A',relation:'same_recording'}});
  assert.match(describeDateEstimate(listed).detail,/also found inside a recording dated 2018-02-14\.$/);
  // A copy date needs no second witness, and a period never carries one.
  assert.throws(()=>project({basis:'exact_copy',date:'2018-02-14',copy:{kind:'exact',recording_id:'d',date:'2018-02-14'},estimate:null,source:{recording_id:'d',date:'2018-02-14',tier:'A',relation:'same_recording'}}),/Invalid source recording/);
});

test('excerpt and replay evidence are named, and a range inside one month reads as such',()=>{
  const ev=project({basis:'evidence_date',date:'2025-09-23',copy:null,estimate:null,evidence:{from:'2025-09-18',to:'2025-09-23',sources:['excerpt','replay','calendar_clue']}});
  assert.equal(describeDateEstimate(ev).card,'circa 2025-09-23 (dated excerpt)');
  assert.match(describeDateEstimate(ev).detail,/using a dated excerpt that was cut from this recording, older dated material played back in it and a weekday or date said in the recording/);
  const tight=project({basis:'text_estimate',date:'2023-11',copy:null,estimate:{month:'2023-11',low:'2023-11',high:'2023-11',confidence_band:2},narrowed_by:['excerpt']});
  assert.match(describeDateEstimate(tight).detail,/^Estimated November 2023; the range lies within November 2023\. The range was narrowed using a dated excerpt that was cut from this recording\./);
});

test('a re-upload note is named as evidence, and a clip inside that recording takes its date',()=>{
  const stream={basis:'evidence_date',date:'2025-12-05',copy:null,estimate:null,evidence:{from:'2025-12-04',to:'2025-12-05',sources:['reupload_note']}};
  const text=describeDateEstimate(project(stream));
  assert.equal(text.card,'circa 2025-12-05 (date given with re-upload)');
  assert.match(text.detail,/^Placed between 2025-12-04 and 2025-12-05 using the date a community archivist gave when re-uploading the stream\..*not a verified/);
  const clip={basis:'source_estimate',date:'2025-12-05',copy:null,estimate:null,evidence:stream.evidence,source:{recording_id:'u2',basis:'evidence_date',relation:'inside_undated_recording'}};
  const out=projectDateEstimates({recordings:{u:clip,u2:stream}},catalog).recordings.u;
  const passed=describeDateEstimate(out);
  assert.equal(passed.card,'circa 2025-12-05 (part of an undated recording)');
  assert.match(passed.detail,/no catalog date either\. That recording is placed between 2025-12-04 and 2025-12-05 using the date a community archivist gave when re-uploading the stream, and its date is passed on\..*not a verified date\. Lists and year filters place this recording at 2025-12-05\.$/);
  assert.equal(circaDate(out),'2025-12-05');assert.equal(passed.circa,'2025-12-05');assert.equal(passed.copy.recording_id,'u2');
  assert.deepEqual(out,{basis:'source_estimate',date:'2025-12-05',copy:null,estimate:null,evidence:stream.evidence,source:{recording_id:'u2',relation:'inside_undated_recording'}});
  // The passed-on date and range must be the containing recording's own, and never come with a wording estimate.
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,date:'2025-12-04'},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,evidence:{...stream.evidence,to:'2025-12-06'}},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,estimate:est},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,source:{...clip.source,basis:'text_estimate'}},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>projectDateEstimates({recordings:{u:{...clip,evidence:undefined},u2:stream}},catalog),/Invalid source estimate/);
  assert.throws(()=>project({basis:'text_estimate',date:'2023-07',copy:null,estimate:est,evidence:stream.evidence}),/Invalid evidence date/);
});

test('a clip inside a stream dated by the archivist says whose date it carries',()=>{
  const dated=[{recording_id:'s',date_label:'2025-11-27',date_basis:'community_reupload_note'},{recording_id:'p',date_label:'2025-11-26',date_basis:'torrent_filename_utc'},{recording_id:'c',date_label:null},{recording_id:'c2',date_label:null}];
  const out=projectDateEstimates({recordings:{c:{basis:'source_recording',date:'2025-11-27',copy:null,estimate:null,source:{recording_id:'s',date:'2025-11-27',tier:'A',relation:'same_recording'}},
    c2:{basis:'source_recording',date:'2025-11-26',copy:null,estimate:null,source:{recording_id:'p',date:'2025-11-26',tier:'A',relation:'same_recording'}}}},dated).recordings;
  assert.equal(out.c.source.stated_date,true);
  assert.equal('stated_date' in out.c2.source,false);
  assert.match(describeDateEstimate(out.c).detail,/That recording's date is the day the community archivist u\/HIMRedits gave when re-uploading it, not a date displayed by the platform or an archive; it is used here as an estimate\./);
  assert.match(describeDateEstimate(out.c2).detail,/The date is that recording's upload or stream day, used here as an estimate\./);
  // the mark comes from the catalog only; the input cannot set it
  assert.throws(()=>projectDateEstimates({recordings:{c:{basis:'source_recording',date:'2025-11-27',copy:null,estimate:null,source:{recording_id:'s',date:'2025-11-27',tier:'A',relation:'same_recording',stated_date:true}}}},dated),/Unexpected source field/);
});

test('a clip inside a stream dated by the earlier index or a link post says whose record the date is',()=>{
  const dated=[{recording_id:'s',date_label:'2024-04-16',date_basis:'earlier_transcript_index'},{recording_id:'p',date_label:'2025-11-16',date_basis:'community_link_post'},{recording_id:'c',date_label:null},{recording_id:'c2',date_label:null}];
  const out=projectDateEstimates({recordings:{c:{basis:'source_recording',date:'2024-04-16',copy:null,estimate:null,source:{recording_id:'s',date:'2024-04-16',tier:'A',relation:'same_recording'}},
    c2:{basis:'source_recording',date:'2025-11-16',copy:null,estimate:null,source:{recording_id:'p',date:'2025-11-16',tier:'A',relation:'same_recording'}}}},dated).recordings;
  assert.equal(out.c.source.other_record,'index');assert.equal(out.c2.source.other_record,'post');
  assert.match(describeDateEstimate(out.c).detail,/That recording's date comes from the file name of an earlier transcript index, not from the platform or an archive; it is used here as an estimate\./);
  assert.match(describeDateEstimate(out.c2).detail,/That recording's date is the day of the first community post that linked it, not a date displayed by the platform or an archive; it is used here as an estimate\./);
  assert.throws(()=>projectDateEstimates({recordings:{c:{basis:'source_recording',date:'2024-04-16',copy:null,estimate:null,source:{recording_id:'s',date:'2024-04-16',tier:'A',relation:'same_recording',other_record:'index'}}}},dated),/Unexpected source field/);
});
