import test from 'node:test';
import assert from 'node:assert/strict';
import {projectDateNotes,describeDateBasis} from '../../src/lib/corpus/public-date-notes.mjs';

const catalog=[{recording_id:'p',date_label:'2024-02-04',date_basis:'archived_video_page'},{recording_id:'k',date_label:'2025-08-31',date_basis:'torrent_filename_utc'},{recording_id:'u',date_label:null,date_basis:'unknown'}];
const page={basis:'archived_video_page',captured:'2024-02-06',source:'https://web.archive.org/web/20240206173207/https://www.youtube.com/watch?v=abcdefghijk'};

test('date notes are kept only for dated recordings and must match the catalog basis',()=>{
  const out=projectDateNotes({recordings:{p:page,k:{basis:'torrent_filename_utc',file_date:'2025-09-01'},u:page,gone:page}},catalog);
  assert.deepEqual(Object.keys(out.recordings),['p','k']);
  assert.match(describeDateBasis('archived_video_page',out.recordings.p),/archived copy of the video's YouTube page, saved on 2024-02-06.*differ by one day/);
  assert.match(describeDateBasis('torrent_filename_utc',out.recordings.k),/converted to UTC\. The file name carries 2025-09-01/);
  assert.equal(describeDateBasis('retained_archive_metadata'),'Archive metadata');
  assert.equal(describeDateBasis('some_new_basis'),'some new basis');
  assert.throws(()=>projectDateNotes({recordings:{k:page}},catalog),/does not match/);
  assert.throws(()=>projectDateNotes({recordings:{p:{...page,source:'https://example.com/x'}}},catalog),/Invalid archived page note/);
  assert.throws(()=>projectDateNotes({recordings:{k:{basis:'torrent_filename_utc',file_date:'2025-09-05'}}},catalog),/catalog date or the day after/);
});

test('a day given with a community re-upload is described as the archivist\'s statement',()=>{
  const dated=[{recording_id:'a',date_label:'2025-12-05',date_basis:'community_reupload_note'},{recording_id:'b',date_label:'2025-11-18',date_basis:'community_reupload_note'}];
  const out=projectDateNotes({recordings:{a:{basis:'community_reupload_note',stated:['2025-12-05']},b:{basis:'community_reupload_note',stated:['2025-11-18','2025-11-19']}}},dated);
  assert.deepEqual(out.recordings,{a:{basis:'community_reupload_note',stated:['2025-12-05']},b:{basis:'community_reupload_note',stated:['2025-11-18','2025-11-19']}});
  assert.equal(describeDateBasis('community_reupload_note'),'Day given by the community archivist u/HIMRedits with a re-upload of the stream');
  assert.match(describeDateBasis('community_reupload_note',out.recordings.a),/^Day given by the community archivist u\/HIMRedits with a re-upload of the stream\. The re-upload's audio was matched.*archivist's statement, not a date displayed by the platform or an archive, and it can be a day off\.$/);
  assert.match(describeDateBasis('community_reupload_note',out.recordings.b),/\(given as 2025-11-18 or 2025-11-19; the earlier day is shown\)\. The re-upload's audio/);
  // the catalog date must be the first stated day; two stated days must be consecutive; no address or name is carried
  assert.throws(()=>projectDateNotes({recordings:{a:{basis:'community_reupload_note',stated:['2025-12-04']}}},dated),/Invalid re-upload note/);
  assert.throws(()=>projectDateNotes({recordings:{b:{basis:'community_reupload_note',stated:['2025-11-18','2025-11-20']}}},dated),/Invalid re-upload note/);
  assert.throws(()=>projectDateNotes({recordings:{a:{basis:'community_reupload_note',stated:[]}}},dated),/Invalid re-upload note/);
  assert.throws(()=>projectDateNotes({recordings:{a:{basis:'community_reupload_note',stated:['2025-12-05'],source:'https://www.youtube.com/watch?v=abcdefghijk'}}},dated),/Invalid re-upload note/);
  assert.throws(()=>projectDateNotes({recordings:{a:{basis:'community_reupload_note',stated:['2025-12-05'],author:'x'}}},dated),/Unexpected date note field/);
});
