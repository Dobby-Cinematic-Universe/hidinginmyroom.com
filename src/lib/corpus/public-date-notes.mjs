// Notes on how a catalog date was established, for the bases that need a sentence:
// an upload date read from an archived copy of the video's own page, a file-name time converted to UTC,
// a day that a community archivist gave when re-uploading the stream (named in the label; no address),
// the date in the file name of an earlier transcript index, and the day of the first community post that linked
// the video (basis only: no post, address or name).
// A sidecar beside the catalog; it never changes a date.
const DAY=/^\d{4}-\d{2}-\d{2}$/;
const PAGE=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https?:\/\/(?:www\.|m\.)?youtube\.com\/watch\?[\w\-=&%.;]+$/;
const valid=v=>typeof v==='string'&&DAY.test(v)&&new Date(v).toISOString().slice(0,10)===v;
export function projectDateNotes(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));const recordings={};
  for(const [id,row] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);if(!target||!target.date_label)continue;
    for(const k of Object.keys(row))if(!['basis','captured','source','file_date','stated'].includes(k))throw Error(`Unexpected date note field ${k}`);
    if(row.basis!==target.date_basis)throw Error('Date note does not match the catalog date basis');
    if(row.basis==='archived_video_page'){
      if(!valid(row.captured)||row.captured<target.date_label||!PAGE.test(row.source||'')||row.file_date!=null)throw Error('Invalid archived page note');
      recordings[id]={basis:row.basis,captured:row.captured,source:row.source};
    }else if(row.basis==='torrent_filename_utc'){
      if(!valid(row.file_date)||row.captured!=null||row.source!=null)throw Error('Invalid file-name note');
      const gap=(Date.parse(row.file_date)-Date.parse(target.date_label))/864e5;
      if(gap!==0&&gap!==1)throw Error('File-name date must be the catalog date or the day after');
      recordings[id]={basis:row.basis,file_date:row.file_date};
    }else if(row.basis==='community_reupload_note'){
      // The day or the two consecutive days the archivist stated; the catalog shows the first.
      const s=row.stated;
      if(!Array.isArray(s)||s.length<1||s.length>2||!s.every(valid)||s[0]!==target.date_label||(s.length===2&&(Date.parse(s[1])-Date.parse(s[0]))/864e5!==1)
        ||row.captured!=null||row.source!=null||row.file_date!=null)throw Error('Invalid re-upload note');
      recordings[id]={basis:row.basis,stated:[...s]};
    }else if(row.basis==='earlier_transcript_index'||row.basis==='community_link_post'){
      if(row.captured!=null||row.source!=null||row.file_date!=null||row.stated!=null)throw Error('Invalid index or link-post note');
      recordings[id]={basis:row.basis};
    }else throw Error('Unsupported date note basis');
  }
  return {schema_version:1,recordings};
}
const LABELS={retained_archive_metadata:'Archive metadata',archive_filename:'Archive file name',filename_date:'Archive file name',torrent_filename:'Archive file name',
  archived_video_page:'Upload date shown on an archived copy of the video\'s YouTube page',torrent_filename_utc:'Archive file name, converted to UTC',
  community_reupload_note:'Day given by the community archivist u/HIMRedits with a re-upload of the stream',
  earlier_transcript_index:'Date in the file name of an earlier transcript index',community_link_post:'Day of the first community post that linked the video',unknown:'Not established'};
// Bases whose date is someone else's record of the day, not a date displayed by the platform or an archive.
export const OTHER_RECORD_BASES=['community_reupload_note','earlier_transcript_index','community_link_post'];
export function describeDateBasis(basis,note){
  const label=LABELS[basis]??(basis?String(basis).replace(/_/g,' '):'Not stated');
  if(!note)return label;
  if(note.basis==='archived_video_page')return `${label}, saved on ${note.captured}. The page shows the date in YouTube's own time zone, so it can differ by one day from dates elsewhere in the corpus.`;
  if(note.basis==='torrent_filename_utc')return note.file_date&&note.file_date!==undefined?`${label}. The file name carries ${note.file_date} on a clock that runs ten hours ahead of UTC.`:label;
  if(note.basis==='community_reupload_note')return `${label}${note.stated?.length===2?` (given as ${note.stated[0]} or ${note.stated[1]}; the earlier day is shown)`:''}. `+
    `The re-upload's audio was matched to this recording. This is the archivist's statement, not a date displayed by the platform or an archive, and it can be a day off.`;
  if(note.basis==='earlier_transcript_index')return `${label}. The index names each transcript file by date and video id. This is that index's record, not a date displayed by the platform or an archive, `+
    `and it can be a day off: checked against archived video pages, archived listings and community posts, 114 of 115 such dates were within a day.`;
  if(note.basis==='community_link_post')return `${label} (UTC). This is not a date displayed by the platform or an archive, and it can be a day off: `+
    `for recordings with a known date, the first such post fell within a day of that date for 84 of 90.`;
  return label;
}
