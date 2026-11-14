// Notes on how a catalog date was established, for the two bases that need a sentence:
// an upload date read from an archived copy of the video's own page, and a file-name time converted to UTC.
// A sidecar beside the catalog; it never changes a date.
const DAY=/^\d{4}-\d{2}-\d{2}$/;
const PAGE=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https?:\/\/(?:www\.|m\.)?youtube\.com\/watch\?[\w\-=&%.;]+$/;
const valid=v=>typeof v==='string'&&DAY.test(v)&&new Date(v).toISOString().slice(0,10)===v;
export function projectDateNotes(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));const recordings={};
  for(const [id,row] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);if(!target||!target.date_label)continue;
    for(const k of Object.keys(row))if(!['basis','captured','source','file_date'].includes(k))throw Error(`Unexpected date note field ${k}`);
    if(row.basis!==target.date_basis)throw Error('Date note does not match the catalog date basis');
    if(row.basis==='archived_video_page'){
      if(!valid(row.captured)||row.captured<target.date_label||!PAGE.test(row.source||'')||row.file_date!=null)throw Error('Invalid archived page note');
      recordings[id]={basis:row.basis,captured:row.captured,source:row.source};
    }else if(row.basis==='torrent_filename_utc'){
      if(!valid(row.file_date)||row.captured!=null||row.source!=null)throw Error('Invalid file-name note');
      const gap=(Date.parse(row.file_date)-Date.parse(target.date_label))/864e5;
      if(gap!==0&&gap!==1)throw Error('File-name date must be the catalog date or the day after');
      recordings[id]={basis:row.basis,file_date:row.file_date};
    }else throw Error('Unsupported date note basis');
  }
  return {schema_version:1,recordings};
}
const LABELS={retained_archive_metadata:'Archive metadata',archive_filename:'Archive file name',filename_date:'Archive file name',torrent_filename:'Archive file name',
  archived_video_page:'Upload date shown on an archived copy of the video\'s YouTube page',torrent_filename_utc:'Archive file name, converted to UTC',unknown:'Not established'};
export function describeDateBasis(basis,note){
  const label=LABELS[basis]??(basis?String(basis).replace(/_/g,' '):'Not stated');
  if(!note)return label;
  if(note.basis==='archived_video_page')return `${label}, saved on ${note.captured}. The page shows the date in YouTube's own time zone, so it can differ by one day from dates elsewhere in the corpus.`;
  if(note.basis==='torrent_filename_utc')return note.file_date&&note.file_date!==undefined?`${label}. The file name carries ${note.file_date} on a clock that runs ten hours ahead of UTC.`:label;
  return label;
}
