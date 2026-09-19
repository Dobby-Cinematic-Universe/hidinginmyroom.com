export function projectAttribution(input, ids) {
  const recordings={};
  for(const [id,row] of Object.entries(input.recordings||{})){
    if(!ids.has(id)||!['cloud','third_party','catalog'].includes(row.origin))continue;
    const clean=(value)=>{
      if(value==null)return null;
      if(typeof value!=='string'||value.length>2000||/\/home\/|\/mnt\/|research\/|api[_-]?key|bearer\s/i.test(value))throw Error('Unsafe public attribution');
      return value.replace(/; original SHA-256 [a-f0-9]{64}/g,'');
    };
    recordings[id]={origin:row.origin,attribution:clean(row.attribution),model:clean(row.model),coverage_note:clean(row.coverage_note)};
    if(row.media_status!==undefined){
      if(!['no_audio','transcript_only'].includes(row.media_status))throw Error('Invalid public media status');
      recordings[id].media_status=row.media_status;
    }
  }
  return {schema_version:1,recordings};
}
