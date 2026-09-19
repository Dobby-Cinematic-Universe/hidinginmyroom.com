export function projectAttribution(input, ids) {
  const recordings={};
  for(const [id,row] of Object.entries(input.recordings||{})){
    if(!ids.has(id)||!['cloud','third_party'].includes(row.origin))continue;
    const clean=(value)=>{
      if(value==null)return null;
      if(typeof value!=='string'||value.length>2000||/\/home\/|\/mnt\/|research\/|api[_-]?key|bearer\s/i.test(value))throw Error('Unsafe public attribution');
      return value.replace(/; original SHA-256 [a-f0-9]{64}/g,'');
    };
    recordings[id]={origin:row.origin,attribution:clean(row.attribution),model:clean(row.model),coverage_note:clean(row.coverage_note)};
  }
  return {schema_version:1,recordings};
}
