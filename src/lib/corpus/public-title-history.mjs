// Earlier titles seen in archived channel listings: a sidecar beside the catalog. The catalog
// title is not changed. Titles are observations of a public listing, not confirmed edits.
const DAY=/^\d{4}-\d{2}-\d{2}$/;
export function projectTitleHistory(input,catalog){
  const byId=new Map(catalog.map(r=>[r.recording_id,r]));
  const recordings={};
  for(const [id,rows] of Object.entries(input?.recordings||{})){
    const target=byId.get(id);
    if(!target||!Array.isArray(rows))continue;
    const seen=new Set([target.title.trim().toLocaleLowerCase()]),out=[];
    for(const row of rows){
      for(const k of Object.keys(row))if(!['title','seen'].includes(k))throw Error(`Unexpected title history field ${k}`);
      if(typeof row.title!=='string'||!row.title.trim()||row.title.length>300||!DAY.test(row.seen)||!Number.isFinite(Date.parse(row.seen)))throw Error('Invalid title history row');
      const key=row.title.trim().toLocaleLowerCase();
      if(seen.has(key))continue;
      seen.add(key);out.push({title:row.title.trim(),seen:row.seen});
    }
    if(out.length)recordings[id]=out;
  }
  return {schema_version:1,recordings};
}
