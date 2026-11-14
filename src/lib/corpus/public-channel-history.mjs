// Channel history from archived channel pages: subscriber readings (each with the archived page
// it was read from) and typical early view counts per half-year. Aggregates only; no titles.
const DAY=/^\d{4}-\d{2}-\d{2}$/,SOURCE=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https:\/\/www\.youtube\.com\/[\w@./%-]+$/;
const count=v=>Number.isInteger(v)&&v>=0;
export function projectChannelHistory(input){
  if(!input)return {schema_version:1,channels:[],early_views:[]};
  const channels=(input.channels||[]).map(c=>{
    if(!['main','second'].includes(c.key)||typeof c.label!=='string'||c.label.length>80)throw Error('Invalid channel');
    const readings=c.readings.map(r=>{
      if(!DAY.test(r.date)||!count(r.subscribers)||!count(r.rounded_to)||!SOURCE.test(r.source))throw Error('Invalid subscriber reading');
      return {date:r.date,subscribers:r.subscribers,rounded_to:r.rounded_to,source:r.source};
    });
    if(readings.some((r,i)=>i&&r.date<=readings[i-1].date))throw Error('Subscriber readings must be in date order');
    // Channel names as archived pages showed them. A withheld name (null) still records that a rename happened.
    const names=(c.names||[]).map(n=>{
      if(!(n.name===null||(typeof n.name==='string'&&n.name.length>0&&n.name.length<=80))||!DAY.test(n.first_seen)||!DAY.test(n.last_seen)||n.first_seen>n.last_seen||!SOURCE.test(n.source))throw Error('Invalid channel name entry');
      if((n.changed_after===null)!==(n.changed_before===null)||(n.changed_after!==null&&(!DAY.test(n.changed_after)||!DAY.test(n.changed_before)||n.changed_after>n.changed_before||n.changed_before>n.first_seen)))throw Error('Invalid channel name interval');
      return {name:n.name,first_seen:n.first_seen,last_seen:n.last_seen,changed_after:n.changed_after,changed_before:n.changed_before,source:n.source};
    });
    if(names.some((n,i)=>i&&n.first_seen<=names[i-1].last_seen))throw Error('Channel names must be in date order');
    return {key:c.key,label:c.label,readings,names};
  });
  const early_views=(input.early_views||[]).map(v=>{
    if(!/^\d{4}H[12]$/.test(v.period)||![v.videos,v.median_views,v.p25,v.p75,v.median_age_days].every(count)||!(v.per_1000_subscribers===null||count(v.per_1000_subscribers)))throw Error('Invalid early view row');
    return {period:v.period,videos:v.videos,median_views:v.median_views,p25:v.p25,p75:v.p75,median_age_days:v.median_age_days,per_1000_subscribers:v.per_1000_subscribers};
  });
  return {schema_version:1,channels,early_views};
}
