// Channel history from archived channel pages: subscriber readings (each with the archived page
// it was read from) and typical early view counts per half-year. Aggregates only; no titles.
// Month-end subscriber and total-view values come from archived copies of a statistics site's pages for the same
// channel id; each row lists the archived pages it was worked out from. Total-view readings come from archived
// copies of the channel's own About page.
// The archived URL keeps any query string it was captured with (e.g. ?disable_polymer=1); dropping it makes the
// Wayback Machine redirect to a nearby capture of a different URL, which can show a different channel name.
const DAY=/^\d{4}-\d{2}-\d{2}$/,SOURCE=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https:\/\/(?:www\.)?youtube\.com\/[\w@./%-]+(?:\?[\w=&%.-]+)?$/;
const STATS=/^https:\/\/web\.archive\.org\/web\/\d{14}\/https:\/\/socialblade\.com\/youtube\/(?:channel|c|user)\/[\w-]+(?:\/monthly)?$/;
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
    const monthly=(c.monthly||[]).map(m=>{
      if(!DAY.test(m.date)||!(m.subscribers===null||count(m.subscribers))||!(m.views===null||count(m.views))||(m.subscribers===null&&m.views===null)||(m.subscribers!==null&&!(count(m.rounded_to)&&m.rounded_to>=1))||
        !Array.isArray(m.sources)||m.sources.length<1||m.sources.length>4||new Set(m.sources).size!==m.sources.length||!m.sources.every(u=>STATS.test(u)))throw Error('Invalid month-end statistics row');
      return {date:m.date,subscribers:m.subscribers,rounded_to:m.subscribers===null?null:m.rounded_to,views:m.views,sources:[...m.sources]};
    });
    if(monthly.some((m,i)=>i&&m.date<=monthly[i-1].date))throw Error('Month-end statistics must be in date order');
    const view_readings=(c.view_readings||[]).map(r=>{
      if(!DAY.test(r.date)||!count(r.views)||!SOURCE.test(r.source))throw Error('Invalid total-view reading');
      return {date:r.date,views:r.views,source:r.source};
    });
    if(view_readings.some((r,i)=>i&&r.date<view_readings[i-1].date))throw Error('Total-view readings must be in date order');
    if(!(c.monthly_views_uncertainty==null||count(c.monthly_views_uncertainty)))throw Error('Invalid month-end view uncertainty');
    return {key:c.key,label:c.label,readings,names,monthly,view_readings,monthly_views_uncertainty:c.monthly_views_uncertainty??null};
  });
  const early_views=(input.early_views||[]).map(v=>{
    if(!/^\d{4}H[12]$/.test(v.period)||![v.videos,v.median_views,v.p25,v.p75,v.median_age_days].every(count)||!(v.per_1000_subscribers===null||count(v.per_1000_subscribers)))throw Error('Invalid early view row');
    return {period:v.period,videos:v.videos,median_views:v.median_views,p25:v.p25,p75:v.p75,median_age_days:v.median_age_days,per_1000_subscribers:v.per_1000_subscribers};
  });
  return {schema_version:1,channels,early_views};
}
