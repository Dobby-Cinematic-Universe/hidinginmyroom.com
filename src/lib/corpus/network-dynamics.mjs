import {createHash} from 'node:crypto';

const hash=/^[a-f0-9]{64}$/;
export const publicNetworkLabels=Object.freeze(['Chihiro','Mila','Sunny','Pia','Tomoko','Faster','Sabrina','Cammie (mother)','Dave Gibson','Pushkin','Mimo','Toffee','Deepak','Kimberly','Eugene','Sora','George','Elvis','Nobita','Blood Bucket','Ice Poseidon','Chris Broad','Anime Man','Onision','Sam Pepper','Ben Dean','Venus Angelic','Sharla','David Bond','Keemstar','Filthy Frank','Coach Red','Asmongold','Johnny Somali','Andrew Tate','Logan Paul','Trash Taste','Casey Neistat','Sora the Troll','Dogen']);
const allowedNames=new Set(publicNetworkLabels);
const exact=(value,keys,field)=>{
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).sort().join('|')!==[...keys].sort().join('|'))throw new Error(`Invalid network ${field} fields`);
};
const number=(value,field,min=0,max=Infinity)=>{
  if(typeof value!=='number'||!Number.isFinite(value)||value<min||value>max)throw new Error(`Invalid network ${field}`);
  return value;
};
const nullable=(value,field,min=0,max=Infinity)=>value===null?null:number(value,field,min,max);
const text=(value,field)=>{
  if(typeof value!=='string'||!value.trim()||value.length>100||/[<>\x00-\x1f]/.test(value)||/https?:|file:|\/home\/|\/mnt\//i.test(value))throw new Error(`Invalid network ${field}`);
  return value;
};
const month=(value)=>{if(typeof value!=='string'||!/^20\d{2}-(0[1-9]|1[0-2])$/.test(value))throw new Error('Invalid network month');return value;};
const array=(value,max,field)=>{if(!Array.isArray(value)||value.length>max)throw new Error(`Invalid network ${field}`);return value;};
export function networkDigest(value){return createHash('sha256').update(JSON.stringify(value)).digest('hex');}

/** This exact schema is the publication boundary, not a pass-through for research JSON. */
export function validateNetworkProjection(value,expected){
  exact(value,['schema_version','kind','generated_at','binding','payload_sha256','data'],'projection');
  if(value.schema_version!==1||value.kind!=='himr_public_mention_dynamics')throw new Error('Unsupported network projection');
  if(typeof value.generated_at!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value.generated_at)||!Number.isFinite(Date.parse(value.generated_at)))throw new Error('Invalid network generation date');
  exact(value.binding,['corpus_release','summary_release','corpus_archive_sha256','corpus_manifest_sha256','transcript_input_sha256','registry_sha256','builder_sha256','method_version'],'binding');
  for(const field of ['corpus_archive_sha256','corpus_manifest_sha256','transcript_input_sha256','registry_sha256','builder_sha256'])if(!hash.test(value.binding[field]))throw new Error(`Invalid network ${field}`);
  if(!/^release_[a-f0-9]{24}$/.test(value.binding.corpus_release)||!/^summaries_[a-f0-9]{24}$/.test(value.binding.summary_release))throw new Error('Invalid network release binding');
  if(value.binding.method_version!=='mention-dynamics-public-v1')throw new Error('Unsupported network method version');
  for(const [field,wanted] of Object.entries(expected||{}))if(value.binding[field]!==wanted)throw new Error(`Stale network ${field} binding`);
  if(!hash.test(value.payload_sha256)||networkDigest(value.data)!==value.payload_sha256)throw new Error('Network projection digest differs');
  const d=value.data;
  exact(d,['counts','names','annual','monthly','edges','bursts','shifts','agreement','sensitivity'],'data');
  exact(d.counts,['recordings','retained_recordings','duplicate_transcript_exclusions','undated_recordings','weighted_matches','paired_recordings','draws','labelled_words','unlabelled_words','ambiguous_duplicate_date_groups'],'counts');
  for(const [key,n] of Object.entries(d.counts)){number(n,key);if(key!=='weighted_matches'&&!Number.isInteger(n))throw new Error('Invalid network count integer');}
  if(d.counts.retained_recordings>d.counts.recordings||d.counts.undated_recordings>d.counts.retained_recordings||d.counts.paired_recordings>d.counts.recordings||d.counts.recordings-d.counts.retained_recordings!==d.counts.duplicate_transcript_exclusions)throw new Error('Inconsistent network counts');
  const names=new Set();
  for(const row of array(d.names,100,'names')){
    exact(row,['name','group'],'name');text(row.name,'name');if(!allowedNames.has(row.name))throw new Error('Unapproved network name');
    if(names.has(row.name))throw new Error('Duplicate network name');names.add(row.name);
    if(!['on_channel','public_creator'].includes(row.group))throw new Error('Invalid network group');
  }
  const bands=['spread','top_share','active_names','rate'];let previousYear=0;
  for(const row of array(d.annual,100,'annual')){
    exact(row,['year','words','spread','top_share','active_names','rate','import_spread','all_renditions_spread','all_renditions_rate','all_renditions_words','question_filtered_spread','question_filtered_rate'],'annual row');
    if(!Number.isInteger(row.year)||row.year<2000||row.year>2100||row.year<=previousYear)throw new Error('Invalid network year order');previousYear=row.year;
    number(row.words,'annual words');nullable(row.import_spread,'import spread',0,1);
    nullable(row.all_renditions_spread,'all-renditions spread',0,1);nullable(row.all_renditions_rate,'all-renditions rate');nullable(row.all_renditions_words,'all-renditions words');nullable(row.question_filtered_spread,'question-filtered spread',0,1);nullable(row.question_filtered_rate,'question-filtered rate');
    for(const key of bands){array(row[key],3,key);if(row[key].length!==3)throw new Error('Invalid network interval');row[key].forEach(v=>nullable(v,key,0,['spread','top_share'].includes(key)?1:Infinity));if(row[key][0]!==null&&row[key][2]!==null&&row[key][0]>row[key][2])throw new Error('Invalid network interval order');}
  }
  let priorMonth='';
  for(const row of array(d.monthly,2400,'monthly')){
    exact(row,['month','recordings','own_job_share','rates'],'monthly row');month(row.month);
    if(row.month<=priorMonth)throw new Error('Invalid network month order');priorMonth=row.month;
    number(row.recordings,'monthly recordings');nullable(row.own_job_share,'own-job share',0,1);
    exact(row.rates,[...names],'monthly rates');for(const n of Object.values(row.rates))nullable(n,'monthly rate');
  }
  const pairs=new Set();
  for(const row of array(d.edges,5000,'edges')){
    exact(row,['a','b','weight','baseline','lift','eras'],'edge');
    if(!names.has(row.a)||!names.has(row.b)||row.a===row.b)throw new Error('Invalid network endpoints');
    const pair=[row.a,row.b].sort().join('|');if(pairs.has(pair))throw new Error('Duplicate network edge');pairs.add(pair);
    for(const key of ['weight','baseline','lift'])number(row[key],key);
    exact(row.eras,['2015-2019','2020-2022','2023-2026'],'eras');Object.values(row.eras).forEach(n=>number(n,'era weight'));
  }
  for(const row of array(d.bursts,1000,'bursts')){exact(row,['name','start','end','months'],'burst');if(!names.has(row.name))throw new Error('Invalid burst name');month(row.start);month(row.end);if(row.start>row.end)throw new Error('Invalid burst period');number(row.months,'burst months',1);}
  for(const row of array(d.shifts,50,'shifts')){exact(row,['month','excess','percentile','permutation_p','own_job_share'],'shift');month(row.month);nullable(row.excess,'excess',-Infinity);nullable(row.percentile,'percentile',0,100);nullable(row.permutation_p,'p',0,1);nullable(row.own_job_share,'shift source share',0,1);}
  for(const row of array(d.agreement,100,'agreement')){exact(row,['name','recordings','presence_agreement','rank_correlation'],'agreement');if(!names.has(row.name))throw new Error('Invalid agreement name');number(row.recordings,'agreement recordings');nullable(row.presence_agreement,'agreement',0,1);nullable(row.rank_correlation,'correlation',-1,1);}
  for(const row of array(d.sensitivity,100,'sensitivity')){exact(row,['year','rates'],'sensitivity');if(!d.annual.some(r=>r.year===row.year))throw new Error('Invalid sensitivity year');exact(row.rates,[...names],'sensitivity rates');for(const pair of Object.values(row.rates)){array(pair,2,'sensitivity pair');if(pair.length!==2)throw new Error('Invalid sensitivity pair');pair.forEach(v=>nullable(v,'sensitivity rate'));}}
  return value;
}

/** Explicit public allowlist: no excerpts, identities, tone profiles, samples or private paths. */
export function createNetworkProjection(results,binding,generatedAt=new Date().toISOString()){
  const names=results.entities.map((name,i)=>({name,group:results.kinds[i]==='public_creator'?'public_creator':'on_channel'}));
  const y=results.yearly;
  const band=(key,i)=>[y[`${key}_lo`]?.[i]??null,y[key]?.[i]??null,y[`${key}_hi`]?.[i]??null];
  const data={
    counts:{recordings:results.counts.recordings,retained_recordings:results.counts.kept_after_dedupe,duplicate_transcript_exclusions:results.counts.duplicates_removed,undated_recordings:results.counts.undated_recordings,weighted_matches:results.counts.expected_mentions_main,paired_recordings:results.counts.paired_recordings,draws:results.monte_carlo_draws,labelled_words:results.counts.certain_labelled_words??0,unlabelled_words:results.counts.unlabelled_words??0,ambiguous_duplicate_date_groups:results.counts.ambiguous_duplicate_date_groups??0},
    names,
    annual:results.years.map((year,i)=>({year,words:y.exposure_words[i],spread:band('H',i),top_share:band('top',i),active_names:band('active',i),rate:band('circle_rate',i),import_spread:y.H_import_only?.[i]??null,all_renditions_spread:results.robustness?.nondeduplicated?.H?.[i]??null,all_renditions_rate:results.robustness?.nondeduplicated?.circle_rate?.[i]??null,all_renditions_words:results.robustness?.nondeduplicated?.exposure_words?.[i]??null,question_filtered_spread:results.robustness?.strict?.H?.[i]??null,question_filtered_rate:results.robustness?.strict?.circle_rate?.[i]??null})),
    monthly:results.months.map((m,i)=>({month:m,recordings:results.monthly.n_recordings[i],own_job_share:results.monthly.own_share?.[i]??null,rates:Object.fromEntries(names.map((n,j)=>[n.name,results.monthly.rate[j][i]??null]))})),
    edges:results.edges.map(e=>({a:e.a,b:e.b,weight:e.weight,baseline:e.null,lift:e.lift,eras:Object.fromEntries(['2015-2019','2020-2022','2023-2026'].map(era=>[era,results.era_edges?.[era]?.find(x=>x.a===e.a&&x.b===e.b)?.weight??0]))})),
    // Only the corrected builder may supply heuristic calendar episodes: no p-values.
    bursts:results.methods?.bursts?.includes('No Poisson p-values')?(results.burst_episodes||[]).map(r=>({name:r.entity,start:r.start,end:r.end,months:r.months})):[],
    // Candidate dates from historical model joins have no refreshed public input binding.
    shifts:[],
    agreement:[],
    sensitivity:results.yearly.scenario_rates?results.years.map((year,i)=>({year,rates:Object.fromEntries(names.map((n,j)=>[n.name,[results.yearly.scenario_rates.main[j][i]??null,results.yearly.scenario_rates.strict[j][i]??null]]))})):[],
  };
  return validateNetworkProjection({schema_version:1,kind:'himr_public_mention_dynamics',generated_at:generatedAt,binding:{...binding},payload_sha256:networkDigest(data),data},binding);
}
