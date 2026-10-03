import { matchesAnalysisIdentity, analysisLoadingHash } from './supplement.mjs';
import { historicalPassageHref } from './historical-compatibility.mjs';

export async function validatePassageInsights(data, release) {
  if (!matchesAnalysisIdentity(data,release) || data.loadings_sha256 !== await analysisLoadingHash(release)) return null;
  const qids=release.questions.filter(q=>q.type==='score').map(q=>q.id);
  if (!Array.isArray(data.question_ids) || data.question_ids.length!==qids.length || data.question_ids.some(id=>!qids.includes(id)) || new Set(data.question_ids).size!==qids.length) return null;
  if (!Array.isArray(data.recording_ids) || data.recording_ids.length!==release.videos.length || data.recording_ids.some((id,i)=>id!==release.videos[i].recording_id)) return null;
  if(!Array.isArray(data.locators)||data.locators.length!==release.videos.length)return null;
  for(const locators of data.locators){if(!Array.isArray(locators)||locators.some(p=>!Array.isArray(p)||p.length!==3||!Number.isInteger(p[0])||p[0]<0||!Number.isInteger(p[1])||p[1]<0||typeof p[2]!=='string'||!/^[A-Za-z0-9_-]{1,160}$/.test(p[2])))return null;}
  if (!Array.isArray(data.rankings)||data.rankings.length!==qids.length||!Array.isArray(data.eligible_recordings)||data.eligible_recordings.length!==qids.length) return null;
  for (const [qi,rows] of data.rankings.entries()) {
    if (!Array.isArray(rows)||rows.length>release.videos.length) return null;
    if(!Number.isInteger(data.eligible_recordings[qi])||data.eligible_recordings[qi]!==rows.length)return null;
    const seen=new Set();
    for(const r of rows){
      if(!Array.isArray(r)||r.length!==12||!Number.isInteger(r[0])||r[0]<0||r[0]>=data.recording_ids.length||seen.has(r[0])||!Number.isInteger(r[1])||r[1]<3) return null;
      seen.add(r[0]);
      if(r.slice(2).some(v=>v!==null&&(typeof v!=='number'||!Number.isFinite(v)))) return null;
      if(r[2]<0||r[2]>4||r[3]<0||r[3]>2.00001||r[4]<0||r[5]>4||r[4]>r[5]||Math.abs(r[6])>4||r[7]!==null&&(r[7]<0||r[7]>4)||!Number.isInteger(r[8])||r[8]<0||!Number.isInteger(r[9])||r[9]<0) return null;
      for(const [chunk,time] of [[r[10],r[8]],[r[11],r[9]]])if(!data.locators[r[0]].some(p=>p[0]===chunk&&p[1]===time))return null;
    }
  }
  if(!Array.isArray(data.groups)||!data.groups.length) return null;
  const groups=new Set();
  for(const group of data.groups){
    if(!['all','year','genre','quality'].includes(group.dimension)||typeof group.label!=='string'||groups.has(`${group.dimension}:${group.label}`)||!Array.isArray(group.values)||group.values.length!==qids.length) return null;
    groups.add(`${group.dimension}:${group.label}`);
    for(const c of group.values){
      if(!Array.isArray(c)||c.length!==10||c.slice(0,6).some(n=>!Number.isInteger(n)||n<0)||!Number.isInteger(c[9])||c[9]<0) return null;
      if(c[1]+c[3]!==c[0]||c[2]+c[4]!==c[1]||c[5]>c[2]||c[6]!==null&&(!(c[6]>=0)||c[6]>1.00001)||c.slice(7,9).some(v=>v!==null&&(!Number.isFinite(v)||v<0||v>4.00001))) return null;
    }
  }
  return data;
}

export function rankPassageVariation(data, questionId, metric='sd') {
  const qi=data.question_ids.indexOf(questionId);if(qi<0)return[];
  const field={sd:3,delta:6,adjacent:7}[metric]??3;
  return [...data.rankings[qi]].filter(r=>r[field]!==null).sort((a,b)=>Math.abs(b[field])-Math.abs(a[field])||a[0]-b[0]);
}
export function coverageSummary(cell) {
  return {total:cell[0],scored:cell[2],insufficient:cell[3],missing:cell[4],coverage:cell[0]?cell[2]/cell[0]:null,entropy:cell[6],modelVariance:cell[7],betweenVariance:cell[8],recordings:cell[9]};
}
export function passageExtremeHref(data,video,row,kind='low',historical=false) {
  const time=row[kind==='high'?9:8],chunk=row[kind==='high'?11:10];
  const locator=data.locators[row[0]].find(p=>p[0]===chunk&&p[1]===time);
  if(!locator)return null;
  return historical ? historicalPassageHref(video.href,time) : `${video.href}?t=${Math.floor(time/1000)}#segment-${video.revision_id}-${locator[2]}`;
}

export function renderPassageInsights(root,release,data){
  const question=root.querySelector('[data-insights-question]'),metric=root.querySelector('[data-insights-metric]'),dimension=root.querySelector('[data-insights-dimension]');
  const category=root.querySelector('[data-insights-category]'),minimum=root.querySelector('[data-insights-minimum]');
  const ranks=root.querySelector('[data-insights-ranks]'),dashboard=root.querySelector('[data-insights-dashboard]'),summary=root.querySelector('[data-insights-summary]');
  const names=new Map(release.questions.map(q=>[q.id,q.short_name]));
  const option=(value,label)=>{const e=document.createElement('option');e.value=value;e.textContent=label;return e;};
  question.replaceChildren(...data.question_ids.map(id=>option(id,names.get(id)||id)));
  category.replaceChildren(option('','All categories'),...[...new Set(release.videos.map(v=>v.category||'unknown'))].sort().map(id=>option(id,id.replaceAll('_',' '))));
  const format=n=>n==null?'Unavailable':n.toFixed(3);
  let page=0;
  const table=(headers)=>{const t=document.createElement('table'),h=document.createElement('thead'),r=document.createElement('tr'),body=document.createElement('tbody');for(const label of headers){const th=document.createElement('th');th.scope='col';th.textContent=label;r.append(th);}h.append(r);t.append(h,body);return[t,body];};
  const cell=(row,value)=>{const td=document.createElement('td');td.textContent=value;row.append(td);return td;};
  const link=(href,label)=>{const a=document.createElement('a');a.href=href;a.textContent=label;return a;};
  function draw(){
    const qi=data.question_ids.indexOf(question.value),rows=rankPassageVariation(data,question.value,metric.value).filter(r=>r[1]>=Number(minimum.value)&&(!category.value||(release.videos[r[0]].category||'unknown')===category.value));
    page=Math.min(page,Math.max(0,Math.ceil(rows.length/20)-1));
    summary.textContent=`${data.eligible_recordings[qi].toLocaleString()} recordings have at least three scored passages for this question. Showing ${rows.length?page*20+1:0}–${Math.min(rows.length,(page+1)*20)} of ${rows.length} recordings matching these controls.`;
    const[t,body]=table(['Recording','Passages','Mean','Passage SD','End − start','Adjacent change','Extremes']);
    for(const r of rows.slice(page*20,(page+1)*20)){
      const video=release.videos[r[0]],tr=document.createElement('tr'),title=cell(tr,'');title.append(link(video.href,video.title));
      const button=document.createElement('button');button.type='button';button.textContent='Inspect recording';button.addEventListener('click',()=>document.dispatchEvent(new CustomEvent('analysis-select-recording',{detail:{recording_id:video.recording_id}})));title.append(document.createElement('br'),button);
      for(const v of [String(r[1]),format(r[2]),format(r[3]),format(r[6]),format(r[7])])cell(tr,v);
      const extremes=cell(tr,'');extremes.append(link(passageExtremeHref(data,video,r,'low',!!release.historical_input),`Low ${format(r[4])}`),document.createElement('br'),link(passageExtremeHref(data,video,r,'high',!!release.historical_input),`High ${format(r[5])}`));body.append(tr);
    }
    const controls=document.createElement('div');controls.className='insights-pagination';
    for(const [label,offset,disabled] of [['Previous page',-1,page===0],['Next page',1,(page+1)*20>=rows.length]]){const b=document.createElement('button');b.type='button';b.textContent=label;b.disabled=disabled;b.addEventListener('click',()=>{page+=offset;draw();});controls.append(b);}
    ranks.replaceChildren(t,controls);
    const groups=data.groups.filter(g=>g.dimension===dimension.value);
    const[dt,db]=table(['Group','Scored / total','Coverage','Insufficient','Missing','Model entropy','Model variance','Passage variance','Recordings']);
    for(const group of groups){const c=coverageSummary(group.values[qi]),tr=document.createElement('tr');
      for(const v of [group.label,`${c.scored.toLocaleString()} / ${c.total.toLocaleString()}`,c.coverage==null?'Unavailable':`${(100*c.coverage).toFixed(1)}%`,String(c.insufficient),String(c.missing),format(c.entropy),format(c.modelVariance),format(c.betweenVariance),String(c.recordings)])cell(tr,v);
      db.append(tr);
    }
    dashboard.replaceChildren(dt);
    const plot=root.querySelector('[data-insights-plot]');
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 640 220');svg.setAttribute('role','img');svg.setAttribute('aria-label','Between-passage score standard deviations for the displayed recordings');
    for(const tick of [0,1,2]){const y=185-tick*75,line=document.createElementNS(ns,'line');line.setAttribute('x1','30');line.setAttribute('x2','620');line.setAttribute('y1',String(y));line.setAttribute('y2',String(y));line.setAttribute('stroke','currentColor');line.setAttribute('opacity','.15');svg.append(line);const text=document.createElementNS(ns,'text');text.setAttribute('x','8');text.setAttribute('y',String(y+4));text.setAttribute('fill','currentColor');text.textContent=String(tick);svg.append(text);}
    rows.slice(page*20,(page+1)*20).forEach((r,i)=>{const rect=document.createElementNS(ns,'rect');rect.setAttribute('x',String(30+i*29));rect.setAttribute('y',String(185-r[3]*75));rect.setAttribute('width','20');rect.setAttribute('height',String(r[3]*75));rect.setAttribute('fill','var(--corpus-accent, #78b7df)');const title=document.createElementNS(ns,'title');title.textContent=`${release.videos[r[0]].title}: passage SD ${format(r[3])}`;rect.append(title);svg.append(rect);});
    for(const [x,y,text]of [[15,20,'Passage SD (0–2)'],[30,210,'Displayed recording rank →']]){const e=document.createElementNS(ns,'text');e.setAttribute('x',String(x));e.setAttribute('y',String(y));e.setAttribute('fill','currentColor');e.textContent=text;svg.append(e);}
    plot.replaceChildren(svg);
  }
  for(const element of [question,metric,dimension,category,minimum])element.addEventListener('change',()=>{page=0;draw();});
  draw();root.hidden=false;
}
