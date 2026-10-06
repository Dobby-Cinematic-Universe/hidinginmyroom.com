import { analysisLoadingHash, matchesAnalysisIdentity } from './supplement.mjs';
import { recordingProjection } from './recording-umap.mjs';
import { attachMapViewport } from './map-viewport.mjs';
import { nearbyMapPoints } from './recording-map-picking.mjs';

const finite = Number.isFinite;
export function completeFactorProfile(video, ids) {
  const row = ids.map(id => video.factor_scores?.[id]?.value);
  return row.every(finite) ? row : null;
}
export function standardizedFactorDistance(a, b, scales) {
  return Math.hypot(...a.map((value, index) => (value - b[index]) / scales[index]));
}
export async function validateDiscovery(release, data) {
  if (!matchesAnalysisIdentity(data, release) || data.loadings_sha256 !== await analysisLoadingHash(release)) return null;
  const ids = release.analysis.factors.map(f => f.id), k = ids.length;
  if (JSON.stringify(ids) !== JSON.stringify(data.factor_ids) || data.total_recordings !== release.videos.length) return null;
  const eligible = new Map(release.videos.flatMap(video => {
    const profile = completeFactorProfile(video, ids); return profile ? [[video.recording_id, profile]] : [];
  }));
  if (data.cohort_n !== eligible.size || data.excluded_recordings !== release.videos.length - eligible.size || data.points?.length !== eligible.size || data.neighbors?.length !== eligible.size) return null;
  const pca = data.pca;
  if (!pca || ![pca.mean,pca.scale,pca.explained_variance_ratio,pca.cumulative_variance_ratio].every(a => Array.isArray(a) && a.length === k && a.every(finite)) || pca.scale.some(v => v <= 0) || pca.components?.length !== k || !pca.components.every(a => a.length === k && a.every(finite))) return null;
  let sum = 0;
  for (let i=0;i<k;i++) {
    if (pca.explained_variance_ratio[i] < 0 || pca.explained_variance_ratio[i] > 1) return null;
    sum += pca.explained_variance_ratio[i];
    if (Math.abs(sum-pca.cumulative_variance_ratio[i])>1e-8) return null;
    for (let j=0;j<k;j++) {
      const dot=pca.components[i].reduce((s,v,t)=>s+v*pca.components[j][t],0);
      if(Math.abs(dot-(i===j?1:0))>1e-7) return null;
    }
  }
  const seen=new Set();
  for (const point of data.points) {
    if (!eligible.has(point.id) || seen.has(point.id) || ![point.x,point.y,point.radius].every(finite) || point.radius<0) return null;
    seen.add(point.id);
    const z=eligible.get(point.id).map((v,i)=>(v-pca.mean[i])/pca.scale[i]);
    for(const [index,key] of [[0,'x'],[1,'y']]) if(Math.abs(z.reduce((s,v,i)=>s+v*pca.components[index][i],0)-point[key])>1e-7) return null;
    if(Math.abs(Math.hypot(...z)-point.radius)>1e-7) return null;
  }
  seen.clear();
  for(const row of data.neighbors) {
    if(!eligible.has(row.id)||seen.has(row.id)||!Array.isArray(row.items)||row.items.length!==Math.min(5,eligible.size-1)) return null;
    seen.add(row.id); const local=new Set(); let previous=-1;
    for(const neighbor of row.items) {
      if(!eligible.has(neighbor.id)||neighbor.id===row.id||local.has(neighbor.id)||!finite(neighbor.distance)||neighbor.distance<previous) return null;
      local.add(neighbor.id); previous=neighbor.distance;
      if(Math.abs(neighbor.distance-standardizedFactorDistance(eligible.get(row.id),eligible.get(neighbor.id),pca.scale))>1e-6) return null;
    }
  }
  return data;
}

const el=(tag,text)=>{const e=document.createElement(tag);if(text!=null)e.textContent=text;return e;};
const svgEl=(tag,attrs)=>{const e=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [key,value] of Object.entries(attrs))e.setAttribute(key,String(value));return e;};
const fmt=value=>value.toFixed(2);
export function renderDiscovery(root,release,data,umap=null) {
  const videos=new Map(release.videos.map(v=>[v.recording_id,v]));
  const points=new Map(data.points.map(p=>[p.id,p]));
  const neighbors=new Map(data.neighbors.map(p=>[p.id,p.items]));
  const select=root.querySelector('[data-discovery-recording]'),search=root.querySelector('[data-discovery-search]');
  const color=root.querySelector('[data-discovery-color]'),chart=root.querySelector('[data-discovery-map]');
  const view=root.querySelector('[data-discovery-view]');
  const detail=root.querySelector('[data-discovery-detail]'),summary=root.querySelector('[data-discovery-summary]');
  if(!select||!search||!color||!chart||!detail||!summary||!view)return;
  const umapOption=view.querySelector('option[value="umap"]');
  umapOption.disabled=!umap;
  if(!umap)umapOption.textContent='UMAP · unavailable for this release';
  if(umap){const p=umap.method.parameters;root.querySelector('[data-discovery-umap-method]').textContent=`${umap.method.package} ${umap.method.package_version}; ${p.n_neighbors} neighbors; minimum distance ${p.min_dist}; ${p.metric} distance; seed ${p.random_state}. Precomputed from the same standardized nine-factor profiles as PCA. No scores or neighbors are recomputed when switching views.`;}
  let selected=release.videos[0]?.recording_id;
  let viewport=null;
  const cameras=new Map();
  const optionNodes=new Map(release.videos.map(v=>{const option=el('option',`${v.title} · ${v.date??'Undated'}${points.has(v.recording_id)?'':' · profile unavailable'}`);option.value=v.recording_id;return [v.recording_id,option];}));
  let lastQuery=null,matches=[];
  const picker=root.querySelector('[data-discovery-picking]');
  const clearPicker=()=>{if(picker){picker.replaceChildren();picker.hidden=true;}};
  const dismissPicker=()=>{clearPicker();chart.querySelector('svg')?.focus();};
  picker?.addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();dismissPicker();}});
  function options() {
    const query=search.value.trim().toLocaleLowerCase();
    if(query!==lastQuery){matches=release.videos.filter(v=>!query||`${v.title} ${v.date??''} ${v.category??''}`.toLocaleLowerCase().includes(query));select.replaceChildren(...matches.map(v=>optionNodes.get(v.recording_id)));lastQuery=query;}
    select.disabled = matches.length === 0;
    const count = root.querySelector('[data-discovery-result-count]');
    if (count) count.textContent = matches.length ? `${matches.length.toLocaleString()} recordings match the selector search.` : 'No recordings match the selector search. Clear or change the search to select a recording.';
    if(matches.some(v=>v.recording_id===selected))select.value=selected;
    else selected=matches[0]?.recording_id;
    show();
  }
  function chooseNeighbor(id) {
    selected=id;search.value='';options();select.focus();
  }
  function show() {
    clearPicker();
    const video=videos.get(selected);detail.replaceChildren();draw();
    const selectionStatus=root.querySelector('[data-discovery-selection]');
    if(selectionStatus)selectionStatus.textContent=video?`Selected: ${video.title}. ${points.has(selected)?'Similar recordings and the profile are shown below.':'No complete factor profile is available.'}`:'No recording selected.';
    if(!video){detail.append(el('p','No recordings match this search.'));return;}
    detail.append(el('h3',video.title));
    const link=el('a','Open transcript');link.href=video.href;detail.append(link);
    const inspect=el('button','Inspect passages');inspect.type='button';inspect.addEventListener('click',()=>document.dispatchEvent(new CustomEvent('analysis-select-recording',{detail:{recording_id:selected}})));detail.append(inspect);
    if(!points.has(selected)){detail.append(el('p','This recording has no complete saved factor profile, so a projection and neighbors cannot be calculated.'));return;}
    const nearby=neighbors.get(selected)??[];
    detail.append(el('p',`Full-profile distance from the cohort center: ${fmt(points.get(selected).radius)}. Greater distance identifies an unusual combination of saved scores; it is not a psychological classification.`));
    const list=el('ol');for(const item of nearby){const li=el('li');const button=el('button',`${videos.get(item.id).title} · distance ${fmt(item.distance)}`);button.type='button';button.addEventListener('click',()=>chooseNeighbor(item.id));li.append(button);list.append(li);}detail.append(el('h4','Closest recording profiles'),list);
    const disclosure=el('details'),caption=el('summary','Compare signed factor profiles');disclosure.append(caption);
    const scroll=el('div');scroll.className='discovery-table-scroll';const table=el('table'),head=el('thead'),tr=el('tr');
    tr.append(el('th','Factor'),el('th','Selected recording'),...nearby.map((_,i)=>el('th',`Neighbor ${i+1}`)));head.append(tr);table.append(head);
    const body=el('tbody');for(const factor of release.analysis.factors){const row=el('tr');row.append(el('th',factor.label));for(const id of [selected,...nearby.map(n=>n.id)])row.append(el('td',fmt(videos.get(id).factor_scores[factor.id].value)));body.append(row);}table.append(body);scroll.append(table);disclosure.append(el('p','Values are the signed saved factor scores. Neighbor numbering follows the list above. Factor labels are provisional.'),scroll);detail.append(disclosure);
  }
  let drawnProjection=null,projectedPoints=null,markSelection=null;
  function draw() {
    const width=720,height=420,pad=44;
    const projection=recordingProjection(data,umap,view.value),plotPoints=projection.points;
    const existing=chart.querySelector('svg');
    if(existing&&chart.dataset.color===color.value&&chart.dataset.projection===projection.id){markSelection(existing);return;}
    clearPicker();
    summary.textContent=`${data.cohort_n.toLocaleString()} complete profiles are mapped; ${data.excluded_recordings} recordings lack a complete profile. ${projection.summary}`;
    const projectionPoints=new Map(plotPoints.map(p=>[p.id,p]));
    const xs=plotPoints.map(p=>p.x),ys=plotPoints.map(p=>p.y);
    const xmin=Math.min(...xs),xmax=Math.max(...xs),ymin=Math.min(...ys),ymax=Math.max(...ys);
    const unitScale=Math.min((width-2*pad)/(xmax-xmin||1),(height-2*pad)/(ymax-ymin||1));
    const ox=(width-(xmax-xmin)*unitScale)/2,oy=(height-(ymax-ymin)*unitScale)/2;
    const px=x=>ox+(x-xmin)*unitScale,py=y=>height-oy-(y-ymin)*unitScale;
    const mark=svg=>{svg.querySelector('[data-discovery-selected-point]')?.remove();const point=projectionPoints.get(selected);if(point){const scale=viewport?.getCamera().scale??1;svg.querySelector('[data-map-layer]').append(svgEl('circle',{cx:px(point.x),cy:py(point.y),r:7/scale,fill:'none',stroke:'currentColor','stroke-width':2,'vector-effect':'non-scaling-stroke','pointer-events':'none','data-discovery-selected-point':''}));}};
    viewport?.destroy();viewport=null;
    drawnProjection=projection.id;projectedPoints=plotPoints.map(point=>({id:point.id,x:px(point.x),y:py(point.y)}));markSelection=mark;
    const svg=svgEl('svg',{viewBox:`0 0 ${width} ${height}`,role:'img',tabindex:0,'aria-label':`Recording ${projection.name} map. Use the recording selector for selection. Arrow keys pan, plus and minus zoom, and zero resets the view.`});
    const defs=svgEl('defs',{}),clip=svgEl('clipPath',{id:'recording-map-clip'});clip.append(svgEl('rect',{x:pad,y:pad,width:width-2*pad,height:height-2*pad}));defs.append(clip);svg.append(defs);
    const clipped=svgEl('g',{'clip-path':'url(#recording-map-clip)'}),layer=svgEl('g',{'data-map-layer':''}),axes=svgEl('g',{'pointer-events':'none'});clipped.append(layer);svg.append(clipped,axes);
    const categories=[...new Set(release.videos.map(v=>v.category??'Unknown'))].sort();
    const years=release.videos.map(v=>Number(v.date?.slice(0,4))).filter(y=>y>0),minyear=Math.min(...years),maxyear=Math.max(...years);
    const paint=video=>color.value==='year'?(video.date?`hsl(${250-210*(Number(video.date.slice(0,4))-minyear)/(maxyear-minyear||1)} 70% 55%)`:'#888'):`hsl(${categories.indexOf(video.category??'Unknown')*137.508%360} 65% 55%)`;
    const circles=[];
    for(const point of plotPoints){const c=svgEl('circle',{cx:px(point.x),cy:py(point.y),r:2.7,fill:paint(videos.get(point.id)),opacity:.55});c.append(svgEl('title',{}));c.firstChild.textContent=`${videos.get(point.id).title} · ${projection.axes[0]} ${fmt(point.x)} · ${projection.axes[1]} ${fmt(point.y)}`;layer.append(c);circles.push(c);}
    mark(svg);
    const ticks=Array.from({length:5},(_,i)=>{const sx=pad+(width-2*pad)*i/4,sy=height-pad-(height-2*pad)*i/4;const xt=svgEl('text',{x:sx,y:height-20,'text-anchor':'middle','font-size':11,fill:'currentColor'}),yt=svgEl('text',{x:pad-6,y:sy,'text-anchor':'end','font-size':11,fill:'currentColor'});axes.append(xt,yt);return {sx,sy,xt,yt};});
    const renderAxes=camera=>{for(const {sx,sy,xt,yt} of ticks){const x=xmin+((sx-camera.x)/camera.scale-ox)/unitScale,y=ymin+(height-oy-(sy-camera.y)/camera.scale)/unitScale;xt.textContent=fmt(x);yt.textContent=fmt(y);}};
    const label=svgEl('text',{x:width/2,y:height-3,'text-anchor':'middle','font-size':12,fill:'currentColor'});label.textContent=projection.axes[0];svg.append(label);
    const ylabel=svgEl('text',{x:12,y:height/2,transform:`rotate(-90 12 ${height/2})`,'text-anchor':'middle','font-size':12,fill:'currentColor'});ylabel.textContent=projection.axes[1];svg.append(ylabel);chart.replaceChildren(svg);chart.dataset.color=color.value;chart.dataset.projection=projection.id;
    let renderedScale=null;
    viewport=attachMapViewport({svg,layer,controls:root.querySelector('[data-discovery-map-controls]'),width,height,initial:cameras.get(projection.id),onChange:camera=>{cameras.set(projection.id,camera);if(renderedScale!==camera.scale){for(const circle of circles)circle.setAttribute('r',String(2.7/camera.scale));const selectedCircle=layer.querySelector('[data-discovery-selected-point]');selectedCircle?.setAttribute('r',String(7/camera.scale));renderedScale=camera.scale;}renderAxes(camera);clearPicker();}});
    svg.addEventListener('click',event=>{
      if(event.defaultPrevented||drawnProjection!==projection.id)return;
      viewport.flush();
      const frame=chart.closest?.('.analysis-chart-frame');let visibleBounds=null;
      if(frame){const rect=frame.getBoundingClientRect(),window=svg.ownerDocument.defaultView;const left=rect.left+frame.clientLeft,top=rect.top+frame.clientTop,right=Math.min(left+frame.clientWidth,window?.innerWidth??Infinity),bottom=Math.min(top+frame.clientHeight,window?.innerHeight??Infinity);visibleBounds={x:Math.max(0,left),y:Math.max(0,top),width:Math.max(0,right-Math.max(0,left)),height:Math.max(0,bottom-Math.max(0,top))};}
      const candidates=nearbyMapPoints(projectedPoints,{x:event.clientX,y:event.clientY},layer.getScreenCTM(),svg.getScreenCTM(),{x:pad,y:pad,width:width-2*pad,height:height-2*pad},8,visibleBounds);
      clearPicker();
      if(candidates.length===1){chooseNeighbor(candidates[0].id);return;}
      if(!picker)return;
      picker.hidden=false;
      const heading=el('h3',candidates.length?'Choose a nearby map recording':'No recording at this map location');picker.append(heading);
      const dismiss=el('button','Dismiss map choices');dismiss.type='button';dismiss.addEventListener('click',dismissPicker);picker.append(dismiss);
      if(!candidates.length){picker.append(el('p','Click closer to a visible dot, zoom in, or use the recording selector.'));return;}
      picker.append(el('p',`${candidates.length} recordings are within 8 screen pixels of this click, nearest first. These are two-dimensional map candidates, not the nine-factor closest-profile list. Zoom in or use the recording selector to narrow the choice.`));
      const list=el('ol');for(const candidate of candidates){const video=videos.get(candidate.id),li=el('li'),button=el('button',`${video.title} · ${video.date??'Undated'} · ${candidate.distance.toFixed(1)} px from click`);button.type='button';button.addEventListener('click',()=>chooseNeighbor(candidate.id));li.append(button);list.append(li);}picker.append(list);list.querySelector('button')?.focus();
    });
    const legend=root.querySelector('[data-discovery-legend]');legend.replaceChildren();
    for(const [text,fill] of color.value==='year'?[[`${minyear} (purple) → ${maxyear} (orange)`,''],['Undated','#888']]:categories.map(c=>[c,paint({category:c})])){const span=el('span',text);if(fill){const marker=el('i');marker.style.backgroundColor=fill;span.prepend(marker);}legend.append(span);}
  }
  search.addEventListener('input',options);select.addEventListener('change',()=>{selected=select.value;show();});color.addEventListener('change',draw);view.addEventListener('change',draw);options();
  const variance=root.querySelector('[data-discovery-variance]');
  if(variance){const table=el('table'),head=el('tr');for(const label of ['Component','Variation retained','Cumulative'])head.append(el('th',label));table.append(head);for(let i=0;i<data.factor_ids.length;i++){const row=el('tr');row.append(el('td',`PC${i+1}`),el('td',`${(data.pca.explained_variance_ratio[i]*100).toFixed(1)}%`),el('td',`${(data.pca.cumulative_variance_ratio[i]*100).toFixed(1)}%`));table.append(row);}variance.append(table);}
  const outliers=root.querySelector('[data-discovery-outliers]');if(outliers){const list=el('ol');for(const point of [...data.points].sort((a,b)=>b.radius-a.radius).slice(0,10)){const li=el('li'),button=el('button',`${videos.get(point.id).title} · center distance ${fmt(point.radius)}`);button.type='button';button.addEventListener('click',()=>chooseNeighbor(point.id));li.append(button);list.append(li);}outliers.append(list);}
}
