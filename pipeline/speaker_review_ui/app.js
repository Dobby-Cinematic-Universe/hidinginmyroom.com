// Saved segment exceptions override label-wide decisions; latest wins within a scope.
function savedDecision(data,index){
 const label=data.segments[index].speaker;let segment=null,whole=null;
 for(const entry of data.decisions){const d=entry.decision;if(d.label!==label)continue;
  if(d.scope==='label')whole=d;else if(d.scope==='segment'&&d.segment_index===index)segment=d;
 }return segment||whole;
}
function speakerDisplay(data,index){
 const label=data.segments[index].speaker,d=savedDecision(data,index);
 if(d){const name=d.source==='participant'?d.name.trim():'';const category={participant:'Participant (unnamed)',playback:'Playback / game audio / background noise',tts:'Text to speech',uncertain:'Uncertain'}[d.source];return `${name||category} · ${label}`;}
 const known=(data.confirmed.confirmed_mappings||[]).find(m=>m.label===label);
 return known?`${known.name} · ${label}`:label;
}
function setAudioSource(value){
 $('source').value=value;
 for(const b of $('sourceButtons').querySelectorAll('button'))b.setAttribute('aria-pressed',String(b.dataset.source===value));
 $('name').disabled=value!=='participant';
 if($('name').disabled)$('name').value='';
}
function setReviewScope(value){
 $('scope').value=value;
 for(const b of $('scopeButtons').querySelectorAll('button'))b.setAttribute('aria-pressed',String(b.dataset.scope===value));
}
function autoSaveReady(data){return data.source!=='participant'||Boolean(data.name.trim());}
function speakerTurn(segments,label,selectedIndex,direction,timeMs){
 const matches=segments.map((s,i)=>({s,i})).filter(({s,i})=>s.speaker===label&&(selectedIndex>=0?(direction>0?i>selectedIndex:i<selectedIndex):(direction>0?s.start_ms>=timeMs:s.start_ms<timeMs)));
 return matches.length?(direction>0?matches[0].i:matches[matches.length-1].i):-1;
}
const $=id=>document.getElementById(id);
let token='',record=null,selected=-1,visible=[],pendingSeek=null,loadVersion=0;
let saveChain=Promise.resolve(),queuedSaves=0;
let recordChoices=[],choicesVersion=0;
const time=ms=>{const s=Math.floor(ms/1000);return `${Math.floor(s/3600)}:${String(Math.floor(s/60)%60).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`;};
async function api(path,options){const r=await fetch(path,options);if(!r.ok)throw Error(`Request failed (${r.status})`);return r.json();}
function renderRecordChoices(){
 const current=$('record').value||record?.id,only=$('incompleteOnly').checked;
 const shown=recordChoices.filter(r=>!only||!r.labeling_complete);
 $('record').replaceChildren();
 for(const r of shown)$('record').add(new Option(`${r.title} (${r.labeling_complete?'complete':`${r.unresolved_labels} labels incomplete`})`,r.id));
 // Keep the currently playing video stable when its last label is saved.
 const active=recordChoices.find(r=>r.id===current);
 if(active&&!shown.some(r=>r.id===current)){const option=new Option(`${active.title} (complete — currently open)`,active.id);option.disabled=true;$('record').add(option);}
 if(active)$('record').value=current;
 if(!$('record').options.length)$('record').add(new Option('No incomplete recordings',''));
 $('recordCount').textContent=`${recordChoices.filter(r=>!r.labeling_complete).length} of ${recordChoices.length} recordings incomplete.${only&&active?.labeling_complete?' Current completed recording stays open until you choose another.':''}`;
}
async function refreshRecordChoices(){const version=++choicesVersion;const data=await api('/api/records');if(version!==choicesVersion)return;token=data.token;recordChoices=data.records;recordChoices.sort((a,b)=>{const priority=x=>x.id==='cloudjob_be1be1649cdca10612dba1b391f29299'?-2:x.title.toLowerCase().includes('stalking ice poseidon')?-1:0;return priority(a)-priority(b)||a.title.localeCompare(b.title);});renderRecordChoices();}
$('incompleteOnly').onchange=()=>refreshRecordChoices().catch(showError);
async function refreshNames(){const data=await api('/api/names');$('nameOptions').replaceChildren();$('nameSuggestions').replaceChildren();for(const item of data.names){$('nameOptions').append(new Option(item.name,item.name));const b=document.createElement('button');b.type='button';b.textContent=`${item.name} (${item.uses})`;b.title=`${item.uses} current manual assignments across recordings`;b.disabled=selected<0;b.onclick=()=>{setAudioSource('participant');$('name').value=item.name;$('name').focus();edited();};$('nameSuggestions').append(b);}}
function seek(seconds){const v=$('video');if(v.readyState<1){pendingSeek=seconds;return;}v.currentTime=Math.max(0,Math.min(seconds,Number.isFinite(v.duration)?v.duration:seconds));v.play().catch(()=>{});}
$('video').addEventListener('loadedmetadata',()=>{if(pendingSeek!==null){const t=pendingSeek;pendingSeek=null;seek(t);}});
$('video').addEventListener('error',()=>{$('mediaError').textContent='This browser could not play the original media. No video conversion was performed.';});
function fillEditor(i){const s=record.segments[i],d=savedDecision(record,i);$('selected').textContent=`${speakerDisplay(record,i)} · ${time(s.start_ms)}–${time(s.end_ms)}`;setReviewScope(d?.scope||'label');$('name').value=d?.name||'';setAudioSource(d?.source||'participant');$('notes').value=d?.notes||'';$('save').disabled=false;
 const match=(record.confirmed.confirmed_mappings||[]).find(m=>m.label===s.speaker);$('known').textContent=match?`Previously confirmed label mapping: ${match.name}`:'No confirmed name for this label.';
 for(const b of $('nameSuggestions').children)b.disabled=false;
}
function choose(i){selected=i;const s=record.segments[i];fillEditor(i);$('jumpSpeaker').value=s.speaker;updateSpeakerNavigation();
 for(const el of $('turns').children)el.classList.toggle('active',Number(el.dataset.index)===i);
 seek(s.start_ms/1000);}
function render(){const label=$('label').value,q=$('search').value.toLowerCase(),scroll=$('turns').scrollTop;visible=[];$('turns').replaceChildren();const frag=document.createDocumentFragment();
 const labels=new Map();record.segments.forEach((s,i)=>{if(!s.speaker)return;if(!labels.has(s.speaker))labels.set(s.speaker,new Set());labels.get(s.speaker).add(speakerDisplay(record,i));});const priorJump=$('jumpSpeaker').value;$('jumpSpeaker').replaceChildren();$('label').replaceChildren(new Option('All labels',''));for(const [id,names] of labels){const display=names.size===1?[...names][0]:`${id} · multiple reviewed names/sources`;$('label').add(new Option(display,id));$('jumpSpeaker').add(new Option(display,id));}$('label').value=label;if(labels.has(priorJump))$('jumpSpeaker').value=priorJump;updateSpeakerNavigation();
 record.segments.forEach((s,i)=>{const display=speakerDisplay(record,i);if(label&&s.speaker!==label||q&&!`${s.text} ${display}`.toLowerCase().includes(q)||$('flagged').checked&&!(record.flags[i]||[]).length)return;visible.push(i);const row=document.createElement('article');row.className=i===selected?'turn active':'turn';row.dataset.index=i;
 const b=document.createElement('button');b.textContent=time(s.start_ms);b.title='Seek video to this turn';b.onclick=()=>choose(i);row.append(b);
 const badge=document.createElement('span');badge.className='badge';badge.textContent=display;row.append(badge);const text=document.createElement('p');text.textContent=s.text;row.append(text);
 const flags=document.createElement('div');flags.className='flags';flags.textContent=(record.flags[i]||[]).join(' · ');row.append(flags);frag.append(row);});$('turns').append(frag);$('turns').scrollTop=scroll;$('count').textContent=`${visible.length} turns`;}
async function load(){const version=++loadVersion;selected=-1;for(const b of $('nameSuggestions').children)b.disabled=true;$('save').disabled=true;$('status').textContent='Loading…';$('selected').textContent='Click a transcript timestamp to seek.';$('known').textContent='';pendingSeek=null;const data=await api('/api/record/'+$('record').value);if(version!==loadVersion)return;record=data;$('video').src=record.media_url;$('mediaError').textContent='';$('label').replaceChildren(new Option('All labels',''));for(const l of new Set(record.segments.map(s=>s.speaker).filter(Boolean)))$('label').add(new Option(l,l));$('search').value='';render();$('status').textContent=`${record.decisions.length} saved decisions for this recording.`;refreshNames().catch(showError);}
$('record').onchange=()=>{renderRecordChoices();if($('record').value)load().catch(showError);};['label','search','flagged'].forEach(id=>$(id).addEventListener('input',()=>record&&render()));
function advance(delta){if(!visible.length)return;let pos=visible.indexOf(selected);pos=pos<0?0:Math.max(0,Math.min(visible.length-1,pos+delta));choose(visible[pos]);document.querySelector(`[data-index="${visible[pos]}"]`)?.scrollIntoView({block:'nearest'});}
function speakerTarget(direction){return record?speakerTurn(record.segments,$('jumpSpeaker').value,selected,direction,$('video').currentTime*1000):-1;}
function updateSpeakerNavigation(){$('speakerPrev').disabled=speakerTarget(-1)<0;$('speakerNext').disabled=speakerTarget(1)<0;}
function jumpSpeakerTurn(direction){const index=speakerTarget(direction);if(index<0)return;choose(index);if(!visible.includes(index)){$('label').value=record.segments[index].speaker;$('search').value='';$('flagged').checked=false;render();}document.querySelector(`[data-index="${index}"]`)?.scrollIntoView({block:'nearest'});}
$('jumpSpeaker').onchange=updateSpeakerNavigation;$('speakerPrev').onclick=()=>jumpSpeakerTurn(-1);$('speakerNext').onclick=()=>jumpSpeakerTurn(1);
$('label').addEventListener('change',()=>{if($('label').value)$('jumpSpeaker').value=$('label').value;updateSpeakerNavigation();});
$('prev').onclick=()=>advance(-1);$('next').onclick=()=>advance(1);$('back').onclick=()=>seek($('video').currentTime-5);$('forward').onclick=()=>seek($('video').currentTime+5);$('speed').onchange=()=>{$('video').playbackRate=Number($('speed').value);};
$('go').onclick=()=>{const value=$('jump').value.trim();if(!/^\d+(?::\d{1,2}){0,2}(?:\.\d+)?$/.test(value))return showError(Error('Use seconds or hh:mm:ss'));seek(value.split(':').reduce((a,b)=>a*60+Number(b),0));};
for(const button of $('sourceButtons').querySelectorAll('button'))button.onclick=()=>{setAudioSource(button.dataset.source);edited();};
for(const button of $('scopeButtons').querySelectorAll('button'))button.onclick=()=>{setReviewScope(button.dataset.scope);edited();};
$('name').addEventListener('change',edited);$('notes').addEventListener('change',edited);
$('autoSave').onchange=()=>{$('status').textContent=$('autoSave').checked?'Auto-save on. Future edits will be saved.':'Auto-save off. Use Save review.';};
function showError(e){$('status').textContent=e.message;}
function editorDecision(){if(selected<0||!record)return null;return {job_id:record.id,scope:$('scope').value,segment_index:selected,label:record.segments[selected].speaker,name:$('name').value.trim(),source:$('source').value,notes:$('notes').value};}
function edited(){if(!$('autoSave').checked)return;const data=editorDecision();if(!data)return;if(!autoSaveReady(data)){$('status').textContent='Choose or enter a name to auto-save this participant.';return;}queueSave(data);}
function queueSave(data){
 // Capture the decision now; navigation must never change its target while saving.
 queuedSaves++;$('status').textContent='Saving…';
 saveChain=saveChain.then(async()=>{
  try{await api('/api/decision',{method:'POST',headers:{'Content-Type':'application/json','X-Review-Token':token},body:JSON.stringify(data)});
   const updated=await api('/api/record/'+data.job_id);
   if(record?.id===data.job_id){record.decisions=updated.decisions;render();if(selected>=0){const s=record.segments[selected];$('selected').textContent=`${speakerDisplay(record,selected)} · ${time(s.start_ms)}–${time(s.end_ms)}`;}}
   $('status').textContent=`Saved ${data.name||data.source} for ${data.label}${queuedSaves>1?' · more saves queued':''}.`;await refreshNames();await refreshRecordChoices();
  }catch(e){showError(Error(`Save failed for ${data.label}: ${e.message}. Select that turn and save again.`));}
  finally{queuedSaves--;}
 });return saveChain;
}
$('review').onsubmit=e=>{e.preventDefault();const data=editorDecision();if(data)queueSave(data);};
$('download').onclick=()=>{if(!record)return;const blob=new Blob([JSON.stringify(record.decisions,null,2)],{type:'application/json'});const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=record.id+'-reviews.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
refreshRecordChoices().then(()=>{
 const requested=new URLSearchParams(window.location.search).get('record');
 if(requested&&recordChoices.some(r=>r.id===requested)){
  if(![...$('record').options].some(o=>o.value===requested)){
   $('incompleteOnly').checked=false;renderRecordChoices();
  }
  $('record').value=requested;
 }
 if($('record').value)return load();
}).catch(showError);
