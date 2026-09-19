export {};
const workspace = document.querySelector<HTMLElement>('.recording__workspace');
const wideButton = document.querySelector<HTMLButtonElement>('[data-wide-player]');
if(workspace && wideButton){
  wideButton.hidden=false;
  function setWide(wide:boolean){
    workspace!.classList.toggle('is-wide',wide);
    wideButton!.setAttribute('aria-pressed',String(wide));
    wideButton!.textContent=wide?'Side-by-side view':'Wide player';
  }
  try{setWide(localStorage.getItem('corpus-wide-player')==='true');}catch{/* Storage may be disabled. */}
  wideButton.addEventListener('click',()=>{const wide=!workspace.classList.contains('is-wide');setWide(wide);try{localStorage.setItem('corpus-wide-player',String(wide));}catch{}});
}
const player = document.querySelector<HTMLElement>('[data-recording-player]');
const source = player?.querySelector<HTMLSelectElement>('[data-video-source]');
const video = player?.querySelector<HTMLVideoElement>('[data-video]');
const frame = player?.querySelector<HTMLIFrameElement>('[data-youtube]');
const youtubeButton = player?.querySelector<HTMLButtonElement>('[data-load-youtube]');
const status = player?.querySelector<HTMLElement>('[data-player-status]');
let pendingTime: number | null = null;
const sections = [...document.querySelectorAll<HTMLElement>('.transcript')].map(section => {
  const list = section.querySelector<HTMLOListElement>('.transcript__segments');
  const rows = list ? [...list.querySelectorAll<HTMLElement>(':scope > li[data-start-ms]')] : [];
  return {section,list,rows,active:null as HTMLElement|null};
});
const kind = () => source?.selectedOptions[0]?.dataset.kind;
function reveal(list: HTMLElement, row: HTMLElement) {
  const outer=list.getBoundingClientRect(), inner=row.getBoundingClientRect();
  if(inner.top<outer.top || inner.bottom>outer.bottom) list.scrollTop+=inner.top-outer.top-list.clientHeight/3;
}
function loadYoutube(seconds=0) {
  if(!source || !frame || !youtubeButton) return;
  const url=new URL(source.value); url.searchParams.set('start',String(Math.floor(seconds)));
  frame.src=url.href;frame.hidden=false;youtubeButton.hidden=true;
}
function seek(seconds:number) {
  if(!source || !video || !Number.isFinite(seconds) || seconds<0) return;
  if(kind()==='youtube') { loadYoutube(seconds); if(status)status.textContent='Player opened at the selected timestamp. Press play to continue.'; return; }
  pendingTime=seconds;
  if(video.readyState>=1) {
    video.currentTime=Number.isFinite(video.duration)?Math.min(seconds,Math.max(0,video.duration-.1)):seconds;
    pendingTime=null;
  } else { video.preload='metadata';video.load(); }
  if(status)status.textContent='Timestamp selected. Press play to watch; playback does not start automatically.';
}
function chooseSource() {
  if(!source || !video || !frame || !youtubeButton)return;
  video.pause();video.removeAttribute('src');frame.removeAttribute('src');frame.hidden=true;pendingTime=null;
  video.hidden=kind()!=='video';youtubeButton.hidden=kind()!=='youtube';
  if(kind()==='video'){ video.preload='none';video.src=source.value; } else video.load();
  for(const {section} of sections){const follow=section.querySelector<HTMLInputElement>('[data-follow-playback]');if(follow){follow.disabled=kind()!=='video';if(follow.disabled)follow.checked=false;}}
  if(status)status.textContent='Click a transcript timestamp to seek.';
}
source?.addEventListener('change',chooseSource);chooseSource();
youtubeButton?.addEventListener('click',()=>loadYoutube());
video?.addEventListener('loadedmetadata',()=>{if(pendingTime!==null)seek(pendingTime);});
video?.addEventListener('error',()=>{if(status)status.textContent='This source cannot be played here. Try another source, or use “Open original source” below.';});
video?.addEventListener('timeupdate',()=>{
  const now=video.currentTime*1000;
  for(const data of sections){
    let lo=0,hi=data.rows.length;
    while(lo<hi){const mid=(lo+hi)>>>1;if(Number(data.rows[mid].dataset.startMs)<=now)lo=mid+1;else hi=mid;}
    const candidate=data.rows[lo-1];const active=candidate&&now<Number(candidate.dataset.endMs)?candidate:null;
    if(active===data.active)continue;
    data.active?.classList.remove('is-playing');data.active?.removeAttribute('aria-current');
    data.active=active;
    if(active){active.classList.add('is-playing');active.setAttribute('aria-current','true');
      if(data.list&&!active.hidden&&data.section.querySelector<HTMLInputElement>('[data-follow-playback]')?.checked)reveal(data.list,active);
    }
  }
});
for(const {section,list,rows} of sections){
  if(!list)continue;
  const tools=section.querySelector<HTMLElement>('[data-transcript-tools]');if(tools)tools.hidden=false;
  const search=section.querySelector<HTMLInputElement>('[data-transcript-search]');
  const count=section.querySelector<HTMLElement>('[data-transcript-count]');
  const text=rows.map(row=>[row.querySelector('p')?.textContent,row.querySelector('.transcript__speaker')?.textContent].join(' ').toLocaleLowerCase());
  let timer: ReturnType<typeof setTimeout>;
  function filterTranscript(){
    if(!search)return;
    const query=search.value.trim().toLocaleLowerCase();let matches=0;
    rows.forEach((row,i)=>{row.hidden=Boolean(query&&!text[i].includes(query));if(!row.hidden)matches++;});
    if(count)count.textContent=matches?`${matches.toLocaleString()} of ${rows.length.toLocaleString()} ${rows.length===1?'turn':'turns'}`:'No exact phrase matches. Try fewer words or clear the search to see all turns.';
  }
  search?.addEventListener('input',()=>{clearTimeout(timer);timer=setTimeout(filterTranscript,120);});
  const incoming=new URLSearchParams(location.search).get('find');
  if(search&&incoming){search.value=incoming.replace(/^"|"$/g,'');filterTranscript();}
  section.querySelector<HTMLInputElement>('[data-full-transcript]')?.addEventListener('change',event=>{
    list.classList.toggle('is-full-height',(event.target as HTMLInputElement).checked);
  });
  list.addEventListener('click',event=>{
    const link=(event.target as Element).closest<HTMLAnchorElement>('.transcript__timestamp');if(!link)return;
    const row=link.closest<HTMLElement>('li[data-start-ms]');if(!row)return;
    event.preventDefault();history.replaceState(null,'',link.hash);reveal(list,row);seek(Number(row.dataset.startMs)/1000);
  });
}
// Hash links remain useful without JavaScript; with it they also cue the player.
function visitHash(){
  let id;try{id=decodeURIComponent(location.hash.slice(1));}catch{return;}
  const row=document.getElementById(id);
  if(row?.dataset.startMs){const list=row.closest<HTMLElement>('.transcript__segments');if(list){row.hidden=false;reveal(list,row);}seek(Number(row.dataset.startMs)/1000);}
}
window.addEventListener('hashchange',visitHash);if(location.hash)visitHash();
