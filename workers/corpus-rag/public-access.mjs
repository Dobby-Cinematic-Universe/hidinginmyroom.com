// Public access is deliberately separate from the authenticated private pilot.
export const reply=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer',...(status===429?{'Retry-After':'60'}:{})}});
export function publicReady(env,sources){
  try{return env.PUBLIC_RELEASE_APPROVED==='true' && env.INSTANCE!== 'himr-private-pilot-v1'
    && /^himr-public-/.test(env.INSTANCE) && env.CORPUS_VERSION===sources.release
    && Object.keys(sources.documents).length>0 && new URL(env.ALLOWED_ORIGIN).origin===env.ALLOWED_ORIGIN
    && new URL(env.ALLOWED_ORIGIN).protocol==='https:' && !!env.TURNSTILE_SECRET && env.CLIENT_HASH_SECRET?.length>=32;}catch{return false;}
}
export function cors(response,origin){
  const headers=new Headers(response.headers);headers.set('Access-Control-Allow-Origin',origin);headers.set('Vary','Origin');
  headers.set('Access-Control-Allow-Methods','POST, OPTIONS');headers.set('Access-Control-Allow-Headers','Content-Type');
  return new Response(response.body,{status:response.status,headers});
}
export async function clientKey(ip,secret){
  const day=new Date().toISOString().slice(0,10);
  const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  const bytes=await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(`${day}|${ip}`));
  return [...new Uint8Array(bytes)].map(b=>b.toString(16).padStart(2,'0')).join('');
}
export async function publicAdmission(request,body,env,fetcher=fetch){
  const ip=request.headers.get('CF-Connecting-IP');
  if(!ip||typeof body.turnstileToken!=='string'||!body.turnstileToken||body.turnstileToken.length>2048)return reply({error:'Verification required.'},403);
  // Count attempts before Siteverify; invalid/replayed tokens cannot cause AI work.
  for(const name of [await clientKey(ip,env.CLIENT_HASH_SECRET)]){
    const r=await env.ADMISSION.get(env.ADMISSION.idFromName(name)).fetch('https://internal/admit',{method:'POST',body:'{}'});
    if(!r.ok)return reply({error:'Request allowance reached. Please use ordinary corpus search.'},429);
  }
  try{
    const r=await fetcher('https://challenges.cloudflare.com/turnstile/v0/siteverify',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({secret:env.TURNSTILE_SECRET,response:body.turnstileToken,remoteip:ip}),signal:AbortSignal.timeout(8000)});
    const v=await r.json();
    if(!r.ok||v.success!==true||v.hostname!==new URL(env.ALLOWED_ORIGIN).hostname||v.action!=='corpus-query')return reply({error:'Verification expired or failed. Please try again.'},403);
    return null;
  }catch{return reply({error:'Verification unavailable. Please use ordinary corpus search.'},503);}
}
export class Admission {
  constructor(state){this.state=state;this.busy=false;}
  async fetch(request){
    if(this.busy)return reply({error:'Busy'},429);this.busy=true;
    try{
      const now=Date.now(),day=new Date(now).toISOString().slice(0,10);
      let usage=await this.state.storage.get('usage');
      if(!usage||usage.day!==day)usage={day,count:0,hour:Math.floor(now/3600000),hourCount:0,last:0};
      if(usage.hour!==Math.floor(now/3600000)){usage.hour=Math.floor(now/3600000);usage.hourCount=0;}
      if(usage.count>=50||usage.hourCount>=20||now-usage.last<3000)return reply({error:'Rate limited'},429);
      usage.count++;usage.hourCount++;usage.last=now;await this.state.storage.put('usage',usage);
      // Fixed cleanup deadline, not extended with each request.
      if(!await this.state.storage.getAlarm())await this.state.storage.setAlarm(now+172800000);
      return reply({allowed:true});
    }finally{this.busy=false;}
  }
  async alarm(){await this.state.storage.deleteAll();}
}
