// Pure, deterministic presentation of admitted aggregate co-mentions. No identity inference.
export const mentionNetworkDefaults=Object.freeze({group:'all',minSupport:5,minLift:1.5,maxEdges:40,focus:''});
const compare=(a,b)=>a<b?-1:a>b?1:0;
const pair=e=>[e.a,e.b].sort(compare).join('|');

export function selectMentionNetwork(data,options={}){
  const o={...mentionNetworkDefaults,...options};
  if(!['all','on_channel','public_creator'].includes(o.group)||!Number.isFinite(o.minSupport)||o.minSupport<0||!Number.isFinite(o.minLift)||o.minLift<0||!Number.isInteger(o.maxEdges)||o.maxEdges<1||o.maxEdges>1000)throw new Error('Invalid mention-map controls');
  const registry=new Map(data.names.map(n=>[n.name,n.group]));
  const requested=String(o.focus||'').trim();
  const focus=requested?[...registry.keys()].find(n=>n.toLowerCase()===requested.toLowerCase()):'';
  if(requested&&!focus)return {nodes:[],edges:[],eligibleEdges:0,hiddenEdges:0,focus:'',reason:'unknown_focus'};
  const allowed=new Set(data.names.filter(n=>o.group==='all'||n.group===o.group).map(n=>n.name));
  if(focus&&!allowed.has(focus))return {nodes:[],edges:[],eligibleEdges:0,hiddenEdges:0,focus,reason:'excluded_focus'};
  const eligible=data.edges.filter(e=>Number.isFinite(e.weight)&&Number.isFinite(e.lift)&&e.a!==e.b&&allowed.has(e.a)&&allowed.has(e.b)&&e.weight>=o.minSupport&&e.weight>0&&e.lift>=o.minLift&&(!focus||e.a===focus||e.b===focus)).sort((a,b)=>b.weight-a.weight||compare(pair(a),pair(b)));
  const edges=eligible.slice(0,o.maxEdges);
  const degrees=new Map();
  for(const e of edges)for(const name of [e.a,e.b]){const old=degrees.get(name)||{degree:0,links:0};degrees.set(name,{degree:old.degree+e.weight,links:old.links+1});}
  const nodes=[...degrees].map(([name,support])=>({name,group:registry.get(name),...support})).sort((a,b)=>b.degree-a.degree||compare(a.name,b.name));
  return {nodes,edges,eligibleEdges:eligible.length,hiddenEdges:eligible.length-edges.length,focus:focus||'',reason:edges.length?'ready':'no_edges'};
}

/** Connected components describe displayed co-mentions, not communities or relationships. */
export function layoutMentionNetwork(selection,{width=900,height=selection.focus&&selection.nodes.length<=12?420:620}={}){
  if(!Number.isFinite(width)||!Number.isFinite(height)||width<300||height<200)throw new Error('Invalid mention-map dimensions');
  const {nodes,edges,focus}=selection;
  if(!nodes.length)return {nodes:[],components:[],width,height};
  const byName=new Map(nodes.map(n=>[n.name,n]));
  const adjacent=new Map(nodes.map(n=>[n.name,[]]));
  for(const e of edges){adjacent.get(e.a).push({name:e.b,weight:e.weight});adjacent.get(e.b).push({name:e.a,weight:e.weight});}
  for(const links of adjacent.values())links.sort((a,b)=>b.weight-a.weight||compare(a.name,b.name));
  const seen=new Set(),components=[];
  for(const seed of nodes){if(seen.has(seed.name))continue;const names=[],queue=[seed.name];seen.add(seed.name);for(let i=0;i<queue.length;i++){const name=queue[i];names.push(name);for(const next of adjacent.get(name))if(!seen.has(next.name)){seen.add(next.name);queue.push(next.name);}}components.push({names,weight:names.reduce((sum,n)=>sum+byName.get(n).degree,0),size:Math.pow(names.length,.8),x:0,y:0,width:0,height:0});}
  components.sort((a,b)=>b.weight-a.weight||compare(a.names[0],b.names[0]));
  const pack=(items,x,y,w,h)=>{
    if(items.length===1){Object.assign(items[0],{x,y,width:w,height:h});return;}
    const total=items.reduce((s,c)=>s+c.size,0);let split=1,weight=items[0].size;while(split<items.length-1&&weight+items[split].size<=total/2){weight+=items[split].size;split++;}
    const fraction=weight/total,gap=18;
    if(w>=h){const available=w-gap;pack(items.slice(0,split),x,y,available*fraction,h);pack(items.slice(split),x+available*fraction+gap,y,available*(1-fraction),h);}
    else{const available=h-gap;pack(items.slice(0,split),x,y,w,available*fraction);pack(items.slice(split),x,y+available*fraction+gap,w,available*(1-fraction));}
  };
  pack(components,16,16,width-32,height-32);
  const positioned=[];
  for(let c=0;c<components.length;c++){
    const component=components[c],names=component.names;
    const paddingX=Math.min(110,component.width*.25),paddingY=Math.min(50,component.height*.2);
    const innerW=Math.max(10,component.width-paddingX*2),innerH=Math.max(10,component.height-paddingY*2);
    const points=names.map((name,i)=>{
      const angle=(i+1)*2.399963229728653,rad=Math.sqrt((i+.5)/names.length);
      return {name,x:Math.cos(angle)*rad,y:Math.sin(angle)*rad,vx:0,vy:0};
    });
    const pointsByName=new Map(points.map(p=>[p.name,p]));
    const localEdges=edges.filter(e=>pointsByName.has(e.a)&&pointsByName.has(e.b));
    const maximum=Math.max(1,...localEdges.map(e=>e.weight));
    // Fixed iterations + connection-ranked initial seeds make the geometry repeatable.
    for(let iteration=0;iteration<260;iteration++){
      const cooling=1-iteration/260;
      for(let i=0;i<points.length;i++)for(let j=i+1;j<points.length;j++){
        const a=points[i],b=points[j],dx=a.x-b.x,dy=a.y-b.y,d2=dx*dx+dy*dy+.025;
        const push=.004*cooling/d2;a.vx+=dx*push;a.vy+=dy*push;b.vx-=dx*push;b.vy-=dy*push;
      }
      for(const e of localEdges){const a=pointsByName.get(e.a),b=pointsByName.get(e.b),dx=b.x-a.x,dy=b.y-a.y,distance=Math.hypot(dx,dy)+.0001;const pull=(distance-.35)*.02*cooling*(.25+Math.sqrt(e.weight/maximum));a.vx+=dx/distance*pull;a.vy+=dy/distance*pull;b.vx-=dx/distance*pull;b.vy-=dy/distance*pull;}
      for(const p of points){p.vx-=p.x*.002;p.vy-=p.y*.002;p.x+=p.vx;p.y+=p.vy;p.vx*=.65;p.vy*=.65;}
    }
    const minX=Math.min(...points.map(p=>p.x)),maxX=Math.max(...points.map(p=>p.x)),minY=Math.min(...points.map(p=>p.y)),maxY=Math.max(...points.map(p=>p.y));
    const scale=Math.min(innerW/Math.max(.01,maxX-minX),innerH/Math.max(.01,maxY-minY));
    const midX=(minX+maxX)/2,midY=(minY+maxY)/2;
    for(const p of points)positioned.push({...byName.get(p.name),x:component.x+component.width/2+(p.x-midX)*scale,y:component.y+component.height/2+(p.y-midY)*scale,component:c,focused:p.name===focus});
  }
  // Labels get deterministic lanes to reduce overlaps without moving the evidence nodes.
  const placed=[];
  const maximumDegree=Math.max(1,...nodes.map(n=>n.degree));
  for(const node of [...positioned].sort((a,b)=>b.degree-a.degree||compare(a.name,b.name))){
    const component=components[node.component],labelWidth=Math.min(170,Math.max(35,node.name.length*6.7));
    const right=node.x<=component.x+component.width/2;
    const radius=Math.max(5,20*Math.sqrt(node.degree/maximumDegree)),labelGap=radius+8;
    let lx=Math.max(6,Math.min(width-labelWidth-6,node.x+(right?labelGap:-labelWidth-labelGap))),ly=node.y-7;
    for(let attempt=0;attempt<32;attempt++){
      const y=node.y-7+(attempt===0?0:Math.ceil(attempt/2)*17*(attempt%2?1:-1));
      if(y<8||y>height-22)continue;
      if(!placed.some(p=>lx<p.x+p.width+5&&lx+labelWidth+5>p.x&&y<p.y+16&&y+16>p.y)){ly=y;break;}
    }
    node.labelX=lx;node.labelY=ly+12;placed.push({x:lx,y:ly,width:labelWidth});
  }
  return {nodes:positioned.sort((a,b)=>compare(a.name,b.name)),components:components.map(c=>({x:c.x,y:c.y,width:c.width,height:c.height,names:c.names})),width,height};
}
