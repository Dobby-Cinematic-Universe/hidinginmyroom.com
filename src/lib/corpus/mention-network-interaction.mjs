// Local-only map geometry. These controls never modify aggregate data or filters.
const clamp=n=>Math.max(.4,Math.min(5,n));
export const initialMentionView=()=>({scale:1,x:0,y:0});
export function zoomMentionView(view,factor,anchor){
  if(![view.scale,view.x,view.y,factor,anchor.x,anchor.y].every(Number.isFinite)||view.scale<=0||factor<=0)throw new Error('Invalid mention-map zoom');
  const scale=clamp(view.scale*factor),ratio=scale/view.scale;
  return {scale,x:anchor.x-(anchor.x-view.x)*ratio,y:anchor.y-(anchor.y-view.y)*ratio};
}
export function panMentionView(view,dx,dy){
  if(![view.scale,view.x,view.y,dx,dy].every(Number.isFinite))throw new Error('Invalid mention-map pan');
  return {...view,x:view.x+dx,y:view.y+dy};
}
export function fitMentionView(bounds,width,height,padding=24){
  if(![width,height,padding].every(Number.isFinite)||width<=0||height<=0||padding<0)throw new Error('Invalid mention-map viewport');
  if(!bounds)return initialMentionView();
  if(![bounds.left,bounds.right,bounds.top,bounds.bottom].every(Number.isFinite)||bounds.right<bounds.left||bounds.bottom<bounds.top)throw new Error('Invalid mention-map bounds');
  const scale=clamp(Math.min(Math.max(1,width-2*padding)/Math.max(1,bounds.right-bounds.left),Math.max(1,height-2*padding)/Math.max(1,bounds.bottom-bounds.top)));
  return {scale,x:width/2-(bounds.left+bounds.right)/2*scale,y:height/2-(bounds.top+bounds.bottom)/2*scale};
}

/** Pointer capture stays on this SVG; a destroy hook removes the shared keyboard listener. */
export function attachMentionMapInteraction(svg,layer,{width,height,nodes,edges,onActivate,onViewChange}){
  let view=initialMentionView(),gesture=null,suppressClick=false;
  const pointers=new Map(),offsets=new Map(nodes.map(n=>[n.name,{x:0,y:0}]));
  const byName=new Map(nodes.map(n=>[n.name,n]));
  const host=svg.parentElement;
  const point=event=>{
    const matrix=svg.getScreenCTM?.();
    if(matrix&&typeof DOMPoint!=='undefined'){const p=new DOMPoint(event.clientX,event.clientY).matrixTransform(matrix.inverse());return {x:p.x,y:p.y};}
    const r=svg.getBoundingClientRect();return {x:(event.clientX-r.left)*width/Math.max(1,r.width),y:(event.clientY-r.top)*height/Math.max(1,r.height)};
  };
  const apply=()=>{layer.setAttribute('transform',`translate(${view.x} ${view.y}) scale(${view.scale})`);onViewChange?.(view);};
  const nodeFrom=target=>target?.closest?.('[data-node-name]')?.getAttribute('data-node-name');
  const redrawNode=name=>{
    const n=byName.get(name),o=offsets.get(name);n.element.setAttribute('transform',`translate(${o.x} ${o.y})`);
    for(const e of edges){const a=byName.get(e.a),b=byName.get(e.b),ao=offsets.get(e.a),bo=offsets.get(e.b);e.element.setAttribute('x1',a.x+ao.x);e.element.setAttribute('y1',a.y+ao.y);e.element.setAttribute('x2',b.x+bo.x);e.element.setAttribute('y2',b.y+bo.y);}
  };
  const beginPinch=()=>{
    const [a,b]=[...pointers.values()];gesture={kind:'pinch',view:{...view},distance:Math.max(1,Math.hypot(a.point.x-b.point.x,a.point.y-b.point.y)),center:{x:(a.point.x+b.point.x)/2,y:(a.point.y+b.point.y)/2}};suppressClick=true;
  };
  const down=event=>{
    if(event.button!==undefined&&event.button!==0)return;
    if(pointers.size>=2&&!pointers.has(event.pointerId))return;
    const p=point(event);pointers.set(event.pointerId,{point:p,startClient:{x:event.clientX,y:event.clientY}});
    if(pointers.size===1){suppressClick=false;const name=nodeFrom(event.target);gesture=name&&byName.has(name)?{kind:'node',name,start:p,offset:{...offsets.get(name)},scale:view.scale}:{kind:'pan',start:p,view:{...view}};}
    else if(pointers.size===2)beginPinch();
    try{svg.setPointerCapture(event.pointerId);}catch{/* Capture may be unavailable for synthetic events. */}
  };
  const move=event=>{
    const p=pointers.get(event.pointerId);if(!p||!gesture)return;p.point=point(event);
    if(Math.hypot(event.clientX-p.startClient.x,event.clientY-p.startClient.y)>5)suppressClick=true;
    if(gesture.kind==='pinch'&&pointers.size>=2){const [a,b]=[...pointers.values()];const center={x:(a.point.x+b.point.x)/2,y:(a.point.y+b.point.y)/2};view=zoomMentionView(gesture.view,Math.max(.001,Math.hypot(a.point.x-b.point.x,a.point.y-b.point.y))/gesture.distance,gesture.center);view=panMentionView(view,center.x-gesture.center.x,center.y-gesture.center.y);apply();}
    else if(gesture.kind==='node'){offsets.set(gesture.name,{x:gesture.offset.x+(p.point.x-gesture.start.x)/gesture.scale,y:gesture.offset.y+(p.point.y-gesture.start.y)/gesture.scale});redrawNode(gesture.name);}
    else if(gesture.kind==='pan'){view=panMentionView(gesture.view,p.point.x-gesture.start.x,p.point.y-gesture.start.y);apply();}
    event.preventDefault();
  };
  const up=event=>{
    const activate=event.type!=='pointercancel'&&gesture?.kind==='node'&&!suppressClick&&pointers.size===1?gesture.name:null;
    pointers.delete(event.pointerId);try{svg.releasePointerCapture(event.pointerId);}catch{}
    if(!pointers.size)gesture=null;
    else if(pointers.size===1){const p=[...pointers.values()][0];gesture={kind:'pan',start:{...p.point},view:{...view}};}
    // Pointer capture may retarget the synthetic click to the SVG instead of the node.
    if(activate){suppressClick=true;onActivate?.(activate,false);}
  };
  const click=event=>{if(suppressClick){suppressClick=false;event.preventDefault();return;}const name=nodeFrom(event.target);if(name&&byName.has(name))onActivate?.(name,false);};
  const wheel=event=>{event.preventDefault();const delta=event.deltaY*(event.deltaMode===1?16:event.deltaMode===2?height:1);view=zoomMentionView(view,Math.exp(-Math.max(-1000,Math.min(1000,delta))*.002),point(event));apply();};
  const bounds=()=>{
    if(!nodes.length)return null;const boxes=nodes.map(n=>{const o=offsets.get(n.name);return {left:Math.min(n.x-24,n.labelX)+o.x,right:Math.max(n.x+24,n.labelX+n.name.length*6.7)+o.x,top:Math.min(n.y-24,n.labelY-15)+o.y,bottom:Math.max(n.y+24,n.labelY+5)+o.y};});
    return {left:Math.min(...boxes.map(b=>b.left)),right:Math.max(...boxes.map(b=>b.right)),top:Math.min(...boxes.map(b=>b.top)),bottom:Math.max(...boxes.map(b=>b.bottom))};
  };
  const api={
    zoom:factor=>{view=zoomMentionView(view,factor,{x:width/2,y:height/2});apply();},
    fit:()=>{view=fitMentionView(bounds(),width,height);apply();},
    resetView:()=>{view=initialMentionView();apply();},
    getView:()=>({...view}),
    getNodeOffset:name=>({...offsets.get(name)}),
    destroy:()=>{for(const [event,handler] of listeners)svg.removeEventListener(event,handler);host?.removeEventListener('keydown',keyboard);},
  };
  const keyboard=event=>{
    const name=nodeFrom(event.target),arrow={ArrowLeft:[-16,0],ArrowRight:[16,0],ArrowUp:[0,-16],ArrowDown:[0,16]}[event.key];
    if((event.key==='Enter'||event.key===' ')&&name){event.preventDefault();onActivate?.(name,true);return;}
    if(arrow){event.preventDefault();if(name&&byName.has(name)){const o=offsets.get(name);offsets.set(name,{x:o.x+arrow[0]/view.scale,y:o.y+arrow[1]/view.scale});redrawNode(name);}else{view=panMentionView(view,arrow[0],arrow[1]);apply();}}
    else if(event.key==='+'||event.key==='='){event.preventDefault();api.zoom(1.25);}else if(event.key==='-'){event.preventDefault();api.zoom(.8);}else if(event.key==='0'){event.preventDefault();api.fit();}
  };
  const listeners=[['pointerdown',down],['pointermove',move],['pointerup',up],['pointercancel',up],['click',click],['wheel',wheel]];
  for(const [event,handler] of listeners)svg.addEventListener(event,handler,event==='wheel'?{passive:false}:undefined);
  host?.addEventListener('keydown',keyboard);apply();return api;
}
