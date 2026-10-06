import test from 'node:test';
import assert from 'node:assert/strict';
import { renderDiscovery } from '../../src/lib/analysis/discovery.mjs';

// Deliberately small DOM double: these are write-count/lifecycle tests, not a
// browser layout or frame-rate benchmark. Real SVG geometry is checked in UI QA.
class Node {
  constructor(tag, doc) { this.tagName=tag;this.ownerDocument=doc;this.children=[];this.dataset={};this.attributes={};this.listeners=new Map();this.writes=new Map();this.style={};this.value='';this.replacements=0; }
  setAttribute(name,value) { this.attributes[name]=String(value);this.writes.set(name,(this.writes.get(name)??0)+1);if(name.startsWith('data-'))this.dataset[name.slice(5).replace(/-([a-z])/g,(_,c)=>c.toUpperCase())]=String(value); }
  getAttribute(name) { return this.attributes[name]??null; }
  append(...children) { for(const child of children){child.parentNode=this;this.children.push(child);} }
  prepend(...children) { for(const child of children)child.parentNode=this;this.children.unshift(...children); }
  replaceChildren(...children) { this.replacements++;for(const child of this.children)child.parentNode=null;this.children=[];this.append(...children); }
  remove() { if(this.parentNode){this.parentNode.children=this.parentNode.children.filter(child=>child!==this);this.parentNode=null;} }
  get firstChild() { return this.children[0]??null; }
  get firstElementChild() { return this.firstChild; }
  get childElementCount() { return this.children.length; }
  addEventListener(type,callback,options) { if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push({callback,capture:options===true||options?.capture}); }
  removeEventListener(type,callback) { this.listeners.set(type,(this.listeners.get(type)??[]).filter(item=>item.callback!==callback)); }
  emit(type,values={}) { const event={target:this,currentTarget:this,button:0,pointerId:1,clientX:100,clientY:100,preventDefault(){this.defaultPrevented=true;},stopImmediatePropagation(){this.stopped=true;},...values};for(const {callback}of[...(this.listeners.get(type)??[])].sort((a,b)=>Number(b.capture)-Number(a.capture))){callback(event);if(event.stopped)break;}return event; }
  matches(selector) { if(selector.startsWith('[')){const match=/^\[([^=\]]+)(?:="([^"]*)")?\]$/.exec(selector);return !!match&&Object.hasOwn(this.attributes,match[1])&&(match[2]===undefined||this.attributes[match[1]]===match[2]);}if(selector.startsWith('option['))return this.tagName==='option'&&this.value===selector.match(/value="([^"]*)"/)[1];return this.tagName===selector; }
  querySelectorAll(selector) { const descendants=this.children.flatMap(child=>[child,...child.querySelectorAll('*')]);return selector==='*'?descendants:descendants.filter(child=>child.matches(selector)); }
  querySelector(selector) { return this.querySelectorAll(selector)[0]??null; }
  focus() { this.ownerDocument.activeElement=this; }
  getBoundingClientRect() { return {left:0,top:0,width:720,height:420}; }
  getScreenCTM() { const transform=this.attributes.transform;const match=transform?.match(/^translate\(([-\d.]+) ([-\d.]+)\) scale\(([-\d.]+)\)$/);return match?{a:Number(match[3]),b:0,c:0,d:Number(match[3]),e:Number(match[1]),f:Number(match[2])}:{a:1,b:0,c:0,d:1,e:0,f:0}; }
  setPointerCapture() {}
  releasePointerCapture() {}
}

function fixture() {
  let serial=0;const frames=new Map();const doc=new Node('document',null);doc.ownerDocument=doc;doc.defaultView={requestAnimationFrame:callback=>{frames.set(++serial,callback);return serial;},cancelAnimationFrame:id=>frames.delete(id)};
  doc.createElement=tag=>new Node(tag,doc);doc.createElementNS=(_,tag)=>new Node(tag,doc);
  const root=new Node('section',doc);const nodes={};
  for(const key of ['recording','search','color','map','view','detail','summary','result-count','selection','legend','map-controls','picking','umap-method']){const node=new Node(key==='recording'||key==='view'||key==='color'?'select':'div',doc);node.setAttribute(`data-discovery-${key}`,'');root.append(node);nodes[key]=node;}
  for(const action of ['in','out','fit','reset']){const button=new Node('button',doc);button.setAttribute('data-map-action',action);nodes['map-controls'].append(button);}
  const status=new Node('span',doc);status.setAttribute('data-map-zoom','');nodes['map-controls'].append(status);
  for(const name of ['pca','umap']){const option=new Node('option',doc);option.value=name;nodes.view.append(option);}nodes.view.value='pca';nodes.color.value='year';
  const release={videos:[{recording_id:'a',title:'First',date:'2020-01-01',category:'Vlog',href:'/a/',factor_scores:{F1:{value:0}}},{recording_id:'b',title:'Second',date:'2021-01-01',category:'Vlog',href:'/b/',factor_scores:{F1:{value:1}}},{recording_id:'c',title:'Crowded',date:null,category:'Vlog',href:'/c/',factor_scores:{F1:{value:.01}}}],analysis:{factors:[{id:'F1',label:'One'}]}};
  const data={points:[{id:'a',x:-1,y:-1,radius:1},{id:'b',x:1,y:1,radius:1},{id:'c',x:-.99,y:-1,radius:1}],neighbors:[{id:'a',items:[{id:'b',distance:1}]},{id:'b',items:[{id:'a',distance:1}]},{id:'c',items:[{id:'a',distance:.01}]}],factor_ids:['F1'],cohort_n:3,excluded_recordings:0,pca:{cumulative_variance_ratio:[.5,1],explained_variance_ratio:[.5,.5]}};
  const tick=()=>{const pending=[...frames.values()];frames.clear();for(const callback of pending)callback();};
  const umap={points:data.points.map(point=>({...point,x:point.y,y:-point.x})),method:{package:'umap-learn',package_version:'0.5.9',parameters:{n_neighbors:2,min_dist:.1,metric:'euclidean',random_state:42}}};
  return {root,nodes,doc,release,data,umap,tick,frames};
}

function withFixture(callback) { const previous=globalThis.document;const f=fixture();globalThis.document=f.doc;try{renderDiscovery(f.root,f.release,f.data,f.umap);callback(f);}finally{globalThis.document=previous;} }

test('recording map batches camera writes and reuses marker sizes and axes on pan',()=>withFixture(f=>{
  const svg=f.nodes.map.querySelector('svg'),layer=svg.querySelector('[data-map-layer]');
  const circles=layer.querySelectorAll('circle').filter(circle=>!circle.hasAttribute?.('data-discovery-selected-point')&&!Object.hasOwn(circle.attributes,'data-discovery-selected-point'));
  const texts=svg.querySelectorAll('text'),radiusWrites=circles.map(circle=>circle.writes.get('r'));
  const transforms=layer.writes.get('transform');
  for(let i=0;i<20;i++)svg.emit('keydown',{key:'ArrowRight'});
  assert.equal(f.frames.size,1);assert.equal(layer.writes.get('transform'),transforms);
  f.tick();assert.equal(layer.writes.get('transform'),transforms+1);
  assert.deepEqual(circles.map(circle=>circle.writes.get('r')),radiusWrites,'panning does not resize every marker');
  assert.deepEqual(svg.querySelectorAll('text'),texts,'numeric axis nodes are retained');
  svg.emit('keydown',{key:'+'});f.tick();
  assert.deepEqual(circles.map(circle=>circle.writes.get('r')),radiusWrites.map(count=>count+1),'zoom updates each marker radius once');
}));

test('recording selection retains selector option nodes and chart geometry',()=>withFixture(f=>{
  const options=[...f.nodes.recording.children],svg=f.nodes.map.querySelector('svg');
  f.nodes.recording.value='b';f.nodes.recording.emit('change');
  assert.deepEqual(f.nodes.recording.children,options);assert.equal(f.nodes.map.querySelector('svg'),svg);
  f.nodes.search.value='First';f.nodes.search.emit('input');const filtered=[...f.nodes.recording.children];
  f.nodes.search.emit('input');assert.deepEqual(f.nodes.recording.children,filtered,'identical selector query reuses options');
  assert.equal(f.nodes.recording.value,'a');
}));

test('recording dots have one delegated selection path rather than per-marker listeners',()=>withFixture(f=>{
  const svg=f.nodes.map.querySelector('svg'),layer=svg.querySelector('[data-map-layer]');
  for(const circle of layer.querySelectorAll('circle'))assert.equal(circle.listeners.get('click')?.length??0,0);
  assert.ok((svg.listeners.get('click')?.length??0)>=1);
}));

test('crowded click leaves selection unchanged, offers dated candidates and accepts explicit choice',()=>withFixture(f=>{
  const svg=f.nodes.map.querySelector('svg');svg.emit('click',{clientX:194,clientY:376});
  assert.equal(f.nodes.recording.value,'a');assert.equal(f.nodes.picking.hidden,false);
  const buttons=f.nodes.picking.querySelector('ol').querySelectorAll('button');assert.equal(buttons.length,2);
  assert.match(buttons[0].textContent,/First · 2020-01-01/);assert.match(buttons[1].textContent,/Crowded · Undated/);
  assert.equal(f.doc.activeElement,buttons[0]);buttons[1].emit('click');
  assert.equal(f.nodes.recording.value,'c');assert.equal(f.nodes.picking.hidden,true);
  assert.match(f.nodes.detail.querySelector('h4').textContent,/Closest recording profiles/);
}));

test('drag suppression blocks delegated picking while the next click still works',()=>withFixture(f=>{
  const svg=f.nodes.map.querySelector('svg');svg.emit('pointerdown',{clientX:194,clientY:300});svg.emit('pointermove',{clientX:204,clientY:300});f.doc.emit('pointerup');
  svg.emit('click',{clientX:526,clientY:44});assert.equal(f.nodes.recording.value,'a');assert.equal(f.nodes.picking.hidden,true);
  svg.emit('click',{clientX:536,clientY:44});assert.equal(f.nodes.recording.value,'b');
}));

test('color redraw and view switching preserve the final pending per-projection camera',()=>withFixture(f=>{
  const first=f.nodes.map.querySelector('svg');first.emit('keydown',{key:'ArrowRight'});first.emit('keydown',{key:'+'});
  f.nodes.color.value='category';f.nodes.color.emit('change');const second=f.nodes.map.querySelector('svg');
  assert.notEqual(second,first);assert.equal(second.querySelector('[data-map-layer]').attributes.transform,'translate(-127.5 -52.5) scale(1.25)');
  assert.equal(f.frames.size,0,'old projection flush cancels its pending frame');assert.equal(f.nodes.recording.value,'a');
  f.nodes.view.value='umap';f.nodes.view.emit('change');const nonlinear=f.nodes.map.querySelector('svg');
  assert.equal(nonlinear.querySelector('[data-map-layer]').attributes.transform,'translate(0 0) scale(1)');
  nonlinear.emit('keydown',{key:'ArrowDown'});f.nodes.view.value='pca';f.nodes.view.emit('change');
  assert.equal(f.nodes.map.querySelector('[data-map-layer]').attributes.transform,'translate(-127.5 -52.5) scale(1.25)');
  f.nodes.view.value='umap';f.nodes.view.emit('change');
  assert.equal(f.nodes.map.querySelector('[data-map-layer]').attributes.transform,'translate(0 -30) scale(1)');
  assert.equal(f.nodes.recording.value,'a');
}));
