import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {initialMentionView,zoomMentionView,panMentionView,fitMentionView,attachMentionMapInteraction} from '../../src/lib/corpus/mention-network-interaction.mjs';

class FakeElement{
  constructor(attributes={}){this.attributes={...attributes};this.listeners=new Map();this.parentElement=null;}
  setAttribute(k,v){this.attributes[k]=String(v);}
  getAttribute(k){return this.attributes[k];}
  closest(){return this.attributes['data-node-name']?this:null;}
  addEventListener(k,fn){if(!this.listeners.has(k))this.listeners.set(k,new Set());this.listeners.get(k).add(fn);}
  removeEventListener(k,fn){this.listeners.get(k)?.delete(fn);}
  getBoundingClientRect(){return {left:0,top:0,width:900,height:620};}
  setPointerCapture(){}
  releasePointerCapture(){}
  emit(type,fields={}){const event={type,target:this,pointerId:1,clientX:0,clientY:0,button:0,preventDefault(){this.prevented=true;},...fields};for(const fn of this.listeners.get(type)||[])fn(event);return event;}
}
const fixture=()=>{
  const svg=new FakeElement(),host=new FakeElement(),layer=new FakeElement();svg.parentElement=host;
  const a=new FakeElement({'data-node-name':'Sunny'}),b=new FakeElement({'data-node-name':'Mila'}),edge=new FakeElement();
  const activated=[],views=[];
  const nodes=[{name:'Sunny',x:100,y:100,labelX:130,labelY:105,element:a},{name:'Mila',x:300,y:300,labelX:325,labelY:305,element:b}];
  const api=attachMentionMapInteraction(svg,layer,{width:900,height:620,nodes,edges:[{a:'Sunny',b:'Mila',element:edge}],onActivate:(...args)=>activated.push(args),onViewChange:view=>views.push({...view})});
  return {svg,host,layer,a,b,edge,nodes,api,activated,views};
};
test('anchored zoom preserves the same data point and clamps usable magnification',()=>{
  const view={scale:2,x:30,y:-15},anchor={x:120,y:80},next=zoomMentionView(view,1.5,anchor);
  assert.equal((anchor.x-view.x)/view.scale,(anchor.x-next.x)/next.scale);assert.equal((anchor.y-view.y)/view.scale,(anchor.y-next.y)/next.scale);
  assert.equal(zoomMentionView(view,100,anchor).scale,5);assert.equal(zoomMentionView(view,.001,anchor).scale,.4);
  assert.throws(()=>zoomMentionView(view,0,anchor),/Invalid/);assert.throws(()=>panMentionView(view,NaN,0),/Invalid/);
});
test('fit centers all geometric bounds and handles an empty map',()=>{
  const bounds={left:100,right:700,top:100,bottom:500},v=fitMentionView(bounds,900,620);
  assert.equal(400*v.scale+v.x,450);assert.equal(300*v.scale+v.y,310);
  assert.deepEqual(fitMentionView(null,900,620),initialMentionView());assert.throws(()=>fitMentionView({...bounds,right:0},900,620),/Invalid/);
});
test('node drag updates node and edge geometry without activating or changing aggregate coordinates',()=>{
  const f=fixture(),original=JSON.stringify(f.nodes.map(({element,...n})=>n));
  f.svg.emit('pointerdown',{target:f.a,clientX:100,clientY:100});f.svg.emit('pointermove',{clientX:140,clientY:120});f.svg.emit('pointerup');f.svg.emit('click',{target:f.a});
  assert.deepEqual(f.api.getNodeOffset('Sunny'),{x:40,y:20});assert.equal(f.edge.getAttribute('x1'),'140');assert.equal(f.edge.getAttribute('y1'),'120');assert.equal(f.a.getAttribute('transform'),'translate(40 20)');assert.deepEqual(f.activated,[]);assert.equal(JSON.stringify(f.nodes.map(({element,...n})=>n)),original);
  f.svg.emit('pointerdown',{target:f.a,clientX:140,clientY:120});f.svg.emit('pointerup');f.svg.emit('click',{target:f.a});assert.deepEqual(f.activated,[['Sunny',false]]);
});
test('background pan, wheel and two-pointer pinch change only local view geometry',()=>{
  const f=fixture();f.svg.emit('pointerdown',{clientX:20,clientY:20});f.svg.emit('pointermove',{clientX:60,clientY:30});f.svg.emit('pointerup');assert.deepEqual(f.api.getView(),{scale:1,x:40,y:10});
  f.svg.emit('wheel',{clientX:450,clientY:310,deltaY:-100,deltaMode:0});assert.ok(f.api.getView().scale>1);f.api.resetView();
  f.svg.emit('pointerdown',{pointerId:1,clientX:200,clientY:200});f.svg.emit('pointerdown',{pointerId:2,clientX:300,clientY:200});f.svg.emit('pointermove',{pointerId:2,clientX:400,clientY:200});assert.equal(f.api.getView().scale,2);assert.deepEqual(f.activated,[]);
  f.svg.emit('pointercancel',{pointerId:1});f.svg.emit('pointerup',{pointerId:2});assert.ok(Object.values(f.api.getView()).every(Number.isFinite));
});
test('keyboard supports view navigation, node movement and explicit activation',()=>{
  const f=fixture();f.host.emit('keydown',{key:'ArrowRight'});assert.equal(f.api.getView().x,16);f.host.emit('keydown',{key:'+'});assert.equal(f.api.getView().scale,1.25);
  f.host.emit('keydown',{target:f.a,key:'ArrowRight'});assert.equal(f.api.getNodeOffset('Sunny').x,12.8);f.host.emit('keydown',{target:f.a,key:'Enter'});assert.deepEqual(f.activated,[['Sunny',true]]);
  f.host.emit('keydown',{key:'0'});assert.ok(f.api.getView().scale>0);f.api.destroy();assert.equal(f.host.listeners.get('keydown').size,0);assert.equal(f.svg.listeners.get('pointerdown').size,0);
});
test('view reset leaves dragged nodes and filters untouched; layout reset remains a separate action',()=>{
  const f=fixture();f.svg.emit('pointerdown',{target:f.a,clientX:100,clientY:100});f.svg.emit('pointermove',{clientX:150,clientY:130});f.svg.emit('pointerup');f.api.zoom(2);f.api.resetView();assert.deepEqual(f.api.getView(),initialMentionView());assert.deepEqual(f.api.getNodeOffset('Sunny'),{x:50,y:30});
  const ui=readFileSync(new URL('../../src/components/corpus/NetworkDynamics.astro',import.meta.url),'utf8');for(const marker of ['data-map-fit','data-map-reset-layout','data-map-reset-view','data-map-zoom-in','data-map-zoom-out','View and layout resets keep your data filters','min-width:0;width:100%','grid-template-columns:minmax(0,1fr)','touch-action:none'])assert.ok(ui.includes(marker),marker);
  assert.ok(ui.includes("q('[data-map-reset-layout]').addEventListener('click',network)"));
});
test('mobile view toolbar wraps compactly without changing selector grids or touch targets',()=>{
  const ui=readFileSync(new URL('../../src/components/corpus/NetworkDynamics.astro',import.meta.url),'utf8');
  assert.ok(ui.includes('.network-controls.network-view-controls{display:flex;flex-wrap:wrap;align-items:center;gap:.5rem}'));
  assert.ok(ui.includes('.network-controls,.network-controls.network-map-controls{display:grid;grid-template-columns:minmax(0,1fr)}'));
  assert.ok(ui.includes('min-height:44px'));
});
