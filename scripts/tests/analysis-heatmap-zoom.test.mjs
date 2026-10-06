import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {heatmapScale, heatmapFitScale, attachHeatmapZoom} from '../../src/lib/analysis/heatmap-zoom.mjs';

class Element {
  constructor(){this.listeners=new Map();this.attributes={};this.dataset={};this.properties={};this.style={setProperty:(name,value,priority)=>this.properties[name]={value,priority}};this.clientWidth=400;this.clientHeight=300;this.scrollLeft=0;this.scrollTop=0;}
  setAttribute(name,value){this.attributes[name]=value;}
  addEventListener(name,handler){this.listeners.set(name,[...(this.listeners.get(name)??[]),handler]);}
  removeEventListener(name,handler){this.listeners.set(name,(this.listeners.get(name)??[]).filter(fn=>fn!==handler));}
  emit(name,fields={}){const event={target:this,key:'',preventDefault(){this.prevented=true},...fields};for(const fn of this.listeners.get(name)??[])fn(event);return event;}
}
function fixture(){const viewport=new Element(),content=new Element();const items=Object.fromEntries(['out','in','fit','reset','level'].map(key=>[key,new Element()]));const controls={querySelector:selector=>items[selector.match(/zoom-(\w+)/)[1]]};const api=attachHeatmapZoom({viewport,content,controls,width:1000,height:800,label:'Test matrix'});return {viewport,content,items,api};}

test('finite heatmap size is clamped and fit does not enlarge small charts',()=>{
  assert.equal(heatmapScale(.01),.2);assert.equal(heatmapScale(99),3);assert.equal(heatmapFitScale(1000,400),.4);assert.equal(heatmapFitScale(200,400),1);
  for(const bad of [0,-1,NaN,Infinity])assert.throws(()=>heatmapScale(bad),/Invalid/);
  assert.throws(()=>heatmapFitScale(1000,0),/Invalid/);
});
test('zoom uses actual layout dimensions and preserves the visible center without touching geometry',()=>{
  const f=fixture();f.content.attributes.viewBox='0 0 1000 800';f.viewport.scrollLeft=100;f.viewport.scrollTop=50;
  f.items.in.emit('click');assert.equal(f.api.getScale(),1.25);assert.deepEqual(f.content.properties.width,{value:'1250px',priority:'important'});assert.equal(f.content.properties.height.value,'1000px');
  assert.equal(f.viewport.scrollLeft,175);assert.equal(f.viewport.scrollTop,100);assert.equal(f.content.attributes.viewBox,'0 0 1000 800');assert.equal(f.items.level.textContent,'125%');
  assert.equal(f.content.properties.transform,undefined);assert.equal(f.viewport.attributes.tabindex,'0');
});
test('fit and reset have distinct sizes and restore top-left scroll position',()=>{
  const f=fixture();f.api.setScale(2);f.api.fit();assert.equal(f.api.getScale(),.4);assert.equal(f.content.properties.width.value,'400px');assert.equal(f.viewport.scrollLeft,0);assert.equal(f.viewport.scrollTop,0);
  f.items.reset.emit('click');assert.equal(f.api.getScale(),1);assert.equal(f.content.properties.width.value,'1000px');assert.equal(f.items.level.textContent,'100%');
  f.api.setScale(3);assert.equal(f.items.in.disabled,true);f.api.setScale(.2);assert.equal(f.items.out.disabled,true);
});
test('keyboard zoom is viewport-only while native scrolling and wheel are not intercepted',()=>{
  const f=fixture();assert.equal(f.viewport.emit('keydown',{key:'+'}).prevented,true);assert.equal(f.api.getScale(),1.25);
  assert.equal(f.viewport.emit('keydown',{key:'ArrowRight'}).prevented,undefined);assert.equal(f.viewport.emit('keydown',{key:'PageDown'}).prevented,undefined);
  assert.equal(f.viewport.listeners.has('wheel'),false);assert.equal(f.viewport.listeners.has('pointerdown'),false);
  f.viewport.emit('keydown',{key:'+',target:f.content});assert.equal(f.api.getScale(),1.25);
  f.viewport.emit('keydown',{key:'+',ctrlKey:true});assert.equal(f.api.getScale(),1.25);
  f.viewport.emit('keydown',{key:'f'});assert.equal(f.api.getScale(),.4);f.viewport.emit('keydown',{key:'0'});assert.equal(f.api.getScale(),1);
  f.api.destroy();assert.equal(f.viewport.listeners.get('keydown').length,0);assert.equal(f.items.in.listeners.get('click').length,0);
});
test('lazy matrix renderers provide visible controls and bounded native overflow',()=>{
  for(const name of ['AnalysisRelationships','AnalysisFactorDiagnostics']){
    const source=readFileSync(new URL(`../../src/components/corpus/${name}.astro`,import.meta.url),'utf8');
    for(const key of ['out','in','fit','reset','level'])assert.ok(source.includes(`data-heatmap-zoom-${key}`));
    assert.ok(source.includes('max-height:36rem'));assert.ok(source.includes('min-height:44px'));assert.ok(source.includes('overflow:auto'));
  }
  for(const name of ['relationships','factor-diagnostics']){
    const source=readFileSync(new URL(`../../src/lib/analysis/${name}.mjs`,import.meta.url),'utf8');assert.ok(source.includes('attachHeatmapZoom({viewport'));assert.ok(source.includes("addEventListener('toggle'"));
  }
});
