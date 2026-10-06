import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { normalizeCamera, zoomCamera, panCamera, attachMapViewport } from '../../src/lib/analysis/map-viewport.mjs';

class Element {
  constructor() { this.listeners = new Map(); this.dataset = {}; this.attributes = {}; this.captures = []; }
  addEventListener(type, fn) { if (!this.listeners.has(type)) this.listeners.set(type, new Set()); this.listeners.get(type).add(fn); }
  removeEventListener(type, fn) { this.listeners.get(type)?.delete(fn); }
  setAttribute(key, value) { this.attributes[key] = value; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 720, height: 420 }; }
  setPointerCapture(id) { this.captures.push(id); }
  releasePointerCapture(id) { this.released = id; this.releaseCount=(this.releaseCount??0)+1; }
  querySelector() { return this.status; }
  querySelectorAll() { return this.buttons ?? []; }
  emit(type, values = {}) { const event = { target: this, button: 0, pointerId: 1, clientX: 100, clientY: 100, preventDefault() { this.prevented = true; }, stopImmediatePropagation() { this.stopped = true; }, ...values }; for (const fn of this.listeners.get(type) ?? []) fn(event); return event; }
}
function fixture() {
  const svg = new Element(), layer = new Element(), controls = new Element(), doc = new Element(); svg.ownerDocument = doc;
  controls.status = {}; controls.buttons = ['in','out','fit','reset'].map(action => { const button = new Element(); button.dataset.mapAction = action; return button; });
  let nextFrame=0; const frames=new Map(), cancelled=[];
  const changes = [], api = attachMapViewport({ svg, layer, controls, width:720, height:420, onChange:camera => changes.push(camera),
    requestFrame:callback=>{const id=nextFrame++;frames.set(id,callback);return id;}, cancelFrame:id=>{cancelled.push(id);frames.delete(id);} });
  const tick=()=>{const pending=[...frames];frames.clear();for(const [,callback] of pending)callback();};
  return { svg, layer, controls, doc, changes, api, frames, cancelled, tick };
}
test('uniform anchored camera zoom preserves data position and does not mutate input', () => {
  const camera = Object.freeze({ x:30,y:-15,scale:2 }), anchor = { x:120,y:80 }, next = zoomCamera(camera,1.5,anchor);
  assert.equal((anchor.x-camera.x)/camera.scale,(anchor.x-next.x)/next.scale);
  assert.equal((anchor.y-camera.y)/camera.scale,(anchor.y-next.y)/next.scale);
  assert.equal(zoomCamera(camera,100,anchor).scale,12); assert.equal(zoomCamera(camera,.001,anchor).scale,.5);
  assert.deepEqual(zoomCamera(camera,NaN,anchor),camera); assert.deepEqual(normalizeCamera({x:NaN,y:Infinity,scale:NaN}),{x:0,y:0,scale:1});
  assert.deepEqual(panCamera(camera,10,-10),{x:40,y:-25,scale:2});
});
test('ordinary scrolling is untouched; Ctrl and Alt enable anchored wheel zoom', () => {
  const f = fixture(); const event=f.svg.emit('wheel',{deltaY:-100}); assert.equal(event.prevented,undefined); assert.equal(f.api.getCamera().scale,1);
  assert.equal(f.svg.emit('wheel',{deltaY:-100,ctrlKey:true}).prevented,true); assert.ok(f.api.getCamera().scale>1);
  f.api.reset(); f.svg.emit('wheel',{deltaY:-100,altKey:true}); assert.ok(f.api.getCamera().scale>1); f.api.destroy();
});
test('drag changes only camera, captures after threshold and suppresses exactly the synthetic click', () => {
  const f=fixture(); f.svg.emit('pointerdown'); f.svg.emit('pointermove',{clientX:102}); assert.deepEqual(f.svg.captures,[]);
  f.svg.emit('pointermove',{clientX:140,clientY:120}); assert.deepEqual(f.api.getCamera(),{x:40,y:20,scale:1}); assert.deepEqual(f.svg.captures,[1]);
  f.doc.emit('pointerup'); const click=f.svg.emit('click'); assert.equal(click.stopped,true); assert.equal(f.svg.emit('click').stopped,undefined); assert.equal(f.svg.released,1);
  f.tick(); assert.deepEqual(f.layer.attributes,{transform:'translate(40 20) scale(1)'}); f.api.destroy();
});
test('keyboard only acts on map itself and toolbar fits reset to the admitted full extent', () => {
  const f=fixture(); const point=new Element(); assert.equal(f.svg.emit('keydown',{key:'ArrowRight',target:point}).prevented,undefined);
  f.svg.emit('keydown',{key:'ArrowRight'}); assert.equal(f.api.getCamera().x,-30); f.svg.emit('keydown',{key:'+'}); assert.equal(f.api.getCamera().scale,1.25);
  f.controls.buttons[2].emit('click'); f.tick(); assert.deepEqual(f.api.getCamera(),{x:0,y:0,scale:1}); assert.equal(f.controls.status.textContent,'100%');
  f.controls.buttons[0].emit('click'); f.svg.emit('keydown',{key:'0'}); assert.equal(f.api.getCamera().scale,1); f.api.destroy();
});
test('destroy releases owned capture and document listeners and ignores later writes', () => {
  const f=fixture(); f.svg.emit('pointerdown'); f.svg.emit('pointermove',{clientX:120}); f.api.destroy(); f.api.destroy();
  assert.equal(f.svg.released,1); assert.equal(f.svg.releaseCount,1); for (const node of [f.svg,f.doc,...f.controls.buttons]) for (const listeners of node.listeners.values()) assert.equal(listeners.size,0);
  const count=f.changes.length; f.api.setCamera({x:999,y:999,scale:2}); assert.equal(f.changes.length,count);
  assert.deepEqual(f.api.getCamera(),{x:20,y:0,scale:1}); assert.equal(f.frames.size,0);
});
test('initial draw is synchronous and burst updates render only the final camera once per frame', () => {
  const f=fixture(); assert.equal(f.changes.length,1); assert.equal(f.frames.size,0);
  for(let x=1;x<=100;x++)f.api.setCamera({x,y:-x,scale:2});
  assert.deepEqual(f.api.getCamera(),{x:100,y:-100,scale:2}); assert.equal(f.frames.size,1);
  assert.equal(f.changes.length,1); assert.equal(f.layer.attributes.transform,'translate(0 0) scale(1)');
  f.tick(); assert.equal(f.changes.length,2); assert.deepEqual(f.changes[1],f.api.getCamera()); assert.equal(f.frames.size,0);
  f.api.destroy();
});
test('reset coalesces pending changes and explicit same-camera resize updates still notify', () => {
  const f=fixture(); f.api.setCamera({x:50,y:20,scale:3});f.api.reset();f.tick();
  assert.equal(f.changes.length,2);assert.deepEqual(f.changes[1],{x:0,y:0,scale:1});
  f.api.setCamera(f.api.getCamera());f.tick();assert.equal(f.changes.length,3);f.api.destroy();
});
test('flush cancels even frame zero, applies latest pending state, and is idempotent', () => {
  const f=fixture();f.api.setCamera({x:25,y:15,scale:2});const queued=[...f.frames.values()][0];f.api.flush();
  assert.deepEqual(f.cancelled,[0]);assert.equal(f.frames.size,0);assert.equal(f.changes.length,2);
  f.api.flush();queued();assert.equal(f.changes.length,2);
  f.api.setCamera({x:30,y:10,scale:2});queued();assert.equal(f.frames.size,1);assert.equal(f.changes.length,2);
  f.tick();assert.equal(f.changes.length,3);f.api.destroy();
});
test('destroy flushes camera handoff once and stale callbacks cannot render later', () => {
  const f=fixture();f.api.setCamera({x:42,y:-12,scale:4});const queued=[...f.frames.values()][0];
  f.api.destroy();assert.deepEqual(f.changes.at(-1),{x:42,y:-12,scale:4});assert.equal(f.changes.length,2);
  f.api.destroy();queued();f.api.flush();assert.equal(f.changes.length,2);assert.equal(f.frames.size,0);
});
test('consumer camera update during rendering schedules only the following frame', () => {
  const svg=new Element(),layer=new Element(),frames=new Map(),changes=[];let id=0,api;
  api=attachMapViewport({svg,layer,width:720,height:420,requestFrame:callback=>{frames.set(id,callback);return id++;},cancelFrame:key=>frames.delete(key),
    onChange:camera=>{changes.push(camera);if(camera.x===1)api.setCamera({x:2,y:0,scale:1});}});
  api.setCamera({x:1,y:0,scale:1});const first=[...frames.values()][0];frames.clear();first();
  assert.equal(changes.length,2);assert.equal(layer.attributes.transform,'translate(1 0) scale(1)');assert.equal(frames.size,1);
  const second=[...frames.values()][0];frames.clear();second();assert.equal(changes.length,3);assert.equal(layer.attributes.transform,'translate(2 0) scale(1)');api.destroy();
});
test('recording map preserves separate cameras, full-profile neighbors and equal-unit axes', () => {
  const source=readFileSync(new URL('../../src/lib/analysis/discovery.mjs',import.meta.url),'utf8'),ui=readFileSync(new URL('../../src/components/corpus/AnalysisDiscovery.astro',import.meta.url),'utf8');
  for (const marker of ['cameras.get(projection.id)','cameras.set(projection.id,camera)','viewport?.destroy()','unitScale=Math.min','(sx-camera.x)/camera.scale','clip-path','7/camera.scale','2.7/camera.scale']) assert.ok(source.includes(marker),marker);
  for (const marker of ['data-map-action="in"','data-map-action="out"','data-map-action="fit"','data-map-action="reset"','Ctrl or Alt','touch-action:pan-y','min-height:2.75rem','Similar-recording lists always use all nine']) assert.ok(ui.includes(marker),marker);
  assert.ok(!source.includes('point.x=')); assert.ok(!source.includes('point.y='));
});
