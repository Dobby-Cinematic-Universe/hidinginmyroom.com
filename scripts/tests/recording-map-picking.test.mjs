import test from 'node:test';
import assert from 'node:assert/strict';
import { nearbyMapPoints, screenPoint } from '../../src/lib/analysis/recording-map-picking.mjs';
const identity={a:1,b:0,c:0,d:1,e:0,f:0},clip={x:44,y:44,width:632,height:332};
test('nearest screen centers win regardless of SVG paint order, with stable ties',()=>{
  const points=[{id:'far',x:106,y:100},{id:'near',x:101,y:100},{id:'tie',x:99,y:100}];
  assert.deepEqual(nearbyMapPoints(points,{x:100,y:100},identity,identity,clip).map(p=>p.id),['near','tie','far']);
});
test('CSS tolerance includes responsive scaling and actual camera pan and zoom',()=>{
  const root={a:.5,b:0,c:0,d:.5,e:100,f:50},camera={a:2,b:0,c:0,d:2,e:20,f:-10};
  const points=[{id:'hit',x:100,y:100},{id:'outside',x:105,y:100}];
  assert.deepEqual(screenPoint(points[0],camera),{x:220,y:190});
  assert.deepEqual(nearbyMapPoints(points,{x:220,y:190},camera,root,clip).map(p=>p.id),['hit']);
});
test('clip excludes hidden centers even when close to the click and excludes clicks on axes',()=>{
  const points=[{id:'hidden',x:43,y:100},{id:'edge',x:44,y:100}];
  assert.deepEqual(nearbyMapPoints(points,{x:45,y:100},identity,identity,clip).map(p=>p.id),['edge']);
  assert.deepEqual(nearbyMapPoints(points,{x:43,y:100},identity,identity,clip),[]);
});
test('letterboxing, offset and affine screen geometry do not change picking semantics',()=>{
  const matrix={a:0,b:2,c:-2,d:0,e:1000,f:40};
  const point={id:'a',x:100,y:100};
  assert.deepEqual(nearbyMapPoints([point],screenPoint(point,matrix),matrix,matrix,clip),[{id:'a',distance:0}]);
});
test('invalid or singular screen geometry fails closed',()=>{
  const point={id:'a',x:100,y:100};
  for(const matrix of [null,{...identity,a:NaN},{...identity,a:0,d:0}])assert.deepEqual(nearbyMapPoints([point],point,identity,matrix,clip),[]);
  assert.deepEqual(nearbyMapPoints([point],point,identity,identity,clip,-1),[]);
});
test('native scroll-frame visibility excludes offscreen centers within the picking tolerance',()=>{
  const visible={x:44,y:44,width:100,height:100};
  const points=[{id:'visible',x:140,y:100},{id:'hidden-right',x:145,y:100},{id:'hidden-bottom',x:140,y:145}];
  assert.deepEqual(nearbyMapPoints(points,{x:142,y:100},identity,identity,clip,8,visible).map(p=>p.id),['visible']);
  assert.deepEqual(nearbyMapPoints(points,{x:140,y:142},identity,identity,clip,8,visible),[]);
  assert.deepEqual(nearbyMapPoints(points,{x:145,y:100},identity,identity,clip,8,visible),[]);
  assert.deepEqual(nearbyMapPoints(points,{x:140,y:100},identity,identity,clip,8,{...visible,width:NaN}),[]);
});
