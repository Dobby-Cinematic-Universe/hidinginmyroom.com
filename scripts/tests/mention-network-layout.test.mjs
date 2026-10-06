import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {selectMentionNetwork,layoutMentionNetwork,mentionNetworkDefaults} from '../../src/lib/corpus/mention-network-layout.mjs';

const data={names:[{name:'Sunny',group:'on_channel'},{name:'Mila',group:'on_channel'},{name:'Chihiro',group:'on_channel'},{name:'Chris Broad',group:'public_creator'},{name:'Anime Man',group:'public_creator'}],edges:[
  {a:'Sunny',b:'Mila',weight:20,baseline:5,lift:4},
  {a:'Sunny',b:'Chihiro',weight:10,baseline:5,lift:2},
  {a:'Mila',b:'Chihiro',weight:8,baseline:4,lift:2},
  {a:'Chris Broad',b:'Anime Man',weight:16,baseline:2,lift:8},
  {a:'Sunny',b:'Chris Broad',weight:.5,baseline:.005,lift:100},
]};

test('defaults rank absolute support and suppress small high-lift links without mutating data',()=>{
  const snapshot=JSON.stringify(data),s=selectMentionNetwork(data);
  assert.deepEqual(mentionNetworkDefaults,{group:'all',minSupport:5,minLift:1.5,maxEdges:40,focus:''});
  assert.equal(s.edges.length,4);assert.deepEqual(s.edges.map(e=>e.weight),[20,16,10,8]);assert.equal(JSON.stringify(data),snapshot);
  const limited=selectMentionNetwork(data,{maxEdges:2});assert.equal(limited.hiddenEdges,2);assert.equal(limited.eligibleEdges,4);
});
test('focus returns direct qualifying neighbors, not neighbors-of-neighbors',()=>{
  const s=selectMentionNetwork(data,{focus:' sunny '});assert.equal(s.focus,'Sunny');assert.equal(s.edges.length,2);assert.deepEqual(new Set(s.nodes.map(n=>n.name)),new Set(['Sunny','Mila','Chihiro']));assert.ok(s.edges.every(e=>e.a==='Sunny'||e.b==='Sunny'));
  assert.equal(selectMentionNetwork(data,{focus:'Chihiro',group:'public_creator'}).reason,'excluded_focus');
  assert.equal(selectMentionNetwork(data,{focus:'New private person'}).reason,'unknown_focus');
  assert.equal(selectMentionNetwork(data,{focus:'Sunny',minSupport:50}).reason,'no_edges');
});
test('clutter cap is applied after focus; group and threshold controls are exact',()=>{
  const focused=selectMentionNetwork(data,{focus:'Chihiro',maxEdges:1});assert.equal(focused.edges[0].weight,10);assert.equal(focused.hiddenEdges,1);
  assert.equal(selectMentionNetwork(data,{group:'public_creator'}).edges.length,1);
  assert.equal(selectMentionNetwork(data,{minSupport:0,minLift:50}).edges[0].weight,.5);
  assert.throws(()=>selectMentionNetwork(data,{minSupport:-1}),/Invalid/);assert.throws(()=>selectMentionNetwork(data,{maxEdges:Infinity}),/Invalid/);
});
test('weighted degree is calculated only from displayed links',()=>{
  const s=selectMentionNetwork(data,{maxEdges:1});assert.equal(s.nodes.find(n=>n.name==='Sunny').degree,20);assert.equal(s.nodes.find(n=>n.name==='Sunny').links,1);assert.equal(s.nodes.length,2);
});
test('connection-driven layout is finite, repeatable, input-order independent and bounds-contained',()=>{
  const s=selectMentionNetwork(data),first=layoutMentionNetwork(s),second=layoutMentionNetwork(s);
  assert.deepEqual(first,second);assert.equal(first.components.length,2);
  const reordered=selectMentionNetwork({names:[...data.names].reverse(),edges:[...data.edges].reverse()});assert.deepEqual(layoutMentionNetwork(reordered),first);
  for(const n of first.nodes){assert.ok(Number.isFinite(n.x)&&Number.isFinite(n.y));assert.ok(n.x>=0&&n.x<=900&&n.y>=0&&n.y<=620);assert.ok(n.labelX>=0&&n.labelX<900&&n.labelY>=0&&n.labelY<=620);}
  assert.notEqual(first.nodes.find(n=>n.name==='Sunny').component,first.nodes.find(n=>n.name==='Chris Broad').component);
  assert.deepEqual(layoutMentionNetwork(selectMentionNetwork(data,{minSupport:100})).nodes,[]);
});
test('focus marks selected node without inferring groups or relationships',()=>{
  const map=layoutMentionNetwork(selectMentionNetwork(data,{focus:'Sunny'}));assert.equal(map.nodes.filter(n=>n.focused).length,1);assert.equal(map.nodes.find(n=>n.focused).name,'Sunny');assert.equal(map.nodes.find(n=>n.name==='Mila').group,'on_channel');assert.equal(map.height,420);
});
test('labels clear actual displayed-degree node radii and focused height remains configurable',()=>{
  const selection=selectMentionNetwork(data,{focus:'Sunny'}),map=layoutMentionNetwork(selection);
  const maxDegree=Math.max(1,...selection.nodes.map(n=>n.degree));
  for(const node of map.nodes){const radius=Math.max(5,20*Math.sqrt(node.degree/maxDegree));const labelWidth=Math.min(170,Math.max(35,node.name.length*6.7));const gap=node.labelX>=node.x?node.labelX-node.x:node.x-(node.labelX+labelWidth);assert.ok(gap>=radius+8-.001,`${node.name}: gap ${gap}, radius ${radius}`);}
  assert.equal(layoutMentionNetwork(selection,{height:620}).height,620);
  assert.equal(layoutMentionNetwork(selectMentionNetwork(data)).height,620);
});
test('layout guards reject invalid dimensions; isolated presentation nodes stay finite',()=>{
  assert.throws(()=>layoutMentionNetwork(selectMentionNetwork(data),{width:NaN}),/Invalid/);
  const map=layoutMentionNetwork({nodes:[{name:'Sunny',group:'on_channel',degree:0,links:0}],edges:[],focus:''});assert.equal(map.nodes.length,1);assert.ok(Number.isFinite(map.nodes[0].x));
});
test('UI supplies searchable focus, support/clutter controls, keyboard nodes and non-color legend',()=>{
  const ui=readFileSync(new URL('../../src/components/corpus/NetworkDynamics.astro',import.meta.url),'utf8');
  for(const marker of ['data-network-focus','data-network-support','data-network-limit','data-network-reset','data-network-overview','datalist','legend-circle','legend-triangle','role:\'button\'','Enter/Space','Lower the minimum weighted co-mentions','not inferred social communities','displayed weighted degree'])assert.ok(ui.includes(marker),marker);
  assert.ok(!ui.includes('Math.cos(angle)*260'));assert.ok(ui.includes("import {selectMentionNetwork,layoutMentionNetwork}"));assert.ok(ui.includes('box-sizing:border-box'));
});
