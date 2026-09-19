import test from 'node:test';
import assert from 'node:assert/strict';
import {createDerivedGraph,dateMentions} from '../../src/lib/corpus/derived-graph.mjs';

const catalog={releaseId:'test',generatedAt:'2026-09-17T00:00:00Z',facets:{speakers:['Daniel','Mila','SPEAKER_0000','Playback / game audio','Unknown speaker']},recordings:[
  {recording_id:'a',slug:'a',title:'First recording',date_label:'2020-01-01',date_basis:'filename'},
  {recording_id:'b',slug:'b',title:'Second recording',date_label:'2021-01-01',date_basis:'filename'}]};
const summary=(id,text,classification='reported_statement')=>({id:'summary-'+id,kind:'transcript',recording_id:id,sections:{summary:[],topics:[],uncertainties:[],events:[{text,classification,source_recording_ids:[id]}]}});

test('mentions are not speaker appearances or asserted event participation',()=>{
  const s=[summary('a','Daniel discussed Mila and Japan in 2016.')];
  const original=JSON.stringify(s);const b=createDerivedGraph(catalog,s);
  b.addRecording({recording_id:'a',transcript_revisions:[{segments:[{text:'Mila went to Japan.',speaker_label:'Daniel'}]}]});
  const g=b.finish(),m=g.entities.find(e=>e.label==='Mila'),d=g.entities.find(e=>e.label==='Daniel');
  assert.equal(m.mentions.length,1);assert.equal(m.speakerRecordings.length,0);
  assert.equal(d.speakerRecordings.length,1);assert.equal(g.eventParticipants.length,0);
  assert.ok(!g.entities.some(e=>/SPEAKER_|Unknown speaker|Playback/.test(e.label)));
  assert.deepEqual(g.events[0].dateMentions,['2016']);
  assert.equal(g.events[0].sourceRecordings[0].recording_date,'2020-01-01');
  assert.equal(g.eventDates.length,0);assert.equal(JSON.stringify(s),original);
});

test('identical descriptions group sources, contradictory accounts remain separate',()=>{
  const b=createDerivedGraph(catalog,[summary('a','Daniel moved to Japan.'),summary('b','Daniel moved to Japan.'),summary('b','Daniel did not move to Japan.')]);
  const g=b.finish();assert.equal(g.events.length,2);
  assert.equal(g.events.find(e=>e.description==='Daniel moved to Japan.').sourceRecordings.length,2);
  assert.equal(g.events.find(e=>e.description==='Daniel did not move to Japan.').sourceRecordings.length,1);
});

test('whole-name matching does not treat Milana as Mila and links resolve to known recordings',()=>{
  const b=createDerivedGraph(catalog,[summary('a','Milana was mentioned.'),summary('missing','Daniel went to Japan.')]);
  const g=b.finish();assert.ok(!g.entities.some(e=>e.label==='Mila'));
  assert.equal(g.events.length,1);assert.equal(g.events[0].sourceRecordings[0].slug,'a');
});

test('stable event IDs and literal date mentions do not resolve relative dates',()=>{
  const s=summary('a','Yesterday Daniel mentioned May 3, 2019 and 2020-04-01.');
  assert.deepEqual(dateMentions(s.sections.events[0].text),['May 3, 2019','2020-04-01']);
  assert.equal(createDerivedGraph(catalog,[s]).finish().events[0].event_id,createDerivedGraph(catalog,[s]).finish().events[0].event_id);
});

test('sentence lead-ins are not promoted to entity names',()=>{
  const s=[summary('a','Whether Daniel met Chris Broad is unclear.'),summary('b','Whether Daniel met Chris Broad is unclear.')];
  const g=createDerivedGraph(catalog,s).finish();
  assert.ok(!g.entities.some(e=>e.label==='Whether Daniel'));
  assert.ok(g.entities.some(e=>e.label==='Daniel'));
  assert.ok(g.entities.some(e=>e.label==='Chris Broad'));
});
