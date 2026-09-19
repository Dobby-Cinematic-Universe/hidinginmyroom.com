import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {groupingInput,groupingDigest,attachEventGroups,eventDirectoryItems} from '../../src/lib/corpus/event-groups.mjs';
const event=(id,recording)=>({event_id:id,slug:id,description:id,label:id,mentionedEntities:[],dateMentions:[],sourceSummaryIds:[id],sourceRecordings:[{recording_id:recording,slug:recording,title:recording}]});
const graph={entities:[],events:[event('a','r1'),event('b','r2'),event('c','r3')]};
const members=['a','b'];
const artifact={kind:'himr_local_related_accounts',schema_version:1,input_sha256:groupingDigest(groupingInput(graph)),groups:[{id:'accounts_'+createHash('sha256').update(JSON.stringify(members)).digest('hex').slice(0,24),members,representative:'a'}]};

test('groups retain originals and every source; unmatched events remain searchable',()=>{
  const g=attachEventGroups(graph,artifact);assert.deepEqual(g.events,graph.events);
  assert.equal(g.eventGroups[0].sourceRecordings.length,2);
  const items=eventDirectoryItems(g);assert.equal(items.length,2);
  assert.match(items[0].href,/event-groups/);assert.equal(items[1].href,'/corpus/events/c/');
});
test('stale artifacts cannot attach groups to changed descriptions',()=>{
  const changed={...graph,events:[{...graph.events[0],description:'Changed'},...graph.events.slice(1)]};
  const g=attachEventGroups(changed,artifact);assert.equal(g.groupingStale,true);assert.equal(g.eventGroups,undefined);
});
test('unknown or repeated membership is rejected',()=>{
  assert.throws(()=>attachEventGroups(graph,{...artifact,groups:[...artifact.groups,...artifact.groups]}),/overlapping/);
});
