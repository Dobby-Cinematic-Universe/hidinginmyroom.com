import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const code=readFileSync(new URL('../speaker_review_ui/app.js',import.meta.url),'utf8').split('const $=')[0];
const context=vm.createContext({});vm.runInContext(code,context);
const fixture=()=>({segments:[{speaker:'A'},{speaker:'A'},{speaker:'B'}],decisions:[],confirmed:{confirmed_mappings:[{label:'A',name:'Daniel'}]}});
const decision=(scope,name,index=0,source='participant')=>({decision:{label:'A',scope,name,segment_index:index,source,notes:''}});
test('confirmed names display while unknown labels remain anonymous',()=>{const d=fixture();assert.equal(context.speakerDisplay(d,0),'Daniel · A');assert.equal(context.speakerDisplay(d,2),'B');});
test('saved label names apply to all matching turns, latest wins',()=>{const d=fixture();d.decisions=[decision('label','First'),decision('label','Updated')];assert.equal(context.speakerDisplay(d,0),'Updated · A');assert.equal(context.speakerDisplay(d,1),'Updated · A');});
test('segment overrides label, including explicit uncertain clearing',()=>{const d=fixture();d.decisions=[decision('segment','',0,'uncertain'),decision('label','Daniel')];assert.equal(context.speakerDisplay(d,0),'Uncertain · A');assert.equal(context.speakerDisplay(d,1),'Daniel · A');});
test('combined playback and background noise category is not a named participant',()=>{const d=fixture();d.decisions=[decision('label','',0,'playback')];assert.equal(context.speakerDisplay(d,0),'Playback / game audio / background noise · A');});
test('audio source buttons synchronize value, highlight, and name field',()=>{
 const buttons=['participant','uncertain','playback','tts'].map(source=>({dataset:{source},attributes:{},setAttribute(k,v){this.attributes[k]=v;}}));
 const elements={source:{value:'participant'},name:{value:'Daniel',disabled:false},sourceButtons:{querySelectorAll:()=>buttons}};
 context.$=id=>elements[id];
 for(const source of ['playback','tts','uncertain','participant']){
  context.setAudioSource(source);
  assert.equal(elements.source.value,source);
  assert.equal(elements.name.disabled,source!=='participant');
  assert.equal(elements.name.value,'');
  assert.deepEqual(buttons.filter(b=>b.attributes['aria-pressed']==='true').map(b=>b.dataset.source),[source]);
 }
 elements.name.value='Sabrina';context.setAudioSource('participant');assert.equal(elements.name.value,'Sabrina');
});
test('scope buttons preserve label and segment values',()=>{
 const buttons=['label','segment'].map(scope=>({dataset:{scope},attributes:{},setAttribute(k,v){this.attributes[k]=v;}}));
 const elements={scope:{value:''},scopeButtons:{querySelectorAll:()=>buttons}};context.$=id=>elements[id];
 for(const scope of ['segment','label']){context.setReviewScope(scope);assert.equal(elements.scope.value,scope);assert.deepEqual(buttons.filter(b=>b.attributes['aria-pressed']==='true').map(b=>b.dataset.scope),[scope]);}
});
test('auto-save waits for participant names but permits nonparticipant categories',()=>{
 assert.equal(context.autoSaveReady({source:'participant',name:'  '}),false);
 assert.equal(context.autoSaveReady({source:'participant',name:'Daniel'}),true);
 for(const source of ['tts','playback','uncertain'])assert.equal(context.autoSaveReady({source,name:''}),true);
});
test('speaker navigation skips other labels and stops at boundaries',()=>{
 const segments=['A','B','A','C','A'].map((speaker,i)=>({speaker,start_ms:i*1000}));
 assert.equal(context.speakerTurn(segments,'A',0,1,0),2);
 assert.equal(context.speakerTurn(segments,'A',4,-1,0),2);
 assert.equal(context.speakerTurn(segments,'A',4,1,0),-1);
 assert.equal(context.speakerTurn(segments,'A',0,-1,0),-1);
 assert.equal(context.speakerTurn(segments,'B',0,1,0),1);
 assert.equal(context.speakerTurn(segments,'missing',-1,1,0),-1);
 assert.equal(context.speakerTurn(segments,'A',-1,1,1500),2);
 assert.equal(context.speakerTurn(segments,'A',-1,-1,1500),0);
});
