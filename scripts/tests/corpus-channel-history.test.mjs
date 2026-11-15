import test from 'node:test';
import assert from 'node:assert/strict';
import {projectChannelHistory} from '../../src/lib/corpus/public-channel-history.mjs';

const page='https://web.archive.org/web/20161002234930/https://www.youtube.com/channel/UC_x';
const about='https://web.archive.org/web/20210917120000/https://www.youtube.com/channel/UC_x/about';
const stats='https://web.archive.org/web/20201122120446/https://socialblade.com/youtube/channel/UC_x/monthly';
const base={key:'main',label:'Main channel',readings:[{date:'2016-10-02',subscribers:328,rounded_to:1,source:page},{date:'2020-01-05',subscribers:101000,rounded_to:1000,source:page}],names:[]};
const project=c=>projectChannelHistory({channels:[{...base,...c}],early_views:[]}).channels[0];

test('a sidecar without statistics-site values still projects, with empty additions',()=>{
  const out=project({});
  assert.equal(out.readings.length,2);assert.deepEqual(out.monthly,[]);assert.deepEqual(out.view_readings,[]);assert.equal(out.monthly_views_uncertainty,null);
  assert.deepEqual(projectChannelHistory(null),{schema_version:1,channels:[],early_views:[]});
});

test('month-end values list the archived statistics pages they come from',()=>{
  const out=project({monthly:[{date:'2018-01-31',subscribers:8900,rounded_to:1,views:1280000,sources:[stats]},{date:'2018-02-28',subscribers:null,rounded_to:null,views:2179000,sources:[stats,'https://web.archive.org/web/20210118061740/https://socialblade.com/youtube/c/name']}],monthly_views_uncertainty:7283});
  assert.deepEqual(out.monthly[0],{date:'2018-01-31',subscribers:8900,rounded_to:1,views:1280000,sources:[stats]});
  assert.equal(out.monthly[1].subscribers,null);assert.equal(out.monthly[1].rounded_to,null);assert.equal(out.monthly_views_uncertainty,7283);
  // Sources must be archived pages of the statistics site; a live address, another site or a stray field is refused.
  assert.throws(()=>project({monthly:[{date:'2018-01-31',subscribers:1,rounded_to:1,views:1,sources:['https://socialblade.com/youtube/channel/UC_x']}]}),/Invalid month-end statistics row/);
  assert.throws(()=>project({monthly:[{date:'2018-01-31',subscribers:1,rounded_to:1,views:1,sources:[page]}]}),/Invalid month-end statistics row/);
  assert.throws(()=>project({monthly:[{date:'2018-01-31',subscribers:1,rounded_to:1,views:1,sources:[]}]}),/Invalid month-end statistics row/);
  assert.throws(()=>project({monthly:[{date:'2018-01-31',subscribers:null,rounded_to:null,views:null,sources:[stats]}]}),/Invalid month-end statistics row/);
  assert.throws(()=>project({monthly:[{date:'2018-02-28',subscribers:1,rounded_to:1,views:1,sources:[stats]},{date:'2018-01-31',subscribers:1,rounded_to:1,views:1,sources:[stats]}]}),/date order/);
  assert.equal('note' in project({monthly:[{date:'2018-01-31',subscribers:1,rounded_to:1,views:1,sources:[stats],note:'/mnt/archive'}]}).monthly[0],false);
});

test('total-view readings come from archived pages of the channel itself',()=>{
  const out=project({view_readings:[{date:'2021-09-17',views:9685783,source:about}]});
  assert.deepEqual(out.view_readings,[{date:'2021-09-17',views:9685783,source:about}]);
  assert.throws(()=>project({view_readings:[{date:'2021-09-17',views:9685783,source:stats}]}),/Invalid total-view reading/);
  assert.throws(()=>project({view_readings:[{date:'2021-09-17',views:-1,source:about}]}),/Invalid total-view reading/);
  assert.throws(()=>project({view_readings:[{date:'2021-09-17',views:1,source:about},{date:'2021-01-01',views:2,source:about}]}),/date order/);
});
