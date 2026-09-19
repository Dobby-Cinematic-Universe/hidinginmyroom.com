import test from 'node:test';
import assert from 'node:assert/strict';
import {resolveCatalogSource} from '../summary-source-mapping.mjs';
test('exact title/platform ID and date resolve one catalog recording',()=>{
  const source={title:'[2023-02-03] dirty floor cooking [mfvEeYhkGKs]',date:{value:'2023-02-03'}};
  const record={recording_id:'rec_example',title:source.title,date_label:source.date.value};
  assert.equal(resolveCatalogSource(source,[record]),'rec_example');
  assert.equal(resolveCatalogSource(source,[record,{...record,recording_id:'rec_other'}]),null);
  assert.equal(resolveCatalogSource(source,[{...record,date_label:'2023-02-04'}]),null);
  assert.equal(resolveCatalogSource({...source,title:'dirty floor cooking'},[{...record,title:'dirty floor cooking'}]),null);
});
