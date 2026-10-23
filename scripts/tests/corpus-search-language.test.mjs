import test from 'node:test';
import assert from 'node:assert/strict';
import {gunzipSync} from 'node:zlib';
import * as pagefind from 'pagefind';
import {corpusIndexLanguage, transcriptLanguage} from '../corpus-index-language.mjs';

test('unknown transcript language shares the English search partition without relabeling source metadata', async () => {
  const {index,errors}=await pagefind.createIndex();
  assert.deepEqual(errors,[]);
  try {
    for(const [id,source] of [['existing','en'],['torrent','und']]) {
      const response=await index.addCustomRecord({
        url:`/corpus/videos/${id}/`,content:`${id} transcript search test`,
        language:corpusIndexLanguage(source),
        meta:{title:id,transcript_language:transcriptLanguage(source)},
        filters:{language:[transcriptLanguage(source)]},
      });
      assert.deepEqual(response.errors,[]);
    }
    const {files,errors:writeErrors}=await index.getFiles();
    assert.deepEqual(writeErrors,[]);
    const entry=JSON.parse(Buffer.from(files.find(f=>f.path==='pagefind-entry.json').content));
    assert.deepEqual(Object.keys(entry.languages),['en']);
    assert.equal(entry.languages.en.page_count,2);
    const fragments=files.filter(f=>f.path.startsWith('fragment/')).map(f=>{
      const bytes=gunzipSync(Buffer.from(f.content));
      return JSON.parse(bytes.toString('utf8').replace(/^pagefind_dcd/,'').replace(/^\0+/,''));
    });
    const torrent=fragments.find(f=>f.meta?.title==='torrent');
    assert.equal(torrent.meta.transcript_language,'und');
    assert.deepEqual(torrent.filters.language,['und']);
  } finally {
    await index.deleteIndex();await pagefind.close();
  }
});

test('known language tags keep their actual partition and invalid tags never invent two-letter codes',()=>{
  assert.equal(corpusIndexLanguage('EN_gb'),'en');
  assert.equal(corpusIndexLanguage('fr-CA'),'fr');
  assert.equal(corpusIndexLanguage('und'),'en');
  assert.equal(corpusIndexLanguage(''),'en');
  assert.equal(corpusIndexLanguage(undefined),'en');
  assert.equal(transcriptLanguage('und'),'und');
  assert.equal(transcriptLanguage(undefined),'und');
});
