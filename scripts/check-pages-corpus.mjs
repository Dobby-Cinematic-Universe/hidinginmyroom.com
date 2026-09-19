// Fail closed before a Pages deployment can replace the archive with placeholders.
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

export function validatePagesCorpus(corpus,summaries){
  if(!(corpus?.counts?.recordings>0)||!(corpus?.counts?.transcript_revisions>0)
      ||!Array.isArray(summaries?.summaries)||!summaries.summaries.some(s=>s.kind==='transcript')){
    throw Error('Pages release blocked: corpus or summaries are empty placeholders. Use the approved populated release artifact; do not deploy a source-only build.');
  }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
  const root=process.cwd();
  const corpus=JSON.parse(await readFile(path.join(root,'src/data/corpus/manifest.json'),'utf8'));
  const summaries=JSON.parse(await readFile(path.join(root,'src/data/summaries/release.json'),'utf8'));
  validatePagesCorpus(corpus,summaries);
  console.log('Pages corpus admission passed.');
}
