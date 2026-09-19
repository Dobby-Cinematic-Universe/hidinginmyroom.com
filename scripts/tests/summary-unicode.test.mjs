import test from 'node:test';
import assert from 'node:assert/strict';
import {summaryUnicode} from '../../src/lib/summaries/unicode.mjs';
test('decodes escaped accents, currency, punctuation and paired emoji',()=>{
  assert.equal(summaryUnicode(String.raw`Pok\u00e9mon costs \u00a35\u2014\ud83d\ude00`),'Pokémon costs £5—😀');
});
test('preserves real Unicode, controls, HTML escapes and malformed surrogates',()=>{
  const text=String.raw`日本 £ \u000a \u202e \u003cscript\u003e \ud800 \n`;
  assert.equal(summaryUnicode(text),text);
});
