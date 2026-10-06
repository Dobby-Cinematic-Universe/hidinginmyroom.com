import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../../src/components/corpus/AnalysisExplorer.astro', import.meta.url), 'utf8');

test('question MDS uses a separate viewing camera and preserves saved point geometry', () => {
  assert.match(source, /attachMapViewport\(\{svg,layer:cameraLayer/);
  assert.match(source, /Repel only text labels: the MDS point coordinates remain untouched/);
  assert.match(source, /scale=Math\.min\(650\/\(xmax-xmin\|\|1\),500\/\(ymax-ymin\|\|1\)\)/);
  assert.match(source, /const px=X\(p\.x\),py=Y\(p\.y\)/);
  assert.match(source, /cameraLayer\.append\(g\)/);
  assert.match(source, /question-map-frame svg\[data-mds\]\{overflow:hidden/);
  assert.doesNotMatch(source, /p\.(?:x|y)\s*=/);
});

test('MDS retains keyboard selection and reveals clipped keyboard targets through camera movement', () => {
  assert.match(source, /role="group" tabindex="0"/);
  assert.match(source, /aria-describedby="question-map-help"/);
  assert.match(source, /e\.key==='Enter'\|\|e\.key===' '/);
  assert.match(source, /group\.addEventListener\('focus'/);
  assert.match(source, /viewport\.getCamera\(\)/);
  assert.match(source, /viewport\.setCamera\(\{\.\.\.camera/);
  assert.match(source, /Ctrl or Alt \+ scroll to zoom/);
  for (const action of ['in','out','fit','reset']) assert.ok(source.includes(`data-map-action="${action}"`));
});

test('responsive MDS labels grow with zoom until readable rather than staying tiny on mobile', () => {
  assert.match(source, /responsiveRatio=Math\.min\(rect\.width\/760,rect\.height\/600\)/);
  assert.match(source, /denominator=Math\.max\(1,camera\.scale\*responsiveRatio\)/);
  assert.match(source, /'font-size',String\(12\/denominator\)/);
  assert.match(source, /'r',String\(8\/denominator\)/);
  assert.match(source, /new ResizeObserver\(\(\)=>viewport\.setCamera\(viewport\.getCamera\(\)\)\)/);
  assert.doesNotMatch(source, /'font-size',String\(12\/camera\.scale\)/);
  const fontPixels=(scale,width)=>12/Math.max(1,scale*width/760)*scale*width/760;
  assert.ok(fontPixels(2,301.8)>fontPixels(1,301.8));
  assert.equal(fontPixels(4,301.8),12);
  assert.equal(fontPixels(12,760),12);
});

test('full correlation matrix remains lazy and uses real cell sizing, not transformed clipping', () => {
  assert.match(source, /correlationDetails\.open&&correlationDetails\.dataset\.rendered!=='true'/);
  assert.match(source, /matrixScale=Math\.max\(\.75,Math\.min\(2,next\)\)/);
  assert.match(source, /--correlation-scale/);
  assert.match(source, /min-width:calc\(4rem \* var\(--correlation-scale\)\)/);
  assert.match(source, /min-height:2\.75rem/);
  assert.match(source, /correlation-scroll.*max-height:36rem/);
  assert.match(source, /aria-label="Scrollable full correlation matrix"/);
  assert.match(source, /button\.setAttribute\('aria-label',`\$\{q\.short_name\} and \$\{other\.short_name\}: correlation/);
  assert.doesNotMatch(source, /correlation-scroll[^}]*transform:/);
});
