// A viewing camera only: never writes to statistical coordinates or scores.
export function normalizeCamera(camera = {}, minScale = .5, maxScale = 12) {
  return { x: Number.isFinite(camera.x) ? camera.x : 0, y: Number.isFinite(camera.y) ? camera.y : 0,
    scale: Math.max(minScale, Math.min(maxScale, Number.isFinite(camera.scale) ? camera.scale : 1)) };
}
export function zoomCamera(camera, multiplier, anchor, minScale = .5, maxScale = 12) {
  const before = normalizeCamera(camera, minScale, maxScale);
  if (!Number.isFinite(multiplier) || multiplier <= 0 || !Number.isFinite(anchor?.x) || !Number.isFinite(anchor?.y)) return before;
  const scale = Math.max(minScale, Math.min(maxScale, before.scale * multiplier)), ratio = scale / before.scale;
  return { x: anchor.x - (anchor.x - before.x) * ratio, y: anchor.y - (anchor.y - before.y) * ratio, scale };
}
export function panCamera(camera, dx, dy) {
  return { ...camera, x: camera.x + (Number.isFinite(dx) ? dx : 0), y: camera.y + (Number.isFinite(dy) ? dy : 0) };
}
export const cameraTransform = camera => `translate(${camera.x} ${camera.y}) scale(${camera.scale})`;

/**
 * @param {{svg: SVGSVGElement, layer: SVGGElement, controls?: Element|null,
 * width:number, height:number, initial?:{x:number,y:number,scale:number},
 * onChange?:(camera:{x:number,y:number,scale:number})=>void, minScale?:number, maxScale?:number,
 * requestFrame?:(callback:()=>void)=>number, cancelFrame?:(id:number)=>void}} options
 */
export function attachMapViewport({ svg, layer, controls, width, height, initial, onChange = () => {}, minScale = .5, maxScale = 12, requestFrame, cancelFrame }) {
  if (!svg || !layer || !Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) throw new TypeError('A valid SVG viewport is required.');
  if (!Number.isFinite(minScale) || !Number.isFinite(maxScale) || minScale <= 0 || maxScale < minScale) throw new TypeError('Valid positive camera limits are required.');
  const frameHost = svg.ownerDocument?.defaultView ?? globalThis;
  const request = requestFrame ?? (callback => typeof frameHost.requestAnimationFrame === 'function' ? frameHost.requestAnimationFrame(callback) : setTimeout(callback, 16));
  const cancel = cancelFrame ?? (id => typeof frameHost.cancelAnimationFrame === 'function' ? frameHost.cancelAnimationFrame(id) : clearTimeout(id));
  let camera = normalizeCamera(initial, minScale, maxScale), pointer = null, suppressClick = false, timer = null, destroyed = false, frame = null, dirty = false, generation = 0;
  const removers = [];
  const listen = (node, type, fn, options) => { node?.addEventListener(type, fn, options); removers.push(() => node?.removeEventListener(type, fn, options)); };
  const local = event => {
    const rect = svg.getBoundingClientRect();
    // The responsive SVG preserves aspect ratio; account for letterboxing.
    const ratio = Math.min(rect.width / width, rect.height / height), ox = (rect.width - width * ratio) / 2, oy = (rect.height - height * ratio) / 2;
    return ratio > 0 ? { x: (event.clientX - rect.left - ox) / ratio, y: (event.clientY - rect.top - oy) / ratio } : null;
  };
  const apply = () => {
    if (destroyed) return;
    // Snapshot before notifying: consumers may request another camera update.
    const rendered = { ...camera };
    layer.setAttribute('transform', cameraTransform(rendered));
    const status = controls?.querySelector('[data-map-zoom]');
    if (status) status.textContent = `${Math.round(rendered.scale * 100)}%`;
    svg.dataset.mapZoom = String(rendered.scale);
    onChange(rendered);
  };
  const schedule = () => {
    if (frame !== null) return;
    const scheduled = ++generation;
    frame = request(() => { if (scheduled !== generation) return; frame = null; if (destroyed || !dirty) return; dirty = false; apply(); });
  };
  // Logical state is immediate; DOM writes and expensive consumers are frame-batched.
  // Explicit same-camera updates still notify consumers (for responsive resizing).
  const setCamera = next => { if (destroyed) return; camera = normalizeCamera(next, minScale, maxScale); dirty = true; schedule(); };
  const flush = () => {
    if (destroyed) return;
    if (frame !== null) { ++generation; cancel(frame); frame = null; }
    if (dirty) { dirty = false; apply(); }
  };
  const reset = () => setCamera({ x: 0, y: 0, scale: 1 });
  const zoom = (multiplier, anchor = { x: width / 2, y: height / 2 }) => setCamera(zoomCamera(camera, multiplier, anchor, minScale, maxScale));
  listen(svg, 'wheel', event => {
    if (!(event.ctrlKey || event.altKey)) return;
    const anchor = local(event); if (!anchor) return;
    event.preventDefault(); zoom(Math.exp(-Math.max(-100, Math.min(100, event.deltaY)) * .006), anchor);
  }, { passive: false });
  listen(svg, 'pointerdown', event => {
    if (event.button !== 0 || pointer) return;
    const point = local(event); if (!point) return;
    pointer = { id: event.pointerId, start: point, last: point, moved: false, captured: false };
  });
  listen(svg, 'pointermove', event => {
    if (!pointer || pointer.id !== event.pointerId) return;
    const point = local(event); if (!point) return;
    if (!pointer.moved && Math.hypot(point.x-pointer.start.x, point.y-pointer.start.y) < 4) return;
    if (!pointer.moved) {
      pointer.moved = true;
      try { svg.setPointerCapture(pointer.id); pointer.captured = true; } catch { /* No capture available: document release still cleans up. */ }
    }
    event.preventDefault();
    setCamera(panCamera(camera, point.x-pointer.last.x, point.y-pointer.last.y)); pointer.last = point;
  });
  const release = event => {
    if (!pointer || pointer.id !== event.pointerId) return;
    if (pointer.moved) {
      suppressClick = true; clearTimeout(timer); timer = setTimeout(() => { suppressClick = false; }, 250);
    }
    const ended = pointer; pointer = null;
    if (ended.captured) try { svg.releasePointerCapture(ended.id); } catch { /* Already released. */ }
  };
  listen(svg.ownerDocument ?? svg, 'pointerup', release);
  listen(svg.ownerDocument ?? svg, 'pointercancel', release);
  listen(svg, 'lostpointercapture', event => { if (pointer?.id === event.pointerId) release(event); });
  listen(svg, 'click', event => { if (suppressClick) { event.preventDefault(); event.stopImmediatePropagation(); suppressClick = false; } }, true);
  listen(svg, 'keydown', event => {
    // Preserve point-specific selection and keyboard semantics.
    if (event.target !== svg) return;
    const moves = { ArrowLeft: [30,0], ArrowRight: [-30,0], ArrowUp: [0,30], ArrowDown: [0,-30] };
    if (moves[event.key]) { event.preventDefault(); setCamera(panCamera(camera, ...moves[event.key])); }
    else if (event.key === '+' || event.key === '=') { event.preventDefault(); zoom(1.25); }
    else if (event.key === '-' || event.key === '_') { event.preventDefault(); zoom(.8); }
    else if (event.key === '0' || event.key === 'Home') { event.preventDefault(); reset(); }
  });
  for (const button of controls?.querySelectorAll('[data-map-action]') ?? []) listen(button, 'click', () => {
    if (button.dataset.mapAction === 'in') zoom(1.25);
    else if (button.dataset.mapAction === 'out') zoom(.8);
    else if (['fit','reset'].includes(button.dataset.mapAction)) reset();
  });
  apply();
  return { getCamera: () => ({ ...camera }), setCamera, reset, flush, destroy() {
    if (destroyed) return;
    // Preserve the final camera in consumer per-projection memory before detaching.
    flush();
    const ended = pointer; pointer = null; destroyed = true;
    if (frame !== null) { ++generation; cancel(frame); frame = null; } dirty = false;
    if (ended?.captured) try { svg.releasePointerCapture(ended.id); } catch { /* Already released. */ }
    clearTimeout(timer); removers.forEach(remove => remove());
  } };
}
