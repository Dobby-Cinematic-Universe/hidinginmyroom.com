// Heatmaps keep native scrolling and real layout extents. Only display size changes;
// cell geometry, values, ordering and selection remain owned by the renderer.
export function heatmapScale(value) {
  if (!Number.isFinite(value) || value <= 0) throw new TypeError('Invalid heatmap scale');
  return Math.min(3, Math.max(.2, value));
}

export function heatmapFitScale(width, availableWidth) {
  if (![width, availableWidth].every(value => Number.isFinite(value) && value > 0)) throw new TypeError('Invalid heatmap width');
  return heatmapScale(Math.min(1, availableWidth / width));
}

export function attachHeatmapZoom({viewport, content, controls, width, height, label = 'Heatmap'}) {
  if (!viewport || !content || !controls || !Number.isFinite(width) || width <= 0 ||
      (height !== undefined && (!Number.isFinite(height) || height <= 0))) throw new TypeError('Invalid heatmap zoom configuration');
  let scale = 1;
  const listeners = [];
  const out = controls.querySelector('[data-heatmap-zoom-out]');
  const inside = controls.querySelector('[data-heatmap-zoom-in]');
  const level = controls.querySelector('[data-heatmap-zoom-level]');
  viewport.setAttribute('tabindex', '0');
  viewport.setAttribute('role', 'region');
  viewport.setAttribute('aria-label', `${label}. Scroll to inspect; plus and minus resize, F fits width, 0 resets.`);
  function apply(next, resetPosition = false) {
    const centerX = (viewport.scrollLeft + viewport.clientWidth / 2) / scale;
    const centerY = (viewport.scrollTop + viewport.clientHeight / 2) / scale;
    scale = heatmapScale(next);
    // Explicit width/height, not a transform: browser scroll bounds match the image.
    content.style.setProperty('width', `${width * scale}px`, 'important');
    content.style.setProperty('max-width', 'none', 'important');
    if (height !== undefined) content.style.setProperty('height', `${height * scale}px`, 'important');
    viewport.scrollLeft = resetPosition ? 0 : Math.max(0, centerX * scale - viewport.clientWidth / 2);
    viewport.scrollTop = resetPosition ? 0 : Math.max(0, centerY * scale - viewport.clientHeight / 2);
    if (level) level.textContent = `${Math.round(scale * 100)}%`;
    if (out) out.disabled = scale <= .2;
    if (inside) inside.disabled = scale >= 3;
    viewport.dataset.heatmapScale = String(scale);
  }
  const fit = () => apply(heatmapFitScale(width, Math.max(1, viewport.clientWidth)), true);
  const reset = () => apply(1, true);
  function listen(element, type, callback) {
    if (!element) return;
    element.addEventListener(type, callback); listeners.push(() => element.removeEventListener(type, callback));
  }
  listen(out, 'click', () => apply(scale / 1.25));
  listen(inside, 'click', () => apply(scale * 1.25));
  listen(controls.querySelector('[data-heatmap-zoom-fit]'), 'click', fit);
  listen(controls.querySelector('[data-heatmap-zoom-reset]'), 'click', reset);
  listen(viewport, 'keydown', event => {
    if (event.target !== viewport || event.ctrlKey || event.altKey || event.metaKey) return;
    if (event.key === '+' || event.key === '=') apply(scale * 1.25);
    else if (event.key === '-' || event.key === '_') apply(scale / 1.25);
    else if (event.key === '0') reset();
    else if (event.key.toLowerCase() === 'f') fit();
    else return; // Arrow/Page keys and wheel scrolling remain entirely native.
    event.preventDefault();
  });
  apply(1, true);
  return {setScale: value => apply(value), fit, reset, getScale: () => scale, destroy: () => listeners.forEach(remove => remove())};
}
