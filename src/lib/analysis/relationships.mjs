import { attachHeatmapZoom } from './heatmap-zoom.mjs';

const stable = (value) => Array.isArray(value)
  ? `[${value.map(stable).join(',')}]`
  : value && typeof value === 'object'
    ? `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`
    : JSON.stringify(value);

export function correlationSignature(release) {
  const analysis = release.analysis;
  const scaled = analysis.correlations.map((row) => row.map((value) => value == null ? null : Math.floor(Math.abs(value) * 1e8 + 0.5) * (value < 0 ? -1 : 1)));
  return stable({
    question_ids: analysis.question_ids,
    correlations_scaled: scaled,
    pair_counts: analysis.pair_counts,
    mds_method: analysis.mds_method,
  });
}

export async function validateMapDiagnostics(release, candidate) {
  if (!candidate || candidate.schema_version !== 1) return null;
  for (const key of ['corpus_release_id', 'questionnaire_version', 'model']) {
    if (candidate[key] !== release[key]) return null;
  }
  if (candidate.source_generated_at !== release.generated_at || candidate.cohort_n !== release.analysis.n) return null;
  if (stable(candidate.question_ids) !== stable(release.analysis.question_ids)) return null;
  if (candidate.correlation_signature !== await sha256(correlationSignature(release))) return null;
  const ids = new Set(release.analysis.question_ids);
  if (!Array.isArray(candidate.clustering?.leaf_order) || candidate.clustering.leaf_order.length !== ids.size ||
      candidate.clustering.leaf_order.some((id) => !ids.has(id)) || new Set(candidate.clustering.leaf_order).size !== ids.size) return null;
  if (!Array.isArray(candidate.stress_curve?.points) || !candidate.stress_curve.points.every((p, i) =>
    p.dimensions === i + 1 && Number.isFinite(p.normalized_rms_error) && p.normalized_rms_error >= 0)) return null;
  const stress2 = candidate.stress_curve.points.find((p) => p.dimensions === 2);
  if (stress2 && release.analysis.mds_stress != null && Math.abs(stress2.normalized_rms_error - release.analysis.mds_stress) > 1e-5) return null;
  if (!Array.isArray(candidate.shepard?.points) || candidate.shepard.points.length !== (ids.size * (ids.size - 1)) / 2) return null;
  const mapPoints = new Map(release.analysis.mds.map((p) => [p.id, p]));
  const seen = new Set();
  for (const pair of candidate.shepard.points) {
    if (!ids.has(pair.a) || !ids.has(pair.b) || pair.a === pair.b || ![pair.correlation, pair.chord_distance, pair.map_distance].every(Number.isFinite) || !Number.isInteger(pair.n) || pair.n < 0) return null;
    const i = release.analysis.question_ids.indexOf(pair.a), j = release.analysis.question_ids.indexOf(pair.b);
    const key = i < j ? `${i}:${j}` : `${j}:${i}`;
    if (seen.has(key)) return null;
    seen.add(key);
    const correlation = release.analysis.correlations[i]?.[j];
    if (correlation == null || Math.abs(pair.correlation - correlation) > 1e-8 ||
        pair.n !== release.analysis.pair_counts[i]?.[j] ||
        Math.abs(pair.chord_distance - Math.sqrt(Math.max(2 * (1 - correlation), 0))) > 1e-8) return null;
    const a = mapPoints.get(pair.a), b = mapPoints.get(pair.b);
    if (!a || !b || Math.abs(pair.map_distance - Math.hypot(a.x - b.x, a.y - b.y)) > 1e-8) return null;
  }
  return candidate;
}

async function sha256(value) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

export function rankedRelationships(release, questionId) {
  const index = release.analysis.question_ids.indexOf(questionId);
  if (index < 0) return [];
  return release.analysis.question_ids.flatMap((id, otherIndex) => {
    if (index === otherIndex) return [];
    const correlation = release.analysis.correlations[index]?.[otherIndex] ?? null;
    if (correlation == null) return [];
    return [{ id, correlation, n: release.analysis.pair_counts[index]?.[otherIndex] ?? 0 }];
  }).sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation) || a.id.localeCompare(b.id));
}

export function correlationColor(value) {
  if (value == null) return '#808080';
  const amount = Math.round(15 + Math.abs(value) * 75);
  return value < 0 ? `hsl(3 68% ${100 - amount / 1.8}%)` : `hsl(204 68% ${100 - amount / 1.8}%)`;
}

export function renderQuestionRelationships(root, release, diagnostics = null) {
  const questions = new Map(release.questions.map((question) => [question.id, question]));
  const ids = release.analysis.question_ids;
  const select = root.querySelector('[data-relationship-question]');
  const summary = root.querySelector('[data-relationship-summary]');
  const ranks = root.querySelector('[data-relationship-ranks]');
  const allPairs = root.querySelector('[data-relationship-all-pairs-table]');
  const matrix = root.querySelector('[data-relationship-matrix]');
  const mdsPane = root.querySelector('[data-relationship-diagnostics]');
  if (!select || !summary || !ranks || !allPairs || !matrix || !mdsPane) return;
  select.replaceChildren(...ids.map((id) => {
    const option = document.createElement('option');
    option.value = id;
    option.textContent = questions.get(id)?.short_name ?? id;
    return option;
  }));
  const label = (id) => questions.get(id)?.short_name ?? id;
  function showRanks() {
    const id = select.value;
    const neighbors = rankedRelationships(release, id);
    const strongPositive = [...neighbors].filter((item) => item.correlation > 0).sort((a, b) => b.correlation - a.correlation).slice(0, 5);
    const strongNegative = [...neighbors].filter((item) => item.correlation < 0).sort((a, b) => a.correlation - b.correlation).slice(0, 5);
    const list = document.createElement('div');
    for (const [heading, values] of [['Most positively related', strongPositive], ['Most negatively related', strongNegative]]) {
      const section = document.createElement('section');
      const h = document.createElement('h4'); h.textContent = heading; section.append(h);
      if (!values.length) { const p = document.createElement('p'); p.textContent = 'No estimated pairs.'; section.append(p); }
      else {
        const ul = document.createElement('ul');
        for (const item of values) { const li = document.createElement('li'); li.textContent = `${label(item.id)} · r = ${item.correlation.toFixed(3)} · paired n = ${item.n.toLocaleString()}`; ul.append(li); }
        section.append(ul);
      }
      list.append(section);
    }
    ranks.replaceChildren(list);
    const table = document.createElement('table');
    const caption = document.createElement('caption'); caption.textContent = `All estimated pairs for ${label(id)}. Rows remain in the fixed analyzed cohort.`;
    const thead = document.createElement('thead'), head = document.createElement('tr');
    for (const title of ['Question', 'Correlation', 'Paired sample size']) { const th = document.createElement('th'); th.scope = 'col'; th.textContent = title; head.append(th); }
    thead.append(head); const tbody = document.createElement('tbody');
    for (const item of neighbors) { const tr = document.createElement('tr'); for (const text of [label(item.id), item.correlation.toFixed(3), item.n.toLocaleString()]) { const td = document.createElement('td'); td.textContent = text; tr.append(td); } tbody.append(tr); }
    table.append(caption, thead, tbody); allPairs.replaceChildren(table);
    const question = questions.get(id);
    summary.textContent = `${question?.short_name ?? id} is compared with the same fixed ${release.analysis.n.toLocaleString()}-recording cohort used by the correlation release. Pair-specific n is shown for each estimate. This panel is not affected by year, category, or table filters.`;
  }
  select.addEventListener('change', showRanks);
  showRanks();

  const matrixDetails = root.querySelector('[data-matrix-details]');
  matrixDetails?.addEventListener('toggle', () => {
    if (!matrixDetails.open || matrix.dataset.drawn === 'yes') return;
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    const cell = Math.max(9, Math.min(15, 720 / ids.length));
    const left = 96, top = 120;
    const width = left + cell * ids.length + 8, height = top + cell * ids.length + 8;
    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    svg.setAttribute('width', String(width)); svg.setAttribute('height', String(height));
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', `${ids.length} by ${ids.length} signed correlation heatmap. Question order uses average-linkage clustering on chord distance.`);
    const order = diagnostics?.clustering?.leaf_order?.filter((id) => ids.includes(id)) ?? ids;
    const positions = order.map((id) => ids.indexOf(id));
    const namespace = 'http://www.w3.org/2000/svg';
    const columnLabels = [];
    for (const [position, qid] of order.entries()) {
      const text = document.createElementNS(namespace, 'text');
      text.setAttribute('x', String(left - 5)); text.setAttribute('y', String(top + position * cell + cell * .72));
      text.setAttribute('text-anchor', 'end'); text.setAttribute('font-size', String(Math.min(11, cell * .8)));
      text.textContent = label(qid); svg.append(text);
      const column = document.createElementNS(namespace, 'text');
      const columnX = left + position * cell + cell * .72;
      column.setAttribute('x', String(columnX)); column.setAttribute('y', String(top - 5));
      column.setAttribute('transform', `rotate(-60 ${columnX} ${top - 5})`);
      column.setAttribute('text-anchor', 'start'); column.setAttribute('font-size', String(Math.min(10, cell * .72)));
      column.textContent = label(qid); columnLabels.push(column);
    }
    for (let row = 0; row < order.length; row++) for (let col = 0; col < order.length; col++) {
      const i = positions[row], j = positions[col], value = release.analysis.correlations[i]?.[j] ?? null;
      const rect = document.createElementNS(namespace, 'rect');
      rect.setAttribute('x', String(left + col * cell)); rect.setAttribute('y', String(top + row * cell));
      rect.setAttribute('width', String(cell)); rect.setAttribute('height', String(cell));
      rect.setAttribute('fill', correlationColor(value)); rect.setAttribute('stroke', 'var(--corpus-bg)'); rect.setAttribute('stroke-width', '.5');
      const title = document.createElementNS(namespace, 'title');
      title.textContent = `${label(order[row])} × ${label(order[col])}: r ${value == null ? 'not estimated' : value.toFixed(3)}; n ${release.analysis.pair_counts[i]?.[j] ?? 0}`;
      rect.append(title); svg.append(rect);
    }
    svg.append(...columnLabels);
    const colorKey = document.createElementNS(namespace, 'text'); colorKey.setAttribute('x', String(left)); colorKey.setAttribute('y', '20'); colorKey.setAttribute('font-size', '12');
    colorKey.textContent = 'Blue = positive · red = negative · stronger color = larger |r|'; svg.append(colorKey);
    matrix.replaceChildren(svg); matrix.dataset.drawn = 'yes';
    attachHeatmapZoom({viewport:matrix,content:svg,controls:root.querySelector('[data-relationship-zoom]'),width,height,label:'Clustered correlation heatmap'});
  });

  const valid = diagnostics;
  if (!valid) {
    const unavailable = document.createElement('p');
    unavailable.textContent = 'Dimension and map-distance diagnostics are unavailable for this release. The correlation heatmap and rankings above use the release directly.';
    mdsPane.append(unavailable);
    return;
  }
  const curve = valid.stress_curve.points;
  const note = document.createElement('p');
  note.textContent = `${valid.stress_curve.method}. The curve describes geometric distance loss as dimensions are removed; lower is better. This is not a confidence interval or a validation of the question model.`;
  mdsPane.append(note);
  const curveSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  curveSvg.setAttribute('viewBox', '0 0 620 250'); curveSvg.setAttribute('role', 'img'); curveSvg.setAttribute('aria-label', 'Normalized RMS chord-distance error by MDS dimensions');
  drawLineChart(curveSvg, curve.map((p) => [p.dimensions, p.normalized_rms_error]), 620, 250, 'Dimensions', 'Normalized RMS error');
  mdsPane.append(curveSvg);
  const stressTable = document.createElement('table'); stressTable.className = 'diagnostic-table';
  const header = document.createElement('tr'); for (const value of ['Dimensions', 'Normalized RMS error']) { const th = document.createElement('th'); th.textContent = value; header.append(th); }
  const thead = document.createElement('thead'); thead.append(header); const tbody = document.createElement('tbody');
  for (const point of curve) { const row = document.createElement('tr'); for (const value of [point.dimensions, point.normalized_rms_error.toFixed(4)]) { const td = document.createElement('td'); td.textContent = String(value); row.append(td); } tbody.append(row); }
  stressTable.append(thead, tbody);
  const stressScroll = document.createElement('div'); stressScroll.className = 'table-scroll'; stressScroll.append(stressTable); mdsPane.append(stressScroll);

  const shepard = valid.shepard.points;
  const scatter = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  scatter.setAttribute('viewBox', '0 0 620 300'); scatter.setAttribute('role', 'img');
  scatter.setAttribute('aria-label', 'Shepard plot comparing original signed chord dissimilarity to distance in the two-dimensional map');
  const maximum = Math.max(...shepard.flatMap((p) => [p.chord_distance, p.map_distance]), 1);
  const left = 62, top = 16, innerW = 536, innerH = 230;
  const identity = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  identity.setAttribute('d', `M${left} ${top + innerH}L${left + innerW} ${top}`);
  identity.setAttribute('stroke', 'currentColor'); identity.setAttribute('stroke-dasharray', '5 4'); identity.setAttribute('opacity', '.65');
  scatter.append(identity);
  for (const pair of shepard) {
    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    circle.setAttribute('cx', String(left + pair.chord_distance / maximum * innerW));
    circle.setAttribute('cy', String(top + innerH - pair.map_distance / maximum * innerH));
    circle.setAttribute('r', '2.2'); circle.setAttribute('fill', 'var(--corpus-accent)'); circle.setAttribute('fill-opacity', '.45');
    const title = document.createElementNS('http://www.w3.org/2000/svg', 'title'); title.textContent = `${label(pair.a)} × ${label(pair.b)}: chord distance ${pair.chord_distance.toFixed(3)}, 2D map distance ${pair.map_distance.toFixed(3)}, r=${pair.correlation.toFixed(3)}, paired n=${pair.n}`;
    circle.append(title); scatter.append(circle);
  }
  axis(scatter, left, top, innerW, innerH, 'Original chord dissimilarity', 'Distance in 2D map', maximum, maximum);
  const shepardNote = document.createElement('p');
  shepardNote.textContent = `${valid.shepard.method}. Each point is one question pair; departures from the dashed equal-distance line show how the 2D map distorts the original distances. Both axes use the same numeric range. Hover a point for its correlation and paired sample size.`;
  mdsPane.append(shepardNote, scatter);
}

function drawLineChart(svg, points, width, height, xTitle, yTitle) {
  const maxX = Math.max(...points.map((p) => p[0]), 1), maxY = Math.max(...points.map((p) => p[1]), .01);
  const left = 62, top = 18, innerW = width - 82, innerH = height - 68;
  const namespace = 'http://www.w3.org/2000/svg';
  const poly = document.createElementNS(namespace, 'polyline');
  poly.setAttribute('fill', 'none'); poly.setAttribute('stroke', 'var(--corpus-accent)'); poly.setAttribute('stroke-width', '3');
  poly.setAttribute('points', points.map(([x, y]) => `${left + x / maxX * innerW},${top + innerH - y / maxY * innerH}`).join(' ')); svg.append(poly);
  for (const [x, y] of points) { const c = document.createElementNS(namespace, 'circle'); c.setAttribute('cx', String(left + x / maxX * innerW)); c.setAttribute('cy', String(top + innerH - y / maxY * innerH)); c.setAttribute('r', '4'); c.setAttribute('fill', 'var(--corpus-accent)'); const t = document.createElementNS(namespace, 'title'); t.textContent = `${x} dimensions: normalized RMS error ${y.toFixed(4)}`; c.append(t); svg.append(c); }
  axis(svg, left, top, innerW, innerH, xTitle, yTitle, maxX, maxY);
}

function axis(svg, left, top, innerW, innerH, xTitle, yTitle, maxX, maxY) {
  const namespace = 'http://www.w3.org/2000/svg';
  const path = document.createElementNS(namespace, 'path'); path.setAttribute('d', `M${left} ${top}V${top + innerH}H${left + innerW}`); path.setAttribute('fill', 'none'); path.setAttribute('stroke', 'currentColor'); svg.append(path);
  for (let step = 0; step <= 4; step++) {
    const fraction = step / 4;
    const xTick = document.createElementNS(namespace, 'text');
    xTick.setAttribute('x', String(left + fraction * innerW)); xTick.setAttribute('y', String(top + innerH + 15)); xTick.setAttribute('text-anchor', 'middle'); xTick.setAttribute('font-size', '10');
    xTick.textContent = Number((fraction * maxX).toFixed(2)).toString(); svg.append(xTick);
    const yTick = document.createElementNS(namespace, 'text');
    yTick.setAttribute('x', String(left - 7)); yTick.setAttribute('y', String(top + innerH - fraction * innerH + 3)); yTick.setAttribute('text-anchor', 'end'); yTick.setAttribute('font-size', '10');
    yTick.textContent = (fraction * maxY).toFixed(2); svg.append(yTick);
  }
  const x = document.createElementNS(namespace, 'text'); x.setAttribute('x', String(left + innerW / 2)); x.setAttribute('y', String(top + innerH + 34)); x.setAttribute('text-anchor', 'middle'); x.setAttribute('font-size', '12'); x.textContent = xTitle; svg.append(x);
  const y = document.createElementNS(namespace, 'text'); y.setAttribute('x', '14'); y.setAttribute('y', String(top + innerH / 2)); y.setAttribute('transform', `rotate(-90 14 ${top + innerH / 2})`); y.setAttribute('text-anchor', 'middle'); y.setAttribute('font-size', '12'); y.textContent = yTitle; svg.append(y);
}
