import { buildPassageFrame, findPassages, heldConstantTrend, passageYears, recordingArc, subjectProfile, subjectShares, topicContrast, topicMask, TOPIC_TYPES } from './passage-table.mjs';
import { matchedAlias, matchesAll, searchText, searchWords } from '../corpus/title-search.mjs';

// The passage explorer: four views on single passages (subjects, the course of one recording, topics, a finder).
// Everything is computed in the browser from the release's passage table.
const NS = 'http://www.w3.org/2000/svg', NONE = 255, KINDS = ['Videos', 'Streams'], COLORS = ['var(--pe-s1)', 'var(--pe-s2)'];
const h = (tag, attrs = {}, ...kids) => { const e = document.createElement(tag); for (const [k, v] of Object.entries(attrs)) { if (k === 'class') e.className = v; else if (k.startsWith('on')) e.addEventListener(k.slice(2), v); else if (v !== false && v != null) e.setAttribute(k, v === true ? '' : v); } for (const c of kids.flat()) if (c != null && c !== false) e.append(c); return e; };
const s = (tag, attrs = {}, ...kids) => { const e = document.createElementNS(NS, tag); for (const [k, v] of Object.entries(attrs)) if (v != null) e.setAttribute(k, v); for (const c of kids.flat()) if (c != null) e.append(c); return e; };
const signed = x => Number.isFinite(x) ? (x < 0 ? '−' : '+') + Math.abs(x).toFixed(2) : '—';
const percent = x => Number.isFinite(x) ? (100 * x).toFixed(x < 0.1 && x > 0 ? 1 : 0) + '%' : '—';
const count = x => x.toLocaleString('en-US');
const words = value => String(value).replace(/[_-]+/g, ' ').replace(/^./, c => c.toUpperCase());
const clock = seconds => { const hh = Math.floor(seconds / 3600), mm = Math.floor(seconds % 3600 / 60), ss = seconds % 60; return (hh ? hh + ':' + String(mm).padStart(2, '0') : String(mm)) + ':' + String(ss).padStart(2, '0'); };
const tint = v => Number.isFinite(v) ? `background:${v >= 0 ? 'rgba(57,135,229,' : 'rgba(217,89,38,'}${(Math.min(Math.abs(v) / 1.5, 1) * 0.5).toFixed(2)})` : '';

export function mountPassageExplorer(root, { dimensions, loadingsSha256 = null, endpoint = '/corpus/analysis/passage-table.json' }) {
  const status = root.querySelector('[data-status]'), tip = h('div', { class: 'pe-tip' }); root.append(tip);
  const showTip = (html, ev) => { tip.replaceChildren(...html); tip.style.display = 'block'; const r = tip.getBoundingClientRect(); tip.style.left = Math.min(ev.clientX + 14, innerWidth - r.width - 8) + 'px'; tip.style.top = Math.min(ev.clientY + 14, innerHeight - r.height - 8) + 'px'; };
  const hideTip = () => { tip.style.display = 'none'; };
  const tipLine = (color, label, value) => h('div', {}, color ? h('i', { style: `background:${color}` }) : null, label + ': ', h('b', {}, value));

  // ---------------------------------------------------------------- charts
  function lineChart({ series, xs, xfmt = x => String(x), yfmt = signed, ymin, ymax, w = 640, hgt = 250, compact = false, onClick, xTitle, label }) {
    const m = { l: compact ? 36 : 48, r: compact ? 10 : 22, t: 8, b: compact ? 20 : xTitle ? 38 : 26 }, iw = w - m.l - m.r, ih = hgt - m.t - m.b, vals = series.flatMap(q => q.values.filter(Number.isFinite));
    let lo = ymin ?? Math.min(...vals), hi = ymax ?? Math.max(...vals); if (!Number.isFinite(lo) || !Number.isFinite(hi)) { lo = 0; hi = 1; } if (hi === lo) hi = lo + 1;
    if (ymin == null) { const pad = (hi - lo) * 0.08; lo -= pad; hi += pad; }
    let ticks = [lo, (lo + hi) / 2, hi];
    if (!compact) { const span = hi - lo, step = [0.05, 0.1, 0.2, 0.25, 0.5, 1, 2, 5, 10, 20, 25, 50, 100, 200, 500].find(x => span / x <= 6) ?? span / 4; lo = Math.floor(lo / step + 1e-9) * step; hi = Math.ceil(hi / step - 1e-9) * step; ticks = []; for (let v = lo; v <= hi + step / 2; v += step) ticks.push(Math.abs(v) < 1e-9 ? 0 : v); }
    const X = i => m.l + (xs.length === 1 ? iw / 2 : i / (xs.length - 1) * iw), Y = v => m.t + (1 - (v - lo) / (hi - lo)) * ih, svg = s('svg', { viewBox: `0 0 ${w} ${hgt}`, role: 'img', 'aria-label': label, style: compact ? null : `max-width:${w}px` });
    for (const v of ticks) svg.append(s('line', { x1: m.l, x2: w - m.r, y1: Y(v), y2: Y(v), class: 'pe-grid' }), s('text', { x: m.l - 6, y: Y(v) + 4, 'text-anchor': 'end' }, yfmt(v)));
    if (lo < 0 && hi > 0) svg.append(s('line', { x1: m.l, x2: w - m.r, y1: Y(0), y2: Y(0), class: 'pe-zero' }));
    const step = Math.ceil(xs.length / (compact ? 4 : 12)); xs.forEach((x, i) => { if (i % step === 0) svg.append(s('text', { x: X(i), y: hgt - m.b + 14, 'text-anchor': 'middle' }, xfmt(x))); });
    if (xTitle) svg.append(s('text', { x: m.l + iw / 2, y: hgt - 4, 'text-anchor': 'middle' }, xTitle));
    for (const q of series) {
      let d = '', pen = false; q.values.forEach((v, i) => { if (Number.isFinite(v)) { d += (pen ? 'L' : 'M') + X(i).toFixed(1) + ' ' + Y(v).toFixed(1); pen = true; } else pen = false; });
      svg.append(s('path', { d, fill: 'none', stroke: q.color, 'stroke-width': q.thin ? 1 : 2, 'stroke-dasharray': q.dash ?? null, opacity: q.thin ? 0.55 : 1, 'stroke-linejoin': 'round' }));
      if (q.points !== false && xs.length <= 60) q.values.forEach((v, i) => { if (Number.isFinite(v)) svg.append(s('circle', { cx: X(i), cy: Y(v), r: compact ? 2.2 : 3.2, fill: q.hollow?.[i] ? 'var(--corpus-panel)' : q.color, stroke: q.hollow?.[i] ? q.color : 'var(--corpus-panel)', 'stroke-width': 1.5 })); });
    }
    const cross = s('line', { y1: m.t, y2: m.t + ih, class: 'pe-cross', opacity: 0 }); svg.append(cross);
    const at = ev => { const r = svg.getBoundingClientRect(), x = (ev.clientX - r.left) / r.width * w; return Math.max(0, Math.min(xs.length - 1, Math.round((x - m.l) / iw * (xs.length - 1)))); };
    svg.addEventListener('mousemove', ev => { const i = at(ev); cross.setAttribute('x1', X(i)); cross.setAttribute('x2', X(i)); cross.setAttribute('opacity', 0.7); showTip([h('b', {}, xfmt(xs[i])), ...series.filter(q => q.name).map(q => tipLine(q.color, q.name, yfmt(q.values[i]) + (q.counts ? ` · ${count(q.counts[i] ?? 0)} ${q.unit ?? 'passages'}` : '')))], ev); });
    svg.addEventListener('mouseleave', () => { cross.setAttribute('opacity', 0); hideTip(); });
    if (onClick) { svg.style.cursor = 'pointer'; svg.addEventListener('click', ev => onClick(at(ev))); }
    return svg;
  }
  function intervalChart({ rows, label }) {
    const w = 720, rowH = 36, m = { l: 320, r: 16, t: 8, b: 26 }, hgt = m.t + m.b + rowH * rows.length, iw = w - m.l - m.r, all = rows.flatMap(r => r.marks.flatMap(q => [q.v, q.lo, q.hi])).filter(Number.isFinite);
    const lo = Math.floor(Math.min(-0.5, ...all) * 2) / 2, hi = Math.ceil(Math.max(0.5, ...all) * 2) / 2, X = v => m.l + (Math.max(lo, Math.min(hi, v)) - lo) / (hi - lo) * iw, svg = s('svg', { viewBox: `0 0 ${w} ${hgt}`, role: 'img', 'aria-label': label, style: `max-width:${w}px` });
    for (let t = lo; t <= hi + 1e-9; t += 0.5) svg.append(s('line', { x1: X(t), x2: X(t), y1: m.t, y2: hgt - m.b, class: Math.abs(t) < 1e-9 ? 'pe-zero' : 'pe-grid' }), s('text', { x: X(t), y: hgt - 8, 'text-anchor': 'middle' }, signed(t)));
    rows.forEach((r, i) => {
      const y = m.t + rowH * i + rowH / 2; svg.append(s('text', { x: m.l - 26, y: y + 4, 'text-anchor': 'end', class: 'pe-rowlabel' }, r.label), s('circle', { cx: m.l - 14, cy: y, r: 4, class: 'pe-stand pe-stand-' + r.standing }));
      if (r.standing === 'mid') svg.append(s('path', { d: `M${m.l - 14} ${y - 4}a4 4 0 0 0 0 8z`, class: 'pe-stand-half' }));
      r.marks.forEach((q, j) => {
        if (!Number.isFinite(q.v)) return; const yy = y + (j - 0.5) * 12, g = s('g');
        if (Number.isFinite(q.lo)) g.append(s('line', { x1: X(q.lo), x2: X(q.hi), y1: yy, y2: yy, stroke: q.color, 'stroke-width': 2, 'stroke-linecap': 'round' }));
        g.append(q.shape === 'diamond' ? s('rect', { x: X(q.v) - 4, y: yy - 4, width: 8, height: 8, transform: `rotate(45 ${X(q.v)} ${yy})`, fill: q.color, class: 'pe-ring' }) : s('circle', { cx: X(q.v), cy: yy, r: 4.5, fill: q.color, class: 'pe-ring' }), s('rect', { x: m.l, y: yy - 6, width: iw, height: 12, fill: 'transparent' }));
        g.addEventListener('mousemove', ev => showTip([h('b', {}, r.label), tipLine(q.color, q.name, signed(q.v) + (Number.isFinite(q.lo) ? ` (95% interval ${signed(q.lo)} to ${signed(q.hi)})` : ''))], ev)); g.addEventListener('mouseleave', hideTip); svg.append(g);
      });
    });
    return svg;
  }

  // ---------------------------------------------------------------- start
  (async () => {
    let payload;
    try { const response = await fetch(endpoint); if (!response.ok) throw new Error('unavailable'); payload = await response.json(); }
    catch { status.textContent = 'The passage table could not be loaded.'; return; }
    if (!payload.available) { status.textContent = 'This release holds no passage table, so the passage views are not available.'; return; }
    let frame;
    try { frame = buildPassageFrame(payload.table, payload.recordings); } catch { status.textContent = 'The passage table failed validation.'; return; }
    const YEARS = passageYears(frame), bound = !loadingsSha256 || loadingsSha256 === frame.loadingsSha256;
    const dims = frame.factorIds.map((id, k) => { const d = dimensions.find(x => x.id === id); return { k, id, label: bound && d ? d.label : id, standing: d?.standing ?? 'none' }; });
    const standClass = d => d.standing === 'supported' ? 'ok' : d.standing === 'at the line' ? 'mid' : d.standing === 'raters do not agree' ? 'no' : 'none';
    const suffix = d => d.standing === 'supported' || d.standing === 'none' ? '' : ' · ' + d.standing;
    const badge = (d, text = d.label) => h('span', { class: 'pe-badge pe-' + standClass(d), title: d.standing === 'none' ? null : `${d.label}: ${d.standing} in the human rating studies` }, text);
    let onlySupported = false; const shown = () => dims.filter(d => !onlySupported || d.standing === 'supported');
    const dimSelect = (state, key, redraw) => { if (!shown().some(d => d.k === state[key])) state[key] = shown()[0].k; return h('label', {}, 'Dimension', h('select', { onchange: e => { state[key] = Number(e.target.value); redraw(); } }, shown().map(d => h('option', { value: d.k, selected: d.k === state[key] }, d.label + suffix(d))))); };
    const legendStanding = () => h('div', { class: 'pe-legend' }, h('span', { class: 'pe-badge pe-ok' }, 'supported by human raters'), h('span', { class: 'pe-badge pe-mid' }, 'at the line'), h('span', { class: 'pe-badge pe-no' }, 'raters do not agree'), h('a', { href: '/corpus/analysis/validation/' }, 'Human rating studies'));
    const kindLegend = () => h('div', { class: 'pe-legend' }, h('span', {}, h('i', { style: 'background:var(--pe-s1)' }), 'Videos'), h('span', {}, h('i', { style: 'background:var(--pe-s2)' }), 'Streams'));
    const kindSelect = (state, redraw, both = false) => h('label', {}, 'Kind', h('select', { onchange: e => { state.kind = Number(e.target.value); redraw(); } }, [...(both ? [[-1, 'Videos and streams']] : []), [0, 'Videos'], [1, 'Streams']].map(([v, t]) => h('option', { value: v, selected: v === state.kind }, t))));
    const subjectName = i => frame.subject[i] === NONE ? 'Unknown' : words(frame.subjects[frame.subject[i]]), addresseeName = i => frame.addressee[i] === NONE ? 'Unknown' : words(frame.addressees[frame.addressee[i]]);
    const passageHref = i => `${frame.recordings[frame.rec[i]].href}?t=${frame.startS[i]}`;
    const passageCard = i => { const r = frame.recordings[frame.rec[i]]; return h('div', { class: 'pe-card' }, h('h4', {}, r.title), h('p', {}, `${words(r.kind)} · ${r.dateText} · passage ${i - r.start + 1} of ${r.count}, from ${clock(frame.startS[i])}`), h('p', {}, `Subject: ${subjectName(i)} · addressed to: ${addresseeName(i).toLowerCase()}`),
      h('div', { class: 'pe-scores' }, shown().map(d => h('span', { style: tint(frame.dim(i, d.k)), title: d.label }, badge(d, d.id), ' ', signed(frame.dim(i, d.k))))), h('p', {}, h('a', { href: passageHref(i) }, 'Open the transcript at this passage'))); };
    const masks = new Map(), maskOf = name => { if (!masks.has(name)) masks.set(name, topicMask(frame, frame.topics.find(t => t.name === name))); return masks.get(name); };
    const subjectIds = frame.subjects.map((id, v) => ({ id, v })).filter(x => x.id !== 'unknown');

    // ---------------------------------------------------------------- views
    const state = { subjects: { kind: 0, kind2: 0, dim: 0 }, recording: { r: null, dim: 0, selected: null }, topics: { name: frame.topics[0]?.name ?? null, kind: -1 }, finder: { kind: -1, subject: -1, addressee: -1, from: '', to: '', topic: '', levels: {}, sort: 0, descending: true, versions: false } };
    const views = [
      { id: 'subjects', name: 'Subjects', render(el) {
        const st = state.subjects, shares = subjectShares(frame, YEARS).filter(x => x.id !== 'unknown').sort((a, b) => b.overall - a.overall), top = Math.max(0.1, Math.ceil(Math.max(...shares.flatMap(x => x.share.flat().filter(Number.isFinite))) * 10) / 10);
        el.append(h('h2', {}, 'What the passages are about, year by year'), h('p', {}, 'Share of passages that the model classifies under each subject, for videos and for streams. A year needs 30 passages of a kind to be drawn. The subject is a model classification and can be wrong. A recording without a catalog date is placed by its estimated date.'), kindLegend(),
          h('div', { class: 'pe-grid-panels' }, shares.map(x => h('div', { class: 'pe-panel' }, h('h4', {}, words(x.id)), h('small', {}, percent(x.overall) + ' of dated passages'), lineChart({ compact: true, w: 300, hgt: 130, ymin: 0, ymax: top, yfmt: percent, xs: YEARS, label: `Share of passages about ${words(x.id).toLowerCase()} by year, videos and streams`, series: [0, 1].map(k => ({ name: KINDS[k], color: COLORS[k], values: x.share[k], counts: x.passages[k] })) })))));
        const matrix = h('div', {}), drawMatrix = () => { const rows = subjectProfile(frame, st.kind).filter(x => x.id !== 'unknown').sort((a, b) => b.passages - a.passages), ds = shown();
          matrix.replaceChildren(h('div', { class: 'pe-panel pe-scroll' }, h('table', {}, h('thead', {}, h('tr', {}, h('th', { scope: 'col' }, 'Subject'), h('th', { scope: 'col' }, 'Passages'), ds.map(d => h('th', { scope: 'col' }, badge(d, d.id))))), h('tbody', {}, rows.map(x => h('tr', {}, h('th', { scope: 'row' }, words(x.id)), h('td', { class: 'pe-num' }, count(x.passages)), ds.map(d => h('td', { class: 'pe-num', style: tint(x.difference[d.k]), title: d.label }, signed(x.difference[d.k])))))))),
            h('p', { class: 'pe-key' }, ds.map(d => `${d.id} ${d.label}`).join(' · '))); };
        el.append(h('h3', {}, 'How each subject is worded'), h('p', {}, 'Mean dimension score of the passages on a subject, minus the mean of all passages of the same kind, in standard deviations of recording scores. Blue is higher, orange lower. A subject needs 30 passages.'), h('div', { class: 'pe-controls' }, kindSelect(st, drawMatrix)), legendStanding(), matrix); drawMatrix();
        const trendBox = h('div', { class: 'pe-panel' }), st2 = { get kind() { return st.kind2; }, set kind(v) { st.kind2 = v; } }, drawTrend = () => { const rows = heldConstantTrend(frame, st.kind2, st.dim, YEARS);
          trendBox.replaceChildren(lineChart({ xs: YEARS, w: 760, hgt: 270, label: 'Yearly mean of the dimension as observed and with the mix of subjects held constant', series: [{ name: 'As observed', color: COLORS[st.kind2], values: rows.map(r => r.observed), counts: rows.map(r => r.passages) }, { name: 'Subjects held constant', color: 'var(--pe-s3)', dash: '5 4', values: rows.map(r => r.held), hollow: rows.map(r => r.covered < 0.8) }] })); };
        el.append(h('h3', {}, 'A trend with the subjects held constant'), h('p', {}, 'The yearly mean of a dimension as observed, and a comparison using the subject mix of the whole period. Differences between the lines show how this adjustment changes the estimate; they do not establish what caused a trend. A hollow point means that the subjects with five passages or more that year hold under 80% of the mix.'),
          h('div', { class: 'pe-controls' }, dimSelect(st, 'dim', drawTrend), kindSelect(st2, drawTrend)), h('div', { class: 'pe-legend' }, h('span', {}, h('i', { style: 'background:var(--corpus-muted)' }), 'As observed, in the colour of the kind'), h('span', {}, h('i', { style: 'background:var(--pe-s3)' }), 'Subjects held constant (dashed)')), trendBox); drawTrend();
      } },
      { id: 'recording', name: 'Within a recording', render(el) {
        const st = state.recording, LIMIT = 300, long = frame.recordings.map((_, r) => r).filter(r => frame.recordings[r].count >= 2).sort((a, b) => frame.recordings[b].count - frame.recordings[a].count || frame.recordings[a].title.localeCompare(frame.recordings[b].title)); if (st.r == null) st.r = long.find(r => frame.recordings[r].counted) ?? long[0] ?? 0;
        const list = h('div', { class: 'pe-list', role: 'listbox', 'aria-label': 'Recordings' }), listNote = h('p', { class: 'pe-key' }), box = h('div', {}), search = h('input', { type: 'search', placeholder: 'Words of a title, earlier title or date', oninput: () => fill() });
        const fill = () => { const q = searchWords(search.value), found = q.length ? long.filter(r => matchesAll(searchText(frame.recordings[r].title, frame.recordings[r].aliases, frame.recordings[r].dateText), q)) : long;
          listNote.textContent = found.length > LIMIT ? `${count(found.length)} recordings, the longest first; the first ${LIMIT} are listed. Type part of a title or a date to narrow the list.` : `${count(found.length)} recordings, the longest first.`;
          list.replaceChildren(...found.slice(0, LIMIT).map(r => h('button', { type: 'button', role: 'option', 'aria-selected': r === st.r ? 'true' : 'false', onclick: () => { st.r = r; st.selected = null; fill(); draw(); } }, `${frame.recordings[r].title} · ${words(frame.recordings[r].kind)} · ${frame.recordings[r].dateText} · ${frame.recordings[r].count} passages${frame.recordings[r].counted ? '' : ' · another version of a recording'}${matchedAlias(frame.recordings[r].title, frame.recordings[r].aliases, q) ? ` · also titled: ${matchedAlias(frame.recordings[r].title, frame.recordings[r].aliases, q)}` : ''}`))); };
        const draw = () => { const r = frame.recordings[st.r], d = dims[st.dim], arc = recordingArc(frame, st.r, st.dim), detail = h('div', {}), pick = i => { st.selected = i; detail.replaceChildren(passageCard(i)); };
          const row = item => h('tr', {}, h('th', { scope: 'row' }, h('button', { type: 'button', class: 'pe-link', onclick: () => pick(item.passage) }, 'Passage ' + (item.passage - r.start + 1))), h('td', {}, 'from ' + clock(frame.startS[item.passage])), h('td', {}, subjectName(item.passage)), h('td', { class: 'pe-num', style: tint(item.value) }, signed(item.value)));
          box.replaceChildren(h('h3', {}, r.title), h('p', {}, `${words(r.kind)} · ${r.dateText} · ${r.count} passages of ${frame.windowWords} words. Mean ${signed(arc.mean)}, spread between passages ${Number.isFinite(arc.sd) ? arc.sd.toFixed(2) : '—'}. `, badge(d, d.standing === 'none' ? d.label : d.standing)),
            h('div', { class: 'pe-legend' }, h('span', {}, h('i', { style: 'background:var(--pe-s1);opacity:.55;height:1px' }), 'Each passage'), h('span', {}, h('i', { style: 'background:var(--pe-s1)' }), 'Average of five passages')),
            h('div', { class: 'pe-panel' }, lineChart({ xs: arc.values.map((_, j) => j + 1), w: 980, hgt: 300, xTitle: 'Passage. Select a point for its place in the transcript.', label: `${d.label} through the passages of ${r.title}`, series: [{ name: 'Passage', color: 'var(--pe-s1)', thin: true, values: arc.values, points: arc.values.length <= 60 }, { name: 'Average of five', color: 'var(--pe-s1)', values: arc.smooth, points: false }], onClick: j => pick(r.start + j) })), detail,
            h('div', { class: 'pe-two' }, h('div', { class: 'pe-panel' }, h('table', {}, h('caption', {}, 'Highest passages'), h('tbody', {}, arc.ranked.slice(0, 5).map(row)))), h('div', { class: 'pe-panel' }, h('table', {}, h('caption', {}, 'Lowest passages'), h('tbody', {}, arc.ranked.slice(-5).reverse().map(row))))));
          if (st.selected != null && frame.rec[st.selected] === st.r) pick(st.selected); };
        el.append(h('h2', {}, 'The course of one recording'), h('p', {}, 'A dimension passage by passage through one recording. A change can be a change of subject or of voice; it does not by itself show a change of mood. Every video and stream with two passages or more can be opened.'),
          h('div', { class: 'pe-controls' }, h('label', {}, 'Find a recording', search), dimSelect(st, 'dim', draw)), legendStanding(), listNote, list, box); fill(); draw();
      } },
      { id: 'topics', name: 'Topics', render(el) {
        const st = state.topics, box = h('div', {}); if (!frame.topics.length) { el.append(h('p', {}, 'This release names no topics.')); return; }
        const select = h('select', { onchange: e => { st.name = e.target.value; draw(); } }, TOPIC_TYPES.filter(t => frame.topics.some(x => x.type === t)).map(t => h('optgroup', { label: words(t) + 's' }, frame.topics.filter(x => x.type === t).map(x => h('option', { value: x.name, selected: x.name === st.name }, `${x.name} (${count(x.passages.length)} passages)`)))));
        const draw = () => { const topic = frame.topics.find(x => x.name === st.name), mask = maskOf(topic.name), rows = topicContrast(frame, mask, st.kind), used = rows[0]?.recordings ?? 0, perYear = YEARS.map(() => new Set());
          for (const i of topic.passages) { const y = frame.year[i] - YEARS[0]; if (frame.counted[i] && y >= 0 && y < YEARS.length && (st.kind < 0 || frame.kind[i] === st.kind)) perYear[y].add(frame.rec[i]); }
          box.replaceChildren(h('div', { class: 'pe-legend' }, h('span', {}, h('i', { class: 'pe-dot', style: 'background:var(--pe-s1)' }), `Within the same recording (${count(used)} recordings hold passages with and without the name)`), h('span', {}, h('i', { class: 'pe-dot pe-diamond', style: 'background:var(--pe-s2)' }), 'Against all other passages')),
            h('div', { class: 'pe-panel' }, used >= 10 ? intervalChart({ label: `Dimension scores of passages that name ${topic.name} against passages that do not`, rows: shown().map(d => ({ label: d.label, standing: standClass(d), marks: [{ v: rows[d.k].within, lo: rows[d.k].low, hi: rows[d.k].high, color: 'var(--pe-s1)', name: 'Within the same recording' }, { v: rows[d.k].across, color: 'var(--pe-s2)', name: 'Against all other passages', shape: 'diamond' }] })) }) : h('p', {}, 'Fewer than ten recordings hold passages with and without this name.')),
            h('div', { class: 'pe-panel pe-scroll' }, h('table', {}, h('caption', {}, 'The same differences as numbers, in standard deviations of recording scores.'), h('thead', {}, h('tr', {}, h('th', { scope: 'col' }, 'Dimension'), h('th', { scope: 'col' }, 'Within the same recording'), h('th', { scope: 'col' }, '95% interval'), h('th', { scope: 'col' }, 'Against all other passages'))), h('tbody', {}, shown().map(d => h('tr', {}, h('th', { scope: 'row' }, badge(d)), h('td', { class: 'pe-num' }, signed(rows[d.k].within)), h('td', { class: 'pe-num' }, `${signed(rows[d.k].low)} to ${signed(rows[d.k].high)}`), h('td', { class: 'pe-num' }, signed(rows[d.k].across))))))),
            h('h3', {}, 'Recordings that name it, by year'), h('div', { class: 'pe-panel' }, lineChart({ xs: YEARS, w: 760, hgt: 200, ymin: 0, yfmt: v => String(Math.round(v)), label: `Recordings that name ${topic.name} by year`, series: [{ name: 'Recordings', color: 'var(--pe-s3)', values: perYear.map(x => x.size) }] })),
            h('p', {}, h('a', { href: '#finder', onclick: () => { state.finder.topic = topic.name; } }, 'Open these passages in the passage finder'))); };
        el.append(h('h2', {}, 'How passages that name a topic are worded'), h('p', {}, 'Passages in which a product, game, platform or place is named, against passages in which it is not. The first comparison stays inside one recording, so the day, the format and the occasion are the same. Names are matched letter for letter, so some matches are another sense of the word. This compares the wording around a name. It says nothing about what anyone thinks of the thing named.'),
          h('div', { class: 'pe-controls' }, h('label', {}, 'Topic', select), kindSelect(st, draw, true)), legendStanding(), box); draw();
      } },
      { id: 'finder', name: 'Passage finder', render(el) {
        const st = state.finder, box = h('div', {}), LEVELS = [['', 'Any'], ['2', 'Very high (+2 or more)'], ['1', 'High (+1 or more)'], ['-1', 'Low (−1 or less)'], ['-2', 'Very low (−2 or less)']];
        const pick = (label, opts, value, set) => h('label', {}, label, h('select', { onchange: e => { set(e.target.value); run(); } }, opts.map(([v, t]) => h('option', { value: v, selected: String(v) === String(value ?? '') }, t))));
        if (!shown().some(d => d.k === st.sort)) st.sort = shown()[0].k;
        const run = () => { const ds = shown(), levels = Object.fromEntries(Object.entries(st.levels).filter(([k]) => ds.some(d => d.k === Number(k))));
          const hits = findPassages(frame, { kind: st.kind, subject: st.subject, addressee: st.addressee, from: st.from === '' ? -Infinity : Number(st.from), to: st.to === '' ? Infinity : Number(st.to), mask: st.topic ? maskOf(st.topic) : null, levels, sort: st.sort, descending: st.descending, versions: st.versions });
          box.replaceChildren(h('p', {}, `${count(hits.length)} passages in ${count(new Set(hits.map(i => frame.rec[i])).size)} recordings. The first 60 are listed; each links to its place in the transcript.`), h('p', { class: 'pe-key' }, ds.map(d => `${d.id} ${d.label}`).join(' · ')), h('div', { class: 'pe-panel pe-scroll pe-results' }, h('table', {}, h('thead', {}, h('tr', {}, h('th', { scope: 'col' }, 'Recording'), h('th', { scope: 'col' }, 'Passage'), h('th', { scope: 'col' }, 'Subject'), ds.map(d => h('th', { scope: 'col', title: d.label }, badge(d, d.id))))),
            h('tbody', {}, hits.slice(0, 60).map(i => { const r = frame.recordings[frame.rec[i]]; return h('tr', {}, h('th', { scope: 'row' }, h('a', { href: passageHref(i) }, r.title), h('br'), h('small', {}, `${words(r.kind)} · ${r.dateText}${r.counted ? '' : ' · another version of a recording'}`)), h('td', {}, `${i - r.start + 1} of ${r.count}`, h('br'), h('small', {}, 'from ' + clock(frame.startS[i]))), h('td', {}, subjectName(i)), ds.map(d => h('td', { class: 'pe-num', style: tint(frame.dim(i, d.k)), title: d.label }, signed(frame.dim(i, d.k))))); })))), ); };
        el.append(h('h2', {}, 'Find passages by how they are worded'), h('p', {}, 'Passages that meet every condition, ordered by one dimension. A profile is the model\'s reading of 500 words and can be wrong: read the passage before relying on it.'),
          h('div', { class: 'pe-controls' }, pick('Kind', [[-1, 'Any'], [0, 'Videos'], [1, 'Streams']], st.kind, v => { st.kind = Number(v); }), pick('Subject', [[-1, 'Any'], ...subjectIds.map(x => [x.v, words(x.id)])], st.subject, v => { st.subject = Number(v); }),
            pick('Addressed to', [[-1, 'Anyone'], ...frame.addressees.map((id, v) => [v, words(id)]).filter(([, t]) => t !== 'Unknown')], st.addressee, v => { st.addressee = Number(v); }), pick('Names the topic', [['', 'Any'], ...frame.topics.map(t => [t.name, t.name])], st.topic, v => { st.topic = v; }),
            pick('From year', [['', 'Any'], ...YEARS.map(y => [y, y])], st.from, v => { st.from = v; }), pick('To year', [['', 'Any'], ...YEARS.map(y => [y, y])], st.to, v => { st.to = v; })),
          h('div', { class: 'pe-controls' }, shown().map(d => pick(d.label + suffix(d), LEVELS, st.levels[d.k], v => { st.levels[d.k] = v; }))),
          h('div', { class: 'pe-controls' }, pick('Order by', shown().map(d => [d.k, d.label + suffix(d)]), st.sort, v => { st.sort = Number(v); }), pick('Direction', [['1', 'Highest first'], ['0', 'Lowest first']], st.descending ? '1' : '0', v => { st.descending = v === '1'; }), pick('Other versions of a recording', [['0', 'Left out'], ['1', 'Included']], st.versions ? '1' : '0', v => { st.versions = v === '1'; })), legendStanding(), box); run();
      } },
    ];

    // ---------------------------------------------------------------- shell
    const tabs = h('div', { class: 'pe-tabs', role: 'tablist', 'aria-label': 'Passage views' }), body = h('div', { class: 'pe-body' });
    const show = id => { const view = views.find(v => v.id === id) ?? views[0]; for (const b of tabs.querySelectorAll('button')) b.setAttribute('aria-selected', b.dataset.id === view.id ? 'true' : 'false'); body.replaceChildren(); view.render(body); if (location.hash !== '#' + view.id) history.replaceState(null, '', '#' + view.id); };
    tabs.append(...views.map(v => h('button', { type: 'button', role: 'tab', 'data-id': v.id, onclick: () => show(v.id) }, v.name)));
    if (dims.some(d => d.standing === 'supported')) tabs.append(h('label', { class: 'pe-only' }, h('input', { type: 'checkbox', onchange: e => { onlySupported = e.target.checked; show(location.hash.slice(1)); } }), 'Only dimensions that human raters support'));
    const others = frame.recordings.filter(r => !r.counted).length, estimated = frame.recordings.filter(r => r.estimated).length, undated = frame.recordings.filter(r => !r.date && !r.estimated).length;
    status.textContent = `${count(frame.n)} passages of ${count(frame.recordings.length)} videos and streams. ${count(others)} of them are other versions of a recording: they can be opened and searched, and are left out where passages are added up. ${count(estimated)} recordings without a catalog date are placed by their estimated date in the views by year${undated ? `, and ${count(undated)} with no date at all are left out of them` : ''}.`; root.append(tabs, body);
    addEventListener('hashchange', () => show(location.hash.slice(1))); show(location.hash.slice(1));
  })();
}
