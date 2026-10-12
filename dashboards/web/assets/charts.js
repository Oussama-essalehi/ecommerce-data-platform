// Small chart kit on top of d3: lines, columns, bars, donut, heatmap, sparkline.
// Every chart is redrawn from scratch at the width of its box, with colours
// read from the CSS tokens so that both themes work.
(function () {
  'use strict';

  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const color = (i) => css('--s' + (i + 1));

  // ---------- tooltip, positioned inside the chart box
  function tipOf(box) {
    let t = box.querySelector(':scope > .tip');
    if (!t) {
      t = document.createElement('div');
      t.className = 'tip';
      t.hidden = true;
      box.append(t);
    }
    return t;
  }
  function hideTip(box) {
    const t = box.querySelector(':scope > .tip');
    if (t) t.hidden = true;
  }
  // lines: [{color, line (bool), name, value, note (bool)}]
  function showTip(box, x, y, title, lines) {
    const t = tipOf(box);
    const h = document.createElement('div');
    h.className = 'tip-h';
    h.textContent = title;
    t.replaceChildren(h, ...lines.map((l) => {
      const r = document.createElement('div');
      r.className = 'tip-r' + (l.note ? ' note' : '');
      if (l.color) {
        const s = document.createElement('i');
        s.className = 'sw' + (l.line ? ' line' : '');
        s.style.background = l.color;
        r.append(s);
      }
      const n = document.createElement('span');
      n.textContent = l.name || '';
      r.append(n);
      if (l.value != null) {
        const v = document.createElement('span');
        v.className = 'v';
        v.textContent = l.value;
        r.append(v);
      }
      return r;
    }));
    t.hidden = false;
    const bw = box.clientWidth;
    const tw = t.offsetWidth;
    const th = t.offsetHeight;
    let left = x + 14;
    if (left + tw > bw) left = x - 14 - tw;
    if (left < 0) left = Math.max(0, Math.min(bw - tw, x - tw / 2));
    let top = y - th - 12;
    if (top < 0) top = y + 16;
    t.style.left = left + 'px';
    t.style.top = top + 'px';
  }

  // ---------- shared plumbing
  function setup(box, height, label, clickable) {
    box.querySelectorAll(':scope > svg, :scope > .chart-empty').forEach((n) => n.remove());
    hideTip(box);
    box.classList.toggle('clickable', !!clickable);
    const W = Math.floor(box.clientWidth);
    const svg = d3.select(box).insert('svg', ':first-child')
      .attr('width', W).attr('height', height).attr('viewBox', `0 0 ${W} ${height}`)
      .attr('role', 'img').attr('aria-label', label);
    return { svg, W };
  }
  function empty(box, svg, text) {
    svg.remove();
    const p = document.createElement('p');
    p.className = 'chart-empty';
    p.textContent = text || 'Aucune donnée pour cette sélection.';
    box.append(p);
  }
  function measure(svg, text, cls) {
    const t = svg.append('text').attr('class', cls || null).text(text);
    const w = t.node().getComputedTextLength();
    t.remove();
    return w;
  }
  const topRounded = (x, y, w, h, r) => {
    r = Math.max(0, Math.min(r, w / 2, h));
    return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
  };
  const rightRounded = (x, y, w, h, r) => {
    r = Math.max(0, Math.min(r, h / 2, w));
    return `M${x},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h - r}Q${x + w},${y + h} ${x + w - r},${y + h}H${x}Z`;
  };
  function yAxis(g, svg, y, W, ml, fmt) {
    const ticks = y.ticks(4);
    g.selectAll(null).data(ticks).join('line').attr('class', 'gridline')
      .attr('x1', ml).attr('x2', W).attr('y1', (d) => y(d)).attr('y2', (d) => y(d));
    g.selectAll(null).data(ticks).join('text')
      .attr('x', ml - 8).attr('y', (d) => y(d)).attr('dy', '0.32em').attr('text-anchor', 'end').text(fmt);
  }
  const leftMargin = (svg, y, fmt) => Math.ceil(d3.max(y.ticks(4), (t) => measure(svg, fmt(t)))) + 12;

  // ---------- line chart over dates
  // o: {label, height, series: [{name, color, values: [{x, y}]}] (same x for every series),
  //     yFmt, valFmt, tipX, xTickFmt, xTicks, dots, area, bands: [{from, to, label}], yMin}
  function line(box, o) {
    const H = o.height || 260;
    const { svg, W } = setup(box, H, o.label);
    if (!W) return;
    const pts = o.series.flatMap((s) => s.values).filter((v) => v.y != null);
    if (!pts.length) return empty(box, svg);
    const bands = o.bands || [];
    const top = bands.length ? 24 : 10;
    const bottom = H - 24;
    const y = d3.scaleLinear([o.yMin != null ? o.yMin : 0, d3.max(pts, (d) => d.y) || 1], [bottom, top]).nice();
    const ml = leftMargin(svg, y, o.yFmt);
    let [x0, x1] = d3.extent(pts, (d) => d.x);
    if (+x0 === +x1) { x0 = d3.utcDay.offset(x0, -15); x1 = d3.utcDay.offset(x1, 15); }
    const pad = o.dots ? 14 : 2;
    const x = d3.scaleUtc([x0, x1], [ml + pad, W - pad - 2]);
    const g = svg.append('g');

    // sales events as shaded bands, labels kept apart
    let labelEnd = -Infinity;
    bands.forEach((b) => {
      const bx0 = Math.max(x(b.from), ml);
      const bx1 = Math.min(x(d3.utcDay.offset(b.to, 1)), W);
      if (bx1 <= bx0) return;
      g.append('rect').attr('class', 'band').attr('x', bx0).attr('y', top).attr('width', bx1 - bx0).attr('height', bottom - top);
      const lw = measure(svg, b.label, 'band-label');
      const lx = Math.min(bx0, W - lw);
      if (lx > labelEnd + 8 && lx >= ml) {
        g.append('text').attr('class', 'band-label').attr('x', lx).attr('y', top - 8).text(b.label);
        labelEnd = lx + lw;
      }
    });

    yAxis(g, svg, y, W, ml, o.yFmt);
    g.append('line').attr('class', 'axis').attr('x1', ml).attr('x2', W).attr('y1', bottom).attr('y2', bottom);

    // x ticks: as many as fit, never overlapping, inside the drawing
    const fmt = o.xTickFmt || d3.utcFormat('%b %Y');
    const ticks = o.xTicks || x.ticks(Math.max(2, Math.floor((W - ml) / (measure(svg, fmt(x0)) + 28))));
    let end = -Infinity;
    ticks.forEach((t) => {
      const lw = measure(svg, fmt(t));
      const cx = Math.min(Math.max(x(t), ml + lw / 2), W - lw / 2);
      if (cx - lw / 2 < end + 10) return;
      g.append('text').attr('x', cx).attr('y', H - 6).attr('text-anchor', 'middle').text(fmt(t));
      end = cx + lw / 2;
    });

    const gen = d3.line().defined((d) => d.y != null).x((d) => x(d.x)).y((d) => y(d.y));
    if (o.area) {
      const s = o.series[0];
      const id = 'grad-' + (box.id || Math.random().toString(36).slice(2));
      const grad = svg.append('defs').append('linearGradient').attr('id', id).attr('x1', 0).attr('x2', 0).attr('y1', 0).attr('y2', 1);
      [[0, 0.22], [0.5, 0.08], [1, 0]].forEach(([off, op]) => grad.append('stop').attr('offset', off).style('stop-color', s.color).style('stop-opacity', op));
      g.append('path').datum(s.values).attr('fill', `url(#${id})`)
        .attr('d', d3.area().defined((d) => d.y != null).x((d) => x(d.x)).y0(bottom).y1((d) => y(d.y)));
    }
    o.series.forEach((s) => {
      g.append('path').datum(s.values).attr('fill', 'none').attr('stroke', s.color).attr('stroke-width', s.width || 2)
        .attr('stroke-linecap', 'round').attr('stroke-linejoin', 'round').attr('d', gen);
      if (o.dots) {
        g.selectAll(null).data(s.values.filter((v) => v.y != null)).join('circle')
          .attr('cx', (d) => x(d.x)).attr('cy', (d) => y(d.y)).attr('r', 4)
          .attr('fill', s.color).attr('stroke', css('--surface')).attr('stroke-width', 2);
      }
    });

    // hover: a rule at the nearest date, a dot per series, one tooltip
    const xs = o.series[0].values.map((v) => +v.x);
    const rule = g.append('line').attr('class', 'rule').attr('y1', top).attr('y2', bottom).style('opacity', 0);
    const marks = o.series.map((s) => g.append('circle').attr('r', 5).attr('fill', s.color)
      .attr('stroke', css('--surface')).attr('stroke-width', 2).style('opacity', 0).style('pointer-events', 'none'));
    const hide = () => { rule.style('opacity', 0); marks.forEach((m) => m.style('opacity', 0)); hideTip(box); };
    svg.append('rect').attr('class', 'hit').attr('x', ml).attr('y', 0).attr('width', Math.max(0, W - ml)).attr('height', H)
      .on('pointermove', (ev) => {
        const [mx] = d3.pointer(ev);
        const i = d3.bisectCenter(xs, +x.invert(mx));
        const xv = o.series[0].values[i].x;
        const px = x(xv);
        rule.attr('x1', px).attr('x2', px).style('opacity', 1);
        let py = bottom;
        o.series.forEach((s, j) => {
          const v = s.values[i];
          if (v && v.y != null) { marks[j].attr('cx', px).attr('cy', y(v.y)).style('opacity', 1); py = Math.min(py, y(v.y)); }
          else marks[j].style('opacity', 0);
        });
        const lines = o.series.map((s) => ({
          color: o.series.length > 1 ? s.color : null, line: true, name: s.name,
          value: s.values[i] && s.values[i].y != null ? o.valFmt(s.values[i].y) : '—',
        }));
        const band = bands.find((b) => xv >= b.from && xv <= b.to);
        if (band) lines.push({ name: band.label, note: true });
        showTip(box, px, py, o.tipX(xv), lines);
      })
      .on('pointerleave', hide);
  }

  // ---------- vertical columns
  // o: {label, height, data: [{key, label, value, partial, tip}], yFmt, valFmt, valShort, valName,
  //     showValues, selected, onClick, color}
  function columns(box, o) {
    const H = o.height || 240;
    const { svg, W } = setup(box, H, o.label, !!o.onClick);
    if (!W) return;
    if (!o.data.length) return empty(box, svg, o.empty);
    const top = o.showValues ? 22 : 10;
    const bottom = H - 24;
    const y = d3.scaleLinear([0, d3.max(o.data, (d) => d.value) || 1], [bottom, top]).nice();
    const ml = leftMargin(svg, y, o.yFmt);
    const x = d3.scaleBand(o.data.map((d) => d.key), [ml, W]).paddingInner(0.28).paddingOuter(0.12);
    const bw = Math.min(x.bandwidth(), 52);
    const g = svg.append('g');
    yAxis(g, svg, y, W, ml, o.yFmt);
    const base = o.color || color(0);
    g.selectAll(null).data(o.data).join('path')
      .attr('fill', (d) => (o.selected && o.selected !== d.key ? css('--axis') : base))
      .attr('fill-opacity', (d) => (d.partial ? 0.45 : 1))
      .attr('d', (d) => topRounded(x(d.key) + (x.bandwidth() - bw) / 2, y(d.value), bw, Math.max(0, bottom - y(d.value)), 4));
    g.append('line').attr('class', 'axis').attr('x1', ml).attr('x2', W).attr('y1', bottom).attr('y2', bottom);

    const widest = d3.max(o.data, (d) => measure(svg, d.label)) + 10;
    const every = Math.max(1, Math.ceil(widest / x.step()));
    o.data.forEach((d, i) => {
      if (i % every) return;
      g.append('text').attr('x', x(d.key) + x.bandwidth() / 2).attr('y', H - 6).attr('text-anchor', 'middle').text(d.label);
    });
    if (o.showValues) {
      const vw = d3.max(o.data, (d) => measure(svg, o.valShort(d.value), 'val'));
      if (vw + 6 <= x.step()) {
        g.selectAll(null).data(o.data).join('text').attr('class', 'val')
          .attr('x', (d) => x(d.key) + x.bandwidth() / 2).attr('y', (d) => y(d.value) - 6)
          .attr('text-anchor', 'middle').text((d) => o.valShort(d.value));
      }
    }
    g.selectAll(null).data(o.data).join('rect').attr('class', 'hit')
      .attr('x', (d) => x(d.key) - (x.step() - x.bandwidth()) / 2).attr('width', x.step())
      .attr('y', top).attr('height', bottom - top)
      .on('pointermove', (ev, d) => {
        const [mx, my] = d3.pointer(ev, box);
        const lines = [{ name: o.valName, value: o.valFmt(d.value) }];
        if (d.tip) lines.push(...d.tip);
        showTip(box, mx, my, d.title || d.label, lines);
      })
      .on('pointerleave', () => hideTip(box))
      .on('click', (ev, d) => o.onClick && o.onClick(d));
  }

  // ---------- horizontal bars
  // o: {label, data: [{key, label, value, color, tip}], valFmt, valShort, valName, selected, onClick}
  function bars(box, o) {
    const rowH = o.rowH || 32;
    const barH = Math.min(18, rowH - 10);
    const H = Math.max(o.data.length, 1) * rowH + 6;
    box.style.minHeight = H + 'px';
    const { svg, W } = setup(box, H, o.label, !!o.onClick);
    if (!W) return;
    if (!o.data.length) return empty(box, svg, o.empty);
    const lw = Math.min(W * 0.42, d3.max(o.data, (d) => measure(svg, d.label)) + 12);
    const vw = d3.max(o.data, (d) => measure(svg, o.valShort(d.value), 'val')) + 12;
    const x = d3.scaleLinear([0, d3.max(o.data, (d) => d.value) || 1], [lw, Math.max(lw + 10, W - vw)]);
    const g = svg.append('g');
    o.data.forEach((d, i) => {
      const cy = i * rowH + rowH / 2 + 3;
      const muted = o.selected && o.selected !== d.key;
      g.append('text').attr('x', lw - 10).attr('y', cy).attr('dy', '0.32em').attr('text-anchor', 'end').text(d.label);
      g.append('path').attr('fill', muted ? css('--axis') : (d.color || color(0)))
        .attr('d', rightRounded(lw, cy - barH / 2, Math.max(0, x(d.value) - lw), barH, 4));
      g.append('text').attr('class', 'val').attr('x', x(d.value) + 6).attr('y', cy).attr('dy', '0.32em').text(o.valShort(d.value));
    });
    g.append('line').attr('class', 'axis').attr('x1', lw).attr('x2', lw).attr('y1', 2).attr('y2', H - 2);
    g.selectAll(null).data(o.data).join('rect').attr('class', 'hit')
      .attr('x', 0).attr('width', W).attr('y', (d, i) => i * rowH + 3).attr('height', rowH)
      .on('pointermove', (ev, d) => {
        const [mx, my] = d3.pointer(ev, box);
        const lines = [{ color: d.tipColor ? d.color : null, name: d.tipName || o.valName, value: o.valFmt(d.value) }];
        if (d.tip) lines.push(...d.tip);
        showTip(box, mx, my, d.label, lines);
      })
      .on('pointerleave', () => hideTip(box))
      .on('click', (ev, d) => o.onClick && o.onClick(d));
  }

  // ---------- donut with a total in the middle
  // o: {label, data: [{key, label, value, color}], selected, onClick, centerValue, centerLabel, valFmt}
  function donut(box, o) {
    const H = o.height || 210;
    const { svg, W } = setup(box, H, o.label, !!o.onClick);
    if (!W) return;
    const total = d3.sum(o.data, (d) => d.value);
    if (!total) return empty(box, svg);
    const r = Math.min(H / 2 - 4, W / 2 - 4);
    const arc = d3.arc().innerRadius(r * 0.66).outerRadius(r).cornerRadius(3).padAngle(0.014);
    const g = svg.append('g').attr('transform', `translate(${W / 2},${H / 2})`);
    g.selectAll(null).data(d3.pie().sort(null).value((d) => d.value)(o.data)).join('path')
      .attr('class', 'slice').attr('d', arc)
      .attr('fill', (a) => (o.selected && o.selected !== a.data.key ? css('--axis') : a.data.color))
      .on('pointermove', (ev, a) => {
        const [mx, my] = d3.pointer(ev, box);
        showTip(box, mx, my, a.data.label, [
          { name: o.valName, value: o.valFmt(a.data.value) },
          { name: 'Part', value: o.shareFmt(a.data.value / total) },
        ]);
      })
      .on('pointerleave', () => hideTip(box))
      .on('click', (ev, a) => o.onClick && o.onClick(a.data));
    g.append('text').attr('class', 'center-big').attr('text-anchor', 'middle').attr('dy', '0.1em').text(o.centerValue);
    g.append('text').attr('text-anchor', 'middle').attr('dy', '1.6em').text(o.centerLabel);
  }

  // ---------- heatmap: rows x columns of counts
  // o: {label, rows, cols, value(r, c), tip(r, c, v)}
  function heatmap(box, o) {
    const cell = 40;
    const H = o.rows.length * cell + 28;
    const { svg, W } = setup(box, H, o.label);
    if (!W) return;
    const lw = d3.max(o.rows, (r) => measure(svg, r)) + 14;
    const x = d3.scaleBand(o.cols, [lw, W]).paddingInner(0.06);
    const y = d3.scaleBand(o.rows, [0, o.rows.length * cell]).paddingInner(0.06);
    const max = d3.max(o.rows.flatMap((r) => o.cols.map((c) => o.value(r, c)))) || 1;
    const ramp = d3.interpolateRgb(css('--seq-lo'), color(0));
    const g = svg.append('g');
    o.rows.forEach((r) => {
      g.append('text').attr('x', lw - 10).attr('y', y(r) + y.bandwidth() / 2).attr('dy', '0.32em').attr('text-anchor', 'end').text(r);
      o.cols.forEach((c) => {
        const v = o.value(r, c);
        const t = v ? 0.18 + 0.82 * (v / max) : 0;
        g.append('rect').attr('x', x(c)).attr('y', y(r)).attr('width', x.bandwidth()).attr('height', y.bandwidth()).attr('rx', 4)
          .attr('fill', v ? ramp(t) : css('--surface-2')).attr('stroke', v ? 'none' : css('--border'));
        g.append('text').attr('class', 'val').attr('x', x(c) + x.bandwidth() / 2).attr('y', y(r) + y.bandwidth() / 2)
          .attr('dy', '0.32em').attr('text-anchor', 'middle')
          .style('fill', v && t > 0.55 ? '#ffffff' : null).text(v ? v : '–');
        g.append('rect').attr('class', 'hit').attr('x', x(c)).attr('y', y(r)).attr('width', x.bandwidth()).attr('height', y.bandwidth())
          .on('pointermove', (ev) => {
            const [mx, my] = d3.pointer(ev, box);
            showTip(box, mx, my, `${r} · ${c}`, [{ name: o.valName, value: String(v) }]);
          })
          .on('pointerleave', () => hideTip(box));
      });
    });
    const widest = d3.max(o.cols, (c) => measure(svg, c)) + 6;
    const rot = widest > x.bandwidth();
    o.cols.forEach((c) => {
      const t = g.append('text').attr('y', H - 8).text(c);
      if (rot) t.attr('x', x(c) + x.bandwidth() / 2).attr('text-anchor', 'middle').text(c.slice(0, Math.max(3, Math.floor(x.bandwidth() / 7))) + (c.length * 7 > x.bandwidth() ? '…' : ''));
      else t.attr('x', x(c) + x.bandwidth() / 2).attr('text-anchor', 'middle');
    });
  }

  // ---------- sparkline: values (numbers or null), last point emphasised
  function spark(el, values, col) {
    el.replaceChildren();
    const W = 88, H = 36;
    const pts = values.map((v, i) => ({ i, v })).filter((d) => d.v != null);
    if (pts.length < 2) return;
    const svg = d3.select(el).append('svg').attr('viewBox', `0 0 ${W} ${H}`).attr('width', '100%').attr('height', '100%')
      .attr('preserveAspectRatio', 'none').attr('aria-hidden', 'true');
    const x = d3.scaleLinear([0, values.length - 1], [2, W - 4]);
    const [lo, hi] = d3.extent(pts, (d) => d.v);
    const y = d3.scaleLinear([lo, hi === lo ? lo + 1 : hi], [H - 4, 4]);
    svg.append('path').datum(pts).attr('fill', 'none').attr('stroke', col).attr('stroke-width', 1.75)
      .attr('stroke-linejoin', 'round').attr('stroke-linecap', 'round').attr('vector-effect', 'non-scaling-stroke')
      .attr('d', d3.line().x((d) => x(d.i)).y((d) => y(d.v)));
    const last = pts[pts.length - 1];
    svg.append('circle').attr('cx', x(last.i)).attr('cy', y(last.v)).attr('r', 2.5).attr('fill', col);
  }

  window.Charts = { css, color, line, columns, bars, donut, heatmap, spark, hideTip };
})();
