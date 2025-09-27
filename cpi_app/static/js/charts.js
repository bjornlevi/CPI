// static/js/charts.js  (v23)
(function (global) {
  console.log("charts.js loaded 23");

  const fmt = v => v == null ? '—'
    : Number(v).toLocaleString('is-IS', { maximumFractionDigits: 2 });
  const pct = (a, b) => (a == null || b == null || b === 0) ? null : (a / b - 1) * 100;

  function getCtx(id){ const c=document.getElementById(id); return c ? c.getContext('2d') : null; }

  // ---- Legend: keep our own visibility set, and persist it across rebuilds
  function defaultLegendOnClick(e, legendItem, legend){
    const def = Chart.defaults.plugins.legend.onClick;
    def && def.call(this, e, legendItem, legend);
    const ci   = legend.chart;
    const idx  = legendItem.datasetIndex;
    const meta = ci.getDatasetMeta(idx);
    const ds   = ci.data.datasets?.[idx];
    const key  = ds && ds._key;
    if (!key || !ci.$state) return;
    if (meta.hidden === true) ci.$state.activeKeys.delete(key);
    else                      ci.$state.activeKeys.add(key);
  }

  // ---- Hover marker (current + 12 months back)
  const hoverYearMarker = {
    id: 'hoverYearMarker',
    afterDraw(chart){
      const t = chart.tooltip;
      if (!t || !t.getActiveElements().length) return;
      const i = t.getActiveElements()[0].index;
      const j = i - 12;
      const x = chart.scales.x, top = chart.chartArea.top, bot = chart.chartArea.bottom, ctx = chart.ctx;
      ctx.save();
      if (j >= 0){
        ctx.setLineDash([5,4]); ctx.strokeStyle='rgba(255,255,255,.35)'; ctx.lineWidth=1;
        ctx.beginPath(); ctx.moveTo(x.getPixelForValue(j),top); ctx.lineTo(x.getPixelForValue(j),bot); ctx.stroke();
      }
      ctx.setLineDash([]); ctx.strokeStyle='rgba(255,255,255,.6)';
      ctx.beginPath(); ctx.moveTo(x.getPixelForValue(i),top); ctx.lineTo(x.getPixelForValue(i),bot); ctx.stroke();
      ctx.restore();
    }
  };

  // ---- Tooltip builder
  function makeTooltipConfig(labels, seriesFor){
    return {
      displayColors: false,
      callbacks: {
        title: items => items?.[0]?.label ?? '',
        label: ctx => `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}`,
        afterBody(items){
          if (!items?.length) return [];
          const idx = items[0].dataIndex;
          const s = seriesFor(items[0].dataset.label, items[0].datasetIndex);
          if (!s) return [];
          const curr = s[idx], prev = idx>0 ? s[idx-1] : null;
          const lines = [];
          const mom = pct(curr, prev);
          if (mom != null) lines.push(`Mánaðarbreyting: ${mom.toFixed(2)}%`);
          const j = idx - 12, prev12 = j >= 0 ? s[j] : null;
          const yoy = pct(curr, prev12);
          if (yoy != null) lines.push(`Ársbreyting: ${yoy.toFixed(2)}% (vs ${labels[j]})`);
          return lines;
        }
      }
    };
  }

  // ---- Range & normalization helpers
  const monthsForRange = (key, total) =>
    key === 'all' ? total : key === '10y' ? Math.min(total,120)
    : key === '5y' ? Math.min(total,60) : Math.min(total,24);

  // Normalize so that value at baseIdx is exactly 100 (when it exists)
  function normalizeStrict(arr, baseIdx){
    if (!Array.isArray(arr) || baseIdx == null) return arr;
    const base = arr[baseIdx];
    if (base == null || base === 0) return arr.map(_ => null);
    const out = arr.map(v => (v == null ? null : 100 * (v / base)));
    if (out.length > baseIdx && out[baseIdx] != null) out[baseIdx] = 100;
    return out;
  }
  // Flexible: if base point is null, anchor at first non-null >= baseIdx
  function normalizeFromIndex(arr, baseIdx){
    if (!Array.isArray(arr) || baseIdx == null) return arr;
    let k = baseIdx;
    while (k < arr.length && (arr[k] == null || arr[k] === 0)) k++;
    if (k >= arr.length) return arr.map(_ => null);
    const base = arr[k];
    const out = arr.map(v => (v == null ? null : 100 * (v / base)));
    if (out.length > k && out[k] != null) out[k] = 100;
    return out;
  }

  // Range buttons (2y/5y/10y/all) — they set the visible window length
  function hookRangeButtons(canvasId, onChange, initial='2y'){
    const box = document.querySelector(`.range-controls[data-chart="${canvasId}"]`);
    if (!box) { onChange(initial); return; }
    const activate = key => box.querySelectorAll('button').forEach(
      b => b.classList.toggle('is-active', b.dataset.range === key)
    );
    box.addEventListener('click', e=>{
      const b = e.target.closest('button[data-range]'); if (!b) return;
      const key = b.dataset.range; activate(key); onChange(key);
    });
    activate(initial); onChange(initial);
  }

  // Slider + toggle: here the slider sets the **window start** (like the 2y button does)
  function attachControls(canvasId, { onToggle, onStartChange, onHoverPreview }){
    const toggle = document.getElementById(`${canvasId}-norm-toggle`);
    const slider = document.getElementById(`${canvasId}-base-slider`);
    const label  = document.getElementById(`${canvasId}-base-label`);
    if (toggle) toggle.addEventListener('change', e => onToggle(!!e.target.checked));
    if (slider) {
      slider.addEventListener('input', e => { onStartChange(+e.target.value); });
      const updateTitle = (i)=>{ const t = onHoverPreview?.(i) || '—'; slider.title = t; if (label) label.textContent = t; };
      slider.addEventListener('mousemove', e => {
        const r = slider.getBoundingClientRect();
        const ratio = Math.min(1, Math.max(0,(e.clientX - r.left)/r.width));
        const i = Math.round(ratio * (+slider.max || 0));
        updateTitle(i);
      });
      slider.addEventListener('mouseleave', ()=> updateTitle(+slider.value || 0));
    }
  }

  // Update slider UI for current window: max = L.length - n; value = startAbs; label = L[startAbs]
  function setWindowUI(canvasId, totalMonths, n, startAbs, lookupLabel){
    const slider = document.getElementById(`${canvasId}-base-slider`);
    const label  = document.getElementById(`${canvasId}-base-label`);
    if (!slider || !label) return;
    const maxStart = Math.max(0, totalMonths - n);
    slider.max   = maxStart;
    slider.value = Math.min(Math.max(0, startAbs|0), maxStart);
    const txt = lookupLabel(slider.value) || '—';
    label.textContent = txt; slider.title = txt;
  }

  // Label lookup for slider hover text (using FULL labels)
  function setFullLabelLookup(id, fullLabels){
    global.EconCharts = global.EconCharts || {};
    (global.EconCharts._fullLabelLookup ||= {})[id] = fullLabels || [];
  }
  function fullLabelAt(canvasId, idx){
    const L = global.EconCharts?._fullLabelLookup?.[canvasId] || [];
    return L[idx] || '—';
  }

  // ------------------ CPI (total + forecast + sub overlays) -------------
  function initCPIChart(canvasId, { fullLabels, fullValues, futLabels, futValues, subMeta, subSeries, initialRange='2y' }){
    const FULL = fullLabels || [];
    const VALL = fullValues || [];
    const FL   = futLabels  || [];
    const FV   = futValues  || [];
    const meta = subMeta    || [];
    const subs = subSeries  || {};

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;
    setFullLabelLookup(canvasId, FULL);

    const chart = new Chart(ctx, {
      type: 'line',
      data: { labels: [], datasets: [] },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode:'nearest', intersect:false },
        plugins: {
          legend:{ position:'bottom', onClick: defaultLegendOnClick },
          tooltip: makeTooltipConfig([], () => null)
        },
        scales: { x:{ ticks:{ maxRotation:0, autoSkip:true, maxTicksLimit:12 } }, y:{ beginAtZero:false } }
      },
      plugins: [hoverYearMarker]
    });

    // Window state: range selects length; slider selects start index in FULL;
    chart.$state = {
      rangeKey: initialRange,
      startAbs: Math.max(0, FULL.length - monthsForRange(initialRange, FULL.length)),
      norm: false,
      activeKeys: new Set(['total','forecast'])
    };

    function rebuild(){
      const stt = chart.$state;

      const totalMonths = FULL.length;
      const n  = monthsForRange(stt.rangeKey, totalMonths);
      // clamp start so that we always have n months in the window
      stt.startAbs = Math.min(Math.max(0, stt.startAbs|0), Math.max(0, totalMonths - n));

      const st = stt.startAbs;
      const L  = FULL.slice(st, st + n);       // visible actual labels
      const lab = L.concat(FL);                // + forecast labels
      const actualSlice = VALL.slice(st, st + n);

      // Total (actual + forecast), base at index 0 of the visible window
      const combinedTot  = actualSlice.concat(FV);
      const normalizedTot= stt.norm ? normalizeStrict(combinedTot, 0) : combinedTot;

      const actualPlot   = normalizedTot.slice(0, actualSlice.length);
      const forecastPlot = normalizedTot.slice(actualSlice.length);

      const ds = [];
      ds.push({
        _key:'total',
        label:'VNV vísitala',
        data: actualPlot.concat(Array(FL.length).fill(null)),
        borderWidth: 2, tension: 0, spanGaps: false,
        pointRadius: 2, pointHoverRadius: 4,
        hidden: !stt.activeKeys.has('total')
      });
      ds.push({
        _key:'forecast',
        label:'Spáð þróun',
        data: Array(actualPlot.length).fill(null).concat(forecastPlot),
        borderDash:[6,4], borderWidth:2, tension:0, spanGaps:false,
        pointRadius:2, pointHoverRadius:4, pointHitRadius:6,
        hidden: !stt.activeKeys.has('forecast')
      });

      // Sub-series over the same window; flexible normalization from start (0)
      const fullLen = FULL.length;
      meta.forEach(m=>{
        const raw = subs[m.code] || [];
        const padded  = raw.length < fullLen ? Array(fullLen - raw.length).fill(null).concat(raw) : raw;
        const visible = padded.slice(st, st + n); // align with visible window
        const combined= visible.concat(Array(FL.length).fill(null));
        const plot    = stt.norm ? normalizeFromIndex(combined, 0) : combined;
        ds.push({
          _key:`sub:${m.code}`,
          label: m.label,
          data: plot,
          borderWidth: 2, tension: 0, spanGaps: false,
          pointRadius: 0,
          hidden: !stt.activeKeys.has(`sub:${m.code}`)
        });
      });

      chart.data.labels   = lab;
      chart.data.datasets = ds;

      chart.options.plugins.tooltip = makeTooltipConfig(lab, (_lbl, dsIdx) => {
        const d = chart.data.datasets[dsIdx];
        if (!d) return null;
        if (d._key === 'total' || d._key === 'forecast') return normalizedTot;
        return d.data;
      });

      // Slider shows FULL label at the window start
      setWindowUI(canvasId, totalMonths, n, stt.startAbs, i => fullLabelAt(canvasId, i));
      chart.update();
    }

    // Range buttons: set length and snap start to the latest n months (like before)
    hookRangeButtons(canvasId, key => {
      chart.$state.rangeKey = key;
      const n = monthsForRange(key, FULL.length);
      chart.$state.startAbs = Math.max(0, FULL.length - n);
      rebuild();
    }, initialRange);

    // Slider now moves the window start (same semantics as range buttons)
    attachControls(canvasId, {
      onToggle: on => { chart.$state.norm = on; rebuild(); },
      onStartChange: i => { chart.$state.startAbs = i; rebuild(); },
      onHoverPreview: i => fullLabelAt(canvasId, i)
    });

    rebuild();

    global.EconCharts = global.EconCharts || {};
    (global.EconCharts.DEBUG ||= {})[canvasId] = { kind:'cpi', chart, state: chart.$state, meta, subs, FULL };
    return chart;
  }

  // -------- Generic chart (Wages / BCI / PPI), with optional sub overlays
  function initLineForecastChart(canvasId, params){
    const FULL = params.fullLabels || params.labels || [];
    const VALL = params.fullValues || params.values || [];
    const FL   = params.futLabels  || [];
    const FV   = params.futValues  || [];
    const meta = params.subMeta    || [];
    const subs = params.subSeries  || {};
    const initialRange = params.initialRange || '2y';

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;
    setFullLabelLookup(canvasId, FULL);

    const chart = new Chart(ctx, {
      type: 'line',
      data: { labels: [], datasets: [] },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode:'nearest', intersect:false },
        plugins: {
          legend:{ position:'bottom', onClick: defaultLegendOnClick },
          tooltip: makeTooltipConfig([], () => null)
        },
        scales: { x:{ ticks:{ maxRotation:0, autoSkip:true, maxTicksLimit:12 } }, y:{ beginAtZero:false } }
      },
      plugins: [hoverYearMarker]
    });

    chart.$state = {
      rangeKey: initialRange,
      startAbs: Math.max(0, FULL.length - monthsForRange(initialRange, FULL.length)),
      norm: false,
      activeKeys: new Set(['main','forecast'])
    };

    function rebuild(){
      const stt = chart.$state;

      const total = FULL.length;
      const n  = monthsForRange(stt.rangeKey, total);
      stt.startAbs = Math.min(Math.max(0, stt.startAbs|0), Math.max(0, total - n));

      const st = stt.startAbs;
      const L  = FULL.slice(st, st + n);
      const lab= L.concat(FL);
      const actualSlice = VALL.slice(st, st + n);

      const combinedMain   = actualSlice.concat(FV);
      const normalizedMain = stt.norm ? normalizeStrict(combinedMain, 0) : combinedMain;

      const actualPlot   = normalizedMain.slice(0, actualSlice.length);
      const forecastPlot = normalizedMain.slice(actualSlice.length);

      const ds = [];
      ds.push({
        _key:'main',
        label:'Þróun',
        data: actualPlot.concat(Array(FL.length).fill(null)),
        borderWidth: 2, tension: 0, spanGaps: false,
        pointRadius: 2, pointHoverRadius: 4,
        hidden: !stt.activeKeys.has('main')
      });
      ds.push({
        _key:'forecast',
        label:'Spá',
        data: Array(actualPlot.length).fill(null).concat(forecastPlot),
        borderDash:[6,4], borderWidth:2, tension:0, spanGaps:false,
        pointRadius:2, pointHoverRadius:4, pointHitRadius:6,
        hidden: !stt.activeKeys.has('forecast')
      });

      // Optional sub overlays aligned to the same window
      const fullLen = FULL.length;
      meta.forEach(m=>{
        const raw = subs[m.code] || [];
        const padded  = raw.length < fullLen ? Array(fullLen - raw.length).fill(null).concat(raw) : raw;
        const visible = padded.slice(st, st + n);
        const combined= visible.concat(Array(FL.length).fill(null));
        const plot    = stt.norm ? normalizeFromIndex(combined, 0) : combined;
        ds.push({
          _key:`sub:${m.code}`,
          label: m.label,
          data: plot,
          borderWidth: 2, tension: 0, spanGaps: false,
          pointRadius: 0,
          hidden: !stt.activeKeys.has(`sub:${m.code}`)
        });
      });

      chart.data.labels   = lab;
      chart.data.datasets = ds;

      chart.options.plugins.tooltip = makeTooltipConfig(lab, (_lbl, dsIdx) => {
        const d = chart.data.datasets[dsIdx];
        if (!d) return null;
        if (d._key === 'main' || d._key === 'forecast') return normalizedMain;
        return d.data;
      });

      setWindowUI(canvasId, total, n, stt.startAbs, i => fullLabelAt(canvasId, i));
      chart.update();
    }

    hookRangeButtons(canvasId, key => {
      chart.$state.rangeKey = key;
      const n = monthsForRange(key, FULL.length);
      chart.$state.startAbs = Math.max(0, FULL.length - n);
      rebuild();
    }, initialRange);

    attachControls(canvasId, {
      onToggle: on => { chart.$state.norm = on; rebuild(); },
      onStartChange: i => { chart.$state.startAbs = i; rebuild(); },
      onHoverPreview: i => fullLabelAt(canvasId, i)
    });

    rebuild();

    global.EconCharts = global.EconCharts || {};
    (global.EconCharts.DEBUG ||= {})[canvasId] = { kind:'generic', chart, state: chart.$state, meta, subs, FULL };
    return chart;
  }

  // Thin alias
  function initWageChart(canvasId, opts){ return initLineForecastChart(canvasId, opts); }

  // Export
  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart, initLineForecastChart, initWageChart
  });
})(window);
