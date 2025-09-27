// static/js/charts.js  (v19)
(function (global) {
  console.log("charts.js loaded 19");

  const fmt = v => v == null ? '—' : Number(v).toLocaleString('is-IS', { maximumFractionDigits: 2 });
  const pct = (a, b) => (a == null || b == null || b === 0) ? null : (a / b - 1) * 100;

  function getCtx(id){ const c=document.getElementById(id); return c ? c.getContext('2d') : null; }

  function applyVisibility(chart){
    // Ensure we have a state bag
    const active = chart.$state?.activeKeys || new Set();
    (chart.data.datasets || []).forEach((d, i) => {
      const key = d && d._key;
      const visible = key ? active.has(key) : true;   // no key => treat as visible
      const meta = chart.getDatasetMeta(i);
      // Chart interprets null as visible, true as hidden
      meta.hidden = visible ? null : true;
    });
  }


  function defaultLegendOnClick(e, legendItem, legend){
    const def = Chart.defaults.plugins.legend.onClick;
    def && def.call(this, e, legendItem, legend);      // let Chart.js toggle

    const ci   = legend.chart;
    const idx  = legendItem.datasetIndex;
    const meta = ci.getDatasetMeta(idx);
    const ds   = ci.data.datasets?.[idx];
    const key  = ds && ds._key;
    if (!key || !ci.$state) return;

    // meta.hidden === true -> currently hidden; false/null -> visible
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
      const j = i - 12, x = chart.scales.x, top = chart.chartArea.top, bot = chart.chartArea.bottom, ctx = chart.ctx;
      ctx.save();
      if (j >= 0){ ctx.setLineDash([5,4]); ctx.strokeStyle='rgba(255,255,255,.35)'; ctx.lineWidth=1;
        ctx.beginPath(); ctx.moveTo(x.getPixelForValue(j),top); ctx.lineTo(x.getPixelForValue(j),bot); ctx.stroke();
      }
      ctx.setLineDash([]); ctx.strokeStyle='rgba(255,255,255,.6)';
      ctx.beginPath(); ctx.moveTo(x.getPixelForValue(i),top); ctx.lineTo(x.getPixelForValue(i),bot); ctx.stroke();
      ctx.restore();
    }
  };

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
          const mom = pct(curr, prev); if (mom != null) lines.push(`Mánaðarbreyting: ${mom.toFixed(2)}%`);
          const j = idx - 12, prev12 = j >= 0 ? s[j] : null;
          const yoy = pct(curr, prev12); if (yoy != null) lines.push(`Ársbreyting: ${yoy.toFixed(2)}% (vs ${labels[j]})`);
          return lines;
        }
      }
    };
  }

  // ---- Normalization + range helpers
  const normalizeToBase = (arr, baseIdx) => {
    if (!Array.isArray(arr) || baseIdx == null) return arr;
    const base = arr[baseIdx];
    if (base == null || base === 0) return arr.map(_ => null);
    return arr.map(v => (v == null ? null : 100 * (v / base)));
  };
  const monthsForRange = (key, total) =>
    key === 'all' ? total : key === '10y' ? Math.min(total,120)
    : key === '5y' ? Math.min(total,60) : Math.min(total,24);

  function hookRangeButtons(canvasId, onChange, initial='2y'){
    const box = document.querySelector(`.range-controls[data-chart="${canvasId}"]`);
    if (!box) { onChange(initial); return; }
    const activate = key => box.querySelectorAll('button').forEach(b=>b.classList.toggle('is-active', b.dataset.range===key));
    box.addEventListener('click', e=>{
      const b = e.target.closest('button[data-range]'); if (!b) return;
      const key = b.dataset.range; activate(key); onChange(key);
    });
    activate(initial); onChange(initial);
  }

  function attachNormalizeControls(canvasId, onToggle, onBaseChange, onHoverPreview){
    const toggle = document.getElementById(`${canvasId}-norm-toggle`);
    const slider = document.getElementById(`${canvasId}-base-slider`);
    const label  = document.getElementById(`${canvasId}-base-label`);
    if (toggle) toggle.addEventListener('change', e => onToggle(!!e.target.checked));
    if (slider) {
      slider.addEventListener('input', e => { onBaseChange(+e.target.value); });
      const updateTitle = (i)=>{ const t = onHoverPreview?.(i) || '—'; slider.title = t; if (label) label.textContent = t; };
      slider.addEventListener('mousemove', e => {
        const r = slider.getBoundingClientRect(), ratio = Math.min(1, Math.max(0,(e.clientX-r.left)/r.width));
        const i = Math.round(ratio * (+slider.max || 0)); updateTitle(i);
      });
      slider.addEventListener('mouseleave', ()=> updateTitle(+slider.value || 0));
    }
  }
  function setNormalizeUI(canvasId, maxBaseIdx, currBaseIdx, labelAt){
    const slider = document.getElementById(`${canvasId}-base-slider`);
    const label  = document.getElementById(`${canvasId}-base-label`);
    if (!slider || !label) return;
    slider.max   = Math.max(0, maxBaseIdx|0);
    slider.value = Math.min(currBaseIdx|0, +slider.max);
    const txt = labelAt(slider.value) || '—';
    label.textContent = txt; slider.title = txt;
  }

  // label lookup for slider hover text
  function setLabelLookup(id, labels){
    global.EconCharts = global.EconCharts || {};
    (global.EconCharts._labelLookup ||= {})[id] = labels;
  }
  function labelAt(canvasId, idx){
    const L = global.EconCharts?._labelLookup?.[canvasId] || [];
    return L[idx] || '—';
  }

  // Snapshot visible datasets BEFORE rebuild: look at meta.hidden
  function snapshotActiveKeys(chart){
    const set = new Set();
    (chart.data.datasets || []).forEach((d, i) => {
      if (!d || !d._key) return;
      const hidden = chart.getDatasetMeta(i)?.hidden === true;
      if (!hidden) set.add(d._key);
    });
    return set;
  }

  // ------------------ CPI (total + forecast + sub overlays) ------------------
  function initCPIChart(canvasId, { fullLabels, fullValues, futLabels, futValues, subMeta, subSeries, initialRange='2y' }){
    const L  = fullLabels || [];
    const V  = fullValues || [];
    const FL = futLabels   || [];
    const FV = futValues   || [];
    const meta = subMeta   || [];
    const subs = subSeries || {};

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;

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
      norm: false,
      baseIdx: 0,
      activeKeys: new Set(['total','forecast'])
    };

    function rebuild(reason){
      // preserve current visible keys unless first init
      if (reason !== 'init') {
        const prev = snapshotActiveKeys(chart);
        if (prev.size) chart.$state.activeKeys = prev;
      }

      const stt = chart.$state;

      const totalMonths = L.length;
      const n  = monthsForRange(stt.rangeKey, totalMonths);
      const st = Math.max(0, totalMonths - n);
      const lab = L.slice(st).concat(FL);
      setLabelLookup(canvasId, lab);

      if ((reason === 'range' || reason === 'toggle') && stt.norm) stt.baseIdx = 0;

      const maxBase = Math.max(0, lab.length - FL.length - 1);
      if (stt.baseIdx > maxBase) stt.baseIdx = maxBase;

      const actualSlice  = V.slice(st);
      const combinedTot  = actualSlice.concat(FV);
      const normalizedTot= stt.norm ? normalizeToBase(combinedTot, stt.baseIdx) : combinedTot;

      const actualPlot   = normalizedTot.slice(0, actualSlice.length);
      const forecastPlot = normalizedTot.slice(actualSlice.length);

      const ds = [];
      ds.push({
        _key:'total',
        label:'VNV vísitala',
        data: actualPlot.concat(Array(FL.length).fill(null)),
        borderWidth:2, tension:.2, pointRadius:2, pointHoverRadius:4,
        hidden: !stt.activeKeys.has('total')
      });
      ds.push({
        _key:'forecast',
        label:'Spáð þróun',
        data: Array(actualPlot.length).fill(null).concat(forecastPlot),
        borderDash:[6,4], borderWidth:2, tension:.2, pointRadius:2, pointHoverRadius:4, pointHitRadius:6,
        hidden: !stt.activeKeys.has('forecast')
      });

      const fullLen = L.length;
      meta.forEach(m=>{
        const raw = subs[m.code] || [];
        const padded  = raw.length < fullLen ? Array(fullLen - raw.length).fill(null).concat(raw) : raw;
        const visible = padded.slice(st, st + actualSlice.length);
        const combined= visible.concat(Array(FL.length).fill(null));
        const plot    = stt.norm ? normalizeToBase(combined, stt.baseIdx) : combined;
        ds.push({
          _key:`sub:${m.code}`,
          label: m.label,
          data: plot,
          borderWidth:2, tension:.2, pointRadius:0,
          hidden: !stt.activeKeys.has(`sub:${m.code}`)
        });
      });

      chart.data.labels   = lab;
      chart.data.datasets = ds;

      chart.options.plugins.tooltip = makeTooltipConfig(lab, (lbl, dsIdx) => {
        const d = chart.data.datasets[dsIdx];
        if (!d) return null;
        if (d._key === 'total' || d._key === 'forecast') return normalizedTot;
        return d.data; // sub
      });

      applyVisibility(chart);
      setNormalizeUI(canvasId, maxBase, stt.baseIdx, i => lab[i] || '—');
      chart.update();
    }

    hookRangeButtons(canvasId, key => { chart.$state.rangeKey = key; rebuild('range'); }, initialRange);
    attachNormalizeControls(
      canvasId,
      on => { chart.$state.norm = on; rebuild('toggle'); },
      i  => { chart.$state.baseIdx = i; rebuild('slider'); },
      i  => labelAt(canvasId, i)
    );

    rebuild('init');

    // Debug handle
    global.EconCharts = global.EconCharts || {};
    (global.EconCharts.DEBUG ||= {})[canvasId] = { kind:'cpi', chart, state: chart.$state, meta, subs };
    return chart;
  }

  // -------- Generic chart (Wages / BCI / PPI), supports optional sub overlays --------
  function initLineForecastChart(canvasId, params){
    const L   = params.fullLabels || params.labels || [];
    const V   = params.fullValues || params.values || [];
    const FL  = params.futLabels  || [];
    const FV  = params.futValues  || [];
    const meta= params.subMeta    || [];
    const subs= params.subSeries  || {};
    const initialRange = params.initialRange || '2y';

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;

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
      norm: false,
      baseIdx: 0,
      activeKeys: new Set(['main','forecast'])
    };

    function rebuild(reason){
      if (reason !== 'init') {
        const prev = snapshotActiveKeys(chart);
        if (prev.size) chart.$state.activeKeys = prev;
      }

      const stt = chart.$state;

      const total = L.length;
      const n  = monthsForRange(stt.rangeKey, total);
      const st = Math.max(0, total - n);
      const lab= L.slice(st).concat(FL);

      if ((reason === 'range' || reason === 'toggle') && stt.norm) stt.baseIdx = 0;

      const maxBase = Math.max(0, lab.length - FL.length - 1);
      if (stt.baseIdx > maxBase) stt.baseIdx = maxBase;

      const actualSlice   = V.slice(st);
      const combinedMain  = actualSlice.concat(FV);
      const normalizedMain= stt.norm ? normalizeToBase(combinedMain, stt.baseIdx) : combinedMain;

      const actualPlot    = normalizedMain.slice(0, actualSlice.length);
      const forecastPlot  = normalizedMain.slice(actualSlice.length);

      const ds = [];
      ds.push({
        _key:'main',
        label:'Þróun',
        data: actualPlot.concat(Array(FL.length).fill(null)),
        borderWidth:2, tension:.25, pointRadius:2, pointHoverRadius:4,
        hidden: !stt.activeKeys.has('main')
      });
      ds.push({
        _key:'forecast',
        label:'Spá',
        data: Array(actualPlot.length).fill(null).concat(forecastPlot),
        borderDash:[6,4], borderWidth:2, tension:.25, pointRadius:2, pointHoverRadius:4, pointHitRadius:6,
        hidden: !stt.activeKeys.has('forecast')
      });

      const fullLen = L.length;
      meta.forEach(m=>{
        const raw = subs[m.code] || [];
        const padded  = raw.length < fullLen ? Array(fullLen - raw.length).fill(null).concat(raw) : raw;
        const visible = padded.slice(st, st + actualSlice.length);
        const combined= visible.concat(Array(FL.length).fill(null));
        const plot    = stt.norm ? normalizeToBase(combined, stt.baseIdx) : combined;
        ds.push({
          _key:`sub:${m.code}`,
          label: m.label,
          data: plot,
          borderWidth:2, tension:.25, pointRadius:0,
          hidden: !stt.activeKeys.has(`sub:${m.code}`)
        });
      });

      chart.data.labels   = lab;
      chart.data.datasets = ds;

      chart.options.plugins.tooltip = makeTooltipConfig(lab, (lbl, dsIdx) => {
        const d = chart.data.datasets[dsIdx];
        if (!d) return null;
        if (d._key === 'main' || d._key === 'forecast') return normalizedMain;
        return d.data;
      });

      applyVisibility(chart);
      setLabelLookup(canvasId, lab);
      chart.update();
    }

    hookRangeButtons(canvasId, key => { chart.$state.rangeKey = key; rebuild('range'); }, initialRange);
    attachNormalizeControls(
      canvasId,
      on => { chart.$state.norm = on; rebuild('toggle'); },
      i  => { chart.$state.baseIdx = i; rebuild('slider'); },
      i  => labelAt(canvasId, i)
    );

    rebuild('init');

    global.EconCharts = global.EconCharts || {};
    (global.EconCharts.DEBUG ||= {})[canvasId] = { kind:'generic', chart, state: chart.$state, meta, subs };
    return chart;
  }

  function initWageChart(canvasId, opts){ return initLineForecastChart(canvasId, opts); }

  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart, initLineForecastChart, initWageChart
  });
})(window);
