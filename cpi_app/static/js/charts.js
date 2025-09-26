// static/js/charts.js   (v16)
console.log("charts.js v16 loaded");
(function (global) {
  const fmt = v => v == null ? '—' : Number(v).toLocaleString('is-IS', { maximumFractionDigits: 2 });
  const pct = (a, b) => (a == null || b == null || b === 0) ? null : (a / b - 1) * 100;

  function getCtx(id){ const c=document.getElementById(id); return c ? c.getContext('2d') : null; }

  // Vertical guide lines on hover (now-ish and 12m earlier)
  const hoverYearMarker = {
    id: 'hoverYearMarker',
    afterDraw(chart){
      const t = chart.tooltip;
      if (!t || !t.getActiveElements().length) return;
      const i = t.getActiveElements()[0].index;
      const j = i - 12, x = chart.scales.x, top = chart.chartArea.top, bot = chart.chartArea.bottom, ctx = chart.ctx;
      ctx.save();
      if (j >= 0){ ctx.setLineDash([5,4]); ctx.strokeStyle = 'rgba(255,255,255,.35)';
        ctx.beginPath(); ctx.moveTo(x.getPixelForValue(j), top); ctx.lineTo(x.getPixelForValue(j), bot); ctx.stroke();
      }
      ctx.setLineDash([]); ctx.strokeStyle = 'rgba(255,255,255,.6)';
      ctx.beginPath(); ctx.moveTo(x.getPixelForValue(i), top); ctx.lineTo(x.getPixelForValue(i), bot); ctx.stroke();
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
          const s = seriesFor(items[0].dataset.label);
          if (!s) return [];
          const curr = s[idx], prev = idx>0 ? s[idx-1] : null;
          const out = [];
          const mom = pct(curr, prev); if (mom != null) out.push(`Mánaðarbreyting: ${mom.toFixed(2)}%`);
          const j = idx - 12, prev12 = j >= 0 ? s[j] : null;
          const yoy = pct(curr, prev12); if (yoy != null) out.push(`Ársbreyting: ${yoy.toFixed(2)}% (vs ${labels[j]})`);
          return out;
        }
      }
    };
  }

  function monthsForRange(key, total){
    if (key === 'all') return total;
    if (key === '10y') return Math.min(total, 120);
    if (key === '5y')  return Math.min(total, 60);
    return Math.min(total, 24);
  }

  function hookRangeButtons(canvasId, setRange, initial='2y'){
    const box = document.querySelector(`.range-controls[data-chart="${canvasId}"]`);
    if (!box) return;
    function activate(k){ box.querySelectorAll('button').forEach(b => b.classList.toggle('is-active', b.dataset.range===k)); }
    box.addEventListener('click', (e)=>{
      const b = e.target.closest('button[data-range]'); if (!b) return;
      const k = b.dataset.range; activate(k); setRange(k);
    });
    activate(initial); setRange(initial);
  }

  // -------------------- CPI (full + sub-series) --------------------
  function initCPIChart(canvasId, { fullLabels, fullValues, futLabels, futValues, subMeta, subSeries, initialRange='2y' }){
    const L  = fullLabels || [];
    const V  = fullValues || [];
    const FL = futLabels   || [];
    const FV = futValues   || [];
    const meta = subMeta   || [];
    const subs = subSeries || {};

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;

    // Precompute full-length combined series (for tooltips)
    const totalCombined = V.concat(FV);

    // Ensure every sub-series is right-aligned to the full timeline length (front-pad with nulls if short)
    const subCombined = Object.fromEntries(
      meta.map(m => {
        const raw = subs[m.code] || [];
        const pad = L.length > raw.length ? Array(L.length - raw.length).fill(null).concat(raw) : raw.slice(-L.length);
        return [m.code, pad.concat(Array(FL.length).fill(null))];
      })
    );

    const chart = new Chart(ctx, {
      type: 'line',
      data: { labels: [], datasets: [] },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'nearest', intersect: false },
        plugins: { legend: { position: 'bottom' }, tooltip: makeTooltipConfig([], label=>{
          if (label === 'VNV vísitala' || label === 'Spáð þróun') return totalCombined;
          const m = meta.find(x => x.label === label); return m ? subCombined[m.code] : null;
        })},
        scales: { x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } }, y: { beginAtZero: false } }
      },
      plugins: [hoverYearMarker]
    });

    function rebuild(rangeKey){
      const n   = monthsForRange(rangeKey, L.length);
      const st  = Math.max(0, L.length - n);
      const lab = L.slice(st).concat(FL);

      const vSlice = V.slice(st);
      const ds = [
        { label:'VNV vísitala',
          data: vSlice.concat(Array(FL.length).fill(null)),
          borderWidth:2, tension:.2, pointRadius:2, pointHoverRadius:4 },
        { label:'Spáð þróun',
          data: Array(vSlice.length).fill(null).concat(FV),
          borderDash:[6,4], borderWidth:2, tension:.2, pointRadius:2, pointHoverRadius:4, pointHitRadius:6 }
      ];

      // sub-series: slice the already padded arrays
      meta.forEach(m=>{
        const padded = subCombined[m.code].slice(0, L.length); // drop forecast nulls for base
        const base   = padded.slice(st);
        ds.push({
          label:m.label,
          data: base.concat(Array(FL.length).fill(null)),
          borderWidth:2, tension:.2, pointRadius:0, hidden:true
        });
      });

      chart.data.labels   = lab;
      chart.data.datasets = ds;

      chart.options.plugins.tooltip = makeTooltipConfig(lab, label=>{
        if (label === 'VNV vísitala' || label === 'Spáð þróun') return vSlice.concat(FV);
        const m = meta.find(x => x.label === label);
        if (!m) return null;
        const padded = subCombined[m.code].slice(0, L.length);
        return padded.slice(st).concat(Array(FL.length).fill(null));
      });

      chart.update();
    }

    hookRangeButtons(canvasId, rebuild, initialRange);

    // debug hook
    global.EconCharts = global.EconCharts || {};
    (global.EconCharts.DEBUG ||= {})[canvasId] = {
      fullLabels:L, fullValues:V, futLabels:FL, futValues:FV,
      subMeta:meta, subLens:Object.fromEntries(Object.entries(subs).map(([k,v])=>[k, (v||[]).length]))
    };
    return chart;
  }

  // -------- Generic line + forecast (Wages / BCI / PPI) --------
  function initLineForecastChart(canvasId, params){
    const L  = params.fullLabels || params.labels || [];
    const V  = params.fullValues || params.values || [];
    const FL = params.futLabels  || [];
    const FV = params.futValues  || [];
    const initialRange = params.initialRange || '2y';

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;

    const chart = new Chart(ctx, {
      type: 'line',
      data: { labels: [], datasets: [] },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'nearest', intersect: false },
        plugins: { legend: { position: 'bottom' }, tooltip: makeTooltipConfig([], () => null) },
        scales: { x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } }, y: { beginAtZero: false } }
      },
      plugins: [hoverYearMarker]
    });

    function rebuild(rangeKey){
      const n   = monthsForRange(rangeKey, L.length);
      const st  = Math.max(0, L.length - n);
      const lab = L.slice(st).concat(FL);

      const vSlice = V.slice(st);
      const combined = vSlice.concat(FV);

      chart.data.labels   = lab;
      chart.data.datasets = [
        { label:'Þróun', data: vSlice.concat(Array(FL.length).fill(null)), borderWidth:2, tension:.25, pointRadius:2, pointHoverRadius:4 },
        { label:'Spá',   data: Array(vSlice.length).fill(null).concat(FV), borderDash:[6,4], borderWidth:2, tension:.25, pointRadius:2, pointHoverRadius:4, pointHitRadius:6 }
      ];
      chart.options.plugins.tooltip = makeTooltipConfig(lab, () => combined);
      chart.update();
    }

    hookRangeButtons(canvasId, rebuild, initialRange);

    // debug
    global.EconCharts = global.EconCharts || {};
    (global.EconCharts.DEBUG ||= {})[canvasId] = {fullLabels:L, fullValues:V, futLabels:FL, futValues:FV};
    return chart;
  }

  // Thin alias
  function initWageChart(id, opts){ return initLineForecastChart(id, opts); }

  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart, initLineForecastChart, initWageChart
  });
})(window);
