// cpi_app/static/js/charts.js
console.log("charts.js v14 loaded");

(function (global) {
  const fmt = v => v != null ? Number(v).toLocaleString('is-IS', { maximumFractionDigits: 2 }) : '—';
  const pct = (curr, prev) => (curr == null || prev == null || prev === 0) ? null : (curr / prev - 1) * 100;

  function getCtx(id) {
    const el = document.getElementById(id);
    if (!el) { console.warn(`[charts] canvas #${id} not found`); return null; }
    return el.getContext('2d');
  }

  // vertical guide (current solid, t-12 dashed)
  const hoverYearMarker = {
    id: 'hoverYearMarker',
    afterDraw(chart) {
      const t = chart.tooltip;
      if (!t || !t.getActiveElements().length) return;
      const i = t.getActiveElements()[0].index;
      const j = i - 12, x = chart.scales.x, ctx = chart.ctx;
      const yTop = chart.chartArea.top, yBot = chart.chartArea.bottom;

      ctx.save();
      if (j >= 0) {
        ctx.setLineDash([5,4]);
        ctx.strokeStyle = 'rgba(255,255,255,.35)';
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(x.getPixelForValue(j), yTop); ctx.lineTo(x.getPixelForValue(j), yBot); ctx.stroke();
      }
      ctx.setLineDash([]);
      ctx.strokeStyle = 'rgba(255,255,255,.6)';
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(x.getPixelForValue(i), yTop); ctx.lineTo(x.getPixelForValue(i), yBot); ctx.stroke();
      ctx.restore();
    }
  };

  function makeTooltipConfig(labels, seriesFor) {
    return {
      displayColors: false,
      callbacks: {
        title(items){ return items && items[0] ? items[0].label : ''; },
        label(ctx){ return `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}`; },
        afterBody(items){
          if (!items || !items.length) return [];
          const idx = items[0].dataIndex;
          const s = seriesFor(items[0].dataset.label);
          if (!s) return [];
          const curr = s[idx], prev = idx>0 ? s[idx-1] : null;
          const lines = [];
          const mom = pct(curr, prev); if (mom != null) lines.push(`Mánaðarbreyting: ${mom.toFixed(2)}%`);
          const j = idx-12, prev12 = j>=0 ? s[j] : null;
          const yoy = pct(curr, prev12); if (yoy != null) lines.push(`Ársbreyting: ${yoy.toFixed(2)}% (vs ${labels[j]})`);
          return lines;
        }
      }
    };
  }

  // ---- range helpers (2y/5y/10y/all) ----
  function monthsFor(key, allLen){
    if (key === '2y') return 24;
    if (key === '5y') return 60;
    if (key === '10y') return 120;
    return allLen; // 'all' or fallback
  }
  function sliceForRange(fullL, fullV, FL, FV, key){
    const n = monthsFor(key, fullL.length);
    const L  = fullL.slice(-n);
    const V  = fullV.slice(-n);
    return {
      labels: L.concat(FL || []),
      values: V,
      forecastLabels: FL || [],
      forecastValues: FV || []
    };
  }
  function wireRangeButtons(chartId, chart, opts){
    const box = document.querySelector(`.range-controls[data-chart="${chartId}"]`);
    if (!box || !opts.fullLabels) return; // no range controls on page
    const { fullLabels, fullValues, futLabels, futValues, initialRange } = opts;
    const buttons = Array.from(box.querySelectorAll('[data-range]'));

    function setActive(btn){
      buttons.forEach(b=>b.classList.toggle('is-active', b===btn));
    }
    function apply(rangeKey){
      const s = sliceForRange(fullLabels, fullValues, futLabels, futValues, rangeKey);
      chart.data.labels = s.labels;
      // dataset[0]=actuals, dataset[1]=forecast
      chart.data.datasets[0].data = s.values.concat(Array(s.forecastLabels.length).fill(null));
      chart.data.datasets[1].data = Array(s.values.length).fill(null).concat(s.forecastValues);
      chart.update();
    }

    // init
    const initKey = initialRange || '2y';
    apply(initKey);
    const initBtn = buttons.find(b => b.dataset.range === initKey);
    if (initBtn) setActive(initBtn);

    buttons.forEach(btn => {
      btn.addEventListener('click', () => {
        apply(btn.dataset.range);
        setActive(btn);
      });
    });
  }

  // ---------- CPI (supports sub-series + full history) ----------
  function initCPIChart(canvasId, { labels, values, futLabels, futValues, subMeta, subSeries, fullLabels, fullValues, initialRange }){
    const fallbackL = labels || [], fallbackV = values || [];
    const FL = futLabels || [], FV = futValues || [];
    const hasFull = Array.isArray(fullLabels) && Array.isArray(fullValues);

    // start with last 24 if no full history given
    let L = fallbackL, V = fallbackV;
    if (hasFull) {
      const s = sliceForRange(fullLabels, fullValues, FL, FV, initialRange || '2y');
      L = s.labels.slice(0, s.labels.length - FL.length);
      V = s.values;
    }

    const labelsAll = L.concat(FL);
    const totalCombined = V.concat(FV);

    const datasets = [
      { label:'VNV vísitala', data: V.concat(Array(FL.length).fill(null)), borderWidth:2, tension:.2, pointRadius:2, pointHoverRadius:4 },
      { label:'Spáð þróun',   data: Array(V.length).fill(null).concat(FV), borderDash:[6,4], borderWidth:2, tension:.2, pointRadius:2, pointHoverRadius:4, pointHitRadius:6 }
    ];
    (subMeta || []).forEach(m=>{
      const base = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      datasets.push({ label:m.label, data: base.concat(Array(FL.length).fill(null)), borderWidth:2, tension:.2, pointRadius:0, hidden:true });
    });

    function seriesFor(label){
      if (label==='VNV vísitala' || label==='Spáð þróun') return totalCombined;
      const m = (subMeta||[]).find(x=>x.label===label);
      if (!m) return null;
      const base = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      return base.concat(Array(FL.length).fill(null));
    }

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart==='undefined') return null;

    const chart = new Chart(ctx, {
      type:'line',
      data:{ labels: labelsAll, datasets },
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{ mode:'nearest', intersect:false },
        plugins:{ legend:{ position:'bottom' }, tooltip: makeTooltipConfig(labelsAll, seriesFor) },
        scales:{ x:{ ticks:{ maxRotation:0, autoSkip:true, maxTicksLimit:12 }}, y:{ beginAtZero:false } }
      },
      plugins:[hoverYearMarker]
    });

    // range controls if full history provided
    if (hasFull) {
      wireRangeButtons(canvasId, chart, { fullLabels, fullValues, futLabels:FL, futValues:FV, initialRange });
    }
    return chart;
  }

  // ---------- Generic (BCI/PPI/Wages detail) with full history ----------
  function initLineForecastChart(canvasId, opts){
    const { labels, values, futLabels, futValues, fullLabels, fullValues, initialRange } = opts || {};
    const FL = futLabels || [], FV = futValues || [];
    const hasFull = Array.isArray(fullLabels) && Array.isArray(fullValues);

    let L = labels || [], V = values || [];
    if (hasFull) {
      const s = sliceForRange(fullLabels, fullValues, FL, FV, initialRange || '2y');
      // for initial render we need labels incl. forecast, and actuals only for first dataset
      L = s.labels.slice(0, s.labels.length - FL.length);
      V = s.values;
    }

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart==='undefined') return null;

    const chart = new Chart(ctx, {
      type:'line',
      data:{
        labels: L.concat(FL),
        datasets:[
          { label:'Þróun', data: V.concat(Array(FL.length).fill(null)), borderWidth:2, tension:.25, pointRadius:2, pointHoverRadius:4 },
          { label:'Spá',   data: Array(V.length).fill(null).concat(FV), borderDash:[6,4], borderWidth:2, tension:.25, pointRadius:2, pointHoverRadius:4, pointHitRadius:6 }
        ]
      },
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{ mode:'nearest', intersect:false },
        plugins:{ legend:{ position:'bottom' },
          tooltip: makeTooltipConfig(L.concat(FL), () => V.concat(FV)) },
        scales:{ x:{ ticks:{ maxRotation:0, autoSkip:true, maxTicksLimit:12 }}, y:{ beginAtZero:false } }
      },
      plugins:[hoverYearMarker]
    });

    if (hasFull) {
      wireRangeButtons(canvasId, chart, { fullLabels, fullValues, futLabels:FL, futValues:FV, initialRange });
    }
    return chart;
  }

  // ---------- Wages (keep for homepage); detail pages can use generic ----------
  function initWageChart(canvasId, cfg){ return initLineForecastChart(canvasId, cfg); }

  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart, initWageChart, initLineForecastChart
  });
})(window);
