// static/js/charts.js (v23)
(function (global) {
  console.log("charts.js v23 (dual window)");

  const fmt = v => v == null ? '—' : Number(v).toLocaleString('is-IS', { maximumFractionDigits: 2 });
  const pct = (a,b)=> (a==null||b==null||b===0)?null:(a/b-1)*100;

  const getCtx = id => document.getElementById(id)?.getContext('2d') || null;

  // Legend: keep visibility in our $state.activeKeys
  function defaultLegendOnClick(e, li, legend){
    const def = Chart.defaults.plugins.legend.onClick;
    def && def.call(this, e, li, legend);
    const ci = legend.chart, ds = ci.data.datasets?.[li.datasetIndex];
    const key = ds && ds._key; if (!key || !ci.$state) return;
    const hidden = ci.getDatasetMeta(li.datasetIndex)?.hidden === true;
    if (hidden) ci.$state.activeKeys.delete(key); else ci.$state.activeKeys.add(key);
  }

  // Hover month + t-12 line
  const hoverYearMarker = {
    id: 'hoverYearMarker',
    afterDraw(chart){
      const t = chart.tooltip;
      if (!t || !t.getActiveElements().length) return;
      const i = t.getActiveElements()[0].index, j = i-12;
      const x = chart.scales.x, top = chart.chartArea.top, bot = chart.chartArea.bottom, ctx = chart.ctx;
      ctx.save();
      if (j>=0){ ctx.setLineDash([5,4]); ctx.strokeStyle='rgba(255,255,255,.35)';
        ctx.beginPath(); ctx.moveTo(x.getPixelForValue(j),top); ctx.lineTo(x.getPixelForValue(j),bot); ctx.stroke();
      }
      ctx.setLineDash([]); ctx.strokeStyle='rgba(255,255,255,.6)';
      ctx.beginPath(); ctx.moveTo(x.getPixelForValue(i),top); ctx.lineTo(x.getPixelForValue(i),bot); ctx.stroke();
      ctx.restore();
    }
  };

  function makeTooltipConfig(labels, seriesFor){
    return {
      displayColors:false,
      callbacks:{
        title: items => items?.[0]?.label ?? '',
        label: ctx => `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}`,
        afterBody(items){
          if (!items?.length) return [];
          const idx = items[0].dataIndex, s = seriesFor(items[0].datasetIndex);
          if (!s) return [];
          const curr = s[idx], prev = idx>0 ? s[idx-1] : null;
          const lines = [];
          const m = pct(curr, prev); if (m!=null) lines.push(`Mánaðarbreyting: ${m.toFixed(2)}%`);
          const j = idx-12, prev12 = j>=0 ? s[j] : null;
          const y = pct(curr, prev12); if (y!=null) lines.push(`Ársbreyting: ${y.toFixed(2)}% (vs ${labels[j]})`);
          return lines;
        }
      }
    };
  }

  // ---------- helpers ----------
  const monthsForRange = (key, total) =>
    key==='all'? total : key==='10y'? Math.min(total,120)
    : key==='5y'? Math.min(total,60) : Math.min(total,24);

  // normalize anchoring the FIRST visible sample to 100 (index 0 of array)
  function normalizeTo100AtZero(arr){
    if (!arr?.length) return arr;
    const base = arr[0];
    if (base == null || base === 0) return arr.map(_=>null);
    const out = arr.map(v => v==null ? null : 100*(v/base));
    out[0] = 100; // hard anchor
    return out;
  }

  // label lookup for FULL history (for slider labels)
  function setFullLabels(id, labels){
    (global.EconCharts ||= {})._fullLabels = (global.EconCharts._fullLabels || {});
    global.EconCharts._fullLabels[id] = labels;
  }
  const fullLabelAt = (id, idx) => (global.EconCharts?._fullLabels?.[id]||[])[idx] || '—';

  // window slider plumbing
  function attachWindowControls(canvasId, onStart, onEnd){
    const s = document.getElementById(`${canvasId}-win-start`);
    const e = document.getElementById(`${canvasId}-win-end`);
    const sl= document.getElementById(`${canvasId}-win-start-label`);
    const el= document.getElementById(`${canvasId}-win-end-label`);
    if (s) s.addEventListener('input', ev => onStart(+ev.target.value));
    if (e) e.addEventListener('input', ev => onEnd(+ev.target.value));
    if (s) s.addEventListener('mousemove', ()=> { s.title = sl.textContent = fullLabelAt(canvasId, +s.value); });
    if (e) e.addEventListener('mousemove', ()=> { e.title = el.textContent = fullLabelAt(canvasId, Math.max(0,+e.value-1)); });
  }
  function setWindowUI(canvasId, total, start, end){
    const s = document.getElementById(`${canvasId}-win-start`);
    const e = document.getElementById(`${canvasId}-win-end`);
    const sl= document.getElementById(`${canvasId}-win-start-label`);
    const el= document.getElementById(`${canvasId}-win-end-label`);
    if (!s || !e) return;
    s.min = 0;     s.max = Math.max(0,total-1);
    e.min = 1;     e.max = Math.max(1,total);
    s.value = start; e.value = end;
    if (sl) sl.textContent = fullLabelAt(canvasId, start);
    if (el) el.textContent = fullLabelAt(canvasId, Math.max(0,end-1));
  }

  function hookRangeButtons(canvasId, setWindow, initialKey, total){
    const box = document.querySelector(`.range-controls[data-chart="${canvasId}"]`);
    const buttons = box?.querySelectorAll('.range-buttons button[data-range]');
    function activate(k){ buttons?.forEach(b=>b.classList.toggle('is-active', b.dataset.range===k)); }
    if (buttons) box.addEventListener('click', e=>{
      const b = e.target.closest('button[data-range]'); if (!b) return;
      const key = b.dataset.range;
      // set right handle to latest, left by duration (or 0 for all)
      const end = total; // latest actual
      const n   = monthsForRange(key, total);
      const start = key==='all' ? 0 : Math.max(0, end - n);
      setWindow(start, end); activate(key);
    });
    activate(initialKey);
  }

  // ---------------- CPI ----------------
  function initCPIChart(canvasId, { fullLabels, fullValues, futLabels, futValues, subMeta, subSeries, initialRange='2y' }){
    const FULL = fullLabels || [];
    const VALL = fullValues || [];
    const FL   = futLabels  || [];
    const FV   = futValues  || [];
    const meta = subMeta    || [];
    const subs = subSeries  || {};

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;

    setFullLabels(canvasId, FULL);

    const chart = new Chart(ctx, {
      type:'line',
      data:{ labels:[], datasets:[] },
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{ mode:'nearest', intersect:false },
        plugins:{ legend:{ position:'bottom', onClick: defaultLegendOnClick }, tooltip: makeTooltipConfig([], ()=>null) },
        scales:{ x:{ ticks:{ maxRotation:0, autoSkip:true, maxTicksLimit:12 } }, y:{ beginAtZero:false } }
      },
      plugins:[hoverYearMarker]
    });

    const initialN = monthsForRange(initialRange, FULL.length);
    chart.$state = {
      rangeKey: initialRange,
      startAbs: Math.max(0, FULL.length - initialN), // left handle
      endAbs:   FULL.length,                          // right handle (latest)
      norm: false,
      activeKeys: new Set(['total','forecast'])
    };

    function rebuild(){
      const S = chart.$state;
      const total = FULL.length;

      // clamp
      S.startAbs = Math.min(Math.max(0, S.startAbs), Math.max(0, S.endAbs-1));
      S.endAbs   = Math.max(Math.min(total, S.endAbs), Math.min(total, S.startAbs+1));

      const atEnd = (S.endAbs === total);
      const Lvis  = FULL.slice(S.startAbs, S.endAbs);
      const labels= atEnd ? Lvis.concat(FL) : Lvis;

      // TOTAL
      const actual = VALL.slice(S.startAbs, S.endAbs);
      const combo  = atEnd ? actual.concat(FV) : actual;
      const normed = S.norm ? normalizeTo100AtZero(combo) : combo;

      const actualPlot   = normed.slice(0, actual.length);
      const forecastPlot = atEnd ? normed.slice(actual.length) : [];

      const ds = [];
      ds.push({
        _key:'total', label:'VNV vísitala',
        data: actualPlot.concat(atEnd ? Array(FL.length).fill(null) : []),
        borderWidth:2, tension:0, spanGaps:false, pointRadius:2, pointHoverRadius:4,
        hidden: !S.activeKeys.has('total')
      });
      ds.push({
        _key:'forecast', label:'Spáð þróun',
        data: atEnd ? Array(actualPlot.length).fill(null).concat(forecastPlot)
                    : Array(labels.length).fill(null),
        borderDash:[6,4], borderWidth:2, tension:0, spanGaps:false, pointRadius:2, pointHoverRadius:4, pointHitRadius:6,
        hidden: !S.activeKeys.has('forecast')
      });

      // SUBS (aligned to FULL)
      const fullLen = FULL.length;
      meta.forEach(m=>{
        const raw = subs[m.code] || [];
        const padded = raw.length < fullLen ? Array(fullLen - raw.length).fill(null).concat(raw) : raw;
        const vis    = padded.slice(S.startAbs, S.endAbs);
        const combined = atEnd ? vis.concat(Array(FL.length).fill(null)) : vis;
        const plot     = S.norm ? normalizeTo100AtZero(combined) : combined;
        ds.push({
          _key:`sub:${m.code}`, label:m.label, data:plot,
          borderWidth:2, tension:0, spanGaps:false, pointRadius:0,
          hidden: !S.activeKeys.has(`sub:${m.code}`)
        });
      });

      chart.data.labels   = labels;
      chart.data.datasets = ds;
      chart.options.plugins.tooltip = makeTooltipConfig(labels, (dsIdx)=> chart.data.datasets[dsIdx]?.data || null);

      setWindowUI(canvasId, total, S.startAbs, S.endAbs);
      chart.update();
    }

    // Controls wiring
    hookRangeButtons(canvasId, (start,end)=>{ chart.$state.startAbs=start; chart.$state.endAbs=end; rebuild(); }, chart.$state.rangeKey, FULL.length);
    attachWindowControls(canvasId,
      start => { chart.$state.startAbs = Math.min(start, chart.$state.endAbs-1); chart.$state.rangeKey = 'custom'; rebuild(); },
      end   => { chart.$state.endAbs   = Math.max(end,   chart.$state.startAbs+1); chart.$state.rangeKey = 'custom'; rebuild(); }
    );
    const normToggle = document.getElementById(`${canvasId}-norm-toggle`);
    if (normToggle) normToggle.addEventListener('change', e => { chart.$state.norm = !!e.target.checked; rebuild(); });

    rebuild();

    (global.EconCharts ||= {}).DEBUG = { ...(global.EconCharts.DEBUG||{}), [canvasId]:{ kind:'cpi', chart, state:chart.$state } };
    return chart;
  }

  // ------------- Generic (Wages / BCI / PPI) -------------
  function initLineForecastChart(canvasId, params){
    const FULL = params.fullLabels || params.labels || [];
    const VALL = params.fullValues || params.values || [];
    const FL   = params.futLabels  || [];
    const FV   = params.futValues  || [];
    const meta = params.subMeta    || [];
    const subs = params.subSeries  || {};
    const initialRange = params.initialRange || '2y';

    const ctx = getCtx(canvasId); if (!ctx || typeof Chart === 'undefined') return null;

    setFullLabels(canvasId, FULL);

    const chart = new Chart(ctx, {
      type:'line',
      data:{ labels:[], datasets:[] },
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{ mode:'nearest', intersect:false },
        plugins:{ legend:{ position:'bottom', onClick: defaultLegendOnClick }, tooltip: makeTooltipConfig([], ()=>null) },
        scales:{ x:{ ticks:{ maxRotation:0, autoSkip:true, maxTicksLimit:12 } }, y:{ beginAtZero:false } }
      },
      plugins:[hoverYearMarker]
    });

    const initialN = monthsForRange(initialRange, FULL.length);
    chart.$state = {
      rangeKey: initialRange,
      startAbs: Math.max(0, FULL.length - initialN),
      endAbs:   FULL.length,
      norm: false,
      activeKeys: new Set(['main','forecast'])
    };

    function rebuild(){
      const S = chart.$state; const total = FULL.length;
      S.startAbs = Math.min(Math.max(0, S.startAbs), Math.max(0, S.endAbs-1));
      S.endAbs   = Math.max(Math.min(total, S.endAbs), Math.min(total, S.startAbs+1));

      const atEnd = (S.endAbs === total);
      const Lvis  = FULL.slice(S.startAbs, S.endAbs);
      const labels= atEnd ? Lvis.concat(FL) : Lvis;

      const actual = VALL.slice(S.startAbs, S.endAbs);
      const combo  = atEnd ? actual.concat(FV) : actual;
      const normed = S.norm ? normalizeTo100AtZero(combo) : combo;

      const actualPlot   = normed.slice(0, actual.length);
      const forecastPlot = atEnd ? normed.slice(actual.length) : [];

      const ds = [];
      ds.push({
        _key:'main', label:'Þróun',
        data: actualPlot.concat(atEnd ? Array(FL.length).fill(null) : []),
        borderWidth:2, tension:0, spanGaps:false, pointRadius:2, pointHoverRadius:4,
        hidden: !S.activeKeys.has('main')
      });
      ds.push({
        _key:'forecast', label:'Spá',
        data: atEnd ? Array(actualPlot.length).fill(null).concat(forecastPlot)
                    : Array(labels.length).fill(null),
        borderDash:[6,4], borderWidth:2, tension:0, spanGaps:false, pointRadius:2, pointHoverRadius:4, pointHitRadius:6,
        hidden: !S.activeKeys.has('forecast')
      });

      // optional subs
      const fullLen = FULL.length;
      meta.forEach(m=>{
        const raw = subs[m.code] || [];
        const padded = raw.length < fullLen ? Array(fullLen - raw.length).fill(null).concat(raw) : raw;
        const vis    = padded.slice(S.startAbs, S.endAbs);
        const combined = atEnd ? vis.concat(Array(FL.length).fill(null)) : vis;
        const plot     = S.norm ? normalizeTo100AtZero(combined) : combined;
        ds.push({
          _key:`sub:${m.code}`, label:m.label, data:plot,
          borderWidth:2, tension:0, spanGaps:false, pointRadius:0,
          hidden: !S.activeKeys.has(`sub:${m.code}`)
        });
      });

      chart.data.labels   = labels;
      chart.data.datasets = ds;
      chart.options.plugins.tooltip = makeTooltipConfig(labels, (dsIdx)=> chart.data.datasets[dsIdx]?.data || null);

      setWindowUI(canvasId, total, S.startAbs, S.endAbs);
      chart.update();
    }

    hookRangeButtons(canvasId, (start,end)=>{ chart.$state.startAbs=start; chart.$state.endAbs=end; rebuild(); }, chart.$state.rangeKey, FULL.length);
    attachWindowControls(canvasId,
      start => { chart.$state.startAbs = Math.min(start, chart.$state.endAbs-1); chart.$state.rangeKey='custom'; rebuild(); },
      end   => { chart.$state.endAbs   = Math.max(end,   chart.$state.startAbs+1); chart.$state.rangeKey='custom'; rebuild(); }
    );
    const normToggle = document.getElementById(`${canvasId}-norm-toggle`);
    if (normToggle) normToggle.addEventListener('change', e => { chart.$state.norm = !!e.target.checked; rebuild(); });

    rebuild();

    (global.EconCharts ||= {}).DEBUG = { ...(global.EconCharts.DEBUG||{}), [canvasId]:{ kind:'generic', chart, state:chart.$state } };
    return chart;
  }

  // thin alias
  function initWageChart(id, opts){ return initLineForecastChart(id, opts); }

  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart, initLineForecastChart, initWageChart
  });
})(window);
