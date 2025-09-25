// cpi_app/static/js/charts.js
// bump ?v=13 in templates after updating this file
console.log("charts.js v13 loaded");

(function (global) {
  // ---------- helpers ----------
  const fmt = v =>
    v != null ? Number(v).toLocaleString("is-IS", { maximumFractionDigits: 2 }) : "—";

  function pct(curr, prev) {
    if (curr == null || prev == null || prev === 0) return null;
    return (curr / prev - 1) * 100;
  }

  // YYYY-MM -> months since year 0 (fast compare)
  function ymToIndex(ym) {
    const y = Number(ym.slice(0, 4));
    const m = Number(ym.slice(5, 7));
    return y * 12 + (m - 1);
  }

  // Compute slice start index for last N years; if "all" return 0
  function sliceStart(fullLabels, range) {
    if (range === "all") return 0;
    const years = range === "10y" ? 10 : range === "5y" ? 5 : 2; // default mapping
    const lastIdx = fullLabels.length - 1;
    if (lastIdx < 0) return 0;
    const lastMonthIdx = ymToIndex(fullLabels[lastIdx]);
    const minMonthIdx = lastMonthIdx - years * 12 + 1;
    // find first label >= min month
    for (let i = 0; i < fullLabels.length; i++) {
      if (ymToIndex(fullLabels[i]) >= minMonthIdx) return i;
    }
    return 0;
  }

  function getCtx(id) {
    const el = document.getElementById(id);
    if (!el) {
      console.warn(`[charts] canvas #${id} not found`);
      return null;
    }
    return el.getContext("2d");
  }

  // Vertical guides for hovered month (solid) and month-12 (dashed)
  const hoverYearMarker = {
    id: "hoverYearMarker",
    afterDraw(chart) {
      const t = chart.tooltip;
      if (!t || !t.getActiveElements().length) return;
      const i = t.getActiveElements()[0].index;
      const j = i - 12;

      const x = chart.scales.x;
      const yTop = chart.chartArea.top;
      const yBot = chart.chartArea.bottom;
      const ctx = chart.ctx;

      ctx.save();
      if (j >= 0) {
        ctx.setLineDash([5, 4]);
        ctx.strokeStyle = "rgba(255,255,255,.35)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x.getPixelForValue(j), yTop);
        ctx.lineTo(x.getPixelForValue(j), yBot);
        ctx.stroke();
      }
      ctx.setLineDash([]);
      ctx.strokeStyle = "rgba(255,255,255,.6)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x.getPixelForValue(i), yTop);
      ctx.lineTo(x.getPixelForValue(i), yBot);
      ctx.stroke();
      ctx.restore();
    },
  };

  // Tooltip config (Chart.js v4) that computes MoM/YoY from the currently shown combined series
  function makeTooltipConfig(labels, seriesFor) {
    return {
      displayColors: false,
      callbacks: {
        title(items) {
          return items && items[0] ? items[0].label : "";
        },
        label(ctx) {
          return `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}`;
        },
        afterBody(items) {
          if (!items || !items.length) return [];
          const idx = items[0].dataIndex;
          const dsLabel = items[0].dataset.label;
          const s = seriesFor(dsLabel); // combined series (history slice + forecast)
          if (!s) return [];
          const curr = s[idx];
          const prev = idx > 0 ? s[idx - 1] : null;

          const lines = [];
          const mom = pct(curr, prev);
          if (mom != null) lines.push(`Mánaðarbreyting: ${mom.toFixed(2)}%`);

          const j = idx - 12;
          const prev12 = j >= 0 ? s[j] : null;
          const yoy = pct(curr, prev12);
          if (yoy != null) lines.push(`Ársbreyting: ${yoy.toFixed(2)}% (vs ${labels[j]})`);

          return lines;
        },
      },
    };
  }

  // Store charts + data to enable range switching from buttons
  const registry = {}; // id -> { chart, applyRange, buttons }

  // Common range control binding for <div class="range-controls" data-chart="...">
  function wireRangeControls(canvasId, applyRange, initialRange) {
    const ctrls = document.querySelectorAll(`.range-controls[data-chart="${canvasId}"]`);
    ctrls.forEach(ctrl => {
      ctrl.addEventListener("click", (e) => {
        const btn = e.target.closest("button[data-range]");
        if (!btn) return;
        const r = btn.dataset.range;
        // toggle active class
        ctrl.querySelectorAll("button[data-range]").forEach(b => b.classList.toggle("is-active", b === btn));
        applyRange(r);
      });
      // set default active
      const def = ctrl.querySelector(`button[data-range="${initialRange}"]`) ||
                  ctrl.querySelector(`button[data-range]`);
      if (def) def.classList.add("is-active");
    });
  }

  // ---------- CPI (with optional sub-series) ----------
  function initCPIChart(
    canvasId,
    { fullLabels, fullValues, futLabels, futValues, subMeta, subSeries, initialRange = "2y" }
  ) {
    const FL = Array.isArray(fullLabels) ? fullLabels : [];
    const FVals = Array.isArray(fullValues) ? fullValues : [];
    const FTL = Array.isArray(futLabels) ? futLabels : [];
    const FTV = Array.isArray(futValues) ? futValues : [];

    // datasets (we will re-slice them in applyRange)
    const baseDatasets = [
      {
        key: "__TOTAL__", // internal key
        label: "VNV vísitala",
        baseSeries: FVals,      // full history
        colorHint: 0,
      },
      {
        key: "__TOTAL_FORE__",
        label: "Spáð þróun",
        baseSeries: FVals,      // same base, forecast appended separately
        dash: [6,4],
        colorHint: 1,
      }
    ];

    (subMeta || []).forEach(m => {
      const series = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      baseDatasets.push({
        key: `SUB:${m.code}`,
        label: m.label,
        baseSeries: series,
        hidden: true
      });
    });

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart === "undefined") return null;

    // build initial skeleton chart (empty data for now)
    const chart = new Chart(ctx, {
      type: "line",
      data: { labels: [], datasets: baseDatasets.map(d => ({
        label: d.label,
        data: [],
        borderWidth: 2,
        tension: 0.2,
        pointRadius: d.key.includes("__FORE__") ? 2 : 2,
        pointHoverRadius: 4,
        borderDash: d.dash || [],
        hidden: !!d.hidden
      })) },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "nearest", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: makeTooltipConfig([], () => null), // will be replaced on applyRange
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false },
        },
      },
      plugins: [hoverYearMarker],
    });

    function applyRange(range) {
      const start = sliceStart(FL, range);
      const histL = FL.slice(start);
      const histV = FVals.slice(start);

      const labelsAll = histL.concat(FTL);

      // Build dataset values
      const combinedForTooltip = {}; // label -> combined numeric series

      chart.data.labels = labelsAll;

      chart.data.datasets.forEach((ds, idx) => {
        const spec = baseDatasets[idx];
        const base = spec.baseSeries.slice(start); // slice history for this series

        if (spec.key === "__TOTAL__") {
          // actuals + nulls for forecast
          ds.data = base.concat(Array(FTV.length).fill(null));
          combinedForTooltip[ds.label] = base.concat(FTV);
        } else if (spec.key === "__TOTAL_FORE__") {
          // nulls for history + forecast
          ds.data = Array(base.length).fill(null).concat(FTV);
          combinedForTooltip[ds.label] = base.concat(FTV);
        } else {
          // sub-series: history only + nulls during forecast
          ds.data = base.concat(Array(FTV.length).fill(null));
          combinedForTooltip[ds.label] = base.concat(FTV.map(() => null)); // do not compute MoM/YoY vs nulls
        }
      });

      // Replace tooltip with one that uses the current combined series
      chart.options.plugins.tooltip = makeTooltipConfig(labelsAll, (label) => combinedForTooltip[label] || null);
      chart.update();
    }

    registry[canvasId] = { chart, applyRange };
    wireRangeControls(canvasId, applyRange, initialRange);
    applyRange(initialRange);

    return chart;
  }

  // ---------- Wages ----------
  function initWageChart(canvasId, { fullLabels, fullValues, futLabels, futValues, initialRange = "2y" }) {
    const FL = Array.isArray(fullLabels) ? fullLabels : [];
    const FV = Array.isArray(fullValues) ? fullValues : [];
    const FTL = Array.isArray(futLabels) ? futLabels : [];
    const FTV = Array.isArray(futValues) ? futValues : [];

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart === "undefined") return null;

    const chart = new Chart(ctx, {
      type: "line",
      data: { labels: [], datasets: [
        { label: "Launavísitala (þróun)", data: [], borderWidth: 2, tension: .25, pointRadius: 2, pointHoverRadius: 4 },
        { label: "Launavísitala (spá)",   data: [], borderWidth: 2, tension: .25, pointRadius: 2, pointHoverRadius: 4, pointHitRadius: 6, borderDash: [6,4] }
      ]},
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "nearest", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: makeTooltipConfig([], () => null),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false },
        },
      },
      plugins: [hoverYearMarker],
    });

    function applyRange(range) {
      const start = sliceStart(FL, range);
      const histL = FL.slice(start);
      const histV = FV.slice(start);
      const labelsAll = histL.concat(FTL);

      chart.data.labels = labelsAll;
      const actual = histV.concat(Array(FTV.length).fill(null));
      const fore   = Array(histV.length).fill(null).concat(FTV);
      chart.data.datasets[0].data = actual;
      chart.data.datasets[1].data = fore;

      const combined = histV.concat(FTV);
      chart.options.plugins.tooltip = makeTooltipConfig(labelsAll, () => combined);
      chart.update();
    }

    registry[canvasId] = { chart, applyRange };
    wireRangeControls(canvasId, applyRange, initialRange);
    applyRange(initialRange);
    return chart;
  }

  // ---------- Generic (BCI/PPI etc.) ----------
  function initLineForecastChart(canvasId, { fullLabels, fullValues, futLabels, futValues, initialRange = "2y" }) {
    const FL = Array.isArray(fullLabels) ? fullLabels : [];
    const FV = Array.isArray(fullValues) ? fullValues : [];
    const FTL = Array.isArray(futLabels) ? futLabels : [];
    const FTV = Array.isArray(futValues) ? futValues : [];

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart === "undefined") return null;

    const chart = new Chart(ctx, {
      type: "line",
      data: { labels: [], datasets: [
        { label: "Þróun", data: [], borderWidth: 2, tension: .25, pointRadius: 2, pointHoverRadius: 4 },
        { label: "Spá",   data: [], borderWidth: 2, tension: .25, pointRadius: 2, pointHoverRadius: 4, pointHitRadius: 6, borderDash: [6,4] }
      ]},
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "nearest", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: makeTooltipConfig([], () => null),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false },
        },
      },
      plugins: [hoverYearMarker],
    });

    function applyRange(range) {
      const start = sliceStart(FL, range);
      const histL = FL.slice(start);
      const histV = FV.slice(start);
      const labelsAll = histL.concat(FTL);

      chart.data.labels = labelsAll;
      const actual = histV.concat(Array(FTV.length).fill(null));
      const fore   = Array(histV.length).fill(null).concat(FTV);
      chart.data.datasets[0].data = actual;
      chart.data.datasets[1].data = fore;

      const combined = histV.concat(FTV);
      chart.options.plugins.tooltip = makeTooltipConfig(labelsAll, () => combined);
      chart.update();
    }

    registry[canvasId] = { chart, applyRange };
    wireRangeControls(canvasId, applyRange, initialRange);
    applyRange(initialRange);
    return chart;
  }

  // ---------- export ----------
  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart,
    initWageChart,
    initLineForecastChart,
  });
})(window);
