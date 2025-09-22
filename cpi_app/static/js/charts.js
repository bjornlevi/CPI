// cpi_app/static/js/charts.js
// bump version in your template to force reload: ?v=12
console.log("charts.js v12 loaded");

(function (global) {
  // ---------- helpers ----------
  const fmt = v =>
    v != null ? Number(v).toLocaleString("is-IS", { maximumFractionDigits: 2 }) : "—";

  function pct(curr, prev) {
    if (curr == null || prev == null || prev === 0) return null;
    return (curr / prev - 1) * 100;
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

  // Tooltip config (Chart.js v4)
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
          const s = seriesFor(dsLabel);
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

  // ---------- CPI (with optional sub-series) ----------
  function initCPIChart(
    canvasId,
    { labels, values, futLabels, futValues, subMeta, subSeries }
  ) {
    const L = labels || [];
    const FL = futLabels || [];
    const V = values || [];
    const FV = futValues || [];

    const labelsAll = L.concat(FL);
    const totalCombined = V.concat(FV);

    const datasets = [
      {
        label: "VNV vísitala",
        data: V.concat(Array(FL.length).fill(null)),
        borderWidth: 2,
        tension: 0.2,
        pointRadius: 2,
        pointHoverRadius: 4,
      },
      {
        label: "Spáð þróun",
        data: Array(V.length).fill(null).concat(FV),
        borderDash: [6, 4],
        borderWidth: 2,
        tension: 0.2,
        pointRadius: 2,
        pointHoverRadius: 4,
        pointHitRadius: 6,
      },
    ];

    // optional sub-series (hidden by default)
    (subMeta || []).forEach((m) => {
      const base = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      datasets.push({
        label: m.label,
        data: base.concat(Array(FL.length).fill(null)),
        borderWidth: 2,
        tension: 0.2,
        pointRadius: 0,
        hidden: true,
      });
    });

    function seriesFor(label) {
      if (label === "VNV vísitala" || label === "Spáð þróun") return totalCombined;
      const m = (subMeta || []).find((x) => x.label === label);
      if (!m) return null;
      const base = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      return base.concat(Array(FL.length).fill(null));
    }

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart === "undefined") return null;

    return new Chart(ctx, {
      type: "line",
      data: { labels: labelsAll, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false, // let CSS container control height
        interaction: { mode: "nearest", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: makeTooltipConfig(labelsAll, seriesFor),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false },
        },
      },
      plugins: [hoverYearMarker],
    });
  }

  // ---------- Wages ----------
  function initWageChart(canvasId, { labels, values, futLabels, futValues }) {
    const L = labels || [];
    const FL = futLabels || [];
    const V = values || [];
    const FV = futValues || [];

    const labelsAll = L.concat(FL);
    const combined = V.concat(FV);
    const seriesFor = () => combined;

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart === "undefined") return null;

    return new Chart(ctx, {
      type: "line",
      data: {
        labels: labelsAll,
        datasets: [
          {
            label: "Launavísitala (þróun)",
            data: V.concat(Array(FL.length).fill(null)),
            borderWidth: 2,
            tension: 0.25,
            pointRadius: 2,
            pointHoverRadius: 4,
          },
          {
            label: "Launavísitala (spá)",
            data: Array(V.length).fill(null).concat(FV),
            borderDash: [6, 4],
            borderWidth: 2,
            tension: 0.25,
            pointRadius: 2,
            pointHoverRadius: 4,
            pointHitRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "nearest", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: makeTooltipConfig(labelsAll, seriesFor),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false },
        },
      },
      plugins: [hoverYearMarker],
    });
  }

  // ---------- Generic (BCI/PPI etc.) ----------
  function initLineForecastChart(canvasId, { labels, values, futLabels, futValues }) {
    const L = labels || [];
    const FL = futLabels || [];
    const V = values || [];
    const FV = futValues || [];

    const labelsAll = L.concat(FL);
    const combined = V.concat(FV);
    const seriesFor = () => combined;

    const ctx = getCtx(canvasId);
    if (!ctx || typeof Chart === "undefined") return null;

    return new Chart(ctx, {
      type: "line",
      data: {
        labels: labelsAll,
        datasets: [
          {
            label: "Þróun",
            data: V.concat(Array(FL.length).fill(null)),
            borderWidth: 2,
            tension: 0.25,
            pointRadius: 2,
            pointHoverRadius: 4,
          },
          {
            label: "Spá",
            data: Array(V.length).fill(null).concat(FV),
            borderDash: [6, 4],
            borderWidth: 2,
            tension: 0.25,
            pointRadius: 2,
            pointHoverRadius: 4,
            pointHitRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "nearest", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: makeTooltipConfig(labelsAll, seriesFor),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false },
        },
      },
      plugins: [hoverYearMarker],
    });
  }

  // ---------- export (merge, don't overwrite) ----------
  global.EconCharts = Object.assign({}, global.EconCharts, {
    initCPIChart,
    initWageChart,
    initLineForecastChart,
  });
})(window);
