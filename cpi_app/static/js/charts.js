// cpi_app/static/js/charts.js
console.log("charts.js v7 loaded"); // cache-buster marker

(function (global) {
  const fmt = v => v != null
    ? Number(v).toLocaleString('is-IS', { maximumFractionDigits: 2 })
    : '—';

  function pct(curr, prev) {
    if (curr == null || prev == null || prev === 0) return null;
    return (curr / prev - 1) * 100;
  }

  // Vertical guides for hovered month (solid) and month-12 (dashed)
  const hoverYearMarker = {
    id: 'hoverYearMarker',
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
        ctx.strokeStyle = 'rgba(255,255,255,.35)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x.getPixelForValue(j), yTop);
        ctx.lineTo(x.getPixelForValue(j), yBot);
        ctx.stroke();
      }
      ctx.setLineDash([]);
      ctx.strokeStyle = 'rgba(255,255,255,.6)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x.getPixelForValue(i), yTop);
      ctx.lineTo(x.getPixelForValue(i), yBot);
      ctx.stroke();
      ctx.restore();
    }
  };

  // Build a full tooltip CONFIG (with callbacks nested correctly)
  function makeTooltipConfig(labels, seriesFor) {
    return {
      displayColors: false,
      callbacks: {
        title(items) {
          return items && items[0] ? items[0].label : '';
        },
        label(ctx) {
          return `${ctx.dataset.label}: ${fmt(ctx.parsed.y)}`;
        },
        // add our extra lines once per hover
        afterBody(items) {
          if (!items || !items.length) return [];
          const idx = items[0].dataIndex;
          const dsLabel = items[0].dataset.label;
          const s = seriesFor(dsLabel);
          if (!s) return [];

          const curr = s[idx];
          const prev = idx > 0 ? s[idx - 1] : null;
          const j = idx - 12;
          const prev12 = j >= 0 ? s[j] : null;

          const lines = [];
          const mom = pct(curr, prev);
          if (mom != null) lines.push(`Mánaðarbreyting: ${mom.toFixed(2)}%`);
          const yoy = pct(curr, prev12);
          if (yoy != null) lines.push(`Ársbreyting: ${yoy.toFixed(2)}% (vs ${labels[j]})`);
          return lines;
        }
      }
    };
  }

  // ---------- CPI ----------
  function initCPIChart(canvasId, { labels, values, futLabels, futValues, subMeta, subSeries }) {
    const L  = labels || [];
    const FL = futLabels || [];
    const V  = values || [];
    const FV = futValues || [];

    const labelsAll = L.concat(FL);
    const totalCombined = V.concat(FV);

    const datasets = [
      {
        label: 'VNV vísitala',
        data: V.concat(Array(FL.length).fill(null)),
        borderWidth: 2, tension: 0.2,
        pointRadius: 2, pointHoverRadius: 4
      },
      {
        label: 'Spáð þróun',
        data: Array(V.length).fill(null).concat(FV),
        borderDash: [6, 4], borderWidth: 2, tension: 0.2,
        pointRadius: 2, pointHoverRadius: 4, pointHitRadius: 6
      }
    ];

    (subMeta || []).forEach(m => {
      const base = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      datasets.push({
        label: m.label,
        data: base.concat(Array(FL.length).fill(null)),
        borderWidth: 2, tension: 0.2,
        pointRadius: 0, hidden: true
      });
    });

    function seriesFor(label) {
      if (label === 'VNV vísitala' || label === 'Spáð þróun') return totalCombined;
      const m = (subMeta || []).find(x => x.label === label);
      if (!m) return null;
      const base = (subSeries && subSeries[m.code]) ? subSeries[m.code] : [];
      return base.concat(Array(FL.length).fill(null));
    }

    return new Chart(document.getElementById(canvasId).getContext('2d'), {
      type: 'line',
      data: { labels: labelsAll, datasets },
      options: {
        responsive: true,
        interaction: { mode: 'nearest', intersect: false },
        plugins: {
          legend: { position: 'bottom' },
          tooltip: makeTooltipConfig(labelsAll, seriesFor),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false }
        }
      },
      plugins: [hoverYearMarker]
    });
  }

  // ---------- Wages ----------
  function initWageChart(canvasId, { labels, values, futLabels, futValues }) {
    const L  = labels || [];
    const FL = futLabels || [];
    const V  = values || [];
    const FV = futValues || [];

    const labelsAll = L.concat(FL);
    const combined  = V.concat(FV);

    const seriesFor = () => combined;

    return new Chart(document.getElementById(canvasId).getContext('2d'), {
      type: 'line',
      data: {
        labels: labelsAll,
        datasets: [
          { label: 'Launavísitala (þróun)',
            data: V.concat(Array(FL.length).fill(null)),
            borderWidth: 2, tension: 0.25,
            pointRadius: 2, pointHoverRadius: 4 },
          { label: 'Launavísitala (spá)',
            data: Array(V.length).fill(null).concat(FV),
            borderDash: [6, 4], borderWidth: 2, tension: 0.25,
            pointRadius: 2, pointHoverRadius: 4, pointHitRadius: 6 }
        ]
      },
      options: {
        responsive: true,
        interaction: { mode: 'nearest', intersect: false },
        plugins: {
          legend: { position: 'bottom' },
          tooltip: makeTooltipConfig(labelsAll, seriesFor),
        },
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { beginAtZero: false }
        }
      },
      plugins: [hoverYearMarker] // now on wages too
    });
  }

  global.EconCharts = { initCPIChart, initWageChart };
})(window);
