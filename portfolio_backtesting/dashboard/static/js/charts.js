function toNumericArray(values) {
  return (values || []).map((x) => {
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  });
}

function drawLineChart(canvasId, labels, values, label, borderColor) {
  const el = document.getElementById(canvasId);
  if (!el || !labels || labels.length === 0) {
    return;
  }

  new Chart(el, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label,
          data: toNumericArray(values),
          borderColor,
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          tension: 0.1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      scales: {
        x: { display: false },
      },
    },
  });
}

document.addEventListener("DOMContentLoaded", () => {
  if (!window.runChartData) {
    return;
  }
  drawLineChart(
    "equity-chart",
    window.runChartData.equity_labels,
    window.runChartData.equity_values,
    "Equity",
    "#2563eb",
  );
  drawLineChart(
    "drawdown-chart",
    window.runChartData.drawdown_labels,
    window.runChartData.drawdown_values,
    "Drawdown",
    "#dc2626",
  );
});
