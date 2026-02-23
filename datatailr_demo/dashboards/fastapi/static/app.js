/*
 * FastAPI Showcase — shared Chart.js helpers and tab logic.
 */

function chartOpts(scaleOverrides, showLegend) {
  if (showLegend === undefined) showLegend = true;
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: {
        display: showLegend,
        position: 'bottom',
        labels: { padding: 14, font: { family: 'Inter, sans-serif', size: 12 } }
      },
      tooltip: {
        backgroundColor: '#0f172a',
        titleFont: { family: 'Inter, sans-serif' },
        bodyFont: { family: 'Inter, sans-serif' },
        cornerRadius: 8,
        padding: 10
      }
    },
    scales: Object.assign({
      x: { grid: { display: false }, ticks: { font: { family: 'Inter, sans-serif', size: 11 }, maxRotation: 45 } },
      y: { grid: { color: '#f1f5f9' }, ticks: { font: { family: 'Inter, sans-serif', size: 11 } }, beginAtZero: true }
    }, scaleOverrides || {})
  };
}

function initTabs() {
  document.querySelectorAll('.tab-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var parent = btn.closest('.tab-bar') || btn.parentNode;
      parent.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.remove('active'); });
      btn.classList.add('active');

      var tabId = 'tab-' + btn.dataset.tab;
      var scope = parent.parentNode;
      scope.querySelectorAll('.tab-content').forEach(function(tc) { tc.classList.remove('active'); });
      var target = scope.querySelector('#' + tabId) || document.getElementById(tabId);
      if (target) target.classList.add('active');
    });
  });
}
