/*
 * Flask Showcase — shared Chart.js helpers and tab logic.
 */

/* Default Chart.js options factory */
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
        backgroundColor: '#1a1a2e',
        titleFont: { family: 'Inter, sans-serif' },
        bodyFont: { family: 'Inter, sans-serif' },
        cornerRadius: 6,
        padding: 10
      }
    },
    scales: Object.assign({
      x: { grid: { display: false }, ticks: { font: { family: 'Inter, sans-serif', size: 11 }, maxRotation: 45 } },
      y: { grid: { color: '#f0f0f5' }, ticks: { font: { family: 'Inter, sans-serif', size: 11 } }, beginAtZero: true }
    }, scaleOverrides || {})
  };
}

/* Tab switching */
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
