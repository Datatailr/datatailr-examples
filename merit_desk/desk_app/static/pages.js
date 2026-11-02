/* Merit Desk: the five pages. Each registers itself with page(name, handlers), and app.js
 * shows it. showRun(record) opens a run of that page, with its result when it has one. */

'use strict';

const BLUE = '#58a6ff';
const GREY = '#8b949e';
const GREEN = '#3fb950';
const RED = '#f85149';
const YELLOW = '#e3b341';

// ---- shared: months, and a curve against its base with the weather-path band of a run

function monthLabel(month) {  // '2026-11' -> 'Nov 26'
  const [year, number] = month.split('-').map(Number);
  return new Date(Date.UTC(year, number - 1, 1)).toLocaleString('en-GB',
    {month: 'short', year: '2-digit', timeZone: 'UTC'});
}

function isoDay(day) {  // a local date as 2026-11-01
  const pad = n => String(n).padStart(2, '0');
  return `${day.getFullYear()}-${pad(day.getMonth() + 1)}-${pad(day.getDate())}`;
}

function lastDay(month) {  // '2026-11' -> '2026-11-30'
  const [year, number] = month.split('-').map(Number);
  return new Date(Date.UTC(year, number, 0)).toISOString().slice(0, 10);
}

function line(label, data, color, more = {}) {
  return {label, data, borderColor: color, backgroundColor: color, borderWidth: 2,
          tension: 0.25, ...more};
}

/** Base (dashed) and curve (solid), and the P10 to P90 band when a run gives one. */
function curveDatasets(names, base, curve, band) {
  const sets = [];
  if (band) {
    const none = {borderWidth: 0, backgroundColor: 'rgba(88, 166, 255, .18)'};
    sets.push(line('P10 to P90', band.p90, 'transparent', {...none, fill: '+1'}),
              line('P10', band.p10, 'transparent', none),
              line('P50', band.p50, BLUE, {borderWidth: 1, borderDash: [2, 3]}));
  }
  sets.push(line(names[1], curve, BLUE, {pointRadius: 2.5}),
            line(names[0], base, GREY, {borderWidth: 1.5, borderDash: [6, 4]}));
  return sets;
}

function bandChart(canvas) {
  return monthChart(canvas, {plugins: {
    legend: {labels: {filter: item => item.text !== 'P10'}},
    tooltip: {callbacks: {label: c => {
      const name = c.dataset.label === 'P10 to P90' ? 'P90' : c.dataset.label;
      return `${name}: ${eur(c.parsed.y)}`;
    }}},
  }});
}

function drawCurve(chart, months, names, base, curve, band) {
  chart.data.labels = months.map(monthLabel);
  chart.data.datasets = curveDatasets(names, base, curve, band);
  chart.update();
}

function curveTiles(node, names, base, curve, band) {
  const change = mean(curve) - mean(base);
  node.replaceChildren(...[
    tile(`${names[0]} · 12-month average`, eur(mean(base)), 'EUR/MWh'),
    tile(`${names[1]} · 12-month average`, eur(mean(curve)), 'EUR/MWh'),
    tile('Change', signed(change), 'EUR/MWh', trend(change)),
    band && tile('P10 to P90 · average', `${eur(mean(band.p10))} to ${eur(mean(band.p90))}`),
  ].filter(Boolean));
}

function curveTable(node, months, names, base, curve, band) {
  const head = ['Month', names[0], names[1], 'Change', ...(band ? ['P10', 'P50', 'P90'] : [])];
  table(node, head, months.map((month, i) => el('tr', {},
    el('td', {text: monthLabel(month)}),
    el('td', {text: eur(base[i])}),
    el('td', {text: eur(curve[i])}),
    el('td', {class: trend(curve[i] - base[i]), text: signed(curve[i] - base[i])}),
    ...(band ? [band.p10[i], band.p50[i], band.p90[i]].map(p => el('td', {text: eur(p)})) : []))));
}

function runCaption(record) {
  const r = record.result;
  return `${record.label} · ${num(r.n_paths, 0)} weather paths · forecast run ` +
         `${when(r.forecast_run)} · priced on ${r.as_of}`;
}

/** Start a run from a button. The note says what happened, or the server's refusal. */
async function startRun(button, note, body, waiting) {
  button.disabled = true;
  try {
    const record = await api('/api/runs', body);
    watch(record);
    say(note, `${record.label} runs as a job on Datatailr. ${waiting}`, 'ok');
  } catch (error) {
    say(note, error.message, 'bad');
  } finally {
    button.disabled = false;
  }
}

/** What a page says about a run it opens that has no result. */
function runState(note, record, waiting) {
  if (record.status === 'failed') say(note, `${record.label} failed: ${record.error}`, 'bad');
  else say(note, `${record.label} is ${record.status}. ${waiting}`);
}

// ---- Curves: the newest curve of each market, from the Pricing API

page('curves', (() => {
  let chart;
  let curves = [];
  const hidden = new Set();

  async function load() {
    try {
      const data = await api('/api/curves');
      curves = data.curves;
      $('curves-errors').textContent =
        Object.entries(data.errors).map(([code, text]) => `${code}: ${text}`).join('  ·  ');
    } catch (error) {
      curves = [];
      $('curves-errors').textContent = error.message;
    }
    draw();
  }

  function draw() {
    const months = [...new Set(curves.flatMap(c => c.months))].sort();
    const shown = curves.filter(c => !hidden.has(c.market));
    const priceOf = (curve, month) => curve.prices[curve.months.indexOf(month)] ?? null;
    chart.data.labels = months.map(monthLabel);
    chart.data.datasets = shown.map(c => line(c.market, months.map(m => priceOf(c, m)),
                                              COLORS[c.market], {pointRadius: 2.5}));
    chart.update();
    const sources = [...new Set(curves.map(c => `${c.as_of} · ${c.source}`))];
    $('curves-caption').textContent = sources.length === 1 ? `Curves of ${sources[0]}`
      : curves.map(c => `${c.market}: ${c.as_of} · ${c.source}`).join('  ·  ');
    table($('curves-table'), ['Month', ...shown.map(c => c.market)], months.map(month =>
      el('tr', {}, el('td', {text: monthLabel(month)}),
        ...shown.map(c => el('td', {text: eur(priceOf(c, month))})))));
  }

  function toggle(code, on) {
    if (on) hidden.delete(code);
    else hidden.add(code);
    draw();
  }

  return {
    init() {
      chart = monthChart($('curves-chart'));
      chart.options.plugins.legend.display = false;  // the check boxes above are the legend
      $('curves-toggles').replaceChildren(...MARKETS.map(code => el('label', {},
        el('input', {type: 'checkbox', checked: true, 'aria-label': `Show ${code} curve`,
                     onchange: event => toggle(code, event.target.checked)}),
        el('span', {class: 'swatch', style: `background:${COLORS[code]}`}), code)));
      $('curves-reload').addEventListener('click', load);
      load();
    },
  };
})());

// ---- Scenario: a temperature shift over a window, previewed at once, priced as a job

page('scenario', (() => {
  const WAITING = 'Its band shows here when it is done.';
  let chart;
  let preview = null;  // the newest preview
  let shownRun = null;  // a finished run whose band the chart shows
  let asked = 0;  // a preview that comes back after a newer one is dropped
  const form = () => $('scenario-form').elements;

  function inputs() {
    const f = form();
    return {market: f.market.value, shift_c: Number(f.shift_c.value),
            start: f.start.value || null, end: f.end.value || null, overrides: []};
  }

  function readouts() {
    const f = form();
    $('scenario-shift').textContent = `${signed(Number(f.shift_c.value), 1)} °C`;
    $('scenario-run').textContent = `Run ${num(Number(f.n_paths.value), 0)} weather paths as job`;
  }

  async function refresh() {
    shownRun = null;  // a changed input leaves the band of the run behind
    const mine = ++asked;
    try {
      const data = await api('/api/scenario/preview', inputs());
      if (mine !== asked) return;
      preview = data;
      $('scenario-error').textContent = '';
      const f = form();
      f.start.min = f.end.min = isoDay(new Date());
      f.start.max = f.end.max = lastDay(data.months[data.months.length - 1]);
    } catch (error) {
      if (mine === asked) $('scenario-error').textContent = error.message;
      return;
    }
    draw();
  }

  function draw() {
    const names = ['Base', 'Scenario'];
    if (shownRun) {
      const r = shownRun.result;
      drawCurve(chart, r.months, names, r.unshifted, r.base, r);
      curveTiles($('scenario-tiles'), names, r.unshifted, r.base, r);
      curveTable($('scenario-table'), r.months, names, r.unshifted, r.base, r);
      $('scenario-caption').textContent = runCaption(shownRun);
    } else if (preview) {
      const p = preview;
      drawCurve(chart, p.months, names, p.base, p.scenario);
      curveTiles($('scenario-tiles'), names, p.base, p.scenario);
      curveTable($('scenario-table'), p.months, names, p.base, p.scenario);
      $('scenario-caption').textContent = 'Preview: the shifted curve against the base curve ' +
        '(dashed). Run it as a job to add the P10 to P90 band of the weather paths.';
    }
  }

  return {
    init() {
      chart = bandChart($('scenario-chart'));
      const f = form();
      const next = new Date(new Date().getFullYear(), new Date().getMonth() + 1, 1);
      f.start.value = isoDay(next);
      f.end.value = isoDay(new Date(next.getFullYear(), next.getMonth(), 14));
      const later = debounce(refresh);
      $('scenario-form').addEventListener('input', event => {
        readouts();
        if (event.target.name !== 'n_paths') later();
      });
      $('scenario-run').addEventListener('click', () => {
        const body = {kind: 'scenario', inputs: {...inputs(), n_paths: Number(f.n_paths.value)}};
        startRun($('scenario-run'), $('scenario-note'), body, WAITING);
      });
      readouts();
      refresh();
    },
    showRun(record) {
      const f = form();
      const i = record.inputs;
      f.market.value = i.market;
      f.shift_c.value = i.shift_c;
      f.start.value = i.start || '';
      f.end.value = i.end || '';
      f.n_paths.value = i.n_paths;
      readouts();
      if (record.status === 'done' && record.result) {
        shownRun = record;
        say($('scenario-note'), `${record.label} is done.`, 'ok');
        draw();
      } else {
        runState($('scenario-note'), record, WAITING);
        refresh();
      }
    },
  };
})());

// ---- Merit order: the stack at one temperature, with a cost and a capacity per plant

const plantNames = {  // writes each plant's name above its step
  id: 'plantNames',
  afterDatasetsDraw(chart) {
    const points = chart.data.datasets[0]?.data || [];
    const drawn = chart.getDatasetMeta(0).data;
    const ctx = chart.ctx;
    ctx.save();
    Object.assign(ctx, {fillStyle: '#c9d1d9', font: '600 10px sans-serif', textAlign: 'center'});
    for (let i = 0; i + 1 < drawn.length; i += 2) {
      const [a, b] = [drawn[i], drawn[i + 1]];
      if (b.x - a.x > 34) ctx.fillText(points[i].plant, (a.x + b.x) / 2, a.y - 7);
    }
    ctx.restore();
  },
};

page('merit', (() => {
  const WAITING = 'The re-priced curve shows here when it is done.';
  const own = {};  // market -> its own plants, as a preview without changes gives them
  let chart;
  let curveChart;
  let asked = 0;
  const form = () => $('merit-form').elements;
  const temp = () => Number(form().temp_c.value);

  async function ownPlants(market) {
    own[market] ??= (await api('/api/stack/preview', {market, temp_c: 10})).plants;
    return own[market];
  }

  function plantRow(plant) {
    const field = (name, max, label) => el('input', {type: 'number', min: 0, max, step: 'any',
      value: plant[name], 'aria-label': `${plant.name} ${label}`,
      'data-plant': plant.name, 'data-field': name, 'data-own': plant[name]});
    return el('tr', {}, el('td', {text: plant.name}),
              el('td', {}, field('capacity_gw', 200, 'capacity in GW')),
              el('td', {}, field('cost_eur_mwh', 1000, 'cost in EUR/MWh')));
  }

  async function loadMarket() {
    const market = form().market.value;
    try {
      const plants = await ownPlants(market);
      if (market !== form().market.value) return;  // another market was picked meanwhile
      table($('merit-plants'), ['Plant', 'Capacity GW', 'Cost EUR/MWh'], plants.map(plantRow));
    } catch (error) {
      $('merit-error').textContent = error.message;
      return;
    }
    refresh();
  }

  /** The changed plants. A field left as it was is null, so the plant keeps its own value. */
  function overrides() {
    const changed = {};
    for (const input of $('merit-plants').querySelectorAll('input')) {
      const value = input.valueAsNumber;
      if (!Number.isFinite(value)) throw new Error(`Enter a number for ${input.dataset.plant}.`);
      const isChanged = value !== Number(input.dataset.own);
      input.classList.toggle('changed', isChanged);
      if (!isChanged) continue;
      const name = input.dataset.plant;
      changed[name] ??= {name, cost_eur_mwh: null, capacity_gw: null};
      changed[name][input.dataset.field] = value;
    }
    return Object.values(changed);
  }

  async function refresh() {
    const mine = ++asked;
    try {
      const body = {market: form().market.value, temp_c: temp(), overrides: overrides()};
      const view = await api('/api/stack/preview', body);
      if (mine !== asked) return;
      $('merit-error').textContent = '';
      draw(view);
    } catch (error) {
      if (mine === asked) $('merit-error').textContent = error.message;
    }
  }

  function draw(view) {
    const total = view.plants.reduce((sum, p) => sum + p.capacity_gw, 0);
    const top = Math.max(view.price, ...view.plants.map(p => p.cost_eur_mwh)) * 1.15 || 1;
    const right = Math.max(total, view.demand_gw) * 1.04 || 1;
    const steps = view.plants.flatMap(p => [{x: p.from_gw, y: p.cost_eur_mwh, plant: p.name},
                                            {x: p.to_gw, y: p.cost_eur_mwh, plant: p.name}]);
    chart.data.datasets = [
      {label: 'Plants, cheapest first', data: steps, borderColor: BLUE, borderWidth: 2,
       backgroundColor: 'rgba(88, 166, 255, .12)', fill: 'origin'},
      {label: `Demand ${num(view.demand_gw, 1)} GW`, data: [{x: view.demand_gw, y: 0},
       {x: view.demand_gw, y: top}], borderColor: YELLOW, borderWidth: 2, borderDash: [6, 4]},
      {label: `Clearing price ${eur(view.price)} EUR/MWh`, data: [{x: 0, y: view.price},
       {x: right, y: view.price}], borderColor: GREEN, borderWidth: 1, borderDash: [2, 3]},
    ];
    chart.options.scales.x.suggestedMax = right;
    chart.options.scales.y.suggestedMax = top;
    chart.update();
    const marginal = view.plants.find(p => view.demand_gw > p.from_gw && view.demand_gw <= p.to_gw);
    const spare = total - view.demand_gw;
    $('merit-tiles').replaceChildren(
      tile('Demand', num(view.demand_gw, 1), 'GW'),
      tile('Clearing price', eur(view.price), 'EUR/MWh'),
      tile('Marginal plant', marginal ? marginal.name : 'None (scarcity)'),
      tile('Capacity left', num(spare, 1), 'GW', spare < 0 ? 'down' : ''));
  }

  function showResult(record) {
    const r = record.result;
    const names = ['Default stack', 'This stack'];
    $('merit-result').hidden = false;
    curveChart ??= bandChart($('merit-curve-chart'));  // made once it shows, so it has a size
    drawCurve(curveChart, r.months, names, r.unshifted, r.base, r);
    curveTiles($('merit-result-tiles'), names, r.unshifted, r.base, r);
    $('merit-caption').textContent = runCaption(record);
  }

  function run() {
    let changes;
    try {
      changes = overrides();
    } catch (error) {
      return say($('merit-note'), error.message, 'bad');
    }
    if (!changes.length) {
      return say($('merit-note'), 'Change the cost or the capacity of a plant first.', 'bad');
    }
    const inputs = {market: form().market.value, shift_c: 0, start: null, end: null,
                    n_paths: Number($('merit-paths').value), overrides: changes};
    startRun($('merit-run'), $('merit-note'), {kind: 'scenario', inputs}, WAITING);
  }

  return {
    init() {
      chart = new Chart($('merit-chart'), {
        type: 'line',
        data: {datasets: []},
        plugins: [plantNames],
        options: {
          interaction: {mode: 'nearest', intersect: false},
          scales: {x: {type: 'linear', min: 0, title: {display: true, text: 'Capacity, GW'}},
                   y: {min: 0, title: {display: true, text: 'EUR/MWh'}}},
          plugins: {tooltip: {callbacks: {
            title: () => '',
            label: c => (c.raw.plant ? `${c.raw.plant}: ${eur(c.parsed.y)} EUR/MWh`
                                     : c.dataset.label),
          }}},
        },
      });
      const later = debounce(refresh);
      $('merit-form').addEventListener('input', event => {
        $('merit-temp').textContent = `${num(temp(), 1)} °C`;
        if (event.target.name !== 'market') later();  // a new market first loads its plants
      });
      $('merit-form').elements.market.addEventListener('change', loadMarket);
      $('merit-plants').addEventListener('input', later);
      $('merit-reset').addEventListener('click', loadMarket);
      $('merit-run').addEventListener('click', run);
      loadMarket();
    },
    async showRun(record) {
      form().market.value = record.inputs.market;
      $('merit-paths').value = record.inputs.n_paths;
      await loadMarket();
      for (const change of record.inputs.overrides) {
        const plant = `input[data-plant="${CSS.escape(change.name)}"]`;
        for (const input of $('merit-plants').querySelectorAll(plant)) {
          const value = change[input.dataset.field];
          if (value !== null && value !== undefined) input.value = value;
        }
      }
      refresh();
      if (record.status === 'done' && record.result) {
        showResult(record);
        say($('merit-note'), `${record.label} is done.`, 'ok');
      } else {
        $('merit-result').hidden = true;  // the result of another run must not show with it
        runState($('merit-note'), record, WAITING);
      }
    },
  };
})());

// ---- Backtest: the newest sweep as a heatmap, and one setting in full as a job

page('backtest', (() => {
  const WAITING = 'Its P&L shows here when it is done.';
  let chart;
  let sweep = null;
  const form = () => $('backtest-form').elements;
  const key = (tenor, threshold) => `${Number(tenor)}|${Number(threshold)}`;

  async function loadSweep() {
    const market = form().market.value;
    let found = null;
    let problem = '';
    try {
      found = await api(`/api/sweep/${market}`);
    } catch (error) {
      problem = error.status === 404
        ? `${error.message} Start the Strategy Sweep on the Jobs page.` : error.message;
    }
    if (market !== form().market.value) return;  // another market was picked meanwhile
    sweep = found;
    $('sweep-error').textContent = problem;
    drawSweep();
  }

  function short(value) {  // 15,210 -> 15.2k, so that a full sweep fits on a laptop screen
    return Math.abs(value) >= 1000 ? `${num(value / 1000, 1)}k` : num(value, 0);
  }

  function cell(tenor, threshold, value, scale) {
    if (value === null) return el('td', {class: 'none', text: '–'});
    const strength = (0.18 + 0.67 * Math.abs(value) / scale).toFixed(2);
    const rgb = value < 0 ? '248, 81, 73' : '63, 185, 80';
    return el('td', {text: short(value), 'data-key': key(tenor, threshold),
                     title: `M${tenor} · ${threshold} °C · ${num(value, 2)} EUR`,
                     style: `background: rgba(${rgb}, ${strength})`,
                     onclick: () => pick(tenor, threshold)});
  }

  function drawSweep() {
    const node = $('sweep-table');
    if (!sweep?.tenors.length) {
      node.replaceChildren();
      $('sweep-caption').textContent =
        sweep ? 'The newest sweep has no results for this market.' : '';
      return;
    }
    const values = sweep.pnl.flat().filter(v => v !== null);
    const scale = Math.max(1, ...values.map(Math.abs));
    table(node, ['Tenor', ...sweep.thresholds.map(t => `${num(t, 2)} °C`)],
      sweep.tenors.map((tenor, row) => el('tr', {}, el('th', {text: `M${tenor}`}),
        ...sweep.thresholds.map((th, col) => cell(tenor, th, sweep.pnl[row][col], scale)))));
    $('sweep-caption').textContent = `Sweep of ${sweep.as_of} · ${num(sweep.size, 0)} settings`;
    markPicked();
  }

  function pick(tenor, threshold) {
    form().tenor.value = tenor;
    form().threshold.value = threshold;
    markPicked();
  }

  function markPicked() {
    const picked = key(form().tenor.value, form().threshold.value);
    for (const td of $('sweep-table').querySelectorAll('td[data-key]')) {
      td.classList.toggle('picked', td.dataset.key === picked);
    }
  }

  function showResult(record) {
    const r = record.result;
    $('backtest-result').hidden = false;
    chart ??= new Chart($('backtest-chart'), {  // made once it shows, so it has a size
      type: 'line',
      data: {labels: [], datasets: []},
      options: {
        animation: false,
        scales: {x: {ticks: {maxTicksLimit: 8, maxRotation: 0}},
                 y: {title: {display: true, text: 'EUR'}}},
        plugins: {tooltip: {callbacks: {label: c => `${c.dataset.label}: ${num(c.parsed.y, 2)}`}}},
      },
    });
    chart.data.labels = r.dates;
    chart.data.datasets = [
      line('Cumulative P&L', r.cum_pnl_eur, GREEN, {tension: 0}),
      line('Drawdown', r.drawdown_eur, RED, {tension: 0, borderWidth: 1, fill: 'origin',
                                            backgroundColor: 'rgba(248, 81, 73, .18)'}),
    ];
    chart.update();
    $('backtest-caption').textContent = `${record.label} · ${r.dates[0]} to ${r.dates.at(-1)}`;
    $('backtest-tiles').replaceChildren(
      tile('Total P&L', signed(r.total_pnl_eur, 0), 'EUR', trend(r.total_pnl_eur, 0)),
      tile('Max drawdown', num(Math.min(0, ...r.drawdown_eur), 0), 'EUR', 'down'),
      tile('Trades', num(r.n_trades, 0)),
      tile('Hit rate', num(r.hit_rate * 100, 1), '%'),
      tile('Sharpe', num(r.sharpe, 2)));
    table($('backtest-trades'), ['Start', 'End', 'Days', 'P&L EUR'], r.trades.map(t =>
      el('tr', {}, el('td', {text: t.start}), el('td', {text: t.end}), el('td', {text: t.days}),
        el('td', {class: trend(t.pnl_eur), text: signed(t.pnl_eur)}))));
  }

  return {
    init() {
      $('backtest-form').elements.market.addEventListener('change', loadSweep);
      $('backtest-form').addEventListener('input', markPicked);
      $('backtest-run').addEventListener('click', () => {
        const f = form();
        const inputs = {market: f.market.value, tenor: Number(f.tenor.value),
                        threshold: Number(f.threshold.value)};
        startRun($('backtest-run'), $('backtest-note'), {kind: 'backtest', inputs}, WAITING);
      });
    },
    show: loadSweep,  // a sweep may have finished since the page was last open
    showRun(record) {
      const i = record.inputs;
      form().market.value = i.market;
      pick(i.tenor, i.threshold);
      loadSweep();
      if (record.status === 'done' && record.result) {
        showResult(record);
        say($('backtest-note'), `${record.label} is done.`, 'ok');
      } else {
        $('backtest-result').hidden = true;  // the result of another run must not show with it
        runState($('backtest-note'), record, WAITING);
      }
    },
  };
})());

// ---- Jobs: start the desk's two workflows now, and the history of runs from this app

page('jobs', (() => {
  async function loadJobs() {
    try {
      const jobs = await api('/api/jobs');
      $('jobs-error').textContent = '';
      $('job-cards').replaceChildren(...jobs.map(jobCard));
    } catch (error) {
      $('jobs-error').textContent = error.message;
    }
    try {
      const sweep = await api('/api/sweep/GB');  // its size and date are those of the whole sweep
      $('sweep-size')?.replaceChildren(`${num(sweep.size, 0)} settings, ${sweep.as_of}`);
    } catch (error) {
      $('sweep-size')?.replaceChildren(error.message);
    }
  }

  function jobCard(job) {
    const note = el('span', {class: 'note'});
    const button = el('button', {class: 'btn primary', type: 'button', text: 'Run now'});
    button.addEventListener('click', () => runNow(job, button, note));
    const state = job.last_state || 'none';
    return el('div', {class: 'job-card'},
      el('h2', {text: job.name}),
      el('dl', {},
        el('dt', {text: 'Schedule'}), el('dd', {text: job.schedule}),
        el('dt', {text: 'Last run'}), el('dd', {},
          el('span', {class: `status ${state}`, text: state}), ' ',
          job.last_time ? when(job.last_time) : ''),
        ...(job.key === 'strategy-sweep' ? [
          el('dt', {text: 'Newest sweep'}), el('dd', {id: 'sweep-size', text: '–'}),
          el('dt', {text: 'Variable sweep_size'}),  // the workflow reads it when it is deployed
          el('dd', {text: job.sweep_size ? `${job.sweep_size} (applies at the next deploy)`
                                         : 'cannot be read'}),
        ] : [])),
      job.error ? el('p', {class: 'error', text: job.error}) : null,
      el('div', {class: 'actions'}, button, note));
  }

  async function runNow(job, button, note) {
    button.disabled = true;
    try {
      runChanged(await api(`/api/jobs/${job.key}/start`, {}));
      say(note, 'Started. It shows in the runs rail.', 'ok');
    } catch (error) {
      say(note, error.message, 'bad');
    } finally {
      button.disabled = false;
    }
  }

  function drawHistory(list) {
    const rows = list.map(run => el('tr', {class: 'clickable', onclick: () => openRun(run.id)},
      el('td', {text: when(run.created)}),
      el('td', {text: run.label}),
      el('td', {text: run.kind}),
      el('td', {}, el('span', {class: `status ${run.status}`, text: run.status})),
      el('td', {'data-elapsed': run.id, text: elapsed(run)}),
      el('td', {text: run.started_by || '–'})));
    table($('history'), ['Started', 'Run', 'Kind', 'Status', 'Took', 'Started by'], rows.length
      ? rows : [el('tr', {}, el('td', {colspan: 6, class: 'empty', text: 'No runs yet.'}))]);
  }

  return {
    show() {
      loadJobs();
      drawHistory([...runs.values()].sort((a, b) => b.id.localeCompare(a.id)));
    },
    runsChanged: drawHistory,
  };
})());
