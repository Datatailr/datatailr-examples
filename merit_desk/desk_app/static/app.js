/* Merit Desk: the shell of the page. The API helper, the live event stream, the top bar, the
 * runs rail and the menu. pages.js holds the five pages.
 *
 * The platform serves an app under a path of its own. The page's <base> names it, and every
 * URL starts from BASE. Text from the server goes into the page as text (textContent), never as
 * HTML. */

'use strict';

const BASE = new URL(document.baseURI).pathname.replace(/\/+$/, '');  // '' at the root
const MARKETS = ['GB', 'DE', 'FR'];
const COLORS = {GB: '#58a6ff', DE: '#e3b341', FR: '#39d2c0'};
const ENDED = ['done', 'failed'];
const MAX_ACTIVE = 3;  // the app runs at most three clicks at a time

// ---- small helpers

const $ = id => document.getElementById(id);

/** An element with attributes, listeners (onclick: ...), text and children. */
function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'text') node.textContent = value;
    else if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  node.append(...children.filter(child => child !== null && child !== undefined));
  return node;
}

/** 1,234.57: a fixed number of decimals, and never "-0.00". */
function num(value, digits = 2) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '–';
  const rounded = Number(value.toFixed(digits)) || 0;  // || 0 turns -0 into 0
  return rounded.toLocaleString('en-GB',
    {minimumFractionDigits: digits, maximumFractionDigits: digits});
}

/** The sign of a value as it shows with these decimals: 1, -1 or 0. */
function sign(value, digits = 2) {
  return Math.sign(Number(value.toFixed(digits)) || 0);
}

function signed(value, digits = 2) {
  return (sign(value, digits) > 0 ? '+' : '') + num(value, digits);
}

const trend = (value, digits = 2) => ['down', 'flat', 'up'][sign(value, digits) + 1];
const eur = value => num(value, 2);  // a price in EUR/MWh
const mean = values => values.reduce((a, b) => a + b, 0) / values.length;

function when(iso) {
  const time = new Date(iso);
  if (Number.isNaN(time.getTime())) return '–';
  return time.toLocaleString('en-GB', {day: '2-digit', month: 'short', hour: '2-digit',
                                       minute: '2-digit'});
}

function elapsed(run) {
  const end = ENDED.includes(run.status) ? Date.parse(run.updated) : Date.now();
  const s = Math.max(0, Math.round((end - Date.parse(run.created)) / 1000)) || 0;
  const pad = n => String(n).padStart(2, '0');
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} min ${pad(s % 60)} s`;
  return `${Math.floor(s / 3600)} h ${pad(Math.floor(s / 60) % 60)} min`;
}

function debounce(work, ms = 250) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => work(...args), ms); };
}

/** A note next to a button: a plain text, an 'ok' text or a 'bad' one. */
function say(node, text, tone = '') {
  node.textContent = text;
  node.className = `note ${tone}`;
}

function tile(label, value, unit = '', tone = '') {
  return el('div', {class: 'tile'}, el('div', {class: 'label', text: label}),
    el('div', {class: `value ${tone}`, text: value},
      unit ? el('span', {class: 'unit', text: unit}) : null));
}

function table(node, head, rows) {
  node.replaceChildren(
    el('thead', {}, el('tr', {}, ...head.map(text => el('th', {text})))),
    el('tbody', {}, ...rows));
}

// ---- the API: every error comes back as one text, the server's `detail`

class ApiError extends Error {
  constructor(status, detail) { super(detail); this.status = status; }
}

/** GET a path, or POST a body to it. The JSON answer, or an ApiError with the server's text. */
async function api(path, body) {
  const options = body === undefined ? {} : {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)};
  let response;
  try {
    response = await fetch(BASE + path, options);
  } catch {
    throw new ApiError(0, 'The app is not reachable.');
  }
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = data?.detail;
    throw new ApiError(response.status,
                       typeof detail === 'string' ? detail : `The app answered ${response.status}.`);
  }
  return data;
}

// ---- charts: one dark theme for all of them

function themeCharts() {
  Object.assign(Chart.defaults, {color: '#8b949e', borderColor: 'rgba(48, 54, 61, .7)',
                                 maintainAspectRatio: false});
  Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
  Chart.defaults.font.size = 11;
  Chart.defaults.animation.duration = 250;
  Chart.defaults.elements.point.radius = 0;
  Chart.defaults.elements.point.hoverRadius = 4;
  Object.assign(Chart.defaults.interaction, {mode: 'index', intersect: false});
  Object.assign(Chart.defaults.plugins.legend.labels, {boxWidth: 12, boxHeight: 3});
  Object.assign(Chart.defaults.plugins.tooltip, {backgroundColor: '#1c2333', borderColor: '#30363d',
                                                 borderWidth: 1, padding: 10});
}

/** A line chart of prices by month. Its datasets come later, with chart.data. */
function monthChart(canvas, extra = {}) {
  return new Chart(canvas, {
    type: 'line',
    data: {labels: [], datasets: []},
    options: {
      scales: {y: {title: {display: true, text: 'EUR/MWh'}}},
      plugins: {tooltip: {callbacks: {label: c => `${c.dataset.label}: ${eur(c.parsed.y)}`}}},
      ...extra,
    },
  });
}

// ---- the top bar: simulated prices, the connection, the environment and the user

const firstPrice = {};  // the change shows against the first price this page received

function showPrices(data) {
  $('prices').classList.toggle('stale', Boolean(data.error));
  $('feed-error').textContent = data.error || '';
  if (data.error) return;
  for (const code of MARKETS) {
    const tick = data[code];
    if (typeof tick?.price !== 'number') continue;
    firstPrice[code] ??= tick.price;
    const change = tick.price - firstPrice[code];
    const node = document.querySelector(`.tick[data-market="${code}"]`);
    const before = node.querySelector('.px').textContent;
    node.querySelector('.px').textContent = eur(tick.price);
    const percent = firstPrice[code] ? (change / firstPrice[code]) * 100 : 0;
    const chg = node.querySelector('.chg');
    chg.textContent = `${signed(change)} (${signed(percent)}%)`;
    chg.className = `chg ${trend(change)}`;
    if (before !== '–' && before !== node.querySelector('.px').textContent) {
      const up = tick.price > Number(before.replace(/,/g, ''));
      node.classList.remove('flash-up', 'flash-down');
      void node.offsetWidth;  // restart the animation
      node.classList.add(up ? 'flash-up' : 'flash-down');
    }
  }
}

function setLive(state) {
  $('live').className = `live ${{live: 'on', closed: 'off'}[state] || ''}`;
  $('live-text').textContent = state;
}

async function loadContext() {
  try {
    const context = await api('/api/context');
    $('env').textContent = context.environment;
    $('env').className = `env ${context.environment}`;
    $('user').textContent = context.user || '';
  } catch (error) {
    $('env').textContent = '?';
    $('env').title = error.message;
  }
}

// ---- the live stream: prices every second, and each change of a run

let reconnected = false;

function connect() {
  const source = new EventSource(BASE + '/api/events');  // it reconnects by itself
  source.addEventListener('open', () => {
    setLive('live');
    if (reconnected) loadRuns();  // run changes during the gap did not reach this page
    reconnected = true;
  });
  source.addEventListener('error', () => {
    if (source.readyState !== EventSource.CLOSED) return setLive('reconnecting');
    setLive('closed');  // the server refused the stream: try again a little later
    setTimeout(connect, 5000);
  });
  source.addEventListener('prices', event => showPrices(JSON.parse(event.data)));
  source.addEventListener('run', event => runChanged(JSON.parse(event.data)));
}

// ---- the runs rail

const runs = new Map();     // id -> the run summary, as the list and the events give it
const watched = new Set();  // runs started from a page: their result opens on that page
let openedRun = null;

async function loadRuns() {
  try {
    const list = await api('/api/runs?limit=50');
    runs.clear();
    for (const run of list) runs.set(run.id, run);
    $('runs-error').textContent = '';
    renderRuns();
  } catch (error) {
    $('runs-error').textContent = error.message;
  }
}

/** The page that shows a run: a scenario with only plant changes came from Merit order. */
function pageOf(run) {
  if (run.kind === 'backtest') return 'backtest';
  if (run.kind === 'job') return 'jobs';
  const inputs = run.inputs || {};
  return inputs.shift_c === 0 && inputs.overrides?.length ? 'merit' : 'scenario';
}

function runChanged(run) {
  const before = runs.get(run.id);
  if (ENDED.includes(before?.status) && !ENDED.includes(run.status)) {
    run = before;  // a late event: a run that has ended never shows as active again
  }
  runs.set(run.id, run);
  renderRuns();
  if (ENDED.includes(run.status) && watched.delete(run.id) && current === pageOf(run)) {
    openRun(run.id);  // a run started on this page shows its result here when it ends
  }
}

function watch(run) {
  watched.add(run.id);
  runChanged(run);
}

function runCard(run) {
  const open = () => openRun(run.id);
  const key = event => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    open();
  };
  return el('div', {class: `run ${run.status}${run.id === openedRun ? ' open' : ''}`,
                    role: 'button', tabindex: '0', title: 'Open this run on its page',
                    onclick: open, onkeydown: key},
    el('div', {class: 'run-top'},
      el('span', {class: 'run-label', text: run.label}),
      el('span', {class: `status ${run.status}`, text: run.status})),
    el('div', {class: 'run-meta'},
      el('span', {'data-elapsed': run.id, text: elapsed(run)}),
      el('span', {text: when(run.created)}),
      el('span', {text: run.started_by || ''})),
    run.job_name && run.job_name !== run.label
      ? el('div', {class: 'run-job', title: 'The job on Datatailr', text: run.job_name}) : null,
    run.status === 'failed' && run.error ? el('div', {class: 'run-error', text: run.error}) : null);
}

function renderRuns() {
  const newest = [...runs.values()].sort((a, b) => b.id.localeCompare(a.id)).slice(0, 50);
  const kept = new Set(newest.map(run => run.id));
  for (const id of runs.keys()) if (!kept.has(id)) runs.delete(id);
  const active = newest.filter(run => run.kind !== 'job' && !ENDED.includes(run.status)).length;
  $('slots').textContent = `${active} of ${MAX_ACTIVE} run slots busy`;
  $('runs').replaceChildren(...(newest.length ? newest.map(runCard)
    : [el('p', {class: 'empty', text: 'No runs yet. Run a scenario or a backtest as a job.'})]));
  pages[current]?.runsChanged?.(newest);
}

function tickElapsed() {
  for (const node of document.querySelectorAll('[data-elapsed]')) {
    const run = runs.get(node.dataset.elapsed);
    if (run && !ENDED.includes(run.status)) node.textContent = elapsed(run);
  }
}

/** Show a run on its page. The list has no results, so the whole record is fetched. */
async function openRun(id) {
  const run = runs.get(id);
  if (!run) return;
  openedRun = id;
  go(pageOf(run));
  renderRuns();
  try {
    const record = await api(`/api/runs/${id}`);
    if (openedRun === id) pages[pageOf(record)].showRun?.(record);
    if (!ENDED.includes(record.status)) watched.add(id);  // its result opens when it ends
  } catch (error) {
    $('runs-error').textContent = `Could not open ${run.label}: ${error.message}`;
  }
}

// ---- the menu: one page shows at a time, and the address keeps it (#scenario)

const pages = {};  // name -> {init, show, showRun, runsChanged}: pages.js fills it
let current = null;

function page(name, handlers) {
  pages[name] = handlers;
}

function show(name) {
  if (!pages[name]) name = 'curves';
  for (const section of document.querySelectorAll('.page')) {
    section.hidden = section.dataset.page !== name;
  }
  for (const link of document.querySelectorAll('.menu a')) {
    link.classList.toggle('on', link.dataset.page === name);
  }
  const handlers = pages[name];
  if (!handlers.started) {
    handlers.started = true;
    handlers.init?.();
  }
  current = name;
  handlers.show?.();
}

function go(name) {
  if (location.hash !== `#${name}`) history.pushState(null, '', `#${name}`);
  if (current !== name) show(name);
}

function start() {
  if (location.pathname !== `${BASE}/`) {  // the menu links resolve against <base>: no reload
    history.replaceState(null, '', `${BASE}/${location.search}${location.hash}`);
  }
  themeCharts();
  window.addEventListener('hashchange', () => show(location.hash.slice(1)));
  show(location.hash.slice(1));
  loadContext();
  loadRuns();
  connect();
  setInterval(tickElapsed, 1000);
}

document.addEventListener('DOMContentLoaded', start);
