/* LLM Service Admin: the page. Three tabs (usage, audit, models) over the JSON API of the app.
 *
 * The platform serves the app under a path prefix, so every URL is relative to BASE, the
 * location of the page. Text from the service goes into the page with textContent or as an
 * attribute value, never as HTML. */

'use strict';

const BASE = new URL(location.pathname.endsWith('/') ? location.pathname : location.pathname + '/',
                     location.origin);
const PAGE_SIZE = 50;
const POLL_MS = 3000;
const MAX_POLL_FAILURES = 5;
const COLORS = {bar: '#4c8dff', bar2: '#36c2b4', grid: 'rgba(56, 66, 82, .55)', text: '#9aa7b8'};

const $ = id => document.getElementById(id);
const number = new Intl.NumberFormat();
const state = {tab: null, range: '30', loaded: {}, offset: 0, polling: false, pollTimer: null, pollFailures: 0,
               detailUser: null, chartsOff: false,
               charts: {}, pendingDelete: null, auditUsersRead: false, calls: {}};

/** A number for a new call of `name`. A call that is no longer the newest drops its answer. */
const nextCall = name => (state.calls[name] = (state.calls[name] || 0) + 1);

// ---- helpers

/** An element with properties (text, className, title, onclick and attributes) and children. */
function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === undefined || value === null) continue;
    if (key === 'text') node.textContent = value;
    else if (key === 'className') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const child of children) if (child) node.append(child);
  return node;
}

const fmt = value => (value === null || value === undefined ? '-' : number.format(value));

function fmtSize(bytes) {
  if (bytes === null || bytes === undefined) return 'unknown';
  const gb = bytes / 1024 ** 3;
  if (gb >= 1) return `${gb.toLocaleString(undefined, {maximumFractionDigits: 1})} GB`;
  return `${(bytes / 1024 ** 2).toLocaleString(undefined, {maximumFractionDigits: 1})} MB`;
}

/** A time in the local time of the viewer, with the UTC value as a tooltip. */
function timeCell(iso) {
  if (!iso) return el('td', {text: '-', className: 'muted'});
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return el('td', {text: iso});
  return el('td', {text: date.toLocaleString(), title: `${date.toISOString()} UTC`, className: 'nowrap'});
}

const shorten = (text, size) => (text.length > size ? `${text.slice(0, size - 1)}…` : text);
const modelText = ref => `${ref.model} @ ${ref.revision}`;
const servingText = engine => (engine.serving
  ? `The service serves ${modelText(engine.serving)}.` : 'The service serves no model.');

function replaceRows(body, rows, columns, empty) {
  body.replaceChildren(...rows);
  if (!rows.length) body.append(el('tr', {}, el('td', {colspan: columns, className: 'empty', text: empty})));
}

// ---- API

class ApiError extends Error {}

/** Call the app. Return the JSON answer, or throw an ApiError with the message of the service. */
async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(new URL(path, BASE), options);
  } catch {
    throw new ApiError('The admin app does not answer.');
  }
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    const message = data && data.error && data.error.message;
    throw new ApiError(typeof message === 'string' ? message : `The admin app answered ${response.status}.`);
  }
  return data;
}

const query = params => {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) if (value) search.set(key, value);
  const text = search.toString();
  return text ? `?${text}` : '';
};

/** A model id such as `org/name` as a path, with each segment encoded. */
const modelPath = id => id.split('/').map(encodeURIComponent).join('/');

function showBanner(message, kind = 'error') {
  $('banner-text').textContent = message;
  $('banner').className = `banner ${kind}`;
  $('banner').hidden = false;
}

/** Run a load and show its error in the banner. The last data stays on screen. */
async function guarded(work) {
  try {
    await work();
    return true;
  } catch (error) {
    showBanner(error instanceof ApiError ? error.message : String(error));
    return false;
  }
}

// ---- tabs

const TABS = ['usage', 'audit', 'models'];
const LOADERS = {usage: loadUsage, audit: loadAudit, models: loadStatus};

function selectTab(name, focus = false) {
  if (!TABS.includes(name)) name = 'usage';
  state.tab = name;
  for (const tab of TABS) {
    const button = $(`tab-${tab}`);
    const active = tab === name;
    button.setAttribute('aria-selected', String(active));
    button.tabIndex = active ? 0 : -1;
    $(`panel-${tab}`).hidden = !active;
    if (active && focus) button.focus();
  }
  if (location.hash !== `#${name}`) history.replaceState(null, '', `#${name}`);
  if (!state.loaded[name]) {
    state.loaded[name] = true;
    guarded(LOADERS[name]);
  }
}

function tabKeys(event) {
  const index = TABS.indexOf(state.tab);
  const moves = {ArrowRight: index + 1, ArrowLeft: index - 1, Home: 0, End: TABS.length - 1};
  if (!(event.key in moves)) return;
  event.preventDefault();
  selectTab(TABS[(moves[event.key] + TABS.length) % TABS.length], true);
}

// ---- usage

const day = date => date.toISOString().slice(0, 10);

/** The range as UTC days, both inclusive. */
function usageRange() {
  if (state.range === 'custom') return {since: $('range-since').value, until: $('range-until').value};
  const until = new Date();
  const since = new Date(until.getTime() - (Number(state.range) - 1) * 86400000);
  return {since: day(since), until: day(until)};
}

function themeCharts() {
  Object.assign(Chart.defaults, {color: COLORS.text, borderColor: COLORS.grid, maintainAspectRatio: false});
  Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
  Chart.defaults.font.size = 11;
  Chart.defaults.animation.duration = 250;
  Chart.defaults.plugins.legend.display = false;
  Object.assign(Chart.defaults.plugins.tooltip, {backgroundColor: '#1a212c', borderColor: '#2c3542',
                                                 borderWidth: 1, padding: 10, titleColor: '#e7edf5',
                                                 bodyColor: '#e7edf5'});
}

const compact = new Intl.NumberFormat(undefined, {notation: 'compact'});

/** A bar chart of total tokens. `shortLabel` shortens the category labels on the axis. */
function barChart(id, color, horizontal, shortLabel) {
  const [value, category] = horizontal ? ['x', 'y'] : ['y', 'x'];
  return new Chart($(id), {
    type: 'bar',
    data: {labels: [], datasets: [{label: 'Total tokens', data: [], backgroundColor: color,
                                   borderRadius: 4, maxBarThickness: 36}]},
    options: {
      indexAxis: category,
      scales: {
        [value]: {beginAtZero: true, ticks: {callback: v => compact.format(v), maxRotation: 0}},
        [category]: {grid: {display: false},
                     ticks: {maxRotation: 0, autoSkipPadding: 12,
                             callback(v) { return shortLabel(this.getLabelForValue(v)); }}},
      },
      plugins: {tooltip: {callbacks: {label: item => `${number.format(item.raw)} tokens`}}},
    },
  });
}

/** Turn the charts off after a failure. The tiles and the tables still work. */
function chartsFailed() {
  state.chartsOff = true;
  document.querySelector('.charts').hidden = true;
  showBanner('The charts could not load. The numbers and tables still work.');
}

function setChart(chart, labels, values) {
  chart.data.labels = labels;
  chart.data.datasets[0].data = values;
  chart.update();
}

/** Each day of the range, with zero for a day without requests. */
function daySeries(rows, since, until) {
  const totals = new Map(rows.map(row => [row.day, row.total_tokens]));
  const days = [];
  const end = new Date(`${until}T00:00:00Z`);
  for (let at = new Date(`${since}T00:00:00Z`); at <= end && days.length < 400; at = new Date(at.getTime() + 86400000)) {
    days.push(day(at));
  }
  if (!since || !until || !days.length) days.push(...[...totals.keys()].sort());
  return [days, days.map(d => totals.get(d) || 0)];
}

async function loadUsage() {
  const call = nextCall('usage');
  const range = usageRange();
  $('range-text').textContent = range.since && range.until ? `${range.since} to ${range.until}, UTC days` : '';
  const [users, days, models] = await Promise.all(['user', 'day', 'model'].map(
    group => api(`api/usage${query({...range, group_by: group})}`)));
  if (call !== state.calls.usage) return;
  const sum = key => users.data.reduce((total, row) => total + row[key], 0);
  $('tile-requests').textContent = fmt(sum('requests'));
  $('tile-failed').textContent = fmt(sum('failed_requests'));
  $('tile-tokens').textContent = fmt(sum('total_tokens'));
  $('tile-users').textContent = fmt(users.data.length);
  if (!state.chartsOff) {
    try {
      state.charts.days ||= barChart('chart-days', COLORS.bar, false, label => label.slice(5));
      state.charts.models ||= barChart('chart-models', COLORS.bar2, true,
                                       label => shorten(label.split('/').pop(), 24));
      setChart(state.charts.days, ...daySeries(days.data, range.since, range.until));
      setChart(state.charts.models, models.data.map(row => row.model), models.data.map(row => row.total_tokens));
    } catch {
      chartsFailed();  // only once: the charts stay off after this
    }
  }
  replaceRows($('users-body'), users.data.map(userRow), 7, 'No requests in this range.');
  if (state.detailUser) await loadDetail(state.detailUser);
}

function userRow(row) {
  const open = () => guarded(() => loadDetail(row.user));
  return el('tr', {tabindex: '0', className: row.user === state.detailUser ? 'selected' : null, onclick: open,
                   onkeydown: event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); open(); } }},
    el('th', {scope: 'row', text: row.user}),
    el('td', {className: 'num', text: fmt(row.requests)}),
    el('td', {className: row.failed_requests ? 'num bad' : 'num', text: fmt(row.failed_requests)}),
    el('td', {className: 'num', text: fmt(row.prompt_tokens)}),
    el('td', {className: 'num', text: fmt(row.completion_tokens)}),
    el('td', {className: 'num strong', text: fmt(row.total_tokens)}),
    timeCell(row.last_request));
}

function smallRow(key, row) {
  return el('tr', {}, el('th', {scope: 'row', text: row[key]}),
    el('td', {className: 'num', text: fmt(row.requests)}),
    el('td', {className: 'num', text: fmt(row.failed_requests)}),
    el('td', {className: 'num', text: fmt(row.total_tokens)}));
}

async function loadDetail(user) {
  const call = nextCall('detail');
  const detail = await api(`api/usage/${encodeURIComponent(user)}${query(usageRange())}`);
  if (call !== state.calls.detail) return;
  state.detailUser = user;
  for (const row of $('users-body').rows) row.classList.toggle('selected', row.cells[0].textContent === user);
  $('detail-user').textContent = user;
  replaceRows($('detail-models'), detail.by_model.map(row => smallRow('model', row)), 4, 'No requests.');
  const days = [...detail.by_day].sort((a, b) => b.day.localeCompare(a.day));
  replaceRows($('detail-days'), days.map(row => smallRow('day', row)), 4, 'No requests.');
  $('detail').hidden = false;
}

function setRange(range) {
  state.range = range;
  for (const button of document.querySelectorAll('[data-range]')) {
    button.setAttribute('aria-pressed', String(button.dataset.range === range));
  }
  $('dates').hidden = range !== 'custom';
  if (range === 'custom' && !$('range-since').value) {
    $('range-until').value = day(new Date());
    $('range-since').value = day(new Date(Date.now() - 29 * 86400000));
  }
  guarded(loadUsage);
}

// ---- audit

/** A datetime-local value (local time) as an ISO time in UTC. */
const isoTime = value => (value ? new Date(value).toISOString() : '');

/** Fill the suggestions of the user filter. The filter also takes free text, so a failure only
 * shows a banner. Reload tries again. */
async function loadAuditUsers() {
  state.auditUsersRead = true;
  try {
    const answer = await api('api/audit/users');
    $('audit-users').replaceChildren(...answer.data.map(row => el('option', {value: row.user})));
  } catch (error) {
    showBanner(`The page could not read the list of users: ${error.message}`);
  }
}

async function loadAudit() {
  const call = nextCall('audit');
  if (!state.auditUsersRead) await loadAuditUsers();
  const filters = {user: $('audit-user').value.trim(), model: $('audit-model').value.trim(),
                   since: isoTime($('audit-since').value), until: isoTime($('audit-until').value)};
  const answer = await api(`api/audit${query({...filters, limit: PAGE_SIZE, offset: state.offset})}`);
  if (call !== state.calls.audit) return;
  const rows = answer.data;
  replaceRows($('audit-body'), rows.map(auditRow), 7, 'No records match the filters.');
  $('audit-newer').disabled = state.offset === 0;
  $('audit-older').disabled = rows.length < PAGE_SIZE;
  $('audit-page').textContent = rows.length
    ? `Records ${fmt(state.offset + 1)} to ${fmt(state.offset + rows.length)}` : '';
}

function auditRow(record) {
  const open = () => {
    $('record-json').textContent = JSON.stringify(record, null, 2);
    $('record-dialog').showModal();
  };
  const event = record.prompt && typeof record.prompt === 'object' && !Array.isArray(record.prompt)
    ? record.prompt.event : null;
  const ok = record.status >= 200 && record.status < 300;
  return el('tr', {tabindex: '0', onclick: open,
                   onkeydown: e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); } }},
    timeCell(record.ts),
    el('td', {text: record.user}),
    el('td', {className: 'mono'}, record.endpoint || '-',
       typeof event === 'string' ? el('span', {className: 'tag', text: event.replace(/_/g, ' ')}) : null),
    el('td', {text: record.model || '-'}),
    el('td', {}, el('span', {className: `pill ${ok ? 'good' : 'bad'}`, text: String(record.status ?? '-')})),
    el('td', {className: 'num', text: fmt(record.total_tokens)}),
    el('td', {className: 'num', text: record.duration_ms === null || record.duration_ms === undefined
      ? '-' : `${fmt(record.duration_ms)} ms`}));
}

/** Move the audit table by `step` pages. A step of 0 goes back to the newest page. */
function pageAudit(step) {
  state.offset = step === 0 ? 0 : Math.max(0, state.offset + step * PAGE_SIZE);
  guarded(loadAudit);
}

// ---- models

/** True while a switch runs or a failed switch loads the previous model again. */
const busy = engine => engine.state === 'loading' || Boolean(engine.rolling_back);

function showHeader(engine) {
  const loading = engine.state === 'loading';
  $('serving-text').textContent = loading && engine.target ? `Loading ${modelText(engine.target)}`
    : engine.rolling_back ? `Loading ${modelText(engine.rolling_back)} again`
    : engine.serving ? modelText(engine.serving) : 'The service serves no model.';
  $('serving-dot').className = `dot ${busy(engine) ? 'loading' : engine.state}`;
}

function showStatus({engine, models}) {
  showHeader(engine);
  $('st-state').textContent = engine.state;
  $('st-state').className = `badge ${engine.state}`;
  $('st-serving').textContent = engine.serving ? modelText(engine.serving) : 'The service serves no model.';
  $('st-target').textContent = engine.target ? modelText(engine.target) : 'No switch runs.';
  $('st-error').textContent = engine.error || 'None';
  $('st-error').classList.toggle('bad', Boolean(engine.error));
  $('st-rollback-row').hidden = !engine.rolling_back;
  $('st-rollback').textContent = engine.rolling_back
    ? `The service loads ${modelText(engine.rolling_back)} again after a failed switch.` : '-';
  replaceRows($('models-body'), models.map(modelRow), 5, 'The service has no cached model.');
  setBusy(state.polling);
}

function modelRow(entry) {
  const actions = el('div', {className: 'stack'});
  if (entry.status === 'cached') {
    const many = entry.revisions.length > 1;
    for (const revision of entry.revisions) {
      const label = many ? ` ${revision}` : '';
      actions.append(el('div', {className: 'pair'},
        el('button', {type: 'button', className: 'small', 'data-busy': '', text: `Serve${label}`,
                      'aria-label': `Serve ${entry.id} at ${revision}`,
                      onclick: () => serve(entry.id, revision)}),
        el('button', {type: 'button', className: 'small danger-ghost', 'data-busy': '', text: `Delete${label}`,
                      'aria-label': `Delete ${entry.id} at ${revision}`,
                      onclick: () => askDelete(entry.id, revision)})));
    }
  }
  return el('tr', {},
    el('th', {scope: 'row', className: 'mono', text: entry.id}),
    el('td', {}, el('span', {className: `badge ${entry.status}`, text: entry.status})),
    el('td', {className: 'mono', text: entry.revisions.length ? entry.revisions.join(', ') : '-'}),
    el('td', {className: 'num', text: fmtSize(entry.size_bytes)}),
    el('td', {}, actions));
}

function setBusy(busy) {
  for (const button of document.querySelectorAll('[data-busy]')) button.disabled = busy;
  $('serve-submit').disabled = busy;
  $('poll-text').hidden = !busy;
}

async function loadStatus() {
  const status = await api('api/status');
  showStatus(status);
  if (busy(status.engine)) startPoll();
  return status;
}

function startPoll() {
  if (state.polling) return;
  state.polling = true;
  state.pollFailures = 0;
  setBusy(true);
  state.pollTimer = setTimeout(poll, POLL_MS);
}

function stopPoll() {
  clearTimeout(state.pollTimer);
  state.pollTimer = null;
  state.polling = false;
  setBusy(false);
}

/** Read the state until the switch and a rollback after it end. Stop after MAX_POLL_FAILURES failed
 * reads in a row. */
async function poll() {
  state.pollTimer = null;
  let status;
  try {
    status = await api('api/status');
  } catch (error) {
    state.pollFailures += 1;
    if (state.pollFailures < MAX_POLL_FAILURES) {
      state.pollTimer = setTimeout(poll, POLL_MS);
    } else {
      stopPoll();
      showBanner(`The page could not read the state of the service: ${error.message} Use Reload to read it again.`);
    }
    return;
  }
  state.pollFailures = 0;
  showStatus(status);
  if (busy(status.engine)) {
    state.pollTimer = setTimeout(poll, POLL_MS);
    return;
  }
  stopPoll();
  reportSwitch(status.engine);
}

/** Say how the switch ended. The poll ends only after a rollback, so no rollback runs now. A
 * successful load clears the error, so an error means a failed switch, also when the state is
 * `ready` again with the old model. */
function reportSwitch(engine) {
  if (engine.error) {
    const now = engine.serving ? `The service still serves ${modelText(engine.serving)}.` : 'The service serves no model.';
    showBanner(`The switch failed: ${engine.error.replace(/[.\s]+$/, '')}. ${now}`);
  } else if (engine.state === 'ready') {
    showBanner(servingText(engine), 'ok');
  } else {
    showBanner(`The switch ended in the state ${engine.state}. ${servingText(engine)}`);
  }
}

async function serve(model, revision) {
  setBusy(true);
  const done = await guarded(async () => {
    await api('api/model', {method: 'POST', headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({model, revision})});
    showBanner(`The service loads ${model} @ ${revision}.`, 'ok');
    startPoll();  // first, so the poll runs even if the next read fails
    await loadStatus();
  });
  if (!done) setBusy(state.polling);
}

function askDelete(model, revision) {
  state.pendingDelete = {model, revision};
  $('confirm-text').textContent =
    `Delete the cached weights of ${model} at revision ${revision}? The service removes them from blob storage. ` +
    'A later switch to this model downloads them again.';
  $('confirm-dialog').showModal();
}

async function confirmDelete() {
  const {model, revision} = state.pendingDelete;
  $('confirm-dialog').close();
  setBusy(true);
  await guarded(async () => {
    await api(`api/models/${modelPath(model)}${query({revision})}`, {method: 'DELETE'});
    showBanner(`The service deleted ${model} @ ${revision}.`, 'ok');
  });
  setBusy(state.polling);
  guarded(loadStatus);
}

// ---- start

function wire() {
  for (const tab of TABS) $(`tab-${tab}`).addEventListener('click', () => selectTab(tab));
  document.querySelector('[role="tablist"]').addEventListener('keydown', tabKeys);
  $('reload').addEventListener('click', () => {
    $('banner').hidden = true;
    if (state.tab === 'audit') state.auditUsersRead = false;
    guarded(LOADERS[state.tab]);
    if (state.tab !== 'models') guarded(loadStatus);  // the header shows the served model
  });
  $('banner-close').addEventListener('click', () => { $('banner').hidden = true; });
  for (const button of document.querySelectorAll('[data-range]')) {
    button.addEventListener('click', () => setRange(button.dataset.range));
  }
  for (const id of ['range-since', 'range-until']) $(id).addEventListener('change', () => guarded(loadUsage));
  $('detail-close').addEventListener('click', () => {
    state.detailUser = null;
    $('detail').hidden = true;
    for (const row of $('users-body').rows) row.classList.remove('selected');
  });
  $('audit-form').addEventListener('submit', event => { event.preventDefault(); pageAudit(0); });
  $('audit-clear').addEventListener('click', () => { $('audit-form').reset(); pageAudit(0); });
  $('audit-newer').addEventListener('click', () => pageAudit(-1));
  $('audit-older').addEventListener('click', () => pageAudit(1));
  $('serve-form').addEventListener('submit', event => {
    event.preventDefault();
    serve($('serve-model').value.trim(), $('serve-revision').value.trim() || 'main');
  });
  $('confirm-delete').addEventListener('click', confirmDelete);
  for (const button of document.querySelectorAll('dialog [data-close]')) {
    button.addEventListener('click', () => button.closest('dialog').close());
  }
}

async function start() {
  wire();
  try {
    themeCharts();
  } catch {
    state.chartsOff = true;  // without Chart.js, the tiles and tables still work
  }
  let me;
  try {
    me = await api('api/me');
  } catch (error) {
    showBanner(error.message);
    return;
  }
  if (!me.admin) {
    $('denied').hidden = false;
    return;
  }
  $('viewer').textContent = me.user;
  $('topbar').hidden = false;
  $('main').hidden = false;
  if (state.chartsOff) chartsFailed();
  for (const button of document.querySelectorAll('[data-range]')) {
    button.setAttribute('aria-pressed', String(button.dataset.range === state.range));
  }
  selectTab(location.hash.slice(1));
  if (state.tab !== 'models') guarded(loadStatus);
}

document.addEventListener('DOMContentLoaded', start);
