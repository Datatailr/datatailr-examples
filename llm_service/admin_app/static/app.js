/* LLM Service Admin: the page. A service picker and six tabs (usage, load, connect, audit, models,
 * limits) over the JSON API of the app.
 *
 * The platform serves the app under a path prefix, so every URL is relative to BASE, the
 * location of the page. Text from the service goes into the page with textContent or as an
 * attribute value, never as HTML. The URL hash is `#<tab>` or `#<tab>/<service>`. */

'use strict';

const BASE = new URL(location.pathname.endsWith('/') ? location.pathname : location.pathname + '/',
                     location.origin);
const PAGE_SIZE = 50;
const POLL_MS = 3000;
const MAX_POLL_FAILURES = 5;
// The limits of the chat route of the app.
const CHAT_MAX_MESSAGES = 50;
const CHAT_MAX_CHARACTERS = 100000;
const COPIED_MS = 1500;
const COLORS = {bar: '#4c8dff', bar2: '#36c2b4', red: '#f26d6d', yellow: '#e2b444', grid: 'rgba(56, 66, 82, .55)',
                text: '#9aa7b8'};

const $ = id => document.getElementById(id);
const number = new Intl.NumberFormat();
// `service` is the picked service, or null for "All services". `services` is the answer of
// `GET api/services`. A request of a tab keeps the service of its start and drops a late answer.
const state = {tab: null, service: null, services: null, range: '30', loaded: {}, offset: 0,
               polling: false, pollTimer: null, pollFailures: 0, pollService: null,
               detailUser: null, chartsOff: false,
               loadRange: 'hour', loadTimer: null, loadFailures: 0, loadBusy: null, loadCharts: {},
               charts: {}, pendingDelete: null, pendingRemove: null, auditUsersRead: false, calls: {},
               chat: {messages: [], controller: null, defaultModel: ''}};

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

/** An error of the app or of a service. `status` and `code` come from an error answer. `retryAfter`
 * is the wait in whole seconds from the `Retry-After` header of that answer, or null. */
class ApiError extends Error {
  constructor(message, {status = null, code = null, retryAfter = null} = {}) {
    super(message);
    Object.assign(this, {status, code, retryAfter});
  }
}

/** The whole seconds of a `Retry-After` value, or null for a value that is not 1 to 5 digits. */
const retryWait = value => (typeof value === 'string' && /^\d{1,5}$/.test(value) ? Number(value) : null);

/** The ApiError of an error answer with the JSON body `data`, with the message of the service. */
function answerError(response, data) {
  const error = data && typeof data.error === 'object' ? data.error : null;
  const message = error && typeof error.message === 'string' ? error.message : `The admin app answered ${response.status}.`;
  return new ApiError(message, {status: response.status, code: error && typeof error.code === 'string' ? error.code : null,
                                retryAfter: retryWait(response.headers.get('Retry-After'))});
}

/** Call the app. Return the JSON answer, or throw an ApiError with the message of the service. */
async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(new URL(path, BASE), options);
  } catch {
    throw new ApiError('The admin app does not answer.');
  }
  const data = await response.json().catch(() => null);
  if (!response.ok) throw answerError(response, data);
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

/** The path of a route of a service, relative to BASE. The default is the picked service. */
function servicePath(path, service = state.service) {
  return `api/services/${encodeURIComponent(service)}/${path}`;
}

/** The entry of the service `name` in the list of services, or undefined. */
const entryOf = name => (state.services ? state.services.services.find(entry => entry.name === name) : undefined);

/** The role of the viewer on the picked service. With "All services", no role. */
function currentRole() {
  const entry = state.service ? entryOf(state.service) : undefined;
  return {access: Boolean(entry && entry.access), admin: Boolean(entry && entry.admin)};
}

function showBanner(message, kind = 'error') {
  $('banner-text').textContent = message;
  $('banner').className = `banner ${kind}`;
  $('banner').hidden = false;
}

/** The answer or the error of a call that is no longer current. `guarded` drops it. */
class StaleAnswer extends Error {}

/** Await `request`, the call number `call` of `name` for `service`. Throw StaleAnswer for its
 * answer and for its error when a newer call of `name` started or the picked service changed. */
async function current(name, call, service, request) {
  const stale = () => call !== state.calls[name] || service !== state.service;
  try {
    const answer = await request;
    if (stale()) throw new StaleAnswer();
    return answer;
  } catch (error) {
    if (stale()) throw new StaleAnswer();
    throw error;
  }
}

/** Run a load and show its error in the banner. The last data stays on screen. A stale answer
 * shows nothing. */
async function guarded(work) {
  try {
    await work();
    return true;
  } catch (error) {
    if (error instanceof StaleAnswer) return false;
    showBanner(error instanceof ApiError ? error.message : String(error));
    return false;
  }
}

// ---- tabs

const TABS = ['usage', 'load', 'connect', 'audit', 'models', 'limits'];
const LOADERS = {usage: loadUsage, load: loadLoad, connect: loadConnect, audit: loadAudit, models: loadStatus,
                 limits: loadLimits};
/** `one`: the tab works on one service. With "All services", the elements `<tab>-pick` (a note)
 * and `<tab>-content` swap. `admin`: the tab shows only for an admin of the picked service. An
 * admin tab without `one` also works with "All services", as the Load tab with its table does. */
const TAB_RULES = {usage: {}, load: {admin: true}, connect: {one: true}, audit: {one: true, admin: true},
                   models: {one: true, admin: true}, limits: {one: true, admin: true}};

/** An admin tab shows for an admin of the picked service. With "All services", it shows for an
 * admin of at least one service. */
function tabShown(tab) {
  if (!TAB_RULES[tab].admin) return true;
  if (state.service) return currentRole().admin;
  return Boolean(state.services && state.services.services.some(entry => entry.admin));
}

function writeHash() {
  const hash = `#${state.tab}${state.service ? `/${encodeURIComponent(state.service)}` : ''}`;
  if (location.hash !== hash) history.replaceState(null, '', hash);
}

/** The tab and the service in the URL hash. */
function readHash() {
  const [tab, service] = location.hash.slice(1).split('/');
  try {
    return {tab, service: service ? decodeURIComponent(service) : null};
  } catch {
    return {tab, service: null};
  }
}

function selectTab(name, focus = false) {
  if (!TABS.includes(name) || !tabShown(name)) name = 'usage';
  state.tab = name;
  for (const tab of TABS) {
    const button = $(`tab-${tab}`);
    const active = tab === name;
    button.hidden = !tabShown(tab);
    button.setAttribute('aria-selected', String(active));
    button.tabIndex = active ? 0 : -1;
    $(`panel-${tab}`).hidden = !active;
    if (active && focus) button.focus();
  }
  const pick = Boolean(TAB_RULES[name].one) && !state.service;
  if (TAB_RULES[name].one) {
    $(`${name}-pick`).hidden = !pick;
    $(`${name}-content`).hidden = pick;
  }
  writeHash();
  // The Load tab reads on a timer only while it is open.
  if (name !== 'load') stopLoadTimer();
  else if (state.loaded.load) startLoadTimer();
  if (!pick && !state.loaded[name]) {
    state.loaded[name] = true;
    guarded(LOADERS[name]);
  }
}

function tabKeys(event) {
  const shown = TABS.filter(tabShown);
  const index = shown.indexOf(state.tab);
  const moves = {ArrowRight: index + 1, ArrowLeft: index - 1, Home: 0, End: shown.length - 1};
  if (!(event.key in moves)) return;
  event.preventDefault();
  selectTab(shown[(moves[event.key] + shown.length) % shown.length], true);
}

// ---- services

/** Read the list of services. Return false when a newer read started meanwhile. */
async function loadServices() {
  const call = nextCall('services');
  const answer = await api('api/services');
  if (call !== state.calls.services) return false;
  state.services = answer;
  $('viewer').textContent = answer.user;
  showPicker();
  showServicesPanel();
  return true;
}

const stateText = value => String(value).replace(/_/g, ' ');
const NO_ANSWER = 'The service did not answer.';
/** The role of the viewer on an `ok` entry. A null access means that the role call failed. */
const roleText = entry => (entry.access === null ? 'no answer'
  : entry.admin ? 'admin' : entry.access ? 'user' : 'no access');

function showPicker() {
  const options = [el('option', {value: '', text: 'All services'})];
  for (const entry of state.services.services) {
    let text = entry.state === 'ok' ? `${entry.name} (ok, ${roleText(entry)})` : `${entry.name} (${stateText(entry.state)})`;
    if (entry.state === 'ok' && entry.access === null) text = `${entry.name}: ${NO_ANSWER}`;
    options.push(el('option', {value: entry.name, text, disabled: entry.access ? null : ''}));
  }
  $('service-picker').replaceChildren(...options);
  $('service-picker').value = state.service ?? '';
}

function showServicesPanel() {
  const {services, owner, listing} = state.services;
  $('services-listing').hidden = listing !== 'failed';
  $('service-add').hidden = !owner;
  $('services-actions-head').hidden = !owner;
  replaceRows($('services-body'), services.map(serviceEntryRow), owner ? 7 : 6, 'The app knows no LLM service.');
}

function serviceEntryRow(entry) {
  let actions = null;
  if (state.services.owner) {
    actions = el('td', {}, entry.source === 'manual'
      ? el('button', {type: 'button', className: 'small danger-ghost', text: 'Remove',
                      'aria-label': `Remove ${entry.name} from the added services`,
                      onclick: () => removeService(entry.name)})
      : null);
  }
  return el('tr', {},
    el('th', {scope: 'row', className: 'mono', text: entry.name}),
    el('td', {text: entry.source}),
    el('td', {}, el('span', {className: `badge state-${entry.state}`, text: stateText(entry.state)})),
    el('td', {className: entry.detail ? null : 'muted', text: entry.detail || '-'}),
    el('td', {className: 'mono', text: entry.model || '-'}),
    el('td', {text: entry.state === 'ok' ? roleText(entry) : '-'}),
    actions);
}

/** The reason why the viewer cannot use a service. */
const noAccessReason = entry => (entry.state === 'ok' && entry.access === null ? NO_ANSWER : entry.detail
  || (entry.state === 'ok' ? 'The service gives you no access.' : `The service has the state ${stateText(entry.state)}.`));

/** Show the tabs if the viewer has access to a service, else one message. Return true for access. */
function showAccess() {
  const {services, listing} = state.services;
  const any = services.some(entry => entry.access);
  $('main').hidden = !any;
  $('no-access').hidden = any;
  if (any) return true;
  selectService(null);
  const items = services.map(entry => el('li', {}, el('b', {text: entry.name}), `: ${noAccessReason(entry)}`));
  if (!services.length) items.push(el('li', {text: 'The app knows no LLM service.'}));
  if (listing === 'failed') items.push(el('li', {text: 'The platform listing of services failed.'}));
  $('no-access-list').replaceChildren(...items);
  return false;
}

/** The served model of the picked service in the header. Nothing for "All services". */
function showServing() {
  const entry = state.service ? entryOf(state.service) : undefined;
  $('serving').hidden = !entry;
  if (!entry) return;
  $('serving-text').textContent = entry.model || 'The service serves no model.';
  $('serving-dot').className = `dot ${entry.engine || ''}`;
}

/** Pick the service `name`, or "All services" for null or a name without access. Return true
 * if the pick changed. The caller then selects the tab, which loads its data. */
function selectService(name) {
  const entry = name ? entryOf(name) : undefined;
  const next = entry && entry.access ? entry.name : null;
  $('service-picker').value = next ?? '';
  if (next === state.service) return false;
  state.service = next;
  clearServiceData();
  showServing();
  return true;
}

/** Remove the data of the last service from the page, so it never shows under another name. */
function clearServiceData() {
  stopPoll();
  state.loaded = {};
  state.offset = 0;
  state.auditUsersRead = false;
  state.detailUser = null;
  state.pendingDelete = null;
  state.pendingRemove = null;
  $('detail').hidden = true;
  for (const id of ['record-dialog', 'confirm-dialog', 'remove-dialog']) if ($(id).open) $(id).close();
  for (const id of ['tile-requests', 'tile-failed', 'tile-tokens', 'tile-users']) $(id).textContent = '-';
  for (const chart of Object.values(state.charts)) setChart(chart, [], []);
  $('usage-note').hidden = true;
  for (const id of ['users-body', 'services-usage-body', 'audit-body', 'audit-users', 'models-body']) {
    $(id).replaceChildren();
  }
  $('audit-form').reset();
  $('audit-page').textContent = '';
  for (const id of ['st-state', 'st-serving', 'st-target', 'st-error']) $(id).textContent = '-';
  $('st-state').className = 'badge';
  $('st-error').classList.remove('bad');
  $('st-rollback-row').hidden = true;
  clearChat();
  clearConnect();
  clearMyLimits();
  clearLimits();
  clearLoad();
}

/** Read the list of services again and show what depends on it. */
async function refreshServices() {
  if (!await loadServices()) return;
  if (!showAccess()) return;
  selectService(state.service);
  selectTab(state.tab);
  // During a switch, the poll shows the newer state of the engine in the header.
  if (!state.polling) showServing();
}

function onHashChange() {
  if ($('main').hidden) return;
  const {tab, service} = readHash();
  selectService(service);
  selectTab(tab);
}

function toggleServices(open) {
  $('services-panel').hidden = !open;
  $('services-toggle').setAttribute('aria-expanded', String(open));
}

async function addService(event) {
  event.preventDefault();
  const name = $('service-add-name').value.trim();
  await guarded(async () => {
    const entry = await api('api/services', {method: 'POST', headers: {'Content-Type': 'application/json'},
                                             body: JSON.stringify({name})});
    $('service-add-name').value = '';
    const detail = entry.detail ? ` ${entry.detail}` : '';
    showBanner(`The app added ${entry.name}. Its state is ${stateText(entry.state)}.${detail}`,
               entry.state === 'ok' ? 'ok' : 'error');
    await refreshServices();
  });
}

async function removeService(name) {
  await guarded(async () => {
    await api(`api/services/${encodeURIComponent(name)}`, {method: 'DELETE'});
    showBanner(`The app removed ${name} from the added services.`, 'ok');
    await refreshServices();
  });
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
  for (const charts of document.querySelectorAll('.charts')) charts.hidden = true;
  showBanner('The charts did not load. The numbers and tables still work.');
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

/** The detail of a user needs one service and an admin of it. */
const detailOn = () => Boolean(state.service) && currentRole().admin;

async function loadUsage() {
  const call = nextCall('usage');
  const service = state.service;
  const range = usageRange();
  $('range-text').textContent = range.since && range.until ? `${range.since} to ${range.until}, UTC days` : '';
  const [users, days, models, services] = await current('usage', call, service, Promise.all(
    ['user', 'day', 'model', 'service'].map(group => api(`api/usage${query({...range, services: service, group_by: group})}`))));
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
  const detail = detailOn();
  $('users-hint').hidden = !detail;
  replaceRows($('users-body'), users.data.map(row => usageRow('user', row, detail)), 7, 'No requests in this range.');
  replaceRows($('services-usage-body'), services.data.map(row => usageRow('service', row, false)), 7,
              'No requests in this range.');
  showUsageNote(users.services);
  if (state.detailUser && detail) await loadDetail(state.detailUser);
}

/** Name each service without numbers and each service where the viewer sees only the own numbers. */
function showUsageNote(services) {
  const parts = services.filter(entry => !entry.ok)
    .map(entry => `No numbers from ${entry.name}: ${entry.error || 'the call failed.'}`);
  const own = services.filter(entry => entry.ok && entry.scope === 'own').map(entry => entry.name);
  if (own.length) parts.push(`You see only your own numbers from ${own.join(', ')}.`);
  $('usage-note').textContent = parts.join(' ');
  $('usage-note').hidden = !parts.length;
}

/** A row of a usage table by `key` (user or service). A row of a user can open the detail. */
function usageRow(key, row, opens) {
  const open = () => guarded(() => loadDetail(row.user));
  const props = opens
    ? {tabindex: '0', className: row.user === state.detailUser ? 'selected' : null, onclick: open,
       onkeydown: event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); open(); } }}
    : {};
  return el('tr', props,
    el('th', {scope: 'row', text: row[key]}),
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
  const service = state.service;
  if (!service) return;
  const detail = await current('detail', call, service,
    api(`${servicePath(`usage/${encodeURIComponent(user)}`, service)}${query(usageRange())}`));
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

// ---- load

const LOAD_MS = 5000;
const LOAD_MAX_FAILURES = 5;
const HOUR_MS = 3600000;
const MAX_RANGE_DAYS = 31;
/** The ranges that end now, by their name in `data-load-range`. "hour" reads the live points. */
const LOAD_SPANS = {'6h': 6 * HOUR_MS, '24h': 24 * HOUR_MS, '7d': 7 * 24 * HOUR_MS};
const POINT_FIELDS = ['requests', 'failed_requests', 'prompt_tokens', 'completion_tokens', 'ttft_ms_p50', 'ttft_ms_p95',
                      'duration_ms_p50', 'duration_ms_p95', 'in_flight_max', 'waiting_max', 'gpu_memory_used_max',
                      'gpu_memory_total'];
const LOAD_TILES = ['load-in-flight', 'load-waiting', 'load-completion-rate', 'load-prompt-rate', 'load-gpu',
                    'load-requests', 'load-failed', 'load-ttft-p50', 'load-ttft-p95', 'load-duration-p50',
                    'load-duration-p95'];
// The steps of the time axis in minutes, from 1 minute to 1 week.
const TICK_STEPS = [1, 5, 10, 15, 30, 60, 120, 180, 360, 720, 1440, 2880, 10080].map(minutes => minutes * 60000);
const oneDecimal = new Intl.NumberFormat(undefined, {maximumFractionDigits: 1});

/** The number `value`, or null for null, a missing value and each value that is not a finite number. */
const numeric = value => (typeof value === 'number' && Number.isFinite(value) ? value : null);

/** The series of each point field as `{x, y}` pairs, with `x` in milliseconds. A missing point, or a
 * missing value, is a null `y`. A step of more than 1.5 times `stepSeconds` between two points adds a
 * null pair, so the chart shows a gap. A point without a valid `ts` stays out. */
function loadSeries(points, stepSeconds) {
  const step = (numeric(stepSeconds) || 60) * 1000;
  const timed = (Array.isArray(points) ? points : [])
    .map(point => ({point, x: point ? Date.parse(point.ts) : NaN}))
    .filter(({x}) => Number.isFinite(x))
    .sort((a, b) => a.x - b.x);
  const series = Object.fromEntries(POINT_FIELDS.map(name => [name, []]));
  let last = null;
  for (const {point, x} of timed) {
    if (last !== null && x - last > 1.5 * step) {
      for (const name of POINT_FIELDS) series[name].push({x: last + step, y: null});
    }
    for (const name of POINT_FIELDS) series[name].push({x, y: numeric(point[name])});
    last = x;
  }
  return series;
}

/** The `since` and `until` of the range `range` as ISO times in UTC, null for the last hour, or
 * `{error}`. A custom range takes the values of two datetime-local fields, in local time. */
function loadRange(range, now, sinceText, untilText) {
  if (range === 'hour') return null;
  if (Object.hasOwn(LOAD_SPANS, range)) {
    return {since: new Date(now - LOAD_SPANS[range]).toISOString(), until: new Date(now).toISOString()};
  }
  if (!sinceText || !untilText) return {error: 'Fill in both times of the range.'};
  const since = new Date(sinceText);
  const until = new Date(untilText);
  if (Number.isNaN(since.getTime()) || Number.isNaN(until.getTime())) {
    return {error: 'The page cannot read a time of the range.'};
  }
  if (until <= since) return {error: 'The end of the range must come after its start.'};
  if (until - since > MAX_RANGE_DAYS * 24 * HOUR_MS) {
    return {error: `The range is longer than ${MAX_RANGE_DAYS} days. Pick a shorter range.`};
  }
  return {since: since.toISOString(), until: until.toISOString()};
}

/** The text of the step of the points, or an empty text for a step that is not a number. */
function stepText(seconds) {
  const minutes = numeric(seconds) === null ? 0 : Math.round(seconds / 60);
  if (minutes < 1) return '';
  if (minutes === 1) return 'One point for each minute.';
  if (minutes % 60 === 0) return minutes === 60 ? 'One point for each hour.' : `One point for each ${fmt(minutes / 60)} hours.`;
  return `One point for each ${fmt(minutes)} minutes.`;
}

/** A time in milliseconds: "310 ms" below one second, else "2.10 s". */
const msText = ms => (numeric(ms) === null ? '-' : ms < 1000 ? `${fmt(Math.round(ms))} ms` : seconds(ms));
/** Tokens per second with at most one decimal. */
const rateText = value => (numeric(value) === null ? '-' : oneDecimal.format(value));
/** GPU memory from MiB as GiB: "13.9 of 15 GiB", or only the used part without a total. */
function gpuText(used, total) {
  if (numeric(used) === null) return '-';
  const gib = value => oneDecimal.format(value / 1024);
  return numeric(total) === null ? `${gib(used)} GiB` : `${gib(used)} of ${gib(total)} GiB`;
}

/** The step of the time axis for a span in milliseconds: at most 8 steps. */
const tickStep = span => TICK_STEPS.find(step => span / step <= 8) || TICK_STEPS[TICK_STEPS.length - 1];

/** A label of the time axis in local time. A span of more than a day adds the date. */
function timeTick(ms, span) {
  const date = new Date(ms);
  const time = {hour: '2-digit', minute: '2-digit'};
  return span <= 24 * HOUR_MS ? date.toLocaleTimeString([], time)
    : date.toLocaleString([], {month: 'short', day: 'numeric', ...time});
}

const pad = value => String(value).padStart(2, '0');
/** A date as the value of a datetime-local field, in local time. */
const localInputValue = date => `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
  `T${pad(date.getHours())}:${pad(date.getMinutes())}`;

/** A time of the service in local time, or "-". `withDate` adds the date. */
function localText(iso, withDate = false) {
  const date = new Date(iso);
  if (!iso || Number.isNaN(date.getTime())) return '-';
  return withDate ? date.toLocaleString() : date.toLocaleTimeString();
}

/** True for the 404 of a service without the load endpoints. The rule is the same as for the limits. */
const loadMissing = error => limitsMissing(error);

/** Each line chart: its canvas, the format of its values and its series as [field, label, color, dashed].
 * `axis` formats the values on the axis when it differs from `format`. `whole` keeps the axis on whole numbers. */
const LOAD_CHARTS = [
  {id: 'chart-load-requests', format: fmt, whole: true,
   series: [['requests', 'Requests', COLORS.bar], ['failed_requests', 'Failed requests', COLORS.red]]},
  {id: 'chart-load-tokens', format: fmt, axis: value => compact.format(value), whole: true,
   series: [['prompt_tokens', 'Prompt tokens', COLORS.bar], ['completion_tokens', 'Completion tokens', COLORS.bar2]]},
  {id: 'chart-load-ttft', format: msText,
   series: [['ttft_ms_p50', 'p50', COLORS.bar], ['ttft_ms_p95', 'p95', COLORS.yellow]]},
  {id: 'chart-load-duration', format: msText,
   series: [['duration_ms_p50', 'p50', COLORS.bar], ['duration_ms_p95', 'p95', COLORS.yellow]]},
  {id: 'chart-load-queue', format: fmt, whole: true,
   series: [['in_flight_max', 'In flight', COLORS.bar], ['waiting_max', 'Waiting', COLORS.yellow]]},
  {id: 'chart-load-gpu', format: value => `${oneDecimal.format(value)} GiB`, gib: true,
   series: [['gpu_memory_used_max', 'Used', COLORS.bar2], ['gpu_memory_total', 'Total', COLORS.text, true]]},
];

/** A line chart over a linear time axis. The build of Chart.js has no date adapter, so the axis
 * holds milliseconds and its labels come from `timeTick`. A null value is a gap in the line. */
function lineChart({id, format, axis, whole, series}) {
  return new Chart($(id), {
    type: 'line',
    data: {datasets: series.map(([, label, color, dashed]) => ({
      label, data: [], borderColor: color, backgroundColor: color, borderWidth: 2, borderDash: dashed ? [5, 4] : [],
      pointRadius: 0, pointHoverRadius: 3, pointHitRadius: 6, tension: 0, spanGaps: false}))},
    options: {
      interaction: {mode: 'index', intersect: false},
      scales: {
        x: {type: 'linear', grid: {display: false},
            ticks: {maxRotation: 0, includeBounds: false, callback(value) { return timeTick(value, this.max - this.min); }}},
        y: {beginAtZero: true, ticks: {...(whole ? {precision: 0} : {}), callback: value => (axis || format)(value)}},
      },
      plugins: {
        legend: {display: true, labels: {boxWidth: 12, boxHeight: 2}},
        tooltip: {callbacks: {title: items => (items.length ? new Date(items[0].parsed.x).toLocaleString() : ''),
                              label: item => `${item.dataset.label}: ${format(item.parsed.y)}`}},
      },
    },
  });
}

/** Show `series` from `min` to `max` (milliseconds). Each chart keeps its object and takes the new data. */
function setLoadCharts(series, min, max) {
  if (state.chartsOff) return;
  try {
    for (const spec of LOAD_CHARTS) {
      const chart = (state.loadCharts[spec.id] ||= lineChart(spec));
      spec.series.forEach(([name], index) => {
        chart.data.datasets[index].data = spec.gib
          ? series[name].map(({x, y}) => ({x, y: y === null ? null : y / 1024})) : series[name];
      });
      Object.assign(chart.options.scales.x, {min, max});
      chart.options.scales.x.ticks.stepSize = tickStep(max - min);
      chart.update('none');
    }
  } catch {
    chartsFailed();  // only once: the charts stay off after this
  }
}

function showLoadNote(text, stopped = false) {
  $('load-note-text').textContent = text;
  $('load-retry').hidden = !stopped;
  $('load-note').hidden = !text;
}

/** One service shows tiles and charts, "All services" shows the table. `missing`: the service has
 * no load endpoints, so only that note shows. */
function showLoadView(missing = false) {
  $('load-missing').hidden = !missing;
  $('load-one').hidden = !state.service || missing;
  $('load-all').hidden = Boolean(state.service);
}

function showLoadRange() {
  for (const button of document.querySelectorAll('[data-load-range]')) {
    button.setAttribute('aria-pressed', String(button.dataset.loadRange === state.loadRange));
  }
  $('load-dates').hidden = state.loadRange !== 'custom';
}

/** True when the tab shows numbers of this moment: the tab is open with "All services", or with
 * one service that offers the endpoints and the range "Last hour". */
const loadLive = () => state.tab === 'load' && !$('main').hidden && $('load-missing').hidden
  && (!state.service || state.loadRange === 'hour');

/** The tiles and the charts of the last hour from the answer of the live route. */
function showLive({now, points}) {
  const at = now || {};
  $('load-in-flight').textContent = fmt(numeric(at.in_flight));
  $('load-waiting').textContent = fmt(numeric(at.waiting));
  $('load-completion-rate').textContent = rateText(at.completion_tokens_per_s);
  $('load-prompt-rate').textContent = rateText(at.prompt_tokens_per_s);
  $('load-gpu').textContent = gpuText(at.gpu_memory_used, at.gpu_memory_total);
  $('load-now-text').textContent = at.ts ? `Read at ${localText(at.ts)}` : 'The service has no sample yet.';
  const list = Array.isArray(points) ? points : [];
  const last = list.length ? list[list.length - 1] || {} : {};
  $('load-requests').textContent = fmt(numeric(last.requests));
  $('load-failed').textContent = fmt(numeric(last.failed_requests));
  $('load-ttft-p50').textContent = msText(last.ttft_ms_p50);
  $('load-ttft-p95').textContent = msText(last.ttft_ms_p95);
  $('load-duration-p50').textContent = msText(last.duration_ms_p50);
  $('load-duration-p95').textContent = msText(last.duration_ms_p95);
  $('load-minute-text').textContent = last.ts ? `The minute from ${localText(last.ts)}` : 'The service has no point yet.';
  const series = loadSeries(list, 60);
  const xs = series.requests.map(({x}) => x);
  // The clock of the browser can differ from the clock of the service, so the axis holds each point.
  const end = Math.max(Date.now(), ...xs.slice(-1).map(x => x + 60000));
  setLoadCharts(series, Math.min(end - HOUR_MS, ...xs.slice(0, 1)), end);
  $('load-range-text').textContent =
    `Last hour. ${stepText(60)} Times in your local time. The page reads the numbers every 5 seconds.`;
  $('load-days-note').hidden = true;
}

/** A row of the table of "All services". A failed service shows its error. */
function loadRow(entry) {
  const name = el('th', {scope: 'row', className: 'mono', text: entry.name});
  if (!entry.ok) return el('tr', {}, name, el('td', {colspan: 5, className: 'bad', text: entry.error || NO_ANSWER}));
  const now = entry.now || {};
  return el('tr', {}, name,
    el('td', {className: 'num', text: fmt(numeric(now.in_flight))}),
    el('td', {className: 'num', text: fmt(numeric(now.waiting))}),
    el('td', {className: 'num', text: rateText(now.completion_tokens_per_s)}),
    el('td', {className: 'num', text: rateText(now.prompt_tokens_per_s)}),
    el('td', {className: 'num', text: gpuText(now.gpu_memory_used, now.gpu_memory_total)}));
}

function showAll(answer) {
  const services = answer && Array.isArray(answer.services) ? answer.services : [];
  replaceRows($('load-all-body'), services.map(loadRow), 6, 'No service shows its load numbers to you.');
  $('load-all-text').textContent =
    `Read at ${new Date().toLocaleTimeString()}, your local time. The page reads the numbers every 5 seconds.`;
}

/** One read of the timer `run`: the live route of the picked service, or `api/load` for "All
 * services". A tick while a read runs does nothing. A stopped timer drops the answer. */
async function loadTick(run) {
  const stale = () => run !== state.calls.loadTimer;
  if (stale() || state.loadBusy) return;
  const service = state.service;
  state.loadBusy = run;
  try {
    if (service) {
      const call = nextCall('loadLive');
      const answer = await current('loadLive', call, service, api(servicePath('metrics/live', service)));
      if (stale()) return;
      showLive(answer);
    } else {
      const call = nextCall('loadAll');
      const answer = await current('loadAll', call, null, api('api/load'));
      if (stale()) return;
      showAll(answer);
    }
    state.loadFailures = 0;
    showLoadNote('');
  } catch (error) {
    if (stale() || error instanceof StaleAnswer) return;
    if (service && loadMissing(error)) {
      stopLoadTimer();
      showLoadView(true);
      return;
    }
    state.loadFailures += 1;
    const message = error instanceof ApiError ? error.message : String(error);
    if (state.loadFailures < LOAD_MAX_FAILURES) {
      showLoadNote(`The page did not get the load numbers: ${message} The page tries again in 5 seconds.`);
    } else {
      stopLoadTimer();
      showLoadNote(`The page did not get the load numbers ${LOAD_MAX_FAILURES} times in a row: ${message} ` +
                   'The page stopped. Use Reload to read the numbers again.', true);
    }
  } finally {
    if (state.loadBusy === run) state.loadBusy = null;
  }
}

/** Read now and then every 5 seconds, while the tab shows numbers of this moment and the page is
 * visible. A start drops the timer before it. */
function startLoadTimer() {
  stopLoadTimer();
  if (!loadLive() || document.visibilityState !== 'visible') return;
  state.loadFailures = 0;
  const run = nextCall('loadTimer');
  state.loadTimer = setInterval(() => loadTick(run), LOAD_MS);
  loadTick(run);
}

/** Stop the timer. A read of the timer that still runs drops its answer. */
function stopLoadTimer() {
  clearInterval(state.loadTimer);
  state.loadTimer = null;
  state.loadBusy = null;
  nextCall('loadTimer');
}

function onVisibility() {
  if (document.visibilityState !== 'visible') stopLoadTimer();
  else if (state.loaded.load && state.loadFailures < LOAD_MAX_FAILURES) startLoadTimer();
}

/** Read the history of the picked service for the picked range. The timer stops. */
async function readHistory() {
  const service = state.service;
  if (!service) return;
  stopLoadTimer();
  const range = loadRange(state.loadRange, Date.now(), $('load-since').value, $('load-until').value);
  if (!range) return;
  const call = nextCall('loadHistory');
  $('load-dates-status').textContent = range.error || '';
  if (range.error) return;
  showLoadNote('');
  let answer;
  try {
    answer = await current('loadHistory', call, service,
      api(`${servicePath('metrics/history', service)}${query(range)}`));
  } catch (error) {
    if (error instanceof StaleAnswer) return;
    if (loadMissing(error)) {
      showLoadView(true);
      return;
    }
    showLoadNote(`The page did not get the history: ${error instanceof ApiError ? error.message : String(error)}`);
    return;
  }
  setLoadCharts(loadSeries(answer.points, answer.step_seconds), Date.parse(range.since), Date.parse(range.until));
  $('load-range-text').textContent = `${localText(range.since, true)} to ${localText(range.until, true)}. ` +
    `${stepText(answer.step_seconds)} Times in your local time.`;
  const days = Array.isArray(answer.missing_days) ? answer.missing_days : [];
  $('load-days-note').textContent = days.length
    ? `The service did not read the history of these UTC days, so their points are missing: ${days.join(', ')}.` : '';
  $('load-days-note').hidden = !days.length;
}

function setLoadRange(range) {
  state.loadRange = range;
  showLoadRange();
  if (range === 'custom' && !$('load-since').value) {
    const now = new Date();
    $('load-until').value = localInputValue(now);
    $('load-since').value = localInputValue(new Date(now.getTime() - 24 * HOUR_MS));
  }
  if (range === 'hour') {
    nextCall('loadHistory');  // a history read that still runs drops its answer
    $('load-dates-status').textContent = '';
    startLoadTimer();
  } else {
    guarded(readHistory);
  }
}

async function loadLoad() {
  showLoadView(false);
  showLoadRange();
  showLoadNote('');
  if (state.service && state.loadRange !== 'hour') await readHistory();
  else startLoadTimer();
}

/** Remove the numbers of the last service. The range goes back to the last hour. */
function clearLoad() {
  stopLoadTimer();
  for (const name of ['loadLive', 'loadAll', 'loadHistory']) nextCall(name);
  state.loadRange = 'hour';
  state.loadFailures = 0;
  showLoadRange();
  showLoadNote('');
  $('load-missing').hidden = true;
  $('load-days-note').hidden = true;
  for (const id of LOAD_TILES) $(id).textContent = '-';
  for (const id of ['load-now-text', 'load-minute-text', 'load-range-text', 'load-dates-status', 'load-all-text']) {
    $(id).textContent = '';
  }
  $('load-all-body').replaceChildren();
  for (const chart of Object.values(state.loadCharts)) {
    for (const dataset of chart.data.datasets) dataset.data = [];
    chart.update('none');
  }
}

// ---- connect

const internalBaseUrl = name => `http://${name}/v1`;

/** The base URL of the service `name` for a client outside the platform. The page runs under a
 * path such as `/job/dev/llm-service-admin/`, and the service runs beside it. Null for a page
 * under any other path, for example a local run. */
function outsideBaseUrl(name) {
  const match = /^\/job\/([^/]+)\/[^/]+\/?$/.exec(location.pathname);
  if (!match) return null;
  return `${location.origin}/job/${match[1]}/${name}/v1`;
}

const EXAMPLE_PROMPT = 'Say hello in one sentence.';
const EXAMPLE_IDS = ['example-curl', 'example-python', 'example-outside', 'example-chat'];

/** The text of each example for the service `name` and the model `model`, by element id. The
 * outside example reads the token from LLM_TOKEN, so no example has a token value. */
function examples(name, model) {
  const internal = internalBaseUrl(name);
  const outside = outsideBaseUrl(name) || `https://<your-domain>/job/<environment>/${name}/v1`;
  const shown = model || '<model>';
  const body = JSON.stringify({model: shown, messages: [{role: 'user', content: EXAMPLE_PROMPT}]});
  const ask = [
    'reply = client.chat.completions.create(',
    `    model=${JSON.stringify(shown)},`,
    `    messages=[{"role": "user", "content": ${JSON.stringify(EXAMPLE_PROMPT)}}],`,
    ')',
    'print(reply.choices[0].message.content)',
  ];
  const chosen = model ? ` LLM_MODEL="${model}"` : '';
  return {
    'example-curl': [
      `curl ${internal}/chat/completions \\`,
      '  -H "Content-Type: application/json" \\',
      `  -d '${body}'`,
    ].join('\n'),
    'example-python': [
      'from openai import OpenAI',
      '',
      `client = OpenAI(base_url="${internal}", api_key="unused")`,
      ...ask,
    ].join('\n'),
    'example-outside': [
      'import os',
      '',
      'from openai import OpenAI',
      '',
      'client = OpenAI(',
      `    base_url="${outside}",`,
      '    api_key="unused",',
      '    default_headers={"Cookie": "__datatailr_user_cookie__=" + os.environ["LLM_TOKEN"]},',
      ')',
      ...ask,
    ].join('\n'),
    'example-chat': [
      'pip install openai',
      '# On the platform:',
      `LLM_BASE_URL="${internal}"${chosen} python chat.py`,
      '# Outside the platform, with your token in the variable LLM_TOKEN:',
      `LLM_BASE_URL="${outside}"${chosen} python chat.py`,
    ].join('\n'),
  };
}

/** Show the addresses and the examples of the service `name`. */
function showConnect(name, model, engine) {
  const outside = outsideBaseUrl(name);
  $('conn-internal').textContent = internalBaseUrl(name);
  $('conn-outside').textContent = outside || 'The page knows this URL only on the platform.';
  $('conn-outside-note').hidden = Boolean(outside);
  $('conn-model').textContent = model || 'The service serves no model.';
  $('conn-engine').textContent = engine ? stateText(engine) : 'unknown';
  $('conn-engine').className = `badge ${engine || ''}`;
  for (const [id, text] of Object.entries(examples(name, model))) $(id).textContent = text;
  // The model field follows the served model until the viewer types another one.
  const field = $('chat-model');
  if (!field.value.trim() || field.value.trim() === state.chat.defaultModel) field.value = model || '';
  state.chat.defaultModel = model || '';
}

/** The id of the active model in an answer of `GET /v1/models`, or null. */
function activeModel(answer) {
  const data = answer && Array.isArray(answer.data) ? answer.data : [];
  const entry = data.find(item => item && item.status === 'active' && typeof item.id === 'string');
  return entry ? entry.id : null;
}

async function loadConnect() {
  const service = state.service;
  if (!service) return;
  loadMyLimits(service);  // the card shows its own errors, so the rest of the tab still loads
  const call = nextCall('connect');
  const entry = entryOf(service) || {};
  showConnect(service, entry.model, entry.engine);
  const answer = await current('connect', call, service, api(servicePath('models', service)));
  // The fresh answer wins. Without an active model, the page shows none, not the model that the list still names.
  showConnect(service, activeModel(answer), entry.engine);
}

function clearConnect() {
  for (const id of ['conn-internal', 'conn-outside', 'conn-model', 'conn-engine']) $(id).textContent = '-';
  $('conn-engine').className = 'badge';
  $('conn-outside-note').hidden = true;
  for (const id of EXAMPLE_IDS) $(id).textContent = '';
  $('chat-model').value = '';
  state.chat.defaultModel = '';
}

async function copyExample(button) {
  try {
    await navigator.clipboard.writeText($(button.dataset.copy).textContent);
  } catch {
    showBanner('The browser did not copy the example. Select the text of the example and copy it by hand.');
    return;
  }
  button.textContent = 'Copied';
  $('copy-status').textContent = 'The page copied the example.';
  clearTimeout(Number(button.dataset.timer));
  button.dataset.timer = setTimeout(() => {
    button.textContent = 'Copy';
    $('copy-status').textContent = '';
  }, COPIED_MS);
}

// ---- testing area

/** The messages of the next request: the system prompt, the kept messages and `prompt`. */
function chatMessages(prompt) {
  const system = $('chat-system').value.trim();
  return [...(system ? [{role: 'system', content: system}] : []), ...state.chat.messages,
          {role: 'user', content: prompt}];
}

/** A text that names the limit of the app that `messages` passes, or null. */
function chatLimit(messages) {
  if (messages.length > CHAT_MAX_MESSAGES) {
    return `The conversation reached the limit of ${CHAT_MAX_MESSAGES} messages. Use Clear to start again.`;
  }
  const characters = messages.reduce((total, message) => total + message.content.length, 0);
  if (characters > CHAT_MAX_CHARACTERS) {
    return `The conversation passed the limit of ${number.format(CHAT_MAX_CHARACTERS)} characters. ` +
           'Use Clear to start again.';
  }
  return null;
}

/** Split `text` into whole lines and the rest after the last line break. A network chunk can end
 * in the middle of a line, so the caller keeps the rest for the next chunk. */
function splitLines(text) {
  const lines = text.split('\n');
  const rest = lines.pop();
  return {lines: lines.map(line => line.replace(/\r$/, '')), rest};
}

/** Post the chat request and call `onText` with each piece of the answer. Return the usage of the
 * last chunk. Throw an ApiError when the answer fails or ends without `[DONE]`. A stop throws the
 * AbortError of the browser. */
async function streamChat(service, body, signal, onText) {
  let response;
  try {
    response = await fetch(new URL(servicePath('chat', service), BASE),
                           {method: 'POST', headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify(body), signal});
  } catch (error) {
    if (error.name === 'AbortError') throw error;
    throw new ApiError('The admin app does not answer.');
  }
  if (!response.ok) {
    const data = await response.json().catch(() => null);
    // A stop during the read of the error body is a stop, not an error of the service.
    if (signal.aborted) throw new DOMException('You stopped the answer.', 'AbortError');
    // The error keeps the wait of a 429 answer from its Retry-After header.
    throw answerError(response, data);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let rest = '';
  let usage = null;
  try {
    for (;;) {
      let part;
      try {
        part = await reader.read();
      } catch (error) {
        if (error.name === 'AbortError') throw error;
        throw new ApiError('The stream of the answer broke.');
      }
      rest += part.done ? decoder.decode() : decoder.decode(part.value, {stream: true});
      const split = splitLines(part.done ? `${rest}\n` : rest);
      rest = split.rest;
      for (const line of split.lines) {
        if (!line.startsWith('data:')) continue;
        const data = line.slice(5).trim();
        if (data === '[DONE]') return usage;
        let chunk;
        try {
          chunk = JSON.parse(data);
        } catch {
          continue;
        }
        if (chunk && chunk.error) {
          const message = chunk.error.message;
          throw new ApiError(typeof message === 'string' ? message : 'The service reported an error.');
        }
        if (chunk && chunk.usage) usage = chunk.usage;
        const content = chunk?.choices?.[0]?.delta?.content;
        if (typeof content === 'string' && content) onText(content);
      }
      if (part.done) throw new ApiError('The stream ended before the service finished the answer.');
    }
  } finally {
    // Release the stream at each end: the last event, an error or a stop.
    reader.cancel().catch(() => {});
  }
}

/** Add a turn to the conversation. The text goes in with textContent only. */
function chatTurn(role, text) {
  const body = el('div', {className: 'turn-text', text});
  const turn = el('div', {className: `turn ${role}`},
                  el('div', {className: 'label', text: role === 'user' ? 'You' : 'Model'}), body);
  $('chat-log').append(turn);
  return {turn, body};
}

function setChatBusy(running) {
  $('chat-send').disabled = running;
  $('chat-stop').disabled = !running;
  $('chat-log').setAttribute('aria-busy', String(running));
}

const seconds = ms => `${(ms / 1000).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})} s`;

function showChatStats(usage, firstMs, totalMs) {
  $('chat-prompt-tokens').textContent = fmt(usage && usage.prompt_tokens);
  $('chat-completion-tokens').textContent = fmt(usage && usage.completion_tokens);
  $('chat-first-time').textContent = firstMs === null ? '-' : seconds(firstMs);
  $('chat-total-time').textContent = seconds(totalMs);
  $('chat-stats').hidden = false;
}

/** Send the prompt with the kept messages, and show the answer while it streams. A stopped or
 * failed answer removes its prompt from the kept messages. */
async function sendChat(event) {
  event.preventDefault();
  const service = state.service;
  const prompt = $('chat-prompt').value.trim();
  if (state.chat.controller || !service || !prompt) return;
  const messages = chatMessages(prompt);
  const limit = chatLimit(messages);
  if (limit) {
    $('chat-status').textContent = limit;
    return;
  }
  const body = {messages, temperature: Number($('chat-temperature').value),
                max_tokens: Number($('chat-max-tokens').value)};
  const model = $('chat-model').value.trim();
  if (model) body.model = model;
  const asked = messages[messages.length - 1];
  state.chat.messages.push(asked);
  $('chat-prompt').value = '';
  const question = chatTurn('user', prompt);
  const answer = chatTurn('assistant', '');
  const controller = new AbortController();
  state.chat.controller = controller;
  setChatBusy(true);
  $('chat-stats').hidden = true;
  $('chat-status').textContent = 'The model answers.';
  const started = performance.now();
  let firstMs = null;
  try {
    const usage = await streamChat(service, body, controller.signal, piece => {
      if (controller !== state.chat.controller) return;
      if (firstMs === null) firstMs = performance.now() - started;
      answer.body.textContent += piece;
    });
    if (controller !== state.chat.controller) return;
    state.chat.messages.push({role: 'assistant', content: answer.body.textContent});
    showChatStats(usage, firstMs, performance.now() - started);
    $('chat-status').textContent = chatLimit(chatMessages('')) || 'The answer is complete.';
  } catch (error) {
    // Clear or another service made this answer stale. They also removed its turns.
    if (controller !== state.chat.controller) return;
    state.chat.messages = state.chat.messages.filter(message => message !== asked);
    question.turn.classList.add('dropped');
    answer.turn.classList.add('dropped');
    if (!$('chat-prompt').value) $('chat-prompt').value = prompt;
    const reason = error.name === 'AbortError' ? 'You stopped the answer.'
      : `The answer failed: ${error instanceof ApiError ? error.message : String(error)}`;
    const limited = error instanceof ApiError && error.status === 429;
    const wait = limited ? waitText(error.retryAfter) : '';
    $('chat-status').textContent = [reason, wait, 'The conversation does not keep this prompt.'].filter(Boolean).join(' ');
    // The card of the limits then shows the use that made the service refuse the request.
    if (limited) loadMyLimits(service);
  } finally {
    if (controller === state.chat.controller) {
      state.chat.controller = null;
      setChatBusy(false);
    }
  }
}

function stopChat() {
  if (state.chat.controller) state.chat.controller.abort();
}

/** Stop a running answer and forget the conversation. */
function clearChat() {
  const controller = state.chat.controller;
  state.chat.controller = null;
  if (controller) controller.abort();
  setChatBusy(false);
  state.chat.messages = [];
  $('chat-log').replaceChildren();
  $('chat-stats').hidden = true;
  $('chat-status').textContent = '';
}

// ---- audit

/** A datetime-local value (local time) as an ISO time in UTC. */
const isoTime = value => (value ? new Date(value).toISOString() : '');

/** Fill the suggestions of the user filter. The filter also takes free text, so a failure only
 * shows a banner. Reload tries again. */
async function loadAuditUsers(service) {
  state.auditUsersRead = true;
  try {
    const answer = await api(servicePath('audit/users', service));
    if (service !== state.service) return;
    $('audit-users').replaceChildren(...answer.data.map(row => el('option', {value: row.user})));
  } catch (error) {
    if (service === state.service) showBanner(`The page did not get the list of users: ${error.message}`);
  }
}

async function loadAudit() {
  const service = state.service;
  if (!service) return;
  const call = nextCall('audit');
  if (!state.auditUsersRead) await loadAuditUsers(service);
  const filters = {user: $('audit-user').value.trim(), model: $('audit-model').value.trim(),
                   since: isoTime($('audit-since').value), until: isoTime($('audit-until').value)};
  const answer = await current('audit', call, service,
    api(`${servicePath('audit', service)}${query({...filters, limit: PAGE_SIZE, offset: state.offset})}`));
  const rows = answer.data;
  replaceRows($('audit-body'), rows.map(auditRow), 7, 'No records match the filters.');
  $('audit-newer').disabled = state.offset === 0;
  $('audit-older').disabled = rows.length < PAGE_SIZE;
  $('audit-page').textContent = rows.length
    ? `Records ${fmt(state.offset + 1)} to ${fmt(state.offset + rows.length)}` : '';
}

/** The UTC day of an ISO time such as `2026-11-11T09:30:00Z`. The record files are kept by UTC day. */
const utcDay = iso => new Date(iso).toISOString().slice(0, 10);

/** Show the dialog with a note and a body. A body of `null` leaves the JSON empty. */
function showRecord(note, body) {
  $('record-note').textContent = note;
  $('record-note').hidden = !note;
  $('record-json').textContent = body === null ? '' : JSON.stringify(body, null, 2);
  if (!$('record-dialog').open) $('record-dialog').showModal();
}

/** Open the dialog for a list row. The list has no prompt, so the page loads the full record. */
async function openRecord(row) {
  const call = nextCall('record');
  const service = state.service;
  if (!row.request_id) {
    showRecord('This record has no id. The page shows the fields of the list.', row);
    return;
  }
  showRecord('Loading the record.', null);
  // A later click, a closed dialog or another service makes this answer stale.
  const stale = () => call !== state.calls.record || !$('record-dialog').open || service !== state.service;
  try {
    const record = await api(`${servicePath(`audit/records/${encodeURIComponent(row.request_id)}`, service)}` +
                             query({day: utcDay(row.ts)}));
    if (stale()) return;
    showRecord(record.prompt_deleted ? 'The prompt of this record is deleted.' : '', record);
  } catch (error) {
    if (stale()) return;
    showRecord(error instanceof ApiError ? error.message : String(error), row);
  }
}

function auditRow(record) {
  const open = () => openRecord(record);
  const event = typeof record.event === 'string' ? record.event : null;
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
  const service = state.service;
  if (!service) return;
  const call = nextCall('status');
  const status = await current('status', call, service, api(servicePath('status', service)));
  showStatus(status);
  if (busy(status.engine)) startPoll();
}

/** Poll the state of the picked service. A stop or a new poll makes the answers of the old poll stale. */
function startPoll() {
  if (state.polling) return;
  state.polling = true;
  state.pollFailures = 0;
  state.pollService = state.service;
  setBusy(true);
  const run = nextCall('poll');
  state.pollTimer = setTimeout(() => poll(run), POLL_MS);
}

function stopPoll() {
  clearTimeout(state.pollTimer);
  nextCall('poll');
  state.pollTimer = null;
  state.polling = false;
  setBusy(false);
}

/** Read the state until the switch and a rollback after it end. Stop after MAX_POLL_FAILURES failed
 * reads in a row. */
async function poll(run) {
  state.pollTimer = null;
  const stale = () => run !== state.calls.poll;
  let status;
  try {
    status = await api(servicePath('status', state.pollService));
  } catch (error) {
    if (stale()) return;
    state.pollFailures += 1;
    if (state.pollFailures < MAX_POLL_FAILURES) {
      state.pollTimer = setTimeout(() => poll(run), POLL_MS);
    } else {
      stopPoll();
      showBanner(`The page did not get the state of the service: ${error.message} Use Reload to read it again.`);
    }
    return;
  }
  if (stale()) return;
  state.pollFailures = 0;
  showStatus(status);
  if (busy(status.engine)) {
    state.pollTimer = setTimeout(() => poll(run), POLL_MS);
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
  const service = state.service;
  setBusy(true);
  const done = await guarded(async () => {
    await api(servicePath('model', service), {method: 'POST', headers: {'Content-Type': 'application/json'},
                                              body: JSON.stringify({model, revision})});
    showBanner(`The service ${service} loads ${model} @ ${revision}.`, 'ok');
    if (service !== state.service) return;
    startPoll();  // first, so the poll runs even if the next read fails
    await loadStatus();
  });
  if (!done) setBusy(state.polling);
}

function askDelete(model, revision) {
  state.pendingDelete = {model, revision, service: state.service};
  $('confirm-text').textContent =
    `Delete the cached weights of ${model} at revision ${revision}? The service removes them from blob storage. ` +
    'A later switch to this model downloads them again.';
  $('confirm-dialog').showModal();
}

async function confirmDelete() {
  const {model, revision, service} = state.pendingDelete;
  $('confirm-dialog').close();
  setBusy(true);
  await guarded(async () => {
    await api(`${servicePath(`models/${modelPath(model)}`, service)}${query({revision})}`, {method: 'DELETE'});
    showBanner(`The service ${service} deleted ${model} @ ${revision}.`, 'ok');
  });
  if (service !== state.service) return;
  setBusy(state.polling);
  guarded(loadStatus);
}

// ---- limits

/** The three limits, the field of the current use of each, and the time of that use. */
const LIMITS = [
  {name: 'requests_per_minute', use: 'requests_last_minute', when: 'in the last minute'},
  {name: 'tokens_per_day', use: 'tokens_today', when: 'today'},
  {name: 'parallel_requests', use: 'parallel_requests', when: 'now'},
];
const LIMIT_SOURCES = {
  user: 'An own entry for you sets these limits. Each field that the entry leaves out takes the default.',
  default: 'The default limits of the service apply to you.',
  admin: 'You are an admin of the service and have no own entry, so you have no limit.',
  none: 'The service sets no limit for you.',
};
const NOT_OFFERED = 'This service does not offer limits.';
const UNLOADED = 'The service did not read the limits file. No limit applies now. The service tries again.';

/** The value of the own property `key` of `object`, or undefined. */
const ownValue = (object, key) => (object && Object.hasOwn(object, key) ? object[key] : undefined);
const noLimit = value => value === null || value === undefined;
const limitText = value => (noLimit(value) ? 'No limit' : fmt(value));

/** "Try again in N seconds." for a wait of N seconds, or an empty text without a wait or for a wait of 0. */
const waitText = wait => (wait === null || wait === undefined || wait === 0 ? ''
  : `Try again in ${fmt(wait)} ${wait === 1 ? 'second' : 'seconds'}.`);

/** True for the 404 of a service without the limit endpoints. The 404 of an unknown service is another error. */
const limitsMissing = error => error instanceof ApiError && error.status === 404 && error.code !== 'unknown_service';

/** The cell of the limit `name` from the effective limits `effective` of the service and the entry `entry` (or
 * none). The limit is muted when it comes from the default. The value is undefined without effective limits. */
function limitCell(effective, entry, name) {
  const source = ownValue(effective, 'source');
  const muted = source === 'default' || (source === 'user' && !(entry && Object.hasOwn(entry, name)));
  const title = muted ? 'From the default' : source === 'admin' ? 'Admin of the service' : null;
  return {value: ownValue(ownValue(effective, 'limits'), name), muted, title};
}

/** The note of the card "Your limits" for the answer of `limits/me`. */
function myLimitsNote({limits, loaded}) {
  if (loaded === false) return UNLOADED;
  return LIMITS.every(({name}) => noLimit(ownValue(limits, name))) ? 'You have no limit on this service.' : '';
}

/** The body of `PUT limits/default` from the text of each field. An empty field means no limit. */
function defaultLimitBody(values) {
  return Object.fromEntries(Object.entries(values).map(([name, value]) => [name, value.trim() ? Number(value) : null]));
}

/** The body of `PUT limits/users/{user}` from the choice of each field: `default`, `none` or `number` with its
 * value. A field with the choice `default` stays out of the body. Null when each field has the choice `default`. */
function userLimitBody(choices) {
  const body = {};
  for (const [name, {mode, value}] of Object.entries(choices)) {
    if (mode === 'none') body[name] = null;
    else if (mode === 'number') body[name] = Number(value);
  }
  return Object.keys(body).length ? body : null;
}

// The card "Your limits" of the Connect tab.

function showMyLimitsNote(text) {
  $('my-limits-note').textContent = text;
  $('my-limits-note').hidden = !text;
}

function showMyLimits(answer) {
  const {limits, use, source} = answer;
  for (const {name, use: key, when} of LIMITS) {
    const limit = ownValue(limits, name);
    const used = fmt(ownValue(use, key) ?? 0);
    $(`my-${name}`).textContent = noLimit(limit) ? `No limit, ${used} ${when}` : `${used} of ${fmt(limit)} ${when}`;
  }
  $('my-limits-source').textContent = ownValue(LIMIT_SOURCES, source) || String(source ?? '-');
  $('my-limits-list').hidden = false;
  showMyLimitsNote(myLimitsNote(answer));
}

/** Read the limits and the use of the viewer on `service`. A failure shows a note in the card only. */
async function loadMyLimits(service) {
  const call = nextCall('limitsMe');
  try {
    showMyLimits(await current('limitsMe', call, service, api(servicePath('limits/me', service))));
  } catch (error) {
    if (error instanceof StaleAnswer) return;
    $('my-limits-list').hidden = true;
    showMyLimitsNote(limitsMissing(error) ? NOT_OFFERED
      : `The page did not get your limits: ${error instanceof ApiError ? error.message : String(error)}`);
  }
}

function clearMyLimits() {
  nextCall('limitsMe');
  showMyLimitsNote('');
  $('my-limits-list').hidden = false;
  for (const id of [...LIMITS.map(({name}) => `my-${name}`), 'my-limits-source']) $(id).textContent = '-';
}

// The Limits tab.

function showLimitsMissing(missing) {
  $('limits-missing').hidden = !missing;
  $('limits-cards').hidden = missing;
}

async function loadLimits() {
  const service = state.service;
  if (!service) return;
  const call = nextCall('limits');
  let answer;
  try {
    answer = await current('limits', call, service, api(servicePath('limits', service)));
  } catch (error) {
    if (!limitsMissing(error)) throw error;
    showLimitsMissing(true);
    return;
  }
  showLimitsMissing(false);
  showLimits(answer);
}

function showLimits(answer) {
  const defaults = answer.default || null;
  const users = answer.users || {};
  const use = answer.use || {};
  const effective = answer.effective || {};
  $('limits-unloaded').textContent = UNLOADED;
  $('limits-unloaded').hidden = answer.loaded !== false;
  for (const {name} of LIMITS) {
    const value = ownValue(defaults, name);
    $(`limits-default-${name}`).value = noLimit(value) ? '' : String(value);
  }
  const names = [...new Set([...Object.keys(users), ...Object.keys(use)])].sort();
  replaceRows($('limits-body'),
              names.map(user => limitRow(user, ownValue(users, user), ownValue(effective, user), ownValue(use, user))),
              8, 'No user has an entry or a current use.');
}

/** A row of a user with the effective limits of the service. A limit from the default is muted. A row with an
 * entry has Edit and Remove. */
function limitRow(user, entry, effective, use) {
  const limits = LIMITS.map(({name}) => {
    const {value, muted, title} = limitCell(effective, entry, name);
    return el('td', {className: muted ? 'num muted' : 'num', title},
              value === undefined ? '-' : limitText(value),
              title ? el('span', {className: 'sr-only', text: `, ${title.toLowerCase()}`}) : null);
  });
  const used = LIMITS.map(({use: key}) => el('td', {className: 'num', text: fmt(ownValue(use, key) ?? 0)}));
  const actions = entry ? el('div', {className: 'pair'},
    el('button', {type: 'button', className: 'small ghost', text: 'Edit', 'aria-label': `Edit the limits of ${user}`,
                  onclick: () => editLimits(user, entry)}),
    el('button', {type: 'button', className: 'small danger-ghost', text: 'Remove',
                  'aria-label': `Remove the limits of ${user}`, onclick: () => askRemove(user)})) : null;
  return el('tr', {}, el('th', {scope: 'row', text: user}), ...limits, ...used, el('td', {}, actions));
}

/** Set the choice of the field `name` of the user form. Only the choice `number` takes a value. */
function setChoice(name, mode, value = '') {
  $(`limits-user-${name}-mode`).value = mode;
  $(`limits-user-${name}`).value = mode === 'number' ? String(value) : '';
  $(`limits-user-${name}`).disabled = mode !== 'number';
}

function editLimits(user, entry) {
  $('limits-user').value = user;
  for (const {name} of LIMITS) {
    const value = ownValue(entry, name);
    setChoice(name, value === undefined ? 'default' : value === null ? 'none' : 'number', value);
  }
  $('limits-user-status').textContent = `The form shows the entry of ${user}.`;
  $('limits-user').focus();
}

function clearUserForm() {
  $('limits-user-form').reset();
  for (const {name} of LIMITS) setChoice(name, 'default');
  $('limits-user-status').textContent = '';
}

function clearLimits() {
  nextCall('limits');
  showLimitsMissing(false);
  $('limits-unloaded').hidden = true;
  $('limits-default-form').reset();
  $('limits-body').replaceChildren();
  clearUserForm();
}

async function saveDefaultLimits(event) {
  event.preventDefault();
  const service = state.service;
  if (!service) return;
  const body = defaultLimitBody(Object.fromEntries(LIMITS.map(({name}) => [name, $(`limits-default-${name}`).value])));
  await guarded(async () => {
    await api(servicePath('limits/default', service), {method: 'PUT', headers: {'Content-Type': 'application/json'},
                                                       body: JSON.stringify(body)});
    showBanner(`The service ${service} saved the default limits.`, 'ok');
    if (service === state.service) await loadLimits();
  });
}

async function saveUserLimits(event) {
  event.preventDefault();
  const service = state.service;
  const user = $('limits-user').value.trim();
  if (!service) return;
  if (!user) {
    $('limits-user-status').textContent = 'Type a user name.';
    $('limits-user').focus();
    return;
  }
  const body = userLimitBody(Object.fromEntries(LIMITS.map(({name}) => [name, {
    mode: $(`limits-user-${name}-mode`).value, value: $(`limits-user-${name}`).value}])));
  if (!body) {
    $('limits-user-status').textContent =
      'Each field is "Default", so the entry sets nothing. To give the user the default limits, remove the entry of the user.';
    return;
  }
  $('limits-user-status').textContent = '';
  await guarded(async () => {
    const userPath = servicePath(`limits/users/${encodeURIComponent(user)}`, service);
    await api(userPath, {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
    showBanner(`The service ${service} saved the limits of ${user}.`, 'ok');
    if (service !== state.service) return;
    clearUserForm();
    await loadLimits();
  });
}

function askRemove(user) {
  state.pendingRemove = {user, service: state.service};
  $('remove-text').textContent = `Remove the entry of ${user}? The user then gets the default limits. ` +
    'An admin of the service then has no limit.';
  $('remove-dialog').showModal();
}

async function confirmRemove() {
  const pending = state.pendingRemove;
  state.pendingRemove = null;
  $('remove-dialog').close();
  if (!pending) return;
  const {user, service} = pending;
  await guarded(async () => {
    const userPath = servicePath(`limits/users/${encodeURIComponent(user)}`, service);
    await api(userPath, {method: 'DELETE'});
    showBanner(`The service ${service} removed the limits of ${user}.`, 'ok');
  });
  if (service === state.service) guarded(loadLimits);
}

// ---- start

function wire() {
  for (const tab of TABS) $(`tab-${tab}`).addEventListener('click', () => selectTab(tab));
  document.querySelector('[role="tablist"]').addEventListener('keydown', tabKeys);
  $('reload').addEventListener('click', reload);
  $('service-picker').addEventListener('change', () => {
    selectService($('service-picker').value || null);
    selectTab(state.tab);
  });
  window.addEventListener('hashchange', onHashChange);
  $('services-toggle').addEventListener('click', () => toggleServices($('services-panel').hidden));
  $('services-close').addEventListener('click', () => { toggleServices(false); $('services-toggle').focus(); });
  $('service-add').addEventListener('submit', addService);
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
  $('limits-default-form').addEventListener('submit', saveDefaultLimits);
  $('limits-user-form').addEventListener('submit', saveUserLimits);
  $('limits-user-clear').addEventListener('click', clearUserForm);
  $('limits-reload').addEventListener('click', () => guarded(loadLimits));
  $('remove-confirm').addEventListener('click', confirmRemove);
  for (const button of document.querySelectorAll('[data-load-range]')) {
    button.addEventListener('click', () => setLoadRange(button.dataset.loadRange));
  }
  $('load-dates').addEventListener('submit', event => { event.preventDefault(); guarded(readHistory); });
  $('load-retry').addEventListener('click', startLoadTimer);
  document.addEventListener('visibilitychange', onVisibility);
  for (const {name} of LIMITS) {
    $(`limits-user-${name}-mode`).addEventListener('change', event => {
      setChoice(name, event.target.value, $(`limits-user-${name}`).value);
      if (event.target.value === 'number') $(`limits-user-${name}`).focus();
    });
  }
  for (const button of document.querySelectorAll('[data-copy]')) {
    button.addEventListener('click', () => copyExample(button));
  }
  $('chat-form').addEventListener('submit', sendChat);
  $('chat-stop').addEventListener('click', stopChat);
  $('chat-clear').addEventListener('click', clearChat);
  for (const button of document.querySelectorAll('dialog [data-close]')) {
    button.addEventListener('click', () => button.closest('dialog').close());
  }
}

/** Read the list of services and the data of the tab again. */
async function reload() {
  $('banner').hidden = true;
  state.loaded = {};
  state.auditUsersRead = false;
  await guarded(refreshServices);
  // If the list failed, the tab still reads its data again.
  if (!$('main').hidden && !state.loaded[state.tab]) selectTab(state.tab);
}

async function start() {
  wire();
  try {
    themeCharts();
  } catch {
    state.chartsOff = true;  // without Chart.js, the tiles and tables still work
  }
  const wanted = readHash();
  try {
    await loadServices();
  } catch (error) {
    showBanner(error.message);
    return;
  }
  $('topbar').hidden = false;
  if (state.chartsOff) chartsFailed();
  for (const button of document.querySelectorAll('[data-range]')) {
    button.setAttribute('aria-pressed', String(button.dataset.range === state.range));
  }
  showLoadRange();
  if (!showAccess()) return;
  selectService(wanted.service);
  selectTab(wanted.tab);
}

document.addEventListener('DOMContentLoaded', start);
