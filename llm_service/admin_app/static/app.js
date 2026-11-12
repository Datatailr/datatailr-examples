/* LLM Service Admin: the page. A service picker and four tabs (usage, connect, audit, models)
 * over the JSON API of the app.
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
const COLORS = {bar: '#4c8dff', bar2: '#36c2b4', grid: 'rgba(56, 66, 82, .55)', text: '#9aa7b8'};

const $ = id => document.getElementById(id);
const number = new Intl.NumberFormat();
// `service` is the picked service, or null for "All services". `services` is the answer of
// `GET api/services`. A request of a tab keeps the service of its start and drops a late answer.
const state = {tab: null, service: null, services: null, range: '30', loaded: {}, offset: 0,
               polling: false, pollTimer: null, pollFailures: 0, pollService: null,
               detailUser: null, chartsOff: false,
               charts: {}, pendingDelete: null, auditUsersRead: false, calls: {},
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

const TABS = ['usage', 'connect', 'audit', 'models'];
const LOADERS = {usage: loadUsage, connect: loadConnect, audit: loadAudit, models: loadStatus};
/** `one`: the tab works on one service. With "All services", the elements `<tab>-pick` (a note)
 * and `<tab>-content` swap. `admin`: the tab shows only for an admin of the picked service. */
const TAB_RULES = {usage: {}, connect: {one: true}, audit: {one: true, admin: true},
                   models: {one: true, admin: true}};

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
  $('detail').hidden = true;
  for (const id of ['record-dialog', 'confirm-dialog']) if ($(id).open) $(id).close();
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
  document.querySelector('.charts').hidden = true;
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
  const call = nextCall('connect');
  const entry = entryOf(service) || {};
  showConnect(service, entry.model, entry.engine);
  const answer = await current('connect', call, service, api(servicePath('models', service)));
  showConnect(service, activeModel(answer) || entry.model, entry.engine);
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
    const message = data && data.error && data.error.message;
    throw new ApiError(typeof message === 'string' ? message : `The admin app answered ${response.status}.`);
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
    $('chat-status').textContent = `${reason} The conversation does not keep this prompt.`;
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
  if (!showAccess()) return;
  selectService(wanted.service);
  selectTab(wanted.tab);
}

document.addEventListener('DOMContentLoaded', start);
