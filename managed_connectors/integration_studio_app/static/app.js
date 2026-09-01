/* Integration Studio front end.
 *
 * The app is served under /job/<env>/<name>/ by the platform's nginx, which
 * strips that prefix before Flask sees it — so every URL here stays relative to
 * the document, never rooted at "/". BASE tolerates a missing trailing slash.
 */

const BASE = location.pathname.endsWith('/') ? location.pathname : location.pathname + '/';

const state = {
  platformName: 'Datatailr',
  user: null,
  sources: [],
  settings: null,
  callbacks: {},
  personalSettings: {},
  auditEvents: [],
  connectorAccess: { workspace_configurable: [], personal_connectable: ['gmail', 'outlook', 'zoom'] },
};

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const el = (tag, props = {}, children = []) => {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (key === 'class') node.className = value;
    else if (key === 'text') node.textContent = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (value !== null && value !== undefined && value !== false) node.setAttribute(key, value);
  }
  for (const child of [].concat(children)) {
    if (child) node.append(child);
  }
  return node;
};

/* ── plumbing ──────────────────────────────────────────────────────────── */

let toastTimer;
function toast(message, isError = false) {
  const node = $('#toast');
  node.textContent = message;
  node.className = `toast show${isError ? ' error' : ''}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { node.className = 'toast'; }, 4000);
}

async function api(path, options = {}) {
  const headers = { 'X-Requested-With': 'IntegrationStudio', ...(options.headers || {}) };
  if (options.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
  const response = await fetch(BASE + path, { ...options, headers });
  const data = await response.json().catch(() => ({ error: `Request failed (${response.status})` }));
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

async function loadBranding() {
  let platformName = 'Datatailr';
  try {
    const response = await fetch('/branding.json', { cache: 'no-cache' });
    if (response.ok) {
      const branding = await response.json();
      const configuredName = typeof branding.platformName === 'string'
        ? branding.platformName.trim()
        : '';
      if (configuredName) platformName = configuredName;
    }
  } catch {
    // Match the platform GUI: keep the baked-in Datatailr fallback.
  }
  state.platformName = platformName;
  $$('[data-platform-template]').forEach((node) => {
    node.textContent = node.dataset.platformTemplate.replaceAll('{platform}', platformName);
  });
  $$('[data-copy-template]').forEach((node) => {
    node.dataset.copy = node.dataset.copyTemplate.replaceAll('{platform}', platformName);
  });
  document.title = `Integration Studio | ${platformName}`;
}

function busy(button, isBusy) {
  button.disabled = isBusy;
  if (isBusy) button.setAttribute('aria-busy', 'true');
  else button.removeAttribute('aria-busy');
}

/* ── navigation ────────────────────────────────────────────────────────── */

function showView(name) {
  $$('.nav-item').forEach((btn) => btn.classList.toggle('is-active', btn.dataset.view === name));
  $$('.view').forEach((view) => view.classList.toggle('is-active', view.id === `view-${name}`));
  if (name === 'admin' && state.user?.is_admin) {
    loadAudit().catch((error) => toast(`Could not load audit log: ${error.message}`, true));
  }
}

/* ── sources ───────────────────────────────────────────────────────────── */

const sourceById = (id) => state.sources.find((source) => source.id === id);

async function disconnect(id) {
  try {
    await api(`api/disconnect/${id}`, { method: 'POST', body: '{}' });
    toast(`Disconnected ${sourceById(id)?.label || id}`);
    await load();
  } catch (error) {
    toast(error.message, true);
  }
}

function renderPersonalConnections() {
  const root = $('#personal-connections');
  root.replaceChildren();
  const personalIds = [...new Set([
    ...(state.connectorAccess.personal_configurable || []),
    ...(state.connectorAccess.personal_connectable || []),
  ])];
  for (const id of personalIds) {
    const source = sourceById(id);
    if (!source || !['gmail', 'outlook', 'zoom'].includes(id)) continue;
    const meta = CONNECTORS[id];
    const config = state.personalSettings[id] || {};
    const oauth = ['outlook', 'zoom'].includes(id);
    const configured = oauth ? Boolean(source.configured) : Boolean(config[meta.secret]);
    const status = source.connected
      ? id === 'gmail'
        ? 'Connected through read-only IMAP. Messages are fetched only at request time.'
        : id === 'outlook'
          ? source.calendar_authorized
            ? 'Connected for Mail and Calendar. Messages and events are fetched directly from Microsoft only when requested.'
            : 'Connected for Mail. Reauthorize Outlook after your administrator adds Calendars.ReadBasic to enable Calendar.'
          : source.ai_companion_authorized && source.retained_transcript_authorized
            ? 'Connected for AI Companion summaries and retained transcripts. Meeting content is fetched directly from Zoom only when requested.'
            : source.ai_companion_authorized
              ? 'Connected for AI Companion summaries. Reauthorize after your administrator adds cloud_recording:read:meeting_transcript to enable retained transcripts.'
            : 'Connected with legacy Zoom access. Reauthorize after your administrator adds the AI Companion scopes.'
      : id === 'gmail'
        ? 'Enter your Gmail address and 16-character Google app password.'
        : configured
          ? `Ready to connect. Sign in with ${id === 'outlook' ? 'Microsoft' : 'Zoom'} to authorize live, request-time access.`
          : state.user?.is_admin
            ? `Configure the ${id === 'outlook' ? 'Microsoft Entra' : 'Zoom OAuth'} application once in Admin, then connect your account.`
            : `Your administrator has not configured the ${id === 'outlook' ? 'Microsoft' : 'Zoom'} connection yet.`;

    const body = el('div', { class: 'card-body' });
    for (const [field, label, type, help] of (oauth ? [] : meta.fields)) {
      let value = config[field] ?? '';
      if (Array.isArray(value)) value = value.join(', ');
      const input = el('input', {
        name: field,
        type,
        value: type === 'password' ? '' : String(value),
        placeholder: type === 'password' && configured ? 'Stored privately — leave blank to keep' : '',
        autocomplete: 'off',
      });
      const inputId = `personal-${id}-${field}`;
      body.append(el('div', { class: 'field' }, [
        el('label', { text: label, for: inputId }),
        Object.assign(input, { id: inputId }),
        help ? el('p', { class: 'help', text: help }) : null,
      ]));
    }
    body.append(
      el('p', { class: 'personal-status', text: status }),
      el('p', { class: 'note', text: id === 'gmail'
        ? 'Live-only: message data is discarded after the request. Your app password is isolated under your username and removed when you disconnect.'
        : id === 'outlook'
          ? 'Live-only: email and calendar records are held in request memory and are never written to persistent connector storage. Your delegated OAuth tokens are isolated to your username.'
          : 'Live-only: AI Companion summaries, retained transcripts, and recording metadata are held in request memory and never written to persistent connector storage. Your delegated OAuth tokens are isolated to your username.' }),
    );

    const actions = oauth
      ? [el('button', {
          type: 'button', class: 'btn btn-primary', disabled: !configured,
          text: source.connected ? `Reauthorize ${source.label}` : `Connect ${source.label}`,
          onclick: () => { location.assign(`${BASE}oauth/${id}/start`); },
        })]
      : [
          el('button', {
            type: 'button', class: 'btn', text: 'Save settings',
            onclick: (event) => savePersonalConnector(id, event.currentTarget, false),
          }),
          el('button', {
            type: 'button', class: 'btn btn-primary',
            text: source.connected ? 'Update Gmail connection' : 'Save & connect Gmail',
            onclick: (event) => savePersonalConnector(id, event.currentTarget, true),
          }),
        ];
    if (source.connected) {
      actions.unshift(el('button', {
        type: 'button', class: 'btn btn-quiet', text: 'Disconnect', onclick: () => disconnect(id),
      }));
    }

    root.append(el('article', { class: 'card personal-card', 'data-personal-provider': id }, [
      el('div', { class: 'card-head' }, [
        el('div', { class: 'conn-title' }, [el('h2', { text: source.label })]),
        el('span', {
          class: 'pill',
          'data-on': String(Boolean(source.connected)),
          text: source.connected ? 'Connected' : configured ? 'Configured' : 'Not configured',
        }),
      ]),
      body,
      el('div', { class: 'card-foot personal-actions' }, actions),
    ]));
  }
}

async function savePersonalConnector(id, button, connect) {
  const card = button.closest('[data-personal-provider]');
  const values = {};
  card.querySelectorAll('input[name]').forEach((input) => { values[input.name] = input.value; });
  busy(button, true);
  try {
    await api(`api/personal-settings/${id}`, {
      method: 'POST', body: JSON.stringify({ settings: values }),
    });
    await api(`api/personal-test/${id}`, { method: 'POST', body: '{}' });
    if (connect && id === 'outlook') {
      location.assign(`${BASE}oauth/${id}/start`);
      return;
    }
    await load();
    showView('connections');
    toast(`${CONNECTORS[id].title} settings saved privately`);
  } catch (error) {
    toast(error.message, true);
  } finally {
    busy(button, false);
  }
}

/* ── admin ─────────────────────────────────────────────────────────────── */

const CONNECTORS = {
  slack: {
    title: 'Slack', trust: 'shared',
    blurb: 'Reads joined public channels and can post explicit user-requested messages and generated documents as the bot.',
    setup: 'For posting, add the chat:write and files:write bot scopes, reinstall the Slack app, and invite the bot to every writable channel. Integration Studio never uses chat:write.public.',
    secret: 'has_bot_token',
    fields: [
      ['bot_token', 'Bot token', 'password'],
    ],
  },
  hubspot: {
    title: 'HubSpot', trust: 'shared',
    blurb: 'Reads CRM records plus calls, meetings, notes, tasks, associations, and next-activity dates with a private-app token.',
    setup: 'The private app needs read access to each CRM object you want exposed: contacts, companies, deals, and tickets. HubSpot activity APIs use those CRM read permissions; sales-email bodies are not ingested.',
    secret: 'has_access_token',
    fields: [
      ['access_token', 'Private app token', 'password'],
      ['base_url', 'API base URL', 'url'],
    ],
  },
  github: {
    title: 'GitHub', trust: 'shared',
    blurb: 'Reads shared organization repositories through its own administrator-installed GitHub App. Repository records are fetched live.',
    setup: 'Create a GitHub App with read-only Contents, Issues, Pull requests, and Metadata access. Install it on the organization and select the intended repositories.',
    secret: 'has_private_key',
    fields: [
      ['app_id', 'GitHub App ID', 'text'],
      ['installation_id', 'Installation ID', 'text', 'Find this numeric ID in the installation URL or through the GitHub App installations API.'],
      ['private_key', 'Private key (.pem)', 'textarea', 'Paste the complete PEM private key. It is stored in the owner-only connector state file and never returned by the API.'],
      ['base_url', 'API base URL', 'url', 'Use https://api.github.com for GitHub.com.'],
    ],
  },
  gmail: {
    title: 'Gmail', trust: 'personal',
    blurb: 'Each person connects their mailbox with a Google app password, read only.',
    secret: 'has_app_password',
    fields: [
      ['username', 'Gmail address', 'email'],
      ['app_password', 'Google app password', 'password', 'Use the 16-character app password, not your normal Google password.'],
    ],
  },
  outlook: {
    title: 'Outlook', trust: 'shared-oauth',
    blurb: 'One administrator-managed Entra application; every user authorizes only their own mailbox and calendar.',
    secret: 'has_client_secret',
    fields: [
      ['tenant', 'Directory (tenant) ID', 'text', 'Use your tenant ID for organization-only access, or "common" to also allow personal Microsoft accounts.'],
      ['client_id', 'Application (client) ID', 'text'],
      ['client_secret', 'Client secret value', 'password', 'Paste the secret value, not its secret ID.'],
      ['redirect_uri', 'Redirect URI', 'url', 'Use the callback shown below.'],
    ],
  },
  zoom: {
    title: 'Zoom', trust: 'shared-oauth',
    blurb: 'One administrator-managed user OAuth app; every user authorizes only their own AI Companion summaries, retained transcripts, and optional recording transcripts.',
    setup: 'Required granular scopes: meeting:read:list_meetings, meeting:read:summary, and cloud_recording:read:meeting_transcript. Existing users must reconnect after scopes change.',
    secret: 'has_client_secret',
    fields: [
      ['client_id', 'Client ID', 'text'],
      ['client_secret', 'Client secret', 'password', 'Use the development credentials while testing; switch to production credentials after Marketplace publication.'],
      ['redirect_uri', 'Redirect URI', 'url', 'Use the callback shown below and add the same URL to the OAuth allow list.'],
    ],
  },
};

const GROUPS = [
  { trust: 'shared', title: 'Shared connectors', note: 'One admin-managed connection. Anyone allowed to access the app can use the fetched Slack, HubSpot, and GitHub Organization data.', ids: ['slack', 'hubspot', 'github'] },
  { trust: 'shared-oauth', title: 'Personal OAuth applications', note: 'Register provider applications here. Users then authorize their own Outlook Mail + Calendar or Zoom account with one click.', ids: ['outlook', 'zoom'] },
];

function renderConnectorCard(id) {
  const meta = CONNECTORS[id];
  const config = (state.settings || {})[id] || {};
  const configured = Boolean(config[meta.secret]);

  const body = el('div', { class: 'card-body' });
  for (const [field, label, type, help] of meta.fields) {
    let value = config[field] ?? '';
    if (Array.isArray(value)) value = value.join(', ');
    const isSecret = type === 'password' || field === 'private_key';
    const secretStored = Boolean(config[`has_${field}`]);
    const input = type === 'textarea'
      ? el('textarea', {
          name: field,
          rows: '7',
          value: '',
          placeholder: secretStored ? 'Stored — leave blank to keep' : '-----BEGIN RSA PRIVATE KEY-----',
          autocomplete: 'off',
          spellcheck: 'false',
        })
      : el('input', {
          name: field,
          type,
          value: isSecret ? '' : String(value),
          placeholder: isSecret && secretStored ? 'Stored — leave blank to keep' : '',
          autocomplete: 'off',
        });
    body.append(el('div', { class: 'field' }, [
      el('label', { text: label, for: `${id}-${field}` }),
      Object.assign(input, { id: `${id}-${field}` }),
      help ? el('p', { class: 'help', text: help }) : null,
    ]));
  }

  if (['outlook', 'zoom'].includes(id)) {
    const callback = state.callbacks[id] || '';
    body.append(el('div', { class: 'field' }, [
      el('label', { text: 'Callback to register with the provider' }),
      el('div', { class: 'copy-row' }, [
        el('code', { text: callback }),
        el('button', {
          type: 'button',
          class: 'btn btn-quiet copy-btn',
          text: 'Copy',
          onclick: async (event) => {
            try {
              await navigator.clipboard.writeText(callback);
              const button = event.currentTarget;
              button.textContent = 'Copied';
              setTimeout(() => { button.textContent = 'Copy'; }, 1500);
            } catch {
              toast('Copy the callback manually — the browser blocked clipboard access', true);
            }
          },
        }),
      ]),
    ]));
  }
  if (meta.setup) body.append(el('p', { class: 'help', text: meta.setup }));

  return el('div', { class: 'card', 'data-provider': id }, [
    el('div', { class: 'card-head' }, [
      el('div', { class: 'conn-title' }, [
        el('h3', { text: meta.title }),
      ]),
      el('span', { class: 'pill', 'data-on': String(configured), text: configured ? 'Configured' : 'Not set' }),
    ]),
    body,
    el('div', { class: 'card-foot' }, [
      el('p', { class: 'note', text: meta.blurb, style: 'margin:0' }),
      el('span', { class: 'grow' }),
      el('button', {
        type: 'button',
        class: 'btn',
        text: 'Save & test',
        onclick: (event) => testConnector(id, event.currentTarget),
      }),
    ]),
  ]);
}

function renderSettings() {
  const root = $('#settings-root');
  root.replaceChildren();
  if (!state.settings) return;
  for (const group of GROUPS) {
    root.append(el('section', { class: 'group', id: `settings-${group.trust}` }, [
      el('div', { class: 'group-head' }, [
        el('h2', {}, [
          el('span', { class: 'class-dot', 'data-trust': group.trust, 'aria-hidden': 'true' }),
          document.createTextNode(group.title),
        ]),
        el('p', { text: group.note }),
      ]),
      el('div', { class: 'grid-2' }, group.ids.map(renderConnectorCard)),
    ]));
  }
}

function auditMetadataText(event) {
  const metadata = event.metadata || {};
  const parts = Object.entries(metadata).map(([key, value]) => `${key}: ${String(value)}`);
  if (event.metadata_only) parts.unshift('metadata only');
  return parts.join(' · ') || (event.metadata_only ? 'metadata only' : '—');
}

function renderAudit() {
  const root = $('#audit-rows');
  if (!root) return;
  root.replaceChildren();
  if (!state.auditEvents.length) {
    root.append(el('tr', {}, [
      el('td', { colspan: '8', class: 'audit-empty', text: 'No connector events match these filters.' }),
    ]));
    return;
  }
  for (const event of state.auditEvents) {
    const at = new Date(event.at);
    root.append(el('tr', {}, [
      el('td', { text: Number.isNaN(at.getTime()) ? event.at : at.toLocaleString() }),
      el('td', { text: event.user }),
      el('td', {}, [el('span', {
        class: `audit-connector is-${event.connector}`,
        text: event.connector,
      })]),
      el('td', {}, [
        el('strong', { text: event.operation }),
        el('small', { text: event.capability }),
      ]),
      el('td', {}, [el('span', {
        class: `audit-status is-${event.status}`,
        text: event.status,
      })]),
      el('td', { text: event.result_count === null ? '—' : String(event.result_count) }),
      el('td', { text: event.duration_ms === null ? '—' : `${event.duration_ms} ms` }),
      el('td', { class: 'audit-metadata', text: auditMetadataText(event) }),
    ]));
  }
}

async function loadAudit() {
  const connector = $('#audit-connector')?.value || '';
  const status = $('#audit-status')?.value || '';
  const user = $('#audit-user')?.value.trim() || '';
  const query = new URLSearchParams({ limit: '200' });
  if (connector) query.set('connector', connector);
  if (status) query.set('status', status);
  if (user) query.set('user', user);
  const data = await api(`api/admin/connector-audit?${query}`);
  state.auditEvents = data.events || [];
  renderAudit();
}

async function testConnector(id, button) {
  busy(button, true);
  try {
    const card = button.closest('[data-provider]');
    const values = {};
    card.querySelectorAll('input[name]').forEach((input) => {
      values[input.name] = input.value;
    });
    // A card action must operate on the values the administrator can see. The
    // old implementation tested only the last saved state, so a freshly entered
    // API key always appeared broken until the separate page-level save ran.
    await api('api/settings', {
      method: 'POST',
      body: JSON.stringify({ settings: { [id]: values } }),
    });
    const result = await api(`api/test/${id}`, {
      method: 'POST',
      body: JSON.stringify({ settings: values }),
    });
    await load();
    showView('admin');
    toast(result.label || `${CONNECTORS[id].title} responded`);
  } catch (error) {
    toast(error.message, true);
  } finally {
    busy(button, false);
  }
}

/* ── load ──────────────────────────────────────────────────────────────── */

async function load() {
  const data = await api('api/bootstrap');
  state.user = data.user;
  state.sources = data.sources;
  state.settings = data.settings;
  state.personalSettings = data.personal_settings || {};
  state.callbacks = data.oauth_callbacks || {};
  state.connectorAccess = data.connector_access || state.connectorAccess;

  $('#username').textContent = data.user.name;
  $('#role').textContent = data.user.is_admin ? 'Administrator' : 'Member';
  $('#avatar').textContent = data.user.name.slice(0, 1);
  $$('.admin-only').forEach((node) => { node.hidden = !data.user.is_admin; });

  renderPersonalConnections();
  if (data.user.is_admin) renderSettings();
}

/* ── interactions ──────────────────────────────────────────────────────── */

$$('.nav-item').forEach((btn) => btn.addEventListener('click', () => showView(btn.dataset.view)));

$$('[data-copy]').forEach((button) => button.addEventListener('click', async (event) => {
  const target = event.currentTarget;
  const previous = target.textContent;
  try {
    await navigator.clipboard.writeText(target.dataset.copy || '');
    target.textContent = 'Copied';
    setTimeout(() => { target.textContent = previous; }, 1400);
  } catch {
    toast('The browser blocked clipboard access', true);
  }
}));

$('#save-settings').addEventListener('click', async (event) => {
  const settings = {};
  $$('#settings-root [data-provider]').forEach((card) => {
    const provider = card.dataset.provider;
    settings[provider] = {};
    card.querySelectorAll('input[name]').forEach((input) => {
      settings[provider][input.name] = input.value;
    });
  });
  const button = event.currentTarget;
  busy(button, true);
  try {
    await api('api/settings', { method: 'POST', body: JSON.stringify({ settings }) });
    await load();
    toast('Settings saved');
  } catch (error) {
    toast(error.message, true);
  } finally {
    busy(button, false);
  }
});

$('#refresh-audit').addEventListener('click', async (event) => {
  busy(event.currentTarget, true);
  try { await loadAudit(); }
  catch (error) { toast(error.message, true); }
  finally { busy(event.currentTarget, false); }
});

$('#apply-audit-filters').addEventListener('click', () => {
  loadAudit().catch((error) => toast(error.message, true));
});

$('#audit-user').addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    loadAudit().catch((error) => toast(error.message, true));
  }
});

/* Surface the outcome of an OAuth round trip, then clean the URL. */
const oauthResult = new URLSearchParams(location.search).get('oauth');
if (oauthResult) {
  const messages = {
    connected: ['Account connected', false],
    failed: ['The provider did not return an authorization code', true],
    invalid_state: ['That authorization link is no longer valid — try connecting again', true],
    expired: ['The authorization link expired — try connecting again', true],
  };
  const [message, isError] = messages[oauthResult] || ['Authorization finished', false];
  toast(message, isError);
  history.replaceState(null, '', location.pathname);
}

Promise.all([loadBranding(), load()]).catch((error) => toast(error.message, true));
