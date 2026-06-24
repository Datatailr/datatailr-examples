"""Flask GUI for the pi agent service.

Provides a chat interface and an activity dashboard. The browser talks to this
app same-origin; the app proxies requests to the internal Datatailr service URL
(`http://pi-agent-service` by default), so the service stays internal and there
are no CORS concerns. All history/stats originate from the service parsing its
~/.pi session store.
"""

from __future__ import annotations

import os

import requests
from flask import Flask, Response, jsonify, render_template_string, request

# Internal service URL. "Pi Agent Service" normalizes to "pi-agent-service".
SERVICE_URL = os.environ.get("AGENT_SERVICE_URL", "http://pi-agent-service").rstrip("/")
REQUEST_TIMEOUT = int(os.environ.get("AGENT_REQUEST_TIMEOUT", "650"))

app = Flask(__name__)


def _service_get(path: str):
    resp = requests.get(f"{SERVICE_URL}{path}", timeout=REQUEST_TIMEOUT)
    return resp.json(), resp.status_code


# --------------------------------------------------------------------------- #
# API proxy routes
# --------------------------------------------------------------------------- #
@app.route("/api/chat", methods=["POST"])
def api_chat():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        resp = requests.post(
            f"{SERVICE_URL}/chat", json=payload, timeout=REQUEST_TIMEOUT
        )
        return Response(
            resp.content,
            status=resp.status_code,
            content_type=resp.headers.get("content-type", "application/json"),
        )
    except requests.RequestException as exc:
        return jsonify({"detail": f"Could not reach agent service: {exc}"}), 502


@app.route("/api/sessions")
def api_sessions():
    try:
        data, status = _service_get("/sessions")
        return jsonify(data), status
    except requests.RequestException as exc:
        return jsonify({"detail": f"Could not reach agent service: {exc}"}), 502


@app.route("/api/sessions/<session_id>")
def api_session(session_id: str):
    try:
        data, status = _service_get(f"/sessions/{session_id}")
        return jsonify(data), status
    except requests.RequestException as exc:
        return jsonify({"detail": f"Could not reach agent service: {exc}"}), 502


@app.route("/api/stats")
def api_stats():
    try:
        data, status = _service_get("/stats")
        return jsonify(data), status
    except requests.RequestException as exc:
        return jsonify({"detail": f"Could not reach agent service: {exc}"}), 502


@app.route("/health")
def health():
    return "OK\n", 200, {"Content-Type": "text/plain"}


@app.route("/")
def index():
    return render_template_string(_PAGE)


# --------------------------------------------------------------------------- #
# Single-page UI (HTML + CSS + JS)
# --------------------------------------------------------------------------- #
_PAGE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Pi Agent</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1117; --panel: #181b24; --panel-2: #1f232f; --border: #2a2f3d;
    --text: #e6e9ef; --muted: #9aa3b2; --accent: #6d8bff; --accent-2: #38d39f;
    --user: #243049; --assistant: #1d2330; --tool: #16202a;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: var(--bg); color: var(--text); height: 100vh; display: flex; flex-direction: column; }
  header { display: flex; align-items: center; gap: 16px; padding: 12px 20px;
           border-bottom: 1px solid var(--border); background: var(--panel); }
  header h1 { font-size: 16px; margin: 0; font-weight: 600; }
  header .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--muted); }
  header .dot.ok { background: var(--accent-2); }
  nav { margin-left: auto; display: flex; gap: 6px; }
  nav button { background: transparent; color: var(--muted); border: 1px solid transparent;
               padding: 7px 14px; border-radius: 8px; cursor: pointer; font-size: 14px; }
  nav button.active { color: var(--text); background: var(--panel-2); border-color: var(--border); }
  main { flex: 1; overflow: hidden; }
  .view { height: 100%; display: none; }
  .view.active { display: flex; }

  /* Chat */
  .sidebar { width: 280px; border-right: 1px solid var(--border); background: var(--panel);
             display: flex; flex-direction: column; }
  .sidebar .new { margin: 12px; padding: 10px; border-radius: 8px; border: 1px solid var(--border);
                  background: var(--accent); color: #fff; cursor: pointer; font-weight: 600; }
  .sessions { overflow-y: auto; flex: 1; padding: 0 8px 8px; }
  .session-item { padding: 10px 12px; border-radius: 8px; cursor: pointer; margin-bottom: 4px; }
  .session-item:hover { background: var(--panel-2); }
  .session-item.active { background: var(--panel-2); border: 1px solid var(--border); }
  .session-item .title { font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .session-item .meta { font-size: 11px; color: var(--muted); margin-top: 3px; }
  .chat-main { flex: 1; display: flex; flex-direction: column; }
  .messages { flex: 1; overflow-y: auto; padding: 24px; display: flex; flex-direction: column; gap: 14px; }
  .msg { max-width: 760px; padding: 12px 16px; border-radius: 12px; line-height: 1.5; white-space: pre-wrap;
         word-wrap: break-word; }
  .msg.user { align-self: flex-end; background: var(--user); }
  .msg.assistant { align-self: flex-start; background: var(--assistant); border: 1px solid var(--border); }
  .msg.tool { align-self: flex-start; background: var(--tool); border: 1px dashed var(--border);
              font-family: ui-monospace, monospace; font-size: 12px; color: var(--muted); max-width: 760px; }
  .msg .role { font-size: 11px; text-transform: uppercase; letter-spacing: .5px; color: var(--muted);
               margin-bottom: 6px; }
  .composer { border-top: 1px solid var(--border); padding: 14px 20px; display: flex; gap: 10px;
              background: var(--panel); }
  .composer textarea { flex: 1; resize: none; height: 52px; background: var(--panel-2); color: var(--text);
                       border: 1px solid var(--border); border-radius: 10px; padding: 12px; font-size: 14px;
                       font-family: inherit; }
  .composer button { padding: 0 22px; border-radius: 10px; border: none; background: var(--accent);
                     color: #fff; font-weight: 600; cursor: pointer; }
  .composer button:disabled { opacity: .5; cursor: not-allowed; }
  .empty { margin: auto; color: var(--muted); text-align: center; }

  /* Dashboard */
  #dashboard { flex-direction: column; overflow-y: auto; padding: 24px; gap: 20px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
  .card .label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; }
  .card .value { font-size: 26px; font-weight: 700; margin-top: 6px; }
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .chart-box { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
  .chart-box h3 { margin: 0 0 12px; font-size: 14px; font-weight: 600; }
  .chart-box.wide { grid-column: 1 / -1; }
  .refresh { align-self: flex-start; background: var(--panel-2); color: var(--text);
             border: 1px solid var(--border); border-radius: 8px; padding: 8px 14px; cursor: pointer; }
  @media (max-width: 820px) { .charts { grid-template-columns: 1fr; } .sidebar { display: none; } }
</style>
</head>
<body>
<header>
  <span class="dot" id="status-dot"></span>
  <h1>Pi Agent</h1>
  <nav>
    <button id="tab-chat" class="active" onclick="showView('chat')">Chat</button>
    <button id="tab-dash" onclick="showView('dashboard')">Dashboard</button>
  </nav>
</header>
<main>
  <section id="chat" class="view active">
    <aside class="sidebar">
      <button class="new" onclick="newChat()">+ New chat</button>
      <div class="sessions" id="session-list"></div>
    </aside>
    <div class="chat-main">
      <div class="messages" id="messages">
        <div class="empty">Start a conversation with the agent.</div>
      </div>
      <div class="composer">
        <textarea id="input" placeholder="Message the agent..."
                  onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();send();}"></textarea>
        <button id="send-btn" onclick="send()">Send</button>
      </div>
    </div>
  </section>
  <section id="dashboard" class="view">
    <button class="refresh" onclick="loadDashboard()">Refresh</button>
    <div class="cards" id="cards"></div>
    <div class="charts">
      <div class="chart-box wide"><h3>Activity over time</h3><canvas id="timelineChart"></canvas></div>
      <div class="chart-box"><h3>Tool usage</h3><canvas id="toolChart"></canvas></div>
      <div class="chart-box"><h3>Model usage</h3><canvas id="modelChart"></canvas></div>
    </div>
  </section>
</main>

<script>
let currentSession = null;
let streaming = false;
const charts = {};

// Datatailr serves apps behind a URL prefix that the platform strips before
// the request reaches this app. The browser must therefore include that prefix,
// which equals the page's own path. Build every API URL relative to it so
// requests are routed back to this app instead of the platform root.
const API_BASE = window.location.pathname.replace(/\/+$/, '');
function apiUrl(p) { return API_BASE + p; }

// Tolerant parser: an empty or non-JSON body (e.g. a proxy/gateway error page)
// must not crash the UI with "Unexpected end of JSON input".
async function readJson(r) {
  const text = await r.text();
  if (!text) return {};
  try { return JSON.parse(text); }
  catch { return { detail: text.slice(0, 300) }; }
}

function showView(name) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById(name).classList.add('active');
  document.getElementById('tab-chat').classList.toggle('active', name === 'chat');
  document.getElementById('tab-dash').classList.toggle('active', name === 'dashboard');
  if (name === 'dashboard') loadDashboard();
}

function esc(s) { const d = document.createElement('div'); d.textContent = s || ''; return d.innerHTML; }

async function checkHealth() {
  try { const r = await fetch(apiUrl('/api/sessions')); document.getElementById('status-dot').classList.toggle('ok', r.ok); }
  catch { document.getElementById('status-dot').classList.remove('ok'); }
}

async function loadSessions() {
  try {
    const r = await fetch(apiUrl('/api/sessions'));
    const data = await readJson(r);
    const list = document.getElementById('session-list');
    list.innerHTML = '';
    (data.sessions || []).forEach(s => {
      const div = document.createElement('div');
      div.className = 'session-item' + (s.id === currentSession ? ' active' : '');
      div.onclick = () => openSession(s.id);
      div.innerHTML = `<div class="title">${esc(s.name)}</div>
        <div class="meta">${s.message_count} msgs · ${s.tokens.toLocaleString()} tok</div>`;
      list.appendChild(div);
    });
  } catch (e) { /* service may be warming up */ }
}

function renderMessages(messages) {
  const box = document.getElementById('messages');
  box.innerHTML = '';
  if (!messages || !messages.length) {
    box.innerHTML = '<div class="empty">No messages yet.</div>';
    return;
  }
  messages.forEach(m => addMessage(m.role, m.text, m.tool_name));
  box.scrollTop = box.scrollHeight;
}

function addMessage(role, text, toolName) {
  const box = document.getElementById('messages');
  const empty = box.querySelector('.empty');
  if (empty) empty.remove();
  const div = document.createElement('div');
  div.className = 'msg ' + role;
  const label = role === 'tool' ? ('tool: ' + (toolName || '')) : role;
  div.innerHTML = `<div class="role">${esc(label)}</div>${esc(text)}`;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
  return div;
}

async function openSession(id) {
  currentSession = id;
  await loadSessions();
  try {
    const r = await fetch(apiUrl('/api/sessions/' + id));
    const data = await readJson(r);
    renderMessages(data.messages);
  } catch (e) { renderMessages([]); }
}

function newChat() {
  currentSession = null;
  document.getElementById('messages').innerHTML = '<div class="empty">Start a conversation with the agent.</div>';
  loadSessions();
}

async function send() {
  if (streaming) return;
  const input = document.getElementById('input');
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  addMessage('user', text);
  const pending = addMessage('assistant', 'Thinking...');
  streaming = true;
  document.getElementById('send-btn').disabled = true;
  try {
    const r = await fetch(apiUrl('/api/chat'), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, session_id: currentSession }),
    });
    const data = await readJson(r);
    if (!r.ok) {
      pending.innerHTML = `<div class="role">error</div>${esc(data.detail || 'Request failed')}`;
    } else {
      currentSession = data.session_id || currentSession;
      pending.innerHTML = `<div class="role">assistant</div>${esc(data.reply)}`;
      loadSessions();
    }
  } catch (e) {
    pending.innerHTML = `<div class="role">error</div>${esc(String(e))}`;
  } finally {
    streaming = false;
    document.getElementById('send-btn').disabled = false;
  }
}

function card(label, value) {
  return `<div class="card"><div class="label">${label}</div><div class="value">${value}</div></div>`;
}

function drawChart(id, type, labels, values, label) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  charts[id] = new Chart(ctx, {
    type,
    data: {
      labels,
      datasets: [{
        label, data: values,
        backgroundColor: type === 'bar'
          ? '#6d8bff'
          : ['#6d8bff', '#38d39f', '#f0a93b', '#e0607e', '#9b6dff', '#46c7e8'],
        borderColor: '#6d8bff', borderWidth: 2, tension: 0.3, fill: false,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: type === 'doughnut', labels: { color: '#9aa3b2' } } },
      scales: type === 'doughnut' ? {} : {
        x: { ticks: { color: '#9aa3b2' }, grid: { color: '#2a2f3d' } },
        y: { ticks: { color: '#9aa3b2' }, grid: { color: '#2a2f3d' } },
      },
    },
  });
}

async function loadDashboard() {
  try {
    const r = await fetch(apiUrl('/api/stats'));
    const s = await readJson(r);
    const t = s.totals || {};
    document.getElementById('cards').innerHTML =
      card('Sessions', (t.sessions || 0).toLocaleString()) +
      card('Messages', (t.messages || 0).toLocaleString()) +
      card('Total tokens', (t.total_tokens || 0).toLocaleString()) +
      card('Input / Output', `${(t.input_tokens||0).toLocaleString()} / ${(t.output_tokens||0).toLocaleString()}`) +
      card('Cache read', (t.cache_read_tokens || 0).toLocaleString()) +
      card('Est. cost', '$' + (t.cost || 0).toFixed(4));

    const tl = s.timeline || [];
    drawChart('timelineChart', 'line', tl.map(d => d.date), tl.map(d => d.messages), 'Messages');
    const tools = s.tool_usage || [];
    drawChart('toolChart', 'bar', tools.map(d => d.tool), tools.map(d => d.count), 'Calls');
    const models = s.model_usage || [];
    drawChart('modelChart', 'doughnut', models.map(d => d.model), models.map(d => d.count), 'Messages');
  } catch (e) {
    document.getElementById('cards').innerHTML = card('Status', 'Service unavailable');
  }
}

checkHealth();
loadSessions();
setInterval(checkHealth, 15000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
