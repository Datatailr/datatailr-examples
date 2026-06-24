"""Flask GUI for the pi agent service.

Provides a chat interface and an activity dashboard. The browser talks to this
app same-origin; the app proxies requests to the internal Datatailr service URL
(`http://pi-agent-service` by default), so the service stays internal and there
are no CORS concerns. All history/stats originate from the service parsing its
~/.pi session store.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import requests
from flask import (
    Flask,
    Response,
    jsonify,
    render_template_string,
    request,
    stream_with_context,
)

# Internal service URL. "Pi Agent Service" normalizes to "pi-agent-service".
SERVICE_URL = os.environ.get("AGENT_SERVICE_URL", "http://pi-agent-service").rstrip("/")
REQUEST_TIMEOUT = int(os.environ.get("AGENT_REQUEST_TIMEOUT", "650"))

# Header the Datatailr platform sets on requests to the app, identifying the
# authenticated browser user (a JSON blob with a "name" field). We forward the
# resolved username to the service so it can isolate sessions per user.
USER_HEADER = "x-datatailr-user"
FORWARD_HEADER = "X-Agent-User"

log = logging.getLogger("agent_app")
app = Flask(__name__)


def username_from_request() -> Optional[str]:
    """Return the authenticated username from the platform header, or None.

    Reads and parses the ``x-datatailr-user`` header only -- no platform call,
    so it is safe to invoke on every request.
    """
    raw = request.headers.get(USER_HEADER)
    if not raw:
        return None
    try:
        name = json.loads(raw).get("name")
    except (ValueError, TypeError, AttributeError):
        log.warning("could not parse %s header as JSON", USER_HEADER)
        return None
    return name or None


def _forward_headers() -> dict:
    """Headers to attach to the proxied service request (carries identity)."""
    user = username_from_request()
    return {FORWARD_HEADER: user} if user else {}


def _service_get(path: str):
    resp = requests.get(
        f"{SERVICE_URL}{path}", headers=_forward_headers(), timeout=REQUEST_TIMEOUT
    )
    return resp.json(), resp.status_code


# --------------------------------------------------------------------------- #
# API proxy routes
# --------------------------------------------------------------------------- #
@app.route("/api/whoami")
def api_whoami():
    return jsonify({"user": username_from_request()})


@app.route("/api/chat", methods=["POST"])
def api_chat():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        resp = requests.post(
            f"{SERVICE_URL}/chat",
            json=payload,
            headers=_forward_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        return Response(
            resp.content,
            status=resp.status_code,
            content_type=resp.headers.get("content-type", "application/json"),
        )
    except requests.RequestException as exc:
        return jsonify({"detail": f"Could not reach agent service: {exc}"}), 502


@app.route("/api/chat/stream", methods=["POST"])
def api_chat_stream():
    payload = request.get_json(force=True, silent=True) or {}
    headers = _forward_headers()

    def relay():
        try:
            with requests.post(
                f"{SERVICE_URL}/chat/stream",
                json=payload,
                headers=headers,
                stream=True,
                timeout=REQUEST_TIMEOUT,
            ) as resp:
                for chunk in resp.iter_content(chunk_size=None):
                    if chunk:
                        yield chunk
        except requests.RequestException as exc:
            yield (
                "data: "
                + json.dumps({"type": "error", "detail": f"Could not reach agent service: {exc}"})
                + "\n\n"
            ).encode()

    return Response(
        stream_with_context(relay()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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

  /* Streaming / CLI feel */
  .msg .thinking { display: none; margin: 2px 0 8px; padding: 6px 10px;
                   border-left: 2px solid var(--border); background: rgba(255,255,255,.02);
                   border-radius: 6px; }
  .msg .thinking.show { display: block; }
  .msg .th-label { cursor: pointer; color: var(--muted); font-size: 10px;
                   text-transform: uppercase; letter-spacing: .6px; user-select: none; }
  .msg .th-body { display: none; white-space: pre-wrap; color: var(--muted);
                  font-style: italic; font-size: 13px; margin-top: 6px;
                  font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .msg .thinking.open .th-body { display: block; }
  .msg .tools { display: flex; flex-direction: column; gap: 4px; margin: 4px 0; }
  .msg .tool { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px;
               color: var(--muted); }
  .msg .tool .spin { color: var(--accent); animation: pulse 1s ease-in-out infinite; }
  .msg .tool.done { color: var(--accent-2); }
  .msg .tool.error { color: #e0607e; }
  .msg .text { white-space: pre-wrap; }
  .cursor { display: inline-block; width: 7px; height: 1em; vertical-align: text-bottom;
            background: var(--accent); margin-left: 1px;
            animation: blink 1s steps(2, start) infinite; }
  @keyframes blink { 0%, 50% { opacity: 1; } 50.01%, 100% { opacity: 0; } }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .3; } }

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
  <span id="whoami" style="font-size:13px;color:var(--muted);"></span>
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

async function loadWhoami() {
  try {
    const r = await fetch(apiUrl('/api/whoami'));
    const data = await readJson(r);
    document.getElementById('whoami').textContent = data.user ? ('@' + data.user) : '';
  } catch (e) { /* ignore */ }
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

// Builds a live-updating assistant message. The service streams normalized
// events (thinking / text / tool_start / tool_end / done / error) which we
// render incrementally to mimic the CLI experience.
function createAssistantStream() {
  const box = document.getElementById('messages');
  const empty = box.querySelector('.empty');
  if (empty) empty.remove();
  const root = document.createElement('div');
  root.className = 'msg assistant';
  root.innerHTML =
    '<div class="role">assistant</div>' +
    '<div class="thinking"><span class="th-label">thinking</span><div class="th-body"></div></div>' +
    '<div class="tools"></div>' +
    '<div class="text"></div><span class="cursor"></span>';
  box.appendChild(root);
  const thinkingEl = root.querySelector('.thinking');
  const thBody = root.querySelector('.th-body');
  const thLabel = root.querySelector('.th-label');
  const toolsEl = root.querySelector('.tools');
  const textEl = root.querySelector('.text');
  const cursor = root.querySelector('.cursor');
  const tools = {};
  let hasText = false;
  thLabel.onclick = () => thinkingEl.classList.toggle('open');
  const scroll = () => { box.scrollTop = box.scrollHeight; };

  return {
    handle(ev) {
      switch (ev.type) {
        case 'session':
          if (ev.session_id) currentSession = ev.session_id;
          break;
        case 'thinking':
          thinkingEl.classList.add('show', 'open');
          thBody.textContent += ev.delta || '';
          scroll();
          break;
        case 'text':
          hasText = true;
          textEl.textContent += ev.delta || '';
          scroll();
          break;
        case 'tool_start': {
          const t = document.createElement('div');
          t.className = 'tool';
          t.innerHTML = `<span class="spin">●</span> ${esc(ev.name || 'tool')}`;
          toolsEl.appendChild(t);
          tools[ev.id || ev.name] = t;
          scroll();
          break;
        }
        case 'tool_end': {
          const t = tools[ev.id || ev.name];
          if (t) {
            t.classList.add(ev.is_error ? 'error' : 'done');
            t.innerHTML = `<span>${ev.is_error ? '\u2717' : '\u2713'}</span> ${esc(ev.name || 'tool')}`;
          }
          break;
        }
        case 'done':
          if (ev.session_id) currentSession = ev.session_id;
          if (!hasText && ev.reply) textEl.textContent = ev.reply;
          break;
        case 'error':
          this.fail(ev.detail || 'Request failed');
          break;
      }
    },
    finish() {
      cursor.remove();
      // Collapse thinking once the turn is done, but keep it toggleable.
      thinkingEl.classList.remove('open');
    },
    fail(msg) {
      cursor.remove();
      root.querySelector('.role').textContent = 'error';
      textEl.textContent = msg;
    },
  };
}

async function send() {
  if (streaming) return;
  const input = document.getElementById('input');
  const text = input.value.trim();
  if (!text) return;
  input.value = '';
  addMessage('user', text);
  const stream = createAssistantStream();
  streaming = true;
  document.getElementById('send-btn').disabled = true;
  try {
    const r = await fetch(apiUrl('/api/chat/stream'), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, session_id: currentSession }),
    });
    if (!r.ok || !r.body) {
      const data = await readJson(r);
      stream.fail(data.detail || ('HTTP ' + r.status));
      return;
    }
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      // SSE frames are separated by a blank line.
      let idx;
      while ((idx = buffer.indexOf('\n\n')) >= 0) {
        const frame = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const payload = frame.split('\n')
          .filter(l => l.startsWith('data:'))
          .map(l => l.slice(5).trim())
          .join('');
        if (!payload) continue;
        let ev;
        try { ev = JSON.parse(payload); } catch { continue; }
        stream.handle(ev);
      }
    }
    stream.finish();
  } catch (e) {
    stream.fail(String(e));
  } finally {
    streaming = false;
    document.getElementById('send-btn').disabled = false;
    loadSessions();
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

loadWhoami();
checkHealth();
loadSessions();
setInterval(checkHealth, 15000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
