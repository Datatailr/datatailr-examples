"""Self-contained pi agent app for Datatailr.

This single FastAPI app owns the whole agent runtime and the UI:

- runs the `pi` coding agent in this container (Node + pi installed via
  build_script_pre)
- serves an interactive terminal (xterm.js) wired to a real `pi` PTY over a
  WebSocket -- the terminal and the WebSocket are local, so the only network
  hop is browser -> app (which the platform ingress forwards; internal
  service-to-service WebSocket upgrades are NOT forwarded, which is why the
  agent runtime lives here rather than in a separate service)
- exposes a JSON HTTP API (`/chat`, `/chat/stream`) for programmatic access
- serves an activity dashboard sourced from the on-disk `~/.pi` session store
- isolates sessions per authenticated user and persists per-user sessions plus
  the global config dirs (`~/.pi`, `~/.agents`) to Datatailr blob storage so
  state survives container restarts
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Mapping, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)
from pydantic import BaseModel

from agent_app import blob_sync, pi_runner, pty_runner, sessions

# Default model if the `agent_model` KV key is not set. Provider-prefixed so pi
# selects OpenAI without a separate --provider flag.
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "openai/gpt-5.1")
# Thinking/reasoning level passed to pi (off, minimal, low, medium, high, xhigh).
AGENT_THINKING = os.environ.get("AGENT_THINKING", "medium")
# Datatailr secret/KV key names (create the secret in the Secrets Manager UI).
OPENAI_SECRET_KEY = os.environ.get("OPENAI_SECRET_KEY", "openai_api_key")
MODEL_KV_KEY = os.environ.get("MODEL_KV_KEY", "agent_model")

# Fallback user when no identity header is present (e.g. local dev).
DEFAULT_USER = os.environ.get("AGENT_DEFAULT_USER", "shared")

# Blob prefixes.
SESSIONS_BLOB_PREFIX = os.environ.get("AGENT_SESSIONS_PREFIX", "agent_sessions")
PI_CONFIG_BLOB_PREFIX = os.environ.get("AGENT_PI_CONFIG_PREFIX", "agent_state/pi")
AGENTS_BLOB_PREFIX = os.environ.get("AGENT_AGENTS_PREFIX", "agent_state/agents")

# Config sync excludes the per-user sessions tree (persisted separately).
_PI_CONFIG_EXCLUDES = {"sessions"}

# Header the Datatailr platform sets on requests, identifying the authenticated
# browser user (a JSON blob with a "name" field).
USER_HEADER = "x-datatailr-user"

log = logging.getLogger("agent_app")


# --------------------------------------------------------------------------- #
# Runtime configuration + identity
# --------------------------------------------------------------------------- #
class _Config:
    model: str = DEFAULT_MODEL


_config = _Config()

_session_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()
_USER_RE = re.compile(r"[^A-Za-z0-9_.-]")


def _username(headers: Mapping[str, str]) -> Optional[str]:
    """Parse the platform identity header into a username, or None."""
    raw = headers.get(USER_HEADER)
    if not raw:
        return None
    try:
        name = json.loads(raw).get("name")
    except (ValueError, TypeError, AttributeError):
        log.warning("could not parse %s header as JSON", USER_HEADER)
        return None
    return name or None


def _safe_user(name: Optional[str]) -> str:
    """Normalize a username into a filesystem/blob-safe token."""
    cleaned = _USER_RE.sub("_", (name or "").strip())
    return cleaned or DEFAULT_USER


def _user_session_dir(user: str) -> str:
    return os.path.join(pi_runner.PI_SESSION_DIR, user)


def _user_workspace_dir(user: str) -> str:
    """Per-user working directory for the agent's file/bash/edit tools.

    Isolating this keeps one user's files from being visible to another, since
    all users' pi processes share this single container.
    """
    return os.path.join(pi_runner.PI_WORKSPACE_DIR, user)


def _lock_for(user: str, session_id: Optional[str]) -> threading.Lock:
    key = f"{user}:{session_id or '__new__'}"
    with _locks_guard:
        return _session_locks[key]


# --------------------------------------------------------------------------- #
# Datatailr integration (secrets, KV, skills) + blob persistence
# --------------------------------------------------------------------------- #
def _load_openai_key() -> bool:
    if os.environ.get("OPENAI_API_KEY"):
        return True
    try:
        from datatailr import Secrets

        key = Secrets().get(OPENAI_SECRET_KEY)
        if key:
            os.environ["OPENAI_API_KEY"] = key
            return True
    except Exception:
        pass
    return False


def _load_model() -> str:
    try:
        from datatailr import KV

        value = KV().get(MODEL_KV_KEY)
        if isinstance(value, str) and value.strip():
            return value.strip()
    except Exception:
        pass
    return DEFAULT_MODEL


def _write_pi_settings() -> None:
    """Pin pi's default model and skip first-run network calls."""
    os.makedirs(pi_runner.PI_AGENT_DIR, exist_ok=True)
    settings_path = os.path.join(pi_runner.PI_AGENT_DIR, "settings.json")
    provider, _, model_id = _config.model.partition("/")
    settings = {
        "defaultProjectTrust": "always",
        "enableInstallTelemetry": False,
    }
    if model_id:
        settings["model"] = {"provider": provider, "id": model_id}
    try:
        with open(settings_path, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2)
    except OSError:
        pass


def _setup_datatailr_skills() -> None:
    try:
        from datatailr.sbin.datatailr_cli import setup_skills

        setup_skills(global_dir=True)
    except Exception:
        pass


def _restore_state() -> None:
    blob_sync.pull_dir(PI_CONFIG_BLOB_PREFIX, pi_runner.PI_AGENT_DIR)
    blob_sync.pull_dir(AGENTS_BLOB_PREFIX, pi_runner.AGENTS_DIR)
    blob_sync.pull_dir(SESSIONS_BLOB_PREFIX, pi_runner.PI_SESSION_DIR)


def _persist_config() -> None:
    blob_sync.push_dir(
        pi_runner.PI_AGENT_DIR, PI_CONFIG_BLOB_PREFIX, exclude_dirs=_PI_CONFIG_EXCLUDES
    )
    blob_sync.push_dir(pi_runner.AGENTS_DIR, AGENTS_BLOB_PREFIX)


def _persist_user_sessions(user: str) -> None:
    blob_sync.push_dir(_user_session_dir(user), f"{SESSIONS_BLOB_PREFIX}/{user}")


def _persist_after_pty(user: str) -> None:
    try:
        _persist_user_sessions(user)
        _persist_config()
    except Exception:
        pass


def _startup() -> None:
    os.makedirs(pi_runner.PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(pi_runner.PI_SESSION_DIR, exist_ok=True)
    os.makedirs(pi_runner.AGENTS_DIR, exist_ok=True)
    _config.model = _load_model()
    _load_openai_key()
    _restore_state()
    _setup_datatailr_skills()
    _write_pi_settings()
    _persist_config()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _startup()
    yield


app = FastAPI(title="Pi Agent", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    session_name: Optional[str] = None


def _require_key() -> None:
    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        raise HTTPException(
            status_code=503,
            detail=(
                f"OpenAI API key not configured. Create a secret named "
                f"'{OPENAI_SECRET_KEY}' in the Datatailr Secrets Manager."
            ),
        )


# --------------------------------------------------------------------------- #
# HTTP API
# --------------------------------------------------------------------------- #
@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "OK\n"


@app.get("/api/whoami")
def api_whoami(request: Request) -> JSONResponse:
    return JSONResponse({"user": _username(request.headers)})


@app.get("/api/sessions")
def api_sessions(request: Request) -> JSONResponse:
    user = _safe_user(_username(request.headers))
    return JSONResponse(
        {"user": user, "sessions": sessions.list_sessions(_user_session_dir(user))}
    )


@app.get("/api/sessions/{session_id}")
def api_session(session_id: str, request: Request) -> JSONResponse:
    user = _safe_user(_username(request.headers))
    transcript = sessions.get_transcript(session_id, _user_session_dir(user))
    if transcript is None:
        raise HTTPException(status_code=404, detail="session not found")
    return JSONResponse(transcript)


@app.get("/api/stats")
def api_stats(request: Request) -> JSONResponse:
    user = _safe_user(_username(request.headers))
    stats = sessions.aggregate_stats(_user_session_dir(user))
    stats["user"] = user
    return JSONResponse(stats)


@app.post("/chat")
def chat(req: ChatRequest, request: Request) -> dict:
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")
    _require_key()

    user = _safe_user(_username(request.headers))
    session_dir = _user_session_dir(user)

    with _lock_for(user, req.session_id):
        try:
            result = pi_runner.run_pi(
                message=req.message,
                session_id=req.session_id,
                model=_config.model,
                session_name=req.session_name,
                session_dir=session_dir,
                thinking=AGENT_THINKING,
                workspace_dir=_user_workspace_dir(user),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"pi run failed: {exc}")

    _persist_user_sessions(user)
    _persist_config()
    return {
        "session_id": result.session_id,
        "reply": result.reply,
        "usage": result.usage,
        "user": user,
    }


def _sse(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


@app.post("/chat/stream")
def chat_stream(req: ChatRequest, request: Request):
    """Stream pi's thinking/text/tool events as Server-Sent Events."""
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")
    _require_key()

    user = _safe_user(_username(request.headers))
    session_dir = _user_session_dir(user)
    lock = _lock_for(user, req.session_id)

    def event_stream():
        lock.acquire()
        try:
            for event in pi_runner.stream_pi(
                message=req.message,
                session_id=req.session_id,
                model=_config.model,
                session_name=req.session_name,
                session_dir=session_dir,
                thinking=AGENT_THINKING,
                workspace_dir=_user_workspace_dir(user),
            ):
                if event.get("type") == "done":
                    event["user"] = user
                yield _sse(event)
        except Exception as exc:  # noqa: BLE001
            yield _sse({"type": "error", "detail": f"pi run failed: {exc}"})
        finally:
            try:
                _persist_user_sessions(user)
                _persist_config()
            finally:
                lock.release()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# --------------------------------------------------------------------------- #
# WebSocket: browser terminal  <->  local pi PTY
# --------------------------------------------------------------------------- #
@app.websocket("/ws/pty")
async def ws_pty(websocket: WebSocket) -> None:
    """Bridge an interactive `pi` PTY (in this container) to the browser.

    Protocol (client -> server, JSON text frames):
        {"type": "input",  "data": "<keystrokes>"}
        {"type": "resize", "cols": <int>, "rows": <int>}
    Server -> client: raw PTY output as binary frames (fed to xterm.js).
    """
    await websocket.accept()

    params = websocket.query_params
    user = _safe_user(_username(websocket.headers) or params.get("user"))
    session_id = params.get("session") or None
    try:
        cols = int(params.get("cols", "80"))
        rows = int(params.get("rows", "24"))
    except (TypeError, ValueError):
        cols, rows = 80, 24

    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        await websocket.send_bytes(
            f"\r\n\x1b[31mOpenAI API key not configured. Create a secret named "
            f"'{OPENAI_SECRET_KEY}'.\x1b[0m\r\n".encode()
        )
        await websocket.close()
        return

    session_dir = _user_session_dir(user)
    proc, master_fd = pty_runner.spawn(
        session_dir=session_dir,
        workspace_dir=_user_workspace_dir(user),
        model=_config.model,
        session_id=session_id,
        cols=cols,
        rows=rows,
    )
    loop = asyncio.get_running_loop()

    async def pump_out() -> None:
        try:
            while True:
                data = await loop.run_in_executor(None, pty_runner.read, master_fd)
                if not data:
                    break
                await websocket.send_bytes(data)
        except Exception:  # noqa: BLE001
            pass

    async def pump_in() -> None:
        try:
            while True:
                msg = await websocket.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                text = msg.get("text")
                if text is None:
                    data = msg.get("bytes")
                    if data:
                        pty_runner.write(master_fd, data)
                    continue
                try:
                    obj = json.loads(text)
                except (ValueError, TypeError):
                    pty_runner.write(master_fd, text.encode())
                    continue
                mtype = obj.get("type")
                if mtype == "input":
                    pty_runner.write(master_fd, (obj.get("data") or "").encode())
                elif mtype == "resize":
                    pty_runner.set_winsize(
                        master_fd, int(obj.get("rows", 24)), int(obj.get("cols", 80))
                    )
        except Exception:  # noqa: BLE001
            pass

    out_task = asyncio.create_task(pump_out())
    in_task = asyncio.create_task(pump_in())
    try:
        await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (out_task, in_task):
            task.cancel()
        pty_runner.terminate(proc, master_fd)
        try:
            await websocket.close()
        except Exception:
            pass
        await loop.run_in_executor(None, _persist_after_pty, user)


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_PAGE)


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
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.min.css" />
<script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1117; --panel: #181b24; --panel-2: #1f232f; --border: #2a2f3d;
    --text: #e6e9ef; --muted: #9aa3b2; --accent: #6d8bff; --accent-2: #38d39f;
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
  .view.active { display: flex; flex-direction: column; }

  /* Terminal */
  .term-toolbar { display: flex; align-items: center; gap: 10px; padding: 8px 16px;
                  border-bottom: 1px solid var(--border); background: var(--panel); }
  .term-toolbar button { background: var(--panel-2); color: var(--text); border: 1px solid var(--border);
                         border-radius: 8px; padding: 6px 12px; cursor: pointer; font-size: 13px; }
  .term-toolbar .hint { color: var(--muted); font-size: 12px; margin-left: auto; }
  #terminal { flex: 1; padding: 8px 10px; background: #0f1117; overflow: hidden; }
  .xterm .xterm-viewport { background: transparent !important; }

  /* Dashboard */
  #dashboard { overflow-y: auto; padding: 24px; gap: 20px; }
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
  @media (max-width: 820px) { .charts { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<header>
  <span class="dot" id="status-dot"></span>
  <h1>Pi Agent</h1>
  <span id="whoami" style="font-size:13px;color:var(--muted);"></span>
  <nav>
    <button id="tab-term" class="active" onclick="showView('terminal')">Terminal</button>
    <button id="tab-dash" onclick="showView('dashboard')">Dashboard</button>
  </nav>
</header>
<main>
  <section id="terminal-view" class="view active">
    <div class="term-toolbar">
      <button onclick="restartSession()">Restart session</button>
      <button onclick="reconnect()">Reconnect</button>
      <span class="hint">A live <code>pi</code> session. Type as you would in the CLI.</span>
    </div>
    <div id="terminal"></div>
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
const charts = {};
let term, fitAddon, ws, resumeSession = null, currentUser = null;

// Datatailr serves apps behind a URL prefix that the platform strips before the
// request reaches this app. The browser must include that prefix (= the page's
// own path) so requests route back here instead of the platform root.
const API_BASE = window.location.pathname.replace(/\/+$/, '');
function apiUrl(p) { return API_BASE + p; }

async function readJson(r) {
  const text = await r.text();
  if (!text) return {};
  try { return JSON.parse(text); } catch { return { detail: text.slice(0, 300) }; }
}

function setStatus(ok) { document.getElementById('status-dot').classList.toggle('ok', ok); }

function showView(name) {
  const isTerm = name === 'terminal';
  document.getElementById('terminal-view').classList.toggle('active', isTerm);
  document.getElementById('dashboard').classList.toggle('active', !isTerm);
  document.getElementById('tab-term').classList.toggle('active', isTerm);
  document.getElementById('tab-dash').classList.toggle('active', !isTerm);
  if (isTerm) { setTimeout(fitTerminal, 0); }
  else { loadDashboard(); }
}

async function loadWhoami() {
  try {
    const r = await fetch(apiUrl('/api/whoami'));
    const data = await readJson(r);
    currentUser = data.user || null;
    document.getElementById('whoami').textContent = currentUser ? ('@' + currentUser) : '';
  } catch (e) { /* ignore */ }
}

// --------------------------- Terminal ---------------------------
function fitTerminal() {
  if (!fitAddon) return;
  try { fitAddon.fit(); sendResize(); } catch (e) { /* container not visible yet */ }
}

function wsUrl() {
  const base = window.location.pathname.replace(/\/+$/, '');
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const cols = term ? term.cols : 80;
  const rows = term ? term.rows : 24;
  let url = `${proto}://${location.host}${base}/ws/pty?cols=${cols}&rows=${rows}`;
  if (currentUser) url += '&user=' + encodeURIComponent(currentUser);
  if (resumeSession) url += '&session=' + encodeURIComponent(resumeSession);
  return url;
}

function connect() {
  if (ws) { try { ws.close(); } catch (e) {} }
  ws = new WebSocket(wsUrl());
  ws.binaryType = 'arraybuffer';
  ws.onopen = () => { setStatus(true); sendResize(); if (term) term.focus(); };
  ws.onmessage = (e) => {
    if (typeof e.data === 'string') term.write(e.data);
    else term.write(new Uint8Array(e.data));
  };
  ws.onclose = () => { setStatus(false); if (term) term.write('\r\n\x1b[90m[disconnected — press Reconnect]\x1b[0m\r\n'); };
  ws.onerror = () => { setStatus(false); };
}

function sendResize() {
  if (ws && ws.readyState === 1 && term) {
    ws.send(JSON.stringify({ type: 'resize', cols: term.cols, rows: term.rows }));
  }
}

function reconnect() { if (term) term.reset(); connect(); }
function restartSession() { resumeSession = null; reconnect(); }

function initTerminal() {
  term = new Terminal({
    cursorBlink: true,
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
    fontSize: 13,
    theme: { background: '#0f1117', foreground: '#e6e9ef', cursor: '#6d8bff',
             selectionBackground: '#33415e' },
  });
  fitAddon = new FitAddon.FitAddon();
  term.loadAddon(fitAddon);
  term.open(document.getElementById('terminal'));
  fitTerminal();
  term.onData(d => { if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'input', data: d })); });
  window.addEventListener('resize', fitTerminal);
  connect();
}

// --------------------------- Dashboard ---------------------------
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

// Resolve the user first (so the WS carries it as a fallback identity), then
// boot the terminal.
(async () => { await loadWhoami(); initTerminal(); })();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
