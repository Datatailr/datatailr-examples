"""FastAPI dashboard for the reactive-graph stock-exchange demo.

Subscribes to every configured node via ZMQ DEALER→ROUTER, aggregates
the live message stream in-memory, and serves a single-page dashboard
that renders stock prices, analytics, and per-node statistics in real
time.

Data path::

    Node ROUTER (ZMQ)  ──EVT──>  DEALER thread (per node)
                                        │
                                        │ call_soon_threadsafe
                                        v
                                    asyncio Queue  ──>  _message_processor
                                        │
                                        │ send_text
                                        v
                                    Browser WebSocket

Control path::

    Browser  ──WS──>  dashboard  ──DEALER(CTL)──>  node ROUTER
                                    <──CTL reply──

All communication uses a single port per node (8080 by default).

Configuration (environment variables)
--------------------------------------
REACTIVE_GRAPH_NODES       comma-separated node specs
                           (``name`` or ``host:port``)
ZMQ_PORT                   default ROUTER port for nodes (default 8080)
RECENT_BUFFER_SIZE         capacity of the in-memory log (default 2000)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any, Deque, Dict, List, Optional, Set

import zmq
from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]

log = logging.getLogger("reactive_graph.dashboard")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_NODES = "market-feed,analytics-engine"
ZMQ_PORT: int = int(os.environ.get("ZMQ_PORT", os.environ.get("ZMQ_PUB_PORT", "8080")))
RECENT_BUFFER_SIZE: int = int(os.environ.get("RECENT_BUFFER_SIZE", "2000"))
SNAPSHOT_LIMIT: int = int(os.environ.get("SNAPSHOT_LIMIT", "50"))
SUB_REFRESH_S: float = float(os.environ.get("SUB_REFRESH_S", "30"))


def _parse_nodes(raw: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for entry in raw.split(","):
        parts = entry.strip().split(":")
        if not parts or not parts[0]:
            continue
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else ZMQ_PORT
        result.append({"host": host, "port": port})
    return result


NODES = _parse_nodes(os.environ.get("REACTIVE_GRAPH_NODES", DEFAULT_NODES))

# ---------------------------------------------------------------------------
# Dashboard state
# ---------------------------------------------------------------------------


class _DashState:
    def __init__(self, buf_size: int) -> None:
        self._buf: Deque[Dict] = deque(maxlen=buf_size)
        self.total_seen: int = 0
        self.stock_prices: Dict[str, Dict] = {}
        self.analytics: Dict[str, Dict] = {}
        self.per_node: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"total": 0}
        )
        self.node_last_seen: Dict[str, float] = {}

    def record(self, frame: Dict) -> None:
        self._buf.append(frame)
        self.total_seen += 1
        node = frame.get("from_node", "")
        if node:
            self.per_node[node]["total"] += 1
            self.node_last_seen[node] = time.time()
        kind = frame.get("kind", "")
        data = frame.get("data", {})
        if kind == "tick" and isinstance(data, dict):
            sym = data.get("symbol", "")
            if sym:
                self.stock_prices[sym] = data
        elif kind == "analytics" and isinstance(data, dict):
            sym = data.get("symbol", "")
            if sym:
                self.analytics[sym] = data

    def snapshot(self, limit: int = 50) -> List[Dict]:
        items = list(self._buf)
        if limit > 0:
            items = items[-limit:]
        return items


_state = _DashState(RECENT_BUFFER_SIZE)
_browser_subs: Set[WebSocket] = set()
_msg_queue: asyncio.Queue[Dict] = asyncio.Queue()
_loop: Optional[asyncio.AbstractEventLoop] = None

# ---------------------------------------------------------------------------
# ZMQ DEALER subscriber thread (one per node)
# ---------------------------------------------------------------------------


def _subscriber_thread(node_cfg: Dict[str, Any]) -> None:
    """Connect a DEALER to a node's ROUTER, send SUB, and stream EVTs."""
    host = node_cfg["host"]
    port = node_cfg["port"]
    endpoint = f"tcp://{host}:{port}"

    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    identity = f"dashboard-sub-{host}-{os.getpid()}".encode("utf-8")
    dealer.setsockopt(zmq.IDENTITY, identity)
    dealer.setsockopt(zmq.LINGER, 200)
    dealer.setsockopt(zmq.RCVHWM, 100_000)
    dealer.setsockopt(zmq.RCVTIMEO, 5000)
    dealer.setsockopt(zmq.RECONNECT_IVL, 500)
    dealer.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
    dealer.connect(endpoint)
    dealer.send(b"SUB")
    log.info("DEALER subscriber connected to %s", endpoint)

    last_sub = time.time()

    while True:
        # Periodically re-register (handles node restarts)
        now = time.time()
        if now - last_sub >= SUB_REFRESH_S:
            try:
                dealer.send(b"SUB", zmq.NOBLOCK)
            except zmq.ZMQError:
                pass
            last_sub = now

        try:
            frames = dealer.recv_multipart()
        except zmq.Again:
            continue
        except zmq.ContextTerminated:
            return
        except zmq.ZMQError as exc:
            log.warning("DEALER %s recv error: %s", endpoint, exc)
            time.sleep(0.5)
            try:
                dealer.send(b"SUB", zmq.NOBLOCK)
            except zmq.ZMQError:
                pass
            last_sub = time.time()
            continue

        if len(frames) < 3 or frames[0] != b"EVT":
            log.debug(
                "DEALER %s: skipping non-EVT frame (len=%d, tag=%r)",
                endpoint, len(frames), frames[0] if frames else b"",
            )
            continue

        topic = frames[1].decode("utf-8", "replace")
        msg = GraphMessage()
        try:
            msg.ParseFromString(frames[2])
        except Exception:
            log.warning("DEALER %s: failed to parse protobuf for topic %s", endpoint, topic)
            continue

        try:
            data = json.loads(msg.text)
        except (json.JSONDecodeError, TypeError):
            data = {}

        frame: Dict[str, Any] = {
            "node": host,
            "topic": topic,
            "kind": msg.kind,
            "from_node": msg.from_node,
            "to_node": msg.to_node,
            "text": msg.text,
            "data": data,
            "timestamp": msg.timestamp,
            "sequence": msg.sequence,
            "hops": list(msg.hops),
            "at": time.time(),
        }
        if _loop is not None:
            _loop.call_soon_threadsafe(_msg_queue.put_nowait, frame)
        else:
            log.warning("DEALER %s: _loop is None, cannot enqueue message", endpoint)


# ---------------------------------------------------------------------------
# Async message processor
# ---------------------------------------------------------------------------

_processor_count: int = 0
_sse_queues: Set[asyncio.Queue] = set()


async def _message_processor() -> None:
    global _processor_count
    while True:
        frame = await _msg_queue.get()
        _processor_count += 1
        if _processor_count <= 3 or _processor_count % 100 == 0:
            log.info(
                "message_processor: processed %d messages (total_seen=%d, queue_size=%d)",
                _processor_count, _state.total_seen + 1, _msg_queue.qsize(),
            )
        _state.record(frame)
        text = json.dumps(frame)

        # Push to WebSocket clients
        dead: List[WebSocket] = []
        for ws in list(_browser_subs):
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _browser_subs.discard(ws)

        # Push to SSE clients
        dead_sse: List[asyncio.Queue] = []
        for q in list(_sse_queues):
            try:
                q.put_nowait(text)
            except asyncio.QueueFull:
                dead_sse.append(q)
        for q in dead_sse:
            _sse_queues.discard(q)


# ---------------------------------------------------------------------------
# Control proxy via short-lived DEALER
# ---------------------------------------------------------------------------


def _send_control_sync(host: str, port: int, cmd: dict) -> dict:
    """Blocking DEALER→ROUTER CTL exchange (run via to_thread)."""
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, f"ctl-{os.getpid()}-{time.time():.0f}".encode())
    dealer.setsockopt(zmq.LINGER, 1000)
    dealer.setsockopt(zmq.RCVTIMEO, 3000)
    dealer.connect(f"tcp://{host}:{port}")
    try:
        dealer.send_multipart([b"CTL", json.dumps(cmd).encode()])
        frames = dealer.recv_multipart()
        if len(frames) >= 2 and frames[0] == b"CTL":
            return json.loads(frames[1])
        return {"ok": False, "error": "unexpected response"}
    except zmq.Again:
        return {"ok": False, "error": "timeout"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        dealer.close()
        ctx.term()


async def _send_control(node_name: str, cmd: dict) -> dict:
    for cfg in NODES:
        if cfg["host"] == node_name:
            return await asyncio.to_thread(
                _send_control_sync, cfg["host"], cfg["port"], cmd
            )
    return {"ok": False, "error": f"unknown node: {node_name}"}


# ---------------------------------------------------------------------------
# FastAPI lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    global _loop
    _loop = asyncio.get_running_loop()

    threads: list[threading.Thread] = []
    for cfg in NODES:
        t = threading.Thread(
            target=_subscriber_thread, args=(cfg,),
            daemon=True, name=f"sub-{cfg['host']}",
        )
        t.start()
        threads.append(t)
        log.info("started DEALER subscriber for %s", cfg["host"])

    processor = asyncio.create_task(_message_processor(), name="msg-processor")
    try:
        yield
    finally:
        processor.cancel()
        try:
            await processor
        except (asyncio.CancelledError, Exception):
            pass


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Reactive Graph Dashboard", lifespan=_lifespan)


@app.get("/health")
def health() -> Response:
    return Response("OK\n", media_type="text/plain")


@app.get("/api/state")
def api_state(limit: int = 50) -> dict:
    return {
        "nodes": [n["host"] for n in NODES],
        "stock_prices": _state.stock_prices,
        "analytics": dict(_state.analytics),
        "per_node": dict(_state.per_node),
        "total_seen": _state.total_seen,
        "messages": _state.snapshot(limit),
    }


@app.get("/api/stream")
async def api_stream() -> StreamingResponse:
    """SSE endpoint — works through any HTTP proxy (unlike WebSocket)."""
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=512)

    # Send initial snapshot as the first SSE event
    snapshot = json.dumps({
        "type": "snapshot",
        "nodes": [n["host"] for n in NODES],
        "stock_prices": _state.stock_prices,
        "analytics": dict(_state.analytics),
        "per_node": dict(_state.per_node),
        "total_seen": _state.total_seen,
        "messages": _state.snapshot(SNAPSHOT_LIMIT),
    })

    async def event_generator():
        _sse_queues.add(q)
        try:
            yield f"data: {snapshot}\n\n"
            while True:
                text = await q.get()
                yield f"data: {text}\n\n"
        finally:
            _sse_queues.discard(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.websocket("/ws")
async def ws_browser(ws: WebSocket) -> None:
    await ws.accept()
    snapshot = {
        "type": "snapshot",
        "nodes": [n["host"] for n in NODES],
        "stock_prices": _state.stock_prices,
        "analytics": dict(_state.analytics),
        "per_node": dict(_state.per_node),
        "total_seen": _state.total_seen,
        "messages": _state.snapshot(SNAPSHOT_LIMIT),
    }
    await ws.send_text(json.dumps(snapshot))
    _browser_subs.add(ws)
    log.info("browser connected (now %d)", len(_browser_subs))
    try:
        while True:
            raw = await ws.receive_text()
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if payload.get("type") == "control":
                result = await _send_control(
                    payload.get("node", ""),
                    {"action": payload.get("action", ""), **payload.get("params", {})},
                )
                await ws.send_text(
                    json.dumps({"type": "control_result", "result": result})
                )
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("browser ws error: %s", exc)
    finally:
        _browser_subs.discard(ws)
        log.info("browser disconnected (now %d)", len(_browser_subs))


@app.post("/api/control")
async def api_control(request: dict) -> dict:
    """HTTP fallback for control commands when WebSocket isn't available."""
    result = await _send_control(
        request.get("node", ""),
        {"action": request.get("action", ""), **request.get("params", {})},
    )
    return {"type": "control_result", "result": result}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return _INDEX_HTML


# ---------------------------------------------------------------------------
# Embedded single-page dashboard
# ---------------------------------------------------------------------------

_INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reactive Graph &mdash; Stock Exchange</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2333;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--green:#3fb950;--red:#f85149;--blue:#58a6ff;--orange:#d29922;--cyan:#39d2c0;--purple:#bc8cff}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5}
header{background:var(--panel);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;flex-wrap:wrap;gap:20px;position:sticky;top:0;z-index:10}
header h1{font-size:16px;font-weight:700;letter-spacing:.3px}
header .stat{color:var(--muted);font-size:13px}
header .stat b{color:var(--text);font-weight:600}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);margin-right:6px;vertical-align:middle;transition:background .3s}
.dot.ok{background:var(--green)}
main{padding:20px 24px 40px;max-width:1600px;margin:0 auto}
h2.section{font-size:11px;color:var(--muted);margin:0 0 10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px}

.controls{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:20px;display:flex;flex-wrap:wrap;gap:16px;align-items:center}
.controls label{font-size:12px;color:var(--muted);display:flex;align-items:center;gap:8px}
.controls input[type=range]{width:120px;accent-color:var(--blue)}
.controls input[type=text]{background:var(--panel2);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:4px 8px;font-size:12px;width:80px}
.btn{background:var(--panel2);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:5px 14px;font-size:12px;cursor:pointer;font-weight:600;transition:background .15s}
.btn:hover{background:var(--border)}
.btn.primary{background:var(--blue);border-color:var(--blue);color:#fff}
.btn.primary:hover{opacity:.85}
.btn.danger{background:var(--red);border-color:var(--red);color:#fff}
.btn.danger:hover{opacity:.85}

.ticker-ribbon{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:20px}
.ticker-card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px 18px;min-width:170px;flex:1}
.ticker-card .sym{font-size:13px;font-weight:700;color:var(--muted);margin-bottom:2px}
.ticker-card .price{font-size:24px;font-weight:700;font-variant-numeric:tabular-nums}
.ticker-card .change{font-size:12px;font-weight:600;margin-top:2px}
.ticker-card .change.up{color:var(--green)}
.ticker-card .change.down{color:var(--red)}
.ticker-card .change.flat{color:var(--muted)}
.ticker-card .meta{font-size:11px;color:var(--muted);margin-top:6px;display:flex;gap:12px}

.node-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-bottom:20px}
.node-card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.node-card h3{font-size:13px;font-weight:600;margin-bottom:8px}
.node-card .counts{display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:11px;color:var(--muted)}
.node-card .counts b{display:block;font-size:16px;color:var(--text);font-weight:600;font-variant-numeric:tabular-nums}

.tbl-wrap{overflow-x:auto;margin-bottom:20px}
table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--border);border-radius:10px;overflow:hidden}
thead th{background:var(--panel2);text-align:left;padding:10px 12px;font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);font-weight:700}
tbody td{padding:7px 12px;border-top:1px solid var(--border);font-size:12px;font-variant-numeric:tabular-nums;vertical-align:middle;font-family:ui-monospace,"SF Mono",Menlo,monospace}
tbody tr:hover{background:var(--panel2)}
.up-col{color:var(--green);font-weight:600}
.down-col{color:var(--red);font-weight:600}
.neutral-col{color:var(--muted)}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;font-family:-apple-system,sans-serif}
.badge.tick{background:rgba(88,166,255,.15);color:var(--blue)}
.badge.analytics{background:rgba(188,140,255,.15);color:var(--purple)}
.badge.validated_tick{background:rgba(63,185,80,.15);color:var(--green)}
.badge.rejected{background:rgba(248,81,73,.15);color:var(--red)}
.text-muted{color:var(--muted)}
@keyframes flash-green{from{background:rgba(63,185,80,.15)}to{background:transparent}}
@keyframes flash-blue{from{background:rgba(88,166,255,.12)}to{background:transparent}}
tr.new-tick td{animation:flash-green .8s ease-out}
tr.new-analytics td{animation:flash-blue .8s ease-out}
.empty-msg{padding:24px;text-align:center;color:var(--muted);background:var(--panel);border:1px solid var(--border);border-radius:10px}
</style>
</head>
<body>
<header>
  <h1>Reactive Graph &mdash; Stock Exchange</h1>
  <span class="stat"><span class="dot" id="ws-dot"></span><span id="ws-text">connecting&hellip;</span></span>
  <span class="stat">Events: <b id="total-count">0</b></span>
</header>

<main>
  <h2 class="section">Controls</h2>
  <div class="controls" id="controls">
    <button class="btn primary" id="btn-pause" onclick="sendControl('market-feed','pause')">Pause Feed</button>
    <button class="btn" id="btn-resume" onclick="sendControl('market-feed','resume')">Resume Feed</button>
    <label>Interval
      <input type="range" id="rng-interval" min="0.1" max="5" step="0.1" value="1.0"
             oninput="document.getElementById('lbl-interval').textContent=this.value+'s'"
             onchange="sendControl('market-feed','set_interval',{interval:parseFloat(this.value)})">
      <span id="lbl-interval">1.0s</span>
    </label>
    <label>Add symbol
      <input type="text" id="inp-symbol" placeholder="e.g. META" maxlength="6">
      <button class="btn" onclick="addSymbol()">+</button>
    </label>
    <label>Analytics window
      <input type="range" id="rng-window" min="5" max="100" step="5" value="20"
             oninput="document.getElementById('lbl-window').textContent=this.value"
             onchange="sendControl('analytics-engine','set_analytics_window',{window:parseInt(this.value)})">
      <span id="lbl-window">20</span>
    </label>
  </div>

  <h2 class="section">Stock Ticker</h2>
  <div class="ticker-ribbon" id="ticker"></div>

  <h2 class="section">Nodes</h2>
  <div class="node-grid" id="nodes"></div>

  <h2 class="section">Analytics</h2>
  <div class="tbl-wrap">
    <table id="analytics-table" hidden>
      <thead><tr>
        <th>Symbol</th><th>Price</th><th>SMA (short)</th><th>SMA (long)</th>
        <th>VWAP</th><th>High</th><th>Low</th><th>Volatility</th>
        <th>Trend</th><th>Session &Delta;</th><th>Samples</th>
      </tr></thead>
      <tbody id="analytics-body"></tbody>
    </table>
    <div class="empty-msg" id="analytics-empty">Waiting for analytics data&hellip;</div>
  </div>

  <h2 class="section">Live Feed</h2>
  <div class="tbl-wrap">
    <table id="feed-table" hidden>
      <thead><tr>
        <th>Time</th><th>Node</th><th>Kind</th><th>Symbol</th><th>Price</th><th>Details</th>
      </tr></thead>
      <tbody id="feed-body"></tbody>
    </table>
    <div class="empty-msg" id="feed-empty">Waiting for messages&hellip;</div>
  </div>
</main>

<script>
(function(){
  const MAX_FEED=30;
  const wsDot=document.getElementById('ws-dot'),wsText=document.getElementById('ws-text');
  const totalEl=document.getElementById('total-count');
  const tickerEl=document.getElementById('ticker');
  const nodesEl=document.getElementById('nodes');
  const abody=document.getElementById('analytics-body'),atable=document.getElementById('analytics-table'),aempty=document.getElementById('analytics-empty');
  const fbody=document.getElementById('feed-body'),ftable=document.getElementById('feed-table'),fempty=document.getElementById('feed-empty');

  let state={stockPrices:{},analytics:{},perNode:{},total:0};

  function fmt(n,d){return n!=null?Number(n).toFixed(d!=null?d:2):'\u2014';}
  function fmtTime(epoch){
    const d=new Date(epoch*1000);
    return [d.getHours(),d.getMinutes(),d.getSeconds()].map(v=>String(v).padStart(2,'0')).join(':')+'.'+String(d.getMilliseconds()).padStart(3,'0');
  }
  function chgClass(v){return v>0?'up':v<0?'down':'flat';}
  function trendClass(t){return t==='up'?'up-col':t==='down'?'down-col':'neutral-col';}

  function renderTicker(){
    const syms=Object.keys(state.stockPrices).sort();
    tickerEl.innerHTML='';
    syms.forEach(s=>{
      const d=state.stockPrices[s];const cc=chgClass(d.change_pct);
      const div=document.createElement('div');div.className='ticker-card';
      div.innerHTML=`<div class="sym">${s}</div>
        <div class="price">$${fmt(d.price)}</div>
        <div class="change ${cc}">${d.change_pct>=0?'+':''}${fmt(d.change_pct,3)}%&ensp;(${d.change>=0?'+':''}${fmt(d.change)})</div>
        <div class="meta"><span>Bid ${fmt(d.bid)}</span><span>Ask ${fmt(d.ask)}</span><span>Vol ${d.volume!=null?d.volume.toLocaleString():'\u2014'}</span></div>`;
      tickerEl.appendChild(div);
    });
  }

  function renderNodes(){
    const names=Object.keys(state.perNode).sort();
    nodesEl.innerHTML='';
    names.forEach(n=>{
      const p=state.perNode[n]||{};
      const div=document.createElement('div');div.className='node-card';
      div.innerHTML=`<h3><span class="dot ok"></span>${n}</h3>
        <div class="counts"><div>total<b>${p.total||0}</b></div></div>`;
      nodesEl.appendChild(div);
    });
  }

  function renderAnalytics(){
    const syms=Object.keys(state.analytics).sort();
    if(!syms.length)return;
    atable.hidden=false;aempty.hidden=true;
    abody.innerHTML='';
    syms.forEach(s=>{
      const a=state.analytics[s];const tr=document.createElement('tr');
      const tc=trendClass(a.trend);
      tr.innerHTML=`<td><b>${s}</b></td><td>${fmt(a.price)}</td><td>${fmt(a.sma_short)}</td><td>${fmt(a.sma_long)}</td>
        <td>${fmt(a.vwap)}</td><td>${fmt(a.high)}</td><td>${fmt(a.low)}</td><td>${fmt(a.volatility,4)}</td>
        <td class="${tc}">${(a.trend||'\u2014').toUpperCase()}</td>
        <td class="${chgClass(a.session_change_pct)==='up'?'up-col':'down-col'}">${a.session_change_pct>=0?'+':''}${fmt(a.session_change_pct)}%</td>
        <td class="text-muted">${a.samples||0}</td>`;
      abody.appendChild(tr);
    });
  }

  function addFeedRow(m,animate){
    ftable.hidden=false;fempty.hidden=true;
    const tr=document.createElement('tr');
    if(animate)tr.className=m.kind==='tick'?'new-tick':'new-analytics';
    const sym=(m.data&&m.data.symbol)||'';
    const price=(m.data&&m.data.price!=null)?'$'+fmt(m.data.price):'';
    let detail='';
    if(m.kind==='tick') detail=`vol=${(m.data&&m.data.volume)||''} chg=${(m.data&&m.data.change_pct!=null)?m.data.change_pct+'%':''}`;
    else if(m.kind==='analytics') detail=`sma=${fmt(m.data&&m.data.sma_short)} vwap=${fmt(m.data&&m.data.vwap)} trend=${(m.data&&m.data.trend)||''}`;
    else if(m.kind==='validated_tick') detail='validated';
    else if(m.kind==='rejected') detail=(m.data&&m.data.reason)||'rejected';
    else detail=(m.text||'').slice(0,60);
    tr.innerHTML=`<td class="text-muted">${fmtTime(m.at||m.timestamp||0)}</td>
      <td>${m.from_node||m.node||''}</td>
      <td><span class="badge ${m.kind||''}">${m.kind||''}</span></td>
      <td><b>${sym}</b></td><td>${price}</td><td class="text-muted">${detail}</td>`;
    fbody.insertBefore(tr,fbody.firstChild);
    while(fbody.children.length>MAX_FEED)fbody.removeChild(fbody.lastChild);
  }

  const base=location.pathname.replace(/\/+$/,'');

  function applySnapshot(p){
    state.stockPrices=p.stock_prices||{};state.analytics=p.analytics||{};
    state.perNode=p.per_node||{};state.total=p.total_seen||0;
    totalEl.textContent=state.total;
    renderTicker();renderNodes();renderAnalytics();
    fbody.innerHTML='';
    (p.messages||[]).forEach(m=>addFeedRow(m,false));
  }
  function applyEvent(p){
    state.total++;totalEl.textContent=state.total;
    if(p.data){
      if(p.kind==='tick'&&p.data.symbol) state.stockPrices[p.data.symbol]=p.data;
      if(p.kind==='analytics'&&p.data.symbol) state.analytics[p.data.symbol]=p.data;
    }
    const nn=p.from_node||p.node||'';
    if(nn){if(!state.perNode[nn])state.perNode[nn]={total:0};state.perNode[nn].total++;}
    renderTicker();renderNodes();renderAnalytics();
    addFeedRow(p,true);
  }

  /* ---- SSE (real-time, works through any HTTP proxy) ---- */
  let sseConnected=false;
  function connectSSE(){
    const es=new EventSource(`${base}/api/stream`);
    es.onopen=()=>{sseConnected=true;wsDot.classList.add('ok');wsText.textContent='connected (live)';};
    es.onerror=()=>{
      sseConnected=false;wsDot.classList.remove('ok');wsText.textContent='reconnecting\u2026';
      es.close();setTimeout(connectSSE,3000);
    };
    es.onmessage=(ev)=>{
      let p;try{p=JSON.parse(ev.data);}catch(e){return;}
      if(p.type==='snapshot') applySnapshot(p);
      else applyEvent(p);
    };
  }
  connectSSE();

  /* ---- control via HTTP POST ---- */
  window.sendControl=async function(node,action,params){
    const body=JSON.stringify({type:'control',node:node,action:action,params:params||{}});
    try{
      await fetch(`${base}/api/control`,{method:'POST',headers:{'Content-Type':'application/json'},body:body});
    }catch(e){console.warn('control failed',e);}
  };
  window.addSymbol=function(){
    const inp=document.getElementById('inp-symbol');
    const sym=inp.value.trim().toUpperCase();
    if(sym){sendControl('market-feed','add_symbol',{symbol:sym});inp.value='';}
  };
  document.getElementById('inp-symbol').addEventListener('keydown',e=>{if(e.key==='Enter')addSymbol();});
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Standalone & Datatailr entrypoint
# ---------------------------------------------------------------------------


def main(port: int) -> None:
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    uvicorn.run(app, host="0.0.0.0", port=int(port), log_level="info")


if __name__ == "__main__":
    main(int(os.environ.get("PORT", 8000)))
