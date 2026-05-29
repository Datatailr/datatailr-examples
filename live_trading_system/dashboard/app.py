"""FastAPI dashboard for the Live Trading System demo.

A reactive-graph trading pipeline: every event stamps the chain of
nodes it passed through, so the dashboard can reconstruct the live
topology from the stream itself.

Subscribes to every configured node via ZMQ DEALER -> ROUTER, aggregates
the live event stream in-memory, and serves a single-page dashboard
with:

* a **live topology view** built from every event's ``hops`` field
  (nodes + edges, with per-edge messages-per-second);
* **ticker** + **analytics** panels (existing);
* a **positions / PnL** panel (from ``position_update`` events);
* an **order blotter** (signals, order intents, fills, rejections);
* a **system events** ribbon (broadcast by ``notification-bus``, e.g.
  for ``market_open`` and ``eod_complete``);
* runtime **controls** that issue CTL frames to the appropriate
  services (pause feed, change tick interval, toggle strategies,
  tune risk limits).

Data path::

    Node ROUTER (ZMQ)  --EVT-->  DEALER thread (per node)
                                        |
                                        | call_soon_threadsafe
                                        v
                                    asyncio Queue  -->  _message_processor
                                        |
                                        | send_text
                                        v
                                    Browser WebSocket / SSE

Control path::

    Browser  --HTTP-->  dashboard  --DEALER(CTL)-->  target node ROUTER
                                    <--CTL reply--

Configuration (environment variables)
--------------------------------------
LIVE_TRADING_SYSTEM_NODES       comma-separated node specs (``name`` or
                           ``host:port``); each spec maps to a
                           DEALER subscription.
ZMQ_PORT                   default ROUTER port for nodes (default 8080).
RECENT_BUFFER_SIZE         capacity of the in-memory log (default 2000).
EDGE_WINDOW_S              sliding window for per-edge msg/s (default 5).
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
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import zmq
from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse

from live_trading_system.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]
from live_trading_system.persistence import parquet_io

log = logging.getLogger("live_trading_system.dashboard")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_NODES = (
    "market-feed,analytics-engine,signal-engine,"
    "risk-engine,execution-simulator,notification-bus"
)
ZMQ_PORT: int = int(os.environ.get("ZMQ_PORT", os.environ.get("ZMQ_PUB_PORT", "8080")))
RECENT_BUFFER_SIZE: int = int(os.environ.get("RECENT_BUFFER_SIZE", "2000"))
SNAPSHOT_LIMIT: int = int(os.environ.get("SNAPSHOT_LIMIT", "60"))
SUB_REFRESH_S: float = float(os.environ.get("SUB_REFRESH_S", "5"))
EDGE_WINDOW_S: float = float(os.environ.get("EDGE_WINDOW_S", "5"))


def _parse_nodes(raw: str) -> List[Dict[str, Any]]:
    """Parse LIVE_TRADING_SYSTEM_NODES.

    Supported entry syntaxes::

        host                       (port defaults to ZMQ_PORT, name == host)
        host:port                  (name == host)
        name@host                  (port defaults to ZMQ_PORT)
        name@host:port             (fully qualified)

    The optional ``name@`` alias lets the dashboard address services
    by their logical name even when several entries share the same
    physical host (typical when running the whole graph on
    ``127.0.0.1`` during local development).
    """
    result: List[Dict[str, Any]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "@" in entry:
            name, _, addr = entry.partition("@")
            name = name.strip() or addr
        else:
            name = ""
            addr = entry
        parts = addr.split(":")
        if not parts or not parts[0]:
            continue
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else ZMQ_PORT
        result.append({"host": host, "port": port, "name": name or host})
    return result


NODES = _parse_nodes(os.environ.get("LIVE_TRADING_SYSTEM_NODES", DEFAULT_NODES))


# ---------------------------------------------------------------------------
# Protobuf payload helpers
# ---------------------------------------------------------------------------


def _payload_to_dict(msg: GraphMessage) -> Dict[str, Any]:
    """Project the active ``oneof`` payload of *msg* into a plain dict."""
    which = msg.WhichOneof("payload")
    if which == "tick":
        t = msg.tick
        return {
            "symbol": t.symbol, "price": t.price, "bid": t.bid, "ask": t.ask,
            "volume": int(t.volume), "change": t.change, "change_pct": t.change_pct,
        }
    if which == "analytics":
        a = msg.analytics
        return {
            "symbol": a.symbol, "price": a.price,
            "sma_short": a.sma_short, "sma_long": a.sma_long,
            "vwap": a.vwap, "high": a.high, "low": a.low,
            "volatility": a.volatility, "volume_avg": int(a.volume_avg),
            "session_change": a.session_change,
            "session_change_pct": a.session_change_pct,
            "trend": a.trend, "samples": int(a.samples), "window": int(a.window),
        }
    if which == "signal":
        s = msg.signal
        return {
            "symbol": s.symbol, "side": s.side, "strategy": s.strategy,
            "strength": s.strength, "reference_price": s.reference_price,
            "suggested_qty": int(s.suggested_qty),
        }
    if which == "order_intent":
        o = msg.order_intent
        return {
            "symbol": o.symbol, "side": o.side, "status": o.status,
            "reason": o.reason, "qty": int(o.qty), "price": o.price,
            "strategy": o.strategy,
        }
    if which == "fill":
        f = msg.fill
        return {
            "symbol": f.symbol, "side": f.side, "qty": int(f.qty),
            "price": f.price, "slippage": f.slippage,
            "strategy": f.strategy, "order_id": f.order_id,
        }
    if which == "position_update":
        p = msg.position_update
        return {
            "symbol": p.symbol, "net_qty": int(p.net_qty),
            "avg_price": p.avg_price, "market_price": p.market_price,
            "realised_pnl": p.realised_pnl, "unrealised_pnl": p.unrealised_pnl,
        }
    if which == "system":
        s = msg.system
        return {
            "kind": s.kind, "summary": s.summary,
            "detail": s.detail, "source": s.source,
        }
    if which == "rejected":
        r = msg.rejected
        return {
            "symbol": r.symbol, "reason": r.reason, "original_id": r.original_id,
        }
    if which == "text":
        return {"text": msg.text}
    return {}


# ---------------------------------------------------------------------------
# Dashboard state
# ---------------------------------------------------------------------------


class _DashState:
    def __init__(self, buf_size: int) -> None:
        self._buf: Deque[Dict] = deque(maxlen=buf_size)
        self.total_seen: int = 0
        self.stock_prices: Dict[str, Dict] = {}
        self.analytics: Dict[str, Dict] = {}
        self.positions: Dict[str, Dict] = {}
        self.last_signals: Dict[str, Dict] = {}
        self.system_events: Deque[Dict] = deque(maxlen=50)
        self.per_node: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"total": 0, "by_kind": {}}
        )
        self.node_last_seen: Dict[str, float] = {}
        self.edge_counts: Dict[Tuple[str, str], int] = defaultdict(int)
        self.edge_window: Dict[Tuple[str, str], Deque[float]] = defaultdict(
            lambda: deque(maxlen=500)
        )

    def record(self, frame: Dict) -> None:
        self._buf.append(frame)
        self.total_seen += 1

        node = frame.get("from_node", "") or frame.get("node", "")
        kind = frame.get("kind", "")
        if node:
            entry = self.per_node[node]
            entry["total"] += 1
            entry["by_kind"][kind] = entry["by_kind"].get(kind, 0) + 1
            self.node_last_seen[node] = time.time()

        hops = frame.get("hops") or []
        if hops and node and (not hops or hops[-1] != node):
            hops = list(hops) + [node]
        now = time.time()
        for src, dst in zip(hops, hops[1:]):
            key = (src, dst)
            self.edge_counts[key] += 1
            self.edge_window[key].append(now)

        data = frame.get("data") or {}
        if kind == "tick" and isinstance(data, dict) and data.get("symbol"):
            self.stock_prices[data["symbol"]] = data
        elif kind == "analytics" and isinstance(data, dict) and data.get("symbol"):
            self.analytics[data["symbol"]] = data
        elif kind == "position_update" and isinstance(data, dict) and data.get("symbol"):
            self.positions[data["symbol"]] = data
        elif kind == "signal" and isinstance(data, dict) and data.get("symbol"):
            self.last_signals[data["symbol"]] = {**data, "at": frame.get("at", now)}
        elif kind == "system" and isinstance(data, dict):
            self.system_events.append({**data, "at": frame.get("at", now)})

    def snapshot(self, limit: int = 60) -> List[Dict]:
        items = list(self._buf)
        if limit > 0:
            items = items[-limit:]
        return items

    def edges(self) -> List[Dict]:
        now = time.time()
        cutoff = now - EDGE_WINDOW_S
        out: List[Dict] = []
        for (src, dst), total in self.edge_counts.items():
            window = self.edge_window[(src, dst)]
            while window and window[0] < cutoff:
                window.popleft()
            rate = len(window) / max(EDGE_WINDOW_S, 0.001)
            out.append({
                "from": src, "to": dst,
                "total": total, "rate_per_s": round(rate, 2),
            })
        return out


_state = _DashState(RECENT_BUFFER_SIZE)
_browser_subs: Set[WebSocket] = set()
_msg_queue: asyncio.Queue[Dict] = asyncio.Queue()
_loop: Optional[asyncio.AbstractEventLoop] = None


# ---------------------------------------------------------------------------
# ZMQ DEALER subscriber thread (one per node)
# ---------------------------------------------------------------------------


SUBSCRIBER_RECONNECT_AFTER_S = float(
    os.environ.get("SUBSCRIBER_RECONNECT_AFTER_S", "20")
)


def _open_dealer(host: str, port: int, name: str) -> "zmq.Socket":
    ctx = zmq.Context.instance()
    dealer = ctx.socket(zmq.DEALER)
    identity = (
        f"dashboard-sub-{name}-{os.getpid()}-{int(time.time())}".encode("utf-8")
    )
    dealer.setsockopt(zmq.IDENTITY, identity)
    dealer.setsockopt(zmq.LINGER, 200)
    dealer.setsockopt(zmq.RCVHWM, 100_000)
    dealer.setsockopt(zmq.RCVTIMEO, 5000)
    dealer.setsockopt(zmq.RECONNECT_IVL, 500)
    dealer.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
    dealer.connect(f"tcp://{host}:{port}")
    dealer.send(b"SUB")
    return dealer


def _subscriber_thread(node_cfg: Dict[str, Any]) -> None:
    """Connect a DEALER to a node's ROUTER, send SUB, and stream EVTs.

    The DEALER is automatically torn down and re-created if no events
    have been received for ``SUBSCRIBER_RECONNECT_AFTER_S`` seconds.
    This recovers from a common deployment gotcha: ZMQ DEALERs cache
    DNS resolution and may stick to a dead IP after a container restart.
    """
    host = node_cfg["host"]
    port = node_cfg["port"]
    name = node_cfg.get("name") or host
    endpoint = f"tcp://{host}:{port}"

    dealer = _open_dealer(host, port, name)
    log.info("DEALER subscriber connected to %s", endpoint)

    last_sub = time.time()
    last_event_at = time.time()
    reconnect_attempts = 0

    while True:
        now = time.time()

        if now - last_event_at >= SUBSCRIBER_RECONNECT_AFTER_S:
            reconnect_attempts += 1
            log.warning(
                "DEALER %s silent for %.0fs -> recreating socket (attempt %d)",
                endpoint, now - last_event_at, reconnect_attempts,
            )
            try:
                dealer.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                dealer = _open_dealer(host, port, name)
                last_sub = time.time()
                last_event_at = time.time()
            except Exception as exc:  # noqa: BLE001
                log.warning("DEALER %s reconnect failed: %s", endpoint, exc)
                time.sleep(1.0)
                continue

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
            continue

        topic = frames[1].decode("utf-8", "replace")
        msg = GraphMessage()
        try:
            msg.ParseFromString(frames[2])
        except Exception:
            log.warning("DEALER %s: parse error topic=%s", endpoint, topic)
            continue

        last_event_at = time.time()
        frame: Dict[str, Any] = {
            "node": host,
            "topic": topic,
            "kind": msg.kind,
            "from_node": msg.from_node,
            "to_node": msg.to_node,
            "data": _payload_to_dict(msg),
            "timestamp": msg.timestamp,
            "sequence": int(msg.sequence),
            "hops": list(msg.hops),
            "at": last_event_at,
        }
        if _loop is not None:
            _loop.call_soon_threadsafe(_msg_queue.put_nowait, frame)


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
        if _processor_count <= 3 or _processor_count % 500 == 0:
            log.info(
                "message_processor: processed %d (total_seen=%d, queue=%d)",
                _processor_count, _state.total_seen + 1, _msg_queue.qsize(),
            )
        _state.record(frame)
        text = json.dumps(frame)

        dead: List[WebSocket] = []
        for ws in list(_browser_subs):
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _browser_subs.discard(ws)

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
    ctx = zmq.Context.instance()
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
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}
    finally:
        dealer.close()


async def _send_control(node_name: str, cmd: dict) -> dict:
    """Locate a node by logical name (preferred) or physical host."""
    for cfg in NODES:
        if cfg.get("name") == node_name:
            return await asyncio.to_thread(
                _send_control_sync, cfg["host"], cfg["port"], cmd
            )
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

app = FastAPI(title="Live Trading System", lifespan=_lifespan)


def _build_snapshot() -> Dict[str, Any]:
    return {
        "type": "snapshot",
        "nodes": [n.get("name") or n["host"] for n in NODES],
        "stock_prices": _state.stock_prices,
        "analytics": dict(_state.analytics),
        "positions": dict(_state.positions),
        "last_signals": dict(_state.last_signals),
        "system_events": list(_state.system_events),
        "per_node": dict(_state.per_node),
        "edges": _state.edges(),
        "total_seen": _state.total_seen,
        "messages": _state.snapshot(SNAPSHOT_LIMIT),
        "edge_window_s": EDGE_WINDOW_S,
    }


@app.get("/health")
def health() -> Response:
    return Response("OK\n", media_type="text/plain")


@app.get("/api/state")
def api_state() -> dict:
    return _build_snapshot()


def _probe_node_sync(cfg: Dict[str, Any], timeout_ms: int = 1500) -> Dict[str, Any]:
    """Send a CTL status to *cfg* and report reachability + a snapshot."""
    name = cfg.get("name") or cfg["host"]
    host, port = cfg["host"], cfg["port"]
    started_at = time.time()
    ctx = zmq.Context.instance()
    dealer = ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, f"diag-{os.getpid()}-{name}".encode())
    dealer.setsockopt(zmq.LINGER, 200)
    dealer.setsockopt(zmq.RCVTIMEO, timeout_ms)
    try:
        dealer.connect(f"tcp://{host}:{port}")
        dealer.send_multipart([b"CTL", json.dumps({"action": "status"}).encode()])
        frames = dealer.recv_multipart()
        elapsed_ms = int((time.time() - started_at) * 1000)
        if len(frames) >= 2 and frames[0] == b"CTL":
            payload = json.loads(frames[1])
            return {
                "name": name, "host": host, "port": port,
                "reachable": True,
                "elapsed_ms": elapsed_ms,
                "role": payload.get("role"),
                "uptime_s": payload.get("uptime_s"),
                "total_received": payload.get("total_received"),
                "total_published": payload.get("total_published"),
                "subscribers": payload.get("subscribers"),
                "upstreams_connected": payload.get("upstreams_connected"),
                "upstreams": payload.get("upstreams") or [],
            }
        return {
            "name": name, "host": host, "port": port,
            "reachable": False,
            "elapsed_ms": elapsed_ms,
            "error": "unexpected CTL response",
        }
    except zmq.Again:
        return {
            "name": name, "host": host, "port": port,
            "reachable": False,
            "elapsed_ms": int((time.time() - started_at) * 1000),
            "error": "timeout",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "name": name, "host": host, "port": port,
            "reachable": False,
            "elapsed_ms": int((time.time() - started_at) * 1000),
            "error": str(exc),
        }
    finally:
        dealer.close()


@app.get("/api/diagnostics")
async def api_diagnostics() -> dict:
    """Probe every configured node and report whether the dashboard can reach it.

    Returns reachability for each node plus a summary of the dashboard's
    subscriber-thread receive counters.  Use this when the dashboard shows
    no live data: any node with ``reachable=False`` means either the
    container is not running, the platform DNS does not resolve its name,
    or its ROUTER port is blocked.
    """
    results = await asyncio.gather(
        *(asyncio.to_thread(_probe_node_sync, cfg) for cfg in NODES)
    )
    return {
        "at": time.time(),
        "configured_nodes": NODES,
        "raw_env": os.environ.get("LIVE_TRADING_SYSTEM_NODES", ""),
        "zmq_port_default": ZMQ_PORT,
        "nodes": list(results),
        "dashboard_subscriber_counts": dict(_state.per_node),
        "total_seen": _state.total_seen,
    }


@app.get("/api/stream")
async def api_stream() -> StreamingResponse:
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=512)
    snapshot = json.dumps(_build_snapshot())

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
    await ws.send_text(json.dumps(_build_snapshot()))
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
                await ws.send_text(json.dumps({"type": "control_result", "result": result}))
    except WebSocketDisconnect:
        pass
    finally:
        _browser_subs.discard(ws)
        log.info("browser disconnected (now %d)", len(_browser_subs))


_history_cache: Dict[str, Any] = {"at": 0.0, "data": None}
_HISTORY_TTL_S = float(os.environ.get("HISTORY_TTL_S", "3"))


def _history_summary_sync() -> Dict[str, Any]:
    """Compute the persisted-history summary via DuckDB."""
    try:
        return parquet_io.history_summary()
    except Exception as exc:  # noqa: BLE001
        log.exception("history_summary failed")
        return {"error": str(exc)}


@app.get("/api/history")
async def api_history() -> dict:
    """Return persisted trade + position summary computed via DuckDB."""
    now = time.time()
    if _history_cache["data"] and (now - _history_cache["at"]) < _HISTORY_TTL_S:
        return _history_cache["data"]
    data = await asyncio.to_thread(_history_summary_sync)
    data["computed_at"] = now
    _history_cache["at"] = now
    _history_cache["data"] = data
    return data


@app.post("/api/control")
async def api_control(request: dict) -> dict:
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
<title>Live Trading System</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--panel2:#1c2333;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--green:#3fb950;--red:#f85149;--blue:#58a6ff;--orange:#d29922;--cyan:#39d2c0;--purple:#bc8cff;--yellow:#e3b341}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5}
header{background:var(--panel);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;flex-wrap:wrap;gap:20px;position:sticky;top:0;z-index:10}
header h1{font-size:16px;font-weight:700;letter-spacing:.3px}
header .stat{color:var(--muted);font-size:13px}
header .stat b{color:var(--text);font-weight:600}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);margin-right:6px;vertical-align:middle;transition:background .3s}
.dot.ok{background:var(--green)}
main{padding:18px 24px 40px;max-width:1700px;margin:0 auto}
h2.section{font-size:11px;color:var(--muted);margin:18px 0 8px;font-weight:700;text-transform:uppercase;letter-spacing:.8px}

.controls{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:16px;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px 22px;align-items:center}
.controls .group{display:flex;flex-wrap:wrap;gap:10px;align-items:center;border-right:1px dashed var(--border);padding-right:18px}
.controls .group:last-child{border-right:none}
.controls label{font-size:12px;color:var(--muted);display:flex;align-items:center;gap:8px}
.controls input[type=range]{width:110px;accent-color:var(--blue)}
.controls input[type=text],.controls input[type=number]{background:var(--panel2);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:4px 8px;font-size:12px;width:90px}
.btn{background:var(--panel2);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:5px 12px;font-size:12px;cursor:pointer;font-weight:600;transition:background .15s}
.btn:hover{background:var(--border)}
.btn.primary{background:var(--blue);border-color:var(--blue);color:#fff}
.btn.primary:hover{opacity:.85}
.btn.danger{background:var(--red);border-color:var(--red);color:#fff}
.btn.danger:hover{opacity:.85}
.btn.small{padding:3px 8px;font-size:11px}

.topology{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:10px;margin-bottom:18px}
.topology svg{width:100%;height:380px;display:block}
.topology .legend{font-size:11px;color:var(--muted);padding:6px 4px 0;display:flex;gap:18px;flex-wrap:wrap}
.topology .legend b{color:var(--text);font-weight:600}
.topo-swatch{display:inline-block;width:18px;height:3px;border-radius:2px;vertical-align:middle;margin-right:4px}
.topo-swatch.cold{background:#3a4250}
.topo-swatch.warm{background:var(--blue)}
.topo-swatch.hot{background:var(--green)}
.topo-swatch.dashed{background:linear-gradient(90deg,var(--muted) 60%,transparent 0) 0 0/8px 3px}

/* nodes */
.topo-node-bg{fill:var(--panel2);stroke:var(--border);stroke-width:1.4;transition:stroke .25s,filter .25s}
.topo-node-bg.active{stroke:var(--green)}
.topo-node-bg.dashboard{fill:#162130;stroke:var(--blue)}
.topo-node-bg.workflows{fill:#1d1a2e;stroke:var(--purple);stroke-dasharray:5 3}
.topo-node-label{fill:var(--text);font-size:11px;font-weight:600;text-anchor:middle;font-family:-apple-system,sans-serif;pointer-events:none}
.topo-node-role{fill:var(--muted);font-size:9px;text-anchor:middle;text-transform:uppercase;letter-spacing:.5px;pointer-events:none}
.topo-node-rate{fill:var(--muted);font-size:10px;text-anchor:middle;font-family:ui-monospace,"SF Mono",Menlo,monospace;pointer-events:none}
.topo-node-rate.active{fill:var(--green)}
@keyframes topo-pulse{0%{filter:drop-shadow(0 0 0 rgba(63,185,80,.0));stroke-width:1.4}40%{filter:drop-shadow(0 0 8px rgba(63,185,80,.7));stroke-width:3}100%{filter:drop-shadow(0 0 0 rgba(63,185,80,0));stroke-width:1.4}}
.topo-node.pulse .topo-node-bg{animation:topo-pulse .7s ease-out}

/* edges */
.topo-edge{stroke:#3a4250;stroke-width:1.5;fill:none;opacity:.65;transition:stroke .25s,opacity .25s,stroke-width .25s}
.topo-edge.warm{stroke:var(--blue);opacity:.9;stroke-width:2.1}
.topo-edge.hot{stroke:var(--green);opacity:1;stroke-width:2.4}
.topo-edge.dashed{stroke-dasharray:5 4;opacity:.35}
.topo-edge.dashed.warm{opacity:.55}
.topo-edge.dashed.hot{opacity:.75}
.topo-edge-label{fill:var(--muted);font-size:10.5px;font-family:ui-monospace,"SF Mono",Menlo,monospace;text-anchor:middle;pointer-events:none;font-weight:600}
.topo-edge-label.warm{fill:var(--blue)}
.topo-edge-label.hot{fill:var(--green)}

/* arrow markers via CSS-driven currentColor */
.arrow-host{color:#3a4250}
.arrow-host.warm{color:var(--blue)}
.arrow-host.hot{color:var(--green)}

/* flowing dots along edges */
.edge-dot{fill:var(--blue);opacity:0}
.edge-dot.warm{opacity:.85;fill:var(--blue)}
.edge-dot.hot{opacity:1;fill:var(--green);r:4}

.ticker-ribbon{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}
.ticker-card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:12px 16px;min-width:160px;flex:1}
.ticker-card .sym{font-size:13px;font-weight:700;color:var(--muted);margin-bottom:2px}
.ticker-card .price{font-size:22px;font-weight:700;font-variant-numeric:tabular-nums}
.ticker-card .change{font-size:12px;font-weight:600;margin-top:2px}
.ticker-card .change.up{color:var(--green)}
.ticker-card .change.down{color:var(--red)}
.ticker-card .change.flat{color:var(--muted)}
.ticker-card .meta{font-size:11px;color:var(--muted);margin-top:6px;display:flex;gap:12px}

.sys-ribbon{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:10px 14px;margin-bottom:16px;display:flex;align-items:center;gap:14px;font-size:12px;color:var(--muted);overflow-x:auto;white-space:nowrap}
.sys-event{display:inline-flex;align-items:center;gap:6px;padding:2px 8px;background:var(--panel2);border:1px solid var(--border);border-radius:999px}
.sys-event.market_open{color:var(--green)}
.sys-event.eod_complete{color:var(--yellow)}
.sys-event.custom{color:var(--cyan)}
.sys-event time{color:var(--muted);font-family:ui-monospace,"SF Mono",Menlo,monospace}

.grid-2{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:16px;margin-bottom:16px}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:0;overflow:hidden}
.panel header.phead{position:static;border:0;border-bottom:1px solid var(--border);padding:8px 14px;font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:700;background:transparent}

.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;background:transparent;border:0}
thead th{background:var(--panel2);text-align:left;padding:7px 12px;font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);font-weight:700}
tbody td{padding:6px 12px;border-top:1px solid var(--border);font-size:12px;font-variant-numeric:tabular-nums;vertical-align:middle;font-family:ui-monospace,"SF Mono",Menlo,monospace}
tbody tr:hover{background:var(--panel2)}
.up-col{color:var(--green);font-weight:600}
.down-col{color:var(--red);font-weight:600}
.neutral-col{color:var(--muted)}
.badge{display:inline-block;padding:1px 7px;border-radius:4px;font-size:10px;font-weight:700;font-family:-apple-system,sans-serif}
.badge.tick{background:rgba(88,166,255,.15);color:var(--blue)}
.badge.analytics{background:rgba(188,140,255,.15);color:var(--purple)}
.badge.validated_tick{background:rgba(63,185,80,.15);color:var(--green)}
.badge.rejected{background:rgba(248,81,73,.15);color:var(--red)}
.badge.signal{background:rgba(57,210,192,.15);color:var(--cyan)}
.badge.order_intent{background:rgba(210,153,34,.15);color:var(--orange)}
.badge.fill{background:rgba(63,185,80,.18);color:var(--green)}
.badge.position_update{background:rgba(227,179,65,.15);color:var(--yellow)}
.badge.system{background:rgba(188,140,255,.18);color:var(--purple)}
.text-muted{color:var(--muted)}
@keyframes flash-green{from{background:rgba(63,185,80,.15)}to{background:transparent}}
@keyframes flash-blue{from{background:rgba(88,166,255,.12)}to{background:transparent}}
@keyframes flash-yellow{from{background:rgba(227,179,65,.15)}to{background:transparent}}
tr.new-fill td{animation:flash-green .8s ease-out}
tr.new-signal td{animation:flash-blue .8s ease-out}
tr.new-intent td{animation:flash-yellow .8s ease-out}
.empty-msg{padding:18px;text-align:center;color:var(--muted)}
.kbd{font-family:ui-monospace,"SF Mono",Menlo,monospace;color:var(--muted);font-size:11px}
.hint{background:#10182a;border:1px solid var(--border);border-left:3px solid var(--blue);padding:10px 14px;border-radius:6px;font-size:12px;color:var(--muted);margin-bottom:16px}
.hint code{background:var(--panel2);padding:1px 6px;border-radius:4px;color:var(--blue);font-family:ui-monospace,"SF Mono",Menlo,monospace;font-size:11px}

/* persisted-history tiles */
.hist-summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px}
.hist-tile{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:12px 14px}
.hist-tile .lbl{font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);font-weight:700}
.hist-tile .val{font-size:22px;font-weight:700;font-variant-numeric:tabular-nums;margin-top:4px;font-family:ui-monospace,"SF Mono",Menlo,monospace}

/* control toasts ----- */
#toasts{position:fixed;top:64px;right:20px;display:flex;flex-direction:column;gap:8px;z-index:200;pointer-events:none;max-width:360px}
.toast{background:var(--panel);border:1px solid var(--border);border-left:3px solid var(--blue);border-radius:6px;padding:10px 14px;font-size:12px;color:var(--text);box-shadow:0 6px 18px rgba(0,0,0,.45);pointer-events:auto;opacity:0;transform:translateY(-6px);transition:opacity .18s,transform .18s}
.toast.in{opacity:1;transform:translateY(0)}
.toast.success{border-left-color:var(--green)}
.toast.error{border-left-color:var(--red)}
.toast .t-title{font-weight:700;margin-bottom:2px}
.toast .t-detail{color:var(--muted);font-family:ui-monospace,"SF Mono",Menlo,monospace;font-size:11px;word-break:break-all}

/* flash for new system events */
@keyframes sys-flash{0%{background:rgba(57,210,192,.4)}100%{background:var(--panel2)}}
.sys-event.flash{animation:sys-flash 1.6s ease-out}
.sys-ribbon.flash{box-shadow:0 0 0 2px rgba(57,210,192,.5)}
</style>
</head>
<body>
<div id="toasts"></div>
<header>
  <h1>Live Trading System</h1>
  <span class="stat"><span class="dot" id="ws-dot"></span><span id="ws-text">connecting&hellip;</span></span>
  <span class="stat">Events: <b id="total-count">0</b></span>
  <span class="stat">PnL: <b id="pnl-total">$0.00</b></span>
</header>

<main>
  <h2 class="section">System events</h2>
  <div class="sys-ribbon" id="sys-ribbon"><span class="text-muted">no system events yet&hellip;</span></div>

  <h2 class="section">Controls &mdash; click buttons to drive the live graph</h2>
  <div class="controls" id="controls">
    <div class="group">
      <button class="btn primary" onclick="sendControl('market-feed','pause')">Pause feed</button>
      <button class="btn" onclick="sendControl('market-feed','resume')">Resume feed</button>
      <label>Interval
        <input type="range" min="0.1" max="5" step="0.1" value="1.0"
               oninput="document.getElementById('lbl-interval').textContent=this.value+'s'"
               onchange="sendControl('market-feed','set_interval',{interval:parseFloat(this.value)})">
        <span id="lbl-interval">1.0s</span>
      </label>
      <label>Add symbol
        <input type="text" id="inp-symbol" placeholder="e.g. META" maxlength="6">
        <button class="btn small" onclick="addSymbol()">+</button>
      </label>
    </div>

    <div class="group">
      <label>Strategies
        <button class="btn small" onclick="sendControl('signal-engine','enable_strategy',{strategy:'momentum'})">+mom</button>
        <button class="btn small" onclick="sendControl('signal-engine','disable_strategy',{strategy:'momentum'})">-mom</button>
        <button class="btn small" onclick="sendControl('signal-engine','enable_strategy',{strategy:'mean_reversion'})">+mr</button>
        <button class="btn small" onclick="sendControl('signal-engine','disable_strategy',{strategy:'mean_reversion'})">-mr</button>
      </label>
      <label>Min strength
        <input type="range" min="0" max="1" step="0.05" value="0.1"
               oninput="document.getElementById('lbl-strength').textContent=this.value"
               onchange="sendControl('signal-engine','set_min_strength',{min_strength:parseFloat(this.value)})">
        <span id="lbl-strength">0.1</span>
      </label>
      <label>Cooldown
        <input type="range" min="0.5" max="10" step="0.5" value="1.5"
               oninput="document.getElementById('lbl-cooldown').textContent=this.value+'s'"
               onchange="sendControl('signal-engine','set_cooldown',{cooldown_s:parseFloat(this.value)})">
        <span id="lbl-cooldown">1.5s</span>
      </label>
    </div>

    <div class="group">
      <label>Max position
        <input type="number" id="inp-max-pos" value="1000" min="0" step="100">
      </label>
      <label>Max notional
        <input type="number" id="inp-max-not" value="250000" min="0" step="10000">
      </label>
      <button class="btn small" onclick="applyRiskLimits()">Apply risk limits</button>
      <button class="btn danger small" title="Broadcast a custom system event"
              onclick="broadcastPing()">Broadcast ping</button>
    </div>

    <div class="group" title="Diagnostics &mdash; bypass strategies and force a signal end-to-end through the pipeline">
      <label for="inp-force-sym">Force signal</label>
      <input type="text" id="inp-force-sym" placeholder="AAPL" maxlength="6" value="AAPL">
      <button type="button" class="btn small" data-force-side="buy">Buy</button>
      <button type="button" class="btn small" data-force-side="sell">Sell</button>
      <button type="button" id="btn-diagnostics" class="btn small" style="margin-left:8px"
              title="Probe every configured node and show whether the dashboard can reach it">Diagnostics</button>
    </div>
  </div>

  <div class="hint">
    All trades are produced by the on-line strategies in <code>signal-engine</code>
    consuming market data; there are no manual order buttons. Toggle a strategy
    off above and watch the order flow stop within one cooldown window. The
    <b>Force signal</b> buttons are a diagnostic that bypasses the strategies and
    pushes a synthetic signal into the chain &mdash; use them to confirm
    risk-engine + execution-simulator are wired up.
  </div>

  <h2 class="section">Topology &mdash; live data flow, per-edge msg/s</h2>
  <div class="topology">
    <svg id="topology" viewBox="0 0 1100 420" preserveAspectRatio="xMidYMid meet"></svg>
    <div class="legend">
      <span><b>edges</b>
        <span class="topo-swatch cold"></span>cold
        <span class="topo-swatch warm"></span>warm
        <span class="topo-swatch hot"></span>hot (&ge;5 msg/s)
        <span class="topo-swatch dashed"></span>subscription / CTL</span>
      <span><b>nodes</b> ring lights when emitting; pulses on each event</span>
      <span class="kbd" id="edge-window-label">window = 5s</span>
    </div>
  </div>

  <h2 class="section">Stock ticker</h2>
  <div class="ticker-ribbon" id="ticker"></div>

  <div class="grid-2">
    <div class="panel">
      <header class="phead">Positions &amp; PnL</header>
      <div class="tbl-wrap">
        <table id="pos-table" hidden>
          <thead><tr><th>Symbol</th><th>Net qty</th><th>Avg</th><th>Mkt</th><th>Realised</th><th>Unrealised</th></tr></thead>
          <tbody id="pos-body"></tbody>
        </table>
        <div class="empty-msg" id="pos-empty">No positions yet&hellip;</div>
      </div>
    </div>

    <div class="panel">
      <header class="phead">Analytics</header>
      <div class="tbl-wrap">
        <table id="analytics-table" hidden>
          <thead><tr>
            <th>Sym</th><th>Price</th><th>SMA(s)</th><th>SMA(l)</th>
            <th>VWAP</th><th>Vol</th><th>Trend</th><th>Sess &Delta;</th>
          </tr></thead>
          <tbody id="analytics-body"></tbody>
        </table>
        <div class="empty-msg" id="analytics-empty">Waiting for analytics&hellip;</div>
      </div>
    </div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <header class="phead">Order blotter &mdash; signals, intents &amp; fills</header>
      <div class="tbl-wrap">
        <table id="blotter-table" hidden>
          <thead><tr><th>Time</th><th>Kind</th><th>Sym</th><th>Side</th><th>Qty</th><th>Px</th><th>Detail</th></tr></thead>
          <tbody id="blotter-body"></tbody>
        </table>
        <div class="empty-msg" id="blotter-empty">No orders yet&hellip;</div>
      </div>
    </div>

    <div class="panel">
      <header class="phead">Live feed &mdash; raw events</header>
      <div class="tbl-wrap">
        <table id="feed-table" hidden>
          <thead><tr><th>Time</th><th>Node</th><th>Kind</th><th>Sym</th><th>Px</th><th>Hops</th></tr></thead>
          <tbody id="feed-body"></tbody>
        </table>
        <div class="empty-msg" id="feed-empty">Waiting for messages&hellip;</div>
      </div>
    </div>
  </div>

  <h2 class="section">Persisted history &mdash; DuckDB on Parquet in blob storage</h2>
  <div class="hist-summary" id="hist-summary">
    <div class="hist-tile"><div class="lbl">Trade files (today)</div><div class="val" id="hist-files">&mdash;</div></div>
    <div class="hist-tile"><div class="lbl">Fills persisted</div><div class="val" id="hist-fills">&mdash;</div></div>
    <div class="hist-tile"><div class="lbl">Notional traded</div><div class="val" id="hist-notional">&mdash;</div></div>
    <div class="hist-tile"><div class="lbl">Total slippage cost</div><div class="val" id="hist-slip">&mdash;</div></div>
    <div class="hist-tile"><div class="lbl">Last refresh</div><div class="val" id="hist-when">never</div></div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <header class="phead">By symbol &mdash; today's flow</header>
      <div class="tbl-wrap">
        <table id="hist-sym-table" hidden>
          <thead><tr><th>Symbol</th><th>Net qty</th><th>Total qty</th><th>Fills</th><th>Avg fill price</th></tr></thead>
          <tbody id="hist-sym-body"></tbody>
        </table>
        <div class="empty-msg" id="hist-sym-empty">Waiting for the first flush&hellip;</div>
      </div>
    </div>

    <div class="panel">
      <header class="phead">By strategy</header>
      <div class="tbl-wrap">
        <table id="hist-strat-table" hidden>
          <thead><tr><th>Strategy</th><th>Fills</th><th>Qty</th></tr></thead>
          <tbody id="hist-strat-body"></tbody>
        </table>
        <div class="empty-msg" id="hist-strat-empty">Waiting for the first flush&hellip;</div>
      </div>
    </div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <header class="phead">Recent fills (DuckDB)</header>
      <div class="tbl-wrap">
        <table id="hist-recent-table" hidden>
          <thead><tr><th>Time</th><th>Sym</th><th>Side</th><th>Qty</th><th>Px</th><th>Slip</th><th>Strategy</th></tr></thead>
          <tbody id="hist-recent-body"></tbody>
        </table>
        <div class="empty-msg" id="hist-recent-empty">Waiting&hellip;</div>
      </div>
    </div>

    <div class="panel">
      <header class="phead">Positions (from positions/latest.parquet)</header>
      <div class="tbl-wrap">
        <table id="hist-pos-table" hidden>
          <thead><tr><th>Symbol</th><th>Net qty</th><th>Avg</th><th>Mkt</th><th>Realised</th><th>Unrealised</th></tr></thead>
          <tbody id="hist-pos-body"></tbody>
        </table>
        <div class="empty-msg" id="hist-pos-empty">No positions snapshot yet&hellip;</div>
      </div>
    </div>
  </div>
</main>

<script>
(function(){
  const MAX_FEED=40, MAX_BLOTTER=40;
  const wsDot=document.getElementById('ws-dot'),wsText=document.getElementById('ws-text');
  const totalEl=document.getElementById('total-count'),pnlEl=document.getElementById('pnl-total');
  const tickerEl=document.getElementById('ticker');
  const sysRibbon=document.getElementById('sys-ribbon');
  const svgEl=document.getElementById('topology');
  const abody=document.getElementById('analytics-body'),atable=document.getElementById('analytics-table'),aempty=document.getElementById('analytics-empty');
  const pbody=document.getElementById('pos-body'),ptable=document.getElementById('pos-table'),pempty=document.getElementById('pos-empty');
  const bbody=document.getElementById('blotter-body'),btable=document.getElementById('blotter-table'),bempty=document.getElementById('blotter-empty');
  const fbody=document.getElementById('feed-body'),ftable=document.getElementById('feed-table'),fempty=document.getElementById('feed-empty');

  /* ---- local state mirror ---- */
  const state={
    stockPrices:{}, analytics:{}, positions:{}, lastSignals:{},
    sysEvents:[], nodes:[], total:0, edgeWindowS:5,
  };

  function fmt(n,d){return n!=null && !isNaN(n) ? Number(n).toFixed(d!=null?d:2) : '\u2014';}
  function fmtTime(epoch){
    if(!epoch) return '';
    const d=new Date(epoch*1000);
    return [d.getHours(),d.getMinutes(),d.getSeconds()].map(v=>String(v).padStart(2,'0')).join(':');
  }
  function chgClass(v){return v>0?'up':v<0?'down':'flat';}
  function trendClass(t){return t==='up'?'up-col':t==='down'?'down-col':'neutral-col';}

  /* ---- topology: static skeleton + live rate overlays ----------------- */
  const NODES={
    'market-feed':       {x:100, y:95,  r:38, role:'feed'},
    'analytics-engine':  {x:285, y:95,  r:38, role:'analytics'},
    'signal-engine':     {x:470, y:95,  r:38, role:'signals'},
    'risk-engine':       {x:655, y:95,  r:38, role:'risk'},
    'execution-simulator':{x:845,y:95,  r:38, role:'execution'},
    'notification-bus':  {x:470, y:240, r:34, role:'bus'},
    'dashboard':         {x:190, y:355, rx:120, ry:28, role:'this app', shape:'ellipse'},
    'workflows':         {x:780, y:355, rx:108, ry:28, role:'tasks',     shape:'ellipse'},
  };
  // (src, dst, curve, dashed). curve: 0 = straight; +N = bulge upward; -N downward.
  const EDGES=[
    // pipeline
    {from:'market-feed',         to:'analytics-engine',    curve:0,    label:'tick'},
    {from:'analytics-engine',    to:'signal-engine',       curve:0,    label:'analytics'},
    {from:'signal-engine',       to:'risk-engine',         curve:0,    label:'signal'},
    {from:'risk-engine',         to:'execution-simulator', curve:0,    label:'order_intent'},
    // feedback / side-channels
    {from:'execution-simulator', to:'risk-engine',         curve:90,   label:'fill (feedback)'},
    {from:'market-feed',         to:'execution-simulator', curve:-95,  label:'tick (last-price)'},
    // dashboard subscriptions
    {from:'market-feed',         to:'dashboard', dashed:true},
    {from:'analytics-engine',    to:'dashboard', dashed:true},
    {from:'signal-engine',       to:'dashboard', dashed:true},
    {from:'risk-engine',         to:'dashboard', dashed:true},
    {from:'execution-simulator', to:'dashboard', dashed:true},
    {from:'notification-bus',    to:'dashboard', dashed:true, label:'system events'},
    // workflows talk to the bus via CTL broadcast
    {from:'workflows',           to:'notification-bus', dashed:true, label:'CTL broadcast'},
  ];

  function edgeKey(src,dst){return src+'||'+dst;}
  function rateClass(r){if(r>=5)return 'hot'; if(r>=1)return 'warm'; return '';}

  // Compute endpoints on node boundary along a vector
  function attach(node, tx, ty){
    if(node.shape==='ellipse'){
      const dx=tx-node.x, dy=ty-node.y;
      const k=1/Math.sqrt((dx*dx)/(node.rx*node.rx)+(dy*dy)/(node.ry*node.ry));
      return {x:node.x+dx*k, y:node.y+dy*k};
    }
    const dx=tx-node.x, dy=ty-node.y;
    const len=Math.sqrt(dx*dx+dy*dy)||1;
    return {x:node.x+dx*node.r/len, y:node.y+dy*node.r/len};
  }

  function edgePath(e){
    const a=NODES[e.from], b=NODES[e.to];
    if(!a || !b) return '';
    if(!e.curve){
      const p1=attach(a, b.x, b.y);
      const p2=attach(b, a.x, a.y);
      return `M${p1.x},${p1.y} L${p2.x},${p2.y}`;
    }
    const midX=(a.x+b.x)/2;
    const midY=(a.y+b.y)/2 - e.curve;
    const p1=attach(a, midX, midY);
    const p2=attach(b, midX, midY);
    return `M${p1.x},${p1.y} Q${midX},${midY} ${p2.x},${p2.y}`;
  }

  // Midpoint label position (for labels above the edge midpoint)
  function labelPos(e){
    const a=NODES[e.from], b=NODES[e.to];
    if(!a || !b) return {x:0,y:0};
    const midX=(a.x+b.x)/2;
    if(!e.curve){
      return {x:midX, y:(a.y+b.y)/2 - 8};
    }
    // place label on the bulged side
    const baseY=(a.y+b.y)/2;
    return {x:midX, y:baseY - e.curve*0.55 - 4};
  }

  // ------ build the SVG once at load --------
  const SVG_NS='http://www.w3.org/2000/svg';
  function ce(tag, attrs={}, children=[]){
    const el=document.createElementNS(SVG_NS, tag);
    for(const k in attrs){ if(attrs[k]!=null) el.setAttribute(k, attrs[k]); }
    children.forEach(c=>el.appendChild(c));
    return el;
  }

  const edgeEls=new Map();   // key -> {path, label, dot, arrowHost}
  const nodeEls=new Map();   // name -> {group, bg, label, role, rate}

  function buildTopology(){
    svgEl.innerHTML='';
    // defs: per-rate arrow markers
    const defs=ce('defs');
    [['arrow-cold','#3a4250'],['arrow-warm','#58a6ff'],['arrow-hot','#3fb950']].forEach(([id,color])=>{
      const m=ce('marker',{id,viewBox:'0 0 10 10',refX:'9',refY:'5',markerWidth:'6',markerHeight:'6',orient:'auto-start-reverse'});
      m.appendChild(ce('path',{d:'M0,0 L10,5 L0,10 Z', fill:color}));
      defs.appendChild(m);
    });
    svgEl.appendChild(defs);

    const edgesG=ce('g',{id:'edges'});
    const dotsG=ce('g',{id:'dots'});
    const labelsG=ce('g',{id:'edge-labels'});
    const nodesG=ce('g',{id:'nodes'});
    svgEl.appendChild(edgesG);
    svgEl.appendChild(dotsG);
    svgEl.appendChild(labelsG);
    svgEl.appendChild(nodesG);

    // Edges
    EDGES.forEach((e, i)=>{
      const id='edge-'+i;
      const cls='topo-edge'+(e.dashed?' dashed':'');
      const path=ce('path',{id, class:cls, d:edgePath(e),
                            'marker-end':'url(#arrow-cold)'});
      edgesG.appendChild(path);

      // flowing dot (only one per edge; opacity 0 by default)
      const dot=ce('circle',{class:'edge-dot', r:3.5});
      const animate=ce('animateMotion',{dur:'2.4s', repeatCount:'indefinite', rotate:'auto'});
      const mpath=ce('mpath');
      mpath.setAttributeNS('http://www.w3.org/1999/xlink','href','#'+id);
      animate.appendChild(mpath);
      dot.appendChild(animate);
      dotsG.appendChild(dot);

      // label (rate)
      const lp=labelPos(e);
      const label=ce('text',{class:'topo-edge-label', x:lp.x, y:lp.y});
      label.textContent='';
      labelsG.appendChild(label);

      edgeEls.set(edgeKey(e.from, e.to), {path, dot, animate, label, def:e});
    });

    // Nodes
    Object.keys(NODES).forEach(name=>{
      const n=NODES[name];
      const g=ce('g',{class:'topo-node', 'data-node':name});
      let bg;
      if(n.shape==='ellipse'){
        bg=ce('ellipse',{class:'topo-node-bg '+name, cx:n.x, cy:n.y, rx:n.rx, ry:n.ry});
      } else {
        bg=ce('circle',{class:'topo-node-bg '+(name==='notification-bus'?'bus':''), cx:n.x, cy:n.y, r:n.r});
      }
      g.appendChild(bg);
      const cy=n.y;
      const lbl=ce('text',{class:'topo-node-label', x:n.x, y:cy-2});
      lbl.textContent=name;
      const role=ce('text',{class:'topo-node-role', x:n.x, y:cy+12});
      role.textContent=n.role;
      const rate=ce('text',{class:'topo-node-rate', x:n.x, y:cy + (n.shape==='ellipse'?n.ry+14:n.r+14)});
      rate.textContent='0/s';
      g.appendChild(lbl); g.appendChild(role); g.appendChild(rate);
      nodesG.appendChild(g);
      nodeEls.set(name, {group:g, bg, label:lbl, role, rate});
    });
  }

  // ----- sliding-window counters -----
  const nodeWin=new Map();   // name -> [ts...]
  const edgeWin=new Map();   // key  -> [ts...]
  const WINDOW_S=()=>state.edgeWindowS||5;

  function bumpEdgeWin(src,dst){
    const k=edgeKey(src,dst);
    if(!edgeWin.has(k)) edgeWin.set(k,[]);
    edgeWin.get(k).push(performance.now()/1000);
  }
  function bumpNodeWin(name){
    if(!nodeWin.has(name)) nodeWin.set(name,[]);
    nodeWin.get(name).push(performance.now()/1000);
  }
  function decay(){
    const now=performance.now()/1000;
    const win=WINDOW_S();
    nodeWin.forEach(arr=>{while(arr.length && (now-arr[0])>win) arr.shift();});
    edgeWin.forEach(arr=>{while(arr.length && (now-arr[0])>win) arr.shift();});
  }
  function nodeRate(name){
    const a=nodeWin.get(name); if(!a) return 0;
    return a.length/WINDOW_S();
  }
  function edgeRate(src,dst){
    const a=edgeWin.get(edgeKey(src,dst)); if(!a) return 0;
    return a.length/WINDOW_S();
  }

  // ----- per-event pulse trigger -----
  const pulseQueue=new Map();
  function schedulePulse(name){
    const els=nodeEls.get(name); if(!els) return;
    if(pulseQueue.has(name)) return;
    const g=els.group;
    g.classList.add('pulse');
    pulseQueue.set(name, true);
    setTimeout(()=>{
      g.classList.remove('pulse');
      pulseQueue.delete(name);
    }, 700);
  }

  // ----- per-tick render of edge/node visuals -----
  function updateTopologyVisuals(){
    decay();
    const now=performance.now()/1000;
    // edges
    EDGES.forEach(e=>{
      const els=edgeEls.get(edgeKey(e.from, e.to));
      if(!els) return;
      const r=edgeRate(e.from, e.to);
      const cls=rateClass(r);
      const baseClass='topo-edge'+(e.dashed?' dashed':'')+(cls?' '+cls:'');
      if(els.path.getAttribute('class')!==baseClass) els.path.setAttribute('class', baseClass);
      const marker='url(#arrow-'+(cls||'cold')+')';
      if(els.path.getAttribute('marker-end')!==marker) els.path.setAttribute('marker-end', marker);
      const labelText = r>0 ? (r>=10?r.toFixed(0):r.toFixed(1))+'/s' : '';
      if(els.label.textContent!==labelText) els.label.textContent=labelText;
      const lblCls='topo-edge-label'+(cls?' '+cls:'');
      if(els.label.getAttribute('class')!==lblCls) els.label.setAttribute('class', lblCls);

      // dot visibility + speed
      const dotCls = 'edge-dot'+(cls?' '+cls:'');
      if(els.dot.getAttribute('class')!==dotCls) els.dot.setAttribute('class', dotCls);
      // adjust speed: faster when hotter
      const dur = r>=5 ? '1.0s' : (r>=1 ? '1.7s' : '2.4s');
      if(els.animate.getAttribute('dur')!==dur) els.animate.setAttribute('dur', dur);
    });

    // nodes
    nodeEls.forEach((els, name)=>{
      const r=nodeRate(name);
      const active=r>0;
      const cls='topo-node-bg'+(active?' active':'') + (name==='dashboard'?' dashboard':'') + (name==='workflows'?' workflows':'');
      if(els.bg.getAttribute('class')!==cls) els.bg.setAttribute('class', cls);
      const rateText = r>0 ? (r>=10?r.toFixed(0):r.toFixed(1))+' msg/s' : 'idle';
      if(els.rate.textContent!==rateText) els.rate.textContent=rateText;
      const rateCls='topo-node-rate'+(active?' active':'');
      if(els.rate.getAttribute('class')!==rateCls) els.rate.setAttribute('class', rateCls);
    });
  }

  function applyHopsForTopology(hops, from_node, kind){
    // 1. consecutive hops -> edges (multi-stage lineage)
    for(let i=0;i<hops.length-1;i++) bumpEdgeWin(hops[i], hops[i+1]);
    // 2. node activity for every hop in the trail
    hops.forEach(h=>bumpNodeWin(h));
    // 3. dashboard always consumes -> implicit edge from emitter -> dashboard
    if(from_node) bumpEdgeWin(from_node, 'dashboard');
    bumpNodeWin('dashboard');
    // 4. trigger pulse on the most recent emitter
    if(from_node) schedulePulse(from_node);
    // 5. system events imply a workflow producer -> bus connection
    if(kind==='system'){
      bumpEdgeWin('workflows', 'notification-bus');
      bumpNodeWin('workflows');
      schedulePulse('workflows');
    }
  }

  /* ---- renderers ---- */
  function renderTicker(){
    const syms=Object.keys(state.stockPrices).sort();
    tickerEl.innerHTML=syms.map(s=>{
      const d=state.stockPrices[s];const cc=chgClass(d.change_pct);
      const vol=d.volume!=null?Number(d.volume).toLocaleString():'\u2014';
      return `<div class="ticker-card"><div class="sym">${s}</div>
        <div class="price">$${fmt(d.price)}</div>
        <div class="change ${cc}">${d.change_pct>=0?'+':''}${fmt(d.change_pct,3)}%&ensp;(${d.change>=0?'+':''}${fmt(d.change)})</div>
        <div class="meta"><span>Bid ${fmt(d.bid)}</span><span>Ask ${fmt(d.ask)}</span><span>Vol ${vol}</span></div></div>`;
    }).join('');
  }

  function renderAnalytics(){
    const syms=Object.keys(state.analytics).sort();
    if(!syms.length){atable.hidden=true;aempty.hidden=false;return;}
    atable.hidden=false;aempty.hidden=true;
    abody.innerHTML=syms.map(s=>{
      const a=state.analytics[s];const tc=trendClass(a.trend);
      return `<tr><td><b>${s}</b></td><td>${fmt(a.price)}</td><td>${fmt(a.sma_short)}</td><td>${fmt(a.sma_long)}</td>
        <td>${fmt(a.vwap)}</td><td>${fmt(a.volatility,4)}</td>
        <td class="${tc}">${(a.trend||'').toUpperCase()}</td>
        <td class="${chgClass(a.session_change_pct)==='up'?'up-col':'down-col'}">${a.session_change_pct>=0?'+':''}${fmt(a.session_change_pct)}%</td></tr>`;
    }).join('');
  }

  function renderPositions(){
    const syms=Object.keys(state.positions).sort();
    if(!syms.length){ptable.hidden=true;pempty.hidden=false;pnlEl.textContent='$0.00';return;}
    ptable.hidden=false;pempty.hidden=true;
    let totalPnl=0;
    pbody.innerHTML=syms.map(s=>{
      const p=state.positions[s];
      const pnl=(p.realised_pnl||0)+(p.unrealised_pnl||0);
      totalPnl+=pnl;
      const rc=p.realised_pnl>=0?'up-col':'down-col';
      const uc=p.unrealised_pnl>=0?'up-col':'down-col';
      return `<tr><td><b>${s}</b></td><td>${p.net_qty}</td><td>${fmt(p.avg_price)}</td><td>${fmt(p.market_price)}</td>
        <td class="${rc}">${fmt(p.realised_pnl)}</td><td class="${uc}">${fmt(p.unrealised_pnl)}</td></tr>`;
    }).join('');
    pnlEl.textContent='$'+fmt(totalPnl);
    pnlEl.className=totalPnl>=0?'':'down-col';
  }

  function renderSystemEvents(opts){
    if(!state.sysEvents.length){sysRibbon.innerHTML='<span class="text-muted">no system events yet&hellip;</span>';return;}
    const items=state.sysEvents.slice(-20).reverse();
    const flashIdx = (opts && opts.flashLatest) ? 0 : -1;
    sysRibbon.innerHTML=items.map((e,i)=>{
      const kind=e.kind||'custom';
      const fcls = i===flashIdx ? ' flash' : '';
      return `<span class="sys-event ${kind}${fcls}"><b>${kind}</b> &middot; ${(e.summary||'').slice(0,80)} ` +
             `<time>${fmtTime(e.at)}</time></span>`;
    }).join('');
    if(opts && opts.flashLatest){
      sysRibbon.classList.remove('flash'); void sysRibbon.offsetWidth;
      sysRibbon.classList.add('flash');
      setTimeout(()=>sysRibbon.classList.remove('flash'), 1700);
    }
  }

  function addBlotterRow(m){
    btable.hidden=false;bempty.hidden=true;
    const d=m.data||{};
    const tr=document.createElement('tr');
    let cls='', detail='';
    if(m.kind==='signal'){cls='new-signal';detail=`${d.strategy||''} str=${fmt(d.strength,2)}`;}
    else if(m.kind==='order_intent'){cls='new-intent';detail=`${d.status||''} reason=${d.reason||''}`;}
    else if(m.kind==='fill'){cls='new-fill';detail=`slip=${fmt(d.slippage,3)} strat=${d.strategy||''}`;}
    else if(m.kind==='rejected'){detail=`reason=${d.reason||''}`;}
    tr.className=cls;
    tr.innerHTML=`<td class="text-muted">${fmtTime(m.at||m.timestamp)}</td>
      <td><span class="badge ${m.kind||''}">${m.kind||''}</span></td>
      <td><b>${d.symbol||''}</b></td><td>${d.side||''}</td><td>${d.qty||''}</td><td>${d.price!=null?fmt(d.price):''}</td>
      <td class="text-muted">${detail}</td>`;
    bbody.insertBefore(tr,bbody.firstChild);
    while(bbody.children.length>MAX_BLOTTER) bbody.removeChild(bbody.lastChild);
  }

  function addFeedRow(m){
    ftable.hidden=false;fempty.hidden=true;
    const d=m.data||{};
    const tr=document.createElement('tr');
    const sym=d.symbol||'';
    const px=d.price!=null?'$'+fmt(d.price):'';
    tr.innerHTML=`<td class="text-muted">${fmtTime(m.at||m.timestamp)}</td>
      <td>${m.from_node||m.node||''}</td>
      <td><span class="badge ${m.kind||''}">${m.kind||''}</span></td>
      <td><b>${sym}</b></td><td>${px}</td>
      <td class="text-muted">${(m.hops||[]).join('->')}</td>`;
    fbody.insertBefore(tr,fbody.firstChild);
    while(fbody.children.length>MAX_FEED) fbody.removeChild(fbody.lastChild);
  }

  function seedFromSnapshot(p){
    // Use recent messages to bootstrap node/edge windows so the topology
    // shows realistic rates immediately on first connect.
    nodeWin.clear(); edgeWin.clear();
    (p.messages||[]).forEach(m=>{
      const hops=m.hops||[]; const from=m.from_node||m.node||'';
      applyHopsForTopology(hops, from, m.kind);
    });
  }

  function applySnapshot(p){
    state.stockPrices=p.stock_prices||{}; state.analytics=p.analytics||{};
    state.positions=p.positions||{}; state.lastSignals=p.last_signals||{};
    state.sysEvents=p.system_events||[]; state.nodes=p.nodes||[];
    state.total=p.total_seen||0; state.edgeWindowS=p.edge_window_s||5;
    document.getElementById('edge-window-label').textContent='window = '+state.edgeWindowS+'s';
    seedFromSnapshot(p);
    totalEl.textContent=state.total;
    renderTicker(); renderAnalytics(); renderPositions(); renderSystemEvents();
    bbody.innerHTML=''; fbody.innerHTML='';
    (p.messages||[]).forEach(m=>{
      if(['signal','order_intent','fill','rejected'].includes(m.kind)) addBlotterRow(m);
      addFeedRow(m);
    });
    updateTopologyVisuals();
  }

  function applyEvent(p){
    state.total++; totalEl.textContent=state.total;
    const node=p.from_node||p.node||'';
    const hops=p.hops||[];
    applyHopsForTopology(hops, node, p.kind);

    const d=p.data||{};
    if(p.kind==='tick' && d.symbol) state.stockPrices[d.symbol]=d;
    else if(p.kind==='analytics' && d.symbol) state.analytics[d.symbol]=d;
    else if(p.kind==='position_update' && d.symbol) state.positions[d.symbol]=d;
    else if(p.kind==='signal' && d.symbol) state.lastSignals[d.symbol]={...d, at:p.at};
    else if(p.kind==='system') state.sysEvents.push({...d, at:p.at});

    renderTicker(); renderAnalytics(); renderPositions();
    if(['signal','order_intent','fill','rejected'].includes(p.kind)) addBlotterRow(p);
    if(p.kind==='system') renderSystemEvents({flashLatest:true});
    addFeedRow(p);
  }

  /* ---- SSE ---- */
  const base=location.pathname.replace(/\/+$/,'');
  function connectSSE(){
    const es=new EventSource(`${base}/api/stream`);
    es.onopen=()=>{wsDot.classList.add('ok');wsText.textContent='connected (live)';};
    es.onerror=()=>{wsDot.classList.remove('ok');wsText.textContent='reconnecting\u2026';
      es.close(); setTimeout(connectSSE,3000);};
    es.onmessage=(ev)=>{
      let p; try{p=JSON.parse(ev.data);}catch(e){return;}
      if(p.type==='snapshot') applySnapshot(p); else applyEvent(p);
    };
  }
  connectSSE();

  /* ---- topology refresh: rebuild the SVG once, then update visuals -- */
  buildTopology();
  updateTopologyVisuals();
  setInterval(updateTopologyVisuals, 400);

  /* ---- toasts ---- */
  const toastsEl=document.getElementById('toasts');
  function toast(title, detail, kind){
    const el=document.createElement('div');
    el.className='toast '+(kind||'success');
    const safe=(s)=>String(s==null?'':s).replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
    el.innerHTML='<div class="t-title">'+safe(title)+'</div>'+
                 (detail?'<div class="t-detail">'+safe(detail)+'</div>':'');
    toastsEl.appendChild(el);
    requestAnimationFrame(()=>el.classList.add('in'));
    setTimeout(()=>{el.classList.remove('in');setTimeout(()=>el.remove(),250);}, 3200);
  }

  /* ---- controls ---- */
  function summariseResult(action, result){
    if(!result || result.ok===false){
      return 'failed: '+((result&&result.error)||'no response');
    }
    const interesting=['tick_interval_s','paused','symbols','min_strength','cooldown_s',
                       'enabled_strategies','max_position','max_notional','subscribers',
                       'topic','kind','side','qty','strategy','reference_price'];
    const pairs=[];
    for(const k of interesting){
      if(k in result){
        let v=result[k];
        if(Array.isArray(v)) v=v.join(',');
        else if(typeof v==='object') v=JSON.stringify(v);
        pairs.push(k+'='+v);
        if(pairs.length>=3) break;
      }
    }
    return pairs.length ? pairs.join(' ') : 'ok';
  }
  window.sendControl=async function(node,action,params){
    let res;
    try{
      const r=await fetch(`${base}/api/control`,{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({node:node,action:action,params:params||{}}),
      });
      res=(await r.json()).result;
    }catch(e){
      toast(node+'.'+action, 'network error: '+e, 'error');
      return {ok:false, error:String(e)};
    }
    const ok=res && res.ok!==false;
    toast(node+' \u2192 '+action, summariseResult(action,res), ok?'success':'error');
    return res;
  };
  window.addSymbol=function(){
    const inp=document.getElementById('inp-symbol');
    const sym=inp.value.trim().toUpperCase();
    if(sym){
      sendControl('market-feed','add_symbol',{symbol:sym});
      inp.value='';
    }
  };
  window.applyRiskLimits=function(){
    const pos=parseInt(document.getElementById('inp-max-pos').value, 10);
    const not=parseFloat(document.getElementById('inp-max-not').value);
    sendControl('risk-engine','set_limits',{max_position:pos,max_notional:not});
  };
  window.broadcastPing=function(){
    sendControl('notification-bus','broadcast',{
      topic:'system.custom', kind:'system',
      system:{kind:'custom', summary:'manual ping from dashboard', source:'dashboard'},
    });
  };
  let _forceBusy=false;
  window.forceSignal=function(side){
    if(_forceBusy) return;
    _forceBusy=true;
    setTimeout(()=>{_forceBusy=false;}, 500);
    const inp=document.getElementById('inp-force-sym');
    const sym=(inp.value||'AAPL').trim().toUpperCase();
    const tick=(state.stockPrices||{})[sym];
    const ref=tick && tick.price ? Number(tick.price) : 100.0;
    sendControl('signal-engine','force_signal',{
      symbol:sym, side:side, strategy:'manual',
      strength:0.99, reference_price:ref,
    });
  };
  document.querySelectorAll('button[data-force-side]').forEach(btn=>{
    btn.addEventListener('click', ev => {
      ev.preventDefault();
      ev.stopPropagation();
      forceSignal(btn.dataset.forceSide);
    });
  });

  function fmtDiagRow(n){
    const ok = n.reachable;
    const icon = ok ? '\u2713' : '\u2717';
    if(!ok){
      return `${icon} ${n.name||n.host}  (${n.host}:${n.port})  UNREACHABLE  `
        + `error=${n.error||'?'} (${n.elapsed_ms}ms)`;
    }
    const head = `${icon} ${n.name||n.host}  (${n.host}:${n.port})  role=${n.role||'?'} `
      + `uptime=${n.uptime_s||0}s  rx=${n.total_received||0}  tx=${n.total_published||0}  `
      + `subs=${n.subscribers!=null?n.subscribers:'-'}  ups=${n.upstreams_connected!=null?n.upstreams_connected:'-'}  `
      + `(${n.elapsed_ms}ms)`;
    const ups=(n.upstreams||[]);
    if(!ups.length) return head;
    const upLines = ups.map(u=>{
      const stuck = (u.silent_for_s||0) > 15 ? ' STUCK' : '';
      return `        - ${u.host}:${u.port}  rx=${u.events_received||0}  `
        + `silent=${(u.silent_for_s||0).toFixed?u.silent_for_s.toFixed(1):u.silent_for_s}s  `
        + `recreations=${u.recreations||0}${stuck}`;
    });
    return head + '\n' + upLines.join('\n');
  }
  const diagBtn=document.getElementById('btn-diagnostics');
  if(diagBtn){
    diagBtn.addEventListener('click', async ev=>{
      ev.preventDefault();
      ev.stopPropagation();
      diagBtn.disabled=true; diagBtn.textContent='Probing\u2026';
      try{
        const r=await fetch(`${base}/api/diagnostics`);
        if(!r.ok){
          toast('diagnostics','HTTP '+r.status,'error');
          return;
        }
        const j=await r.json();
        const lines=(j.nodes||[]).map(fmtDiagRow);
        const env = j.raw_env || '(unset)';
        const summary = `LIVE_TRADING_SYSTEM_NODES = ${env}\n`
          + `ZMQ_PORT default = ${j.zmq_port_default}\n`
          + `dashboard total_seen = ${j.total_seen}\n\n`
          + lines.join('\n');
        console.log('diagnostics', j);
        toast('diagnostics', (j.nodes||[]).filter(n=>n.reachable).length + '/' + (j.nodes||[]).length + ' nodes reachable', 'success');
        alert(summary);
      }catch(e){
        toast('diagnostics','network error: '+e,'error');
      }finally{
        diagBtn.disabled=false; diagBtn.textContent='Diagnostics';
      }
    });
  }

  document.getElementById('inp-symbol').addEventListener('keydown',e=>{
    if(e.key==='Enter')addSymbol();
  });

  /* ---- persisted history (DuckDB on Parquet) ---- */
  const histFiles=document.getElementById('hist-files');
  const histFills=document.getElementById('hist-fills');
  const histNotional=document.getElementById('hist-notional');
  const histSlip=document.getElementById('hist-slip');
  const histWhen=document.getElementById('hist-when');
  const hsymBody=document.getElementById('hist-sym-body'),hsymTbl=document.getElementById('hist-sym-table'),hsymEmpty=document.getElementById('hist-sym-empty');
  const hstratBody=document.getElementById('hist-strat-body'),hstratTbl=document.getElementById('hist-strat-table'),hstratEmpty=document.getElementById('hist-strat-empty');
  const hrecBody=document.getElementById('hist-recent-body'),hrecTbl=document.getElementById('hist-recent-table'),hrecEmpty=document.getElementById('hist-recent-empty');
  const hposBody=document.getElementById('hist-pos-body'),hposTbl=document.getElementById('hist-pos-table'),hposEmpty=document.getElementById('hist-pos-empty');

  function renderHistory(h){
    if(!h){return;}
    const totals=h.totals||{};
    const tradesFiles=h.trades_files||0;
    const tradesDl=h.trades_downloaded!=null?h.trades_downloaded:tradesFiles;
    const posFiles=h.positions_files||0;
    histFiles.textContent=tradesFiles.toLocaleString();
    histFills.textContent=(totals.fill_count||0).toLocaleString();
    histNotional.textContent='$'+Math.round(totals.notional||0).toLocaleString();
    histSlip.textContent='$'+fmt(totals.slippage_cost||0);
    if(h.error){
      histWhen.textContent='error  \u00b7  hover for details';
      histWhen.title=`error: ${h.error}\n\nbackend: ${h.blob_backend||'?'}`
        + `\ntrades_prefix: ${h.trades_prefix||'?'}`
        + `\nfiles found: ${tradesFiles}  downloaded: ${tradesDl}`
        + `\npositions found: ${posFiles}`;
      console.warn('history error', h);
    } else if (tradesFiles > tradesDl) {
      histWhen.textContent=`${tradesDl}/${tradesFiles} files OK \u00b7 ${fmtTime(h.computed_at)}`;
      histWhen.title=`${tradesFiles - tradesDl} file(s) failed to download.\n`
        + (h.trades_download_errors||[]).map(e=>`  ${e.path}: ${e.error}`).join('\n');
      console.warn('history partial download', h);
    } else if (tradesFiles === 0 && posFiles === 0) {
      histWhen.textContent=`no parquet yet \u00b7 ${fmtTime(h.computed_at)}`;
      histWhen.title=`backend=${h.blob_backend||'?'}\ntrades_prefix=${h.trades_prefix||'?'}\npositions_prefix=${h.positions_prefix||'?'}`;
    } else {
      histWhen.textContent=fmtTime(h.computed_at);
      histWhen.title=`backend=${h.blob_backend||'?'} \u00b7 trades=${tradesDl}/${tradesFiles} positions=${posFiles}`;
    }

    const bs=h.by_symbol||[];
    if(bs.length){
      hsymTbl.hidden=false;hsymEmpty.hidden=true;
      hsymBody.innerHTML=bs.map(r=>{
        const nc=r.net_qty>0?'up-col':r.net_qty<0?'down-col':'neutral-col';
        return `<tr><td><b>${r.symbol}</b></td><td class="${nc}">${r.net_qty}</td>
                <td>${r.total_qty}</td><td>${r.fills}</td><td>${fmt(r.avg_price)}</td></tr>`;
      }).join('');
    } else { hsymTbl.hidden=true; hsymEmpty.hidden=false; }

    const bst=h.by_strategy||[];
    if(bst.length){
      hstratTbl.hidden=false;hstratEmpty.hidden=true;
      hstratBody.innerHTML=bst.map(r=>
        `<tr><td>${r.strategy}</td><td>${r.fills}</td><td>${r.qty}</td></tr>`).join('');
    } else { hstratTbl.hidden=true; hstratEmpty.hidden=false; }

    const rec=h.recent||[];
    if(rec.length){
      hrecTbl.hidden=false;hrecEmpty.hidden=true;
      hrecBody.innerHTML=rec.map(r=>{
        const sc=r.side==='buy'?'up-col':'down-col';
        return `<tr><td class="text-muted">${fmtTime(r.ts)}</td>
                <td><b>${r.symbol}</b></td>
                <td class="${sc}">${r.side}</td>
                <td>${r.qty}</td><td>${fmt(r.price)}</td>
                <td>${fmt(r.slippage,3)}</td>
                <td class="text-muted">${r.strategy}</td></tr>`;
      }).join('');
    } else { hrecTbl.hidden=true; hrecEmpty.hidden=false; }

    const pos=h.positions||[];
    if(pos.length){
      hposTbl.hidden=false;hposEmpty.hidden=true;
      hposBody.innerHTML=pos.map(r=>{
        const rc=r.realised_pnl>=0?'up-col':'down-col';
        const uc=r.unrealised_pnl>=0?'up-col':'down-col';
        return `<tr><td><b>${r.symbol}</b></td><td>${r.net_qty}</td>
                <td>${fmt(r.avg_price)}</td><td>${fmt(r.market_price)}</td>
                <td class="${rc}">${fmt(r.realised_pnl)}</td>
                <td class="${uc}">${fmt(r.unrealised_pnl)}</td></tr>`;
      }).join('');
    } else { hposTbl.hidden=true; hposEmpty.hidden=false; }
  }

  let _historyInFlight=false;
  async function refreshHistory(){
    if(_historyInFlight) return;
    _historyInFlight=true;
    try{
      const r=await fetch(`${base}/api/history`);
      if(!r.ok) return;
      const h=await r.json();
      renderHistory(h);
    }catch(e){console.warn('history fetch failed',e);}
    finally{_historyInFlight=false;}
  }
  refreshHistory();
  setInterval(refreshHistory, 5000);
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
