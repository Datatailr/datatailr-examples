"""Reactive-graph node service for live stock-exchange data.

A pure ZMQ service that can operate in one of two roles:

* **market-feed** -- simulates a stock exchange, broadcasting
  ``GraphMessage(kind="tick")`` frames to all subscribed peers.
* **analytics** -- subscribes to an upstream market-feed, validates
  each tick, computes rolling analytics (SMA, VWAP, volatility, …)
  and broadcasts them as ``kind="analytics"`` to its own subscribers.

Transport
---------
Each node binds a single **ZMQ ROUTER** socket on PORT (the
platform-assigned port, typically 8080).  Downstream peers (the
analytics engine, the dashboard) connect with DEALER sockets.

A simple three-command protocol runs over ROUTER/DEALER:

  +-----------+-----------------------------------+---------------------+
  | Direction | Frames                            | Meaning             |
  +-----------+-----------------------------------+---------------------+
  | DEALER→R  | [b"SUB"]                          | register subscriber |
  | R→DEALER  | [b"EVT", topic, protobuf_payload] | event broadcast     |
  | DEALER→R  | [b"CTL", json_bytes]              | control command     |
  | R→DEALER  | [b"CTL", json_bytes]              | control reply       |
  +-----------+-----------------------------------+---------------------+

Peers that send only ``CTL`` (never ``SUB``) do **not** receive
``EVT`` frames, so short-lived control connections stay clean.

Configuration (environment variables)
--------------------------------------
NODE_NAME               logical name (default ``market-feed``)
NODE_ROLE               ``market-feed`` | ``analytics``
UPSTREAM_NODES          comma-separated hostnames to subscribe to
UPSTREAM_ZMQ_PORT       port of upstream ROUTER sockets (default 8080)
TICK_SYMBOLS            comma-separated stock symbols
TICK_INTERVAL_S         seconds between full rounds of ticks (default 1.0)
ANALYTICS_WINDOW        short-window size for SMA (default 20)
RECENT_BUFFER_SIZE      capacity of the in-memory log (default 2000)
SUB_REFRESH_S           re-send SUB to upstreams every N seconds (default 30)
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import time
import uuid
from collections import defaultdict, deque
from typing import Deque, Dict, List, Set

import zmq

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]

log = logging.getLogger("reactive_graph.node")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NODE_NAME: str = os.environ.get("NODE_NAME", "market-feed")
NODE_ROLE: str = os.environ.get("NODE_ROLE", "market-feed")
UPSTREAM_NODES: List[str] = [
    n.strip()
    for n in os.environ.get("UPSTREAM_NODES", "").split(",")
    if n.strip()
]
UPSTREAM_ZMQ_PORT: int = int(os.environ.get("UPSTREAM_ZMQ_PORT", "8080"))

DEFAULT_SYMBOLS = "AAPL,GOOGL,MSFT,AMZN,TSLA"
TICK_SYMBOLS: List[str] = [
    s.strip()
    for s in os.environ.get("TICK_SYMBOLS", DEFAULT_SYMBOLS).split(",")
    if s.strip()
]
TICK_INTERVAL_S: float = float(os.environ.get("TICK_INTERVAL_S", "1.0"))
ANALYTICS_WINDOW: int = int(os.environ.get("ANALYTICS_WINDOW", "20"))
RECENT_BUFFER_SIZE: int = int(os.environ.get("RECENT_BUFFER_SIZE", "2000"))
SUB_REFRESH_S: float = float(os.environ.get("SUB_REFRESH_S", "30"))

# ---------------------------------------------------------------------------
# Realistic initial prices
# ---------------------------------------------------------------------------

INITIAL_PRICES: Dict[str, float] = {
    "AAPL": 195.0, "GOOGL": 178.0, "MSFT": 430.0, "AMZN": 190.0,
    "TSLA": 255.0, "META": 510.0, "NVDA": 950.0, "JPM": 205.0,
    "V": 285.0, "WMT": 172.0, "NFLX": 720.0, "DIS": 112.0,
    "BABA": 85.0, "INTC": 32.0, "AMD": 165.0,
}

# ---------------------------------------------------------------------------
# Per-process mutable state
# ---------------------------------------------------------------------------


class NodeState:
    """All mutable state for one node process."""

    def __init__(self) -> None:
        self.paused: bool = False
        self.tick_interval_s: float = TICK_INTERVAL_S
        self.symbols: List[str] = list(TICK_SYMBOLS)
        self.analytics_window: int = ANALYTICS_WINDOW
        self.total_published: int = 0
        self.total_received: int = 0
        self.total_rejected: int = 0
        self.started_at: float = time.time()
        self._seq: int = 0
        self._recent: Deque[Dict] = deque(maxlen=RECENT_BUFFER_SIZE)
        self.prices: Dict[str, float] = {}
        self.price_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=200)
        )
        self.volume_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=200)
        )
        self.analytics_cache: Dict[str, Dict] = {}

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def record(self, direction: str, entry: Dict) -> None:
        self._recent.append(
            {**entry, "observed_at": time.time(), "direction": direction}
        )

    def recent(self, limit: int = 50) -> List[Dict]:
        items = list(self._recent)
        if limit > 0:
            items = items[-limit:]
        return items


# ---------------------------------------------------------------------------
# Market data simulation
# ---------------------------------------------------------------------------


def simulate_tick(state: NodeState, symbol: str) -> dict:
    """Generate one realistic stock tick using a random walk."""
    last = state.prices.get(symbol, INITIAL_PRICES.get(symbol, 100.0))
    volatility = last * 0.0008
    change = random.gauss(0, volatility)
    price = max(0.01, last + change)
    state.prices[symbol] = price
    spread = price * random.uniform(0.0003, 0.001)
    volume = random.randint(100, 50_000)
    return {
        "symbol": symbol,
        "price": round(price, 2),
        "bid": round(price - spread / 2, 2),
        "ask": round(price + spread / 2, 2),
        "volume": volume,
        "change": round(change, 2),
        "change_pct": round((change / last) * 100, 4) if last else 0.0,
    }


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------


def _new_message(
    state: NodeState, kind: str, text: str, to_node: str = ""
) -> GraphMessage:
    msg = GraphMessage(
        id=str(uuid.uuid4()),
        kind=kind,
        from_node=NODE_NAME,
        to_node=to_node,
        text=text,
        timestamp=time.time(),
        sequence=state.next_seq(),
    )
    msg.correlation_id = msg.id
    msg.hops.append(NODE_NAME)
    return msg


def _msg_to_dict(msg: GraphMessage, topic: str = "") -> dict:
    return {
        "id": msg.id,
        "correlation_id": msg.correlation_id,
        "kind": msg.kind,
        "from_node": msg.from_node,
        "to_node": msg.to_node,
        "text": msg.text,
        "timestamp": msg.timestamp,
        "sequence": msg.sequence,
        "hops": list(msg.hops),
        "topic": topic,
    }


def _broadcast_event(
    state: NodeState,
    router: zmq.Socket,
    subscribers: Set[bytes],
    kind: str,
    text: str,
    topic: str,
) -> None:
    """Build a GraphMessage and send it to every subscribed peer."""
    msg = _new_message(state, kind, text)
    payload = msg.SerializeToString()
    topic_bytes = topic.encode("utf-8")
    for peer_id in list(subscribers):
        try:
            router.send_multipart(
                [peer_id, b"EVT", topic_bytes, payload], zmq.NOBLOCK
            )
        except zmq.ZMQError:
            pass
    state.total_published += 1
    state.record("out", _msg_to_dict(msg, topic))


# ---------------------------------------------------------------------------
# Analytics processing
# ---------------------------------------------------------------------------


def _process_tick(
    state: NodeState,
    router: zmq.Socket,
    subscribers: Set[bytes],
    topic: str,
    msg: GraphMessage,
) -> None:
    """Validate a tick, compute analytics, and broadcast results."""
    state.total_received += 1
    state.record("in", _msg_to_dict(msg, topic))

    try:
        tick = json.loads(msg.text)
    except (json.JSONDecodeError, TypeError):
        state.total_rejected += 1
        return

    symbol: str = tick.get("symbol", "")
    price: float = tick.get("price", 0)
    volume: int = tick.get("volume", 0)

    if not symbol or price <= 0 or volume < 0:
        state.total_rejected += 1
        _broadcast_event(
            state, router, subscribers,
            "rejected",
            json.dumps({
                "symbol": symbol,
                "reason": "invalid price/volume/symbol",
                "original_id": msg.id,
            }),
            f"rejected.{symbol}",
        )
        return

    _broadcast_event(
        state, router, subscribers,
        "validated_tick", msg.text, f"validated_tick.{symbol}",
    )

    # -- accumulate history --------------------------------------------------
    state.price_history[symbol].append(price)
    state.volume_history[symbol].append(volume)
    prices = list(state.price_history[symbol])
    volumes = list(state.volume_history[symbol])

    w = state.analytics_window
    short_slice = prices[-w:] if len(prices) >= w else prices
    sma_short = sum(short_slice) / len(short_slice)

    long_w = min(50, len(prices))
    sma_long = sum(prices[-long_w:]) / long_w if long_w else price

    total_pv = sum(p * v for p, v in zip(prices, volumes))
    total_vol = sum(volumes)
    vwap = total_pv / total_vol if total_vol > 0 else price

    if len(short_slice) > 1:
        mean = sum(short_slice) / len(short_slice)
        variance = sum((p - mean) ** 2 for p in short_slice) / len(short_slice)
        vol = math.sqrt(variance)
    else:
        vol = 0.0

    first_price = prices[0]
    session_change = price - first_price
    session_change_pct = (
        (session_change / first_price) * 100 if first_price else 0
    )
    trend = "up" if sma_short > sma_long else (
        "down" if sma_short < sma_long else "neutral"
    )

    analytics = {
        "symbol": symbol,
        "price": round(price, 2),
        "sma_short": round(sma_short, 2),
        "sma_long": round(sma_long, 2),
        "vwap": round(vwap, 2),
        "high": round(max(prices), 2),
        "low": round(min(prices), 2),
        "volatility": round(vol, 4),
        "volume_avg": round(sum(volumes) / len(volumes)),
        "session_change": round(session_change, 2),
        "session_change_pct": round(session_change_pct, 2),
        "trend": trend,
        "samples": len(prices),
        "window": w,
    }
    state.analytics_cache[symbol] = analytics
    _broadcast_event(
        state, router, subscribers,
        "analytics", json.dumps(analytics), f"analytics.{symbol}",
    )


# ---------------------------------------------------------------------------
# Control commands
# ---------------------------------------------------------------------------


def _handle_control(state: NodeState, cmd: dict) -> dict:
    action = cmd.get("action", "")

    if action == "pause":
        state.paused = True
        return {"ok": True, "paused": True}

    if action == "resume":
        state.paused = False
        return {"ok": True, "paused": False}

    if action == "set_interval":
        val = max(0.1, min(10.0, float(cmd.get("interval", state.tick_interval_s))))
        state.tick_interval_s = val
        return {"ok": True, "tick_interval_s": val}

    if action == "set_symbols":
        symbols = cmd.get("symbols", [])
        if isinstance(symbols, str):
            symbols = [s.strip() for s in symbols.split(",") if s.strip()]
        if symbols:
            state.symbols = symbols
        return {"ok": True, "symbols": state.symbols}

    if action == "add_symbol":
        symbol = cmd.get("symbol", "").upper().strip()
        if symbol and symbol not in state.symbols:
            state.symbols.append(symbol)
        return {"ok": True, "symbols": state.symbols}

    if action == "remove_symbol":
        symbol = cmd.get("symbol", "").upper().strip()
        if symbol in state.symbols:
            state.symbols.remove(symbol)
        return {"ok": True, "symbols": state.symbols}

    if action == "set_analytics_window":
        w = max(5, min(200, int(cmd.get("window", state.analytics_window))))
        state.analytics_window = w
        return {"ok": True, "analytics_window": w}

    if action == "status":
        return {
            "ok": True,
            "node_name": NODE_NAME,
            "node_role": NODE_ROLE,
            "paused": state.paused,
            "tick_interval_s": state.tick_interval_s,
            "symbols": state.symbols,
            "analytics_window": state.analytics_window,
            "total_published": state.total_published,
            "total_received": state.total_received,
            "total_rejected": state.total_rejected,
            "uptime_s": round(time.time() - state.started_at, 1),
        }

    return {"ok": False, "error": f"unknown action: {action!r}"}


# ---------------------------------------------------------------------------
# Main event loop
# ---------------------------------------------------------------------------


def main(port: int) -> None:
    """Datatailr entrypoint.  *port* is the platform-assigned port."""
    port = int(port)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    state = NodeState()
    ctx = zmq.Context()

    # -- ROUTER: single service socket (events + control) --------------------
    router = ctx.socket(zmq.ROUTER)
    router.setsockopt(zmq.LINGER, 200)
    router.setsockopt(zmq.SNDHWM, 100_000)
    router.setsockopt(zmq.RCVHWM, 100_000)
    router.bind(f"tcp://*:{port}")
    log.info("[%s] ROUTER bound on port %d", NODE_NAME, port)

    subscribers: Set[bytes] = set()

    # -- DEALERs: upstream subscriptions -------------------------------------
    dealers: list[tuple[str, zmq.Socket]] = []
    for upstream in UPSTREAM_NODES:
        dealer = ctx.socket(zmq.DEALER)
        identity = f"{NODE_NAME}".encode("utf-8")
        dealer.setsockopt(zmq.IDENTITY, identity)
        dealer.setsockopt(zmq.LINGER, 200)
        dealer.setsockopt(zmq.RCVHWM, 100_000)
        dealer.setsockopt(zmq.RECONNECT_IVL, 500)
        dealer.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
        endpoint = f"tcp://{upstream}:{UPSTREAM_ZMQ_PORT}"
        dealer.connect(endpoint)
        dealer.send(b"SUB")
        dealers.append((upstream, dealer))
        log.info("[%s] DEALER connected to %s (subscribed)", NODE_NAME, endpoint)

    time.sleep(0.5)

    poller = zmq.Poller()
    poller.register(router, zmq.POLLIN)
    for _, dealer in dealers:
        poller.register(dealer, zmq.POLLIN)

    log.info(
        "[%s] main loop starting (role=%s, symbols=%s, interval=%.2fs)",
        NODE_NAME, NODE_ROLE, state.symbols, state.tick_interval_s,
    )

    last_tick_time = time.time()
    last_sub_refresh = time.time()
    tick_idx = 0

    try:
        while True:
            now = time.time()

            # -- Periodically re-subscribe to upstreams ----------------------
            if dealers and now - last_sub_refresh >= SUB_REFRESH_S:
                for _, dealer in dealers:
                    try:
                        dealer.send(b"SUB", zmq.NOBLOCK)
                    except zmq.ZMQError:
                        pass
                last_sub_refresh = now

            # -- Market feed: generate ticks ---------------------------------
            if NODE_ROLE == "market-feed" and not state.paused and state.symbols:
                per_symbol = state.tick_interval_s / len(state.symbols)
                if now - last_tick_time >= per_symbol:
                    symbol = state.symbols[tick_idx % len(state.symbols)]
                    tick_idx += 1
                    tick = simulate_tick(state, symbol)
                    _broadcast_event(
                        state, router, subscribers,
                        "tick", json.dumps(tick), f"tick.{symbol}",
                    )
                    last_tick_time = now

            # -- Poll timeout ------------------------------------------------
            if NODE_ROLE == "market-feed" and state.symbols and not state.paused:
                per_symbol = state.tick_interval_s / len(state.symbols)
                remaining = per_symbol - (time.time() - last_tick_time)
                timeout_ms = max(1, int(remaining * 1000))
            else:
                timeout_ms = 100

            events = dict(poller.poll(timeout=timeout_ms))

            # -- Handle ROUTER incoming (SUB / CTL) --------------------------
            if router in events:
                while True:
                    try:
                        frames = router.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(frames) < 2:
                        continue
                    peer_id = bytes(frames[0])
                    cmd = frames[1]

                    if cmd == b"SUB":
                        if peer_id not in subscribers:
                            log.info("[%s] subscriber registered: %r", NODE_NAME, peer_id)
                        subscribers.add(peer_id)

                    elif cmd == b"CTL" and len(frames) >= 3:
                        try:
                            ctl_data = json.loads(frames[2])
                            result = _handle_control(state, ctl_data)
                        except Exception as exc:
                            result = {"ok": False, "error": str(exc)}
                        try:
                            router.send_multipart(
                                [peer_id, b"CTL", json.dumps(result).encode()],
                                zmq.NOBLOCK,
                            )
                        except zmq.ZMQError:
                            pass
                        log.info(
                            "[%s] control: %s -> ok=%s",
                            NODE_NAME,
                            ctl_data.get("action") if isinstance(ctl_data, dict) else "?",
                            result.get("ok"),
                        )

            # -- Handle DEALER incoming (EVT from upstream) ------------------
            for _upstream_name, dealer in dealers:
                if dealer not in events:
                    continue
                while True:
                    try:
                        frames = dealer.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(frames) < 3 or frames[0] != b"EVT":
                        continue
                    topic = frames[1].decode("utf-8", "replace")
                    msg = GraphMessage()
                    try:
                        msg.ParseFromString(frames[2])
                    except Exception as exc:
                        log.warning("[%s] bad protobuf: %s", NODE_NAME, exc)
                        continue
                    if NODE_ROLE == "analytics":
                        _process_tick(state, router, subscribers, topic, msg)

    except KeyboardInterrupt:
        log.info("[%s] interrupted", NODE_NAME)
    finally:
        router.close()
        for _, dealer in dealers:
            dealer.close()
        ctx.term()
        log.info("[%s] shutdown complete", NODE_NAME)


if __name__ == "__main__":
    main(int(os.environ.get("PORT", 8080)))
