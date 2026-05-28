"""analytics-engine role: validates ticks and computes rolling analytics.

Subscribes to upstream market-feed(s), validates each ``Tick``, computes
SMA / VWAP / volatility / session change, and re-broadcasts both the
``validated_tick.<symbol>`` (the original Tick) and an
``analytics.<symbol>`` (Analytics proto).  Invalid ticks are broadcast
as ``rejected.<symbol>`` events for visibility on the dashboard.
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]
from reactive_graph.node.transport import ZmqNode

log = logging.getLogger("reactive_graph.analytics")


class AnalyticsState:
    def __init__(self, window: int) -> None:
        self.window = window
        self.total_rejected = 0
        self.price_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=200)
        )
        self.volume_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=200)
        )
        self.cache: Dict[str, Dict[str, float]] = {}


def _compute(state: AnalyticsState, symbol: str, price: float, volume: int) -> dict:
    state.price_history[symbol].append(price)
    state.volume_history[symbol].append(volume)
    prices = list(state.price_history[symbol])
    volumes = list(state.volume_history[symbol])

    w = state.window
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
        (session_change / first_price) * 100 if first_price else 0.0
    )
    trend = "up" if sma_short > sma_long else (
        "down" if sma_short < sma_long else "neutral"
    )

    return {
        "symbol": symbol,
        "price": round(price, 2),
        "sma_short": round(sma_short, 2),
        "sma_long": round(sma_long, 2),
        "vwap": round(vwap, 2),
        "high": round(max(prices), 2),
        "low": round(min(prices), 2),
        "volatility": round(vol, 4),
        "volume_avg": int(sum(volumes) / len(volumes)),
        "session_change": round(session_change, 2),
        "session_change_pct": round(session_change_pct, 2),
        "trend": trend,
        "samples": len(prices),
        "window": w,
    }


def run(node: ZmqNode, config: dict) -> None:
    state = AnalyticsState(window=int(config.get("window", 20)))

    def emit_rejected(
        symbol: str, reason: str, original_id: str, parent: GraphMessage
    ) -> None:
        msg = node.new_message(kind="rejected", parent=parent)
        msg.rejected.symbol = symbol
        msg.rejected.reason = reason
        msg.rejected.original_id = original_id
        node.broadcast(f"rejected.{symbol}", msg)

    def emit_validated(orig: GraphMessage) -> None:
        msg = node.new_message(kind="validated_tick", parent=orig)
        msg.tick.CopyFrom(orig.tick)
        node.broadcast(f"validated_tick.{orig.tick.symbol}", msg)

    def emit_analytics(symbol: str, a: dict, parent: GraphMessage) -> None:
        msg = node.new_message(kind="analytics", parent=parent)
        msg.analytics.symbol = a["symbol"]
        msg.analytics.price = a["price"]
        msg.analytics.sma_short = a["sma_short"]
        msg.analytics.sma_long = a["sma_long"]
        msg.analytics.vwap = a["vwap"]
        msg.analytics.high = a["high"]
        msg.analytics.low = a["low"]
        msg.analytics.volatility = a["volatility"]
        msg.analytics.volume_avg = a["volume_avg"]
        msg.analytics.session_change = a["session_change"]
        msg.analytics.session_change_pct = a["session_change_pct"]
        msg.analytics.trend = a["trend"]
        msg.analytics.samples = a["samples"]
        msg.analytics.window = a["window"]
        node.broadcast(f"analytics.{symbol}", msg)

    def event(topic: str, orig: GraphMessage) -> None:
        if orig.kind != "tick" or orig.WhichOneof("payload") != "tick":
            return
        symbol = orig.tick.symbol
        price = orig.tick.price
        volume = orig.tick.volume

        if not symbol or price <= 0 or volume < 0:
            state.total_rejected += 1
            emit_rejected(symbol, "invalid price/volume/symbol", orig.id, orig)
            return

        emit_validated(orig)
        a = _compute(state, symbol, price, volume)
        state.cache[symbol] = a
        emit_analytics(symbol, a, parent=orig)

    def control(cmd: dict) -> dict:
        action = cmd.get("action", "")
        if action == "set_analytics_window":
            w = max(5, min(200, int(cmd.get("window", state.window))))
            state.window = w
            return {"ok": True, "analytics_window": w}
        if action in ("status", "snapshot"):
            return {
                "ok": True,
                "node_name": node.name,
                "role": "analytics",
                "analytics_window": state.window,
                "total_received": node.total_received,
                "total_published": node.total_published,
                "total_rejected": state.total_rejected,
                "analytics": dict(state.cache),
                "uptime_s": round(time.time() - node.started_at, 1),
            }
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_control(control)
    node.on_event(event)
    log.info("[%s] analytics-engine ready (window=%d)", node.name, state.window)
    node.run()


def config_from_env() -> dict:
    return {
        "window": int(os.environ.get("ANALYTICS_WINDOW", "20")),
    }
