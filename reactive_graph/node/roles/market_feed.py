"""market-feed role: simulates a stock exchange.

Generates ``Tick`` protobuf events on a configurable interval and
broadcasts them on the ``tick.<symbol>`` topic.  Accepts control
commands for pause/resume, interval changes, and symbol management.
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Dict, List

from reactive_graph.node.transport import ZmqNode

log = logging.getLogger("reactive_graph.market_feed")

DEFAULT_SYMBOLS = "AAPL,GOOGL,MSFT,AMZN,TSLA"
INITIAL_PRICES: Dict[str, float] = {
    "AAPL": 195.0, "GOOGL": 178.0, "MSFT": 430.0, "AMZN": 190.0,
    "TSLA": 255.0, "META": 510.0, "NVDA": 950.0, "JPM": 205.0,
    "V": 285.0, "WMT": 172.0, "NFLX": 720.0, "DIS": 112.0,
    "BABA": 85.0, "INTC": 32.0, "AMD": 165.0,
}


class FeedState:
    def __init__(self, symbols: List[str], tick_interval_s: float) -> None:
        self.paused = False
        self.tick_interval_s = tick_interval_s
        self.symbols = list(symbols)
        self.prices: Dict[str, float] = {}
        self.tick_idx = 0
        self.last_tick_time = time.time()


def _simulate(state: FeedState, symbol: str) -> Dict[str, float]:
    last = state.prices.get(symbol, INITIAL_PRICES.get(symbol, 100.0))
    volatility = last * 0.0008
    change = random.gauss(0, volatility)
    price = max(0.01, last + change)
    state.prices[symbol] = price
    spread = price * random.uniform(0.0003, 0.001)
    return {
        "symbol": symbol,
        "price": round(price, 2),
        "bid": round(price - spread / 2, 2),
        "ask": round(price + spread / 2, 2),
        "volume": random.randint(100, 50_000),
        "change": round(change, 2),
        "change_pct": round((change / last) * 100, 4) if last else 0.0,
    }


def run(node: ZmqNode, config: dict) -> None:
    state = FeedState(
        symbols=config.get("symbols", []),
        tick_interval_s=float(config.get("tick_interval_s", 1.0)),
    )

    def emit_tick(symbol: str) -> None:
        tick = _simulate(state, symbol)
        msg = node.new_message(kind="tick")
        msg.tick.symbol = tick["symbol"]
        msg.tick.price = tick["price"]
        msg.tick.bid = tick["bid"]
        msg.tick.ask = tick["ask"]
        msg.tick.volume = tick["volume"]
        msg.tick.change = tick["change"]
        msg.tick.change_pct = tick["change_pct"]
        node.broadcast(f"tick.{symbol}", msg)

    def idle() -> float | None:
        if state.paused or not state.symbols:
            return 0.1
        per_symbol = state.tick_interval_s / max(1, len(state.symbols))
        now = time.time()
        if now - state.last_tick_time >= per_symbol:
            symbol = state.symbols[state.tick_idx % len(state.symbols)]
            state.tick_idx += 1
            emit_tick(symbol)
            state.last_tick_time = now
        remaining = per_symbol - (time.time() - state.last_tick_time)
        return max(0.001, remaining)

    def control(cmd: dict) -> dict:
        action = cmd.get("action", "")
        if action == "pause":
            state.paused = True
            return {"ok": True, "paused": True}
        if action == "resume":
            state.paused = False
            return {"ok": True, "paused": False}
        if action == "set_interval":
            v = max(0.1, min(10.0, float(cmd.get("interval", state.tick_interval_s))))
            state.tick_interval_s = v
            return {"ok": True, "tick_interval_s": v}
        if action == "set_symbols":
            symbols = cmd.get("symbols", [])
            if isinstance(symbols, str):
                symbols = [s.strip() for s in symbols.split(",") if s.strip()]
            if symbols:
                state.symbols = symbols
            return {"ok": True, "symbols": state.symbols}
        if action == "add_symbol":
            sym = cmd.get("symbol", "").upper().strip()
            if sym and sym not in state.symbols:
                state.symbols.append(sym)
            return {"ok": True, "symbols": state.symbols}
        if action == "remove_symbol":
            sym = cmd.get("symbol", "").upper().strip()
            if sym in state.symbols:
                state.symbols.remove(sym)
            return {"ok": True, "symbols": state.symbols}
        if action in ("status", "snapshot"):
            return node.status_snapshot({
                "role": "market-feed",
                "paused": state.paused,
                "tick_interval_s": state.tick_interval_s,
                "symbols": state.symbols,
                "prices": {s: round(p, 2) for s, p in state.prices.items()},
            })
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_control(control)
    node.on_idle(idle)
    log.info(
        "[%s] market-feed ready (symbols=%s, interval=%.2fs)",
        node.name, state.symbols, state.tick_interval_s,
    )
    node.run()


def config_from_env() -> dict:
    symbols = [
        s.strip()
        for s in os.environ.get("TICK_SYMBOLS", DEFAULT_SYMBOLS).split(",")
        if s.strip()
    ]
    return {
        "symbols": symbols,
        "tick_interval_s": float(os.environ.get("TICK_INTERVAL_S", "1.0")),
    }
