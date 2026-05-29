"""signal-engine role: turns analytics into trading signals.

Subscribes to the analytics-engine and emits ``Signal`` messages based
on simple momentum (``sma_short`` vs ``sma_long``) and mean-reversion
(price vs VWAP) strategies.  Strategies can be toggled per-symbol via
CTL ``enable_strategy`` / ``disable_strategy`` commands.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Dict, Set

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]
from reactive_graph.node.transport import ZmqNode

log = logging.getLogger("reactive_graph.signals")

STRATEGIES = ("momentum", "mean_reversion")


class SignalState:
    def __init__(
        self,
        enabled_strategies: Set[str],
        suggested_qty: int,
        min_strength: float,
        cooldown_s: float,
    ) -> None:
        self.enabled_strategies: Set[str] = set(enabled_strategies)
        self.suggested_qty = int(suggested_qty)
        self.min_strength = float(min_strength)
        self.cooldown_s = float(cooldown_s)
        self.last_signal_at: Dict[str, float] = {}
        self.signals_emitted = 0
        self.analytics_seen = 0
        self.last_heartbeat = time.time()
        self.last_signals: Dict[str, dict] = {}


def _momentum(a) -> tuple[str, float]:
    """Return (side, strength) for momentum on SMA cross.

    The simulated market-feed has ~0.08 % per-tick volatility, so a
    SMA-short / SMA-long gap of even 0.01 % is meaningful and should
    fire a signal (subject to the engine's cooldown + strength gate).
    """
    if a.sma_long <= 0:
        return ("flat", 0.0)
    gap_pct = (a.sma_short - a.sma_long) / a.sma_long
    if gap_pct > 0.0001:
        return ("buy", min(1.0, gap_pct * 1000))
    if gap_pct < -0.0001:
        return ("sell", min(1.0, -gap_pct * 1000))
    return ("flat", 0.0)


def _mean_reversion(a) -> tuple[str, float]:
    """Return (side, strength) for mean-reversion on price vs VWAP."""
    if a.vwap <= 0:
        return ("flat", 0.0)
    deviation = (a.price - a.vwap) / a.vwap
    if deviation > 0.0005:
        return ("sell", min(1.0, deviation * 400))
    if deviation < -0.0005:
        return ("buy", min(1.0, -deviation * 400))
    return ("flat", 0.0)


def run(node: ZmqNode, config: dict) -> None:
    state = SignalState(
        enabled_strategies=set(config.get("enabled_strategies", STRATEGIES)),
        suggested_qty=int(config.get("suggested_qty", 100)),
        min_strength=float(config.get("min_strength", 0.2)),
        cooldown_s=float(config.get("cooldown_s", 5.0)),
    )

    def emit(symbol: str, side: str, strategy: str, strength: float,
             reference_price: float, parent: GraphMessage) -> None:
        msg = node.new_message(kind="signal", parent=parent)
        msg.signal.symbol = symbol
        msg.signal.side = side
        msg.signal.strategy = strategy
        msg.signal.strength = round(strength, 4)
        msg.signal.reference_price = round(reference_price, 2)
        msg.signal.suggested_qty = state.suggested_qty
        node.broadcast(f"signal.{symbol}", msg)
        state.signals_emitted += 1
        state.last_signals[symbol] = {
            "symbol": symbol, "side": side, "strategy": strategy,
            "strength": round(strength, 4),
            "reference_price": round(reference_price, 2),
            "suggested_qty": state.suggested_qty,
            "at": time.time(),
        }
        log.info(
            "[%s] emit signal symbol=%s side=%s strategy=%s strength=%.3f ref=%.2f total=%d",
            node.name, symbol, side, strategy, strength, reference_price,
            state.signals_emitted,
        )

    def event(topic: str, orig: GraphMessage) -> None:
        if orig.kind != "analytics" or orig.WhichOneof("payload") != "analytics":
            return
        a = orig.analytics
        symbol = a.symbol
        state.analytics_seen += 1

        now = time.time()
        if now - state.last_heartbeat >= 10.0:
            log.info(
                "[%s] heartbeat analytics_seen=%d signals_emitted=%d subs=%d",
                node.name, state.analytics_seen, state.signals_emitted,
                len(node.subscribers),
            )
            state.last_heartbeat = now

        if not symbol or a.samples < 3:
            return
        last = state.last_signal_at.get(symbol, 0)
        if now - last < state.cooldown_s:
            return

        candidates: list[tuple[str, str, float]] = []
        if "momentum" in state.enabled_strategies:
            side, strength = _momentum(a)
            if side != "flat" and strength >= state.min_strength:
                candidates.append(("momentum", side, strength))
        if "mean_reversion" in state.enabled_strategies:
            side, strength = _mean_reversion(a)
            if side != "flat" and strength >= state.min_strength:
                candidates.append(("mean_reversion", side, strength))

        if not candidates:
            return

        candidates.sort(key=lambda c: c[2], reverse=True)
        strategy, side, strength = candidates[0]
        emit(symbol, side, strategy, strength, a.price, parent=orig)
        state.last_signal_at[symbol] = now

    def control(cmd: dict) -> dict:
        action = cmd.get("action", "")
        if action == "enable_strategy":
            s = cmd.get("strategy", "")
            if s in STRATEGIES:
                state.enabled_strategies.add(s)
            return {"ok": True, "enabled_strategies": sorted(state.enabled_strategies)}
        if action == "disable_strategy":
            s = cmd.get("strategy", "")
            state.enabled_strategies.discard(s)
            return {"ok": True, "enabled_strategies": sorted(state.enabled_strategies)}
        if action == "enable_strategies":
            strategies = cmd.get("strategies", [])
            if isinstance(strategies, str):
                strategies = [s.strip() for s in strategies.split(",") if s.strip()]
            state.enabled_strategies = {s for s in strategies if s in STRATEGIES}
            return {"ok": True, "enabled_strategies": sorted(state.enabled_strategies)}
        if action == "set_suggested_qty":
            state.suggested_qty = max(1, int(cmd.get("qty", state.suggested_qty)))
            return {"ok": True, "suggested_qty": state.suggested_qty}
        if action == "set_min_strength":
            state.min_strength = max(0.0, min(1.0, float(cmd.get("min_strength", state.min_strength))))
            return {"ok": True, "min_strength": state.min_strength}
        if action == "set_cooldown":
            state.cooldown_s = max(0.0, float(cmd.get("cooldown_s", state.cooldown_s)))
            return {"ok": True, "cooldown_s": state.cooldown_s}
        if action == "force_signal":
            symbol = str(cmd.get("symbol", "") or "AAPL").upper()
            side = str(cmd.get("side", "buy")).lower()
            if side not in ("buy", "sell"):
                return {"ok": False, "error": f"invalid side: {side!r}"}
            strategy = str(cmd.get("strategy", "manual"))
            strength = float(cmd.get("strength", 0.5))
            reference_price = float(cmd.get("reference_price", 100.0))
            emit(symbol, side, strategy, strength, reference_price, parent=None)
            return {
                "ok": True, "forced": True,
                "symbol": symbol, "side": side,
                "strategy": strategy, "strength": strength,
                "signals_emitted": state.signals_emitted,
            }
        if action in ("status", "snapshot"):
            return node.status_snapshot({
                "role": "signal-engine",
                "enabled_strategies": sorted(state.enabled_strategies),
                "suggested_qty": state.suggested_qty,
                "min_strength": state.min_strength,
                "cooldown_s": state.cooldown_s,
                "analytics_seen": state.analytics_seen,
                "signals_emitted": state.signals_emitted,
                "last_signals": state.last_signals,
            })
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_control(control)
    node.on_event(event)
    log.info(
        "[%s] signal-engine ready (strategies=%s, qty=%d)",
        node.name, sorted(state.enabled_strategies), state.suggested_qty,
    )
    node.run()


def config_from_env() -> dict:
    raw = os.environ.get("ENABLED_STRATEGIES", ",".join(STRATEGIES))
    strategies = {s.strip() for s in raw.split(",") if s.strip() in STRATEGIES}
    return {
        "enabled_strategies": strategies or set(STRATEGIES),
        "suggested_qty": int(os.environ.get("SUGGESTED_QTY", "100")),
        "min_strength": float(os.environ.get("MIN_SIGNAL_STRENGTH", "0.2")),
        "cooldown_s": float(os.environ.get("SIGNAL_COOLDOWN_S", "5.0")),
    }
