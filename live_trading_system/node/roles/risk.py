"""risk-engine role: applies pre-trade limits to incoming signals.

Subscribes to the signal-engine and execution-simulator.  For every
incoming :class:`~live_trading_system.node.messages_pb2.Signal` it checks
position, exposure, and daily-loss limits and emits an ``OrderIntent``
with ``status="approved"`` or ``"rejected"`` and a reason string.

Fill events from the execution-simulator update internal position
tracking so subsequent signals are scored against the correct exposure.

Limits can be tuned at runtime via CTL ``set_limits`` and seeded by the
pre-market warm-up workflow.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Dict

from live_trading_system.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]
from live_trading_system.node.transport import ZmqNode

log = logging.getLogger("live_trading_system.risk")


class Position:
    def __init__(self) -> None:
        self.net_qty: int = 0
        self.avg_price: float = 0.0
        self.realised_pnl: float = 0.0

    def apply_fill(self, side: str, qty: int, price: float) -> None:
        signed = qty if side == "buy" else -qty
        prev = self.net_qty
        new = prev + signed
        if prev == 0 or (prev > 0) == (signed > 0):
            total_cost = self.avg_price * abs(prev) + price * abs(signed)
            self.avg_price = total_cost / abs(new) if new != 0 else 0.0
        else:
            closed = min(abs(prev), abs(signed))
            direction = 1 if prev > 0 else -1
            self.realised_pnl += direction * (price - self.avg_price) * closed
            if abs(signed) > abs(prev):
                self.avg_price = price
        self.net_qty = new


class RiskState:
    def __init__(
        self,
        max_position: int,
        max_notional: float,
        max_daily_loss: float,
    ) -> None:
        self.max_position = int(max_position)
        self.max_notional = float(max_notional)
        self.max_daily_loss = float(max_daily_loss)
        self.positions: Dict[str, Position] = {}
        self.approved = 0
        self.rejected = 0


def run(node: ZmqNode, config: dict) -> None:
    state = RiskState(
        max_position=int(config.get("max_position", 1000)),
        max_notional=float(config.get("max_notional", 250_000.0)),
        max_daily_loss=float(config.get("max_daily_loss", 50_000.0)),
    )

    def _pos(symbol: str) -> Position:
        if symbol not in state.positions:
            state.positions[symbol] = Position()
        return state.positions[symbol]

    def _total_realised_pnl() -> float:
        return sum(p.realised_pnl for p in state.positions.values())

    def emit_intent(
        symbol: str, side: str, qty: int, price: float, strategy: str,
        status: str, reason: str, parent: GraphMessage,
    ) -> None:
        msg = node.new_message(kind="order_intent", parent=parent)
        msg.order_intent.symbol = symbol
        msg.order_intent.side = side
        msg.order_intent.status = status
        msg.order_intent.reason = reason
        msg.order_intent.qty = qty
        msg.order_intent.price = price
        msg.order_intent.strategy = strategy
        node.broadcast(f"order_intent.{symbol}", msg)
        if status == "approved":
            state.approved += 1
        else:
            state.rejected += 1

    def evaluate_signal(orig: GraphMessage) -> None:
        s = orig.signal
        symbol = s.symbol
        if not symbol or s.side not in ("buy", "sell"):
            return
        pos = _pos(symbol)
        signed = s.suggested_qty if s.side == "buy" else -s.suggested_qty
        proposed_net = pos.net_qty + signed
        proposed_notional = abs(proposed_net) * s.reference_price

        if abs(proposed_net) > state.max_position:
            emit_intent(symbol, s.side, s.suggested_qty, s.reference_price,
                        s.strategy, "rejected",
                        f"position_limit ({state.max_position})", orig)
            return
        if proposed_notional > state.max_notional:
            emit_intent(symbol, s.side, s.suggested_qty, s.reference_price,
                        s.strategy, "rejected",
                        f"notional_limit ({state.max_notional:.0f})", orig)
            return
        if _total_realised_pnl() < -state.max_daily_loss:
            emit_intent(symbol, s.side, s.suggested_qty, s.reference_price,
                        s.strategy, "rejected", "daily_loss_breached", orig)
            return

        emit_intent(symbol, s.side, s.suggested_qty, s.reference_price,
                    s.strategy, "approved", "ok", orig)

    def consume_fill(orig: GraphMessage) -> None:
        f = orig.fill
        if not f.symbol or f.side not in ("buy", "sell"):
            return
        _pos(f.symbol).apply_fill(f.side, int(f.qty), f.price)

    def event(topic: str, orig: GraphMessage) -> None:
        kind = orig.WhichOneof("payload")
        if orig.kind == "signal" and kind == "signal":
            evaluate_signal(orig)
        elif orig.kind == "fill" and kind == "fill":
            consume_fill(orig)

    def control(cmd: dict) -> dict:
        action = cmd.get("action", "")
        if action == "set_limits":
            if "max_position" in cmd:
                state.max_position = max(0, int(cmd["max_position"]))
            if "max_notional" in cmd:
                state.max_notional = max(0.0, float(cmd["max_notional"]))
            if "max_daily_loss" in cmd:
                state.max_daily_loss = max(0.0, float(cmd["max_daily_loss"]))
            return {
                "ok": True,
                "max_position": state.max_position,
                "max_notional": state.max_notional,
                "max_daily_loss": state.max_daily_loss,
            }
        if action == "seed_positions":
            positions = cmd.get("positions", {})
            if isinstance(positions, dict):
                for sym, data in positions.items():
                    pos = _pos(sym)
                    pos.net_qty = int(data.get("net_qty", 0))
                    pos.avg_price = float(data.get("avg_price", 0.0))
                    pos.realised_pnl = float(data.get("realised_pnl", 0.0))
            return {"ok": True, "seeded": list(state.positions.keys())}
        if action in ("status", "snapshot"):
            return node.status_snapshot({
                "role": "risk-engine",
                "max_position": state.max_position,
                "max_notional": state.max_notional,
                "max_daily_loss": state.max_daily_loss,
                "approved": state.approved,
                "rejected": state.rejected,
                "positions": {
                    sym: {
                        "net_qty": p.net_qty,
                        "avg_price": round(p.avg_price, 4),
                        "realised_pnl": round(p.realised_pnl, 2),
                    }
                    for sym, p in state.positions.items()
                },
            })
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_control(control)
    node.on_event(event)
    log.info(
        "[%s] risk-engine ready (max_pos=%d, max_notional=%.0f, max_daily_loss=%.0f)",
        node.name, state.max_position, state.max_notional, state.max_daily_loss,
    )
    node.run()


def config_from_env() -> dict:
    return {
        "max_position": int(os.environ.get("MAX_POSITION", "1000")),
        "max_notional": float(os.environ.get("MAX_NOTIONAL", "250000")),
        "max_daily_loss": float(os.environ.get("MAX_DAILY_LOSS", "50000")),
    }
