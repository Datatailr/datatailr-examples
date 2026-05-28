"""execution-simulator role: simulates broker fills for approved orders.

Subscribes to the risk-engine and the market-feed (for last-price
tracking).  Every approved :class:`OrderIntent` is converted into a
:class:`Fill` with random slippage and a small delay window before
emission.  Internal positions and realised PnL are maintained and a
:class:`PositionUpdate` is broadcast after each fill so the rest of
the graph (and the dashboard) sees the new state.

CTL ``seed_positions`` lets the pre-market warm-up workflow restore
yesterday's positions from blob storage; CTL ``snapshot`` lets the EOD
reconciliation workflow read the current state.
"""

from __future__ import annotations

import logging
import os
import random
import time
import uuid
from collections import deque
from typing import Deque, Dict, List

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]
from reactive_graph.node.transport import ZmqNode

log = logging.getLogger("reactive_graph.execution")


class Position:
    def __init__(self) -> None:
        self.net_qty: int = 0
        self.avg_price: float = 0.0
        self.realised_pnl: float = 0.0
        self.market_price: float = 0.0

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

    def unrealised(self) -> float:
        if self.net_qty == 0 or self.market_price == 0:
            return 0.0
        return (self.market_price - self.avg_price) * self.net_qty


class ExecutionState:
    def __init__(self, slippage_bps: float, fill_delay_s: float) -> None:
        self.slippage_bps = float(slippage_bps)
        self.fill_delay_s = float(fill_delay_s)
        self.positions: Dict[str, Position] = {}
        self.fills_history: Deque[dict] = deque(maxlen=500)
        self.pending: List[dict] = []
        self.total_fills = 0


def run(node: ZmqNode, config: dict) -> None:
    state = ExecutionState(
        slippage_bps=float(config.get("slippage_bps", 5.0)),
        fill_delay_s=float(config.get("fill_delay_s", 0.2)),
    )

    def _pos(symbol: str) -> Position:
        if symbol not in state.positions:
            state.positions[symbol] = Position()
        return state.positions[symbol]

    def emit_fill_and_position(intent: dict) -> None:
        symbol = intent["symbol"]
        side = intent["side"]
        qty = int(intent["qty"])
        ref = float(intent["price"])
        strategy = intent.get("strategy", "")
        order_id = intent["order_id"]
        parent = {
            "hops": intent.get("upstream_hops", []),
            "correlation_id": intent.get("correlation_id", order_id),
        }

        slip_pct = random.uniform(-state.slippage_bps, state.slippage_bps) / 10_000.0
        sign = 1 if side == "buy" else -1
        fill_price = max(0.01, ref * (1 + sign * abs(slip_pct)))
        slippage = (fill_price - ref) * sign

        pos = _pos(symbol)
        pos.apply_fill(side, qty, fill_price)
        if pos.market_price == 0:
            pos.market_price = fill_price

        fmsg = node.new_message(kind="fill", parent=parent)
        fmsg.fill.symbol = symbol
        fmsg.fill.side = side
        fmsg.fill.qty = qty
        fmsg.fill.price = round(fill_price, 4)
        fmsg.fill.slippage = round(slippage, 4)
        fmsg.fill.strategy = strategy
        fmsg.fill.order_id = order_id
        node.broadcast(f"fill.{symbol}", fmsg)

        state.fills_history.append({
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": round(fill_price, 4),
            "slippage": round(slippage, 4),
            "strategy": strategy,
            "at": time.time(),
        })
        state.total_fills += 1

        pmsg = node.new_message(kind="position_update", parent=parent)
        pmsg.position_update.symbol = symbol
        pmsg.position_update.net_qty = pos.net_qty
        pmsg.position_update.avg_price = round(pos.avg_price, 4)
        pmsg.position_update.market_price = round(pos.market_price, 4)
        pmsg.position_update.realised_pnl = round(pos.realised_pnl, 2)
        pmsg.position_update.unrealised_pnl = round(pos.unrealised(), 2)
        node.broadcast(f"position.{symbol}", pmsg)

    def queue_intent(orig: GraphMessage) -> None:
        oi = orig.order_intent
        if oi.status != "approved" or oi.qty <= 0:
            return
        state.pending.append({
            "due_at": time.time() + state.fill_delay_s,
            "symbol": oi.symbol,
            "side": oi.side,
            "qty": int(oi.qty),
            "price": oi.price,
            "strategy": oi.strategy,
            "order_id": str(uuid.uuid4()),
            "correlation_id": orig.correlation_id or orig.id,
            "upstream_hops": list(orig.hops),
        })

    def update_market_price(orig: GraphMessage) -> None:
        if orig.WhichOneof("payload") != "tick":
            return
        pos = _pos(orig.tick.symbol)
        pos.market_price = orig.tick.price

    def event(topic: str, orig: GraphMessage) -> None:
        kind = orig.WhichOneof("payload")
        if orig.kind == "order_intent" and kind == "order_intent":
            queue_intent(orig)
        elif orig.kind == "tick" and kind == "tick":
            update_market_price(orig)

    def idle() -> float | None:
        now = time.time()
        if not state.pending:
            return 0.1
        due_now = [i for i in state.pending if i["due_at"] <= now]
        for intent in due_now:
            emit_fill_and_position(intent)
        if due_now:
            state.pending = [i for i in state.pending if i["due_at"] > now]
        if state.pending:
            return max(0.005, min(p["due_at"] - now for p in state.pending))
        return 0.1

    def control(cmd: dict) -> dict:
        action = cmd.get("action", "")
        if action == "seed_positions":
            positions = cmd.get("positions", {})
            if isinstance(positions, dict):
                for sym, data in positions.items():
                    pos = _pos(sym)
                    pos.net_qty = int(data.get("net_qty", 0))
                    pos.avg_price = float(data.get("avg_price", 0.0))
                    pos.realised_pnl = float(data.get("realised_pnl", 0.0))
                    if "market_price" in data:
                        pos.market_price = float(data["market_price"])
            return {"ok": True, "seeded": list(state.positions.keys())}
        if action == "set_slippage_bps":
            state.slippage_bps = max(0.0, float(cmd.get("slippage_bps", state.slippage_bps)))
            return {"ok": True, "slippage_bps": state.slippage_bps}
        if action == "set_fill_delay":
            state.fill_delay_s = max(0.0, float(cmd.get("fill_delay_s", state.fill_delay_s)))
            return {"ok": True, "fill_delay_s": state.fill_delay_s}
        if action == "snapshot_orders":
            limit = int(cmd.get("limit", 200))
            return {
                "ok": True,
                "fills": list(state.fills_history)[-limit:],
                "total_fills": state.total_fills,
            }
        if action in ("status", "snapshot"):
            return {
                "ok": True,
                "node_name": node.name,
                "role": "execution-simulator",
                "slippage_bps": state.slippage_bps,
                "fill_delay_s": state.fill_delay_s,
                "total_fills": state.total_fills,
                "positions": {
                    sym: {
                        "net_qty": p.net_qty,
                        "avg_price": round(p.avg_price, 4),
                        "market_price": round(p.market_price, 4),
                        "realised_pnl": round(p.realised_pnl, 2),
                        "unrealised_pnl": round(p.unrealised(), 2),
                    }
                    for sym, p in state.positions.items()
                },
                "uptime_s": round(time.time() - node.started_at, 1),
            }
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_control(control)
    node.on_event(event)
    node.on_idle(idle)
    log.info(
        "[%s] execution-simulator ready (slippage_bps=%.1f, delay=%.2fs)",
        node.name, state.slippage_bps, state.fill_delay_s,
    )
    node.run()


def config_from_env() -> dict:
    return {
        "slippage_bps": float(os.environ.get("SLIPPAGE_BPS", "5.0")),
        "fill_delay_s": float(os.environ.get("FILL_DELAY_S", "0.2")),
    }
