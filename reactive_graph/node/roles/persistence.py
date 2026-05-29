"""persistence-sink role: stream fills + position snapshots to Parquet.

Subscribes to the execution-simulator and:

* appends every :class:`Fill` to an in-memory buffer;
* tracks the latest :class:`PositionUpdate` per symbol;
* every ``FLUSH_INTERVAL_S`` seconds (default 10), flushes both to
  blob storage as Parquet files;
* broadcasts a ``system.persistence_flush`` event after each flush so
  the dashboard's system-event ribbon visibly confirms the persistence
  step.

The on-disk layout (see :mod:`reactive_graph.persistence.parquet_io`)
is::

    trades/dt=YYYY-MM-DD/HHMMSS-<seq>.parquet
    positions/latest.parquet                 (overwritten)
    positions/history/dt=YYYY-MM-DD/HHMMSS-<seq>.parquet
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]
from reactive_graph.node.transport import ZmqNode
from reactive_graph.persistence.parquet_io import (
    POSITIONS_HISTORY_PREFIX,
    POSITIONS_LATEST,
    TRADES_PREFIX,
    fills_to_parquet_bytes,
    get_blob,
    positions_to_parquet_bytes,
)

log = logging.getLogger("reactive_graph.persistence_sink")


class PersistenceState:
    def __init__(self, flush_interval_s: float, max_buffer: int) -> None:
        self.flush_interval_s = float(flush_interval_s)
        self.max_buffer = int(max_buffer)
        self.unflushed_fills: List[Dict[str, Any]] = []
        self.latest_positions: Dict[str, Dict[str, Any]] = {}
        self.dirty_positions: bool = False
        self.last_flush_at: float = 0.0
        self.flush_seq: int = 0
        self.total_flushes: int = 0
        self.total_fills_persisted: int = 0
        self.last_flush_summary: Dict[str, Any] = {}
        self.last_error: str = ""


def run(node: ZmqNode, config: dict) -> None:
    state = PersistenceState(
        flush_interval_s=float(config.get("flush_interval_s", 10.0)),
        max_buffer=int(config.get("max_buffer", 200)),
    )
    blob = get_blob()
    log.info(
        "[%s] persistence-sink ready (flush_interval=%.1fs, blob=%s)",
        node.name, state.flush_interval_s, type(blob).__name__,
    )

    def _broadcast_flush_event(result: Dict[str, Any], reason: str) -> None:
        ev = node.new_message(kind="system")
        ev.system.kind = "persistence_flush"
        ev.system.summary = (
            f"flushed {result.get('fills', 0)} fill(s), "
            f"{result.get('positions', 0)} position(s) -> blob "
            f"({reason})"
        )
        ev.system.detail = (
            f"trades={result.get('trades_path', '-')} "
            f"positions={result.get('positions_latest', '-')}"
        )
        ev.system.source = "persistence-sink"
        node.broadcast("system.persistence_flush", ev)

    def flush(reason: str) -> Dict[str, Any]:
        now = time.time()
        if not state.unflushed_fills and not state.dirty_positions:
            state.last_flush_at = now
            return {"ok": True, "skipped": "empty"}
        state.flush_seq += 1
        seq = state.flush_seq
        date = time.strftime("%Y-%m-%d", time.gmtime(now))
        hms = time.strftime("%H%M%S", time.gmtime(now))
        n_fills = len(state.unflushed_fills)
        result: Dict[str, Any] = {
            "ok": True,
            "fills": n_fills,
            "positions": len(state.latest_positions),
            "seq": seq,
            "at": now,
        }
        try:
            if state.unflushed_fills:
                path = f"{TRADES_PREFIX}/dt={date}/{hms}-{seq:04d}.parquet"
                blob.put(path, fills_to_parquet_bytes(state.unflushed_fills))
                result["trades_path"] = path
                state.total_fills_persisted += n_fills
                state.unflushed_fills = []

            if state.dirty_positions and state.latest_positions:
                payload = positions_to_parquet_bytes(state.latest_positions)
                blob.put(POSITIONS_LATEST, payload)
                history_path = (
                    f"{POSITIONS_HISTORY_PREFIX}/dt={date}/"
                    f"{hms}-{seq:04d}.parquet"
                )
                blob.put(history_path, payload)
                result["positions_latest"] = POSITIONS_LATEST
                result["positions_history"] = history_path
                state.dirty_positions = False

            state.total_flushes += 1
            state.last_flush_at = now
            state.last_flush_summary = result
            state.last_error = ""
            log.info(
                "[%s] flushed fills=%d positions=%d reason=%s -> %s",
                node.name, n_fills, len(state.latest_positions), reason,
                result.get("trades_path", "(no fills)"),
            )
            _broadcast_flush_event(result, reason)
        except Exception as exc:  # noqa: BLE001
            log.exception("[%s] flush failed", node.name)
            state.last_error = str(exc)
            return {
                "ok": False,
                "error": str(exc),
                "buffered_fills": len(state.unflushed_fills),
            }
        return result

    def event(_topic: str, orig: GraphMessage) -> None:
        kind = orig.WhichOneof("payload")
        if orig.kind == "fill" and kind == "fill":
            f = orig.fill
            state.unflushed_fills.append({
                "order_id": f.order_id,
                "symbol": f.symbol,
                "side": f.side,
                "qty": int(f.qty),
                "price": float(f.price),
                "slippage": float(f.slippage),
                "strategy": f.strategy,
                "at": float(orig.timestamp),
                "correlation_id": orig.correlation_id or orig.id,
            })
        elif orig.kind == "position_update" and kind == "position_update":
            p = orig.position_update
            state.latest_positions[p.symbol] = {
                "symbol": p.symbol,
                "net_qty": int(p.net_qty),
                "avg_price": float(p.avg_price),
                "market_price": float(p.market_price),
                "realised_pnl": float(p.realised_pnl),
                "unrealised_pnl": float(p.unrealised_pnl),
                "at": float(orig.timestamp),
            }
            state.dirty_positions = True

    def idle() -> float:
        now = time.time()
        if state.last_flush_at == 0.0:
            state.last_flush_at = now
        elapsed = now - state.last_flush_at
        if (
            elapsed >= state.flush_interval_s
            or len(state.unflushed_fills) >= state.max_buffer
        ):
            flush("idle")
            return min(state.flush_interval_s, 0.5)
        return max(0.05, min(0.5, state.flush_interval_s - elapsed))

    def control(cmd: dict) -> Dict[str, Any]:
        action = cmd.get("action", "")
        if action == "flush_now":
            return flush("ctl")
        if action == "set_flush_interval":
            v = max(0.5, float(cmd.get("flush_interval_s", state.flush_interval_s)))
            state.flush_interval_s = v
            return {"ok": True, "flush_interval_s": v}
        if action in ("status", "snapshot"):
            return node.status_snapshot({
                "role": "persistence-sink",
                "flush_interval_s": state.flush_interval_s,
                "buffered_fills": len(state.unflushed_fills),
                "tracked_positions": len(state.latest_positions),
                "total_flushes": state.total_flushes,
                "total_fills_persisted": state.total_fills_persisted,
                "last_flush_at": state.last_flush_at,
                "last_flush_summary": state.last_flush_summary,
                "last_error": state.last_error,
            })
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_event(event)
    node.on_idle(idle)
    node.on_control(control)
    node.run()


def config_from_env() -> dict:
    return {
        "flush_interval_s": float(os.environ.get("FLUSH_INTERVAL_S", "10")),
        "max_buffer": int(os.environ.get("FLUSH_MAX_BUFFER", "200")),
    }
