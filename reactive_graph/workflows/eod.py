"""End-of-day reconciliation workflow.

Tasks read the Parquet store written by ``persistence-sink`` (using
DuckDB), pull live counters from the risk- and signal-engines via CTL,
briefly subscribe to the analytics-engine to measure live-stream
quality, fold everything into a single report, persist it to blob
storage, and finally publish an ``eod_complete`` system event through
the notification-bus so the dashboard shows the workflow finished.

The fill history + position snapshot come from
``reactive_graph/trades/dt=YYYY-MM-DD/*.parquet`` and
``reactive_graph/positions/latest.parquet`` -- the same files the
dashboard's persisted-history panel queries.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from datatailr import task

from reactive_graph.persistence import parquet_io
from reactive_graph.workflows.zmq_client import (
    broadcast,
    ctl_request,
    sample_events,
)

logger = logging.getLogger("reactive_graph.workflows.eod")

EXECUTION_HOST = os.environ.get("EXECUTION_HOST", "execution-simulator")
RISK_HOST = os.environ.get("RISK_HOST", "risk-engine")
SIGNAL_HOST = os.environ.get("SIGNAL_HOST", "signal-engine")
ANALYTICS_HOST = os.environ.get("ANALYTICS_HOST", "analytics-engine")
PERSISTENCE_HOST = os.environ.get("PERSISTENCE_HOST", "persistence-sink")
BUS_HOST = os.environ.get("BUS_HOST", "notification-bus")
BLOB_BUCKET = os.environ.get("REACTIVE_GRAPH_BLOB_BUCKET", "reactive_graph/eod")
SAMPLE_DURATION_S = float(os.environ.get("EOD_SAMPLE_DURATION_S", "20"))


def _rundate() -> str:
    return os.environ.get("DATATAILR_BATCH_ARG_RUNDATE") or time.strftime("%Y-%m-%d")


@task()
def flush_persistence() -> Dict[str, Any]:
    """Ask persistence-sink to flush any buffered fills before we read."""
    reply = ctl_request(PERSISTENCE_HOST, "flush_now")
    if not reply.get("ok"):
        logger.warning("persistence-sink flush_now failed: %s", reply)
    return reply


@task()
def snapshot_positions(_flushed: Dict[str, Any]) -> Dict[str, Any]:
    """Read the latest positions snapshot from ``positions/latest.parquet``."""
    blob = parquet_io.get_blob()
    if not blob.exists(parquet_io.POSITIONS_LATEST):
        logger.info("no positions/latest.parquet yet -- treating as flat book")
        return {"ok": True, "positions": {}, "source": parquet_io.POSITIONS_LATEST}
    rows = parquet_io.duckdb_query(
        "SELECT * FROM positions",
        blob=blob,
        views={"positions": [parquet_io.POSITIONS_LATEST]},
    )
    positions = {
        r["symbol"]: {
            "net_qty": r["net_qty"],
            "avg_price": r["avg_price"],
            "market_price": r["market_price"],
            "realised_pnl": r["realised_pnl"],
            "unrealised_pnl": r["unrealised_pnl"],
        }
        for r in rows
    }
    return {
        "ok": True,
        "positions": positions,
        "total_fills": None,  # filled in by snapshot_orders below
        "source": parquet_io.POSITIONS_LATEST,
    }


@task()
def snapshot_orders(_flushed: Dict[str, Any]) -> Dict[str, Any]:
    """Read every fill in today's trades partition via DuckDB."""
    blob = parquet_io.get_blob()
    trades_paths = parquet_io.today_trades_paths(blob, _rundate())
    if not trades_paths:
        return {"ok": True, "fills": [], "files": 0,
                "source": parquet_io.TRADES_PREFIX}
    rows = parquet_io.duckdb_query(
        'SELECT order_id, symbol, side, qty, price, slippage, strategy, '
        '       "at" AS at, correlation_id '
        'FROM trades ORDER BY "at"',
        blob=blob,
        views={"trades": trades_paths},
    )
    return {
        "ok": True,
        "fills": rows,
        "files": len(trades_paths),
        "source": parquet_io.TRADES_PREFIX,
    }


@task()
def snapshot_risk() -> Dict[str, Any]:
    """Read approved/rejected counters + limits from risk-engine."""
    reply = ctl_request(RISK_HOST, "snapshot")
    if not reply.get("ok"):
        logger.warning("risk-engine snapshot failed: %s", reply)
        return {"ok": False, "error": reply.get("error")}
    return {
        "ok": True,
        "approved": reply.get("approved", 0),
        "rejected": reply.get("rejected", 0),
        "max_position": reply.get("max_position"),
        "max_notional": reply.get("max_notional"),
        "max_daily_loss": reply.get("max_daily_loss"),
        "risk_positions": reply.get("positions", {}),
    }


@task()
def snapshot_signals() -> Dict[str, Any]:
    """Read enabled strategies + signal counters from signal-engine."""
    reply = ctl_request(SIGNAL_HOST, "snapshot")
    if not reply.get("ok"):
        logger.warning("signal-engine snapshot failed: %s", reply)
        return {"ok": False, "error": reply.get("error")}
    return {
        "ok": True,
        "enabled_strategies": reply.get("enabled_strategies", []),
        "signals_emitted": reply.get("signals_emitted", 0),
        "last_signals": reply.get("last_signals", {}),
    }


@task(memory="256m")
def sample_market_quality() -> Dict[str, Any]:
    """Briefly subscribe to analytics-engine and measure stream health."""
    start = time.time()
    events = sample_events([ANALYTICS_HOST], duration_s=SAMPLE_DURATION_S)
    elapsed = time.time() - start
    kinds: Dict[str, int] = {}
    symbols: set[str] = set()
    for ev in events:
        kinds[ev.get("kind", "?")] = kinds.get(ev.get("kind", "?"), 0) + 1
        sym = (ev.get("data") or {}).get("symbol")
        if sym:
            symbols.add(sym)
    return {
        "sampled_events": len(events),
        "duration_s": round(elapsed, 2),
        "rate_per_s": round(len(events) / max(elapsed, 0.001), 2),
        "by_kind": kinds,
        "symbols": sorted(symbols),
    }


@task()
def compute_pnl_report(
    positions: Dict[str, Any],
    orders: Dict[str, Any],
    risk: Dict[str, Any],
    signals: Dict[str, Any],
    market_quality: Dict[str, Any],
) -> Dict[str, Any]:
    """Fold every snapshot into a single end-of-day report."""
    rundate = _rundate()
    pos = positions.get("positions", {}) or {}
    fills: List[Dict[str, Any]] = orders.get("fills", []) or []

    realised_total = sum(float(p.get("realised_pnl", 0.0)) for p in pos.values())
    unrealised_total = sum(float(p.get("unrealised_pnl", 0.0)) for p in pos.values())
    gross_pnl = realised_total + unrealised_total

    buy_qty = sum(f.get("qty", 0) for f in fills if f.get("side") == "buy")
    sell_qty = sum(f.get("qty", 0) for f in fills if f.get("side") == "sell")
    avg_slippage = (
        sum(abs(f.get("slippage", 0.0)) for f in fills) / len(fills)
        if fills else 0.0
    )

    by_strategy: Dict[str, int] = {}
    for f in fills:
        s = f.get("strategy") or "unknown"
        by_strategy[s] = by_strategy.get(s, 0) + 1

    return {
        "rundate": rundate,
        "generated_at": time.time(),
        "totals": {
            "symbols": len(pos),
            "realised_pnl": round(realised_total, 2),
            "unrealised_pnl": round(unrealised_total, 2),
            "gross_pnl": round(gross_pnl, 2),
            "total_fills": len(fills),
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "avg_slippage": round(avg_slippage, 4),
            "approved": risk.get("approved", 0),
            "rejected": risk.get("rejected", 0),
            "signals_emitted": signals.get("signals_emitted", 0),
        },
        "positions": pos,
        "by_strategy": by_strategy,
        "risk": {
            "max_position": risk.get("max_position"),
            "max_notional": risk.get("max_notional"),
            "max_daily_loss": risk.get("max_daily_loss"),
        },
        "strategies": signals.get("enabled_strategies", []),
        "market_quality": market_quality,
        "fill_count": len(fills),
        "trades_source": orders.get("source"),
        "trades_files": orders.get("files"),
        "positions_source": positions.get("source"),
    }


@task(memory="256m", cpu=0.5)
def persist_to_blob(report: Dict[str, Any]) -> Dict[str, Any]:
    """Write the report to Datatailr blob storage and return the path."""
    from datatailr import Blob

    rundate = report.get("rundate", _rundate())
    path = f"{BLOB_BUCKET}/{rundate}.json"
    body = json.dumps(report, default=str, indent=2).encode("utf-8")
    Blob().put(path, body)
    logger.info("EOD report persisted to blob:%s (%d bytes)", path, len(body))
    return {
        "path": path,
        "size_bytes": len(body),
        "totals": report.get("totals", {}),
    }


@task()
def broadcast_eod_complete(persisted: Dict[str, Any]) -> Dict[str, Any]:
    """Inject ``eod_complete`` into the live stream via notification-bus."""
    totals = persisted.get("totals", {})
    summary = (
        f"PnL ${totals.get('gross_pnl', 0):,.2f} | "
        f"fills {totals.get('total_fills', 0)} | "
        f"approved {totals.get('approved', 0)} / "
        f"rejected {totals.get('rejected', 0)}"
    )
    detail = json.dumps(
        {"path": persisted.get("path"), "totals": totals},
        default=str,
    )
    return broadcast(
        BUS_HOST,
        topic="system.eod_complete",
        kind="system",
        summary=summary,
        detail=detail,
        source="EOD Reconciliation",
    )
