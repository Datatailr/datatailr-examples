"""Pre-market warm-up workflow.

Reads yesterday's portfolio from the Parquet store written by
``persistence-sink`` (via DuckDB) and the strategy / risk configuration
from KV, then seeds the running services through CTL frames so the
trading graph starts the new day in a known state.  Finally a
``market_open`` system event is broadcast through the notification-bus
so the dashboard knows the warm-up has finished.

Persistence-Parquet is the primary source for positions; if no Parquet
history exists yet (first run), we fall back to the JSON EOD report
that the EOD workflow also writes.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from datatailr import task

from reactive_graph.persistence import parquet_io
from reactive_graph.workflows.zmq_client import broadcast, ctl_request

logger = logging.getLogger("reactive_graph.workflows.warmup")

EXECUTION_HOST = os.environ.get("EXECUTION_HOST", "execution-simulator")
RISK_HOST = os.environ.get("RISK_HOST", "risk-engine")
SIGNAL_HOST = os.environ.get("SIGNAL_HOST", "signal-engine")
BUS_HOST = os.environ.get("BUS_HOST", "notification-bus")
BLOB_BUCKET = os.environ.get("REACTIVE_GRAPH_BLOB_BUCKET", "reactive_graph/eod")

DEFAULT_STRATEGIES = ["momentum", "mean_reversion"]
DEFAULT_RISK_LIMITS = {
    "max_position": 1000,
    "max_notional": 250_000,
    "max_daily_loss": 50_000,
}


def _maybe_parse_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


@task()
def load_previous_positions() -> Dict[str, Any]:
    """Load yesterday's positions from the persistence Parquet store.

    Reads ``positions/latest.parquet`` via DuckDB.  Falls back to the
    older JSON EOD report under ``BLOB_BUCKET`` when no Parquet
    snapshot exists yet (first-ever warm-up).
    """
    blob = parquet_io.get_blob()

    if blob.exists(parquet_io.POSITIONS_LATEST):
        try:
            rows = parquet_io.duckdb_query(
                "SELECT * FROM positions",
                blob=blob,
                views={"positions": [parquet_io.POSITIONS_LATEST]},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("duckdb read of positions/latest.parquet failed: %s", exc)
            rows = []
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
        logger.info(
            "loaded %d positions from %s", len(positions),
            parquet_io.POSITIONS_LATEST,
        )
        return {
            "ok": True,
            "source": parquet_io.POSITIONS_LATEST,
            "positions": positions,
        }

    # Fall back to the JSON EOD report.
    try:
        entries = blob.ls(BLOB_BUCKET + "/")
    except Exception as exc:  # noqa: BLE001
        logger.warning("blob.ls(%s) failed: %s", BLOB_BUCKET, exc)
        return {"ok": False, "positions": {}, "reason": "ls_failed"}

    json_entries = [e for e in entries if e.endswith(".json")]
    if not json_entries:
        logger.info("no historical positions under %s yet -- starting flat", BLOB_BUCKET)
        return {"ok": True, "positions": {}, "reason": "no_history"}

    latest = sorted(json_entries)[-1]
    raw = blob.get(latest)
    try:
        report = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("could not decode %s: %s", latest, exc)
        return {"ok": False, "positions": {}, "reason": "decode_failed"}

    positions = report.get("positions", {}) or {}
    logger.info(
        "loaded %d positions from JSON fallback %s", len(positions), latest,
    )
    return {
        "ok": True,
        "source": latest,
        "positions": positions,
        "rundate": report.get("rundate"),
    }


@task()
def load_strategy_config() -> Dict[str, Any]:
    """Read enabled strategies + risk limits from KV (with sane defaults)."""
    from datatailr import KV

    kv = KV()

    strategies = DEFAULT_STRATEGIES
    try:
        raw = _maybe_parse_json(kv.get("reactive_graph/strategies"))
        if isinstance(raw, list):
            strategies = [s for s in raw if isinstance(s, str)]
        elif isinstance(raw, dict) and "enabled" in raw:
            enabled = raw.get("enabled")
            if isinstance(enabled, list):
                strategies = [s for s in enabled if isinstance(s, str)]
    except Exception as exc:  # noqa: BLE001
        logger.info(
            "kv.get('reactive_graph/strategies') missing/invalid (%s); using defaults",
            exc,
        )

    risk_limits = dict(DEFAULT_RISK_LIMITS)
    try:
        raw = _maybe_parse_json(kv.get("reactive_graph/risk_limits"))
        if isinstance(raw, dict):
            for k, v in raw.items():
                if k in risk_limits:
                    risk_limits[k] = v
    except Exception as exc:  # noqa: BLE001
        logger.info(
            "kv.get('reactive_graph/risk_limits') missing/invalid (%s); using defaults",
            exc,
        )

    return {"strategies": strategies, "risk_limits": risk_limits}


@task()
def seed_execution_simulator(prev: Dict[str, Any]) -> Dict[str, Any]:
    """Push yesterday's positions into the execution-simulator."""
    positions = prev.get("positions", {}) or {}
    if not positions:
        return {"ok": True, "skipped": "no positions"}
    return ctl_request(
        EXECUTION_HOST, "seed_positions", {"positions": positions}
    )


@task()
def seed_risk_engine(config: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
    """Apply risk limits and seed the risk-engine with current positions."""
    limits = config.get("risk_limits", {}) or {}
    set_limits = ctl_request(RISK_HOST, "set_limits", limits)
    positions = prev.get("positions", {}) or {}
    seed = {"ok": True, "skipped": "no positions"}
    if positions:
        seed = ctl_request(RISK_HOST, "seed_positions", {"positions": positions})
    return {"set_limits": set_limits, "seed_positions": seed}


@task()
def enable_strategies(config: Dict[str, Any]) -> Dict[str, Any]:
    """Tell the signal-engine which strategies to run today."""
    strategies: List[str] = config.get("strategies", []) or []
    if not strategies:
        return {"ok": False, "error": "no strategies configured"}
    return ctl_request(
        SIGNAL_HOST, "enable_strategies", {"strategies": strategies}
    )


@task()
def broadcast_market_open(
    prev: Dict[str, Any],
    seed_exec: Dict[str, Any],
    seed_risk: Dict[str, Any],
    strategies: Dict[str, Any],
) -> Dict[str, Any]:
    """Inject ``market_open`` into the live stream via notification-bus."""
    rundate = (
        prev.get("rundate")
        or os.environ.get("DATATAILR_BATCH_ARG_RUNDATE")
        or time.strftime("%Y-%m-%d")
    )
    summary = (
        f"market open for {rundate} | "
        f"positions seeded: {len(prev.get('positions', {}) or {})} | "
        f"strategies: {','.join(strategies.get('enabled_strategies', []) or [])}"
    )
    detail = json.dumps(
        {
            "rundate": rundate,
            "seed_execution": seed_exec,
            "seed_risk": seed_risk,
            "strategies": strategies,
        },
        default=str,
    )
    return broadcast(
        BUS_HOST,
        topic="system.market_open",
        kind="system",
        summary=summary,
        detail=detail,
        source="Pre-Market Warmup",
    )
