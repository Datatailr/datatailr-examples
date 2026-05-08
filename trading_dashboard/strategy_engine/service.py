"""Strategy engine service.

Polls the market-data service for live quotes, marks every position to
market, computes per-strategy PnL, and serves both the latest snapshot
and a short rolling history (for the dashboard's PnL chart) over REST.

Endpoints:

    GET /strategies              - list of strategies + per-position metrics
    GET /strategies/{name}       - one strategy detail
    GET /pnl/summary             - aggregate PnL + capital metrics
    GET /pnl/history             - last N PnL snapshots per strategy and total
    GET /health
"""

from __future__ import annotations

import collections
import datetime as dt
import logging
import os
import sys
import threading
import time
import urllib.parse

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

from trading_dashboard.strategy_engine.strategies import STRATEGIES, STRATEGIES_BY_NAME


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8091"))

# When running locally on a workstation the market-data service is on
# localhost; when running on Datatailr it is reachable via its
# internal hostname (derived from the Service name).
_JOB_TYPE = os.environ.get("DATATAILR_JOB_TYPE", "")
_DEFAULT_MD = (
    "http://localhost:8090"
    if _JOB_TYPE in ("workstation", "")
    else "http://trading-market-data"
)
MARKET_DATA_URL = os.environ.get("MARKET_DATA_URL", _DEFAULT_MD).rstrip("/")

POLL_INTERVAL_SEC = float(os.environ.get("ENGINE_POLL_INTERVAL_SEC", "1.0"))
HISTORY_LEN = int(os.environ.get("ENGINE_HISTORY_LEN", "600"))   # 10 min @ 1s

log = logging.getLogger("strategy_engine")
log.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [engine] %(message)s"))
log.addHandler(_h)


# ----------------------------------------------------------------------------
# In-memory state
# ----------------------------------------------------------------------------

_state_lock = threading.Lock()
_quotes: dict[str, dict] = {}                                    # ticker -> last quote
_strategy_history: dict[str, collections.deque] = {              # name -> deque[(ts, pnl)]
    s.name: collections.deque(maxlen=HISTORY_LEN) for s in STRATEGIES
}
_total_history: collections.deque = collections.deque(maxlen=HISTORY_LEN)
_last_poll_ts: float = 0.0
_last_poll_error: str | None = None
_stop = threading.Event()


# ----------------------------------------------------------------------------
# Pricing & PnL math
# ----------------------------------------------------------------------------

def _price_for(ticker: str) -> float | None:
    q = _quotes.get(ticker)
    if not q:
        return None
    for k in ("last", "mid"):
        v = q.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


def _build_strategy_view(strategy) -> dict:
    positions_out: list[dict] = []
    market_value = 0.0
    pnl = 0.0
    gross_exposure = 0.0
    long_exposure = 0.0
    short_exposure = 0.0

    for pos in strategy.positions:
        last = _price_for(pos.ticker)
        if last is None:
            mv = pos.quantity * pos.avg_price
            ppnl = 0.0
        else:
            mv = pos.quantity * last
            ppnl = (last - pos.avg_price) * pos.quantity
        market_value += mv
        pnl += ppnl
        exposure = abs(mv)
        gross_exposure += exposure
        if pos.quantity >= 0:
            long_exposure += mv
        else:
            short_exposure += mv

        positions_out.append({
            "ticker": pos.ticker,
            "quantity": pos.quantity,
            "avg_price": round(pos.avg_price, 4),
            "last_price": round(last, 4) if last is not None else None,
            "market_value": round(mv, 2),
            "pnl": round(ppnl, 2),
            "pnl_pct": (
                round((last - pos.avg_price) / pos.avg_price * 100, 4)
                if last is not None
                else 0.0
            ),
        })

    return {
        "name": strategy.name,
        "description": strategy.description,
        "style": strategy.style,
        "positions": positions_out,
        "market_value": round(market_value, 2),
        "pnl": round(pnl, 2),
        "gross_exposure": round(gross_exposure, 2),
        "long_exposure": round(long_exposure, 2),
        "short_exposure": round(short_exposure, 2),
        "net_exposure": round(long_exposure + short_exposure, 2),
    }


def _compute_all() -> tuple[list[dict], dict]:
    views = [_build_strategy_view(s) for s in STRATEGIES]
    total = {
        "pnl": round(sum(v["pnl"] for v in views), 2),
        "market_value": round(sum(v["market_value"] for v in views), 2),
        "gross_exposure": round(sum(v["gross_exposure"] for v in views), 2),
        "long_exposure": round(sum(v["long_exposure"] for v in views), 2),
        "short_exposure": round(sum(v["short_exposure"] for v in views), 2),
        "net_exposure": round(sum(v["net_exposure"] for v in views), 2),
    }
    return views, total


# ----------------------------------------------------------------------------
# Market-data poller
# ----------------------------------------------------------------------------

def _poll_loop() -> None:
    global _last_poll_ts, _last_poll_error
    url = f"{MARKET_DATA_URL}/quotes"
    sess = requests.Session()
    while not _stop.is_set():
        try:
            r = sess.get(url, timeout=5)
            r.raise_for_status()
            payload = r.json()
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
            with _state_lock:
                for q in payload:
                    sym = q.get("ticker")
                    if sym:
                        _quotes[sym] = q
                _last_poll_ts = time.time()
                _last_poll_error = None

                views, total = _compute_all()
                ts_iso = now_iso
                for v in views:
                    _strategy_history[v["name"]].append((ts_iso, v["pnl"]))
                _total_history.append((ts_iso, total["pnl"]))
        except Exception as exc:
            with _state_lock:
                _last_poll_error = str(exc)
            log.warning("poll %s failed: %s", url, exc)
        time.sleep(POLL_INTERVAL_SEC)


# ----------------------------------------------------------------------------
# HTTP API
# ----------------------------------------------------------------------------

app = FastAPI(
    title="Trading Dashboard - Strategy Engine",
    description="Live-marked strategy book and PnL.",
    version="0.1.0",
)


@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK\n")


@app.get("/strategies")
async def strategies() -> JSONResponse:
    with _state_lock:
        views, total = _compute_all()
        return JSONResponse({
            "strategies": views,
            "total": total,
            "as_of": dt.datetime.now(dt.timezone.utc).isoformat(),
            "market_data_age_sec": (
                round(time.time() - _last_poll_ts, 2) if _last_poll_ts else None
            ),
            "market_data_error": _last_poll_error,
        })


@app.get("/strategies/{name}")
async def strategy(name: str) -> JSONResponse:
    decoded = urllib.parse.unquote(name)
    s = STRATEGIES_BY_NAME.get(decoded)
    if s is None:
        raise HTTPException(404, f"unknown strategy {decoded}")
    with _state_lock:
        return JSONResponse(_build_strategy_view(s))


@app.get("/pnl/summary")
async def pnl_summary() -> JSONResponse:
    with _state_lock:
        _, total = _compute_all()
        return JSONResponse({
            **total,
            "as_of": dt.datetime.now(dt.timezone.utc).isoformat(),
            "strategy_count": len(STRATEGIES),
        })


@app.get("/pnl/history")
async def pnl_history() -> JSONResponse:
    with _state_lock:
        out = {
            "total": [{"ts": ts, "pnl": v} for ts, v in _total_history],
            "strategies": {
                name: [{"ts": ts, "pnl": v} for ts, v in dq]
                for name, dq in _strategy_history.items()
            },
        }
    return JSONResponse(out)


def main(port: int | None = None) -> None:
    p = int(port or PORT)
    threading.Thread(target=_poll_loop, daemon=True).start()
    log.info("Strategy engine on %s:%s -> market data %s", HOST, p, MARKET_DATA_URL)
    uvicorn.run(app, host=HOST, port=p, log_level="info")


if __name__ == "__main__":
    main(PORT)
