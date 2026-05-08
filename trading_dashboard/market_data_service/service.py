"""Live market-data service.

Drives a Geometric-Brownian-Motion price for every symbol in the trading
universe and serves quotes over REST and Server-Sent Events.

This is the data source consumed by ``strategy_engine`` to mark positions and
by the dashboard's "Live Prices" tab.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import sys
import threading
import time

import numpy as np
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from sse_starlette.sse import EventSourceResponse

from trading_dashboard.universe import BY_SYMBOL, SYMBOLS, TickerInfo


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8090"))
TICK_INTERVAL_SEC = float(os.environ.get("MARKET_TICK_INTERVAL_SEC", "1.0"))

# A very loose annualization factor for our GBM step. It does not need to
# be financially exact -- we just want plausible-looking ticks.
SECONDS_PER_YEAR = 252 * 6.5 * 3600

log = logging.getLogger("market_data")
log.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [market] %(message)s"))
log.addHandler(_h)


class _Tape:
    """In-memory tape of last quote per symbol."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._quotes: dict[str, dict] = {}
        for info in BY_SYMBOL.values():
            self._quotes[info.symbol] = self._make_initial_quote(info)

    @staticmethod
    def _make_initial_quote(info: TickerInfo) -> dict:
        spread = max(info.initial_price * 0.0005, 0.01)
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        return {
            "ticker": info.symbol,
            "name": info.name,
            "sector": info.sector,
            "mid": round(info.initial_price, 4),
            "bid": round(info.initial_price - spread / 2, 4),
            "ask": round(info.initial_price + spread / 2, 4),
            "last": round(info.initial_price, 4),
            "open": round(info.initial_price, 4),
            "change": 0.0,
            "change_pct": 0.0,
            "volume": 0,
            "timestamp": now,
        }

    def step(self, dt_step: float) -> list[dict]:
        """Advance every symbol one step and return the updated quote list."""
        updated: list[dict] = []
        with self._lock:
            for sym, q in self._quotes.items():
                info = BY_SYMBOL[sym]
                sigma = info.annual_vol
                drift = -0.5 * sigma * sigma * dt_step
                shock = sigma * np.sqrt(dt_step) * np.random.standard_normal()
                new_mid = q["mid"] * float(np.exp(drift + shock))

                spread = max(new_mid * 0.0005, 0.01)
                trade_size = int(np.random.lognormal(mean=4.5, sigma=0.8)) * 100
                last = new_mid + (np.random.uniform(-1, 1) * spread / 2)

                q["mid"] = round(new_mid, 4)
                q["bid"] = round(new_mid - spread / 2, 4)
                q["ask"] = round(new_mid + spread / 2, 4)
                q["last"] = round(last, 4)
                q["change"] = round(new_mid - q["open"], 4)
                q["change_pct"] = round((new_mid - q["open"]) / q["open"] * 100, 4)
                q["volume"] += trade_size
                q["timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()
                updated.append(dict(q))
        return updated

    def snapshot(self) -> list[dict]:
        with self._lock:
            return [dict(q) for q in self._quotes.values()]

    def get(self, symbol: str) -> dict | None:
        with self._lock:
            q = self._quotes.get(symbol.upper())
            return dict(q) if q else None


tape = _Tape()
_stop = threading.Event()


def _ticker_loop() -> None:
    """Background thread driving the tape forward at TICK_INTERVAL_SEC."""
    last_log = 0.0
    while not _stop.is_set():
        tape.step(TICK_INTERVAL_SEC / SECONDS_PER_YEAR)
        now = time.time()
        if now - last_log > 30:
            log.info("ticked %d symbols (interval=%.2fs)", len(SYMBOLS), TICK_INTERVAL_SEC)
            last_log = now
        time.sleep(TICK_INTERVAL_SEC)


app = FastAPI(
    title="Trading Dashboard - Market Data",
    description="Synthetic live quotes (GBM) over REST and SSE.",
    version="0.1.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK\n")


@app.get("/tickers")
async def tickers() -> JSONResponse:
    return JSONResponse(list(SYMBOLS))


@app.get("/quotes")
async def quotes() -> JSONResponse:
    return JSONResponse(tape.snapshot())


@app.get("/quotes/{ticker}")
async def quote(ticker: str) -> JSONResponse:
    q = tape.get(ticker)
    if q is None:
        return JSONResponse({"error": f"unknown ticker {ticker}"}, status_code=404)
    return JSONResponse(q)


@app.get("/stream", include_in_schema=False)
async def stream() -> EventSourceResponse:
    """SSE feed publishing the entire tape after every tick."""

    async def gen():
        while True:
            yield json.dumps(tape.snapshot())
            await asyncio.sleep(TICK_INTERVAL_SEC)

    return EventSourceResponse(gen())


def main(port: int | None = None) -> None:
    p = int(port or PORT)
    threading.Thread(target=_ticker_loop, daemon=True).start()
    log.info("Market data service on %s:%s (tick=%.2fs)", HOST, p, TICK_INTERVAL_SEC)
    uvicorn.run(app, host=HOST, port=p, log_level="info")


if __name__ == "__main__":
    main(PORT)
