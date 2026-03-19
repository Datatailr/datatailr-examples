import asyncio
import json
import logging
import math
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from queue import Queue, Empty

import csp
from csp import ts
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse
import uvicorn


HOST = "0.0.0.0"
PORT = 8081

PRICE_SERVER_URL = "http://price-server"
if os.getenv("DATATAILR_JOB_TYPE", "workstation") in ("workstation", ""):
    PRICE_SERVER_URL = "http://localhost:8080"

log_format = logging.Formatter("[%(asctime)s] [%(levelname)s] - %(message)s")
log = logging.getLogger("Price processing")
log.setLevel(logging.INFO)
log_handler = logging.StreamHandler(sys.stdout)
log_handler.setLevel(logging.INFO)
log_handler.setFormatter(log_format)
log.addHandler(log_handler)


# ---------------------------------------------------------------------------
# Shared state: CSP graph writes here, FastAPI reads
# ---------------------------------------------------------------------------

analytics_store: dict[str, dict] = {}
analytics_queue: Queue = Queue(maxsize=10_000)


# ---------------------------------------------------------------------------
# CSP nodes
# ---------------------------------------------------------------------------

@csp.node
def vwap(price: ts[float], size: ts[float]) -> ts[float]:
    with csp.state():
        s_pv = 0.0
        s_vol = 0.0
    if csp.ticked(price) and csp.valid(price, size):
        s_pv += price * size
        s_vol += size
        if s_vol > 0:
            return round(s_pv / s_vol, 4)


@csp.node
def ema(price: ts[float], span: int) -> ts[float]:
    with csp.state():
        s_ema = None
        s_alpha = 2.0 / (span + 1)
    if csp.ticked(price):
        if s_ema is None:
            s_ema = price
        else:
            s_ema = s_alpha * price + (1 - s_alpha) * s_ema
        return round(s_ema, 4)


@csp.node
def ema_crossover_signal(fast: ts[float], slow: ts[float]) -> ts[str]:
    with csp.state():
        s_prev_signal = ""
    if csp.valid(fast, slow):
        signal = "buy" if fast > slow else "sell"
        if signal != s_prev_signal:
            s_prev_signal = signal
            return signal


VOLATILITY_WINDOW = 50


@csp.node
def rolling_volatility(price: ts[float]) -> ts[float]:
    with csp.state():
        s_prices = deque(maxlen=VOLATILITY_WINDOW)
    if csp.ticked(price):
        s_prices.append(price)
        if len(s_prices) >= 3:
            returns = []
            prices_list = list(s_prices)
            for i in range(1, len(prices_list)):
                if prices_list[i - 1] > 0:
                    returns.append(math.log(prices_list[i] / prices_list[i - 1]))
            if len(returns) >= 2:
                mean_r = sum(returns) / len(returns)
                var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
                return round(math.sqrt(var), 6)


IMBALANCE_WINDOW = 100


@csp.node
def trade_imbalance(size: ts[float], side: ts[str]) -> ts[float]:
    with csp.state():
        s_trades = deque(maxlen=IMBALANCE_WINDOW)
    if csp.ticked(size) and csp.valid(size, side):
        s_trades.append((size, side))
        buy_vol = sum(s for s, sd in s_trades if sd == "buy")
        total_vol = sum(s for s, _ in s_trades)
        if total_vol > 0:
            return round(buy_vol / total_vol, 4)


SPREAD_WINDOW = 100


@csp.node
def spread_stats(spread: ts[float]) -> csp.Outputs(
    spread_min=ts[float], spread_max=ts[float], spread_mean=ts[float]
):
    with csp.state():
        s_spreads = deque(maxlen=SPREAD_WINDOW)
    if csp.ticked(spread):
        s_spreads.append(spread)
        if len(s_spreads) >= 1:
            csp.output(
                spread_min=round(min(s_spreads), 4),
                spread_max=round(max(s_spreads), 4),
                spread_mean=round(sum(s_spreads) / len(s_spreads), 4),
            )


@csp.node
def publish_analytics(
    ticker_name: str,
    trade_price: ts[float],
    vwap_val: ts[float],
    ema_fast: ts[float],
    ema_slow: ts[float],
    signal: ts[str],
    volatility: ts[float],
    imbalance: ts[float],
    spread_min: ts[float],
    spread_max: ts[float],
    spread_mean: ts[float],
    last_spread: ts[float],
):
    with csp.alarms():
        publish_timer = csp.alarm(bool)
    with csp.state():
        s_snapshot = {"ticker": ticker_name}

    with csp.start():
        csp.schedule_alarm(publish_timer, timedelta(seconds=1), True)
        csp.make_passive(trade_price)
        csp.make_passive(vwap_val)
        csp.make_passive(ema_fast)
        csp.make_passive(ema_slow)
        csp.make_passive(signal)
        csp.make_passive(volatility)
        csp.make_passive(imbalance)
        csp.make_passive(spread_min)
        csp.make_passive(spread_max)
        csp.make_passive(spread_mean)
        csp.make_passive(last_spread)

    if csp.ticked(publish_timer):
        csp.schedule_alarm(publish_timer, timedelta(seconds=1), True)

        if csp.valid(trade_price):
            s_snapshot["last_price"] = trade_price
        if csp.valid(vwap_val):
            s_snapshot["vwap"] = vwap_val
        if csp.valid(ema_fast):
            s_snapshot["ema_fast"] = ema_fast
        if csp.valid(ema_slow):
            s_snapshot["ema_slow"] = ema_slow
        if csp.valid(signal):
            s_snapshot["signal"] = signal
        if csp.valid(volatility):
            s_snapshot["volatility"] = volatility
        if csp.valid(imbalance):
            s_snapshot["trade_imbalance"] = imbalance
        if csp.valid(spread_min):
            s_snapshot["spread_min"] = spread_min
        if csp.valid(spread_max):
            s_snapshot["spread_max"] = spread_max
        if csp.valid(spread_mean):
            s_snapshot["spread_mean"] = spread_mean
        if csp.valid(last_spread):
            s_snapshot["spread"] = last_spread

        s_snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()

        analytics_store[ticker_name] = dict(s_snapshot)
        try:
            analytics_queue.put_nowait(dict(s_snapshot))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# CSP graph: per-ticker analytics pipeline
# ---------------------------------------------------------------------------

@csp.node
def demux_field_float(event: ts[dict], field: str) -> ts[float]:
    if csp.ticked(event):
        val = event.get(field)
        if val is not None:
            return float(val)


@csp.node
def demux_field_str(event: ts[dict], field: str) -> ts[str]:
    if csp.ticked(event):
        val = event.get(field)
        if val is not None:
            return str(val)


@csp.node
def filter_event_type(events: ts[dict], event_type: str) -> ts[dict]:
    if csp.ticked(events):
        if events.get("type") == event_type:
            return events


@csp.graph
def ticker_analytics(ticker_name: str, events: ts[dict]):
    trades = filter_event_type(events, "trade")
    quotes = filter_event_type(events, "quote")

    trade_price = demux_field_float(trades, "price")
    trade_size = demux_field_float(trades, "size")
    trade_side = demux_field_str(trades, "side")
    quote_spread = demux_field_float(quotes, "spread")

    vwap_val = vwap(trade_price, trade_size)
    ema_fast = ema(trade_price, 10)
    ema_slow = ema(trade_price, 50)
    signal = ema_crossover_signal(ema_fast, ema_slow)
    vol = rolling_volatility(trade_price)
    imb = trade_imbalance(trade_size, trade_side)
    ss = spread_stats(quote_spread)

    publish_analytics(
        ticker_name,
        trade_price, vwap_val, ema_fast, ema_slow, signal,
        vol, imb,
        ss.spread_min, ss.spread_max, ss.spread_mean,
        quote_spread,
    )


# ---------------------------------------------------------------------------
# Engine runner
# ---------------------------------------------------------------------------

def _drain_queue(q, max_items):
    batch = []
    try:
        for _ in range(max_items):
            batch.append(q.get_nowait())
    except Exception:
        pass
    return batch


@csp.node
def _sse_source_batched(url: str) -> ts[[dict]]:
    """Drains the SSE queue each poll cycle and emits a list of events."""
    with csp.alarms():
        poll = csp.alarm(bool)
    with csp.state():
        s_thread = None
        s_running = False
        s_queue = None

    with csp.start():
        import queue as _queue_mod
        s_queue = _queue_mod.Queue(maxsize=50_000)
        s_running = True

        def _run_feed():
            while s_running:
                try:
                    log.info(f"Connecting to SSE feed at {url}/stream")
                    with requests.get(
                        f"{url}/stream",
                        stream=True,
                        timeout=(10, None),
                        headers={"Accept": "text/event-stream"},
                    ) as resp:
                        resp.raise_for_status()
                        for line in resp.iter_lines(decode_unicode=True):
                            if not s_running:
                                break
                            if not line or not line.startswith("data:"):
                                continue
                            raw = line[5:].strip()
                            if not raw:
                                continue
                            try:
                                event = json.loads(raw)
                            except json.JSONDecodeError:
                                continue
                            try:
                                s_queue.put_nowait(event)
                            except Exception:
                                pass
                except Exception as exc:
                    log.warning(f"SSE feed error: {exc} – retrying in 2s")
                    time.sleep(2)

        s_thread = threading.Thread(target=_run_feed, daemon=True)
        s_thread.start()
        csp.schedule_alarm(poll, timedelta(milliseconds=50), True)

    with csp.stop():
        s_running = False

    if csp.ticked(poll):
        csp.schedule_alarm(poll, timedelta(milliseconds=50), True)
        batch = _drain_queue(s_queue, 500)
        if batch:
            return batch


@csp.node
def filter_ticker(events: ts[dict], ticker: str) -> ts[dict]:
    if csp.ticked(events):
        if events.get("ticker") == ticker:
            return events


def run_csp_engine(url: str, tickers_list: list):
    log.info(f"Starting CSP engine for tickers: {tickers_list}")

    @csp.graph
    def analytics_main():
        all_events = csp.unroll(_sse_source_batched(url))
        for ticker in tickers_list:
            ticker_events = filter_ticker(all_events, ticker)
            ticker_analytics(ticker, ticker_events)

    csp.run(analytics_main, starttime=datetime.now(timezone.utc), endtime=timedelta(days=365), realtime=True)


def start_engine_thread(url: str, tickers_list: list):
    t = threading.Thread(target=run_csp_engine, args=(url, tickers_list), daemon=True)
    t.start()
    return t


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    initial_tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
    try:
        resp = requests.get(f"{PRICE_SERVER_URL}/tickers", timeout=5)
        if resp.ok:
            initial_tickers = resp.json()
            log.info(f"Fetched tickers from price server: {initial_tickers}")
    except Exception:
        log.warning("Could not fetch tickers from price server, using defaults")
    start_engine_thread(PRICE_SERVER_URL, initial_tickers)


@app.get("/__health_check__.html")
async def health_check():
    return PlainTextResponse("OK\n")


@app.get("/analytics")
async def get_all_analytics():
    log.info(f"GET /analytics -> {list(analytics_store.keys())}")
    return JSONResponse(analytics_store)


@app.get("/analytics/{ticker}")
async def get_ticker_analytics(ticker: str):
    ticker = ticker.upper()
    data = analytics_store.get(ticker)
    if data is None:
        log.info(f"GET /analytics/{ticker} -> not found")
        return JSONResponse({"error": f"no analytics for {ticker}"}, status_code=404)
    log.info(f"GET /analytics/{ticker} -> ok")
    return JSONResponse(data)


async def _analytics_event_generator():
    while True:
        try:
            item = analytics_queue.get(timeout=0.1)
            yield json.dumps(item)
        except Empty:
            await asyncio.sleep(0.05)
        except Exception:
            await asyncio.sleep(0.1)


@app.get("/stream")
async def stream_analytics():
    return EventSourceResponse(_analytics_event_generator())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port: int = PORT):
    log.info(f"Price processing service on {HOST}:{port}")
    uvicorn.run(app, host=HOST, port=int(port), log_level="info")


if __name__ == "__main__":
    main(PORT)
