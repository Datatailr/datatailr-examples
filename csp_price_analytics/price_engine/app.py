import logging
import math
import threading
import time
from collections import deque
from datetime import datetime, timedelta

import csp
from csp import ts
from flask import Flask, jsonify, request
from flask_sock import Sock

from csp_price_analytics.common.models import ALL_SYMBOLS, WS_PATH
from csp_price_analytics.common.ws_adapter import WebSocketAdapterManager, get_ws_stats
from csp_price_analytics.common.ws_server import WebSocketBroadcaster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_state = {
    "analytics": {},
    "signals": deque(maxlen=200),
    "started_at": None,
    "tick_count": 0,
}

_broadcaster = WebSocketBroadcaster(name="price-engine-ws")

MARKET_DATA_WS_URL = "ws://market-data-generator/ws"

SMA_SHORT_WINDOW = 10
SMA_LONG_WINDOW = 30
RSI_PERIOD = 14
BOLLINGER_WINDOW = 20
BOLLINGER_STD = 2.0


# ---------------------------------------------------------------------------
# CSP graph: price analytics engine
# ---------------------------------------------------------------------------

@csp.node
def compute_analytics(
    raw_tick: ts[dict],
    symbol: str,
) -> csp.Outputs(
    analytics=ts[dict],
    signal=ts[dict],
):
    with csp.state():
        s_prices = deque(maxlen=max(SMA_LONG_WINDOW, BOLLINGER_WINDOW, RSI_PERIOD + 1))
        s_volumes = deque(maxlen=SMA_LONG_WINDOW)
        s_cum_pv = 0.0
        s_cum_vol = 0.0
        s_prev_sma_short = None
        s_prev_sma_long = None
        s_gains = deque(maxlen=RSI_PERIOD)
        s_losses = deque(maxlen=RSI_PERIOD)

    if csp.ticked(raw_tick):
        price = raw_tick["price"]
        volume = raw_tick.get("volume", 0.0)

        if len(s_prices) > 0:
            change = price - s_prices[-1]
            s_gains.append(max(change, 0.0))
            s_losses.append(max(-change, 0.0))

        s_prices.append(price)
        s_volumes.append(volume)
        s_cum_pv += price * volume
        s_cum_vol += volume

        vwap = s_cum_pv / s_cum_vol if s_cum_vol > 0 else price

        n = len(s_prices)
        prices_list = list(s_prices)

        sma_short = sum(prices_list[-SMA_SHORT_WINDOW:]) / min(n, SMA_SHORT_WINDOW)
        sma_long = sum(prices_list[-SMA_LONG_WINDOW:]) / min(n, SMA_LONG_WINDOW)

        alpha = 2.0 / (SMA_SHORT_WINDOW + 1)
        ema_short = prices_list[0]
        for p in prices_list[1:]:
            ema_short = alpha * p + (1 - alpha) * ema_short

        rsi = 50.0
        if len(s_gains) >= RSI_PERIOD:
            avg_gain = sum(s_gains) / RSI_PERIOD
            avg_loss = sum(s_losses) / RSI_PERIOD
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
            else:
                rsi = 100.0

        if n >= BOLLINGER_WINDOW:
            bb_prices = prices_list[-BOLLINGER_WINDOW:]
            bb_mean = sum(bb_prices) / BOLLINGER_WINDOW
            bb_std = math.sqrt(sum((p - bb_mean) ** 2 for p in bb_prices) / BOLLINGER_WINDOW)
        else:
            bb_mean = sma_short
            bb_std = 0.0
        bollinger_upper = bb_mean + BOLLINGER_STD * bb_std
        bollinger_lower = bb_mean - BOLLINGER_STD * bb_std

        spread = raw_tick.get("ask", price) - raw_tick.get("bid", price)

        analytics_data = {
            "symbol": symbol,
            "price": round(price, 6),
            "vwap": round(vwap, 6),
            "sma_short": round(sma_short, 6),
            "sma_long": round(sma_long, 6),
            "ema_short": round(ema_short, 6),
            "rsi": round(rsi, 2),
            "bollinger_upper": round(bollinger_upper, 6),
            "bollinger_lower": round(bollinger_lower, 6),
            "spread": round(spread, 6),
            "timestamp": datetime.utcnow().isoformat(),
            "source": "csp:price_engine_graph/compute_analytics <- ws://market-data-generator/ws",
        }
        csp.output(analytics=analytics_data)

        if s_prev_sma_short is not None and s_prev_sma_long is not None and n >= SMA_LONG_WINDOW:
            crossed_up = s_prev_sma_short <= s_prev_sma_long and sma_short > sma_long
            crossed_down = s_prev_sma_short >= s_prev_sma_long and sma_short < sma_long

            if crossed_up or crossed_down:
                sig_type = "BUY" if crossed_up else "SELL"
                reason = "SMA crossover" + (" (bullish)" if crossed_up else " (bearish)")
                strength = min(abs(sma_short - sma_long) / sma_long * 1000, 1.0)
                signal_data = {
                    "symbol": symbol,
                    "signal_type": sig_type,
                    "price": round(price, 6),
                    "reason": reason,
                    "strength": round(strength, 4),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                csp.output(signal=signal_data)

            if rsi > 70:
                signal_data = {
                    "symbol": symbol,
                    "signal_type": "SELL",
                    "price": round(price, 6),
                    "reason": f"RSI overbought ({rsi:.1f})",
                    "strength": round(min((rsi - 70) / 30, 1.0), 4),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                csp.output(signal=signal_data)
            elif rsi < 30:
                signal_data = {
                    "symbol": symbol,
                    "signal_type": "BUY",
                    "price": round(price, 6),
                    "reason": f"RSI oversold ({rsi:.1f})",
                    "strength": round(min((30 - rsi) / 30, 1.0), 4),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                csp.output(signal=signal_data)

        s_prev_sma_short = sma_short
        s_prev_sma_long = sma_long


@csp.node
def analytics_publisher(symbol: str, analytics: ts[dict]):
    if csp.ticked(analytics):
        _state["analytics"][symbol] = analytics
        _state["tick_count"] += 1
        _broadcaster.broadcast({"type": "analytics", "data": analytics})


@csp.node
def signal_publisher(symbol: str, signal: ts[dict]):
    if csp.ticked(signal):
        _state["signals"].append(signal)
        _broadcaster.broadcast({"type": "signal", "data": signal})


@csp.graph
def price_engine_graph():
    adapter = WebSocketAdapterManager(MARKET_DATA_WS_URL)
    for symbol in ALL_SYMBOLS:
        raw = adapter.subscribe(symbol, push_mode=csp.PushMode.LAST_VALUE)
        result = compute_analytics(raw, symbol)
        analytics_publisher(symbol, result.analytics)
        signal_publisher(symbol, result.signal)


def _run_csp_engine():
    logger.info("Starting CSP price engine in realtime mode")
    time.sleep(3)
    try:
        csp.run(
            price_engine_graph,
            starttime=datetime.utcnow(),
            endtime=timedelta(days=365),
            realtime=True,
        )
    except Exception:
        logger.exception("CSP price engine error")


# ---------------------------------------------------------------------------
# Flask REST + WebSocket API (single port)
# ---------------------------------------------------------------------------

flask_app = Flask(__name__)
sock = Sock(flask_app)


@sock.route(WS_PATH)
def ws_handler(ws):
    _broadcaster.handle(ws)


@flask_app.route("/__health_check__.html")
def health_check():
    return "OK\n"


@flask_app.route("/")
def index():
    return jsonify({
        "service": "Price Engine",
        "status": "running",
        "symbols_tracked": len(_state["analytics"]),
        "tick_count": _state["tick_count"],
        "started_at": _state["started_at"],
    })


@flask_app.route("/status")
def status():
    return jsonify({
        "status": "running",
        "symbols_tracked": len(_state["analytics"]),
        "tick_count": _state["tick_count"],
        "signals_generated": len(_state["signals"]),
        "started_at": _state["started_at"],
    })


@flask_app.route("/snapshot")
def snapshot():
    return jsonify(_state["analytics"])


@flask_app.route("/signals")
def signals():
    limit = request.args.get("limit", 50, type=int)
    return jsonify(list(_state["signals"])[-limit:])


@flask_app.route("/symbols")
def symbols():
    return jsonify(list(_state["analytics"].keys()))


@flask_app.route("/ws-stats")
def ws_stats():
    return jsonify(get_ws_stats())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port):
    _state["started_at"] = datetime.utcnow().isoformat()

    csp_thread = threading.Thread(target=_run_csp_engine, daemon=True, name="csp-price-engine")
    csp_thread.start()

    logger.info(f"Price Engine starting on port {port} (REST + WebSocket on {WS_PATH})")
    flask_app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
