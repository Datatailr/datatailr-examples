import logging
import math
import random
import threading
from collections import deque
from datetime import datetime, timedelta

import csp
from csp import ts
from flask import Flask, jsonify
from flask_sock import Sock

from csp_price_analytics.common.models import (
    ALL_SYMBOLS,
    INITIAL_PRICES,
    SYMBOLS,
    VOLATILITIES,
    WS_PATH,
)
from csp_price_analytics.common.ws_server import WebSocketBroadcaster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_state = {
    "recent_ticks": deque(maxlen=500),
    "latest": {},
    "started_at": None,
    "tick_count": 0,
}

_broadcaster = WebSocketBroadcaster(name="market-data-ws")


# ---------------------------------------------------------------------------
# CSP graph: synthetic market data generation
# ---------------------------------------------------------------------------

@csp.node
def price_generator(
    timer: ts[bool],
    symbol: str,
    initial_price: float,
    volatility: float,
) -> csp.Outputs(
    price=ts[float],
    volume=ts[float],
    bid=ts[float],
    ask=ts[float],
):
    with csp.state():
        s_price = initial_price
        s_drift = random.uniform(-0.0001, 0.0001)

    if csp.ticked(timer):
        dt = 0.5
        z = random.gauss(0, 1)
        s_price *= math.exp((s_drift - 0.5 * volatility ** 2) * dt + volatility * math.sqrt(dt) * z)

        half_spread = s_price * random.uniform(0.0001, 0.001)
        bid = round(s_price - half_spread, 6)
        ask = round(s_price + half_spread, 6)
        vol = round(random.lognormvariate(math.log(1000), 1.0), 2)

        csp.output(price=round(s_price, 6), volume=vol, bid=bid, ask=ask)


@csp.node
def tick_publisher(
    symbol: str,
    price: ts[float],
    volume: ts[float],
    bid: ts[float],
    ask: ts[float],
):
    if csp.ticked(price) and csp.valid(price, volume, bid, ask):
        tick = {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "bid": bid,
            "ask": ask,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "csp:market_data_graph/price_generator",
        }
        _state["latest"][symbol] = tick
        _state["recent_ticks"].append(tick)
        _state["tick_count"] += 1
        _broadcaster.broadcast(tick)


@csp.graph
def market_data_graph():
    for symbol in ALL_SYMBOLS:
        interval = timedelta(milliseconds=random.randint(300, 800))
        timer = csp.timer(interval, True)
        gen = price_generator(timer, symbol, INITIAL_PRICES[symbol], VOLATILITIES[symbol])
        tick_publisher(symbol, gen.price, gen.volume, gen.bid, gen.ask)


def _run_csp_engine():
    logger.info("Starting CSP market data engine in realtime mode")
    try:
        csp.run(
            market_data_graph,
            starttime=datetime.utcnow(),
            endtime=timedelta(days=365),
            realtime=True,
        )
    except Exception:
        logger.exception("CSP engine error")


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
        "service": "Market Data Generator",
        "status": "running",
        "symbols": len(ALL_SYMBOLS),
        "tick_count": _state["tick_count"],
        "started_at": _state["started_at"],
    })


@flask_app.route("/status")
def status():
    return jsonify({
        "status": "running",
        "tick_count": _state["tick_count"],
        "symbols_active": len(_state["latest"]),
        "symbols_total": len(ALL_SYMBOLS),
        "started_at": _state["started_at"],
    })


@flask_app.route("/symbols")
def symbols():
    return jsonify(SYMBOLS)


@flask_app.route("/latest")
def latest():
    return jsonify(_state["latest"])


@flask_app.route("/ticks")
def recent_ticks():
    return jsonify(list(_state["recent_ticks"])[-100:])


@flask_app.route("/config")
def config():
    return jsonify({
        "symbols": ALL_SYMBOLS,
        "initial_prices": INITIAL_PRICES,
        "volatilities": VOLATILITIES,
    })


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port):
    _state["started_at"] = datetime.utcnow().isoformat()

    csp_thread = threading.Thread(target=_run_csp_engine, daemon=True, name="csp-engine")
    csp_thread.start()

    logger.info(f"Market Data Generator starting on port {port} (REST + WebSocket on {WS_PATH})")
    flask_app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
