import logging
import math
import random
import threading
from collections import deque
from datetime import datetime, timedelta

import csp
from csp import ts
from flask import Flask, jsonify

from csp_price_analytics.common.models import (
    ALL_SYMBOLS,
    INITIAL_PRICES,
    SYMBOLS,
    VOLATILITIES,
    WS_PORT_MARKET_DATA,
)
from csp_price_analytics.common.ws_server import WebSocketBroadcastServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_state = {
    "recent_ticks": deque(maxlen=500),
    "latest": {},
    "started_at": None,
    "tick_count": 0,
}

_ws_server: WebSocketBroadcastServer | None = None


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
        # Geometric Brownian motion step
        dt = 0.5  # ~0.5 second intervals
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
        }
        _state["latest"][symbol] = tick
        _state["recent_ticks"].append(tick)
        _state["tick_count"] += 1

        if _ws_server is not None:
            _ws_server.broadcast(tick)


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
# Flask REST API
# ---------------------------------------------------------------------------

flask_app = Flask(__name__)


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
        "ws_port": WS_PORT_MARKET_DATA,
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
        "ws_port": WS_PORT_MARKET_DATA,
    })


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port):
    global _ws_server
    _state["started_at"] = datetime.utcnow().isoformat()

    _ws_server = WebSocketBroadcastServer(WS_PORT_MARKET_DATA, name="market-data-ws")
    _ws_server.start()
    logger.info(f"WebSocket broadcast server started on port {WS_PORT_MARKET_DATA}")

    csp_thread = threading.Thread(target=_run_csp_engine, daemon=True, name="csp-engine")
    csp_thread.start()

    flask_app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
