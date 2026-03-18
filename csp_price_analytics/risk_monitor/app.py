import logging
import math
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta

import csp
from csp import ts
from flask import Flask, jsonify, request

from csp_price_analytics.common.models import (
    ALL_SYMBOLS,
    INITIAL_PRICES,
    WS_PORT_MARKET_DATA,
    WS_PORT_PRICE_ENGINE,
)
from csp_price_analytics.common.ws_adapter import WebSocketAdapterManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MARKET_DATA_WS_URL = f"ws://market-data-generator:{WS_PORT_MARKET_DATA}"
PRICE_ENGINE_WS_URL = f"ws://price-engine:{WS_PORT_PRICE_ENGINE}"

SPIKE_THRESHOLD_PCT = 2.0
HIGH_VOL_THRESHOLD = 0.05
DRAWDOWN_ALERT_PCT = 5.0

_state = {
    "alerts": deque(maxlen=500),
    "active_alerts": {},
    "portfolio": {},
    "risk_metrics": {
        "total_pnl": 0.0,
        "max_drawdown": 0.0,
        "current_drawdown": 0.0,
        "volatility": 0.0,
        "sharpe_ratio": 0.0,
        "num_positions": 0,
    },
    "started_at": None,
    "alert_count": 0,
}


# ---------------------------------------------------------------------------
# CSP graph: risk monitoring
# ---------------------------------------------------------------------------

@csp.node
def track_position(
    raw_tick: ts[dict],
    symbol: str,
    initial_price: float,
) -> csp.Outputs(
    pnl=ts[float],
    drawdown=ts[float],
    volatility=ts[float],
):
    with csp.state():
        s_entry = initial_price
        s_peak_price = initial_price
        s_returns = deque(maxlen=100)
        s_prev_price = initial_price

    if csp.ticked(raw_tick):
        price = raw_tick["price"]

        pnl = (price - s_entry) / s_entry * 100.0

        s_peak_price = max(s_peak_price, price)
        drawdown = (s_peak_price - price) / s_peak_price * 100.0

        ret = (price - s_prev_price) / s_prev_price if s_prev_price > 0 else 0.0
        s_returns.append(ret)
        s_prev_price = price

        vol = 0.0
        if len(s_returns) > 1:
            mean_r = sum(s_returns) / len(s_returns)
            var = sum((r - mean_r) ** 2 for r in s_returns) / (len(s_returns) - 1)
            vol = math.sqrt(var)

        csp.output(pnl=round(pnl, 4), drawdown=round(drawdown, 4), volatility=round(vol, 6))


@csp.node
def detect_price_spike(
    raw_tick: ts[dict],
    symbol: str,
) -> ts[dict]:
    with csp.state():
        s_prev_price = None

    if csp.ticked(raw_tick):
        price = raw_tick["price"]
        if s_prev_price is not None and s_prev_price > 0:
            pct_change = abs(price - s_prev_price) / s_prev_price * 100.0
            if pct_change > SPIKE_THRESHOLD_PCT:
                direction = "up" if price > s_prev_price else "down"
                return {
                    "alert_id": str(uuid.uuid4())[:8],
                    "symbol": symbol,
                    "alert_type": "PRICE_SPIKE",
                    "severity": "HIGH" if pct_change > SPIKE_THRESHOLD_PCT * 2 else "MEDIUM",
                    "message": f"{symbol} spiked {direction} {pct_change:.2f}%",
                    "value": round(pct_change, 4),
                    "threshold": SPIKE_THRESHOLD_PCT,
                    "timestamp": datetime.utcnow().isoformat(),
                }
        s_prev_price = price


@csp.node
def detect_volatility_alert(
    volatility: ts[float],
    symbol: str,
) -> ts[dict]:
    if csp.ticked(volatility):
        if volatility > HIGH_VOL_THRESHOLD:
            return {
                "alert_id": str(uuid.uuid4())[:8],
                "symbol": symbol,
                "alert_type": "HIGH_VOLATILITY",
                "severity": "HIGH" if volatility > HIGH_VOL_THRESHOLD * 2 else "MEDIUM",
                "message": f"{symbol} volatility elevated at {volatility:.4f}",
                "value": volatility,
                "threshold": HIGH_VOL_THRESHOLD,
                "timestamp": datetime.utcnow().isoformat(),
            }


@csp.node
def detect_drawdown_alert(
    drawdown: ts[float],
    symbol: str,
) -> ts[dict]:
    if csp.ticked(drawdown):
        if drawdown > DRAWDOWN_ALERT_PCT:
            severity = "CRITICAL" if drawdown > DRAWDOWN_ALERT_PCT * 2 else "HIGH"
            return {
                "alert_id": str(uuid.uuid4())[:8],
                "symbol": symbol,
                "alert_type": "DRAWDOWN",
                "severity": severity,
                "message": f"{symbol} drawdown at {drawdown:.2f}%",
                "value": drawdown,
                "threshold": DRAWDOWN_ALERT_PCT,
                "timestamp": datetime.utcnow().isoformat(),
            }


@csp.node
def portfolio_aggregator(
    symbol: str,
    pnl: ts[float],
    drawdown: ts[float],
    volatility: ts[float],
):
    if csp.ticked(pnl):
        _state["portfolio"][symbol] = {
            "symbol": symbol,
            "pnl": pnl,
            "drawdown": drawdown if csp.valid(drawdown) else 0.0,
            "volatility": volatility if csp.valid(volatility) else 0.0,
            "timestamp": datetime.utcnow().isoformat(),
        }
        positions = _state["portfolio"]
        if positions:
            total_pnl = sum(p["pnl"] for p in positions.values())
            max_dd = max(p["drawdown"] for p in positions.values())
            avg_vol = sum(p["volatility"] for p in positions.values()) / len(positions)
            _state["risk_metrics"].update({
                "total_pnl": round(total_pnl, 4),
                "max_drawdown": round(max_dd, 4),
                "current_drawdown": round(max_dd, 4),
                "volatility": round(avg_vol, 6),
                "sharpe_ratio": round(total_pnl / (avg_vol * 100) if avg_vol > 0 else 0.0, 4),
                "num_positions": len(positions),
                "timestamp": datetime.utcnow().isoformat(),
            })


@csp.node
def alert_publisher(alert: ts[dict]):
    if csp.ticked(alert):
        _state["alerts"].append(alert)
        _state["active_alerts"][alert["alert_id"]] = alert
        _state["alert_count"] += 1
        # Auto-expire active alerts (keep only last 50)
        if len(_state["active_alerts"]) > 50:
            oldest_key = next(iter(_state["active_alerts"]))
            del _state["active_alerts"][oldest_key]


@csp.node
def signal_processor(raw_signal: ts[dict]):
    """Process signals from the price engine (log them for risk context)."""
    pass


@csp.graph
def risk_monitor_graph():
    market_adapter = WebSocketAdapterManager(MARKET_DATA_WS_URL)
    signal_adapter = WebSocketAdapterManager(PRICE_ENGINE_WS_URL)

    for symbol in ALL_SYMBOLS:
        raw = market_adapter.subscribe(symbol, push_mode=csp.PushMode.LAST_VALUE)
        initial = INITIAL_PRICES.get(symbol, 100.0)

        pos = track_position(raw, symbol, initial)
        portfolio_aggregator(symbol, pos.pnl, pos.drawdown, pos.volatility)

        spike_alert = detect_price_spike(raw, symbol)
        alert_publisher(spike_alert)

        vol_alert = detect_volatility_alert(pos.volatility, symbol)
        alert_publisher(vol_alert)

        dd_alert = detect_drawdown_alert(pos.drawdown, symbol)
        alert_publisher(dd_alert)

    # Subscribe to price engine signals (for future correlation with risk)
    for symbol in ALL_SYMBOLS:
        sig_raw = signal_adapter.subscribe(symbol, push_mode=csp.PushMode.LAST_VALUE)
        signal_processor(sig_raw)


def _run_csp_engine():
    logger.info("Starting CSP risk monitor engine in realtime mode")
    time.sleep(5)  # wait for upstream services
    try:
        csp.run(
            risk_monitor_graph,
            starttime=datetime.utcnow(),
            endtime=timedelta(days=365),
            realtime=True,
        )
    except Exception:
        logger.exception("CSP risk monitor error")


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
        "service": "Risk Monitor",
        "status": "running",
        "alert_count": _state["alert_count"],
        "positions_tracked": len(_state["portfolio"]),
        "started_at": _state["started_at"],
    })


@flask_app.route("/status")
def status():
    return jsonify({
        "status": "running",
        "alert_count": _state["alert_count"],
        "active_alerts": len(_state["active_alerts"]),
        "positions_tracked": len(_state["portfolio"]),
        "started_at": _state["started_at"],
    })


@flask_app.route("/alerts")
def alerts():
    limit = request.args.get("limit", 50, type=int)
    severity = request.args.get("severity")
    all_alerts = list(_state["alerts"])
    if severity:
        all_alerts = [a for a in all_alerts if a.get("severity") == severity.upper()]
    return jsonify(all_alerts[-limit:])


@flask_app.route("/alerts/active")
def active_alerts():
    return jsonify(list(_state["active_alerts"].values()))


@flask_app.route("/portfolio")
def portfolio():
    return jsonify(_state["portfolio"])


@flask_app.route("/risk-metrics")
def risk_metrics():
    return jsonify(_state["risk_metrics"])


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port):
    _state["started_at"] = datetime.utcnow().isoformat()

    csp_thread = threading.Thread(target=_run_csp_engine, daemon=True, name="csp-risk-monitor")
    csp_thread.start()

    flask_app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
