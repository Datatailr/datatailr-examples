"""Merit Desk Tools: the desk's power prices as Excel functions, written in Python."""

import time

import requests
from datatailr.excel import Addin, Queue

from merit_desk.common import stack
from merit_desk.common.markets import get_market

PRICING_API = "http://merit-desk-pricing-api"
MARKET_FEED = "http://merit-desk-market-feed"

addin = Addin("Merit Desk Tools", "Power prices for the trading desk")


def stack_price_for(market: str, temp_c: float) -> float:
    if not -60 <= temp_c <= 60:  # nan and inf fail this test too
        raise ValueError("temp_c must be a number between -60 and 60.")
    return stack.stack_price(temp_c, get_market(market).stack)


def curve_price_for(market: str, month: str) -> float:
    code = get_market(market).code
    return _get_price(f"{PRICING_API}/curve/{code}/{month}", "Pricing API")


def live_price_once(market: str) -> float:
    code = get_market(market).code
    return _get_price(f"{MARKET_FEED}/latest/{code}", "Market Feed")


def _get_price(url: str, service: str) -> float:
    """The price the service answers with. Without one, the service's own message."""
    not_reachable = f"The {service} is not reachable."
    try:
        reply = requests.get(url, timeout=5)
        body = reply.json()
    except (requests.RequestException, ValueError) as err:
        raise ValueError(not_reachable) from err
    if reply.status_code != 200:
        detail = body.get("detail") if isinstance(body, dict) else None
        raise ValueError(detail if detail and isinstance(detail, str) else not_reachable)
    try:
        return float(body["price"])
    except (KeyError, TypeError, ValueError) as err:  # a 200 reply with no number in it
        raise ValueError(not_reachable) from err


@addin.expose(
    description="Stack model power price in EUR/MWh",
    help="market: GB, DE or FR. temp_c: average temperature in degrees Celsius.",
)
def stack_price(market: str, temp_c: float) -> float:
    return stack_price_for(market, temp_c)


@addin.expose(
    description="Monthly price from the newest forward curve, in EUR/MWh",
    help="market: GB, DE or FR. month: YYYY-MM.",
)
def curve_price(market: str, month: str) -> float:
    return curve_price_for(market, month)


@addin.expose(
    description="Simulated live price, updated every second",
    help="market: GB, DE or FR.",
    streaming=True,
)
def live_price(queue: Queue, market: str) -> float:
    get_market(market)  # a typo fails at once, not on the first tick
    while True:
        try:
            queue.push(live_price_once(market))
        except ValueError as err:  # a Market Feed restart must not end the stream
            queue.push(str(err))
        time.sleep(1)


def main(port=8080, ws_port=8000):
    addin.run(port, ws_port)
