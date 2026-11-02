"""The desk app's calls to the Pricing API and the Market Feed."""

import os

import requests

PRICING_API_URL = os.environ.get("PRICING_API_URL", "http://pricing-api")
MARKET_FEED_URL = os.environ.get("MARKET_FEED_URL", "http://market-feed")


class ServiceUnavailable(Exception):
    """A service is down or has nothing to give. The text is ready to show to the trader."""


def get_curve(market: str) -> dict:
    return _get(f"{PRICING_API_URL}/curve/{market}", "Pricing API")


def get_latest_prices() -> dict:
    return _get(f"{MARKET_FEED_URL}/latest", "Market Feed")


def _get(url: str, service: str) -> dict:
    """The JSON the service answers with. Without a 2xx answer, the service's own message."""
    not_reachable = f"The {service} is not reachable."
    try:
        response = requests.get(url, timeout=5)
        body = response.json()
    except (requests.RequestException, ValueError) as err:
        raise ServiceUnavailable(not_reachable) from err
    if not 200 <= response.status_code < 300:
        detail = body.get("detail") if isinstance(body, dict) else None
        raise ServiceUnavailable(detail if detail and isinstance(detail, str) else not_reachable)
    return body
