import logging
import random
import threading
import time
import warnings
from datetime import date, datetime

import pytest

with warnings.catch_warnings():
    # starlette 1.10 prefers httpx2 and warns about httpx: switch when it drops the httpx fallback
    warnings.filterwarnings("ignore", message=r"Using `httpx` with `starlette\.testclient`")
    from fastapi.testclient import TestClient

from merit_desk.common.climate import normal_temp
from merit_desk.common.curve import build_curve
from merit_desk.common.dates import london_today
from merit_desk.common.markets import MARKETS, get_market
from merit_desk.common.stack import stack_price
from merit_desk.common.store import MemoryStore, load_forecast, save_curves
from merit_desk.services import market_feed, pricing_api

TODAY = date(2026, 10, 29)


def store_with_de_curve() -> MemoryStore:
    store = MemoryStore()
    forecast = load_forecast("sample", store)
    save_curves([build_curve("DE", forecast, TODAY).to_dict()], store)
    return store


def client(store: MemoryStore | None = None) -> TestClient:
    return TestClient(pricing_api.create_app(MemoryStore() if store is None else store))


# Pricing API


def test_health():
    assert client().get("/health").json() == {"status": "ok"}


def test_price_endpoint():
    assert client().get("/price", params={"market": "de", "temp_c": 4.5}).json() == 170.0


@pytest.mark.parametrize("temp_c", ["nan", "inf", "-inf", "61", "-61"])
def test_price_refuses_a_temperature_that_is_not_between_minus_60_and_60(temp_c):
    response = client().get("/price", params={"market": "DE", "temp_c": temp_c})
    assert response.status_code == 422


def test_price_accepts_the_limits():
    for temp_c in (-60, 60):
        assert client().get("/price", params={"market": "DE", "temp_c": temp_c}).status_code == 200


def test_price_unknown_market_404():
    response = client().get("/price", params={"market": "NL", "temp_c": 4.5})
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown market 'NL'. Use one of: GB, DE, FR."


def test_curve_503_without_curve():
    response = client().get("/curve/DE")
    assert response.status_code == 503
    assert response.json()["detail"] == "No curve yet. The daily curve workflow has not run."


def test_curve_unknown_market_404_before_the_503_check():
    response = client().get("/curve/NL")
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown market 'NL'. Use one of: GB, DE, FR."


def test_curve_and_month():
    store = store_with_de_curve()
    stored = build_curve("DE", load_forecast("sample", store), TODAY).to_dict()

    assert client(store).get("/curve/de").json() == stored
    month = client(store).get("/curve/DE/2026-11").json()
    assert month == {"market": "DE", "month": "2026-11", "price": stored["prices"][1]}


def test_month_without_curve_is_503():
    assert client().get("/curve/DE/2026-11").status_code == 503


def test_curve_month_404():
    response = client(store_with_de_curve()).get("/curve/DE/2030-01")
    assert response.status_code == 404
    assert response.json()["detail"] == (
        "Month 2030-01 is not in the newest curve (2026-10 to 2027-09).")


def test_pricing_api_is_titled():
    assert pricing_api.create_app(MemoryStore()).title == "Merit Desk Pricing API"


# Market Feed


class BigDrop:
    """A random source whose noise is a huge drop."""

    def gauss(self, mu: float, sigma: float) -> float:
        return -1000.0


def test_next_price_reverts_and_is_seeded():
    above = market_feed.next_price(150.0, 100.0, random.Random(1))
    below = market_feed.next_price(50.0, 100.0, random.Random(1))
    assert 130 < above < 150  # a fifth of the gap back to the anchor, plus a little noise
    assert 50 < below < 70
    assert market_feed.next_price(150.0, 100.0, random.Random(1)) == above
    assert market_feed.next_price(150.0, 100.0, random.Random(2)) != above


def test_next_price_is_never_below_zero():
    assert market_feed.next_price(10.0, 10.0, BigDrop()) == 0.0


def test_anchor_is_the_front_month_of_the_newest_curve():
    store = store_with_de_curve()
    front_month = build_curve("DE", load_forecast("sample", store), TODAY).prices[0]
    assert market_feed.anchor_price("de", store) == front_month


def test_anchor_without_curve_is_the_stack_price_at_normal_temperature():
    de = get_market("DE")
    expected = stack_price(normal_temp(de, london_today()), de.stack)
    assert market_feed.anchor_price("DE", MemoryStore()) == expected


def test_feed_without_curve():
    with TestClient(market_feed.create_app(MemoryStore(), tick_seconds=3600)) as feed:
        response = feed.get("/latest/DE")
    assert response.status_code == 200
    body = response.json()
    assert body["market"] == "DE" and body["price"] > 0
    assert datetime.fromisoformat(body["time"]).utcoffset().total_seconds() == 0


def test_feed_starts_on_the_front_month_price():
    store = store_with_de_curve()
    front_month = build_curve("DE", load_forecast("sample", store), TODAY).prices[0]
    with TestClient(market_feed.create_app(store, tick_seconds=3600)) as feed:
        assert feed.get("/latest/DE").json()["price"] == front_month


def test_latest_lists_every_market():
    with TestClient(market_feed.create_app(MemoryStore(), tick_seconds=3600)) as feed:
        latest = feed.get("/latest").json()
    assert sorted(latest) == sorted(m.code for m in MARKETS)
    assert all(set(quote) == {"price", "time"} for quote in latest.values())


def test_latest_is_filled_before_the_thread_runs():
    feed = TestClient(market_feed.create_app(MemoryStore()))  # no `with`: no startup, no thread
    assert feed.get("/latest/GB").json()["price"] > 0


def test_feed_unknown_market_404():
    with TestClient(market_feed.create_app(MemoryStore(), tick_seconds=3600)) as feed:
        response = feed.get("/latest/NL")
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown market 'NL'. Use one of: GB, DE, FR."


def test_feed_health_and_title():
    app = market_feed.create_app(MemoryStore())
    assert app.title == "Merit Desk Market Feed"
    assert TestClient(app).get("/health").json() == {"status": "ok"}


def test_seeded_feeds_tick_the_same_way():
    def run(seed: int) -> list[float]:
        feed = market_feed.Feed(MemoryStore(), seed)
        for _ in range(5):
            feed.tick()
        return [feed.latest[m.code]["price"] for m in MARKETS]

    assert run(42) == run(42)
    assert run(42) != run(43)


def test_refresh_moves_the_anchors_to_the_new_curve(monkeypatch):
    # Without a curve the anchor is a normal day for today's date, so pin the date.
    monkeypatch.setattr(market_feed, "london_today", lambda: TODAY)
    store = MemoryStore()
    feed = market_feed.Feed(store, seed=42)
    before = feed.anchors["DE"]
    save_curves([build_curve("DE", load_forecast("sample", store), TODAY).to_dict()], store)
    feed.refresh_anchors()
    assert feed.anchors["DE"] == build_curve("DE", load_forecast("sample", store), TODAY).prices[0]
    assert feed.anchors["DE"] != before


def wait_until(condition, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.01)
    return condition()


def test_thread_ticks_while_the_app_runs_and_stops_with_it():
    threads_before = threading.active_count()
    with TestClient(market_feed.create_app(MemoryStore(), tick_seconds=0.01)) as feed:
        first = feed.get("/latest/DE").json()["time"]
        assert wait_until(lambda: feed.get("/latest/DE").json()["time"] != first)
    assert threading.active_count() == threads_before


class BrokenStore(MemoryStore):
    def get_text(self, name: str) -> str | None:
        raise OSError("blob storage is down")


def test_a_failed_anchor_refresh_does_not_stop_the_feed(monkeypatch, caplog):
    monkeypatch.setattr(market_feed, "ANCHOR_REFRESH_SECONDS", 0)  # refresh on every tick
    feed = market_feed.Feed(MemoryStore(), seed=42)
    feed.store = BrokenStore()  # the store works at startup and fails later
    stop = threading.Event()
    thread = threading.Thread(target=feed.run, args=(stop, 0.01), daemon=True)
    first = feed.latest["DE"]["time"]
    with caplog.at_level(logging.WARNING, logger=market_feed.logger.name):
        thread.start()
        assert wait_until(lambda: feed.latest["DE"]["time"] != first)
        stop.set()
        thread.join(timeout=5)
    assert "Anchor refresh failed, keeping the old anchors: blob storage is down." in caplog.text
