from datetime import date

import pytest

import merit_desk.common.weather as weather
from merit_desk import run_local
from merit_desk.common.markets import DE, MARKETS
from merit_desk.common.store import latest_curve
from merit_desk.run_local import run_daily_curve_locally


def raise_offline(markets):
    raise OSError("no network")


@pytest.fixture(autouse=True)
def offline_with_default_markets(monkeypatch):
    """No ECMWF download, and no reading of the desk's real 'markets' variable."""
    monkeypatch.setattr(weather, "download_forecast", raise_offline)
    monkeypatch.setattr(run_local, "get_markets", lambda: MARKETS)


def test_local_run_builds_and_publishes_three_curves(memory_store):
    text = run_daily_curve_locally(memory_store, today=date(2026, 10, 29))
    for code in ("GB", "DE", "FR"):
        assert latest_curve(code, memory_store)["months"][0] == "2026-10"
        assert code in text


def test_local_run_summary_gives_the_front_month_price(memory_store):
    text = run_daily_curve_locally(memory_store, today=date(2026, 10, 29))
    front_month = latest_curve("DE", memory_store)["prices"][0]
    assert f"DE 2026-10: {front_month:.2f} EUR/MWh" in text


def test_local_run_prices_only_the_markets_in_the_variable(memory_store, monkeypatch):
    monkeypatch.setattr(run_local, "get_markets", lambda: (DE,))
    run_daily_curve_locally(memory_store, today=date(2026, 10, 29))
    assert latest_curve("DE", memory_store) is not None
    assert latest_curve("GB", memory_store) is None
