from datetime import date

import pytest

import merit_desk.common.weather as weather
from merit_desk import run_local
from merit_desk.common.markets import DE, MARKETS
from merit_desk.common.store import latest_curve
from merit_desk.run_local import run_daily_curve_locally
from merit_desk.workflows import daily_curve


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


def test_workflow_tasks_run_as_plain_functions_in_a_chain(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # the local store is the folder .merit_desk_data here
    monkeypatch.setenv("MERIT_DESK_STORE", "local")
    posted = []
    monkeypatch.setattr(daily_curve, "post_summary", posted.append)

    key = daily_curve.fetch_forecast()
    curves = [daily_curve.build_curve(code, key) for code in ("GB", "DE")]
    daily_curve.notify_desk(daily_curve.publish_curves(curves))

    assert key == "sample"  # the download is offline, and nothing is saved yet
    assert [c["market"] for c in curves] == ["GB", "DE"]
    assert latest_curve("DE")["prices"] == curves[1]["prices"]
    assert len(posted) == 1 and "GB" in posted[0] and "DE" in posted[0]
