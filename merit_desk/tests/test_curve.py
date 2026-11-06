import json
from datetime import date, datetime, timedelta, timezone

import pytest

from merit_desk.common.climate import normal_temp
from merit_desk.common.curve import Curve, MonthNotInCurveError, build_curve, summary
from merit_desk.common.markets import get_market
from merit_desk.common.stack import stack_price
from merit_desk.common.weather import Forecast

TODAY = date(2026, 10, 29)
RUN_TIME = datetime(2026, 10, 29, 6, 0, tzinfo=timezone.utc)


def empty_forecast() -> Forecast:
    return Forecast(run_time=RUN_TIME, source="test forecast", temps={})


def forecast_offset(code: str, start: date, days: int, offset_c: float) -> Forecast:
    """A forecast whose days are the normal temperature plus a fixed offset."""
    market = get_market(code)
    temps = {start + timedelta(d): normal_temp(market, start + timedelta(d)) + offset_c
             for d in range(days)}
    return Forecast(run_time=RUN_TIME, source="test forecast", temps={code: temps})


def test_curve_has_twelve_months_from_today():
    c = build_curve("DE", empty_forecast(), today=TODAY)
    assert c.months[0] == "2026-10" and c.months[-1] == "2027-09" and len(c.prices) == 12


def test_front_month_covers_only_the_days_from_today():
    de = get_market("DE")
    c = build_curve(de, empty_forecast(), today=TODAY)
    balance = [stack_price(normal_temp(de, date(2026, 10, d)), de.stack) for d in (29, 30, 31)]
    assert c.prices[0] == round(sum(balance) / 3, 2)


def test_cold_forecast_raises_front_month_only():
    base = build_curve("DE", empty_forecast(), today=TODAY)
    cold = build_curve("DE", forecast_offset("DE", TODAY, days=10, offset_c=-10), today=TODAY)
    assert cold.prices[0] > base.prices[0]
    assert cold.prices[2:] == base.prices[2:]


def test_forecast_for_another_market_is_ignored():
    base = build_curve("DE", empty_forecast(), today=TODAY)
    other = build_curve("DE", forecast_offset("FR", TODAY, days=10, offset_c=-10), today=TODAY)
    assert other.prices == base.prices


def test_curve_round_trips_through_dict():
    c = build_curve("GB", empty_forecast(), today=TODAY)
    d = c.to_dict()
    assert list(d) == ["market", "as_of", "source", "months", "prices"]
    assert d["as_of"] == "2026-10-29" and d["source"] == "test forecast"
    assert Curve.from_dict(json.loads(json.dumps(d))) == c


def test_forecast_round_trips_through_json():
    f = forecast_offset("FR", TODAY, days=3, offset_c=2.5)
    text = f.to_json()
    raw = json.loads(text)
    assert raw["run_time"] == "2026-10-29T06:00:00+00:00"
    assert list(raw["temps"]["FR"]) == ["2026-10-29", "2026-10-30", "2026-10-31"]
    assert Forecast.from_json(text) == f


def test_price_for_month():
    c = build_curve("DE", empty_forecast(), today=TODAY)
    assert c.price_for("2027-03") == c.prices[5]


def test_price_for_month_outside_curve():
    c = build_curve("DE", empty_forecast(), today=TODAY)
    message = r"2030-01 is not in the newest curve \(2026-10 to 2027-09\)"
    with pytest.raises(MonthNotInCurveError, match=message):
        c.price_for("2030-01")
    with pytest.raises(MonthNotInCurveError, match=r"Jan-27 is not in the newest curve"):
        c.price_for("Jan-27")


def test_summary_names_date_source_and_each_market():
    curves = [build_curve(code, empty_forecast(), TODAY).to_dict() for code in ("GB", "DE", "FR")]
    lines = summary(curves).splitlines()
    assert "2026-10-29" in lines[0] and "test forecast" in lines[0]
    assert len(lines) == 4
    for line, curve in zip(lines[1:], curves):
        assert line.startswith(curve["market"])
        assert f"{curve['prices'][0]:.2f} EUR/MWh" in line
