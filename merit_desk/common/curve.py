"""The forward curve: twelve monthly prices from the weather forecast and normal weather."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta

from merit_desk.common.climate import normal_temp
from merit_desk.common.markets import Market, get_market
from merit_desk.common.stack import StackConfig, stack_price
from merit_desk.common.weather import Forecast

MONTHS_IN_CURVE = 12


class MonthNotInCurveError(ValueError):
    """Raised when a month is malformed or outside the curve."""


@dataclass
class Curve:
    market: str
    as_of: date
    source: str
    months: list[str]  # "YYYY-MM"
    prices: list[float]  # EUR/MWh, one per month

    def to_dict(self) -> dict:
        return {"market": self.market, "as_of": self.as_of.isoformat(), "source": self.source,
                "months": self.months, "prices": self.prices}

    @classmethod
    def from_dict(cls, d: dict) -> Curve:
        return cls(market=d["market"], as_of=date.fromisoformat(d["as_of"]), source=d["source"],
                   months=list(d["months"]), prices=list(d["prices"]))

    def price_for(self, month: str) -> float:
        if month not in self.months:
            raise MonthNotInCurveError(f"Month {month} is not in the newest curve "
                                       f"({self.months[0]} to {self.months[-1]}).")
        return self.prices[self.months.index(month)]


def month_days(market: Market, forecast: Forecast,
               today: date) -> list[tuple[str, list[date], list[float]]]:
    """The 12 months of the curve as (month, days, temperatures).

    A day has the forecast temperature when the forecast has it, else the normal one.
    """
    known = forecast.temps.get(market.code, {})
    months = []
    for i in range(MONTHS_IN_CURVE):
        year, month = divmod(today.year * 12 + today.month - 1 + i, 12)
        month += 1
        first = max(today, date(year, month, 1))  # the front month is a balance of month
        last = date(year, month, calendar.monthrange(year, month)[1])
        days = [first + timedelta(n) for n in range((last - first).days + 1)]
        temps = [known[day] if day in known else normal_temp(market, day) for day in days]
        months.append((f"{year}-{month:02d}", days, temps))
    return months


def _month_prices(market: Market, forecast: Forecast, today: date,
                  stack: StackConfig) -> tuple[list[str], list[float]]:
    months = month_days(market, forecast, today)
    prices = [round(float(stack_price(temps, stack).mean()), 2) for _, _, temps in months]
    return [month for month, _, _ in months], prices


def build_curve(market: Market | str, forecast: Forecast, today: date, *,
                stack: StackConfig | None = None) -> Curve:
    """Price 12 months from today's month: forecast days first, normal weather after.

    The stack is the market's own unless one is given.
    """
    if isinstance(market, str):
        market = get_market(market)
    months, prices = _month_prices(market, forecast, today, stack or market.stack)
    return Curve(market.code, today, forecast.source, months, prices)


def summary(curves: list[dict]) -> str:
    """A short chat message: the curve date, the forecast source and each front-month price."""
    head = f"Merit Desk curve {curves[0]['as_of']} (forecast: {curves[0]['source']})"
    lines = [f"{c['market']} {c['months'][0]}: {c['prices'][0]:.2f} EUR/MWh" for c in curves]
    return "\n".join([head, *lines])
