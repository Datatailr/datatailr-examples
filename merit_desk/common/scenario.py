"""Scenarios: the curve with a weather shift and a changed stack, and its weather-path band."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import date

import numpy as np

from merit_desk.common.curve import Curve, build_curve, month_days
from merit_desk.common.history import ANOMALY_SIGMA_C, PHI
from merit_desk.common.markets import MARKETS, Market, get_market
from merit_desk.common.stack import Plant, StackConfig, demand_gw, price, stack_price
from merit_desk.common.weather import Forecast


class ScenarioError(ValueError):
    """Raised when a scenario cannot be priced. The text says why."""


@dataclass(frozen=True)
class PlantOverride:
    name: str
    cost_eur_mwh: float | None = None  # None keeps the plant's own value
    capacity_gw: float | None = None


@dataclass(frozen=True)
class Scenario:
    market: str
    shift_c: float = 0.0  # added to the temperature on every day of the window
    start: date | None = None
    end: date | None = None
    overrides: tuple[PlantOverride, ...] = ()


def _where(plants: dict[str, Plant]) -> str:
    """The market that has these plants. A stack does not know its market, but its plants do."""
    return next((f"in {m.code}" for m in MARKETS
                 if {p.name for p in m.stack.plants} == set(plants)), "in this stack")


def apply_overrides(stack: StackConfig, overrides: Sequence[PlantOverride]) -> StackConfig:
    """The stack with the changed plant costs and capacities, in merit order again."""
    plants = {p.name: p for p in stack.plants}
    for o in overrides:
        if o.name not in plants:
            raise ScenarioError(f"Unknown plant '{o.name}' {_where(plants)}. "
                                f"Plants: {', '.join(plants)}.")
        changes = {"cost_eur_mwh": o.cost_eur_mwh, "capacity_gw": o.capacity_gw}
        plants[o.name] = replace(plants[o.name],
                                 **{k: v for k, v in changes.items() if v is not None})
    if sum(p.capacity_gw for p in plants.values()) <= 0:  # the scarcity price divides by it
        raise ScenarioError("The plants have no capacity left.")
    return replace(stack, plants=tuple(sorted(plants.values(), key=lambda p: p.cost_eur_mwh)))


def stack_view(market: str, temp_c: float, overrides: Sequence[PlantOverride] = ()) -> dict:
    """The stack as steps, cheapest first, with the demand and the price at one temperature."""
    m = get_market(market)
    stack = apply_overrides(m.stack, overrides)
    demand = demand_gw(temp_c, stack)
    steps, filled = [], 0.0
    for p in stack.plants:
        steps.append({"name": p.name, "capacity_gw": p.capacity_gw, "cost_eur_mwh": p.cost_eur_mwh,
                      "from_gw": filled, "to_gw": filled + p.capacity_gw})
        filled += p.capacity_gw
    return {"market": m.code, "temp_c": temp_c, "demand_gw": round(demand, 2),
            "price": round(price(demand, stack), 2), "plants": steps}


def _inputs(s: Scenario, forecast: Forecast, today: date) -> tuple[Market, StackConfig, Forecast]:
    """The market, the changed stack, and a forecast with every curve day shifted as asked."""
    market = get_market(s.market)
    stack = apply_overrides(market.stack, s.overrides)
    if s.shift_c == 0:  # no shift, so the window does not matter
        return market, stack, forecast
    months = month_days(market, forecast, today)
    first, last = today, months[-1][1][-1]
    if s.start is None or s.end is None or s.start < first or s.end > last:
        raise ScenarioError(f"The shift window must lie between {first} and {last}.")
    if s.start > s.end:
        raise ScenarioError("The shift window must start on or before its end.")
    temps = {day: (t + s.shift_c if s.start <= day <= s.end else t)
             for _, days, ts in months for day, t in zip(days, ts)}
    return market, stack, replace(forecast, temps={**forecast.temps, market.code: temps})


def scenario_curve(s: Scenario, forecast: Forecast, today: date) -> Curve:
    """The 12-month curve with the shift and the stack changes of the scenario."""
    market, stack, shifted = _inputs(s, forecast, today)
    return build_curve(market, shifted, today, stack=stack)


def _ar1_paths(shocks: np.ndarray) -> np.ndarray:
    """One AR(1) series per row of shocks (paths x days), as history does for one series."""
    series = np.empty_like(shocks)
    series[:, 0] = shocks[:, 0] / np.sqrt(1 - PHI**2)  # stationary start
    for i in range(1, shocks.shape[1]):
        series[:, i] = PHI * series[:, i - 1] + shocks[:, i]
    return series


def scenario_band(s: Scenario, forecast: Forecast, today: date, n_paths: int,
                  seed: int = 7) -> dict:
    """P10, P50 and P90 of each month's price over seeded weather paths, next to the base."""
    market, stack, shifted = _inputs(s, forecast, today)
    months = month_days(market, shifted, today)
    temps = np.array([t for _, _, ts in months for t in ts])  # every day of the curve
    sizes = np.array([len(ts) for _, _, ts in months])
    shocks = np.random.default_rng(seed).normal(0.0, ANOMALY_SIGMA_C, (n_paths, len(temps)))
    prices = stack_price(temps + _ar1_paths(shocks), stack)  # paths x days, in one call
    first_days = np.cumsum(sizes) - sizes  # where each month starts in the day axis
    month_means = np.add.reduceat(prices, first_days, axis=1) / sizes
    p10, p50, p90 = np.percentile(month_means, [10, 50, 90], axis=0).round(2).tolist()
    return {"months": [month for month, _, _ in months],
            "base": build_curve(market, shifted, today, stack=stack).prices,
            "p10": p10, "p50": p50, "p90": p90, "n_paths": n_paths}
