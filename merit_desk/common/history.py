"""Synthetic market history: weather, a three-day-ahead forecast, demand and spot price."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from merit_desk.common.climate import normal_temp
from merit_desk.common.markets import Market
from merit_desk.common.stack import demand_gw, price

HISTORY_START = date(2021, 1, 1)
PHI = 0.8  # how much of yesterday's anomaly carries over
ANOMALY_SIGMA_C = 3.0  # the daily shock to the anomaly
FORECAST_DAYS = 3  # the forecast looks this many days ahead
FORECAST_NOISE_C = 3.0  # the forecast is a noisy view of the anomaly, as real ones are
DEMAND_NOISE_GW = 1.0


def _ar1(shocks: np.ndarray) -> np.ndarray:
    """An AR(1) series: each value is PHI times the one before, plus that day's shock.

    The first value is drawn from the stationary distribution, so there is no warm-up.
    """
    series = np.empty_like(shocks)
    series[0] = shocks[0] / np.sqrt(1 - PHI**2)
    for i in range(1, len(shocks)):
        series[i] = PHI * series[i - 1] + shocks[i]
    return series


def synthetic_history(market: Market, years: int = 5, seed: int = 7) -> pd.DataFrame:
    """One row a day for whole calendar years from 2021. The same seed gives the same history."""
    rng = np.random.default_rng([seed, *map(ord, market.code)])  # each market has its own weather
    days = pd.date_range(HISTORY_START, date(HISTORY_START.year + years, 1, 1), inclusive="left")
    n = len(days)

    anomaly = _ar1(rng.normal(0.0, ANOMALY_SIGMA_C, n + FORECAST_DAYS))  # extra days to look ahead
    normal = np.array([normal_temp(market, d) for d in days.date])
    temp = normal + anomaly[:n]
    demand = demand_gw(temp, market.stack) + rng.normal(0.0, DEMAND_NOISE_GW, n)

    return pd.DataFrame({
        "day": days,
        "temp_c": temp,
        "normal_c": normal,
        "anomaly_c": anomaly[:n],
        "forecast_anomaly_c": anomaly[FORECAST_DAYS:] + rng.normal(0.0, FORECAST_NOISE_C, n),
        "demand_gw": demand,
        "spot_price": price(demand, market.stack),
    })
