"""Backtest a weather strategy on a forward contract, and the grid of settings to sweep."""

from __future__ import annotations

import calendar
from datetime import date
from functools import cache

import numpy as np
import pandas as pd

from merit_desk.common.climate import normal_temp
from merit_desk.common.history import synthetic_history
from merit_desk.common.markets import MARKETS, Market, get_market
from merit_desk.common.stack import stack_price

HOURS_PER_DAY = 24
DAYS_PER_YEAR = 365  # power trades every day, so history has a row for each calendar day
ANOMALY_PRICE_EUR_PER_C = 2.0  # a degree colder than normal adds this to the one-month forward
TRADING_COST_EUR_PER_MWH = 0.35  # paid on every entry and every exit of a position

GRIDS = {  # size: (tenors in months, thresholds in degrees C)
    "small": ((1, 2), (1.0, 1.5, 2.0, 2.5)),
    "full": (tuple(range(1, 7)), tuple(0.5 + 0.25 * i for i in range(19))),
}


@cache
def _normal_month_price(market: Market, year: int, month: int) -> float:
    """The mean stack price over the days of a month at normal temperatures."""
    days = (date(year, month, d + 1) for d in range(calendar.monthrange(year, month)[1]))
    return float(stack_price([normal_temp(market, d) for d in days], market.stack).mean())


def anomaly_term(anomaly: np.ndarray, tenor: int) -> np.ndarray:
    """The weather part of the forward price: 2/tenor EUR/MWh lower per degree of warm anomaly."""
    return -(ANOMALY_PRICE_EUR_PER_C / tenor) * anomaly


def forward_prices(history: pd.DataFrame, market: Market, tenor: int) -> np.ndarray:
    """The forward price on each day, for delivery `tenor` months after that day's month.

    It is the stack price at normal weather for that month plus the weather term. Its
    normal part jumps to the next delivery month on the first of each month, so the
    P&L does not use the forward itself (see `daily_pnl`).
    """
    months = history["day"].dt.to_period("M") + tenor
    normal = months.map(lambda m: _normal_month_price(market, m.year, m.month)).to_numpy(float)
    return normal + anomaly_term(history["anomaly_c"].to_numpy(), tenor)


def daily_pnl(forecast_anomaly: np.ndarray, anomaly: np.ndarray, tenor: int, threshold: float,
              cost_eur_mwh: float = TRADING_COST_EUR_PER_MWH) -> tuple[np.ndarray, np.ndarray]:
    """Hold +1 MW the day after the forecast is colder than -threshold; hold nothing otherwise.

    Returns the days with the position on, and each day's P&L in EUR. P&L uses a fixed
    delivery month: the position held from the day before, times the change in the weather
    term of the forward, times 24 hours. It leaves out the seasonal jump of the forward
    to the next month, which a position in one delivery month would not earn.
    Each entry and each exit pays the trading cost on 24 MWh. A position still open
    on the last day is closed on it.
    """
    signal = forecast_anomaly < -threshold
    held = np.concatenate(([False], signal[:-1]))
    weather_change = np.diff(anomaly_term(anomaly, tenor), prepend=0.0)
    entries = _run_starts(held)
    exits = _run_ends(held)
    cost = (entries.astype(int) + exits) * cost_eur_mwh * HOURS_PER_DAY
    return held, held * weather_change * HOURS_PER_DAY - cost


def _run_starts(held: np.ndarray) -> np.ndarray:
    """The first day of each run of consecutive days with the position on."""
    return held & ~np.concatenate(([False], held[:-1]))


def _run_ends(held: np.ndarray) -> np.ndarray:
    """The last day of each run of consecutive days with the position on."""
    return held & ~np.concatenate((held[1:], [False]))


def trade_pnls(held: np.ndarray, pnl: np.ndarray) -> np.ndarray:
    """The P&L of each trade, a trade being a run of consecutive days with the position on."""
    starts = _run_starts(held)
    trade = np.cumsum(starts)[held] - 1
    return np.bincount(trade, weights=pnl[held])


def sharpe_ratio(pnl: np.ndarray) -> float:
    """The mean daily P&L over its sample standard deviation, scaled by sqrt(365)."""
    spread = pnl.std(ddof=1)
    return float(pnl.mean() / spread * np.sqrt(DAYS_PER_YEAR)) if spread > 0 else 0.0


def _simulate(market: str, tenor: int, threshold: float, years: int,
              seed: int) -> tuple[Market, pd.Series, np.ndarray, np.ndarray]:
    """The market, the days of its history, and for each day the position held and its P&L."""
    m = get_market(market)
    history = synthetic_history(m, years, seed)
    held, pnl = daily_pnl(history["forecast_anomaly_c"].to_numpy(),
                          history["anomaly_c"].to_numpy(), tenor, threshold)
    return m, history["day"], held, pnl


def _summary(m: Market, tenor: int, threshold: float, held: np.ndarray, pnl: np.ndarray) -> dict:
    """The result of a backtest: its setting, total P&L, number of trades, hit rate and Sharpe."""
    trades = trade_pnls(held, pnl)
    return {
        "name": task_name(m.code, tenor, threshold),
        "market": m.code,
        "tenor": tenor,
        "threshold": threshold,
        "total_pnl_eur": round(float(pnl.sum()), 2),
        "n_trades": len(trades),
        "hit_rate": round(float((trades > 0).mean()), 4) if len(trades) else 0.0,
        "sharpe": round(sharpe_ratio(pnl), 3),
    }


def run_backtest(market: str, tenor: int, threshold: float, years: int = 5, seed: int = 7) -> dict:
    """Backtest one grid point on its own synthetic history, with a fixed delivery month.

    P&L is net of trading costs. The hit rate is the share of trades that made money.
    """
    m, _, held, pnl = _simulate(market, tenor, threshold, years, seed)
    return _summary(m, tenor, threshold, held, pnl)


def backtest_detail(market: str, tenor: int, threshold: float, years: int = 5,
                    seed: int = 7) -> dict:
    """`run_backtest` plus what the Backtest page draws: a point per day and each trade.

    `cum_pnl_eur` is the running total P&L, `drawdown_eur` is that total less its running
    maximum (never above 0), and each trade has its first and last held day.
    """
    m, days, held, pnl = _simulate(market, tenor, threshold, years, seed)
    dates = days.dt.strftime("%Y-%m-%d").tolist()
    cum = np.cumsum(pnl)
    runs = zip(np.flatnonzero(_run_starts(held)), np.flatnonzero(_run_ends(held)),
               trade_pnls(held, pnl))
    return {
        **_summary(m, tenor, threshold, held, pnl),
        "dates": dates,
        "cum_pnl_eur": np.round(cum, 2).tolist(),
        "drawdown_eur": np.round(cum - np.maximum.accumulate(cum), 2).tolist(),
        "trades": [{"start": dates[a], "end": dates[b], "days": int(b - a + 1),
                    "pnl_eur": round(float(p), 2)} for a, b, p in runs],
    }


def sweep_grid(size: str) -> list[tuple[str, int, float]]:
    """Every (market, tenor, threshold) to backtest: 24 for 'small', 342 for 'full'."""
    if size not in GRIDS:
        raise ValueError(f"Unknown sweep size '{size}'. Use one of: {', '.join(GRIDS)}.")
    tenors, thresholds = GRIDS[size]
    return [(m.code, t, th) for m in MARKETS for t in tenors for th in thresholds]


def task_name(market: str, tenor: int, threshold: float) -> str:
    """A name that reads at a glance, such as bt_DE_M2_t15: the threshold in tenths of a degree.

    A threshold that is not a whole number of tenths keeps its decimal: 0.75 is t7p5.
    """
    tenths = f"{threshold * 10:g}".replace(".", "p")
    return f"bt_{market}_M{tenor}_t{tenths}"
