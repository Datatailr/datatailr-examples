import calendar
from datetime import date

import numpy as np
import pandas as pd
import pytest

from merit_desk.common.backtest import (
    HOURS_PER_DAY,
    TRADING_COST_EUR_PER_MWH,
    backtest_detail,
    daily_pnl,
    forward_prices,
    run_backtest,
    sweep_grid,
    task_name,
    trade_pnls,
)
from merit_desk.common.climate import normal_temp
from merit_desk.common.history import FORECAST_NOISE_C, synthetic_history
from merit_desk.common.markets import get_market
from merit_desk.common.stack import price, stack_price

DE = get_market("DE")
COST = TRADING_COST_EUR_PER_MWH * HOURS_PER_DAY  # EUR for one entry or one exit of 1 MW
COLUMNS = ["day", "temp_c", "normal_c", "anomaly_c", "forecast_anomaly_c", "demand_gw",
           "spot_price"]
RESULT_KEYS = {"name", "market", "tenor", "threshold", "total_pnl_eur", "n_trades", "hit_rate",
               "sharpe"}


def test_history_is_deterministic():
    pd.testing.assert_frame_equal(synthetic_history(DE), synthetic_history(DE))
    assert not synthetic_history(DE, seed=1).equals(synthetic_history(DE, seed=2))


def test_history_has_one_row_per_day_for_whole_years():
    h = synthetic_history(DE, years=2)
    assert list(h.columns) == COLUMNS
    assert h["day"].iloc[0] == pd.Timestamp("2021-01-01")
    assert h["day"].iloc[-1] == pd.Timestamp("2022-12-31")
    assert len(h) == 365 * 2
    assert not h.isna().any().any()


def test_markets_get_different_weather_from_the_same_seed():
    de, fr = synthetic_history(DE), synthetic_history(get_market("FR"))
    assert not de["anomaly_c"].equals(fr["anomaly_c"])


def test_temperature_is_normal_plus_anomaly():
    h = synthetic_history(DE)
    assert h["normal_c"].iloc[100] == pytest.approx(normal_temp(DE, h["day"].iloc[100].date()))
    np.testing.assert_allclose(h["temp_c"], h["normal_c"] + h["anomaly_c"])


def test_anomaly_is_persistent_and_the_forecast_looks_three_days_ahead():
    h = synthetic_history(DE, years=20)
    assert h["anomaly_c"].autocorr(lag=1) == pytest.approx(0.8, abs=0.05)
    error = (h["forecast_anomaly_c"] - h["anomaly_c"].shift(-3)).dropna()
    assert error.std() == pytest.approx(FORECAST_NOISE_C, abs=0.15)


def test_spot_price_is_the_stack_price_of_demand():
    h = synthetic_history(DE)
    np.testing.assert_array_equal(h["spot_price"], price(h["demand_gw"], DE.stack))
    assert h["demand_gw"].corr(h["temp_c"]) < -0.5  # colder weather, more demand


def normal_month_price(year: int, month: int) -> float:
    days = range(1, calendar.monthrange(year, month)[1] + 1)
    return float(np.mean([stack_price(normal_temp(DE, date(year, month, d)), DE.stack)
                          for d in days]))


@pytest.mark.parametrize("day, tenor, delivery", [
    ("2021-01-15", 2, (2021, 3)),
    ("2021-12-10", 2, (2022, 2)),  # the delivery month is in the next year
])
def test_forward_price_is_the_normal_weather_price_of_the_month_less_the_anomaly_term(
        day, tenor, delivery):
    h = synthetic_history(DE, years=2)
    row = h.index[h["day"] == pd.Timestamp(day)][0]
    forward = forward_prices(h, DE, tenor)
    expected = normal_month_price(*delivery) - (2.0 / tenor) * h["anomaly_c"][row]
    assert forward[row] == pytest.approx(expected)


def test_pnl_is_the_weather_move_of_the_position_held_from_yesterday_times_24_hours():
    forecast = np.array([-3.0, -3.0, 0.0, -3.0, 0.0])  # signal on days 0, 1 and 3
    anomaly = np.array([0.0, -3.0, -2.0, -4.0, -3.0])
    held, pnl = daily_pnl(forecast, anomaly, tenor=2, threshold=2.0, cost_eur_mwh=0.0)
    assert held.tolist() == [False, True, True, False, True]
    # a degree colder lifts the forward by 2/tenor = 1 EUR/MWh
    assert pnl.tolist() == [0.0, 3 * 24, -1 * 24, 0.0, -1 * 24]
    assert trade_pnls(held, pnl).tolist() == [2 * 24, -1 * 24]  # days 1-2, then day 4


def test_pnl_pays_the_trading_cost_on_every_entry_and_every_exit():
    forecast = np.array([-3.0, -3.0, 0.0, -3.0, 0.0])
    anomaly = np.array([0.0, -3.0, -2.0, -4.0, -3.0])
    held, pnl = daily_pnl(forecast, anomaly, tenor=2, threshold=2.0)
    # entry on day 1, exit on day 2; entry and exit on day 4 (a position open at the end is closed)
    assert pnl.tolist() == [0.0, 3 * 24 - COST, -1 * 24 - COST, 0.0, -1 * 24 - 2 * COST]
    assert trade_pnls(held, pnl).tolist() == [2 * 24 - 2 * COST, -1 * 24 - 2 * COST]


def test_pnl_ignores_the_jump_of_the_forward_to_the_next_delivery_month():
    h = synthetic_history(DE, years=1)
    forward = forward_prices(h, DE, tenor=1)
    roll = h.index[h["day"] == pd.Timestamp("2021-03-01")][0]  # delivery moves Mar -> Apr
    weather_move = -2.0 * (h["anomaly_c"][roll] - h["anomaly_c"][roll - 1])
    assert abs(forward[roll] - forward[roll - 1] - weather_move) > 40  # a big seasonal jump

    always_long = np.full(len(h), -9.0)  # the signal is on every day
    _, pnl = daily_pnl(always_long, h["anomaly_c"].to_numpy(), tenor=1, threshold=0.0)
    assert pnl[roll] == pytest.approx(weather_move * HOURS_PER_DAY)


def test_no_signal_means_no_trades():
    held, pnl = daily_pnl(np.zeros(4), np.array([1.0, 2.0, 3.0, 4.0]), tenor=1, threshold=1.0)
    assert not held.any() and not pnl.any()
    assert trade_pnls(held, pnl).size == 0


def test_backtest_is_deterministic_and_has_keys():
    a, b = run_backtest("DE", 2, 1.5), run_backtest("DE", 2, 1.5)
    assert a == b
    assert set(a) == RESULT_KEYS
    assert a["name"] == "bt_DE_M2_t15"
    assert (a["market"], a["tenor"], a["threshold"]) == ("DE", 2, 1.5)
    assert a["n_trades"] > 0 and 0.0 <= a["hit_rate"] <= 1.0


def test_detail_agrees_with_the_backtest():
    d = backtest_detail("DE", 2, 1.5)
    r = run_backtest("DE", 2, 1.5)
    assert {k: d[k] for k in r} == r
    assert d["cum_pnl_eur"][-1] == pytest.approx(r["total_pnl_eur"], abs=0.01)
    assert len(d["trades"]) == r["n_trades"]
    rounding = 0.005 * (len(d["trades"]) + 1)  # up to half a cent off for each trade and the total
    assert sum(t["pnl_eur"] for t in d["trades"]) == pytest.approx(r["total_pnl_eur"], abs=rounding)
    assert max(d["drawdown_eur"]) <= 0 and len(d["dates"]) == len(d["cum_pnl_eur"]) == len(d["drawdown_eur"])


def test_detail_of_a_setting_that_never_trades_is_flat():
    d = backtest_detail("DE", 2, 50.0)
    assert d["trades"] == [] and d["hit_rate"] == 0.0
    assert set(d["cum_pnl_eur"]) == {0.0} and set(d["drawdown_eur"]) == {0.0}
    assert len(d["dates"]) == len(synthetic_history(DE))


def test_backtest_accepts_a_lower_case_market_code():
    assert run_backtest("de", 2, 1.5) == run_backtest("DE", 2, 1.5)


def test_huge_threshold_never_trades():
    r = run_backtest("DE", 1, 99.0)
    assert r["n_trades"] == 0 and r["total_pnl_eur"] == 0
    assert r["hit_rate"] == 0 and r["sharpe"] == 0


def test_threshold_decides_how_often_the_desk_trades():
    assert run_backtest("DE", 1, 0.5)["n_trades"] > run_backtest("DE", 1, 5.0)["n_trades"]


def test_full_sweep_has_winners_and_losers_with_a_clearly_positive_top():
    pnl = np.array([run_backtest(*grid)["total_pnl_eur"] for grid in sweep_grid("full")])
    assert 0.2 <= (pnl < 0).mean() <= 0.5
    assert np.sort(pnl)[-10:].min() > 10_000


def test_grid_sizes():
    assert len(sweep_grid("small")) == 24 and len(sweep_grid("full")) == 342


def test_small_grid_covers_three_markets_two_tenors_and_four_thresholds():
    grid = sweep_grid("small")
    assert {m for m, _, _ in grid} == {"GB", "DE", "FR"}
    assert {t for _, t, _ in grid} == {1, 2}
    assert {th for _, _, th in grid} == {1.0, 1.5, 2.0, 2.5}


def test_full_grid_covers_six_tenors_and_thresholds_from_half_a_degree_to_five():
    grid = sweep_grid("full")
    assert {t for _, t, _ in grid} == {1, 2, 3, 4, 5, 6}
    thresholds = sorted({th for _, _, th in grid})
    assert thresholds[0] == 0.5 and thresholds[-1] == 5.0 and len(thresholds) == 19


def test_unknown_grid_size_is_refused():
    with pytest.raises(ValueError, match="Use one of: small, full"):
        sweep_grid("huge")


def test_task_names_are_unique_and_readable():
    names = [task_name(*g) for g in sweep_grid("full")]
    assert len(set(names)) == len(names) and "bt_DE_M2_t15" in names
    assert all(name.replace("_", "").isalnum() for name in names)  # no dots or spaces
    assert task_name("GB", 6, 0.75) == "bt_GB_M6_t7p5"
