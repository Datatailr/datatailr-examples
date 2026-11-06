from datetime import date

import numpy as np
import pytest

from merit_desk.common.climate import normal_temp
from merit_desk.common.curve import build_curve, month_days
from merit_desk.common.history import _ar1
from merit_desk.common.markets import DE, MARKETS
from merit_desk.common.scenario import (
    PlantOverride, Scenario, ScenarioError, _ar1_paths, apply_overrides, scenario_band,
    scenario_curve, stack_view,
)
from merit_desk.common.store import MemoryStore, load_forecast

TODAY = date(2026, 10, 29)


def test_month_days_gives_the_twelve_months_with_forecast_then_normal_temperatures():
    fc = load_forecast("sample", MemoryStore())
    months = month_days(DE, fc, TODAY)
    assert [m for m, _, _ in months][::11] == ["2026-10", "2027-09"] and len(months) == 12
    _, days, temps = months[0]
    assert days == [date(2026, 10, d) for d in (29, 30, 31)]
    assert temps == [fc.temps["DE"][d] for d in days]
    _, days, temps = months[1]
    assert len(days) == len(temps) == 30
    assert temps[0] == fc.temps["DE"][date(2026, 11, 1)]  # the forecast reaches to 7 November
    assert temps[-1] == normal_temp(DE, date(2026, 11, 30))


def test_zero_shift_gives_the_base_curve():
    fc = load_forecast("sample", MemoryStore())
    assert scenario_curve(Scenario("DE"), fc, TODAY).prices == build_curve("DE", fc, TODAY).prices


def test_cold_shift_raises_only_the_shifted_months():
    fc = load_forecast("sample", MemoryStore())
    base = build_curve("DE", fc, TODAY).prices
    cold = scenario_curve(Scenario("DE", -3.0, date(2026, 12, 1), date(2026, 12, 31)), fc, TODAY).prices
    months = build_curve("DE", fc, TODAY).months
    i = months.index("2026-12")
    assert cold[i] > base[i]
    assert [p for j, p in enumerate(cold) if j != i] == [p for j, p in enumerate(base) if j != i]


def test_a_pricier_peaker_raises_the_winter_prices():
    fc = load_forecast("sample", MemoryStore())
    base = build_curve("DE", fc, TODAY)
    s = Scenario("DE", overrides=(PlantOverride("Peaker", cost_eur_mwh=500.0),))
    pricier = scenario_curve(s, fc, TODAY)
    assert pricier.market == "DE" and pricier.months == base.months
    assert all(p >= b for p, b in zip(pricier.prices, base.prices))
    assert pricier.prices[base.months.index("2027-01")] > base.prices[base.months.index("2027-01")]


def test_overrides_sort_the_plants_into_merit_order_again():
    stack = apply_overrides(DE.stack, [PlantOverride("Coal", cost_eur_mwh=200.0)])
    costs = [p.cost_eur_mwh for p in stack.plants]
    assert costs == sorted(costs) and stack.plants[-1].name == "Coal"


def test_overrides_change_what_they_name_and_keep_the_other_plants():
    stack = apply_overrides(DE.stack, [
        PlantOverride("Coal", capacity_gw=4.0),
        PlantOverride("Peaker", cost_eur_mwh=150.0, capacity_gw=9.0),
    ])
    by_name = {p.name: p for p in stack.plants}
    assert (by_name["Coal"].capacity_gw, by_name["Coal"].cost_eur_mwh) == (4.0, 70)
    assert (by_name["Peaker"].capacity_gw, by_name["Peaker"].cost_eur_mwh) == (9.0, 150.0)
    assert [by_name[n] for n in ("Renewables", "Lignite", "CCGT")] == [
        p for p in DE.stack.plants if p.name in ("Renewables", "Lignite", "CCGT")]
    assert stack.base_demand_gw == DE.stack.base_demand_gw


def test_equal_costs_keep_their_order():
    stack = apply_overrides(DE.stack, [PlantOverride("Coal", cost_eur_mwh=90.0)])
    assert [p.name for p in stack.plants] == ["Renewables", "Lignite", "Coal", "CCGT", "Peaker"]


def test_unknown_plant_names_the_plants():
    with pytest.raises(ScenarioError, match="Unknown plant 'Wind' in DE. Plants: Renewables, Lignite, Coal, CCGT, Peaker."):
        apply_overrides(DE.stack, [PlantOverride("Wind", capacity_gw=5.0)])


def test_each_market_has_its_own_set_of_plant_names_so_the_message_names_the_market():
    names = [frozenset(p.name for p in m.stack.plants) for m in MARKETS]
    assert len(set(names)) == len(MARKETS)  # the message finds the market by its plant names
    for m in MARKETS:
        with pytest.raises(ScenarioError, match=f"Unknown plant 'Wind' in {m.code}\\."):
            apply_overrides(m.stack, [PlantOverride("Wind", capacity_gw=5.0)])


def test_a_stack_with_no_capacity_is_refused():
    zero = [PlantOverride(p.name, capacity_gw=0.0) for p in DE.stack.plants]
    with pytest.raises(ScenarioError, match="The plants have no capacity left."):
        apply_overrides(DE.stack, zero)


@pytest.mark.parametrize("start,end", [(date(2026, 12, 31), date(2026, 12, 1)), (date(2026, 12, 1), date(2027, 11, 1))])
def test_a_bad_window_is_refused(start, end):
    fc = load_forecast("sample", MemoryStore())
    with pytest.raises(ScenarioError):
        scenario_curve(Scenario("DE", -3.0, start, end), fc, TODAY)


@pytest.mark.parametrize("start,end", [
    (date(2026, 10, 28), date(2026, 11, 3)),  # starts before today
    (date(2026, 12, 1), date(2027, 10, 1)),  # ends after the twelfth month
    (date(2026, 12, 1), None),  # no end
    (None, None),  # no window at all
])
def test_a_window_outside_the_curve_names_the_allowed_range(start, end):
    fc = load_forecast("sample", MemoryStore())
    message = "The shift window must lie between 2026-10-29 and 2027-09-30."
    with pytest.raises(ScenarioError, match=message):
        scenario_curve(Scenario("DE", 2.0, start, end), fc, TODAY)


def test_the_window_may_span_the_whole_curve():
    fc = load_forecast("sample", MemoryStore())
    warm = scenario_curve(Scenario("DE", 2.0, TODAY, date(2027, 9, 30)), fc, TODAY)
    assert warm.prices != build_curve("DE", fc, TODAY).prices


def test_zero_shift_ignores_the_window():
    fc = load_forecast("sample", MemoryStore())
    s = Scenario("DE", 0.0, date(2030, 1, 1), date(2020, 1, 1))
    assert scenario_curve(s, fc, TODAY).prices == build_curve("DE", fc, TODAY).prices


def test_stack_view_matches_the_stack_price():
    v = stack_view("de", 4.5)
    assert v["price"] == 170.0 and v["plants"][0]["from_gw"] == 0.0
    assert v["plants"][-1]["to_gw"] == sum(p.capacity_gw for p in DE.stack.plants)


def test_stack_view_lists_the_plants_as_steps_with_the_demand_and_the_overrides():
    v = stack_view("DE", 4.5, [PlantOverride("Peaker", cost_eur_mwh=300.0, capacity_gw=2.0)])
    assert list(v) == ["market", "temp_c", "demand_gw", "price", "plants"]
    assert v["market"] == "DE" and v["temp_c"] == 4.5 and v["demand_gw"] == 64.7
    assert list(v["plants"][0]) == ["name", "capacity_gw", "cost_eur_mwh", "from_gw", "to_gw"]
    assert [(p["from_gw"], p["to_gw"]) for p in v["plants"]] == [
        (0, 20), (20, 35), (35, 45), (45, 60), (60, 62)]
    assert v["plants"][-1]["name"] == "Peaker"
    assert v["price"] == 326.13  # scarcity: demand is 2.7 GW above the 62 GW on offer


def test_the_vectorised_ar1_follows_history_path_by_path():
    shocks = np.random.default_rng(1).normal(0.0, 3.0, (4, 30))
    paths = _ar1_paths(shocks)
    for row, path in zip(shocks, paths):
        np.testing.assert_allclose(path, _ar1(row))


def test_the_vectorised_ar1_starts_in_the_stationary_distribution():
    paths = _ar1_paths(np.random.default_rng(2).normal(0.0, 3.0, (20_000, 12)))
    assert paths[:, 0].std() == pytest.approx(5.0, abs=0.15)  # 3 / sqrt(1 - 0.8 ** 2)
    assert np.corrcoef(paths[:, 5], paths[:, 6])[0, 1] == pytest.approx(0.8, abs=0.02)


def test_band_is_ordered_and_repeatable():
    fc = load_forecast("sample", MemoryStore())
    s = Scenario("DE", -3.0, date(2026, 11, 1), date(2026, 11, 14))
    a = scenario_band(s, fc, TODAY, n_paths=200, seed=7)
    assert a == scenario_band(s, fc, TODAY, n_paths=200, seed=7)
    assert all(lo <= mid <= hi for lo, mid, hi in zip(a["p10"], a["p50"], a["p90"]))
    assert a["base"] == scenario_curve(s, fc, TODAY).prices and a["n_paths"] == 200


def test_band_has_a_value_per_month_and_changes_with_the_seed():
    fc = load_forecast("sample", MemoryStore())
    s = Scenario("DE")
    a = scenario_band(s, fc, TODAY, n_paths=200, seed=7)
    assert list(a) == ["months", "base", "p10", "p50", "p90", "n_paths"]
    assert a["months"] == build_curve("DE", fc, TODAY).months
    assert all(len(a[k]) == 12 for k in ("base", "p10", "p50", "p90"))
    january = a["months"].index("2027-01")
    assert a["p10"][january] < a["p90"][january]  # cold days call on the peaker, mild ones do not
    assert scenario_band(s, fc, TODAY, n_paths=200, seed=8)["p50"] != a["p50"]


def test_band_prices_the_overridden_stack():
    fc = load_forecast("sample", MemoryStore())
    plain = scenario_band(Scenario("DE"), fc, TODAY, n_paths=200)
    pricier = scenario_band(
        Scenario("DE", overrides=(PlantOverride("Peaker", cost_eur_mwh=500.0),)), fc, TODAY, 200)
    assert all(p >= b for p, b in zip(pricier["p90"], plain["p90"]))
    assert pricier["p90"] != plain["p90"]
