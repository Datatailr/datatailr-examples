import numpy as np
import pytest

from merit_desk.common.stack import Plant, StackConfig, demand_gw, price, stack_price

DE = StackConfig(
    base_demand_gw=50.0, heating_gw_per_c=1.4, cooling_gw_per_c=0.6,
    plants=(Plant("Renewables", 20, 5), Plant("Lignite", 15, 45), Plant("Coal", 10, 70),
            Plant("CCGT", 15, 90), Plant("Peaker", 8, 170)),
)


def test_demand_uses_heating_and_cooling():
    assert demand_gw(15.0, DE) == 50.0
    assert demand_gw(4.5, DE) == pytest.approx(64.7)
    assert demand_gw(25.0, DE) == pytest.approx(51.8)


def test_price_is_cost_of_last_plant_that_runs():
    assert stack_price(4.5, DE) == 170.0
    assert stack_price(15.0, DE) == 90.0


def test_scarcity_above_total_capacity():
    # demand 78 GW > 68 GW capacity: last cost * (1 + 2 * (78 - 68) / 68)
    assert stack_price(-5.0, DE) == pytest.approx(220.0, rel=1e-3)


def test_price_never_falls_as_demand_rises():
    prices = price(np.linspace(0, 90, 500), DE)
    assert np.all(np.diff(prices) >= 0)


def test_scalar_in_float_out():
    assert type(stack_price(10.0, DE)) is float
