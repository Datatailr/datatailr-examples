"""Merit-order stack model: temperature drives demand, demand sets the price.

Plants run cheapest first. The price is the cost of the last plant that has to
run to meet demand. Above total capacity the price spikes (scarcity).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Plant:
    name: str
    capacity_gw: float
    cost_eur_mwh: float


@dataclass(frozen=True)
class StackConfig:
    base_demand_gw: float
    heating_gw_per_c: float
    cooling_gw_per_c: float
    plants: tuple[Plant, ...]  # in merit order, cheapest first
    heating_threshold_c: float = 15.0
    cooling_threshold_c: float = 22.0


def _plain(values: np.ndarray) -> float | np.ndarray:
    """A scalar in gives a plain float out, because Excel and JSON need one."""
    return float(values) if values.ndim == 0 else values


def demand_gw(temp_c: float | np.ndarray, cfg: StackConfig) -> float | np.ndarray:
    """Demand in GW: a base load plus heating below and cooling above the thresholds."""
    t = np.asarray(temp_c, dtype=float)
    heating = np.maximum(cfg.heating_threshold_c - t, 0.0)
    cooling = np.maximum(t - cfg.cooling_threshold_c, 0.0)
    return _plain(cfg.base_demand_gw + cfg.heating_gw_per_c * heating + cfg.cooling_gw_per_c * cooling)


def price(demand: float | np.ndarray, cfg: StackConfig) -> float | np.ndarray:
    """Price in EUR/MWh: the cost of the first plant whose cumulative capacity meets demand."""
    d = np.asarray(demand, dtype=float)
    capacity = np.cumsum([p.capacity_gw for p in cfg.plants])
    cost = np.array([p.cost_eur_mwh for p in cfg.plants])
    total, last_cost = capacity[-1], cost[-1]
    merit = cost[np.minimum(np.searchsorted(capacity, d), len(cost) - 1)]
    scarcity = last_cost * (1 + 2 * (d - total) / total)
    return _plain(np.where(d > total, scarcity, merit))


def stack_price(temp_c: float | np.ndarray, cfg: StackConfig) -> float | np.ndarray:
    """Price in EUR/MWh for a temperature in degrees C."""
    return price(demand_gw(temp_c, cfg), cfg)
