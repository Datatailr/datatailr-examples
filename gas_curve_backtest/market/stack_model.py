"""Marginal-cost (merit-order) stack pricing model for power/gas forwards.

Mirrors the language Marco used on the call: the "stack" sets the marginal
clearing price as a function of demand, where demand is itself a function
of temperature (heating + cooling). Different generators have different
marginal costs (coal, CCGT-baseload, CCGT-peaker, oil-peaker), and the
clearing price is the marginal cost of the last unit dispatched.

The model is intentionally simple — fast and Numba-friendly so we can run
millions of cells in the backtest fan-out.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class StackConfig:
    base_demand_gw: float = 280.0
    heating_coef_gw_per_c: float = 8.5
    cooling_coef_gw_per_c: float = 5.5
    heating_threshold_c: float = 15.0
    cooling_threshold_c: float = 22.0
    capacities_gw: tuple = (110.0, 90.0, 70.0, 60.0, 40.0)
    marginal_costs_eur_mwh: tuple = (35.0, 55.0, 78.0, 110.0, 180.0)
    risk_premium_eur_mwh: float = 4.5


def demand_from_temperature(temp_c: np.ndarray, cfg: StackConfig) -> np.ndarray:
    """Convert temperature (C) to total system demand (GW)."""
    heating = np.maximum(cfg.heating_threshold_c - temp_c, 0.0) * cfg.heating_coef_gw_per_c
    cooling = np.maximum(temp_c - cfg.cooling_threshold_c, 0.0) * cfg.cooling_coef_gw_per_c
    return cfg.base_demand_gw + heating + cooling


def clearing_price(demand_gw: np.ndarray, cfg: StackConfig) -> np.ndarray:
    """Clear demand against the merit-order stack and return price (EUR/MWh)."""
    capacities = np.asarray(cfg.capacities_gw, dtype=np.float64)
    costs = np.asarray(cfg.marginal_costs_eur_mwh, dtype=np.float64)
    cum = np.cumsum(capacities)

    flat = np.atleast_1d(demand_gw).astype(np.float64)
    out = np.empty_like(flat)
    for i, d in enumerate(flat):
        idx = np.searchsorted(cum, d, side="left")
        if idx >= len(costs):
            slack = (d - cum[-1]) / max(capacities[-1], 1e-6)
            out[i] = costs[-1] * (1.0 + 0.5 * slack)
        else:
            out[i] = costs[idx]
    return out.reshape(np.shape(demand_gw)) if np.ndim(demand_gw) > 0 else out[0]


def price_from_temperature_ensemble(
    temp_ensemble: np.ndarray,
    cfg: StackConfig | None = None,
) -> np.ndarray:
    """Vectorised: temperatures (any shape) -> EUR/MWh prices (same shape).

    The clearing price ramps **within** each merit-order tier so the
    resulting distribution is continuous (otherwise many ensemble
    members would coincide on the same tier price, collapsing
    downstream percentile statistics).
    """
    cfg = cfg or StackConfig()
    demand = demand_from_temperature(temp_ensemble, cfg)
    capacities = np.asarray(cfg.capacities_gw, dtype=np.float64)
    costs = np.asarray(cfg.marginal_costs_eur_mwh, dtype=np.float64)
    cum = np.cumsum(capacities)
    starts = np.concatenate([[0.0], cum[:-1]])

    idx = np.searchsorted(cum, demand, side="left")
    idx = np.clip(idx, 0, len(costs) - 1)

    next_costs = np.concatenate([costs[1:], costs[-1:] * 1.25])
    width = capacities[idx]
    fill = (demand - starts[idx]) / np.maximum(width, 1e-6)
    fill = np.clip(fill, 0.0, 1.0)
    price = costs[idx] + fill * (next_costs[idx] - costs[idx])

    overload_mask = demand > cum[-1]
    if np.any(overload_mask):
        slack = (demand - cum[-1]) / max(capacities[-1], 1e-6)
        price = np.where(overload_mask, costs[-1] * (1.4 + 0.5 * slack), price)

    return price + cfg.risk_premium_eur_mwh
