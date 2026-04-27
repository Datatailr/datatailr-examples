"""ECMWF-style ensemble weather forecast simulator.

Generates an ensemble of day-ahead temperature forecasts for European
gas/power markets. The mean follows a seasonal sinusoid; ensemble spread
grows with horizon (forecast skill decays). Spread is non-stationary so
that downstream regimes (high-uncertainty vs calm) actually emerge.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class EnsembleConfig:
    n_members: int = 51
    horizon_days: int = 14
    base_temp_c: float = 9.0
    seasonal_amp_c: float = 12.0
    base_spread_c: float = 0.6
    horizon_spread_growth: float = 0.35
    regime_shock_prob: float = 0.04
    regime_shock_spread_mult: float = 3.0
    seed: int = 7


def _seasonal_temp(day_of_year: np.ndarray, cfg: EnsembleConfig) -> np.ndarray:
    """Northern-hemisphere heating-season sinusoid, coldest in early Feb."""
    phase = 2 * np.pi * (day_of_year - 32) / 365.25
    return cfg.base_temp_c - cfg.seasonal_amp_c * np.cos(phase)


def simulate_ensemble(
    n_days: int,
    cfg: EnsembleConfig | None = None,
    start_doy: int = 274,
) -> dict:
    """Return ensemble forecasts of shape (n_days, horizon, members).

    The forecast issued on day t covers horizons h=1..H.
    Realised temperature for day t is one draw from the t-issued, h=0
    distribution (which we collapse to a deterministic value: the seasonal
    mean plus a single common-shock term).
    """
    cfg = cfg or EnsembleConfig()
    rng = np.random.default_rng(cfg.seed)

    doy = (np.arange(n_days) + start_doy) % 365
    seasonal = _seasonal_temp(doy.astype(float), cfg)

    common_shock = rng.normal(0.0, 1.5, size=n_days)
    realised = seasonal + common_shock

    spread_regime = np.ones(n_days)
    shocks = rng.uniform(0, 1, size=n_days) < cfg.regime_shock_prob
    decay = 0.0
    boost = np.zeros(n_days)
    for i in range(n_days):
        if shocks[i]:
            decay = cfg.regime_shock_spread_mult
        boost[i] = decay
        decay = max(1.0, decay * 0.85)
    spread_regime = boost

    horizons = np.arange(1, cfg.horizon_days + 1, dtype=float)
    horizon_spread = cfg.base_spread_c * (1.0 + cfg.horizon_spread_growth * horizons)

    ensemble = np.empty((n_days, cfg.horizon_days, cfg.n_members), dtype=np.float64)
    for t in range(n_days):
        for h in range(cfg.horizon_days):
            day_idx = min(n_days - 1, t + h)
            mean = seasonal[day_idx]
            spread = horizon_spread[h] * spread_regime[t]
            ensemble[t, h, :] = rng.normal(mean, spread, size=cfg.n_members)

    return {
        "ensemble": ensemble,
        "realised": realised,
        "seasonal": seasonal,
        "doy": doy,
        "spread_regime": spread_regime,
    }
