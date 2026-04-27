"""Forward curve generator.

Combines:
  - the ECMWF-style temperature ensemble
  - the merit-order stack pricing model
  - tenor-specific calendar effects (winter contracts price higher)
  - a market mid that drifts around the model's central forecast with
    autocorrelated noise (so trades are non-trivial)

Returns a tidy dict ready to be written to Blob storage and consumed by
the backtest workflow.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from gas_curve_backtest.market.ecmwf_simulator import (
    EnsembleConfig,
    simulate_ensemble,
)
from gas_curve_backtest.market.stack_model import (
    StackConfig,
    price_from_temperature_ensemble,
)


@dataclass(frozen=True)
class CurveConfig:
    n_days: int = 750
    n_tenors: int = 12
    market_noise_eur_mwh: float = 4.0
    market_ar_coef: float = 0.85
    seed: int = 11


def _tenor_temperature_offset(n_tenors: int) -> np.ndarray:
    """Each tenor (M+1..M+N) sits in a different forward month.

    We bias each tenor's expected temperature relative to the spot day so
    that further-out winter tenors actually see colder weather (i.e. they
    cost more in the stack). Implemented as a soft sinusoid.
    """
    months_ahead = np.arange(1, n_tenors + 1, dtype=float)
    return 4.0 * np.cos(2 * np.pi * months_ahead / 12.0) - 2.0


def generate_history(
    cfg: CurveConfig | None = None,
    ens_cfg: EnsembleConfig | None = None,
    stack_cfg: StackConfig | None = None,
) -> dict:
    cfg = cfg or CurveConfig()
    ens_cfg = ens_cfg or EnsembleConfig()
    stack_cfg = stack_cfg or StackConfig()
    rng = np.random.default_rng(cfg.seed)

    ens = simulate_ensemble(cfg.n_days, ens_cfg)
    raw_ensemble = ens["ensemble"]

    tenor_offset = _tenor_temperature_offset(cfg.n_tenors)
    H = raw_ensemble.shape[1]
    M = raw_ensemble.shape[2]
    tenor_ensemble = np.empty((cfg.n_days, cfg.n_tenors, M), dtype=np.float64)
    for t in range(cfg.n_tenors):
        h_idx = min(H - 1, t)
        tenor_ensemble[:, t, :] = raw_ensemble[:, h_idx, :] + tenor_offset[t]

    model_price_ensemble = price_from_temperature_ensemble(tenor_ensemble, stack_cfg)

    model_mean = model_price_ensemble.mean(axis=2)
    market_mid = np.empty_like(model_mean)
    market_mid[0] = model_mean[0]
    noise = rng.normal(0.0, cfg.market_noise_eur_mwh, size=model_mean.shape)
    for d in range(1, cfg.n_days):
        market_mid[d] = (
            cfg.market_ar_coef * market_mid[d - 1]
            + (1 - cfg.market_ar_coef) * model_mean[d]
            + noise[d]
        )

    spreads = 0.6 + 0.05 * np.arange(cfg.n_tenors, dtype=float)
    bid = market_mid - spreads
    ask = market_mid + spreads

    return {
        "n_days": cfg.n_days,
        "n_tenors": cfg.n_tenors,
        "model_price_ensemble": model_price_ensemble,
        "model_mean": model_mean,
        "market_mid": market_mid,
        "bid": bid,
        "ask": ask,
        "tenor_offset": tenor_offset,
        "spread_regime": ens["spread_regime"],
        "realised_temp": ens["realised"],
    }
