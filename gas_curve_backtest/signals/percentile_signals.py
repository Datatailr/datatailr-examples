"""Where does the market mid sit inside the model price distribution?

For each (day, tenor) we compute the empirical percentile of the market
mid against the ensemble of model prices. Values near 0 mean the market
is rich vs the model's downside; values near 1 mean it's cheap vs the
model's upside (or equivalently the market is below the median model).

Signal is rescaled to [-1, +1] with 0 meaning "market at model median".
"""

from __future__ import annotations

import numpy as np


def market_percentile(
    market_mid: np.ndarray,
    model_price_ensemble: np.ndarray,
) -> np.ndarray:
    if model_price_ensemble.ndim != 3:
        raise ValueError("model_price_ensemble must be (days, tenors, members)")
    n_members = model_price_ensemble.shape[2]
    sorted_ens = np.sort(model_price_ensemble, axis=2)
    pct = np.empty_like(market_mid, dtype=np.float64)
    for d in range(market_mid.shape[0]):
        for t in range(market_mid.shape[1]):
            pct[d, t] = np.searchsorted(sorted_ens[d, t], market_mid[d, t], side="left") / n_members
    return pct


def percentile_signal(market_mid: np.ndarray, model_price_ensemble: np.ndarray) -> np.ndarray:
    pct = market_percentile(market_mid, model_price_ensemble)
    return 2.0 * (0.5 - pct)
