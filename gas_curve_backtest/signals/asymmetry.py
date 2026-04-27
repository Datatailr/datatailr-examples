"""Distribution asymmetry — Marco's "risk-reward" framing.

For each (day, tenor) we measure how skewed the ensemble of model prices
is around its median:

    asymmetry = (P90 - P50) / (P50 - P10)

> 1   : upside fatter than downside (favours longs)
< 1   : downside fatter than upside (favours shorts)

We also expose the absolute spread (P90-P10) which captures
forecast-uncertainty and feeds the regime detector.
"""

from __future__ import annotations

import numpy as np


def asymmetry_and_spread(
    model_price_ensemble: np.ndarray,
    low_q: float = 0.10,
    mid_q: float = 0.50,
    high_q: float = 0.90,
    floor_eur_mwh: float = 0.75,
    cap: float = 6.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (asymmetry, spread, median) on the model-price ensemble.

    A small `floor_eur_mwh` prevents division-by-near-zero when an
    ensemble collapses inside a single merit-order tier, and `cap`
    bounds the resulting ratio so it remains a useful clustering
    feature.
    """
    qs = np.quantile(model_price_ensemble, [low_q, mid_q, high_q], axis=2)
    p_lo, p_mid, p_hi = qs[0], qs[1], qs[2]
    upside = np.maximum(p_hi - p_mid, floor_eur_mwh)
    downside = np.maximum(p_mid - p_lo, floor_eur_mwh)
    asym = np.clip(upside / downside, 1.0 / cap, cap)
    spread = p_hi - p_lo
    return asym, spread, p_mid
