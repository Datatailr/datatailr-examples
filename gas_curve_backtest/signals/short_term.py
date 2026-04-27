"""Short-term ECMWF-driven directional signal.

The ensemble forecast issued today vs the seasonal climatology gives a
directional view: if the next two weeks are forecast much colder than
seasonal, demand-and-therefore-price should rise.

We collapse this to a single short-term score per day, broadcast across
tenors (the same view applies to the front of the curve more strongly,
which is why we down-weight by tenor index).
"""

from __future__ import annotations

import numpy as np


def short_term_signal(
    realised_temp: np.ndarray,
    seasonal: np.ndarray | None = None,
    n_tenors: int = 12,
) -> np.ndarray:
    n = realised_temp.shape[0]
    if seasonal is None:
        seasonal = np.zeros(n)
    anomaly = -(realised_temp - seasonal)
    smoothed = np.convolve(anomaly, np.ones(5) / 5.0, mode="same")
    if smoothed.std() > 0:
        z = smoothed / (smoothed.std() + 1e-9)
    else:
        z = smoothed
    weights = np.exp(-np.arange(n_tenors) / 4.0)
    return z[:, None] * weights[None, :] * 0.4
