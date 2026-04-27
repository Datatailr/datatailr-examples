"""Threshold grid generator.

The grid is two-dimensional: percentile-signal threshold (the
"how-cheap-vs-model" cutoff) and an asymmetry pivot (the "how
risk-reward-skewed before we trade" cutoff). The set of valid pivots
adapts to the regime, which is why the grid has to be built **after**
we've seen the data and clustered the regimes.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class ThresholdGrid:
    signal_thresholds: tuple
    asym_pivots: tuple

    def cells(self) -> list[tuple[float, float]]:
        return [(s, a) for s in self.signal_thresholds for a in self.asym_pivots]

    @property
    def size(self) -> int:
        return len(self.signal_thresholds) * len(self.asym_pivots)


def default_grid(
    n_signal: int = 21,
    n_pivot: int = 9,
    sig_lo: float = 0.05,
    sig_hi: float = 0.65,
    pivot_lo: float = 0.6,
    pivot_hi: float = 1.6,
) -> ThresholdGrid:
    return ThresholdGrid(
        signal_thresholds=tuple(np.round(np.linspace(sig_lo, sig_hi, n_signal), 4).tolist()),
        asym_pivots=tuple(np.round(np.linspace(pivot_lo, pivot_hi, n_pivot), 4).tolist()),
    )


def regime_aware_grid(regime_stats: dict, base: ThresholdGrid | None = None) -> ThresholdGrid:
    """Slide the asymmetry pivots toward the regime's median asymmetry.

    This is exactly the data-dependence Marco described: the grid we
    want to evaluate depends on the regime detected at runtime, so we
    cannot pre-bake the workflow shape.
    """
    base = base or default_grid()
    median_asym = float(regime_stats.get("median_asymmetry", 1.0))
    centre = max(0.5, min(1.8, median_asym))
    span = 0.45
    pivots = tuple(
        np.round(np.linspace(centre - span, centre + span, len(base.asym_pivots)), 4).tolist()
    )
    return ThresholdGrid(signal_thresholds=base.signal_thresholds, asym_pivots=pivots)
