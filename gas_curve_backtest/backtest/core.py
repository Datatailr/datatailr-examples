"""Numba-accelerated single-cell backtest.

A "cell" is one (regime, tenor, threshold) triple. Given the daily
combined signal and the daily PnL-per-unit-position, we produce an
equity curve. Position sizing is asymmetry-aware: the upside/downside
ratio scales the size when the distribution favours one side.

The kernel is intentionally tight so that thousands of cells fit
comfortably inside one container: this is exactly the workload Marco
described as currently sitting in his Numba code.
"""

from __future__ import annotations

import numpy as np

try:
    from numba import njit

    _HAS_NUMBA = True
except ImportError:
    _HAS_NUMBA = False

    def njit(*args, **kwargs):
        if len(args) == 1 and callable(args[0]):
            return args[0]

        def _wrap(fn):
            return fn

        return _wrap


@njit(cache=True, fastmath=True)
def _backtest_cell_with_indices(
    signal: np.ndarray,
    asymmetry: np.ndarray,
    pnl_per_unit: np.ndarray,
    indices: np.ndarray,
    threshold: float,
    asym_pivot: float,
    cost_per_trade: float,
) -> np.ndarray:
    n = indices.shape[0]
    equity = np.empty(n + 1, dtype=np.float64)
    equity[0] = 0.0
    pos = 0.0
    for k in range(n):
        t = indices[k]
        s = signal[t]
        a = asymmetry[t]
        if s > threshold:
            size = 1.0 * (a / asym_pivot)
        elif s < -threshold:
            size = -1.0 * (asym_pivot / a)
        else:
            size = 0.0
        if size > 3.0:
            size = 3.0
        elif size < -3.0:
            size = -3.0
        trade_cost = 0.0
        if (pos > 0.0 and size <= 0.0) or (pos < 0.0 and size >= 0.0) or (pos == 0.0 and size != 0.0):
            trade_cost = cost_per_trade * abs(size - pos)
        equity[k + 1] = equity[k] + pos * pnl_per_unit[t] - trade_cost
        pos = size
    return equity


@njit(cache=True, fastmath=True)
def _backtest_cell(
    signal: np.ndarray,
    asymmetry: np.ndarray,
    pnl_per_unit: np.ndarray,
    threshold: float,
    asym_pivot: float,
    cost_per_trade: float,
) -> np.ndarray:
    n = signal.shape[0]
    equity = np.empty(n + 1, dtype=np.float64)
    equity[0] = 0.0
    pos = 0.0
    for t in range(n):
        s = signal[t]
        a = asymmetry[t]
        if s > threshold:
            size = 1.0 * (a / asym_pivot)
        elif s < -threshold:
            size = -1.0 * (asym_pivot / a)
        else:
            size = 0.0
        if size > 3.0:
            size = 3.0
        elif size < -3.0:
            size = -3.0
        trade_cost = 0.0
        if (pos > 0.0 and size <= 0.0) or (pos < 0.0 and size >= 0.0) or (pos == 0.0 and size != 0.0):
            trade_cost = cost_per_trade * abs(size - pos)
        equity[t + 1] = equity[t] + pos * pnl_per_unit[t] - trade_cost
        pos = size
    return equity


def backtest_cell(
    signal: np.ndarray,
    asymmetry: np.ndarray,
    pnl_per_unit: np.ndarray,
    threshold: float,
    asym_pivot: float = 1.0,
    cost_per_trade: float = 0.05,
) -> np.ndarray:
    return _backtest_cell(
        np.ascontiguousarray(signal, dtype=np.float64),
        np.ascontiguousarray(asymmetry, dtype=np.float64),
        np.ascontiguousarray(pnl_per_unit, dtype=np.float64),
        float(threshold),
        float(asym_pivot),
        float(cost_per_trade),
    )


def block_bootstrap_indices(
    n: int,
    block_size: int,
    n_samples: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Stationary block bootstrap of indices into a length-n series.

    Returns shape (n_samples, n). Used to evaluate the same threshold
    across many resampled histories, which is the standard test for
    "is this Sharpe a fluke?".
    """
    if n <= 0:
        return np.zeros((n_samples, 0), dtype=np.int64)
    block_size = max(1, min(block_size, n))
    n_blocks = (n + block_size - 1) // block_size
    out = np.empty((n_samples, n), dtype=np.int64)
    for s in range(n_samples):
        starts = rng.integers(0, n, size=n_blocks)
        idx = []
        for st in starts:
            idx.extend(range(st, st + block_size))
        idx = np.array(idx[:n], dtype=np.int64) % n
        out[s] = idx
    return out


def bootstrap_summarise(
    signal: np.ndarray,
    asymmetry: np.ndarray,
    pnl_per_unit: np.ndarray,
    threshold: float,
    asym_pivot: float = 1.0,
    n_samples: int = 64,
    block_size: int = 20,
    cost_per_trade: float = 0.05,
    seed: int = 0,
) -> dict:
    """Run the cell across `n_samples` bootstrapped paths, return Sharpe stats."""
    n = signal.shape[0]
    if n_samples <= 1:
        eq = backtest_cell(signal, asymmetry, pnl_per_unit, threshold, asym_pivot, cost_per_trade)
        d = np.diff(eq)
        sharpe = (d.mean() / d.std() * np.sqrt(252)) if d.std() > 1e-12 else 0.0
        return {"sharpe_mean": float(sharpe), "sharpe_std": 0.0, "sharpe_p05": float(sharpe), "n_samples": 1}

    rng = np.random.default_rng(seed)
    indices = block_bootstrap_indices(n, block_size, n_samples, rng)
    sig = np.ascontiguousarray(signal, dtype=np.float64)
    asy = np.ascontiguousarray(asymmetry, dtype=np.float64)
    pnl = np.ascontiguousarray(pnl_per_unit, dtype=np.float64)
    sharpes = np.empty(n_samples, dtype=np.float64)
    for s in range(n_samples):
        eq = _backtest_cell_with_indices(sig, asy, pnl, indices[s], float(threshold), float(asym_pivot), float(cost_per_trade))
        d = np.diff(eq)
        sd = d.std()
        sharpes[s] = (d.mean() / sd * np.sqrt(252)) if sd > 1e-12 else 0.0
    return {
        "sharpe_mean": float(sharpes.mean()),
        "sharpe_std": float(sharpes.std()),
        "sharpe_p05": float(np.quantile(sharpes, 0.05)),
        "n_samples": int(n_samples),
    }


def warmup_jit() -> None:
    """Trigger Numba JIT compilation upfront so per-cell timings are honest."""
    s = np.zeros(8)
    backtest_cell(s, np.ones(8), np.zeros(8), 0.1)
    _backtest_cell_with_indices(s, np.ones(8), np.zeros(8), np.arange(8), 0.1, 1.0, 0.05)
