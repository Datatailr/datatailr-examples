"""Performance metrics on an equity curve."""

from __future__ import annotations

import numpy as np


def summarise_equity(equity: np.ndarray, ann_days: int = 252) -> dict:
    if equity.size < 2:
        return {"pnl": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "hit_rate": 0.0, "turnover": 0.0}
    daily = np.diff(equity)
    mu = daily.mean()
    sd = daily.std()
    sharpe = float(mu / sd * np.sqrt(ann_days)) if sd > 1e-12 else 0.0

    cummax = np.maximum.accumulate(equity)
    drawdown = equity - cummax
    max_dd = float(drawdown.min())

    nonzero = daily[daily != 0.0]
    hit_rate = float((nonzero > 0).mean()) if nonzero.size else 0.0

    return {
        "pnl": float(equity[-1]),
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "hit_rate": hit_rate,
        "n_trading_days": int((daily != 0).sum()),
    }
