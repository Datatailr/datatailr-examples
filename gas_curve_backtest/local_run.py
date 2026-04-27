"""Run the full backtest **locally** with the exact same Python kernels.

Used by the dashboard's "Run on laptop" button and by humans who want
to time the platform-vs-laptop contrast. Writes the same blob layout
that the deployed workflow does, so the dashboard reads it back the
same way.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import time
from pathlib import Path

import numpy as np

from gas_curve_backtest.backtest.core import backtest_cell, bootstrap_summarise, warmup_jit
from gas_curve_backtest.backtest.grid import default_grid, regime_aware_grid
from gas_curve_backtest.backtest.metrics import summarise_equity
from gas_curve_backtest.market.curve_generator import CurveConfig, generate_history
from gas_curve_backtest.signals.asymmetry import asymmetry_and_spread
from gas_curve_backtest.signals.percentile_signals import percentile_signal
from gas_curve_backtest.signals.short_term import short_term_signal
from gas_curve_backtest.workflows import blob_paths

LOCAL_FALLBACK_ROOT = Path(os.environ.get("GAS_BACKTEST_LOCAL_ROOT", "/tmp/gas_curve_backtest"))


class _LocalBlob:
    """Filesystem-backed Blob shim so local runs need no Datatailr connection."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key.lstrip("/")

    def put(self, key: str, data: bytes | str) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(data, str):
            data = data.encode("utf-8")
        p.write_bytes(data)

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def ls(self, prefix: str) -> list[str]:
        p = self._path(prefix)
        if not p.exists():
            return []
        return [str("/" + str(f.relative_to(self.root))) for f in p.rglob("*") if f.is_file()]

    def exists(self, key: str) -> bool:
        return self._path(key).exists()


def _get_blob(local: bool):
    if local:
        return _LocalBlob(LOCAL_FALLBACK_ROOT)
    from datatailr import Blob
    return Blob()


def _put_npz(blob, key: str, **arrays) -> None:
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    blob.put(key, buf.getvalue())


def _put_json(blob, key: str, payload) -> None:
    blob.put(key, json.dumps(payload, default=str).encode("utf-8"))


def run_locally(
    run_id: str | None = None,
    n_days: int = 500,
    n_tenors: int = 6,
    n_regimes: int = 4,
    grid_signal_steps: int = 7,
    grid_pivot_steps: int = 3,
    bootstrap_samples: int = 16,
    use_datatailr_blob: bool = False,
    progress_cb=None,
) -> dict:
    """Run the entire pipeline serially in this process."""
    from sklearn.cluster import KMeans

    rid = run_id or f"local-{time.strftime('%Y%m%d-%H%M%S')}"
    blob = _get_blob(local=not use_datatailr_blob)
    timings: dict[str, float] = {}

    if progress_cb:
        progress_cb("Generating market", 0.0)
    t0 = time.time()
    h = generate_history(CurveConfig(n_days=n_days, n_tenors=n_tenors))
    timings["generate_market"] = time.time() - t0
    _put_npz(
        blob,
        blob_paths.market_data(rid),
        model_price_ensemble=h["model_price_ensemble"],
        market_mid=h["market_mid"],
        bid=h["bid"],
        ask=h["ask"],
        realised_temp=h["realised_temp"],
        spread_regime=h["spread_regime"],
    )

    if progress_cb:
        progress_cb("Computing signals", 0.1)
    t0 = time.time()
    pct = percentile_signal(h["market_mid"], h["model_price_ensemble"])
    asym, spread, _p_mid = asymmetry_and_spread(h["model_price_ensemble"])
    st = short_term_signal(h["realised_temp"], n_tenors=n_tenors)
    combined = pct + 0.5 * st
    pnl_per_unit = np.diff(h["market_mid"], axis=0, prepend=h["market_mid"][0:1])
    timings["compute_signals"] = time.time() - t0

    _put_npz(
        blob,
        blob_paths.signals(rid),
        percentile_signal=pct,
        asymmetry=asym,
        spread=spread,
        short_term=st,
        combined_signal=combined,
        pnl_per_unit=pnl_per_unit,
    )

    if progress_cb:
        progress_cb("Detecting regimes", 0.2)
    feat_asym = np.log(np.clip(asym.mean(axis=1), 1e-3, None))
    feat_spread = (spread.mean(axis=1) - spread.mean()) / (spread.std() + 1e-9)
    feat_sig = np.tanh(combined.mean(axis=1))
    X = np.stack([feat_asym, feat_spread, feat_sig], axis=1)
    km = KMeans(n_clusters=n_regimes, n_init=8, random_state=0).fit(X)
    labels = km.labels_.astype(int)

    regimes = []
    for r in range(n_regimes):
        mask = labels == r
        if mask.sum() < 5:
            continue
        regimes.append(
            {
                "regime_id": int(r),
                "size": int(mask.sum()),
                "median_asymmetry": float(np.median(asym[mask])),
                "median_spread": float(np.median(spread[mask])),
                "median_signal": float(np.median(combined[mask])),
                "day_indices": np.where(mask)[0].astype(int).tolist(),
            }
        )
    expected_cells = (
        len(regimes) * n_tenors * grid_signal_steps * grid_pivot_steps
    )
    _put_json(
        blob,
        blob_paths.regimes(rid),
        {
            "regimes": regimes,
            "n_days": int(n_days),
            "n_tenors": int(n_tenors),
            "grid_signal_steps": int(grid_signal_steps),
            "grid_pivot_steps": int(grid_pivot_steps),
            "bootstrap_samples": int(bootstrap_samples),
            "expected_cells": int(expected_cells),
        },
    )

    warmup_jit()

    if progress_cb:
        progress_cb("Backtesting cells", 0.25)
    base = default_grid(n_signal=grid_signal_steps, n_pivot=grid_pivot_steps)
    total_cells = sum(base.size for _ in regimes) * n_tenors
    done = 0
    rows = []
    t0 = time.time()
    for regime in regimes:
        grid = regime_aware_grid(regime, base)
        days = np.array(regime["day_indices"], dtype=int)
        for tenor in range(n_tenors):
            sig = combined[days, tenor]
            asy = asym[days, tenor]
            pnl = pnl_per_unit[days, tenor]
            for sig_idx, st_th in enumerate(grid.signal_thresholds):
                for pivot_idx, ap in enumerate(grid.asym_pivots):
                    eq = backtest_cell(sig, asy, pnl, st_th, ap)
                    metrics = summarise_equity(eq)
                    boot = bootstrap_summarise(
                        sig, asy, pnl, st_th, ap, n_samples=bootstrap_samples
                    )
                    metrics.update(
                        {
                            "regime_id": int(regime["regime_id"]),
                            "tenor": int(tenor),
                            "sig_threshold": float(st_th),
                            "asym_pivot": float(ap),
                            "sig_idx": int(sig_idx),
                            "pivot_idx": int(pivot_idx),
                            "n_days": int(days.size),
                            "sharpe_boot_mean": boot["sharpe_mean"],
                            "sharpe_boot_std": boot["sharpe_std"],
                            "sharpe_boot_p05": boot["sharpe_p05"],
                            "bootstrap_samples": boot["n_samples"],
                        }
                    )
                    rows.append(metrics)
                    _put_json(
                        blob,
                        blob_paths.cell_result(rid, int(regime["regime_id"]), tenor, sig_idx, pivot_idx),
                        metrics,
                    )
                    done += 1
                    if progress_cb and done % max(1, total_cells // 50) == 0:
                        progress_cb(
                            f"Backtested {done}/{total_cells} cells",
                            0.25 + 0.7 * (done / max(1, total_cells)),
                        )
    timings["backtest"] = time.time() - t0

    by_regime: dict[int, dict] = {}
    for row in rows:
        r = int(row["regime_id"])
        cur = by_regime.setdefault(r, {"best_sharpe": -1e9, "best": None})
        if row["sharpe"] > cur["best_sharpe"]:
            cur["best_sharpe"] = float(row["sharpe"])
            cur["best"] = row
    summary = {"cells": len(rows), "best_per_regime": list(by_regime.values()), "timings": timings}
    _put_json(blob, blob_paths.aggregated(rid), summary)
    blob.put(blob_paths.latest_pointer(), rid.encode("utf-8"))

    if progress_cb:
        progress_cb("Done", 1.0)
    return {"run_id": rid, "summary": summary, "total_cells": total_cells, "timings": timings}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-days", type=int, default=500)
    parser.add_argument("--n-tenors", type=int, default=6)
    parser.add_argument("--n-regimes", type=int, default=4)
    parser.add_argument("--sig-steps", type=int, default=7)
    parser.add_argument("--pivot-steps", type=int, default=3)
    parser.add_argument("--bootstrap", type=int, default=16)
    parser.add_argument("--use-blob", action="store_true", help="Write to Datatailr Blob (default: local /tmp)")
    args = parser.parse_args()
    out = run_locally(
        n_days=args.n_days,
        n_tenors=args.n_tenors,
        n_regimes=args.n_regimes,
        grid_signal_steps=args.sig_steps,
        grid_pivot_steps=args.pivot_steps,
        bootstrap_samples=args.bootstrap,
        use_datatailr_blob=args.use_blob,
    )
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
