"""@task functions used by the parent and child backtest workflows.

Each task's return value is automatically persisted by the Datatailr
platform — we only write to Blob storage explicitly when the data is
either too large for the cache (numpy arrays) or needs a stable,
predictable key for the dashboard to poll without knowing the batch
run id (regimes, cell results, aggregated summary).
"""

from __future__ import annotations

import io
import json
import os
from typing import Any

import numpy as np

from datatailr import task
from datatailr.logging import DatatailrLogger

from gas_curve_backtest.workflows import blob_paths

logger = DatatailrLogger(__name__).get_logger()


def _blob():
    from datatailr import Blob

    return Blob()


def _put_npz(key: str, **arrays) -> None:
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    _blob().put(key, buf.getvalue())


def _get_npz(key: str) -> dict[str, np.ndarray]:
    raw = _blob().get(key)
    with np.load(io.BytesIO(raw)) as data:
        return {k: data[k] for k in data.files}


def _put_json(key: str, payload: Any) -> None:
    _blob().put(key, json.dumps(payload, default=str).encode("utf-8"))


def _get_json(key: str) -> Any:
    return json.loads(_blob().get(key).decode("utf-8"))


@task(memory="1g", cpu=1)
def generate_market(run_id: str, n_days: int = 750, n_tenors: int = 8, seed: int = 11) -> str:
    """Stage 1 — synthesise forward curves, ECMWF ensembles and market mid."""
    from gas_curve_backtest.market.curve_generator import CurveConfig, generate_history

    h = generate_history(CurveConfig(n_days=n_days, n_tenors=n_tenors, seed=seed))
    _put_npz(
        blob_paths.market_data(run_id),
        model_price_ensemble=h["model_price_ensemble"],
        market_mid=h["market_mid"],
        bid=h["bid"],
        ask=h["ask"],
        realised_temp=h["realised_temp"],
        spread_regime=h["spread_regime"],
    )
    _blob().put(blob_paths.latest_pointer(), run_id.encode("utf-8"))
    return run_id


@task(memory="2g", cpu=1)
def compute_signals(run_id: str) -> str:
    """Stage 2 — derive percentile signal, asymmetry, short-term overlay."""
    from gas_curve_backtest.signals.asymmetry import asymmetry_and_spread
    from gas_curve_backtest.signals.percentile_signals import percentile_signal
    from gas_curve_backtest.signals.short_term import short_term_signal

    market = _get_npz(blob_paths.market_data(run_id))
    pct_sig = percentile_signal(market["market_mid"], market["model_price_ensemble"])
    asym, spread, p_mid = asymmetry_and_spread(market["model_price_ensemble"])
    st = short_term_signal(market["realised_temp"], n_tenors=market["market_mid"].shape[1])

    combined = pct_sig + 0.5 * st
    pnl_per_unit = np.diff(market["market_mid"], axis=0, prepend=market["market_mid"][0:1])

    _put_npz(
        blob_paths.signals(run_id),
        percentile_signal=pct_sig,
        asymmetry=asym,
        spread=spread,
        short_term=st,
        combined_signal=combined,
        pnl_per_unit=pnl_per_unit,
    )
    return run_id


@task(memory="2g", cpu=1)
def detect_regimes_and_launch(
    run_id: str,
    n_regimes: int = 4,
    grid_signal_steps: int = 11,
    grid_pivot_steps: int = 5,
    bootstrap_samples: int = 64,
) -> dict:
    """Stage 3 — cluster the historical signal/asymmetry/spread into regimes
    and dynamically launch a child workflow with one branch per
    (regime × tenor × threshold cell).

    This is the moment the DAG shape is decided by data, not by code.
    """
    from sklearn.cluster import KMeans

    market = _get_npz(blob_paths.market_data(run_id))
    sigs = _get_npz(blob_paths.signals(run_id))

    asym = sigs["asymmetry"]
    spread = sigs["spread"]
    combined = sigs["combined_signal"]
    n_days, n_tenors = combined.shape

    feat_asym = np.log(np.clip(asym.mean(axis=1), 1e-3, None))
    feat_spread = (spread.mean(axis=1) - spread.mean()) / (spread.std() + 1e-9)
    feat_sig = np.tanh(combined.mean(axis=1))
    X = np.stack([feat_asym, feat_spread, feat_sig], axis=1)

    km = KMeans(n_clusters=n_regimes, n_init=8, random_state=0).fit(X)
    labels = km.labels_.astype(int)

    regimes: list[dict] = []
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
    regimes.sort(key=lambda r: -r["size"])

    expected_cells = (
        len(regimes) * int(n_tenors) * grid_signal_steps * grid_pivot_steps
    )
    _put_json(
        blob_paths.regimes(run_id),
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

    child_skip = os.environ.get("DATATAILR_BATCH_DONT_RUN_WORKFLOW", "").lower() == "true"
    launched = False
    if not child_skip:
        from gas_curve_backtest.workflows.regime_workflow import build_regime_workflow

        child = build_regime_workflow(
            run_id=run_id,
            regimes=regimes,
            n_tenors=int(n_tenors),
            grid_signal_steps=grid_signal_steps,
            grid_pivot_steps=grid_pivot_steps,
            bootstrap_samples=bootstrap_samples,
        )
        try:
            child()
            launched = True
        except Exception as e:
            logger.warning(f"child workflow launch failed: {e}")

    return {
        "regime_count": len(regimes),
        "tenors": int(n_tenors),
        "grid_size": grid_signal_steps * grid_pivot_steps,
        "expected_cells": int(expected_cells),
        "child_launched": launched,
    }


@task(memory="500m", cpu=0.5)
def run_backtest_cell(
    run_id: str,
    regime_id: int,
    tenor: int,
    sig_threshold: float,
    asym_pivot: float,
    sig_idx: int,
    pivot_idx: int,
    bootstrap_samples: int = 64,
) -> dict:
    """Run one (regime, tenor, threshold) backtest cell and persist its result.

    A point estimate (deterministic equity curve) plus a block-bootstrap
    distribution of Sharpe ratios so we can pick *robust* thresholds
    rather than thresholds that just happened to fit the in-sample path.
    """
    from gas_curve_backtest.backtest.core import (
        backtest_cell,
        bootstrap_summarise,
        warmup_jit,
    )
    from gas_curve_backtest.backtest.metrics import summarise_equity

    warmup_jit()
    sigs = _get_npz(blob_paths.signals(run_id))
    regimes_payload = _get_json(blob_paths.regimes(run_id))
    regime = next(r for r in regimes_payload["regimes"] if r["regime_id"] == regime_id)
    days = np.array(regime["day_indices"], dtype=int)

    signal = sigs["combined_signal"][days, tenor]
    asymmetry = sigs["asymmetry"][days, tenor]
    pnl_per_unit = sigs["pnl_per_unit"][days, tenor]

    equity = backtest_cell(signal, asymmetry, pnl_per_unit, sig_threshold, asym_pivot)
    metrics = summarise_equity(equity)
    boot = bootstrap_summarise(
        signal, asymmetry, pnl_per_unit,
        sig_threshold, asym_pivot,
        n_samples=bootstrap_samples,
    )
    metrics.update(
        {
            "regime_id": regime_id,
            "tenor": tenor,
            "sig_threshold": sig_threshold,
            "asym_pivot": asym_pivot,
            "sig_idx": sig_idx,
            "pivot_idx": pivot_idx,
            "n_days": int(days.size),
            "sharpe_boot_mean": boot["sharpe_mean"],
            "sharpe_boot_std": boot["sharpe_std"],
            "sharpe_boot_p05": boot["sharpe_p05"],
            "bootstrap_samples": boot["n_samples"],
        }
    )
    _put_json(
        blob_paths.cell_result(run_id, regime_id, tenor, sig_idx, pivot_idx),
        metrics,
    )
    return metrics


@task(memory="2g", cpu=1)
def aggregate_results(run_id: str, *_cells: Any) -> dict:
    """Sweep all cell result blobs, write a tidy parquet + a summary JSON.

    The `*_cells` varargs make this task wait for every fan-out cell
    before running. We then re-read the per-cell JSON blobs (rather
    than the in-memory varargs) so the same code path works whether
    the cells ran in this DAG or in a detached child run.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    blob = _blob()
    rows: list[dict] = []
    cell_root = blob_paths.cell_dir(run_id)

    keys: list[str] = []
    try:
        keys = [k for k in blob.ls(cell_root) if str(k).endswith(".json")]
    except Exception:
        keys = []
    for k in keys:
        try:
            rows.append(_get_json(k))
        except Exception:
            continue

    if not rows:
        _put_json(blob_paths.aggregated(run_id), {"cells": 0})
        return {"cells": 0}

    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    blob.put(blob_paths.heatmap(run_id), buf.getvalue())

    by_regime: dict[int, dict] = {}
    for row in rows:
        r = int(row["regime_id"])
        cur = by_regime.setdefault(r, {"best_sharpe": -1e9, "best": None})
        if row["sharpe"] > cur["best_sharpe"]:
            cur["best_sharpe"] = float(row["sharpe"])
            cur["best"] = row

    summary = {"cells": len(rows), "best_per_regime": list(by_regime.values())}
    _put_json(blob_paths.aggregated(run_id), summary)
    return summary
