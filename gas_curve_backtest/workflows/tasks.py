"""@task functions used by the parent and child backtest workflows.

Within a single workflow run, payloads flow between tasks via the
platform's auto-persisted return-value channel — we only write to Blob
storage when data has to cross a boundary the channel does not span:

  * the parent → dynamically-deployed child workflow boundary (the
    cells live in a separate DAG), and
  * the workflow → out-of-DAG dashboard boundary (Streamlit polls Blob
    by `run_id` because it has no task handle).

So `generate_market` returns its payload directly; `compute_signals`
consumes that payload and persists `signals.npz` once, since the child
cells and the Regime Drilldown page both read it back; regimes / cell
results / aggregated summary are persisted because the dashboard reads
them.
"""

from __future__ import annotations

import io
import json
import os
import time
from typing import Any

import numpy as np

from datatailr import task
from datatailr.logging import DatatailrLogger

from gas_curve_backtest.workflows import blob_paths

logger = DatatailrLogger(__name__).get_logger()


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TiB"


def _blob():
    from datatailr import Blob

    return Blob()


def _put_npz(key: str, **arrays) -> None:
    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    payload = buf.getvalue()
    _blob().put(key, payload)
    shapes = {k: tuple(v.shape) for k, v in arrays.items()}
    logger.info(
        f"blob put npz key={key} size={_fmt_bytes(len(payload))} arrays={shapes}"
    )


def _get_npz(key: str) -> dict[str, np.ndarray]:
    raw = _blob().get(key)
    with np.load(io.BytesIO(raw)) as data:
        out = {k: data[k] for k in data.files}
    shapes = {k: tuple(v.shape) for k, v in out.items()}
    logger.info(
        f"blob get npz key={key} size={_fmt_bytes(len(raw))} arrays={shapes}"
    )
    return out


def _put_json(key: str, payload: Any) -> None:
    blob_data = json.dumps(payload, default=str).encode("utf-8")
    _blob().put(key, blob_data)
    logger.info(f"blob put json key={key} size={_fmt_bytes(len(blob_data))}")


def _get_json(key: str) -> Any:
    raw = _blob().get(key)
    logger.info(f"blob get json key={key} size={_fmt_bytes(len(raw))}")
    return json.loads(raw.decode("utf-8"))


@task(memory="1g", cpu=1)
def generate_market(n_days: int = 750, n_tenors: int = 8, seed: int = 11) -> dict:
    """Stage 1 — synthesise forward curves, ECMWF ensembles and market mid.

    Returned dict is consumed directly by `compute_signals` via the
    platform's task-return channel; nothing is written to Blob here.
    """
    from gas_curve_backtest.market.curve_generator import CurveConfig, generate_history

    t0 = time.perf_counter()
    logger.info(
        f"[generate_market] start n_days={n_days} n_tenors={n_tenors} seed={seed}"
    )

    h = generate_history(CurveConfig(n_days=n_days, n_tenors=n_tenors, seed=seed))

    mid = h["market_mid"]
    ens = h["model_price_ensemble"]
    spread_pct = float(np.mean((h["ask"] - h["bid"]) / np.clip(mid, 1e-9, None))) * 100
    logger.info(
        f"[generate_market] generated market_mid shape={mid.shape} "
        f"price range=[{float(mid.min()):.3f}, {float(mid.max()):.3f}] "
        f"mean={float(mid.mean()):.3f} | "
        f"ensemble shape={ens.shape} | "
        f"avg bid-ask spread={spread_pct:.3f}%"
    )
    logger.info(
        f"[generate_market] done elapsed={time.perf_counter() - t0:.2f}s"
    )
    return {
        "model_price_ensemble": ens,
        "market_mid": mid,
        "bid": h["bid"],
        "ask": h["ask"],
        "realised_temp": h["realised_temp"],
        "spread_regime": h["spread_regime"],
    }


@task(memory="2g", cpu=1)
def compute_signals(run_id: str, market: dict) -> dict:
    """Stage 2 — derive percentile signal, asymmetry, short-term overlay.

    Receives the market payload directly from `generate_market`; persists
    `signals.npz` so the dynamically-deployed child workflow and the
    dashboard's Regime Drilldown page can fetch it by `run_id`.
    """
    from gas_curve_backtest.signals.asymmetry import asymmetry_and_spread
    from gas_curve_backtest.signals.percentile_signals import percentile_signal
    from gas_curve_backtest.signals.short_term import short_term_signal

    t0 = time.perf_counter()
    n_days, n_tenors = market["market_mid"].shape
    logger.info(
        f"[compute_signals] start run_id={run_id} n_days={n_days} n_tenors={n_tenors}"
    )

    t = time.perf_counter()
    pct_sig = percentile_signal(market["market_mid"], market["model_price_ensemble"])
    logger.info(
        f"[compute_signals] percentile_signal mean={float(pct_sig.mean()):+.3f} "
        f"std={float(pct_sig.std()):.3f} ({time.perf_counter() - t:.2f}s)"
    )

    t = time.perf_counter()
    asym, spread, p_mid = asymmetry_and_spread(market["model_price_ensemble"])
    logger.info(
        f"[compute_signals] asymmetry median={float(np.median(asym)):.3f} | "
        f"spread median={float(np.median(spread)):.4f} "
        f"({time.perf_counter() - t:.2f}s)"
    )

    t = time.perf_counter()
    st = short_term_signal(market["realised_temp"], n_tenors=n_tenors)
    logger.info(
        f"[compute_signals] short_term mean={float(st.mean()):+.3f} "
        f"std={float(st.std()):.3f} ({time.perf_counter() - t:.2f}s)"
    )

    combined = pct_sig + 0.5 * st
    pnl_per_unit = np.diff(market["market_mid"], axis=0, prepend=market["market_mid"][0:1])
    logger.info(
        f"[compute_signals] combined mean={float(combined.mean()):+.3f} "
        f"std={float(combined.std()):.3f} | "
        f"pnl_per_unit shape={pnl_per_unit.shape} "
        f"abs-mean={float(np.abs(pnl_per_unit).mean()):.4f}"
    )

    signals_payload = {
        "percentile_signal": pct_sig,
        "asymmetry": asym,
        "spread": spread,
        "short_term": st,
        "combined_signal": combined,
        "pnl_per_unit": pnl_per_unit,
    }
    _put_npz(blob_paths.signals(run_id), **signals_payload)
    _blob().put(blob_paths.latest_pointer(), run_id.encode("utf-8"))
    logger.info(
        f"[compute_signals] done run_id={run_id} "
        f"elapsed={time.perf_counter() - t0:.2f}s"
    )
    return signals_payload


@task(memory="2g", cpu=1)
def detect_regimes_and_launch(
    run_id: str,
    signals: dict,
    n_regimes: int = 4,
    grid_signal_steps: int = 3,
    grid_pivot_steps: int = 2,
    bootstrap_samples: int = 64,
) -> dict:
    """Stage 3 — cluster the historical signal/asymmetry/spread into regimes
    and dynamically launch a child workflow with one branch per
    (regime × tenor × threshold cell).

    Receives the in-memory signals payload from `compute_signals`; only
    the regimes summary is persisted here, since the cells will pull
    `signals.npz` back from Blob (written upstream by `compute_signals`).

    This is the moment the DAG shape is decided by data, not by code.
    """
    from sklearn.cluster import KMeans

    t0 = time.perf_counter()
    asym = signals["asymmetry"]
    spread = signals["spread"]
    combined = signals["combined_signal"]
    n_days, n_tenors = combined.shape
    logger.info(
        f"[detect_regimes] start run_id={run_id} n_regimes={n_regimes} "
        f"grid={grid_signal_steps}x{grid_pivot_steps} "
        f"bootstrap_samples={bootstrap_samples} "
        f"feature inputs n_days={n_days} n_tenors={n_tenors}"
    )

    feat_asym = np.log(np.clip(asym.mean(axis=1), 1e-3, None))
    feat_spread = (spread.mean(axis=1) - spread.mean()) / (spread.std() + 1e-9)
    feat_sig = np.tanh(combined.mean(axis=1))
    X = np.stack([feat_asym, feat_spread, feat_sig], axis=1)
    logger.info(
        f"[detect_regimes] feature matrix shape={X.shape} "
        f"asym[min={float(feat_asym.min()):+.2f} max={float(feat_asym.max()):+.2f}] "
        f"spread[min={float(feat_spread.min()):+.2f} max={float(feat_spread.max()):+.2f}]"
    )

    t = time.perf_counter()
    km = KMeans(n_clusters=n_regimes, n_init=8, random_state=0).fit(X)
    labels = km.labels_.astype(int)
    raw_counts = {int(r): int((labels == r).sum()) for r in range(n_regimes)}
    logger.info(
        f"[detect_regimes] KMeans converged inertia={float(km.inertia_):.2f} "
        f"raw_counts={raw_counts} ({time.perf_counter() - t:.2f}s)"
    )

    regimes: list[dict] = []
    skipped: list[int] = []
    for r in range(n_regimes):
        mask = labels == r
        if mask.sum() < 5:
            skipped.append(int(r))
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

    if skipped:
        logger.warning(
            f"[detect_regimes] dropped {len(skipped)} regime(s) with <5 days: {skipped}"
        )
    for reg in regimes:
        logger.info(
            f"[detect_regimes] regime_id={reg['regime_id']} size={reg['size']} days "
            f"median_asym={reg['median_asymmetry']:.3f} "
            f"median_spread={reg['median_spread']:.4f} "
            f"median_signal={reg['median_signal']:+.3f}"
        )

    expected_cells = (
        len(regimes) * int(n_tenors) * grid_signal_steps * grid_pivot_steps
    )
    logger.info(
        f"[detect_regimes] expected_cells={expected_cells} "
        f"= {len(regimes)} regimes x {n_tenors} tenors x "
        f"{grid_signal_steps} sig x {grid_pivot_steps} pivot"
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
    if child_skip:
        logger.info(
            "[detect_regimes] DATATAILR_BATCH_DONT_RUN_WORKFLOW=true; "
            "skipping child workflow launch"
        )
    else:
        from gas_curve_backtest.workflows.regime_workflow import build_regime_workflow

        logger.info(
            f"[detect_regimes] building child regime_sweep workflow "
            f"({expected_cells} cells)"
        )
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
            logger.info("[detect_regimes] child workflow launched successfully")
        except Exception as e:
            logger.warning(f"[detect_regimes] child workflow launch failed: {e}")

    logger.info(
        f"[detect_regimes] done run_id={run_id} "
        f"regimes={len(regimes)} expected_cells={expected_cells} "
        f"child_launched={launched} elapsed={time.perf_counter() - t0:.2f}s"
    )
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

    cell_tag = f"r{regime_id}_t{tenor}_s{sig_idx}_p{pivot_idx}"
    t0 = time.perf_counter()
    logger.info(
        f"[run_backtest_cell] start {cell_tag} run_id={run_id} "
        f"sig_threshold={sig_threshold:.4f} asym_pivot={asym_pivot:.4f} "
        f"bootstrap_samples={bootstrap_samples}"
    )

    t = time.perf_counter()
    warmup_jit()
    logger.info(
        f"[run_backtest_cell] {cell_tag} JIT warmup {time.perf_counter() - t:.2f}s"
    )

    sigs = _get_npz(blob_paths.signals(run_id))
    regimes_payload = _get_json(blob_paths.regimes(run_id))
    regime = next(r for r in regimes_payload["regimes"] if r["regime_id"] == regime_id)
    days = np.array(regime["day_indices"], dtype=int)

    signal = sigs["combined_signal"][days, tenor]
    asymmetry = sigs["asymmetry"][days, tenor]
    pnl_per_unit = sigs["pnl_per_unit"][days, tenor]
    logger.info(
        f"[run_backtest_cell] {cell_tag} sliced n_days={days.size} "
        f"signal_mean={float(signal.mean()):+.3f} "
        f"asym_mean={float(asymmetry.mean()):.3f} "
        f"pnl_abs_mean={float(np.abs(pnl_per_unit).mean()):.4f}"
    )

    t = time.perf_counter()
    equity = backtest_cell(signal, asymmetry, pnl_per_unit, sig_threshold, asym_pivot)
    metrics = summarise_equity(equity)
    logger.info(
        f"[run_backtest_cell] {cell_tag} point estimate "
        f"sharpe={metrics.get('sharpe', float('nan')):+.3f} "
        f"final_equity={float(equity[-1]):+.4f} "
        f"({time.perf_counter() - t:.2f}s)"
    )

    t = time.perf_counter()
    boot = bootstrap_summarise(
        signal, asymmetry, pnl_per_unit,
        sig_threshold, asym_pivot,
        n_samples=bootstrap_samples,
    )
    logger.info(
        f"[run_backtest_cell] {cell_tag} bootstrap n={boot['n_samples']} "
        f"sharpe_mean={boot['sharpe_mean']:+.3f} "
        f"sharpe_std={boot['sharpe_std']:.3f} "
        f"sharpe_p05={boot['sharpe_p05']:+.3f} "
        f"({time.perf_counter() - t:.2f}s)"
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
    logger.info(
        f"[run_backtest_cell] done {cell_tag} "
        f"elapsed={time.perf_counter() - t0:.2f}s"
    )
    return metrics


@task(memory="2g", cpu=1)
def aggregate_results(*_cells: Any) -> dict:
    """Sweep all cell result blobs, write a tidy parquet + a summary JSON.

    The `*_cells` varargs make this task wait for every fan-out cell
    before running. We then re-read the per-cell JSON blobs (rather
    than the in-memory varargs) so the same code path works whether
    the cells ran in this DAG or in a detached child run.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    t0 = time.perf_counter()
    backtest_id = _cells[0]
    cells = _cells[1:]
    logger.info(
        f"[aggregate_results] start run_id={backtest_id} "
        f"upstream_cells_signalled={len(_cells)}"
    )

    blob = _blob()
    rows: list[dict] = []
    cell_root = blob_paths.cell_dir(backtest_id)

    keys: list[str] = []
    try:
        keys = [k for k in blob.ls(cell_root) if str(k).endswith(".json")]
        logger.info(
            f"[aggregate_results] listed {len(keys)} cell blobs under {cell_root}"
        )
    except Exception as e:
        logger.warning(
            f"[aggregate_results] blob.ls({cell_root}) failed: {e}; "
            "proceeding with empty key list"
        )
        keys = []

    failed = 0
    for k in keys:
        try:
            rows.append(_get_json(k))
        except Exception as e:
            failed += 1
            logger.warning(f"[aggregate_results] failed to read cell {k}: {e}")
            continue
    logger.info(
        f"[aggregate_results] read {len(rows)} rows ({failed} failed) "
        f"from {len(keys)} keys"
    )

    if not rows:
        logger.warning(
            f"[aggregate_results] no cells available for run_id={backtest_id}; "
            "writing empty summary"
        )
        _put_json(blob_paths.aggregated(backtest_id), {"cells": 0})
        logger.info(
            f"[aggregate_results] done (empty) run_id={backtest_id} "
            f"elapsed={time.perf_counter() - t0:.2f}s"
        )
        return {"cells": 0}

    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    blob.put(blob_paths.heatmap(backtest_id), buf.getvalue())
    logger.info(
        f"[aggregate_results] wrote heatmap parquet "
        f"rows={table.num_rows} cols={table.num_columns} "
        f"size={_fmt_bytes(buf.tell())} key={blob_paths.heatmap(backtest_id)}"
    )

    by_regime: dict[int, dict] = {}
    for row in rows:
        r = int(row["regime_id"])
        cur = by_regime.setdefault(r, {"best_sharpe": -1e9, "best": None})
        if row["sharpe"] > cur["best_sharpe"]:
            cur["best_sharpe"] = float(row["sharpe"])
            cur["best"] = row

    for r, cur in sorted(by_regime.items()):
        best = cur["best"] or {}
        logger.info(
            f"[aggregate_results] best for regime_id={r}: "
            f"sharpe={cur['best_sharpe']:+.3f} "
            f"tenor={best.get('tenor')} "
            f"sig_threshold={best.get('sig_threshold')} "
            f"asym_pivot={best.get('asym_pivot')}"
        )

    summary = {"cells": len(rows), "best_per_regime": list(by_regime.values())}
    _put_json(blob_paths.aggregated(backtest_id), summary)
    logger.info(
        f"[aggregate_results] done run_id={backtest_id} cells={len(rows)} "
        f"regimes={len(by_regime)} elapsed={time.perf_counter() - t0:.2f}s"
    )
    return summary
