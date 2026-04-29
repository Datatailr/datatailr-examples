"""Centralised blob layout for the backtest run.

A single `run_id` namespaces every artifact a workflow run produces, so
the dashboard can read everything by just knowing the run_id.
"""

from __future__ import annotations

ROOT = "gas_curve_backtest"


def run_root(run_id: str) -> str:
    return f"{ROOT}/runs/{run_id}"


def signals(run_id: str) -> str:
    return f"{run_root(run_id)}/signals.npz"


def regimes(run_id: str) -> str:
    return f"{run_root(run_id)}/regimes.json"


def cell_result(run_id: str, regime_id: int, tenor: int, sig_idx: int, pivot_idx: int) -> str:
    return (
        f"{run_root(run_id)}/cells/regime={regime_id}/tenor={tenor}"
        f"/sig={sig_idx}/pivot={pivot_idx}.json"
    )


def cell_dir(run_id: str) -> str:
    return f"{run_root(run_id)}/cells/"


def aggregated(run_id: str) -> str:
    return f"{run_root(run_id)}/aggregated.json"


def heatmap(run_id: str) -> str:
    return f"{run_root(run_id)}/heatmap.parquet"


def latest_pointer() -> str:
    return f"{ROOT}/latest_run.txt"
