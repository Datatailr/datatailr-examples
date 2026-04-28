"""Dynamic child workflow generator.

Built at runtime by `detect_regimes_and_launch` once the regimes are
known. The shape of this DAG is therefore data-dependent: nothing
about it could have been declared up-front.

For every (regime, tenor, signal-threshold, asymmetry-pivot) cell we
schedule one Numba-accelerated backtest task. After all cells finish a
single aggregator collects them into a Parquet heatmap.
"""

from __future__ import annotations

from pathlib import Path

from datatailr import Resources, workflow

from gas_curve_backtest.backtest.grid import default_grid, regime_aware_grid
from gas_curve_backtest.workflows.tasks import (
    aggregate_results,
    run_backtest_cell,
)

_REQ = str(Path(__file__).parent.parent / "requirements.txt")


def build_regime_workflow(
    run_id: str,
    regimes: list[dict],
    n_tenors: int,
    grid_signal_steps: int = 3,
    grid_pivot_steps: int = 2,
    bootstrap_samples: int = 64,
):
    """Return a fresh @workflow-decorated function ready to deploy.

    The decorator captures `regimes`/`n_tenors`/`run_id` in the closure
    so the deployed DAG embeds the runtime decisions made by the
    parent's regime-detection task.
    """
    base = default_grid(n_signal=grid_signal_steps, n_pivot=grid_pivot_steps)
    n_cells = len(regimes) * n_tenors * base.size

    @workflow(
        name=f"Regime Sweep — {run_id} ({n_cells} cells)",
        python_requirements=_REQ,
        resources=Resources(memory="500m", cpu=0.5),
        env_vars={"DATATAILR_RUN_ID": run_id},
    )
    def regime_sweep():
        cell_results = []
        for regime in regimes:
            grid = regime_aware_grid(regime, base)
            r_id = int(regime["regime_id"])
            for tenor in range(n_tenors):
                for sig_idx, sig_threshold in enumerate(grid.signal_thresholds):
                    for pivot_idx, asym_pivot in enumerate(grid.asym_pivots):
                        out = run_backtest_cell(
                            run_id,
                            r_id,
                            tenor,
                            float(sig_threshold),
                            float(asym_pivot),
                            sig_idx,
                            pivot_idx,
                            bootstrap_samples,
                        ).alias(f"r{r_id}_t{tenor}_s{sig_idx}_p{pivot_idx}")
                        cell_results.append(out)
        aggregate_results(*cell_results).alias("aggregate")

    return regime_sweep
