"""Parent backtest workflow.

Stage 1: synthesise the market data.
Stage 2: compute signals.
Stage 3: detect regimes; this task **dynamically deploys a child
         workflow** that fans out a backtest cell per
         (regime, tenor, threshold).

Market and signals payloads flow between tasks via the platform's
auto-persisted task-return channel; only the artifacts that have to
cross the parent → child or workflow → dashboard boundaries are
written to Blob storage (see `tasks.py`).

The parent itself ends as soon as the child is launched; the child
contains the aggregator and writes the final heatmap to Blob storage.
The dashboard reads the heatmap by `run_id`.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from datatailr import Resources, workflow

from gas_curve_backtest.workflows.tasks import (
    compute_signals,
    detect_regimes_and_launch,
    generate_market,
)

_REQ = str(Path(__file__).parent.parent / "requirements.txt")


def make_run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


@workflow(
    name="Gas Curve Backtest — Parent",
    python_requirements=_REQ,
    resources=Resources(memory="1g", cpu=1),
)
def parent_backtest_workflow(
    run_id: str | None = None,
    n_days: int = 750,
    n_tenors: int = 8,
    n_regimes: int = 4,
    grid_signal_steps: int = 3,
    grid_pivot_steps: int = 2,
    bootstrap_samples: int = 64,
):
    rid = run_id or make_run_id()
    market = generate_market(n_days, n_tenors).alias("generate_market")
    signals = compute_signals(rid, market).alias("compute_signals")
    detect_regimes_and_launch(
        rid,
        signals,
        n_regimes,
        grid_signal_steps,
        grid_pivot_steps,
        bootstrap_samples,
    ).alias("detect_regimes_and_launch_child")


if __name__ == "__main__":
    rid = make_run_id()
    print(f"Deploying parent backtest workflow with run_id={rid}")
    parent_backtest_workflow(rid)
