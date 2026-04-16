"""Standalone evaluation workflow."""

from __future__ import annotations

from datatailr import workflow

from credit_spread_prediction.evaluation.tasks import evaluate_run
from credit_spread_prediction.modeling.tasks import discover_job_results


@workflow(
    name="Credit Spread Evaluation Refresh",
    python_requirements=["pandas", "pyarrow", "numpy"],
)
def credit_spread_evaluation_workflow(run_id: str):
    jobs = discover_job_results(run_id).alias("discover_jobs")
    evaluate_run(run_id=run_id, job_results=jobs).alias("evaluate_predictions")

