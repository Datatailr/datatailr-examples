"""Deployment entrypoint for the credit spread prediction example."""

from __future__ import annotations

import pathlib

from datatailr import App, Resources

import credit_spread_prediction.dashboard.app as dashboard_entrypoint
from credit_spread_prediction.workflows.evaluation_workflow import credit_spread_evaluation_workflow
from credit_spread_prediction.workflows.ingest_workflow import credit_spread_ingestion_workflow
from credit_spread_prediction.workflows.training_workflow import credit_spread_training_workflow

CURRENT_DIR = pathlib.Path(__file__).parent
REQUIREMENTS_FILE = str(CURRENT_DIR / "requirements.txt")

dashboard_app = App(
    name="Credit Spread Examination Dashboard",
    entrypoint=dashboard_entrypoint,
    framework="streamlit",
    app_section="Credit Spread Prediction",
    resources=Resources(memory="2g", cpu=1),
    python_requirements=REQUIREMENTS_FILE,
)


def deploy_all(run_jobs: bool = False) -> None:
    dashboard_app.run()
    credit_spread_ingestion_workflow(save_only=not run_jobs)
    credit_spread_training_workflow(save_only=not run_jobs)


def deploy_dashboard_only() -> None:
    dashboard_app.run()


def deploy_workflows_only(run_jobs: bool = False) -> None:
    credit_spread_ingestion_workflow(save_only=not run_jobs)
    credit_spread_training_workflow(save_only=not run_jobs)


def refresh_evaluation(run_id: str) -> None:
    credit_spread_evaluation_workflow(run_id=run_id)


if __name__ == "__main__":
    deploy_all(run_jobs=False)

