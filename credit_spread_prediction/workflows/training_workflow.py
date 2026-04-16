"""Parallel model calibration and training workflow."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from datatailr import Resources, workflow

from credit_spread_prediction.config import ALL_SERIES
from credit_spread_prediction.data_ingestion.tasks import collect_ingestion_summary, fetch_fred_series
from credit_spread_prediction.evaluation.tasks import evaluate_run
from credit_spread_prediction.features.tasks import build_features_from_latest_ingestion
from credit_spread_prediction.modeling.calibration import load_local_calibration_config
from credit_spread_prediction.modeling.tasks import (
    aggregate_training_results,
    train_model_job,
    write_calibration_manifest,
)

DEFAULT_LOCAL_CALIBRATION = Path(__file__).resolve().parents[1] / "notebooks" / "calibration_config.json"


@workflow(
    name="Credit Spread Parallel Calibration",
    python_requirements=[
        "pandas",
        "pyarrow",
        "numpy",
        "scikit-learn",
    ],
    resources=Resources(memory="2g", cpu=1),
)
def credit_spread_training_workflow(
    calibration_config_path: str = str(DEFAULT_LOCAL_CALIBRATION),
    max_jobs: int = 24,
    bootstrap_ingestion: bool = True,
    observation_start: str = "1990-01-01",
):
    config = load_local_calibration_config(calibration_config_path)
    jobs = list(config.get("jobs", []))[:max_jobs]
    cv_splits = int(config.get("cv_splits", 4))
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if bootstrap_ingestion:
        fetches = []
        for series_id in ALL_SERIES:
            fetches.append(
                fetch_fred_series(
                    series_id=series_id,
                    observation_start=observation_start,
                ).alias(f"bootstrap_{series_id.lower()}")
            )
        collect_ingestion_summary(fetches).alias("bootstrap_ingestion_summary")

    feature_key = build_features_from_latest_ingestion().alias("build_feature_matrix")

    write_calibration_manifest(run_id=run_id, config=config).alias("save_calibration_manifest")

    train_jobs = []
    for idx, job in enumerate(jobs):
        train_jobs.append(
            train_model_job(
                feature_key=feature_key,
                job=job,
                run_id=run_id,
                cv_splits=cv_splits,
            ).alias(f"train_{idx}_{job['model_family']}_{job['label_kind']}")
        )
    aggregate_training_results(train_jobs).alias("aggregate_results")
    evaluate_run(run_id=run_id, job_results=train_jobs).alias("evaluate_run")

