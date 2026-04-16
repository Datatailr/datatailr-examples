"""Datatailr tasks for calibration and model training."""

from __future__ import annotations

import io
import json
from hashlib import sha1
from typing import Any

import pandas as pd
from datatailr import task

from credit_spread_prediction.blob_io import blob_get, blob_put
from credit_spread_prediction.config import MODELS_PREFIX
from credit_spread_prediction.modeling.models import run_walk_forward_cv


def _load_feature_frame(blob: Any, feature_key: str) -> pd.DataFrame:
    raw = blob_get(blob, feature_key)
    return pd.read_parquet(io.BytesIO(raw))


def _job_id(job: dict[str, Any]) -> str:
    digest = sha1(json.dumps(job, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:12]


def _entry_name(entry: str | dict[str, Any]) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name", "")).strip()
    return str(entry).strip()


@task()
def write_calibration_manifest(run_id: str, config: dict[str, Any]) -> dict[str, str]:
    from datatailr import Blob

    key = f"{MODELS_PREFIX}/runs/{run_id}/calibration_config.json"
    blob_put(Blob(), key, json.dumps(config).encode("utf-8"))
    return {"run_id": run_id, "calibration_key": key}


@task(memory="2g", cpu=1)
def train_model_job(
    feature_key: str,
    job: dict[str, Any],
    run_id: str,
    cv_splits: int = 4,
) -> dict[str, str]:
    from datatailr import Blob

    blob = Blob()
    frame = _load_feature_frame(blob, feature_key)

    target = str(job["target"])
    horizon = int(job["horizon"])
    label_kind = str(job["label_kind"])
    family = str(job["model_family"])
    params = dict(job.get("params", {}))
    label_col = f"y_{label_kind}_{target}_h{horizon}"

    fold_results, y_true, y_pred = run_walk_forward_cv(
        frame=frame,
        label_col=label_col,
        model_family=family,
        params=params,
        cv_splits=cv_splits,
    )
    job_id = _job_id(job)
    base = f"{MODELS_PREFIX}/runs/{run_id}/job={job_id}"

    payload = {
        "run_id": run_id,
        "job_id": job_id,
        "job": job,
        "cv_splits": cv_splits,
        "metrics": [r.__dict__ for r in fold_results],
        "mae_mean": float(sum(r.mae for r in fold_results) / len(fold_results)),
        "rmse_mean": float(sum(r.rmse for r in fold_results) / len(fold_results)),
        "directional_accuracy_mean": float(
            sum(r.directional_accuracy for r in fold_results) / len(fold_results)
        ),
        "feature_key": feature_key,
    }
    blob_put(blob, f"{base}/summary.json", json.dumps(payload).encode("utf-8"))

    preds = pd.DataFrame({"y_true": y_true, "y_pred": y_pred})
    pred_bytes = io.BytesIO()
    preds.to_parquet(pred_bytes, index=False)
    blob_put(blob, f"{base}/predictions.parquet", pred_bytes.getvalue())

    return {
        "run_id": run_id,
        "job_id": job_id,
        "summary_key": f"{base}/summary.json",
        "predictions_key": f"{base}/predictions.parquet",
    }


@task()
def aggregate_training_results(job_results: list[dict[str, str]]) -> dict[str, str]:
    from datatailr import Blob

    if not job_results:
        raise ValueError("No job results were provided to aggregation.")

    blob = Blob()
    summaries: list[dict[str, Any]] = []
    for item in job_results:
        raw = blob_get(blob, item["summary_key"])
        summaries.append(json.loads(raw.decode("utf-8")))

    board = pd.DataFrame(
        [
            {
                "job_id": s["job_id"],
                "target": s["job"]["target"],
                "horizon": s["job"]["horizon"],
                "label_kind": s["job"]["label_kind"],
                "model_family": s["job"]["model_family"],
                "mae_mean": s["mae_mean"],
                "rmse_mean": s["rmse_mean"],
                "directional_accuracy_mean": s["directional_accuracy_mean"],
            }
            for s in summaries
        ]
    )
    leader = board.sort_values(["target", "horizon", "mae_mean"]).groupby(
        ["target", "horizon"], as_index=False
    ).head(1)

    run_id = summaries[0]["run_id"]
    leaderboard_key = f"{MODELS_PREFIX}/runs/{run_id}/leaderboard.parquet"
    table_bytes = io.BytesIO()
    board.to_parquet(table_bytes, index=False)
    blob_put(blob, leaderboard_key, table_bytes.getvalue())

    best_key = f"{MODELS_PREFIX}/runs/{run_id}/best_models.json"
    blob_put(blob, best_key, leader.to_json(orient="records").encode("utf-8"))
    return {"run_id": run_id, "leaderboard_key": leaderboard_key, "best_models_key": best_key}


@task()
def discover_job_results(run_id: str) -> list[dict[str, str]]:
    from datatailr import Blob

    blob = Blob()
    base = f"{MODELS_PREFIX}/runs/{run_id}"
    entries = blob.ls(base) or []
    jobs: dict[str, dict[str, str]] = {}
    for entry in entries:
        name = _entry_name(entry)
        if "/job=" not in name:
            continue
        rel = name.split("/job=", 1)[1]
        job_id = rel.split("/", 1)[0]
        if not job_id:
            continue
        ref = jobs.setdefault(job_id, {"run_id": run_id, "job_id": job_id})
        if name.endswith("/summary.json"):
            ref["summary_key"] = name if name.startswith("/") else f"/{name}"
        if name.endswith("/predictions.parquet"):
            ref["predictions_key"] = name if name.startswith("/") else f"/{name}"

    return [
        v
        for v in jobs.values()
        if "summary_key" in v and "predictions_key" in v
    ]

