"""Datatailr tasks for evaluation report generation."""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd
from datatailr import task

from credit_spread_prediction.blob_io import blob_get, blob_put
from credit_spread_prediction.config import EVAL_PREFIX
from credit_spread_prediction.evaluation.metrics import summarize_predictions


@task()
def evaluate_run(run_id: str, job_results: list[dict[str, str]]) -> dict[str, str]:
    from datatailr import Blob

    blob = Blob()
    report: list[dict[str, Any]] = []
    for result in job_results:
        summary = json.loads(blob_get(blob, result["summary_key"]).decode("utf-8"))
        preds = pd.read_parquet(io.BytesIO(blob_get(blob, result["predictions_key"])))
        metrics = summarize_predictions(preds)

        abs_err = (preds["y_true"] - preds["y_pred"]).abs()
        high_vol_mask = preds["y_true"].diff().abs().fillna(0) > preds["y_true"].diff().abs().median()
        low_vol_mask = ~high_vol_mask
        high_vol_mae = float(abs_err[high_vol_mask].mean()) if high_vol_mask.any() else metrics["mae"]
        low_vol_mae = float(abs_err[low_vol_mask].mean()) if low_vol_mask.any() else metrics["mae"]

        report.append(
            {
                "run_id": run_id,
                "job_id": summary["job_id"],
                "target": summary["job"]["target"],
                "horizon": summary["job"]["horizon"],
                "label_kind": summary["job"]["label_kind"],
                "model_family": summary["job"]["model_family"],
                **metrics,
                "high_vol_mae": high_vol_mae,
                "low_vol_mae": low_vol_mae,
                "summary_key": result["summary_key"],
                "predictions_key": result["predictions_key"],
            }
        )

    report_key = f"{EVAL_PREFIX}/runs/{run_id}/report.json"
    blob_put(blob, report_key, json.dumps(report).encode("utf-8"))

    table_key = f"{EVAL_PREFIX}/runs/{run_id}/report.parquet"
    table_bytes = io.BytesIO()
    pd.DataFrame(report).to_parquet(table_bytes, index=False)
    blob_put(blob, table_key, table_bytes.getvalue())
    return {"run_id": run_id, "report_key": report_key, "report_table_key": table_key}

