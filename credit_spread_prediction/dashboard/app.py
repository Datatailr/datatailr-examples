"""Streamlit dashboard for model evaluation and inspection."""

from __future__ import annotations

import io
import json

import pandas as pd
import streamlit as st

from credit_spread_prediction.blob_io import blob_get
from credit_spread_prediction.evaluation.registry import list_model_runs, load_eval_report, load_leaderboard


def _load_predictions(blob, predictions_key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(blob_get(blob, predictions_key)))


def main() -> None:
    from datatailr import Blob

    st.set_page_config(page_title="Credit Spread Model Examination", layout="wide")
    st.title("Credit Spread Prediction Dashboard")
    st.caption("Evaluate spread-level and spread-change forecasts across 1d/5d/20d horizons.")

    blob = Blob()
    runs = list_model_runs(blob)
    if not runs:
        st.warning("No model runs found yet. Execute the training workflow first.")
        return

    selected_run = st.selectbox("Model run", runs, index=0)
    leaderboard = load_leaderboard(blob, selected_run)
    report = pd.DataFrame(load_eval_report(blob, selected_run))

    st.subheader("Leaderboard")
    st.dataframe(leaderboard, use_container_width=True)

    st.subheader("Evaluation Metrics")
    st.dataframe(
        report[
            [
                "target",
                "horizon",
                "label_kind",
                "model_family",
                "mae",
                "rmse",
                "mape",
                "directional_accuracy",
                "high_vol_mae",
                "low_vol_mae",
            ]
        ],
        use_container_width=True,
    )

    st.subheader("Prediction Examination")
    labels = [
        f"{r['target']} | h{r['horizon']} | {r['label_kind']} | {r['model_family']}"
        for _, r in report.iterrows()
    ]
    picked_label = st.selectbox("Series/model", labels, index=0)
    picked_row = report.iloc[labels.index(picked_label)]
    preds = _load_predictions(blob, picked_row["predictions_key"])
    preds = preds.reset_index(drop=True)
    st.line_chart(preds.rename(columns={"y_true": "actual", "y_pred": "predicted"}), use_container_width=True)

    st.subheader("Configuration Snapshot")
    try:
        raw_cfg = blob_get(blob, f"/credit_spread_prediction/models/runs/{selected_run}/calibration_config.json")
        st.json(json.loads(raw_cfg.decode("utf-8")))
    except Exception:
        st.info("Calibration config artifact not found for this run.")


if __name__ == "__main__":
    main()

