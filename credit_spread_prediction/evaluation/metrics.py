"""Evaluation metrics for prediction quality."""

from __future__ import annotations

import numpy as np
import pandas as pd


def summarize_predictions(predictions: pd.DataFrame) -> dict[str, float]:
    y_true = predictions["y_true"].astype(float).to_numpy()
    y_pred = predictions["y_pred"].astype(float).to_numpy()
    err = y_true - y_pred
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err ** 2)))
    mape = float(np.mean(np.abs(err / np.where(np.abs(y_true) < 1e-12, np.nan, y_true))) * 100.0)
    if len(y_true) > 1:
        dir_true = np.sign(np.diff(y_true))
        dir_pred = np.sign(np.diff(y_pred))
        directional = float(np.mean(dir_true == dir_pred))
    else:
        directional = 0.0
    return {"mae": mae, "rmse": rmse, "mape": mape, "directional_accuracy": directional}

