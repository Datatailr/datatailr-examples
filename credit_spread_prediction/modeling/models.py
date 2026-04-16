"""Model family implementations and cross-validation runner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


@dataclass
class CVResult:
    fold: int
    mae: float
    rmse: float
    directional_accuracy: float


def build_model(model_family: str, params: dict[str, Any]) -> Any:
    if model_family == "elasticnet":
        return Pipeline(
            steps=[
                ("scaler", StandardScaler(with_mean=True, with_std=True)),
                ("model", ElasticNet(**params, random_state=42)),
            ]
        )
    if model_family == "gbr":
        return GradientBoostingRegressor(**params, random_state=42)
    raise ValueError(f"Unknown model family: {model_family}")


def select_xy(frame: pd.DataFrame, label_col: str) -> tuple[pd.DataFrame, pd.Series]:
    cols = [c for c in frame.columns if not c.startswith("y_") and c != "date"]
    subset = frame.loc[:, cols + [label_col]].dropna()
    x = subset[cols]
    y = subset[label_col]
    return x, y


def run_walk_forward_cv(
    frame: pd.DataFrame,
    label_col: str,
    model_family: str,
    params: dict[str, Any],
    cv_splits: int = 4,
) -> tuple[list[CVResult], np.ndarray, np.ndarray]:
    x, y = select_xy(frame, label_col)
    tscv = TimeSeriesSplit(n_splits=cv_splits)
    out: list[CVResult] = []
    y_true_all: list[np.ndarray] = []
    y_pred_all: list[np.ndarray] = []

    for fold_idx, (train_idx, test_idx) in enumerate(tscv.split(x), start=1):
        x_train, x_test = x.iloc[train_idx], x.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        model = build_model(model_family, params)
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)

        mae = float(mean_absolute_error(y_test, y_pred))
        rmse = float(mean_squared_error(y_test, y_pred) ** 0.5)
        if len(y_test) > 1:
            # Directional sign agreement on one-step differences.
            true_dir = np.sign(np.diff(y_test.values))
            pred_dir = np.sign(np.diff(y_pred))
            dir_acc = float(np.mean(true_dir == pred_dir)) if len(true_dir) else 0.0
        else:
            dir_acc = 0.0

        out.append(CVResult(fold=fold_idx, mae=mae, rmse=rmse, directional_accuracy=dir_acc))
        y_true_all.append(y_test.values)
        y_pred_all.append(np.asarray(y_pred))

    return out, np.concatenate(y_true_all), np.concatenate(y_pred_all)

