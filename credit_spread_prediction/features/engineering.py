"""Feature and label engineering routines."""

from __future__ import annotations

import pandas as pd

from credit_spread_prediction.config import HORIZONS, TARGET_SERIES


def build_feature_matrix(series_frame: pd.DataFrame) -> pd.DataFrame:
    frame = series_frame.copy()
    frame = frame.sort_values("date")

    value_cols = [c for c in frame.columns if c != "date"]
    for col in value_cols:
        frame[f"{col}_lag1"] = frame[col].shift(1)
        frame[f"{col}_lag5"] = frame[col].shift(5)
        frame[f"{col}_chg1"] = frame[col].diff(1)
        frame[f"{col}_rolling_mean20"] = frame[col].rolling(20).mean()
        frame[f"{col}_rolling_std20"] = frame[col].rolling(20).std()

    for target in TARGET_SERIES:
        if target not in frame.columns:
            continue
        for horizon in HORIZONS:
            frame[f"y_level_{target}_h{horizon}"] = frame[target].shift(-horizon)
            frame[f"y_delta_{target}_h{horizon}"] = frame[target].shift(-horizon) - frame[target]

    return frame

