"""Calibration config loading and defaults."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from credit_spread_prediction.blob_io import blob_exists, blob_get
from credit_spread_prediction.config import CALIBRATION_CONFIG_KEY, HORIZONS, TARGET_SERIES


def default_calibration_config() -> dict[str, Any]:
    jobs: list[dict[str, Any]] = []
    for target in TARGET_SERIES:
        for horizon in HORIZONS:
            jobs.extend(
                [
                    {
                        "target": target,
                        "horizon": horizon,
                        "label_kind": "level",
                        "model_family": "elasticnet",
                        "params": {"alpha": 0.1, "l1_ratio": 0.5},
                    },
                    {
                        "target": target,
                        "horizon": horizon,
                        "label_kind": "delta",
                        "model_family": "elasticnet",
                        "params": {"alpha": 0.1, "l1_ratio": 0.5},
                    },
                    {
                        "target": target,
                        "horizon": horizon,
                        "label_kind": "delta",
                        "model_family": "gbr",
                        "params": {"n_estimators": 200, "learning_rate": 0.05, "max_depth": 2},
                    },
                ]
            )
    return {"jobs": jobs, "cv_splits": 4}


def load_calibration_config(blob: Any) -> dict[str, Any]:
    if blob_exists(blob, CALIBRATION_CONFIG_KEY):
        raw = blob_get(blob, CALIBRATION_CONFIG_KEY)
        return json.loads(raw.decode("utf-8"))
    return default_calibration_config()


def load_local_calibration_config(path: str) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return default_calibration_config()
    return json.loads(file_path.read_text(encoding="utf-8"))

