"""Project-wide constants and defaults."""

from __future__ import annotations

import os

RAW_PREFIX = os.environ.get("CREDIT_SPREAD_RAW_PREFIX", "credit_spread_prediction/raw")
FEATURES_PREFIX = os.environ.get("CREDIT_SPREAD_FEATURES_PREFIX", "credit_spread_prediction/features")
MODELS_PREFIX = os.environ.get("CREDIT_SPREAD_MODELS_PREFIX", "credit_spread_prediction/models")
EVAL_PREFIX = os.environ.get("CREDIT_SPREAD_EVAL_PREFIX", "credit_spread_prediction/evaluation")
CALIBRATION_CONFIG_KEY = os.environ.get(
    "CREDIT_SPREAD_CALIBRATION_CONFIG_KEY",
    f"{MODELS_PREFIX}/calibration/calibration_config.json",
)

# FRED API key is optional for many endpoints but improves reliability.
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE_URL = os.environ.get("FRED_BASE_URL", "https://api.stlouisfed.org/fred")

TARGET_SERIES = (
    "BAMLC0A0CM",
    "BAMLC0A4CBBB",
    "BAMLH0A0HYM2",
    "BAA10Y",
)

DRIVER_SERIES = (
    "DGS10",
    "DGS2",
    "T10Y2Y",
    "VIXCLS",
    "UNRATE",
    "CPIAUCSL",
    "FEDFUNDS",
)

ALL_SERIES = TARGET_SERIES + DRIVER_SERIES
HORIZONS = (1, 5, 20)

