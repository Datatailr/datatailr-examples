"""Artifact discovery helpers for evaluation and dashboard."""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd

from credit_spread_prediction.blob_io import blob_get, normalize_key
from credit_spread_prediction.config import EVAL_PREFIX, MODELS_PREFIX


def _entry_name(entry: str | dict[str, Any]) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name", "")).strip()
    return str(entry).strip()


def list_model_runs(blob: Any) -> list[str]:
    prefix = normalize_key(f"{MODELS_PREFIX}/runs")
    entries = blob.ls(prefix) or []
    runs: list[str] = []
    for entry in entries:
        name = _entry_name(entry)
        if "/runs/" in name:
            token = name.split("/runs/", 1)[1].split("/", 1)[0]
            if token:
                runs.append(token)
    return sorted(set(runs), reverse=True)


def load_leaderboard(blob: Any, run_id: str) -> pd.DataFrame:
    key = f"{MODELS_PREFIX}/runs/{run_id}/leaderboard.parquet"
    return pd.read_parquet(io.BytesIO(blob_get(blob, key)))


def load_eval_report(blob: Any, run_id: str) -> list[dict[str, Any]]:
    key = f"{EVAL_PREFIX}/runs/{run_id}/report.json"
    raw = blob_get(blob, key)
    return json.loads(raw.decode("utf-8"))

