"""Datatailr tasks for feature dataset construction."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from datatailr import task

from credit_spread_prediction.blob_io import blob_get, blob_put, normalize_key
from credit_spread_prediction.config import ALL_SERIES, FEATURES_PREFIX, RAW_PREFIX
from credit_spread_prediction.features.engineering import build_feature_matrix


def _entry_name(entry: str | dict[str, Any]) -> str:
    if isinstance(entry, dict):
        return str(entry.get("name", "")).strip()
    return str(entry).strip()


def _extract_dt_from_path(path: str) -> str | None:
    if "dt=" not in path:
        return None
    token = path.split("dt=", 1)[1].split("/", 1)[0].strip()
    return token or None


def _list_entry_names(blob: Any, prefix: str) -> list[str]:
    out = blob.ls(normalize_key(prefix))
    names: list[str] = []
    for entry in out or []:
        name = _entry_name(entry)
        if not name:
            continue
        names.append(name if name.startswith("/") else f"/{name}")
    return names


def _latest_snapshot_from_runs(blob: Any) -> str | None:
    run_entries = _list_entry_names(blob, f"{RAW_PREFIX}/runs")
    if not run_entries:
        return None
    run_files = sorted([p for p in run_entries if p.endswith(".json")], reverse=True)
    for run_file in run_files:
        try:
            raw = blob_get(blob, run_file)
            payload = json.loads(raw.decode("utf-8"))
            snapshot_dt = str(payload.get("snapshot_dt", "")).strip()
            if snapshot_dt:
                return snapshot_dt
        except Exception:
            continue
    return None


def _latest_snapshot_dt(blob: Any) -> str:
    from_runs = _latest_snapshot_from_runs(blob)
    if from_runs:
        return from_runs

    dts: set[str] = set()
    for candidate_prefix in (
        f"{RAW_PREFIX}/dataset=fred_clean",
        RAW_PREFIX,
    ):
        for name in _list_entry_names(blob, candidate_prefix):
            dt = _extract_dt_from_path(name)
            if dt:
                dts.add(dt)

    if not dts:
        raise RuntimeError(
            "No ingested clean FRED data found. Run `credit_spread_ingestion_workflow` "
            "or execute training with ingestion first, then retry feature build."
        )
    return sorted(dts)[-1]


def _load_series_frame(blob: Any, snapshot_dt: str) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for series_id in ALL_SERIES:
        key = f"{RAW_PREFIX}/dataset=fred_clean/dt={snapshot_dt}/series_id={series_id}/observations.parquet"
        raw = blob_get(blob, key)
        part = pd.read_parquet(io.BytesIO(raw))
        part["date"] = pd.to_datetime(part["date"], errors="coerce")
        merged = part if merged is None else merged.merge(part, on="date", how="outer")
    if merged is None:
        raise RuntimeError("Unable to build a merged time-series frame.")
    merged = merged.sort_values("date").drop_duplicates(subset=["date"])
    return merged


@task()
def build_features_from_latest_ingestion() -> str:
    from datatailr import Blob

    blob = Blob()
    snapshot_dt = _latest_snapshot_dt(blob)
    merged = _load_series_frame(blob, snapshot_dt)
    features = build_feature_matrix(merged)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{FEATURES_PREFIX}/dataset=feature_matrix/dt={snapshot_dt}/run={run_ts}/feature_matrix.parquet"
    data = io.BytesIO()
    features.to_parquet(data, index=False)
    blob_put(blob, key, data.getvalue())

    meta = {
        "snapshot_dt": snapshot_dt,
        "rows": int(features.shape[0]),
        "cols": int(features.shape[1]),
        "feature_key": key,
    }
    blob_put(
        blob,
        f"{FEATURES_PREFIX}/dataset=feature_matrix/dt={snapshot_dt}/run={run_ts}/metadata.json",
        json.dumps(meta).encode("utf-8"),
    )
    return key

