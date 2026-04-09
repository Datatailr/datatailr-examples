from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import pandas as pd
import pyarrow.parquet as pq

DEFAULT_BLOB_PREFIX = os.environ.get("ENERGY_BLOB_PREFIX", "smart_building_energy").strip("/")


def _blob():
    from datatailr import Blob

    return Blob()


def _entry_name(entry: Any) -> str:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return str(entry.get("name", ""))
    return str(entry)


def _strip_blob_prefix(path: str) -> str:
    p = path.strip()
    if p.startswith("blob://"):
        p = p[len("blob://") :]
    return p.lstrip("/")


def _join_blob_path(parent: str, child: str) -> str:
    parent = _strip_blob_prefix(parent).rstrip("/")
    child = _strip_blob_prefix(child)
    if not child:
        return parent
    if child.startswith(parent + "/") or child == parent:
        return child

    parent_parts = [p for p in parent.split("/") if p]
    child_parts = [p for p in child.split("/") if p]
    max_overlap = min(len(parent_parts), len(child_parts))
    for k in range(max_overlap, 0, -1):
        if parent_parts[-k:] == child_parts[:k]:
            merged = parent_parts + child_parts[k:]
            return "/".join(merged)

    return f"{parent}/{child}"


def _last_segment(path: str) -> str:
    return _strip_blob_prefix(path).rstrip("/").split("/")[-1]


def _list_children(prefix: str) -> list[str]:
    base = _strip_blob_prefix(prefix).rstrip("/") + "/"
    children: list[str] = []
    for item in _blob().ls(base):
        name = _entry_name(item)
        if not name:
            continue
        children.append(_join_blob_path(base, name))
    return children


def _list_dataset_files(blob_prefix: str, dataset: str) -> list[str]:
    dataset_root = f"{_strip_blob_prefix(blob_prefix)}/{dataset}"
    files: list[str] = []
    for dt_path in _list_children(dataset_root):
        if not _last_segment(dt_path).startswith("dt="):
            continue
        for hour_path in _list_children(dt_path):
            if not _last_segment(hour_path).startswith("hour="):
                continue
            for building_path in _list_children(hour_path):
                if not _last_segment(building_path).startswith("building_id="):
                    continue
                for fpath in _list_children(building_path):
                    if fpath.endswith(".parquet"):
                        files.append(_strip_blob_prefix(fpath))
    return files


def _read_paths(paths: list[str]) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    b = _blob()
    for p in paths:
        payload = b.get(p)
        table = pq.read_table(io.BytesIO(payload))
        df = table.to_pandas()
        rows.extend(df.to_dict(orient="records"))
    return pd.DataFrame(rows)


def _hours_filter(df: pd.DataFrame, hours: int) -> pd.DataFrame:
    if df.empty or "window_start" not in df.columns:
        return df
    ts = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours)))
    return df[ts >= cutoff]


def load_curated_df(blob_prefix: str = DEFAULT_BLOB_PREFIX, hours: int = 24) -> pd.DataFrame:
    df = _read_paths(_list_dataset_files(blob_prefix, "curated"))
    return _hours_filter(df, hours)


def load_alerts_df(blob_prefix: str = DEFAULT_BLOB_PREFIX, hours: int = 24) -> pd.DataFrame:
    df = _read_paths(_list_dataset_files(blob_prefix, "alerts"))
    return _hours_filter(df, hours)


def latest_kpis(blob_prefix: str = DEFAULT_BLOB_PREFIX, building_id: str | None = None) -> list[dict[str, Any]]:
    df = load_curated_df(blob_prefix=blob_prefix, hours=48)
    if df.empty:
        return []
    if building_id:
        df = df[df["building_id"] == building_id]
    if df.empty:
        return []

    df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
    latest_ts = df["window_start"].max()
    latest = df[df["window_start"] == latest_ts]

    cols = [
        "building_id",
        "floor",
        "zone",
        "window_start",
        "window_end",
        "energy_total_kwh",
        "energy_per_occupant",
        "comfort_score",
        "co2_risk_index",
        "avg_co2_ppm",
        "anomaly_score",
        "is_anomaly",
    ]
    for c in cols:
        if c not in latest.columns:
            latest[c] = None
    return latest[cols].to_dict(orient="records")


def query_timeseries(
    blob_prefix: str = DEFAULT_BLOB_PREFIX,
    building_id: str | None = None,
    metric: str = "energy_per_occupant",
    hours: int = 24,
) -> list[dict[str, Any]]:
    df = load_curated_df(blob_prefix=blob_prefix, hours=hours)
    if df.empty:
        return []

    allowed_metrics = {
        "energy_per_occupant",
        "comfort_score",
        "avg_co2_ppm",
        "energy_total_kwh",
        "anomaly_score",
    }
    if metric not in allowed_metrics:
        raise ValueError(f"Unsupported metric '{metric}'")

    if building_id:
        df = df[df["building_id"] == building_id]
    if df.empty:
        return []

    df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
    if "avg_occupancy" not in df.columns:
        df["avg_occupancy"] = None

    conn = duckdb.connect(database=":memory:")
    conn.register("curated_df", df)
    result = conn.execute(
        f"""
        SELECT
            window_start,
            building_id,
            AVG({metric}) AS value,
            AVG(avg_occupancy) AS avg_occupancy
        FROM curated_df
        GROUP BY 1,2
        ORDER BY 1
        """
    ).fetchdf()
    return result.to_dict(orient="records")


def query_alerts(
    blob_prefix: str = DEFAULT_BLOB_PREFIX,
    building_id: str | None = None,
    severity: str | None = None,
    status: str | None = "open",
    hours: int = 24,
) -> list[dict[str, Any]]:
    df = load_alerts_df(blob_prefix=blob_prefix, hours=hours)
    if df.empty:
        return []

    if building_id:
        df = df[df["building_id"] == building_id]
    if severity:
        df = df[df["severity"] == severity]
    if status:
        df = df[df["status"] == status]
    if df.empty:
        return []
    return df.sort_values("opened_at", ascending=False).to_dict(orient="records")


def top_anomalies(
    blob_prefix: str = DEFAULT_BLOB_PREFIX,
    building_id: str | None = None,
    limit: int = 20,
    hours: int = 24,
) -> list[dict[str, Any]]:
    df = load_curated_df(blob_prefix=blob_prefix, hours=hours)
    if df.empty:
        return []
    if building_id:
        df = df[df["building_id"] == building_id]
    if "anomaly_score" not in df.columns or df.empty:
        return []
    top = df.sort_values("anomaly_score", ascending=False).head(max(1, int(limit)))
    return top.to_dict(orient="records")


def list_buildings(blob_prefix: str = DEFAULT_BLOB_PREFIX) -> list[str]:
    df = load_curated_df(blob_prefix=blob_prefix, hours=24 * 14)
    if df.empty or "building_id" not in df.columns:
        return []
    return sorted([x for x in df["building_id"].dropna().unique().tolist() if isinstance(x, str)])
