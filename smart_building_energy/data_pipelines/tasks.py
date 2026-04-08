from __future__ import annotations

import io
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datatailr import task

DEFAULT_BLOB_PREFIX = os.environ.get("ENERGY_BLOB_PREFIX", "smart_building_energy").strip("/")
DEFAULT_LOOKBACK_HOURS = int(os.environ.get("PIPELINE_LOOKBACK_HOURS", "2"))
DEFAULT_COMFORT_MIN = float(os.environ.get("ALERT_COMFORT_MIN", "60"))
DEFAULT_CO2_THRESHOLD = float(os.environ.get("ALERT_CO2_THRESHOLD", "1200"))
DEFAULT_ANOMALY_Z = float(os.environ.get("ANOMALY_ZSCORE_THRESHOLD", "3.0"))


def _blob():
    from datatailr import Blob

    return Blob()


def _parse_dt_hour(path: str) -> tuple[str, str] | None:
    # Expected .../dt=YYYY-MM-DD/hour=HH/...
    pieces = path.split("/")
    dt_s = None
    hour_s = None
    for p in pieces:
        if p.startswith("dt="):
            dt_s = p[3:]
        elif p.startswith("hour="):
            hour_s = p[5:]
    if dt_s and hour_s:
        return dt_s, hour_s
    return None


def _iter_recent_hours(now: datetime, n: int) -> set[tuple[str, str]]:
    vals: set[tuple[str, str]] = set()
    for i in range(max(1, n)):
        t = now - timedelta(hours=i)
        vals.add((t.strftime("%Y-%m-%d"), t.strftime("%H")))
    return vals


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


def _list_raw_parquet_files(blob_prefix: str) -> list[str]:
    raw_root = f"{_strip_blob_prefix(blob_prefix)}/raw"
    files: list[str] = []
    for dt_path in _list_children(raw_root):
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


@task(memory="512m", cpu=0.5)
def list_recent_raw_files(blob_prefix: str = DEFAULT_BLOB_PREFIX, lookback_hours: int = DEFAULT_LOOKBACK_HOURS) -> list[str]:
    all_paths = _list_raw_parquet_files(blob_prefix)
    cutoff_hours = _iter_recent_hours(datetime.now(timezone.utc), lookback_hours)
    recent: list[str] = []
    for p in all_paths:
        parsed = _parse_dt_hour(p)
        if parsed is None:
            continue
        if parsed in cutoff_hours and p.endswith(".parquet"):
            recent.append(p)
    return sorted(recent)


@task(memory="1g", cpu=1)
def load_raw_data(paths: list[str]) -> dict[str, Any]:
    if not paths:
        return {"records": [], "num_rows": 0}
    rows: list[dict[str, Any]] = []
    b = _blob()
    for p in paths:
        payload = b.get(p)
        table = pq.read_table(io.BytesIO(payload))
        df = table.to_pandas()
        rows.extend(df.to_dict(orient="records"))
    return {"records": rows, "num_rows": len(rows)}


@task(memory="1g", cpu=1)
def clean_validate(raw: dict[str, Any]) -> dict[str, Any]:
    df = pd.DataFrame(raw.get("records", []))
    if df.empty:
        return {"records": [], "num_rows": 0, "quality": {"kept": 0, "dropped": 0}}

    before = len(df)
    required_cols = [
        "event_ts",
        "building_id",
        "floor",
        "zone",
        "temperature_c",
        "humidity_pct",
        "co2_ppm",
        "occupancy",
        "hvac_power_kw",
        "lighting_power_kw",
        "plug_load_kw",
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = np.nan

    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    numeric_cols = [
        "temperature_c",
        "humidity_pct",
        "co2_ppm",
        "occupancy",
        "hvac_power_kw",
        "lighting_power_kw",
        "plug_load_kw",
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["event_ts", "building_id", "zone"])
    df = df[
        (df["temperature_c"].between(-10, 60))
        & (df["humidity_pct"].between(0, 100))
        & (df["co2_ppm"].between(250, 8000))
        & (df["occupancy"] >= 0)
    ]
    df["occupancy"] = df["occupancy"].fillna(0)
    df["floor"] = df["floor"].fillna(0).astype(int)

    # Normalize to 15-minute windows.
    df["window_start"] = df["event_ts"].dt.floor("15min")
    df["window_end"] = df["window_start"] + pd.Timedelta(minutes=15)
    kept = len(df)
    return {
        "records": df.to_dict(orient="records"),
        "num_rows": kept,
        "quality": {"kept": kept, "dropped": max(0, before - kept)},
    }


@task(memory="1g", cpu=1)
def compute_kpis(cleaned: dict[str, Any]) -> dict[str, Any]:
    df = pd.DataFrame(cleaned.get("records", []))
    if df.empty:
        return {"kpis": [], "num_rows": 0}

    df["energy_total_kw"] = df["hvac_power_kw"] + df["lighting_power_kw"] + df["plug_load_kw"]
    df["energy_total_kwh"] = df["energy_total_kw"] * (1.0 / 60.0)
    df["comfort_score"] = (
        100
        - (df["temperature_c"].sub(22.0).abs() * 6.0)
        - (df["humidity_pct"].sub(45.0).abs() * 0.8)
    ).clip(lower=0, upper=100)
    df["co2_risk_index"] = (df["co2_ppm"] / 1500.0).clip(lower=0, upper=1.5)
    df["energy_per_occupant"] = np.where(df["occupancy"] > 0, df["energy_total_kw"] / df["occupancy"], df["energy_total_kw"])

    grouped = (
        df.groupby(["building_id", "floor", "zone", "window_start", "window_end"], as_index=False)
        .agg(
            energy_total_kwh=("energy_total_kwh", "sum"),
            avg_temperature_c=("temperature_c", "mean"),
            avg_humidity_pct=("humidity_pct", "mean"),
            avg_co2_ppm=("co2_ppm", "mean"),
            avg_occupancy=("occupancy", "mean"),
            energy_per_occupant=("energy_per_occupant", "mean"),
            comfort_score=("comfort_score", "mean"),
            co2_risk_index=("co2_risk_index", "mean"),
            raw_rows_used=("event_id", "count"),
        )
        .sort_values(["window_start", "building_id", "zone"])
    )
    grouped["pipeline_run_id"] = str(uuid.uuid4())
    return {"kpis": grouped.to_dict(orient="records"), "num_rows": len(grouped)}


@task(memory="1g", cpu=1)
def detect_anomalies(kpis_result: dict[str, Any], z_threshold: float = DEFAULT_ANOMALY_Z) -> dict[str, Any]:
    df = pd.DataFrame(kpis_result.get("kpis", []))
    if df.empty:
        return {"kpis": [], "num_anomalies": 0}

    df["anomaly_score"] = 0.0
    for col in ["energy_per_occupant", "avg_co2_ppm"]:
        series = pd.to_numeric(df[col], errors="coerce")
        mu = float(series.mean()) if len(series) else 0.0
        sigma = float(series.std()) if len(series) else 0.0
        if sigma > 0:
            z = ((series - mu).abs() / sigma).fillna(0.0)
            df["anomaly_score"] = np.maximum(df["anomaly_score"], z)

    df["is_anomaly"] = df["anomaly_score"] >= float(z_threshold)
    return {
        "kpis": df.to_dict(orient="records"),
        "num_anomalies": int(df["is_anomaly"].sum()),
    }


@task(memory="512m", cpu=0.5)
def generate_alerts(
    scored: dict[str, Any],
    comfort_min: float = DEFAULT_COMFORT_MIN,
    co2_threshold: float = DEFAULT_CO2_THRESHOLD,
) -> list[dict[str, Any]]:
    rows = scored.get("kpis", [])
    alerts: list[dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for row in rows:
        base = {
            "alert_id": str(uuid.uuid4()),
            "building_id": row.get("building_id"),
            "floor": int(row.get("floor", 0)),
            "zone": row.get("zone"),
            "opened_at": now_iso,
            "resolved_at": None,
            "status": "open",
            "window_start": row.get("window_start"),
            "window_end": row.get("window_end"),
        }
        comfort = float(row.get("comfort_score", 0))
        co2 = float(row.get("avg_co2_ppm", 0))
        anomaly = float(row.get("anomaly_score", 0))

        if comfort < comfort_min:
            alerts.append(
                {
                    **base,
                    "severity": "high" if comfort < (comfort_min - 15) else "medium",
                    "type": "comfort",
                    "message": "Comfort score below threshold",
                    "metric_name": "comfort_score",
                    "metric_value": comfort,
                    "threshold": comfort_min,
                }
            )
        if co2 > co2_threshold:
            alerts.append(
                {
                    **base,
                    "severity": "high" if co2 > (co2_threshold * 1.2) else "medium",
                    "type": "air_quality",
                    "message": "CO2 above threshold",
                    "metric_name": "avg_co2_ppm",
                    "metric_value": co2,
                    "threshold": co2_threshold,
                }
            )
        if anomaly >= 4:
            alerts.append(
                {
                    **base,
                    "severity": "high",
                    "type": "anomaly",
                    "message": "Anomaly score exceeded expected range",
                    "metric_name": "anomaly_score",
                    "metric_value": anomaly,
                    "threshold": 4,
                }
            )
    return alerts


def _put_df(prefix: str, rel_root: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    b = _blob()
    written = 0
    for (dt_s, hour_s, building), part in df.groupby(
        [
            pd.to_datetime(df["window_start"], utc=True).dt.strftime("%Y-%m-%d"),
            pd.to_datetime(df["window_start"], utc=True).dt.strftime("%H"),
            df["building_id"],
        ]
    ):
        table = pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        path = (
            f"{prefix}/{rel_root}/dt={dt_s}/hour={hour_s}/building_id={building}/"
            f"part-{time.time_ns()}.parquet"
        )
        b.put(path, buf.getvalue())
        written += len(part)
    return written


@task(memory="1g", cpu=1)
def write_outputs(
    scored: dict[str, Any],
    alerts: list[dict[str, Any]],
    blob_prefix: str = DEFAULT_BLOB_PREFIX,
) -> dict[str, Any]:
    kpis_df = pd.DataFrame(scored.get("kpis", []))
    alerts_df = pd.DataFrame(alerts)

    written_kpis = _put_df(blob_prefix, "curated", kpis_df) if not kpis_df.empty else 0
    written_alerts = _put_df(blob_prefix, "alerts", alerts_df) if not alerts_df.empty else 0

    return {
        "written_curated_rows": int(written_kpis),
        "written_alert_rows": int(written_alerts),
    }


@task(memory="512m", cpu=0.5)
def summarize(
    files: list[str],
    raw: dict[str, Any],
    cleaned: dict[str, Any],
    scored: dict[str, Any],
    alerts: list[dict[str, Any]],
    write_result: dict[str, Any],
) -> dict[str, Any]:
    summary = {
        "run_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_files": len(files),
        "raw_rows": int(raw.get("num_rows", 0)),
        "clean_rows": int(cleaned.get("num_rows", 0)),
        "kpi_rows": len(scored.get("kpis", [])),
        "alerts": len(alerts),
        "anomalies": int(scored.get("num_anomalies", 0)),
        "writes": write_result,
    }
    return json.loads(json.dumps(summary, default=str))

