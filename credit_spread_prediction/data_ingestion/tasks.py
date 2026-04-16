"""Datatailr tasks for FRED ingestion."""

from __future__ import annotations

import io
import json
from dataclasses import asdict
from datetime import datetime, timezone

import pandas as pd
from datatailr import task

from credit_spread_prediction.blob_io import blob_put
from credit_spread_prediction.config import ALL_SERIES, RAW_PREFIX
from credit_spread_prediction.data_ingestion.fred_client import (
    FredSeriesRequest,
    fetch_observations,
    observations_to_frame,
    today_iso,
)


def _raw_payload_key(series_id: str, snapshot_dt: str) -> str:
    return f"{RAW_PREFIX}/dataset=fred_raw/dt={snapshot_dt}/series_id={series_id}/payload.json"


def _series_parquet_key(series_id: str, snapshot_dt: str) -> str:
    return f"{RAW_PREFIX}/dataset=fred_clean/dt={snapshot_dt}/series_id={series_id}/observations.parquet"


@task()
def fetch_fred_series(
    series_id: str,
    observation_start: str = "1990-01-01",
    observation_end: str | None = None,
) -> dict[str, str]:
    from datatailr import Blob

    req = FredSeriesRequest(
        series_id=series_id,
        observation_start=observation_start,
        observation_end=observation_end,
    )
    payload = fetch_observations(req)
    snapshot_dt = today_iso()
    blob = Blob()

    blob_put(blob, _raw_payload_key(series_id, snapshot_dt), json.dumps(payload).encode("utf-8"))

    frame = observations_to_frame(series_id, payload)
    table_bytes = io.BytesIO()
    frame.to_parquet(table_bytes, index=False)
    blob_put(blob, _series_parquet_key(series_id, snapshot_dt), table_bytes.getvalue())

    return {
        "series_id": series_id,
        "snapshot_dt": snapshot_dt,
        "rows": str(len(frame)),
        "request": json.dumps(asdict(req)),
    }


@task()
def collect_ingestion_summary(fetch_results: list[dict[str, str]]) -> dict[str, str]:
    from datatailr import Blob

    now_utc = datetime.now(timezone.utc).isoformat()
    ok = len(fetch_results)
    snapshot_candidates = sorted({r.get("snapshot_dt", "") for r in fetch_results if r.get("snapshot_dt")})
    summary = {
        "ingested_series_count": str(ok),
        "ingested_series": ",".join(sorted(r["series_id"] for r in fetch_results)),
        "completed_at_utc": now_utc,
        "snapshot_dt": snapshot_candidates[-1] if snapshot_candidates else "",
    }
    key = f"{RAW_PREFIX}/runs/{now_utc.replace(':', '-')}.json"
    blob_put(Blob(), key, json.dumps(summary).encode("utf-8"))
    return summary


@task()
def get_default_series() -> list[str]:
    return list(ALL_SERIES)


@task()
def fetch_all_fred_series(
    observation_start: str = "1990-01-01",
    observation_end: str | None = None,
) -> dict[str, str]:
    results = []
    for series_id in ALL_SERIES:
        results.append(
            fetch_fred_series(
                series_id=series_id,
                observation_start=observation_start,
                observation_end=observation_end,
            )
        )
    return collect_ingestion_summary(results)

