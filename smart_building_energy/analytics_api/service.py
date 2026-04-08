from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
import uvicorn

from smart_building_energy.lake_query.reader import (
    DEFAULT_BLOB_PREFIX,
    latest_kpis,
    list_buildings,
    query_alerts,
    query_timeseries,
    top_anomalies,
)

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8091"))
BLOB_PREFIX = os.environ.get("ENERGY_BLOB_PREFIX", DEFAULT_BLOB_PREFIX)
CACHE_TTL_SEC = int(os.environ.get("API_CACHE_TTL_SEC", "30"))

log = logging.getLogger("analytics_api")
log.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [analytics_api] %(message)s"))
log.addHandler(_h)

_cache: dict[str, tuple[float, Any]] = {}


def _cache_get(key: str) -> Any | None:
    got = _cache.get(key)
    if not got:
        return None
    ts, payload = got
    if (time.time() - ts) > CACHE_TTL_SEC:
        _cache.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Any) -> Any:
    _cache[key] = (time.time(), payload)
    return payload


app = FastAPI(title="Building Analytics API", version="0.1.0")


@app.get("/__health_check__.html")
async def health_check() -> PlainTextResponse:
    return PlainTextResponse("OK\n")


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "blob_prefix": BLOB_PREFIX,
        "cache_size": len(_cache),
        "cache_ttl_sec": CACHE_TTL_SEC,
    }


@app.get("/metadata/buildings")
async def buildings() -> dict[str, Any]:
    key = "buildings"
    cached = _cache_get(key)
    if cached is not None:
        return {"buildings": cached, "cached": True}
    vals = list_buildings(BLOB_PREFIX)
    return {"buildings": _cache_set(key, vals), "cached": False}


@app.get("/kpi/latest")
async def kpi_latest(building_id: str | None = None) -> dict[str, Any]:
    key = f"latest:{building_id or '*'}"
    cached = _cache_get(key)
    if cached is not None:
        return {"items": cached, "cached": True}
    data = latest_kpis(BLOB_PREFIX, building_id=building_id)
    return {"items": _cache_set(key, data), "cached": False}


@app.get("/kpi/timeseries")
async def kpi_timeseries(
    building_id: str | None = None,
    metric: str = Query("energy_per_occupant"),
    hours: int = Query(24, ge=1, le=24 * 14),
) -> dict[str, Any]:
    key = f"ts:{building_id or '*'}:{metric}:{hours}"
    cached = _cache_get(key)
    if cached is not None:
        return {"items": cached, "cached": True}
    try:
        data = query_timeseries(BLOB_PREFIX, building_id=building_id, metric=metric, hours=hours)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"items": _cache_set(key, data), "cached": False}


@app.get("/alerts")
async def alerts(
    building_id: str | None = None,
    severity: str | None = None,
    status: str | None = "open",
    hours: int = Query(24, ge=1, le=24 * 14),
) -> dict[str, Any]:
    key = f"alerts:{building_id or '*'}:{severity or '*'}:{status or '*'}:{hours}"
    cached = _cache_get(key)
    if cached is not None:
        return {"items": cached, "cached": True}
    data = query_alerts(
        BLOB_PREFIX,
        building_id=building_id,
        severity=severity,
        status=status,
        hours=hours,
    )
    return {"items": _cache_set(key, data), "cached": False}


@app.get("/anomalies/top")
async def anomalies_top(
    building_id: str | None = None,
    limit: int = Query(20, ge=1, le=200),
    hours: int = Query(24, ge=1, le=24 * 14),
) -> dict[str, Any]:
    key = f"anom:{building_id or '*'}:{limit}:{hours}"
    cached = _cache_get(key)
    if cached is not None:
        return {"items": cached, "cached": True}
    data = top_anomalies(BLOB_PREFIX, building_id=building_id, limit=limit, hours=hours)
    return {"items": _cache_set(key, data), "cached": False}


def main(port: int | None = None) -> None:
    bind_port = int(port or PORT)
    log.info("Starting analytics API on %s:%s with blob prefix %s", HOST, bind_port, BLOB_PREFIX)
    uvicorn.run(app, host=HOST, port=bind_port, log_level="info")


if __name__ == "__main__":
    main(PORT)

