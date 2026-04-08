from __future__ import annotations

import io
import logging
import math
import os
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import uvicorn

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8090"))

BLOB_PREFIX = os.environ.get("ENERGY_BLOB_PREFIX", "smart_building_energy").strip("/")
LOCAL_DIR = os.environ.get("ENERGY_LOCAL_DIR", "").strip()
FLUSH_INTERVAL_SEC = float(os.environ.get("INGEST_FLUSH_INTERVAL_SEC", "30"))
MAX_BUFFER_ROWS = int(os.environ.get("INGEST_MAX_BUFFER_ROWS", "5000"))
NUM_BUILDINGS = int(os.environ.get("NUM_BUILDINGS", "3"))
ZONES_PER_BUILDING = int(os.environ.get("ZONES_PER_BUILDING", "12"))
EVENTS_PER_SEC = float(os.environ.get("INGEST_EVENTS_PER_SEC", "10"))

log = logging.getLogger("sensor_ingestor")
log.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [ingestor] %(message)s"))
log.addHandler(_handler)

_stop = threading.Event()
_buf_lock = threading.Lock()
_flush_lock = threading.Lock()
_rows: list[dict[str, Any]] = []
_total_generated = 0
_total_flushed = 0
_last_flush_iso: str | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _zone_label(idx: int) -> str:
    return f"Z{idx:02d}"


def _hourly_occupancy_multiplier(hour: int) -> float:
    if 7 <= hour <= 9:
        return 0.9
    if 10 <= hour <= 16:
        return 1.0
    if 17 <= hour <= 19:
        return 0.6
    return 0.2


def _random_weather(now: datetime) -> tuple[str, float]:
    season_shift = math.sin(2 * math.pi * (now.timetuple().tm_yday / 365.0))
    daily_shift = math.sin(2 * math.pi * ((now.hour + now.minute / 60.0) / 24.0))
    outside = 10.0 + 12.0 * season_shift + 6.0 * daily_shift + random.gauss(0, 1.2)
    if outside < 5:
        condition = "cold"
    elif outside > 25:
        condition = "hot"
    else:
        condition = "mild"
    return condition, round(outside, 2)


def _generate_event() -> dict[str, Any]:
    now = _utc_now()
    building_id = f"B{random.randint(1, NUM_BUILDINGS)}"
    zone_idx = random.randint(1, ZONES_PER_BUILDING)
    floor = 1 + (zone_idx - 1) // 4
    zone = _zone_label(zone_idx)

    occ_mult = _hourly_occupancy_multiplier(now.hour)
    occupancy = max(0, int(random.gauss(16 * occ_mult, 4)))
    weather_condition, outside_temp = _random_weather(now)

    comfort_target = 22.0
    temperature = comfort_target + random.gauss(0, 1.0) + (outside_temp - comfort_target) * 0.08
    humidity = 45 + random.gauss(0, 6)
    co2 = 450 + occupancy * random.uniform(8, 16) + random.gauss(0, 25)

    hvac_kw = max(2.0, abs(comfort_target - temperature) * random.uniform(0.8, 1.4) + occupancy * 0.12)
    lighting_kw = max(0.2, occupancy * random.uniform(0.02, 0.06))
    plug_load_kw = max(0.4, occupancy * random.uniform(0.03, 0.1))

    if random.random() < 0.01:
        # Occasional synthetic anomaly to make downstream alerting visible.
        co2 *= random.uniform(1.8, 2.4)
        hvac_kw *= random.uniform(1.5, 2.2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "ingested_at": now.isoformat(),
        "building_id": building_id,
        "floor": int(floor),
        "zone": zone,
        "temperature_c": round(temperature, 3),
        "humidity_pct": round(max(15, min(85, humidity)), 3),
        "co2_ppm": round(max(350, co2), 3),
        "occupancy": int(occupancy),
        "hvac_power_kw": round(hvac_kw, 3),
        "lighting_power_kw": round(lighting_kw, 3),
        "plug_load_kw": round(plug_load_kw, 3),
        "outside_temp_c": outside_temp,
        "weather_condition": weather_condition,
        "source": "simulator",
    }


def _partition_key(row: dict[str, Any]) -> tuple[str, str, str]:
    ts = datetime.fromisoformat(row["event_ts"].replace("Z", "+00:00")).astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%d"), ts.strftime("%H"), str(row["building_id"])


def _store_parquet(rel_path: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    payload = buf.getvalue()

    if LOCAL_DIR:
        path = Path(LOCAL_DIR) / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return

    from datatailr import Blob

    Blob().put(f"{BLOB_PREFIX}/{rel_path}", payload)


def flush_rows() -> int:
    global _rows, _total_flushed, _last_flush_iso
    with _flush_lock:
        with _buf_lock:
            rows = _rows
            _rows = []
        if not rows:
            return 0

        df = pd.DataFrame(rows)
        grouped = df.groupby(df.apply(lambda r: _partition_key(r.to_dict()), axis=1))

        flushed = 0
        for (dt_s, hour_s, building), part in grouped:
            rel_path = (
                f"raw/dt={dt_s}/hour={hour_s}/building_id={building}/"
                f"part-{time.time_ns()}.parquet"
            )
            table = pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False)
            _store_parquet(rel_path, table)
            flushed += len(part)

        _total_flushed += flushed
        _last_flush_iso = _utc_now().isoformat()
        log.info("Flushed %s rows to lake", flushed)
        return flushed


def _flush_loop() -> None:
    while not _stop.wait(timeout=FLUSH_INTERVAL_SEC):
        flush_rows()
    flush_rows()


def _producer_loop() -> None:
    global _total_generated
    sleep_for = max(0.01, 1.0 / max(EVENTS_PER_SEC, 0.1))
    while not _stop.is_set():
        row = _generate_event()
        with _buf_lock:
            _rows.append(row)
            _total_generated += 1
            buf_len = len(_rows)
        if buf_len >= MAX_BUFFER_ROWS:
            threading.Thread(target=flush_rows, daemon=True).start()
        time.sleep(sleep_for)


app = FastAPI(title="Smart building sensor ingestor", version="0.1.0")


@app.get("/__health_check__.html")
async def health() -> PlainTextResponse:
    return PlainTextResponse("OK\n")


@app.get("/status")
async def status() -> dict[str, Any]:
    with _buf_lock:
        buffered = len(_rows)
    return {
        "buffered_rows": buffered,
        "total_generated_rows": _total_generated,
        "total_flushed_rows": _total_flushed,
        "last_flush_at": _last_flush_iso,
        "blob_prefix": BLOB_PREFIX,
        "local_dir": LOCAL_DIR or None,
        "flush_interval_sec": FLUSH_INTERVAL_SEC,
        "max_buffer_rows": MAX_BUFFER_ROWS,
        "events_per_sec": EVENTS_PER_SEC,
        "num_buildings": NUM_BUILDINGS,
        "zones_per_building": ZONES_PER_BUILDING,
    }


def _start_threads() -> None:
    threading.Thread(target=_producer_loop, daemon=True).start()
    threading.Thread(target=_flush_loop, daemon=True).start()


def main(port: int | None = None) -> None:
    bind_port = int(port or PORT)
    if LOCAL_DIR:
        log.info("Using local dir: %s", Path(LOCAL_DIR).resolve())
    else:
        log.info("Using blob prefix: %s", BLOB_PREFIX)
    _start_threads()
    log.info("Starting sensor ingestor on %s:%s", HOST, bind_port)
    uvicorn.run(app, host=HOST, port=bind_port, log_level="info")


if __name__ == "__main__":
    main(PORT)

