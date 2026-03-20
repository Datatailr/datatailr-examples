"""
Data collector service: subscribes to price-server and price-processor SSE streams,
buffers rows, and flushes Parquet files under a Hive-style layout for DuckDB.

Layout (under COLLECTOR_BLOB_PREFIX or COLLECTOR_LOCAL_DIR):

  analytics/dt=YYYY-MM-DD/hour=HH/ticker=<TICKER>/part-<nanos>.parquet
  market_events/dt=YYYY-MM-DD/hour=HH/ticker=<TICKER>/part-<nanos>.parquet

DuckDB (after sync or with mounted paths):

  SELECT * FROM read_parquet('.../analytics/**/*.parquet', hive_partitioning = true);
  SELECT * FROM read_parquet('.../market_events/**/*.parquet', hive_partitioning = true);
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
import uvicorn


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8082"))

PRICE_SERVER_URL = os.environ.get("PRICE_SERVER_URL", "http://price-server")
PRICE_PROCESSOR_URL = os.environ.get("PRICE_PROCESSOR_URL", "http://price-processor")
if os.getenv("DATATAILR_JOB_TYPE", "workstation") in ("workstation", ""):
    PRICE_SERVER_URL = os.environ.get("PRICE_SERVER_URL", "http://localhost:8080")
    PRICE_PROCESSOR_URL = os.environ.get("PRICE_PROCESSOR_URL", "http://localhost:8081")

BLOB_PREFIX = os.environ.get("COLLECTOR_BLOB_PREFIX", "stock_price_lake").strip("/")
LOCAL_DIR = os.environ.get("COLLECTOR_LOCAL_DIR", "").strip()
FLUSH_INTERVAL_SEC = float(os.environ.get("COLLECTOR_FLUSH_INTERVAL_SEC", "120"))
MAX_BUFFER_ROWS = int(os.environ.get("COLLECTOR_MAX_BUFFER_ROWS", "250000"))
ENABLE_ANALYTICS = os.environ.get("COLLECTOR_ANALYTICS", "1").lower() not in ("0", "false", "no")
ENABLE_MARKET = os.environ.get("COLLECTOR_MARKET_EVENTS", "1").lower() not in ("0", "false", "no")

log_format = logging.Formatter("[%(asctime)s] [%(levelname)s] [collector] %(message)s")
log = logging.getLogger("data_collector")
log.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(log_format)
log.addHandler(_h)

_stop = threading.Event()
_analytics_buf: list[dict[str, Any]] = []
_market_buf: list[dict[str, Any]] = []
_buf_lock = threading.Lock()
_flush_lock = threading.Lock()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _partition_from_ts(ts_str: str | None) -> tuple[str, str]:
    """Return (dt YYYY-MM-DD, hour zero-padded HH) from ISO timestamp or now."""
    if ts_str:
        try:
            # Handle ...Z suffix
            s = ts_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H")
        except Exception:
            pass
    now = _utc_now()
    return now.strftime("%Y-%m-%d"), now.strftime("%H")


def _store_parquet(rel_path: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    data = buf.getvalue()
    full_rel = rel_path.lstrip("/")
    if LOCAL_DIR:
        path = Path(LOCAL_DIR) / full_rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        log.info("Wrote local Parquet %s (%d bytes)", path, len(data))
        return
    try:
        from datatailr import Blob

        key = f"{BLOB_PREFIX}/{full_rel}"
        Blob().put(key, data)
        log.info("Wrote blob %s (%d bytes)", key, len(data))
    except Exception as exc:
        log.error("Blob put failed for %s: %s", full_rel, exc)
        raise


def _ticker_partition_value(raw: Any) -> str:
    t = str(raw or "").strip().upper()
    if not t:
        return "UNKNOWN"
    # Keep Hive partition values path-safe and predictable.
    cleaned = "".join(ch if (ch.isalnum() or ch in ("_", "-", ".")) else "_" for ch in t)
    return cleaned or "UNKNOWN"


def _flush_partitioned(kind: str, rows: list[dict[str, Any]]) -> None:
    """Write one Parquet file per (dt, hour, ticker) partition (Hive-style paths for DuckDB)."""
    if not rows:
        return
    parts: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        ts_raw = r.get("timestamp")
        if not isinstance(ts_raw, str):
            ts_raw = r.get("ingested_at")
        dt_s, h_s = _partition_from_ts(ts_raw if isinstance(ts_raw, str) else None)
        ticker = _ticker_partition_value(r.get("ticker"))
        parts[(dt_s, h_s, ticker)].append(r)

    for (dt_s, h_s, ticker), part_rows in parts.items():
        df = pd.DataFrame(part_rows)
        table = pa.Table.from_pandas(df, preserve_index=False)
        name = f"{kind}/dt={dt_s}/hour={h_s}/ticker={ticker}/part-{time.time_ns()}.parquet"
        _store_parquet(name, table)


def flush_buffers() -> None:
    global _analytics_buf, _market_buf
    with _flush_lock:
        with _buf_lock:
            a = _analytics_buf
            m = _market_buf
            _analytics_buf = []
            _market_buf = []
        try:
            _flush_partitioned("analytics", a)
        except Exception:
            log.exception("analytics flush failed")
        try:
            _flush_partitioned("market_events", m)
        except Exception:
            log.exception("market_events flush failed")


def _flush_loop() -> None:
    while not _stop.wait(timeout=FLUSH_INTERVAL_SEC):
        flush_buffers()
    flush_buffers()


def _normalize_analytics(obj: dict[str, Any]) -> dict[str, Any]:
    row = dict(obj)
    row["ingested_at"] = _utc_now().isoformat()
    return row


def _normalize_market(obj: dict[str, Any]) -> dict[str, Any]:
    typ = obj.get("type", "")
    base = {
        "ingested_at": _utc_now().isoformat(),
        "event_type": typ,
        "ticker": obj.get("ticker"),
        "seq": obj.get("seq"),
        "timestamp": obj.get("timestamp"),
    }
    if typ == "trade":
        base.update(
            {
                "price": obj.get("price"),
                "size": obj.get("size"),
                "side": obj.get("side"),
                "bid": None,
                "ask": None,
                "mid": None,
                "spread": None,
                "quote_side": None,
                "quote_price": None,
                "quote_size": None,
            }
        )
    elif typ == "quote":
        base.update(
            {
                "price": None,
                "size": None,
                "side": None,
                "bid": obj.get("bid"),
                "ask": obj.get("ask"),
                "mid": obj.get("mid"),
                "spread": obj.get("spread"),
                "quote_side": obj.get("quote_side"),
                "quote_price": obj.get("quote_price"),
                "quote_size": obj.get("quote_size"),
            }
        )
    else:
        base["raw_json"] = json.dumps(obj)[:8000]
    return base


def _sse_reader_loop(url: str, label: str, on_event) -> None:
    while not _stop.is_set():
        try:
            log.info("SSE connect %s (%s)", label, url)
            with requests.get(
                url,
                stream=True,
                timeout=(15, None),
                headers={"Accept": "text/event-stream"},
            ) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines(decode_unicode=True):
                    if _stop.is_set():
                        break
                    if not line or not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    try:
                        on_event(obj)
                    except Exception as exc:
                        log.warning("%s handler error: %s", label, exc)
        except Exception as exc:
            log.warning("SSE %s error %s — retry in 3s", label, exc)
            time.sleep(3)


def _on_analytics(obj: dict[str, Any]) -> None:
    row = _normalize_analytics(obj)
    with _buf_lock:
        _analytics_buf.append(row)
        n = len(_analytics_buf) + len(_market_buf)
    if n >= MAX_BUFFER_ROWS:
        threading.Thread(target=flush_buffers, daemon=True).start()


def _on_market(obj: dict[str, Any]) -> None:
    row = _normalize_market(obj)
    with _buf_lock:
        _market_buf.append(row)
        n = len(_analytics_buf) + len(_market_buf)
    if n >= MAX_BUFFER_ROWS:
        threading.Thread(target=flush_buffers, daemon=True).start()


def _start_stream_threads() -> list[threading.Thread]:
    threads: list[threading.Thread] = []
    if ENABLE_ANALYTICS:
        t = threading.Thread(
            target=_sse_reader_loop,
            args=(f"{PRICE_PROCESSOR_URL.rstrip('/')}/stream", "analytics", _on_analytics),
            daemon=True,
        )
        t.start()
        threads.append(t)
    if ENABLE_MARKET:
        t = threading.Thread(
            target=_sse_reader_loop,
            args=(f"{PRICE_SERVER_URL.rstrip('/')}/stream", "market", _on_market),
            daemon=True,
        )
        t.start()
        threads.append(t)
    ft = threading.Thread(target=_flush_loop, daemon=True)
    ft.start()
    threads.append(ft)
    return threads


app = FastAPI(title="Stock data collector", version="0.1.0")


@app.get("/__health_check__.html")
async def health():
    return PlainTextResponse("OK\n")


@app.get("/status")
async def status():
    with _buf_lock:
        return {
            "analytics_buffer_rows": len(_analytics_buf),
            "market_buffer_rows": len(_market_buf),
            "blob_prefix": BLOB_PREFIX,
            "local_dir": LOCAL_DIR or None,
            "flush_interval_sec": FLUSH_INTERVAL_SEC,
            "analytics_sse": ENABLE_ANALYTICS,
            "market_sse": ENABLE_MARKET,
        }


def main(port: int | None = None) -> None:
    p = int(port or PORT)
    if LOCAL_DIR:
        log.info("Collector local output: %s", Path(LOCAL_DIR).resolve())
    else:
        log.info("Collector blob prefix: %s", BLOB_PREFIX)
    log.info(
        "Flush every %ss | analytics=%s market=%s | processor=%s price=%s",
        FLUSH_INTERVAL_SEC,
        ENABLE_ANALYTICS,
        ENABLE_MARKET,
        PRICE_PROCESSOR_URL,
        PRICE_SERVER_URL,
    )
    _start_stream_threads()
    log.info("Data collector HTTP on %s:%s", HOST, p)
    uvicorn.run(app, host=HOST, port=p, log_level="info")


if __name__ == "__main__":
    main(PORT)
