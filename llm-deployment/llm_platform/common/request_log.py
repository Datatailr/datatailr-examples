"""Request/reply log records and the Parquet writer used by the gateway.

Records are buffered in memory and flushed as small Parquet files to blob
storage under ``llm_platform/logs/date=YYYY-MM-DD/``. The admin app reads the
whole tree with DuckDB (``read_parquet`` with hive partitioning).
"""

from __future__ import annotations

import io
import os
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from datatailr import Blob
from datatailr.logging import DatatailrLogger
from pydantic import BaseModel, Field

from llm_platform.common.naming import LOGS_PREFIX

log = DatatailrLogger(__name__).get_logger()

SCHEMA = pa.schema(
    [
        ("request_id", pa.string()),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("user", pa.string()),
        ("model", pa.string()),
        ("deployment", pa.string()),
        ("endpoint", pa.string()),
        ("stream", pa.bool_()),
        ("status", pa.int32()),
        ("latency_ms", pa.int64()),
        ("prompt_tokens", pa.int64()),
        ("completion_tokens", pa.int64()),
        ("request_json", pa.string()),
        ("response_text", pa.string()),
        ("error", pa.string()),
        ("client", pa.string()),
    ]
)


class RequestRecord(BaseModel):
    request_id: str
    ts: float = Field(default_factory=time.time)
    user: str = ""
    model: str = ""
    deployment: str = ""
    endpoint: str = ""
    stream: bool = False
    status: int = 0
    latency_ms: int = 0
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    request_json: str | None = None
    response_text: str | None = None
    error: str | None = None
    client: str = ""


def records_to_table(records: list[RequestRecord]) -> pa.Table:
    columns: dict[str, list[Any]] = {name: [] for name in SCHEMA.names}
    for rec in records:
        data = rec.model_dump()
        data["ts"] = datetime.fromtimestamp(rec.ts, tz=timezone.utc)
        for name in SCHEMA.names:
            columns[name].append(data.get(name))
    return pa.Table.from_pydict(columns, schema=SCHEMA)


class ParquetLogWriter:
    """Thread-safe buffer that periodically flushes Parquet files to blob."""

    def __init__(
        self,
        prefix: str = LOGS_PREFIX,
        flush_interval: float = 10.0,
        flush_records: int = 200,
        local_spool: str = "/tmp/llm_platform_spool",
    ):
        self._blob = Blob()
        self._prefix = prefix.rstrip("/") + "/"
        self._interval = flush_interval
        self._max_records = flush_records
        self._buffer: list[RequestRecord] = []
        self._lock = threading.Lock()
        self._host = os.environ.get("HOSTNAME") or socket.gethostname()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._spool = local_spool
        os.makedirs(self._spool, exist_ok=True)

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="parquet-log-writer", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self.flush()

    def append(self, record: RequestRecord) -> None:
        with self._lock:
            self._buffer.append(record)
            should_flush = len(self._buffer) >= self._max_records
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            batch, self._buffer = self._buffer, []
        try:
            table = records_to_table(batch)
            sink = io.BytesIO()
            pq.write_table(table, sink, compression="zstd")
            day = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            name = f"{self._prefix}date={day}/{self._host}-{int(time.time() * 1000)}.parquet"
            self._blob.put(name, sink.getvalue())
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to flush %d log records to blob: %s", len(batch), exc)
            # Keep the records so the next flush retries them.
            with self._lock:
                self._buffer = batch + self._buffer

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            self.flush()
