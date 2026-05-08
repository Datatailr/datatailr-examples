"""Mock external API that streams parquet files of arbitrary size on demand.

This Flask service simulates an external data provider exposing a single
endpoint that returns a parquet file whose size is controlled by the caller.

The implementation is designed to demonstrate true streaming behaviour:
parquet row groups are produced one at a time and immediately flushed onto
the response body. The whole file is never buffered in memory, so the
service can serve arbitrarily large responses with a small, constant
memory footprint.
"""

from __future__ import annotations

import os
import random
import string
import time
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from flask import Flask, Response, jsonify, request, stream_with_context

app = Flask(__name__)


# Mock market data schema. Picked to produce non-trivial row sizes
# (~70 bytes uncompressed) so users see realistic file sizes for the
# row counts the service generates.
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("event_time", pa.timestamp("ms")),
        ("symbol", pa.string()),
        ("price", pa.float64()),
        ("volume", pa.int64()),
        ("category", pa.string()),
        ("note", pa.string()),
    ]
)

_SYMBOLS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "META", "NVDA", "AMD", "NFLX", "INTC"]
_CATEGORIES = ["large_cap", "mid_cap", "small_cap"]


class _StreamingSink:
    """File-like sink that buffers writes and exposes them via ``drain()``.

    pyarrow's ``ParquetWriter`` only needs ``write``, ``tell``, ``flush`` and
    ``close`` from the underlying file object. It never seeks during writes:
    row group offsets are computed from ``tell()`` and the footer is appended
    at the current position when the writer is closed.

    By draining the buffer between row groups, we forward bytes to the
    network as soon as they are produced, while the position counter keeps
    growing so the eventual footer remains consistent.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._position = 0
        self._closed = False

    def write(self, data) -> int:
        if isinstance(data, memoryview):
            data = bytes(data)
        self._buffer.extend(data)
        self._position += len(data)
        return len(data)

    def tell(self) -> int:
        return self._position

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def drain(self) -> bytes:
        data = bytes(self._buffer)
        self._buffer.clear()
        return data


def _generate_chunk(num_rows: int, start_idx: int) -> pa.Table:
    """Build a pyarrow Table with mock market data."""
    now_ms = int(time.time() * 1000)
    return pa.table(
        {
            "id": list(range(start_idx, start_idx + num_rows)),
            "event_time": pa.array(
                [now_ms + i for i in range(num_rows)], type=pa.timestamp("ms")
            ),
            "symbol": [random.choice(_SYMBOLS) for _ in range(num_rows)],
            "price": [round(random.uniform(50.0, 500.0), 2) for _ in range(num_rows)],
            "volume": [random.randint(100, 100_000) for _ in range(num_rows)],
            "category": [random.choice(_CATEGORIES) for _ in range(num_rows)],
            "note": [
                "".join(random.choices(string.ascii_letters + " ", k=24))
                for _ in range(num_rows)
            ],
        },
        schema=SCHEMA,
    )


def stream_parquet(target_bytes: int, rows_per_chunk: int = 50_000) -> Iterator[bytes]:
    """Yield parquet bytes until at least ``target_bytes`` have been emitted.

    Each iteration writes one row group through ``ParquetWriter``, drains the
    sink, and yields the produced bytes. Memory usage stays bounded by a
    single row group's compressed size, regardless of the requested target.
    """
    sink = _StreamingSink()
    writer = pq.ParquetWriter(sink, SCHEMA, compression="snappy")

    rows_written = 0
    bytes_streamed = 0

    while bytes_streamed < target_bytes:
        chunk = _generate_chunk(rows_per_chunk, start_idx=rows_written)
        writer.write_table(chunk)
        rows_written += rows_per_chunk

        data = sink.drain()
        if data:
            bytes_streamed += len(data)
            yield data

    # Closing the writer appends the parquet footer (a few KB).
    writer.close()
    final = sink.drain()
    if final:
        yield final


@app.route("/")
def index():
    return jsonify(
        {
            "service": "Parquet Mock API",
            "description": "Streams synthetic parquet files of arbitrary size.",
            "endpoints": {
                "GET /parquet?size_mb=N": "Stream a mock parquet file of approximately N MB",
                "GET /health": "Health check",
            },
        }
    )


@app.route("/health")
def health():
    return "OK\n"


@app.route("/parquet")
def parquet_stream():
    try:
        size_mb = float(request.args.get("size_mb", 50))
    except ValueError:
        return jsonify({"error": "size_mb must be a number"}), 400

    if size_mb <= 0 or size_mb > 100_000:
        return jsonify({"error": "size_mb must be between 0 and 100000"}), 400

    target_bytes = int(size_mb * 1024 * 1024)

    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f'attachment; filename="mock_{size_mb:g}mb.parquet"',
        # Approximate hint to clients. The real size depends on snappy
        # compression so it will land within ~5% of this number.
        "X-Approx-Size-Bytes": str(target_bytes),
        "X-Accel-Buffering": "no",
    }

    return Response(
        stream_with_context(stream_parquet(target_bytes)),
        headers=headers,
    )


def main(port):
    app.run(host="0.0.0.0", port=int(port), debug=False, threaded=True)


if __name__ == "__main__":
    main(int(os.environ.get("PORT", 1024)))
