"""Stream an HTTP response directly into Datatailr blob storage.

The :func:`stream_to_blob` generator wires together two streaming endpoints:

  * a streaming HTTP GET request (``requests`` with ``stream=True``)
  * the ``dt blob put blob://<path>`` CLI, started as a subprocess with its
    stdin attached to a pipe

Bytes from the HTTP response are forwarded chunk-by-chunk to the subprocess
stdin. Memory usage stays bounded by the chunk size; no temporary file is
written and the process never holds the whole payload at once.

The generator yields progress events that callers can use to drive a UI:

* ``{"event": "started", "blob_path": str, "api_url": str, "estimated_bytes": int}``
* ``{"event": "progress", "bytes_streamed": int, "elapsed": float, "throughput_mbps": float, "estimated_bytes": int}``
* ``{"event": "complete", "bytes_streamed": int, "elapsed": float, "throughput_mbps": float, "blob_path": str}``
* ``{"event": "error", "message": str}``
"""

from __future__ import annotations

import shutil
import subprocess
import time
from typing import Iterator

import requests


_DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def _early_exit_event(cmd: list[str], proc: subprocess.Popen) -> dict:
    """Build an `error` event when `dt blob put` exits before we finish writing."""
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)
    err = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
    return {
        "event": "error",
        "message": (
            f"`{' '.join(cmd)}` exited early with code {proc.returncode}: "
            f"{err.strip() or '(no stderr)'}"
        ),
    }


def stream_to_blob(
    api_url: str,
    blob_path: str,
    *,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    request_timeout: float = 30.0,
    put_timeout: float = 600.0,
) -> Iterator[dict]:
    """Stream the response of ``api_url`` directly into ``blob://blob_path``.

    Args:
        api_url: HTTP URL to GET. The response body is streamed.
        blob_path: Destination path inside blob storage,
            e.g. ``"my_bucket/file.parquet"`` (no ``blob://`` prefix).
        chunk_size: Number of bytes per read/write iteration.
        request_timeout: Connection/read timeout for the HTTP request.
        put_timeout: Maximum seconds to wait for ``dt blob put`` to finish
            after stdin has been closed.

    Yields:
        Progress event dicts. See module docstring for the schemas.
    """
    if not shutil.which("dt"):
        yield {
            "event": "error",
            "message": (
                "The `dt` CLI is not on PATH. This function requires the "
                "Datatailr CLI; run `datatailr setup-cli` locally or deploy "
                "the dashboard onto the platform."
            ),
        }
        return

    cmd = ["dt", "blob", "put", f"blob://{blob_path.lstrip('/')}"]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    bytes_streamed = 0
    start = time.monotonic()

    try:
        with requests.get(api_url, stream=True, timeout=request_timeout) as resp:
            resp.raise_for_status()

            estimated = int(
                resp.headers.get("X-Approx-Size-Bytes")
                or resp.headers.get("Content-Length")
                or 0
            )

            yield {
                "event": "started",
                "blob_path": blob_path,
                "api_url": api_url,
                "estimated_bytes": estimated,
            }

            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue

                # If `dt blob put` died early (e.g. permissions), stop now
                # with a useful error message rather than a BrokenPipeError.
                if proc.poll() is not None:
                    yield _early_exit_event(cmd, proc)
                    return

                try:
                    proc.stdin.write(chunk)
                except BrokenPipeError:
                    yield _early_exit_event(cmd, proc)
                    return
                bytes_streamed += len(chunk)
                elapsed = time.monotonic() - start
                yield {
                    "event": "progress",
                    "bytes_streamed": bytes_streamed,
                    "elapsed": elapsed,
                    "estimated_bytes": estimated,
                    "throughput_mbps": (
                        (bytes_streamed / 1e6) / elapsed if elapsed > 0 else 0.0
                    ),
                }

        # Closing stdin signals EOF to `dt blob put`, which then commits the blob.
        proc.stdin.close()
        return_code = proc.wait(timeout=put_timeout)
        elapsed = time.monotonic() - start

        if return_code != 0:
            err = (
                proc.stderr.read().decode(errors="replace")
                if proc.stderr
                else ""
            )
            yield {
                "event": "error",
                "message": (
                    f"`{' '.join(cmd)}` exited with code {return_code}: "
                    f"{err.strip()}"
                ),
            }
            return

        yield {
            "event": "complete",
            "bytes_streamed": bytes_streamed,
            "elapsed": elapsed,
            "throughput_mbps": (
                (bytes_streamed / 1e6) / elapsed if elapsed > 0 else 0.0
            ),
            "blob_path": blob_path,
        }
    except Exception as exc:
        if proc.poll() is None:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        yield {"event": "error", "message": f"{type(exc).__name__}: {exc}"}
