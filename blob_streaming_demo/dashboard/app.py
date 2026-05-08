"""Streamlit dashboard showcasing direct streaming into Datatailr blob storage.

The page lets the user pick a target file size and a destination blob path,
calls the mock parquet API, and pipes the streaming response straight into
``dt blob put`` via the :func:`stream_to_blob` helper. Progress, throughput
and a live throughput chart are rendered as bytes flow through.
"""

from __future__ import annotations

import os
import time
from typing import List, Tuple

import pandas as pd
import requests
import streamlit as st

from blob_streaming_demo.streamer.stream_to_blob import stream_to_blob


# Hostname derived from the Service `name` parameter in deploy.py.
# "Parquet Mock API" -> lowercased, non-alphanum -> hyphens -> "parquet-mock-api".
SERVICE_HOSTNAME = "parquet-mock-api"


def _service_url() -> str:
    """Return the base URL of the mock parquet service.

    On Datatailr the service is reachable at ``http://<hostname>``. When the
    dashboard is run locally (no platform job context) we fall back to
    ``localhost`` so users can ``streamlit run`` the file alongside a local
    copy of the service.
    """
    job_type = os.getenv("DATATAILR_JOB_TYPE", "")
    if job_type in ("workstation", "workspace", ""):
        return os.getenv("PARQUET_API_URL", "http://localhost:1024")
    return os.getenv("PARQUET_API_URL", f"http://{SERVICE_HOSTNAME}")


def _humanize_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:,.2f} PB"


def _ping(url: str) -> Tuple[bool, str]:
    try:
        r = requests.get(f"{url}/health", timeout=2)
        ok = r.status_code == 200 and r.text.strip() == "OK"
        return ok, r.text.strip() or f"HTTP {r.status_code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def main() -> None:
    st.set_page_config(
        page_title="Blob Streaming Demo",
        layout="wide",
    )

    st.title("Streaming Large Parquet Files into Datatailr Blob Storage")
    st.caption(
        "Pipe a streaming HTTP response from a mock external API directly "
        "into `dt blob put`, with no temporary file and no full-payload "
        "buffer in memory."
    )

    api_url = _service_url()
    is_healthy, health_text = _ping(api_url)

    # ---------------------------------------------------------------- status
    status_cols = st.columns([2, 1, 1])
    with status_cols[0]:
        st.markdown("**Mock parquet API**")
        st.code(api_url, language="text")
    with status_cols[1]:
        if is_healthy:
            st.success("Service: healthy")
        else:
            st.error(f"Service: {health_text}")
    with status_cols[2]:
        env = os.getenv("DATATAILR_JOB_ENVIRONMENT") or "local"
        st.metric("Environment", env)

    with st.expander("How this works"):
        st.markdown(
            """
            1. **Mock API service** (`parquet-mock-api`) generates parquet
               row groups on the fly and streams them back as the response
               body. Memory usage on the server stays bounded by a single
               row group, regardless of the requested size.
            2. **Streamer function** opens a streaming HTTP connection,
               spawns `dt blob put blob://<path>` as a subprocess, and pipes
               response bytes straight into the subprocess stdin.
            3. **This dashboard** iterates over progress events emitted by
               the streamer and renders them as bytes flow.

            The equivalent shell command is:

            ```bash
            curl -s "http://parquet-mock-api/parquet?size_mb=100" \\
              | dt blob put blob://parquet-streaming-demo/file.parquet
            ```
            """
        )

    st.divider()

    # -------------------------------------------------------- configuration
    st.subheader("1. Configure the stream")

    cfg_cols = st.columns(3)
    with cfg_cols[0]:
        size_mb = st.number_input(
            "Target file size (MB)",
            min_value=1,
            max_value=10_000,
            value=200,
            step=10,
            help="Approximate, since parquet uses snappy compression.",
        )
    with cfg_cols[1]:
        bucket = st.text_input(
            "Blob bucket",
            value="parquet-streaming-demo",
        )
    with cfg_cols[2]:
        filename = st.text_input(
            "Blob filename",
            value=f"mock_{int(time.time())}.parquet",
        )

    chunk_size_kb = st.slider(
        "Network chunk size (KB)",
        min_value=64,
        max_value=4096,
        value=1024,
        step=64,
        help=(
            "Bytes per read/write iteration. Larger values trade UI update "
            "frequency for raw throughput."
        ),
    )

    blob_path = f"{bucket.strip().strip('/')}/{filename.strip()}"
    request_url = f"{api_url}/parquet?size_mb={size_mb:g}"

    summary_cols = st.columns(2)
    with summary_cols[0]:
        st.markdown(f"**Source:** `GET {request_url}`")
    with summary_cols[1]:
        st.markdown(f"**Destination:** `blob://{blob_path}`")

    st.divider()

    # ---------------------------------------------------------------- run
    st.subheader("2. Run the stream")

    if "history" not in st.session_state:
        st.session_state["history"] = []

    if st.button("Start streaming", type="primary"):
        if not is_healthy:
            st.error("Mock API is not reachable; cannot start a stream.")
        elif "/" not in blob_path or blob_path.startswith("/"):
            st.error("Blob path must look like `bucket/filename`.")
        else:
            _run_stream(
                request_url,
                blob_path,
                chunk_size_bytes=int(chunk_size_kb) * 1024,
            )

    # ---------------------------------------------------------------- history
    st.divider()
    st.subheader("3. Recent streams")

    history: List[dict] = st.session_state["history"]
    if history:
        st.dataframe(
            pd.DataFrame(history),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No streams yet. Click **Start streaming** above.")


def _run_stream(api_url: str, blob_path: str, *, chunk_size_bytes: int) -> None:
    """Drive a single stream and update the page in real time."""
    status_box = st.empty()
    progress_bar = st.progress(0.0)

    metric_cols = st.columns(4)
    bytes_metric = metric_cols[0].empty()
    throughput_metric = metric_cols[1].empty()
    elapsed_metric = metric_cols[2].empty()
    eta_metric = metric_cols[3].empty()

    chart_placeholder = st.empty()

    timeline: List[dict] = []
    last_render = 0.0

    for event in stream_to_blob(api_url, blob_path, chunk_size=chunk_size_bytes):
        kind = event["event"]

        if kind == "started":
            est = event["estimated_bytes"]
            status_box.info(
                f"Streaming `{api_url}` -> `blob://{blob_path}`"
                + (f" (target ~{_humanize_bytes(est)})" if est else "")
            )
            bytes_metric.metric("Bytes streamed", "0 B")
            throughput_metric.metric("Throughput", "0.0 MB/s")
            elapsed_metric.metric("Elapsed", "0.0 s")
            eta_metric.metric("ETA", "-")

        elif kind == "progress":
            now = time.monotonic()
            # Throttle UI re-renders to ~10 Hz to keep Streamlit responsive.
            if now - last_render < 0.1:
                continue
            last_render = now

            est = event["estimated_bytes"]
            done = event["bytes_streamed"]
            progress_fraction = min(done / est, 1.0) if est else 0.0
            progress_bar.progress(progress_fraction)

            bytes_metric.metric("Bytes streamed", _humanize_bytes(done))
            throughput_metric.metric(
                "Throughput", f"{event['throughput_mbps']:.1f} MB/s"
            )
            elapsed_metric.metric("Elapsed", f"{event['elapsed']:.1f} s")

            if est > 0 and event["throughput_mbps"] > 0:
                remaining_mb = max(est - done, 0) / 1e6
                eta_metric.metric(
                    "ETA", f"{remaining_mb / event['throughput_mbps']:.1f} s"
                )
            else:
                eta_metric.metric("ETA", "-")

            timeline.append(
                {
                    "elapsed_s": round(event["elapsed"], 2),
                    "throughput_mbps": round(event["throughput_mbps"], 2),
                    "mb_streamed": round(done / 1e6, 2),
                }
            )
            if len(timeline) >= 2:
                chart_placeholder.line_chart(
                    pd.DataFrame(timeline).set_index("elapsed_s"),
                    use_container_width=True,
                )

        elif kind == "complete":
            progress_bar.progress(1.0)
            bytes_metric.metric(
                "Bytes streamed", _humanize_bytes(event["bytes_streamed"])
            )
            throughput_metric.metric(
                "Avg throughput", f"{event['throughput_mbps']:.1f} MB/s"
            )
            elapsed_metric.metric("Total time", f"{event['elapsed']:.1f} s")
            eta_metric.metric("ETA", "0.0 s")

            status_box.success(
                f"Streamed {_humanize_bytes(event['bytes_streamed'])} "
                f"to `blob://{blob_path}` in {event['elapsed']:.1f} s "
                f"({event['throughput_mbps']:.1f} MB/s)."
            )

            st.session_state["history"].insert(
                0,
                {
                    "blob_path": blob_path,
                    "size": _humanize_bytes(event["bytes_streamed"]),
                    "duration_s": round(event["elapsed"], 2),
                    "avg_throughput_mbps": round(event["throughput_mbps"], 2),
                },
            )

        elif kind == "error":
            status_box.error(f"Error: {event['message']}")
            return


if __name__ == "__main__":
    main()
