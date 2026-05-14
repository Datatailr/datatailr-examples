# Blob Streaming Demo

This example showcases Datatailr's blob storage by streaming a large parquet
file from a (mock) external API directly into a blob, without ever
buffering the full payload in memory or on disk.

## What's inside

```
blob_streaming_demo/
├── api_service/
│   └── service.py        # Flask service: streams parquet of any size
├── streamer/
│   └── stream_to_blob.py # Pipes HTTP response into `dt blob put`
├── dashboard/
│   └── app.py            # Streamlit UI with live progress + chart
├── deploy.py
├── requirements.txt
└── README.md
```

### 1. Mock parquet API (`api_service/service.py`)

A Flask service that exposes:

- `GET /parquet?size_mb=N` - streams a synthetic parquet file of
  approximately `N` megabytes back to the caller.
- `GET /health` - returns `OK`.

The service generates parquet row groups on the fly via `pyarrow` and writes
them through a custom file-like sink that flushes each row group to the
HTTP response as soon as it's produced. Memory usage stays bounded by a
single row group, so the service can serve arbitrarily large files.

### 2. Streamer function (`streamer/stream_to_blob.py`)

`stream_to_blob(api_url, blob_path)` is a generator that:

1. Opens a streaming HTTP `GET` to `api_url`.
2. Spawns `dt blob put blob://<blob_path>` as a subprocess with stdin
   attached to a pipe.
3. Forwards response chunks straight into the subprocess stdin.
4. Yields progress events that callers can render in real time.

No temp file is created and the process never holds the whole payload at
once. The equivalent shell command is:

```bash
curl -s "http://parquet-mock-api/parquet?size_mb=200" \
  | dt blob put blob://parquet-streaming-demo/file.parquet
```

### 3. Live dashboard (`dashboard/app.py`)

A Streamlit app that lets the user pick a target file size and a destination
blob path, then drives `stream_to_blob` and renders progress as bytes flow:
bytes streamed, throughput, elapsed time, ETA and a live throughput chart.

## Deployment

From this directory, run:

```bash
# deploy both the service and the dashboard
python deploy.py

# or one at a time
python deploy.py service
python deploy.py app
```

This registers two jobs on Datatailr:

| Component | Type    | Internal hostname   |
|-----------|---------|---------------------|
| Parquet Mock API   | Service | `http://parquet-mock-api` |
| Blob Streaming Demo | App (Streamlit) | (opens in the apps section) |

## Trying it

1. Open **Blob Streaming Demo** from the Datatailr apps section.
2. Pick a target file size, bucket and filename, then click **Start streaming**.
3. Watch the metrics and the throughput chart update in real time.
4. Confirm the blob landed in storage:

   ```bash
   dt blob ls parquet-streaming-demo/
   ```

## Local development

You can run the service and the dashboard locally too:

```bash
# terminal 1: service
PYTHONPATH=. python blob_streaming_demo/api_service/service.py

# terminal 2: dashboard
PYTHONPATH=. streamlit run blob_streaming_demo/dashboard/app.py
```

The dashboard auto-detects local mode and points at `http://localhost:1024`.

The streamer uses the `dt` CLI, so a working Datatailr CLI is required for
the upload step to actually write to blob storage. Without it, the streamer
emits an `error` event explaining how to set it up.
