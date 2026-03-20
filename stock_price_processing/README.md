# Stock price processing

<!-- Brief overview of what this project does and how the pieces fit together. -->

## Overview

<!-- High-level purpose: synthetic exchange feed, real-time dashboard, CSP analytics, and planned batch/blob pipeline. -->

## Architecture

<!-- Diagram or narrative: services, apps, internal URLs on Datatailr, data directions (SSE, HTTP). -->

## Components

### Price server (`price_server/`)

FastAPI synthetic feed: REST tickers/quotes, SSE `/stream`. **OpenAPI 3** is automatic:

| URL | Purpose |
|-----|---------|
| **`/openapi.json`** | Machine-readable schema (import into Postman, codegen, gateways) |
| **`/docs`** | **Swagger UI** — try REST calls in the browser |
| **`/redoc`** | **ReDoc** — alternate readable reference |

Default local base: `http://localhost:8080` (see `server.py`).

### Price processor (`price_processor/`)

FastAPI CSP service: `/analytics`, `/stats`, `/topology`, SSE `/stream`. Same OpenAPI surface:

- **`http://localhost:8081/openapi.json`**, **`/docs`**, **`/redoc`**

`GET /stream` on each service is **left out of the OpenAPI document** (`include_in_schema=False`) so Swagger UI can load; SSE is described in each app’s top-level **description** and in code docstrings. Use **`/redoc`** if you prefer a static layout, or **`curl …/openapi.json`** to inspect the JSON.

**Production:** disable interactive docs if you want (`FastAPI(docs_url=None, redoc_url=None)`), or protect `/docs` behind auth; keep **`/openapi.json`** for internal tooling if useful.

### Data collector (`data_collector/`)

Long-running **Service** that reads:

- **Price processor** SSE (`/stream`) → Parquet dataset **`analytics/`**
- **Price server** SSE (`/stream`) → Parquet dataset **`market_events/`**

Every **`COLLECTOR_FLUSH_INTERVAL_SEC`** (default **120**), buffered rows are written under a **Hive-style** path (DuckDB-friendly):

```text
{blob_prefix}/analytics/dt=YYYY-MM-DD/hour=HH/part-<nanos>.parquet
{blob_prefix}/market_events/dt=YYYY-MM-DD/hour=HH/part-<nanos>.parquet
```

On **Datatailr**, files go to **blob** via `Blob().put` under `COLLECTOR_BLOB_PREFIX` (default `stock_price_lake`). Locally, set **`COLLECTOR_LOCAL_DIR`** (e.g. `./_lake`) to skip blob and write files on disk.

| Env | Meaning |
|-----|---------|
| `COLLECTOR_BLOB_PREFIX` | Blob key prefix (default `stock_price_lake`) |
| `COLLECTOR_LOCAL_DIR` | If set, write Parquet here instead of blob |
| `COLLECTOR_FLUSH_INTERVAL_SEC` | Flush period (default `120`) |
| `COLLECTOR_MAX_BUFFER_ROWS` | Force flush if combined buffers exceed this |
| `COLLECTOR_ANALYTICS` | `0` to disable processor SSE |
| `COLLECTOR_MARKET_EVENTS` | `0` to disable price-server SSE |
| `PRICE_SERVER_URL` / `PRICE_PROCESSOR_URL` | Upstream bases |

Minimal HTTP: **`GET /__health_check__.html`**, **`GET /status`** (buffer sizes, config). Default listen port **8082** when run standalone; the platform passes **`PORT`** via `main(port)`.

**DuckDB** (after downloading or mounting the lake):

```sql
SELECT * FROM read_parquet('analytics/**/*.parquet', hive_partitioning = true);
SELECT * FROM read_parquet('market_events/**/*.parquet', hive_partitioning = true);
```

Use paths that match where you synced blobs (e.g. `dt` / `hour` columns appear from directory names).

### Lake query (`lake_query/`)

Read Parquet from **blob** using the same mechanism as the collector write path: **`Blob().ls`** and **`Blob().get_blob`** (i.e. `dt blob ls` / `dt blob get`). This avoids assuming a local filesystem or direct object-store URLs.
Directory listing is always done as `blob.ls('/path/to/dir/')` (absolute path, trailing slash).

**Python API** (in a job or notebook with `datatailr` + deps installed):

```python
import os
from datatailr import Blob
from stock_price_processing.lake_query import (
    iter_dataset_parquet_keys,
    load_parquet_keys_arrow,
    query_lake_sql,
)

blob = Blob()
base = "/" + os.environ.get("COLLECTOR_BLOB_PREFIX", "stock_price_lake").strip("/")  # /stock_price_lake
# One dataset per query: analytics (processor) vs market_events (trades/quotes) have different schemas.
keys = list(
    iter_dataset_parquet_keys(
        blob,
        base,
        "analytics",
        dt="2026-03-20",   # optional
        hour="15",         # optional
        max_files=10,
    )
)
table = load_parquet_keys_arrow(blob, keys, max_rows_per_file=1000)

# DuckDB over downloaded files (hive_partitioning keeps dt= / hour= columns)
out = query_lake_sql(
    blob,
    base,
    "SELECT ticker, COUNT(*) AS n FROM lake GROUP BY ticker ORDER BY n DESC LIMIT 20",
    dataset="analytics",
    dt="2026-03-20",  # optional
    hour="15",        # optional
    max_files=32,
)
print(out.to_pandas())
```

Do **not** query both datasets together: **analytics** and **market_events** have different schemas. Query one dataset at a time.

**CLI** (same env vars as collector for `COLLECTOR_BLOB_PREFIX`):

```bash
python -m stock_price_processing.lake_query ls analytics --dt 2026-03-20 --hour 15 --max 200
python -m stock_price_processing.lake_query head analytics --dt 2026-03-20 --files 2 --rows-per-file 20
python -m stock_price_processing.lake_query sql -d market_events --dt 2026-03-20 "SELECT * FROM lake LIMIT 5"
```

`sql` builds a DuckDB view **`lake`** over matching `.parquet` files in `/prefix/dataset/` (and optional `dt`/`hour` partitions), up to `--max-files`. Set `LAKE_QUERY_JSON=1` to print results as JSON lines-friendly records.

**Jupyter notebooks**

1. **Kernel** — Use an environment where `datatailr`, `pyarrow`, `pandas`, and `duckdb` are installed (same as `stock_price_processing/requirements.txt`). The repo root must be on **`PYTHONPATH`** so `import stock_price_processing.lake_query` works, e.g. at the top of the notebook:

   ```python
   import sys
   sys.path.insert(0, "/path/to/datatailr-examples")  # adjust
   ```

   On a **Datatailr** job / workstation notebook that already has your project code, you can skip this if imports resolve.

2. **Run SQL and get a DataFrame** (view `lake` is created for you; increase `max_files` if you need more Parquet objects scanned):

   ```python
   import os
   from datatailr import Blob
   from stock_price_processing.lake_query import query_lake_sql

   blob = Blob()
   base = "/" + os.environ.get("COLLECTOR_BLOB_PREFIX", "stock_price_lake").strip("/")
   df = query_lake_sql(
       blob,
       base,
       "SELECT * FROM lake WHERE dt >= '2025-01-01' LIMIT 500",
       dataset="analytics",  # or "market_events"
       max_files=64,
       hive_partitioning=True,
   ).to_pandas()

   df.head()
   ```

3. **Inspect a few files in Arrow/Pandas** without DuckDB:

   ```python
   from stock_price_processing.lake_query import iter_dataset_parquet_keys, load_parquet_keys_arrow

   keys = list(iter_dataset_parquet_keys(blob, base, "analytics", max_files=5))
   table = load_parquet_keys_arrow(blob, keys, max_rows_per_file=10_000)
   table.to_pandas()
   ```

4. **Reusable DuckDB connection** — If you prefer many ad-hoc cells against the same downloaded snapshot, call `query_lake_sql` once per “batch” of files, or copy the temp-dir pattern from `lake_query/reader.py` and keep a single `duckdb` connection open in the notebook (advanced).

### Exchange Monitor dashboard (`price_server_dashboard/`)

Flask + Perspective: proxies the price server SSE at `/stream`, main UI at `/`, **ticker admin** at `/tickers`, and the **price processor cockpit** at `/processor` (interactive CSP DAG via vis-network, live Chart.js series, SSE from `/api/processor/stream`, stats from `/api/processor/stats`). The dashboard reaches the processor at `http://price-processor` on Datatailr or `http://localhost:8081` locally — override with env `PRICE_PROCESSOR_URL` if your service DNS differs.

### Deployment (`deploy.py`)

<!-- Datatailr Service / App definitions, normalized hostnames, deploy order notes. -->

## Data flow

<!-- End-to-end: price_server → (SSE) → price_processing → analytics; dashboard → proxy → streams. -->

## Event formats

<!-- Reference: trade vs quote payloads (fields), analytics snapshot shape from price_processing. -->

## Configuration

<!-- Environment: `PRICE_SERVER_URL`, workspace vs platform, ports for local dev. -->

## Local development

<!-- How to run price_server, price_processing, and dashboard locally; dependencies. -->

## Datatailr deployment

<!-- `python deploy.py`, job names, internal service URLs (`price-server`, `price-processing`, etc.). -->

---

## Roadmap (from `TODO.md`)

### Existing dashboard

<!-- User/email display, trade chart x-axis, Perspective filtering notes, add/remove ticker controls. -->

### Analytics dashboard

<!-- Surface CSP-processed data; service health and system info in real time. -->

### Data collector and aggregator server

Implemented as **`data_collector/`** (Parquet + blob / local Hive paths). Optional aggregation batch job TBD.

### Data aggregation batch job

<!-- Read Parquet from blob; aggregate; write back; DuckDB-friendly indexing. -->

---

## Further reading

<!-- Links to Datatailr skills, CSP, Perspective docs as needed. -->
