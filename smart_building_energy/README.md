# Smart Building Energy Intelligence on Datatailr

This example demonstrates a complete, multi-job analytics system on Datatailr:

1. **Data collection** via a long-running telemetry ingestor service  
2. **Data processing** via a scheduled workflow and task DAG  
3. **Data display** via a Flask dashboard app  
4. **Analytics exposure** via a FastAPI REST service  
5. **Storage maintenance** via an hourly compaction workflow

## Components

- `sensor_ingestor/service.py`  
  Simulates sensor telemetry for buildings/zones and writes raw partitioned Parquet.
- `data_pipelines/tasks.py` + `data_pipelines/deploy.py`  
  Cleans and transforms raw telemetry into curated KPIs and alerts every 15 minutes.
- `analytics_api/service.py`  
  Exposes `/kpi/latest`, `/kpi/timeseries`, `/alerts`, and `/anomalies/top`.
- `dashboard/app.py`  
  Displays KPI cards, trend chart, and open alerts by consuming the analytics API.
- `compaction_workflow/tasks.py` + `compaction_workflow/deploy.py`  
  Compacts small Parquet files in `raw`, `curated`, and `alerts`.

## Lake layout

```text
smart_building_energy/
  raw/dt=YYYY-MM-DD/hour=HH/building_id=.../part-*.parquet
  curated/dt=YYYY-MM-DD/hour=HH/building_id=.../part-*.parquet
  alerts/dt=YYYY-MM-DD/hour=HH/building_id=.../part-*.parquet
```

## Deploy

From this folder:

```bash
python deploy.py all
```

Or deploy individual components:

```bash
python deploy.py collector
python deploy.py api
python deploy.py dashboard
python deploy.py workflow
python deploy.py compaction
```

## Environment variables

Optional knobs:

- `ENERGY_BLOB_PREFIX` (default: `smart_building_energy`)
- `INGEST_FLUSH_INTERVAL_SEC` (default: `30`)
- `INGEST_MAX_BUFFER_ROWS` (default: `5000`)
- `NUM_BUILDINGS` (default: `3`)
- `ZONES_PER_BUILDING` (default: `12`)
- `INGEST_EVENTS_PER_SEC` (default: `10`)
- `PIPELINE_LOOKBACK_HOURS` (default: `2`)
- `ALERT_CO2_THRESHOLD` (default: `1200`)
- `ALERT_COMFORT_MIN` (default: `60`)
- `ANOMALY_ZSCORE_THRESHOLD` (default: `3.0`)
- `API_CACHE_TTL_SEC` (default: `30`)
- `COMPACTION_LAST_N_HOURS` (default: `24`)
- `COMPACTION_MIN_FILES` (default: `8`)
- `COMPACTION_DRY_RUN` (default: `0`)

## Notes

- In local workstation mode, the dashboard defaults to `http://localhost:8091` for the API.
- In Datatailr mode, the dashboard defaults to `http://building-analytics-api`.
- The system is intentionally synthetic so it runs without external credentials or data sources.

