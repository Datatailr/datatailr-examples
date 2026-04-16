# Credit Spread Prediction (Datatailr)

This example demonstrates a complete Datatailr project for forecasting credit spreads using public FRED data.

## Modules

1. **Data collection** (`data_ingestion/`)
   - Pulls FRED series via REST.
   - Stores raw payload snapshots and cleaned Parquet in Blob storage.
2. **Research and calibration notebook** (`notebooks/credit_spread_research.ipynb`)
   - Compares level and delta targets at 1d/5d/20d horizons.
   - Exports `notebooks/calibration_config.json`.
3. **Parallel model training** (`workflows/training_workflow.py`)
   - Runs one job per `(target, horizon, label_kind, model_family, params)`.
   - Aggregates leaderboard and writes model artifacts.
4. **Evaluation dashboard** (`dashboard/app.py`)
   - Run selector, leaderboard, evaluation metrics, and prediction charting.
   - Includes high-volatility vs low-volatility error slice.

## Core FRED Series

- `BAMLC0A0CM` - ICE BofA US Corporate OAS
- `BAMLC0A4CBBB` - ICE BofA BBB OAS
- `BAMLH0A0HYM2` - ICE BofA High Yield OAS
- `BAA10Y` - Moody's Baa minus 10Y Treasury
- `DGS10`, `DGS2`, `T10Y2Y`, `VIXCLS`, `UNRATE`, `CPIAUCSL`, `FEDFUNDS`

## Week-1 Vertical Slice (Implemented)

- Ingestion workflow for the full starter series list.
- Feature matrix generation and baseline labels.
- Training capped to a configurable number of calibration jobs (`max_jobs`).
- Evaluation report and Streamlit dashboard for run comparison.

## Deploy

```bash
python credit_spread_prediction/deploy.py
```

Before running ingestion/training, create Datatailr secret `fred_api_key` in Secrets Manager
(or set `FRED_API_KEY` as an environment variable).

This deploys:
- Streamlit app: **Credit Spread Examination Dashboard**
- Scheduled ingestion workflow
- Parallel calibration/training workflow

To deploy only the dashboard or only workflows, use helper functions in `deploy.py`.

