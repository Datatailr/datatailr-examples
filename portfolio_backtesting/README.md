# Portfolio Backtesting Example

This example demonstrates multiple Datatailr components working together:

- A **Flask dashboard app** as the center of the user experience.
- A **Datatailr workflow** that runs strategy backtests.
- **Blob storage** for persistent run artifacts and run history.
- `vectorbt` + `yfinance` as the backtesting/data stack.

## Architecture

1. User opens the Flask dashboard.
2. User submits backtest parameters (symbol, date range, SMA settings).
3. Dashboard submits a Datatailr workflow run.
4. Workflow fetches market data, runs the strategy, computes metrics.
5. Workflow writes artifacts to Blob under `portfolio_backtesting/runs/<run_id>/`.
6. Dashboard reads run index and artifacts for history + analysis pages.

## Project Layout

```text
portfolio_backtesting/
├── dashboard/
│   ├── app.py
│   ├── static/
│   │   ├── css/app.css
│   │   └── js/charts.js
│   └── templates/
│       ├── base.html
│       ├── index.html
│       ├── runs.html
│       └── run_detail.html
├── workflows/
│   ├── __init__.py
│   └── tasks.py
├── deploy.py
├── README.md
└── requirements.txt
```

## Workflow Details

`workflows/tasks.py` defines:

- `fetch_price_data`: pulls prices from `yfinance`.
- `run_sma_strategy`: runs SMA crossover with `vectorbt`.
- `compute_metrics`: builds summary metadata and KPI output.
- `persist_artifacts`: writes JSON/CSV artifacts + updates run index.

Artifacts are persisted at:

- `portfolio_backtesting/runs/index.json`
- `portfolio_backtesting/runs/<run_id>/params.json`
- `portfolio_backtesting/runs/<run_id>/metrics.json`
- `portfolio_backtesting/runs/<run_id>/summary.json`
- `portfolio_backtesting/runs/<run_id>/equity_curve.csv`
- `portfolio_backtesting/runs/<run_id>/drawdown_curve.csv`
- `portfolio_backtesting/runs/<run_id>/trades.csv`

## Dashboard Pages

- **New Backtest** (`/`): submit parameters and launch a workflow run.
- **Run History** (`/runs`): inspect known run records from Blob index.
- **Run Analysis** (`/runs/<run_id>`): view metrics, equity/drawdown charts, and trades.

## Deployment

Run commands from `portfolio_backtesting/`.

Deploy everything:

```bash
python deploy.py
```

Deploy only workflow definition:

```bash
python -c "from deploy import deploy_workflow; deploy_workflow()"
```

Deploy only dashboard app:

```bash
python -c "from deploy import deploy_app; deploy_app()"
```

## Usage Flow

1. Open the deployed app from Datatailr App Launcher.
2. Submit a new backtest on the main page.
3. Open Run History and select a run.
4. Review KPIs and charts in the analysis page.

## Local Notes

- This example expects Datatailr SDK/runtime for workflow submission and Blob access.
- For local iterative UI work, run the Flask app directly:

```bash
python -m dashboard.app
```

Blob-backed pages will require Datatailr credentials/runtime to load real data.

## Troubleshooting

- **No data returned**: verify ticker/date range and supported interval.
- **Submission error**: verify the workflow definition is deployed and SDK auth is valid.
- **Missing run artifacts**: check workflow logs and Blob path permissions.
