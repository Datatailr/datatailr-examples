import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from flask import Flask, redirect, render_template, request, url_for

from portfolio_backtesting.workflows.tasks import get_run_artifacts, list_runs, submit_backtest_workflow

__FILE_DIR = Path(__file__).parent

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")
_job_type = 'job' if _job_type != 'workstation' else 'workstation'

_PREFIX = f'/{_job_type}/{_env}/{_job}' if _env and _job else ""

if _job_type == 'workstation':
    _PREFIX += '/ide/proxy/5000/'

app = Flask(
    __name__,
    template_folder=__FILE_DIR / 'templates',
    static_folder=__FILE_DIR / 'static',
    static_url_path="/static",
)


@app.context_processor
def _inject_prefix():
    return {"prefix": _PREFIX, "prefixed_url": _prefixed_url}


def _prefixed_url(path: str) -> str:
    if not _PREFIX:
        return path
    if path.startswith(_PREFIX):
        return path
    return f"{_PREFIX}{path}"


def _default_dates() -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=365 * 2)
    return start.isoformat(), end.isoformat()


def _parse_float(value: str, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


@app.route("/")
def index():
    start, end = _default_dates()
    return render_template(
        "index.html",
        defaults={
            "symbol": "AAPL",
            "start": start,
            "end": end,
            "fast_window": 10,
            "slow_window": 50,
            "fee": 0.001,
            "init_cash": 10000,
            "interval": "1d",
        },
    )


@app.route("/launch", methods=["POST"])
def launch_backtest():
    params = {
        "symbol": request.form.get("symbol", "AAPL"),
        "start": request.form.get("start"),
        "end": request.form.get("end"),
        "fast_window": _parse_int(request.form.get("fast_window", "10"), 10),
        "slow_window": _parse_int(request.form.get("slow_window", "50"), 50),
        "fee": _parse_float(request.form.get("fee", "0.001"), 0.001),
        "init_cash": _parse_float(request.form.get("init_cash", "10000"), 10000),
        "interval": request.form.get("interval", "1d"),
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        result = submit_backtest_workflow(params)
        return redirect(_prefixed_url(url_for("run_detail", run_id=result["run_id"])))
    except Exception as exc:
        return render_template(
            "index.html",
            defaults=params,
            error=f"Backtest submission failed: {exc}",
        )


@app.route("/runs")
def runs():
    rows = list_runs(limit=200)
    return render_template("runs.html", runs=rows)


@app.route("/runs/<run_id>")
def run_detail(run_id: str):
    artifacts = get_run_artifacts(run_id)
    equity = artifacts.get("equity_curve", [])
    drawdown = artifacts.get("drawdown_curve", [])
    trades = artifacts.get("trades", [])
    metrics = artifacts.get("metrics", {})
    summary = artifacts.get("summary", {})
    params = artifacts.get("params", {})

    chart_payload: dict[str, Any] = {
        "equity_labels": [row.get("timestamp") for row in equity],
        "equity_values": [row.get("equity") for row in equity],
        "drawdown_labels": [row.get("timestamp") for row in drawdown],
        "drawdown_values": [row.get("drawdown") for row in drawdown],
    }

    return render_template(
        "run_detail.html",
        run_id=run_id,
        params=params,
        metrics=metrics,
        summary=summary,
        trades=trades[:200],
        trade_count=len(trades),
        chart_payload=json.dumps(chart_payload),
    )



if __name__ == "__main__":
    app.run(debug=True)
