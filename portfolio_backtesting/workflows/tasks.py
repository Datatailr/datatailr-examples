import csv
import io
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import vectorbt as vbt
import yfinance as yf
from datatailr import Blob, Resources, task, workflow

RUNS_ROOT = "portfolio_backtesting/runs"
RUN_INDEX_PATH = f"{RUNS_ROOT}/index.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        arr = np.asarray(value)
        if arr.size == 0:
            return default
        return float(arr.reshape(-1)[0])
    except Exception:
        return default


def _parse_date(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    return pd.to_datetime(value, utc=True)


def _normalize_params(run_params: dict[str, Any]) -> dict[str, Any]:
    symbol = str(run_params.get("symbol", "AAPL")).upper()
    fast_window = int(run_params.get("fast_window", 10))
    slow_window = int(run_params.get("slow_window", 50))
    fee = float(run_params.get("fee", 0.001))
    init_cash = float(run_params.get("init_cash", 10_000))
    interval = str(run_params.get("interval", "1d"))
    start = run_params.get("start")
    end = run_params.get("end")
    run_id = str(run_params.get("run_id") or uuid.uuid4().hex[:12])

    if fast_window <= 1 or slow_window <= 2:
        raise ValueError("SMA windows must be > 1 and > 2 respectively.")
    if fast_window >= slow_window:
        raise ValueError("fast_window must be smaller than slow_window.")
    if fee < 0:
        raise ValueError("fee must be non-negative.")
    if init_cash <= 0:
        raise ValueError("init_cash must be positive.")

    return {
        "run_id": run_id,
        "symbol": symbol,
        "start": start,
        "end": end,
        "interval": interval,
        "fast_window": fast_window,
        "slow_window": slow_window,
        "fee": fee,
        "init_cash": init_cash,
        "submitted_at": run_params.get("submitted_at") or _utc_now_iso(),
    }


def _read_run_index(blob: Blob) -> list[dict[str, Any]]:
    if not blob.exists(RUN_INDEX_PATH):
        return []
    data = blob.get(RUN_INDEX_PATH)
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    parsed = json.loads(data)
    return parsed if isinstance(parsed, list) else []


def _write_run_index(blob: Blob, rows: list[dict[str, Any]]) -> None:
    blob.put(RUN_INDEX_PATH, json.dumps(rows, indent=2, sort_keys=True))


def _upsert_run_index(run_entry: dict[str, Any]) -> None:
    blob = Blob()
    rows = _read_run_index(blob)
    run_id = run_entry["run_id"]
    remaining = [row for row in rows if row.get("run_id") != run_id]
    remaining.append(run_entry)
    remaining.sort(key=lambda row: row.get("submitted_at", ""), reverse=True)
    _write_run_index(blob, remaining)


def register_run_submission(run_params: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_params(run_params)
    entry = {
        "run_id": normalized["run_id"],
        "symbol": normalized["symbol"],
        "status": "submitted",
        "submitted_at": normalized["submitted_at"],
        "completed_at": None,
        "metrics_path": None,
        "params_path": f"{RUNS_ROOT}/{normalized['run_id']}/params.json",
    }
    _upsert_run_index(entry)
    return normalized


def list_runs(limit: int = 100) -> list[dict[str, Any]]:
    blob = Blob()
    rows = _read_run_index(blob)
    return rows[:limit]


def get_run_artifacts(run_id: str) -> dict[str, Any]:
    blob = Blob()
    base = f"{RUNS_ROOT}/{run_id}"
    payload: dict[str, Any] = {"run_id": run_id}

    def _read_json(path: str, fallback: Any) -> Any:
        if not blob.exists(path):
            return fallback
        data = blob.get(path)
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    payload["params"] = _read_json(f"{base}/params.json", {})
    payload["metrics"] = _read_json(f"{base}/metrics.json", {})
    payload["summary"] = _read_json(f"{base}/summary.json", {})

    equity_rows: list[dict[str, Any]] = []
    equity_path = f"{base}/equity_curve.csv"
    if blob.exists(equity_path):
        csv_data = blob.get(equity_path)
        if isinstance(csv_data, bytes):
            csv_data = csv_data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(csv_data))
        equity_rows = list(reader)
    payload["equity_curve"] = equity_rows

    drawdown_rows: list[dict[str, Any]] = []
    drawdown_path = f"{base}/drawdown_curve.csv"
    if blob.exists(drawdown_path):
        csv_data = blob.get(drawdown_path)
        if isinstance(csv_data, bytes):
            csv_data = csv_data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(csv_data))
        drawdown_rows = list(reader)
    payload["drawdown_curve"] = drawdown_rows

    trade_rows: list[dict[str, Any]] = []
    trades_path = f"{base}/trades.csv"
    if blob.exists(trades_path):
        csv_data = blob.get(trades_path)
        if isinstance(csv_data, bytes):
            csv_data = csv_data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(csv_data))
        trade_rows = list(reader)
    payload["trades"] = trade_rows
    return payload


@task()
def fetch_price_data(params: dict[str, Any]) -> dict[str, Any]:
    start = _parse_date(params.get("start"))
    end = _parse_date(params.get("end"))
    ticker = params["symbol"]
    interval = params["interval"]

    data = yf.download(
        ticker,
        start=start.tz_convert(None) if start is not None else None,
        end=end.tz_convert(None) if end is not None else None,
        interval=interval,
        progress=False,
        auto_adjust=True,
    )

    if data.empty or "Close" not in data.columns:
        raise ValueError(f"No close price data found for symbol '{ticker}'.")

    frame = data.reset_index().rename(columns={"Date": "timestamp"})
    if "timestamp" not in frame.columns:
        frame = frame.rename(columns={frame.columns[0]: "timestamp"})
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return {"rows": frame[["timestamp", "Close"]].to_dict(orient="records")}


@task(memory="2g", cpu=1)
def run_sma_strategy(
    market_data: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    frame = pd.DataFrame(market_data["rows"])
    if frame.empty:
        raise ValueError("No market data available for strategy run.")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.set_index("timestamp").sort_index()
    price = frame["Close"].astype(float)

    fast_ma = vbt.MA.run(price, window=params["fast_window"])
    slow_ma = vbt.MA.run(price, window=params["slow_window"])
    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)

    pf = vbt.Portfolio.from_signals(
        close=price,
        entries=entries,
        exits=exits,
        fees=params["fee"],
        init_cash=params["init_cash"],
        freq="1D",
    )

    equity = pf.value()
    equity_df = pd.DataFrame(
        {
            "timestamp": equity.index.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "equity": equity.astype(float).values,
        }
    )

    drawdown = equity / equity.cummax() - 1
    drawdown_df = pd.DataFrame(
        {
            "timestamp": drawdown.index.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "drawdown": drawdown.astype(float).values,
        }
    )

    trades_df = pf.trades.records_readable.copy()
    for col in trades_df.columns:
        trades_df[col] = trades_df[col].astype(str)

    metrics = {
        "total_return_pct": round(_to_float(pf.total_return()) * 100, 4),
        "max_drawdown_pct": round(_to_float(pf.max_drawdown()) * 100, 4),
        "sharpe_ratio": round(_to_float(pf.sharpe_ratio()), 4),
        "trade_count": int(_to_float(pf.trades.count(), default=0)),
        "win_rate_pct": round(_to_float(pf.trades.win_rate()) * 100, 4),
        "start_value": round(_to_float(pf.value().iloc[0]), 4),
        "end_value": round(_to_float(pf.value().iloc[-1]), 4),
    }

    return {
        "equity_curve": equity_df.to_dict(orient="records"),
        "drawdown_curve": drawdown_df.to_dict(orient="records"),
        "trades": trades_df.to_dict(orient="records"),
        "metrics": metrics,
    }


@task()
def compute_metrics(
    strategy_output: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(strategy_output)
    payload["summary"] = {
        "run_id": params["run_id"],
        "symbol": params["symbol"],
        "status": "completed",
        "submitted_at": params["submitted_at"],
        "completed_at": _utc_now_iso(),
        "strategy": {
            "type": "sma_crossover",
            "fast_window": params["fast_window"],
            "slow_window": params["slow_window"],
            "fee": params["fee"],
            "init_cash": params["init_cash"],
        },
        "data": {
            "interval": params["interval"],
            "start": params.get("start"),
            "end": params.get("end"),
        },
        "metrics": strategy_output["metrics"],
    }
    payload["params"] = params
    return payload


@task()
def persist_artifacts(result_payload: dict[str, Any]) -> dict[str, Any]:
    blob = Blob()
    params = result_payload["params"]
    run_id = params["run_id"]
    base = f"{RUNS_ROOT}/{run_id}"

    blob.put(f"{base}/params.json", json.dumps(params, indent=2, sort_keys=True))
    blob.put(
        f"{base}/metrics.json",
        json.dumps(result_payload["metrics"], indent=2, sort_keys=True),
    )
    blob.put(
        f"{base}/summary.json",
        json.dumps(result_payload["summary"], indent=2, sort_keys=True),
    )

    equity_df = pd.DataFrame(result_payload["equity_curve"])
    drawdown_df = pd.DataFrame(result_payload["drawdown_curve"])
    trades_df = pd.DataFrame(result_payload["trades"])
    blob.put(f"{base}/equity_curve.csv", equity_df.to_csv(index=False))
    blob.put(f"{base}/drawdown_curve.csv", drawdown_df.to_csv(index=False))
    blob.put(f"{base}/trades.csv", trades_df.to_csv(index=False))

    run_entry = {
        "run_id": run_id,
        "symbol": params["symbol"],
        "status": "completed",
        "submitted_at": params["submitted_at"],
        "completed_at": result_payload["summary"]["completed_at"],
        "metrics_path": f"{base}/metrics.json",
        "params_path": f"{base}/params.json",
    }
    _upsert_run_index(run_entry)
    return run_entry


def create_backtest_workflow():
    @workflow(
        name="Portfolio Backtesting Workflow",
        python_requirements=["vectorbt", "pandas", "numpy", "yfinance"],
        resources=Resources(memory="3g", cpu=1),
    )
    def portfolio_backtest_workflow(run_params: dict[str, Any] | None = None):
        params = _normalize_params(run_params or {})
        market_data = fetch_price_data(params)
        strategy_output = run_sma_strategy(market_data, params)
        computed = compute_metrics(strategy_output, params)
        persist_artifacts(computed)

    return portfolio_backtest_workflow


def submit_backtest_workflow(run_params: dict[str, Any]) -> dict[str, Any]:
    params = register_run_submission(run_params)
    wf = create_backtest_workflow()
    wf(run_params=params)
    return {"run_id": params["run_id"], "status": "submitted"}
