"""Flask cockpit for the Gas Curve Backtest demo.

Pages:
- `/`                       overview + recent parent runs + trigger form
- `/runs/<run_id>`          per-run drilldown (parent + child + heatmap)

JSON APIs (consumed by the page-level Chart.js dashboards):
- `/api/runs`                      list of parent runs
- `/api/runs/<run_id>`             aggregated run summary
- `/api/runs/<run_id>/cells`       per-cell metric rows (filterable)
- `/api/runs/<run_id>/heatmap`     pivoted Sharpe / PnL grid for one (regime, tenor)

All run discovery and result retrieval go through `Workflow.runs()`,
`Workflow.run_details()`, and `Workflow.result()` — no direct blob
storage scraping in this app.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from gas_curve_backtest.flask_app import workflow_io

_STATIC_DIR = Path(__file__).parent / "static"
_TEMPLATES_DIR = Path(__file__).parent / "templates"

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")
_job_type = "job" if _job_type and _job_type != "workstation" else _job_type or ""

_PREFIX = f"/{_job_type}/{_env}/{_job}" if _env and _job and _job_type else ""
if _job_type == "workstation":
    _PREFIX += "/ide/proxy/5000/"

app = Flask(
    __name__,
    template_folder=str(_TEMPLATES_DIR),
    static_folder=str(_STATIC_DIR),
    static_url_path="/static",
)


@app.context_processor
def _inject_prefix():
    return {"prefix": _PREFIX}


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


@app.template_filter("hms")
def _format_hms(value: Any) -> str:
    """Render a timestamp as HH:MM:SS (no fractional seconds, no tz)."""
    parsed = _parse_iso(value)
    return parsed.strftime("%H:%M:%S") if parsed else "—"


@app.template_filter("ymd")
def _format_ymd(value: Any) -> str:
    """Render a timestamp as YYYY-MM-DD."""
    parsed = _parse_iso(value)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def _round_or_none(value: Any, ndigits: int = 3) -> float | None:
    try:
        return round(float(value), ndigits)
    except Exception:
        return None


def _pretty_run(run: dict) -> dict:
    return {**run, "label": f"#{run.get('run_id')}"}


def _build_gantt(tasks: list[dict]) -> dict:
    """Compute % offsets and % widths so the template can render bars in CSS.

    Bars are anchored to the earliest task `start_time` and span up to
    the latest known boundary (`end_time`, falling back to `now`). Tasks
    without a start are rendered as a thin marker at the right edge so
    the user still sees that they exist.
    """
    parsed: list[tuple[dict, datetime | None, datetime | None]] = []
    starts: list[datetime] = []
    ends: list[datetime] = []
    for t in tasks:
        s = _parse_iso(t.get("start_time"))
        e = _parse_iso(t.get("end_time"))
        if s is not None:
            starts.append(s)
        if e is not None:
            ends.append(e)
        parsed.append((t, s, e))

    if not starts:
        return {"rows": [], "t_min": None, "t_max": None, "duration": 0.0}

    t_min = min(starts)
    t_max = max(ends) if ends else datetime.now(t_min.tzinfo)
    if t_max <= t_min:
        t_max = t_min
    span = max((t_max - t_min).total_seconds(), 1.0)

    rows: list[dict] = []
    for t, s, e in parsed:
        state = (t.get("state") or "unknown").lower()
        if s is None:
            rows.append(
                {
                    "name": t.get("name", ""),
                    "state": state,
                    "offset_pct": 100.0,
                    "width_pct": 0.0,
                    "pending": True,
                    "start_label": "—",
                    "end_label": "—",
                    "duration_s": None,
                }
            )
            continue
        end = e or t_max
        offset_pct = max(0.0, (s - t_min).total_seconds() / span * 100.0)
        width_pct = max(
            0.6,
            (end - s).total_seconds() / span * 100.0,
        )
        if offset_pct + width_pct > 100.0:
            width_pct = max(0.6, 100.0 - offset_pct)
        rows.append(
            {
                "name": t.get("name", ""),
                "state": state,
                "offset_pct": round(offset_pct, 2),
                "width_pct": round(width_pct, 2),
                "pending": False,
                "start_label": s.strftime("%H:%M:%S"),
                "end_label": e.strftime("%H:%M:%S") if e else "running",
                "duration_s": round((end - s).total_seconds(), 2),
            }
        )

    rows.sort(key=lambda r: (r["pending"], r["offset_pct"], r["name"]))

    return {
        "rows": rows,
        "t_min": t_min.strftime("%H:%M:%S"),
        "t_max": t_max.strftime("%H:%M:%S"),
        "duration": round(span, 1),
    }


@app.route("/")
def index():
    runs = [_pretty_run(r) for r in workflow_io.list_parent_runs(20)]
    states = {}
    for r in runs:
        s = (r.get("state") or "unknown").lower()
        states[s] = states.get(s, 0) + 1
    return render_template(
        "overview.html",
        page="overview",
        runs=runs,
        states=states,
        parent_workflow=workflow_io.PARENT_WORKFLOW_NAME,
    )


@app.route("/runs/<int:run_id>")
def run_detail(run_id: int):
    summary = workflow_io.collect_run_summary(run_id)
    if not summary.get("found"):
        return render_template(
            "run_detail.html",
            page="runs",
            run_id=run_id,
            summary=None,
            cells=[],
            regimes=[],
            tenors=[],
        ), 404

    cells = workflow_io.get_cell_rows(run_id)
    regimes = sorted({int(c["regime_id"]) for c in cells if "regime_id" in c})
    tenors = sorted({int(c["tenor"]) for c in cells if "tenor" in c})
    best_per_regime: list[dict] = []
    child_result = (summary.get("child") or {}).get("result") or {}
    if isinstance(child_result, dict):
        for entry in child_result.get("best_per_regime", []) or []:
            best = entry.get("best") or {}
            best_per_regime.append(
                {
                    "regime_id": best.get("regime_id"),
                    "tenor": best.get("tenor"),
                    "sig_threshold": _round_or_none(best.get("sig_threshold"), 4),
                    "asym_pivot": _round_or_none(best.get("asym_pivot"), 4),
                    "sharpe": _round_or_none(best.get("sharpe"), 3),
                    "pnl": _round_or_none(best.get("pnl"), 2),
                    "max_drawdown": _round_or_none(best.get("max_drawdown"), 2),
                    "hit_rate": _round_or_none(best.get("hit_rate"), 3),
                }
            )
    child_tasks = (summary.get("child") or {}).get("tasks") or []
    gantt = _build_gantt(child_tasks)
    return render_template(
        "run_detail.html",
        page="runs",
        run_id=run_id,
        summary=summary,
        cells=cells,
        regimes=regimes,
        tenors=tenors,
        best_per_regime=best_per_regime,
        gantt=gantt,
    )


@app.route("/api/runs")
def api_runs():
    return jsonify(runs=workflow_io.list_parent_runs(50))


@app.route("/api/runs/<int:run_id>")
def api_run(run_id: int):
    summary = workflow_io.collect_run_summary(run_id)
    if not summary.get("found"):
        return jsonify(error=f"run {run_id} not found"), 404
    return jsonify(summary)


@app.route("/api/runs/<int:run_id>/cells")
def api_cells(run_id: int):
    rows = workflow_io.get_cell_rows(run_id)
    regime_q = request.args.get("regime")
    tenor_q = request.args.get("tenor")
    if regime_q is not None and regime_q != "":
        try:
            regime = int(regime_q)
            rows = [r for r in rows if int(r.get("regime_id", -1)) == regime]
        except ValueError:
            pass
    if tenor_q is not None and tenor_q != "":
        try:
            tenor = int(tenor_q)
            rows = [r for r in rows if int(r.get("tenor", -1)) == tenor]
        except ValueError:
            pass
    return jsonify(rows=rows, count=len(rows))


@app.route("/api/runs/<int:run_id>/heatmap")
def api_heatmap(run_id: int):
    rows = workflow_io.get_cell_rows(run_id)
    metric = request.args.get("metric", "sharpe")
    try:
        regime = int(request.args["regime"])
        tenor = int(request.args["tenor"])
    except (KeyError, ValueError):
        return jsonify(error="regime and tenor query params are required"), 400

    sub = [
        r
        for r in rows
        if int(r.get("regime_id", -1)) == regime and int(r.get("tenor", -1)) == tenor
    ]
    sig_axis = sorted({float(r["sig_threshold"]) for r in sub})
    pivot_axis = sorted({float(r["asym_pivot"]) for r in sub})
    grid: list[list[float | None]] = [
        [None for _ in sig_axis] for _ in pivot_axis
    ]
    for r in sub:
        try:
            i = pivot_axis.index(float(r["asym_pivot"]))
            j = sig_axis.index(float(r["sig_threshold"]))
            grid[i][j] = _round_or_none(r.get(metric), 4)
        except (ValueError, KeyError):
            continue
    best = None
    if sub:
        sub_with_metric = [r for r in sub if r.get(metric) is not None]
        if sub_with_metric:
            chooser = min if metric == "max_drawdown" else max
            best_row = chooser(sub_with_metric, key=lambda r: float(r[metric]))
            best = {
                "sig_threshold": _round_or_none(best_row.get("sig_threshold"), 4),
                "asym_pivot": _round_or_none(best_row.get("asym_pivot"), 4),
                "sharpe": _round_or_none(best_row.get("sharpe"), 3),
                "pnl": _round_or_none(best_row.get("pnl"), 2),
                "max_drawdown": _round_or_none(best_row.get("max_drawdown"), 2),
                "hit_rate": _round_or_none(best_row.get("hit_rate"), 3),
            }
    return jsonify(
        regime=regime,
        tenor=tenor,
        metric=metric,
        sig_thresholds=[round(s, 4) for s in sig_axis],
        asym_pivots=[round(p, 4) for p in pivot_axis],
        grid=grid,
        best=best,
    )


@app.route("/api/launch", methods=["POST"])
def api_launch():
    payload = request.get_json(silent=True) or {}
    params: dict[str, Any] = {}
    for key in (
        "n_days",
        "n_tenors",
        "n_regimes",
        "grid_signal_steps",
        "grid_pivot_steps",
        "bootstrap_samples",
    ):
        if key in payload and payload[key] not in (None, ""):
            try:
                params[key] = int(payload[key])
            except (TypeError, ValueError):
                continue
    try:
        out = workflow_io.trigger_parent_run(params)
    except Exception as e:
        return jsonify(error=str(e)), 500
    return jsonify(out)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
