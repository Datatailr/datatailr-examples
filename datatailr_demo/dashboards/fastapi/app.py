# *************************************************************************
#  *
#  * Copyright (c) 2026 - Datatailr Inc.
#  * All Rights Reserved.
#  *
#  * This file is part of Datatailr and subject to the terms and conditions
#  * defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  * of this file, in parts or full, via any medium is strictly prohibited.
#  *************************************************************************

"""
FastAPI framework showcase deployed via datatailr_run_app.py (uvicorn).

Features demonstrated:
- Multi-page navigation with Jinja2 template inheritance
- Interactive Chart.js charts (line, bar, doughnut, scatter, radar)
- KPI cards, data tables, and widget controls
- JSON API endpoints that feed charts dynamically
- Clean, modern layout with a dark sidebar
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

_STATIC_DIR = Path(__file__).parent / 'static'
_TEMPLATES_DIR = Path(__file__).parent / 'templates'

app = FastAPI(title="FastAPI Showcase")

if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

# ---------------------------------------------------------------------------
# Synthetic data (generated once at import time, deterministic seed)
# ---------------------------------------------------------------------------

random.seed(42)

_START = datetime(2024, 1, 1)
_DAYS = 365
_DATES = [(_START + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(_DAYS)]
_DATE_LABELS = [(_START + timedelta(days=i)).strftime("%b %d") for i in range(_DAYS)]

_revenue: list[float] = []
_users: list[int] = []
_sessions: list[int] = []
_acc_r = 0.0
_acc_u = 0
_acc_s = 0
for _ in range(_DAYS):
    _acc_r += random.gauss(20, 50)
    _acc_u += random.randint(5, 30)
    _acc_s += random.randint(10, 80)
    _revenue.append(round(_acc_r, 2))
    _users.append(_acc_u)
    _sessions.append(_acc_s)

_CATEGORIES = ["Electronics", "Clothing", "Groceries", "Books", "Sports"]
_REGIONS = ["North", "South", "East", "West"]

_scatter_data: list[dict] = []
for _ in range(200):
    _scatter_data.append(
        {
            "category": random.choice(_CATEGORIES),
            "region": random.choice(_REGIONS),
            "sales": round(random.expovariate(1 / 500), 2),
            "profit": round(random.gauss(100, 200), 2),
            "units": random.randint(1, 100),
        }
    )

_MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]
_WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_heatmap = [[round(random.gauss(0, 1), 2) for _ in _WEEKDAYS] for _ in _MONTHS]


def _category_totals() -> dict[str, float]:
    totals: dict[str, float] = {c: 0.0 for c in _CATEGORIES}
    for row in _scatter_data:
        totals[row["category"]] += row["sales"]
    return totals


def _region_totals() -> dict[str, float]:
    totals: dict[str, float] = {r: 0.0 for r in _REGIONS}
    for row in _scatter_data:
        totals[row["region"]] += row["sales"]
    return totals


def _rolling_average(values: list[float], window: int) -> list[float]:
    result: list[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start : i + 1]
        result.append(round(sum(chunk) / len(chunk), 2))
    return result


def _daily_change(values: list[float]) -> list[float]:
    return [0.0] + [round(values[i] - values[i - 1], 2) for i in range(1, len(values))]


# ---------------------------------------------------------------------------
# Page routes
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    cat_totals = _category_totals()
    reg_totals = _region_totals()
    return templates.TemplateResponse(
        request,
        "overview.html",
        {
            "request": request,
            "page": "overview",
            "revenue": f"${_revenue[-1]:,.0f}",
            "users": f"{_users[-1]:,}",
            "sessions": f"{_sessions[-1]:,}",
            "avg_order": "$127",
            "cat_labels": list(cat_totals.keys()),
            "cat_values": [round(v, 0) for v in cat_totals.values()],
            "reg_labels": list(reg_totals.keys()),
            "reg_values": [round(v, 0) for v in reg_totals.values()],
        },
    )


@app.get("/time-series", response_class=HTMLResponse)
async def time_series(request: Request):
    return templates.TemplateResponse(
        request,
        "time_series.html",
        {"request": request, "page": "time_series"},
    )


@app.get("/explorer", response_class=HTMLResponse)
async def explorer(request: Request):
    return templates.TemplateResponse(
        request,
        "explorer.html",
        {
            "request": request,
            "page": "explorer",
            "categories": _CATEGORIES,
            "regions": _REGIONS,
        },
    )


@app.get("/distributions", response_class=HTMLResponse)
async def distributions(request: Request):
    return templates.TemplateResponse(
        request,
        "distributions.html",
        {"request": request, "page": "distributions"},
    )


@app.get("/data-table", response_class=HTMLResponse)
async def data_table(request: Request):
    return templates.TemplateResponse(
        request,
        "data_table.html",
        {
            "request": request,
            "page": "data_table",
            "rows": _scatter_data,
            "categories": _CATEGORIES,
            "regions": _REGIONS,
        },
    )


# ---------------------------------------------------------------------------
# JSON API endpoints (feed charts via fetch)
# ---------------------------------------------------------------------------


@app.get("/api/time-series")
async def api_time_series(
    metric: str = Query("revenue"),
    window: int = Query(7),
    transform: str = Query("raw"),
):
    source = {
        "revenue": _revenue,
        "users": [float(u) for u in _users],
        "sessions": [float(s) for s in _sessions],
    }
    values = source.get(metric, _revenue)

    if transform == "rolling":
        values = _rolling_average(values, window)
    elif transform == "diff":
        values = _daily_change(values)

    step = max(1, len(_DATE_LABELS) // 60)
    return {
        "labels": _DATE_LABELS[::step],
        "values": values[::step],
        "metric": metric,
        "transform": transform,
    }


@app.get("/api/scatter")
async def api_scatter(
    color: str = Query("category"),
    x: str = Query("sales"),
    y: str = Query("profit"),
):
    groups: dict[str, list[dict]] = {}
    for row in _scatter_data:
        key = row[color]
        groups.setdefault(key, []).append(
            {"x": row[x], "y": row[y], "r": max(3, row["units"] / 10)}
        )
    return {"groups": groups}


@app.get("/api/distribution")
async def api_distribution(
    var: str = Query("sales"),
    bins: int = Query(20),
    group: str = Query("none"),
):
    values = [row[var] for row in _scatter_data]
    lo, hi = min(values), max(values)
    if lo == hi:
        hi = lo + 1
    bin_width = (hi - lo) / bins
    edges = [round(lo + i * bin_width, 2) for i in range(bins + 1)]
    labels = [f"{edges[i]:.0f}" for i in range(bins)]

    if group == "none":
        counts = [0] * bins
        for v in values:
            idx = min(int((v - lo) / bin_width), bins - 1)
            counts[idx] += 1
        return {"labels": labels, "datasets": [{"label": var.title(), "data": counts}]}

    group_keys = _CATEGORIES if group == "category" else _REGIONS
    datasets = []
    for gk in group_keys:
        counts = [0] * bins
        for row in _scatter_data:
            if row[group] == gk:
                v = row[var]
                idx = min(int((v - lo) / bin_width), bins - 1)
                counts[idx] += 1
        datasets.append({"label": gk, "data": counts})

    return {"labels": labels, "datasets": datasets}


@app.get("/api/heatmap")
async def api_heatmap():
    return {"months": _MONTHS, "weekdays": _WEEKDAYS, "data": _heatmap}


@app.get("/api/box")
async def api_box(
    var: str = Query("sales"),
    group: str = Query("none"),
):
    def _box_stats(vals: list[float]) -> dict:
        if not vals:
            return {"min": 0, "q1": 0, "median": 0, "q3": 0, "max": 0}
        s = sorted(vals)
        n = len(s)
        return {
            "min": round(s[0], 2),
            "q1": round(s[n // 4], 2),
            "median": round(s[n // 2], 2),
            "q3": round(s[3 * n // 4], 2),
            "max": round(s[-1], 2),
        }

    if group == "none":
        vals = [row[var] for row in _scatter_data]
        return {"labels": [var.title()], "stats": [_box_stats(vals)]}

    group_keys = _CATEGORIES if group == "category" else _REGIONS
    labels = []
    stats = []
    for gk in group_keys:
        vals = [row[var] for row in _scatter_data if row[group] == gk]
        labels.append(gk)
        stats.append(_box_stats(vals))

    return {"labels": labels, "stats": stats}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
