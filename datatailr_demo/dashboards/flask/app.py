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
Flask framework showcase deployed via datatailr_run_app.py (gunicorn).

Features demonstrated:
- Multi-page navigation with Jinja2 template inheritance
- Interactive Chart.js charts (line, bar, doughnut, scatter, radar)
- KPI cards, data tables, and widget controls
- JSON API endpoints that feed charts dynamically
- Clean, modern layout with a dark sidebar
"""

from __future__ import annotations
import os
import random
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path

from flask import Flask, jsonify, render_template, request

_STATIC_DIR = Path(__file__).parent / 'static'
_TEMPLATES_DIR = Path(__file__).parent / 'templates'

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")
_job_type = 'job' if _job_type != 'workstation' else 'workstation'

_PREFIX = f'/{_job_type}/{_env}/{_job}' if _env and _job else ""

if _job_type == 'workstation':
    _PREFIX += '/ide/proxy/5000/'

app = Flask(
    __name__,
    template_folder=_TEMPLATES_DIR,
    static_folder=_STATIC_DIR,
    static_url_path="/static",
)


@app.context_processor
def _inject_prefix():
    return {"prefix": _PREFIX}


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


@app.route("/")
def index():
    cat_totals = _category_totals()
    reg_totals = _region_totals()
    return render_template(
        "overview.html",
        page="overview",
        revenue=f"${_revenue[-1]:,.0f}",
        users=f"{_users[-1]:,}",
        sessions=f"{_sessions[-1]:,}",
        avg_order="$127",
        cat_labels=list(cat_totals.keys()),
        cat_values=[round(v, 0) for v in cat_totals.values()],
        reg_labels=list(reg_totals.keys()),
        reg_values=[round(v, 0) for v in reg_totals.values()],
    )


@app.route("/time-series")
def time_series():
    return render_template("time_series.html", page="time_series")


@app.route("/explorer")
def explorer():
    return render_template(
        "explorer.html",
        page="explorer",
        categories=_CATEGORIES,
        regions=_REGIONS,
    )


@app.route("/distributions")
def distributions():
    return render_template("distributions.html", page="distributions")


@app.route("/data-table")
def data_table():
    return render_template(
        "data_table.html",
        page="data_table",
        rows=_scatter_data,
        categories=_CATEGORIES,
        regions=_REGIONS,
    )


# ---------------------------------------------------------------------------
# Blob Storage Browser Page and API
# ---------------------------------------------------------------------------

try:
    from datatailr import Blob
    blob_storage = Blob()
except ImportError:
    blob_storage = None


def _normalize_blob_ls_prefix(raw: str | None) -> str:
    """Prefix passed to Blob.ls: root is ``/``; nested dirs use a trailing slash, e.g. ``/bucket/``."""
    if raw is None or str(raw).strip() == "":
        return "/"
    p = str(raw).strip()
    if p == "/":
        return "/"
    if not p.startswith("/"):
        p = "/" + p
    if not p.endswith("/"):
        p = p + "/"
    return p


def _dirname_label(list_prefix: str) -> str:
    if list_prefix in ("/", ""):
        return "blob:/"
    base = list_prefix.rstrip("/")
    return base.rsplit("/", 1)[-1] if base else "blob:/"


def _child_dir_prefix(list_prefix: str, segment: str) -> str:
    if list_prefix in ("/", ""):
        return f"/{segment}/"
    return f"{list_prefix.rstrip('/')}/{segment}/"


def _full_blob_path(list_prefix: str, entry_name: str) -> str:
    """Resolve ls() entry name to an absolute blob path (leading ``/``, no trailing ``/`` except root)."""
    e = entry_name.strip()
    if e.startswith("/"):
        out = e.rstrip("/") if e.endswith("/") and e != "/" else e
        return out
    base = list_prefix.rstrip("/")
    if not base:
        return "/" + e.lstrip("/")
    e_clean = e.lstrip("/")
    # API often returns the full key without a leading slash (e.g. ``job/env/app/file``). Joining
    # that onto ``/job/env/app/`` would duplicate the prefix; treat it as already absolute.
    base_tail = base.lstrip("/")
    if e_clean == base_tail or e_clean.startswith(base_tail + "/"):
        return "/" + e_clean
    # Some Blob.ls() implementations return names relative to the bucket root:
    # list_prefix: /bucket/a/b/, entry: a/b/file.parquet (bucket omitted).
    # Reattach the bucket segment to avoid duplicating prefix segments.
    if "/" in base_tail:
        bucket, current_rel = base_tail.split("/", 1)
        if current_rel and (e_clean == current_rel or e_clean.startswith(current_rel + "/")):
            return f"/{bucket}/{e_clean}"
    return f"{base}/{e_clean}"


def _relative_under_prefix(list_prefix: str, full_path: str) -> str:
    """Path under ``list_prefix`` (no leading slash); empty if ``full_path`` is the prefix itself."""
    fp = (full_path or "").strip()
    if not fp:
        return ""
    lp = (list_prefix or "/").rstrip("/")
    if not lp:
        return fp.lstrip("/")
    if not fp.startswith("/"):
        fp = "/" + fp
    fp = fp.rstrip("/")
    if fp == lp:
        return ""
    pfx = lp + "/"
    if fp.startswith(pfx):
        return fp[len(pfx) :]
    return ""


def _parse_ls_entry(list_prefix: str, entry) -> tuple[str, dict | None]:
    if isinstance(entry, dict):
        name = entry.get("name")
        if not name:
            return "", None
        return _full_blob_path(list_prefix, str(name)), entry
    return _full_blob_path(list_prefix, str(entry)), None


def _entry_implies_directory(is_nested: bool, meta: dict | None) -> bool:
    """True if this ls entry is a directory (prefix) rather than a blob object."""
    if is_nested:
        return True
    if meta is not None and "is_file" in meta:
        return not bool(meta["is_file"])
    return False


def _blob_entry_meta(meta: dict | None, full_path: str) -> tuple[int | None, object | None]:
    if meta:
        return meta.get("size", 0), meta.get("last_modified")
    stat_fn = getattr(blob_storage, "stat", None)
    if stat_fn:
        try:
            st = stat_fn(full_path)
            return st.get("size", 0), st.get("last_modified")
        except Exception:
            pass
    return 0, None


def _collect_immediate_children(list_prefix: str, entries: list) -> list[dict]:
    """Group flat ls() results into direct children of list_prefix (non-recursive)."""
    parsed: list[tuple[str, bool, str, dict | None]] = []
    for entry in entries:
        full, meta = _parse_ls_entry(list_prefix, entry)
        if not full:
            continue
        rel = _relative_under_prefix(list_prefix, full)
        if not rel:
            continue
        parts = [p for p in rel.split("/") if p]
        if not parts:
            continue
        head = parts[0]
        is_nested = len(parts) > 1
        parsed.append((head, is_nested, full, meta))

    dir_heads = {h for h, is_nested, _, meta in parsed if _entry_implies_directory(is_nested, meta)}
    children: list[dict] = []

    for h in sorted(dir_heads, key=str.casefold):
        children.append(
            {
                "name": h,
                "type": "dir",
                "is_file": False,
                "path": _child_dir_prefix(list_prefix, h),
                "children": None,
                "size": None,
                "last_modified": None,
            }
        )

    seen_file: set[str] = set()
    for head, is_nested, full, meta in parsed:
        if _entry_implies_directory(is_nested, meta):
            continue
        if head in dir_heads:
            continue
        if head in seen_file:
            continue
        seen_file.add(head)
        size, last_modified = _blob_entry_meta(meta, full)
        children.append(
            {
                "name": head,
                "type": "file",
                "is_file": True,
                "path": full.rstrip("/"),
                "size": size,
                "last_modified": last_modified,
            }
        )

    children.sort(
        key=lambda x: (0 if x.get("is_file") is False else 1, x["name"].casefold())
    )
    return children


def _list_blob_dir_at_prefix(prefix: str | None = None) -> dict:
    list_prefix = _normalize_blob_ls_prefix(prefix)
    display_path = "/" if list_prefix == "/" else list_prefix
    if not blob_storage:
        return {
            "name": _dirname_label(list_prefix),
            "type": "dir",
            "is_file": False,
            "path": display_path,
            "children": [],
            "size": None,
            "last_modified": None,
        }
    try:
        blobs = blob_storage.ls(list_prefix)
    except Exception as e:
        return {
            "name": _dirname_label(list_prefix),
            "type": "dir",
            "path": display_path,
            "children": [
                {
                    "name": str(e),
                    "type": "file",
                    "is_file": True,
                    "path": "",
                    "size": 0,
                    "last_modified": None,
                }
            ],
            "size": None,
            "last_modified": None,
        }

    if not isinstance(blobs, list):
        blobs = list(blobs) if blobs else []

    return {
        "name": _dirname_label(list_prefix),
        "type": "dir",
        "is_file": False,
        "path": display_path,
        "children": _collect_immediate_children(list_prefix, blobs),
        "size": None,
        "last_modified": None,
    }


@app.route("/blob-browser")
def blob_browser():
    return render_template("blob_browser.html", page="blob_browser")


@app.route("/api/blob-tree")
def api_blob_tree():
    prefix = request.args.get("prefix", "/")
    return jsonify(_list_blob_dir_at_prefix(prefix))


# ---------------------------------------------------------------------------
# JSON API endpoints (feed charts via fetch)
# ---------------------------------------------------------------------------


@app.route("/api/time-series")
def api_time_series():
    metric = request.args.get("metric", "revenue")
    window = int(request.args.get("window", "7"))
    transform = request.args.get("transform", "raw")

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
    return jsonify(
        labels=_DATE_LABELS[::step],
        values=values[::step],
        metric=metric,
        transform=transform,
    )


@app.route("/api/scatter")
def api_scatter():
    color_by = request.args.get("color", "category")
    x_axis = request.args.get("x", "sales")
    y_axis = request.args.get("y", "profit")

    groups: dict[str, list[dict]] = {}
    for row in _scatter_data:
        key = row[color_by]
        groups.setdefault(key, []).append(
            {"x": row[x_axis], "y": row[y_axis], "r": max(3, row["units"] / 10)}
        )

    return jsonify(groups=groups)


@app.route("/api/distribution")
def api_distribution():
    variable = request.args.get("var", "sales")
    bins = int(request.args.get("bins", "20"))
    group_by = request.args.get("group", "none")

    values = [row[variable] for row in _scatter_data]
    lo, hi = min(values), max(values)
    if lo == hi:
        hi = lo + 1
    bin_width = (hi - lo) / bins
    edges = [round(lo + i * bin_width, 2) for i in range(bins + 1)]
    labels = [f"{edges[i]:.0f}" for i in range(bins)]

    if group_by == "none":
        counts = [0] * bins
        for v in values:
            idx = min(int((v - lo) / bin_width), bins - 1)
            counts[idx] += 1
        return jsonify(
            labels=labels, datasets=[{"label": variable.title(), "data": counts}]
        )

    group_keys = _CATEGORIES if group_by == "category" else _REGIONS
    datasets = []
    for gk in group_keys:
        counts = [0] * bins
        for row in _scatter_data:
            if row[group_by] == gk:
                v = row[variable]
                idx = min(int((v - lo) / bin_width), bins - 1)
                counts[idx] += 1
        datasets.append({"label": gk, "data": counts})

    return jsonify(labels=labels, datasets=datasets)


@app.route("/api/heatmap")
def api_heatmap():
    return jsonify(months=_MONTHS, weekdays=_WEEKDAYS, data=_heatmap)


@app.route("/api/box")
def api_box():
    variable = request.args.get("var", "sales")
    group_by = request.args.get("group", "none")

    def _box_stats(vals: list[float]) -> dict:
        if not vals:
            return {"min": 0, "q1": 0, "median": 0, "q3": 0, "max": 0}
        s = sorted(vals)
        n = len(s)
        q1 = s[n // 4]
        median = s[n // 2]
        q3 = s[3 * n // 4]
        return {
            "min": round(s[0], 2),
            "q1": round(q1, 2),
            "median": round(median, 2),
            "q3": round(q3, 2),
            "max": round(s[-1], 2),
        }

    if group_by == "none":
        vals = [row[variable] for row in _scatter_data]
        return jsonify(labels=[variable.title()], stats=[_box_stats(vals)])

    group_keys = _CATEGORIES if group_by == "category" else _REGIONS
    labels = []
    stats = []
    for gk in group_keys:
        vals = [row[variable] for row in _scatter_data if row[group_by] == gk]
        labels.append(gk)
        stats.append(_box_stats(vals))

    return jsonify(labels=labels, stats=stats)


if __name__ == "__main__":
    app.run(debug=True)
