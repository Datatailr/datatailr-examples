from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request

try:
    from stock_price_processing.lake_query.reader import default_blob_prefix, query_lake_sql
except ModuleNotFoundError:
    # Allow direct execution: `python stock_price_processing/lake_query/dashboard.py`
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from stock_price_processing.lake_query.reader import default_blob_prefix, query_lake_sql

_PKG_DIR = Path(__file__).parent
_TEMPLATES_DIR = str(_PKG_DIR / "templates")

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")

if _job_type == "workstation":
    _PREFIX = f"/workstation/{_env}/{_job}/ide/proxy/5060/"
elif _env and _job:
    _PREFIX = f"/job/{_env}/{_job}/"
else:
    _PREFIX = "/"

if _job_type in ("workstation", ""):
    DATA_COLLECTOR_URL = os.environ.get("DATA_COLLECTOR_URL", "http://localhost:8082")
else:
    DATA_COLLECTOR_URL = os.environ.get("DATA_COLLECTOR_URL", "http://stock-data-collector")

app = Flask(__name__, template_folder=_TEMPLATES_DIR)


def _app_path(suffix: str = "") -> str:
    base = _PREFIX.rstrip("/")
    suf = suffix.strip("/")
    if not suf:
        return f"{base}/" if base else "/"
    return f"{base}/{suf}" if base else f"/{suf}"


def _normalize_blob_ls_prefix(raw: str | None) -> str:
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


def _child_dir_prefix(list_prefix: str, segment: str) -> str:
    if list_prefix in ("/", ""):
        return f"/{segment}/"
    return f"{list_prefix.rstrip('/')}/{segment}/"


def _full_blob_path(list_prefix: str, entry_name: str) -> str:
    e = entry_name.strip()
    if e.startswith("/"):
        return e.rstrip("/") if e.endswith("/") and e != "/" else e
    base = list_prefix.rstrip("/")
    if not base:
        return "/" + e.lstrip("/")
    e_clean = e.lstrip("/")
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
    if is_nested:
        return True
    if meta is not None and "is_file" in meta:
        return not bool(meta["is_file"])
    return False


def _collect_immediate_children(list_prefix: str, entries: list) -> list[dict]:
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
        if _entry_implies_directory(is_nested, meta) or head in dir_heads or head in seen_file:
            continue
        seen_file.add(head)
        size = meta.get("size", 0) if meta else 0
        last_modified = meta.get("last_modified") if meta else None
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

    children.sort(key=lambda x: (0 if x.get("is_file") is False else 1, x["name"].casefold()))
    return children


def _list_blob_dir_at_prefix(prefix: str | None = None) -> dict:
    list_prefix = _normalize_blob_ls_prefix(prefix)
    try:
        from datatailr import Blob

        blob_storage = Blob()
    except Exception:
        return {"name": list_prefix, "type": "dir", "is_file": False, "path": list_prefix, "children": []}

    try:
        blobs = blob_storage.ls(list_prefix)
    except Exception as e:
        return {
            "name": list_prefix,
            "type": "dir",
            "is_file": False,
            "path": list_prefix,
            "children": [{"name": str(e), "type": "file", "is_file": True, "path": "", "size": 0}],
        }
    if not isinstance(blobs, list):
        blobs = list(blobs) if blobs else []
    return {
        "name": "blob:/" if list_prefix == "/" else list_prefix.rstrip("/").split("/")[-1],
        "type": "dir",
        "is_file": False,
        "path": list_prefix,
        "children": _collect_immediate_children(list_prefix, blobs),
    }


def _to_json_value(v: Any) -> Any:
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return str(v)
    return v


@app.route("/")
def index():
    browser_root = _normalize_blob_ls_prefix(default_blob_prefix())
    return render_template(
        "lake_dashboard.html",
        api_base=_app_path("api"),
        app_prefix=_PREFIX,
        default_prefix=default_blob_prefix(),
        blob_browser_root=browser_root,
    )


@app.get("/api/collector-status")
def collector_status():
    try:
        r = requests.get(f"{DATA_COLLECTOR_URL.rstrip('/')}/status", timeout=10)
        r.raise_for_status()
        return app.response_class(response=r.content, status=r.status_code, mimetype="application/json")
    except requests.RequestException as exc:
        return jsonify({"error": str(exc), "upstream": DATA_COLLECTOR_URL}), 502


@app.get("/api/blob-tree")
def blob_tree():
    prefix = request.args.get("prefix") or _normalize_blob_ls_prefix(default_blob_prefix())
    return jsonify(_list_blob_dir_at_prefix(prefix))


@app.post("/api/query")
def run_query():
    payload = request.get_json(silent=True) or {}
    base_prefix = str(payload.get("base_prefix") or default_blob_prefix()).strip()
    sql = str(payload.get("sql") or "").strip()
    dataset = str(payload.get("dataset") or "analytics").strip()
    dt_value = str(payload.get("dt") or "").strip() or None
    hour_value = str(payload.get("hour") or "").strip() or None
    max_files = int(payload.get("max_files") or 64)
    if not sql:
        return jsonify({"error": "sql is required"}), 400
    try:
        from datatailr import Blob
    except Exception as exc:
        return jsonify({"error": f"datatailr Blob unavailable: {exc}"}), 500

    try:
        table = query_lake_sql(
            blob=Blob(),
            base_prefix=base_prefix,
            sql=sql,
            dataset=dataset,
            dt=dt_value,
            hour=hour_value,
            max_files=max_files,
            hive_partitioning=True,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    cols = [str(n) for n in table.column_names]
    out_rows = []
    for rec in table.to_pylist():
        out_rows.append({k: _to_json_value(v) for k, v in rec.items()})
    return jsonify({"columns": cols, "rows": out_rows, "row_count": len(out_rows)})


@app.route("/health")
def health_check():
    return "OK\n"


if __name__ == "__main__":
    app.run(debug=True, port=5050)
