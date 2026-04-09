from __future__ import annotations

import os
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request

_env = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "")
_job = os.environ.get("DATATAILR_JOB_NAME", "")
_job_type = os.environ.get("DATATAILR_JOB_TYPE", "")
if _job_type == "workstation":
    _PREFIX = f"/workstation/{_env}/{_job}/ide/proxy/5060/"
elif _env and _job:
    _PREFIX = f"/job/{_env}/{_job}/"
else:
    _PREFIX = "/"

app = Flask(__name__)

if _job_type in ("workstation", ""):
    ANALYTICS_API_URL = os.environ.get("ANALYTICS_API_URL", "http://localhost:8091")
else:
    ANALYTICS_API_URL = os.environ.get("ANALYTICS_API_URL", "http://building-analytics-api")

def _app_path(suffix: str = "") -> str:
    base = _PREFIX.rstrip("/")
    suf = suffix.strip("/")
    if not suf:
        return f"{base}/" if base else "/"
    return f"{base}/{suf}" if base else f"/{suf}"

def _fetch_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{ANALYTICS_API_URL}/{path.lstrip('/')}"
    r = requests.get(url, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()


@app.get("/__health_check__.html")
def health_check() -> str:
    return "OK\n"


@app.get("/")
def index():
    return render_template("index.html", api_base=_app_path("api"))


@app.get("/api/buildings")
def buildings():
    data = _fetch_json("/metadata/buildings")
    return jsonify(data)


@app.get("/api/latest")
def latest():
    building_id = request.args.get("building_id", default=None, type=str)
    params = {"building_id": building_id} if building_id else None
    data = _fetch_json("/kpi/latest", params=params)
    return jsonify(data)


@app.get("/api/timeseries")
def timeseries():
    building_id = request.args.get("building_id", default=None, type=str)
    metric = request.args.get("metric", default="energy_per_occupant", type=str)
    hours = request.args.get("hours", default=24, type=int)
    params: dict[str, Any] = {"metric": metric, "hours": hours}
    if building_id:
        params["building_id"] = building_id
    data = _fetch_json("/kpi/timeseries", params=params)
    return jsonify(data)


@app.get("/api/alerts")
def alerts():
    building_id = request.args.get("building_id", default=None, type=str)
    severity = request.args.get("severity", default=None, type=str)
    status = request.args.get("status", default="open", type=str)
    params: dict[str, Any] = {"status": status, "hours": 24}
    if building_id:
        params["building_id"] = building_id
    if severity:
        params["severity"] = severity
    data = _fetch_json("/alerts", params=params)
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)