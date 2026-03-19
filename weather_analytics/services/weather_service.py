"""Weather Analytics REST API Service.

Endpoints:
    GET  /                     -- service info
    GET  /__health_check__.html -- health probe
    GET  /current              -- current weather for a city
    GET  /rankings             -- top/bottom cities by metric
    GET  /compare              -- side-by-side city comparison
    GET  /alerts               -- active weather alerts
    GET  /forecast             -- 7-day forecast for a city
    GET  /stats                -- global aggregate statistics
    GET  /cities               -- list of available cities
    POST /trigger              -- trigger a new pipeline run
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, request

app = Flask(__name__)

# ---------------------------------------------------------------------------
# In-memory data store -- populated by the pipeline or by /trigger
# ---------------------------------------------------------------------------

_DATA_LOCK = threading.Lock()
_REPORT: dict | None = None
_LAST_UPDATED: str | None = None


def _load_report(report: dict) -> None:
    global _REPORT, _LAST_UPDATED
    with _DATA_LOCK:
        _REPORT = report
        _LAST_UPDATED = datetime.now(timezone.utc).isoformat()


def _get_report() -> dict | None:
    with _DATA_LOCK:
        return _REPORT


def _run_pipeline_background(num_cities: int, include_forecast: bool) -> None:
    """Run the weather pipeline in a background thread and load results."""
    from weather_analytics.data_pipelines.weather_pipeline import (
        ingest_weather_data,
        clean_and_normalize,
        enrich_and_classify,
        statistical_analysis,
        alerts_and_rankings,
        forecast_summary,
    )

    try:
        raw = ingest_weather_data.__wrapped__(num_cities)
        clean = clean_and_normalize.__wrapped__(raw)
        enriched = enrich_and_classify.__wrapped__(clean)
        stats = statistical_analysis.__wrapped__(enriched)
        ranked = alerts_and_rankings.__wrapped__(stats)
        report = forecast_summary.__wrapped__(ranked, enriched)
        _load_report(report)
    except Exception as e:
        app.logger.error(f"Pipeline run failed: {e}")


def _find_city(name: str) -> dict | None:
    """Find a city record by name (case-insensitive partial match)."""
    report = _get_report()
    if not report:
        return None
    name_lower = name.lower()
    for rec in report.get("cities", []):
        if rec["city"].lower() == name_lower:
            return rec
    for rec in report.get("cities", []):
        if name_lower in rec["city"].lower():
            return rec
    return None


def _find_city_stats(name: str) -> dict | None:
    report = _get_report()
    if not report:
        return None
    name_lower = name.lower()
    for st in report.get("per_city_stats", []):
        if st["city"].lower() == name_lower:
            return st
    for st in report.get("per_city_stats", []):
        if name_lower in st["city"].lower():
            return st
    return None


def _find_forecast(name: str) -> dict | None:
    report = _get_report()
    if not report:
        return None
    name_lower = name.lower()
    for f in report.get("forecast_trends", []):
        if f["city"].lower() == name_lower:
            return f
    for f in report.get("forecast_trends", []):
        if name_lower in f["city"].lower():
            return f
    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": "Weather Analytics API",
        "version": "1.0.0",
        "last_updated": _LAST_UPDATED,
        "data_loaded": _get_report() is not None,
        "endpoints": [
            "GET /current?city=<name>",
            "GET /rankings?metric=<metric>&order=<asc|desc>&limit=<n>",
            "GET /compare?cities=<city1,city2,...>",
            "GET /alerts",
            "GET /forecast?city=<name>",
            "GET /stats",
            "GET /cities",
            "POST /trigger",
        ],
    })


@app.route("/__health_check__.html", methods=["GET"])
def health_check():
    return "OK\n"


@app.route("/cities", methods=["GET"])
def list_cities():
    report = _get_report()
    if not report:
        return jsonify({"error": "No data loaded. Trigger a pipeline run first."}), 503
    cities = [
        {"city": r["city"], "country": r["country"], "continent": r.get("continent", "Unknown")}
        for r in report.get("cities", [])
    ]
    return jsonify({"cities": cities, "count": len(cities)})


@app.route("/current", methods=["GET"])
def current():
    city_name = request.args.get("city", "")
    if not city_name:
        return jsonify({"error": "Provide ?city=<name>"}), 400

    rec = _find_city(city_name)
    if not rec:
        return jsonify({"error": f"City '{city_name}' not found"}), 404

    return jsonify({
        "city": rec["city"],
        "country": rec["country"],
        "continent": rec.get("continent"),
        "lat": rec["lat"],
        "lon": rec["lon"],
        "elevation": rec.get("elevation"),
        "current": rec["current"],
        "derived": rec.get("derived"),
        "weather_category": rec.get("weather_category"),
        "last_updated": _LAST_UPDATED,
    })


@app.route("/rankings", methods=["GET"])
def rankings():
    report = _get_report()
    if not report:
        return jsonify({"error": "No data loaded."}), 503

    metric = request.args.get("metric", "hottest")
    limit = int(request.args.get("limit", 10))

    available = report.get("rankings", {})
    if metric not in available:
        return jsonify({
            "error": f"Unknown metric '{metric}'",
            "available_metrics": list(available.keys()),
        }), 400

    data = available[metric][:limit]
    return jsonify({"metric": metric, "limit": limit, "rankings": data})


@app.route("/compare", methods=["GET"])
def compare():
    cities_param = request.args.get("cities", "")
    if not cities_param:
        return jsonify({"error": "Provide ?cities=city1,city2,..."}), 400

    names = [n.strip() for n in cities_param.split(",") if n.strip()]
    results = []
    for name in names:
        rec = _find_city(name)
        st = _find_city_stats(name)
        if rec and st:
            results.append({
                "city": rec["city"],
                "country": rec["country"],
                "current": rec["current"],
                "weather_category": rec.get("weather_category"),
                "derived": rec.get("derived"),
                "stats": st,
            })
        else:
            results.append({"city": name, "error": "not found"})

    return jsonify({"comparison": results})


@app.route("/alerts", methods=["GET"])
def alerts():
    report = _get_report()
    if not report:
        return jsonify({"error": "No data loaded."}), 503

    severity = request.args.get("severity")
    alert_list = report.get("alerts", [])
    if severity:
        alert_list = [a for a in alert_list if a["severity"] == severity]

    return jsonify({
        "alerts": alert_list,
        "count": len(alert_list),
        "last_updated": _LAST_UPDATED,
    })


@app.route("/forecast", methods=["GET"])
def forecast():
    city_name = request.args.get("city", "")
    if not city_name:
        return jsonify({"error": "Provide ?city=<name>"}), 400

    rec = _find_city(city_name)
    fc = _find_forecast(city_name)
    if not rec or not fc:
        return jsonify({"error": f"City '{city_name}' not found"}), 404

    return jsonify({
        "city": rec["city"],
        "country": rec["country"],
        "daily": rec.get("daily"),
        "trend": fc.get("temp_trend"),
        "temp_change": fc.get("temp_change"),
        "precip_total_7d": fc.get("precip_total_7d"),
        "max_wind_7d": fc.get("max_wind_7d"),
    })


@app.route("/stats", methods=["GET"])
def stats():
    report = _get_report()
    if not report:
        return jsonify({"error": "No data loaded."}), 503

    return jsonify({
        "global_aggregates": report.get("global_aggregates"),
        "continent_summaries": report.get("continent_summaries"),
        "correlations": report.get("correlations"),
        "trend_summary": report.get("trend_summary"),
        "last_updated": _LAST_UPDATED,
    })


@app.route("/trigger", methods=["POST"])
def trigger():
    data = request.get_json(silent=True) or {}
    num_cities = int(data.get("num_cities", 192))
    include_forecast = bool(data.get("include_forecast", True))

    thread = threading.Thread(
        target=_run_pipeline_background,
        args=(num_cities, include_forecast),
        daemon=True,
    )
    thread.start()

    return jsonify({
        "status": "pipeline_started",
        "num_cities": num_cities,
        "include_forecast": include_forecast,
    }), 202


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(port):
    app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(1024)
