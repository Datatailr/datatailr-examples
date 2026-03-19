"""Global Weather Analytics Pipeline -- 6-stage data processing workflow.

Stages:
  1. ingest_weather_data   -- fetch raw data from Open-Meteo for N cities
  2. clean_and_normalize   -- fill gaps, normalise units, tabularise
  3. enrich_and_classify   -- add metadata, derived fields, weather categories
  4. statistical_analysis  -- per-city & global aggregates, correlations
  5. alerts_and_rankings   -- severity alerts, comfort scores, ranked lists
  6. forecast_summary      -- 7-day trend analysis, biggest-change detection
"""

from __future__ import annotations

import math
from typing import Any

from datatailr import task
from datatailr.logging import DatatailrLogger

logger = DatatailrLogger(__name__).get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
BATCH_SIZE = 100  # cities per API call (stay well under 1000 limit)

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "uv_index",
]

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "uv_index_max",
    "sunrise",
    "sunset",
]

CURRENT_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]


# ===================================================================
# Task 1 -- Ingest Raw Data
# ===================================================================

@task(memory="512m", cpu=0.5)
def ingest_weather_data(num_cities: int = 192) -> dict:
    """Fetch current + hourly + daily forecast data from Open-Meteo."""
    import requests
    from weather_analytics.data_pipelines.cities import get_cities

    cities = get_cities(num_cities)
    logger.info(f"Ingesting weather data for {len(cities)} cities")

    all_raw: list[dict] = []

    for batch_start in range(0, len(cities), BATCH_SIZE):
        batch = cities[batch_start : batch_start + BATCH_SIZE]
        lats = ",".join(str(c["lat"]) for c in batch)
        lons = ",".join(str(c["lon"]) for c in batch)

        params = {
            "latitude": lats,
            "longitude": lons,
            "hourly": ",".join(HOURLY_VARS),
            "daily": ",".join(DAILY_VARS),
            "current": ",".join(CURRENT_VARS),
            "timezone": "UTC",
            "forecast_days": 7,
            "forecast_hours": 48,
        }

        resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        if isinstance(payload, list):
            results = payload
        else:
            results = [payload]

        for i, result in enumerate(results):
            city_meta = batch[i]
            all_raw.append({
                "city": city_meta["name"],
                "country": city_meta["country"],
                "lat": city_meta["lat"],
                "lon": city_meta["lon"],
                "api_response": result,
            })

        logger.info(
            f"  Batch {batch_start // BATCH_SIZE + 1}: "
            f"fetched {len(results)} cities"
        )

    logger.info(f"Ingestion complete: {len(all_raw)} city records")
    return {"cities_raw": all_raw, "num_cities": len(all_raw)}


# ===================================================================
# Task 2 -- Clean & Normalise
# ===================================================================

def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def _fill_nones(series: list, default: float = 0.0) -> list:
    """Forward-fill then back-fill, falling back to *default*."""
    out = list(series)
    for i in range(1, len(out)):
        if out[i] is None:
            out[i] = out[i - 1]
    for i in range(len(out) - 2, -1, -1):
        if out[i] is None:
            out[i] = out[i + 1]
    return [default if v is None else v for v in out]


@task(memory="512m", cpu=0.5)
def clean_and_normalize(raw: dict) -> dict:
    """Sanitise raw API responses into uniform per-city records."""
    logger.info("Cleaning & normalising data")
    cleaned: list[dict] = []

    for entry in raw["cities_raw"]:
        api = entry["api_response"]
        current = api.get("current", {})
        hourly = api.get("hourly", {})
        daily = api.get("daily", {})

        current_clean = {}
        for key in CURRENT_VARS:
            current_clean[key] = _safe_float(current.get(key))
        current_clean["time"] = current.get("time")

        hourly_clean: dict[str, list] = {"time": hourly.get("time", [])}
        for key in HOURLY_VARS:
            raw_series = hourly.get(key, [])
            hourly_clean[key] = _fill_nones(
                [_safe_float(v) for v in raw_series]
            )

        daily_clean: dict[str, list] = {"time": daily.get("time", [])}
        for key in DAILY_VARS:
            raw_series = daily.get(key, [])
            if key in ("sunrise", "sunset"):
                daily_clean[key] = raw_series
            else:
                daily_clean[key] = _fill_nones(
                    [_safe_float(v) for v in raw_series]
                )

        cleaned.append({
            "city": entry["city"],
            "country": entry["country"],
            "lat": entry["lat"],
            "lon": entry["lon"],
            "elevation": api.get("elevation"),
            "current": current_clean,
            "hourly": hourly_clean,
            "daily": daily_clean,
        })

    logger.info(f"Cleaned {len(cleaned)} city records")
    return {"cities": cleaned}


# ===================================================================
# Task 3 -- Enrich & Classify
# ===================================================================

def _compute_heat_index(temp_c: float | None, rh: float | None) -> float | None:
    """Simplified Rothfusz regression (valid above 27 C / 40% RH)."""
    if temp_c is None or rh is None:
        return None
    if temp_c < 27 or rh < 40:
        return temp_c
    t = temp_c * 9 / 5 + 32  # to Fahrenheit
    hi = (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * rh
        - 0.22475541 * t * rh
        - 0.00683783 * t * t
        - 0.05481717 * rh * rh
        + 0.00122874 * t * t * rh
        + 0.00085282 * t * rh * rh
        - 0.00000199 * t * t * rh * rh
    )
    return round((hi - 32) * 5 / 9, 1)


def _compute_wind_chill(temp_c: float | None, wind_kph: float | None) -> float | None:
    """Environment Canada / NWS wind-chill (valid below 10 C, wind > 4.8 kph)."""
    if temp_c is None or wind_kph is None:
        return None
    if temp_c > 10 or wind_kph <= 4.8:
        return temp_c
    wc = (
        13.12
        + 0.6215 * temp_c
        - 11.37 * (wind_kph ** 0.16)
        + 0.3965 * temp_c * (wind_kph ** 0.16)
    )
    return round(wc, 1)


def _compute_dew_point(temp_c: float | None, rh: float | None) -> float | None:
    """Magnus formula approximation."""
    if temp_c is None or rh is None or rh <= 0:
        return None
    a, b = 17.27, 237.7
    alpha = (a * temp_c) / (b + temp_c) + math.log(rh / 100.0)
    dp = (b * alpha) / (a - alpha)
    return round(dp, 1)


def _classify_weather(
    temp: float | None,
    precip: float | None,
    snow: float | None,
    cloud: float | None,
    wind: float | None,
) -> str:
    if snow and snow > 0:
        return "snowy"
    if wind and wind > 60:
        return "stormy"
    if precip and precip > 2:
        if wind and wind > 40:
            return "stormy"
        return "rainy"
    if precip and precip > 0:
        return "drizzle"
    if cloud is not None:
        if cloud < 20:
            return "clear"
        if cloud < 60:
            return "partly cloudy"
        return "cloudy"
    return "unknown"


@task(memory="512m", cpu=0.5)
def enrich_and_classify(clean_data: dict) -> dict:
    """Add metadata, derived fields, weather classification."""
    from weather_analytics.data_pipelines.cities import CITIES

    city_meta_map = {c["name"]: c for c in CITIES}
    logger.info("Enriching & classifying data")
    enriched: list[dict] = []

    for rec in clean_data["cities"]:
        meta = city_meta_map.get(rec["city"], {})
        cur = rec["current"]

        temp = cur.get("temperature_2m")
        rh = cur.get("relative_humidity_2m")
        wind = cur.get("wind_speed_10m")
        precip = cur.get("precipitation")
        snow = cur.get("snowfall")
        cloud = cur.get("cloud_cover")

        rec["continent"] = meta.get("continent", "Unknown")
        rec["population"] = meta.get("population", 0)
        rec["timezone"] = meta.get("timezone", "UTC")

        rec["derived"] = {
            "heat_index": _compute_heat_index(temp, rh),
            "wind_chill": _compute_wind_chill(temp, wind),
            "dew_point": _compute_dew_point(temp, rh),
        }
        rec["weather_category"] = _classify_weather(
            temp, precip, snow, cloud, wind
        )

        enriched.append(rec)

    logger.info(f"Enriched {len(enriched)} city records")
    return {"cities": enriched}


# ===================================================================
# Task 4 -- Statistical Analysis
# ===================================================================

def _mean(vals: list[float | None]) -> float | None:
    nums = [v for v in vals if v is not None]
    return round(sum(nums) / len(nums), 2) if nums else None


def _stdev(vals: list[float | None]) -> float | None:
    nums = [v for v in vals if v is not None]
    if len(nums) < 2:
        return None
    m = sum(nums) / len(nums)
    var = sum((x - m) ** 2 for x in nums) / (len(nums) - 1)
    return round(math.sqrt(var), 2)


def _pearson(xs: list, ys: list) -> float | None:
    pairs = [
        (x, y) for x, y in zip(xs, ys) if x is not None and y is not None
    ]
    n = len(pairs)
    if n < 3:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((x - mx) * (y - my) for x, y in pairs)
    dx = math.sqrt(sum((x - mx) ** 2 for x, _ in pairs))
    dy = math.sqrt(sum((y - my) ** 2 for _, y in pairs))
    if dx == 0 or dy == 0:
        return None
    return round(num / (dx * dy), 4)


@task(memory="512m", cpu=0.5)
def statistical_analysis(enriched: dict) -> dict:
    """Compute per-city stats, global aggregates, continent summaries, and correlations."""
    logger.info("Running statistical analysis")
    cities = enriched["cities"]

    per_city_stats: list[dict] = []
    continent_buckets: dict[str, list[dict]] = {}

    all_temps: list[float | None] = []
    all_humidities: list[float | None] = []
    all_pressures: list[float | None] = []
    all_winds: list[float | None] = []

    for rec in cities:
        cur = rec["current"]
        hourly = rec["hourly"]
        daily = rec["daily"]

        temp = cur.get("temperature_2m")
        rh = cur.get("relative_humidity_2m")
        pressure = cur.get("pressure_msl")
        wind = cur.get("wind_speed_10m")

        temp_series = hourly.get("temperature_2m", [])
        precip_series = hourly.get("precipitation", [])
        wind_series = hourly.get("wind_speed_10m", [])

        daily_max = daily.get("temperature_2m_max", [])
        daily_min = daily.get("temperature_2m_min", [])
        daily_precip = daily.get("precipitation_sum", [])

        stats = {
            "city": rec["city"],
            "country": rec["country"],
            "continent": rec["continent"],
            "current_temp": temp,
            "current_humidity": rh,
            "current_pressure": pressure,
            "current_wind": wind,
            "hourly_temp_mean": _mean(temp_series),
            "hourly_temp_stdev": _stdev(temp_series),
            "hourly_temp_min": min((v for v in temp_series if v is not None), default=None),
            "hourly_temp_max": max((v for v in temp_series if v is not None), default=None),
            "hourly_precip_total": round(sum(v for v in precip_series if v is not None), 2),
            "hourly_wind_mean": _mean(wind_series),
            "hourly_wind_max": max((v for v in wind_series if v is not None), default=None),
            "daily_temp_range": (
                round(max(v for v in daily_max if v is not None)
                      - min(v for v in daily_min if v is not None), 1)
                if daily_max and daily_min
                and any(v is not None for v in daily_max)
                and any(v is not None for v in daily_min)
                else None
            ),
            "daily_precip_total_7d": round(
                sum(v for v in daily_precip if v is not None), 2
            ),
        }
        per_city_stats.append(stats)

        cont = rec.get("continent", "Unknown")
        continent_buckets.setdefault(cont, []).append(stats)

        all_temps.append(temp)
        all_humidities.append(rh)
        all_pressures.append(pressure)
        all_winds.append(wind)

    continent_summaries: dict[str, dict] = {}
    for cont, bucket in continent_buckets.items():
        temps = [s["current_temp"] for s in bucket]
        continent_summaries[cont] = {
            "num_cities": len(bucket),
            "avg_temp": _mean(temps),
            "min_temp": min((v for v in temps if v is not None), default=None),
            "max_temp": max((v for v in temps if v is not None), default=None),
            "avg_humidity": _mean([s["current_humidity"] for s in bucket]),
            "avg_wind": _mean([s["current_wind"] for s in bucket]),
        }

    correlations = {
        "temp_vs_humidity": _pearson(all_temps, all_humidities),
        "temp_vs_pressure": _pearson(all_temps, all_pressures),
        "temp_vs_wind": _pearson(all_temps, all_winds),
        "pressure_vs_wind": _pearson(all_pressures, all_winds),
        "humidity_vs_wind": _pearson(all_humidities, all_winds),
    }

    global_agg = {
        "total_cities": len(cities),
        "global_avg_temp": _mean(all_temps),
        "global_avg_humidity": _mean(all_humidities),
        "global_avg_pressure": _mean(all_pressures),
        "global_avg_wind": _mean(all_winds),
    }

    logger.info("Statistical analysis complete")
    return {
        "cities": enriched["cities"],
        "per_city_stats": per_city_stats,
        "continent_summaries": continent_summaries,
        "correlations": correlations,
        "global_aggregates": global_agg,
    }


# ===================================================================
# Task 5 -- Alerts & Rankings
# ===================================================================

def _comfort_score(
    temp: float | None,
    rh: float | None,
    wind: float | None,
    uv: float | None,
) -> float | None:
    """0-100 score: 100 = perfect comfort (21 C, 45% RH, light breeze, low UV)."""
    if temp is None or rh is None or wind is None:
        return None
    temp_score = max(0, 100 - 5 * abs(temp - 21))
    rh_score = max(0, 100 - 2 * abs(rh - 45))
    wind_score = max(0, 100 - 3 * abs(wind - 10))
    uv_score = max(0, 100 - 10 * (uv or 0))
    raw = 0.40 * temp_score + 0.25 * rh_score + 0.20 * wind_score + 0.15 * uv_score
    return round(max(0, min(100, raw)), 1)


@task(memory="512m", cpu=0.5)
def alerts_and_rankings(stats_data: dict) -> dict:
    """Generate alerts and rank cities by multiple criteria."""
    logger.info("Generating alerts & rankings")
    cities = stats_data["cities"]
    per_city = stats_data["per_city_stats"]

    alerts: list[dict] = []
    comfort_records: list[dict] = []

    for rec, st in zip(cities, per_city):
        cur = rec["current"]
        temp = cur.get("temperature_2m")
        wind = cur.get("wind_speed_10m")
        precip = cur.get("precipitation")
        snow = cur.get("snowfall")
        rh = cur.get("relative_humidity_2m")
        uv = None
        hourly_uv = rec.get("hourly", {}).get("uv_index", [])
        if hourly_uv:
            valid_uv = [v for v in hourly_uv if v is not None]
            uv = max(valid_uv) if valid_uv else None

        if temp is not None and temp > 40:
            alerts.append({"city": rec["city"], "type": "extreme_heat", "severity": "high", "value": temp, "message": f"Extreme heat: {temp} C"})
        elif temp is not None and temp > 35:
            alerts.append({"city": rec["city"], "type": "heat", "severity": "medium", "value": temp, "message": f"Heat advisory: {temp} C"})

        if temp is not None and temp < -20:
            alerts.append({"city": rec["city"], "type": "extreme_cold", "severity": "high", "value": temp, "message": f"Extreme cold: {temp} C"})
        elif temp is not None and temp < -10:
            alerts.append({"city": rec["city"], "type": "cold", "severity": "medium", "value": temp, "message": f"Cold advisory: {temp} C"})

        if wind is not None and wind > 80:
            alerts.append({"city": rec["city"], "type": "extreme_wind", "severity": "high", "value": wind, "message": f"Extreme wind: {wind} km/h"})
        elif wind is not None and wind > 50:
            alerts.append({"city": rec["city"], "type": "wind", "severity": "medium", "value": wind, "message": f"Wind advisory: {wind} km/h"})

        if precip is not None and precip > 20:
            alerts.append({"city": rec["city"], "type": "heavy_rain", "severity": "high", "value": precip, "message": f"Heavy precipitation: {precip} mm/h"})

        if snow is not None and snow > 5:
            alerts.append({"city": rec["city"], "type": "heavy_snow", "severity": "high", "value": snow, "message": f"Heavy snowfall: {snow} cm/h"})

        if uv is not None and uv >= 11:
            alerts.append({"city": rec["city"], "type": "uv_extreme", "severity": "high", "value": uv, "message": f"Extreme UV index: {uv}"})

        score = _comfort_score(temp, rh, wind, uv)
        comfort_records.append({
            "city": rec["city"],
            "country": rec["country"],
            "continent": rec.get("continent", "Unknown"),
            "comfort_score": score,
            "current_temp": temp,
            "current_humidity": rh,
            "current_wind": wind,
            "uv_max": uv,
        })

    def _sort_key(items, key, reverse=False):
        valid = [r for r in items if r.get(key) is not None]
        return sorted(valid, key=lambda x: x[key], reverse=reverse)

    rankings = {
        "hottest": [{"city": r["city"], "value": r["current_temp"]} for r in _sort_key(per_city, "current_temp", True)[:15]],
        "coldest": [{"city": r["city"], "value": r["current_temp"]} for r in _sort_key(per_city, "current_temp", False)[:15]],
        "wettest_7d": [{"city": r["city"], "value": r["daily_precip_total_7d"]} for r in _sort_key(per_city, "daily_precip_total_7d", True)[:15]],
        "windiest": [{"city": r["city"], "value": r["current_wind"]} for r in _sort_key(per_city, "current_wind", True)[:15]],
        "most_comfortable": [{"city": r["city"], "score": r["comfort_score"]} for r in _sort_key(comfort_records, "comfort_score", True)[:15]],
        "least_comfortable": [{"city": r["city"], "score": r["comfort_score"]} for r in _sort_key(comfort_records, "comfort_score", False)[:15]],
    }

    logger.info(f"Generated {len(alerts)} alerts, ranked {len(comfort_records)} cities")
    return {
        **stats_data,
        "alerts": alerts,
        "comfort_scores": comfort_records,
        "rankings": rankings,
    }


# ===================================================================
# Task 6 -- Forecast Summary
# ===================================================================

@task(memory="512m", cpu=0.5)
def forecast_summary(ranked_data: dict, enriched_data: dict) -> dict:
    """Analyse 7-day forecast trends, find biggest upcoming changes."""
    logger.info("Building forecast summary")
    cities = enriched_data["cities"]

    forecast_trends: list[dict] = []

    for rec in cities:
        daily = rec.get("daily", {})
        temps_max = daily.get("temperature_2m_max", [])
        temps_min = daily.get("temperature_2m_min", [])
        precip_daily = daily.get("precipitation_sum", [])
        wind_max = daily.get("wind_speed_10m_max", [])

        valid_max = [v for v in temps_max if v is not None]
        valid_min = [v for v in temps_min if v is not None]
        valid_precip = [v for v in precip_daily if v is not None]
        valid_wind = [v for v in wind_max if v is not None]

        if len(valid_max) < 2:
            forecast_trends.append({
                "city": rec["city"],
                "country": rec["country"],
                "continent": rec.get("continent", "Unknown"),
                "temp_trend": "insufficient data",
                "temp_change": None,
                "precip_total_7d": sum(valid_precip) if valid_precip else 0,
                "max_wind_7d": max(valid_wind) if valid_wind else None,
                "daily_highs": valid_max,
                "daily_lows": valid_min,
            })
            continue

        first_half_avg = sum(valid_max[:3]) / min(3, len(valid_max[:3]))
        second_half_avg = sum(valid_max[-3:]) / min(3, len(valid_max[-3:]))
        temp_change = round(second_half_avg - first_half_avg, 1)

        if temp_change > 3:
            trend = "warming"
        elif temp_change < -3:
            trend = "cooling"
        else:
            trend = "stable"

        forecast_trends.append({
            "city": rec["city"],
            "country": rec["country"],
            "continent": rec.get("continent", "Unknown"),
            "temp_trend": trend,
            "temp_change": temp_change,
            "precip_total_7d": round(sum(valid_precip), 1) if valid_precip else 0,
            "max_wind_7d": round(max(valid_wind), 1) if valid_wind else None,
            "daily_highs": valid_max,
            "daily_lows": valid_min,
        })

    biggest_warming = sorted(
        [f for f in forecast_trends if f["temp_change"] is not None],
        key=lambda x: x["temp_change"],
        reverse=True,
    )[:10]

    biggest_cooling = sorted(
        [f for f in forecast_trends if f["temp_change"] is not None],
        key=lambda x: x["temp_change"],
    )[:10]

    trend_counts = {}
    for f in forecast_trends:
        t = f["temp_trend"]
        trend_counts[t] = trend_counts.get(t, 0) + 1

    report = {
        **ranked_data,
        "forecast_trends": forecast_trends,
        "biggest_warming": biggest_warming,
        "biggest_cooling": biggest_cooling,
        "trend_summary": trend_counts,
    }

    logger.info("Forecast summary complete")
    return report
