from __future__ import annotations

import os

from datatailr import Resources, Schedule, workflow

from smart_building_energy.data_pipelines.tasks import (
    DEFAULT_ANOMALY_Z,
    DEFAULT_BLOB_PREFIX,
    DEFAULT_CO2_THRESHOLD,
    DEFAULT_COMFORT_MIN,
    DEFAULT_LOOKBACK_HOURS,
    clean_validate,
    compute_kpis,
    detect_anomalies,
    generate_alerts,
    list_recent_raw_files,
    load_raw_data,
    summarize,
    write_outputs,
)

LOOKBACK_HOURS = int(os.environ.get("PIPELINE_LOOKBACK_HOURS", str(DEFAULT_LOOKBACK_HOURS)))
BLOB_PREFIX = os.environ.get("ENERGY_BLOB_PREFIX", DEFAULT_BLOB_PREFIX)
COMFORT_MIN = float(os.environ.get("ALERT_COMFORT_MIN", str(DEFAULT_COMFORT_MIN)))
CO2_THRESHOLD = float(os.environ.get("ALERT_CO2_THRESHOLD", str(DEFAULT_CO2_THRESHOLD)))
ANOMALY_Z = float(os.environ.get("ANOMALY_ZSCORE_THRESHOLD", str(DEFAULT_ANOMALY_Z)))


@workflow(
    name="Smart Building Energy Processing",
    schedule=Schedule(every_minute=15),
    python_requirements="smart_building_energy/requirements.txt",
    resources=Resources(memory="1g", cpu=1),
    env_vars={
        "ENERGY_BLOB_PREFIX": BLOB_PREFIX,
        "PIPELINE_LOOKBACK_HOURS": str(LOOKBACK_HOURS),
        "ALERT_COMFORT_MIN": str(COMFORT_MIN),
        "ALERT_CO2_THRESHOLD": str(CO2_THRESHOLD),
        "ANOMALY_ZSCORE_THRESHOLD": str(ANOMALY_Z),
    },
)
def processing_workflow():
    files = list_recent_raw_files(BLOB_PREFIX, LOOKBACK_HOURS)
    raw = load_raw_data(files).alias("load_raw")
    cleaned = clean_validate(raw)
    kpis = compute_kpis(cleaned)
    scored = detect_anomalies(kpis, ANOMALY_Z)
    alerts = generate_alerts(scored, COMFORT_MIN, CO2_THRESHOLD)
    write_result = write_outputs(scored, alerts, BLOB_PREFIX)
    summarize(files, raw, cleaned, scored, alerts, write_result).alias("run_summary")


if __name__ == "__main__":
    processing_workflow()

