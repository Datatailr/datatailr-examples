from datatailr import workflow, Resources

from weather_pipeline.tasks.processing import (
    ingest_stations,
    ingest_weather,
    clean_and_validate,
    compute_statistics,
    detect_anomalies,
    aggregate_and_store,
)


@workflow(
    name="Weather Analytics Pipeline",
    python_requirements=["requests", "pandas", "numpy", "pyarrow"],
    resources=Resources(memory="4g", cpu=2),
)
def weather_analytics_pipeline():
    stations = ingest_stations()
    raw_data = ingest_weather(stations)
    clean_data = clean_and_validate(raw_data)
    stats = compute_statistics(clean_data)
    anomalies = detect_anomalies(clean_data)
    aggregate_and_store(stats, anomalies)


if __name__ == "__main__":
    weather_analytics_pipeline()
