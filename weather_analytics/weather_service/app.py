import io
import json
from typing import Optional


BLOB_PREFIX = "weather_analytics"


def create_app():
    import pandas as pd
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.responses import PlainTextResponse
    from pydantic import BaseModel
    from datatailr import Blob

    app = FastAPI(title="Weather Analytics API")
    blob = Blob()

    def _read_parquet(blob_path: str) -> pd.DataFrame:
        if not blob.exists(blob_path):
            raise HTTPException(status_code=404, detail=f"Data not found: {blob_path}. Run the pipeline first.")
        data = blob.get(blob_path)
        return pd.read_parquet(io.BytesIO(data))

    @app.get("/__health_check__.html", response_class=PlainTextResponse)
    def health_check():
        return "OK\n"

    @app.get("/")
    def index():
        return {
            "service": "Weather Analytics API",
            "endpoints": [
                {"method": "GET", "path": "/stations", "description": "List all weather stations"},
                {"method": "GET", "path": "/weather", "description": "Query cleaned weather data"},
                {"method": "GET", "path": "/statistics", "description": "Get computed statistics per city"},
                {"method": "GET", "path": "/daily-statistics", "description": "Get daily statistics per city"},
                {"method": "GET", "path": "/continent-statistics", "description": "Get continent-level statistics"},
                {"method": "GET", "path": "/anomalies", "description": "Get detected anomalies"},
                {"method": "GET", "path": "/run-metadata", "description": "Get last pipeline run metadata"},
                {"method": "POST", "path": "/trigger-run", "description": "Trigger a new pipeline run"},
            ],
        }

    @app.get("/stations")
    def get_stations(continent: Optional[str] = Query(None)):
        df = _read_parquet(f"{BLOB_PREFIX}/stations.parquet")
        if continent:
            df = df[df["continent"].str.lower() == continent.lower()]
        return df.to_dict(orient="records")

    @app.get("/weather")
    def get_weather(
        city: Optional[str] = Query(None),
        continent: Optional[str] = Query(None),
        limit: int = Query(1000, ge=1, le=50000),
        offset: int = Query(0, ge=0),
    ):
        df = _read_parquet(f"{BLOB_PREFIX}/clean_weather.parquet")
        if city:
            df = df[df["city"].str.lower() == city.lower()]
        if continent:
            df = df[df["continent"].str.lower() == continent.lower()]

        total = len(df)
        df = df.iloc[offset:offset + limit]
        df["time"] = df["time"].astype(str)
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)

        return {"total": total, "offset": offset, "limit": limit, "data": df.to_dict(orient="records")}

    @app.get("/statistics")
    def get_statistics(
        city: Optional[str] = Query(None),
        continent: Optional[str] = Query(None),
    ):
        df = _read_parquet(f"{BLOB_PREFIX}/statistics.parquet")
        if city:
            df = df[df["city"].str.lower() == city.lower()]
        if continent:
            df = df[df["continent"].str.lower() == continent.lower()]
        return df.to_dict(orient="records")

    @app.get("/daily-statistics")
    def get_daily_statistics(
        city: Optional[str] = Query(None),
        continent: Optional[str] = Query(None),
        limit: int = Query(1000, ge=1, le=50000),
        offset: int = Query(0, ge=0),
    ):
        df = _read_parquet(f"{BLOB_PREFIX}/daily_statistics.parquet")
        if city:
            df = df[df["city"].str.lower() == city.lower()]
        if continent:
            df = df[df["continent"].str.lower() == continent.lower()]

        total = len(df)
        df = df.iloc[offset:offset + limit]
        return {"total": total, "offset": offset, "limit": limit, "data": df.to_dict(orient="records")}

    @app.get("/continent-statistics")
    def get_continent_statistics():
        df = _read_parquet(f"{BLOB_PREFIX}/continent_statistics.parquet")
        return df.to_dict(orient="records")

    @app.get("/anomalies")
    def get_anomalies(
        severity: Optional[str] = Query(None),
        city: Optional[str] = Query(None),
        continent: Optional[str] = Query(None),
        variable: Optional[str] = Query(None),
        limit: int = Query(1000, ge=1, le=50000),
        offset: int = Query(0, ge=0),
    ):
        df = _read_parquet(f"{BLOB_PREFIX}/anomalies.parquet")
        if severity:
            df = df[df["severity"].str.lower() == severity.lower()]
        if city:
            df = df[df["city"].str.lower() == city.lower()]
        if continent:
            df = df[df["continent"].str.lower() == continent.lower()]
        if variable:
            df = df[df["variable"].str.lower() == variable.lower()]

        total = len(df)
        df = df.iloc[offset:offset + limit]
        if "time" in df.columns:
            df["time"] = df["time"].astype(str)
        return {"total": total, "offset": offset, "limit": limit, "data": df.to_dict(orient="records")}

    @app.get("/run-metadata")
    def get_run_metadata():
        path = f"{BLOB_PREFIX}/run_metadata.json"
        if not blob.exists(path):
            raise HTTPException(status_code=404, detail="No pipeline run metadata found. Run the pipeline first.")
        data = blob.get(path)
        return json.loads(data)

    class TriggerRunRequest(BaseModel):
        days_back: int = 30
        variables: Optional[list[str]] = None

    @app.post("/trigger-run")
    def trigger_run(req: TriggerRunRequest):
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
        def triggered_pipeline():
            stations = ingest_stations()
            raw_data = ingest_weather(stations, days_back=req.days_back,
                                      variables=req.variables)
            clean_data = clean_and_validate(raw_data)
            stats = compute_statistics(clean_data)
            anomalies = detect_anomalies(clean_data)
            aggregate_and_store(stats, anomalies)

        triggered_pipeline()
        return {
            "status": "triggered",
            "days_back": req.days_back,
            "variables": req.variables,
        }

    return app


def main(port):
    import uvicorn
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=int(port))


if __name__ == "__main__":
    main(8000)
