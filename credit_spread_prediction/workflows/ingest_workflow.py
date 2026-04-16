"""Scheduled ingestion workflow for FRED data."""

from __future__ import annotations

from datatailr import Schedule, workflow

from credit_spread_prediction.config import ALL_SERIES
from credit_spread_prediction.data_ingestion.tasks import collect_ingestion_summary, fetch_fred_series


@workflow(
    name="Credit Spread FRED Ingestion",
    python_requirements=["requests", "pandas", "pyarrow"],
    schedule=Schedule(at_hours=[2], at_minutes=[0], weekdays=["mon", "tue", "wed", "thu", "fri"]),
)
def credit_spread_ingestion_workflow(
    observation_start: str = "1990-01-01", observation_end: str | None = None
):
    fetches = []
    for series_id in ALL_SERIES:
        fetches.append(
            fetch_fred_series(
                series_id=series_id,
                observation_start=observation_start,
                observation_end=observation_end,
            ).alias(f"fetch_{series_id.lower()}")
        )
    collect_ingestion_summary(fetches).alias("ingestion_summary")

