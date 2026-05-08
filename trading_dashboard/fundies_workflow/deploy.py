"""Workflow definition for the daily fundamentals snapshot.

Runs every weekday at 06:30 UTC (just before the US open) and republishes
the per-ticker fundamentals to blob storage where the dashboard reads them.
"""

from __future__ import annotations

from pathlib import Path

from datatailr import Resources, Schedule, workflow

from trading_dashboard.fundies_workflow.tasks import generate_fundies, publish_fundies


REQUIREMENTS_PATH = str(Path(__file__).resolve().parent.parent / "requirements.txt")


@workflow(
    name="Trading Fundies Snapshot",
    schedule=Schedule(at_hours=[6], at_minutes=[30], weekdays=["Mon", "Tue", "Wed", "Thu", "Fri"]),
    python_requirements=REQUIREMENTS_PATH,
    resources=Resources(memory="512m", cpu=1),
)
def fundies_workflow():
    payload = generate_fundies()
    publish_fundies(payload)


if __name__ == "__main__":
    fundies_workflow()
