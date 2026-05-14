"""Workflow that periodically ingests the EIA Weekly Petroleum Status Report.

Runs Wed/Thu/Fri at 16:00 UTC. EIA publishes the WPSR (Table 9) on Wednesday
around 10:30 AM ET; the extra Thursday/Friday slots are safety retries when
publication slips. The task is idempotent on the report's week-ending date,
so duplicate runs are a no-op.
"""

from __future__ import annotations

from datatailr import Resources, Schedule, workflow

from weekly_oil_reports.ingest_workflow.tasks import (
    download_report,
    parse_and_store,
    summarize,
)

WEEKLY_SCHEDULE = Schedule(at_hours=[16], weekdays=["Wed", "Thu", "Fri"])


@workflow(
    name="EIA Weekly Oil Report Ingestion",
    # schedule=WEEKLY_SCHEDULE,
    python_requirements=["requests", "pandas", "pyarrow"],
    resources=Resources(memory="512m", cpu=0.5),
)
def weekly_oil_report_ingestion():
    csv_text = download_report()
    result = parse_and_store(csv_text)
    summarize(result)


if __name__ == "__main__":
    weekly_oil_report_ingestion()
