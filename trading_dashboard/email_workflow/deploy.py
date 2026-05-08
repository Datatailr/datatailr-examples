"""Workflow definition for the vendor-inbox AI pipeline.

Runs every 15 minutes during US market hours. Each run pulls the latest
batch of vendor emails, summarizes them with the AI summarizer and writes
the enriched messages into blob storage where the dashboard reads them.
"""

from __future__ import annotations

from pathlib import Path

from datatailr import Resources, Schedule, workflow

from trading_dashboard.email_workflow.tasks import (
    fetch_new_emails,
    publish_to_blob,
    summarize_with_ai,
)


REQUIREMENTS_PATH = str(Path(__file__).resolve().parent.parent / "requirements.txt")


@workflow(
    name="Trading Vendor Inbox AI",
    schedule=Schedule(at_minutes=[0, 15, 30, 45]),
    python_requirements=REQUIREMENTS_PATH,
    resources=Resources(memory="1g", cpu=1),
)
def vendor_inbox_workflow():
    raw = fetch_new_emails()
    enriched = summarize_with_ai(raw)
    publish_to_blob(enriched)


if __name__ == "__main__":
    vendor_inbox_workflow()
