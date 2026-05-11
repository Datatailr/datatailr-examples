"""Top-level deploy for the EIA Weekly Oil Reports demo.

Deploys two jobs:

  - "EIA Weekly Oil Report Ingestion" : scheduled workflow that downloads
    https://ir.eia.gov/wpsr/table9.csv on Wed/Thu/Fri at 16:00 UTC, parses
    it, and stores both raw and tidy versions in blob storage.
  - "EIA Weekly Oil Report Dashboard" : Streamlit app that reads every
    stored report and renders KPIs, time-series, and a section browser.

Usage:
    python weekly_oil_reports/deploy.py            # deploy both
    python weekly_oil_reports/deploy.py workflow   # deploy ingestion only
    python weekly_oil_reports/deploy.py dashboard  # deploy app only
"""

from __future__ import annotations

import sys
from pathlib import Path

from datatailr import App, Resources

import weekly_oil_reports.dashboard.app as dashboard_entrypoint
from weekly_oil_reports.ingest_workflow.deploy import weekly_oil_report_ingestion

REQUIREMENTS = str(Path(__file__).parent / "requirements.txt")


def deploy_workflow():
    weekly_oil_report_ingestion()


def deploy_dashboard():
    app = App(
        name="EIA Weekly Oil Report Dashboard",
        entrypoint=dashboard_entrypoint,
        framework="streamlit",
        resources=Resources(memory="1g", cpu=0.5),
        app_section="Weekly Oil Reports",
        python_requirements=REQUIREMENTS,
    )
    app.run()


def deploy_all():
    deploy_workflow()
    deploy_dashboard()


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    if command == "all":
        deploy_all()
    elif command == "workflow":
        deploy_workflow()
    elif command == "dashboard":
        deploy_dashboard()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)
