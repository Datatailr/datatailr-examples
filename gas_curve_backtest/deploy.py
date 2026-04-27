"""One-shot deployment of the gas curve backtest demo.

Components:
  - parent workflow (data + signals + regime detection + dynamic child)
  - dashboard app (Streamlit cockpit, all pages bundled)

The child workflow is *not* deployed up-front: it is generated and
deployed by the parent's `detect_regimes_and_launch` task at runtime,
which is the entire point of the demo.

Usage:
    python deploy.py                 # deploy workflow + dashboard
    python deploy.py workflow        # workflow only
    python deploy.py app             # dashboard only
    python deploy.py run             # also kick off one parent run
"""

from __future__ import annotations

import sys
from pathlib import Path

from datatailr import App, Resources
from datatailr.logging import CYAN

import gas_curve_backtest.dashboard.app as dashboard_entrypoint
from gas_curve_backtest.workflows.parent_workflow import (
    make_run_id,
    parent_backtest_workflow,
)

REQUIREMENTS = str(Path(__file__).parent / "requirements.txt")


def deploy_workflow(run_now: bool = False) -> str | None:
    print(CYAN("Saving parent backtest workflow..."))
    parent_backtest_workflow(save_only=True)
    if run_now:
        rid = make_run_id()
        print(CYAN(f"Triggering a parent run with run_id={rid}"))
        parent_backtest_workflow(rid)
        return rid
    return None


def deploy_dashboard() -> None:
    print(CYAN("Deploying Streamlit cockpit..."))
    app = App(
        name="Gas Curve Backtest",
        entrypoint=dashboard_entrypoint,
        framework="streamlit",
        resources=Resources(memory="2g", cpu=1),
        app_section="Gas Curve Backtest",
        python_requirements=REQUIREMENTS,
    )
    app.run()


def deploy_all(run_now: bool = False) -> None:
    deploy_workflow(run_now=run_now)
    deploy_dashboard()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "all":
        deploy_all(run_now=False)
    elif cmd == "workflow":
        deploy_workflow(run_now=False)
    elif cmd == "app":
        deploy_dashboard()
    elif cmd == "run":
        deploy_workflow(run_now=True)
    else:
        print(__doc__)
        sys.exit(1)
