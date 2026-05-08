"""Datatailr deployment for the trading dashboard demo.

Registers and starts everything required for the dashboard to be live:

    Services
        - Trading Market Data       (synthetic ticking quotes)
        - Trading Strategy Engine   (live-marked PnL on top of the market data)

    Workflows
        - Trading Fundies Snapshot  (daily; writes fundamentals to blob)
        - Trading Vendor Inbox AI   (every 15 min; writes vendor emails + AI summaries to blob)

    App
        - Trading Dashboard         (Dash UI consuming the above)

Internal hostnames (used by the strategy engine and the dashboard) are
derived from the Service ``name`` parameter (lowercased, non-alphanumerics
replaced with hyphens). Keeping the names consistent here, in the strategy
engine code and in the dashboard code is intentional.
"""

from __future__ import annotations

import pathlib
import sys

current_dir = pathlib.Path(__file__).resolve().parent
sys.path.append(str(current_dir.parent))

from datatailr import App, Resources, Service

import trading_dashboard.dashboard.app as dashboard_entrypoint
from trading_dashboard.email_workflow.deploy import vendor_inbox_workflow
from trading_dashboard.fundies_workflow.deploy import fundies_workflow
from trading_dashboard.market_data_service.service import main as market_data_main
from trading_dashboard.strategy_engine.service import main as strategy_engine_main


REQUIREMENTS = str(current_dir / "requirements.txt")
APP_SECTION = "Trading Dashboard"


market_data = Service(
    name="Trading Market Data",
    entrypoint=market_data_main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements=REQUIREMENTS,
)

strategy_engine = Service(
    name="Trading Strategy Engine",
    entrypoint=strategy_engine_main,
    resources=Resources(memory="1g", cpu=1),
    python_requirements=REQUIREMENTS,
)

dashboard = App(
    name="Trading Dashboard",
    entrypoint=dashboard_entrypoint,
    framework="dash",
    resources=Resources(memory="2g", cpu=1),
    app_section=APP_SECTION,
    python_requirements=REQUIREMENTS,
)


if __name__ == "__main__":
    market_data.run()
    strategy_engine.run()
    dashboard.run()
    fundies_workflow()
    vendor_inbox_workflow()
