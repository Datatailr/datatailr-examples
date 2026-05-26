"""Deploy the reactive-graph stock-exchange demo on Datatailr.

Three jobs:

* **market-feed** (Service) -- pure ZMQ service that simulates a stock
  exchange, broadcasting tick data to subscribed peers via ROUTER.
* **analytics-engine** (Service) -- pure ZMQ service that subscribes to
  the market feed via DEALER, validates every tick, computes rolling
  analytics (SMA, VWAP, volatility, …) and broadcasts the results.
* **Reactive Graph Dashboard** (App, FastAPI) -- subscribes to both
  services via ZMQ DEALER, displays prices, analytics, and the live
  message feed in a browser.  Includes controls (pause / resume, tick
  interval, symbol management, analytics window) that are sent to the
  nodes via ZMQ DEALER→ROUTER control frames.

All ZMQ communication uses each service's single port (8080).

Usage::

    python deploy.py                # everything
    python deploy.py services       # both ZMQ services
    python deploy.py feed           # market-feed only
    python deploy.py analytics      # analytics-engine only
    python deploy.py dashboard      # dashboard only
"""

from __future__ import annotations

import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
sys.path.append(str(current_dir.parent))

from datatailr import App, Resources, Service  # noqa: E402
from datatailr.logging import CYAN  # noqa: E402

import reactive_graph.dashboard.app as dashboard_module  # noqa: E402
from reactive_graph.node.app import main as node_main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
APP_SECTION = "Reactive Graph Demo"

MARKET_FEED_NAME = "market-feed"
ANALYTICS_ENGINE_NAME = "analytics-engine"
DASHBOARD_NAME = "Reactive Graph Dashboard"

SERVICE_PORT = 8080


def deploy_market_feed() -> None:
    print(CYAN(f"Deploying {MARKET_FEED_NAME} ..."))
    Service(
        name=MARKET_FEED_NAME,
        entrypoint=node_main,
        resources=Resources(memory="512m", cpu=1),
        python_requirements=REQUIREMENTS,
        env_vars={
            "NODE_NAME": MARKET_FEED_NAME,
            "NODE_ROLE": "market-feed",
            "TICK_SYMBOLS": "AAPL,GOOGL,MSFT,AMZN,TSLA",
            "TICK_INTERVAL_S": "1.0",
            "RECENT_BUFFER_SIZE": "2000",
        },
    ).run()


def deploy_analytics_engine() -> None:
    print(CYAN(f"Deploying {ANALYTICS_ENGINE_NAME} ..."))
    Service(
        name=ANALYTICS_ENGINE_NAME,
        entrypoint=node_main,
        resources=Resources(memory="512m", cpu=1),
        python_requirements=REQUIREMENTS,
        env_vars={
            "NODE_NAME": ANALYTICS_ENGINE_NAME,
            "NODE_ROLE": "analytics",
            "UPSTREAM_NODES": MARKET_FEED_NAME,
            "UPSTREAM_ZMQ_PORT": str(SERVICE_PORT),
            "ANALYTICS_WINDOW": "20",
            "RECENT_BUFFER_SIZE": "2000",
        },
    ).run()


def deploy_dashboard() -> None:
    print(CYAN(f"Deploying {DASHBOARD_NAME!r} ..."))
    App(
        name=DASHBOARD_NAME,
        entrypoint=dashboard_module,
        framework="fastapi",
        resources=Resources(memory="512m", cpu=1),
        app_section=APP_SECTION,
        python_requirements=REQUIREMENTS,
        env_vars={
            "REACTIVE_GRAPH_NODES": f"{MARKET_FEED_NAME},{ANALYTICS_ENGINE_NAME}",
            "ZMQ_PORT": str(SERVICE_PORT),
            "RECENT_BUFFER_SIZE": "2000",
        },
    ).run()


def deploy_services() -> None:
    deploy_market_feed()
    deploy_analytics_engine()


def deploy_all() -> None:
    deploy_services()
    deploy_dashboard()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    actions = {
        "all": deploy_all,
        "services": deploy_services,
        "feed": deploy_market_feed,
        "analytics": deploy_analytics_engine,
        "dashboard": deploy_dashboard,
        "app": deploy_dashboard,
    }
    fn = actions.get(cmd)
    if fn:
        fn()
    else:
        print(__doc__)
        sys.exit(1)
