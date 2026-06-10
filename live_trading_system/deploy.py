"""Deploy the Live Trading System demo on Datatailr.

A reactive-graph trading pipeline: seven services, one dashboard, and
two scheduled workflows.  All deployable jobs share the same
``app_section`` so they appear together on the platform's launcher
page.

Services
--------

* **market-feed** -- simulates a stock exchange (ROUTER on 8080).
* **analytics-engine** -- validates ticks and computes rolling
  analytics; subscribes to market-feed.
* **signal-engine** -- turns analytics into trading signals; subscribes
  to analytics-engine.
* **risk-engine** -- applies pre-trade limits to signals and tracks
  positions; subscribes to signal-engine and execution-simulator.
* **execution-simulator** -- simulates broker fills for approved order
  intents; subscribes to risk-engine and market-feed (for last-price).
* **notification-bus** -- relays ``broadcast`` CTL frames as EVTs to
  every subscriber, letting workflow tasks publish into the live
  stream.
* **persistence-sink** -- subscribes to execution-simulator and flushes
  fills + position snapshots to Parquet in blob storage every N seconds
  (queryable from the dashboard + workflows via DuckDB).

App
---

* **Live Trading System Dashboard** (FastAPI) -- subscribes to every node
  via ZMQ DEALER and serves a single-page dashboard with topology,
  ticker, analytics, positions/PnL, blotter, and live system events.

Workflows
---------

See :mod:`live_trading_system.workflows_deploy`.

Usage::

    python live_trading_system/deploy.py                # services + dashboard
    python live_trading_system/deploy.py services       # all 7 services
    python live_trading_system/deploy.py feed           # market-feed only
    python live_trading_system/deploy.py analytics      # analytics-engine only
    python live_trading_system/deploy.py signals        # signal-engine only
    python live_trading_system/deploy.py risk           # risk-engine only
    python live_trading_system/deploy.py execution      # execution-simulator only
    python live_trading_system/deploy.py bus            # notification-bus only
    python live_trading_system/deploy.py persistence    # persistence-sink only
    python live_trading_system/deploy.py dashboard      # dashboard only
"""

from __future__ import annotations

import pathlib
import sys

current_dir = pathlib.Path(__file__).parent
sys.path.append(str(current_dir.parent))

from datatailr import App, Resources, Service  # noqa: E402
from datatailr.logging import CYAN  # noqa: E402

import live_trading_system.dashboard.app as dashboard_module  # noqa: E402
from live_trading_system.node.app import main as node_main  # noqa: E402

REQUIREMENTS = str(current_dir / "requirements.txt")
APP_SECTION = "Live Trading System"

MARKET_FEED_NAME = "market-feed"
ANALYTICS_ENGINE_NAME = "analytics-engine"
SIGNAL_ENGINE_NAME = "signal-engine"
RISK_ENGINE_NAME = "risk-engine"
EXECUTION_SIMULATOR_NAME = "execution-simulator"
NOTIFICATION_BUS_NAME = "notification-bus"
PERSISTENCE_SINK_NAME = "persistence-sink"
DASHBOARD_NAME = "Live Trading System Dashboard"

SERVICE_PORT = 8080
ALL_NODES = ",".join([
    MARKET_FEED_NAME,
    ANALYTICS_ENGINE_NAME,
    SIGNAL_ENGINE_NAME,
    RISK_ENGINE_NAME,
    EXECUTION_SIMULATOR_NAME,
    NOTIFICATION_BUS_NAME,
    PERSISTENCE_SINK_NAME,
])


def _service(
    name: str,
    role: str,
    upstream_nodes: str = "",
    *,
    extra_env: dict | None = None,
    memory: str = "512m",
    cpu: float = 1,
) -> Service:
    env_vars = {
        "NODE_NAME": name,
        "NODE_ROLE": role,
        "UPSTREAM_NODES": upstream_nodes,
        "UPSTREAM_ZMQ_PORT": str(SERVICE_PORT),
    }
    if extra_env:
        env_vars.update(extra_env)
    svc = Service(
        name=name,
        entrypoint=node_main,
        resources=Resources(memory=memory, cpu=cpu),
        python_requirements=REQUIREMENTS,
        env_vars=env_vars,
    )
    # Service() doesn't expose app_section in its __init__ signature, but
    # the parent Job class stores and serialises it.  Assign directly so
    # every service (and the dashboard App below) lands in the same
    # section on the Datatailr launcher page.
    svc.app_section = APP_SECTION
    return svc


def deploy_market_feed() -> None:
    print(CYAN(f"Deploying {MARKET_FEED_NAME} ..."))
    _service(
        MARKET_FEED_NAME,
        role="market-feed",
        extra_env={
            "TICK_SYMBOLS": "AAPL,GOOGL,MSFT,AMZN,TSLA",
            "TICK_INTERVAL_S": "1.0",
        },
    ).run()


def deploy_analytics_engine() -> None:
    print(CYAN(f"Deploying {ANALYTICS_ENGINE_NAME} ..."))
    _service(
        ANALYTICS_ENGINE_NAME,
        role="analytics",
        upstream_nodes=MARKET_FEED_NAME,
        extra_env={"ANALYTICS_WINDOW": "20"},
    ).run()


def deploy_signal_engine() -> None:
    print(CYAN(f"Deploying {SIGNAL_ENGINE_NAME} ..."))
    _service(
        SIGNAL_ENGINE_NAME,
        role="signal-engine",
        upstream_nodes=ANALYTICS_ENGINE_NAME,
        extra_env={
            "ENABLED_STRATEGIES": "momentum,mean_reversion",
            "SUGGESTED_QTY": "100",
            "MIN_SIGNAL_STRENGTH": "0.1",
            "SIGNAL_COOLDOWN_S": "1.5",
        },
    ).run()


def deploy_risk_engine() -> None:
    print(CYAN(f"Deploying {RISK_ENGINE_NAME} ..."))
    _service(
        RISK_ENGINE_NAME,
        role="risk-engine",
        upstream_nodes=f"{SIGNAL_ENGINE_NAME},{EXECUTION_SIMULATOR_NAME}",
        extra_env={
            "MAX_POSITION": "1000",
            "MAX_NOTIONAL": "250000",
            "MAX_DAILY_LOSS": "50000",
        },
    ).run()


def deploy_execution_simulator() -> None:
    print(CYAN(f"Deploying {EXECUTION_SIMULATOR_NAME} ..."))
    _service(
        EXECUTION_SIMULATOR_NAME,
        role="execution-simulator",
        upstream_nodes=f"{RISK_ENGINE_NAME},{MARKET_FEED_NAME}",
        extra_env={
            "SLIPPAGE_BPS": "5.0",
            "FILL_DELAY_S": "0.1",
        },
    ).run()


def deploy_notification_bus() -> None:
    print(CYAN(f"Deploying {NOTIFICATION_BUS_NAME} ..."))
    _service(
        NOTIFICATION_BUS_NAME,
        role="notification-bus",
        memory="256m",
        cpu=0.5,
    ).run()


def deploy_persistence_sink() -> None:
    print(CYAN(f"Deploying {PERSISTENCE_SINK_NAME} ..."))
    _service(
        PERSISTENCE_SINK_NAME,
        role="persistence-sink",
        upstream_nodes=EXECUTION_SIMULATOR_NAME,
        extra_env={
            "FLUSH_INTERVAL_S": "10",
            "FLUSH_MAX_BUFFER": "200",
        },
        memory="512m",
        cpu=0.5,
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
            "LIVE_TRADING_SYSTEM_NODES": ALL_NODES,
            "ZMQ_PORT": str(SERVICE_PORT),
            "RECENT_BUFFER_SIZE": "2000",
            "EDGE_WINDOW_S": "5",
        },
    ).run()


def deploy_services() -> None:
    deploy_market_feed()
    deploy_analytics_engine()
    deploy_signal_engine()
    deploy_risk_engine()
    deploy_execution_simulator()
    deploy_notification_bus()
    deploy_persistence_sink()


def deploy_all() -> None:
    deploy_services()
    deploy_dashboard()


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    actions = {
        "all": deploy_all,
        "services": deploy_services,
        "feed": deploy_market_feed,
        "market-feed": deploy_market_feed,
        "analytics": deploy_analytics_engine,
        "analytics-engine": deploy_analytics_engine,
        "signals": deploy_signal_engine,
        "signal-engine": deploy_signal_engine,
        "risk": deploy_risk_engine,
        "risk-engine": deploy_risk_engine,
        "execution": deploy_execution_simulator,
        "execution-simulator": deploy_execution_simulator,
        "bus": deploy_notification_bus,
        "notification-bus": deploy_notification_bus,
        "persistence": deploy_persistence_sink,
        "persistence-sink": deploy_persistence_sink,
        "dashboard": deploy_dashboard,
        "app": deploy_dashboard,
    }
    fn = actions.get(cmd)
    if fn:
        fn()
    else:
        print(__doc__)
        sys.exit(1)
