"""Live Trading System node entry point.

Dispatches to a role-specific module based on the ``NODE_ROLE``
environment variable.  Each role module owns its business logic and
runs on top of :class:`live_trading_system.node.transport.ZmqNode`.

Recognised roles
----------------

============================  ===========================================
``NODE_ROLE``                  Module
============================  ===========================================
``market-feed``                :mod:`live_trading_system.node.roles.market_feed`
``analytics``                  :mod:`live_trading_system.node.roles.analytics`
``signal-engine``              :mod:`live_trading_system.node.roles.signals`
``risk-engine``                :mod:`live_trading_system.node.roles.risk`
``execution-simulator``        :mod:`live_trading_system.node.roles.execution`
``notification-bus``           :mod:`live_trading_system.node.roles.bus`
``persistence-sink``           :mod:`live_trading_system.node.roles.persistence`
============================  ===========================================

Common environment variables
----------------------------

NODE_NAME               logical name (defaults to ``NODE_ROLE``)
NODE_ROLE               role selector (see above)
UPSTREAM_NODES          comma-separated upstream specs ``host[:port]``
UPSTREAM_ZMQ_PORT       default upstream port when not in ``UPSTREAM_NODES``
SUB_REFRESH_S           re-send SUB to upstreams every N seconds (default 5)
DEALER_RECREATE_AFTER_S  recreate upstream DEALER if silent for this many
                        seconds (default 30); set to 0 to disable
"""

from __future__ import annotations

import logging
import os

from live_trading_system.node.transport import ZmqNode, parse_upstreams
from live_trading_system.node.roles import (
    analytics as analytics_role,
    bus as bus_role,
    execution as execution_role,
    market_feed as market_feed_role,
    persistence as persistence_role,
    risk as risk_role,
    signals as signals_role,
)

log = logging.getLogger("live_trading_system.node")

ROLES = {
    "market-feed": (market_feed_role.run, market_feed_role.config_from_env),
    "analytics": (analytics_role.run, analytics_role.config_from_env),
    "signal-engine": (signals_role.run, signals_role.config_from_env),
    "risk-engine": (risk_role.run, risk_role.config_from_env),
    "execution-simulator": (execution_role.run, execution_role.config_from_env),
    "notification-bus": (bus_role.run, bus_role.config_from_env),
    "persistence-sink": (persistence_role.run, persistence_role.config_from_env),
}


def main(port: int) -> None:
    """Datatailr entry point.  *port* is the platform-assigned port."""
    port = int(port)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    role = os.environ.get("NODE_ROLE", "market-feed")
    name = os.environ.get("NODE_NAME", role)
    if role not in ROLES:
        raise SystemExit(
            f"unknown NODE_ROLE={role!r}; expected one of {sorted(ROLES)}"
        )
    run_fn, config_fn = ROLES[role]

    sub_refresh = float(os.environ.get("SUB_REFRESH_S", "5"))
    node = ZmqNode(name=name, port=port, sub_refresh_s=sub_refresh)
    node.bind()

    default_port = int(os.environ.get("UPSTREAM_ZMQ_PORT", "8080"))
    for host, p in parse_upstreams(
        os.environ.get("UPSTREAM_NODES", ""), default_port
    ):
        node.connect_upstream(host, p)

    config = config_fn()
    log.info(
        "[%s] starting role=%s port=%d upstreams=%d",
        name, role, port, len(node.dealers),
    )
    run_fn(node, config)


if __name__ == "__main__":
    main(int(os.environ.get("PORT", 8080)))
