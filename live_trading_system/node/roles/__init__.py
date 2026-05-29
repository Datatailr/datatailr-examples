"""Role-specific business logic for reactive-graph nodes.

Each module exposes a single ``run(node, config)`` function that wires
handlers onto a :class:`live_trading_system.node.transport.ZmqNode` and
starts the loop.  The role is selected by the ``NODE_ROLE`` environment
variable in :mod:`live_trading_system.node.app`.
"""
