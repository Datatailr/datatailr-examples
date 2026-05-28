"""Role-specific business logic for reactive-graph nodes.

Each module exposes a single ``run(node, config)`` function that wires
handlers onto a :class:`reactive_graph.node.transport.ZmqNode` and
starts the loop.  The role is selected by the ``NODE_ROLE`` environment
variable in :mod:`reactive_graph.node.app`.
"""
