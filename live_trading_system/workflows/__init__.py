"""Workflows that participate in the reactive-graph ZMQ fabric.

Each module under this package defines tasks (``@task``-decorated
functions) and a workflow (``@workflow``-decorated function).  All
network communication happens over ZMQ via the helpers in
:mod:`live_trading_system.workflows.zmq_client` so workflow tasks are
indistinguishable from any other DEALER peer talking to the services.
"""
