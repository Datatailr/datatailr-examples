"""notification-bus role: a tiny ZMQ topic exchange.

Most reactive-graph nodes both consume and produce events.  The
notification-bus is special: it only exposes a ROUTER socket and
re-emits whatever ``broadcast`` CTL command it receives as an ``EVT``
frame to every subscriber.  This lets **ephemeral processes** -- e.g.
workflow tasks for EOD reconciliation or pre-market warm-up -- inject
events into the live stream that the dashboard already consumes,
without any subscriber having to know about the workflow itself.

Recognised CTL commands::

    {"action": "broadcast", "topic": "system.market_open",
     "kind": "system",        # message kind
     "system": {              # SystemEvent fields (optional)
         "kind": "market_open", "summary": "...",
         "detail": "...", "source": "Pre-Market Warmup"
     }}

    {"action": "broadcast", "topic": "custom.<x>",
     "kind": "text", "text": "free-form payload"}

    {"action": "status"}  -- returns counters + recent broadcasts
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Deque, Dict

from reactive_graph.node.transport import ZmqNode

log = logging.getLogger("reactive_graph.bus")


class BusState:
    def __init__(self) -> None:
        self.total_broadcasts = 0
        self.recent: Deque[Dict] = deque(maxlen=200)


def run(node: ZmqNode, config: dict) -> None:
    state = BusState()

    def do_broadcast(cmd: dict) -> dict:
        topic = cmd.get("topic", "system")
        kind = cmd.get("kind", "system")
        msg = node.new_message(kind=kind)

        if kind == "system":
            sys_fields = cmd.get("system", {}) or {}
            msg.system.kind = str(sys_fields.get("kind", topic))
            msg.system.summary = str(sys_fields.get("summary", ""))
            msg.system.detail = str(sys_fields.get("detail", ""))
            msg.system.source = str(sys_fields.get("source", ""))
        else:
            msg.text = str(cmd.get("text", ""))

        node.broadcast(topic, msg)
        state.total_broadcasts += 1
        state.recent.append({
            "topic": topic,
            "kind": kind,
            "summary": (msg.system.summary if kind == "system" else msg.text)[:160],
            "source": msg.system.source if kind == "system" else cmd.get("source", ""),
            "at": time.time(),
        })
        return {
            "ok": True,
            "topic": topic,
            "kind": kind,
            "subscribers": len(node.subscribers),
        }

    def control(cmd: dict) -> dict:
        action = cmd.get("action", "")
        if action == "broadcast":
            return do_broadcast(cmd)
        if action in ("status", "snapshot"):
            return node.status_snapshot({
                "role": "notification-bus",
                "total_broadcasts": state.total_broadcasts,
                "recent": list(state.recent),
            })
        return {"ok": False, "error": f"unknown action: {action!r}"}

    node.on_control(control)
    log.info("[%s] notification-bus ready", node.name)
    node.run()


def config_from_env() -> dict:
    return {}
