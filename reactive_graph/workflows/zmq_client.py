"""ZMQ helpers used by reactive-graph workflow tasks.

Workflow tasks run in ephemeral containers but talk to long-running
ZMQ services using exactly the same SUB/EVT/CTL framing the dashboard
and the other services use.  These helpers wrap a short-lived DEALER
socket so each task is a one-call self-contained client:

* :func:`ctl_request` -- DEALER -> CTL -> ROUTER -> CTL reply
  (round-trip request/response)
* :func:`broadcast` -- DEALER -> CTL ``broadcast`` to
  ``notification-bus`` (one-way fire-and-forget)
* :func:`sample_events` -- DEALER + SUB to one or more services for a
  fixed duration, returning every received event as a dict.

These helpers create their own :class:`zmq.Context` and close it before
returning so they are safe to call multiple times from the same Python
process (the platform reuses task containers across runs).
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Iterable, List, Tuple

import zmq

from reactive_graph.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]

log = logging.getLogger("reactive_graph.workflows.zmq_client")

DEFAULT_PORT = int(os.environ.get("REACTIVE_GRAPH_PORT", "8080"))
DEFAULT_TIMEOUT_MS = 5000


def _new_dealer(ctx: zmq.Context, identity: str, timeout_ms: int) -> zmq.Socket:
    dealer = ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, identity.encode("utf-8"))
    dealer.setsockopt(zmq.LINGER, 1000)
    dealer.setsockopt(zmq.RCVTIMEO, int(timeout_ms))
    dealer.setsockopt(zmq.SNDTIMEO, int(timeout_ms))
    dealer.setsockopt(zmq.RECONNECT_IVL, 500)
    dealer.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
    return dealer


def ctl_request(
    host: str,
    action: str,
    params: Dict[str, Any] | None = None,
    port: int = DEFAULT_PORT,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> dict:
    """Send a single CTL command to *host:port* and return the reply.

    Returns a dict; failures return ``{"ok": False, "error": "..."}``.
    """
    cmd: Dict[str, Any] = {"action": action}
    if params:
        cmd.update(params)
    ctx = zmq.Context()
    identity = f"workflow-ctl-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    dealer = _new_dealer(ctx, identity, timeout_ms)
    try:
        dealer.connect(f"tcp://{host}:{port}")
        dealer.send_multipart([b"CTL", json.dumps(cmd).encode("utf-8")])
        frames = dealer.recv_multipart()
        if len(frames) >= 2 and frames[0] == b"CTL":
            return json.loads(frames[1])
        return {"ok": False, "error": "unexpected response", "frames": len(frames)}
    except zmq.Again:
        return {"ok": False, "error": "timeout", "host": host, "action": action}
    except Exception as exc:  # noqa: BLE001
        log.warning("ctl_request to %s failed: %s", host, exc)
        return {"ok": False, "error": str(exc), "host": host, "action": action}
    finally:
        dealer.close()
        ctx.term()


def broadcast(
    bus_host: str,
    topic: str,
    *,
    kind: str = "system",
    summary: str = "",
    detail: str = "",
    source: str = "workflow",
    text: str | None = None,
    port: int = DEFAULT_PORT,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> dict:
    """Inject an event into the live stream via the notification-bus.

    For ``kind="system"`` the ``summary`` / ``detail`` / ``source``
    fields populate :class:`~reactive_graph.node.messages_pb2.SystemEvent`.
    For ``kind="text"`` the *text* parameter is used as the payload.
    """
    payload: Dict[str, Any] = {
        "topic": topic,
        "kind": kind,
    }
    if kind == "system":
        payload["system"] = {
            "kind": topic.split(".")[-1],
            "summary": summary,
            "detail": detail,
            "source": source,
        }
    else:
        payload["text"] = text or summary or topic
    return ctl_request(
        bus_host, "broadcast", payload, port=port, timeout_ms=timeout_ms
    )


def sample_events(
    hosts: Iterable[str],
    duration_s: float,
    port: int = DEFAULT_PORT,
) -> List[Dict[str, Any]]:
    """Subscribe to every host for *duration_s* seconds and return events.

    Useful inside a workflow task that wants to observe the live stream
    (for example to compute message-rate metrics).  Every event is
    returned as a dict with ``node``, ``topic``, ``kind``, ``from_node``,
    ``timestamp``, ``hops``, and ``data`` (a dict projection of the
    active oneof payload).
    """
    from reactive_graph.dashboard.app import _payload_to_dict  # local import

    ctx = zmq.Context()
    dealers: List[Tuple[str, zmq.Socket]] = []
    poller = zmq.Poller()
    try:
        for host in hosts:
            d = _new_dealer(ctx, f"workflow-sub-{host}-{os.getpid()}", 1000)
            d.connect(f"tcp://{host}:{port}")
            d.send(b"SUB")
            dealers.append((host, d))
            poller.register(d, zmq.POLLIN)

        deadline = time.time() + max(0.0, duration_s)
        out: List[Dict[str, Any]] = []
        while time.time() < deadline:
            remaining_ms = max(1, int((deadline - time.time()) * 1000))
            events = dict(poller.poll(timeout=min(500, remaining_ms)))
            for host, dealer in dealers:
                if dealer not in events:
                    continue
                while True:
                    try:
                        frames = dealer.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(frames) < 3 or frames[0] != b"EVT":
                        continue
                    topic = frames[1].decode("utf-8", "replace")
                    msg = GraphMessage()
                    try:
                        msg.ParseFromString(frames[2])
                    except Exception:  # noqa: BLE001
                        continue
                    out.append({
                        "node": host,
                        "topic": topic,
                        "kind": msg.kind,
                        "from_node": msg.from_node,
                        "timestamp": msg.timestamp,
                        "sequence": int(msg.sequence),
                        "hops": list(msg.hops),
                        "data": _payload_to_dict(msg),
                    })
        return out
    finally:
        for _, d in dealers:
            d.close()
        ctx.term()
