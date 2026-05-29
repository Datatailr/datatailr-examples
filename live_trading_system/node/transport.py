"""Shared ZMQ ROUTER/DEALER transport for reactive-graph nodes.

Every reactive-graph node binds a single **ROUTER** socket on its
platform-assigned port and exchanges three frame types with peers that
connect via **DEALER**:

  +------------------+-------------------------------------+-------------------------+
  | Direction        | Frames                              | Meaning                 |
  +------------------+-------------------------------------+-------------------------+
  | DEALER -> ROUTER | ``[b"SUB"]``                        | register subscriber     |
  | ROUTER -> DEALER | ``[b"EVT", topic, protobuf]``       | event broadcast         |
  | DEALER -> ROUTER | ``[b"CTL", json_bytes]``            | control command         |
  | ROUTER -> DEALER | ``[b"CTL", json_bytes]``            | control reply           |
  +------------------+-------------------------------------+-------------------------+

`ZmqNode` packages this protocol so the role-specific service modules
(market-feed, analytics, signal-engine, risk-engine, execution-simulator,
notification-bus) can focus on business logic.  Callers register
callbacks for control commands and upstream events, optionally an idle
callback that drives time-based behaviour (e.g. tick generation), and
call :meth:`ZmqNode.run`.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import zmq

from live_trading_system.node.messages_pb2 import GraphMessage  # type: ignore[import-not-found]

log = logging.getLogger("live_trading_system.transport")

EventHandler = Callable[[str, GraphMessage], None]
ControlHandler = Callable[[dict], dict]
IdleHandler = Callable[[], Optional[float]]

DEFAULT_SUB_REFRESH_S = 5.0

# How long an upstream DEALER may go without receiving any EVT before we
# tear it down and re-open the socket.  Restarts in containerised /
# platform-managed environments often shift the upstream to a new IP;
# ZMQ caches DNS at connect() time, so without a recreate the DEALER
# stays stuck pointing at the old IP forever.
DEFAULT_DEALER_RECREATE_AFTER_S = 30.0


class ZmqNode:
    """ROUTER + (optional) upstream DEALERs with SUB/EVT/CTL framing.

    Upstream DEALERs are tracked in ``self._upstreams``; the legacy
    ``self.dealers`` attribute is preserved as a derived read-only list
    of ``(host, socket)`` tuples for compatibility with existing roles
    that just call ``len(node.dealers)``.

    Each upstream auto-recovers from silent / stuck connections: if no
    EVT arrives for ``dealer_recreate_after_s`` seconds the DEALER is
    closed and re-opened (forcing DNS re-resolution and a fresh peer-id
    handshake with the upstream's ROUTER).
    """

    def __init__(
        self,
        name: str,
        port: int,
        sub_refresh_s: float = DEFAULT_SUB_REFRESH_S,
        dealer_recreate_after_s: Optional[float] = None,
    ) -> None:
        self.name = name
        self.port = int(port)
        self.sub_refresh_s = float(sub_refresh_s)
        if dealer_recreate_after_s is None:
            dealer_recreate_after_s = float(
                os.environ.get(
                    "DEALER_RECREATE_AFTER_S",
                    DEFAULT_DEALER_RECREATE_AFTER_S,
                )
            )
        self.dealer_recreate_after_s = float(dealer_recreate_after_s)

        self.ctx = zmq.Context.instance()
        self.router: Optional[zmq.Socket] = None
        self._upstreams: List[Dict[str, Any]] = []
        self.subscribers: Set[bytes] = set()
        self.poller = zmq.Poller()

        self._seq = 0
        self._on_event: Optional[EventHandler] = None
        self._on_control: Optional[ControlHandler] = None
        self._on_idle: Optional[IdleHandler] = None
        self._last_sub_refresh = 0.0
        self._running = False

        self.total_published = 0
        self.total_received = 0
        self.started_at = time.time()

    @property
    def dealers(self) -> List[Tuple[str, zmq.Socket]]:
        """Backwards-compat view of upstream DEALERs as ``(host, sock)``."""
        return [(u["host"], u["socket"]) for u in self._upstreams]

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def bind(self) -> None:
        router = self.ctx.socket(zmq.ROUTER)
        router.setsockopt(zmq.LINGER, 200)
        router.setsockopt(zmq.SNDHWM, 100_000)
        router.setsockopt(zmq.RCVHWM, 100_000)
        router.bind(f"tcp://*:{self.port}")
        self.router = router
        self.poller.register(router, zmq.POLLIN)
        log.info("[%s] ROUTER bound on port %d", self.name, self.port)

    def _open_dealer(self, host: str, port: int) -> zmq.Socket:
        """Create + connect a DEALER and send an initial SUB.

        The DEALER identity is suffixed with ``pid+ts`` so when this
        node recreates a socket the upstream ROUTER sees it as a fresh
        peer (rather than reusing a stale entry in its subscriber set).
        """
        dealer = self.ctx.socket(zmq.DEALER)
        identity = (
            f"{self.name}-to-{host}-{os.getpid()}-{int(time.time() * 1000)}"
        ).encode("utf-8")
        dealer.setsockopt(zmq.IDENTITY, identity)
        dealer.setsockopt(zmq.LINGER, 200)
        dealer.setsockopt(zmq.SNDHWM, 100_000)
        dealer.setsockopt(zmq.RCVHWM, 100_000)
        dealer.setsockopt(zmq.RECONNECT_IVL, 500)
        dealer.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)
        try:
            dealer.setsockopt(zmq.TCP_KEEPALIVE, 1)
            dealer.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)
            dealer.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 10)
        except (zmq.ZMQError, AttributeError):
            pass
        dealer.connect(f"tcp://{host}:{port}")
        try:
            dealer.send(b"SUB", zmq.NOBLOCK)
        except zmq.ZMQError:
            pass
        self.poller.register(dealer, zmq.POLLIN)
        return dealer

    def connect_upstream(self, host: str, port: int) -> zmq.Socket:
        """Open a DEALER to an upstream ROUTER and register as subscriber."""
        endpoint = f"tcp://{host}:{port}"
        dealer = self._open_dealer(host, int(port))
        now = time.time()
        info = {
            "host": host,
            "port": int(port),
            "socket": dealer,
            "connected_at": now,
            "last_recv_at": now,
            "events_received": 0,
            "recreations": 0,
        }
        self._upstreams.append(info)
        log.info(
            "[%s] DEALER connected to %s (subscribed)", self.name, endpoint,
        )
        return dealer

    def _recreate_upstream(self, info: Dict[str, Any], reason: str) -> None:
        """Close + re-open the DEALER for a single upstream entry."""
        old = info["socket"]
        endpoint = f"tcp://{info['host']}:{info['port']}"
        log.warning(
            "[%s] recreating DEALER to %s after %s (recreations=%d)",
            self.name, endpoint, reason, info["recreations"] + 1,
        )
        try:
            self.poller.unregister(old)
        except (KeyError, zmq.ZMQError):
            pass
        try:
            old.close(linger=0)
        except zmq.ZMQError:
            pass
        new = self._open_dealer(info["host"], info["port"])
        info["socket"] = new
        info["connected_at"] = time.time()
        info["last_recv_at"] = time.time()
        info["recreations"] += 1

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def on_event(self, handler: EventHandler) -> None:
        """Register a callback for ``[EVT, topic, protobuf]`` from upstream."""
        self._on_event = handler

    def on_control(self, handler: ControlHandler) -> None:
        """Register a callback for ``[CTL, json]`` commands.

        The handler returns a JSON-serialisable dict that is sent back as
        the control reply.
        """
        self._on_control = handler

    def on_idle(self, handler: IdleHandler) -> None:
        """Register an idle callback driven by the poll loop.

        The handler is called every iteration of the loop.  It may
        return the desired next poll timeout in seconds; if it returns
        ``None`` the default 100 ms timeout is used.
        """
        self._on_idle = handler

    # ------------------------------------------------------------------
    # Message factory + broadcast
    # ------------------------------------------------------------------

    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def new_message(
        self,
        kind: str,
        to_node: str = "",
        parent: Optional[object] = None,
    ) -> GraphMessage:
        """Build a GraphMessage with header fields populated.

        If *parent* is provided, the new message inherits its ``hops``
        and ``correlation_id`` so the dashboard can reconstruct the
        full path the lineage has taken through the graph.  *parent*
        may be a :class:`GraphMessage`, a dict carrying ``hops`` /
        ``correlation_id`` keys (useful when the original protobuf is
        not retained), or an iterable of hop names.
        """
        msg = GraphMessage(
            id=str(uuid.uuid4()),
            kind=kind,
            from_node=self.name,
            to_node=to_node,
            timestamp=time.time(),
            sequence=self.next_seq(),
        )

        upstream_hops: List[str] = []
        correlation_id = ""
        if parent is None:
            pass
        elif isinstance(parent, GraphMessage):
            upstream_hops = list(parent.hops)
            correlation_id = parent.correlation_id or parent.id
        elif isinstance(parent, dict):
            raw_hops = parent.get("hops") or []
            upstream_hops = [str(h) for h in raw_hops]
            correlation_id = (
                str(parent.get("correlation_id") or parent.get("id") or "")
            )
        else:
            try:
                upstream_hops = [str(h) for h in parent]  # type: ignore[union-attr]
            except TypeError:
                pass

        msg.correlation_id = correlation_id or msg.id
        for h in upstream_hops:
            msg.hops.append(h)
        if not list(msg.hops) or list(msg.hops)[-1] != self.name:
            msg.hops.append(self.name)
        return msg

    def broadcast(self, topic: str, msg: GraphMessage) -> None:
        """Send ``[EVT, topic, protobuf]`` to every subscribed peer."""
        if self.router is None or not self.subscribers:
            return
        payload = msg.SerializeToString()
        topic_bytes = topic.encode("utf-8")
        for peer_id in list(self.subscribers):
            try:
                self.router.send_multipart(
                    [peer_id, b"EVT", topic_bytes, payload], zmq.NOBLOCK
                )
            except zmq.ZMQError:
                pass
        self.total_published += 1

    def forward(self, topic: str, msg: GraphMessage) -> None:
        """Re-broadcast an incoming message, stamping our node into ``hops``."""
        if self.name not in list(msg.hops):
            msg.hops.append(self.name)
        self.broadcast(topic, msg)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run the poll loop until interrupted."""
        if self.router is None:
            self.bind()
        self._running = True
        self._last_sub_refresh = time.time()
        log.info(
            "[%s] main loop starting (sub_refresh=%.1fs recreate_after=%.1fs)",
            self.name, self.sub_refresh_s, self.dealer_recreate_after_s,
        )
        try:
            while self._running:
                now = time.time()

                # Auto-recreate DEALERs that have been silent for too long
                # (the upstream container may have moved to a new IP).
                if self._upstreams and self.dealer_recreate_after_s > 0:
                    for info in self._upstreams:
                        silent_for = now - info["last_recv_at"]
                        if silent_for >= self.dealer_recreate_after_s:
                            self._recreate_upstream(
                                info, reason=f"silent {silent_for:.0f}s",
                            )

                # Periodically re-subscribe to upstreams (handles restarts
                # where the upstream's subscriber set was cleared).
                if (
                    self._upstreams
                    and now - self._last_sub_refresh >= self.sub_refresh_s
                ):
                    for info in self._upstreams:
                        try:
                            info["socket"].send(b"SUB", zmq.NOBLOCK)
                        except zmq.ZMQError:
                            pass
                    self._last_sub_refresh = now

                next_timeout: Optional[float] = None
                if self._on_idle is not None:
                    try:
                        next_timeout = self._on_idle()
                    except Exception:  # noqa: BLE001
                        log.exception("[%s] idle handler raised", self.name)

                timeout_ms = (
                    max(1, int(next_timeout * 1000))
                    if next_timeout is not None
                    else 100
                )
                events = dict(self.poller.poll(timeout=timeout_ms))

                if self.router in events:
                    self._drain_router()

                for info in self._upstreams:
                    if info["socket"] in events:
                        self._drain_dealer(info)
        except KeyboardInterrupt:
            log.info("[%s] interrupted", self.name)
        finally:
            self.close()

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        if self.router is not None:
            self.router.close()
            self.router = None
        for info in self._upstreams:
            try:
                info["socket"].close()
            except zmq.ZMQError:
                pass
        self._upstreams = []
        log.info("[%s] shutdown complete", self.name)

    # ------------------------------------------------------------------
    # Internal frame dispatch
    # ------------------------------------------------------------------

    def _drain_router(self) -> None:
        assert self.router is not None
        while True:
            try:
                frames = self.router.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            if len(frames) < 2:
                continue
            peer_id = bytes(frames[0])
            cmd = frames[1]

            if cmd == b"SUB":
                if peer_id not in self.subscribers:
                    log.info(
                        "[%s] subscriber registered: %r", self.name, peer_id
                    )
                self.subscribers.add(peer_id)
                continue

            if cmd == b"CTL" and len(frames) >= 3:
                try:
                    ctl_data = json.loads(frames[2])
                except (json.JSONDecodeError, TypeError):
                    ctl_data = {"action": ""}
                try:
                    if self._on_control is not None:
                        result: Dict = self._on_control(ctl_data)
                    else:
                        result = {"ok": False, "error": "no control handler"}
                except Exception as exc:  # noqa: BLE001
                    log.exception("[%s] control handler raised", self.name)
                    result = {"ok": False, "error": str(exc)}
                try:
                    self.router.send_multipart(
                        [peer_id, b"CTL", json.dumps(result).encode()],
                        zmq.NOBLOCK,
                    )
                except zmq.ZMQError:
                    pass
                action = (
                    ctl_data.get("action") if isinstance(ctl_data, dict) else "?"
                )
                log.info(
                    "[%s] control: %s -> ok=%s",
                    self.name,
                    action,
                    result.get("ok"),
                )

    def _drain_dealer(self, info: Dict[str, Any]) -> None:
        dealer = info["socket"]
        while True:
            try:
                frames = dealer.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            if len(frames) < 3 or frames[0] != b"EVT":
                continue
            topic = frames[1].decode("utf-8", "replace")
            msg = GraphMessage()
            try:
                msg.ParseFromString(frames[2])
            except Exception as exc:  # noqa: BLE001
                log.warning("[%s] bad protobuf: %s", self.name, exc)
                continue
            self.total_received += 1
            info["last_recv_at"] = time.time()
            info["events_received"] += 1
            if self._on_event is not None:
                try:
                    self._on_event(topic, msg)
                except Exception:  # noqa: BLE001
                    log.exception("[%s] event handler raised", self.name)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def upstream_status(self) -> List[Dict[str, Any]]:
        """Per-upstream snapshot for status / diagnostics CTL replies."""
        now = time.time()
        out: List[Dict[str, Any]] = []
        for info in self._upstreams:
            out.append({
                "host": info["host"],
                "port": info["port"],
                "events_received": info["events_received"],
                "recreations": info["recreations"],
                "connected_for_s": round(now - info["connected_at"], 1),
                "silent_for_s": round(now - info["last_recv_at"], 1),
            })
        return out

    def status_snapshot(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Common status payload that every role can fold into its CTL reply.

        Returns a dict with uptime, total counters, subscriber count, and
        per-upstream diagnostics.  Roles can ``return {**node.status_snapshot(), **role_specific}``.
        """
        base: Dict[str, Any] = {
            "ok": True,
            "node_name": self.name,
            "uptime_s": round(time.time() - self.started_at, 1),
            "total_received": self.total_received,
            "total_published": self.total_published,
            "subscribers": len(self.subscribers),
            "upstreams_connected": len(self._upstreams),
            "upstreams": self.upstream_status(),
        }
        if extra:
            base.update(extra)
        return base


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_upstreams(raw: str, default_port: int) -> List[Tuple[str, int]]:
    """Parse ``"host[:port],host[:port]"`` env-style strings."""
    result: List[Tuple[str, int]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else default_port
        result.append((host, port))
    return result
