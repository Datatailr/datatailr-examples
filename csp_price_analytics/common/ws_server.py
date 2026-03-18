import json
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)


class WebSocketBroadcaster:
    """Manages WebSocket clients connected via flask-sock and broadcasts messages to all of them.

    Usage:
        broadcaster = WebSocketBroadcaster("market-data")
        # Register the /ws route:
        @sock.route("/ws")
        def ws_handler(ws):
            broadcaster.handle(ws)
        # From any thread (e.g. CSP engine):
        broadcaster.broadcast({"symbol": "AAPL", "price": 185.0})
    """

    def __init__(self, name: str = "ws-broadcaster"):
        self._name = name
        self._clients: set = set()
        self._lock = threading.Lock()

    def handle(self, ws):
        """Called by flask-sock route handler. Blocks until the client disconnects."""
        with self._lock:
            self._clients.add(ws)
        logger.info(f"{self._name}: client connected ({len(self._clients)} total)")
        try:
            while True:
                ws.receive(timeout=60)
        except Exception:
            pass
        finally:
            with self._lock:
                self._clients.discard(ws)
            logger.info(f"{self._name}: client disconnected ({len(self._clients)} total)")

    def broadcast(self, data: dict[str, Any]):
        """Thread-safe broadcast from any thread (e.g. the CSP engine thread)."""
        with self._lock:
            if not self._clients:
                return
            clients = self._clients.copy()

        message = json.dumps(data)
        dead = set()
        for ws in clients:
            try:
                ws.send(message)
            except Exception:
                dead.add(ws)

        if dead:
            with self._lock:
                self._clients -= dead
