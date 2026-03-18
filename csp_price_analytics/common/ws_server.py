import asyncio
import json
import logging
import threading
from typing import Any

import websockets

logger = logging.getLogger(__name__)


class WebSocketBroadcastServer:
    """Runs a WebSocket server on a background thread, broadcasting messages to all connected clients."""

    def __init__(self, port: int, name: str = "ws-server"):
        self._port = port
        self._name = name
        self._clients: set = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name=self._name)
        self._thread.start()

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())

    async def _serve(self):
        async with websockets.serve(self._handler, "0.0.0.0", self._port):
            logger.info(f"{self._name}: WebSocket server listening on port {self._port}")
            await asyncio.Future()  # run forever

    async def _handler(self, ws):
        self._clients.add(ws)
        logger.info(f"{self._name}: client connected ({len(self._clients)} total)")
        try:
            async for _msg in ws:
                pass  # broadcast-only server; ignore incoming messages
        except websockets.ConnectionClosed:
            pass
        finally:
            self._clients.discard(ws)
            logger.info(f"{self._name}: client disconnected ({len(self._clients)} total)")

    def broadcast(self, data: dict[str, Any]):
        """Thread-safe broadcast from any thread (e.g. the CSP engine thread)."""
        if self._loop is None or not self._clients:
            return
        message = json.dumps(data)
        asyncio.run_coroutine_threadsafe(self._broadcast_async(message), self._loop)

    async def _broadcast_async(self, message: str):
        if not self._clients:
            return
        dead = set()
        for ws in self._clients.copy():
            try:
                await ws.send(message)
            except websockets.ConnectionClosed:
                dead.add(ws)
        self._clients -= dead
