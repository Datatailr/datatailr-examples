import asyncio
import json
import logging
import threading
import time

import csp
from csp import ts
from csp.impl.adaptermanager import AdapterManagerImpl
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def

import websockets

logger = logging.getLogger(__name__)

# Global registry of all adapter manager instances so services can query connection stats
_adapter_managers: list["WebSocketAdapterManagerImpl"] = []


def get_ws_stats() -> list[dict]:
    """Return connection stats for all active WebSocket adapter managers."""
    return [mgr.get_stats() for mgr in _adapter_managers]


class WebSocketAdapterManager:
    """Graph-time representation of a WebSocket data source that feeds multiple symbol streams."""

    def __init__(self, ws_url: str, reconnect_interval: float = 2.0):
        self._ws_url = ws_url
        self._reconnect_interval = reconnect_interval

    def subscribe(self, symbol: str, push_mode=csp.PushMode.LAST_VALUE):
        return WebSocketInput(self, symbol, push_mode=push_mode)

    def _create(self, engine, memo):
        return WebSocketAdapterManagerImpl(engine, self._ws_url, self._reconnect_interval)


class WebSocketAdapterManagerImpl(AdapterManagerImpl):
    """Runtime implementation that connects to a WebSocket and dispatches messages to per-symbol adapters."""

    def __init__(self, engine, ws_url: str, reconnect_interval: float):
        super().__init__(engine)
        self._ws_url = ws_url
        self._reconnect_interval = reconnect_interval
        self._adapters: dict[str, list] = {}
        self._running = False
        self._thread: threading.Thread | None = None

        self._msgs_received = 0
        self._msgs_dispatched = 0
        self._connected_at: float | None = None
        self._last_msg_at: float | None = None
        self._reconnect_count = 0
        self._connected = False

        _adapter_managers.append(self)

    def get_stats(self) -> dict:
        now = time.time()
        uptime = None
        if self._connected and self._connected_at is not None:
            uptime = round(now - self._connected_at, 1)
        last_msg_ago = None
        if self._last_msg_at is not None:
            last_msg_ago = round(now - self._last_msg_at, 1)
        return {
            "source": self._ws_url,
            "connected": self._connected,
            "uptime_seconds": uptime,
            "messages_received": self._msgs_received,
            "messages_dispatched": self._msgs_dispatched,
            "last_message_seconds_ago": last_msg_ago,
            "reconnect_count": self._reconnect_count,
            "symbols_subscribed": len(self._adapters),
        }

    def start(self, starttime, endtime):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def register_input_adapter(self, symbol: str, adapter):
        if symbol not in self._adapters:
            self._adapters[symbol] = []
        self._adapters[symbol].append(adapter)

    def process_next_sim_timeslice(self, now):
        return None

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._consume())

    async def _consume(self):
        while self._running:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    self._connected = True
                    self._connected_at = time.time()
                    logger.info(f"WebSocketAdapter: connected to {self._ws_url}")
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        self._msgs_received += 1
                        self._last_msg_at = time.time()

                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if "type" in data and "data" in data:
                            data = data["data"]

                        symbol = data.get("symbol")
                        if symbol and symbol in self._adapters:
                            for adapter in self._adapters[symbol]:
                                adapter.push_tick(data)
                            self._msgs_dispatched += 1
            except Exception as e:
                self._connected = False
                if self._running:
                    self._reconnect_count += 1
                    logger.warning(f"WebSocketAdapter: connection lost ({e}), reconnecting in {self._reconnect_interval}s")
                    time.sleep(self._reconnect_interval)


class WebSocketInputImpl(PushInputAdapter):
    """Runtime per-symbol adapter that registers itself with the manager."""

    def __init__(self, manager_impl, symbol):
        self._symbol = symbol
        manager_impl.register_input_adapter(symbol, self)
        super().__init__()


WebSocketInput = py_push_adapter_def(
    "WebSocketInput",
    WebSocketInputImpl,
    ts[dict],
    WebSocketAdapterManager,
    symbol=str,
)
