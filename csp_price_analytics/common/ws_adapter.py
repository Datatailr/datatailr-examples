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
                    logger.info(f"WebSocketAdapter: connected to {self._ws_url}")
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        # Handle envelope messages from price engine: {"type": "...", "data": {...}}
                        if "type" in data and "data" in data:
                            data = data["data"]

                        symbol = data.get("symbol")
                        if symbol and symbol in self._adapters:
                            for adapter in self._adapters[symbol]:
                                adapter.push_tick(data)
            except Exception as e:
                if self._running:
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
