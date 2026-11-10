"""Tests for the vLLM engine wrapper."""

from __future__ import annotations

import asyncio
import socket
import sys
import time
from pathlib import Path

import pytest

from llm_service.gateway.engine import EngineError, VllmEngine


def test_vllm_command():
    engine = VllmEngine(8001, ["--max-model-len", "4096"])
    assert engine.command(Path("/w/m"), "org/m") == [
        "vllm", "serve", "/w/m", "--served-model-name", "org/m", "--host",
        "127.0.0.1", "--port", "8001", "--max-model-len", "4096",
    ]
    assert engine.base_url == "http://127.0.0.1:8001"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _ScriptEngine(VllmEngine):
    def __init__(self, code: str, **kwargs) -> None:
        super().__init__(_free_port(), [], **kwargs)
        self._code = code

    def command(self, model_dir, served_name):
        return [sys.executable, "-c", self._code]


def test_start_fails_fast_when_process_exits():
    engine = _ScriptEngine("print('boom'); raise SystemExit(3)")
    begin = time.monotonic()
    with pytest.raises(EngineError, match="boom"):
        asyncio.run(engine.start(Path("/w/m"), "org/m"))
    assert time.monotonic() - begin < 10
    assert not engine.alive()


def test_start_times_out_and_stops_the_process():
    engine = _ScriptEngine(
        "import time; print('slow', flush=True); time.sleep(60)",
        startup_timeout=0.5,
        poll_interval=0.1,
    )

    async def run() -> None:
        with pytest.raises(EngineError, match="slow"):
            await engine.start(Path("/w/m"), "org/m")
        assert not engine.alive()

    asyncio.run(run())


def test_start_returns_when_health_answers_and_stop_ends_process():
    port = _free_port()
    code = (
        f"PORT = {port}\n"
        "import http.server\n"
        "class H(http.server.BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        self.send_response(200); self.end_headers(); self.wfile.write(b'ok')\n"
        "    def log_message(self, *a): pass\n"
        "http.server.HTTPServer(('127.0.0.1', PORT), H).serve_forever()\n"
    )
    engine = VllmEngine(port, [], startup_timeout=20, poll_interval=0.1)
    engine.command = lambda d, n: [sys.executable, "-c", code]  # type: ignore

    async def run() -> None:
        await engine.start(Path("/w/m"), "org/m")
        assert engine.alive()
        await engine.stop()
        assert not engine.alive()
        await engine.stop()

    asyncio.run(run())


def test_stop_when_not_running_does_nothing():
    asyncio.run(VllmEngine(8001, []).stop())
