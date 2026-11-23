"""Tests for the vLLM engine wrapper."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import time
from pathlib import Path

import pytest

from llm_service.gateway.engine import EngineError, VllmEngine, reasoning_parser


def test_vllm_command():
    engine = VllmEngine(8001, ["--max-model-len", "4096"])
    assert engine.command(Path("/w/m"), "org/m") == [
        "vllm", "serve", "/w/m", "--served-model-name", "org/m", "--host",
        "127.0.0.1", "--port", "8001", "--max-model-len", "4096",
    ]
    assert engine.base_url == "http://127.0.0.1:8001"


def _tokenizer_config(model_dir: Path, tokens: list[str]) -> None:
    decoder = {str(i): {"content": token, "special": True} for i, token in enumerate(tokens)}
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "tokenizer_config.json").write_text(json.dumps({"added_tokens_decoder": decoder}))


def test_a_thinking_model_gets_the_reasoning_parser(tmp_path):
    _tokenizer_config(tmp_path, ["<|im_end|>", "<think>", "</think>"])
    assert reasoning_parser(tmp_path) == "qwen3"
    command = VllmEngine(8001, ["--max-model-len", "4096"]).command(tmp_path, "org/m")
    assert command[-4:] == ["--reasoning-parser", "qwen3", "--max-model-len", "4096"]


def test_a_model_without_a_think_token_gets_no_parser(tmp_path):
    _tokenizer_config(tmp_path, ["<|im_end|>", "<|endoftext|>"])
    assert reasoning_parser(tmp_path) is None
    assert "--reasoning-parser" not in VllmEngine(8001, []).command(tmp_path, "org/m")


def test_the_think_token_is_also_found_in_tokenizer_json(tmp_path):
    (tmp_path / "tokenizer.json").write_text(json.dumps({"added_tokens": [{"content": "<think>"}]}))
    assert reasoning_parser(tmp_path) == "qwen3"


def test_a_missing_or_broken_tokenizer_file_gives_no_parser(tmp_path):
    assert reasoning_parser(tmp_path / "absent") is None
    (tmp_path / "tokenizer_config.json").write_text("{not json")
    assert reasoning_parser(tmp_path) is None


def test_an_own_reasoning_parser_argument_wins(tmp_path):
    _tokenizer_config(tmp_path, ["<think>", "</think>"])
    command = VllmEngine(8001, ["--reasoning-parser", "deepseek_r1"]).command(tmp_path, "org/m")
    assert command.count("--reasoning-parser") == 1
    assert command[-2:] == ["--reasoning-parser", "deepseek_r1"]
    joined = VllmEngine(8001, ["--reasoning-parser=deepseek_r1"]).command(tmp_path, "org/m")
    assert [arg for arg in joined if arg.startswith("--reasoning-parser")] == ["--reasoning-parser=deepseek_r1"]


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


def _server_code(port: int, name: str = "org/m", prefix: str = "") -> str:
    """Return child code that answers `/health` and `/v1/models`."""
    return (
        prefix
        + "import http.server, json\n"
        + "class H(http.server.BaseHTTPRequestHandler):\n"
        + "    def do_GET(self):\n"
        + f"        body = json.dumps({{'data': [{{'id': {name!r}}}]}}).encode()\n"
        + "        self.send_response(200); self.end_headers(); self.wfile.write(body)\n"
        + "    def log_message(self, *a): pass\n"
        + f"http.server.HTTPServer(('127.0.0.1', {port}), H).serve_forever()\n"
    )


def _gone(pid: int) -> bool:
    for _ in range(60):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


def test_start_returns_when_health_answers_and_stop_ends_process():
    port = _free_port()
    engine = _ScriptEngine(_server_code(port), startup_timeout=20, poll_interval=0.1)
    engine._port = port
    engine.base_url = f"http://127.0.0.1:{port}"

    async def run() -> None:
        await engine.start(Path("/w/m"), "org/m")
        assert engine.alive()
        await engine.stop()
        assert not engine.alive()
        await engine.stop()

    asyncio.run(run())


def _engine_on(code_for_port, **kwargs) -> _ScriptEngine:
    port = _free_port()
    engine = _ScriptEngine(code_for_port(port), **kwargs)
    engine._port = port
    engine.base_url = f"http://127.0.0.1:{port}"
    return engine


def test_start_refuses_when_port_is_in_use():
    async def run() -> None:
        server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        engine = _ScriptEngine("raise SystemExit(0)")
        engine._port = port
        with pytest.raises(EngineError, match="already in use"):
            await engine.start(Path("/w/m"), "org/m")
        assert engine._proc is None
        server.close()
        await server.wait_closed()

    asyncio.run(run())


def test_start_requires_served_name_in_models_list():
    engine = _engine_on(
        lambda port: _server_code(port, name="other/model"),
        startup_timeout=1.0,
        poll_interval=0.1,
    )

    async def run() -> None:
        with pytest.raises(EngineError, match="not healthy"):
            await engine.start(Path("/w/m"), "org/m")
        assert not engine.alive()

    asyncio.run(run())


def test_long_output_line_does_not_break_the_engine():
    prefix = "import sys\nsys.stdout.write('x' * 300000 + '\\n'); sys.stdout.flush()\n"
    engine = _engine_on(
        lambda port: _server_code(port, prefix=prefix),
        startup_timeout=20,
        poll_interval=0.1,
    )

    async def run() -> None:
        await engine.start(Path("/w/m"), "org/m")
        await engine.stop()
        await engine.start(Path("/w/m"), "org/m")
        assert engine.alive()
        await engine.stop()

    asyncio.run(run())


def test_stop_kills_grandchild_that_holds_the_pipe(tmp_path):
    pidfile = tmp_path / "grandchild.pid"
    prefix = (
        "import subprocess, sys\n"
        "gc = subprocess.Popen([sys.executable, '-c', "
        "'import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "time.sleep(120)'])\n"
        f"open({str(pidfile)!r}, 'w').write(str(gc.pid))\n"
    )
    engine = _engine_on(
        lambda port: _server_code(port, prefix=prefix),
        startup_timeout=20,
        poll_interval=0.1,
        stop_grace=0.5,
    )

    async def run() -> int:
        await engine.start(Path("/w/m"), "org/m")
        pid = int(pidfile.read_text())
        begin = time.monotonic()
        await engine.stop()
        assert time.monotonic() - begin < 10
        return pid

    pid = asyncio.run(run())
    assert _gone(pid)


def test_stop_when_not_running_does_nothing():
    asyncio.run(VllmEngine(8001, []).stop())


def test_stop_treats_a_permission_error_as_a_gone_group(monkeypatch):
    # On macOS, killpg can raise PermissionError for a group whose members
    # have all exited.
    def refuse(pid, sig):
        raise PermissionError(1, "Operation not permitted")

    async def run() -> None:
        engine = VllmEngine(_free_port(), [])
        engine._proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "pass", start_new_session=True
        )
        await engine._proc.wait()
        monkeypatch.setattr(os, "killpg", refuse)
        await asyncio.wait_for(engine.stop(), 10)
        assert engine._proc is None and not engine.alive()

    asyncio.run(run())
