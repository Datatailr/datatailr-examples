"""The vLLM child process."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import os
import signal
from pathlib import Path
from typing import Protocol

import httpx

log = logging.getLogger("llm_service.engine")

TAIL_LINES = 40
READER_TIMEOUT = 5.0
CHUNK = 65536


class EngineError(Exception):
    """The engine failed to start or to become healthy."""


class Engine(Protocol):
    """An inference engine that serves one model at a time."""

    base_url: str

    async def start(self, model_dir: Path, served_name: str) -> None:
        """Start the engine. Return when it answers its health check."""

    async def stop(self) -> None:
        """Stop the engine. Do nothing if it is not running."""

    def alive(self) -> bool:
        """Return true while the engine process runs."""


class VllmEngine:
    """Run `vllm serve` as a child process in its own process group."""

    def __init__(
        self,
        port: int,
        extra_args: list[str],
        startup_timeout: float = 1800.0,
        poll_interval: float = 2.0,
        stop_grace: float = 30.0,
    ) -> None:
        self._port = port
        self._extra_args = list(extra_args)
        self._startup_timeout = startup_timeout
        self._poll_interval = poll_interval
        self._stop_grace = stop_grace
        self._proc: asyncio.subprocess.Process | None = None
        self._reader: asyncio.Task | None = None
        self._tail: collections.deque[str] = collections.deque(maxlen=TAIL_LINES)
        self.base_url = f"http://127.0.0.1:{port}"

    def command(self, model_dir: Path, served_name: str) -> list[str]:
        """Return the command line that starts vLLM."""
        return [
            "vllm", "serve", str(model_dir),
            "--served-model-name", served_name,
            "--host", "127.0.0.1",
            "--port", str(self._port),
            *self._extra_args,
        ]

    def alive(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def _port_in_use(self) -> bool:
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", self._port)
        except OSError:
            return False
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
        return True

    async def start(self, model_dir: Path, served_name: str) -> None:
        await self.stop()
        if await self._port_in_use():
            raise EngineError(f"port {self._port} is already in use")
        self._tail.clear()
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self.command(model_dir, served_name),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as exc:
            raise EngineError(f"cannot run the engine: {exc}") from exc
        self._reader = asyncio.create_task(self._forward_output(self._proc))
        try:
            await self._wait_healthy(served_name)
        except BaseException:
            await self.stop()
            raise

    async def _forward_output(self, proc: asyncio.subprocess.Process) -> None:
        """Copy the child output to the log. Never raise, and keep draining."""
        assert proc.stdout is not None
        pending = b""
        try:
            while True:
                chunk = await proc.stdout.read(CHUNK)
                if not chunk:
                    break
                pending += chunk
                *lines, pending = pending.split(b"\n")
                if len(pending) >= CHUNK:
                    lines.append(pending)
                    pending = b""
                for raw in lines:
                    self._record(raw)
            if pending:
                self._record(pending)
        except Exception:  # noqa: BLE001
            log.exception("cannot read the engine output")

    def _record(self, raw: bytes) -> None:
        line = raw.decode(errors="replace").rstrip()
        self._tail.append(line[:2000])
        log.info("vllm: %s", line[:2000])

    def _tail_text(self) -> str:
        return "\n".join(self._tail)

    async def _serves(self, client: httpx.AsyncClient, served_name: str) -> bool:
        """Return true when `/health` is 200 and `/v1/models` lists the model."""
        try:
            health = await client.get(f"{self.base_url}/health")
            if health.status_code != 200:
                return False
            models = await client.get(f"{self.base_url}/v1/models")
            if models.status_code != 200:
                return False
            ids = [entry.get("id") for entry in models.json().get("data", [])]
        except (httpx.HTTPError, ValueError, AttributeError):
            return False
        return served_name in ids

    async def _wait_healthy(self, served_name: str) -> None:
        assert self._proc is not None and self._reader is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._startup_timeout
        async with httpx.AsyncClient(timeout=5.0) as client:
            while True:
                if self._proc.returncode is not None:
                    wait = max(0.0, min(READER_TIMEOUT, deadline - loop.time()))
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(asyncio.shield(self._reader), wait)
                    raise EngineError(
                        f"engine exited with code {self._proc.returncode} "
                        f"before it was healthy. Last output:\n{self._tail_text()}"
                    )
                if await self._serves(client, served_name):
                    if self._proc.returncode is None:
                        return
                    continue
                if loop.time() >= deadline:
                    raise EngineError(
                        f"engine not healthy after {self._startup_timeout:.0f} "
                        f"seconds. Last output:\n{self._tail_text()}"
                    )
                await asyncio.sleep(self._poll_interval)

    def _signal_group(self, pid: int, sig: int) -> bool:
        """Signal the process group. Return false when the group is gone."""
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            return False
        return True

    async def stop(self) -> None:
        proc, reader = self._proc, self._reader
        self._proc = None
        self._reader = None
        if proc is None:
            return
        # The group holds vLLM worker processes too. Signal all of them.
        if self._signal_group(proc.pid, signal.SIGTERM):
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._stop_grace
            while loop.time() < deadline and self._signal_group(proc.pid, 0):
                await asyncio.sleep(0.05)
                if proc.returncode is None:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(proc.wait(), 0.05)
            self._signal_group(proc.pid, signal.SIGKILL)
        await proc.wait()
        if reader is not None:
            try:
                await asyncio.wait_for(reader, READER_TIMEOUT)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                reader.cancel()
