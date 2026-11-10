"""The vLLM child process."""

from __future__ import annotations

import asyncio
import collections
import logging
import signal
from pathlib import Path
from typing import Protocol

import httpx

log = logging.getLogger("llm_service.engine")

TAIL_LINES = 40
STOP_TIMEOUT = 30.0


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
    """Run `vllm serve` as a child process."""

    def __init__(
        self,
        port: int,
        extra_args: list[str],
        startup_timeout: float = 1800.0,
        poll_interval: float = 2.0,
    ) -> None:
        self._port = port
        self._extra_args = list(extra_args)
        self._startup_timeout = startup_timeout
        self._poll_interval = poll_interval
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

    async def start(self, model_dir: Path, served_name: str) -> None:
        await self.stop()
        self._tail.clear()
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self.command(model_dir, served_name),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except OSError as exc:
            raise EngineError(f"cannot run the engine: {exc}") from exc
        self._reader = asyncio.create_task(self._forward_output(self._proc))
        try:
            await self._wait_healthy()
        except BaseException:
            await self.stop()
            raise

    async def _forward_output(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        async for raw in proc.stdout:
            line = raw.decode(errors="replace").rstrip()
            self._tail.append(line)
            log.info("vllm: %s", line)

    def _tail_text(self) -> str:
        return "\n".join(self._tail)

    async def _wait_healthy(self) -> None:
        assert self._proc is not None and self._reader is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._startup_timeout
        async with httpx.AsyncClient(timeout=5.0) as client:
            while True:
                if self._proc.returncode is not None:
                    await asyncio.shield(self._reader)
                    raise EngineError(
                        f"engine exited with code {self._proc.returncode} "
                        f"before it was healthy. Last output:\n{self._tail_text()}"
                    )
                try:
                    response = await client.get(f"{self.base_url}/health")
                    if response.status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                if loop.time() >= deadline:
                    raise EngineError(
                        f"engine not healthy after {self._startup_timeout:.0f} "
                        f"seconds. Last output:\n{self._tail_text()}"
                    )
                await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        proc, reader = self._proc, self._reader
        if proc is not None and proc.returncode is None:
            proc.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), STOP_TIMEOUT)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        if reader is not None:
            await reader
        self._proc = None
        self._reader = None
