"""Test doubles shared by the gateway tests."""

from __future__ import annotations

from pathlib import Path

from llm_service.gateway.engine import EngineError
from llm_service.gateway.storage import MemoryBlobStore


class FlakyBlobStore(MemoryBlobStore):
    """A memory store whose `put` fails while `fail_puts` is true."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_puts = False

    def put(self, path: str, data: bytes | str) -> None:
        if self.fail_puts:
            raise OSError("simulated storage failure")
        super().put(path, data)


class FakeEngine:
    """An engine double. It records calls and fails for chosen model names."""

    def __init__(self) -> None:
        self.base_url = "http://127.0.0.1:0"
        self.started: list[tuple[Path, str]] = []
        self.stopped = 0
        self.fail_on: set[str] = set()
        self.alive_flag = True

    async def start(self, model_dir: Path, served_name: str) -> None:
        if served_name in self.fail_on:
            raise EngineError(f"cannot start {served_name}")
        self.started.append((model_dir, served_name))
        self.alive_flag = True

    async def stop(self) -> None:
        self.stopped += 1

    def alive(self) -> bool:
        return self.alive_flag
