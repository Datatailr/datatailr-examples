"""Test doubles shared by the gateway tests."""

from __future__ import annotations

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
