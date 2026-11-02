import pytest

from merit_desk.common.store import MemoryStore


@pytest.fixture
def memory_store() -> MemoryStore:
    return MemoryStore()
