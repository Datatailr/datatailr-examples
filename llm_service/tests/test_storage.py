import pytest

from llm_service.gateway.storage import MemoryBlobStore


def test_memory_store_roundtrip(tmp_path):
    s = MemoryBlobStore()
    s.put("r/a/1.txt", "hello")
    s.put("r/a/b/2.bin", b"\x00")
    assert s.get("r/a/1.txt") == b"hello"
    assert s.ls("r/a") == ["r/a/1.txt", "r/a/b/2.bin"]
    assert s.ls("r/a/") == s.ls("r/a")
    assert s.ls("r/ab") == []  # prefix is a directory, not a string prefix
    assert s.exists("r/a/1.txt") and not s.exists("r/a")
    s.get_file("r/a/1.txt", tmp_path / "d" / "1.txt")  # creates parent directories
    assert (tmp_path / "d" / "1.txt").read_text() == "hello"
    s.put_file("r/c", tmp_path / "d" / "1.txt")
    assert s.get("r/c") == b"hello"
    with pytest.raises(KeyError):
        s.get("missing")
