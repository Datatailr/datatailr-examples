import sys
import types
from pathlib import Path

import pytest

from llm_service.gateway.storage import DatatailrBlobStore, MemoryBlobStore


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


class FakeBlob:
    """A stand-in for `datatailr.Blob`. It records each call."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []
        self.broken_reads: set[str] = set()
        self.entries: list[dict] = []

    def get(self, path):
        self.calls.append(("get", path))
        if path in self.broken_reads or path not in self.files:
            raise RuntimeError(f"dt blob get failed for {path}")
        return self.files[path]

    def get_file(self, path, local):
        self.calls.append(("get_file", path))
        if path in self.broken_reads or path not in self.files:
            raise RuntimeError(f"dt blob cp failed for {path}")
        Path(local).write_bytes(self.files[path])

    def exists(self, path):
        self.calls.append(("exists", path))
        return path in self.files

    def ls(self, path, recursive=False):
        self.calls.append(("ls", path))
        assert recursive is True
        return self.entries


@pytest.fixture
def blob(monkeypatch):
    fake_blob = FakeBlob()
    module = types.ModuleType("datatailr")
    module.Blob = lambda: fake_blob
    monkeypatch.setitem(sys.modules, "datatailr", module)
    return fake_blob


def test_datatailr_get_reads_first_and_costs_one_call(blob):
    blob.files["r/a"] = b"data"
    store = DatatailrBlobStore()
    assert store.get("r/a") == b"data"
    assert blob.calls == [("get", "r/a")]


def test_datatailr_get_of_a_missing_blob_raises_key_error(blob):
    store = DatatailrBlobStore()
    with pytest.raises(KeyError):
        store.get("r/missing")
    assert blob.calls == [("get", "r/missing"), ("exists", "r/missing")]


def test_datatailr_get_reraises_the_read_error_of_an_existing_blob(blob):
    blob.files["r/a"] = b"data"
    blob.broken_reads.add("r/a")
    store = DatatailrBlobStore()
    with pytest.raises(RuntimeError, match="dt blob get failed"):
        store.get("r/a")


def test_datatailr_get_file_reads_first(blob, tmp_path):
    blob.files["r/a"] = b"data"
    store = DatatailrBlobStore()
    store.get_file("r/a", tmp_path / "sub" / "a")
    assert (tmp_path / "sub" / "a").read_bytes() == b"data"
    assert blob.calls == [("get_file", "r/a")]
    blob.calls.clear()
    with pytest.raises(KeyError):
        store.get_file("r/missing", tmp_path / "b")
    assert blob.calls == [("get_file", "r/missing"), ("exists", "r/missing")]
    blob.broken_reads.add("r/a")
    with pytest.raises(RuntimeError, match="dt blob cp failed"):
        store.get_file("r/a", tmp_path / "c")


def test_datatailr_ls_keeps_files_and_strips_the_scheme(blob):
    blob.entries = [
        {"name": "root/models/", "is_file": False},
        {"name": "blob://bucket/root/models/b.json", "is_file": True},
        {"name": "/root/models/a.json", "is_file": True},
        {"name": "root/models/c.json"},
    ]
    store = DatatailrBlobStore()
    assert store.ls("/root/models") == [
        "root/models/a.json",
        "root/models/b.json",
        "root/models/c.json",
    ]
    assert blob.calls == [("ls", "root/models/")]
