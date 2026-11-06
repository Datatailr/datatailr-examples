"""Backend contract tests: the in-memory, local-directory and Datatailr blob backends."""

from __future__ import annotations

import time

import pytest

from neon_postgres.s3_gateway.backends import (
    DatatailrBlobBackend,
    LocalDirBackend,
    MemoryBackend,
    NotFound,
)


class FakeBlob:
    """Mimics ``datatailr.Blob`` with the subset the backend uses."""

    def __init__(self, silent_on_missing=False):
        self.objects: dict[str, bytes] = {}
        self.silent_on_missing = silent_on_missing

    def put_file(self, name, path):
        with open(path, "rb") as handle:
            self.objects[name] = handle.read()

    def get_file(self, name, path):
        if name not in self.objects:
            if self.silent_on_missing:
                open(path, "wb").close()  # some providers write an empty file for a missing object
                return
            raise RuntimeError("dt blob cp: object not found")
        with open(path, "wb") as handle:
            handle.write(self.objects[name])

    def exists(self, name):
        return name in self.objects

    def delete(self, name):
        if name not in self.objects:
            raise RuntimeError("dt blob rm: object not found")
        del self.objects[name]

    def ls(self, prefix, recursive=False, filter_by="", limit=-1, offset=0):
        assert recursive, "the backend always lists recursively"
        return [
            {"name": name, "size": len(data), "last_modified": 1_700_000_000, "is_file": True}
            for name, data in self.objects.items()
            if name.startswith(prefix)
        ]


@pytest.fixture(params=["memory", "localdir", "datatailr"])
def backend(request, tmp_path):
    if request.param == "memory":
        return MemoryBackend()
    if request.param == "localdir":
        return LocalDirBackend(tmp_path / "store")
    return DatatailrBlobBackend(blob=FakeBlob(), concurrency=2)


def test_upload_download_stat_delete(backend, tmp_path):
    src = tmp_path / "src"
    src.write_bytes(b"hello")
    before = time.time() - 1

    backend.upload("neon", "a/b/c", str(src))

    info = backend.stat("neon", "a/b/c")
    assert info.size == 5
    assert info.mtime >= before or isinstance(backend, DatatailrBlobBackend)

    dst = tmp_path / "dst"
    backend.download("neon", "a/b/c", str(dst))
    assert dst.read_bytes() == b"hello"

    backend.delete("neon", "a/b/c")
    assert backend.stat("neon", "a/b/c") is None
    backend.delete("neon", "a/b/c")  # deleting a missing key is not an error


def test_download_missing_raises_not_found(backend, tmp_path):
    with pytest.raises(NotFound):
        backend.download("neon", "missing", str(tmp_path / "out"))


def test_list_is_recursive_and_sorted(backend, tmp_path):
    src = tmp_path / "src"
    src.write_bytes(b"x")
    for key in ["t/b/2", "t/a/1", "t/a/10", "u/1"]:
        backend.upload("neon", key, str(src))

    listed = backend.list("neon", "t/")
    assert [item.key for item in listed] == ["t/a/1", "t/a/10", "t/b/2"]
    assert all(item.size == 1 for item in listed)

    # A non-directory prefix lists the parent directory and filters by string prefix.
    assert [item.key for item in backend.list("neon", "t/a/1")] == ["t/a/1", "t/a/10"]
    assert backend.list("neon", "nothing/") == []


def test_datatailr_backend_maps_bucket_and_key_to_blob_paths(tmp_path):
    blob = FakeBlob()
    backend = DatatailrBlobBackend(blob=blob, concurrency=1)
    src = tmp_path / "src"
    src.write_bytes(b"data")

    backend.upload("neon", "pageserver/tenants/t/index_part.json", str(src))

    assert list(blob.objects) == ["neon/pageserver/tenants/t/index_part.json"]
    assert backend.list("neon", "pageserver/")[0].key == "pageserver/tenants/t/index_part.json"


def test_datatailr_backend_detects_missing_object_behind_an_empty_download(tmp_path):
    backend = DatatailrBlobBackend(blob=FakeBlob(silent_on_missing=True), concurrency=1)
    with pytest.raises(NotFound):
        backend.download("neon", "neon-postgres/state.json", str(tmp_path / "state.json"))

    # A real empty object is still returned as such.
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    backend.upload("neon", "empty", str(empty))
    backend.download("neon", "empty", str(tmp_path / "out"))
    assert (tmp_path / "out").read_bytes() == b""
