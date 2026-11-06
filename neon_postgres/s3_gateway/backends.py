"""Storage backends for the S3 gateway.

A backend stores whole objects under ``(bucket, key)``. The gateway never needs
byte ranges, metadata or versions from a backend: it emulates those on top of
``upload``, ``download``, ``stat``, ``list`` and ``delete``.

Three implementations:

- ``DatatailrBlobBackend``: the real one, built on ``datatailr.Blob`` (the
  ``dt blob`` CLI underneath). Every object lives at ``<bucket>/<key>`` inside
  the user's blob storage.
- ``LocalDirBackend``: a directory on disk, for running the whole stack in a
  plain Docker container without a Datatailr cluster.
- ``MemoryBackend``: for tests.
"""

from __future__ import annotations

import logging
import os
import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

log = logging.getLogger("s3_gateway.backends")


class NotFound(Exception):
    """The object does not exist."""


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    mtime: float


class Backend(Protocol):
    def upload(self, bucket: str, key: str, src_path: str) -> None: ...

    def download(self, bucket: str, key: str, dst_path: str) -> None: ...

    def stat(self, bucket: str, key: str) -> ObjectInfo | None: ...

    def list(self, bucket: str, prefix: str) -> list[ObjectInfo]: ...

    def delete(self, bucket: str, key: str) -> None: ...


def _directory_of(prefix: str) -> str:
    """The directory part of a prefix: ``a/b/index_part.json`` -> ``a/b/``."""
    if prefix.endswith("/") or "/" not in prefix:
        return prefix if prefix.endswith("/") else ""
    return prefix.rsplit("/", 1)[0] + "/"


def _filter_and_sort(items: list[ObjectInfo], prefix: str) -> list[ObjectInfo]:
    return sorted((item for item in items if item.key.startswith(prefix)), key=lambda item: item.key)


class MemoryBackend:
    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], tuple[bytes, float]] = {}
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self._objects.clear()

    def upload(self, bucket: str, key: str, src_path: str) -> None:
        with open(src_path, "rb") as handle:
            data = handle.read()
        with self._lock:
            self._objects[(bucket, key)] = (data, time.time())

    def download(self, bucket: str, key: str, dst_path: str) -> None:
        with self._lock:
            entry = self._objects.get((bucket, key))
        if entry is None:
            raise NotFound(key)
        with open(dst_path, "wb") as handle:
            handle.write(entry[0])

    def stat(self, bucket: str, key: str) -> ObjectInfo | None:
        with self._lock:
            entry = self._objects.get((bucket, key))
        if entry is None:
            return None
        return ObjectInfo(key, len(entry[0]), entry[1])

    def list(self, bucket: str, prefix: str) -> list[ObjectInfo]:
        with self._lock:
            items = [
                ObjectInfo(k, len(data), mtime)
                for (b, k), (data, mtime) in self._objects.items()
                if b == bucket
            ]
        return _filter_and_sort(items, prefix)

    def delete(self, bucket: str, key: str) -> None:
        with self._lock:
            self._objects.pop((bucket, key), None)


class LocalDirBackend:
    def __init__(self, root: str | os.PathLike) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path(self, bucket: str, key: str) -> Path:
        path = (self._root / bucket / key).resolve()
        if self._root.resolve() not in path.parents:
            raise ValueError(f"key escapes the store: {key!r}")
        return path

    def upload(self, bucket: str, key: str, src_path: str) -> None:
        path = self._path(bucket, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp-upload")
        shutil.copyfile(src_path, tmp)
        os.replace(tmp, path)

    def download(self, bucket: str, key: str, dst_path: str) -> None:
        path = self._path(bucket, key)
        if not path.is_file():
            raise NotFound(key)
        shutil.copyfile(path, dst_path)

    def stat(self, bucket: str, key: str) -> ObjectInfo | None:
        path = self._path(bucket, key)
        if not path.is_file():
            return None
        info = path.stat()
        return ObjectInfo(key, info.st_size, info.st_mtime)

    def list(self, bucket: str, prefix: str) -> list[ObjectInfo]:
        directory = self._root / bucket / _directory_of(prefix)
        if not directory.is_dir():
            return []
        base = self._root / bucket
        items = []
        for path in directory.rglob("*"):
            if path.is_file() and not path.name.endswith(".tmp-upload"):
                info = path.stat()
                items.append(ObjectInfo(path.relative_to(base).as_posix(), info.st_size, info.st_mtime))
        return _filter_and_sort(items, prefix)

    def delete(self, bucket: str, key: str) -> None:
        path = self._path(bucket, key)
        try:
            path.unlink()
        except FileNotFoundError:
            pass


class DatatailrBlobBackend:
    """Objects live at ``<bucket>/<key>`` in the user's Datatailr blob storage.

    ``blob`` is a ``datatailr.Blob`` instance (or anything with the same
    ``put_file`` / ``get`` / ``delete`` / ``ls`` methods).
    Each call runs the ``dt`` CLI, so ``concurrency`` bounds how many run at once.
    """

    def __init__(self, blob=None, concurrency: int = 8) -> None:
        if blob is None:
            from datatailr import Blob  # imported lazily: only present on the platform

            blob = Blob()
        self._blob = blob
        self._gate = threading.BoundedSemaphore(concurrency)

    @staticmethod
    def _name(bucket: str, key: str) -> str:
        return f"{bucket}/{key}"

    @staticmethod
    def _key_of(entry_name: str, bucket: str) -> str | None:
        """The key of a listing entry, whether the CLI reports ``bucket/key`` or a longer path."""
        marker = f"{bucket}/"
        if entry_name.startswith(marker):
            return entry_name[len(marker):]
        idx = entry_name.find(f"/{marker}")
        if idx >= 0:
            return entry_name[idx + 1 + len(marker):]
        return None

    def upload(self, bucket: str, key: str, src_path: str) -> None:
        with self._gate:
            self._blob.put_file(self._name(bucket, key), src_path)

    def download(self, bucket: str, key: str, dst_path: str) -> None:
        """Download to ``dst_path`` through ``Blob.get`` (the object's bytes on stdout).

        ``Blob.get_file`` is not used: inside a job it goes through the platform's
        remote client, and on some versions the destination file ends up empty.
        A missing object either raises or comes back empty, so both outcomes are
        checked against the structured listing before they are reported as NotFound.
        """
        name = self._name(bucket, key)
        with self._gate:
            try:
                data = self._blob.get(name)
                error = None
            except Exception as exc:
                data, error = b"", exc
        if error is None and data:
            with open(dst_path, "wb") as handle:
                handle.write(data)
            return
        info = self.stat(bucket, key)
        if info is None:
            raise NotFound(key)
        if error is not None:
            raise error
        log.warning("downloaded %s as 0 bytes while the listing reports %d bytes", name, info.size)
        open(dst_path, "wb").close()

    def stat(self, bucket: str, key: str) -> ObjectInfo | None:
        name = self._name(bucket, key)
        with self._gate:
            entries = self._blob.ls(name, recursive=True)
        for entry in entries:
            if self._key_of(entry.get("name", ""), bucket) == key and entry.get("is_file", True):
                return ObjectInfo(key, int(entry["size"]), float(entry["last_modified"]))
        return None

    def list(self, bucket: str, prefix: str) -> list[ObjectInfo]:
        base = f"{bucket}/"
        with self._gate:
            entries = self._blob.ls(base + _directory_of(prefix), recursive=True)
        items = []
        for entry in entries:
            entry_key = self._key_of(entry.get("name", ""), bucket)
            if entry_key is not None and entry.get("is_file", True):
                items.append(ObjectInfo(entry_key, int(entry["size"]), float(entry["last_modified"])))
        return _filter_and_sort(items, prefix)

    def delete(self, bucket: str, key: str) -> None:
        name = self._name(bucket, key)
        with self._gate:
            try:
                self._blob.delete(name)
            except Exception:
                if self.stat(bucket, key) is not None:
                    raise


def backend_from_env() -> Backend:
    """Pick the backend from ``NEON_STORAGE_BACKEND``: ``datatailr`` (default) or ``local``."""
    kind = os.environ.get("NEON_STORAGE_BACKEND", "datatailr").lower()
    if kind == "local":
        return LocalDirBackend(os.environ.get("NEON_LOCAL_STORE", "/tmp/neon_postgres_store"))
    if kind == "memory":
        return MemoryBackend()
    if kind == "datatailr":
        return DatatailrBlobBackend(concurrency=int(os.environ.get("S3_GATEWAY_CONCURRENCY", "8")))
    raise ValueError(f"unknown NEON_STORAGE_BACKEND={kind!r}")
