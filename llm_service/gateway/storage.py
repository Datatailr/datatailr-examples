"""Blob store interface, with an in-memory and a Datatailr implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class BlobStore(Protocol):
    """The blob operations that the gateway uses."""

    def put(self, path: str, data: bytes | str) -> None:
        """Write `data` to `path`. A string is stored as UTF-8."""

    def get(self, path: str) -> bytes:
        """Return the bytes at `path`. Raise `KeyError` if it does not exist."""

    def put_file(self, path: str, local: Path) -> None:
        """Upload the local file `local` to `path`."""

    def get_file(self, path: str, local: Path) -> None:
        """Download `path` to the local file `local`. Create parent directories."""

    def ls(self, prefix: str) -> list[str]:
        """Return the full paths of all files under `prefix`, sorted.

        The listing is recursive. The prefix is a directory, not a string prefix.
        """

    def exists(self, path: str) -> bool:
        """Return True if a file exists at `path`."""


class MemoryBlobStore:
    """Dict backed blob store for tests and local runs."""

    def __init__(self) -> None:
        self._files: dict[str, bytes] = {}

    def put(self, path: str, data: bytes | str) -> None:
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._files[path] = data

    def get(self, path: str) -> bytes:
        return self._files[path]

    def put_file(self, path: str, local: Path) -> None:
        self.put(path, Path(local).read_bytes())

    def get_file(self, path: str, local: Path) -> None:
        local = Path(local)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(self.get(path))

    def ls(self, prefix: str) -> list[str]:
        directory = prefix.rstrip("/") + "/"
        return sorted(p for p in self._files if p.startswith(directory))

    def exists(self, path: str) -> bool:
        return path in self._files


class DatatailrBlobStore:
    """Blob store on the Datatailr platform. It wraps `datatailr.Blob`."""

    def __init__(self) -> None:
        from datatailr import Blob

        self._blob = Blob()

    def put(self, path: str, data: bytes | str) -> None:
        self._blob.put(path, data)

    def get(self, path: str) -> bytes:
        if not self._blob.exists(path):
            raise KeyError(path)
        return self._blob.get(path)

    def put_file(self, path: str, local: Path) -> None:
        self._blob.put_file(path, str(local))

    def get_file(self, path: str, local: Path) -> None:
        if not self._blob.exists(path):
            raise KeyError(path)
        local = Path(local)
        local.parent.mkdir(parents=True, exist_ok=True)
        self._blob.get_file(path, str(local))

    def ls(self, prefix: str) -> list[str]:
        # `Blob.ls` is not recursive by default and returns dicts with the keys
        # name, size, last_modified and is_file. Ask for a recursive listing and
        # keep only files. Strip a "blob://<bucket>/" scheme if the SDK adds it.
        directory = prefix.strip("/") + "/"
        entries = self._blob.ls(directory, recursive=True)
        names = []
        for entry in entries:
            if not entry.get("is_file", True):
                continue
            name = entry["name"]
            if name.startswith("blob://"):
                name = name[len("blob://"):].split("/", 1)[-1]
            names.append(name.lstrip("/"))
        return sorted(names)

    def exists(self, path: str) -> bool:
        return bool(self._blob.exists(path))
