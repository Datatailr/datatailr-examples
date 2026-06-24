"""Generic directory <-> Datatailr blob storage sync.

Used to persist the per-user pi session stores and the global agent
configuration directories (``~/.pi`` and ``~/.agents``) so that all state
survives container restarts.

`Blob.ls(prefix, recursive=True)` returns a list of dicts shaped like
``{"name": "<key>", "is_file": bool, "size": int, ...}`` where ``name`` is the
key relative to the user bucket (i.e. exactly what ``get_file``/``put_file``
expect). We tolerate plain-string entries too, for safety.
"""

from __future__ import annotations

import os
from typing import Iterable, Optional

# Directory names never worth syncing (large, regenerable, or noise).
DEFAULT_EXCLUDE_DIRS = {
    "node_modules",
    "__pycache__",
    ".cache",
    ".git",
    "tmp",
    "logs",
}


def blob_client():
    """Return a Datatailr Blob client, or None if unavailable (e.g. local dev)."""
    try:
        from datatailr import Blob  # only importable on the platform

        return Blob()
    except Exception:
        return None


def _entry_name(entry) -> tuple[Optional[str], Optional[bool]]:
    if isinstance(entry, dict):
        return entry.get("name"), entry.get("is_file")
    if isinstance(entry, str):
        return entry, None
    return None, None


def _iter_files(local_dir: str, exclude_dirs: set[str]) -> Iterable[str]:
    for root, dirs, files in os.walk(local_dir):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for name in files:
            yield os.path.join(root, name)


def push_dir(
    local_dir: str,
    prefix: str,
    exclude_dirs: Optional[Iterable[str]] = None,
    blob=None,
) -> int:
    """Upload every file under ``local_dir`` to blob storage under ``prefix``.

    Relative paths are preserved (``<prefix>/<relative/path>``). Returns the
    number of files uploaded.
    """
    blob = blob or blob_client()
    if blob is None or not os.path.isdir(local_dir):
        return 0
    excludes = DEFAULT_EXCLUDE_DIRS | set(exclude_dirs or [])
    prefix = prefix.strip("/")
    count = 0
    for path in _iter_files(local_dir, excludes):
        rel = os.path.relpath(path, local_dir).replace(os.sep, "/")
        key = f"{prefix}/{rel}"
        try:
            blob.put_file(key, path)
            count += 1
        except Exception:
            continue
    return count


def pull_dir(prefix: str, local_dir: str, blob=None) -> int:
    """Download all blobs under ``prefix`` into ``local_dir``, preserving paths.

    Returns the number of files downloaded.
    """
    blob = blob or blob_client()
    if blob is None:
        return 0
    prefix = prefix.strip("/")
    os.makedirs(local_dir, exist_ok=True)
    try:
        entries = blob.ls(f"{prefix}/", recursive=True)
    except Exception:
        return 0
    count = 0
    for entry in entries or []:
        name, is_file = _entry_name(entry)
        if not name or is_file is False:
            continue
        rel = name[len(prefix):].lstrip("/")
        if not rel:
            continue
        dest = os.path.join(local_dir, *rel.split("/"))
        os.makedirs(os.path.dirname(dest) or local_dir, exist_ok=True)
        try:
            blob.get_file(name, dest)
            count += 1
        except Exception:
            continue
    return count
