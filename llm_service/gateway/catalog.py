"""The catalog of recommended models: the catalog file and the lookup of its entries."""

from __future__ import annotations

import copy
import json
import logging
import threading

from .limits import StorageUnavailable
from .storage import BlobStore
from .weights import validate_model_name, validate_revision

logger = logging.getLogger(__name__)

MAX_ENTRIES = 200
MAX_NOTE = 500
MAX_GPU_MEMORY_MIB = 10_000_000
ENTRY_FIELDS = ("model", "revision", "note", "gpu_memory_mib")

__all__ = [
    "MAX_ENTRIES",
    "MAX_NOTE",
    "MAX_GPU_MEMORY_MIB",
    "InvalidEntry",
    "CatalogFull",
    "StorageUnavailable",
    "check_entry",
    "CatalogStore",
]

Key = tuple[str, str]


class InvalidEntry(ValueError):
    """A catalog entry is not valid. The message is the text for the user."""


class CatalogFull(Exception):
    """The catalog has `MAX_ENTRIES` entries and the new entry is not a replacement."""


def check_entry(value: object) -> dict:
    """Return a checked copy of `value` with the defaults filled in. Raise `InvalidEntry` if not.

    The result has the keys `model`, `revision`, `note` and `gpu_memory_mib`.
    """
    if not isinstance(value, dict):
        raise InvalidEntry("The catalog entry must be a JSON object.")
    unknown = sorted(str(key) for key in value if key not in ENTRY_FIELDS)
    if unknown:
        raise InvalidEntry(f"Unknown catalog field: {', '.join(unknown)}.")
    for name in ("model", "revision"):
        if not isinstance(value.get(name), str):
            raise InvalidEntry(f"The catalog field {name} must be a string.")
    try:
        validate_model_name(value["model"])
        validate_revision(value["revision"])
    except ValueError as exc:
        raise InvalidEntry(str(exc)) from exc
    note = value.get("note", "")
    if not isinstance(note, str):
        raise InvalidEntry("The catalog field note must be a string.")
    if len(note) > MAX_NOTE:
        raise InvalidEntry(f"The catalog field note has at most {MAX_NOTE} characters.")
    memory = value.get("gpu_memory_mib")
    if memory is not None:
        if isinstance(memory, bool) or not isinstance(memory, int):
            raise InvalidEntry("The catalog field gpu_memory_mib must be null or a whole number.")
        if not 1 <= memory <= MAX_GPU_MEMORY_MIB:
            raise InvalidEntry(f"The catalog field gpu_memory_mib must be from 1 to {MAX_GPU_MEMORY_MIB}.")
    return {
        "model": value["model"],
        "revision": value["revision"],
        "note": note,
        "gpu_memory_mib": memory,
    }


def _key(entry: dict) -> Key:
    return entry["model"], entry["revision"]


class CatalogStore:
    """The catalog of the service, in memory and in `<blob_root>/catalog.json`."""

    def __init__(self, store: BlobStore, blob_root: str) -> None:
        self._store = store
        self._path = f"{blob_root.strip('/')}/catalog.json"
        self._write_lock = threading.Lock()  # serializes `load` and the writers
        self._lock = threading.Lock()  # guards `_entries`, never held for a blob call
        self._entries: dict[Key, dict] = {}
        self._loaded = False  # False after a read that failed for a reason other than content
        self._read_error: str | None = None  # the text of the last read error, None after a good read

    def load(self) -> None:
        """Read the file. A missing or broken file means an empty catalog.

        Another read error also means an empty catalog and an error log. The next write or
        `retry_load` then reads again.
        """
        with self._write_lock:
            self._load()

    def _load(self) -> None:
        loaded = True
        try:
            raw = self._store.get(self._path)
        except KeyError:
            entries: dict[Key, dict] = {}
            self._read_error = None
        except Exception as exc:  # noqa: BLE001
            # A lasting error logs one line. A new text or a good read in between logs again.
            if str(exc) != self._read_error:
                logger.error("cannot read %s, the catalog is empty: %s", self._path, exc)
            self._read_error = str(exc)
            entries, loaded = {}, False
        else:
            self._read_error = None
            try:
                entries = self._parse(raw)
            except (ValueError, TypeError, RecursionError) as exc:
                logger.error("ignoring broken %s, the catalog is empty: %s", self._path, exc)
                entries = {}
        with self._lock:
            self._entries = entries
            self._loaded = loaded

    @staticmethod
    def _parse(raw: bytes) -> dict[Key, dict]:
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) != {"models"} or not isinstance(data["models"], list):
            raise ValueError("the file must be an object with the list models")
        if len(data["models"]) > MAX_ENTRIES:
            raise ValueError(f"the file has more than {MAX_ENTRIES} entries")
        entries: dict[Key, dict] = {}
        for item in data["models"]:
            entry = check_entry(item)
            if _key(entry) in entries:
                raise ValueError(f"duplicate entry {_key(entry)}")
            entries[_key(entry)] = entry
        return entries

    def loaded(self) -> bool:
        """Return False after a read that failed for a reason other than the content."""
        with self._lock:
            return self._loaded

    def retry_load(self) -> bool:
        """Read the file again if the last read failed. Return `loaded()`. Never raises."""
        try:
            with self._write_lock:
                if not self._loaded:
                    self._load()
        except Exception as exc:  # noqa: BLE001
            logger.error("the new read of %s failed: %s", self._path, exc)
        return self.loaded()

    def entries(self) -> list[dict]:
        """Return copies of all entries, sorted by model and then revision."""
        with self._lock:
            return [copy.deepcopy(self._entries[key]) for key in sorted(self._entries)]

    def get(self, model: str, revision: str) -> dict | None:
        """Return a copy of one entry, or None."""
        with self._lock:
            entry = self._entries.get((model, revision))
            return copy.deepcopy(entry) if entry is not None else None

    def _write(self, entries: dict[Key, dict]) -> None:
        models = [entries[key] for key in sorted(entries)]
        body = json.dumps({"models": models}, indent=2)
        try:
            self._store.put(self._path, body)
        except Exception as exc:  # noqa: BLE001
            logger.error("cannot write %s: %s", self._path, exc)
            raise StorageUnavailable("The catalog cannot be saved now.") from exc

    def _prepare_write(self) -> None:
        """Read the file again after a failed read. Call it with the write lock held."""
        if not self._loaded:
            self._load()
        if not self._loaded:
            raise StorageUnavailable("The catalog cannot be read now.")

    def put(self, entry: dict) -> dict | None:
        """Add an entry or replace the entry with the same key. Return the old entry."""
        new = check_entry(entry)
        with self._write_lock:
            self._prepare_write()
            old = self._entries.get(_key(new))
            if old is None and len(self._entries) >= MAX_ENTRIES:
                raise CatalogFull(f"The catalog has {MAX_ENTRIES} entries already.")
            entries = {**self._entries, _key(new): new}
            self._write(entries)
            with self._lock:
                self._entries = entries
            return copy.deepcopy(old) if old is not None else None

    def remove(self, model: str, revision: str) -> dict:
        """Remove one entry. Return it. Raise `KeyError` without one."""
        with self._write_lock:
            self._prepare_write()
            key = (model, revision)
            if key not in self._entries:
                raise KeyError(key)
            entries = {k: v for k, v in self._entries.items() if k != key}
            self._write(entries)
            with self._lock:
                old, self._entries = self._entries[key], entries
            return copy.deepcopy(old)
