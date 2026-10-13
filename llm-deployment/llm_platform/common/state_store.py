"""Registry persistence on Datatailr blob storage.

The registry is one JSON document. The admin app is the single writer; the
gateway only reads it to build its routing table. Writes go through a process
lock and are debounced by the caller (the orchestrator persists after every
state transition, which is a handful of writes per minute at most).
"""

from __future__ import annotations

import json
import threading
import time

from datatailr import Blob
from datatailr.logging import DatatailrLogger

from llm_platform.common.naming import STATE_BLOB
from llm_platform.common.schemas import Registry

log = DatatailrLogger(__name__).get_logger()


class BlobStateStore:
    def __init__(self, blob_path: str = STATE_BLOB):
        self._blob = Blob()
        self._path = blob_path
        self._io_lock = threading.Lock()
        self._mutation_lock = threading.RLock()
        self._cache: Registry | None = None
        self._cache_ts = 0.0

    # ---- reads -----------------------------------------------------------
    def load(self, max_age: float = 0.0) -> Registry:
        """Return the registry. ``max_age`` > 0 allows serving a cached copy.

        Reads only take the short I/O lock, so the UI stays responsive while
        the orchestrator holds the mutation lock during slow platform calls.
        """
        with self._io_lock:
            if self._cache is not None and max_age > 0 and time.time() - self._cache_ts < max_age:
                return self._cache
            try:
                if self._blob.exists(self._path):
                    raw = self._blob.get(self._path)
                    registry = Registry.model_validate(json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw))
                else:
                    registry = Registry()
            except Exception as exc:  # noqa: BLE001
                if self._cache is not None:
                    log.warning("Failed to read registry from blob, serving cache: %s", exc)
                    return self._cache
                raise
            self._cache = registry
            self._cache_ts = time.time()
            return registry

    # ---- writes ----------------------------------------------------------
    def save(self, registry: Registry) -> None:
        with self._io_lock:
            registry.updated_at = time.time()
            payload = registry.model_dump_json(indent=1)
            self._blob.put(self._path, payload)
            self._cache = registry
            self._cache_ts = time.time()

    def lock(self) -> threading.RLock:
        """Lock to hold around a load -> mutate -> save sequence."""
        return self._mutation_lock
