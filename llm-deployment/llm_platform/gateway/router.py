"""Model -> deployment routing table, refreshed from the shared registry."""

from __future__ import annotations

import threading
import time
from typing import Any

from datatailr.logging import DatatailrLogger

from llm_platform.common.schemas import Deployment, DeploymentState
from llm_platform.common.state_store import BlobStateStore

log = DatatailrLogger(__name__).get_logger()


class ModelRouter:
    def __init__(self, refresh_interval: float = 15.0):
        self._store = BlobStateStore()
        self._interval = refresh_interval
        self._table: dict[str, Deployment] = {}
        self._deployments: list[Deployment] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.refresh()
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="model-router", daemon=True)
            self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            self.refresh()

    def refresh(self) -> None:
        try:
            registry = self._store.load()
        except Exception as exc:  # noqa: BLE001
            log.warning("Registry refresh failed: %s", exc)
            return
        with self._lock:
            self._table = registry.running_models()
            self._deployments = [d for d in registry.deployments.values() if d.state == DeploymentState.RUNNING]

    def resolve(self, model: str) -> Deployment | None:
        with self._lock:
            dep = self._table.get(model)
        if dep is None and model:
            # A client may ask for a model we just started: refresh once.
            self.refresh()
            with self._lock:
                dep = self._table.get(model)
        return dep

    def aliases(self) -> list[str]:
        with self._lock:
            return list(self._table.keys())

    def models(self) -> list[dict[str, Any]]:
        with self._lock:
            deployments = list(self._deployments)
        out = []
        for dep in deployments:
            created = int(dep.created_at)
            out.append({"id": dep.slug, "object": "model", "created": created, "owned_by": dep.created_by or "datatailr", "root": dep.spec.model})
            out.append({"id": dep.spec.model, "object": "model", "created": created, "owned_by": dep.created_by or "datatailr", "root": dep.spec.model, "parent": dep.slug})
        return out
