"""The model manager: load a model at startup and switch on request."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

from .engine import Engine
from .storage import BlobStore
from .weights import WeightCache, validate_model_name, validate_revision

log = logging.getLogger("llm_service.models")


class SwitchInProgress(Exception):
    """A model switch is already running."""


class ModelManager:
    """Hold the state of the served model: empty, loading, ready or failed."""

    def __init__(
        self,
        weights: WeightCache,
        engine: Engine,
        store: BlobStore,
        root: str,
        default_model: str,
        on_event: Callable[[dict], None] = lambda event: None,
    ) -> None:
        self._weights = weights
        self._engine = engine
        self._store = store
        self._root = root.strip("/")
        self._default_model = default_model
        self._on_event = on_event
        self._task: asyncio.Task | None = None
        self.state = "empty"
        self.active: dict | None = None
        self.target: dict | None = None
        self.error: str | None = None

    @property
    def _active_path(self) -> str:
        return f"{self._root}/state/active.json"

    def status(self) -> dict:
        """Return the state for `GET /admin/model`."""
        return {
            "state": self.state,
            "active": self.active,
            "target": self.target,
            "error": self.error,
        }

    async def startup(self) -> None:
        """Load the saved model, or the default one. Never raise."""
        try:
            wanted = self._read_active()
            self._begin(wanted)
            await self._load(wanted, previous=None)
        except Exception as exc:  # noqa: BLE001
            log.exception("startup failed")
            self.state = "failed"
            self.target = None
            self.error = str(exc)

    def _read_active(self) -> dict:
        default = {"model": self._default_model, "revision": "main"}
        if not self._store.exists(self._active_path):
            return default
        # A store error propagates and fails the startup. Bad content does not.
        raw = self._store.get(self._active_path)
        try:
            saved = json.loads(raw)
            return {
                "model": validate_model_name(saved["model"]),
                "revision": validate_revision(saved.get("revision", "main")),
            }
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            log.warning("ignoring unusable %s: %s", self._active_path, exc)
            return default

    def _begin(self, wanted: dict) -> None:
        self.state = "loading"
        self.target = dict(wanted)

    def request_switch(self, model: str, revision: str = "main") -> asyncio.Task:
        """Start a switch in the background. Call it inside a running loop."""
        validate_model_name(model)
        validate_revision(revision)
        if self.state == "loading":
            raise SwitchInProgress("a model switch is already running")
        wanted = {"model": model, "revision": revision}
        self._begin(wanted)
        self._task = asyncio.create_task(self._load(wanted, previous=self.active))
        return self._task

    def check_engine(self) -> None:
        """Set `failed` when the engine process has exited."""
        if self.state == "ready" and not self._engine.alive():
            self.state = "failed"
            self.error = "engine exited"

    async def _load(self, wanted: dict, previous: dict | None) -> None:
        model, revision = wanted["model"], wanted["revision"]
        try:
            try:
                model_dir = await asyncio.to_thread(
                    self._weights.ensure, model, revision
                )
            except Exception as exc:  # noqa: BLE001
                self._fail_before_stop(wanted, previous, f"{model}: {exc}")
                return
            if previous is not None:
                await self._engine.stop()
            try:
                await self._engine.start(model_dir, model)
            except Exception as exc:  # noqa: BLE001
                await self._fail_after_stop(wanted, previous, f"{model}: {exc}")
                return
            self.active = dict(wanted)
            self.target = None
            self.error = None
            self.state = "ready"
            self._store.put(self._active_path, json.dumps(self.active))
            self._emit(wanted, "ok", None)
        except Exception as exc:  # noqa: BLE001
            log.exception("model switch crashed")
            self.target = None
            self.state = "failed"
            self.error = f"{model}: {exc}"
            self._emit(wanted, "failed", self.error)

    def _fail_before_stop(
        self, wanted: dict, previous: dict | None, error: str
    ) -> None:
        # The old engine still runs, so the old model keeps serving.
        self.target = None
        self.error = error
        if previous is None:
            self.state = "empty"
        else:
            self.state = "ready" if self._engine.alive() else "failed"
        self._emit(wanted, "failed", error)

    async def _fail_after_stop(
        self, wanted: dict, previous: dict | None, error: str
    ) -> None:
        self.target = None
        self.error = error
        self.active = None
        self.state = "failed"
        if previous is not None:
            try:
                model_dir = await asyncio.to_thread(
                    self._weights.ensure, previous["model"], previous["revision"]
                )
                await self._engine.start(model_dir, previous["model"])
            except Exception as exc:  # noqa: BLE001
                log.error("rollback to %s failed: %s", previous["model"], exc)
            else:
                self.active = dict(previous)
                self.state = "ready"
                self._store.put(self._active_path, json.dumps(self.active))
        self._emit(wanted, "failed", error)

    def _emit(self, wanted: dict, result: str, error: str | None) -> None:
        try:
            self._on_event(
                {
                    "event": "model_switch",
                    "model": wanted["model"],
                    "revision": wanted["revision"],
                    "result": result,
                    "error": error,
                }
            )
        except Exception:  # noqa: BLE001
            log.exception("event handler failed")
