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


class ModelInUse(Exception):
    """A model revision is in use or is being removed."""


class ModelManager:
    """Hold the state of the served model: empty, loading, ready or failed.

    `active` is the model of the last successful load. `target` is the model
    that loads. `serving` is the model that the engine serves right now, or
    None when the engine serves no healthy model. During the weight fetch of
    an admin switch, `serving` is the old model and the state is `loading`.
    `rolling_back` is the previous model while a failed switch reloads it.
    `removing` holds the (model, revision) pairs that a delete removes now.
    """

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
        self.serving: dict | None = None
        self.rolling_back: dict | None = None
        self.removing: set[tuple[str, str]] = set()

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
            "serving": self.serving,
        }

    async def startup(self) -> None:
        """Load the saved model, or the default one. Never raise."""
        try:
            wanted = await asyncio.to_thread(self._read_active)
            self._begin(wanted)
            await self._load(wanted, previous=None, trigger="startup")
        except Exception as exc:  # noqa: BLE001
            log.exception("startup failed")
            self.state = "failed"
            self.target = None
            self.error = str(exc)

    def _read_active(self) -> dict:
        """Return the saved model. It runs in a worker thread."""
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
        if (model, revision) in self.removing:
            raise ModelInUse(f"{model} {revision} is being removed")
        if self.state == "loading":
            raise SwitchInProgress("a model switch is already running")
        wanted = {"model": model, "revision": revision}
        self._begin(wanted)
        self._task = asyncio.create_task(
            self._load(wanted, previous=self.active, trigger="switch")
        )
        return self._task

    def begin_remove(self, model: str, revisions: list[str]) -> None:
        """Mark revisions of a model as being removed.

        Raise `ModelInUse` if one of them is the active, serving, target or
        rollback model, or is already being removed. Nothing is marked in that case.
        Call `end_remove` when the removal ends. The check and the mark run
        without an `await`, so no switch can start between them.
        """
        pairs = {(model, revision) for revision in revisions}
        busy = {
            (entry["model"], entry["revision"])
            for entry in (self.active, self.serving, self.target, self.rolling_back)
            if entry
        }
        if pairs & busy:
            name, revision = sorted(pairs & busy)[0]
            raise ModelInUse(f"{name} {revision} is in use")
        if pairs & self.removing:
            name, revision = sorted(pairs & self.removing)[0]
            raise ModelInUse(f"{name} {revision} is being removed")
        self.removing |= pairs

    def end_remove(self, model: str, revisions: list[str]) -> None:
        """Clear the marks that `begin_remove` set."""
        self.removing -= {(model, revision) for revision in revisions}

    def check_engine(self) -> asyncio.Task | None:
        """Notice an engine process that has exited while it served a model.

        Then `serving` becomes None. In the `ready` state, this also starts one
        automatic reload of the active model with the startup load path and
        returns the task. During the weight fetch of a switch, it starts no
        reload: the switch goes on. If the reload fails, the state is `failed`
        until an admin switch. Call it inside a running loop.
        """
        if self.serving is None or self._engine.alive():
            return None
        self.serving = None
        if self.state != "ready":
            log.warning("the engine exited during a model switch")
            return None
        wanted = dict(self.active)
        log.warning("the engine exited, reloading %s", wanted["model"])
        self._begin(wanted)
        self.error = "engine exited"
        self._task = asyncio.create_task(self._reload(wanted))
        return self._task

    async def _reload(self, wanted: dict) -> None:
        await self._load(wanted, previous=None, trigger="reload")
        if self.state != "ready":
            self.state = "failed"
            self.active = None

    def _in_use(self) -> list[dict]:
        """Return the active model and the model that loads, if any."""
        return [entry for entry in (self.active, self.target) if entry]

    async def _save_active(self) -> None:
        """Write `active.json` in a worker thread. A failure only logs a warning."""
        try:
            await asyncio.to_thread(
                self._store.put, self._active_path, json.dumps(self.active)
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("could not write %s: %s", self._active_path, exc)

    async def _prune(self) -> None:
        """Delete the local copies of the models that are not in use."""
        try:
            await asyncio.to_thread(self._weights.prune, self._in_use)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not delete old local weights: %s", exc)

    async def _load(self, wanted: dict, previous: dict | None, trigger: str) -> None:
        model, revision = wanted["model"], wanted["revision"]
        try:
            try:
                model_dir = await asyncio.to_thread(
                    self._weights.ensure, model, revision
                )
            except Exception as exc:  # noqa: BLE001
                self._fail_before_stop(wanted, previous, f"{model}: {exc}", trigger)
                return
            if previous is not None:
                self.serving = None
                await self._engine.stop()
            try:
                await self._engine.start(model_dir, model)
            except Exception as exc:  # noqa: BLE001
                await self._fail_after_stop(wanted, previous, f"{model}: {exc}", trigger)
                return
            self.active = dict(wanted)
            self.serving = dict(wanted)
            self.target = None
            self.error = None
            self.state = "ready"
            await self._save_active()
            self._emit(wanted, "ok", None, trigger)
        except Exception as exc:  # noqa: BLE001
            log.exception("model switch crashed")
            self.serving = None
            self.target = None
            self.state = "failed"
            self.error = f"{model}: {exc}"
            self._emit(wanted, "failed", self.error, trigger)
            return
        await self._prune()

    def _fail_before_stop(
        self, wanted: dict, previous: dict | None, error: str, trigger: str
    ) -> None:
        # The old engine was not stopped. If it still runs, the old model keeps
        # serving. If it has exited, nothing serves.
        self.target = None
        self.error = error
        if not self._engine.alive():
            self.serving = None
        if previous is None:
            self.state = "empty"
        else:
            self.state = "ready" if self.serving is not None else "failed"
        self._emit(wanted, "failed", error, trigger)

    async def _fail_after_stop(
        self, wanted: dict, previous: dict | None, error: str, trigger: str
    ) -> None:
        self.target = None
        self.error = error
        self.active = None
        self.state = "failed"
        if previous is not None:
            self.rolling_back = dict(previous)
            try:
                try:
                    model_dir = await asyncio.to_thread(
                        self._weights.ensure, previous["model"], previous["revision"]
                    )
                    await self._engine.start(model_dir, previous["model"])
                except Exception as exc:  # noqa: BLE001
                    log.error("rollback to %s failed: %s", previous["model"], exc)
                else:
                    self.active = dict(previous)
                    self.serving = dict(previous)
                    self.state = "ready"
                    await self._save_active()
            finally:
                self.rolling_back = None
        self._emit(wanted, "failed", error, trigger)

    def _emit(
        self, wanted: dict, result: str, error: str | None, trigger: str
    ) -> None:
        """Report a load. `trigger` is `startup`, `switch` or `reload`."""
        try:
            self._on_event(
                {
                    "event": "model_switch",
                    "model": wanted["model"],
                    "revision": wanted["revision"],
                    "result": result,
                    "error": error,
                    "trigger": trigger,
                }
            )
        except Exception:  # noqa: BLE001
            log.exception("event handler failed")
