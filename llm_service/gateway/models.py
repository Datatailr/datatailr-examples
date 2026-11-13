"""The model manager: load a model at startup and switch on request."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Callable

from .audit import iso, utc_now
from .engine import Engine
from .storage import BlobStore
from .weights import WeightCache, validate_model_name, validate_revision

log = logging.getLogger("llm_service.models")


class SwitchInProgress(Exception):
    """A model switch is already running."""


class ModelInUse(Exception):
    """A model revision is in use or is being removed."""


class DownloadInProgress(Exception):
    """A download without a switch is already running."""


# The longest error text that `download` keeps.
DOWNLOAD_ERROR_CHARS = 300


class ModelManager:
    """Hold the state of the served model: empty, loading, ready or failed.

    `active` is the model of the last successful load. `target` is the model
    that loads. `serving` is the model that the engine serves right now, or
    None when the engine serves no healthy model. During the weight fetch of
    an admin switch, `serving` is the old model and the state is `loading`.
    `rolling_back` is the previous model while a failed switch reloads it.
    `removing` holds the (model, revision) pairs that a delete removes now.
    `starting` is True while `startup` reads the saved model, before `target`
    names it.
    `download` is the last download without a switch, or None. It holds
    `model`, `revision`, `state` (`running`, `done` or `failed`), `error` and
    `started_at`.
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
        self.download: dict | None = None
        # `startup` sets the flag. A manager that never starts accepts deletes.
        self.starting = False

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
            "rolling_back": self.rolling_back,
            "download": dict(self.download) if self.download else None,
        }

    async def startup(self) -> None:
        """Load the saved model, or the default one. Never raise.

        No delete runs while the saved model is read: `begin_remove` refuses it.
        """
        self.starting = True
        try:
            wanted = await asyncio.to_thread(self._read_active)
            self._begin(wanted)
            self.starting = False
            await self._load(wanted, previous=None, trigger="startup")
        except Exception as exc:  # noqa: BLE001
            log.exception("startup failed")
            self.state = "failed"
            self.target = None
            self.error = str(exc)
        finally:
            self.starting = False

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
            raise ModelInUse(f"a delete removes {model} {revision} now")
        if self.state == "loading":
            raise SwitchInProgress("a model switch is already running")
        if self.rolling_back is not None:
            raise SwitchInProgress("the service loads the previous model again after a failed switch")
        wanted = {"model": model, "revision": revision}
        self._begin(wanted)
        self._task = asyncio.create_task(
            self._load(wanted, previous=self.active, trigger="switch")
        )
        return self._task

    def request_download(self, model: str, revision: str, admin: str) -> asyncio.Task:
        """Start a download of the weights without a switch. Call it inside a running loop.

        Raise `ValueError` for a bad name, `ModelInUse` for a pair that a delete
        removes, and `DownloadInProgress` while a download runs. The check and
        the mark run without an `await`, so no delete can start between them.
        """
        validate_model_name(model)
        validate_revision(revision)
        if (model, revision) in self.removing:
            raise ModelInUse(f"a delete removes {model} {revision} now")
        if self.running_download() is not None:
            raise DownloadInProgress("a model download is already running")
        self.download = {
            "model": model,
            "revision": revision,
            "state": "running",
            "error": None,
            "started_at": iso(utc_now()),
        }
        return asyncio.create_task(self._download(model, revision, admin))

    def running_download(self) -> dict | None:
        """Return the pair of the running download as `model` and `revision`, or None."""
        if self.download is None or self.download["state"] != "running":
            return None
        return {"model": self.download["model"], "revision": self.download["revision"]}

    async def _download(self, model: str, revision: str, admin: str) -> None:
        # `ensure` holds the cache lock for the whole fetch. A switch to the
        # same pair waits for it and then finds the files.
        error = None
        try:
            try:
                await asyncio.to_thread(self._weights.ensure, model, revision)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                error = f"{type(exc).__name__}: {exc}"[:DOWNLOAD_ERROR_CHARS]
                log.warning("the download of %s %s failed: %s", model, revision, error)
            else:
                # The local copy goes unless the engine uses the pair. During
                # the start, `startup` has not named the saved model yet.
                if not self.starting:
                    await self._prune()
        except asyncio.CancelledError:
            self._end_download(model, revision, admin, "cancelled")
            raise
        self._end_download(model, revision, admin, error)

    def _end_download(self, model: str, revision: str, admin: str, error: str | None) -> None:
        """Set the end state of the download and report it."""
        state = "done" if error is None else "failed"
        self.download = {**self.download, "state": state, "error": error}
        try:
            self._on_event(
                {
                    "event": "model_download",
                    "model": model,
                    "revision": revision,
                    "result": "ok" if error is None else "error",
                    "error": error,
                    "admin": admin,
                }
            )
        except Exception:  # noqa: BLE001
            log.exception("event handler failed")

    def begin_remove(self, model: str, revisions: list[str]) -> None:
        """Mark revisions of a model as being removed.

        Raise `ModelInUse` while the service starts, or if one of them is the
        active, serving, target, rollback or downloading model, or is already
        being removed.
        Nothing is marked in that case.
        Call `end_remove` when the removal ends. The check and the mark run
        without an `await`, so no switch can start between them.
        """
        if self.starting:
            raise ModelInUse("the service is starting")
        pairs = {(model, revision) for revision in revisions}
        busy = {
            (entry["model"], entry["revision"])
            for entry in (self.active, self.serving, self.target, self.rolling_back)
            if entry
        }
        downloading = self.running_download()
        if downloading is not None:
            busy.add((downloading["model"], downloading["revision"]))
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
        """Return the active, loading, serving and rollback models, if any.

        During a rollback, only `rolling_back` names the model that the engine
        starts. A prune keeps the local copies of all of them.
        """
        return [
            entry
            for entry in (self.active, self.target, self.serving, self.rolling_back)
            if entry
        ]

    async def _save_active(self) -> None:
        """Write `active.json` in a worker thread. A failure only logs a warning."""
        try:
            await asyncio.to_thread(
                self._store.put, self._active_path, json.dumps(self.active)
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("the write of %s failed: %s", self._active_path, exc)

    async def _prune(self) -> None:
        """Delete the local copies of the models that are not in use."""
        try:
            await asyncio.to_thread(self._weights.prune, self._in_use)
        except Exception as exc:  # noqa: BLE001
            log.warning("the delete of old local weights failed: %s", exc)

    async def _served_copy(self, model: str, revision: str) -> Path:
        """Return the local copy of a model that the engine served last.

        A complete local copy is used without the cache lock, so a download
        does not hold up a reload or a rollback. That is safe: the pair is the
        active or previous model, so no delete or prune removes it. Without a
        local copy, `ensure` fetches the files.
        """
        local = await asyncio.to_thread(self._weights.local_copy, model, revision)
        if local is not None:
            return local
        return await asyncio.to_thread(self._weights.ensure, model, revision)

    async def _load(self, wanted: dict, previous: dict | None, trigger: str) -> None:
        model, revision = wanted["model"], wanted["revision"]
        try:
            try:
                if trigger == "reload":
                    model_dir = await self._served_copy(model, revision)
                else:
                    # An admin switch takes the cache lock: a prune at the
                    # same time can delete the local copy of a new target.
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
                    model_dir = await self._served_copy(
                        previous["model"], previous["revision"]
                    )
                    await self._engine.start(model_dir, previous["model"])
                except Exception as exc:  # noqa: BLE001
                    log.error("rollback to %s failed: %s", previous["model"], exc)
                else:
                    # Clear the mark with the state change, so no reader sees
                    # `ready` while a rollback still shows.
                    self.rolling_back = None
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
