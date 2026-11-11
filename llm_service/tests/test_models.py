"""Tests for the model manager."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from pathlib import Path

import pytest

from llm_service.gateway.models import ModelManager, SwitchInProgress
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.gateway.weights import WeightCache
from llm_service.tests.fakes import FakeEngine


def _make(tmp_path: Path, bad_download: str | None = None):
    store = MemoryBlobStore()
    engine = FakeEngine()
    events: list[dict] = []

    def download(model: str, revision: str, target: Path) -> None:
        if model == bad_download:
            raise OSError("download failed")
        (target / "config.json").write_text("{}")

    weights = WeightCache(store, "root", tmp_path / "local", download)
    mgr = ModelManager(weights, engine, store, "root", "org/a", events.append)
    return mgr, engine, store, events


def _active(store) -> dict:
    return json.loads(store.get("root/state/active.json"))


def test_startup_loads_default_and_persists(tmp_path):
    mgr, engine, store, _ = _make(tmp_path)
    asyncio.run(mgr.startup())
    assert mgr.state == "ready"
    assert mgr.active == {"model": "org/a", "revision": "main"}
    assert _active(store) == mgr.active
    assert [name for _, name in engine.started] == ["org/a"]


def test_startup_prefers_active_json(tmp_path):
    mgr, engine, store, _ = _make(tmp_path)
    store.put("root/state/active.json", json.dumps({"model": "org/c", "revision": "v1"}))
    asyncio.run(mgr.startup())
    assert mgr.active == {"model": "org/c", "revision": "v1"}


def test_startup_failure_sets_failed_and_never_raises(tmp_path):
    mgr, engine, store, _ = _make(tmp_path)
    engine.fail_on = {"org/a"}
    asyncio.run(mgr.startup())
    assert mgr.state == "failed"
    assert "org/a" in mgr.error
    assert mgr.active is None


def test_startup_download_failure_gives_empty(tmp_path):
    mgr, engine, store, _ = _make(tmp_path, bad_download="org/a")
    asyncio.run(mgr.startup())
    assert mgr.state == "empty"
    assert "org/a" in mgr.error


def test_switch_ok(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        task = mgr.request_switch("org/b")
        assert mgr.state == "loading"
        assert mgr.target == {"model": "org/b", "revision": "main"}
        await task
        assert mgr.state == "ready"
        assert mgr.active["model"] == "org/b" and engine.stopped == 1
        assert mgr.target is None and mgr.error is None
        assert _active(store)["model"] == "org/b"

    asyncio.run(run())


def test_second_switch_during_loading_raises_switch_in_progress(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        task = mgr.request_switch("org/b")
        with pytest.raises(SwitchInProgress):
            mgr.request_switch("org/c")
        await task

    asyncio.run(run())


def test_switch_start_failure_rolls_back(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        engine.fail_on = {"org/b"}
        await mgr.request_switch("org/b")
        assert mgr.state == "ready" and mgr.active["model"] == "org/a"
        assert "org/b" in mgr.error
        assert _active(store)["model"] == "org/a"
        assert [name for _, name in engine.started] == ["org/a", "org/a"]

    asyncio.run(run())


def test_rollback_failure_sets_failed(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        engine.fail_on = {"org/a", "org/b"}
        await mgr.request_switch("org/b")
        assert mgr.state == "failed"
        assert "org/b" in mgr.error

    asyncio.run(run())


def test_download_failure_keeps_old_model_running(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path, bad_download="org/b")
        await mgr.startup()
        await mgr.request_switch("org/b")
        assert mgr.state == "ready" and mgr.active["model"] == "org/a"
        assert engine.stopped == 0
        assert "org/b" in mgr.error
        assert mgr.target is None
        assert _active(store)["model"] == "org/a"

    asyncio.run(run())


def test_download_failure_without_active_model_gives_empty(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path, bad_download="org/b")
        await mgr.request_switch("org/b")
        assert mgr.state == "empty" and mgr.active is None
        assert mgr.error

    asyncio.run(run())


def test_invalid_name_raises_and_touches_nothing(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()
        listing = store.ls("")
        for model, revision in [("../x", "main"), ("org/b", "a/b"), ("nope", "main")]:
            with pytest.raises(ValueError):
                mgr.request_switch(model, revision)
        assert mgr.state == "ready" and mgr.target is None
        assert store.ls("") == listing
        assert engine.stopped == 0
        assert len(events) == 1

    asyncio.run(run())


def test_engine_exit_reloads_the_active_model_once(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()
        events.clear()
        engine.alive_flag = False
        task = mgr.check_engine()
        assert isinstance(task, asyncio.Task)
        assert mgr.state == "loading" and mgr.error == "engine exited"
        assert mgr.target == {"model": "org/a", "revision": "main"}
        assert mgr.check_engine() is None  # no second reload while loading
        with pytest.raises(SwitchInProgress):
            mgr.request_switch("org/b")
        await task
        assert mgr.state == "ready" and mgr.error is None and mgr.target is None
        assert mgr.active == {"model": "org/a", "revision": "main"}
        assert [name for _, name in engine.started] == ["org/a", "org/a"]
        assert [(e["model"], e["result"], e["trigger"]) for e in events] == [
            ("org/a", "ok", "reload")
        ]
        # The model was ready again, so the next exit gets a new reload.
        engine.alive_flag = False
        await mgr.check_engine()
        assert mgr.state == "ready"
        assert len(engine.started) == 3

    asyncio.run(run())


def test_failed_automatic_reload_stays_failed_until_an_admin_switch(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()
        events.clear()
        engine.alive_flag = False
        engine.fail_on = {"org/a"}
        await mgr.check_engine()
        assert mgr.state == "failed" and mgr.active is None
        assert "org/a" in mgr.error
        assert [(e["result"], e["trigger"]) for e in events] == [("failed", "reload")]
        assert mgr.check_engine() is None
        assert mgr.state == "failed"
        assert len(events) == 1  # no second automatic attempt
        engine.fail_on = set()
        await mgr.request_switch("org/a")
        assert mgr.state == "ready" and mgr.error is None

    asyncio.run(run())


def test_failed_reload_without_weights_is_failed(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()

        def broken_ensure(model, revision):
            raise OSError("disk gone")

        mgr._weights.ensure = broken_ensure
        engine.alive_flag = False
        await mgr.check_engine()
        assert mgr.state == "failed" and mgr.active is None
        assert "disk gone" in mgr.error

    asyncio.run(run())


def test_check_engine_ignores_other_states(tmp_path):
    mgr, engine, store, _ = _make(tmp_path)
    engine.alive_flag = False
    mgr.check_engine()
    assert mgr.state == "empty"


def test_status_shape(tmp_path):
    mgr, *_ = _make(tmp_path)
    assert mgr.status() == {
        "state": "empty", "active": None, "target": None, "error": None, "serving": None,
    }


A = {"model": "org/a", "revision": "main"}
B = {"model": "org/b", "revision": "main"}


class WatchingEngine(FakeEngine):
    """A fake engine that records `manager.serving` at each start and stop."""

    def __init__(self) -> None:
        super().__init__()
        self.manager: ModelManager | None = None
        self.seen: list[tuple[str, dict | None]] = []

    async def start(self, model_dir: Path, served_name: str) -> None:
        self.seen.append((f"start {served_name}", self.manager.serving))
        await super().start(model_dir, served_name)

    async def stop(self) -> None:
        self.seen.append(("stop", self.manager.serving))
        await super().stop()


def _make_gated(tmp_path: Path, gated: str, bad_download: str | None = None):
    """Return a manager whose download of `gated` waits for `gate`.

    `fetching` is set when that download begins, so a test can act during the
    fetch phase of a switch.
    """
    store = MemoryBlobStore()
    engine = WatchingEngine()
    events: list[dict] = []
    gate, fetching = threading.Event(), threading.Event()

    def download(model: str, revision: str, target: Path) -> None:
        if model == gated:
            fetching.set()
            gate.wait(10)
        if model == bad_download:
            raise OSError("download failed")
        (target / "config.json").write_text("{}")

    weights = WeightCache(store, "root", tmp_path / "local", download)
    mgr = ModelManager(weights, engine, store, "root", "org/a", events.append)
    engine.manager = mgr
    return mgr, engine, gate, fetching


async def _in_fetch_phase(fetching: threading.Event) -> None:
    assert await asyncio.to_thread(fetching.wait, 10)


def test_serving_is_none_during_the_startup_load(tmp_path):
    mgr, engine, gate, _ = _make_gated(tmp_path, gated="none")
    asyncio.run(mgr.startup())
    assert engine.seen == [("start org/a", None)]
    assert mgr.serving == A


def test_serving_stays_none_after_a_failed_startup(tmp_path):
    mgr, engine, gate, _ = _make_gated(tmp_path, gated="none")
    engine.fail_on = {"org/a"}
    asyncio.run(mgr.startup())
    assert mgr.state == "failed" and mgr.serving is None


def test_serving_through_a_switch(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, gated="org/b")
        await mgr.startup()
        task = mgr.request_switch("org/b")
        await _in_fetch_phase(fetching)
        # The old model serves while the new weights download.
        assert mgr.state == "loading" and mgr.serving == A
        assert mgr.status()["serving"] == A
        gate.set()
        await task
        assert mgr.state == "ready" and mgr.serving == B
        assert engine.seen == [
            ("start org/a", None),
            ("stop", None),
            ("start org/b", None),
        ]

    asyncio.run(run())


def test_serving_keeps_the_old_model_after_a_failed_download(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, gated="org/b", bad_download="org/b")
        await mgr.startup()
        task = mgr.request_switch("org/b")
        await _in_fetch_phase(fetching)
        assert mgr.serving == A
        gate.set()
        await task
        assert mgr.state == "ready" and mgr.serving == A
        assert engine.stopped == 0

    asyncio.run(run())


def test_serving_after_a_failed_start_with_a_rollback(tmp_path):
    async def run():
        mgr, engine, gate, _ = _make_gated(tmp_path, gated="none")
        await mgr.startup()
        engine.fail_on = {"org/b"}
        await mgr.request_switch("org/b")
        assert mgr.state == "ready" and mgr.serving == A
        # Nothing serves while vLLM restarts, and during the rollback.
        assert engine.seen[1:] == [("stop", None), ("start org/b", None), ("start org/a", None)]

    asyncio.run(run())


def test_serving_is_none_after_a_failed_rollback(tmp_path):
    async def run():
        mgr, engine, gate, _ = _make_gated(tmp_path, gated="none")
        await mgr.startup()
        engine.fail_on = {"org/a", "org/b"}
        await mgr.request_switch("org/b")
        assert mgr.state == "failed" and mgr.serving is None

    asyncio.run(run())


def test_serving_through_an_engine_exit_and_reload(tmp_path):
    async def run():
        mgr, engine, gate, _ = _make_gated(tmp_path, gated="none")
        await mgr.startup()
        engine.alive_flag = False
        task = mgr.check_engine()
        assert mgr.state == "loading" and mgr.serving is None
        await task
        assert mgr.state == "ready" and mgr.serving == A
        engine.alive_flag = False
        engine.fail_on = {"org/a"}
        await mgr.check_engine()
        assert mgr.state == "failed" and mgr.serving is None

    asyncio.run(run())


def test_engine_exit_during_the_fetch_phase(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, gated="org/b")
        await mgr.startup()
        task = mgr.request_switch("org/b")
        await _in_fetch_phase(fetching)
        engine.alive_flag = False
        assert mgr.check_engine() is None  # the running switch goes on, no reload
        assert mgr.state == "loading" and mgr.serving is None
        assert mgr.target == B
        gate.set()
        await task
        assert mgr.state == "ready" and mgr.serving == B
        assert [name for _, name in engine.started] == ["org/a", "org/b"]

    asyncio.run(run())


def test_engine_exit_during_the_fetch_phase_then_a_failed_download(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, gated="org/b", bad_download="org/b")
        await mgr.startup()
        task = mgr.request_switch("org/b")
        await _in_fetch_phase(fetching)
        engine.alive_flag = False
        mgr.check_engine()
        gate.set()
        await task
        assert mgr.state == "failed" and mgr.serving is None

    asyncio.run(run())


def test_events_emitted_for_ok_and_failed(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()
        events.clear()
        await mgr.request_switch("org/b")
        engine.fail_on = {"org/c"}
        await mgr.request_switch("org/c")
        assert events[0] == {
            "event": "model_switch", "model": "org/b", "revision": "main",
            "result": "ok", "error": None, "trigger": "switch",
        }
        assert events[1]["model"] == "org/c" and events[1]["result"] == "failed"
        assert "org/c" in events[1]["error"]

    asyncio.run(run())


def test_failed_download_after_engine_crash_stays_failed(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path, bad_download="org/b")
        await mgr.startup()
        engine.alive_flag = False  # the engine died, check_engine has not run yet
        await mgr.request_switch("org/b")
        assert mgr.state == "failed"
        assert "org/b" in mgr.error
        assert mgr.serving is None

    asyncio.run(run())


def test_startup_ignores_corrupt_active_json(tmp_path):
    for content in ["not json", "[1, 2]", json.dumps({"revision": "v1"}),
                    json.dumps({"model": "../x", "revision": "main"})]:
        mgr, engine, store, _ = _make(tmp_path)
        store.put("root/state/active.json", content)
        asyncio.run(mgr.startup())
        assert mgr.state == "ready"
        assert mgr.active == {"model": "org/a", "revision": "main"}


def test_startup_fails_on_store_error(tmp_path):
    class BrokenStore(MemoryBlobStore):
        def get(self, path):
            raise OSError("store down")

    mgr, engine, store, _ = _make(tmp_path)
    broken = BrokenStore()
    broken.put("root/state/active.json", "{}")
    mgr._store = broken
    asyncio.run(mgr.startup())
    assert mgr.state == "failed"
    assert "store down" in mgr.error


def test_startup_event_has_the_startup_trigger(tmp_path):
    mgr, engine, store, events = _make(tmp_path)
    asyncio.run(mgr.startup())
    assert [e["trigger"] for e in events] == ["startup"]


class ActiveWriteFails(MemoryBlobStore):
    """A store whose writes of active.json fail. It records the calling threads."""

    def __init__(self) -> None:
        super().__init__()
        self.threads: list[int] = []

    def _note(self) -> None:
        self.threads.append(threading.get_ident())

    def put(self, path, data):
        if path.endswith("state/active.json"):
            self._note()
            raise OSError("store down")
        super().put(path, data)

    def exists(self, path):
        if path.endswith("state/active.json"):
            self._note()
        return super().exists(path)

    def get(self, path):
        if path.endswith("state/active.json"):
            self._note()
        return super().get(path)


def _with_store(tmp_path, store):
    engine = FakeEngine()
    events: list[dict] = []

    def download(model, revision, target):
        (target / "config.json").write_text("{}")

    weights = WeightCache(store, "root", tmp_path / "local", download)
    return ModelManager(weights, engine, store, "root", "org/a", events.append), engine


def test_failed_active_json_write_keeps_the_model_ready(tmp_path, caplog):
    store = ActiveWriteFails()

    async def run():
        mgr, engine = _with_store(tmp_path, store)
        loop_thread = threading.get_ident()
        with caplog.at_level(logging.WARNING, logger="llm_service.models"):
            await mgr.startup()
            assert mgr.state == "ready" and mgr.error is None
            await mgr.request_switch("org/b")
            assert mgr.state == "ready" and mgr.active["model"] == "org/b"
            engine.fail_on = {"org/c"}
            await mgr.request_switch("org/c")  # the rollback writes active.json too
            assert mgr.state == "ready" and mgr.active["model"] == "org/b"
        return loop_thread

    loop_thread = asyncio.run(run())
    assert any("active.json" in m for m in caplog.messages)
    assert len(store.threads) >= 4  # exists at startup, then three writes
    assert loop_thread not in store.threads


def test_successful_switch_deletes_other_local_models(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        local = tmp_path / "local"
        assert (local / "org--a" / "main").is_dir()
        await mgr.request_switch("org/b")
        assert not (local / "org--a").exists()
        assert (local / "org--b" / "main" / ".complete").exists()
        assert store.exists("root/models/org--a/main/.complete")  # the blob copy stays
        engine.fail_on = {"org/c"}
        await mgr.request_switch("org/c")  # a failed switch deletes nothing
        assert (local / "org--b" / "main" / ".complete").exists()
        assert (local / "org--c" / "main" / ".complete").exists()

    asyncio.run(run())
