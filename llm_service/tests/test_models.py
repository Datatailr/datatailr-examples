"""Tests for the model manager."""

from __future__ import annotations

import asyncio
import json
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


def test_startup_download_failure_gives_failed(tmp_path):
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


def test_engine_exit_sets_failed_and_allows_new_switch(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        engine.alive_flag = False
        mgr.check_engine()
        assert mgr.state == "failed" and mgr.error == "engine exited"
        engine.alive_flag = True
        await mgr.request_switch("org/a")
        assert mgr.state == "ready"
        assert mgr.error is None

    asyncio.run(run())


def test_check_engine_ignores_other_states(tmp_path):
    mgr, engine, store, _ = _make(tmp_path)
    engine.alive_flag = False
    mgr.check_engine()
    assert mgr.state == "empty"


def test_status_shape(tmp_path):
    mgr, *_ = _make(tmp_path)
    assert mgr.status() == {"state": "empty", "active": None, "target": None, "error": None}


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
            "result": "ok", "error": None,
        }
        assert events[1]["model"] == "org/c" and events[1]["result"] == "failed"
        assert "org/c" in events[1]["error"]

    asyncio.run(run())


def test_failed_download_after_engine_crash_stays_failed(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path, bad_download="org/b")
        await mgr.startup()
        engine.alive_flag = False
        mgr.check_engine()
        assert mgr.state == "failed"
        await mgr.request_switch("org/b")
        assert mgr.state == "failed"
        assert "org/b" in mgr.error

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
