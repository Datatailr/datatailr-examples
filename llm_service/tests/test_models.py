"""Tests for the model manager."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
from pathlib import Path

import pytest

from llm_service.gateway.models import DownloadInProgress, ModelInUse, ModelManager, SwitchInProgress
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
        # Without a complete local copy, the reload fetches the files.
        (tmp_path / "local" / "org--a" / "main" / ".complete").unlink()
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
        "rolling_back": None, "download": None,
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


def test_begin_remove_refuses_the_active_and_the_target_model(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        with pytest.raises(ModelInUse):
            mgr.begin_remove("org/a", ["old", "main"])
        assert mgr.removing == set()
        task = mgr.request_switch("org/b", "v1")
        with pytest.raises(ModelInUse):
            mgr.begin_remove("org/b", ["v1"])
        mgr.begin_remove("org/b", ["main"])  # another revision of the target is fine
        assert mgr.removing == {("org/b", "main")}
        await task

    asyncio.run(run())


def test_begin_remove_refuses_the_serving_model(tmp_path):
    mgr, *_ = _make(tmp_path)
    mgr.serving = {"model": "org/s", "revision": "main"}
    with pytest.raises(ModelInUse):
        mgr.begin_remove("org/s", ["main"])


def test_begin_remove_refuses_a_pair_that_is_already_being_removed(tmp_path):
    mgr, *_ = _make(tmp_path)
    mgr.begin_remove("org/b", ["main", "v1"])
    with pytest.raises(ModelInUse):
        mgr.begin_remove("org/b", ["v1", "v2"])
    assert mgr.removing == {("org/b", "main"), ("org/b", "v1")}


def test_request_switch_refuses_a_model_that_is_being_removed(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        mgr.begin_remove("org/b", ["main"])
        with pytest.raises(ModelInUse):
            mgr.request_switch("org/b")
        assert mgr.state == "ready" and mgr.target is None
        task = mgr.request_switch("org/b", "v2")  # another revision is fine
        await task

    asyncio.run(run())


def test_end_remove_allows_the_switch_again(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        mgr.begin_remove("org/b", ["main"])
        mgr.end_remove("org/b", ["main"])
        assert mgr.removing == set()
        await mgr.request_switch("org/b")
        assert mgr.active["model"] == "org/b"

    asyncio.run(run())


class HoldingEngine(FakeEngine):
    """A fake engine. Its `start` of `held` waits for `gate` after `armed` is set."""

    def __init__(self, held: str) -> None:
        super().__init__()
        self.held = held
        self.armed = False
        self.waiting = threading.Event()
        self.gate = threading.Event()

    async def start(self, model_dir: Path, served_name: str) -> None:
        if self.armed and served_name == self.held:
            self.waiting.set()
            await asyncio.to_thread(self.gate.wait, 10)
        await super().start(model_dir, served_name)


def test_begin_remove_refuses_the_model_of_a_rollback(tmp_path):
    async def run():
        mgr, _, store, _ = _make(tmp_path)
        engine = HoldingEngine("org/a")
        mgr._engine = engine
        await mgr.startup()
        engine.armed = True
        engine.fail_on = {"org/b"}
        task = mgr.request_switch("org/b")
        while not engine.waiting.is_set():
            await asyncio.sleep(0.01)
        # The rollback to org/a runs: it is not active, serving or target.
        assert mgr.active is None and mgr.serving is None and mgr.target is None
        with pytest.raises(ModelInUse):
            mgr.begin_remove("org/a", ["main"])
        engine.gate.set()
        await task
        assert mgr.state == "ready" and mgr.rolling_back is None
        with pytest.raises(ModelInUse):  # active again
            mgr.begin_remove("org/a", ["main"])
        mgr.begin_remove("org/b", ["main"])

    asyncio.run(run())


def test_rollback_mark_is_cleared_when_the_rollback_fails(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        engine.fail_on = {"org/a", "org/b"}
        await mgr.request_switch("org/b")
        assert mgr.state == "failed" and mgr.rolling_back is None
        mgr.begin_remove("org/a", ["main"])

    asyncio.run(run())


def test_begin_remove_message_names_a_removal(tmp_path):
    mgr, *_ = _make(tmp_path)
    mgr.begin_remove("org/b", ["main"])
    with pytest.raises(ModelInUse, match="being removed"):
        mgr.begin_remove("org/b", ["main"])


def test_switch_during_a_rollback_raises_switch_in_progress(tmp_path):
    async def run():
        mgr, _, store, _ = _make(tmp_path)
        engine = HoldingEngine("org/a")
        mgr._engine = engine
        await mgr.startup()
        engine.armed = True
        engine.fail_on = {"org/b"}
        task = mgr.request_switch("org/b")
        while not engine.waiting.is_set():
            await asyncio.sleep(0.01)
        # The failed switch reloads org/a. The state is `failed` until it ends.
        assert mgr.state == "failed"
        assert mgr.status()["rolling_back"] == A
        with pytest.raises(SwitchInProgress):
            mgr.request_switch("org/c")
        assert mgr.target is None
        engine.gate.set()
        await task
        assert mgr.state == "ready" and mgr.status()["rolling_back"] is None
        assert [name for _, name in engine.started] == ["org/a", "org/a"]
        await mgr.request_switch("org/c")
        assert mgr.active["model"] == "org/c"

    asyncio.run(run())


class HeldActiveStore(MemoryBlobStore):
    """A store whose check of active.json waits for `gate`. `reading` is set when it begins."""

    def __init__(self) -> None:
        super().__init__()
        self.reading = threading.Event()
        self.gate = threading.Event()
        self.fail = False

    def exists(self, path):
        if path.endswith("state/active.json"):
            self.reading.set()
            self.gate.wait(10)
            if self.fail:
                raise OSError("store down")
        return super().exists(path)


@pytest.mark.parametrize("fail", [False, True])
def test_begin_remove_refuses_while_the_service_starts(tmp_path, fail):
    store = HeldActiveStore()
    store.fail = fail

    async def run():
        mgr, _ = _with_store(tmp_path, store)
        assert mgr.starting is False  # a manager without a startup accepts deletes
        task = asyncio.create_task(mgr.startup())
        assert await asyncio.to_thread(store.reading.wait, 10)
        assert mgr.starting is True and mgr.target is None
        with pytest.raises(ModelInUse, match="starting"):
            mgr.begin_remove("org/x", ["main"])
        assert mgr.removing == set()
        store.gate.set()
        await task
        assert mgr.starting is False
        mgr.begin_remove("org/x", ["main"])

    try:
        asyncio.run(run())
    finally:
        store.gate.set()


def _download_events(events: list[dict]) -> list[dict]:
    return [e for e in events if e["event"] == "model_download"]


def test_download_runs_then_is_done_and_emits_the_event(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()
        task = mgr.request_download("org/b", "v1", "alice")
        download = mgr.status()["download"]
        assert download["model"] == "org/b" and download["revision"] == "v1"
        assert download["state"] == "running" and download["error"] is None
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", download["started_at"])
        await task
        assert mgr.download["state"] == "done" and mgr.download["error"] is None
        assert mgr.download["started_at"] == download["started_at"]
        assert store.exists("root/models/org--b/v1/.complete")
        assert _download_events(events) == [
            {"event": "model_download", "model": "org/b", "revision": "v1",
             "result": "ok", "error": None, "admin": "alice"}
        ]
        # A download changes nothing of the served model.
        assert mgr.state == "ready" and mgr.active == A and mgr.target is None
        assert [name for _, name in engine.started] == ["org/a"]

    asyncio.run(run())


def test_failed_download_keeps_the_error_and_leaves_no_marker(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path, bad_download="org/b")
        await mgr.startup()
        await mgr.request_download("org/b", "main", "bob")
        assert mgr.download["state"] == "failed"
        assert mgr.download["error"] == "OSError: download failed"
        assert not store.exists("root/models/org--b/main/.complete")
        assert _download_events(events) == [
            {"event": "model_download", "model": "org/b", "revision": "main",
             "result": "error", "error": "OSError: download failed", "admin": "bob"}
        ]
        assert mgr.state == "ready" and mgr.error is None

    asyncio.run(run())


def test_download_error_text_is_at_most_300_characters(tmp_path):
    async def run():
        store = MemoryBlobStore()

        def download(model: str, revision: str, target: Path) -> None:
            raise RuntimeError("x" * 1000)

        weights = WeightCache(store, "root", tmp_path / "local", download)
        mgr = ModelManager(weights, FakeEngine(), store, "root", "org/a")
        await mgr.request_download("org/b", "main", "alice")
        assert mgr.download["error"] == ("RuntimeError: " + "x" * 1000)[:300]

    asyncio.run(run())


def test_second_download_while_one_runs_raises_download_in_progress(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, "org/b")
        await mgr.startup()
        task = mgr.request_download("org/b", "main", "alice")
        await _in_fetch_phase(fetching)
        with pytest.raises(DownloadInProgress):
            mgr.request_download("org/c", "main", "alice")
        assert mgr.download["model"] == "org/b" and mgr.download["state"] == "running"
        gate.set()
        await task
        assert mgr.download["state"] == "done"

    asyncio.run(run())


def test_download_refuses_a_pair_that_is_being_removed(tmp_path):
    async def run():
        mgr, *_ = _make(tmp_path)
        await mgr.startup()
        mgr.begin_remove("org/b", ["main"])
        with pytest.raises(ModelInUse, match="a delete removes org/b main now"):
            mgr.request_download("org/b", "main", "alice")
        assert mgr.download is None
        mgr.end_remove("org/b", ["main"])
        await mgr.request_download("org/b", "v2", "alice")  # no delete runs now

    asyncio.run(run())


@pytest.mark.parametrize("model,revision", [("nonsense", "main"), ("org/b", "a/b")])
def test_download_validates_the_names(tmp_path, model, revision):
    async def run():
        mgr, *_ = _make(tmp_path)
        with pytest.raises(ValueError):
            mgr.request_download(model, revision, "alice")
        assert mgr.download is None

    asyncio.run(run())


def test_a_new_download_after_a_failed_one_starts_again(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path, bad_download="org/b")
        await mgr.startup()
        await mgr.request_download("org/b", "main", "alice")
        assert mgr.download["state"] == "failed"
        task = mgr.request_download("org/c", "main", "bob")
        assert mgr.download["model"] == "org/c" and mgr.download["state"] == "running"
        assert mgr.download["error"] is None
        await task
        assert mgr.download["state"] == "done"
        assert [e["result"] for e in _download_events(events)] == ["error", "ok"]

    asyncio.run(run())


def test_begin_remove_refuses_the_pair_of_a_running_download(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, "org/b")
        await mgr.startup()
        task = mgr.request_download("org/b", "main", "alice")
        await _in_fetch_phase(fetching)
        with pytest.raises(ModelInUse):
            mgr.begin_remove("org/b", ["main"])
        assert mgr.removing == set()
        mgr.begin_remove("org/b", ["v2"])  # another revision is fine
        mgr.end_remove("org/b", ["v2"])
        gate.set()
        await task
        mgr.begin_remove("org/b", ["main"])  # a finished download holds nothing
        assert mgr.removing == {("org/b", "main")}

    asyncio.run(run())


def test_download_leaves_no_local_copy_unless_the_engine_uses_it(tmp_path):
    async def run():
        mgr, engine, store, _ = _make(tmp_path)
        await mgr.startup()
        local = tmp_path / "local"
        await mgr.request_download("org/b", "main", "alice")
        assert not (local / "org--b").exists()
        assert store.exists("root/models/org--b/main/.complete")
        assert (local / "org--a" / "main" / ".complete").exists()
        await mgr.request_download("org/a", "main", "alice")  # the served pair stays
        assert (local / "org--a" / "main" / ".complete").exists()
        assert mgr.download["state"] == "done"

    asyncio.run(run())


def test_switch_to_the_pair_of_a_running_download_finds_the_files(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, "org/b")
        await mgr.startup()
        download = mgr.request_download("org/b", "main", "alice")
        await _in_fetch_phase(fetching)
        switch = mgr.request_switch("org/b")
        gate.set()
        await download
        await switch
        assert mgr.download["state"] == "done"
        assert mgr.state == "ready" and mgr.active == B
        assert (tmp_path / "local" / "org--b" / "main" / ".complete").exists()

    asyncio.run(run())


def test_a_cancelled_download_is_failed_and_a_new_one_starts(tmp_path):
    async def run():
        mgr, engine, gate, fetching = _make_gated(tmp_path, "org/b")
        await mgr.startup()
        task = mgr.request_download("org/b", "main", "alice")
        await _in_fetch_phase(fetching)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert mgr.download["state"] == "failed" and mgr.download["error"] == "cancelled"
        mgr.begin_remove("org/b", ["main"])  # the cancelled download holds nothing
        mgr.end_remove("org/b", ["main"])
        gate.set()
        await mgr.request_download("org/c", "main", "alice")
        assert mgr.download["model"] == "org/c" and mgr.download["state"] == "done"

    asyncio.run(run())


def test_download_during_a_rollback_keeps_the_local_copy_of_the_rollback_model(tmp_path):
    async def run():
        mgr, _, store, _ = _make(tmp_path)
        engine = HoldingEngine("org/a")
        mgr._engine = engine
        await mgr.startup()
        engine.armed = True
        engine.fail_on = {"org/b"}
        task = mgr.request_switch("org/b")
        while not engine.waiting.is_set():
            await asyncio.sleep(0.01)
        assert mgr.active is None and mgr.target is None and mgr.rolling_back == A
        await mgr.request_download("org/c", "main", "alice")
        assert mgr.download["state"] == "done"
        assert (tmp_path / "local" / "org--a" / "main" / ".complete").exists()
        engine.gate.set()
        await task
        assert mgr.state == "ready" and mgr.active == A

    asyncio.run(run())


def test_in_use_names_the_serving_and_the_rollback_model(tmp_path):
    mgr, *_ = _make(tmp_path)
    mgr.serving = {"model": "org/s", "revision": "main"}
    mgr.rolling_back = {"model": "org/r", "revision": "v1"}
    assert {"model": "org/s", "revision": "main"} in mgr._in_use()
    assert {"model": "org/r", "revision": "v1"} in mgr._in_use()


def test_download_during_the_start_prunes_nothing(tmp_path):
    first, _ = _with_store(tmp_path, MemoryBlobStore())
    asyncio.run(first.startup())
    saved = tmp_path / "local" / "org--a" / "main" / ".complete"
    assert saved.exists()
    store = HeldActiveStore()

    async def run():
        mgr, _ = _with_store(tmp_path, store)
        task = asyncio.create_task(mgr.startup())
        assert await asyncio.to_thread(store.reading.wait, 10)
        assert mgr.starting is True and mgr.active is None and mgr.target is None
        await mgr.request_download("org/c", "main", "alice")
        assert mgr.download["state"] == "done"
        assert saved.exists()
        store.gate.set()
        await task
        assert mgr.state == "ready"

    try:
        asyncio.run(run())
    finally:
        store.gate.set()


async def _wait_ready(task: asyncio.Task) -> None:
    """Wait at most 5 seconds for `task`. A task that waits for the cache lock stays pending."""
    await asyncio.wait({task}, timeout=5)


def test_reload_after_an_engine_exit_does_not_wait_for_a_download(tmp_path):
    mgr, engine, gate, fetching = _make_gated(tmp_path, "org/c")

    async def run():
        await mgr.startup()
        download = mgr.request_download("org/c", "main", "alice")
        await _in_fetch_phase(fetching)
        engine.alive_flag = False
        reload = mgr.check_engine()
        for _ in range(500):
            if mgr.state == "ready":
                break
            await asyncio.sleep(0.01)
        # The prune after the load still waits for the cache lock. It is harmless.
        assert mgr.state == "ready" and mgr.active == A and mgr.serving == A
        assert [name for _, name in engine.started] == ["org/a", "org/a"]
        assert mgr.download["state"] == "running"
        gate.set()
        await download
        await reload
        assert mgr.download["state"] == "done"

    try:
        asyncio.run(run())
    finally:
        gate.set()


def test_reload_uses_a_complete_local_copy_without_ensure(tmp_path):
    async def run():
        mgr, engine, store, events = _make(tmp_path)
        await mgr.startup()

        def broken_ensure(model, revision):
            raise OSError("disk gone")

        mgr._weights.ensure = broken_ensure
        engine.alive_flag = False
        await mgr.check_engine()
        assert mgr.state == "ready" and mgr.active == A

    asyncio.run(run())


def test_rollback_after_a_failed_switch_does_not_wait_for_a_download(tmp_path):
    mgr, _, gate, fetching = _make_gated(tmp_path, "org/c")
    engine = HoldingEngine("org/b")
    mgr._engine = engine

    async def run():
        await mgr.startup()
        engine.armed = True
        engine.fail_on = {"org/b"}
        switch = mgr.request_switch("org/b")
        assert await asyncio.to_thread(engine.waiting.wait, 10)
        download = mgr.request_download("org/c", "main", "alice")
        await _in_fetch_phase(fetching)
        engine.gate.set()  # the start of org/b fails, the rollback to org/a runs
        await _wait_ready(switch)
        assert switch.done() and mgr.state == "ready" and mgr.active == A
        assert mgr.download["state"] == "running"
        gate.set()
        await download
        assert mgr.download["state"] == "done"

    try:
        asyncio.run(run())
    finally:
        gate.set()
        engine.gate.set()


def test_download_waits_for_a_running_delete_of_another_pair(tmp_path):
    async def run():
        mgr, *_ = _make(tmp_path)
        await mgr.startup()
        mgr.begin_remove("org/b", ["main", "v1"])
        for model in ("org/c", "org/b"):
            with pytest.raises(ModelInUse, match=r"^A delete runs now\. Download after it ends\.$"):
                mgr.request_download(model, "v2", "alice")
        assert mgr.download is None
        mgr.end_remove("org/b", ["main", "v1"])
        await mgr.request_download("org/c", "v2", "alice")
        assert mgr.download["state"] == "done"

    asyncio.run(run())


def test_a_reload_during_a_download_does_not_hold_its_prune_or_fail_a_later_switch(tmp_path):
    mgr, _, gate, fetching = _make_gated(tmp_path, "org/c")
    engine = HoldingEngine("org/b")
    mgr._engine = engine
    states: list[str] = []

    async def watch():
        while True:
            states.append(mgr.state)
            await asyncio.sleep(0.005)

    async def run():
        await mgr.startup()
        watcher = asyncio.create_task(watch())
        download = mgr.request_download("org/c", "main", "alice")
        await _in_fetch_phase(fetching)
        engine.alive_flag = False
        reload = mgr.check_engine()
        # The reload task ends while the download still holds the cache lock.
        await asyncio.wait({reload}, timeout=5)
        assert reload.done() and mgr.state == "ready" and mgr.active == A
        engine.armed = True
        switch = mgr.request_switch("org/b")
        gate.set()  # the download ends, the switch fetches org/b and is held in its start
        assert await asyncio.to_thread(engine.waiting.wait, 10)
        await download
        for _ in range(20):  # time for a waiting prune to end
            await asyncio.sleep(0.01)
        assert mgr.state == "loading" and mgr.target == B
        with pytest.raises(SwitchInProgress):
            mgr.request_switch("org/c")
        engine.gate.set()
        await switch
        assert mgr.state == "ready" and mgr.active == B and mgr.serving == B
        watcher.cancel()
        assert "failed" not in states

    try:
        asyncio.run(run())
    finally:
        gate.set()
        engine.gate.set()
