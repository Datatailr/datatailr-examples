import asyncio
import json
import subprocess

import httpx
import pytest

from llm_service.admin_app import registry
from llm_service.admin_app.registry import BlobManualStore, Registry, platform_listing, service_url, valid_name


def descriptor(**changes):
    body = {
        "api": "datatailr-llm-service",
        "api_version": 1,
        "name": "svc",
        "environment": "dev",
        "state": "ready",
        "model": "org/model",
        "user": "alice",
        "admin": True,
        "caller_admin": True,
    }
    body.update(changes)
    return body


class FakeServices:
    """Answers the probe of each service name from a table."""

    def __init__(self):
        self.answers: dict[str, object] = {}
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.answers.get(request.url.host)
        if isinstance(answer, Exception):
            raise answer
        if callable(answer):
            return await answer(request)
        if answer is None:
            return httpx.Response(404, json={})
        status, body = answer
        if isinstance(body, str):
            return httpx.Response(status, text=body, headers={"content-type": "text/html"})
        return httpx.Response(status, json=body)

    def hosts(self):
        return [request.url.host for request in self.requests]


class FakeStore:
    def __init__(self, names=None):
        self.names = list(names or [])
        self.saved: list[list[str]] = []

    def load(self):
        return list(self.names)

    def save(self, names):
        self.names = list(names)
        self.saved.append(list(names))


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def services():
    return FakeServices()


@pytest.fixture
def clock():
    return Clock()


def make(services, clock, configured=(), manual=(), found=(), **options):
    store = FakeStore(manual)
    listing_names = list(found)
    http = httpx.AsyncClient(transport=httpx.MockTransport(services))
    reg = Registry(http, list(configured), store, lambda: list(listing_names), clock=clock, **options)
    reg.store = store
    return reg


def refresh(reg):
    asyncio.run(reg.refresh())


def states(reg):
    return {entry.name: (entry.source, entry.state) for entry in reg.entries()}


def probe_one(services, clock, answer):
    services.answers["svc"] = answer
    reg = make(services, clock, configured=["svc"])
    refresh(reg)
    return reg.entries()[0]


def test_name_check():
    assert valid_name("llm-service-2")
    assert valid_name("a" * 63)
    for bad in ["evil.example.com", "a/b", "..", "", "Upper", "a" * 64, None, 5, "a b", "a\n"]:
        assert not valid_name(bad)


def test_service_url():
    assert service_url("llm-service") == "http://llm-service"


@pytest.mark.parametrize("bad", ["evil.example.com", "a/b", "", "UP", "a" * 64, None])
def test_service_url_refuses_a_bad_name(bad):
    with pytest.raises(ValueError):
        service_url(bad)


def test_probe_sends_plain_get_with_connection_close(services, clock):
    probe_one(services, clock, (200, descriptor()))
    request = services.requests[0]
    assert request.method == "GET"
    assert str(request.url) == "http://svc/v1/service"
    assert request.headers["connection"] == "close"
    assert "x-llm-acting-user" not in request.headers


def test_probe_ok(services, clock):
    entry = probe_one(services, clock, (200, descriptor(state="loading", model="org/m")))
    assert (entry.state, entry.detail) == ("ok", None)
    assert (entry.engine, entry.model, entry.checked_at) == ("loading", "org/m", 1000.0)
    assert entry.to_dict() == {
        "name": "svc",
        "source": "configured",
        "state": "ok",
        "detail": None,
        "model": "org/m",
        "engine": "loading",
        "checked_at": 1000.0,
    }


def test_probe_caller_not_admin(services, clock):
    entry = probe_one(services, clock, (200, descriptor(caller_admin=False)))
    assert entry.state == "no_access"
    assert "not an admin" in entry.detail


@pytest.mark.parametrize("status", [401, 403])
def test_probe_refused_is_no_access(services, clock, status):
    entry = probe_one(services, clock, (status, {"error": {}}))
    assert entry.state == "no_access"


def test_probe_404_is_not_llm(services, clock):
    assert probe_one(services, clock, (404, {})).state == "not_llm"


def test_probe_html_is_not_llm(services, clock):
    assert probe_one(services, clock, (200, "<html></html>")).state == "not_llm"


def test_probe_wrong_api_is_not_llm(services, clock):
    assert probe_one(services, clock, (200, descriptor(api="other"))).state == "not_llm"


@pytest.mark.parametrize("version", [2, "1", True, None])
def test_probe_wrong_version_is_not_llm(services, clock, version):
    assert probe_one(services, clock, (200, descriptor(api_version=version))).state == "not_llm"


def test_probe_json_that_is_not_an_object_is_not_llm(services, clock):
    assert probe_one(services, clock, (200, [1, 2])).state == "not_llm"


def test_probe_transport_error_is_unreachable(services, clock):
    entry = probe_one(services, clock, httpx.ConnectError("refused"))
    assert entry.state == "unreachable"
    assert entry.detail


def test_get_returns_only_ok_entries(services, clock):
    services.answers["good"] = (200, descriptor())
    services.answers["closed"] = (403, {})
    reg = make(services, clock, configured=["good", "closed"])
    refresh(reg)
    assert reg.get("good").state == "ok"
    assert reg.get("closed") is None
    assert reg.get("missing") is None
    assert [e.name for e in reg.entries()] == ["closed", "good"]


def test_three_sources_and_priority(services, clock):
    for name in ["conf", "man", "fnd", "all"]:
        services.answers[name] = (200, descriptor(name=name))
    reg = make(services, clock, configured=["conf", "all"], manual=["man", "all", "conf"], found=["fnd", "all", "man"])
    refresh(reg)
    assert states(reg) == {
        "all": ("configured", "ok"),
        "conf": ("configured", "ok"),
        "fnd": ("found", "ok"),
        "man": ("manual", "ok"),
    }
    assert sorted(services.hosts()) == ["all", "conf", "fnd", "man"]
    assert reg.manual_names() == ["man", "all", "conf"]


def test_invalid_names_from_sources_are_dropped(services, clock):
    services.answers["ok"] = (200, descriptor())
    reg = make(services, clock, configured=["ok", "bad.name"], manual=["A/B"], found=["x y"])
    refresh(reg)
    assert services.hosts() == ["ok"]


def test_failed_listing_keeps_other_sources(services, clock, caplog):
    services.answers["conf"] = (200, descriptor())
    reg = make(services, clock, configured=["conf"])

    def failing():
        raise RuntimeError("dt missing")

    reg._listing = failing
    with caplog.at_level("WARNING"):
        refresh(reg)
        refresh(reg)
    assert reg.listing_state == "failed"
    assert states(reg) == {"conf": ("configured", "ok")}
    assert len([r for r in caplog.records if "listing" in r.getMessage().lower()]) == 1
    reg._listing = lambda: []
    refresh(reg)
    assert reg.listing_state == "ok"


def test_listing_failure_logs_again_after_recovery(services, clock, caplog):
    reg = make(services, clock)
    calls = iter([RuntimeError("a"), [], RuntimeError("b")])

    def listing():
        value = next(calls)
        if isinstance(value, Exception):
            raise value
        return value

    reg._listing = listing
    with caplog.at_level("WARNING"):
        for _ in range(3):
            refresh(reg)
    assert len([r for r in caplog.records if "listing" in r.getMessage().lower()]) == 2


@pytest.mark.parametrize("answer", [(404, {}), httpx.ConnectError("refused")])
def test_found_failure_is_hidden_and_paused(services, clock, answer):
    services.answers["fnd"] = answer
    reg = make(services, clock, found=["fnd"])
    refresh(reg)
    assert reg.entries() == []
    clock.now += 599
    refresh(reg)
    assert services.hosts() == ["fnd"]
    clock.now += 2
    services.answers["fnd"] = (200, descriptor())
    refresh(reg)
    assert services.hosts() == ["fnd", "fnd"]
    assert states(reg) == {"fnd": ("found", "ok")}


def test_found_no_access_is_shown_and_not_paused(services, clock):
    services.answers["fnd"] = (403, {})
    reg = make(services, clock, found=["fnd"])
    refresh(reg)
    refresh(reg)
    assert states(reg) == {"fnd": ("found", "no_access")}
    assert services.hosts() == ["fnd", "fnd"]


def test_configured_and_manual_failures_always_show(services, clock):
    services.answers["conf"] = httpx.ConnectError("refused")
    services.answers["man"] = (404, {})
    reg = make(services, clock, configured=["conf"], manual=["man"])
    refresh(reg)
    refresh(reg)
    assert states(reg) == {"conf": ("configured", "unreachable"), "man": ("manual", "not_llm")}
    assert services.hosts() == ["conf", "man", "conf", "man"]


def test_name_that_leaves_the_listing_disappears(services, clock):
    services.answers["fnd"] = (200, descriptor())
    reg = make(services, clock)
    reg._listing = lambda: ["fnd"]
    refresh(reg)
    assert reg.get("fnd")
    reg._listing = lambda: []
    refresh(reg)
    assert reg.entries() == []


def test_hanging_service_does_not_stop_the_others(services, clock):
    async def hang(request):
        await asyncio.sleep(30)

    services.answers["slow"] = hang
    services.answers["fast"] = (200, descriptor())
    reg = make(services, clock, configured=["slow", "fast"], probe_timeout=0.05)

    async def go():
        await asyncio.wait_for(reg.refresh(), 5)

    asyncio.run(go())
    assert states(reg) == {"fast": ("configured", "ok"), "slow": ("configured", "unreachable")}


def test_concurrency_is_limited(services, clock):
    running = 0
    peak = 0

    async def slow(request):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.01)
        running -= 1
        return httpx.Response(200, json=descriptor())

    names = [f"s{i}" for i in range(8)]
    for name in names:
        services.answers[name] = slow
    reg = make(services, clock, configured=names, concurrency=3)
    refresh(reg)
    assert peak == 3
    assert len(reg.entries()) == 8


def test_add_manual_saves_and_probes(services, clock):
    services.answers["new"] = (200, descriptor())
    reg = make(services, clock, manual=["old"])

    entry = asyncio.run(reg.add_manual("new"))
    assert (entry.name, entry.source, entry.state) == ("new", "manual", "ok")
    assert reg.store.saved == [["old", "new"]]
    assert reg.get("new") is entry
    assert reg.manual_names() == ["old", "new"]
    assert services.hosts() == ["new"]


def test_add_manual_twice_saves_once(services, clock):
    services.answers["new"] = (200, descriptor())
    reg = make(services, clock)
    asyncio.run(reg.add_manual("new"))
    asyncio.run(reg.add_manual("new"))
    assert reg.store.saved == [["new"]]


@pytest.mark.parametrize("bad", ["evil.example.com", "a/b", "", "UP"])
def test_add_manual_bad_name_raises_and_calls_nothing(services, clock, bad):
    reg = make(services, clock)
    with pytest.raises(ValueError):
        asyncio.run(reg.add_manual(bad))
    assert services.requests == []
    assert reg.store.saved == []


def test_add_and_remove_keep_only_valid_manual_names(services, clock):
    services.answers["new"] = (200, descriptor())
    reg = make(services, clock, manual=["old", "Bad.Name", "a/b"])
    asyncio.run(reg.add_manual("new"))
    assert reg.manual_names() == ["old", "new"]
    reg.store.names = ["old", "Bad.Name", "new"]
    assert asyncio.run(reg.remove_manual("new")) is True
    assert reg.manual_names() == ["old"]
    reg.store.names = ["old", "UP"]
    assert asyncio.run(reg.remove_manual("missing")) is False
    assert reg.manual_names() == ["old"]


def test_remove_manual(services, clock):
    services.answers["man"] = (200, descriptor())
    reg = make(services, clock, manual=["man", "other"])
    refresh(reg)
    assert asyncio.run(reg.remove_manual("man")) is True
    assert reg.store.saved == [["other"]]
    assert reg.get("man") is None
    assert reg.manual_names() == ["other"]
    assert asyncio.run(reg.remove_manual("man")) is False
    assert reg.store.saved == [["other"]]


def test_wait_ready(services, clock):
    reg = make(services, clock)

    async def go():
        assert await reg.wait_ready(0.01) is False
        await reg.refresh()
        assert await reg.wait_ready(0.01) is True

    asyncio.run(go())


def test_run_loops_and_never_raises(services, clock, caplog):
    reg = make(services, clock)
    calls = 0

    async def refresh_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        if calls == 3:
            raise asyncio.CancelledError

    reg.refresh = refresh_once

    async def go():
        with pytest.raises(asyncio.CancelledError):
            await reg.run(interval=0.001)

    with caplog.at_level("ERROR"):
        asyncio.run(go())
    assert calls == 3
    assert any("boom" in r.getMessage() or r.exc_info for r in caplog.records)


def test_run_sets_ready_even_when_first_refresh_fails(services, clock):
    reg = make(services, clock)

    async def broken():
        raise RuntimeError("boom")

    reg.refresh = broken

    async def go():
        task = asyncio.create_task(reg.run(interval=10))
        assert await reg.wait_ready(1) is True
        task.cancel()

    asyncio.run(go())


SAMPLE = json.dumps(
    [
        {"name": "llm-service", "state": "ready", "environment": "dev", "type": "service"},
        {"name": "llm-big", "state": "created", "environment": "dev", "type": "service"},
        {"name": "other-env", "state": "ready", "environment": "prod", "type": "service"},
        {"name": "Bad.Name", "state": "ready", "environment": "dev", "type": "service"},
        {"state": "ready", "environment": "dev"},
    ]
)


def completed(stdout="", returncode=0, stderr=""):
    def run(command, **kwargs):
        run.command = command
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    return run


def test_platform_listing_filters():
    run = completed(SAMPLE)
    assert platform_listing("dev", run=run) == ["llm-service"]
    assert run.command == ["dt", "job", "ls", "-j", "-f", "type = service"]


def test_platform_listing_failed_command_raises():
    with pytest.raises(RuntimeError):
        platform_listing("dev", run=completed("", returncode=1, stderr="no login"))


def test_platform_listing_bad_output_raises():
    with pytest.raises(ValueError):
        platform_listing("dev", run=completed("not json"))
    with pytest.raises(ValueError):
        platform_listing("dev", run=completed('{"a": 1}'))


class FakeBlob:
    files: dict[str, bytes] = {}
    fail = False

    def get(self, path):
        if self.fail or path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    def exists(self, path):
        return path in self.files

    def put(self, path, data):
        self.files[path] = data.encode() if isinstance(data, str) else data


@pytest.fixture
def blob(monkeypatch):
    import datatailr

    FakeBlob.files = {}
    FakeBlob.fail = False
    monkeypatch.setattr(datatailr, "Blob", FakeBlob, raising=False)
    return FakeBlob


def test_blob_store_round_trip(blob):
    store = BlobManualStore()
    assert store.load() == []
    store.save(["a", "b"])
    assert json.loads(blob.files["llm-service-admin/services.json"]) == ["a", "b"]
    assert store.load() == ["a", "b"]


@pytest.mark.parametrize("content", [b"not json", b'{"a": 1}', b'["ok", 5, "bad.name"]'])
def test_blob_store_broken_file(blob, content):
    blob.files["llm-service-admin/services.json"] = content
    assert BlobManualStore().load() == (["ok"] if b"ok" in content else [])


def test_blob_store_read_error_propagates(blob):
    blob.files["llm-service-admin/services.json"] = b'["a"]'
    blob.fail = True
    with pytest.raises(FileNotFoundError):
        BlobManualStore().load()


class FailingLoadStore(FakeStore):
    def load(self):
        raise OSError("blob down")


def test_failed_load_blocks_add_and_remove_without_save(services, clock):
    services.answers["new"] = (200, descriptor())
    reg = make(services, clock)
    reg._manual_store = store = FailingLoadStore(["keep"])
    with pytest.raises(OSError):
        asyncio.run(reg.add_manual("new"))
    with pytest.raises(OSError):
        asyncio.run(reg.remove_manual("keep"))
    assert store.saved == []
    assert services.requests == []


def test_failed_load_in_refresh_keeps_last_manual_list(services, clock):
    services.answers["man"] = (200, descriptor())
    reg = make(services, clock, manual=["man"])
    refresh(reg)
    reg._manual_store = FailingLoadStore()
    refresh(reg)
    assert states(reg) == {"man": ("manual", "ok")}


def test_paused_found_name_shows_when_it_becomes_manual(services, clock):
    services.answers["svc"] = (404, {})
    reg = make(services, clock, found=["svc"])
    refresh(reg)
    assert reg.entries() == []
    reg.store.names = ["svc"]
    refresh(reg)
    assert states(reg) == {"svc": ("manual", "not_llm")}


def test_add_manual_during_refresh_is_not_lost(services, clock):
    gate = asyncio.Event()

    async def slow(request):
        await gate.wait()
        return httpx.Response(200, json=descriptor())

    services.answers["slow"] = slow
    services.answers["new"] = (200, descriptor())
    services.answers["new2"] = (200, descriptor())
    reg = make(services, clock, configured=["slow"])

    async def go():
        refreshing = asyncio.create_task(reg.refresh())
        await asyncio.sleep(0.01)
        adds = [asyncio.create_task(reg.add_manual("new")), asyncio.create_task(reg.add_manual("new2"))]
        await asyncio.sleep(0.01)
        gate.set()
        await asyncio.gather(refreshing, *adds)

    asyncio.run(go())
    assert set(states(reg)) == {"slow", "new", "new2"}
    assert reg.store.names == ["new", "new2"]


def test_unexpected_probe_error_is_unreachable_and_others_continue(services, clock):
    services.answers["bad"] = ValueError("odd")
    services.answers["good"] = (200, descriptor())
    reg = make(services, clock, configured=["bad", "good"])
    refresh(reg)
    assert states(reg) == {"bad": ("configured", "unreachable"), "good": ("configured", "ok")}


def test_module_does_not_import_datatailr_at_import():
    assert "datatailr" not in vars(registry)


def test_configured_and_manual_names_show_before_a_slow_listing(services, clock):
    import threading

    for name in ("conf", "man", "fnd"):
        services.answers[name] = (200, descriptor(name=name))
    release = threading.Event()
    reg = make(services, clock, configured=["conf"], manual=["man"])

    def slow_listing():
        release.wait(10)
        return ["fnd"]

    reg._listing = slow_listing

    async def go():
        task = asyncio.create_task(reg.refresh())
        try:
            ready = await reg.wait_ready(5)
            early = states(reg)
        finally:
            release.set()
        await task
        return ready, early

    ready, early = asyncio.run(go())
    assert ready is True
    assert early == {"conf": ("configured", "ok"), "man": ("manual", "ok")}
    assert states(reg) == {"conf": ("configured", "ok"), "fnd": ("found", "ok"), "man": ("manual", "ok")}


def test_found_entries_stay_while_the_next_listing_runs(services, clock):
    import threading

    for name in ("conf", "fnd"):
        services.answers[name] = (200, descriptor(name=name))
    reg = make(services, clock, configured=["conf"], found=["fnd"])
    refresh(reg)
    release = threading.Event()
    seen = []

    def slow_listing():
        release.wait(10)
        return ["fnd"]

    reg._listing = slow_listing

    async def go():
        task = asyncio.create_task(reg.refresh())
        try:
            while len(services.requests) < 3:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.01)
            seen.append(states(reg))
        finally:
            release.set()
        await task

    asyncio.run(go())
    assert seen == [{"conf": ("configured", "ok"), "fnd": ("found", "ok")}]
