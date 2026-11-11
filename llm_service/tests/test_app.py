"""Tests for the gateway application."""

import asyncio
import json
import threading
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from llm_service.gateway.app import create_app, read_hf_token
from llm_service.gateway.config import Config
from llm_service.gateway.identity import AdminCheck
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.tests.fakes import FakeEngine, fake_vllm_app

ALICE = {"X-Datatailr-User": "alice"}
BOB = {"X-Datatailr-User": "bob"}
CHAT = "/v1/chat/completions"
HI = {"model": "org/a", "messages": [{"role": "user", "content": "hi"}]}


class GatedEngine(FakeEngine):
    """A fake engine whose `start` waits for `gate` when the model is in `blocked`."""

    def __init__(self, blocked: set[str]) -> None:
        super().__init__()
        self.blocked = blocked
        self.gate = threading.Event()

    async def start(self, model_dir: Path, served_name: str) -> None:
        if served_name in self.blocked:
            await asyncio.to_thread(self.gate.wait, 10)
        await super().start(model_dir, served_name)


class GatedDownload:
    """A downloader whose download of `gated` waits for `gate`.

    `fetching` is set when that download begins.
    """

    def __init__(self, gated: str) -> None:
        self.gated = gated
        self.gate = threading.Event()
        self.fetching = threading.Event()

    def __call__(self, model: str, revision: str, target: Path) -> None:
        if model == self.gated:
            self.fetching.set()
            self.gate.wait(10)
        (target / "config.json").write_text("{}")


def plain_download(model: str, revision: str, target: Path) -> None:
    (target / "config.json").write_text("{}")


def make_app(tmp_path, engine=None, store=None, http=None, download=plain_download, **config_args):
    store = store or MemoryBlobStore()
    engine = engine or FakeEngine()
    config = Config(
        owner="alice", local_dir=tmp_path / "local", default_model="org/a", **config_args
    )

    http = http or httpx.AsyncClient(
        transport=httpx.ASGITransport(app=fake_vllm_app()), base_url="http://engine"
    )
    admin = AdminCheck("alice", "admins", lambda group: ["carol"])
    return create_app(config, store, engine, admin, download, http)


def wait_for(client, predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = client.get("/admin/model", headers=ALICE).json()
        if predicate(status):
            return status
        time.sleep(0.02)
    raise AssertionError(f"timeout, last status: {status}")


def wait_ready(client):
    return wait_for(client, lambda s: s["state"] == "ready")


@pytest.fixture
def client(tmp_path):
    with TestClient(make_app(tmp_path)) as c:
        wait_ready(c)
        yield c


def audit_records(client, **params):
    return client.get("/admin/audit", headers=ALICE, params=params).json()["data"]


def test_health_needs_no_user(tmp_path):
    with TestClient(make_app(tmp_path)) as c:
        r = c.get("/health")
        assert r.status_code == 200 and r.text == "OK"
        assert c.get("/openapi.json").status_code == 200


def test_chat_completion_roundtrip_and_audit(client):
    r = client.post(CHAT, headers=BOB, json=HI)
    assert r.status_code == 200 and r.headers["x-request-id"]
    rec = client.get("/admin/audit?user=bob", headers=ALICE).json()["data"][0]
    assert rec["prompt"] == {"messages": [{"role": "user", "content": "hi"}]}
    assert rec["total_tokens"] == 5 and rec["prompt_tokens"] == 3 and rec["completion_tokens"] == 2
    assert rec["request_id"] == r.headers["x-request-id"]
    assert rec["status"] == 200 and rec["endpoint"] == CHAT and rec["model"] == "org/a"
    assert rec["stream"] is False and rec["duration_ms"] >= 0
    assert rec["ts"].endswith("Z")


def test_streaming_client_sees_no_usage_chunk_but_audit_has_tokens(client):
    r = client.post(CHAT, headers=BOB, json={**HI, "stream": True})
    assert r.status_code == 200 and r.headers["x-request-id"]
    assert "usage" not in r.text and "[DONE]" in r.text
    rec = audit_records(client, user="bob")[0]
    assert rec["stream"] is True and rec["status"] == 200
    assert rec["total_tokens"] == 5


def test_missing_model_field_uses_active_model(client):
    r = client.post(CHAT, headers=BOB, json={"messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200
    assert audit_records(client, user="bob")[0]["model"] == "org/a"


def test_wrong_model_is_404_model_not_found(client):
    r = client.post(CHAT, headers=BOB, json={**HI, "model": "org/other"})
    assert r.status_code == 404
    error = r.json()["error"]
    assert error["code"] == "model_not_found" and error["type"] == "invalid_request_error"
    assert "org/a" in error["message"]
    rec = audit_records(client, user="bob")[0]
    assert rec["status"] == 404 and rec["model"] == "org/other"


def test_no_user_is_401(client):
    r = client.post(CHAT, json=HI)
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_user"
    assert r.json()["error"]["type"] == "authentication_error"
    assert client.get("/v1/models").status_code == 401


@pytest.mark.parametrize("content", [b"hello", b"[1]", b"", b"[" * 100000])
def test_non_json_and_non_object_body_is_400_and_audited(client, content):
    r = client.post(CHAT, headers=BOB, content=content)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_json"
    rec = audit_records(client, user="bob")[0]
    assert rec["status"] == 400 and rec["request_id"] == r.headers["x-request-id"]
    assert rec["prompt"] == {} and rec["total_tokens"] is None


def test_503_with_retry_after_while_loading(tmp_path):
    engine = GatedEngine({"org/a"})
    try:
        with TestClient(make_app(tmp_path, engine)) as c:
            assert c.get("/health").status_code == 200
            r = c.post(CHAT, headers=BOB, json=HI)
            assert r.status_code == 503
            assert r.headers["retry-after"] == "30"
            assert r.json()["error"]["code"] == "model_loading"
            assert r.json()["error"]["type"] == "service_unavailable"
            models = c.get("/v1/models", headers=BOB).json()["data"]
            assert models[0]["id"] == "org/a" and models[0]["status"] == "loading"
            assert audit_records(c, user="bob")[0]["status"] == 503
            engine.gate.set()
            wait_ready(c)
            assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
    finally:
        engine.gate.set()


def test_503_no_model_when_the_load_failed(tmp_path):
    engine = FakeEngine()
    engine.fail_on = {"org/a"}
    with TestClient(make_app(tmp_path, engine)) as c:
        wait_for(c, lambda s: s["state"] == "failed")
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 503 and r.json()["error"]["code"] == "no_model"
        assert "retry-after" not in r.headers


def test_dead_engine_is_reloaded_once_and_audited_as_system(tmp_path):
    engine = FakeEngine()
    with TestClient(make_app(tmp_path, engine)) as c:
        wait_ready(c)
        assert c.post("/admin/model", headers=ALICE, json={"model": "org/b"}).status_code == 202
        wait_for(c, lambda s: s["state"] == "ready" and s["active"]["model"] == "org/b")
        engine.alive_flag = False
        r = c.post(CHAT, headers=BOB, json={**HI, "model": "org/b"})
        assert r.status_code == 503 and r.json()["error"]["code"] == "model_loading"
        assert r.headers["retry-after"] == "30"
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json={**HI, "model": "org/b"}).status_code == 200
        events = [e for e in audit_records(c, model="org/b") if e["endpoint"] == "/admin/model"]
        assert [(e["user"], e["prompt"]["event"], e["status"]) for e in events] == [
            ("system", "engine_reload", 200),
            ("alice", "model_switch", 200),
        ]


def test_failed_reload_answers_no_model_without_retry_after(tmp_path):
    engine = FakeEngine()
    with TestClient(make_app(tmp_path, engine)) as c:
        wait_ready(c)
        engine.alive_flag = False
        engine.fail_on = {"org/a"}
        c.post(CHAT, headers=BOB, json=HI)
        wait_for(c, lambda s: s["state"] == "failed")
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 503 and r.json()["error"]["code"] == "no_model"
        assert "retry-after" not in r.headers
        assert c.get("/admin/model", headers=ALICE).json()["state"] == "failed"


def test_models_list_has_one_entry_per_model(client):
    store = client.app.state.manager._store
    store.put("llm-service/models/org--b/v2/.complete", "")
    store.put("llm-service/models/org--b/main/.complete", "")
    store.put("llm-service/models/org--a/old/.complete", "")
    body = client.get("/v1/models", headers=BOB).json()
    assert body["object"] == "list"
    # Only org/a main has a usable manifest. The sum skips the unusable ones.
    assert body["data"] == [
        {
            "id": "org/a", "object": "model", "created": 0, "owned_by": "org",
            "status": "active", "revision": "main", "revisions": ["main", "old"],
            "size_bytes": 2,
        },
        {
            "id": "org/b", "object": "model", "created": 0, "owned_by": "org",
            "status": "cached", "revision": "main", "revisions": ["main", "v2"],
            "size_bytes": 0,
        },
    ]


def test_models_list_shows_the_loading_revision(tmp_path):
    engine, download = GatedEngine({"org/b"}), GatedDownload("org/b")
    try:
        with TestClient(make_app(tmp_path, engine, download=download)) as c:
            wait_ready(c)
            c.post("/admin/model", headers=ALICE, json={"model": "org/b", "revision": "v3"})
            assert download.fetching.wait(10)
            # The fetch phase: the old model still serves.
            data = c.get("/v1/models", headers=BOB).json()["data"]
            assert [(e["id"], e["status"], e["revision"]) for e in data] == [
                ("org/a", "active", "main"),
                ("org/b", "loading", "v3"),
            ]
            download.gate.set()
            wait_for(c, lambda s: s["serving"] is None)
            # vLLM restarts: no model is active, the old one is only cached.
            data = c.get("/v1/models", headers=BOB).json()["data"]
            assert [(e["id"], e["status"], e["revision"]) for e in data] == [
                ("org/b", "loading", "v3"),
                ("org/a", "cached", "main"),
            ]
            engine.gate.set()
            wait_ready(c)
    finally:
        download.gate.set()
        engine.gate.set()


def test_old_model_serves_during_the_fetch_phase_of_a_switch(tmp_path):
    download = GatedDownload("org/b")
    try:
        with TestClient(make_app(tmp_path, download=download)) as c:
            wait_ready(c)
            assert c.post("/admin/model", headers=ALICE, json={"model": "org/b"}).status_code == 202
            assert download.fetching.wait(10)
            status = c.get("/admin/model", headers=ALICE).json()
            assert status["state"] == "loading"
            assert status["serving"] == {"model": "org/a", "revision": "main"}
            r = c.post(CHAT, headers=BOB, json=HI)
            assert r.status_code == 200
            rec = audit_records(c, user="bob")[0]
            assert rec["model"] == "org/a" and rec["status"] == 200
            r = c.post(CHAT, headers=BOB, json={"messages": HI["messages"]})
            assert r.status_code == 200
            assert audit_records(c, user="bob")[0]["model"] == "org/a"
            r = c.post(CHAT, headers=BOB, json={**HI, "model": "org/b"})
            assert r.status_code == 404 and r.json()["error"]["code"] == "model_not_found"
            download.gate.set()
            status = wait_ready(c)
            assert status["serving"] == {"model": "org/b", "revision": "main"}
            assert c.post(CHAT, headers=BOB, json={**HI, "model": "org/b"}).status_code == 200
            assert c.post(CHAT, headers=BOB, json=HI).status_code == 404
    finally:
        download.gate.set()


def test_engine_exit_during_the_fetch_phase_answers_model_loading(tmp_path):
    engine, download = FakeEngine(), GatedDownload("org/b")
    try:
        with TestClient(make_app(tmp_path, engine, download=download)) as c:
            wait_ready(c)
            c.post("/admin/model", headers=ALICE, json={"model": "org/b"})
            assert download.fetching.wait(10)
            engine.alive_flag = False
            r = c.post(CHAT, headers=BOB, json=HI)
            assert r.status_code == 503 and r.json()["error"]["code"] == "model_loading"
            assert r.headers["retry-after"] == "30"
            download.gate.set()
            status = wait_ready(c)
            assert status["active"]["model"] == "org/b"
            assert c.post(CHAT, headers=BOB, json={**HI, "model": "org/b"}).status_code == 200
            assert len(engine.started) == 2  # no automatic reload
    finally:
        download.gate.set()


def test_retrieve_one_model(client):
    r = client.get("/v1/models/org/a", headers=BOB)
    assert r.status_code == 200
    assert r.json() == client.get("/v1/models", headers=BOB).json()["data"][0]
    assert client.get("/v1/models/org%2Fa", headers=BOB).json()["id"] == "org/a"
    r = client.get("/v1/models/org/zzz", headers=BOB)
    assert r.status_code == 404
    error = r.json()["error"]
    assert error["code"] == "model_not_found" and error["type"] == "invalid_request_error"
    assert client.get("/v1/models/org/a").status_code == 401


ADMIN_ROUTES = [
    ("POST", "/admin/model", {"json": {"model": "org/b"}}),
    ("GET", "/admin/model", {}),
    ("GET", "/admin/audit", {}),
    ("GET", "/admin/audit/users", {}),
    ("GET", "/admin/usage", {}),
    ("GET", "/admin/usage/bob", {}),
]


@pytest.mark.parametrize("method,path,kwargs", ADMIN_ROUTES)
def test_admin_endpoints_reject_non_admin(client, method, path, kwargs):
    r = client.request(method, path, headers=BOB, **kwargs)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "admin_required"
    assert r.json()["error"]["type"] == "permission_error"
    assert client.request(method, path, **kwargs).status_code == 401


def test_switch_model_202_then_ready_and_event_in_audit(client):
    r = client.post("/admin/model", headers=ALICE, json={"model": "org/b", "revision": "v2"})
    assert r.status_code == 202 and r.json()["state"] == "loading"
    assert r.json()["target"] == {"model": "org/b", "revision": "v2"}
    status = wait_ready(client)
    assert status["active"] == {"model": "org/b", "revision": "v2"}
    events = [r for r in audit_records(client, model="org/b") if r["endpoint"] == "/admin/model"]
    assert len(events) == 1
    event = events[0]
    assert event["user"] == "alice" and event["status"] == 200
    assert event["prompt"] == {"event": "model_switch", "revision": "v2", "error": None}
    assert event["total_tokens"] is None
    assert client.post(CHAT, headers=BOB, json={**HI, "model": "org/b"}).status_code == 200


def test_startup_load_is_audited_as_system(client):
    events = [r for r in audit_records(client) if r["endpoint"] == "/admin/model"]
    assert [(e["user"], e["model"], e["status"]) for e in events] == [("system", "org/a", 200)]


def test_failed_switch_is_audited_with_status_500(tmp_path):
    engine = FakeEngine()
    engine.fail_on = {"org/b"}
    with TestClient(make_app(tmp_path, engine)) as c:
        wait_ready(c)
        assert c.post("/admin/model", headers=ALICE, json={"model": "org/b"}).status_code == 202
        status = wait_for(c, lambda s: s["error"] is not None)
        events = [r for r in audit_records(c, model="org/b") if r["endpoint"] == "/admin/model"]
        assert events[0]["status"] == 500 and events[0]["prompt"]["error"] == status["error"]


def test_switch_invalid_name_400_and_conflict_409(tmp_path):
    engine = GatedEngine({"org/b"})
    try:
        with TestClient(make_app(tmp_path, engine)) as c:
            wait_ready(c)
            for body in ({"model": "nonsense"}, {"model": "org/b", "revision": "a/b"},
                         {"model": 5}, {}, [1]):
                r = c.post("/admin/model", headers=ALICE, json=body)
                assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_model"
            r = c.post("/admin/model", headers=ALICE, content=b"nope")
            assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_model"
            assert c.post("/admin/model", headers=ALICE, json={"model": "org/b"}).status_code == 202
            r = c.post("/admin/model", headers=ALICE, json={"model": "org/c"})
            assert r.status_code == 409 and r.json()["error"]["code"] == "switch_in_progress"
            engine.gate.set()
            wait_ready(c)
    finally:
        engine.gate.set()


def test_audit_filters_and_invalid_time_400(client):
    client.post(CHAT, headers=BOB, json=HI)
    client.post(CHAT, headers=ALICE, json=HI)
    client.post(CHAT, headers=BOB, json={**HI, "model": "org/other"})
    body = client.get("/admin/audit", headers=ALICE).json()
    assert body["limit"] == 100 and body["offset"] == 0
    assert [r["user"] for r in audit_records(client, user="bob")] == ["bob", "bob"]
    assert [r["status"] for r in audit_records(client, user="bob")] == [404, 200]
    assert len(audit_records(client, user="bob", model="org/a")) == 1
    assert len(audit_records(client, user="bob", limit=1)) == 1
    assert audit_records(client, user="bob", limit=1, offset=1)[0]["status"] == 200
    assert audit_records(client, since="2999-01-01") == []
    assert audit_records(client, until="2000-01-01") == []
    assert len(audit_records(client, since="2000-01-01", until="2999-01-01", user="bob")) == 2
    for params in ({"since": "garbage"}, {"until": "13/45"}):
        r = client.get("/admin/audit", headers=ALICE, params=params)
        assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_time"
    assert client.get("/admin/audit?limit=1001", headers=ALICE).status_code == 400
    assert client.get("/admin/audit?limit=0", headers=ALICE).status_code == 400
    assert client.get("/admin/audit?limit=1000", headers=ALICE).status_code == 200


def test_audit_users(client):
    client.post(CHAT, headers=BOB, json=HI)
    rows = {r["user"]: r for r in client.get("/admin/audit/users", headers=ALICE).json()["data"]}
    assert rows["bob"]["requests"] == 1
    assert rows["bob"]["first_request"] <= rows["bob"]["last_request"]
    assert "system" in rows


def test_usage_by_user_and_user_detail(client):
    client.post(CHAT, headers=BOB, json=HI)
    rows = client.get("/admin/usage", headers=ALICE).json()["data"]
    assert rows[0]["user"] == "bob" and rows[0]["total_tokens"] == 5 and rows[0]["requests"] == 1
    assert len(rows) == 1
    by_model = client.get("/admin/usage?group_by=model", headers=ALICE).json()["data"]
    assert by_model[0]["model"] == "org/a"
    assert client.get("/admin/usage?user=alice", headers=ALICE).json()["data"] == []
    detail = client.get("/admin/usage/bob", headers=ALICE).json()
    assert detail["user"] == "bob" and detail["totals"]["total_tokens"] == 5
    assert [r["model"] for r in detail["by_model"]] == ["org/a"]
    assert len(detail["by_day"]) == 1


def test_usage_rejects_bad_group_by_and_bad_days(client):
    r = client.get("/admin/usage?group_by=colour", headers=ALICE)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_group_by"
    r = client.get("/admin/usage?since=yesterday", headers=ALICE)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_time"


def test_usage_me_returns_only_the_caller(client):
    client.post(CHAT, headers=BOB, json=HI)
    client.post(CHAT, headers=ALICE, json=HI)
    client.post(CHAT, headers=ALICE, json=HI)
    bob = client.get("/v1/usage/me", headers=BOB).json()
    assert bob["user"] == "bob" and bob["totals"]["requests"] == 1
    alice = client.get("/v1/usage/me", headers=ALICE).json()
    assert alice["user"] == "alice" and alice["totals"]["requests"] == 2


def test_audit_failure_does_not_fail_the_request(client, caplog):
    def broken(record):
        raise RuntimeError("listener broke")

    client.app.state.audit.listeners.append(broken)
    with caplog.at_level("WARNING", logger="llm_service.app"):
        r = client.post(CHAT, headers=BOB, json=HI)
    assert r.status_code == 200
    assert any("could not write an audit record" in m for m in caplog.messages)


def test_shutdown_flushes_the_audit_log(tmp_path):
    store = MemoryBlobStore()
    with TestClient(make_app(tmp_path, store=store)) as c:
        wait_ready(c)
        c.post(CHAT, headers=BOB, json=HI)
    files = store.ls("llm-service/audit")
    text = "".join(store.get(f).decode() for f in files)
    assert any(json.loads(line)["user"] == "bob" for line in text.splitlines())


def test_openai_sdk_shape_of_errors(tmp_path):
    engine = GatedEngine({"org/b"})
    try:
        with TestClient(make_app(tmp_path, engine)) as c:
            wait_ready(c)
            c.post("/admin/model", headers=ALICE, json={"model": "org/b"})
            wait_for(c, lambda s: s["serving"] is None)  # vLLM restarts
            responses = [
                c.post(CHAT, json=HI),                                          # 401
                c.get("/admin/model", headers=BOB),                             # 403
                c.post(CHAT, headers=BOB, content=b"x"),                        # 400
                c.get("/admin/audit?limit=0", headers=ALICE),                   # 400 validation
                c.get("/admin/audit?since=x", headers=ALICE),                   # 400
                c.post("/admin/model", headers=ALICE, json={"model": "org/c"}), # 409
                c.post(CHAT, headers=BOB, json=HI),                             # 503 loading
                c.get("/no/such/route", headers=BOB),                           # 404
                c.put(CHAT, headers=BOB),                                       # 405
            ]
            assert [r.status_code for r in responses] == [401, 403, 400, 400, 400, 409, 503, 404, 405]
            for r in responses:
                error = r.json()["error"]
                assert set(error) == {"message", "type", "code"}
                assert all(isinstance(v, str) and v for v in error.values())
            engine.gate.set()
            wait_ready(c)
            r = c.post(CHAT, headers=BOB, json={**HI, "model": "org/zzz"})
            assert set(r.json()["error"]) == {"message", "type", "code"}
    finally:
        engine.gate.set()


class OutageStore(MemoryBlobStore):
    """A store whose audit and usage writes fail. It records the writing threads."""

    def __init__(self) -> None:
        super().__init__()
        self.threads: list[int] = []

    def put(self, path: str, data) -> None:
        if "/audit/" in path or "/usage/" in path:
            self.threads.append(threading.get_ident())
            time.sleep(0.01)
            raise OSError("storage is down")
        super().put(path, data)


class LoopThreadEngine(FakeEngine):
    """A fake engine that records the thread of the event loop."""

    loop_thread = None

    async def start(self, model_dir: Path, served_name: str) -> None:
        self.loop_thread = threading.get_ident()
        await super().start(model_dir, served_name)


def test_audit_storage_is_never_written_on_the_event_loop_during_requests(tmp_path):
    store, engine = OutageStore(), LoopThreadEngine()
    app = make_app(tmp_path, engine, store, flush_size=10, flush_interval=0.05)
    with TestClient(app) as c:
        wait_ready(c)
        for _ in range(150):
            assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        threads = list(store.threads)
        assert len(audit_records(c, user="bob", limit=1000)) == 150  # kept in the buffer
    assert engine.loop_thread is not None
    assert engine.loop_thread not in threads


@pytest.mark.parametrize(
    "path,body,prompt",
    [
        ("/v1/completions", {"model": "org/a", "prompt": "Say hi"}, {"prompt": "Say hi"}),
        ("/v1/embeddings", {"model": "org/a", "input": ["a", "b"]}, {"input": ["a", "b"]}),
    ],
)
def test_completions_and_embeddings_are_forwarded_and_audited(client, path, body, prompt):
    r = client.post(path, headers=BOB, json=body)
    assert r.status_code == 200 and r.json()["usage"]["total_tokens"] == 5
    rec = audit_records(client, user="bob")[0]
    assert rec["endpoint"] == path and rec["prompt"] == prompt
    assert rec["status"] == 200 and rec["total_tokens"] == 5
    assert rec["request_id"] == r.headers["x-request-id"]


def test_completions_stream(client):
    r = client.post("/v1/completions", headers=BOB, json={"prompt": "hi", "stream": True})
    assert r.status_code == 200 and "usage" not in r.text and "[DONE]" in r.text
    assert audit_records(client, user="bob")[0]["total_tokens"] == 5


def test_unreachable_engine_is_502_and_audited(tmp_path):
    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(refuse))
    with TestClient(make_app(tmp_path, http=http)) as c:
        wait_ready(c)
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 502
        assert r.json()["error"]["code"] == "engine_unreachable"
        assert r.headers["x-request-id"]
        rec = audit_records(c, user="bob")[0]
        assert rec["status"] == 502 and rec["total_tokens"] is None


def test_read_hf_token():
    assert read_hf_token(None, "dev", lambda key: "never") is None
    assert read_hf_token("hf_token", "dev", lambda key: f"value of {key}") == "value of hf_token"


def test_read_hf_token_failure_logs_the_key_and_environment(caplog):
    def broken(key):
        raise RuntimeError("secret not found")

    with caplog.at_level("ERROR", logger="llm_service.app"):
        assert read_hf_token("hf_token", "pre", broken) is None
    message = " ".join(caplog.messages)
    assert "hf_token" in message and "pre" in message and "secret not found" in message


ACTING = "X-LLM-Acting-User"


def test_acting_user_must_be_admin_too(client):
    r = client.get("/admin/model", headers=ALICE | {ACTING: "bob"})
    assert r.status_code == 403 and r.json()["error"]["code"] == "admin_required"
    assert "acting user" in r.json()["error"]["message"]


def test_non_admin_caller_with_acting_user_is_403(client):
    r = client.get("/admin/model", headers=BOB | {ACTING: "alice"})
    assert r.status_code == 403
    assert "acting user" not in r.json()["error"]["message"]


def test_non_admin_caller_with_bad_acting_user_is_403(client):
    r = client.get("/admin/model", headers=BOB | {ACTING: "x" * 257})
    assert r.status_code == 403


def test_admin_acting_user_is_accepted(client):
    r = client.get("/admin/model", headers=ALICE | {ACTING: "carol"})
    assert r.status_code == 200


def test_switch_is_audited_under_the_acting_user(client):
    client.post("/admin/model", json={"model": "org/other"}, headers=ALICE | {ACTING: "carol"})
    wait_ready(client)
    events = [r for r in audit_records(client) if r["prompt"].get("event") == "model_switch"]
    assert events[0]["user"] == "carol"


@pytest.mark.parametrize("name", ["x" * 257, "a\tb"])
def test_bad_acting_user_is_400(client, name):
    r = client.get("/admin/model", headers=ALICE | {ACTING: name})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_acting_user"


@pytest.mark.parametrize("headers", [{ACTING: "  "}, {}])
def test_empty_acting_user_means_the_caller(client, headers):
    client.post("/admin/model", json={"model": "org/other"}, headers=ALICE | headers)
    wait_ready(client)
    events = [r for r in audit_records(client) if r["prompt"].get("event") == "model_switch"]
    assert events[0]["user"] == "alice"


def test_openai_endpoints_ignore_the_acting_user(client):
    r = client.post(CHAT, headers=BOB | {ACTING: "alice"}, json=HI)
    assert r.status_code == 200
    users = [rec["user"] for rec in audit_records(client) if rec["endpoint"] == CHAT]
    assert users == ["bob"]


def switch_and_wait(client, model, revision="main"):
    r = client.post("/admin/model", headers=ALICE, json={"model": model, "revision": revision})
    assert r.status_code == 202
    wait_for(client, lambda s: s["state"] == "ready" and s["active"]["model"] == model)


def model_ids(client):
    return [m["id"] for m in client.get("/v1/models", headers=BOB).json()["data"]]


def delete_events(client):
    return [r for r in audit_records(client) if r["prompt"].get("event") == "model_delete"]


@pytest.fixture
def two_models(client):
    switch_and_wait(client, "org/other")
    switch_and_wait(client, "org/a")
    return client


def test_delete_cached_model_removes_it_and_is_audited(two_models):
    client = two_models
    r = client.delete("/admin/models/org/other", headers=ALICE | {ACTING: "carol"})
    assert r.status_code == 200 and r.json() == {"deleted": [{"model": "org/other", "revision": "main"}]}
    assert "org/other" not in model_ids(client)
    rec = delete_events(client)[0]
    assert rec["user"] == "carol" and rec["endpoint"] == "/admin/models" and rec["model"] == "org/other"
    assert rec["prompt"]["revisions"] == ["main"] and rec["status"] == 200
    assert rec["total_tokens"] is None


def test_delete_served_model_is_409(client):
    r = client.delete("/admin/models/org/a", headers=ALICE)
    assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
    assert "org/a" in model_ids(client)
    assert delete_events(client) == []


def test_delete_target_of_a_running_switch_is_409(tmp_path):
    engine = GatedEngine({"org/b"})
    engine.gate.set()
    try:
        with TestClient(make_app(tmp_path, engine)) as c:
            wait_ready(c)
            switch_and_wait(c, "org/b")
            switch_and_wait(c, "org/a")
            engine.gate.clear()
            c.post("/admin/model", headers=ALICE, json={"model": "org/b"})
            wait_for(c, lambda s: s["serving"] is None)
            r = c.delete("/admin/models/org/b", headers=ALICE)
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            engine.gate.set()
            wait_ready(c)
    finally:
        engine.gate.set()


class SlowDeleteStore(MemoryBlobStore):
    """A memory store whose `delete` waits for `gate` once `armed` is set."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.deleting = threading.Event()
        self.gate = threading.Event()

    def delete(self, path: str) -> None:
        if self.armed:
            self.deleting.set()
            self.gate.wait(10)
        super().delete(path)


def test_switch_to_a_model_under_removal_is_409(tmp_path):
    store = SlowDeleteStore()
    try:
        with TestClient(make_app(tmp_path, store=store)) as c:
            wait_ready(c)
            switch_and_wait(c, "org/other")
            switch_and_wait(c, "org/a")
            store.armed = True
            result = {}
            worker = threading.Thread(
                target=lambda: result.update(r=c.delete("/admin/models/org/other", headers=ALICE))
            )
            worker.start()
            assert store.deleting.wait(10)
            r = c.post("/admin/model", headers=ALICE, json={"model": "org/other"})
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            assert c.get("/admin/model", headers=ALICE).json()["state"] == "ready"
            r = c.delete("/admin/models/org/other", headers=ALICE)
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            store.gate.set()
            worker.join(10)
            assert result["r"].status_code == 200
            # The mark is gone: the switch works again, after the copy was refetched.
            switch_and_wait(c, "org/other")
    finally:
        store.gate.set()


def test_delete_unknown_model_or_revision_is_404(client):
    for path in ("/admin/models/org/none", "/admin/models/org/a?revision=nope"):
        r = client.delete(path, headers=ALICE)
        assert r.status_code == 404 and r.json()["error"]["code"] == "model_not_found"
    assert delete_events(client) == []


@pytest.mark.parametrize(
    "path", ["/admin/models/bad--name", "/admin/models/nonsense", "/admin/models/org/a?revision=a/b"]
)
def test_delete_invalid_name_is_400(client, path):
    r = client.delete(path, headers=ALICE)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_model"


def test_delete_one_revision_keeps_the_others(two_models):
    client = two_models
    switch_and_wait(client, "org/other", "v2")
    switch_and_wait(client, "org/a")
    r = client.delete("/admin/models/org/other?revision=v2", headers=ALICE)
    assert r.status_code == 200 and r.json() == {"deleted": [{"model": "org/other", "revision": "v2"}]}
    entry = [m for m in client.get("/v1/models", headers=BOB).json()["data"] if m["id"] == "org/other"][0]
    assert entry["revisions"] == ["main"]
    assert delete_events(client)[0]["prompt"]["revisions"] == ["v2"]


def test_delete_all_revisions_without_the_parameter(two_models):
    client = two_models
    switch_and_wait(client, "org/other", "v2")
    switch_and_wait(client, "org/a")
    r = client.delete("/admin/models/org/other", headers=ALICE)
    assert r.json()["deleted"] == [
        {"model": "org/other", "revision": "main"},
        {"model": "org/other", "revision": "v2"},
    ]
    assert "org/other" not in model_ids(client)


def test_delete_needs_admin(two_models):
    r = two_models.delete("/admin/models/org/other", headers=BOB)
    assert r.status_code == 403 and r.json()["error"]["code"] == "admin_required"
    assert "org/other" in model_ids(two_models)


class FailingDeleteStore(MemoryBlobStore):
    """A memory store whose `delete` fails for the paths that contain `broken`."""

    def __init__(self) -> None:
        super().__init__()
        self.broken: str | None = None

    def delete(self, path: str) -> None:
        if self.broken and self.broken in path:
            raise OSError("simulated storage failure")
        super().delete(path)


def test_delete_storage_error_is_500_audits_the_removed_revisions_and_frees_the_mark(tmp_path):
    store = FailingDeleteStore()
    with TestClient(make_app(tmp_path, store=store), raise_server_exceptions=False) as c:
        wait_ready(c)
        switch_and_wait(c, "org/other")
        switch_and_wait(c, "org/other", "v2")
        switch_and_wait(c, "org/a")
        store.broken = "org--other/v2/"
        r = c.delete("/admin/models/org/other", headers=ALICE)
        assert r.status_code == 500
        rec = delete_events(c)[0]
        assert rec["status"] == 500 and rec["prompt"]["revisions"] == ["main"]
        assert c.app.state.manager.removing == set()
        store.broken = None
        switch_and_wait(c, "org/other", "v2")


def test_models_list_has_size_bytes(two_models):
    data = two_models.get("/v1/models", headers=BOB).json()["data"]
    assert [m["id"] for m in data] == ["org/a", "org/other"]
    assert all(isinstance(m["size_bytes"], int) and m["size_bytes"] > 0 for m in data)
    one = two_models.get("/v1/models/org/other", headers=BOB).json()
    assert one["size_bytes"] == data[1]["size_bytes"]
