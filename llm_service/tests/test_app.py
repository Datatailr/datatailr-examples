"""Tests for the gateway application."""

import asyncio
import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from llm_service.gateway.app import create_app, read_hf_token
from llm_service.gateway.config import Config
from llm_service.gateway.identity import AccessCheck, AdminCheck
from llm_service.gateway.storage import MemoryBlobStore
from llm_service.tests.fakes import FakeEngine, fake_vllm_app

ALICE = {"X-Datatailr-User": "alice"}
BOB = {"X-Datatailr-User": "bob"}
ACTING = "X-LLM-Acting-User"
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


def make_app(
    tmp_path,
    engine=None,
    store=None,
    http=None,
    download=plain_download,
    now=None,
    maintenance_interval=None,
    with_access=True,
    **config_args,
):
    store = store or MemoryBlobStore()
    engine = engine or FakeEngine()
    config = Config(
        owner="alice", local_dir=tmp_path / "local", default_model="org/a", **config_args
    )

    http = http or httpx.AsyncClient(
        transport=httpx.ASGITransport(app=fake_vllm_app()), base_url="http://engine"
    )
    admin = AdminCheck("alice", "admins", lambda group: ["carol"])
    access = AccessCheck(admin, "users", lambda group: ["bob"])
    extra = {"access": access} if with_access else {}
    if now is not None:
        extra["now"] = now
    if maintenance_interval is not None:
        extra["maintenance_interval"] = maintenance_interval
    return create_app(config, store, engine, admin, download, http, **extra)


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


def full_record(client, row):
    """Return the full record of a list row, with its prompt. The list has no prompts."""
    return client.app.state.audit.record(row["ts"][:10], row["request_id"])


def test_health_needs_no_user(tmp_path):
    with TestClient(make_app(tmp_path)) as c:
        r = c.get("/health")
        assert r.status_code == 200 and r.text == "OK"
        assert c.get("/openapi.json").status_code == 200


def test_chat_completion_roundtrip_and_audit(client):
    r = client.post(CHAT, headers=BOB, json=HI)
    assert r.status_code == 200 and r.headers["x-request-id"]
    rec = client.get("/admin/audit?user=bob", headers=ALICE).json()["data"][0]
    assert not {"prompt", "seg", "prompt_ref"} & set(rec)
    assert rec["event"] is None and rec["detail"] is None
    assert full_record(client, rec)["prompt"] == {"messages": [{"role": "user", "content": "hi"}]}
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
    assert full_record(client, rec)["prompt"] == {} and rec["total_tokens"] is None


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
        assert [(e["user"], e["event"], e["status"]) for e in events] == [
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
    # org/b has no known size at all, so its size is null.
    assert body["data"] == [
        {
            "id": "org/a", "object": "model", "created": 0, "owned_by": "org",
            "status": "active", "revision": "main", "revisions": ["main", "old"],
            "size_bytes": 2,
        },
        {
            "id": "org/b", "object": "model", "created": 0, "owned_by": "org",
            "status": "cached", "revision": "main", "revisions": ["main", "v2"],
            "size_bytes": None,
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
    ("GET", "/admin/audit/records/abc?day=2026-11-10", {}),
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
    assert event["event"] == "model_switch" and event["detail"] == {"revision": "v2", "error": None}
    assert "prompt" not in event
    prompt = full_record(client, event)["prompt"]
    assert prompt == {"event": "model_switch", "revision": "v2", "error": None}
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
        assert events[0]["status"] == 500 and events[0]["detail"]["error"] == status["error"]


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
    r = client.get("/admin/usage?since=20261110", headers=ALICE)
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
    files = [path for path in store.ls("llm-service/audit") if path.endswith(".jsonl")]
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
    """A store whose audit, prompt and usage writes fail. It records the writing threads."""

    def __init__(self) -> None:
        super().__init__()
        self.threads: list[int] = []

    def put(self, path: str, data) -> None:
        if "/audit/" in path or "/prompts/" in path or "/usage/" in path:
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
    assert rec["endpoint"] == path and full_record(client, rec)["prompt"] == prompt
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
    assert r.json()["error"]["code"] == "admin_required"


def test_admin_acting_user_is_accepted(client):
    r = client.get("/admin/model", headers=ALICE | {ACTING: "carol"})
    assert r.status_code == 200


def test_switch_is_audited_under_the_acting_user(client):
    client.post("/admin/model", json={"model": "org/other"}, headers=ALICE | {ACTING: "carol"})
    wait_ready(client)
    events = [r for r in audit_records(client) if r["event"] == "model_switch"]
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
    events = [r for r in audit_records(client) if r["event"] == "model_switch"]
    assert events[0]["user"] == "alice"


def chat_records(client):
    return [rec for rec in audit_records(client) if rec["endpoint"] == CHAT]


def test_chat_without_header_is_the_caller_and_has_no_via(client):
    assert client.post(CHAT, headers=BOB, json=HI).status_code == 200
    (rec,) = chat_records(client)
    assert rec["user"] == "bob" and "via" not in rec


def test_chat_acts_for_a_user_with_access(client):
    r = client.post(CHAT, headers=ALICE | {ACTING: "bob"}, json=HI)
    assert r.status_code == 200
    (rec,) = chat_records(client)
    assert rec["user"] == "bob" and rec["via"] == "alice"
    assert client.app.state.audit.record(rec["ts"][:10], rec["request_id"])["via"] == "alice"
    detail = client.get(
        f"/admin/audit/records/{rec['request_id']}", headers=ALICE, params={"day": rec["ts"][:10]}
    ).json()
    assert detail["user"] == "bob" and detail["via"] == "alice"


def test_acting_admin_has_access(client):
    r = client.post(CHAT, headers=ALICE | {ACTING: "carol"}, json=HI)
    assert r.status_code == 200
    assert chat_records(client)[0]["user"] == "carol"


@pytest.mark.parametrize("method, path", [("post", CHAT), ("get", "/v1/models"), ("get", "/v1/usage/me")])
def test_non_admin_caller_with_acting_user_is_refused_on_v1(client, method, path):
    kwargs = {"json": HI} if method == "post" else {}
    r = getattr(client, method)(path, headers=BOB | {ACTING: "bob"}, **kwargs)
    assert r.status_code == 403
    error = r.json()["error"]
    assert set(error) == {"message", "type", "code"}
    assert (error["type"], error["code"]) == ("permission_error", "acting_user_not_allowed")
    assert isinstance(error["message"], str) and error["message"]


@pytest.mark.parametrize("method, path", [("post", CHAT), ("get", "/v1/models"), ("get", "/v1/usage/me")])
def test_acting_user_without_access_is_refused_on_v1(client, method, path):
    kwargs = {"json": HI} if method == "post" else {}
    r = getattr(client, method)(path, headers=ALICE | {ACTING: "dave"}, **kwargs)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "access_denied"
    assert r.json()["error"]["type"] == "permission_error"


@pytest.mark.parametrize("method, path", [("post", CHAT), ("get", "/v1/models"), ("get", "/v1/usage/me")])
@pytest.mark.parametrize("name", ["x" * 257, "a\tb"])
def test_bad_acting_user_is_400_on_v1(client, method, path, name):
    kwargs = {"json": HI} if method == "post" else {}
    r = getattr(client, method)(path, headers=ALICE | {ACTING: name}, **kwargs)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_acting_user"
    assert r.json()["error"]["type"] == "invalid_request_error"


def test_refused_acting_user_writes_no_chat_audit_record(client):
    client.post(CHAT, headers=ALICE | {ACTING: "dave"}, json=HI)
    assert chat_records(client) == []


def test_empty_acting_user_on_v1_means_the_caller(client):
    r = client.post(CHAT, headers=BOB | {ACTING: "  "}, json=HI)
    assert r.status_code == 200
    assert chat_records(client)[0]["user"] == "bob"


def test_models_accept_an_acting_user_with_access(client):
    assert client.get("/v1/models", headers=ALICE | {ACTING: "bob"}).status_code == 200
    r = client.get("/v1/models/org/a", headers=ALICE | {ACTING: "bob"})
    assert r.status_code == 200 and r.json()["id"] == "org/a"
    assert client.get("/v1/models/org/a", headers=ALICE | {ACTING: "dave"}).status_code == 403


def test_usage_me_returns_the_numbers_of_the_acting_user(client):
    client.post(CHAT, headers=BOB, json=HI)
    client.post(CHAT, headers=BOB, json=HI)
    client.post(CHAT, headers=ALICE, json=HI)
    bob = client.get("/v1/usage/me", headers=BOB).json()
    via = client.get("/v1/usage/me", headers=ALICE | {ACTING: "bob"}).json()
    assert via == bob
    assert via != client.get("/v1/usage/me", headers=ALICE).json()


def test_app_without_access_check_gives_access_to_admins_only(tmp_path):
    with TestClient(make_app(tmp_path, with_access=False)) as c:
        wait_ready(c)
        assert c.get("/v1/models", headers=ALICE | {ACTING: "carol"}).status_code == 200
        assert c.get("/v1/models", headers=ALICE | {ACTING: "bob"}).status_code == 403


def switch_and_wait(client, model, revision="main"):
    r = client.post("/admin/model", headers=ALICE, json={"model": model, "revision": revision})
    assert r.status_code == 202
    wait_for(client, lambda s: s["state"] == "ready" and s["active"]["model"] == model)


def model_ids(client):
    return [m["id"] for m in client.get("/v1/models", headers=BOB).json()["data"]]


def delete_events(client):
    return [r for r in audit_records(client) if r["event"] == "model_delete"]


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
    assert rec["detail"]["revisions"] == ["main"] and rec["status"] == 200
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
    assert delete_events(client)[0]["detail"]["revisions"] == ["v2"]


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
        error = r.json()["error"]
        assert error["code"] == "delete_failed" and error["type"] == "server_error"
        assert "OSError" in error["message"] and "org--other" not in error["message"]
        rec = delete_events(c)[0]
        assert rec["status"] == 500 and rec["detail"]["revisions"] == ["main"]
        assert c.app.state.manager.removing == set()
        store.broken = None
        switch_and_wait(c, "org/other", "v2")


def test_models_list_has_size_bytes(two_models):
    data = two_models.get("/v1/models", headers=BOB).json()["data"]
    assert [m["id"] for m in data] == ["org/a", "org/other"]
    assert all(isinstance(m["size_bytes"], int) and m["size_bytes"] > 0 for m in data)
    one = two_models.get("/v1/models/org/other", headers=BOB).json()
    assert one["size_bytes"] == data[1]["size_bytes"]


class RollbackEngine(FakeEngine):
    """A fake engine. `org/other` fails to start. `org/a` waits for `gate` once `armed`."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_on = {"org/other"}
        self.armed = False
        self.waiting = threading.Event()
        self.gate = threading.Event()

    async def start(self, model_dir: Path, served_name: str) -> None:
        if self.armed and served_name == "org/a":
            self.waiting.set()
            await asyncio.to_thread(self.gate.wait, 10)
        await super().start(model_dir, served_name)


def test_delete_the_model_of_a_running_rollback_is_409(tmp_path):
    engine = RollbackEngine()
    try:
        with TestClient(make_app(tmp_path, engine)) as c:
            wait_ready(c)
            engine.armed = True
            c.post("/admin/model", headers=ALICE, json={"model": "org/other"})
            assert engine.waiting.wait(10)
            r = c.delete("/admin/models/org/a", headers=ALICE)
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            engine.gate.set()
            wait_ready(c)
            assert "org/a" in model_ids(c)
    finally:
        engine.gate.set()


def test_switch_during_a_rollback_is_409(tmp_path):
    engine = RollbackEngine()
    try:
        with TestClient(make_app(tmp_path, engine)) as c:
            wait_ready(c)
            engine.armed = True
            c.post("/admin/model", headers=ALICE, json={"model": "org/other"})
            assert engine.waiting.wait(10)
            status = c.get("/admin/model", headers=ALICE).json()
            assert status["state"] == "failed"
            assert status["rolling_back"] == {"model": "org/a", "revision": "main"}
            r = c.post("/admin/model", headers=ALICE, json={"model": "org/c"})
            assert r.status_code == 409 and r.json()["error"]["code"] == "switch_in_progress"
            engine.gate.set()
            status = wait_ready(c)
            assert status["rolling_back"] is None
            assert [name for _, name in engine.started] == ["org/a", "org/a"]
    finally:
        engine.gate.set()


def test_a_second_delete_removes_what_a_failed_delete_left(tmp_path):
    store = FailingDeleteStore()
    with TestClient(make_app(tmp_path, store=store), raise_server_exceptions=False) as c:
        wait_ready(c)
        switch_and_wait(c, "org/other")
        switch_and_wait(c, "org/a")
        # The marker goes first, then the delete of a file fails.
        store.broken = "org--other/main/config.json"
        r = c.delete("/admin/models/org/other", headers=ALICE)
        assert r.status_code == 500 and r.json()["error"]["code"] == "delete_failed"
        assert "org/other" not in model_ids(c)
        assert store.ls("llm-service/models/org--other") != []
        store.broken = None
        r = c.delete("/admin/models/org/other", headers=ALICE)
        assert r.status_code == 200
        assert r.json() == {"deleted": [{"model": "org/other", "revision": "main"}]}
        assert store.ls("llm-service/models/org--other") == []
        assert delete_events(c)[0]["detail"]["revisions"] == ["main"]


def test_delete_removes_leftovers_without_a_marker(client):
    store = client.app.state.manager._store
    store.put("llm-service/models/org--x/main/config.json", "{}")
    store.put("llm-service/models/org--x/v2/weights.bin", "w")
    r = client.delete("/admin/models/org/x?revision=v2", headers=ALICE)
    assert r.status_code == 200 and r.json() == {"deleted": [{"model": "org/x", "revision": "v2"}]}
    r = client.delete("/admin/models/org/x", headers=ALICE)
    assert r.json() == {"deleted": [{"model": "org/x", "revision": "main"}]}
    assert store.ls("llm-service/models/org--x") == []
    assert [e["detail"]["revisions"] for e in delete_events(client)] == [["main"], ["v2"]]
    r = client.delete("/admin/models/org/x", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "model_not_found"


class HeldMarkerStore(MemoryBlobStore):
    """A store whose write of the `org--b` marker waits for `gate`. `uploading` is set then."""

    def __init__(self) -> None:
        super().__init__()
        self.uploading = threading.Event()
        self.gate = threading.Event()

    def put(self, path, data):
        if "org--b/main/.complete" in path:
            self.uploading.set()
            self.gate.wait(10)
        super().put(path, data)


def test_delete_of_a_target_in_its_fetch_phase_is_409(tmp_path):
    store = HeldMarkerStore()
    try:
        with TestClient(make_app(tmp_path, store=store)) as c:
            wait_ready(c)
            c.post("/admin/model", headers=ALICE, json={"model": "org/b"})
            assert store.uploading.wait(10)
            # The files of org/b are in blob storage. Its marker is not there yet.
            assert "org/b" not in [e["id"] for e in c.get("/v1/models", headers=BOB).json()["data"]
                                   if e["status"] == "cached"]
            r = c.delete("/admin/models/org/b", headers=ALICE)
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            store.gate.set()
            wait_for(c, lambda s: s["state"] == "ready" and s["active"]["model"] == "org/b")
    finally:
        store.gate.set()


def test_models_list_size_is_null_when_no_size_is_known(client):
    store = client.app.state.manager._store
    store.put("llm-service/models/org--b/main/.complete", "not json")
    entry = client.get("/v1/models/org/b", headers=BOB).json()
    assert entry["revisions"] == ["main"] and entry["size_bytes"] is None


# Record endpoint

def _record_url(row, day=None):
    return f"/admin/audit/records/{row['request_id']}?day={day or row['ts'][:10]}"


def _bob_row(client):
    client.post(CHAT, headers=BOB, json=HI)
    return audit_records(client, user="bob")[0]


def test_list_rows_have_no_prompt_seg_or_prompt_ref(client):
    row = _bob_row(client)
    assert not {"prompt", "seg", "prompt_ref"} & set(row)
    assert "event" in row


def test_record_endpoint_returns_a_buffered_record_with_its_prompt(client):
    row = _bob_row(client)
    r = client.get(_record_url(row), headers=ALICE)
    assert r.status_code == 200
    assert r.json()["prompt"] == {"messages": [{"role": "user", "content": "hi"}]}
    assert r.json()["request_id"] == row["request_id"]


def test_record_endpoint_returns_a_stored_record_with_its_prompt(client):
    row = _bob_row(client)
    client.app.state.audit.flush()
    r = client.get(_record_url(row), headers=ALICE)
    assert r.status_code == 200
    assert r.json()["prompt"] == {"messages": [{"role": "user", "content": "hi"}]}


def test_record_endpoint_returns_an_event(client):
    row = next(r for r in audit_records(client) if r["endpoint"] == "/admin/model")
    client.app.state.audit.flush()
    r = client.get(_record_url(row), headers=ALICE)
    assert r.status_code == 200
    assert r.json()["prompt"]["event"] == "model_switch"


def test_record_endpoint_unknown_id_is_404(client):
    r = client.get("/admin/audit/records/nope?day=2026-11-10", headers=ALICE)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "record_not_found"
    assert set(r.json()["error"]) == {"message", "type", "code"}


@pytest.mark.parametrize(
    "query",
    ["", "?day=", "?day=garbage", "?day=2026-13-40", "?day=20261110", "?day=2026-W45-2", "?day=2026-11-31"],
)
def test_record_endpoint_bad_or_missing_day_is_400(client, query):
    r = client.get("/admin/audit/records/abc" + query, headers=ALICE)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_time"


def test_record_endpoint_reports_a_deleted_prompt(tmp_path):
    store = MemoryBlobStore()
    with TestClient(make_app(tmp_path, store=store)) as c:
        wait_ready(c)
        row = _bob_row(c)
        c.app.state.audit.flush()
        for path in store.ls("llm-service/prompts"):
            store.delete(path)
        r = c.get(_record_url(row), headers=ALICE)
        assert r.status_code == 200
        assert r.json()["prompt_deleted"] is True and r.json()["prompt"] is None


def test_record_endpoint_unreadable_prompt_is_502_without_a_path(tmp_path):
    store = MemoryBlobStore()
    with TestClient(make_app(tmp_path, store=store)) as c:
        wait_ready(c)
        row = _bob_row(c)
        c.app.state.audit.flush()
        for path in store.ls("llm-service/prompts"):
            store.put(path, b"not json at all")
        r = c.get(_record_url(row), headers=ALICE)
        assert r.status_code == 502
        error = r.json()["error"]
        assert (error["type"], error["code"]) == ("server_error", "prompt_unreadable")
        assert "llm-service" not in error["message"] and "/" not in error["message"]


def test_record_endpoint_rejects_a_non_admin_and_a_bad_acting_user(client):
    row = _bob_row(client)
    assert client.get(_record_url(row), headers=BOB).status_code == 403
    assert client.get(_record_url(row)).status_code == 401
    r = client.get(_record_url(row), headers=ALICE | {ACTING: "bob"})
    assert r.status_code == 403 and "acting user" in r.json()["error"]["message"]
    assert client.get(_record_url(row), headers=ALICE | {ACTING: "carol"}).status_code == 200


def test_audit_users_route_is_not_taken_by_the_record_route(client):
    r = client.get("/admin/audit/users", headers=ALICE)
    assert r.status_code == 200 and "data" in r.json()


# Wiring of the merger and the checkpoint


class Clock:
    """A clock that tests move forward."""

    def __init__(self) -> None:
        self.value = datetime(2026, 11, 10, 12, 0, 1, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **kwargs) -> None:
        self.value += timedelta(**kwargs)


def test_the_maintenance_task_merges_and_shutdown_writes_the_checkpoint(tmp_path):
    store, clock = MemoryBlobStore(), Clock()
    app = make_app(tmp_path, store=store, now=clock, flush_interval=0.05, maintenance_interval=0.05)
    with TestClient(app) as c:
        wait_ready(c)
        for _ in range(3):
            assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        c.app.state.audit.flush()  # the segment gets the stamp of the first clock value
        clock.advance(minutes=12)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any("/audit/2026-11-10/parts/" in p for p in store.ls("llm-service/audit")):
                break
            time.sleep(0.05)
        files = store.ls("llm-service/audit")
        assert any("/audit/2026-11-10/parts/" in p for p in files), files
        rows = audit_records(c, user="bob")
        assert len(rows) == 3
    assert "llm-service/usage/2026-11-10.partial.json" in store.ls("llm-service/usage")


def test_shutdown_does_not_run_the_merger(tmp_path):
    store, clock = MemoryBlobStore(), Clock()
    app = make_app(tmp_path, store=store, now=clock, flush_interval=60)
    with TestClient(app) as c:
        wait_ready(c)
        c.post(CHAT, headers=BOB, json=HI)
        clock.advance(minutes=12)
    files = store.ls("llm-service/audit")
    assert files and not any("/parts/" in p for p in files)


def test_shutdown_checkpoint_ignores_the_60_second_limit(tmp_path):
    store, clock = MemoryBlobStore(), Clock()
    app = make_app(tmp_path, store=store, now=clock, flush_interval=0.05)
    path = "llm-service/usage/2026-11-10.partial.json"
    with TestClient(app) as c:
        wait_ready(c)
        c.post(CHAT, headers=BOB, json=HI)
        c.app.state.audit.flush()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and path not in store.ls("llm-service/usage"):
            time.sleep(0.05)
        assert path in store.ls("llm-service/usage")
        before = json.loads(store.get(path))
        clock.advance(seconds=5)
        c.post(CHAT, headers=BOB, json=HI)
        c.app.state.audit.flush()
    after = json.loads(store.get(path))
    assert after["through"] > before["through"]
    assert after["rows"] != before["rows"]


def test_the_start_logs_the_cost_of_the_usage_load(tmp_path, caplog):
    store = MemoryBlobStore()
    store.put("llm-service/audit/2026-11-09/day.jsonl", "")
    with caplog.at_level("INFO", logger="llm_service.app"):
        with TestClient(make_app(tmp_path, store=store)) as c:
            wait_ready(c)
    lines = [m for m in caplog.messages if m.startswith("usage totals loaded in ")]
    assert len(lines) == 1
    assert re.fullmatch(r"usage totals loaded in \d+\.\d s for 1 days", lines[0])


@pytest.mark.parametrize("step", ["close_days", "checkpoint"])
def test_a_failed_flusher_step_names_the_step(tmp_path, caplog, step):
    with caplog.at_level("WARNING", logger="llm_service.app"):
        with TestClient(make_app(tmp_path, flush_interval=0.05)) as c:
            wait_ready(c)

            def broken(*args, **kwargs):
                raise RuntimeError("simulated step failure")

            setattr(c.app.state.usage, step, broken)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if any("simulated step failure" in m for m in caplog.messages):
                    break
                time.sleep(0.05)
            delattr(c.app.state.usage, step)  # the class method again, for the shutdown
    assert f"the flusher step {step} failed: simulated step failure" in caplog.messages


class BlockedDeletes(MemoryBlobStore):
    """A memory store whose deletes of segments wait for `release`. `deleting` is set at the first one."""

    def __init__(self) -> None:
        super().__init__()
        self.deleting = threading.Event()
        self.release = threading.Event()

    def delete(self, path):
        if "/segments/" in path:
            self.deleting.set()
            assert self.release.wait(10)
        super().delete(path)


def test_the_flusher_runs_while_a_merge_deletes_its_inputs(tmp_path):
    store, clock = BlockedDeletes(), Clock()
    app = make_app(tmp_path, store=store, now=clock, flush_interval=0.05, maintenance_interval=0.05)
    path = "llm-service/usage/2026-11-10.partial.json"
    try:
        with TestClient(app) as c:
            wait_ready(c)
            c.post(CHAT, headers=BOB, json=HI)
            c.app.state.audit.flush()  # the segment gets the stamp of the first clock value
            clock.advance(minutes=12)
            assert store.deleting.wait(10)  # the merge waits in its delete
            before = store.ls("llm-service/audit/2026-11-10/segments")
            assert c.post(CHAT, headers=ALICE, json=HI).status_code == 200
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if len(store.ls("llm-service/audit/2026-11-10/segments")) > len(before):
                    break
                time.sleep(0.05)
            assert len(store.ls("llm-service/audit/2026-11-10/segments")) > len(before)
            store.release.set()
            task = c.app.state.maintenance_task
        assert task.done()
    finally:
        store.release.set()
    assert path in store.ls("llm-service/usage")


SERVICE = "/v1/service"


def test_descriptor_for_a_plain_caller(tmp_path):
    app = make_app(tmp_path, job_name="llm-service", environment="dev")
    with TestClient(app) as c:
        wait_ready(c)
        r = c.get(SERVICE, headers=BOB)
    assert r.status_code == 200
    assert r.json() == {
        "api": "datatailr-llm-service",
        "api_version": 1,
        "name": "llm-service",
        "environment": "dev",
        "state": "ready",
        "model": "org/a",
        "user": "bob",
        "admin": False,
        "caller_admin": False,
    }


def test_descriptor_names_default_to_unknown(client):
    body = client.get(SERVICE, headers=BOB).json()
    assert body["name"] == "unknown" and body["environment"] == "unknown"


@pytest.mark.parametrize(
    "headers, user, admin, caller_admin",
    [
        (ALICE, "alice", True, True),
        (ALICE | {ACTING: "bob"}, "bob", False, True),
        (ALICE | {ACTING: "carol"}, "carol", True, True),
        (BOB, "bob", False, False),
    ],
)
def test_descriptor_admin_flags(client, headers, user, admin, caller_admin):
    body = client.get(SERVICE, headers=headers).json()
    assert (body["user"], body["admin"], body["caller_admin"]) == (user, admin, caller_admin)
    assert "acting_user" not in body


@pytest.mark.parametrize(
    "headers",
    [
        BOB | {ACTING: "alice"},
        ALICE | {ACTING: "dave"},
        ALICE | {ACTING: "bad\nname"},
        ALICE | {ACTING: "x" * 257},
    ],
)
def test_descriptor_never_fails_on_the_acting_user(client, headers):
    r = client.get(SERVICE, headers=headers)
    assert r.status_code == 200
    body = r.json()
    caller = headers["X-Datatailr-User"]
    assert body["acting_user"] == "refused"
    assert body["user"] == caller
    assert body["admin"] == body["caller_admin"] == (caller == "alice")


def test_descriptor_model_is_null_while_nothing_is_served(tmp_path):
    engine = GatedEngine({"org/a"})
    try:
        with TestClient(make_app(tmp_path, engine=engine)) as c:
            body = c.get(SERVICE, headers=ALICE).json()
            engine.gate.set()
        assert body["model"] is None and body["state"] == "loading"
    finally:
        engine.gate.set()


def test_descriptor_needs_a_platform_user(client):
    r = client.get(SERVICE)
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_user"
