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


class FakeGpu:
    """A GPU reader double. It gives `value` and counts its reads."""

    def __init__(self, value) -> None:
        self.value = value
        self.reads = 0

    def read(self):
        self.reads += 1
        return self.value


NOT_GIVEN = object()


def make_app(
    tmp_path,
    engine=None,
    store=None,
    http=None,
    download=plain_download,
    now=None,
    maintenance_interval=None,
    with_access=True,
    gpu=None,
    sample_interval=None,
    metrics_http=NOT_GIVEN,
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
    extra["gpu"] = gpu or FakeGpu(None)
    if sample_interval is not None:
        extra["sample_interval"] = sample_interval
    if metrics_http is NOT_GIVEN:
        # The engine reader answers 404 unless a test gives it a client. None lets the app build its own client.
        metrics_http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(404)))
    extra["metrics_http"] = metrics_http
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
    ("POST", "/admin/models/download", {"json": {"model": "org/b"}}),
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
    assert any("the write of an audit record failed" in m for m in caplog.messages)


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


# Limits

LIMITS_PATH = "llm-service/limits.json"


class HeldEngine:
    """The engine side of the app. A request waits for `gate` while `hold` is true."""

    def __init__(self) -> None:
        self.hold = False
        self.gate = threading.Event()
        self.entered = threading.Event()
        self.fail = False
        self.crash = False
        self.hold_stream = False  # a stream waits for `gate` after its first chunk
        self.metrics: str | None = None  # the text of GET /metrics, or None for a 404

    def transport(self) -> httpx.MockTransport:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET" and request.url.path == "/metrics":
                if self.metrics is None:
                    return httpx.Response(404)
                return httpx.Response(200, text=self.metrics)
            if self.fail:
                raise httpx.ConnectError("engine is gone")
            if self.crash:
                raise RuntimeError("the transport broke")
            if self.hold:
                self.entered.set()
                await asyncio.to_thread(self.gate.wait, 10)
            body = json.loads(request.content)
            if body.get("stream") and self.hold_stream:
                gate = self.gate

                class Held(httpx.AsyncByteStream):
                    async def __aiter__(self):
                        yield b'data: {"choices":[{"delta":{"content":"a"}}]}\n\n'
                        await asyncio.to_thread(gate.wait, 10)
                        yield b"data: [DONE]\n\n"

                return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Held())
            if body.get("stream"):
                data = (
                    b'data: {"choices":[{"delta":{"content":"a"}}]}\n\n'
                    b'data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}\n\n'
                    b"data: [DONE]\n\n"
                )
                return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=data)
            return httpx.Response(
                200, json={"id": "x", "choices": [], "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}}
            )

        return httpx.MockTransport(handler)


def limited_client(tmp_path, limits, engine=None, now=None, **app_args):
    """Return a started TestClient whose store holds the limits file `limits` (a dict, bytes or None)."""
    store = MemoryBlobStore()
    if limits is not None:
        store.put(LIMITS_PATH, limits if isinstance(limits, bytes) else json.dumps(limits))
    engine = engine or HeldEngine()
    http = httpx.AsyncClient(transport=engine.transport(), base_url="http://engine")
    app_args.setdefault("metrics_http", httpx.AsyncClient(transport=engine.transport()))
    return TestClient(make_app(tmp_path, store=store, http=http, now=now, **app_args)), engine


FULL_LIMITS = {"requests_per_minute": 5, "tokens_per_day": 1000, "parallel_requests": 2}


def user_limits(**fields):
    return {"users": {"bob": fields}}


def parallel_of(client, user="bob"):
    return client.app.state.limiter.use(user)["parallel_requests"]


def test_requests_per_minute_limit_gives_429(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=1))
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 429
        assert r.json()["error"] == {
            "message": "You reached the limit of 1 request in one minute.",
            "type": "rate_limit_error",
            "code": "requests_per_minute",
        }
        assert 1 <= int(r.headers["Retry-After"]) <= 60
        assert r.headers["x-request-id"]


def test_refused_request_has_an_audit_record_and_counts_as_failed(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=1))
    with c:
        wait_ready(c)
        c.post(CHAT, headers=BOB, json=HI)
        r = c.post(CHAT, headers=BOB, json=HI)
        rec = audit_records(c, user="bob")[0]
        assert rec["status"] == 429 and rec["request_id"] == r.headers["x-request-id"]
        assert rec["total_tokens"] is None
        total = c.app.state.usage.summary(None, None, "bob", None)[0]
        assert total["requests"] == 2 and total["failed_requests"] == 1 and total["total_tokens"] == 5


def test_tokens_per_day_limit_gives_429_after_use(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(tokens_per_day=5))
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 429
        error = r.json()["error"]
        assert error["code"] == "tokens_per_day" and error["type"] == "rate_limit_error"
        assert error["message"] == "You reached the limit of 5 tokens in one day."
        assert int(r.headers["Retry-After"]) >= 1


def test_tokens_per_day_retry_after_is_the_time_to_utc_midnight(tmp_path):
    clock = lambda: datetime(2026, 11, 12, 23, 0, 0, tzinfo=timezone.utc)  # noqa: E731
    c, _ = limited_client(tmp_path, user_limits(tokens_per_day=5), now=clock)
    with c:
        wait_ready(c)
        c.post(CHAT, headers=BOB, json=HI)
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 429 and r.headers["Retry-After"] == "3600"


def test_parallel_requests_limit_gives_429_while_a_request_is_held(tmp_path):
    c, engine = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        engine.hold = True
        first = []
        worker = threading.Thread(target=lambda: first.append(c.post(CHAT, headers=BOB, json=HI)))
        worker.start()
        assert engine.entered.wait(10)
        r = c.post(CHAT, headers=BOB, json=HI)
        assert r.status_code == 429 and r.json()["error"]["code"] == "parallel_requests"
        assert r.headers["Retry-After"] == "1"
        assert parallel_of(c) == 1
        engine.gate.set()
        worker.join(10)
        assert not worker.is_alive()
        assert first[0].status_code == 200
        assert parallel_of(c) == 0
        engine.hold = False
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200


def test_parallel_counter_is_zero_after_complete_and_streamed_answers(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert parallel_of(c) == 0
        assert c.post(CHAT, headers=BOB, json={**HI, "stream": True}).status_code == 200
        assert parallel_of(c) == 0


def test_parallel_counter_is_zero_after_an_engine_failure(tmp_path):
    c, engine = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        engine.fail = True
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 502
        assert parallel_of(c) == 0


def test_parallel_counter_is_zero_after_forward_raises(tmp_path):
    c, engine = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        engine.crash = True
        with pytest.raises(RuntimeError):
            c.post(CHAT, headers=BOB, json=HI)
        assert parallel_of(c) == 0


def leave_a_stream(c):
    """Send a streamed request as bob, read its first chunk, then disconnect. Return the sent messages."""
    sent = []

    async def drop():
        proceed = asyncio.Event()
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": CHAT,
            "raw_path": CHAT.encode(),
            "query_string": b"",
            "headers": [(b"x-datatailr-user", b"bob"), (b"content-type", b"application/json")],
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
        }
        payload = json.dumps({**HI, "stream": True}).encode()
        given = False

        async def receive():
            nonlocal given
            if not given:
                given = True
                return {"type": "http.request", "body": payload, "more_body": False}
            await proceed.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            sent.append(message)
            if message["type"] == "http.response.body" and message.get("body"):
                proceed.set()
                await asyncio.sleep(0.05)

        await c.app(scope, receive, send)

    c.portal.call(drop)
    return sent


def test_parallel_counter_is_zero_after_a_client_leaves_a_stream(tmp_path):
    c, engine = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        engine.hold_stream = True
        sent = leave_a_stream(c)
        engine.gate.set()
        assert any(m.get("body") for m in sent)
        assert parallel_of(c) == 0
        assert audit_records(c, user="bob")[0]["status"] == 499


def test_refused_model_never_takes_or_releases_a_slot(tmp_path):
    c, engine = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        r = c.post(CHAT, headers=BOB, json={**HI, "model": "org/other"})
        assert r.status_code == 404 and parallel_of(c) == 0
        engine.hold = True
        worker = threading.Thread(target=lambda: c.post(CHAT, headers=BOB, json=HI))
        worker.start()
        assert engine.entered.wait(10)
        assert c.post(CHAT, headers=BOB, json={**HI, "model": "org/other"}).status_code == 404
        assert parallel_of(c) == 1  # the refused request did not release the slot of the held one
        engine.gate.set()
        worker.join(10)
        assert not worker.is_alive()
        assert parallel_of(c) == 0


def test_admin_without_an_entry_has_no_limit_even_with_a_default(tmp_path):
    c, _ = limited_client(tmp_path, {"default": {"requests_per_minute": 1, "tokens_per_day": None, "parallel_requests": None}})
    with c:
        wait_ready(c)
        for _ in range(3):
            assert c.post(CHAT, headers=ALICE, json=HI).status_code == 200
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 429


def test_acting_user_counts_for_the_acting_user(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=1))
    with c:
        wait_ready(c)
        acting = {**ALICE, ACTING: "bob"}
        assert c.post(CHAT, headers=acting, json=HI).status_code == 200
        assert c.post(CHAT, headers=acting, json=HI).status_code == 429
        assert c.app.state.limiter.use("alice")["requests_last_minute"] == 0  # the two requests count for bob
        assert c.post(CHAT, headers=ALICE, json=HI).status_code == 200  # alice has no limit


class CountedAdmin:
    """Count the admin lookups of each user by `AdminCheck.is_admin`."""

    def __init__(self, monkeypatch):
        self.users = []
        real = AdminCheck.is_admin

        def is_admin(check, user):
            self.users.append(user)
            return real(check, user)

        monkeypatch.setattr(AdminCheck, "is_admin", is_admin)


@pytest.mark.parametrize(
    "limits, lookups",
    [
        (None, 0),
        (user_limits(requests_per_minute=5), 0),
        ({"default": FULL_LIMITS, "users": {"bob": {"tokens_per_day": 9}}}, 0),
        ({"default": FULL_LIMITS}, 1),
    ],
)
def test_a_request_looks_up_the_admin_right_only_when_it_changes_the_limits(tmp_path, monkeypatch, limits, lookups):
    counted = CountedAdmin(monkeypatch)
    c, _ = limited_client(tmp_path, limits)
    with c:
        wait_ready(c)
        counted.users.clear()
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert counted.users == ["bob"] * lookups


def test_without_a_limits_file_each_request_passes(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        for _ in range(5):
            assert c.post(CHAT, headers=BOB, json=HI).status_code == 200


def test_a_broken_limits_file_lets_the_service_start(tmp_path):
    c, _ = limited_client(tmp_path, b"{not json")
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert c.app.state.limits.limits_for("bob", False)[1] == "none"


# Limit endpoints

ALL_NONE = {"requests_per_minute": None, "tokens_per_day": None, "parallel_requests": None}
FULL = {"requests_per_minute": 5, "tokens_per_day": 1000, "parallel_requests": 2}


def limit_events(client):
    return [r for r in audit_records(client, limit=1000) if r["event"] == "limits_change"]


def test_limits_me_for_a_user_with_an_entry_and_the_use_after_a_request(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=10, tokens_per_day=100))
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        body = c.get("/v1/limits/me", headers=BOB).json()
        assert body == {
            "user": "bob",
            "limits": {"requests_per_minute": 10, "tokens_per_day": 100, "parallel_requests": None},
            "use": {"requests_last_minute": 1, "tokens_today": 5, "parallel_requests": 0},
            "source": "user",
            "loaded": True,
        }


def test_limits_me_for_a_user_on_the_default(tmp_path):
    c, _ = limited_client(tmp_path, {"default": FULL})
    with c:
        wait_ready(c)
        body = c.get("/v1/limits/me", headers=BOB).json()
        assert body["limits"] == FULL and body["source"] == "default"


def test_limits_me_for_an_admin_and_without_a_file(tmp_path):
    c, _ = limited_client(tmp_path, {"default": FULL})
    with c:
        wait_ready(c)
        body = c.get("/v1/limits/me", headers=ALICE).json()
        assert body["user"] == "alice" and body["limits"] == ALL_NONE and body["source"] == "admin"
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        body = c.get("/v1/limits/me", headers=BOB).json()
        assert body["limits"] == ALL_NONE and body["source"] == "none"


def test_limits_me_follows_the_acting_user(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=3))
    with c:
        wait_ready(c)
        body = c.get("/v1/limits/me", headers={**ALICE, ACTING: "bob"}).json()
        assert body["user"] == "bob" and body["limits"]["requests_per_minute"] == 3
        assert c.get("/v1/limits/me", headers={**BOB, ACTING: "alice"}).status_code == 403
        assert c.get("/v1/limits/me").status_code == 401


LIMIT_ROUTES = [
    ("GET", "/admin/limits", {}),
    ("PUT", "/admin/limits/default", {"json": FULL}),
    ("PUT", "/admin/limits/users/bob", {"json": {"parallel_requests": 1}}),
    ("DELETE", "/admin/limits/users/bob", {}),
]


@pytest.mark.parametrize("method,path,kwargs", LIMIT_ROUTES)
def test_limit_admin_endpoints_need_an_admin(tmp_path, method, path, kwargs):
    c, _ = limited_client(tmp_path, user_limits(parallel_requests=1))
    with c:
        wait_ready(c)
        r = c.request(method, path, headers=BOB, **kwargs)
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "admin_required"
        assert c.request(method, path, **kwargs).status_code == 401
        assert c.app.state.limits.users() == {"bob": {"parallel_requests": 1}}


def test_get_limits_shows_default_entries_and_use(tmp_path):
    c, _ = limited_client(tmp_path, {"default": FULL, "users": {"carol": {"tokens_per_day": 9}}})
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        body = c.get("/admin/limits", headers=ALICE).json()
        assert body["default"] == FULL
        assert body["users"] == {"carol": {"tokens_per_day": 9}}
        assert body["use"]["bob"] == {"requests_last_minute": 1, "tokens_today": 5, "parallel_requests": 0}
        assert body["use"]["carol"] == {"requests_last_minute": 0, "tokens_today": 0, "parallel_requests": 0}


def test_get_limits_shows_the_effective_limits_and_their_source(tmp_path, monkeypatch):
    threads = []
    real = AdminCheck.is_admin

    def is_admin(check, user):
        threads.append(threading.get_ident())
        return real(check, user)

    entries = {"bob": {"tokens_per_day": 9}, "carol": {"parallel_requests": None}}
    c, _ = limited_client(tmp_path, {"default": FULL, "users": entries})
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=ALICE, json=HI).status_code == 200
        c.app.state.limiter.acquire("dave", {})
        c.app.state.limiter.release("dave")
        monkeypatch.setattr(AdminCheck, "is_admin", is_admin)
        body = c.get("/admin/limits", headers=ALICE).json()
        assert body["effective"] == {
            "alice": {"limits": ALL_NONE, "source": "admin"},
            "bob": {"limits": {**FULL, "tokens_per_day": 9}, "source": "user"},
            "carol": {"limits": {**FULL, "parallel_requests": None}, "source": "user"},
            "dave": {"limits": FULL, "source": "default"},
        }
        lookups = threads[1:]  # the first lookup is the admin check of the caller
        assert len(lookups) == 4 and len(set(lookups)) == 1  # one thread hop for all users


def test_get_limits_effective_source_is_none_without_a_default(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=3))
    with c:
        wait_ready(c)
        c.app.state.limiter.acquire("dave", {})
        effective = c.get("/admin/limits", headers=ALICE).json()["effective"]
        assert effective["dave"] == {"limits": ALL_NONE, "source": "none"}
        assert effective["bob"] == {"limits": {**ALL_NONE, "requests_per_minute": 3}, "source": "user"}


def test_get_limits_without_a_file(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        body = c.get("/admin/limits", headers=ALICE).json()
        assert body == {"default": None, "users": {}, "use": {}, "effective": {}, "loaded": True}


def test_put_default_changes_the_answer_and_the_enforcement_at_once(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        r = c.put("/admin/limits/default", headers=ALICE, json={**ALL_NONE, "requests_per_minute": 2})
        assert r.status_code == 200 and r.json() == {**ALL_NONE, "requests_per_minute": 2}
        assert c.get("/admin/limits", headers=ALICE).json()["default"]["requests_per_minute"] == 2
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 429
        assert c.get("/v1/limits/me", headers=BOB).json()["source"] == "default"


def test_put_user_and_delete_user_change_the_enforcement_at_once(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        r = c.put("/admin/limits/users/bob", headers=ALICE, json={"requests_per_minute": 1})
        assert r.status_code == 200 and r.json() == {"requests_per_minute": 1}
        assert c.get("/admin/limits", headers=ALICE).json()["users"] == {"bob": {"requests_per_minute": 1}}
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 429
        r = c.delete("/admin/limits/users/bob", headers=ALICE)
        assert r.status_code == 200 and r.json() == {"removed": "bob"}
        assert c.get("/admin/limits", headers=ALICE).json()["users"] == {}
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200


def test_the_changes_reach_the_limits_file(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        c.put("/admin/limits/default", headers=ALICE, json=FULL)
        c.put("/admin/limits/users/bob", headers=ALICE, json={"tokens_per_day": None})
        stored = json.loads(c.app.state.limits._store.get(LIMITS_PATH))
        assert stored == {"default": FULL, "users": {"bob": {"tokens_per_day": None}}}


def test_delete_of_a_missing_entry_is_404(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        r = c.delete("/admin/limits/users/bob", headers=ALICE)
        assert r.status_code == 404
        assert r.json()["error"] == {
            "message": r.json()["error"]["message"],
            "type": "invalid_request_error",
            "code": "limit_not_found",
        }
        assert limit_events(c) == []


@pytest.mark.parametrize(
    "path,body",
    [
        ("/admin/limits/default", {"requests_per_minute": 1}),
        ("/admin/limits/default", {**FULL, "extra": 1}),
        ("/admin/limits/default", {**FULL, "requests_per_minute": 0}),
        ("/admin/limits/default", {**FULL, "requests_per_minute": True}),
        ("/admin/limits/default", {**FULL, "tokens_per_day": "5"}),
        ("/admin/limits/users/bob", {}),
        ("/admin/limits/users/bob", {"nope": 1}),
        ("/admin/limits/users/bob", {"parallel_requests": 1_000_000_001}),
    ],
)
def test_a_bad_limits_body_is_400_invalid_limit(tmp_path, path, body):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        r = c.put(path, headers=ALICE, json=body)
        assert r.status_code == 400
        error = r.json()["error"]
        assert error["code"] == "invalid_limit" and error["type"] == "invalid_request_error"
        assert error["message"]
        assert c.app.state.limits.default() is None and c.app.state.limits.users() == {}
        assert limit_events(c) == []


@pytest.mark.parametrize("path", ["/admin/limits/default", "/admin/limits/users/bob"])
@pytest.mark.parametrize("content", [b"hello", b"[1]", b"", b"5", b"[" * 100000])
def test_a_body_that_is_not_a_json_object_is_400_invalid_limit(tmp_path, path, content):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        r = c.put(path, headers=ALICE, content=content)
        assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_limit"


@pytest.mark.parametrize("name", ["x" * 257, "a%09b", "a%7Fb", "%20%20"])
@pytest.mark.parametrize("method", ["PUT", "DELETE"])
def test_a_bad_user_name_is_400_invalid_user(tmp_path, name, method):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        kwargs = {"json": {"parallel_requests": 1}} if method == "PUT" else {}
        r = c.request(method, f"/admin/limits/users/{name}", headers=ALICE, **kwargs)
        assert r.status_code == 400
        error = r.json()["error"]
        assert error["code"] == "invalid_user" and error["type"] == "invalid_request_error"
        assert c.app.state.limits.users() == {}


def test_a_user_name_is_stripped_and_a_long_name_at_the_limit_is_fine(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        assert c.put("/admin/limits/users/%20bob%20", headers=ALICE, json={"parallel_requests": 1}).status_code == 200
        assert c.put(f"/admin/limits/users/{'x' * 256}", headers=ALICE, json={"parallel_requests": 1}).status_code == 200
        assert sorted(c.app.state.limits.users()) == sorted(["bob", "x" * 256])


def test_a_bad_user_name_is_refused_before_the_body_is_read(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        r = c.put("/admin/limits/users/a%09b", headers=ALICE, json={"nope": 1})
        assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_user"


class FailingLimitsStore(MemoryBlobStore):
    """A store that refuses each write of the limits file while `fail` is true."""

    def __init__(self) -> None:
        super().__init__()
        self.fail = False

    def put(self, path, data):
        if self.fail and path == LIMITS_PATH:
            raise OSError("the store is down")
        super().put(path, data)


@pytest.mark.parametrize("method,path,kwargs", LIMIT_ROUTES[1:])
def test_a_failed_write_is_502_and_changes_nothing(tmp_path, method, path, kwargs):
    store = FailingLimitsStore()
    store.put(LIMITS_PATH, json.dumps({"default": FULL, "users": {"bob": {"parallel_requests": 1}}}))
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    with TestClient(make_app(tmp_path, store=store, http=http)) as c:
        wait_ready(c)
        store.fail = True
        r = c.request(method, path, headers=ALICE, **kwargs)
        assert r.status_code == 502
        error = r.json()["error"]
        assert error["code"] == "storage_unavailable" and error["type"] == "server_error"
        assert c.app.state.limits.default() == FULL
        assert c.app.state.limits.users() == {"bob": {"parallel_requests": 1}}
        assert limit_events(c) == []


def test_a_limits_file_that_could_not_be_read_gives_502(tmp_path):
    class UnreadableStore(MemoryBlobStore):
        def get(self, path):
            if path == LIMITS_PATH:
                raise OSError("the store is down")
            return super().get(path)

    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    with TestClient(make_app(tmp_path, store=UnreadableStore(), http=http)) as c:
        wait_ready(c)
        r = c.put("/admin/limits/default", headers=ALICE, json=FULL)
        assert r.status_code == 502 and r.json()["error"]["code"] == "storage_unavailable"


class SwitchedLimitsStore(MemoryBlobStore):
    """A store whose reads of the limits file fail while `fail` is true."""

    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    def get(self, path):
        if self.fail and path == LIMITS_PATH:
            raise OSError("the store is down")
        return super().get(path)


def test_a_failed_read_shows_loaded_false_and_the_flusher_reads_the_file_again(tmp_path):
    store = SwitchedLimitsStore()
    store.put(LIMITS_PATH, json.dumps(user_limits(requests_per_minute=1)))
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    with TestClient(make_app(tmp_path, store=store, http=http, flush_interval=0.05)) as c:
        wait_ready(c)
        assert c.get("/admin/limits", headers=ALICE).json()["loaded"] is False
        assert c.get("/v1/limits/me", headers=BOB).json()["loaded"] is False
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200  # no limit applies now
        store.fail = False
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not c.app.state.limits.loaded():
            time.sleep(0.05)
        assert c.get("/admin/limits", headers=ALICE).json()["loaded"] is True
        assert c.get("/v1/limits/me", headers=BOB).json()["loaded"] is True
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 429


def test_each_change_is_audited_with_old_and_new(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        c.put("/admin/limits/default", headers=ALICE, json=FULL)
        c.put("/admin/limits/default", headers={**ALICE, ACTING: "carol"}, json=ALL_NONE)
        c.put("/admin/limits/users/bob", headers=ALICE, json={"tokens_per_day": 7})
        c.put("/admin/limits/users/bob", headers=ALICE, json={"tokens_per_day": 8})
        c.delete("/admin/limits/users/bob", headers=ALICE)
        events = sorted(limit_events(c), key=lambda e: e["ts"])
        assert len(events) == 5
        assert all(e["endpoint"] == "/admin/limits" and e["status"] == 200 for e in events)
        details = [(e["user"], e["detail"]) for e in events]
        assert sorted(details, key=json.dumps) == sorted(
            [
                ("alice", {"target": "default", "old": None, "new": FULL}),
                ("carol", {"target": "default", "old": FULL, "new": ALL_NONE}),
                ("alice", {"target": "bob", "old": None, "new": {"tokens_per_day": 7}}),
                ("alice", {"target": "bob", "old": {"tokens_per_day": 7}, "new": {"tokens_per_day": 8}}),
                ("alice", {"target": "bob", "old": {"tokens_per_day": 8}, "new": None}),
            ],
            key=json.dumps,
        )
        prompt = full_record(c, events[0])["prompt"]
        assert prompt["event"] == "limits_change" and set(prompt) == {"event", "target", "old", "new"}


def test_get_limits_keeps_a_user_with_tokens_today_after_the_window_ends(tmp_path):
    c, _ = limited_client(tmp_path, {"default": FULL})
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        # The request window of bob ends, as it does after 60 seconds and after a restart.
        c.app.state.limiter._users.clear()
        body = c.get("/admin/limits", headers=ALICE).json()
        assert body["use"]["bob"] == {"requests_last_minute": 0, "tokens_today": 5, "parallel_requests": 0}
        assert body["effective"]["bob"]["source"] == "default"


def test_an_admin_whose_entry_goes_away_gets_no_limit_from_the_default(tmp_path):
    c, _ = limited_client(tmp_path, {"default": FULL, "users": {"alice": {"requests_per_minute": 1}}})
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=ALICE, json=HI).status_code == 200
        assert c.post(CHAT, headers=ALICE, json=HI).status_code == 429
        assert c.delete("/admin/limits/users/alice", headers=ALICE).status_code == 200
        for _ in range(FULL["requests_per_minute"] + 2):
            assert c.post(CHAT, headers=ALICE, json=HI).status_code == 200


def test_ttft_ms_is_set_for_a_stream_and_null_otherwise(client):
    assert client.post(CHAT, headers=BOB, json={**HI, "stream": True}).status_code == 200
    assert client.post(CHAT, headers=BOB, json=HI).status_code == 200
    assert client.post(CHAT, headers=BOB, json={**HI, "model": "other"}).status_code == 404
    rows = audit_records(client, user="bob")
    streamed = [r for r in rows if r["stream"] is True]
    others = [r for r in rows if r["stream"] is not True]
    assert len(streamed) == 1 and len(others) == 2
    assert isinstance(streamed[0]["ttft_ms"], int)
    assert 0 <= streamed[0]["ttft_ms"] <= streamed[0]["duration_ms"]
    assert all(r["ttft_ms"] is None for r in others)


def test_ttft_ms_is_null_for_a_stream_without_content(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        data = (
            b'data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n'
            b'data: {"choices":[{"index":0,"delta":{"content":""}}]}\n\n'
            b'data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}}\n\n'
            b"data: [DONE]\n\n"
        )
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=data)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://engine")
    with TestClient(make_app(tmp_path, http=http)) as c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json={**HI, "stream": True}).status_code == 200
        (row,) = audit_records(c, user="bob")
    assert row["stream"] is True and row["status"] == 200
    assert row["ttft_ms"] is None


# Live load and the metrics history

METRICS_TEXT = (
    "# TYPE vllm:num_requests_running gauge\n"
    "vllm:num_requests_running 3\n"
    "vllm:num_requests_waiting 2\n"
    "vllm:prompt_tokens_total 100\n"
    "vllm:generation_tokens_total 50\n"
    'vllm:kv_cache_usage_perc{engine="0"} 0.37\n'
)
METRICS_DIR = "llm-service/metrics"
OLD_WRITER = "20261109T080000Z-bbbbbbbb"  # the files of an earlier process
DAY = "2026-11-10"


def in_flight_counts(c):
    """Wrap `begin` and `end` of the monitor to count calls. Return the two lists."""
    begun, ended = [], []
    load = c.app.state.load
    begin, end = load.begin, load.end
    load.begin = lambda: (begun.append(1), begin())[1]
    load.end = lambda: (ended.append(1), end())[1]
    return begun, ended


def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timeout")


def test_in_flight_is_1_while_a_request_is_held_and_0_after(tmp_path):
    c, engine = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        begun, ended = in_flight_counts(c)
        engine.hold = True
        first = []
        worker = threading.Thread(target=lambda: first.append(c.post(CHAT, headers=BOB, json=HI)))
        worker.start()
        assert engine.entered.wait(10)
        assert c.app.state.load.in_flight() == 1
        engine.gate.set()
        worker.join(10)
        assert first[0].status_code == 200
        assert c.app.state.load.in_flight() == 0
        assert (len(begun), len(ended)) == (1, 1)


def test_in_flight_is_0_after_a_stream_and_an_engine_failure(tmp_path):
    c, engine = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        begun, ended = in_flight_counts(c)
        assert c.post(CHAT, headers=BOB, json={**HI, "stream": True}).status_code == 200
        assert c.app.state.load.in_flight() == 0
        engine.fail = True
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 502
        assert c.app.state.load.in_flight() == 0
        engine.fail = False
        engine.crash = True
        with pytest.raises(RuntimeError):
            c.post(CHAT, headers=BOB, json=HI)
        assert c.app.state.load.in_flight() == 0
        assert (len(begun), len(ended)) == (3, 3)


def test_in_flight_is_0_after_a_client_leaves_a_stream(tmp_path):
    c, engine = limited_client(tmp_path, None)
    with c:
        wait_ready(c)
        begun, ended = in_flight_counts(c)
        engine.hold_stream = True
        leave_a_stream(c)
        engine.gate.set()
        assert c.app.state.load.in_flight() == 0
        assert (len(begun), len(ended)) == (1, 1)


def test_a_refused_request_never_counts_as_in_flight(tmp_path):
    c, _ = limited_client(tmp_path, user_limits(requests_per_minute=1))
    with c:
        wait_ready(c)
        begun, ended = in_flight_counts(c)
        assert c.post(CHAT, headers=BOB, json={**HI, "model": "org/other"}).status_code == 404
        assert c.post(CHAT, headers=BOB, content=b"x").status_code == 400
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 429
        assert (len(begun), len(ended)) == (1, 1)
        assert c.app.state.load.in_flight() == 0


def test_live_needs_an_admin(client):
    assert client.get("/admin/metrics/live", headers=BOB).status_code == 403
    assert client.get("/admin/metrics/live").status_code == 401
    assert client.get("/admin/metrics/live", headers=ALICE).status_code == 200


def test_live_shows_the_gpu_and_the_engine_numbers_after_a_round(tmp_path):
    c, engine = limited_client(tmp_path, None, now=Clock(), gpu=FakeGpu((1000, 2000)), sample_interval=0.02)
    engine.metrics = METRICS_TEXT
    with c:
        wait_ready(c)
        wait_until(lambda: c.app.state.load.live()["now"]["running"] == 3)
        body = c.get("/admin/metrics/live", headers=ALICE).json()
        assert body["now"]["running"] == 3 and body["now"]["waiting"] == 2
        assert body["now"]["kv_cache_use"] == 0.37
        assert body["now"]["gpu_memory_used"] == 1000 and body["now"]["gpu_memory_total"] == 2000
        assert body["now"]["in_flight"] == 0
        assert body["points"] == []


def test_live_has_null_numbers_without_a_metrics_endpoint(tmp_path):
    c, engine = limited_client(tmp_path, None, sample_interval=0.02)
    with c:
        wait_ready(c)
        wait_until(lambda: c.app.state.load.live()["now"]["ts"] is not None)
        now = c.get("/admin/metrics/live", headers=ALICE).json()["now"]
        assert now["running"] is None and now["waiting"] is None
        assert now["kv_cache_use"] is None
        assert now["prompt_tokens_per_s"] is None
        assert now["gpu_memory_used"] is None and now["gpu_memory_total"] is None
        assert now["in_flight"] == 0


def test_the_gpu_is_read_at_each_third_round(tmp_path):
    class CountingGpu:
        """Each read gives a new value: (1, 1), then (2, 2), and so on."""

        def __init__(self) -> None:
            self.reads = 0

        def read(self):
            self.reads += 1
            return (self.reads, self.reads)

    c, _ = limited_client(tmp_path, None, gpu=CountingGpu(), sample_interval=0.01)
    load = c.app.state.load
    real, given = load.add_sample, []

    def spy(engine_numbers, gpu):
        given.append(gpu)
        return real(engine_numbers, gpu)

    load.add_sample = spy
    with c:
        wait_ready(c)
        wait_until(lambda: len(given) >= 9)
        assert given[:9] == [(1, 1)] * 3 + [(2, 2)] * 3 + [(3, 3)] * 3


def test_a_minute_with_a_request_becomes_a_point(tmp_path):
    clock = Clock()
    c, _ = limited_client(tmp_path, None, now=clock, sample_interval=0.02)
    with c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json={**HI, "stream": True}).status_code == 200
        assert c.post(CHAT, headers=BOB, json={**HI, "model": "other"}).status_code == 404
        clock.advance(minutes=1)
        wait_until(lambda: c.app.state.load.live()["points"])
        point = c.get("/admin/metrics/live", headers=ALICE).json()["points"][0]
        assert point["ts"] == "2026-11-10T12:00:00Z"
        assert point["requests"] == 2 and point["failed_requests"] == 1
        assert point["prompt_tokens"] == 3 and point["completion_tokens"] == 2
        assert isinstance(point["ttft_ms_p50"], int) and isinstance(point["ttft_ms_p95"], int)


def test_a_round_that_raises_does_not_stop_the_next_round(tmp_path):
    c, _ = limited_client(tmp_path, None, sample_interval=0.02)
    with c:
        wait_ready(c)
        load = c.app.state.load
        real = load.add_sample
        calls = []

        def broken(engine_numbers, gpu):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("one bad round")
            return real(engine_numbers, gpu)

        load.add_sample = broken
        wait_until(lambda: len(calls) >= 3)


def history(c, since, until, headers=ALICE):
    return c.get("/admin/metrics/history", headers=headers, params={"since": since, "until": until})


def test_history_returns_the_points_the_store_wrote(tmp_path):
    clock = Clock()
    store = MemoryBlobStore()
    store.put(f"{METRICS_DIR}/2026-11-09/{OLD_WRITER}.jsonl", json.dumps({"ts": "2026-11-09T23:59:00Z", "requests": 4}) + "\n")
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    app = make_app(tmp_path, store=store, http=http, now=clock, sample_interval=0.02)
    with TestClient(app) as c:
        wait_ready(c)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        clock.advance(minutes=1)
        wait_until(lambda: c.app.state.load.live()["points"])
        c.app.state.metrics.write(force=True)
        assert f"{METRICS_DIR}/{DAY}/{c.app.state.metrics.writer}.jsonl" in store.ls(METRICS_DIR)
        body = history(c, "2026-11-09T23:00:00Z", "2026-11-10T13:00:00+00:00").json()
        # The range touches 841 minutes, so a point covers 2 minutes and has the start of its bucket as ts.
        assert body["step_seconds"] == 120 and body["missing_days"] == []
        assert [p["ts"] for p in body["points"]] == ["2026-11-09T23:58:00Z", "2026-11-10T12:00:00Z"]
        assert body["points"][1]["requests"] == 1
        # A time without a zone counts as UTC.
        naive = history(c, "2026-11-10T12:00:00", "2026-11-10T12:30:00").json()
        assert [p["ts"] for p in naive["points"]] == ["2026-11-10T12:00:00Z"]
        assert history(c, "2026-11-10T12:00:00Z", "2026-11-10T12:30:00Z", BOB).status_code == 403


def test_history_names_the_days_that_cannot_be_read(tmp_path):
    class Broken(MemoryBlobStore):
        def get(self, path):
            if path.startswith(f"{METRICS_DIR}/2026-11-09"):
                raise OSError("the store is down")
            return super().get(path)

    store = Broken()
    store.put(f"{METRICS_DIR}/2026-11-09/{OLD_WRITER}.jsonl", "{}\n")
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    with TestClient(make_app(tmp_path, store=store, http=http, now=Clock())) as c:
        wait_ready(c)
        body = history(c, "2026-11-09T00:00:00Z", "2026-11-10T13:00:00Z").json()
        assert body["missing_days"] == ["2026-11-09"] and body["points"] == []


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"since": "2026-11-10T12:00:00Z"},
        {"until": "2026-11-10T12:00:00Z"},
        {"since": "yesterday", "until": "2026-11-10T12:00:00Z"},
        {"since": "2026-11-10T12:00:00Z", "until": "x"},
        {"since": "2026-11-10T13:00:00Z", "until": "2026-11-10T12:00:00Z"},
        {"since": "2026-10-01T00:00:00Z", "until": "2026-11-10T12:00:00Z"},
    ],
)
def test_history_refuses_a_bad_range(client, params):
    r = client.get("/admin/metrics/history", headers=ALICE, params=params)
    assert r.status_code == 400
    error = r.json()["error"]
    assert error["code"] == "invalid_time" and error["type"] == "invalid_request_error" and error["message"]


@pytest.mark.parametrize(
    "since,until",
    [
        ("0001-01-01T00:00:00+05:00", "2026-11-10T12:00:00Z"),
        ("2026-11-10T12:00:00Z", "9999-12-31T23:59:59-05:00"),
    ],
)
def test_history_answers_400_for_a_time_at_the_edge_of_the_range(client, since, until):
    r = history(client, since, until)
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_time"


def test_history_range_of_31_days_passes_and_one_second_more_is_refused(client):
    assert history(client, "2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z").status_code == 200
    r = history(client, "2026-10-01T00:00:00Z", "2026-11-01T00:00:01Z")
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_time"


def test_history_takes_a_lower_case_z(client):
    r = history(client, "2026-11-10T12:00:00z", "2026-11-10T13:00:00z")
    assert r.status_code == 200


def test_a_failed_limiter_release_still_ends_the_in_flight_count(tmp_path):
    c, _ = limited_client(tmp_path, None)
    with c:
        wait_ready(c)

        def broken(user):
            raise RuntimeError("release failed")

        c.app.state.limiter.release = broken
        c.post(CHAT, headers=BOB, json=HI)  # the proxy logs the failed callback
        assert c.app.state.load.in_flight() == 0


def test_the_points_reach_blob_storage_at_shutdown(tmp_path):
    clock, store = Clock(), MemoryBlobStore()
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    app = make_app(tmp_path, store=store, http=http, now=clock, sample_interval=60)
    with TestClient(app) as c:
        wait_ready(c)
        wait_until(lambda: c.app.state.load.live()["now"]["ts"] is not None)
        assert c.post(CHAT, headers=BOB, json=HI).status_code == 200
        clock.advance(minutes=1)  # the sampler sleeps, so only the last round sees this minute
        writer = c.app.state.metrics.writer
    assert store.ls(METRICS_DIR) == [f"{METRICS_DIR}/{DAY}/{writer}.jsonl"]
    lines = store.get(f"{METRICS_DIR}/{DAY}/{writer}.jsonl").decode().splitlines()
    assert [json.loads(line)["requests"] for line in lines] == [1]


def test_the_service_reads_the_files_of_today(tmp_path):
    clock, store = Clock(), MemoryBlobStore()
    store.put(f"{METRICS_DIR}/{DAY}/{OLD_WRITER}.jsonl", json.dumps({"ts": "2026-11-10T11:00:00Z", "requests": 7}) + "\n")
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    with TestClient(make_app(tmp_path, store=store, http=http, now=clock)) as c:
        wait_ready(c)
        body = history(c, "2026-11-10T00:00:00Z", "2026-11-10T13:00:00Z").json()
        assert [p["requests"] for p in body["points"]] == [7]


def test_history_shows_a_file_of_another_process_written_after_the_start(tmp_path):
    clock, store = Clock(), MemoryBlobStore()
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    with TestClient(make_app(tmp_path, store=store, http=http, now=clock)) as c:
        wait_ready(c)
        store.put(f"{METRICS_DIR}/{DAY}/{OLD_WRITER}.jsonl", json.dumps({"ts": "2026-11-10T11:00:00Z", "requests": 3}) + "\n")
        body = history(c, "2026-11-10T00:00:00Z", "2026-11-10T13:00:00Z").json()
        assert [p["requests"] for p in body["points"]] == [3]
    assert not hasattr(c.app.state.metrics, "load_today")


def test_the_maintenance_task_removes_old_metrics_files(tmp_path):
    store = MemoryBlobStore()
    store.put(f"{METRICS_DIR}/2026-01-01.jsonl", "{}\n")
    store.put(f"{METRICS_DIR}/2026-01-01/{OLD_WRITER}.jsonl", "{}\n")
    store.put(f"{METRICS_DIR}/2026-11-01/{OLD_WRITER}.jsonl", "{}\n")
    http = httpx.AsyncClient(transport=HeldEngine().transport(), base_url="http://engine")
    app = make_app(
        tmp_path, store=store, http=http, now=Clock(), maintenance_interval=0.05, metrics_retention_days=30
    )
    with TestClient(app) as c:
        wait_ready(c)
        wait_until(lambda: store.ls(METRICS_DIR) == [f"{METRICS_DIR}/2026-11-01/{OLD_WRITER}.jsonl"])


def test_the_engine_reader_does_not_use_the_proxy_client(tmp_path):
    proxy_paths, reader_paths = [], []

    def proxy(request: httpx.Request) -> httpx.Response:
        proxy_paths.append(request.url.path)
        return httpx.Response(200, json={"id": "x", "choices": [], "usage": None})

    def reader(request: httpx.Request) -> httpx.Response:
        reader_paths.append(request.url.path)
        return httpx.Response(200, text=METRICS_TEXT)

    http = httpx.AsyncClient(transport=httpx.MockTransport(proxy), base_url="http://engine")
    metrics_http = httpx.AsyncClient(transport=httpx.MockTransport(reader))
    app = make_app(tmp_path, http=http, metrics_http=metrics_http, sample_interval=0.02)
    with TestClient(app) as c:
        wait_ready(c)
        wait_until(lambda: c.app.state.load.live()["now"]["running"] == 3)
    assert "/metrics" in reader_paths
    assert "/metrics" not in proxy_paths


def test_the_app_builds_its_own_client_for_the_engine_metrics(tmp_path):
    proxy_paths = []

    def proxy(request: httpx.Request) -> httpx.Response:
        proxy_paths.append(request.url.path)
        return httpx.Response(200, text=METRICS_TEXT)

    http = httpx.AsyncClient(transport=httpx.MockTransport(proxy), base_url="http://engine")
    app = make_app(tmp_path, http=http, metrics_http=None, sample_interval=0.02)
    with TestClient(app) as c:
        wait_ready(c)
        rounds = []
        real = c.app.state.load.add_sample
        c.app.state.load.add_sample = lambda numbers, gpu: rounds.append(numbers) or real(numbers, gpu)
        wait_until(lambda: len(rounds) >= 3)
    assert "/metrics" not in proxy_paths
    assert rounds[0]["running"] is None


def test_the_sampler_stops_before_the_engine_stops(tmp_path):
    class SlowStop(FakeEngine):
        def __init__(self) -> None:
            super().__init__()
            self.stopping = False

        async def stop(self) -> None:
            self.stopping = True
            await asyncio.sleep(0.2)
            await super().stop()

    engine = SlowStop()
    reads_during_stop = []
    app = make_app(tmp_path, engine=engine, sample_interval=0.01)
    with TestClient(app) as c:
        wait_ready(c)
        load = c.app.state.load
        real = load.add_sample

        def spy(numbers, gpu):
            if engine.stopping and numbers:
                reads_during_stop.append(numbers)
            return real(numbers, gpu)

        load.add_sample = spy
        wait_until(lambda: load.live()["now"]["ts"] is not None)
    assert engine.stopped == 1
    assert reads_during_stop == []


DOWNLOAD = "/admin/models/download"


def download_events(client):
    return [r for r in audit_records(client) if r["event"] == "model_download"]


def wait_download(client, state):
    return wait_for(client, lambda s: (s["download"] or {}).get("state") == state)


def test_download_202_then_done_and_audited_under_the_admin(tmp_path):
    store = MemoryBlobStore()
    with TestClient(make_app(tmp_path, store=store)) as c:
        wait_ready(c)
        r = c.post(DOWNLOAD, headers=ALICE | {ACTING: "carol"}, json={"model": "org/b"})
        assert r.status_code == 202
        assert r.json()["download"]["model"] == "org/b"
        assert r.json()["download"]["revision"] == "main"
        status = wait_download(c, "done")
        assert status["download"]["error"] is None
        assert status["active"] == {"model": "org/a", "revision": "main"}
        assert store.exists("llm-service/models/org--b/main/.complete")
        events = download_events(c)
        assert len(events) == 1
        event = events[0]
        assert event["user"] == "carol" and event["status"] == 200
        assert event["endpoint"] == DOWNLOAD and event["model"] == "org/b"
        assert event["detail"] == {"revision": "main", "error": None}
        prompt = full_record(c, event)["prompt"]
        assert prompt == {"event": "model_download", "revision": "main", "error": None}
        # A download does not change the admin of a later switch event.
        assert c.app.state.last_admin == "system"


def test_download_of_a_cached_pair_answers_200_and_starts_nothing(client):
    r = client.post(DOWNLOAD, headers=ALICE, json={"model": "org/a", "revision": "main"})
    assert r.status_code == 200
    assert r.json() == {"state": "cached", "model": "org/a", "revision": "main"}
    assert client.get("/admin/model", headers=ALICE).json()["download"] is None
    assert download_events(client) == []


def test_failed_download_is_audited_with_status_500_and_leaves_no_marker(tmp_path):
    store = MemoryBlobStore()

    def download(model: str, revision: str, target: Path) -> None:
        if model == "org/b":
            raise OSError("no network")
        plain_download(model, revision, target)

    with TestClient(make_app(tmp_path, store=store, download=download)) as c:
        wait_ready(c)
        assert c.post(DOWNLOAD, headers=ALICE, json={"model": "org/b"}).status_code == 202
        status = wait_download(c, "failed")
        assert status["download"]["error"] == "OSError: no network"
        assert not store.exists("llm-service/models/org--b/main/.complete")
        event = download_events(c)[0]
        assert event["status"] == 500 and event["user"] == "alice"
        assert event["detail"] == {"revision": "main", "error": "OSError: no network"}


def test_download_bad_body_or_name_is_400(client):
    for body in ({"model": "nonsense"}, {"model": "org/b", "revision": "a/b"},
                 {"model": 5}, {"model": "org/b", "revision": 5}, {}, [1]):
        r = client.post(DOWNLOAD, headers=ALICE, json=body)
        assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_model"
    r = client.post(DOWNLOAD, headers=ALICE, content=b"nope")
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_model"
    assert client.get("/admin/model", headers=ALICE).json()["download"] is None


def test_download_conflicts_and_delete_during_a_download_are_409(tmp_path):
    download = GatedDownload("org/b")
    store = MemoryBlobStore()
    try:
        with TestClient(make_app(tmp_path, store=store, download=download)) as c:
            wait_ready(c)
            assert c.post(DOWNLOAD, headers=ALICE, json={"model": "org/b"}).status_code == 202
            assert download.fetching.wait(10)
            r = c.post(DOWNLOAD, headers=ALICE, json={"model": "org/c"})
            assert r.status_code == 409 and r.json()["error"]["code"] == "download_in_progress"
            assert r.json()["error"]["type"] == "invalid_request_error"
            # A file of the running download is in blob storage before the marker.
            store.put("llm-service/models/org--b/main/config.json", "{}")
            r = c.delete("/admin/models/org/b", headers=ALICE)
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            download.gate.set()
            wait_download(c, "done")
    finally:
        download.gate.set()


def test_download_of_a_pair_under_removal_is_409(tmp_path):
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
            r = c.post(DOWNLOAD, headers=ALICE, json={"model": "org/other"})
            assert r.status_code == 409 and r.json()["error"]["code"] == "model_in_use"
            store.gate.set()
            worker.join(10)
            assert result["r"].status_code == 200
    finally:
        store.gate.set()


def test_shutdown_cancels_a_running_download(tmp_path):
    download = GatedDownload("org/b")
    try:
        with TestClient(make_app(tmp_path, download=download)) as c:
            wait_ready(c)
            assert c.post(DOWNLOAD, headers=ALICE, json={"model": "org/b"}).status_code == 202
            assert download.fetching.wait(10)
            task = c.app.state.download_task
        assert task.cancelled()
    finally:
        download.gate.set()
