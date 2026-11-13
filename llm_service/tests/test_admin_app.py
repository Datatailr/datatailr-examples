import asyncio
import json
import logging
import re
import shutil
import subprocess
import sys
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from llm_service.admin_app import app as app_module
from llm_service.admin_app.app import STATIC_DIR, create_app, default_app
from llm_service.admin_app.registry import Registry

ALICE = {"X-Datatailr-User": "alice"}
BOB = {"X-Datatailr-User": "bob"}
CAROL = {"X-Datatailr-User": "carol"}
ERROR_403 = {"error": {"message": "admin only", "type": "permission_error", "code": "admin_required"}}


class FakeService:
    """One fake LLM service. It answers from a table and records each request."""

    def __init__(self, admins=("alice",), users=("bob",)):
        self.admins = set(admins)
        self.users = set(users)  # users with access who are not admins
        self.answers: dict[tuple[str, str], tuple[int, object]] = {}
        self.answer_headers: dict[tuple[str, str], dict[str, str]] = {}
        self.error: Exception | None = None
        self.raw: bytes | None = None
        self.caller_admin = True
        self.requests: list[httpx.Request] = []

    def descriptor(self, acting: str | None) -> dict:
        body = {
            "api": "datatailr-llm-service",
            "api_version": 1,
            "name": "svc",
            "environment": "dev",
            "state": "ready",
            "model": "org/model",
            "user": "alice",
            "admin": True,
            "caller_admin": self.caller_admin,
        }
        if acting is not None:
            if acting in self.admins | self.users:
                body["user"], body["admin"] = acting, acting in self.admins
            else:
                body["acting_user"] = "refused"
        return body

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.raw_path.decode().split("?")[0]
        if self.error:
            raise self.error
        if self.raw is not None:
            return httpx.Response(200, content=self.raw)
        if (request.method, path) in self.answers:
            status, body = self.answers[(request.method, path)]
            return httpx.Response(status, json=body, headers=self.answer_headers.get((request.method, path)))
        if path == "/v1/service":
            return httpx.Response(200, json=self.descriptor(request.headers.get("X-LLM-Acting-User")))
        return httpx.Response(200, json={})


class FakeServices:
    """Routes each request to the fake service of its host."""

    def __init__(self):
        self.hosts: dict[str, FakeService] = {}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        fake = self.hosts.get(request.url.host)
        if fake is None:
            return httpx.Response(404, json={})
        return fake(request)

    def calls(self, path_prefix: str = "") -> list[httpx.Request]:
        """Return the requests that carry an acting user, so the probes do not count."""
        return [
            r for r in self.requests if "X-LLM-Acting-User" in r.headers and r.url.path.startswith(path_prefix)
        ]


class FakeStore:
    def __init__(self, names=None):
        self.names = list(names or [])
        self.error: Exception | None = None

    def load(self):
        return list(self.names)

    def save(self, names):
        if self.error:
            raise self.error
        self.names = list(names)


@pytest.fixture
def fakes():
    fakes = FakeServices()
    fakes.hosts["svc"] = FakeService()
    fakes.hosts["svc-two"] = FakeService(admins=("bob",), users=("alice",))
    return fakes


@pytest.fixture
def store():
    return FakeStore()


@pytest.fixture
def registry(fakes, store):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    return Registry(http, ["svc", "svc-two"], store, lambda: [])


@pytest.fixture
def service(fakes):
    return fakes.hosts["svc"]


async def _refresh_done(registry):
    async with registry._lock:
        pass


@pytest.fixture
def make_client(fakes, registry):
    clients = []

    def make(owner="alice"):
        http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
        test_client = TestClient(create_app(registry, owner, http))
        test_client.__enter__()
        clients.append(test_client)
        test_client.portal.call(registry.wait_ready, 10.0)
        # The first refresh publishes the configured names before the listing ends. Wait for its end.
        test_client.portal.call(_refresh_done, registry)
        fakes.requests.clear()
        for fake in fakes.hosts.values():
            fake.requests.clear()
        return test_client

    yield make
    for test_client in clients:
        test_client.__exit__(None, None, None)


@pytest.fixture
def client(make_client):
    return make_client()


def test_health_calls_nothing(client, fakes):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.text == "OK"
    assert fakes.requests == []


def test_no_platform_user_is_401_and_calls_nothing(client, fakes):
    for path in ("/api/services", "/api/services/svc/status"):
        response = client.get(path)
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "missing_user"
    assert fakes.requests == []


def test_every_call_names_the_viewer(client, service):
    client.get("/api/services/svc/audit", headers=ALICE)
    assert service.requests[0].headers["X-LLM-Acting-User"] == "alice"


def test_list_for_an_admin(client):
    response = client.get("/api/services", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert body["user"] == "alice" and body["owner"] is True and body["listing"] == "ok"
    first, second = body["services"]
    assert first == {
        "name": "svc", "source": "configured", "state": "ok", "detail": None,
        "model": "org/model", "engine": "ready", "access": True, "admin": True,
    }
    assert (second["name"], second["access"], second["admin"]) == ("svc-two", True, False)


def test_list_for_a_user_with_access(make_client):
    body = make_client(owner="alice").get("/api/services", headers=BOB).json()
    assert body["owner"] is False
    assert [(s["access"], s["admin"]) for s in body["services"]] == [(True, False), (True, True)]


def test_list_for_a_user_without_access(client):
    body = client.get("/api/services", headers=CAROL).json()
    assert [(s["access"], s["admin"]) for s in body["services"]] == [(False, False), (False, False)]
    assert all(s["state"] == "ok" for s in body["services"])


def test_list_entries_without_state_ok_have_no_roles(client, fakes):
    fakes.hosts["svc"].caller_admin = False
    client.portal.call(client.app.state.registry.refresh)
    fakes.requests.clear()
    body = client.get("/api/services", headers=ALICE).json()
    first = body["services"][0]
    assert (first["state"], first["access"], first["admin"]) == ("no_access", False, False)
    assert first["detail"]
    assert [r.url.host for r in fakes.calls()] == ["svc-two"]


def test_list_reports_a_failed_listing(client, registry):
    registry.listing_state = "failed"
    assert client.get("/api/services", headers=ALICE).json()["listing"] == "failed"


def test_list_waits_for_the_first_probe(fakes, registry):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    with TestClient(create_app(registry, "alice", http)) as test_client:
        assert len(test_client.get("/api/services", headers=ALICE).json()["services"]) == 2


def test_roles_are_cached_for_30_seconds(client, fakes, monkeypatch):
    now = [100.0]
    monkeypatch.setattr(app_module, "_clock", lambda: now[0])
    client.get("/api/services", headers=ALICE)
    assert len(fakes.calls("/v1/service")) == 2
    now[0] += 29
    client.get("/api/services", headers=ALICE)
    assert len(fakes.calls("/v1/service")) == 2
    client.get("/api/services", headers=BOB)
    assert len(fakes.calls("/v1/service")) == 4
    now[0] += 2
    client.get("/api/services", headers=ALICE)
    assert len(fakes.calls("/v1/service")) == 6


def test_a_role_call_has_a_short_timeout_and_a_failure_names_the_service(client, fakes):
    fakes.hosts["svc"].error = httpx.ConnectError("refused")
    body = client.get("/api/services", headers=ALICE).json()
    first = body["services"][0]
    assert (first["access"], first["admin"], first["role_error"]) == (None, False, "The service did not answer.")
    assert body["services"][1]["access"] is True
    assert "role_error" not in body["services"][1]
    assert fakes.calls()[0].extensions["timeout"]["read"] == 3.0


def test_a_role_call_that_times_out_names_the_service_in_the_list_and_the_usage(client, fakes):
    fakes.hosts["svc"].error = httpx.ReadTimeout("slow")
    body = client.get("/api/services", headers=ALICE).json()
    first = body["services"][0]
    assert (first["access"], first["admin"], first["role_error"]) == (None, False, "The service did not answer.")
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    usage = client.get("/api/usage", headers=ALICE)
    assert usage.status_code == 200
    assert usage.json()["services"] == [
        {"name": "svc", "scope": "own", "ok": False, "error": "The service did not answer."},
        {"name": "svc-two", "scope": "own", "ok": True},
    ]


def test_a_role_answer_that_is_not_200_is_an_error_and_not_no_access(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/v1/service")] = (500, {})
    first = client.get("/api/services", headers=ALICE).json()["services"][0]
    assert (first["access"], first["role_error"]) == (None, "The service did not answer.")


def test_a_refused_acting_user_stays_no_access(client):
    first = client.get("/api/services", headers=CAROL).json()["services"][0]
    assert (first["access"], first["admin"]) == (False, False)
    assert "role_error" not in first


def test_a_failed_role_call_is_cached_for_5_seconds_only(client, fakes, monkeypatch):
    now = [100.0]
    monkeypatch.setattr(app_module, "_clock", lambda: now[0])
    fakes.hosts["svc"].error = httpx.ConnectError("refused")
    client.get("/api/services", headers=ALICE)
    assert len([r for r in fakes.calls("/v1/service") if r.url.host == "svc"]) == 1
    now[0] += 4
    client.get("/api/services", headers=ALICE)
    assert len([r for r in fakes.calls("/v1/service") if r.url.host == "svc"]) == 1
    now[0] += 2
    fakes.hosts["svc"].error = None
    first = client.get("/api/services", headers=ALICE).json()["services"][0]
    assert len([r for r in fakes.calls("/v1/service") if r.url.host == "svc"]) == 2
    assert first["access"] is True and "role_error" not in first


def test_a_role_timeout_logs_one_warning_without_a_traceback(client, fakes, monkeypatch, caplog):
    async def slow(request):
        await asyncio.sleep(5)
        return httpx.Response(200, json={})

    monkeypatch.setattr(app_module, "ROLE_TIMEOUT_SECONDS", 0.05)
    fakes.hosts["svc"] = slow
    with caplog.at_level(logging.WARNING, logger="llm_service.admin_app.app"):
        client.get("/api/services", headers=ALICE)
    records = [r for r in caplog.records if r.name == "llm_service.admin_app.app"]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING and records[0].exc_info is None
    assert "svc" in records[0].getMessage()


def test_post_is_for_the_owner_only(make_client, fakes, store):
    client = make_client(owner="alice")
    fakes.hosts["extra"] = FakeService()
    response = client.post("/api/services", json={"name": "extra"}, headers=BOB)
    assert response.status_code == 403
    error = response.json()["error"]
    assert (error["code"], error["type"]) == ("owner_required", "permission_error")
    assert fakes.requests == [] and store.names == []


def test_post_adds_and_probes_a_name(client, fakes, store):
    fakes.hosts["extra"] = FakeService()
    response = client.post("/api/services", json={"name": "extra"}, headers=ALICE)
    assert response.status_code == 200
    assert response.json()["name"] == "extra" and response.json()["state"] == "ok"
    assert response.json()["source"] == "manual"
    assert store.names == ["extra"]
    assert [r.url.host for r in fakes.requests] == ["extra"]
    assert client.get("/api/services/extra/audit", headers=ALICE).status_code == 200


def test_an_empty_owner_matches_no_one(make_client):
    client = make_client(owner="")
    assert client.post("/api/services", json={"name": "extra"}, headers=ALICE).status_code == 403
    assert client.delete("/api/services/extra", headers=ALICE).status_code == 403
    assert client.get("/api/services", headers=ALICE).json()["owner"] is False


@pytest.mark.parametrize("body", [{"name": "Evil.example.com"}, {"name": ""}, {"name": 5}, {}, ["svc"], {"name": "a" * 64}])
def test_post_with_a_bad_name_is_400_and_calls_nothing(client, fakes, store, body):
    response = client.post("/api/services", json=body, headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_service"
    assert fakes.requests == [] and store.names == []


def test_post_needs_a_json_body(client, fakes):
    response = client.post("/api/services", content=b'{"name": "x"}', headers=ALICE | {"Content-Type": "text/plain"})
    assert response.status_code == 415
    response = client.post("/api/services", content=b"{nope", headers=ALICE | {"Content-Type": "application/json"})
    assert response.status_code == 400 and response.json()["error"]["code"] == "invalid_json"
    assert fakes.requests == []


def test_a_failed_store_is_502_on_post_and_delete(client, store):
    store.names = ["old"]
    store.error = OSError("blob down")
    response = client.post("/api/services", json={"name": "extra"}, headers=ALICE)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "store_unavailable"
    response = client.delete("/api/services/old", headers=ALICE)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "store_unavailable"


def test_delete_removes_a_manual_name(client, fakes, store):
    fakes.hosts["extra"] = FakeService()
    client.post("/api/services", json={"name": "extra"}, headers=ALICE)
    response = client.delete("/api/services/extra", headers=ALICE)
    assert response.status_code == 200
    assert store.names == []
    assert client.get("/api/services/extra/audit", headers=ALICE).status_code == 404


def test_delete_of_a_name_that_is_not_manual_is_404(client):
    response = client.delete("/api/services/svc", headers=ALICE)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_service"


def test_delete_with_a_bad_name_is_400(client):
    response = client.delete("/api/services/Evil.Name", headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_service"


def test_delete_is_for_the_owner_only(client):
    assert client.delete("/api/services/svc", headers=BOB).status_code == 403


def test_the_old_routes_are_gone(client, fakes):
    for path in ("/api/me", "/api/status", "/api/audit", "/api/audit/users"):
        assert client.get(path, headers=ALICE).status_code in (404, 405), path
    assert client.post("/api/model", json={}, headers=ALICE).status_code in (404, 405)
    assert fakes.requests == []


@pytest.mark.parametrize("name", ["nope", "evil.example.com", "Svc", "a" * 64])
def test_an_unknown_or_bad_name_calls_nothing(client, fakes, name):
    response = client.get(f"/api/services/{name}/status", headers=ALICE)
    assert response.status_code in (400, 404)
    assert response.json()["error"]["code"] in ("invalid_service", "unknown_service")
    assert fakes.requests == []


def test_an_unknown_name_is_404_and_a_bad_name_is_400(client):
    assert client.get("/api/services/nope/audit", headers=ALICE).json()["error"]["code"] == "unknown_service"
    assert client.get("/api/services/evil.example.com/audit", headers=ALICE).json()["error"]["code"] == "invalid_service"
    assert client.get("/api/services/nope/audit", headers=ALICE).status_code == 404


def test_a_name_with_state_no_access_is_404_and_calls_nothing(client, fakes):
    fakes.hosts["svc"].caller_admin = False
    client.portal.call(client.app.state.registry.refresh)
    fakes.requests.clear()
    response = client.get("/api/services/svc/status", headers=ALICE)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_service"
    assert fakes.requests == []


def test_each_route_reaches_the_host_and_path_of_its_service(client, fakes):
    fakes.hosts["svc-two"].answers[("POST", "/admin/model")] = (202, {"state": "loading"})
    client.get("/api/services/svc-two/models", headers=ALICE)
    client.post("/api/services/svc-two/model", json={"model": "m"}, headers=ALICE)
    client.delete("/api/services/svc-two/models/org/name?revision=v1", headers=ALICE)
    client.get("/api/services/svc-two/usage/carol?since=a&until=b&evil=1", headers=ALICE)
    client.get("/api/services/svc-two/audit?limit=5&evil=1", headers=ALICE)
    client.get("/api/services/svc-two/audit/users", headers=ALICE)
    client.get("/api/services/svc-two/audit/records/req-1?day=2026-11-11", headers=ALICE)
    sent = fakes.calls()
    assert {r.url.host for r in sent} == {"svc-two"}
    assert [(r.method, r.url.path) for r in sent] == [
        ("GET", "/v1/models"),
        ("POST", "/admin/model"),
        ("DELETE", "/admin/models/org/name"),
        ("GET", "/admin/usage/carol"),
        ("GET", "/admin/audit"),
        ("GET", "/admin/audit/users"),
        ("GET", "/admin/audit/records/req-1"),
    ]
    assert all(r.headers["X-LLM-Acting-User"] == "alice" and r.headers["Connection"] == "close" for r in sent)
    assert dict(sent[2].url.params) == {"revision": "v1"}
    assert dict(sent[3].url.params) == {"since": "a", "until": "b"}
    assert dict(sent[4].url.params) == {"limit": "5"}
    assert dict(sent[6].url.params) == {"day": "2026-11-11"}
    assert json.loads(sent[1].content) == {"model": "m"}
    assert fakes.hosts["svc"].requests == []


def test_status_joins_engine_and_models(client, service):
    service.answers[("GET", "/admin/model")] = (200, {"state": "ready"})
    service.answers[("GET", "/v1/models")] = (200, {"object": "list", "data": [{"id": "org/name"}]})
    response = client.get("/api/services/svc/status", headers=ALICE)
    assert response.json() == {"engine": {"state": "ready"}, "models": [{"id": "org/name"}]}


def test_status_returns_the_first_error(client, service):
    service.answers[("GET", "/admin/model")] = (403, ERROR_403)
    response = client.get("/api/services/svc/status", headers=ALICE)
    assert response.status_code == 403
    assert response.json() == ERROR_403
    assert len(service.requests) == 1


def test_models_passes_the_list_through(client, service):
    body = {"object": "list", "data": [{"id": "org/name"}]}
    service.answers[("GET", "/v1/models")] = (200, body)
    response = client.get("/api/services/svc/models", headers=BOB)
    assert response.json() == body
    assert service.requests[0].headers["X-LLM-Acting-User"] == "bob"


def test_switch_passes_body_and_status_through(client, service):
    service.answers[("POST", "/admin/model")] = (202, {"state": "loading"})
    response = client.post("/api/services/svc/model", json={"model": "org/name"}, headers=ALICE)
    assert response.status_code == 202
    assert response.json() == {"state": "loading"}
    assert json.loads(service.requests[0].content) == {"model": "org/name"}
    error = {"error": {"message": "busy", "type": "conflict", "code": "switch_in_progress"}}
    service.answers[("POST", "/admin/model")] = (409, error)
    response = client.post("/api/services/svc/model", json={"model": "x"}, headers=ALICE)
    assert response.status_code == 409
    assert response.json() == error


def test_delete_keeps_the_model_path(client, service):
    service.answers[("DELETE", "/admin/models/org/name")] = (200, {"deleted": ["org/name"]})
    response = client.delete("/api/services/svc/models/org/name?revision=v1", headers=ALICE)
    assert response.json() == {"deleted": ["org/name"]}
    request = service.requests[0]
    assert request.method == "DELETE"
    assert request.url.path == "/admin/models/org/name"
    assert dict(request.url.params) == {"revision": "v1"}


def test_delete_without_revision_sends_no_param(client, service):
    client.delete("/api/services/svc/models/org/name", headers=ALICE)
    assert dict(service.requests[0].url.params) == {}


def test_service_error_bodies_pass_through(client, service):
    body = {"error": {"message": "bad", "type": "invalid_request_error", "code": "bad_request"}}
    service.answers[("POST", "/admin/model")] = (400, body)
    response = client.post("/api/services/svc/model", json={}, headers=ALICE)
    assert response.status_code == 400
    assert response.json() == body


def test_unreachable_service_is_502(client, service):
    service.error = httpx.ConnectError("refused")
    response = client.get("/api/services/svc/audit", headers=ALICE)
    assert response.status_code == 502
    error = response.json()["error"]
    assert error["code"] == "service_unreachable"
    assert error["type"] == "server_error"
    assert "svc" not in error["message"]


def test_non_json_service_body_is_502(client, service):
    service.raw = b"<html>not json</html>"
    response = client.get("/api/services/svc/audit", headers=ALICE)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"


def test_index_and_static_are_served(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert client.get("/static/index.html").status_code == 200


def test_the_page_is_not_cached_without_a_check(client):
    assert client.get("/").headers["cache-control"] == "no-cache"


def test_the_static_files_are_not_cached_without_a_check(client):
    for path in ("/static/app.js", "/static/app.css", "/static/index.html"):
        assert client.get(path).headers["cache-control"] == "no-cache", path


def test_page_says_that_a_service_did_not_answer():
    text = (STATIC_DIR / "app.js").read_text()
    assert "The service did not answer." in text
    assert "entry.access === null" in text


def test_import_does_not_load_the_gateway_app():
    code = "import sys, llm_service.admin_app.app; assert 'llm_service.gateway.app' not in sys.modules"
    subprocess.run([sys.executable, "-W", "error", "-c", code], check=True)


@pytest.mark.parametrize("model_id", ["x/../../model", "./x", "a//b", "..", "x/."])
def test_delete_rejects_dot_and_empty_segments(client, service, model_id):
    # The test client removes literal dot segments, so send them percent-encoded.
    response = client.delete("/api/services/svc/models/" + model_id.replace(".", "%2e"), headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_model"
    assert service.requests == []


def test_delete_encodes_query_and_fragment_characters(client, service):
    client.delete("/api/services/svc/models/org/na%3Fme%23x", headers=ALICE)
    assert service.requests[0].url.raw_path.decode() == "/admin/models/org/na%3Fme%23x"


@pytest.mark.parametrize("user", ["..", "."])
def test_usage_user_rejects_dot_segments(client, service, user):
    response = client.get("/api/services/svc/usage/" + user.replace(".", "%2e"), headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_user"
    assert service.requests == []


def test_viewer_with_control_character_is_401(client, service):
    header = {"X-Datatailr-User": json.dumps({"name": "ali\nce"})}
    response = client.get("/api/services/svc/audit", headers=header)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_user"
    assert service.requests == []


def test_viewer_that_is_too_long_is_401(client, service):
    response = client.get("/api/services/svc/audit", headers={"X-Datatailr-User": "a" * 257})
    assert response.status_code == 401
    assert service.requests == []


def test_json_form_header_works(client, service):
    header = {"X-Datatailr-User": '{"name": "alice"}'}
    assert client.get("/api/services/svc/audit", headers=header).status_code == 200
    assert service.requests[0].headers["X-LLM-Acting-User"] == "alice"


def test_status_with_a_body_that_is_not_an_object_is_502(client, service):
    service.answers[("GET", "/v1/models")] = (200, ["not", "an", "object"])
    response = client.get("/api/services/svc/status", headers=ALICE)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"


def test_page_references_only_relative_urls(client):
    page = client.get("/").text
    assert "http://" not in page
    assert "https://" not in page
    links = re.findall(r'\b(?:src|href)\s*=\s*["\']([^"\']*)["\']', page)
    assert "static/app.js" in links
    assert all(not link.startswith("/") for link in links)


def test_static_assets_are_served(client):
    for path in ("static/app.js", "static/app.css", "static/vendor/chart.umd.js"):
        response = client.get("/" + path)
        assert response.status_code == 200, path
        assert response.content, path


def test_page_has_no_html_sinks():
    for name in ("app.js", "index.html"):
        text = (STATIC_DIR / name).read_text()
        for sink in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function"):
            assert sink not in text, (name, sink)


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_page_script_parses():
    subprocess.run(["node", "--check", str(STATIC_DIR / "app.js")], check=True, capture_output=True)


PAGE_IDS = (
    "service-picker", "services-toggle", "services-panel", "services-body", "services-listing",
    "service-add", "service-add-name", "no-access", "no-access-list", "usage-note",
    "services-usage-body", "audit-pick", "audit-content", "models-pick", "models-content",
)


@pytest.mark.parametrize("element_id", PAGE_IDS)
def test_page_has_the_elements_of_many_services(element_id):
    assert f'id="{element_id}"' in (STATIC_DIR / "index.html").read_text()


def test_page_calls_no_removed_route():
    text = (STATIC_DIR / "app.js").read_text()
    assert not re.search(r"['`]api/(?:me|status|model|models|audit)\b", text)
    assert "api/usage/" not in text


def test_page_offers_the_names_of_the_next_task():
    text = (STATIC_DIR / "app.js").read_text()
    for name in ("function servicePath(", "function currentRole(", "service: null", "services: null"):
        assert name in text, name


def test_page_drops_late_answers_and_errors_of_a_load():
    text = (STATIC_DIR / "app.js").read_text()
    assert "if (error instanceof StaleAnswer) return false;" in text
    for name in ("usage", "detail", "audit", "status", "limits", "limitsMe"):
        assert f"current('{name}', call, service," in text, name


@pytest.mark.parametrize("name", ["app.js", "index.html", "app.css"])
def test_page_prose_follows_the_rules(name):
    text = (STATIC_DIR / name).read_text()
    assert not re.search(r"\b(should|would|may|might|could)\b", text, re.IGNORECASE)
    assert "n't" not in text and "—" not in text


CONNECT_IDS = (
    "tab-connect", "panel-connect", "connect-pick", "connect-content", "conn-internal", "conn-outside",
    "conn-outside-note", "conn-model", "conn-engine", "example-curl", "example-python", "example-outside",
    "example-chat", "chat-form", "chat-system", "chat-prompt", "chat-model", "chat-temperature",
    "chat-max-tokens", "chat-send", "chat-stop", "chat-clear", "chat-log", "chat-status", "chat-stats",
)


@pytest.mark.parametrize("element_id", CONNECT_IDS)
def test_page_has_the_elements_of_the_connect_tab(element_id):
    assert f'id="{element_id}"' in (STATIC_DIR / "index.html").read_text()


def test_connect_tab_comes_after_usage():
    page = (STATIC_DIR / "index.html").read_text()
    tabs = re.findall(r'role="tab" id="tab-(\w+)"', page)
    assert tabs == ["usage", "connect", "audit", "models", "limits"]
    assert "const TABS = ['usage', 'connect', 'audit', 'models', 'limits'];" in (STATIC_DIR / "app.js").read_text()


@pytest.mark.parametrize(
    "element_id, attributes",
    [("chat-temperature", ('min="0"', 'max="2"', 'step="any"', 'value="0.7"')),
     ("chat-max-tokens", ('min="1"', 'max="4096"', 'step="1"', 'value="512"'))],
)
def test_chat_fields_have_the_limits_of_the_app(element_id, attributes):
    page = (STATIC_DIR / "index.html").read_text()
    tag = re.search(rf'<input[^>]*id="{element_id}"[^>]*>', page).group(0)
    for attribute in attributes:
        assert attribute in tag, attribute


def test_page_examples_have_no_token_value():
    text = (STATIC_DIR / "app.js").read_text() + (STATIC_DIR / "index.html").read_text()
    assert 'os.environ["LLM_TOKEN"]' in text
    assert 'api_key="unused"' in text
    # The cookie name is always followed by the end of a string, never by a value.
    after = re.findall(r"__datatailr_user_cookie__=(.?)", text)
    assert after and all(char in "\"'" for char in after)
    assert not re.search(r"LLM_TOKEN=\S", text)


def test_page_streams_the_answer_with_stop():
    text = (STATIC_DIR / "app.js").read_text()
    for name in ("new AbortController()", ".abort()", "getReader()", "'[DONE]'", "delta"):
        assert name in text, name


def test_page_cancels_the_reader_and_reports_a_stop_before_an_error_body():
    text = (STATIC_DIR / "app.js").read_text()
    body = text[text.index("async function streamChat("):text.index("function chatTurn(")]
    assert re.search(r"finally \{\s*(//[^\n]*\s*)?reader\.cancel\(\)", body)
    assert "if (signal.aborted)" in body
    assert body.index("if (signal.aborted)") < body.index("throw answerError(response, data);")
    assert "The page knows this URL only on the platform." in text
    assert "Known only on the platform" not in text


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_page_reads_split_events_and_builds_the_outside_url(tmp_path):
    # Load app.js with stubs for the browser, then call its pure helpers.
    harness = tmp_path / "harness.js"
    harness.write_text(
        "const vm = require('vm');\n"
        "const fs = require('fs');\n"
        "const context = {URL, TextDecoder, console, document: {addEventListener() {}},\n"
        "  location: {pathname: process.argv[3], origin: 'https://example.test'}};\n"
        "vm.createContext(context);\n"
        "vm.runInContext(fs.readFileSync(process.argv[2], 'utf8') +\n"
        "  '\\nthis.splitLines = splitLines; this.outsideBaseUrl = outsideBaseUrl;', context);\n"
        "let rest = '';\n"
        "const lines = [];\n"
        "for (const part of ['data: {\"a\"', ': 1}\\r\\ndata: [DO', 'NE]\\n']) {\n"
        "  const split = context.splitLines(rest + part);\n"
        "  rest = split.rest;\n"
        "  lines.push(...split.lines);\n"
        "}\n"
        "console.log(JSON.stringify({lines, rest, url: context.outsideBaseUrl('svc')}));\n"
    )

    def run(pathname):
        out = subprocess.run(["node", str(harness), str(STATIC_DIR / "app.js"), pathname],
                             check=True, capture_output=True, text=True).stdout
        return json.loads(out)

    platform = run("/job/dev/llm-service-admin/")
    assert platform["lines"] == ['data: {"a": 1}', "data: [DONE]"]
    assert platform["rest"] == ""
    assert platform["url"] == "https://example.test/job/dev/svc/v1"
    assert run("/job/dev/llm-service-admin")["url"] == "https://example.test/job/dev/svc/v1"
    for other in ("/", "/job/dev/", "/job/dev/llm-service-admin/extra/", "/apps/dev/llm-service-admin/",
                  "/x/job/dev/llm-service-admin/", "/job//llm-service-admin/"):
        assert run(other)["url"] is None, other


LIMITS_IDS = (
    "tab-limits", "panel-limits", "limits-pick", "limits-content", "limits-missing", "limits-cards", "limits-unloaded",
    "limits-default-form", "limits-default-requests_per_minute", "limits-default-tokens_per_day",
    "limits-default-parallel_requests", "limits-reload", "limits-body", "limits-user-form", "limits-user",
    "limits-user-requests_per_minute-mode", "limits-user-requests_per_minute",
    "limits-user-tokens_per_day-mode", "limits-user-tokens_per_day",
    "limits-user-parallel_requests-mode", "limits-user-parallel_requests", "limits-user-status",
    "remove-dialog", "remove-text", "remove-confirm",
    "my-limits", "my-limits-note", "my-limits-list", "my-requests_per_minute", "my-tokens_per_day",
    "my-parallel_requests", "my-limits-source",
)


@pytest.mark.parametrize("element_id", LIMITS_IDS)
def test_page_has_the_elements_of_the_limits(element_id):
    assert f'id="{element_id}"' in (STATIC_DIR / "index.html").read_text()


def test_limits_tab_is_for_an_admin_of_one_service():
    text = (STATIC_DIR / "app.js").read_text()
    assert "limits: {one: true, admin: true}" in text
    assert "limits: loadLimits" in text
    # The tab reads the limits only on a load or a click, never on a timer.
    body = text[text.index("// ---- limits"):text.index("// ---- start")]
    assert "setTimeout" not in body and "setInterval" not in body


@pytest.mark.parametrize("number_id", [f"limits-{form}-{name}" for form in ("default", "user")
                                       for name in ("requests_per_minute", "tokens_per_day", "parallel_requests")])
def test_limit_fields_take_whole_numbers_of_the_range(number_id):
    page = (STATIC_DIR / "index.html").read_text()
    tag = re.search(rf'<input[^>]*id="{number_id}"[^>]*>', page).group(0)
    for attribute in ('type="number"', 'min="1"', 'max="1000000000"', 'step="1"'):
        assert attribute in tag, attribute


def test_page_calls_the_five_limits_routes():
    text = (STATIC_DIR / "app.js").read_text()
    assert "servicePath('limits', service)" in text
    assert "servicePath('limits/me', service)" in text
    assert "servicePath('limits/default', service), {method: 'PUT'" in text
    assert "const userPath = servicePath(`limits/users/${encodeURIComponent(user)}`, service);" in text
    assert "api(userPath, {method: 'PUT'" in text
    assert "api(userPath, {method: 'DELETE'})" in text


def test_page_shows_the_effective_limits_of_the_service():
    text = (STATIC_DIR / "app.js").read_text()
    body = text[text.index("function showLimits("):text.index("function setChoice(")]
    assert "ownValue(effective, user)" in body
    assert "effectiveLimit" not in text  # the page no longer repeats the lookup order
    assert "$('limits-unloaded').hidden = answer.loaded !== false;" in body


def test_a_blank_user_name_in_the_limits_form_shows_a_message():
    text = (STATIC_DIR / "app.js").read_text()
    body = text[text.index("async function saveUserLimits("):text.index("function askRemove(")]
    assert "$('limits-user-status').textContent = 'Type a user name.';" in body
    assert "if (!service || !user) return;" not in body


def test_the_limits_texts_name_the_utc_day_and_the_selects_have_no_data_limit():
    page = (STATIC_DIR / "index.html").read_text()
    assert "Tokens per UTC day" not in page and "data-limit" not in page
    assert '<dt>Tokens per day (UTC)</dt>' in page
    assert '<th scope="col" class="num">Tokens per day (UTC)</th>' in page


def test_page_passes_retry_after_of_a_chat_error():
    text = (STATIC_DIR / "app.js").read_text()
    body = text[text.index("async function streamChat("):text.index("function chatTurn(")]
    assert "throw answerError(response, data);" in body
    assert "response.headers.get('Retry-After')" in text
    send = text[text.index("async function sendChat("):text.index("function stopChat(")]
    assert "waitText(error.retryAfter)" in send
    assert "loadMyLimits(service)" in send


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_page_limit_helpers(tmp_path):
    harness = tmp_path / "harness.js"
    harness.write_text(
        "const vm = require('vm');\n"
        "const fs = require('fs');\n"
        "const context = {URL, console, Intl, document: {addEventListener() {}},\n"
        "  location: {pathname: '/', origin: 'https://example.test'}};\n"
        "vm.createContext(context);\n"
        "vm.runInContext(fs.readFileSync(process.argv[2], 'utf8') + '\\nthis.h = {userLimitBody, defaultLimitBody,'\n"
        "  + ' limitCell, myLimitsNote, retryWait, waitText, limitsMissing, ApiError};', context);\n"
        "const h = context.h;\n"
        "const choice = (mode, value = '') => ({mode, value});\n"
        "const missing = (status, code) => h.limitsMissing(new h.ApiError('x', {status, code}));\n"
        "console.log(JSON.stringify({\n"
        "  allDefault: h.userLimitBody({requests_per_minute: choice('default'), tokens_per_day: choice('default'),\n"
        "                               parallel_requests: choice('default')}),\n"
        "  mixed: h.userLimitBody({requests_per_minute: choice('number', '60'), tokens_per_day: choice('none'),\n"
        "                          parallel_requests: choice('default', '9')}),\n"
        "  defaults: h.defaultLimitBody({requests_per_minute: '5', tokens_per_day: '', parallel_requests: ' 2 '}),\n"
        "  own: h.limitCell({limits: {tokens_per_day: null}, source: 'user'}, {tokens_per_day: null}, 'tokens_per_day'),\n"
        "  leftOut: h.limitCell({limits: {requests_per_minute: 7}, source: 'user'}, {tokens_per_day: 3},\n"
        "                       'requests_per_minute'),\n"
        "  fromDefault: h.limitCell({limits: {tokens_per_day: 9}, source: 'default'}, undefined, 'tokens_per_day'),\n"
        "  admin: h.limitCell({limits: {parallel_requests: null}, source: 'admin'}, undefined, 'parallel_requests'),\n"
        "  none: h.limitCell({limits: {parallel_requests: null}, source: 'none'}, undefined, 'parallel_requests'),\n"
        "  unknown: h.limitCell(undefined, undefined, 'parallel_requests'),\n"
        "  notes: [h.myLimitsNote({limits: {tokens_per_day: 5}, loaded: false}),\n"
        "          h.myLimitsNote({limits: {tokens_per_day: null}, loaded: true}),\n"
        "          h.myLimitsNote({limits: {tokens_per_day: 5}, loaded: true}),\n"
        "          h.myLimitsNote({limits: {tokens_per_day: 5}})],\n"
        "  waits: ['17', '0', '', null, 'soon', '1.5', '123456'].map(h.retryWait),\n"
        "  texts: [h.waitText(1), h.waitText(17), h.waitText(null), h.waitText(0)],\n"
        "  missing: [missing(404, 'not_found'), missing(404, null), missing(404, 'unknown_service'),\n"
        "            missing(502, 'service_unreachable'), h.limitsMissing(new Error('x'))],\n"
        "}));\n"
    )
    out = subprocess.run(["node", str(harness), str(STATIC_DIR / "app.js")],
                         check=True, capture_output=True, text=True).stdout
    result = json.loads(out)
    assert result["allDefault"] is None
    assert result["mixed"] == {"requests_per_minute": 60, "tokens_per_day": None}
    assert result["defaults"] == {"requests_per_minute": 5, "tokens_per_day": None, "parallel_requests": 2}
    assert result["own"] == {"value": None, "muted": False, "title": None}
    assert result["leftOut"] == {"value": 7, "muted": True, "title": "From the default"}
    assert result["fromDefault"] == {"value": 9, "muted": True, "title": "From the default"}
    assert result["admin"] == {"value": None, "muted": False, "title": "Admin of the service"}
    assert result["none"] == {"value": None, "muted": False, "title": None}
    assert result["unknown"] == {"muted": False, "title": None}  # no value: the row shows "-"
    unloaded = "The service did not read the limits file. No limit applies now. The service tries again."
    assert result["notes"] == [unloaded, "You have no limit on this service.", "", ""]
    assert result["waits"] == [17, 0, None, None, None, None, None]
    assert result["texts"] == ["Try again in 1 second.", "Try again in 17 seconds.", "", ""]
    assert result["missing"] == [True, True, False, False, False]


def _timeout(request: httpx.Request) -> float:
    return request.extensions["timeout"]["read"]


def test_switch_and_delete_wait_longer_than_other_calls(client, service):
    client.post("/api/services/svc/model", json={"model": "org/name"}, headers=ALICE)
    client.delete("/api/services/svc/models/org/name", headers=ALICE)
    client.get("/api/services/svc/audit", headers=ALICE)
    client.get("/api/services/svc/status", headers=ALICE)
    switch, delete, audit, *status = service.requests
    assert _timeout(switch) == 300 and _timeout(delete) == 300
    assert _timeout(audit) == 30 and all(_timeout(r) == 30 for r in status)


@pytest.mark.parametrize(
    "method, path, kwargs",
    [("POST", "/api/services/svc/model", {"json": {"model": "org/name"}}), ("DELETE", "/api/services/svc/models/org/name", {})],
)
def test_a_slow_switch_or_delete_says_that_it_can_still_finish(client, service, method, path, kwargs):
    service.error = httpx.ReadTimeout("slow")
    response = client.request(method, path, headers=ALICE, **kwargs)
    assert response.status_code == 502
    error = response.json()["error"]
    assert error["code"] == "service_unreachable"
    assert "in time" in error["message"] and "can still finish" in error["message"]


def test_a_slow_read_keeps_the_plain_message(client, service):
    service.error = httpx.ReadTimeout("slow")
    response = client.get("/api/services/svc/audit", headers=ALICE)
    assert response.status_code == 502
    assert "can still finish" not in response.json()["error"]["message"]


def test_a_refused_connection_on_a_switch_keeps_the_plain_message(client, service):
    service.error = httpx.ConnectError("refused")
    response = client.post("/api/services/svc/model", json={"model": "org/name"}, headers=ALICE)
    assert response.status_code == 502
    assert "can still finish" not in response.json()["error"]["message"]


@pytest.mark.parametrize("name", ["   ", "\t"])
def test_a_blank_viewer_name_is_401(client, service, name):
    response = client.get("/api/services/svc/audit", headers={"X-Datatailr-User": json.dumps({"name": name})})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_user"
    assert service.requests == []


def test_the_viewer_name_is_sent_stripped(client, service):
    client.get("/api/services/svc/audit", headers={"X-Datatailr-User": json.dumps({"name": "  alice "})})
    assert service.requests[0].headers["X-LLM-Acting-User"] == "alice"
    body = client.get("/api/services", headers={"X-Datatailr-User": json.dumps({"name": " bob "})}).json()
    assert body["user"] == "bob"


def test_a_viewer_name_that_is_not_ascii_is_401(client, service):
    response = client.get("/api/services/svc/audit", headers={"X-Datatailr-User": json.dumps({"name": "alïce"})})
    assert response.status_code == 401
    error = response.json()["error"]
    assert error["code"] == "missing_user"
    assert "not usable for the admin app" in error["message"]
    assert service.requests == []


@pytest.mark.parametrize(
    "headers",
    [{}, {"Content-Type": "text/plain"}, {"Content-Type": "application/x-www-form-urlencoded"},
     {"Content-Type": "multipart/form-data; boundary=x"}],
)
def test_switch_needs_a_json_content_type(client, service, headers):
    response = client.post("/api/services/svc/model", content=b'{"model": "org/name"}', headers=ALICE | headers)
    assert response.status_code == 415
    error = response.json()["error"]
    assert error["code"] == "unsupported_media_type" and error["type"] == "invalid_request_error"
    assert service.requests == []


@pytest.mark.parametrize("content_type", ["application/json", "application/json; charset=utf-8", "Application/JSON"])
def test_switch_accepts_a_json_content_type_with_parameters(client, service, content_type):
    response = client.post("/api/services/svc/model", content=b'{"model": "org/name"}',
                           headers=ALICE | {"Content-Type": content_type})
    assert response.status_code == 200
    assert json.loads(service.requests[0].content) == {"model": "org/name"}


def test_switch_with_a_body_that_is_not_json_is_400(client, service):
    response = client.post("/api/services/svc/model", content=b"{nope", headers=ALICE | {"Content-Type": "application/json"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"
    assert service.requests == []


def test_page_sends_a_json_content_type_on_its_post():
    text = (STATIC_DIR / "app.js").read_text()
    posts = re.findall(r"method: 'POST'[^}]*}", text)
    assert posts and all("'Content-Type': 'application/json'" in post for post in posts)


def test_every_call_asks_for_a_new_connection(client, service):
    # The auth proxy of the platform signs only the first request of a connection.
    client.get("/api/services/svc/status", headers=ALICE)
    assert [request.headers["Connection"] for request in service.requests] == ["close", "close"]


def test_audit_record_forwards_the_day_for_the_viewer(client, service):
    record = {"request_id": "req-1", "prompt": "hi"}
    service.answers[("GET", "/admin/audit/records/req-1")] = (200, record)
    response = client.get("/api/services/svc/audit/records/req-1?day=2026-11-11&evil=1", headers=ALICE)
    assert response.status_code == 200
    assert response.json() == record
    assert len(service.requests) == 1
    sent = service.requests[0]
    assert sent.headers["X-LLM-Acting-User"] == "alice"
    assert dict(sent.url.params) == {"day": "2026-11-11"}


def test_audit_record_without_day_sends_no_param(client, service):
    client.get("/api/services/svc/audit/records/req-1", headers=ALICE)
    assert dict(service.requests[0].url.params) == {}


@pytest.mark.parametrize("request_id", ["..", ".", "a/b", "a%2Fb", "x/../y"])
def test_audit_record_rejects_unusable_ids(client, service, request_id):
    # The test client removes literal dot segments, so send them percent-encoded.
    response = client.get("/api/services/svc/audit/records/" + request_id.replace(".", "%2e"), headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_record"
    assert service.requests == []


def test_audit_record_rejects_an_empty_id(client, service):
    response = client.get("/api/services/svc/audit/records/", headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_record"
    assert service.requests == []


def test_audit_record_quotes_the_id_as_one_segment(client, service):
    client.get("/api/services/svc/audit/records/re%3Fq%23%20%25", headers=ALICE)
    assert service.requests[0].url.raw_path.decode() == "/admin/audit/records/re%3Fq%23%20%25"


@pytest.mark.parametrize(
    ("status", "code"), [(404, "record_not_found"), (502, "prompt_unreadable"), (403, "admin_required")]
)
def test_audit_record_passes_errors_through(client, service, status, code):
    body = {"error": {"message": "no", "type": "x", "code": code}}
    service.answers[("GET", "/admin/audit/records/req-1")] = (status, body)
    response = client.get("/api/services/svc/audit/records/req-1?day=2026-11-11", headers=ALICE)
    assert response.status_code == status
    assert response.json() == body


def test_a_name_with_a_slash_reaches_no_service(client, fakes):
    assert client.get("/api/services/x%2Fy/status", headers=ALICE).status_code == 404
    assert fakes.requests == []


def test_default_app_reads_the_environment():
    env = {"LLM_SERVICES": " svc, Bad.Name,svc-two ,,x y", "DATATAILR_USER": "alice", "DATATAILR_JOB_ENVIRONMENT": "dev"}
    built = default_app(env)
    assert built.state.registry._configured == ["svc", "svc-two"]
    assert built.state.registry._http.follow_redirects is False


def test_default_app_without_variables_has_no_services():
    # No lifespan here. A lifespan starts the real store and the real platform listing.
    built = default_app({})
    assert built.state.registry._configured == []
    assert built.state.owner == ""


def test_importing_the_app_makes_no_blob_and_no_subprocess_call():
    code = (
        "import subprocess, sys, types\n"
        "def boom(*a, **k): raise AssertionError('call at import')\n"
        "subprocess.run = subprocess.Popen = boom\n"
        "stub = types.ModuleType('datatailr')\n"
        "stub.__getattr__ = boom\n"
        "sys.modules['datatailr'] = stub\n"
        "import llm_service.admin_app.app\n"
    )
    subprocess.run([sys.executable, "-W", "error", "-c", code], check=True)


def test_a_slow_service_does_not_hold_the_list(client, fakes, monkeypatch):
    async def slow(request):
        await asyncio.sleep(5)
        return httpx.Response(200, json={})

    monkeypatch.setattr(app_module, "ROLE_TIMEOUT_SECONDS", 0.05)
    fakes.hosts["svc"] = slow
    started = time.monotonic()
    body = client.get("/api/services", headers=ALICE).json()
    assert time.monotonic() - started < 2
    assert (body["services"][0]["access"], body["services"][0]["admin"]) == (None, False)
    assert body["services"][1]["access"] is True


def test_an_unexpected_error_in_one_role_call_names_the_service(client, fakes, caplog):
    fakes.hosts["svc"].error = RuntimeError("boom")
    with caplog.at_level(logging.ERROR, logger="llm_service.admin_app.app"):
        response = client.get("/api/services", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert (body["services"][0]["access"], body["services"][0]["admin"]) == (None, False)
    assert body["services"][0]["role_error"] == "The service did not answer."
    assert body["services"][1]["access"] is True
    assert any(r.exc_info for r in caplog.records if r.name == "llm_service.admin_app.app")


def test_a_failed_store_is_logged(client, store, caplog):
    store.error = OSError("blob down")
    with caplog.at_level(logging.ERROR, logger="llm_service.admin_app.app"):
        client.post("/api/services", json={"name": "extra"}, headers=ALICE)
        client.delete("/api/services/svc", headers=ALICE)
    assert len([r for r in caplog.records if r.name == "llm_service.admin_app.app"]) >= 1


def test_without_a_refresh_task_the_list_does_not_wait(fakes, registry):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    with TestClient(create_app(registry, "alice", http, refresh_interval=None)) as test_client:
        started = time.monotonic()
        response = test_client.get("/api/services", headers=ALICE)
        assert response.status_code == 200 and response.json()["services"] == []
        assert time.monotonic() - started < 2


def test_the_refresh_task_is_cancelled_at_the_end(fakes, registry, monkeypatch):
    seen = []

    async def run(interval):
        seen.append(("start", interval))
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            seen.append("cancelled")
            raise

    monkeypatch.setattr(registry, "run", run)
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    with TestClient(create_app(registry, "alice", http, refresh_interval=7.0)) as test_client:
        test_client.get("/health")
        assert seen == [("start", 7.0)]
    assert seen == [("start", 7.0), "cancelled"]


def test_the_lifespan_runs_and_stops_the_registry(fakes, registry):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    with TestClient(create_app(registry, "alice", http, refresh_interval=None)) as test_client:
        assert registry.entries() == []
        assert test_client.get("/health").status_code == 200
    with TestClient(create_app(registry, "alice", http)) as test_client:
        test_client.portal.call(registry.wait_ready, 10.0)
        assert len(registry.entries()) == 2


def _usage_row(key_name, key, requests=1, total=30):
    return {
        key_name: key,
        "requests": requests,
        "failed_requests": 0,
        "prompt_tokens": 10,
        "completion_tokens": 20,
        "total_tokens": total,
        "last_request": "2026-01-01T00:00:00Z",
    }


def _me_detail(user, total=40):
    return {
        "user": user,
        "totals": _usage_row("user", user, requests=4, total=total),
        "by_model": [_usage_row("model", "m1", requests=3, total=30), _usage_row("model", "m2", total=10)],
        "by_day": [_usage_row("day", "2026-01-02", requests=3, total=30), _usage_row("day", "2026-01-01", total=10)],
    }


def _usage_calls(fakes):
    return [r for r in fakes.calls() if r.url.path in ("/admin/usage", "/v1/usage/me")]


def test_usage_merges_two_admin_services(client, fakes):
    # bob is admin of svc-two. Make alice admin of both.
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": [_usage_row("user", "alice", total=100)]})
    fakes.hosts["svc-two"].answers[("GET", "/admin/usage")] = (
        200,
        {"data": [_usage_row("user", "alice", total=50), _usage_row("user", "bob", total=7)]},
    )
    client.portal.call(client.app.state.registry.refresh)
    response = client.get("/api/usage?since=2026-01-01&until=2026-01-31&user=&group_by=user", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert [r["user"] for r in body["data"]] == ["alice", "bob"]
    assert body["data"][0]["total_tokens"] == 150
    assert body["services"] == [
        {"name": "svc", "scope": "all", "ok": True},
        {"name": "svc-two", "scope": "all", "ok": True},
    ]
    sent = _usage_calls(fakes)
    assert {r.url.host for r in sent} == {"svc", "svc-two"}
    assert all(r.url.params["since"] == "2026-01-01" for r in sent)
    assert all(r.url.params["group_by"] == "user" for r in sent)
    assert all(r.headers["Connection"] == "close" for r in sent)


def test_usage_uses_own_numbers_for_a_viewer_who_is_not_admin(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": [_usage_row("user", "alice", total=100)]})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    response = client.get("/api/usage", headers=ALICE)
    body = response.json()
    assert [s["scope"] for s in body["services"]] == ["all", "own"]
    assert body["data"][0]["total_tokens"] == 140
    by_path = {r.url.host: r.url.path for r in _usage_calls(fakes)}
    assert by_path == {"svc": "/admin/usage", "svc-two": "/v1/usage/me"}


def test_usage_filters_apply_in_the_app_for_a_viewer_who_is_not_admin(client, fakes):
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    other = client.get("/api/usage?services=svc-two&user=bob", headers=ALICE).json()
    assert other["data"] == []
    assert other["services"] == [{"name": "svc-two", "scope": "own", "ok": True}]
    by_model = client.get("/api/usage?services=svc-two&group_by=model&model=m2", headers=ALICE).json()
    assert [r["model"] for r in by_model["data"]] == ["m2"]
    by_user = client.get("/api/usage?services=svc-two&model=m2", headers=ALICE).json()
    assert [(r["user"], r["total_tokens"]) for r in by_user["data"]] == [("alice", 10)]
    by_service = client.get("/api/usage?services=svc-two&group_by=service&model=m2", headers=ALICE).json()
    assert [(r["service"], r["total_tokens"]) for r in by_service["data"]] == [("svc-two", 10)]


def test_usage_group_by_service_calls_with_day_and_adds_up(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (
        200,
        {"data": [_usage_row("day", "2026-01-01", requests=2, total=10), _usage_row("day", "2026-01-02", total=20)]},
    )
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    body = client.get("/api/usage?group_by=service", headers=ALICE).json()
    by_service = {r["service"]: r for r in body["data"]}
    assert by_service["svc"]["requests"] == 3
    assert by_service["svc"]["total_tokens"] == 30
    assert by_service["svc-two"]["total_tokens"] == 40
    assert all(r.url.params["group_by"] == "day" for r in _usage_calls(fakes) if r.url.path == "/admin/usage")


def test_usage_a_failed_service_is_reported_and_the_rest_merge(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": [_usage_row("user", "alice")]})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (500, {"error": {}})
    response = client.get("/api/usage", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert len(body["data"]) == 1
    assert body["services"][0] == {"name": "svc", "scope": "all", "ok": True}
    failed = body["services"][1]
    assert failed["ok"] is False and failed["scope"] == "own"
    assert failed["error"] == "The LLM service did not give a valid answer."


@pytest.mark.parametrize("bad", [{"data": "x"}, {"data": [{"user": "a", "requests": "many"}]}, [], {"data": [1]}])
def test_usage_a_malformed_body_fails_that_service_only(client, fakes, bad):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, bad)
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    body = client.get("/api/usage", headers=ALICE).json()
    assert [s["ok"] for s in body["services"]] == [False, True]
    assert body["data"][0]["user"] == "alice"


def test_usage_all_failed_is_502(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (500, {})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (500, {})
    response = client.get("/api/usage", headers=ALICE)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"


def test_usage_an_unexpected_error_fails_one_service_only(client, fakes, monkeypatch):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": [_usage_row("user", "alice")]})
    original = app_module.ServiceClient.request

    async def broken(self, service, method, path, viewer, **options):
        if path == "/v1/usage/me":
            raise RuntimeError("boom")
        return await original(self, service, method, path, viewer, **options)

    monkeypatch.setattr(app_module.ServiceClient, "request", broken)
    body = client.get("/api/usage", headers=ALICE).json()
    assert [s["ok"] for s in body["services"]] == [True, False]


def test_usage_a_hanging_service_times_out_and_the_others_merge(client, fakes, monkeypatch):
    assert app_module.USAGE_TIMEOUT_SECONDS == 10.0
    monkeypatch.setattr(app_module, "USAGE_TIMEOUT_SECONDS", 0.05)
    original = fakes.hosts["svc"]

    async def hang(request):
        if request.url.path == "/admin/usage":
            await asyncio.sleep(5)
        return original(request)

    fakes.hosts["svc"] = hang
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    started = time.monotonic()
    response = client.get("/api/usage", headers=ALICE)
    assert time.monotonic() - started < 2
    assert response.status_code == 200
    body = response.json()
    assert body["services"] == [
        {"name": "svc", "scope": "all", "ok": False, "error": "The LLM service did not answer in time."},
        {"name": "svc-two", "scope": "own", "ok": True},
    ]
    assert [(r["user"], r["total_tokens"]) for r in body["data"]] == [("alice", 40)]


def test_usage_sends_the_filters_to_an_admin_service(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": []})
    query = "services=svc&since=2026-01-01&until=2026-01-31&model=org/model&user=bob&group_by=model"
    assert client.get(f"/api/usage?{query}", headers=ALICE).status_code == 200
    (sent,) = _usage_calls(fakes)
    assert dict(sent.url.params) == {
        "since": "2026-01-01", "until": "2026-01-31", "model": "org/model", "user": "bob", "group_by": "model",
    }


@pytest.mark.parametrize(
    "name, value", [("since", "2026-02-30"), ("until", "2026-1-01"), ("since", "yesterday"),
                    ("until", "2026-01-01T00:00:00Z"), ("since", "20260101")],
)
def test_usage_a_bad_day_is_400_and_calls_nothing(client, fakes, name, value):
    response = client.get("/api/usage", params={name: value}, headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_time"
    assert response.json()["error"]["type"] == "invalid_request_error"
    assert fakes.requests == []


def test_usage_services_limits_the_calls(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": []})
    body = client.get("/api/usage?services=svc", headers=ALICE).json()
    assert [s["name"] for s in body["services"]] == ["svc"]
    assert {r.url.host for r in _usage_calls(fakes)} == {"svc"}


@pytest.mark.parametrize("value", ["svc,Bad", "nope", "svc,,svc-two", "svc,evil.example.com", "a" * 64])
def test_usage_a_bad_or_unknown_service_is_400_and_calls_nothing(client, fakes, value):
    response = client.get("/api/usage", params={"services": value}, headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_service"
    assert fakes.requests == []


@pytest.mark.parametrize("value", ["service,", "total", "USER", ""])
def test_usage_a_bad_group_by_is_400(client, fakes, value):
    response = client.get("/api/usage", params={"group_by": value}, headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_group_by"
    assert fakes.requests == []


def test_usage_without_access_makes_no_usage_call(client, fakes):
    fakes.hosts["svc"].users.clear()
    fakes.hosts["svc"].admins.clear()
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    body = client.get("/api/usage", headers=ALICE).json()
    assert [s["name"] for s in body["services"]] == ["svc-two"]
    assert {r.url.host for r in _usage_calls(fakes)} == {"svc-two"}
    # A requested service without access is left out and has no error.
    named = client.get("/api/usage?services=svc,svc-two", headers=ALICE)
    assert named.status_code == 200
    assert [s["name"] for s in named.json()["services"]] == ["svc-two"]


def test_usage_without_access_to_any_service_is_200_and_empty(client, fakes):
    response = client.get("/api/usage", headers={"X-Datatailr-User": "carol"})
    assert response.status_code == 200
    assert response.json() == {"data": [], "services": []}
    assert _usage_calls(fakes) == []


def test_usage_needs_a_platform_user(client, fakes):
    assert client.get("/api/usage").status_code == 401
    assert fakes.requests == []


def test_usage_keeps_the_registry_order_in_services(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": []})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    body = client.get("/api/usage?services=svc-two,svc", headers=ALICE).json()
    assert [s["name"] for s in body["services"]] == ["svc", "svc-two"]


def test_usage_a_malformed_last_request_fails_that_service_only(client, fakes):
    bad = _usage_row("user", "alice")
    bad["last_request"] = 12
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": [bad]})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    response = client.get("/api/usage", headers=ALICE)
    assert response.status_code == 200
    assert [s["ok"] for s in response.json()["services"]] == [False, True]


def test_usage_a_malformed_body_is_logged(client, fakes, caplog):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": "x"})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    with caplog.at_level(logging.WARNING):
        client.get("/api/usage", headers=ALICE)
    assert any(r.levelno == logging.WARNING and "svc" in r.getMessage() for r in caplog.records)


def test_usage_model_filter_by_day_fails_a_service_of_a_viewer_who_is_not_admin(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": [_usage_row("day", "2026-01-01")]})
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    response = client.get("/api/usage?group_by=day&model=m1", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert body["services"][0] == {"name": "svc", "scope": "all", "ok": True}
    assert body["services"][1] == {
        "name": "svc-two",
        "scope": "own",
        "ok": False,
        "error": "The model filter by day needs an admin of the service.",
    }
    assert len(body["data"]) == 1
    only = client.get("/api/usage?group_by=day&model=m1&services=svc-two", headers=ALICE)
    assert only.status_code == 502
    assert [r for r in _usage_calls(fakes) if r.url.path == "/v1/usage/me"] == []


def test_usage_a_listed_service_that_is_not_ok_is_reported_and_not_called(client, fakes):
    fakes.hosts["svc"].caller_admin = False
    client.portal.call(client.app.state.registry.refresh)
    fakes.requests.clear()
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    entry = client.app.state.registry.entries()[0]
    assert entry.name == "svc" and entry.state != "ok"
    response = client.get("/api/usage?services=svc,svc-two", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert [s["name"] for s in body["services"]] == ["svc", "svc-two"]
    first = body["services"][0]
    assert first["ok"] is False and first["scope"] == "own" and entry.state in first["error"]
    assert {r.url.host for r in fakes.requests} == {"svc-two"}
    alone = client.get("/api/usage?services=svc", headers=ALICE)
    assert alone.status_code == 200
    assert alone.json()["data"] == []
    assert alone.json()["services"][0]["ok"] is False


def test_usage_an_unknown_valid_name_says_unknown(client):
    response = client.get("/api/usage?services=nope", headers=ALICE)
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "invalid_service"
    assert "unknown" in error["message"]
    bad = client.get("/api/usage?services=Bad", headers=ALICE).json()["error"]
    assert "a-z" in bad["message"]


def test_usage_of_all_services_names_a_service_that_is_not_ok(client, fakes):
    fakes.hosts["svc"].caller_admin = False
    client.portal.call(client.app.state.registry.refresh)
    fakes.requests.clear()
    fakes.hosts["svc-two"].answers[("GET", "/v1/usage/me")] = (200, _me_detail("alice"))
    entry = client.app.state.registry.entries()[0]
    assert entry.name == "svc" and entry.state != "ok"
    body = client.get("/api/usage", headers=ALICE).json()
    assert [s["name"] for s in body["services"]] == ["svc", "svc-two"]
    first = body["services"][0]
    assert first["ok"] is False and entry.state in first["error"]
    assert {r.url.host for r in fakes.requests} == {"svc-two"}


def test_page_shows_no_stale_model_after_a_fresh_answer():
    script = (STATIC_DIR / "app.js").read_text()
    assert "showConnect(service, activeModel(answer), entry.engine)" in script
    assert "activeModel(answer) || entry.model" not in script


LIMITS_BODY = {"default": {"requests_per_minute": 5}, "users": {"bob": {"tokens_per_day": 9}}}
ERROR_429 = {"error": {"message": "slow down", "type": "rate_limit_error", "code": "rate_limit_exceeded"}}

LIMIT_ROUTES = [
    ("GET", "/api/services/svc-two/limits", None, "GET", "/admin/limits"),
    ("PUT", "/api/services/svc-two/limits/default", {"requests_per_minute": 3}, "PUT", "/admin/limits/default"),
    ("PUT", "/api/services/svc-two/limits/users/carol", {"tokens_per_day": 7}, "PUT", "/admin/limits/users/carol"),
    ("DELETE", "/api/services/svc-two/limits/users/carol", None, "DELETE", "/admin/limits/users/carol"),
    ("GET", "/api/services/svc-two/limits/me", None, "GET", "/v1/limits/me"),
]


@pytest.mark.parametrize("method,route,body,service_method,service_path", LIMIT_ROUTES)
def test_each_limits_route_reaches_its_service_endpoint(client, fakes, method, route, body, service_method, service_path):
    fakes.hosts["svc-two"].answers[(service_method, service_path)] = (200, LIMITS_BODY)
    response = client.request(method, route, json=body, headers=ALICE)
    assert response.status_code == 200
    assert response.json() == LIMITS_BODY
    sent = fakes.calls()
    assert len(sent) == 1
    assert (sent[0].url.host, sent[0].method, sent[0].url.path) == ("svc-two", service_method, service_path)
    assert sent[0].headers["X-LLM-Acting-User"] == "alice"
    assert sent[0].headers["Connection"] == "close"
    if body is not None:
        assert json.loads(sent[0].content) == body


@pytest.mark.parametrize("route", ["/limits/default", "/limits/users/carol"])
def test_the_limits_put_routes_need_json(client, fakes, route):
    response = client.put("/api/services/svc/" + route.lstrip("/"), content=b"{}", headers={**ALICE, "Content-Type": "text/plain"})
    assert response.status_code == 415
    assert fakes.calls() == []


@pytest.mark.parametrize("route", ["/limits/default", "/limits/users/carol"])
def test_the_limits_put_routes_need_valid_json(client, fakes, route):
    response = client.put(
        "/api/services/svc/" + route.lstrip("/"), content=b"{oops", headers={**ALICE, "Content-Type": "application/json"}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"
    assert fakes.calls() == []


@pytest.mark.parametrize("user", ["a%2Fb", "%2E", "%2E%2E", "%2Fx", "x%2F"])
def test_a_bad_user_name_in_the_limits_routes_makes_no_call(client, fakes, user):
    for method in ("PUT", "DELETE"):
        response = client.request(method, f"/api/services/svc/limits/users/{user}", json={}, headers=ALICE)
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_user"
    assert fakes.calls() == []


@pytest.mark.parametrize("user, status", [(".", 400), ("..", 405)])
def test_a_literal_dot_user_segment_in_the_limits_routes_makes_no_call(client, fakes, user, status):
    # The test client removes a literal dot segment: "users/." becomes "users/", and "users/.." becomes
    # "limits". So the route gets an empty user name, or the path of the limits list.
    for method in ("PUT", "DELETE"):
        response = client.request(method, f"/api/services/svc/limits/users/{user}", json={}, headers=ALICE)
        assert response.status_code == status, method
        if status == 400:
            assert response.json()["error"]["code"] == "invalid_user"
    assert fakes.calls() == []


def test_the_limits_routes_of_an_unknown_service_are_404(client, fakes):
    for method, route, body, *_ in LIMIT_ROUTES:
        response = client.request(method, route.replace("svc-two", "nope"), json=body, headers=ALICE)
        assert response.status_code == 404
    assert fakes.calls() == []


@pytest.mark.parametrize("status,error", [(403, ERROR_403), (429, ERROR_429)])
def test_a_limits_error_of_the_service_passes_through(client, fakes, status, error):
    fakes.hosts["svc-two"].answers[("GET", "/admin/limits")] = (status, error)
    response = client.get("/api/services/svc-two/limits", headers=ALICE)
    assert response.status_code == status
    assert response.json() == error


def test_retry_after_of_a_service_429_reaches_the_client(client, fakes):
    fakes.hosts["svc-two"].answers[("GET", "/v1/limits/me")] = (429, ERROR_429)
    fakes.hosts["svc-two"].answer_headers[("GET", "/v1/limits/me")] = {"Retry-After": "17", "X-Other": "yes"}
    response = client.get("/api/services/svc-two/limits/me", headers=ALICE)
    assert response.status_code == 429
    assert response.headers["retry-after"] == "17"
    assert "x-other" not in response.headers


@pytest.mark.parametrize("value", ["soon", "123456", "-1", "1.5", "", "1 2", "Wed, 21 Oct 2026 07:28:00 GMT"])
def test_a_retry_after_that_is_not_1_to_5_digits_is_dropped(client, fakes, value):
    fakes.hosts["svc-two"].answers[("GET", "/v1/limits/me")] = (429, ERROR_429)
    fakes.hosts["svc-two"].answer_headers[("GET", "/v1/limits/me")] = {"Retry-After": value}
    response = client.get("/api/services/svc-two/limits/me", headers=ALICE)
    assert response.status_code == 429
    assert "retry-after" not in response.headers


def test_a_response_without_retry_after_has_none(client, fakes):
    fakes.hosts["svc-two"].answers[("GET", "/v1/limits/me")] = (429, ERROR_429)
    assert "retry-after" not in client.get("/api/services/svc-two/limits/me", headers=ALICE).headers


@pytest.mark.parametrize("route", ["/limits/default", "/limits/users/carol"])
def test_the_limits_put_routes_refuse_deeply_nested_json(client, fakes, route):
    response = client.put(
        "/api/services/svc/" + route.lstrip("/"),
        content=b"[" * 100_000,
        headers={**ALICE, "Content-Type": "application/json"},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"
    assert fakes.calls() == []


LIVE_NOW = {
    "ts": "2026-11-13T10:00:00Z",
    "in_flight": 3,
    "running": 3,
    "waiting": 0,
    "prompt_tokens_per_s": 210.0,
    "completion_tokens_per_s": None,
    "gpu_memory_used": 14200,
    "gpu_memory_total": 15360,
}


def _live_answer(now=None):
    return 200, {"now": now if now is not None else LIVE_NOW, "points": []}


def test_metrics_live_passes_through_with_the_acting_user(client, fakes):
    answer = _live_answer()
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = answer
    response = client.get("/api/services/svc/metrics/live", headers=ALICE)
    assert response.status_code == 200
    assert response.json() == answer[1]
    (sent,) = fakes.calls("/admin/metrics")
    assert sent.url.host == "svc" and sent.url.path == "/admin/metrics/live"
    assert sent.headers["X-LLM-Acting-User"] == "alice"
    assert sent.headers["Connection"] == "close"


def test_metrics_live_passes_an_error_status_through(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = (403, ERROR_403)
    response = client.get("/api/services/svc/metrics/live", headers=ALICE)
    assert response.status_code == 403
    assert response.json() == ERROR_403


def test_metrics_history_passes_the_times_through(client, fakes):
    answer = (200, {"step_seconds": 60, "points": [], "missing_days": ["2026-11-01"]})
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/history")] = answer
    query = "since=2026-11-13T09:00:00Z&until=2026-11-13T10:00:00Z"
    response = client.get(f"/api/services/svc/metrics/history?{query}", headers=ALICE)
    assert response.status_code == 200
    assert response.json() == answer[1]
    (sent,) = fakes.calls("/admin/metrics")
    assert sent.url.path == "/admin/metrics/history"
    assert dict(sent.url.params) == {"since": "2026-11-13T09:00:00Z", "until": "2026-11-13T10:00:00Z"}
    assert sent.headers["X-LLM-Acting-User"] == "alice"
    assert sent.headers["Connection"] == "close"


def test_metrics_history_passes_a_400_through(client, fakes):
    bad = {"error": {"message": "bad time", "type": "invalid_request_error", "code": "invalid_time"}}
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/history")] = (400, bad)
    response = client.get("/api/services/svc/metrics/history?since=nope", headers=ALICE)
    assert response.status_code == 400
    assert response.json() == bad


def test_metrics_routes_need_a_user_and_a_known_service(client, fakes):
    assert client.get("/api/services/svc/metrics/live").status_code == 401
    assert client.get("/api/services/svc/metrics/history").status_code == 401
    assert client.get("/api/services/unknown/metrics/live", headers=ALICE).status_code == 404
    assert fakes.calls() == []


def _load_calls(fakes):
    return fakes.calls("/admin/metrics/live")


def test_load_lists_two_admin_services_in_registry_order(client, fakes):
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    fakes.hosts["svc-two"].answers[("GET", "/admin/metrics/live")] = _live_answer({**LIVE_NOW, "in_flight": 9})
    response = client.get("/api/load", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert body == {
        "services": [
            {"name": "svc", "ok": True, "now": LIVE_NOW},
            {"name": "svc-two", "ok": True, "now": {**LIVE_NOW, "in_flight": 9}},
        ]
    }
    sent = _load_calls(fakes)
    assert {r.url.host for r in sent} == {"svc", "svc-two"}
    assert all(r.headers["X-LLM-Acting-User"] == "alice" for r in sent)
    assert all(r.headers["Connection"] == "close" for r in sent)


def test_load_leaves_out_a_service_where_the_viewer_is_not_admin(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    body = client.get("/api/load", headers=ALICE).json()
    assert [s["name"] for s in body["services"]] == ["svc"]
    assert [r.url.host for r in _load_calls(fakes)] == ["svc"]


def test_load_leaves_out_a_service_that_answers_403(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc-two"].answers[("GET", "/admin/metrics/live")] = (403, ERROR_403)
    body = client.get("/api/load", headers=ALICE).json()
    assert [s["name"] for s in body["services"]] == ["svc"]


def test_load_lists_a_service_whose_role_call_failed(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    fakes.hosts["svc-two"].answers[("GET", "/v1/service")] = (500, {})
    body = client.get("/api/load", headers=ALICE).json()
    assert body["services"][0]["ok"] is True
    assert body["services"][1] == {"name": "svc-two", "ok": False, "error": "The service did not answer."}


def test_load_a_404_service_does_not_offer_load_numbers(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = (404, {})
    response = client.get("/api/load", headers=ALICE)
    assert response.status_code == 200
    assert response.json()["services"] == [
        {"name": "svc", "ok": False, "error": "The service does not offer load numbers."}
    ]


def test_load_answers_200_when_each_service_fails(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = (500, {})
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc-two"].error = httpx.ConnectError("down")
    response = client.get("/api/load", headers=ALICE)
    assert response.status_code == 200
    assert [s["ok"] for s in response.json()["services"]] == [False, False]


@pytest.mark.parametrize("now", ["x", [], 5, None])
def test_load_a_now_that_is_not_an_object_fails_that_service_only(client, fakes, now):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = (200, {"now": now, "points": []})
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc-two"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    body = client.get("/api/load", headers=ALICE).json()
    assert body["services"][0] == {
        "name": "svc", "ok": False, "error": "The LLM service did not give a valid answer.",
    }
    assert body["services"][1]["ok"] is True


def test_load_a_body_that_is_not_an_object_fails_that_service(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = (200, [1])
    body = client.get("/api/load", headers=ALICE).json()
    assert body["services"][0]["ok"] is False


def test_load_an_unexpected_error_fails_one_service_only(client, fakes, monkeypatch):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    fakes.hosts["svc-two"].admins.add("alice")
    original = app_module.ServiceClient.request

    async def broken(self, service, method, path, viewer, **options):
        if service == "svc-two" and path == "/admin/metrics/live":
            raise RuntimeError("boom")
        return await original(self, service, method, path, viewer, **options)

    monkeypatch.setattr(app_module.ServiceClient, "request", broken)
    body = client.get("/api/load", headers=ALICE).json()
    assert [s["ok"] for s in body["services"]] == [True, False]


def test_load_a_hanging_service_times_out_and_the_others_answer(client, fakes, monkeypatch):
    assert app_module.LOAD_TIMEOUT_SECONDS == 5.0
    monkeypatch.setattr(app_module, "LOAD_TIMEOUT_SECONDS", 0.05)
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc-two"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    original = fakes.hosts["svc"]

    async def hang(request):
        if request.url.path == "/admin/metrics/live":
            await asyncio.sleep(5)
        return original(request)

    fakes.hosts["svc"] = hang
    started = time.monotonic()
    response = client.get("/api/load", headers=ALICE)
    assert time.monotonic() - started < 2
    assert response.status_code == 200
    body = response.json()
    assert body["services"][0] == {
        "name": "svc", "ok": False, "error": "The LLM service did not answer in time.",
    }
    assert body["services"][1]["ok"] is True


def test_load_lists_a_service_that_is_not_ok_and_does_not_call_it(client, fakes):
    fakes.hosts["svc"].caller_admin = False
    client.portal.call(client.app.state.registry.refresh)
    fakes.requests.clear()
    fakes.hosts["svc-two"].admins.add("alice")
    fakes.hosts["svc-two"].answers[("GET", "/admin/metrics/live")] = _live_answer()
    entry = client.app.state.registry.entries()[0]
    assert entry.name == "svc" and entry.state != "ok"
    response = client.get("/api/load", headers=ALICE)
    assert response.status_code == 200
    services = response.json()["services"]
    assert services[0] == {"name": "svc", "ok": False, "error": f"The service has the state {entry.state}."}
    assert services[1]["name"] == "svc-two" and services[1]["ok"] is True
    assert {r.url.host for r in fakes.requests} == {"svc-two"}


def test_metrics_history_without_times_sends_no_time_parameters(client, fakes):
    fakes.hosts["svc"].answers[("GET", "/admin/metrics/history")] = (200, {"step_seconds": 60, "points": []})
    assert client.get("/api/services/svc/metrics/history", headers=ALICE).status_code == 200
    (sent,) = fakes.calls("/admin/metrics")
    assert "since" not in sent.url.params and "until" not in sent.url.params


@pytest.mark.parametrize("route", ["live", "history"])
def test_metrics_routes_refuse_a_bad_service_name_without_a_call(client, fakes, route):
    response = client.get(f"/api/services/Bad_Name/metrics/{route}", headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_service"
    assert fakes.calls() == []


def test_load_needs_a_user(client, fakes):
    assert client.get("/api/load").status_code == 401
    assert fakes.requests == []
