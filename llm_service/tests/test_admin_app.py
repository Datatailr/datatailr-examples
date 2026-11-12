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
            return httpx.Response(status, json=body)
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


@pytest.fixture
def make_client(fakes, registry):
    clients = []

    def make(owner="alice"):
        http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
        test_client = TestClient(create_app(registry, owner, http))
        test_client.__enter__()
        clients.append(test_client)
        test_client.portal.call(registry.wait_ready, 10.0)
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


def test_a_role_call_has_a_short_timeout_and_a_failure_means_no_access(client, fakes):
    fakes.hosts["svc"].error = httpx.ConnectError("refused")
    body = client.get("/api/services", headers=ALICE).json()
    assert (body["services"][0]["access"], body["services"][0]["admin"]) == (False, False)
    assert body["services"][1]["access"] is True
    assert fakes.calls()[0].extensions["timeout"]["read"] == 3.0


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


def test_an_empty_owner_matches_no_one(make_client, store):
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


def test_delete_of_a_name_that_is_not_manual_is_404(client, store):
    response = client.delete("/api/services/svc", headers=ALICE)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_service"


def test_delete_with_a_bad_name_is_400(client, store):
    response = client.delete("/api/services/Evil.Name", headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_service"


def test_delete_is_for_the_owner_only(client, store):
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
def test_a_slow_switch_or_delete_says_that_it_may_still_finish(client, service, method, path, kwargs):
    service.error = httpx.ReadTimeout("slow")
    response = client.request(method, path, headers=ALICE, **kwargs)
    assert response.status_code == 502
    error = response.json()["error"]
    assert error["code"] == "service_unreachable"
    assert "in time" in error["message"] and "may still finish" in error["message"]


def test_a_slow_read_keeps_the_plain_message(client, service):
    service.error = httpx.ReadTimeout("slow")
    response = client.get("/api/services/svc/audit", headers=ALICE)
    assert response.status_code == 502
    assert "may still finish" not in response.json()["error"]["message"]


def test_a_refused_connection_on_a_switch_keeps_the_plain_message(client, service):
    service.error = httpx.ConnectError("refused")
    response = client.post("/api/services/svc/model", json={"model": "org/name"}, headers=ALICE)
    assert response.status_code == 502
    assert "may still finish" not in response.json()["error"]["message"]


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
    # No lifespan here: it would start the real store and the real platform listing.
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
    assert (body["services"][0]["access"], body["services"][0]["admin"]) == (False, False)
    assert body["services"][1]["access"] is True


def test_an_unexpected_error_in_one_role_call_is_no_access(client, fakes):
    fakes.hosts["svc"].error = RuntimeError("boom")
    response = client.get("/api/services", headers=ALICE)
    assert response.status_code == 200
    body = response.json()
    assert (body["services"][0]["access"], body["services"][0]["admin"]) == (False, False)
    assert body["services"][1]["access"] is True


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


def test_usage_a_slow_service_times_out_after_30_seconds(client, fakes, monkeypatch):
    seen = []
    real = asyncio.wait_for

    async def spy(awaitable, timeout):
        seen.append(timeout)
        return await real(awaitable, timeout)

    monkeypatch.setattr(app_module.asyncio, "wait_for", spy)
    fakes.hosts["svc"].answers[("GET", "/admin/usage")] = (200, {"data": []})
    client.get("/api/usage?services=svc", headers=ALICE)
    assert 30.0 in seen


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


def test_usage_an_unknown_valid_name_says_unknown(client, fakes):
    response = client.get("/api/usage?services=nope", headers=ALICE)
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "invalid_service"
    assert "unknown" in error["message"]
    bad = client.get("/api/usage?services=Bad", headers=ALICE).json()["error"]
    assert "a-z" in bad["message"]
