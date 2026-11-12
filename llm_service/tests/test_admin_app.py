import json
import re
import subprocess
import sys

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
    for path in ("/api/me", "/api/status", "/api/usage", "/api/audit", "/api/audit/users"):
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
    built = default_app({})
    assert built.state.registry._configured == []
    with TestClient(built) as test_client:
        assert test_client.post("/api/services", json={"name": "svc"}, headers=ALICE).status_code == 403


def test_the_lifespan_runs_and_stops_the_registry(fakes, registry):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    with TestClient(create_app(registry, "alice", http, refresh_interval=None)) as test_client:
        assert registry.entries() == []
        assert test_client.get("/health").status_code == 200
    with TestClient(create_app(registry, "alice", http)) as test_client:
        test_client.portal.call(registry.wait_ready, 10.0)
        assert len(registry.entries()) == 2
