import json
import re
import subprocess
import sys

import httpx
import pytest
from fastapi.testclient import TestClient

from llm_service.admin_app.app import STATIC_DIR, create_app

ALICE = {"X-Datatailr-User": "alice"}
ERROR_403 = {"error": {"message": "admin only", "type": "permission_error", "code": "admin_required"}}


class FakeService:
    """A fake LLM service that records each request and answers from a table."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.answers: dict[tuple[str, str], tuple[int, object]] = {}
        self.error: Exception | None = None
        self.raw: bytes | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.error:
            raise self.error
        if self.raw is not None:
            return httpx.Response(200, content=self.raw)
        status, body = self.answers.get((request.method, request.url.raw_path.decode().split("?")[0]), (200, {}))
        return httpx.Response(status, json=body)


@pytest.fixture
def service():
    return FakeService()


@pytest.fixture
def client(service):
    http = httpx.AsyncClient(transport=httpx.MockTransport(service), base_url="http://service.test")
    with TestClient(create_app("http://service.test", http)) as test_client:
        yield test_client


def test_health_calls_nothing(client, service):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.text == "OK"
    assert service.requests == []


def test_no_platform_user_is_401_and_calls_nothing(client, service):
    response = client.get("/api/usage")
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_user"
    assert service.requests == []


def test_every_call_names_the_viewer(client, service):
    client.get("/api/usage", headers=ALICE)
    assert service.requests[0].headers["X-LLM-Acting-User"] == "alice"


def test_me_admin_and_not_admin(client, service):
    assert client.get("/api/me", headers=ALICE).json() == {"user": "alice", "admin": True}
    service.answers[("GET", "/admin/model")] = (403, ERROR_403)
    response = client.get("/api/me", headers={"X-Datatailr-User": "bob"})
    assert response.status_code == 200
    assert response.json() == {"user": "bob", "admin": False}


def test_me_passes_other_errors_through(client, service):
    body = {"error": {"message": "broken", "type": "server_error", "code": "boom"}}
    service.answers[("GET", "/admin/model")] = (500, body)
    response = client.get("/api/me", headers=ALICE)
    assert response.status_code == 500
    assert response.json() == body


def test_status_joins_engine_and_models(client, service):
    service.answers[("GET", "/admin/model")] = (200, {"state": "ready"})
    service.answers[("GET", "/v1/models")] = (200, {"object": "list", "data": [{"id": "org/name"}]})
    response = client.get("/api/status", headers=ALICE)
    assert response.json() == {"engine": {"state": "ready"}, "models": [{"id": "org/name"}]}


def test_status_returns_the_first_error(client, service):
    service.answers[("GET", "/admin/model")] = (403, ERROR_403)
    response = client.get("/api/status", headers=ALICE)
    assert response.status_code == 403
    assert response.json() == ERROR_403
    assert len(service.requests) == 1


def test_usage_forwards_only_known_params(client, service):
    client.get(
        "/api/usage?since=2026-01-01&until=2026-02-01&user=carol&model=m&group_by=day&evil=1",
        headers=ALICE,
    )
    assert dict(service.requests[0].url.params) == {
        "since": "2026-01-01",
        "until": "2026-02-01",
        "user": "carol",
        "model": "m",
        "group_by": "day",
    }


def test_usage_user_and_audit_routes_forward(client, service):
    client.get("/api/usage/alice?since=a&until=b&evil=1", headers=ALICE)
    client.get("/api/audit?limit=5&offset=10&user=bob&evil=1", headers=ALICE)
    client.get("/api/audit/users", headers=ALICE)
    usage, audit, users = service.requests
    assert usage.url.path == "/admin/usage/alice"
    assert dict(usage.url.params) == {"since": "a", "until": "b"}
    assert audit.url.path == "/admin/audit"
    assert dict(audit.url.params) == {"limit": "5", "offset": "10", "user": "bob"}
    assert users.url.path == "/admin/audit/users"


def test_switch_passes_body_and_status_through(client, service):
    service.answers[("POST", "/admin/model")] = (202, {"state": "loading"})
    response = client.post("/api/model", json={"model": "org/name"}, headers=ALICE)
    assert response.status_code == 202
    assert response.json() == {"state": "loading"}
    assert json.loads(service.requests[0].content) == {"model": "org/name"}
    error = {"error": {"message": "busy", "type": "conflict", "code": "switch_in_progress"}}
    service.answers[("POST", "/admin/model")] = (409, error)
    response = client.post("/api/model", json={"model": "x"}, headers=ALICE)
    assert response.status_code == 409
    assert response.json() == error


def test_delete_keeps_the_model_path(client, service):
    service.answers[("DELETE", "/admin/models/org/name")] = (200, {"deleted": ["org/name"]})
    response = client.delete("/api/models/org/name?revision=v1", headers=ALICE)
    assert response.json() == {"deleted": ["org/name"]}
    request = service.requests[0]
    assert request.method == "DELETE"
    assert request.url.path == "/admin/models/org/name"
    assert dict(request.url.params) == {"revision": "v1"}


def test_delete_without_revision_sends_no_param(client, service):
    client.delete("/api/models/org/name", headers=ALICE)
    assert dict(service.requests[0].url.params) == {}


def test_service_error_bodies_pass_through(client, service):
    body = {"error": {"message": "bad", "type": "invalid_request_error", "code": "bad_request"}}
    service.answers[("POST", "/admin/model")] = (400, body)
    response = client.post("/api/model", json={}, headers=ALICE)
    assert response.status_code == 400
    assert response.json() == body


def test_unreachable_service_is_502(client, service):
    service.error = httpx.ConnectError("refused")
    response = client.get("/api/usage", headers=ALICE)
    assert response.status_code == 502
    error = response.json()["error"]
    assert error["code"] == "service_unreachable"
    assert error["type"] == "server_error"
    assert "service.test" not in error["message"]


def test_non_json_service_body_is_502(client, service):
    service.raw = b"<html>not json</html>"
    response = client.get("/api/usage", headers=ALICE)
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
    response = client.delete("/api/models/" + model_id.replace(".", "%2e"), headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_model"
    assert service.requests == []


def test_delete_encodes_query_and_fragment_characters(client, service):
    client.delete("/api/models/org/na%3Fme%23x", headers=ALICE)
    assert service.requests[0].url.raw_path.decode() == "/admin/models/org/na%3Fme%23x"


@pytest.mark.parametrize("user", ["..", "."])
def test_usage_user_rejects_dot_segments(client, service, user):
    response = client.get("/api/usage/" + user.replace(".", "%2e"), headers=ALICE)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_user"
    assert service.requests == []


def test_viewer_with_control_character_is_401(client, service):
    header = {"X-Datatailr-User": json.dumps({"name": "ali\nce"})}
    response = client.get("/api/usage", headers=header)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_user"
    assert service.requests == []


def test_viewer_that_is_too_long_is_401(client, service):
    response = client.get("/api/usage", headers={"X-Datatailr-User": "a" * 257})
    assert response.status_code == 401
    assert service.requests == []


def test_json_form_header_works(client, service):
    header = {"X-Datatailr-User": '{"name": "alice"}'}
    assert client.get("/api/usage", headers=header).status_code == 200
    assert service.requests[0].headers["X-LLM-Acting-User"] == "alice"


def test_status_with_a_body_that_is_not_an_object_is_502(client, service):
    service.answers[("GET", "/v1/models")] = (200, ["not", "an", "object"])
    response = client.get("/api/status", headers=ALICE)
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
