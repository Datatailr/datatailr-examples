import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from llm_service.admin_app import app as app_module
from llm_service.admin_app.app import create_app
from llm_service.admin_app.chat import (
    DEFAULT_MAX_TOKENS,
    MAX_CHARACTERS,
    MAX_MESSAGES,
    ChatError,
    chat_request,
)
from llm_service.admin_app.registry import Registry
from llm_service.tests.test_admin_app import ALICE, FakeService, FakeServices, FakeStore

SSE = b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n\ndata: {"usage":{"total_tokens":3}}\n\ndata: [DONE]\n\n'
MESSAGES = [{"role": "user", "content": "Hello"}]


def test_minimal_body_gets_the_defaults():
    assert chat_request({"messages": MESSAGES}) == {
        "messages": MESSAGES,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "stream": True,
        "stream_options": {"include_usage": True},
    }


def test_model_and_temperature_pass_and_other_keys_drop():
    body = {
        "messages": MESSAGES,
        "model": "org/model",
        "temperature": 0,
        "max_tokens": 4096,
        "stream": False,
        "stream_options": {"include_usage": False},
        "tools": [{"type": "function"}],
        "user": "bob",
    }
    assert chat_request(body) == {
        "messages": MESSAGES,
        "model": "org/model",
        "temperature": 0,
        "max_tokens": 4096,
        "stream": True,
        "stream_options": {"include_usage": True},
    }


def test_the_limits_of_max_tokens_and_temperature_are_accepted():
    built = chat_request({"messages": MESSAGES, "max_tokens": 1, "temperature": 2})
    assert (built["max_tokens"], built["temperature"]) == (1, 2)


def test_a_message_keeps_only_role_and_content():
    built = chat_request({"messages": [{"role": "system", "content": "x", "name": "n", "tool_calls": []}]})
    assert built["messages"] == [{"role": "system", "content": "x"}]


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        "text",
        {},
        {"messages": []},
        {"messages": "hi"},
        {"messages": ["hi"]},
        {"messages": [{"role": "tool", "content": "x"}]},
        {"messages": [{"role": "user"}]},
        {"messages": [{"role": "user", "content": 5}]},
        {"messages": [{"role": "user", "content": None}]},
        {"messages": MESSAGES, "max_tokens": 0},
        {"messages": MESSAGES, "max_tokens": 4097},
        {"messages": MESSAGES, "max_tokens": 1.5},
        {"messages": MESSAGES, "max_tokens": True},
        {"messages": MESSAGES, "max_tokens": "5"},
        {"messages": MESSAGES, "temperature": -0.1},
        {"messages": MESSAGES, "temperature": 2.1},
        {"messages": MESSAGES, "temperature": "1"},
        {"messages": MESSAGES, "temperature": True},
        {"messages": MESSAGES, "temperature": float("nan")},
        {"messages": MESSAGES, "model": 5},
    ],
)
def test_a_bad_field_is_invalid_request(body):
    with pytest.raises(ChatError) as caught:
        chat_request(body)
    assert caught.value.code == "invalid_request"
    assert caught.value.message


def test_too_many_messages_is_too_large():
    too_many = [{"role": "user", "content": "x"}] * (MAX_MESSAGES + 1)
    with pytest.raises(ChatError) as caught:
        chat_request({"messages": too_many})
    assert caught.value.code == "request_too_large"
    assert len(chat_request({"messages": too_many[:MAX_MESSAGES]})["messages"]) == MAX_MESSAGES


def test_too_many_characters_is_too_large():
    half = "x" * (MAX_CHARACTERS // 2)
    ok = [{"role": "user", "content": half}, {"role": "user", "content": half}]
    assert chat_request({"messages": ok})
    with pytest.raises(ChatError) as caught:
        chat_request({"messages": ok + [{"role": "user", "content": "y"}]})
    assert caught.value.code == "request_too_large"


class RecordedStream(httpx.AsyncByteStream):
    """A response body that sends its chunks and records the close."""

    def __init__(self, chunks, then=None):
        self.chunks = chunks
        self.then = then  # an exception to raise after the chunks, or "hang"
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.then == "hang":
            await asyncio.sleep(60)
        elif self.then is not None:
            raise self.then

    async def aclose(self):
        self.closed = True


class ChatBackend:
    """Routes the chat call to a configurable answer. Other calls go to the fake services."""

    def __init__(self, fakes):
        self.fakes = fakes
        self.build = lambda request: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=RecordedStream([SSE])
        )
        self.chat_requests: list[httpx.Request] = []

    def __call__(self, request):
        if request.method == "POST" and request.url.path == "/v1/chat/completions":
            self.chat_requests.append(request)
            fake = self.fakes.hosts.get(request.url.host)
            if fake is None:
                return httpx.Response(404, json={})
            if fake.error:
                raise fake.error
            return self.build(request)
        return self.fakes(request)


@pytest.fixture
def fakes():
    fakes = FakeServices()
    fakes.hosts["svc"] = FakeService()
    return fakes


@pytest.fixture
def backend(fakes):
    return ChatBackend(fakes)


@pytest.fixture
def registry(fakes):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fakes))
    return Registry(http, ["svc"], FakeStore(), lambda: [])


@pytest.fixture
def client(registry, backend):
    http = httpx.AsyncClient(transport=httpx.MockTransport(backend))
    with TestClient(create_app(registry, "alice", http)) as test_client:
        test_client.portal.call(registry.wait_ready, 10.0)
        yield test_client


def post(client, name="svc", body=None, headers=ALICE, **options):
    return client.post(f"/api/services/{name}/chat", json=body or {"messages": MESSAGES}, headers=headers, **options)


def test_the_service_gets_the_built_body_and_the_acting_user(client, backend):
    response = post(client, body={"messages": MESSAGES, "stream": False, "tools": [], "temperature": 0.5})
    assert response.status_code == 200
    sent = backend.chat_requests[0]
    assert json.loads(sent.content) == {
        "messages": MESSAGES,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "temperature": 0.5,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    assert sent.headers["X-LLM-Acting-User"] == "alice"
    assert sent.headers["Connection"] == "close"
    assert sent.url.host == "svc"


def test_the_stream_call_asks_the_service_for_no_compression(client, backend):
    assert post(client).status_code == 200
    assert backend.chat_requests[0].headers["Accept-Encoding"] == "identity"


def test_the_stream_answer_asks_the_edge_for_no_buffering(client):
    response = post(client)
    assert response.status_code == 200
    assert response.headers["x-accel-buffering"] == "no"


def test_the_sse_bytes_arrive_unchanged(client):
    response = post(client)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.content == SSE


def test_the_service_error_passes_through(client, backend):
    error = {"error": {"message": "loading", "type": "server_error", "code": "model_loading"}}
    backend.build = lambda request: httpx.Response(503, json=error)
    response = post(client)
    assert response.status_code == 503
    assert response.json() == error


def test_retry_after_of_a_429_passes_through(client, backend):
    error = {"error": {"message": "slow", "type": "rate_limit_error", "code": "rate_limit_exceeded"}}
    backend.build = lambda request: httpx.Response(429, json=error, headers={"Retry-After": "17", "X-Other": "yes"})
    response = post(client)
    assert response.status_code == 429
    assert response.headers["retry-after"] == "17"
    assert "x-other" not in response.headers
    assert response.json() == error


@pytest.mark.parametrize("value", ["soon", "123456"])
def test_a_bad_retry_after_is_dropped_in_the_chat_route(client, backend, value):
    backend.build = lambda request: httpx.Response(429, json={"error": {}}, headers={"Retry-After": value})
    response = post(client)
    assert response.status_code == 429
    assert "retry-after" not in response.headers


def test_a_403_of_the_service_passes_through(client, backend):
    error = {"error": {"message": "no access", "type": "permission_error", "code": "access_denied"}}
    backend.build = lambda request: httpx.Response(403, json=error)
    response = post(client)
    assert response.status_code == 403
    assert response.json() == error


def test_a_status_below_400_that_is_not_200_is_502(client, backend):
    for status in (201, 204, 302):
        backend.build = lambda request, status=status: httpx.Response(status, content=b"data: x\n\n")
        response = post(client)
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "service_unreachable"


def test_an_error_body_over_64_kib_is_502(client, backend):
    big = json.dumps({"error": {"message": "x" * 70_000}}).encode()
    backend.build = lambda request: httpx.Response(500, content=big)
    response = post(client)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"


def test_an_error_body_under_the_limit_passes(client, backend):
    error = {"error": {"message": "x" * 60_000, "type": "server_error", "code": "boom"}}
    backend.build = lambda request: httpx.Response(500, json=error)
    response = post(client)
    assert response.status_code == 500
    assert response.json() == error


def test_an_error_body_that_is_not_json_is_502(client, backend):
    backend.build = lambda request: httpx.Response(500, content=b"<html>oops</html>")
    response = post(client)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"


def test_no_viewer_is_401_and_calls_nothing(client, backend):
    response = post(client, headers={})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_user"
    assert backend.chat_requests == []


def test_the_viewer_check_comes_before_the_name_check(client):
    assert post(client, name="nope", headers={}).status_code == 401


def test_an_unknown_service_is_404(client, backend):
    response = post(client, name="nope")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "unknown_service"
    assert backend.chat_requests == []


def test_a_bad_name_is_400(client, backend):
    response = post(client, name="Bad_Name")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_service"
    assert backend.chat_requests == []


def test_a_body_without_json_content_type_is_415(client, backend):
    response = client.post(
        "/api/services/svc/chat",
        content=json.dumps({"messages": MESSAGES}),
        headers={**ALICE, "Content-Type": "text/plain"},
    )
    assert response.status_code == 415
    assert backend.chat_requests == []


def test_a_body_that_is_not_json_is_400(client):
    response = client.post(
        "/api/services/svc/chat", content=b"{", headers={**ALICE, "Content-Type": "application/json"}
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"


def test_a_bad_field_is_400_and_calls_nothing(client, backend):
    response = post(client, body={"messages": []})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert backend.chat_requests == []


def test_a_large_request_is_400_request_too_large(client, backend):
    response = post(client, body={"messages": [{"role": "user", "content": "x" * (MAX_CHARACTERS + 1)}]})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "request_too_large"
    assert backend.chat_requests == []


def test_a_transport_error_is_502(client, fakes):
    fakes.hosts["svc"].error = httpx.ConnectError("refused")
    response = post(client)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"


def test_the_response_of_the_service_closes_at_the_end(client, backend):
    body = RecordedStream([SSE])
    backend.build = lambda request: httpx.Response(200, stream=body)
    assert post(client).content == SSE
    assert body.closed


def test_an_error_after_the_start_ends_the_stream_and_closes(client, backend):
    body = RecordedStream([b"data: one\n\n"], then=httpx.ReadError("cut"))
    backend.build = lambda request: httpx.Response(200, stream=body)
    response = post(client)
    assert response.status_code == 200
    assert response.content == b"data: one\n\n"
    assert body.closed


def test_the_limit_stops_a_slow_stream(client, backend, monkeypatch):
    monkeypatch.setattr(app_module, "CHAT_TIMEOUT_SECONDS", 0.2)
    body = RecordedStream([b"data: one\n\n"], then="hang")
    backend.build = lambda request: httpx.Response(200, stream=body)
    response = post(client)
    assert response.content == b"data: one\n\n"
    assert body.closed


def test_a_gone_client_closes_the_response_of_the_service(registry, backend, monkeypatch):
    held = []

    class Holding(app_module.ServiceClient):
        """Keep each call alive, so garbage collection cannot close its generators."""

        def stream(self, *args, **kwargs):
            call = super().stream(*args, **kwargs)
            held.append(call)
            return call

    monkeypatch.setattr(app_module, "ServiceClient", Holding)
    body = RecordedStream([b"data: one\n\n"], then="hang")
    backend.build = lambda request: httpx.Response(200, stream=body)
    http = httpx.AsyncClient(transport=httpx.MockTransport(backend))
    app = create_app(registry, "alice", http, refresh_interval=None)

    async def drive():
        await registry.refresh()
        payload = json.dumps({"messages": MESSAGES}).encode()
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/api/services/svc/chat",
            "raw_path": b"/api/services/svc/chat",
            "query_string": b"",
            "root_path": "",
            "headers": [(b"content-type", b"application/json"), (b"x-datatailr-user", b"alice")],
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
        }
        sent_body = False
        first_chunk = asyncio.Event()

        async def receive():
            nonlocal sent_body
            if not sent_body:
                sent_body = True
                return {"type": "http.request", "body": payload, "more_body": False}
            await first_chunk.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.body" and message.get("body"):
                first_chunk.set()

        await asyncio.wait_for(app(scope, receive, send), 10)
        assert held
        # Check before asyncio.run closes the open generators. That close also closes the body.
        assert body.closed

    asyncio.run(drive())


def test_the_limit_stops_a_slow_error_body(client, backend, monkeypatch):
    monkeypatch.setattr(app_module, "CHAT_TIMEOUT_SECONDS", 0.2)
    body = RecordedStream([b'{"error": '], then="hang")
    backend.build = lambda request: httpx.Response(503, stream=body)
    response = post(client)
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "service_unreachable"
    assert body.closed
