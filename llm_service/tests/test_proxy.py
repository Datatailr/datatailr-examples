import asyncio
import json

import httpx

from llm_service.gateway.proxy import StreamUsageFilter, forward, prepare_body, prompt_of
from llm_service.tests.fakes import fake_vllm_app

USAGE = {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}


def _chunk(obj) -> bytes:
    return b"data: " + json.dumps(obj).encode() + b"\n\n"


SSE = (
    _chunk({"id": "x", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {"content": "Hi"}}], "usage": None})
    + _chunk({"id": "x", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {"content": "!"}}], "usage": None})
    + _chunk({"id": "x", "object": "chat.completion.chunk", "choices": [], "usage": USAGE})
    + b"data: [DONE]\n\n"
)


def _feed_all(f, data, step):
    return b"".join(f.feed(data[i:i + step]) for i in range(0, len(data), step)) + f.flush()


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fake")


async def _drain(response) -> bytes:
    return b"".join([part async for part in response.body_iterator])


def test_prepare_body_adds_include_usage():
    out, wanted = prepare_body({"stream": True})
    assert out["stream_options"] == {"include_usage": True} and not wanted
    out, wanted = prepare_body({"stream": True, "stream_options": {"include_usage": True}})
    assert wanted
    body = {"stream": False}
    assert prepare_body(body) == (body, True)


def test_prepare_body_returns_a_copy():
    body = {"stream": True, "stream_options": {"continuous_usage_stats": True}}
    out, wanted = prepare_body(body)
    assert "include_usage" not in body["stream_options"]
    assert out["stream_options"] == {"continuous_usage_stats": True, "include_usage": True}
    assert not wanted


def test_filter_strips_usage_chunk_across_split_bytes():
    f = StreamUsageFilter(keep_usage_chunk=False)
    out = _feed_all(f, SSE, 7)
    assert f.usage == USAGE
    assert b'"choices": []' not in out and b'"usage"' not in out and out.endswith(b"data: [DONE]\n\n")
    assert out.count(b"data: ") == 3


def test_filter_rewrites_chunks_compactly():
    f = StreamUsageFilter(keep_usage_chunk=False)
    out = f.feed(SSE)
    first = out.split(b"\n\n")[0]
    assert first == b'data: {"id":"x","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hi"}}]}'


def test_filter_keeps_usage_chunk_when_wanted():
    f = StreamUsageFilter(keep_usage_chunk=True)
    out = _feed_all(f, SSE, 5)
    assert out == SSE
    assert f.usage == USAGE


def test_filter_passes_chunks_without_usage_key_unchanged():
    raw = b'data: {"choices":[{"delta":{"content":"a"}}]}\n\ndata: [DONE]\n\n'
    f = StreamUsageFilter(keep_usage_chunk=False)
    assert f.feed(raw) == raw
    assert f.usage is None


def test_filter_handles_crlf_boundaries():
    raw = SSE.replace(b"\n\n", b"\r\n\r\n")
    f = StreamUsageFilter(keep_usage_chunk=False)
    out = _feed_all(f, raw, 9)
    assert f.usage == USAGE
    assert b'"usage"' not in out and out.endswith(b"data: [DONE]\r\n\r\n")


def test_filter_passes_invalid_json_unchanged():
    raw = b"data: {not json\n\ndata: [DONE]\n\n"
    f = StreamUsageFilter(keep_usage_chunk=False)
    assert f.feed(raw) == raw
    assert f.usage is None


def test_filter_flush_returns_incomplete_tail():
    f = StreamUsageFilter(keep_usage_chunk=False)
    assert f.feed(b"data: [DO") == b""
    assert f.flush() == b"data: [DO"
    assert f.flush() == b""


def test_forward_plain():
    fake = fake_vllm_app()
    done = []

    async def run():
        async with _client(fake) as client:
            return await forward(client, "http://fake", "/v1/chat/completions", {"model": "m"}, lambda s, u: done.append((s, u)))

    response = asyncio.run(run())
    assert response.status_code == 200
    assert json.loads(response.body)["usage"] == USAGE
    assert done == [(200, USAGE)]
    assert fake.state.requests == [{"model": "m"}]


def test_forward_stream_reports_usage_and_hides_chunk():
    fake = fake_vllm_app()
    done = []

    async def run():
        async with _client(fake) as client:
            response = await forward(
                client, "http://fake", "/v1/chat/completions", {"stream": True}, lambda s, u: done.append((s, u))
            )
            return response, await _drain(response)

    response, data = asyncio.run(run())
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert fake.state.requests[0]["stream_options"] == {"include_usage": True}
    assert b'"usage"' not in data and b'"choices": []' not in data
    assert data.endswith(b"data: [DONE]\n\n")
    assert done == [(200, USAGE)]


def test_forward_stream_keeps_usage_chunk_when_client_asked():
    fake = fake_vllm_app()
    done = []
    body = {"stream": True, "stream_options": {"include_usage": True}}

    async def run():
        async with _client(fake) as client:
            response = await forward(client, "http://fake", "/v1/chat/completions", body, lambda s, u: done.append((s, u)))
            return await _drain(response)

    data = asyncio.run(run())
    assert b'"choices": []' in data
    assert done == [(200, USAGE)]


def test_forward_upstream_error_status_passes_through():
    fake = fake_vllm_app(status=400)
    done = []

    async def run():
        async with _client(fake) as client:
            return await forward(client, "http://fake", "/v1/chat/completions", {"stream": True}, lambda s, u: done.append((s, u)))

    response = asyncio.run(run())
    assert response.status_code == 400
    assert json.loads(response.body) == {"error": {"message": "bad request"}}
    assert response.headers["content-type"].startswith("application/json")
    assert done == [(400, None)]


def test_forward_plain_error_status_passes_through():
    fake = fake_vllm_app(status=400)
    done = []

    async def run():
        async with _client(fake) as client:
            return await forward(client, "http://fake", "/v1/chat/completions", {}, lambda s, u: done.append((s, u)))

    response = asyncio.run(run())
    assert response.status_code == 400
    assert done == [(400, None)]


def _unreachable_client():
    def refuse(request):
        raise httpx.ConnectError("refused", request=request)

    return httpx.AsyncClient(transport=httpx.MockTransport(refuse))


def test_forward_connect_error_is_502():
    expected = {
        "error": {
            "message": "the model engine is not reachable",
            "type": "service_unavailable",
            "code": "engine_unreachable",
        }
    }
    for body in ({}, {"stream": True}):
        done = []

        async def run():
            async with _unreachable_client() as client:
                return await forward(client, "http://fake", "/v1/chat/completions", body, lambda s, u: done.append((s, u)))

        response = asyncio.run(run())
        assert response.status_code == 502
        assert json.loads(response.body) == expected
        assert done == [(502, None)]


def test_forward_error_in_the_middle_of_the_stream_reports_502():
    class Broken(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[{"delta":{"content":"a"}}]}\n\n'
            raise httpx.ReadError("connection lost")

    def handler(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Broken())

    done = []

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            response = await forward(client, "http://fake", "/v1/chat/completions", {"stream": True}, lambda s, u: done.append((s, u)))
            return await _drain(response)

    data = asyncio.run(run())
    assert data.startswith(b"data: ")
    assert done == [(502, None)]


def test_client_disconnect_before_the_usage_chunk_reports_499_without_usage():
    stream = _TrackedStream([_ONE, _USAGE_CHUNK, b"data: [DONE]\n\n"])
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)
            iterator = response.body_iterator
            first = await iterator.__anext__()
            await iterator.aclose()
            return first

    assert asyncio.run(run()) == _ONE
    assert done == [(499, None)]
    assert stream.closed


def test_prompt_of():
    assert prompt_of({"messages": [{"role": "user", "content": "hi"}], "model": "m"}) == {
        "messages": [{"role": "user", "content": "hi"}]
    }
    assert prompt_of({"prompt": "hi"}) == {"prompt": "hi"}
    assert prompt_of({"input": ["a", "b"]}) == {"input": ["a", "b"]}


def test_filter_flush_processes_unterminated_usage_chunk():
    raw = SSE[:-len(b"data: [DONE]\n\n")].rstrip(b"\n")
    f = StreamUsageFilter(keep_usage_chunk=False)
    out = f.feed(raw) + f.flush()
    assert f.usage == USAGE
    assert b'"usage"' not in out and out.count(b"data: ") == 2


class _TrackedStream(httpx.AsyncByteStream):
    """An upstream body that records whether it was closed."""

    def __init__(self, parts, error=None):
        self.parts = parts
        self.error = error
        self.closed = False

    async def __aiter__(self):
        for part in self.parts:
            yield part
        if self.error:
            raise self.error

    async def aclose(self):
        self.closed = True


def _tracked_client(stream):
    def handler(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


_SCOPE = {"type": "http", "method": "POST", "path": "/", "headers": []}
_ONE = b'data: {"choices":[{"delta":{"content":"a"}}]}\n\n'


async def _forward_stream(client, done):
    return await forward(client, "http://fake", "/v1/chat/completions", {"stream": True}, lambda s, u: done.append((s, u)))


def test_send_failure_before_the_generator_starts_reports_499_once():
    stream = _TrackedStream([_ONE])
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)

            async def receive():
                await asyncio.sleep(10)

            async def send(message):
                raise OSError("client gone")

            try:
                await response(_SCOPE, receive, send)
            except BaseException as exc:
                return exc

    assert asyncio.run(run()) is not None
    assert done == [(499, None)]
    assert stream.closed


def test_response_never_iterated_reports_499_once():
    stream = _TrackedStream([_ONE])
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)

            async def receive():
                return {"type": "http.disconnect"}

            async def send(message):
                raise OSError("client gone")

            try:
                await response(_SCOPE, receive, send)
            except BaseException:
                pass
            await response.finish(200, USAGE)

    asyncio.run(run())
    assert done == [(499, None)]
    assert stream.closed


def test_asgi_disconnect_reports_499_once_and_closes_upstream():
    stream = _TrackedStream([_ONE])
    done = []
    sent = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)
            proceed = asyncio.Event()

            async def receive():
                await proceed.wait()
                return {"type": "http.disconnect"}

            async def send(message):
                sent.append(message)
                if message["type"] == "http.response.body" and message.get("body"):
                    proceed.set()
                    await asyncio.sleep(0.05)

            await response(_SCOPE, receive, send)

    asyncio.run(run())
    assert any(m.get("body") == _ONE for m in sent)
    assert done == [(499, None)]
    assert stream.closed


def test_asgi_normal_end_reports_once_and_closes_upstream():
    stream = _TrackedStream([_ONE, b"data: [DONE]\n\n"])
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)

            async def receive():
                await asyncio.sleep(10)

            async def send(message):
                pass

            await response(_SCOPE, receive, send)

    asyncio.run(run())
    assert done == [(200, None)]
    assert stream.closed


def test_unexpected_error_in_the_stream_reports_502_and_raises():
    stream = _TrackedStream([_ONE], error=RuntimeError("boom"))
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)
            try:
                await _drain(response)
            except RuntimeError as exc:
                return exc

    assert isinstance(asyncio.run(run()), RuntimeError)
    assert done == [(502, None)]
    assert stream.closed


def test_decoding_error_before_the_body_is_502():
    def handler(request):
        raise httpx.DecodingError("bad encoding", request=request)

    done = []

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await forward(client, "http://fake", "/v1/chat/completions", {}, lambda s, u: done.append((s, u)))

    assert asyncio.run(run()).status_code == 502
    assert done == [(502, None)]


def test_failing_on_done_is_logged_and_does_not_break_the_response(caplog):
    def bad(status, usage):
        raise ValueError("audit down")

    async def run():
        async with _client(fake_vllm_app()) as client:
            plain = await forward(client, "http://fake", "/v1/chat/completions", {}, bad)
            streamed = await forward(client, "http://fake", "/v1/chat/completions", {"stream": True}, bad)
            return plain, await _drain(streamed)

    with caplog.at_level("WARNING", logger="llm_service.proxy"):
        plain, data = asyncio.run(run())
    assert plain.status_code == 200 and data.endswith(b"data: [DONE]\n\n")
    assert len([r for r in caplog.records if r.name == "llm_service.proxy"]) == 2


_USAGE_CHUNK = _chunk({"id": "x", "choices": [], "usage": USAGE})


def test_client_disconnect_after_the_usage_chunk_reports_the_usage():
    stream = _TrackedStream([_ONE, _USAGE_CHUNK, b"data: [DONE]\n\n"])
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)
            iterator = response.body_iterator
            assert await iterator.__anext__() == _ONE
            assert await iterator.__anext__() == b"data: [DONE]\n\n"
            await iterator.aclose()  # the client goes away before the end

    asyncio.run(run())
    assert done == [(499, USAGE)]
    assert stream.closed


def test_asgi_disconnect_after_the_usage_chunk_reports_the_usage():
    stream = _TrackedStream([_ONE, _USAGE_CHUNK, b"data: [DONE]\n\n"])
    done = []

    async def run():
        async with _tracked_client(stream) as client:
            response = await _forward_stream(client, done)
            proceed = asyncio.Event()
            bodies = []

            async def receive():
                await proceed.wait()
                return {"type": "http.disconnect"}

            async def send(message):
                if message["type"] == "http.response.body" and message.get("body"):
                    bodies.append(message["body"])
                    if len(bodies) == 2:
                        proceed.set()
                        await asyncio.sleep(0.05)

            await response(_SCOPE, receive, send)

    asyncio.run(run())
    assert done == [(499, USAGE)]
