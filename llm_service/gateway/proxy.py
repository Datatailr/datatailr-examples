"""Forward requests to the vLLM process and capture the token counts.

The gateway asks vLLM for a usage chunk on every streaming request. It hides
that chunk from the client when the client did not ask for it.
"""

from __future__ import annotations

import asyncio
import copy
import json
import re
from collections.abc import AsyncIterator, Callable

import httpx
from starlette.responses import JSONResponse, Response, StreamingResponse

_BOUNDARY = re.compile(rb"\r?\n\r?\n")

ENGINE_UNREACHABLE = {
    "error": {
        "message": "the model engine is not reachable",
        "type": "service_unavailable",
        "code": "engine_unreachable",
    }
}


def prepare_body(body: dict) -> tuple[dict, bool]:
    """Return a copy of `body` that asks vLLM for usage on streaming requests.

    The second value says whether the client already asked for the usage chunk.
    A request without streaming comes back unchanged with `True`.
    """
    if not body.get("stream"):
        return body, True
    out = copy.deepcopy(body)
    options = out.get("stream_options")
    if not isinstance(options, dict):
        options = {}
    wanted = bool(options.get("include_usage"))
    options["include_usage"] = True
    out["stream_options"] = options
    return out, wanted


def prompt_of(body: dict) -> dict:
    """Return the prompt part of a request body, for the audit log."""
    for key in ("messages", "prompt", "input"):
        if key in body:
            return {key: body[key]}
    return {}


class StreamUsageFilter:
    """Read the usage object from an SSE stream and hide it when not wanted.

    `feed` takes the raw bytes in any chunking. It works on whole events and
    keeps the incomplete tail until the next call.
    """

    def __init__(self, keep_usage_chunk: bool) -> None:
        self.keep_usage_chunk = keep_usage_chunk
        self.usage: dict | None = None
        self._buffer = b""

    def feed(self, data: bytes) -> bytes:
        """Return the bytes to send to the client for the complete events in `data`."""
        self._buffer += data
        out = []
        position = 0
        for match in _BOUNDARY.finditer(self._buffer):
            event = self._buffer[position:match.start()]
            out.append(self._process(event, match.group()))
            position = match.end()
        self._buffer = self._buffer[position:]
        return b"".join(out)

    def flush(self) -> bytes:
        """Return the bytes still in the buffer."""
        tail, self._buffer = self._buffer, b""
        return tail

    def _process(self, event: bytes, separator: bytes) -> bytes:
        lines = event.split(b"\n")
        for index, line in enumerate(lines):
            line_text = line.rstrip(b"\r")
            if not line_text.startswith(b"data:"):
                continue
            payload = line_text[5:].strip()
            if payload == b"[DONE]":
                continue
            try:
                obj = json.loads(payload)
            except ValueError:
                continue
            if not isinstance(obj, dict) or "usage" not in obj:
                continue
            if isinstance(obj["usage"], dict):
                self.usage = obj["usage"]
                if not self.keep_usage_chunk and obj.get("choices") == []:
                    return b""
            elif obj["usage"] is None and not self.keep_usage_chunk:
                del obj["usage"]
                ending = b"\r" if line.endswith(b"\r") else b""
                lines[index] = b"data: " + json.dumps(obj, separators=(",", ":")).encode() + ending
                return b"\n".join(lines) + separator
        return event + separator


def _passthrough_headers(upstream: httpx.Response) -> dict[str, str]:
    value = upstream.headers.get("content-type")
    return {"content-type": value} if value else {}


def _unreachable() -> Response:
    return JSONResponse(ENGINE_UNREACHABLE, status_code=502)


async def forward(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    body: dict,
    on_done: Callable[[int, dict | None], None],
) -> Response:
    """Send `body` to the engine and return its answer.

    `on_done(status, usage)` runs exactly once per call: when the answer is
    complete, when the client disconnects (status 499) or when the engine fails
    (status 502).
    """
    prepared, usage_wanted = prepare_body(body)
    streaming = bool(prepared.get("stream"))
    url = base_url + path
    try:
        if not streaming:
            upstream = await client.post(url, json=prepared)
        else:
            request = client.build_request("POST", url, json=prepared)
            upstream = await client.send(request, stream=True)
    except httpx.TransportError:
        on_done(502, None)
        return _unreachable()

    if not streaming:
        usage = None
        if upstream.is_success:
            try:
                payload = upstream.json()
            except ValueError:
                payload = None
            if isinstance(payload, dict) and isinstance(payload.get("usage"), dict):
                usage = payload["usage"]
        on_done(upstream.status_code, usage)
        return Response(upstream.content, upstream.status_code, _passthrough_headers(upstream))

    if not upstream.is_success:
        try:
            content = await upstream.aread()
        except httpx.HTTPError:
            await upstream.aclose()
            on_done(502, None)
            return _unreachable()
        await upstream.aclose()
        on_done(upstream.status_code, None)
        return Response(content, upstream.status_code, _passthrough_headers(upstream))

    usage_filter = StreamUsageFilter(keep_usage_chunk=usage_wanted)

    async def events() -> AsyncIterator[bytes]:
        status, usage = 200, None
        try:
            async for chunk in upstream.aiter_bytes():
                out = usage_filter.feed(chunk)
                if out:
                    yield out
            tail = usage_filter.flush()
            if tail:
                yield tail
            usage = usage_filter.usage
        except (asyncio.CancelledError, GeneratorExit):
            status = 499
            raise
        except httpx.HTTPError:
            status = 502
        finally:
            on_done(status, usage if status == 200 else None)
            await upstream.aclose()

    return StreamingResponse(
        events(),
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "text/event-stream"),
    )
