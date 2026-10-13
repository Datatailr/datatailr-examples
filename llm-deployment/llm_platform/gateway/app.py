"""LLM Gateway: one OpenAI-compatible endpoint in front of every deployed LLM.

Clients call ``/v1/chat/completions`` (and friends) with ``model`` set to a
deployment slug or Hugging Face model id. The gateway resolves the model to a
running vLLM service from the shared registry, proxies the request (including
SSE streaming), and logs the request/reply as Parquet to blob storage so the
admin app can query it with DuckDB.

Identity comes from the ``X-Datatailr-User`` header the platform sets after it
validated the caller's session cookie or API token.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, AsyncIterator

import httpx
from datatailr import User
from datatailr.logging import DatatailrLogger
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from llm_platform.common.naming import GATEWAY_NAME, gateway_internal_name
from llm_platform.common.request_log import ParquetLogWriter, RequestRecord
from llm_platform.common.schemas import Deployment
from llm_platform.gateway.router import ModelRouter

log = DatatailrLogger(__name__).get_logger()

LOG_BODIES = os.environ.get("LOG_BODIES", "true").lower() in {"1", "true", "yes"}
MAX_LOGGED_CHARS = int(os.environ.get("LOG_MAX_CHARS", "20000"))
UPSTREAM_TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "600"))

app = FastAPI(title=GATEWAY_NAME, docs_url=None, redoc_url=None)
router = ModelRouter()
writer = ParquetLogWriter()
_http: httpx.AsyncClient | None = None

HOP_HEADERS = {"host", "content-length", "connection", "transfer-encoding", "accept-encoding", "cookie"}
PASS_THROUGH_PATHS = {
    "/v1/chat/completions",
    "/v1/completions",
    "/v1/embeddings",
    "/v1/messages",
    "/v1/responses",
    "/tokenize",
    "/detokenize",
}


@app.on_event("startup")
async def _startup() -> None:
    global _http
    # No keep-alive to upstreams: inter-job calls go through the container's
    # transparent auth-proxy, which injects the caller identity only into the
    # first request of a TCP connection. A reused connection would reach the
    # vLLM service unauthenticated and be rejected.
    _http = httpx.AsyncClient(
        timeout=httpx.Timeout(UPSTREAM_TIMEOUT, connect=10.0),
        limits=httpx.Limits(max_keepalive_connections=0, max_connections=200),
        headers={"connection": "close"},
    )
    writer.start()
    router.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    writer.stop()
    if _http:
        await _http.aclose()


# ---- helpers -----------------------------------------------------------------


def _user_name(request: Request) -> str:
    try:
        user = User.from_request(request)
        return user.name if user else ""
    except Exception:  # noqa: BLE001
        return ""


def _truncate(text: str | None) -> str | None:
    if text is None:
        return None
    return text if len(text) <= MAX_LOGGED_CHARS else text[:MAX_LOGGED_CHARS] + "…[truncated]"


def _upstream_base(dep: Deployment) -> str:
    env = dep.spec.environment
    return f"{dep.internal_url}/job/{env}/{dep.internal_name}"


def _forward_headers(request: Request) -> dict[str, str]:
    """Headers for the upstream call.

    Only content negotiation headers are forwarded. Identity headers from the
    ingress (``X-Datatailr-*``) must not be passed on: the gateway calls the vLLM
    service as itself and the container's auth-proxy injects that identity.
    """
    headers: dict[str, str] = {}
    for key, value in request.headers.items():
        lower = key.lower()
        if lower in HOP_HEADERS or lower.startswith(("x-datatailr-", "x-forwarded-", "x-real-ip", "authorization")):
            continue
        if lower in {"accept", "user-agent", "x-request-id"} or lower.startswith("openai-"):
            headers[key] = value
    headers["content-type"] = "application/json"
    return headers


def _extract_text(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    parts = []
    for choice in choices:
        msg = choice.get("message") or {}
        if msg.get("content"):
            parts.append(str(msg["content"]))
        elif choice.get("text"):
            parts.append(str(choice["text"]))
    if not parts and payload.get("content"):  # anthropic messages shape
        for block in payload["content"]:
            if isinstance(block, dict) and block.get("text"):
                parts.append(block["text"])
    if not parts and payload.get("data"):  # embeddings
        return f"[{len(payload['data'])} embedding vectors]"
    return "".join(parts)


def _extract_usage(payload: dict[str, Any]) -> tuple[int | None, int | None]:
    usage = payload.get("usage") or {}
    prompt = usage.get("prompt_tokens", usage.get("input_tokens"))
    completion = usage.get("completion_tokens", usage.get("output_tokens"))
    return prompt, completion


# ---- endpoints -----------------------------------------------------------------


@app.get("/health")
async def health() -> Response:
    return Response("OK\n", media_type="text/plain")


@app.get("/")
async def index() -> dict[str, Any]:
    return {
        "service": gateway_internal_name(),
        "openai_base_path": "/v1",
        "models": [m["id"] for m in router.models()],
    }


@app.get("/v1/whoami")
async def whoami(request: Request) -> dict[str, Any]:
    name = _user_name(request)
    if not name:
        raise HTTPException(status_code=401, detail="No Datatailr identity on request")
    return {"user": name, "models": [m["id"] for m in router.models()]}


@app.get("/v1/models")
async def list_models(request: Request) -> dict[str, Any]:
    return {"object": "list", "data": router.models()}


@app.get("/v1/models/{model_id:path}")
async def get_model(model_id: str) -> dict[str, Any]:
    for model in router.models():
        if model["id"] == model_id:
            return model
    raise HTTPException(status_code=404, detail=f"Unknown model {model_id!r}")


@app.api_route("/v1/{path:path}", methods=["POST"])
async def proxy(path: str, request: Request) -> Response:
    full_path = f"/v1/{path}"
    if full_path not in PASS_THROUGH_PATHS:
        raise HTTPException(status_code=404, detail=f"Unsupported endpoint {full_path}")
    return await _proxy(full_path, request)


@app.api_route("/{path:path}", methods=["POST"])
async def proxy_misc(path: str, request: Request) -> Response:
    full_path = f"/{path}"
    if full_path not in PASS_THROUGH_PATHS:
        raise HTTPException(status_code=404, detail=f"Unsupported endpoint {full_path}")
    return await _proxy(full_path, request)


async def _proxy(path: str, request: Request) -> Response:
    assert _http is not None
    started = time.time()
    record = RequestRecord(
        request_id=f"req_{uuid.uuid4().hex[:20]}",
        user=_user_name(request),
        endpoint=path,
        client=request.headers.get("user-agent", "")[:200],
    )
    raw = await request.body()
    try:
        payload = json.loads(raw or b"{}")
        if not isinstance(payload, dict):
            raise ValueError("body must be a JSON object")
    except ValueError as exc:
        return _fail(record, started, 400, f"Invalid JSON body: {exc}")

    model = str(payload.get("model") or "")
    record.model = model
    record.stream = bool(payload.get("stream"))
    if LOG_BODIES:
        record.request_json = _truncate(json.dumps(payload, ensure_ascii=False))

    dep = router.resolve(model)
    if dep is None:
        available = ", ".join(sorted(router.aliases())) or "none"
        return _fail(record, started, 404, f"Model {model!r} is not running. Available models: {available}")
    record.deployment = dep.slug

    if record.stream and path in {"/v1/chat/completions", "/v1/completions"}:
        opts = dict(payload.get("stream_options") or {})
        opts.setdefault("include_usage", True)
        payload["stream_options"] = opts

    url = f"{_upstream_base(dep)}{path}"
    headers = _forward_headers(request)
    body = json.dumps(payload).encode("utf-8")

    try:
        if record.stream:
            upstream_req = _http.build_request("POST", url, content=body, headers=headers)
            upstream = await _http.send(upstream_req, stream=True)
            if upstream.status_code >= 400:
                text = (await upstream.aread()).decode("utf-8", errors="replace")
                await upstream.aclose()
                return _fail(record, started, upstream.status_code, text, passthrough=True)
            return StreamingResponse(
                _stream(upstream, record, started),
                status_code=upstream.status_code,
                media_type=upstream.headers.get("content-type", "text/event-stream"),
                headers={"x-request-id": record.request_id, "cache-control": "no-cache"},
            )
        upstream = await _http.post(url, content=body, headers=headers)
    except httpx.HTTPError as exc:
        return _fail(record, started, 502, f"Upstream {dep.internal_name} unreachable: {exc}")

    record.status = upstream.status_code
    record.latency_ms = int((time.time() - started) * 1000)
    content = upstream.content
    if upstream.status_code < 400:
        try:
            data = upstream.json()
            record.prompt_tokens, record.completion_tokens = _extract_usage(data)
            if LOG_BODIES:
                record.response_text = _truncate(_extract_text(data))
        except ValueError:
            pass
    else:
        record.error = _truncate(content.decode("utf-8", errors="replace"))
    writer.append(record)
    media = upstream.headers.get("content-type", "application/json")
    return Response(content, status_code=upstream.status_code, media_type=media, headers={"x-request-id": record.request_id})


async def _stream(upstream: httpx.Response, record: RequestRecord, started: float) -> AsyncIterator[bytes]:
    text_parts: list[str] = []
    buffer = ""
    try:
        async for chunk in upstream.aiter_bytes():
            yield chunk
            if not LOG_BODIES and record.prompt_tokens is not None:
                continue
            buffer += chunk.decode("utf-8", errors="replace")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if not data or data == "[DONE]":
                    continue
                try:
                    event = json.loads(data)
                except ValueError:
                    continue
                if event.get("usage"):
                    record.prompt_tokens, record.completion_tokens = _extract_usage(event)
                for choice in event.get("choices") or []:
                    delta = choice.get("delta") or {}
                    if delta.get("content"):
                        text_parts.append(str(delta["content"]))
                    elif choice.get("text"):
                        text_parts.append(str(choice["text"]))
                # Anthropic-style streaming
                if event.get("type") == "content_block_delta":
                    delta = event.get("delta") or {}
                    if delta.get("text"):
                        text_parts.append(str(delta["text"]))
                if event.get("type") == "message_delta" and event.get("usage"):
                    record.completion_tokens = event["usage"].get("output_tokens")
                if event.get("type") == "message_start":
                    usage = (event.get("message") or {}).get("usage") or {}
                    record.prompt_tokens = usage.get("input_tokens")
        record.status = upstream.status_code
    except Exception as exc:  # noqa: BLE001
        record.status = 499
        record.error = f"stream interrupted: {exc}"
        raise
    finally:
        await upstream.aclose()
        record.latency_ms = int((time.time() - started) * 1000)
        if LOG_BODIES:
            record.response_text = _truncate("".join(text_parts))
        writer.append(record)


def _fail(record: RequestRecord, started: float, status: int, message: str, passthrough: bool = False) -> Response:
    record.status = status
    record.latency_ms = int((time.time() - started) * 1000)
    record.error = _truncate(message)
    writer.append(record)
    if passthrough:
        try:
            return JSONResponse(json.loads(message), status_code=status, headers={"x-request-id": record.request_id})
        except ValueError:
            pass
    return JSONResponse(
        {"error": {"message": message, "type": "gateway_error", "code": status}},
        status_code=status,
        headers={"x-request-id": record.request_id},
    )


def main(port):
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(port), log_level="info", access_log=False)


if __name__ == "__main__":
    main(int(os.environ.get("PORT", "8000")))
