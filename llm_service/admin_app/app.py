"""The admin app: a page and a JSON API that calls the LLM service."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from llm_service.admin_app.client import ServiceClient, ServiceUnreachable
from llm_service.gateway.identity import user_from_headers

STATIC_DIR = Path(__file__).resolve().parent / "static"
TIMEOUT_SECONDS = 30.0
MAX_VIEWER_LENGTH = 256


def _viewer(request: Request) -> str | None:
    """Return the viewer name, or None when it is missing or not usable in a header."""
    name = user_from_headers(request.headers)
    if name is None or len(name) > MAX_VIEWER_LENGTH:
        return None
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        return None
    return name


def _error(status: int, code: str, message: str, kind: str = "server_error") -> JSONResponse:
    return JSONResponse({"error": {"message": message, "type": kind, "code": code}}, status_code=status)


def _reply(result: tuple[int, object]) -> JSONResponse:
    status, body = result
    return JSONResponse(body, status_code=status)


def _query(request: Request, names: tuple[str, ...]) -> dict:
    """Return the listed query params that the request carries."""
    return {name: request.query_params.get(name) for name in names}


def _service_path(prefix: str, value: str) -> str | None:
    """Build a service path and encode each segment once, so the slashes stay.

    Return None when a segment is empty, `.` or `..`. The HTTP client would
    remove such a segment and reach a route outside the prefix.
    """
    segments = value.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        return None
    return prefix + "/".join(quote(segment, safe="") for segment in segments)


def create_app(service_url: str, http: httpx.AsyncClient | None = None) -> FastAPI:
    """Build the admin app for the service at `service_url`."""
    owned = http is None
    http = http or httpx.AsyncClient(timeout=TIMEOUT_SECONDS)
    service = ServiceClient(service_url, http)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            if owned:
                await http.aclose()

    app = FastAPI(title="LLM Service Admin", lifespan=lifespan)

    async def fetch(request: Request, method: str, path: str, **options) -> tuple[int, object] | JSONResponse:
        """Call the service for the viewer. Return the answer, or an error response to send."""
        viewer = _viewer(request)
        if viewer is None:
            return _error(401, "missing_user", "The request has no platform user.", "authentication_error")
        try:
            return await service.request(method, path, viewer, **options)
        except ServiceUnreachable:
            return _error(502, "service_unreachable", "The LLM service did not give a valid answer.")

    async def call(request: Request, method: str, path: str, **options) -> JSONResponse:
        """Call the service and pass its status and body through."""
        result = await fetch(request, method, path, **options)
        return result if isinstance(result, JSONResponse) else _reply(result)

    @app.get("/health")
    async def health() -> PlainTextResponse:
        return PlainTextResponse("OK")

    @app.get("/api/me")
    async def me(request: Request) -> JSONResponse:
        result = await fetch(request, "GET", "/admin/model")
        if isinstance(result, JSONResponse):
            return result
        status = result[0]
        if status == 403:
            return JSONResponse({"user": _viewer(request), "admin": False})
        if status == 200:
            return JSONResponse({"user": _viewer(request), "admin": True})
        return _reply(result)

    @app.get("/api/status")
    async def status(request: Request) -> JSONResponse:
        engine = await fetch(request, "GET", "/admin/model")
        if isinstance(engine, JSONResponse) or engine[0] != 200:
            return engine if isinstance(engine, JSONResponse) else _reply(engine)
        models = await fetch(request, "GET", "/v1/models")
        if isinstance(models, JSONResponse) or models[0] != 200:
            return models if isinstance(models, JSONResponse) else _reply(models)
        if not isinstance(models[1], dict):
            return _error(502, "service_unreachable", "The LLM service did not give a valid answer.")
        return JSONResponse({"engine": engine[1], "models": models[1].get("data", [])})

    @app.post("/api/model")
    async def switch_model(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except ValueError:
            return _error(400, "invalid_json", "The request body is not valid JSON.", "invalid_request_error")
        return await call(request, "POST", "/admin/model", json=body)

    @app.delete("/api/models/{model_id:path}")
    async def delete_model(model_id: str, request: Request) -> JSONResponse:
        path = _service_path("/admin/models/", model_id)
        if path is None:
            return _error(400, "invalid_model", "The model id is not valid.", "invalid_request_error")
        return await call(
            request,
            "DELETE",
            path,
            params=_query(request, ("revision",)),
        )

    @app.get("/api/usage")
    async def usage(request: Request) -> JSONResponse:
        params = _query(request, ("since", "until", "user", "model", "group_by"))
        return await call(request, "GET", "/admin/usage", params=params)

    @app.get("/api/usage/{user}")
    async def usage_of_user(user: str, request: Request) -> JSONResponse:
        path = _service_path("/admin/usage/", user)
        if path is None or "/" in user:
            return _error(400, "invalid_user", "The user name is not valid.", "invalid_request_error")
        params = _query(request, ("since", "until"))
        return await call(request, "GET", path, params=params)

    @app.get("/api/audit")
    async def audit(request: Request) -> JSONResponse:
        params = _query(request, ("user", "model", "since", "until", "limit", "offset"))
        return await call(request, "GET", "/admin/audit", params=params)

    @app.get("/api/audit/users")
    async def audit_users(request: Request) -> JSONResponse:
        return await call(request, "GET", "/admin/audit/users")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


app = create_app(os.environ.get("LLM_SERVICE_URL", "http://llm-service"))
