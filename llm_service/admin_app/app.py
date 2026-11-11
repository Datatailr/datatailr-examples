"""The admin app: a page and a JSON API that calls the LLM service."""

from __future__ import annotations

import json
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


def _error(status: int, code: str, message: str, kind: str = "server_error") -> JSONResponse:
    return JSONResponse({"error": {"message": message, "type": kind, "code": code}}, status_code=status)


def _reply(result: tuple[int, object]) -> JSONResponse:
    status, body = result
    return JSONResponse(body, status_code=status)


def _query(request: Request, names: tuple[str, ...]) -> dict:
    """Return the listed query params that the request carries."""
    return {name: request.query_params.get(name) for name in names}


def _service_path(prefix: str, model_id: str) -> str:
    """Build a service path and encode each segment once, so the slashes stay."""
    return prefix + "/".join(quote(segment, safe="") for segment in model_id.split("/"))


def create_app(service_url: str, http: httpx.AsyncClient | None = None) -> FastAPI:
    """Build the admin app for the service at `service_url`."""
    owned = http is None
    http = http or httpx.AsyncClient(timeout=TIMEOUT_SECONDS)
    service = ServiceClient(service_url, http)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        if owned:
            await http.aclose()

    app = FastAPI(title="LLM Service Admin", lifespan=lifespan)

    async def call(request: Request, method: str, path: str, **options) -> JSONResponse:
        """Call the service for the viewer of the request. Answer 401 without a viewer."""
        viewer = user_from_headers(request.headers)
        if viewer is None:
            return _error(401, "missing_user", "The request has no platform user.", "authentication_error")
        try:
            return _reply(await service.request(method, path, viewer, **options))
        except ServiceUnreachable:
            return _error(502, "service_unreachable", "The LLM service did not give a valid answer.")

    @app.get("/health")
    async def health() -> PlainTextResponse:
        return PlainTextResponse("OK")

    @app.get("/api/me")
    async def me(request: Request) -> JSONResponse:
        response = await call(request, "GET", "/admin/model")
        if response.status_code == 403:
            return JSONResponse({"user": user_from_headers(request.headers), "admin": False})
        if response.status_code == 200:
            return JSONResponse({"user": user_from_headers(request.headers), "admin": True})
        return response

    @app.get("/api/status")
    async def status(request: Request) -> JSONResponse:
        engine = await call(request, "GET", "/admin/model")
        if engine.status_code != 200:
            return engine
        models = await call(request, "GET", "/v1/models")
        if models.status_code != 200:
            return models
        return JSONResponse(
            {"engine": json.loads(engine.body), "models": json.loads(models.body).get("data", [])}
        )

    @app.post("/api/model")
    async def switch_model(request: Request) -> JSONResponse:
        try:
            body = await request.json()
        except ValueError:
            return _error(400, "invalid_json", "The request body is not valid JSON.", "invalid_request_error")
        return await call(request, "POST", "/admin/model", json=body)

    @app.delete("/api/models/{model_id:path}")
    async def delete_model(model_id: str, request: Request) -> JSONResponse:
        return await call(
            request,
            "DELETE",
            _service_path("/admin/models/", model_id),
            params=_query(request, ("revision",)),
        )

    @app.get("/api/usage")
    async def usage(request: Request) -> JSONResponse:
        params = _query(request, ("since", "until", "user", "model", "group_by"))
        return await call(request, "GET", "/admin/usage", params=params)

    @app.get("/api/usage/{user}")
    async def usage_of_user(user: str, request: Request) -> JSONResponse:
        params = _query(request, ("since", "until"))
        return await call(request, "GET", f"/admin/usage/{quote(user, safe='')}", params=params)

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
