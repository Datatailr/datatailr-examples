"""The admin app: a page and a JSON API that calls the LLM services of the registry."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from llm_service.admin_app.client import ServiceClient, ServiceUnreachable
from llm_service.admin_app.registry import BlobManualStore, Registry, platform_listing, valid_name
from llm_service.admin_app.usage_merge import SUM_FIELDS, merge_rows, own_rows
from llm_service.gateway.identity import user_from_headers

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
TIMEOUT_SECONDS = 30.0
# A switch or a delete can wait for the weight cache lock while another model downloads.
ACTION_TIMEOUT_SECONDS = 300.0
MAX_VIEWER_LENGTH = 256
ROLE_TIMEOUT_SECONDS = 3.0
ROLE_CACHE_SECONDS = 30.0
READY_WAIT_SECONDS = 10.0
BAD_NAME = "The service name has 1 to 63 characters from a-z, 0-9 and -."
UNREACHABLE = "The LLM service did not give a valid answer."
USAGE_GROUPS = ("user", "model", "day", "service")
USAGE_TOO_SLOW = "The LLM service did not answer in time."
TOO_SLOW = "The LLM service did not answer in time. The action may still finish. Use Reload to see the state."


def _clock() -> float:
    return time.monotonic()


class UnusableViewer(Exception):
    """The platform user name cannot go into a header to the service."""


def _viewer(request: Request) -> str | None:
    """Return the stripped viewer name, or None when the request has no usable name.

    Raise `UnusableViewer` for a name that is too long, has a control
    character or is not ASCII.
    """
    name = (user_from_headers(request.headers) or "").strip()
    if not name:
        return None
    if len(name) > MAX_VIEWER_LENGTH or not name.isascii():
        raise UnusableViewer(name)
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        raise UnusableViewer(name)
    return name


def _is_json(request: Request) -> bool:
    """Return True if the request has the content type `application/json`, with or without parameters."""
    media_type = request.headers.get("content-type", "").split(";", 1)[0]
    return media_type.strip().lower() == "application/json"


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


def create_app(
    registry: Registry,
    owner: str,
    http: httpx.AsyncClient | None = None,
    refresh_interval: float | None = 60.0,
) -> FastAPI:
    """Build the admin app on top of `registry`.

    `owner` is the platform user who owns the app. An empty `owner` matches no
    viewer. The lifespan runs the registry every `refresh_interval` seconds.
    With None, the app starts no refresh task and `GET /api/services` does not
    wait for a first refresh. The caller then fills the registry itself.
    """
    owned = http is None
    # A redirect must not lead the app to another host, so follow_redirects stays False.
    http = http or httpx.AsyncClient(timeout=TIMEOUT_SECONDS, follow_redirects=False)
    service = ServiceClient(http)
    role_cache: dict[tuple[str, str], tuple[float, dict]] = {}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        task = None
        if refresh_interval is not None:
            task = asyncio.create_task(registry.run(refresh_interval))
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if owned:
                await http.aclose()

    app = FastAPI(title="LLM Service Admin", lifespan=lifespan)
    app.state.registry = registry
    app.state.owner = owner

    async def roles(viewer: str, name: str) -> dict:
        """Return `{"access": bool, "admin": bool}` of `viewer` on the service `name`.

        The answer is cached for 30 seconds for each viewer and service. A failed
        call or a refused acting user means no access.
        """
        key = (viewer, name)
        now = _clock()
        hit = role_cache.get(key)
        if hit is not None and now - hit[0] < ROLE_CACHE_SECONDS:
            return dict(hit[1])
        result = {"access": False, "admin": False}
        try:
            # The httpx timeout applies to each step. The total limit stops a service that sends slowly.
            status, body = await asyncio.wait_for(
                service.request(name, "GET", "/v1/service", viewer, timeout=ROLE_TIMEOUT_SECONDS),
                ROLE_TIMEOUT_SECONDS,
            )
        except ServiceUnreachable:
            pass
        except Exception:  # one bad service must not fail the list. A cancel passes through.
            log.exception("The role call to the service %s failed.", name)
        else:
            if status == 200 and isinstance(body, dict) and body.get("acting_user") != "refused":
                result = {"access": True, "admin": body.get("admin") is True}
        if len(role_cache) > 1000:
            for stale in [k for k, (at, _) in role_cache.items() if now - at >= ROLE_CACHE_SECONDS]:
                del role_cache[stale]
        role_cache[key] = (now, result)
        return dict(result)

    app.state.roles = roles

    def viewer_or_error(request: Request) -> str | JSONResponse:
        try:
            viewer = _viewer(request)
        except UnusableViewer:
            return _error(
                401, "missing_user", "The platform user name is not usable for the admin app.", "authentication_error"
            )
        if viewer is None:
            return _error(401, "missing_user", "The request has no platform user.", "authentication_error")
        return viewer

    def owner_error(viewer: str) -> JSONResponse | None:
        if not owner or viewer != owner:
            return _error(
                403, "owner_required", "Only the owner of the app can change the service list.", "permission_error"
            )
        return None

    def name_error(name: str) -> JSONResponse | None:
        """Return the error for a name that is not a usable service, or None."""
        if not valid_name(name):
            return _error(400, "invalid_service", BAD_NAME, "invalid_request_error")
        if registry.get(name) is None:
            return _error(
                404, "unknown_service", "This service is not in the list of usable services.", "invalid_request_error"
            )
        return None

    async def fetch(
        request: Request, name: str, method: str, path: str, action: bool = False, **options
    ) -> tuple[int, object] | JSONResponse:
        """Call the service `name` for the viewer. Return the answer, or an error response to send.

        `action` marks a switch or a delete. It waits longer, and a timeout says
        that the action may still finish.
        """
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        refused = name_error(name)
        if refused is not None:
            return refused
        try:
            timeout = ACTION_TIMEOUT_SECONDS if action else TIMEOUT_SECONDS
            return await service.request(name, method, path, viewer, timeout=timeout, **options)
        except ServiceUnreachable as error:
            return _error(502, "service_unreachable", TOO_SLOW if action and error.sent else UNREACHABLE)

    async def call(request: Request, name: str, method: str, path: str, **options) -> JSONResponse:
        """Call the service and pass its status and body through."""
        result = await fetch(request, name, method, path, **options)
        return result if isinstance(result, JSONResponse) else _reply(result)

    @app.get("/health")
    async def health() -> PlainTextResponse:
        return PlainTextResponse("OK")

    @app.get("/api/services")
    async def list_services(request: Request) -> JSONResponse:
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        if refresh_interval is not None:  # without a refresh task, nothing sets the ready event
            await registry.wait_ready(READY_WAIT_SECONDS)
        entries = registry.entries()
        found = await asyncio.gather(*(roles(viewer, e.name) for e in entries if e.state == "ok"))
        found = iter(found)
        services = []
        for entry in entries:
            role = next(found) if entry.state == "ok" else {"access": False, "admin": False}
            row = entry.to_dict()
            del row["checked_at"]
            services.append({**row, **role})
        return JSONResponse(
            {
                "user": viewer,
                "owner": bool(owner) and viewer == owner,
                "listing": registry.listing_state,
                "services": services,
            }
        )

    @app.post("/api/services")
    async def add_service(request: Request) -> JSONResponse:
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        refused = owner_error(viewer)
        if refused is not None:
            return refused
        # A form on another site cannot send this content type without a CORS preflight.
        if not _is_json(request):
            return _error(
                415, "unsupported_media_type", "The request body must be application/json.", "invalid_request_error"
            )
        try:
            body = await request.json()
        except ValueError:
            return _error(400, "invalid_json", "The request body is not valid JSON.", "invalid_request_error")
        name = body.get("name") if isinstance(body, dict) else None
        if not valid_name(name):
            return _error(400, "invalid_service", BAD_NAME, "invalid_request_error")
        try:
            entry = await registry.add_manual(name)
        except ValueError:
            return _error(400, "invalid_service", BAD_NAME, "invalid_request_error")
        except Exception:
            log.exception("The manual service list failed.")
            return _error(502, "store_unavailable", "The list of added services is not available.")
        row = entry.to_dict()
        del row["checked_at"]
        return JSONResponse(row)

    @app.delete("/api/services/{name}")
    async def remove_service(name: str, request: Request) -> JSONResponse:
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        refused = owner_error(viewer)
        if refused is not None:
            return refused
        if not valid_name(name):
            return _error(400, "invalid_service", BAD_NAME, "invalid_request_error")
        try:
            removed = await registry.remove_manual(name)
        except Exception:
            log.exception("The manual service list failed.")
            return _error(502, "store_unavailable", "The list of added services is not available.")
        if not removed:
            return _error(404, "unknown_service", "The name is not in the list of added services.", "invalid_request_error")
        return JSONResponse({"removed": name})

    @app.get("/api/services/{name}/status")
    async def status(name: str, request: Request) -> JSONResponse:
        engine = await fetch(request, name, "GET", "/admin/model")
        if isinstance(engine, JSONResponse) or engine[0] != 200:
            return engine if isinstance(engine, JSONResponse) else _reply(engine)
        models = await fetch(request, name, "GET", "/v1/models")
        if isinstance(models, JSONResponse) or models[0] != 200:
            return models if isinstance(models, JSONResponse) else _reply(models)
        if not isinstance(models[1], dict):
            return _error(502, "service_unreachable", UNREACHABLE)
        return JSONResponse({"engine": engine[1], "models": models[1].get("data", [])})

    @app.get("/api/services/{name}/models")
    async def list_models(name: str, request: Request) -> JSONResponse:
        return await call(request, name, "GET", "/v1/models")

    @app.post("/api/services/{name}/model")
    async def switch_model(name: str, request: Request) -> JSONResponse:
        # A form on another site cannot send this content type without a CORS preflight.
        if not _is_json(request):
            return _error(
                415, "unsupported_media_type", "The request body must be application/json.", "invalid_request_error"
            )
        try:
            body = await request.json()
        except ValueError:
            return _error(400, "invalid_json", "The request body is not valid JSON.", "invalid_request_error")
        return await call(request, name, "POST", "/admin/model", action=True, json=body)

    @app.delete("/api/services/{name}/models/{model_id:path}")
    async def delete_model(name: str, model_id: str, request: Request) -> JSONResponse:
        path = _service_path("/admin/models/", model_id)
        if path is None:
            return _error(400, "invalid_model", "The model id is not valid.", "invalid_request_error")
        return await call(request, name, "DELETE", path, action=True, params=_query(request, ("revision",)))

    @app.get("/api/services/{name}/usage/{user}")
    async def usage_of_user(name: str, user: str, request: Request) -> JSONResponse:
        path = _service_path("/admin/usage/", user)
        if path is None or "/" in user:
            return _error(400, "invalid_user", "The user name is not valid.", "invalid_request_error")
        params = _query(request, ("since", "until"))
        return await call(request, name, "GET", path, params=params)

    @app.get("/api/services/{name}/audit")
    async def audit(name: str, request: Request) -> JSONResponse:
        params = _query(request, ("user", "model", "since", "until", "limit", "offset"))
        return await call(request, name, "GET", "/admin/audit", params=params)

    @app.get("/api/services/{name}/audit/users")
    async def audit_users(name: str, request: Request) -> JSONResponse:
        return await call(request, name, "GET", "/admin/audit/users")

    @app.get("/api/services/{name}/audit/records/{request_id:path}")
    async def audit_record(name: str, request_id: str, request: Request) -> JSONResponse:
        path = _service_path("/admin/audit/records/", request_id)
        if path is None or "/" in request_id:
            return _error(400, "invalid_record", "The record id is not valid.", "invalid_request_error")
        return await call(request, name, "GET", path, params=_query(request, ("day",)))

    def usage_rows(body: object, group_by: str) -> list[dict]:
        """Check the rows of an admin usage answer. Raise `ValueError` for a bad shape."""
        rows = body.get("data") if isinstance(body, dict) else None
        if not isinstance(rows, list):
            raise ValueError("data is not a list")
        key = "day" if group_by == "service" else group_by
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get(key), str):
                raise ValueError("bad row")
            for field in SUM_FIELDS:
                value = row.get(field)
                if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                    raise ValueError("bad count")
        return rows

    def own_usage(detail: object, viewer: str, group_by: str, user: str | None, model: str | None) -> list[dict]:
        """Build the rows of a viewer who is not an admin, and apply the filters of the request."""
        if not isinstance(detail, dict):
            raise ValueError("detail is not an object")
        if user is not None and user != viewer:
            return []
        # The detail has no totals for a model by day, so a model filter by day cannot apply.
        # The other groups take the model rows and add them up.
        if model is not None and group_by == "day":
            return []
        if model is None:
            rows = own_rows(detail, viewer, "day" if group_by == "service" else group_by)
        else:
            rows = [r for r in own_rows(detail, viewer, "model") if r.get("model") == model]
            if group_by != "model":
                key = "day" if group_by == "service" else group_by
                rows = [{**r, key: viewer if key == "user" else "all"} for r in rows]
                rows = merge_rows({"own": rows}, key)
        return usage_rows({"data": rows}, group_by)

    async def one_usage(
        viewer: str, name: str, admin: bool, group_by: str, params: dict[str, str | None]
    ) -> tuple[list[dict] | None, str | None]:
        """Return `(rows, None)` for one service, or `(None, error text)`. A cancel passes through."""
        try:
            if admin:
                asked = {**params, "group_by": "day" if group_by == "service" else group_by}
                call = service.request(name, "GET", "/admin/usage", viewer, params=asked, timeout=TIMEOUT_SECONDS)
            else:
                asked = {"since": params["since"], "until": params["until"]}
                call = service.request(name, "GET", "/v1/usage/me", viewer, params=asked, timeout=TIMEOUT_SECONDS)
            status, body = await asyncio.wait_for(call, TIMEOUT_SECONDS)
            if status != 200:
                return None, UNREACHABLE
            if admin:
                return usage_rows(body, group_by), None
            return own_usage(body, viewer, group_by, params["user"], params["model"]), None
        except asyncio.TimeoutError:
            return None, USAGE_TOO_SLOW
        except ServiceUnreachable:
            return None, UNREACHABLE
        except (ValueError, KeyError, TypeError, AttributeError):
            return None, UNREACHABLE
        except Exception:  # one bad service must not fail the others. A cancel passes through.
            log.exception("The usage call to the service %s failed.", name)
            return None, UNREACHABLE

    @app.get("/api/usage")
    async def usage(request: Request) -> JSONResponse:
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        group_by = request.query_params.get("group_by", "user")
        if group_by not in USAGE_GROUPS:
            return _error(
                400, "invalid_group_by", "group_by is user, model, day or service.", "invalid_request_error"
            )
        raw = request.query_params.get("services")
        if raw:
            wanted = raw.split(",")
            if not all(valid_name(n) and registry.get(n) is not None for n in wanted):
                return _error(400, "invalid_service", BAD_NAME, "invalid_request_error")
            entries = [e for e in registry.entries() if e.name in set(wanted)]
        else:
            entries = [e for e in registry.entries() if e.state == "ok"]
        found = await asyncio.gather(*(roles(viewer, e.name) for e in entries))
        chosen = [(e.name, role["admin"]) for e, role in zip(entries, found) if role["access"]]
        params = {name: request.query_params.get(name) or None for name in ("since", "until", "user", "model")}
        results = await asyncio.gather(*(one_usage(viewer, n, admin, group_by, params) for n, admin in chosen))
        services = []
        per_service = {}
        for (name, admin), (rows, error) in zip(chosen, results):
            row = {"name": name, "scope": "all" if admin else "own", "ok": error is None}
            if error is not None:
                row["error"] = error
            else:
                per_service[name] = rows
            services.append(row)
        if chosen and not per_service:
            return _error(502, "service_unreachable", UNREACHABLE)
        return JSONResponse({"data": merge_rows(per_service, group_by), "services": services})

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


def default_app(env) -> FastAPI:
    """Build the app from environment variables. Make no network call and no blob call."""
    configured = [name.strip() for name in env.get("LLM_SERVICES", "").split(",")]
    configured = [name for name in configured if valid_name(name)]
    environment = env.get("DATATAILR_JOB_ENVIRONMENT", "")
    # The client lives as long as the process. A redirect must not lead the app
    # to another host, so follow_redirects stays False.
    http = httpx.AsyncClient(timeout=TIMEOUT_SECONDS, follow_redirects=False)
    registry = Registry(http, configured, BlobManualStore(), lambda: platform_listing(environment))
    return create_app(registry, env.get("DATATAILR_USER", ""), http)


app = default_app(os.environ)
