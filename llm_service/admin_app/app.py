"""The admin app: a page and a JSON API that calls the LLM services of the registry."""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from llm_service.admin_app.chat import ChatError, chat_request
from llm_service.admin_app.client import ServiceClient, ServiceUnreachable
from llm_service.admin_app.registry import BlobManualStore, Registry, platform_listing, valid_name
from llm_service.admin_app.usage_merge import SUM_FIELDS, merge_rows, own_rows
from llm_service.gateway.identity import user_from_headers

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).resolve().parent / "static"
TIMEOUT_SECONDS = 30.0
# The total limit of the usage call of one service. A slow service must not hold the merged answer.
USAGE_TIMEOUT_SECONDS = 10.0
DAY_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
# A switch or a delete can wait for the weight cache lock while another model downloads.
ACTION_TIMEOUT_SECONDS = 300.0
# One limit for the whole chat answer, and the timeout of each step of the call.
CHAT_TIMEOUT_SECONDS = 300.0
MAX_ERROR_BODY_BYTES = 64 * 1024
MAX_VIEWER_LENGTH = 256
ROLE_TIMEOUT_SECONDS = 3.0
ROLE_CACHE_SECONDS = 30.0
ROLE_ERROR_CACHE_SECONDS = 5.0
NO_ANSWER = "The service did not answer."
READY_WAIT_SECONDS = 10.0
BAD_NAME = "The service name has 1 to 63 characters from a-z, 0-9 and -."
UNREACHABLE = "The LLM service did not give a valid answer."
USAGE_GROUPS = ("user", "model", "day", "service")
MODEL_BY_DAY = "The model filter by day needs an admin of the service."
UNKNOWN_SERVICE = "This service is unknown."
USAGE_TOO_SLOW = "The LLM service did not answer in time."
TOO_SLOW = "The LLM service did not answer in time. The action can still finish. Use Reload to see the state."


class NeedsAdmin(Exception):
    """The request needs an admin of the service."""


def _clock() -> float:
    return time.monotonic()


def _valid_day(value: str | None) -> bool:
    """Return True for None and for a real day in the form YYYY-MM-DD."""
    if value is None:
        return True
    if DAY_PATTERN.fullmatch(value) is None:
        return False
    try:
        datetime.date.fromisoformat(value)
    except ValueError:
        return False
    return True


class NoCacheStaticFiles(StaticFiles):
    """Static files that the browser checks with the app before each use."""

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache"
        return response


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

    Return None when a segment is empty, `.` or `..`. The HTTP client
    removes such a segment and reach a route outside the prefix.
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

        A refused acting user means no access. A failed call, a timeout or an
        answer other than 200 gives `{"access": None, "admin": False, "error": text}`.
        The cache keeps an answer for 30 seconds and an error for 5 seconds.
        """
        key = (viewer, name)
        now = _clock()
        hit = role_cache.get(key)
        if hit is not None:
            limit = ROLE_ERROR_CACHE_SECONDS if hit[1]["access"] is None else ROLE_CACHE_SECONDS
            if now - hit[0] < limit:
                return dict(hit[1])
        result = {"access": None, "admin": False, "error": NO_ANSWER}
        try:
            # The httpx timeout applies to each step. The total limit stops a service that sends slowly.
            status, body = await asyncio.wait_for(
                service.request(name, "GET", "/v1/service", viewer, timeout=ROLE_TIMEOUT_SECONDS),
                ROLE_TIMEOUT_SECONDS,
            )
        except ServiceUnreachable:
            pass
        except asyncio.TimeoutError:
            log.warning("The role call to the service %s passed the time limit.", name)
        except Exception:  # one bad service must not fail the list. A cancel passes through.
            log.exception("The role call to the service %s failed.", name)
        else:
            if status == 200 and isinstance(body, dict):
                if body.get("acting_user") == "refused":
                    result = {"access": False, "admin": False}
                else:
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

        `action` marks a switch or a delete. It waits longer. Its timeout message
        says that the action can still finish.
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
            row.update(access=role["access"], admin=role["admin"])
            if role["access"] is None:
                row["role_error"] = role["error"]
            services.append(row)
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

    @app.post("/api/services/{name}/chat")
    async def chat(name: str, request: Request):
        """Send the prompt of the viewer to the service and pass the event stream back."""
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        refused = name_error(name)
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
        try:
            built = chat_request(body)
        except ChatError as error:
            return _error(400, error.code, error.message, "invalid_request_error")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + CHAT_TIMEOUT_SECONDS
        stack = contextlib.AsyncExitStack()
        try:
            response = await stack.enter_async_context(
                service.stream(name, "/v1/chat/completions", viewer, json=built, timeout=CHAT_TIMEOUT_SECONDS)
            )
            if response.status_code != 200:
                if response.status_code < 400:
                    await stack.aclose()
                    return _error(502, "service_unreachable", UNREACHABLE)
                try:
                    error_bytes = b""
                    async for part in response.aiter_bytes():
                        error_bytes += part
                        if len(error_bytes) > MAX_ERROR_BODY_BYTES:
                            raise ValueError("The error body is too large.")
                    error_body = json.loads(error_bytes)
                except (httpx.HTTPError, ValueError):
                    await stack.aclose()
                    return _error(502, "service_unreachable", UNREACHABLE)
                await stack.aclose()
                return JSONResponse(error_body, status_code=response.status_code)
        except ServiceUnreachable:
            await stack.aclose()
            return _error(502, "service_unreachable", UNREACHABLE)
        except BaseException:
            await stack.aclose()
            raise

        async def chunks():
            chunk_iterator = response.aiter_raw().__aiter__()
            try:
                while True:
                    left = deadline - loop.time()
                    if left <= 0:
                        log.warning("The chat answer of the service %s passed the time limit.", name)
                        return
                    try:
                        chunk = await asyncio.wait_for(chunk_iterator.__anext__(), left)
                    except StopAsyncIteration:
                        return
                    except asyncio.TimeoutError:
                        log.warning("The chat answer of the service %s passed the time limit.", name)
                        return
                    except httpx.HTTPError:
                        log.warning("The chat stream of the service %s broke.", name, exc_info=True)
                        return
                    yield chunk
            finally:
                # Also runs when the client is gone and the server cancels or closes the generator.
                await chunk_iterator.aclose()
                await stack.aclose()

        # The background task closes the call when the response never starts to stream.
        return StreamingResponse(
            chunks(),
            media_type="text/event-stream",
            # X-Accel-Buffering asks a proxy at the platform edge to pass each event at once.
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            background=BackgroundTask(stack.aclose),
        )

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
            if row.get("last_request") is not None and not isinstance(row["last_request"], str):
                raise ValueError("bad last_request")
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
            raise NeedsAdmin
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
        # The own numbers have no model split by day, so the call cannot help.
        if not admin and params["model"] is not None and group_by == "day":
            return None, MODEL_BY_DAY
        try:
            limit = USAGE_TIMEOUT_SECONDS
            if admin:
                asked = {**params, "group_by": "day" if group_by == "service" else group_by}
                call = service.request(name, "GET", "/admin/usage", viewer, params=asked, timeout=limit)
            else:
                asked = {"since": params["since"], "until": params["until"]}
                call = service.request(name, "GET", "/v1/usage/me", viewer, params=asked, timeout=limit)
            status, body = await asyncio.wait_for(call, limit)
            if status != 200:
                return None, UNREACHABLE
            if admin:
                return usage_rows(body, group_by), None
            return own_usage(body, viewer, group_by, params["user"], params["model"]), None
        except asyncio.TimeoutError:
            return None, USAGE_TOO_SLOW
        except ServiceUnreachable:
            return None, UNREACHABLE
        except NeedsAdmin:
            return None, MODEL_BY_DAY
        except (ValueError, KeyError, TypeError, AttributeError):
            log.warning("The service %s sent a usage answer with a bad shape.", name, exc_info=True)
            return None, UNREACHABLE
        except Exception:  # one bad service must not fail the others. A cancel passes through.
            log.exception("The usage call to the service %s failed.", name)
            return None, UNREACHABLE

    @app.get("/api/usage")
    async def usage(request: Request) -> JSONResponse:
        """Return the usage of the services, merged.

        A viewer who is not an admin of a service gets the own numbers of that
        service. With such a viewer, a model filter by day cannot apply. That
        service fails with a message and the others still count. The answer is
        502 only when no service gives numbers. A named service that the
        registry shows with a state other than `ok` appears with `ok` false and
        gets no call. A service whose role call fails also appears with `ok`
        false. A service without access for the viewer stays out.
        """
        viewer = viewer_or_error(request)
        if isinstance(viewer, JSONResponse):
            return viewer
        group_by = request.query_params.get("group_by", "user")
        if group_by not in USAGE_GROUPS:
            return _error(
                400, "invalid_group_by", "group_by is user, model, day or service.", "invalid_request_error"
            )
        params = {name: request.query_params.get(name) or None for name in ("since", "until", "user", "model")}
        if not (_valid_day(params["since"]) and _valid_day(params["until"])):
            return _error(
                400, "invalid_time", "since and until are real days in the form YYYY-MM-DD.", "invalid_request_error"
            )
        raw = request.query_params.get("services")
        skipped: dict[str, str] = {}
        if raw:
            wanted = raw.split(",")
            if not all(valid_name(n) for n in wanted):
                return _error(400, "invalid_service", BAD_NAME, "invalid_request_error")
            shown = {e.name: e for e in registry.entries()}
            if any(n not in shown for n in wanted):
                return _error(400, "invalid_service", UNKNOWN_SERVICE, "invalid_request_error")
            entries = [shown[n] for n in shown if n in set(wanted)]
            skipped = {e.name: f"The service has the state {e.state}." for e in entries if e.state != "ok"}
            entries = [e for e in entries if e.state == "ok"]
        else:
            entries = [e for e in registry.entries() if e.state == "ok"]
        found = await asyncio.gather(*(roles(viewer, e.name) for e in entries))
        chosen = [(e.name, role["admin"]) for e, role in zip(entries, found) if role["access"]]
        # A service whose role call failed shows with its error. Only a service without access stays out.
        for entry, role in zip(entries, found):
            if role["access"] is None:
                skipped[entry.name] = role["error"]
        per_service = {}
        results = await asyncio.gather(*(one_usage(viewer, n, admin, group_by, params) for n, admin in chosen))
        rows_of = {}
        for (name, admin), (rows, error) in zip(chosen, results):
            row = {"name": name, "scope": "all" if admin else "own", "ok": error is None}
            if error is not None:
                row["error"] = error
            else:
                per_service[name] = rows
            rows_of[name] = row
        for name, text in skipped.items():
            rows_of[name] = {"name": name, "scope": "own", "ok": False, "error": text}
        services = [rows_of[e.name] for e in registry.entries() if e.name in rows_of]
        if chosen and not per_service:
            return _error(502, "service_unreachable", UNREACHABLE)
        return JSONResponse({"data": merge_rows(per_service, group_by), "services": services})

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})

    # The browser checks the page and its files on each load, so a new version shows at once.
    app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")
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
