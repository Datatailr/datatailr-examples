"""The gateway application: OpenAI routes, admin routes and `main`."""

import asyncio
import contextlib
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Callable

import httpx
from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from llm_service.gateway.audit import AuditLog, PromptUnreadable, iso, parse_time, run_flusher, utc_now
from llm_service.gateway.audit_merge import MAINTENANCE_INTERVAL, AuditMerger, run_maintenance
from llm_service.gateway.config import Config
from llm_service.gateway.engine import Engine
from llm_service.gateway.identity import AccessCheck, AdminCheck, acting_user_from_headers, user_from_headers
from llm_service.gateway.limiter import LimitExceeded, Limiter
from llm_service.gateway.limits import LimitStore
from llm_service.gateway.models import ModelInUse, ModelManager, SwitchInProgress
from llm_service.gateway.proxy import forward, prompt_of
from llm_service.gateway.storage import BlobStore
from llm_service.gateway.usage import UsageTotals
from llm_service.gateway.weights import Downloader, WeightCache, validate_model_name, validate_revision

logger = logging.getLogger("llm_service.app")

SYSTEM_USER = "system"
OPENAI_PATHS = ("/v1/chat/completions", "/v1/completions", "/v1/embeddings")
MAX_AUDIT_LIMIT = 1000
DAY_FORM = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")

SERVICE_API = "datatailr-llm-service"
SERVICE_API_VERSION = 1


class ApiError(Exception):
    """An error that becomes the OpenAI style error body."""

    def __init__(
        self,
        status: int,
        type_: str,
        code: str,
        message: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.type = type_
        self.code = code
        self.message = message
        self.headers = headers or {}


def error_response(error: ApiError) -> JSONResponse:
    """Return the JSON response of an `ApiError`."""
    body = {"error": {"message": error.message, "type": error.type, "code": error.code}}
    return JSONResponse(body, status_code=error.status, headers=error.headers)


def _check_day(name: str, value: str | None) -> str | None:
    """Return `value` if it is a day like `2026-11-10`. Raise `ApiError` otherwise."""
    if value is None:
        return None
    try:
        if not DAY_FORM.fullmatch(value):
            raise ValueError(value)
        date.fromisoformat(value)
    except ValueError:
        raise ApiError(400, "invalid_request_error", "invalid_time", f"{name} must be a day like 2026-11-10") from None
    return value


def create_app(
    config: Config,
    store: BlobStore,
    engine: Engine,
    admin: AdminCheck,
    download: Downloader,
    http: httpx.AsyncClient | None = None,
    now: Callable[[], datetime] = utc_now,
    maintenance_interval: float = MAINTENANCE_INTERVAL,
    access: AccessCheck | None = None,
) -> FastAPI:
    """Build the gateway application from its parts.

    `now` is the clock of the audit log, the merger and the usage totals. Tests replace it.
    `maintenance_interval` is the sleep in seconds between two merge and retention steps.
    `access` decides who has access to the service. Without it, only admins have access.
    """
    access = access or AccessCheck(admin, None, lambda group: [])
    owns_http = http is None
    client = http or httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10.0))
    audit = AuditLog(store, config.blob_root, config.flush_size, now=now)
    usage = UsageTotals(store, config.blob_root, audit, lambda: iso(now())[:10], now=now)
    limits = LimitStore(store, config.blob_root)

    def seconds_to_midnight() -> float:
        current = now()
        midnight = current.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return (midnight - current).total_seconds()

    limiter = Limiter(usage.tokens_today, seconds_to_midnight)
    merger = AuditMerger(store, config.blob_root, audit, now, config.prompt_retention_days)
    stopping = threading.Event()  # set before the last flusher cycle, which forces the checkpoint
    weights = WeightCache(store, config.blob_root, config.local_dir, download)

    def write_audit(record: dict) -> None:
        """Add a record to the audit log. A failure is logged and never raised."""
        try:
            record.setdefault("request_id", uuid.uuid4().hex)
            record["ts"] = iso(now())
            audit.add(record)
        except Exception as error:
            logger.warning("the write of an audit record failed: %s", error)

    def on_event(event: dict) -> None:
        failed = event["result"] != "ok"
        trigger = event.get("trigger")
        write_audit(
            {
                # Only an admin switch has an admin. Startup and reload are the system.
                "user": app.state.last_admin if trigger == "switch" else SYSTEM_USER,
                "endpoint": "/admin/model",
                "model": event["model"],
                "stream": False,
                "prompt": {
                    "event": "engine_reload" if trigger == "reload" else "model_switch",
                    "revision": event["revision"],
                    "error": event["error"],
                },
                "status": 500 if failed else 200,
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "duration_ms": None,
            }
        )

    manager = ModelManager(weights, engine, store, config.blob_root, config.default_model, on_event)

    def on_flusher_tick() -> None:
        """Run the usage steps after each flush, in the flusher thread.

        Each step has its own guard, so one failure does not stop the other. The last cycle
        at shutdown forces the checkpoint. The merge and the retention run in the
        maintenance task, so a flush never waits for their deletes.
        """
        last_cycle = stopping.is_set()
        steps = [
            ("close_days", usage.close_days),
            ("checkpoint", lambda: usage.checkpoint(force=last_cycle)),
        ]
        for name, step in steps:
            try:
                step()
            except Exception as error:
                logger.warning("the flusher step %s failed: %s", name, error)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load the totals first. Register the listeners after that, with no await
        # in between, so that no record is counted twice and no flush runs against
        # a half-loaded day.
        await asyncio.to_thread(limits.load)
        started = time.monotonic()
        days = await asyncio.to_thread(usage.load)
        logger.info("usage totals loaded in %.1f s for %d days", time.monotonic() - started, days)
        audit.listeners.append(usage.record)
        audit.flushed_listeners.append(usage.flushed)
        audit.flushed_listeners.append(merger.flushed)
        flusher = asyncio.create_task(
            run_flusher(audit, config.flush_interval, on_tick=on_flusher_tick)
        )
        maintenance = asyncio.create_task(run_maintenance(merger.tick, maintenance_interval))
        app.state.maintenance_task = maintenance
        app.state.startup_task = asyncio.create_task(manager.startup())
        try:
            yield
        finally:
            tasks = (app.state.startup_task, app.state.switch_task, app.state.reload_task)
            for task in tasks:
                if task is not None and not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            try:
                await engine.stop()
            finally:
                # Do not wait for a merge step that runs now. Its thread ends by itself.
                maintenance.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await maintenance
                stopping.set()
                flusher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await flusher
                if owns_http:
                    await client.aclose()

    app = FastAPI(title="LLM service", lifespan=lifespan)
    app.state.last_admin = SYSTEM_USER
    app.state.switch_task = None
    app.state.startup_task = None
    app.state.reload_task = None
    app.state.maintenance_task = None
    app.state.manager = manager
    app.state.audit = audit
    app.state.usage = usage
    app.state.limits = limits
    app.state.limiter = limiter

    # Error bodies

    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, error: ApiError):
        return error_response(error)

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, error: StarletteHTTPException):
        code = {404: "not_found", 405: "method_not_allowed"}.get(error.status_code, "http_error")
        type_ = "invalid_request_error" if error.status_code < 500 else "server_error"
        return error_response(ApiError(error.status_code, type_, code, str(error.detail)))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, error: RequestValidationError):
        first = error.errors()[0]
        place = ".".join(str(part) for part in first["loc"][1:])
        message = f"invalid {place}: {first['msg']}" if place else first["msg"]
        return error_response(ApiError(400, "invalid_request_error", "invalid_parameter", message))

    # Dependencies

    async def current_user(request: Request) -> str:
        user = user_from_headers(request.headers)
        if user is None:
            raise ApiError(401, "authentication_error", "missing_user", "the request has no platform user")
        return user

    async def require_admin(request: Request, user: str = Depends(current_user)) -> str:
        """Return the admin user: the acting user if the header names one, else the caller."""
        if not await asyncio.to_thread(admin.is_admin, user):
            raise ApiError(403, "permission_error", "admin_required", "this endpoint is for admins")
        try:
            acting = acting_user_from_headers(request.headers)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_acting_user", str(error)) from None
        if acting is None:
            return user
        if not await asyncio.to_thread(admin.is_admin, acting):
            raise ApiError(
                403, "permission_error", "admin_required", "the acting user is not an admin"
            )
        return acting

    async def resolve_effective(request: Request, caller: str) -> str:
        """Return the effective user for the request. Raise `ApiError` if a rule fails."""
        try:
            acting = acting_user_from_headers(request.headers)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_acting_user", str(error)) from None
        if acting is None:
            return caller
        if not await asyncio.to_thread(admin.is_admin, caller):
            raise ApiError(
                403,
                "permission_error",
                "acting_user_not_allowed",
                "only an admin can send the header X-LLM-Acting-User",
            )
        if not await asyncio.to_thread(access.has_access, acting):
            raise ApiError(403, "permission_error", "access_denied", "the acting user has no access to this service")
        return acting

    async def effective_user(request: Request, user: str = Depends(current_user)) -> str:
        """Return the acting user if the header names one with access, else the caller."""
        return await resolve_effective(request, user)

    # Platform

    @app.get("/health")
    async def health() -> PlainTextResponse:
        return PlainTextResponse("OK")

    @app.get("/v1/service")
    async def service_descriptor(request: Request, caller: str = Depends(current_user)) -> dict:
        """Describe this service. A refused acting user never fails the call."""
        refused = False
        try:
            user = await resolve_effective(request, caller)
        except ApiError:
            user, refused = caller, True
        user_admin = await asyncio.to_thread(admin.is_admin, user)
        caller_admin = user_admin if user == caller else await asyncio.to_thread(admin.is_admin, caller)
        serving = manager.serving
        body = {
            "api": SERVICE_API,
            "api_version": SERVICE_API_VERSION,
            "name": config.job_name,
            "environment": config.environment,
            "state": manager.status()["state"],
            "model": serving["model"] if serving else None,
            "user": user,
            "admin": user_admin,
            "caller_admin": caller_admin,
        }
        if refused:
            body["acting_user"] = "refused"
        return body

    # OpenAI API

    def make_handler(path: str):
        async def handler(
            request: Request, caller: str = Depends(current_user), user: str = Depends(effective_user)
        ):
            started = time.monotonic()
            request_id = uuid.uuid4().hex
            headers = {"x-request-id": request_id}
            body: dict | None = None
            model: str | None = None

            slot = False  # True while the limiter holds a slot of this request

            def release_slot() -> None:
                nonlocal slot
                if slot:
                    slot = False
                    limiter.release(user)

            def record(status: int, usage_counts: dict | None = None) -> None:
                release_slot()
                counts = usage_counts or {}
                entry = {
                    "request_id": request_id,
                    "user": user,
                    "endpoint": path,
                    "model": model,
                    "stream": bool(body.get("stream")) if body else False,
                    "prompt": prompt_of(body) if body else {},
                    "status": status,
                    "prompt_tokens": counts.get("prompt_tokens"),
                    "completion_tokens": counts.get("completion_tokens"),
                    "total_tokens": counts.get("total_tokens"),
                    "duration_ms": int((time.monotonic() - started) * 1000),
                }
                if user != caller:
                    entry["via"] = caller
                write_audit(entry)

            def refuse(error: ApiError) -> JSONResponse:
                record(error.status)
                error.headers = {**error.headers, **headers}
                return error_response(error)

            try:
                parsed = json.loads(await request.body())
            except (ValueError, RecursionError):
                parsed = None
            if not isinstance(parsed, dict):
                return refuse(
                    ApiError(400, "invalid_request_error", "invalid_json", "the request body must be a JSON object")
                )
            body = parsed
            if isinstance(body.get("model"), str):
                model = body["model"]

            reload = manager.check_engine()
            if reload is not None:
                app.state.reload_task = reload
            # During the weight fetch of a switch, the old model still serves.
            serving = manager.serving
            if serving is None and manager.state == "loading":
                return refuse(
                    ApiError(
                        503,
                        "service_unavailable",
                        "model_loading",
                        "a model is loading, try again later",
                        {"Retry-After": "30"},
                    )
                )
            if serving is None:
                return refuse(ApiError(503, "service_unavailable", "no_model", "no model is loaded"))
            active = serving["model"]
            if body.get("model") is None:
                body = {**body, "model": active}
                model = active
            elif model != active:
                return refuse(
                    ApiError(
                        404,
                        "invalid_request_error",
                        "model_not_found",
                        f"the model {body['model']!r} is not served, the served model is {active!r}",
                    )
                )

            user_admin = await asyncio.to_thread(admin.is_admin, user)
            user_limits, _ = limits.limits_for(user, user_admin)
            try:
                limiter.acquire(user, user_limits)
            except LimitExceeded as exceeded:
                return refuse(
                    ApiError(
                        429,
                        "rate_limit_error",
                        exceeded.code,
                        exceeded.message,
                        {"Retry-After": str(exceeded.retry_after)},
                    )
                )
            slot = True
            try:
                response = await forward(client, engine.base_url, path, body, record)
            except BaseException:
                release_slot()
                raise
            response.headers["x-request-id"] = request_id
            return response

        return handler

    for openai_path in OPENAI_PATHS:
        app.post(openai_path)(make_handler(openai_path))

    async def model_entries() -> list[dict]:
        """Return one `/v1/models` entry for each model id.

        The status is `loading` for the model that loads, `active` for the
        model that the engine serves now and `cached` for the rest. `revision` is the loading or
        active revision, else the first cached one. `size_bytes` is the sum of the known sizes of
        the cached revisions, or None if no cached revision has a known size.
        """
        revisions: dict[str, list[str]] = {}
        sizes: dict[str, list[int]] = {}
        for entry in await asyncio.to_thread(weights.cached):
            revisions.setdefault(entry["model"], []).append(entry["revision"])
            if entry["size_bytes"] is not None:
                sizes.setdefault(entry["model"], []).append(entry["size_bytes"])
        current: dict[str, tuple[str, str]] = {}
        if manager.serving:
            current[manager.serving["model"]] = ("active", manager.serving["revision"])
        if manager.target:
            current[manager.target["model"]] = ("loading", manager.target["revision"])
        entries = []
        for name in set(revisions) | set(current):
            cached = sorted(revisions.get(name, []))
            status, revision = current.get(name, ("cached", cached[0] if cached else None))
            entries.append(
                {
                    "id": name,
                    "object": "model",
                    "created": 0,
                    "owned_by": name.split("/")[0],
                    "status": status,
                    "revision": revision,
                    "revisions": cached,
                    "size_bytes": sum(sizes[name]) if name in sizes else None,
                }
            )
        rank = {"active": 0, "loading": 1, "cached": 2}
        return sorted(entries, key=lambda e: (rank[e["status"]], e["id"]))

    @app.get("/v1/models")
    async def list_models(user: str = Depends(effective_user)) -> dict:
        return {"object": "list", "data": await model_entries()}

    @app.get("/v1/models/{model_id:path}")
    async def get_model_entry(model_id: str, user: str = Depends(effective_user)) -> dict:
        for entry in await model_entries():
            if entry["id"] == model_id:
                return entry
        raise ApiError(404, "invalid_request_error", "model_not_found", f"the model {model_id!r} is not known")

    @app.get("/v1/usage/me")
    async def usage_me(
        since: str | None = None, until: str | None = None, user: str = Depends(effective_user)
    ) -> dict:
        _check_day("since", since)
        _check_day("until", until)
        return await asyncio.to_thread(usage.user_detail, user, since, until)

    # Admin API

    @app.get("/admin/model")
    async def get_model(admin_user: str = Depends(require_admin)) -> dict:
        return manager.status()

    @app.post("/admin/model", status_code=202)
    async def switch_model(request: Request, admin_user: str = Depends(require_admin)):
        try:
            body = json.loads(await request.body())
        except (ValueError, RecursionError):
            body = None
        if not isinstance(body, dict) or not isinstance(body.get("model"), str):
            raise ApiError(400, "invalid_request_error", "invalid_model", "the body needs a string 'model'")
        revision = body.get("revision", "main")
        if not isinstance(revision, str):
            raise ApiError(400, "invalid_request_error", "invalid_model", "'revision' must be a string")
        try:
            app.state.switch_task = manager.request_switch(body["model"], revision)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_model", str(error)) from None
        except SwitchInProgress as error:
            raise ApiError(409, "invalid_request_error", "switch_in_progress", str(error)) from None
        except ModelInUse as error:
            raise ApiError(409, "invalid_request_error", "model_in_use", str(error)) from None
        app.state.last_admin = admin_user
        return manager.status()

    @app.delete("/admin/models/{model_id:path}")
    async def delete_model(
        model_id: str, revision: str | None = None, admin_user: str = Depends(require_admin)
    ) -> dict:
        try:
            validate_model_name(model_id)
            if revision is not None:
                validate_revision(revision)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_model", str(error)) from None
        # Select from the files in blob storage, not from the complete copies.
        # So a second delete also removes what a failed delete left.
        stored = await asyncio.to_thread(weights.stored, model_id)
        found = [name for name in stored if revision in (None, name)]
        if not found:
            raise ApiError(404, "invalid_request_error", "model_not_found", f"no stored copy of {model_id!r} matches")
        try:
            manager.begin_remove(model_id, found)
        except ModelInUse as error:
            raise ApiError(409, "invalid_request_error", "model_in_use", str(error)) from None
        removed: list[str] = []
        status = 200
        try:
            for name in found:
                await asyncio.to_thread(weights.remove, model_id, name)
                removed.append(name)
        except Exception as error:
            status = 500
            logger.warning("the delete of %s failed: %s", model_id, error)
            raise ApiError(
                500, "server_error", "delete_failed",
                f"the delete of the model {model_id!r} failed ({type(error).__name__})",
            ) from None
        finally:
            # A cancelled request frees the mark while the worker thread can still
            # remove. This is safe: `WeightCache.ensure` and `remove` share one lock.
            manager.end_remove(model_id, found)
            write_audit(
                {
                    "user": admin_user,
                    "endpoint": "/admin/models",
                    "model": model_id,
                    "stream": False,
                    "prompt": {"event": "model_delete", "revisions": removed},
                    "status": status,
                    "prompt_tokens": None,
                    "completion_tokens": None,
                    "total_tokens": None,
                    "duration_ms": None,
                }
            )
        return {"deleted": [{"model": model_id, "revision": name} for name in removed]}

    @app.get("/admin/audit")
    async def get_audit(
        user: str | None = None,
        model: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = Query(100, ge=1, le=MAX_AUDIT_LIMIT),
        offset: int = Query(0, ge=0),
        admin_user: str = Depends(require_admin),
    ) -> dict:
        try:
            start = parse_time(since) if since is not None else None
            end = parse_time(until) if until is not None else None
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_time", str(error)) from None
        records = await asyncio.to_thread(audit.query, user, model, start, end, limit, offset)
        return {"data": records, "limit": limit, "offset": offset}

    @app.get("/admin/audit/records/{request_id}")
    async def get_audit_record(
        request_id: str,
        day: str | None = None,
        admin_user: str = Depends(require_admin),
    ) -> dict:
        if not day:
            raise ApiError(400, "invalid_request_error", "invalid_time", "day is required, like 2026-11-10")
        _check_day("day", day)
        try:
            record = await asyncio.to_thread(audit.record, day, request_id)
        except PromptUnreadable as error:
            logger.warning("the prompt of audit record %r is unreadable: %s", request_id, error)
            raise ApiError(
                502, "server_error", "prompt_unreadable", "the prompt of this record cannot be read"
            ) from None
        if record is None:
            raise ApiError(404, "invalid_request_error", "record_not_found", "no audit record has this id on this day")
        return record

    @app.get("/admin/audit/users")
    async def get_audit_users(admin_user: str = Depends(require_admin)) -> dict:
        return {"data": await asyncio.to_thread(audit.users)}

    @app.get("/admin/usage")
    async def get_usage(
        since: str | None = None,
        until: str | None = None,
        user: str | None = None,
        model: str | None = None,
        group_by: str = "user",
        admin_user: str = Depends(require_admin),
    ) -> dict:
        _check_day("since", since)
        _check_day("until", until)
        try:
            rows = await asyncio.to_thread(usage.summary, since, until, user, model, group_by)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_group_by", str(error)) from None
        return {"data": rows}

    @app.get("/admin/usage/{name}")
    async def get_user_usage(
        name: str,
        since: str | None = None,
        until: str | None = None,
        admin_user: str = Depends(require_admin),
    ) -> dict:
        _check_day("since", since)
        _check_day("until", until)
        return await asyncio.to_thread(usage.user_detail, name, since, until)

    return app


def read_hf_token(key: str | None, environment: str, get_secret) -> str | None:
    """Return the Hugging Face token from the secret `key`, or None.

    If the read fails, log an error that names the key and the environment,
    and return None. Public models still load without a token.
    """
    if not key:
        return None
    try:
        return get_secret(key)
    except Exception as error:
        logger.error(
            "the read of the Hugging Face token from the secret %r in the environment %r failed: %s. "
            "The service continues without a token, so only public models load.",
            key,
            environment,
            error,
        )
        return None


def main(port) -> None:
    """Run the gateway on `port`. The platform calls this with the assigned port."""
    import uvicorn
    from datatailr import Secrets

    from llm_service.gateway.engine import VllmEngine
    from llm_service.gateway.identity import platform_group_members
    from llm_service.gateway.storage import DatatailrBlobStore
    from llm_service.gateway.weights import hf_downloader

    logging.basicConfig(level=logging.INFO)
    config = Config.from_env(os.environ)
    environment = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "unknown")
    token = read_hf_token(config.hf_token_key, environment, lambda key: Secrets().get(key))
    admin = AdminCheck(config.owner, config.admin_group, platform_group_members)
    app = create_app(
        config,
        DatatailrBlobStore(),
        VllmEngine(config.engine_port, config.vllm_args),
        admin,
        hf_downloader(token),
        access=AccessCheck(admin, config.access_group, platform_group_members),
    )
    uvicorn.run(app, host="0.0.0.0", port=int(port))
