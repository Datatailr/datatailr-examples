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
from datetime import date, datetime, timedelta, timezone
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
from llm_service.gateway.engine_metrics import EngineReader, GpuReader, metrics_client
from llm_service.gateway.identity import (
    AccessCheck,
    AdminCheck,
    acting_user_from_headers,
    check_user_name,
    user_from_headers,
)
from llm_service.gateway.limiter import LimitExceeded, Limiter
from llm_service.gateway.catalog import CatalogFull, CatalogStore, InvalidEntry, check_entry
from llm_service.gateway.fit import fit, needed_from
from llm_service.gateway.limits import InvalidLimit, LimitStore, StorageUnavailable
from llm_service.gateway.load import LoadMonitor
from llm_service.gateway.metrics_store import MAX_DAYS, MetricsStore
from llm_service.gateway.models import DownloadInProgress, ModelInUse, ModelManager, SwitchInProgress
from llm_service.gateway.proxy import forward, prompt_of
from llm_service.gateway.storage import BlobStore
from llm_service.gateway.usage import UsageTotals
from llm_service.gateway.weights import Downloader, WeightCache, validate_model_name, validate_revision

logger = logging.getLogger("llm_service.app")

SYSTEM_USER = "system"
OPENAI_PATHS = ("/v1/chat/completions", "/v1/completions", "/v1/embeddings")
MAX_AUDIT_LIMIT = 1000
DAY_FORM = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
SAMPLE_INTERVAL = 5.0
GPU_EVERY = 3  # the GPU memory is read at each third sample

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


def _check_instant(name: str, value: str | None) -> datetime:
    """Return `value` as a time in UTC. A time without a zone counts as UTC. Raise `ApiError` otherwise."""
    try:
        if value is None:
            raise ValueError(name)
        text = value.strip()
        # Python 3.13 accepts a capital `Z` only.
        moment = datetime.fromisoformat(text[:-1] + "Z" if text.endswith("z") else text)
        return moment.astimezone(timezone.utc) if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError):
        raise ApiError(
            400,
            "invalid_request_error",
            "invalid_time",
            f"{name} is required and must be an ISO time like 2026-11-10T12:00:00Z",
        ) from None


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
    gpu: GpuReader | None = None,
    sample_interval: float = SAMPLE_INTERVAL,
    metrics_http: httpx.AsyncClient | None = None,
) -> FastAPI:
    """Build the gateway application from its parts.

    `now` is the clock of the audit log, the merger and the usage totals. Tests replace it.
    `maintenance_interval` is the sleep in seconds between two merge and retention steps.
    `access` decides who has access to the service. Without it, only admins have access.
    `gpu` reads the GPU memory. Without it, the app builds a `GpuReader`.
    `sample_interval` is the sleep in seconds between two load samples.
    `metrics_http` is the client of the engine metrics read. Without it, the app builds a
    client with its own small pool, so a busy proxy does not block the read.
    """
    access = access or AccessCheck(admin, None, lambda group: [])
    owns_http = http is None
    client = http or httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10.0))
    audit = AuditLog(store, config.blob_root, config.flush_size, now=now)
    usage = UsageTotals(store, config.blob_root, audit, lambda: iso(now())[:10], now=now)
    limits = LimitStore(store, config.blob_root)
    catalog = CatalogStore(store, config.blob_root)

    def seconds_to_midnight() -> float:
        current = now()
        midnight = current.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return (midnight - current).total_seconds()

    limiter = Limiter(usage.tokens_today, seconds_to_midnight)
    merger = AuditMerger(store, config.blob_root, audit, now, config.prompt_retention_days)
    stopping = threading.Event()  # set before the last flusher cycle, which forces the checkpoint and the metrics write
    load = LoadMonitor(now)
    metrics = MetricsStore(store, config.blob_root, now, config.metrics_retention_days)
    gpu = gpu or GpuReader()
    owns_metrics_http = metrics_http is None
    metrics_http = metrics_http or metrics_client()
    engine_reader = EngineReader(metrics_http, engine.base_url)
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
        if event["event"] == "model_download":
            # A download names its own admin. It does not change `last_admin`.
            write_audit(
                {
                    "user": event["admin"],
                    "endpoint": "/admin/models/download",
                    "model": event["model"],
                    "stream": False,
                    "prompt": {
                        "event": "model_download",
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
            return
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
        """Run the usage steps, the new read of the limits and the metrics write after each flush.

        The steps run in the flusher thread. Each step has its own guard, so one failure does not
        stop the others. The last cycle at shutdown forces the checkpoint and the metrics write.
        The limits file and the catalog file are read again only after a failed read. The merge and the retention run
        in the maintenance task, so a flush never waits for their deletes.
        """
        last_cycle = stopping.is_set()
        steps = [
            ("close_days", usage.close_days),
            ("checkpoint", lambda: usage.checkpoint(force=last_cycle)),
            ("limits", limits.retry_load),
            ("catalog", catalog.retry_load),
            ("metrics", lambda: metrics.write(force=last_cycle)),
        ]
        for name, step in steps:
            try:
                step()
            except Exception as error:
                logger.warning("the flusher step %s failed: %s", name, error)

    def maintenance_tick() -> None:
        """Run the merge and the metrics retention, each with its own guard."""
        for name, step in (("merge", merger.tick), ("metrics retention", metrics.retain)):
            try:
                step()
            except Exception as error:
                logger.warning("the maintenance step %s failed: %s", name, error)

    async def sample_loop() -> None:
        """Take a load sample each `sample_interval` seconds until a cancel.

        Each round reads the engine numbers, and at each third round the GPU memory in a thread.
        The loop logs a failed round and goes on.
        """
        gpu_memory = None
        rounds = 0
        while True:
            try:
                numbers = await engine_reader.read()
                if rounds % GPU_EVERY == 0:
                    gpu_memory = await asyncio.to_thread(gpu.read)
                rounds += 1
                metrics.add(load.add_sample(numbers, gpu_memory))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("a load sample failed: %s", error)
            await asyncio.sleep(sample_interval)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load the totals first. Register the listeners after that, with no await
        # in between, so that no record is counted twice and no flush runs against
        # a half-loaded day.
        await asyncio.to_thread(limits.load)
        await asyncio.to_thread(catalog.load)
        started = time.monotonic()
        days = await asyncio.to_thread(usage.load)
        logger.info("usage totals loaded in %.1f s for %d days", time.monotonic() - started, days)
        audit.listeners.append(usage.record)
        audit.listeners.append(load.observe)
        audit.flushed_listeners.append(usage.flushed)
        audit.flushed_listeners.append(merger.flushed)
        flusher = asyncio.create_task(
            run_flusher(audit, config.flush_interval, on_tick=on_flusher_tick)
        )
        maintenance = asyncio.create_task(run_maintenance(maintenance_tick, maintenance_interval))
        sampler = asyncio.create_task(sample_loop())
        app.state.maintenance_task = maintenance
        app.state.startup_task = asyncio.create_task(manager.startup())
        try:
            yield
        finally:
            tasks = (
                app.state.startup_task,
                app.state.switch_task,
                app.state.reload_task,
                app.state.download_task,
            )
            for task in tasks:
                if task is not None and not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            # The sampler ends before the engine, so no metrics read fails during the stop.
            sampler.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sampler
            try:
                await engine.stop()
            finally:
                # Do not wait for a merge step that runs now. Its thread ends by itself.
                maintenance.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await maintenance
                # One last round with no reads, so the points of the last minutes reach the store.
                try:
                    metrics.add(load.add_sample({}, None))
                except Exception as error:
                    logger.warning("the last load sample failed: %s", error)
                stopping.set()
                flusher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await flusher
                if owns_http:
                    await client.aclose()
                if owns_metrics_http:
                    await metrics_http.aclose()

    app = FastAPI(title="LLM service", lifespan=lifespan)
    app.state.last_admin = SYSTEM_USER
    app.state.switch_task = None
    app.state.startup_task = None
    app.state.reload_task = None
    app.state.download_task = None
    app.state.maintenance_task = None
    app.state.manager = manager
    app.state.audit = audit
    app.state.usage = usage
    app.state.limits = limits
    app.state.catalog = catalog
    app.state.limiter = limiter
    app.state.load = load
    app.state.metrics = metrics

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

            first_ms: int | None = None  # time to the first content of a stream

            def note_first() -> None:
                nonlocal first_ms
                first_ms = int((time.monotonic() - started) * 1000)

            slot = False  # True while the limiter holds a slot of this request

            def release_slot() -> None:
                nonlocal slot
                if slot:
                    slot = False
                    try:
                        limiter.release(user)
                    finally:
                        load.end()

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
                    "ttft_ms": first_ms,
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

            # One read decides whether the admin right matters: only the default depends on it.
            # A second read after the lookup takes the limits as they are at that moment.
            user_limits, source = limits.limits_for(user, False)
            if source == "default" and await asyncio.to_thread(admin.is_admin, user):
                user_limits, _ = limits.limits_for(user, True)
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
            load.begin()
            try:
                response = await forward(client, engine.base_url, path, body, record, note_first)
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

    @app.get("/v1/limits/me")
    async def limits_me(user: str = Depends(effective_user)) -> dict:
        user_admin = await asyncio.to_thread(admin.is_admin, user)
        user_limits, source = limits.limits_for(user, user_admin)
        return {
            "user": user,
            "limits": user_limits,
            "use": limiter.use(user),
            "source": source,
            "loaded": limits.loaded(),
        }

    # Admin API

    def entry_user(name: str) -> str:
        """Return the stripped user name of a limits entry. Raise `ApiError` with `invalid_user` if it is bad."""
        try:
            return check_user_name(name)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_user", str(error)) from None

    async def limits_body(request: Request) -> object:
        """Return the parsed JSON body. A body that is not JSON gives None, which `check_limits` refuses."""
        try:
            return json.loads(await request.body())
        except (ValueError, RecursionError):
            return None

    async def change_limits(
        target: str, admin_user: str, change: Callable[[], tuple[dict | None, dict | None]]
    ) -> dict | None:
        """Run a store change in a thread and audit it. The change returns the old and the new value."""
        try:
            old, new = await asyncio.to_thread(change)
        except InvalidLimit as error:
            raise ApiError(400, "invalid_request_error", "invalid_limit", str(error)) from None
        except StorageUnavailable as error:
            raise ApiError(502, "server_error", "storage_unavailable", str(error)) from None
        write_audit(
            {
                "user": admin_user,
                "endpoint": "/admin/limits",
                "model": None,
                "stream": False,
                "prompt": {"event": "limits_change", "target": target, "old": old, "new": new},
                "status": 200,
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "duration_ms": None,
            }
        )
        return new

    @app.get("/admin/limits")
    async def get_limits(admin_user: str = Depends(require_admin)) -> dict:
        entries = limits.users()
        # A user with tokens today stays in the list after the request window ends and after a restart.
        names = sorted(set(limiter.active_users()) | set(entries) | set(usage.users_today()))

        def effective() -> dict:
            """The limits of each listed user and their source, with one admin lookup for each."""
            found = {}
            for name in names:
                values, source = limits.limits_for(name, admin.is_admin(name))
                found[name] = {"limits": values, "source": source}
            return found

        return {
            "default": limits.default(),
            "users": entries,
            "use": {name: limiter.use(name) for name in names},
            "effective": await asyncio.to_thread(effective),
            "loaded": limits.loaded(),
        }

    @app.put("/admin/limits/default")
    async def put_default_limits(request: Request, admin_user: str = Depends(require_admin)) -> dict:
        body = await limits_body(request)

        def change():
            return limits.set_default(body), body

        return await change_limits("default", admin_user, change)

    @app.put("/admin/limits/users/{name}")
    async def put_user_limits(name: str, request: Request, admin_user: str = Depends(require_admin)) -> dict:
        name = entry_user(name)
        body = await limits_body(request)

        def change():
            return limits.set_user(name, body), body

        return await change_limits(name, admin_user, change)

    @app.delete("/admin/limits/users/{name}")
    async def delete_user_limits(name: str, admin_user: str = Depends(require_admin)) -> dict:
        name = entry_user(name)

        def change():
            try:
                return limits.remove_user(name), None
            except KeyError:
                raise ApiError(
                    404, "invalid_request_error", "limit_not_found", f"the user {name!r} has no limits entry"
                ) from None

        await change_limits(name, admin_user, change)
        return {"removed": name}

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

    async def fit_of(model: str, revision: str, cached: list[dict] | None = None) -> dict:
        """Return the GPU memory check of one pair.

        The needed memory comes from the catalog entry, else from the size of the cached pair.
        `cached` is an already read list of `weights.cached()`. Without it, the function reads
        the list in a thread. A store error then leaves the size unknown. The available memory is
        the total of `nvidia-smi` in the last load sample, else the memory of the container.
        """
        entry = catalog.get(model, revision)
        size_bytes = None
        if cached is None:
            try:
                cached = await asyncio.to_thread(weights.cached)
            except Exception as error:
                logger.warning("the cache list for the fit of %s %s failed: %s", model, revision, error)
                cached = []
        for item in cached:
            if item["model"] == model and item["revision"] == revision:
                size_bytes = item["size_bytes"]
        needed, basis = needed_from(entry, size_bytes)
        available = load.live()["now"]["gpu_memory_total"]
        source = "nvidia-smi"
        if available is None:
            available, source = config.gpu_memory_mib, "container"
        if available is None:
            source = None
        return fit(needed, basis, available, source)

    app.state.fit_of = fit_of

    @app.get("/admin/models/fit")
    async def get_model_fit(model: str = "", revision: str = "main", admin_user: str = Depends(require_admin)) -> dict:
        try:
            validate_model_name(model)
            validate_revision(revision)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_model", str(error)) from None
        return await fit_of(model, revision)

    @app.get("/admin/catalog")
    async def get_catalog(admin_user: str = Depends(require_admin)) -> dict:
        try:
            cached = await asyncio.to_thread(weights.cached)
        except Exception as error:
            logger.warning("the cache list for the catalog failed: %s", error)
            cached = None
        have = {(item["model"], item["revision"]) for item in cached or []}
        models = []
        for entry in catalog.entries():
            models.append(
                {
                    **entry,
                    "cached": (entry["model"], entry["revision"]) in have,
                    "fit": await fit_of(entry["model"], entry["revision"], cached if cached is not None else []),
                }
            )
        return {"models": models, "loaded": catalog.loaded()}

    async def change_catalog(
        target: str, admin_user: str, change: Callable[[], tuple[dict | None, dict | None]]
    ) -> None:
        """Run a catalog change in a thread and audit it. The change returns the old and the new entry."""
        try:
            old, new = await asyncio.to_thread(change)
        except InvalidEntry as error:
            raise ApiError(400, "invalid_request_error", "invalid_catalog_entry", str(error)[:300]) from None
        except CatalogFull as error:
            raise ApiError(409, "invalid_request_error", "catalog_full", str(error)) from None
        except StorageUnavailable as error:
            raise ApiError(502, "server_error", "storage_unavailable", str(error)) from None
        write_audit(
            {
                "user": admin_user,
                "endpoint": "/admin/catalog",
                "model": None,
                "stream": False,
                "prompt": {"event": "catalog_change", "target": target, "old": old, "new": new},
                "status": 200,
                "prompt_tokens": None,
                "completion_tokens": None,
                "total_tokens": None,
                "duration_ms": None,
            }
        )

    @app.put("/admin/catalog")
    async def put_catalog(request: Request, admin_user: str = Depends(require_admin)) -> dict:
        try:
            body = json.loads(await request.body())
        except (ValueError, RecursionError):
            body = None
        try:
            new = check_entry(body)
        except InvalidEntry as error:
            raise ApiError(400, "invalid_request_error", "invalid_catalog_entry", str(error)[:300]) from None

        def change():
            return catalog.put(new), new

        await change_catalog(f"{new['model']}@{new['revision']}", admin_user, change)
        return new

    @app.delete("/admin/catalog")
    async def delete_catalog(
        model: str = "", revision: str = "main", admin_user: str = Depends(require_admin)
    ) -> dict:
        try:
            validate_model_name(model)
            validate_revision(revision)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_catalog_entry", str(error)[:300]) from None

        def change():
            try:
                return catalog.remove(model, revision), None
            except KeyError:
                raise ApiError(
                    404, "invalid_request_error", "catalog_entry_not_found", f"the catalog has no entry {model}@{revision}"
                ) from None

        await change_catalog(f"{model}@{revision}", admin_user, change)
        return {"removed": {"model": model, "revision": revision}}

    @app.post("/admin/models/download", status_code=202)
    async def download_model(request: Request, admin_user: str = Depends(require_admin)):
        try:
            body = json.loads(await request.body())
        except (ValueError, RecursionError):
            body = None
        if not isinstance(body, dict) or not isinstance(body.get("model"), str):
            raise ApiError(400, "invalid_request_error", "invalid_model", "the body needs a string 'model'")
        model, revision = body["model"], body.get("revision", "main")
        if not isinstance(revision, str):
            raise ApiError(400, "invalid_request_error", "invalid_model", "'revision' must be a string")
        try:
            validate_model_name(model)
            validate_revision(revision)
        except ValueError as error:
            raise ApiError(400, "invalid_request_error", "invalid_model", str(error)) from None
        # A delete removes the marker first. Until then the pair looks cached.
        if (model, revision) in manager.removing:
            raise ApiError(
                409, "invalid_request_error", "model_in_use", f"a delete removes {model} {revision} now"
            )
        try:
            cached = await asyncio.to_thread(weights.is_cached, model, revision)
        except Exception as error:
            logger.warning("the cache check of %s %s failed: %s", model, revision, error)
            raise ApiError(
                500, "server_error", "cache_check_failed",
                f"the cache check of the model {model!r} failed ({type(error).__name__})",
            ) from None
        if cached:
            return JSONResponse({"state": "cached", "model": model, "revision": revision})
        try:
            app.state.download_task = manager.request_download(model, revision, admin_user)
        except DownloadInProgress as error:
            raise ApiError(409, "invalid_request_error", "download_in_progress", str(error)) from None
        except ModelInUse as error:
            raise ApiError(409, "invalid_request_error", "model_in_use", str(error)) from None
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
        # During the Hugging Face phase, blob storage has no file of the
        # download yet. `begin_remove` refuses its pair with 409.
        downloading = manager.running_download()
        if (
            downloading is not None
            and downloading["model"] == model_id
            and revision in (None, downloading["revision"])
            and downloading["revision"] not in found
        ):
            found.append(downloading["revision"])
        if not found:
            raise ApiError(404, "invalid_request_error", "model_not_found", f"no stored copy of {model_id!r} matches")
        # A download holds the cache lock for its whole fetch, and `remove`
        # needs that lock. A delete of the pair of the download gets
        # `model_in_use` from `begin_remove`. Each other delete gets this answer.
        if downloading is not None and not (
            downloading["model"] == model_id and downloading["revision"] in found
        ):
            raise ApiError(
                409, "invalid_request_error", "download_in_progress",
                "A model download runs now. Delete after it ends.",
            )
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

    @app.get("/admin/metrics/live")
    async def get_live_metrics(admin_user: str = Depends(require_admin)) -> dict:
        return load.live()

    @app.get("/admin/metrics/history")
    async def get_metrics_history(
        since: str | None = None, until: str | None = None, admin_user: str = Depends(require_admin)
    ) -> dict:
        start = _check_instant("since", since)
        end = _check_instant("until", until)
        if start > end:
            raise ApiError(400, "invalid_request_error", "invalid_time", "since is after until")
        if end - start > timedelta(days=MAX_DAYS):
            raise ApiError(400, "invalid_request_error", "invalid_time", f"the range is over {MAX_DAYS} days")
        points, step, missing = await asyncio.to_thread(metrics.history, start, end)
        return {"step_seconds": step, "points": points, "missing_days": missing}

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
