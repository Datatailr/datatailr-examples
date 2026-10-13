"""LLM Admin: FastAPI backend + React UI for managing LLM deployments.

Served by Datatailr as an App (``framework="fastapi"``). The platform strips
the ``/job/<env>/<name>`` prefix, so all routes here are root-relative and the
UI uses relative URLs.
"""

from __future__ import annotations

import os
import time
from importlib.resources import files
from pathlib import Path
from typing import Any

from datatailr import User
from datatailr.logging import DatatailrLogger
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from llm_platform.admin_app import services
from llm_platform.admin_app.orchestrator import Orchestrator
from llm_platform.common import dt_nodes
from llm_platform.common.auth import current_user, is_admin, require_admin
from llm_platform.common.log_store import LogStore
from llm_platform.common.naming import ADMIN_APP_NAME, gateway_internal_name, slugify
from llm_platform.common.schemas import (
    ACTIVE_STATES,
    MIN_DISK_SIZE_GB,
    ChangeModelRequest,
    ConfirmNodeRequest,
    CreateDeploymentRequest,
    Deployment,
    DeploymentState,
    NewNodeRequest,
)
from llm_platform.common.state_store import BlobStateStore

log = DatatailrLogger(__name__).get_logger()

app = FastAPI(title=ADMIN_APP_NAME, docs_url=None, redoc_url=None)
store = BlobStateStore()
orchestrator = Orchestrator(store)
logs = LogStore()

_STATIC = Path(str(files("llm_platform.admin_app").joinpath("static")))
ENVIRONMENT = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "dev")
DOMAIN = os.environ.get("DATATAILR_DOMAIN", "")


@app.on_event("startup")
async def _startup() -> None:
    orchestrator.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    orchestrator.stop()


# ---- helpers -----------------------------------------------------------------------


def _get(slug: str) -> Deployment:
    registry = store.load(max_age=2)
    dep = registry.deployments.get(slug)
    if dep is None:
        raise HTTPException(status_code=404, detail=f"Unknown deployment {slug!r}")
    return dep


def _public_domain(request: Request | None) -> str:
    """Public hostname: DATATAILR_DOMAIN if set, else the host the browser used."""
    if DOMAIN:
        return DOMAIN
    if request is None:
        return ""
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    host = host.split(",")[0].strip()
    if not host or host.startswith(("127.", "localhost", "0.0.0.0")) or host.split(":")[0] == gateway_internal_name():
        return ""
    return host


def _gateway_paths(request: Request | None = None) -> dict[str, str]:
    name = gateway_internal_name()
    path = f"/job/{ENVIRONMENT}/{name}"
    domain = _public_domain(request)
    return {
        "path": path,
        "base_path": f"{path}/v1",
        "public_base_url": f"https://{domain}{path}/v1" if domain else "",
        "internal_base_url": f"http://{name}{path}/v1",
        "hostname": name,
    }


def _enrich_nodes(nodes: list[dict[str, Any]], gpu_only: bool) -> list[dict[str, Any]]:
    registry = store.load(max_age=2)
    try:
        gpu_types = {t["name"]: t for t in dt_nodes.gpu_instance_types()}
    except Exception as exc:  # noqa: BLE001
        log.warning("instance types unavailable: %s", exc)
        gpu_types = {}
    managed = {m.node_id: m for m in registry.managed_nodes if m.node_id}
    by_internal = {d.internal_name: d for d in registry.deployments.values()}
    out = []
    for node in nodes:
        node_id = node.get("id") or node.get("instance_id")
        is_gpu = dt_nodes.is_gpu_node(node, set(gpu_types))
        if gpu_only and not is_gpu:
            continue
        info = gpu_types.get(node.get("type", ""), {})
        llm_jobs = []
        for job in node.get("jobs") or []:
            dep = by_internal.get(job.get("name"))
            if dep:
                llm_jobs.append({"slug": dep.slug, "model": dep.spec.model, "state": dep.state.value, "run_state": job.get("state"), "vram": dep.spec.vram})
        for dep in registry.deployments.values():
            if dep.node_id == node_id and dep.state in ACTIVE_STATES and not any(j["slug"] == dep.slug for j in llm_jobs):
                llm_jobs.append({"slug": dep.slug, "model": dep.spec.model, "state": dep.state.value, "run_state": None, "vram": dep.spec.vram})
        entry = dict(node)
        entry.update(
            {
                "node_id": node_id,
                "is_gpu": is_gpu,
                "ready": dt_nodes.node_is_ready(node),
                "price_per_hour": info.get("price"),
                "gpu_memory_gib": dt_nodes.GPU_MEMORY_GIB.get(node.get("type", "")),
                "llm_jobs": llm_jobs,
                "managed": managed.get(node_id).model_dump(mode="json") if node_id in managed else None,
            }
        )
        out.append(entry)
    return out


# ---- identity --------------------------------------------------------------------


@app.get("/api/me")
def me(request: Request) -> dict[str, Any]:
    try:
        user = current_user(request)
    except HTTPException:
        return {"user": None, "is_admin": False, "environment": ENVIRONMENT}
    return {"user": user.name, "is_admin": is_admin(user), "environment": ENVIRONMENT, "domain": _public_domain(request)}


# ---- overview ----------------------------------------------------------------------


@app.get("/api/overview")
def overview(request: Request, _: User = Depends(require_admin)) -> dict[str, Any]:
    registry = store.load(max_age=2)
    nodes = _enrich_nodes(_safe_nodes(), gpu_only=True)
    deployments = [d.public_dict() for d in registry.deployments.values()]
    since = time.time() - 24 * 3600
    try:
        stats = logs.stats(since)
    except Exception as exc:  # noqa: BLE001
        log.warning("stats failed: %s", exc)
        stats = {"totals": {}, "by_hour": [], "by_model": [], "by_user": []}
    events = sorted(
        [e.model_dump(mode="json") for d in registry.deployments.values() for e in d.events[-20:]]
        + [e.model_dump(mode="json") for e in registry.events[-20:]],
        key=lambda e: e["ts"],
        reverse=True,
    )[:30]
    return {
        "deployments": deployments,
        "gpu_nodes": nodes,
        "stats": stats,
        "events": events,
        "orchestrator": {"last_tick": orchestrator.last_tick, "last_error": orchestrator.last_error},
        "gateway": _gateway_paths(request),
    }


def _safe_nodes() -> list[dict[str, Any]]:
    try:
        return dt_nodes.list_nodes()
    except Exception as exc:  # noqa: BLE001
        log.warning("node ls failed: %s", exc)
        return []


# ---- deployments -------------------------------------------------------------------


@app.get("/api/deployments")
def list_deployments(_: User = Depends(require_admin)) -> list[dict[str, Any]]:
    registry = store.load(max_age=2)
    return [d.public_dict() for d in sorted(registry.deployments.values(), key=lambda d: d.created_at)]


@app.post("/api/deployments", status_code=201)
def create_deployment(payload: CreateDeploymentRequest, user: User = Depends(require_admin)) -> dict[str, Any]:
    slug = slugify(payload.name)
    with store.lock():
        registry = store.load()
        if slug in registry.deployments:
            raise HTTPException(status_code=409, detail=f"An LLM named {slug!r} already exists")
        spec = payload.model_dump(exclude={"start"})
        dep = Deployment(slug=slug, spec=spec, created_by=user.name, desired="running" if payload.start else "stopped")  # type: ignore[arg-type]
        dep.state = DeploymentState.SAVING if payload.start else DeploymentState.STOPPED
        dep.add_event(f"Created by {user.name}: {dep.spec.model} on {dep.spec.instance_type}")
        registry.deployments[slug] = dep
        registry.add_event(f"LLM {slug} created by {user.name}", deployment=slug)
        store.save(registry)
    orchestrator.poke()
    return dep.public_dict()


@app.get("/api/deployments/{slug}")
def get_deployment(slug: str, request: Request, _: User = Depends(require_admin)) -> dict[str, Any]:
    dep = _get(slug)
    data = dep.public_dict()
    data["gateway"] = _gateway_paths(request)
    return data


@app.post("/api/deployments/{slug}/start")
def start_deployment(slug: str, user: User = Depends(require_admin)) -> dict[str, Any]:
    with store.lock():
        registry = store.load()
        dep = registry.deployments.get(slug) or _get(slug)
        dep.desired = "running"
        dep.retry_requested = dep.state == DeploymentState.FAILED
        if dep.state == DeploymentState.STOPPED:
            dep.transition(DeploymentState.SAVING, f"Start requested by {user.name}")
        else:
            dep.add_event(f"Start requested by {user.name}")
        store.save(registry)
    orchestrator.poke()
    return dep.public_dict()


@app.post("/api/deployments/{slug}/stop")
def stop_deployment(slug: str, user: User = Depends(require_admin)) -> dict[str, Any]:
    with store.lock():
        registry = store.load()
        dep = registry.deployments.get(slug) or _get(slug)
        dep.desired = "stopped"
        if dep.state == DeploymentState.FAILED:
            dep.transition(DeploymentState.STOPPING, f"Stop requested by {user.name}")
        else:
            dep.add_event(f"Stop requested by {user.name}")
        store.save(registry)
    orchestrator.poke()
    return dep.public_dict()


@app.post("/api/deployments/{slug}/retry")
def retry_deployment(slug: str, user: User = Depends(require_admin)) -> dict[str, Any]:
    with store.lock():
        registry = store.load()
        dep = registry.deployments.get(slug) or _get(slug)
        if dep.state != DeploymentState.FAILED:
            raise HTTPException(status_code=409, detail="Only failed deployments can be retried")
        dep.retry_requested = True
        dep.spec_dirty = True
        dep.add_event(f"Retry requested by {user.name}")
        store.save(registry)
    orchestrator.poke()
    return dep.public_dict()


@app.post("/api/deployments/{slug}/change-model")
def change_model(slug: str, payload: ChangeModelRequest, user: User = Depends(require_admin)) -> dict[str, Any]:
    model = payload.model.strip()
    if not model:
        raise HTTPException(status_code=422, detail="model is required")
    with store.lock():
        registry = store.load()
        dep = registry.deployments.get(slug) or _get(slug)
        if payload.extra_args is not None:
            dep.spec.extra_args = payload.extra_args
        if payload.vram:
            dep.spec.vram = payload.vram
        if payload.hf_token_secret is not None:
            dep.spec.hf_token_secret = payload.hf_token_secret
        dep.spec_dirty = True
        dep.last_error = None
        if dep.state in {DeploymentState.STOPPED, DeploymentState.FAILED}:
            dep.spec.model = model
            dep.pending_model = None
            dep.add_event(f"Model set to {model} by {user.name}")
            if dep.desired == "running" or dep.state == DeploymentState.FAILED:
                dep.desired = "running"
                dep.transition(DeploymentState.SAVING, "Restarting with the new model")
        else:
            dep.pending_model = model
            dep.desired = "running"
            dep.transition(DeploymentState.STOPPING, f"Model change to {model} requested by {user.name}; restarting")
        store.save(registry)
    orchestrator.poke()
    return dep.public_dict()


@app.delete("/api/deployments/{slug}")
def delete_deployment(slug: str, user: User = Depends(require_admin)) -> dict[str, Any]:
    with store.lock():
        registry = store.load()
        dep = registry.deployments.get(slug) or _get(slug)
        try:
            service = services.open_service(dep)
            service.remove(all_versions=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("remove of %s returned: %s", dep.internal_name, exc)
        del registry.deployments[slug]
        registry.add_event(f"LLM {slug} deleted by {user.name}", deployment=slug)
        store.save(registry)
    return {"deleted": slug}


@app.get("/api/deployments/{slug}/logs")
def deployment_logs(
    slug: str,
    stderr: bool = False,
    build: bool = False,
    lines: int = Query(default=300, le=5000),
    _: User = Depends(require_admin),
) -> dict[str, Any]:
    dep = _get(slug)
    if build:
        text = services.build_logs(dep, "stderr" if stderr else "stdout", tail_lines=lines)
        return {"run_id": None, "run_state": "build", "text": text}
    run = services.latest_run(dep)
    if run is None:
        return {"run_id": None, "text": "", "run_state": None}
    run_id = run.get("run_id")
    try:
        text = services.read_logs(dep, run_id=int(run_id), stderr=stderr, lines=lines)
    except Exception as exc:  # noqa: BLE001
        text = f"(logs unavailable: {exc})"
    return {"run_id": run_id, "run_state": run.get("state"), "text": text}


@app.get("/api/deployments/{slug}/events")
def deployment_events(slug: str, _: User = Depends(require_admin)) -> list[dict[str, Any]]:
    dep = _get(slug)
    return [e.model_dump(mode="json") for e in reversed(dep.events)]


# ---- nodes --------------------------------------------------------------------------


@app.get("/api/nodes")
def list_nodes(gpu: bool = True, _: User = Depends(require_admin)) -> list[dict[str, Any]]:
    return _enrich_nodes(_safe_nodes(), gpu_only=gpu)


@app.get("/api/instance-types")
def list_instance_types(gpu: bool = True, _: User = Depends(require_admin)) -> list[dict[str, Any]]:
    try:
        types = dt_nodes.gpu_instance_types() if gpu else dt_nodes.instance_types()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"instance types unavailable: {exc}") from exc
    template = dt_nodes.load_node_template()
    default_type = template[0].get("type") if template else None
    if default_type and not any(t.get("name") == default_type for t in types):
        types.insert(0, {"name": default_type, "node_class": "gpu", "has_gpu": True, "gpu_memory_gib": dt_nodes.GPU_MEMORY_GIB.get(default_type)})
    return types


@app.get("/api/node-template")
def node_template(_: User = Depends(require_admin)) -> dict[str, Any]:
    template = dt_nodes.load_node_template()
    entry = template[0] if template else {}
    return {"instance_type": entry.get("type"), "environment": entry.get("environment", ENVIRONMENT), "disk_size": max(int(entry.get("disk_size", MIN_DISK_SIZE_GB)), MIN_DISK_SIZE_GB), "min_disk_size": MIN_DISK_SIZE_GB}


@app.post("/api/nodes/new", status_code=202)
def create_node(payload: NewNodeRequest, user: User = Depends(require_admin)) -> dict[str, Any]:
    try:
        result = dt_nodes.new_nodes(payload.instance_type, payload.environment, payload.disk_size, payload.num_instances)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    with store.lock():
        registry = store.load()
        registry.add_event(f"{user.name} requested {payload.num_instances} x {payload.instance_type} GPU node(s) with {payload.disk_size} GB disk")
        store.save(registry)
    return {"result": result}


def _node_action(node_id: str, action: str, user: User, confirm_name: str | None = None) -> dict[str, Any]:
    nodes = _safe_nodes()
    node = dt_nodes.find_node(nodes, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Unknown node {node_id!r}")
    if action == "terminate" and confirm_name != node.get("name"):
        raise HTTPException(status_code=400, detail="Type the exact node name to confirm termination")
    fn = {"start": dt_nodes.start_node, "stop": dt_nodes.stop_node, "terminate": dt_nodes.terminate_node}[action]
    try:
        result = fn(node.get("id") or node.get("instance_id"))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    with store.lock():
        registry = store.load()
        registry.add_event(f"{user.name} requested node {action} for {node.get('name')}", level="warning" if action != "start" else "info")
        store.save(registry)
    return {"result": result, "node": node.get("name")}


@app.post("/api/nodes/{node_id}/start")
def node_start(node_id: str, user: User = Depends(require_admin)) -> dict[str, Any]:
    return _node_action(node_id, "start", user)


@app.post("/api/nodes/{node_id}/stop")
def node_stop(node_id: str, user: User = Depends(require_admin)) -> dict[str, Any]:
    return _node_action(node_id, "stop", user)


@app.post("/api/nodes/{node_id}/terminate")
def node_terminate(node_id: str, payload: ConfirmNodeRequest, user: User = Depends(require_admin)) -> dict[str, Any]:
    return _node_action(node_id, "terminate", user, confirm_name=payload.confirm_name)


# ---- requests / observability ---------------------------------------------------------


@app.get("/api/requests")
def list_requests(
    user: str | None = None,
    model: str | None = None,
    status: str | None = None,
    since: float | None = None,
    until: float | None = None,
    search: str | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
    _: User = Depends(require_admin),
) -> dict[str, Any]:
    return logs.list_requests(user=user, model=model, status=status, since=since, until=until, search=search, limit=limit, offset=offset)


@app.get("/api/requests/{request_id}")
def get_request(request_id: str, _: User = Depends(require_admin)) -> dict[str, Any]:
    row = logs.get_request(request_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Request not found")
    return row


@app.get("/api/stats")
def stats(hours: float = Query(default=24, gt=0, le=24 * 90), _: User = Depends(require_admin)) -> dict[str, Any]:
    return logs.stats(time.time() - hours * 3600)


@app.get("/api/users")
def users(_: User = Depends(require_admin)) -> list[dict[str, Any]]:
    return logs.users()


@app.post("/api/logs/refresh")
def refresh_logs(_: User = Depends(require_admin)) -> dict[str, Any]:
    return {"new_files": logs.sync(force=True)}


# ---- connect (available to every signed-in user) -------------------------------------


@app.get("/api/connect")
def connect(request: Request) -> dict[str, Any]:
    user = current_user(request)
    registry = store.load(max_age=5)
    models = [
        {"id": d.slug, "model": d.spec.model, "state": d.state.value, "description": d.spec.description}
        for d in sorted(registry.deployments.values(), key=lambda d: d.slug)
    ]
    return {
        "user": user.name,
        "environment": ENVIRONMENT,
        "domain": _public_domain(request),
        "gateway": _gateway_paths(request),
        "models": models,
    }


# ---- static UI -------------------------------------------------------------------------

if (_STATIC / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=str(_STATIC / "assets")), name="assets")


@app.get("/health")
def health() -> Response:
    return Response("OK\n", media_type="text/plain")


@app.get("/{path:path}", include_in_schema=False)
def spa(path: str) -> Response:
    if path.startswith("api/"):
        raise HTTPException(status_code=404)
    candidate = _STATIC / path
    if path and candidate.is_file():
        return FileResponse(str(candidate))
    index = _STATIC / "index.html"
    if not index.is_file():
        return JSONResponse({"detail": "UI not built. Run `npm run build` in frontend/ before deploying."}, status_code=503)
    return FileResponse(str(index), headers={"cache-control": "no-store"})
