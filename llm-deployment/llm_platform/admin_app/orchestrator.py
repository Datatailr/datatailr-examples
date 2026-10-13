"""Deployment state machine: save the service, make sure a GPU node is up, start.

Runs as a background thread inside the admin app. Every tick it reloads the
registry, advances each deployment one step, and persists the registry when
anything changed. All platform calls are synchronous ``dt`` invocations, so the
loop is deliberately simple and idempotent: if the app restarts mid-flight,
the next tick picks up where the previous one left off.

States (see ``DeploymentState``)::

    saving -> provisioning_node -> waiting_node -> starting -> running
                      \\___________(node available)_________/
    running -> stopping -> stopped -> saving (start again / change model)
    any -> failed -> saving (retry)
"""

from __future__ import annotations

import os
import threading
import time
import traceback
from typing import Any

from datatailr.logging import DatatailrLogger

from llm_platform.admin_app import services
from llm_platform.common import dt_nodes
from llm_platform.common.schemas import Deployment, DeploymentState, ManagedNode, Registry
from llm_platform.common.state_store import BlobStateStore

log = DatatailrLogger(__name__).get_logger()

TICK_SECONDS = float(os.environ.get("LLM_ORCH_TICK", "10"))
NODE_READY_TIMEOUT = float(os.environ.get("LLM_NODE_READY_TIMEOUT", str(20 * 60)))
SERVICE_READY_TIMEOUT = float(os.environ.get("LLM_SERVICE_READY_TIMEOUT", str(45 * 60)))
BUILD_TIMEOUT = float(os.environ.get("LLM_BUILD_TIMEOUT", str(60 * 60)))
START_GRACE_SECONDS = 90.0  # time for a requested run to appear in `runs` before we would start again


_parse_size_gib = dt_nodes.parse_size_gib


class Orchestrator:
    def __init__(self, store: BlobStateStore):
        self._store = store
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._wake = threading.Event()
        self.last_tick: float = 0.0
        self.last_error: str | None = None
        self._start_requested: dict[str, float] = {}

    # ---- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="llm-orchestrator", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def poke(self) -> None:
        """Ask the loop to run a tick as soon as possible (after an admin action)."""
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
                self.last_error = None
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"{exc}"
                log.error("Orchestrator tick failed: %s\n%s", exc, traceback.format_exc())
            self._wake.wait(TICK_SECONDS)
            self._wake.clear()

    # ---- one tick ----------------------------------------------------------
    def tick(self) -> None:
        self.last_tick = time.time()
        slugs = list(self._store.load().deployments.keys())
        if not slugs:
            return
        nodes = self._safe_nodes()
        for slug in slugs:
            # One deployment per critical section so admin actions are never
            # blocked for longer than a single platform call.
            with self._store.lock():
                registry = self._store.load()
                dep = registry.deployments.get(slug)
                if dep is None:
                    continue
                before = dep.model_dump_json()
                managed_before = len(registry.managed_nodes)
                try:
                    self._advance(dep, registry, nodes)
                except Exception as exc:  # noqa: BLE001
                    log.error("Deployment %s failed: %s\n%s", dep.slug, exc, traceback.format_exc())
                    dep.last_error = str(exc)
                    dep.transition(DeploymentState.FAILED, f"Error: {exc}", level="error")
                if before != dep.model_dump_json() or managed_before != len(registry.managed_nodes):
                    self._store.save(registry)

    def _safe_nodes(self) -> list[dict[str, Any]]:
        try:
            return dt_nodes.list_nodes()
        except Exception as exc:  # noqa: BLE001
            log.warning("node ls failed: %s", exc)
            return []

    # ---- state machine -----------------------------------------------------
    def _advance(self, dep: Deployment, registry: Registry, nodes: list[dict[str, Any]]) -> None:
        state = dep.state

        if dep.retry_requested and state == DeploymentState.FAILED:
            dep.retry_requested = False
            dep.last_error = None
            dep.desired = "running"
            dep.spec_dirty = True  # re-apply the service definition (cheap when the image is unchanged)
            dep.run_id = None  # a previous (ended) run must not be mistaken for the new one
            dep.transition(DeploymentState.SAVING, "Retry requested")
            state = dep.state

        if dep.desired == "stopped" and state in {
            DeploymentState.SAVING,
            DeploymentState.PROVISIONING_NODE,
            DeploymentState.WAITING_NODE,
            DeploymentState.STARTING,
            DeploymentState.RUNNING,
        }:
            dep.transition(DeploymentState.STOPPING, "Stop requested")
            state = dep.state

        if state == DeploymentState.SAVING:
            self._do_save(dep)
        elif state == DeploymentState.PROVISIONING_NODE:
            self._do_provision(dep, registry, nodes)
        elif state == DeploymentState.WAITING_NODE:
            self._do_wait_node(dep, registry, nodes)
        elif state == DeploymentState.STARTING:
            self._do_starting(dep, nodes)
        elif state == DeploymentState.RUNNING:
            self._do_running(dep, nodes)
        elif state == DeploymentState.STOPPING:
            self._do_stopping(dep)
        elif state == DeploymentState.STOPPED:
            if dep.desired == "running":
                dep.run_id = None
                dep.transition(DeploymentState.SAVING, "Start requested")
        # FAILED: wait for retry

    # saving ------------------------------------------------------------------
    def _do_save(self, dep: Deployment) -> None:
        if dep.pending_model:
            dep.spec.model = dep.pending_model
            dep.pending_model = None
            dep.add_event(f"Model changed to {dep.spec.model}")
        if dep.spec_dirty:
            service = services.build_service(dep)
            ok, message = service.save()
            if not ok:
                raise RuntimeError(f"Service save failed: {message}")
            dep.spec_dirty = False
            dep.service_version = getattr(service, "version", None)
            dep.add_event(f"Service {dep.internal_name} saved (version {dep.service_version}); image build in progress")
        definition = services.job_definition(dep)
        if definition is None:
            if time.time() - dep.state_since > BUILD_TIMEOUT:
                raise RuntimeError("Service definition never appeared on the platform")
            return  # wait for the platform to report the definition
        dep.service_version = definition.get("version", dep.service_version)
        job_state = str(definition.get("state", "")).lower()
        # Scheduler semantics (scheduler.cpp): a saved dev job goes straight to
        # 'building'; the builder sets 'ready' on success and *back to 'created'*
        # on failure (with last_build_succeeded = false).
        if job_state == "building":
            if time.time() - dep.state_since > BUILD_TIMEOUT:
                raise RuntimeError("Image build did not finish in time")
            return
        if job_state == "created" and definition.get("last_build_succeeded") is False:
            if time.time() - dep.state_since < 90:
                return  # saved a moment ago; give the scheduler time to flip to 'building'
            # A retry must re-save with a changed image definition, otherwise the
            # scheduler reuses the failed version and never builds again.
            dep.spec_dirty = True
            dep.build_nonce += 1
            raise RuntimeError(f"Image build failed: {services.build_failure_summary(dep)}")
        if job_state != "ready":
            if time.time() - dep.state_since > BUILD_TIMEOUT:
                raise RuntimeError(f"Service never became ready (job state '{job_state}')")
            return
        # Image is ready. Do we have a GPU node?
        node = self._pick_node(dep, dt_nodes.list_nodes())
        if node is not None:
            dep.node_id = node.get("id") or node.get("instance_id")
            dep.node_name = node.get("name")
            dep.transition(DeploymentState.STARTING, f"Using GPU node {dep.node_name} ({node.get('type')})")
        else:
            dep.transition(DeploymentState.PROVISIONING_NODE, "No suitable GPU node is running")

    # node provisioning ---------------------------------------------------------
    def _pick_node(self, dep: Deployment, nodes: list[dict[str, Any]]) -> dict[str, Any] | None:
        try:
            gpu_types = {t["name"] for t in dt_nodes.gpu_instance_types()}
        except Exception as exc:  # noqa: BLE001
            log.warning("instance types unavailable, relying on node_class: %s", exc)
            gpu_types = set()
        candidates = []
        for node in nodes:
            if node.get("environment") != dep.spec.environment:
                continue
            if not dt_nodes.is_gpu_node(node, gpu_types):
                continue
            if not dt_nodes.node_is_ready(node):
                continue
            if not self._fits(dep, node):
                continue
            candidates.append(node)
        if not candidates:
            return None
        # Prefer the requested instance type, then the one with most free memory.
        candidates.sort(
            key=lambda n: (
                0 if n.get("type") == dep.spec.instance_type else 1,
                -float((n.get("memory") or {}).get("free_memory") or 0),
            )
        )
        return candidates[0]

    def _fits(self, dep: Deployment, node: dict[str, Any]) -> bool:
        mem = node.get("memory") or {}
        cpu = node.get("cpu") or {}
        if float(mem.get("free_memory") or 0) / 1024**3 < _parse_size_gib(dep.spec.memory):
            return False
        if float(cpu.get("free_cpus") or 0) < float(dep.spec.cpu):
            return False
        gpu_total = dt_nodes.GPU_MEMORY_GIB.get(str(node.get("type", "")))
        llm_jobs = [j for j in node.get("jobs") or [] if str(j.get("name", "")).startswith("llm-") and j.get("name") != dep.internal_name]
        if gpu_total is None:
            return not llm_jobs
        used = 0.0
        registry = self._store.load(max_age=5)
        for job in llm_jobs:
            other = next((d for d in registry.deployments.values() if d.internal_name == job.get("name")), None)
            used += _parse_size_gib(other.spec.vram) if other else gpu_total
        return used + _parse_size_gib(dep.spec.vram) <= gpu_total

    def _do_provision(self, dep: Deployment, registry: Registry, nodes: list[dict[str, Any]]) -> None:
        node = self._pick_node(dep, nodes)
        if node is not None:
            dep.node_id = node.get("id") or node.get("instance_id")
            dep.node_name = node.get("name")
            dep.transition(DeploymentState.STARTING, f"Using GPU node {dep.node_name}")
            return
        # A node we created for this deployment may still be booting.
        pending = [m for m in registry.managed_nodes if m.created_for == dep.slug and time.time() - m.created_at < NODE_READY_TIMEOUT]
        if pending:
            dep.transition(DeploymentState.WAITING_NODE, "Waiting for the new GPU node to come online")
            return
        before = {n.get("id") or n.get("instance_id") for n in nodes}
        result = dt_nodes.new_nodes(dep.spec.instance_type, dep.spec.environment, dep.spec.disk_size)
        managed = ManagedNode(
            instance_type=dep.spec.instance_type,
            environment=dep.spec.environment,
            disk_size=dep.spec.disk_size,
            created_for=dep.slug,
            created_by=dep.created_by,
            request_result=result,
        )
        node_id = _extract_node_id(result)
        if node_id is None:
            after = self._safe_nodes()
            fresh = [n for n in after if (n.get("id") or n.get("instance_id")) not in before]
            if len(fresh) == 1:
                node_id = fresh[0].get("id") or fresh[0].get("instance_id")
                managed.name = fresh[0].get("name")
        managed.node_id = node_id
        registry.managed_nodes.append(managed)
        registry.add_event(f"Created GPU node {dep.spec.instance_type} ({dep.spec.disk_size} GB disk) for {dep.slug}", deployment=dep.slug)
        dep.node_id = node_id
        dep.transition(
            DeploymentState.WAITING_NODE,
            f"Requested a new {dep.spec.instance_type} GPU node with {dep.spec.disk_size} GB disk",
        )

    def _do_wait_node(self, dep: Deployment, registry: Registry, nodes: list[dict[str, Any]]) -> None:
        managed = next((m for m in registry.managed_nodes if m.created_for == dep.slug and (m.node_id is None or m.node_id == dep.node_id)), None)
        target = dt_nodes.find_node(nodes, dep.node_id) if dep.node_id else None
        if target is None and managed is not None and managed.node_id is None:
            # Identify the node by creation time: newest GPU node of the requested type.
            fresh = [
                n
                for n in nodes
                if n.get("type") == dep.spec.instance_type
                and n.get("environment") == dep.spec.environment
                and float(n.get("created_at") or 0) >= managed.created_at - 120
            ]
            if fresh:
                fresh.sort(key=lambda n: float(n.get("created_at") or 0))
                target = fresh[-1]
                managed.node_id = dep.node_id = target.get("id") or target.get("instance_id")
                managed.name = target.get("name")
        if target is not None and dt_nodes.node_is_ready(target):
            dep.node_name = target.get("name")
            if managed:
                managed.name = target.get("name")
            dep.transition(DeploymentState.STARTING, f"GPU node {dep.node_name} is running")
            return
        if target is not None:
            dep.node_name = target.get("name")
        if time.time() - dep.state_since > NODE_READY_TIMEOUT:
            raise RuntimeError("GPU node did not become ready in time")

    # starting / running ----------------------------------------------------
    def _do_starting(self, dep: Deployment, nodes: list[dict[str, Any]]) -> None:
        run = services.latest_run(dep)
        run_state = str((run or {}).get("state", "")).lower()
        if run is None or run_state in {"stopped", "failed", "unknown"}:
            if dep.run_id is not None and run is not None and run.get("run_id") == dep.run_id and run_state == "failed":
                raise RuntimeError(f"Service run {dep.run_id} failed: {run.get('error_message') or 'see logs'}")
            if dep.run_id is not None and run is not None and run.get("run_id") == dep.run_id:
                # Our run ended; do not loop forever restarting it.
                raise RuntimeError(f"Service run {dep.run_id} ended with state {run_state}")
            requested = self._start_requested.get(dep.slug, 0.0)
            if time.time() - requested < START_GRACE_SECONDS:
                return  # start already requested; the new run has not shown up yet
            service = services.open_service(dep)
            ok, message = service.start()
            if not ok:
                raise RuntimeError(f"Service start failed: {message}")
            self._start_requested[dep.slug] = time.time()
            dep.add_event(f"Service {dep.internal_name} start requested")
            dep.run_id = None
            return
        if dep.run_id is None and run.get("run_id") is not None:
            self._start_requested.pop(dep.slug, None)
        dep.run_id = run.get("run_id")
        self._locate_node(dep, nodes, run)
        if run_state == "running" and services.check_health(dep):
            dep.transition(DeploymentState.RUNNING, f"{dep.spec.model} is serving requests")
            return
        if time.time() - dep.state_since > SERVICE_READY_TIMEOUT:
            raise RuntimeError("vLLM did not become healthy in time (model download or load may have failed)")

    def _do_running(self, dep: Deployment, nodes: list[dict[str, Any]]) -> None:
        run = services.latest_run(dep)
        run_state = str((run or {}).get("state", "")).lower()
        if run is not None:
            dep.run_id = run.get("run_id")
            self._locate_node(dep, nodes, run)
        if run is None or run_state in {"stopped", "failed", "unknown"}:
            dep.transition(DeploymentState.STARTING, f"Service run is {run_state or 'missing'}; restarting", level="warning")
            return
        if not services.check_health(dep):
            dep.add_event("Health check failed; waiting for recovery", level="warning")
            dep.transition(DeploymentState.STARTING)

    def _locate_node(self, dep: Deployment, nodes: list[dict[str, Any]], run: dict[str, Any]) -> None:
        instance_id = run.get("instance_id")
        node = dt_nodes.find_node(nodes, instance_id) if instance_id else None
        if node is None:
            for candidate in nodes:
                if any(j.get("name") == dep.internal_name for j in candidate.get("jobs") or []):
                    node = candidate
                    break
        if node is not None:
            dep.node_id = node.get("id") or node.get("instance_id")
            dep.node_name = node.get("name")

    # stopping -----------------------------------------------------------------
    def _do_stopping(self, dep: Deployment) -> None:
        try:
            service = services.open_service(dep)
            service.stop()
        except Exception as exc:  # noqa: BLE001
            dep.add_event(f"Stop returned: {exc}", level="warning")
        dep.run_id = None
        dep.transition(DeploymentState.STOPPED, "Service stopped")


def _extract_node_id(result: Any) -> str | None:
    if isinstance(result, dict):
        for key in ("id", "instance_id", "node_id"):
            if result.get(key):
                return str(result[key])
        for value in result.values():
            found = _extract_node_id(value)
            if found:
                return found
    if isinstance(result, list):
        if len(result) == 1:
            return _extract_node_id(result[0])
        for item in result:
            found = _extract_node_id(item)
            if found:
                return found
    if isinstance(result, str) and result.startswith("i-"):
        return result
    return None
