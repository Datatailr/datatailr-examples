"""Workflow accessors for the Flask cockpit.

All run discovery and result retrieval go through the platform's
`Workflow` API (`runs()`, `run_details()`, `result()`) rather than blob
storage. The parent workflow has a static name; the child workflow's
name is built deterministically by `detect_regimes_and_launch` from the
parent run id and the expected number of cells.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timezone
from typing import Any

PARENT_WORKFLOW_NAME = "Gas Curve Backtest — Parent"


def _environment():
    """Resolve the platform Environment for the running container."""
    from datatailr import Environment

    env_name = (os.environ.get("DATATAILR_JOB_ENVIRONMENT") or "dev").lower()
    return {
        "dev": Environment.DEV,
        "pre": Environment.PRE,
        "prod": Environment.PROD,
    }.get(env_name, Environment.DEV)


_lock = threading.Lock()
_cache: dict[str, Any] = {}


def _open_workflow(name: str):
    """Get an existing-deployed workflow handle, cached per process."""
    from datatailr.scheduler.batch import Workflow

    key = (name, _environment().value)
    with _lock:
        wf = _cache.get(key)
        if wf is None:
            wf = Workflow(name=name, environment=_environment(), get_existing=True)
            _cache[key] = wf
    return wf


def _isoformat(value: Any) -> str | None:
    """Best-effort ISO formatting for datetimes returned by the SDK."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _serialise_run(run: dict) -> dict:
    return {
        "run_id": run.get("run_id"),
        "state": run.get("state"),
        "start_time": _isoformat(run.get("start_time")),
        "end_time": _isoformat(run.get("end_time")),
        "job_version": run.get("job_version"),
        "original_run_id": run.get("original_run_id"),
    }


def _serialise_task(task: dict) -> dict:
    return {
        "id": task.get("id"),
        "name": task.get("name"),
        "state": task.get("state"),
        "start_time": _isoformat(task.get("start_time")),
        "end_time": _isoformat(task.get("end_time")),
    }


def list_parent_runs(limit: int = 50) -> list[dict]:
    try:
        wf = _open_workflow(PARENT_WORKFLOW_NAME)
        runs = wf.runs(refresh=True) or []
    except Exception:
        return []
    runs = sorted(
        runs,
        key=lambda r: r.get("start_time") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return [_serialise_run(r) for r in runs[:limit]]


def get_parent_run_details(run_id: int) -> dict | None:
    try:
        wf = _open_workflow(PARENT_WORKFLOW_NAME)
        details = wf.run_details(run_id=run_id) or {}
    except Exception:
        return None
    tasks = [_serialise_task(t) for t in details.get("tasks", [])]
    return {
        "run_id": run_id,
        "state": details.get("state"),
        "start_time": _isoformat(details.get("start_time")),
        "end_time": _isoformat(details.get("end_time")),
        "tasks": tasks,
    }


def get_parent_task_result(run_id: int, task_name: str) -> Any | None:
    try:
        wf = _open_workflow(PARENT_WORKFLOW_NAME)
        return wf.result(run_id=run_id, task_name=task_name)
    except Exception:
        return None


def child_workflow_name(parent_backtest_id: str, expected_cells: int) -> str:
    """Mirror the deterministic name built in `regime_workflow.build_regime_workflow`."""
    return f"Regime Sweep — {parent_backtest_id} ({expected_cells} cells)"


def get_child_summary(parent_backtest_id: str, expected_cells: int) -> dict | None:
    """Look up the child workflow by deterministic name and return the
    most-recent run plus its `aggregate` task result, if any.
    """
    name = child_workflow_name(parent_backtest_id, expected_cells)
    try:
        wf = _open_workflow(name)
        runs = wf.runs(refresh=True) or []
    except Exception:
        return {"name": name, "found": False, "runs": [], "result": None}
    if not runs:
        return {"name": name, "found": True, "runs": [], "result": None}
    latest = max(
        runs,
        key=lambda r: r.get("start_time") or datetime.min.replace(tzinfo=timezone.utc),
    )
    run_id = latest.get("run_id")
    details = None
    aggregate_result: Any = None
    try:
        details = wf.run_details(run_id=run_id) or {}
    except Exception:
        details = None
    try:
        aggregate_result = wf.result(run_id=run_id, task_name="aggregate")
    except Exception:
        aggregate_result = None
    tasks = [_serialise_task(t) for t in (details or {}).get("tasks", [])]
    return {
        "name": name,
        "found": True,
        "runs": [_serialise_run(r) for r in runs],
        "latest_run": _serialise_run(latest),
        "tasks": tasks,
        "result": aggregate_result,
    }


def collect_run_summary(run_id: int) -> dict:
    """Aggregate everything the dashboard needs to render a run page."""
    parent = get_parent_run_details(run_id)
    if parent is None:
        return {"run_id": run_id, "found": False}

    detect = get_parent_task_result(run_id, "detect_regimes_and_launch_child")
    backtest_id: str | None = None
    expected_cells: int | None = None
    regimes: list[dict] | None = None
    if isinstance(detect, dict):
        backtest_id = (
            str(detect.get("run_id")) if detect.get("run_id") is not None else None
        )
        expected_cells = detect.get("expected_cells")
        if isinstance(detect.get("regimes"), list):
            regimes = detect["regimes"]

    child = None
    if backtest_id and expected_cells:
        child = get_child_summary(backtest_id, int(expected_cells))

    return {
        "run_id": run_id,
        "found": True,
        "parent": parent,
        "backtest_id": backtest_id,
        "expected_cells": expected_cells,
        "detect_summary": detect if isinstance(detect, dict) else None,
        "regimes": regimes,
        "child": child,
    }


def get_cell_rows(parent_run_id: int) -> list[dict]:
    """Return every per-cell metric row produced by the child workflow."""
    summary = collect_run_summary(parent_run_id)
    child = summary.get("child") or {}
    result = child.get("result")
    if isinstance(result, dict):
        rows = result.get("rows")
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def trigger_parent_run(params: dict) -> dict:
    """Deploy a fresh parent backtest workflow run with the given params."""
    from gas_curve_backtest.workflows.parent_workflow import (
        make_run_id,
        parent_backtest_workflow,
    )

    rid = make_run_id()
    safe_params = {
        k: v
        for k, v in params.items()
        if k
        in {
            "n_days",
            "n_tenors",
            "n_regimes",
            "grid_signal_steps",
            "grid_pivot_steps",
            "bootstrap_samples",
        }
    }
    parent_backtest_workflow(rid, **safe_params)
    return {"run_id": rid}
