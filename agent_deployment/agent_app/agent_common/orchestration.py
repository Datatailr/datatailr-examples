"""Shared orchestration primitives: limits, identifiers, workflow naming, and
the Blob layout for assignments / results / registry.

Everything here is pure/stateless and safe to import from either the App
(coordinator) or a sub-agent task container. Datatailr SDK imports are done
lazily inside functions so the module also imports cleanly off-platform
(e.g. local unit runs) where ``datatailr`` may be unavailable.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from typing import Any, Optional

# --------------------------------------------------------------------------- #
# Environment resolution (mirrors gas_curve_backtest.workflow_io._environment)
# --------------------------------------------------------------------------- #
def environment():
    """Resolve the platform ``Environment`` for the running container."""
    from datatailr import Environment

    env_name = (os.environ.get("DATATAILR_JOB_ENVIRONMENT") or "dev").lower()
    return {
        "dev": Environment.DEV,
        "pre": Environment.PRE,
        "prod": Environment.PROD,
    }.get(env_name, Environment.DEV)


# --------------------------------------------------------------------------- #
# Central limits (KV: agent_limits) -- see specification §11
# --------------------------------------------------------------------------- #
LIMITS_KV_KEY = os.environ.get("AGENT_LIMITS_KV_KEY", "agent_limits")

DEFAULT_LIMITS: dict[str, Any] = {
    "per_turn_timeout_s": 600,  # PI_TIMEOUT_SECONDS -- hard ceiling per pi turn
    "max_turns": 25,  # max pi turns per sub-agent
    "wall_clock": "45m",  # workflow fail_after
    "max_cost_usd": 3.0,  # abort when accumulated usage.cost exceeds this
    "max_depth": 2,  # sub-agents may nest at most this deep
    "max_children_per_agent": 8,  # fan-out cap per parent
    "max_active_global": 40,  # concurrent sub-agents across the system
    "max_total_per_request": 30,  # sub-agents spawned per originating request
}


def load_limits() -> dict[str, Any]:
    """Load ``agent_limits`` from KV, merged over the defaults.

    ``KV().get()`` may return a string or an already-parsed object and raises
    if the key is missing (per the kv-and-secrets skill) -- both are handled.
    Only keys present in ``DEFAULT_LIMITS`` are honored so a malformed KV entry
    can never remove a bound.
    """
    limits = dict(DEFAULT_LIMITS)
    try:
        from datatailr import KV

        value = KV().get(LIMITS_KV_KEY)
        if isinstance(value, str):
            value = json.loads(value)
        if isinstance(value, dict):
            for key in DEFAULT_LIMITS:
                if key in value and value[key] is not None:
                    limits[key] = value[key]
    except Exception:
        pass
    return limits


def default_budget(limits: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """A per-sub-agent budget derived from the central limits (§9 schema)."""
    limits = limits or load_limits()
    return {
        "max_turns": int(limits["max_turns"]),
        "turn_timeout_s": int(limits["per_turn_timeout_s"]),
        "wall_clock": str(limits["wall_clock"]),
        "max_cost_usd": float(limits["max_cost_usd"]),
        # By default a spawned sub-agent may not spawn its own children; the
        # coordinator raises this only when depth budget explicitly allows it.
        "max_child_agents": 0,
    }


# --------------------------------------------------------------------------- #
# Identifiers & workflow naming -- see specification §8
# --------------------------------------------------------------------------- #
_SLUG_RE = re.compile(r"[^a-z0-9]+")

# Workflow display-name prefix. The coordinator looks workflows up by *exact*
# display name, so this must stay stable across the App and any tooling.
WORKFLOW_NAME_PREFIX = "SWE Sub-Agent"


def slug(text: str, max_len: int = 40) -> str:
    """Lower-case, hyphenated, filesystem/URL-safe slug of a task title."""
    cleaned = _SLUG_RE.sub("-", (text or "").strip().lower()).strip("-")
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip("-")
    return cleaned or "task"


def short_uuid(n: int = 8) -> str:
    """``n`` hex chars of a random uuid -- collision-proof spawn suffix."""
    return uuid.uuid4().hex[:n]


def make_subagent_id(parent_id: str, seq: int) -> str:
    """``{parent_id}.{seq:03d}.{shortuuid8}`` (§8).

    ``parent_id`` is the main session id (or the parent sub-agent's id when
    nested); ``seq`` is a per-parent monotonic counter for readability; the
    8-hex suffix guarantees global uniqueness across restarts and simultaneous
    spawns even if ``seq`` collides.
    """
    return f"{parent_id}.{seq:03d}.{short_uuid()}"


def workflow_name(subagent_id: str, task_title: str) -> str:
    """The unique, deterministic workflow display name for a sub-agent (§8).

    e.g. ``SWE Sub-Agent — add-retry-to-client [sess-9f2.007.a3b1c8d0]``.
    The parent must persist this exact string (in the registry/assignment) to
    reopen the handle later -- names are never re-derived by guessing.
    """
    return f"{WORKFLOW_NAME_PREFIX} — {slug(task_title)} [{subagent_id}]"


# --------------------------------------------------------------------------- #
# Blob layout (shared handoff medium) -- see specification §4
# --------------------------------------------------------------------------- #
RUNS_ROOT = os.environ.get("AGENT_RUNS_PREFIX", "agent_runs")
REGISTRY_ROOT = os.environ.get("AGENT_REGISTRY_PREFIX", "agent_registry")


def assignment_key(subagent_id: str) -> str:
    return f"{RUNS_ROOT}/{subagent_id}/assignment.json"


def result_key(subagent_id: str) -> str:
    return f"{RUNS_ROOT}/{subagent_id}/result.json"


def logs_prefix(subagent_id: str) -> str:
    return f"{RUNS_ROOT}/{subagent_id}/logs"


def stop_flag_key(subagent_id: str) -> str:
    return f"{RUNS_ROOT}/{subagent_id}/stop.flag"


def callback_key(subagent_id: str) -> str:
    return f"{RUNS_ROOT}/{subagent_id}/callback.json"


def registry_key(parent_id: str) -> str:
    return f"{REGISTRY_ROOT}/{parent_id}.json"


# --------------------------------------------------------------------------- #
# Blob JSON helpers (tolerant of a missing Blob client off-platform)
# --------------------------------------------------------------------------- #
def blob_client():
    """Return a Datatailr ``Blob`` client, or ``None`` if unavailable."""
    try:
        from datatailr import Blob

        return Blob()
    except Exception:
        return None


def put_json(key: str, obj: Any, blob=None) -> bool:
    blob = blob or blob_client()
    if blob is None:
        return False
    try:
        blob.put(key, json.dumps(obj, default=str).encode("utf-8"))
        return True
    except Exception:
        return False


def put_bytes(key: str, data: bytes, blob=None) -> bool:
    """Store raw bytes at ``key`` (used for log/transcript artifacts, §4)."""
    blob = blob or blob_client()
    if blob is None:
        return False
    try:
        blob.put(key, data if isinstance(data, bytes) else str(data).encode("utf-8"))
        return True
    except Exception:
        return False


def get_json(key: str, blob=None) -> Optional[Any]:
    blob = blob or blob_client()
    if blob is None:
        return None
    try:
        if hasattr(blob, "exists") and not blob.exists(key):
            return None
        raw = blob.get(key)
    except Exception:
        return None
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def blob_exists(key: str, blob=None) -> bool:
    blob = blob or blob_client()
    if blob is None:
        return False
    try:
        return bool(blob.exists(key))
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Terminal-state classification for Workflow run states
# --------------------------------------------------------------------------- #
# Datatailr run states (see job-observability skill): pending -> running ->
# completed | failed | failed_after | out_of_memory | stopped | expired.
TERMINAL_RUN_STATES = {
    "completed",
    "failed",
    "failed_after",
    "out_of_memory",
    "stopped",
    "expired",
}


def is_terminal(state: Optional[str]) -> bool:
    return (state or "").lower() in TERMINAL_RUN_STATES
