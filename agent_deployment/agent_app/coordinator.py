"""Coordinator: the only place that talks to the ``Workflow`` API (§7).

Owns the sub-agent lifecycle for the main agent:

  * **spawn**  -- enforce limits, allocate id + unique workflow name, write the
    assignment to Blob, build-and-launch the workflow, record it in the run
    registry (§7 spawn, §8 naming, §9 assignment).
  * **track & harvest** -- a background poller opens each child by exact name,
    polls run state via the ``Workflow`` API, and on a terminal state reads the
    ``result.json`` and folds the report back into the originating pi session
    (§7 track & harvest).
  * **stop / watchdog** -- cooperative stop flags plus a hard-stop watchdog for
    runaway or orphaned runs (§11 point 9).

All limits (depth, fan-out, global active, per-request total, dedup) are
enforced *before* a workflow is launched (§11 points 5, 6, 8).
"""

from __future__ import annotations

import hashlib
import logging
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from agent_app.agent_common import orchestration

log = logging.getLogger("agent_app.coordinator")

# How often the background poller harvests terminal runs.
POLL_INTERVAL_S = 15
# Grace period beyond wall_clock before the watchdog hard-stops a run.
WATCHDOG_GRACE_S = 120

_DURATION_RE = re.compile(r"(\d+)\s*([smhd])", re.IGNORECASE)


def _parse_duration(text: str) -> int:
    """Parse ``"45m"`` / ``"2h"`` / ``"90s"`` into seconds (default 45m)."""
    if not text:
        return 2700
    total = 0
    for value, unit in _DURATION_RE.findall(str(text)):
        mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit.lower()]
        total += int(value) * mult
    return total or 2700


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Coordinator:
    """Spawn, track, and harvest sub-agents for the main agent."""

    def __init__(self, report_sink: Optional[Callable[[dict, dict], None]] = None):
        # report_sink(child_entry, result) folds a finished sub-agent's report
        # back into the parent pi session; wired by the App so the coordinator
        # stays free of session/workspace specifics.
        self._report_sink = report_sink

        self._lock = threading.RLock()
        self._children: dict[str, dict[str, Any]] = {}  # subagent_id -> entry
        self._by_parent: dict[str, list[str]] = {}       # parent_id -> [subagent_id]
        self._seq: dict[str, int] = {}                   # parent_id -> counter
        self._request_count: dict[str, int] = {}         # request_id -> total spawned
        self._dedup: dict[str, str] = {}                 # hash -> subagent_id

        self._wf_cache: dict[tuple, Any] = {}
        self._poller: Optional[threading.Thread] = None
        self._stop_poller = threading.Event()

        self._limits = orchestration.DEFAULT_LIMITS
        self._rehydrate()

    # ------------------------------------------------------------------ #
    # Limits
    # ------------------------------------------------------------------ #
    def refresh_limits(self) -> dict[str, Any]:
        self._limits = orchestration.load_limits()
        return self._limits

    # ------------------------------------------------------------------ #
    # Registry persistence
    # ------------------------------------------------------------------ #
    def _persist_parent(self, parent_id: str) -> None:
        children = [self._children[c] for c in self._by_parent.get(parent_id, [])]
        orchestration.put_json(
            orchestration.registry_key(parent_id),
            {"parent_id": parent_id, "updated": _now_iso(), "children": children},
        )

    def _rehydrate(self) -> None:
        """Reload known registries from Blob after an App restart (best effort)."""
        blob = orchestration.blob_client()
        if blob is None:
            return
        try:
            entries = blob.ls(f"{orchestration.REGISTRY_ROOT}/")
        except Exception:
            return
        for entry in entries or []:
            key = entry.get("name") if isinstance(entry, dict) else entry
            if not key or not str(key).endswith(".json"):
                continue
            doc = orchestration.get_json(str(key), blob=blob)
            if not isinstance(doc, dict):
                continue
            parent_id = doc.get("parent_id")
            for child in doc.get("children", []):
                sid = child.get("subagent_id")
                if not sid:
                    continue
                self._children[sid] = child
                self._by_parent.setdefault(parent_id, []).append(sid)
                self._seq[parent_id] = max(self._seq.get(parent_id, 0), child.get("seq", 0))
                if not orchestration.is_terminal(child.get("state")) and not child.get(
                    "reported"
                ):
                    self._dedup.setdefault(child.get("dedup_hash", ""), sid)

    # ------------------------------------------------------------------ #
    # Counting helpers (call under _lock)
    # ------------------------------------------------------------------ #
    def _active_children_of(self, parent_id: str) -> int:
        return sum(
            1
            for sid in self._by_parent.get(parent_id, [])
            if not self._children[sid].get("reported")
            and not orchestration.is_terminal(self._children[sid].get("state"))
        )

    def _active_global(self) -> int:
        return sum(
            1
            for c in self._children.values()
            if not c.get("reported") and not orchestration.is_terminal(c.get("state"))
        )

    @staticmethod
    def _dedup_hash(parent_id: str, title: str, instructions: str) -> str:
        raw = f"{parent_id}\x00{title}\x00{instructions}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]

    # ------------------------------------------------------------------ #
    # Spawn (§7, §8, §9, §11)
    # ------------------------------------------------------------------ #
    def spawn(
        self,
        parent_id: str,
        brief: dict[str, Any],
        *,
        created_by: str,
        depth: int = 0,
        session_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Enforce limits, then build-and-launch one sub-agent.

        Returns ``{"subagent_id", "name", "state"}`` on success, or
        ``{"refused": True, "reason", "message"}`` when a limit would be
        exceeded (the message is meant to be shown to the requesting pi).
        """
        limits = self.refresh_limits()
        title = str(brief.get("title") or brief.get("task") or "task").strip()
        instructions = str(brief.get("instructions") or brief.get("task") or "").strip()
        request_id = request_id or parent_id

        if not instructions:
            return self._refuse("empty_task", "The task brief has no instructions.")

        with self._lock:
            # 5. Recursion depth.
            if depth >= int(limits["max_depth"]):
                return self._refuse(
                    "max_depth",
                    f"Recursion depth limit reached (max_depth={limits['max_depth']}). "
                    "Finish the work directly instead of delegating further.",
                )
            # 6. Fan-out per parent.
            if self._active_children_of(parent_id) >= int(limits["max_children_per_agent"]):
                return self._refuse(
                    "max_children_per_agent",
                    f"Fan-out limit reached for this agent "
                    f"(max_children_per_agent={limits['max_children_per_agent']}).",
                )
            # 6. Global active cap.
            if self._active_global() >= int(limits["max_active_global"]):
                return self._refuse(
                    "max_active_global",
                    f"System is at its concurrent sub-agent capacity "
                    f"(max_active_global={limits['max_active_global']}). Try again shortly.",
                )
            # Main-agent convergence: total per originating request.
            if self._request_count.get(request_id, 0) >= int(limits["max_total_per_request"]):
                return self._refuse(
                    "max_total_per_request",
                    f"This request has reached its sub-agent budget "
                    f"(max_total_per_request={limits['max_total_per_request']}). "
                    "Summarize with the results already gathered.",
                )
            # 8. Idempotency / dedup.
            dhash = self._dedup_hash(parent_id, title, instructions)
            if dhash in self._dedup:
                existing = self._dedup[dhash]
                return self._refuse(
                    "duplicate",
                    f"An identical task is already active as sub-agent {existing}.",
                    subagent_id=existing,
                )

            seq = self._seq.get(parent_id, 0) + 1
            self._seq[parent_id] = seq
            subagent_id = orchestration.make_subagent_id(parent_id, seq)
            name = orchestration.workflow_name(subagent_id, title)

            # Reserve counters/dedup before the (slow) launch so concurrent
            # spawns see the reservation.
            self._request_count[request_id] = self._request_count.get(request_id, 0) + 1
            self._dedup[dhash] = subagent_id

        assignment = self._build_assignment(
            subagent_id=subagent_id,
            parent_id=parent_id,
            created_by=created_by,
            depth=depth + 1,
            title=title,
            instructions=instructions,
            brief=brief,
            limits=limits,
        )
        orchestration.put_json(orchestration.assignment_key(subagent_id), assignment)

        entry = {
            "subagent_id": subagent_id,
            "seq": seq,
            "name": name,
            "parent_id": parent_id,
            "request_id": request_id,
            "created_by": created_by,
            "session_id": session_id,
            "depth": depth + 1,
            "title": title,
            "dedup_hash": dhash,
            "state": "launched",
            "run_id": None,
            "reported": False,
            "launched_at": _now_iso(),
            "wall_clock_s": _parse_duration(assignment["budget"]["wall_clock"]),
            "status": None,
            "pr_url": None,
            "pr_error": None,
            "summary": None,
            "warnings": [],
            "cost": 0.0,
            "turns": 0,
        }

        launched = self._launch(subagent_id, name, assignment)
        entry["state"] = "launched" if launched else "failed"
        if not launched:
            entry["status"] = "failed"
            entry["reason"] = "launch_failed"

        with self._lock:
            self._children[subagent_id] = entry
            self._by_parent.setdefault(parent_id, []).append(subagent_id)
            if not launched:
                # Roll back the reservations so a retry is allowed.
                self._dedup.pop(dhash, None)
                self._request_count[request_id] = max(
                    0, self._request_count.get(request_id, 1) - 1
                )
            self._persist_parent(parent_id)

        if not launched:
            return self._refuse(
                "launch_failed",
                f"Failed to launch sub-agent workflow '{name}'.",
                subagent_id=subagent_id,
            )
        log.info("spawned sub-agent %s (%s)", subagent_id, name)
        return {"subagent_id": subagent_id, "name": name, "state": entry["state"]}

    def _refuse(self, reason: str, message: str, subagent_id: Optional[str] = None) -> dict:
        out = {"refused": True, "reason": reason, "message": message}
        if subagent_id:
            out["subagent_id"] = subagent_id
        return out

    def _build_assignment(
        self,
        *,
        subagent_id: str,
        parent_id: str,
        created_by: str,
        depth: int,
        title: str,
        instructions: str,
        brief: dict[str, Any],
        limits: dict[str, Any],
    ) -> dict[str, Any]:
        from agent_app.agent_common import git_bootstrap

        base_branch = brief.get("base_branch") or git_bootstrap.default_branch()
        short = subagent_id.rsplit(".", 1)[-1]
        work_branch = brief.get("branch") or f"agent/{orchestration.slug(title)}-{short}"

        budget = orchestration.default_budget(limits)
        override = brief.get("budget") or {}
        for key in ("max_turns", "turn_timeout_s", "wall_clock", "max_cost_usd", "max_child_agents"):
            if key in override and override[key] is not None:
                budget[key] = override[key]
        # A sub-agent may only spawn children if depth still permits it.
        if depth >= int(limits["max_depth"]):
            budget["max_child_agents"] = 0

        return {
            "subagent_id": subagent_id,
            "parent_id": parent_id,
            "created_by": created_by,
            "depth": depth,
            "task": {
                "title": title,
                "instructions": instructions,
                "definition_of_done": brief.get("definition_of_done") or [],
                "context_files": brief.get("files") or brief.get("context_files") or [],
            },
            "git": {
                "repo_url": git_bootstrap.repo_url(),
                "base_branch": base_branch,
                "work_branch": work_branch,
                "may_push": bool(brief.get("may_push", True)),
                "may_open_pr": bool(brief.get("may_open_pr", True)),
            },
            "budget": budget,
        }

    def _launch(self, subagent_id: str, name: str, assignment: dict[str, Any]) -> bool:
        try:
            from agent_app.subagent.build import build_subagent_workflow

            budget = assignment["budget"]
            wf_fn = build_subagent_workflow(
                subagent_id,
                name,
                wall_clock=str(budget["wall_clock"]),
                turn_timeout_s=int(budget["turn_timeout_s"]),
                model=None,
                thinking=None,
            )
            wf_fn()  # build-then-call launches the run
            return True
        except Exception as exc:  # noqa: BLE001
            log.error("launch of %s failed: %s", subagent_id, exc)
            return False

    # ------------------------------------------------------------------ #
    # Workflow API access
    # ------------------------------------------------------------------ #
    def _open_workflow(self, name: str):
        from datatailr.scheduler.batch import Workflow

        env = orchestration.environment()
        key = (name, env.value)
        with self._lock:
            wf = self._wf_cache.get(key)
        if wf is None:
            wf = Workflow(name=name, environment=env, get_existing=True)
            with self._lock:
                self._wf_cache[key] = wf
        return wf

    @staticmethod
    def _run_sort_key(run: dict) -> float:
        """Epoch-seconds sort key that never mixes naive/aware datetimes.

        ``Workflow.runs()`` returns *naive* ``start_time`` datetimes (from
        ``datetime.fromtimestamp``) for started runs and ``0`` for not-yet-started
        ones. Comparing those against a tz-aware fallback (as the old key did)
        raises ``TypeError`` once a workflow has more than one run (e.g. a retry),
        which would abort the whole poll cycle. Normalizing to a float avoids that.
        """
        st = run.get("start_time")
        if isinstance(st, datetime):
            return st.timestamp()
        if isinstance(st, (int, float)):
            return float(st)
        return 0.0

    def _latest_run(self, wf) -> Optional[dict]:
        try:
            runs = wf.runs(refresh=True) or []
        except Exception:
            return None
        if not runs:
            return None
        return max(runs, key=self._run_sort_key)

    # ------------------------------------------------------------------ #
    # Track & harvest (§7)
    # ------------------------------------------------------------------ #
    def start_poller(self) -> None:
        if self._poller and self._poller.is_alive():
            return
        self._stop_poller.clear()
        self._poller = threading.Thread(
            target=self._poll_loop, name="subagent-poller", daemon=True
        )
        self._poller.start()

    def stop(self) -> None:
        self._stop_poller.set()

    def _poll_loop(self) -> None:
        while not self._stop_poller.wait(POLL_INTERVAL_S):
            try:
                self.harvest_once()
            except Exception as exc:  # noqa: BLE001
                log.warning("harvest cycle failed: %s", exc)

    def harvest_once(self) -> None:
        with self._lock:
            pending = [
                dict(c)
                for c in self._children.values()
                if not c.get("reported")
            ]
        for entry in pending:
            # Isolate per-child failures so one bad handle cannot starve the
            # rest of the cycle (the whole cycle is retried on the next poll).
            try:
                self._refresh_and_maybe_report(entry["subagent_id"])
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "harvest of %s failed: %s", entry.get("subagent_id"), exc
                )

    def _refresh_and_maybe_report(self, subagent_id: str) -> None:
        with self._lock:
            entry = self._children.get(subagent_id)
            if not entry or entry.get("reported"):
                return
            name = entry["name"]

        # A callback nudge (optional push path) lets us harvest promptly; the
        # Blob result remains the source of truth regardless.
        try:
            wf = self._open_workflow(name)
        except Exception as exc:  # noqa: BLE001
            log.debug("cannot open workflow %s: %s", name, exc)
            return

        run = self._latest_run(wf)
        state = (run or {}).get("state")
        run_id = (run or {}).get("run_id")

        with self._lock:
            entry = self._children.get(subagent_id)
            if not entry:
                return
            if run_id is not None:
                entry["run_id"] = run_id
            if state:
                entry["state"] = state
            parent_id = entry["parent_id"]

        # Watchdog: hard-stop runs that overrun wall_clock + grace (§11 point 9).
        self._maybe_watchdog(subagent_id)

        if not orchestration.is_terminal(state):
            return

        result = orchestration.get_json(orchestration.result_key(subagent_id))
        if result is None and run_id is not None:
            try:
                result = wf.result(run_id=run_id, task_name="run_subagent")
            except Exception:
                result = None
        if result is None:
            # Terminal run but no result document (e.g. failed_after / OOM).
            result = {
                "subagent_id": subagent_id,
                "status": self._state_to_status(state),
                "reason": state,
                "summary": f"Run ended in state '{state}' with no result document.",
                "final_reply": "",
                "git": {"pushed": False, "pr": None},
                "usage": {"cost": 0.0, "totalTokens": 0},
                "turns": 0,
            }

        self._finalize(subagent_id, parent_id, result)

    @staticmethod
    def _state_to_status(state: Optional[str]) -> str:
        return {
            "completed": "succeeded",
            "failed": "failed",
            "failed_after": "timed_out",
            "out_of_memory": "failed",
            "stopped": "stopped",
            "expired": "stopped",
        }.get((state or "").lower(), "failed")

    def _finalize(self, subagent_id: str, parent_id: str, result: dict) -> None:
        with self._lock:
            entry = self._children.get(subagent_id)
            if not entry or entry.get("reported"):
                return
            entry["reported"] = True
            entry["status"] = result.get("status")
            entry["turns"] = result.get("turns", 0)
            entry["cost"] = float((result.get("usage") or {}).get("cost", 0.0) or 0.0)
            pr = (result.get("git") or {}).get("pr")
            entry["pr_url"] = pr.get("url") if isinstance(pr, dict) else None
            entry["pr_error"] = pr.get("error") if isinstance(pr, dict) else None
            entry["summary"] = result.get("summary")
            entry["warnings"] = result.get("warnings") or []
            entry["reported_at"] = _now_iso()
            # Free the dedup slot now the assignment is complete.
            self._dedup.pop(entry.get("dedup_hash", ""), None)
            self._persist_parent(parent_id)
            sink = self._report_sink
            entry_copy = dict(entry)

        log.info("harvested sub-agent %s -> %s", subagent_id, result.get("status"))
        if sink:
            try:
                sink(entry_copy, result)
            except Exception as exc:  # noqa: BLE001
                log.warning("report sink failed for %s: %s", subagent_id, exc)

    # ------------------------------------------------------------------ #
    # Stop / watchdog (§11 point 9)
    # ------------------------------------------------------------------ #
    def request_stop(self, subagent_id: str) -> dict[str, Any]:
        """Cooperative stop: set the flag the sub-agent checks between turns."""
        with self._lock:
            entry = self._children.get(subagent_id)
        if not entry:
            return {"ok": False, "error": "unknown sub-agent"}
        orchestration.put_json(
            orchestration.stop_flag_key(subagent_id),
            {"stop": True, "requested_at": _now_iso()},
        )
        with self._lock:
            entry["stop_requested_at"] = time.time()
            self._persist_parent(entry["parent_id"])
        log.info("cooperative stop requested for %s", subagent_id)
        return {"ok": True, "subagent_id": subagent_id, "mode": "cooperative"}

    def _maybe_watchdog(self, subagent_id: str) -> None:
        with self._lock:
            entry = self._children.get(subagent_id)
            if not entry or entry.get("reported"):
                return
            launched = entry.get("launched_at")
            wall = entry.get("wall_clock_s", 2700)
            name = entry["name"]
            stop_req = entry.get("stop_requested_at")
        try:
            started = datetime.fromisoformat(launched)
        except (TypeError, ValueError):
            return
        age = (datetime.now(timezone.utc) - started).total_seconds()

        overran = age > (wall + WATCHDOG_GRACE_S)
        stalled_stop = stop_req is not None and (time.time() - stop_req) > WATCHDOG_GRACE_S
        if overran or stalled_stop:
            self._hard_stop(name, subagent_id, "overran" if overran else "stop_stalled")

    def _hard_stop(self, name: str, subagent_id: str, why: str) -> None:
        """Issue ``dt job stop`` for a runaway/orphaned run.

        Mutating job commands are only ever issued here by the system watchdog,
        never surfaced as a casual agent action (§11 point 9).
        """
        log.warning("watchdog hard-stopping %s (%s): %s", subagent_id, why, name)
        try:
            subprocess.run(
                ["dt", "job", "stop", name],
                capture_output=True,
                text=True,
                timeout=60,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("dt job stop failed for %s: %s", name, exc)

    def notify_callback(self, subagent_id: str) -> None:
        """Optional low-latency wake-up (callback Service) to harvest now (§10)."""
        try:
            self._refresh_and_maybe_report(subagent_id)
        except Exception as exc:  # noqa: BLE001
            log.debug("callback harvest for %s failed: %s", subagent_id, exc)

    # ------------------------------------------------------------------ #
    # Read APIs for the App endpoints (§6)
    # ------------------------------------------------------------------ #
    def list_children(self, parent_id: str, *, refresh: bool = True) -> list[dict]:
        if refresh:
            with self._lock:
                sids = list(self._by_parent.get(parent_id, []))
            for sid in sids:
                self._refresh_and_maybe_report(sid)
        with self._lock:
            return [dict(self._children[c]) for c in self._by_parent.get(parent_id, [])]

    def list_children_for_user(self, created_by: str, *, refresh: bool = True) -> list[dict]:
        with self._lock:
            sids = [
                sid for sid, c in self._children.items() if c.get("created_by") == created_by
            ]
        if refresh:
            for sid in sids:
                self._refresh_and_maybe_report(sid)
        with self._lock:
            return [
                dict(c) for c in self._children.values() if c.get("created_by") == created_by
            ]

    def get_child(self, subagent_id: str, *, refresh: bool = True) -> Optional[dict]:
        with self._lock:
            entry = self._children.get(subagent_id)
            if not entry:
                return None
        if refresh:
            self._refresh_and_maybe_report(subagent_id)
        with self._lock:
            entry = dict(self._children.get(subagent_id, {}))
        assignment = orchestration.get_json(orchestration.assignment_key(subagent_id))
        result = orchestration.get_json(orchestration.result_key(subagent_id))
        return {"entry": entry, "assignment": assignment, "result": result}
