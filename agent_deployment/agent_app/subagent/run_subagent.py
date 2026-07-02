"""The single sub-agent task: bootstrap git -> run pi -> push/PR -> report.

Everything for one scoped task happens inside **one** container (Datatailr
tasks do not share a filesystem, so cloning in a separate task would not
persist -- see specification §3). The task takes only the small ``subagent_id``
as its argument and reads the full brief from ``agent_runs/<id>/assignment.json``
(§9). It writes ``agent_runs/<id>/result.json`` and returns the final reply so
the coordinator can harvest it either from Blob or via ``wf.result(...)`` (§10).
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Any, Optional

from datatailr import task
from datatailr.logging import DatatailrLogger

from agent_app import pi_runner
from agent_app.agent_common import git_bootstrap, orchestration

log = DatatailrLogger(__name__).get_logger()

# Sentinel the sub-agent's pi is instructed to emit once the definition of done
# is met; seeing it lets the turn loop converge instead of running open-ended.
DONE_SENTINEL = "<<<PI_TASK_COMPLETE>>>"

WORKDIR = os.environ.get("SUBAGENT_WORKDIR", "/tmp/subagent_repo")
OPENAI_SECRET_KEY = os.environ.get("OPENAI_SECRET_KEY", "openai_api_key")
MODEL_KV_KEY = os.environ.get("MODEL_KV_KEY", "agent_model")
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "openai/gpt-5-mini")
AGENT_THINKING = os.environ.get("AGENT_THINKING", "medium")


# --------------------------------------------------------------------------- #
# Config / secrets loading
# --------------------------------------------------------------------------- #
def _load_openai_key() -> bool:
    if os.environ.get("OPENAI_API_KEY"):
        return True
    try:
        from datatailr import Secrets

        key = Secrets().get(OPENAI_SECRET_KEY)
        if key:
            os.environ["OPENAI_API_KEY"] = key
            return True
    except Exception:
        pass
    return False


def _resolve_model() -> str:
    try:
        from datatailr import KV

        value = KV().get(MODEL_KV_KEY)
        if isinstance(value, str) and value.strip():
            return value.strip()
    except Exception:
        pass
    return DEFAULT_MODEL


# --------------------------------------------------------------------------- #
# Prompt assembly
# --------------------------------------------------------------------------- #
def _build_prompt(assignment: dict[str, Any]) -> str:
    task_spec = assignment.get("task", {})
    git_spec = assignment.get("git", {})
    dod = task_spec.get("definition_of_done") or []
    context_files = task_spec.get("context_files") or []

    dod_block = "\n".join(f"  - {item}" for item in dod) or "  - (none specified)"
    files_block = (
        "\n".join(f"  - {f}" for f in context_files) if context_files else "  - (none)"
    )

    return (
        f"You are a headless software-engineering sub-agent working in a fresh "
        f"clone of the repository, on branch `{git_spec.get('work_branch')}` "
        f"(based on `{git_spec.get('base_branch')}`).\n\n"
        f"# Task\n{task_spec.get('title', '(untitled)')}\n\n"
        f"{task_spec.get('instructions', '')}\n\n"
        f"# Definition of done (your explicit, checkable exit criteria)\n"
        f"{dod_block}\n\n"
        f"# Focus files (hints, not limits)\n{files_block}\n\n"
        f"# Operating rules\n"
        f"  - Make the smallest correct change that satisfies the definition of done.\n"
        f"  - Run the relevant tests/build to verify your change.\n"
        f"  - Use conventional, descriptive commit-style summaries in your final reply.\n"
        f"  - You may push your branch, but you must NEVER merge or modify protected branches.\n"
        f"  - Do not attempt to open or approve pull requests yourself; the harness handles that.\n"
        f"  - When (and only when) every definition-of-done item is satisfied, end your\n"
        f"    reply with the exact marker on its own line: {DONE_SENTINEL}\n"
        f"  - If you are blocked and cannot proceed, explain why and end with the marker\n"
        f"    followed by the word BLOCKED.\n"
    )


def _looks_done(reply: str) -> bool:
    return DONE_SENTINEL in (reply or "")


def _looks_blocked(reply: str) -> bool:
    text = reply or ""
    return DONE_SENTINEL in text and "BLOCKED" in text.rsplit(DONE_SENTINEL, 1)[-1]


# --------------------------------------------------------------------------- #
# Bounded pi turn loop (§11 enforcement points 1, 2, 4, 9)
# --------------------------------------------------------------------------- #
def _run_pi_loop(
    assignment: dict[str, Any],
    workdir: str,
    model: str,
) -> dict[str, Any]:
    budget = assignment.get("budget", {})
    max_turns = int(budget.get("max_turns", 25))
    max_cost = float(budget.get("max_cost_usd", 3.0))
    subagent_id = assignment["subagent_id"]

    session_id: Optional[str] = None
    final_reply = ""
    usage_total = pi_runner._new_usage()
    turns = 0
    status = "failed"
    reason: Optional[str] = "no_progress"

    prompt = _build_prompt(assignment)
    session_dir = os.path.join(workdir, ".pi_sessions")

    while turns < max_turns:
        # Cooperative stop: the coordinator can set a stop flag between turns.
        if orchestration.blob_exists(orchestration.stop_flag_key(subagent_id)):
            status, reason = "stopped", "stop_requested"
            log.info(f"[{subagent_id}] stop flag observed; halting after {turns} turns")
            break

        turns += 1
        log.info(f"[{subagent_id}] pi turn {turns}/{max_turns} (model={model})")
        try:
            result = pi_runner.run_pi(
                message=prompt,
                session_id=session_id,
                model=model,
                session_dir=session_dir,
                thinking=AGENT_THINKING,
                workspace_dir=workdir,
            )
        except subprocess.TimeoutExpired:
            status, reason = "timed_out", "turn_timeout"
            log.warning(f"[{subagent_id}] pi turn {turns} timed out")
            break
        except Exception as exc:  # noqa: BLE001
            status, reason = "failed", f"pi_error: {exc}"
            log.error(f"[{subagent_id}] pi turn {turns} failed: {exc}")
            break

        session_id = result.session_id or session_id
        if result.reply:
            final_reply = result.reply
        _accumulate_usage(usage_total, result.usage)

        if usage_total["cost"] > max_cost:
            status, reason = "stopped", "budget_exceeded"
            log.warning(
                f"[{subagent_id}] cost ${usage_total['cost']:.4f} exceeded "
                f"budget ${max_cost:.2f}"
            )
            break

        if _looks_done(final_reply):
            if _looks_blocked(final_reply):
                status, reason = "blocked", "agent_blocked"
            else:
                status, reason = "succeeded", None
            log.info(f"[{subagent_id}] converged after {turns} turns ({status})")
            break

        # Not done yet -- nudge pi to continue toward the definition of done.
        prompt = (
            "Continue toward the definition of done. If every item is now "
            f"satisfied, end with the marker {DONE_SENTINEL}."
        )
    else:
        status, reason = "stopped", "max_turns"
        log.warning(f"[{subagent_id}] reached max_turns={max_turns}")

    return {
        "status": status,
        "reason": reason,
        "final_reply": final_reply.replace(DONE_SENTINEL, "").strip(),
        "usage": usage_total,
        "turns": turns,
        "session_id": session_id,
    }


def _accumulate_usage(usage_total: dict[str, Any], usage: dict[str, Any]) -> None:
    """Fold one pi run's usage dict into the running total."""
    for key in ("input", "output", "cacheRead", "cacheWrite", "totalTokens"):
        usage_total[key] = int(usage_total.get(key, 0)) + int(usage.get(key, 0) or 0)
    usage_total["cost"] = float(usage_total.get("cost", 0.0)) + float(
        usage.get("cost", 0.0) or 0.0
    )


# --------------------------------------------------------------------------- #
# Git persistence + PR handling (§10 step 4)
# --------------------------------------------------------------------------- #
def _git(args: list[str], cwd: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, env=os.environ
    )


def _has_changes(workdir: str) -> bool:
    proc = _git(["status", "--porcelain"], workdir)
    return bool(proc.stdout.strip())


def _commit_and_push(
    assignment: dict[str, Any], workdir: str
) -> dict[str, Any]:
    git_spec = assignment.get("git", {})
    subagent_id = assignment["subagent_id"]
    title = assignment.get("task", {}).get("title", "agent change")
    work_branch = git_spec.get("work_branch")
    base_branch = git_spec.get("base_branch")

    git_info: dict[str, Any] = {
        "pushed": False,
        "branch": work_branch,
        "base_branch": base_branch,
        "commits": [],
        "pr": None,
    }

    if not git_spec.get("may_push"):
        log.info(f"[{subagent_id}] may_push=false; skipping commit/push")
        return git_info

    if not _has_changes(workdir):
        log.info(f"[{subagent_id}] no working-tree changes to commit")
        return git_info

    _git(["add", "-A"], workdir)
    commit_msg = (
        f"agent({subagent_id}): {title}\n\n"
        f"Assignment: {subagent_id}\n"
        f"Automated change by the SWE sub-agent. Review required; do not auto-merge."
    )
    commit = _git(["commit", "-m", commit_msg], workdir)
    if commit.returncode != 0:
        log.warning(f"[{subagent_id}] commit failed: {commit.stderr.strip()[:300]}")
        return git_info

    push = _git(["push", "-u", "origin", work_branch], workdir)
    if push.returncode != 0:
        log.warning(f"[{subagent_id}] push failed: {push.stderr.strip()[:300]}")
        return git_info

    git_info["pushed"] = True
    sha = _git(["rev-parse", "HEAD"], workdir)
    if sha.returncode == 0:
        git_info["commits"] = [sha.stdout.strip()[:9]]
    log.info(f"[{subagent_id}] pushed branch {work_branch}")

    if git_spec.get("may_open_pr"):
        git_info["pr"] = _open_or_update_pr(assignment, workdir)

    return git_info


def _gh_available() -> bool:
    try:
        return (
            subprocess.run(
                ["gh", "--version"], capture_output=True, text=True
            ).returncode
            == 0
        )
    except Exception:
        return False


def _open_or_update_pr(assignment: dict[str, Any], workdir: str) -> Optional[dict[str, Any]]:
    """Open a PR (or record the existing one) for the work branch. Never merge.

    Idempotent: if an open PR already exists for the branch (e.g. a workflow
    retry pushed more commits), we record ``action="updated"`` instead of
    opening a duplicate (§10 step 4).
    """
    import json as _json

    git_spec = assignment.get("git", {})
    subagent_id = assignment["subagent_id"]
    work_branch = git_spec.get("work_branch")
    base_branch = git_spec.get("base_branch")
    title = f"agent: {assignment.get('task', {}).get('title', 'change')}"

    token = git_bootstrap.git_token()
    if not token:
        log.info(f"[{subagent_id}] no git token; skipping PR (branch pushed)")
        return None
    if not _gh_available():
        log.info(f"[{subagent_id}] gh CLI unavailable; skipping PR (branch pushed)")
        return None

    env = dict(os.environ)
    env["GH_TOKEN"] = token  # never logged

    def gh(args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["gh", *args], cwd=workdir, capture_output=True, text=True, env=env
        )

    existing = gh(
        [
            "pr", "list", "--head", work_branch, "--state", "open",
            "--json", "number,url,title",
        ]
    )
    pr_action = "opened"
    if existing.returncode == 0 and existing.stdout.strip():
        try:
            prs = _json.loads(existing.stdout)
        except ValueError:
            prs = []
        if prs:
            pr_action = "updated"

    if pr_action == "opened":
        body = (
            f"Automated change by SWE sub-agent `{subagent_id}`.\n\n"
            "Definition of done:\n"
            + "\n".join(
                f"- {d}" for d in assignment.get("task", {}).get("definition_of_done", [])
            )
            + "\n\n**Do not auto-merge** -- human review required."
        )
        created = gh(
            [
                "pr", "create", "--base", base_branch, "--head", work_branch,
                "--title", title, "--body", body,
            ]
        )
        if created.returncode != 0:
            log.warning(
                f"[{subagent_id}] pr create failed: {created.stderr.strip()[:300]}"
            )

    view = gh(
        [
            "pr", "view", work_branch,
            "--json", "number,url,title,state,statusCheckRollup",
        ]
    )
    if view.returncode != 0 or not view.stdout.strip():
        return {"action": pr_action, "number": None, "url": None, "title": title,
                "ci_state": "unknown"}
    try:
        data = _json.loads(view.stdout)
    except ValueError:
        return {"action": pr_action, "number": None, "url": None, "title": title,
                "ci_state": "unknown"}

    rollup = data.get("statusCheckRollup") or []
    ci_state = "none"
    if rollup:
        states = {(c.get("conclusion") or c.get("state") or "").upper() for c in rollup}
        if {"FAILURE", "ERROR", "CANCELLED"} & states:
            ci_state = "failing"
        elif {"PENDING", "IN_PROGRESS", ""} & states:
            ci_state = "pending"
        else:
            ci_state = "passing"

    log.info(f"[{subagent_id}] PR #{data.get('number')} action={pr_action}")
    return {
        "action": pr_action,
        "number": data.get("number"),
        "url": data.get("url"),
        "title": data.get("title") or title,
        "ci_state": ci_state,
    }


# --------------------------------------------------------------------------- #
# Result assembly
# --------------------------------------------------------------------------- #
def _build_done_checklist(assignment: dict[str, Any], status: str) -> list[dict[str, Any]]:
    met = status == "succeeded"
    return [
        {"item": item, "met": met}
        for item in assignment.get("task", {}).get("definition_of_done", [])
    ]


def _run(subagent_id: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    log.info(f"[{subagent_id}] sub-agent starting")

    assignment = orchestration.get_json(orchestration.assignment_key(subagent_id))
    if not assignment:
        result = {
            "subagent_id": subagent_id,
            "status": "failed",
            "reason": "assignment_not_found",
            "summary": "Assignment document was missing from Blob storage.",
            "final_reply": "",
            "done_checklist": [],
            "git": {"pushed": False, "pr": None},
            "usage": pi_runner._new_usage(),
            "turns": 0,
            "children": [],
            "artifacts": [],
        }
        orchestration.put_json(orchestration.result_key(subagent_id), result)
        return result

    git_spec = assignment.get("git", {})
    status = "failed"
    reason: Optional[str] = None
    loop_out: dict[str, Any] = {}
    git_info: dict[str, Any] = {"pushed": False, "pr": None}

    if not _load_openai_key():
        status, reason = "failed", "missing_openai_key"
        loop_out = {"final_reply": "", "usage": pi_runner._new_usage(), "turns": 0}
    else:
        model = _resolve_model()
        try:
            git_bootstrap.bootstrap_git(
                WORKDIR,
                base_branch=git_spec.get("base_branch"),
                work_branch=git_spec.get("work_branch"),
            )
        except git_bootstrap.GitBootstrapError as exc:
            status, reason = "failed", f"git_bootstrap_failed: {exc}"
            loop_out = {"final_reply": "", "usage": pi_runner._new_usage(), "turns": 0}
        else:
            loop_out = _run_pi_loop(assignment, WORKDIR, model)
            status = loop_out["status"]
            reason = loop_out["reason"]
            try:
                git_info = _commit_and_push(assignment, WORKDIR)
            except Exception as exc:  # noqa: BLE001
                log.error(f"[{subagent_id}] git persistence failed: {exc}")
                git_info = {"pushed": False, "branch": git_spec.get("work_branch"),
                            "base_branch": git_spec.get("base_branch"),
                            "commits": [], "pr": None}

    summary = _summarize(status, reason, git_info, loop_out.get("turns", 0))
    result = {
        "subagent_id": subagent_id,
        "status": status,
        "reason": reason,
        "summary": summary,
        "final_reply": loop_out.get("final_reply", ""),
        "done_checklist": _build_done_checklist(assignment, status),
        "git": git_info,
        "usage": loop_out.get("usage", pi_runner._new_usage()),
        "turns": loop_out.get("turns", 0),
        "children": [],
        "artifacts": [],
    }
    orchestration.put_json(orchestration.result_key(subagent_id), result)
    _notify_callback(subagent_id, result)
    log.info(
        f"[{subagent_id}] done status={status} turns={result['turns']} "
        f"cost=${result['usage'].get('cost', 0):.4f} "
        f"elapsed={time.perf_counter() - t0:.1f}s"
    )
    return result


def _notify_callback(subagent_id: str, result: dict) -> None:
    """Optional low-latency wake-up to the callback Service (§10).

    Blob remains the source of truth, so any failure here is ignored -- the
    coordinator will still harvest this run on its next poll cycle.
    """
    import json as _json
    import urllib.request

    base = os.environ.get("SWE_CALLBACK_URL", "http://swe-agent-callback")
    pr = (result.get("git") or {}).get("pr") or {}
    payload = {
        "subagent_id": subagent_id,
        "status": result.get("status"),
        "pr_url": pr.get("url"),
    }
    try:
        req = urllib.request.Request(
            f"{base.rstrip('/')}/notify",
            data=_json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def _summarize(status: str, reason: Optional[str], git_info: dict, turns: int) -> str:
    pr = (git_info or {}).get("pr")
    parts = [f"status={status}"]
    if reason:
        parts.append(f"reason={reason}")
    parts.append(f"turns={turns}")
    if git_info.get("pushed"):
        parts.append(f"pushed {git_info.get('branch')}")
    if pr and pr.get("url"):
        parts.append(f"PR {pr.get('action')} {pr.get('url')}")
    return "; ".join(parts)


@task(memory="2g", cpu=1)
def run_subagent(subagent_id: str) -> dict:
    """Single workflow task: clone -> pi loop -> commit/push/PR -> report.

    Returns the full result document (also written to Blob as ``result.json``)
    so the coordinator can retrieve it via ``wf.result(run_id, "run_subagent")``
    as a fallback to reading Blob.
    """
    return _run(subagent_id)
