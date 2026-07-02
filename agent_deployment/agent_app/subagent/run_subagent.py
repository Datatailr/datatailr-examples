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
import re
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
# pi's session store MUST live outside the repo checkout, otherwise its
# transcript files land in the working tree and get swept into the commit/PR by
# ``git add -A``. Keep it as a sibling and archive it to Blob afterwards (§4).
SESSION_DIR = os.environ.get("SUBAGENT_SESSION_DIR", "/tmp/subagent_sessions")
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
    # Session store lives outside ``workdir`` so it never enters the repo tree.
    session_dir = SESSION_DIR
    os.makedirs(session_dir, exist_ok=True)

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
    ref = _run_reference(subagent_id)
    run_trailer = f" ({ref['run_id']})" if ref["run_id"] else ""
    commit_msg = (
        f"{_short_title(title)}\n\n"
        f"Automated change by the SWE sub-agent. Review required; do not auto-merge.\n\n"
        f"Sub-agent: {subagent_id}\n"
        f"Workflow-Run: {ref['job_name'] or 'unknown'}{run_trailer}\n"
    )
    commit = _git(["commit", "-m", commit_msg], workdir)
    if commit.returncode != 0:
        err = f"commit failed: {commit.stderr.strip()[:300]}"
        log.error(f"[{subagent_id}] {err}")
        git_info["error"] = err
        return git_info

    push = _git(["push", "-u", "origin", work_branch], workdir)
    if push.returncode != 0:
        err = f"push failed: {push.stderr.strip()[:300]}"
        log.error(f"[{subagent_id}] {err}")
        git_info["error"] = err
        return git_info

    git_info["pushed"] = True
    sha = _git(["rev-parse", "HEAD"], workdir)
    if sha.returncode == 0:
        git_info["commits"] = [sha.stdout.strip()[:9]]
    log.info(f"[{subagent_id}] pushed branch {work_branch}")

    if git_spec.get("may_open_pr"):
        git_info["pr"] = _open_or_update_pr(assignment, workdir)

    return git_info


# --------------------------------------------------------------------------- #
# PR title / description composition (§10 step 4)
# --------------------------------------------------------------------------- #
# Strip a leading conventional "agent:" / "agent(...):" prefix the brief may add.
_AGENT_PREFIX_RE = re.compile(r"^\s*agent(?:\([^)]*\))?:\s*", re.IGNORECASE)


def _short_title(raw: str, limit: int = 68) -> str:
    """A concise, single-line PR/commit title from a (possibly long) task title.

    Collapses whitespace, drops a redundant ``agent:`` prefix, and truncates at a
    word boundary so the title stays short and readable -- the full detail lives
    in the description instead."""
    text = " ".join((raw or "").split())
    text = _AGENT_PREFIX_RE.sub("", text)
    if not text:
        return "Automated change"
    if len(text) <= limit:
        return text
    clipped = text[:limit].rsplit(" ", 1)[0].rstrip(" ,.;:-([{")
    return (clipped or text[:limit].rstrip()) + "\u2026"


def _run_reference(subagent_id: str) -> dict[str, str]:
    """Identifiers of the batch run executing this sub-agent, for the PR footer.

    ``DATATAILR_JOB_NAME`` is the unique workflow display name (it embeds the
    sub-agent id) and ``DATATAILR_BATCH_RUN_ID`` is the concrete run id -- both
    are set in the task container by the platform (see datatailr batch runner)."""
    return {
        "subagent_id": subagent_id,
        "job_name": os.environ.get("DATATAILR_JOB_NAME", ""),
        "run_id": os.environ.get("DATATAILR_BATCH_RUN_ID", ""),
        "environment": os.environ.get("DATATAILR_JOB_ENVIRONMENT", ""),
    }


def _pr_body(assignment: dict[str, Any]) -> str:
    """A clear, structured PR description with an automated-sub-agent footer."""
    task = assignment.get("task", {})
    git_spec = assignment.get("git", {})
    subagent_id = assignment["subagent_id"]
    instructions = (task.get("instructions") or "").strip()
    dod = task.get("definition_of_done") or []
    ref = _run_reference(subagent_id)

    sections: list[str] = []
    summary = _short_title(task.get("title") or "", limit=120) or "Automated change."
    sections.append(f"## Summary\n\n{summary}\n")

    if instructions:
        sections.append(f"## Task\n\n{instructions}\n")

    if dod:
        checklist = "\n".join(f"- [ ] {item}" for item in dod)
        sections.append(f"## Definition of done\n\n{checklist}\n")

    run_line = f"`{ref['job_name']}`" if ref["job_name"] else "(unknown workflow)"
    if ref["run_id"]:
        run_line += f", run `{ref['run_id']}`"
    if ref["environment"]:
        run_line += f" ({ref['environment']})"
    footer = (
        "---\n\n"
        "### Automated sub-agent PR\n\n"
        "This pull request was opened automatically by an SWE **sub-agent**. "
        "Please review carefully before merging \u2014 **do not auto-merge**.\n\n"
        f"- **Sub-agent:** `{subagent_id}`\n"
        f"- **Workflow run:** {run_line}\n"
        f"- **Branch:** `{git_spec.get('work_branch')}` \u2192 "
        f"`{git_spec.get('base_branch')}`\n"
    )
    sections.append(footer)
    return "\n".join(sections)


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


def _repo_slug(assignment: dict[str, Any], workdir: str) -> Optional[str]:
    """Resolve the ``owner/repo`` slug from the assignment or the git remote."""
    slug = git_bootstrap.parse_repo_slug(
        (assignment.get("git") or {}).get("repo_url") or git_bootstrap.repo_url()
    )
    if slug:
        return slug
    rem = _git(["remote", "get-url", "origin"], workdir)
    if rem.returncode == 0:
        return git_bootstrap.parse_repo_slug(rem.stdout.strip())
    return None


def _pr_ci_state(gh_api, slug: str, sha: Optional[str]) -> str:
    """Best-effort combined CI status for the PR head commit (REST, no GraphQL)."""
    if not sha:
        return "unknown"
    import json as _json

    resp = gh_api([f"repos/{slug}/commits/{sha}/status"])
    if resp.returncode != 0 or not resp.stdout.strip():
        return "unknown"
    try:
        data = _json.loads(resp.stdout)
    except ValueError:
        return "unknown"
    if int(data.get("total_count", 0) or 0) == 0:
        return "none"
    return {
        "success": "passing",
        "pending": "pending",
        "failure": "failing",
        "error": "failing",
    }.get((data.get("state") or "").lower(), "unknown")


def _open_or_update_pr(assignment: dict[str, Any], workdir: str) -> Optional[dict[str, Any]]:
    """Open a PR (or record the existing one) for the work branch. Never merge.

    Uses the GitHub **REST** API via ``gh api`` rather than the ``gh pr``
    porcelain: the porcelain relies on GraphQL (``repository.defaultBranchRef``),
    which a fine-grained PAT often cannot read, whereas ``POST repos/<slug>/pulls``
    needs only *Pull requests: write* (+ *Contents: read*). Idempotent: an open PR
    for the branch is recorded as ``action="updated"`` instead of duplicated.
    """
    import json as _json

    git_spec = assignment.get("git", {})
    subagent_id = assignment["subagent_id"]
    work_branch = git_spec.get("work_branch")
    base_branch = git_spec.get("base_branch")
    title = _short_title(assignment.get("task", {}).get("title", "change"))

    def _skip(reason: str, *, level: str = "warning") -> dict[str, Any]:
        getattr(log, level)(f"[{subagent_id}] PR skipped: {reason} (branch pushed)")
        return {"action": "none", "number": None, "url": None,
                "title": title, "ci_state": "unknown", "error": reason}

    token = git_bootstrap.git_token()
    if not token:
        return _skip("no git token configured (secret 'agent_git_token')")
    if not _gh_available():
        return _skip("gh CLI unavailable in the sub-agent image")

    slug = _repo_slug(assignment, workdir)
    if not slug:
        return _skip("could not resolve owner/repo from the git remote")
    owner = slug.split("/")[0]

    env = dict(os.environ)
    env["GH_TOKEN"] = token  # never logged
    # Point gh at the repo host for GitHub Enterprise (defaults to github.com).
    host, _ = git_bootstrap.parse_git_host(
        (git_spec.get("repo_url") or git_bootstrap.repo_url())
    )
    if host and host != "github.com":
        env["GH_HOST"] = host

    def gh_api(args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["gh", "api", "-H", "Accept: application/vnd.github+json", *args],
            cwd=workdir, capture_output=True, text=True, env=env,
        )

    def _gh_error(action: str, proc: subprocess.CompletedProcess) -> str:
        """Log and return a human-readable error for a failed gh call."""
        msg = (proc.stderr or proc.stdout or "").strip()[:400] or f"exit {proc.returncode}"
        full = f"{action} failed: {msg}"
        log.error(f"[{subagent_id}] {full}")
        lower = msg.lower()
        if "not accessible" in lower or "403" in msg or "not found" in lower or "422" in msg:
            hint = (
                "the git token likely lacks repository access. A fine-grained PAT "
                "needs 'Contents: Read' AND 'Pull requests: Read and write' on this "
                "repo (and, for org repos, the org must approve the token)."
            )
            log.error(f"[{subagent_id}] {hint}")
            full = f"{full} — {hint}"
        return full

    # 1. Is there already an open PR for this head branch?
    pr_obj: Optional[dict[str, Any]] = None
    pr_action = "opened"
    listed = gh_api([f"repos/{slug}/pulls?head={owner}:{work_branch}&state=open"])
    if listed.returncode == 0 and listed.stdout.strip():
        try:
            arr = _json.loads(listed.stdout)
        except ValueError:
            arr = []
        if arr:
            pr_obj, pr_action = arr[0], "updated"
    elif listed.returncode != 0:
        # A lookup failure is not fatal (we still try to create), but record it.
        _gh_error("pr lookup", listed)

    # 2. Create it if none exists (never merge).
    if pr_obj is None:
        created = gh_api(
            [
                "--method", "POST", f"repos/{slug}/pulls",
                "-f", f"title={title}",
                "-f", f"head={work_branch}",
                "-f", f"base={base_branch}",
                "-f", f"body={_pr_body(assignment)}",
            ]
        )
        if created.returncode != 0:
            err = _gh_error("pr create", created)
            return {"action": "none", "number": None, "url": None,
                    "title": title, "ci_state": "unknown", "error": err}
        try:
            pr_obj = _json.loads(created.stdout)
        except ValueError:
            pr_obj = None

    if not pr_obj:
        return {"action": pr_action, "number": None, "url": None,
                "title": title, "ci_state": "unknown",
                "error": "PR API returned no pull-request object"}

    head_sha = (pr_obj.get("head") or {}).get("sha")
    ci_state = _pr_ci_state(gh_api, slug, head_sha)
    log.info(f"[{subagent_id}] PR #{pr_obj.get('number')} action={pr_action}")
    return {
        "action": pr_action,
        "number": pr_obj.get("number"),
        "url": pr_obj.get("html_url") or pr_obj.get("url"),
        "title": pr_obj.get("title") or title,
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


def _archive_session_logs(subagent_id: str, session_dir: str) -> list[str]:
    """Upload pi's run transcript(s) to Blob under ``agent_runs/<id>/logs/`` (§4).

    This is the audit/monitoring trail of what the sub-agent actually did -- the
    same transcript that must NOT be committed into the repo. Returns the list
    of Blob keys written, recorded in ``result.artifacts`` so the coordinator /
    UI can retrieve them later. Best effort: failures never fail the task.
    """
    keys: list[str] = []
    if not os.path.isdir(session_dir):
        return keys
    for root, _dirs, files in os.walk(session_dir):
        for fname in files:
            if not fname.endswith(".jsonl"):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "rb") as fh:
                    data = fh.read()
            except OSError:
                continue
            key = f"{orchestration.logs_prefix(subagent_id)}/{fname}"
            if orchestration.put_bytes(key, data):
                keys.append(key)
    if keys:
        log.info(f"[{subagent_id}] archived {len(keys)} session transcript(s) to Blob")
    return keys


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
    # Collected human-readable problems, surfaced in result.json (and thus in
    # the UI panel / check_subagents / fold-in) so failures are never silent.
    warnings: list[str] = []

    if not _load_openai_key():
        status, reason = "failed", "missing_openai_key"
        loop_out = {"final_reply": "", "usage": pi_runner._new_usage(), "turns": 0}
        warnings.append(
            "OpenAI API key not configured (secret 'openai_api_key'); pi did not run."
        )
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
            warnings.append(f"git bootstrap failed: {exc}")
        else:
            # Load the git API token into the environment now (GH_TOKEN) so gh is
            # authenticated for both our PR code and any gh usage by pi. Preflight
            # it when a PR is expected so a missing/inaccessible token is loud at
            # the start of the run rather than only surfacing at push/PR time.
            gh_ready = git_bootstrap.configure_gh_auth()
            if gh_ready:
                log.info(f"[{subagent_id}] gh authenticated via '{git_bootstrap.GIT_TOKEN_SECRET}' secret")
            elif git_spec.get("may_open_pr"):
                log.error(
                    f"[{subagent_id}] git API token unavailable: secret "
                    f"'{git_bootstrap.GIT_TOKEN_SECRET}' is missing or not accessible "
                    "to this job. The sub-agent will still commit/push, but PR "
                    "creation will be skipped. Grant the job's identity read access "
                    "to that secret in this environment (dev/pre/prod)."
                )
            loop_out = _run_pi_loop(assignment, WORKDIR, model)
            status = loop_out["status"]
            reason = loop_out["reason"]
            try:
                git_info = _commit_and_push(assignment, WORKDIR)
            except Exception as exc:  # noqa: BLE001
                log.error(f"[{subagent_id}] git persistence failed: {exc}")
                git_info = {"pushed": False, "branch": git_spec.get("work_branch"),
                            "base_branch": git_spec.get("base_branch"),
                            "commits": [], "pr": None,
                            "error": f"git persistence crashed: {exc}"}

    # Fold git/PR problems into the warnings list.
    if git_info.get("error"):
        warnings.append(git_info["error"])
    pr_info = git_info.get("pr")
    if isinstance(pr_info, dict) and pr_info.get("error"):
        warnings.append(pr_info["error"])
    elif (
        git_spec.get("may_open_pr")
        and git_info.get("pushed")
        and not (isinstance(pr_info, dict) and pr_info.get("url"))
    ):
        warnings.append("branch pushed but no pull request was created")
    if reason and status != "succeeded":
        warnings.append(f"pi loop ended: {reason}")

    artifacts = _archive_session_logs(subagent_id, SESSION_DIR)
    summary = _summarize(status, reason, git_info, loop_out.get("turns", 0))
    result = {
        "subagent_id": subagent_id,
        "status": status,
        "reason": reason,
        "summary": summary,
        "warnings": warnings,
        "final_reply": loop_out.get("final_reply", ""),
        "done_checklist": _build_done_checklist(assignment, status),
        "git": git_info,
        "usage": loop_out.get("usage", pi_runner._new_usage()),
        "turns": loop_out.get("turns", 0),
        "children": [],
        "artifacts": artifacts,
    }
    if warnings:
        log.warning(f"[{subagent_id}] completed with {len(warnings)} warning(s): "
                    + " | ".join(warnings))
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
    if git_info.get("error"):
        parts.append(f"git FAILED ({git_info.get('error')})")
    if isinstance(pr, dict) and pr.get("url"):
        parts.append(f"PR {pr.get('action')} {pr.get('url')}")
    elif isinstance(pr, dict) and pr.get("error"):
        parts.append(f"PR FAILED ({pr.get('error')})")
    return "; ".join(parts)


@task(memory="2g", cpu=1)
def run_subagent(subagent_id: str) -> dict:
    """Single workflow task: clone -> pi loop -> commit/push/PR -> report.

    Returns the full result document (also written to Blob as ``result.json``)
    so the coordinator can retrieve it via ``wf.result(run_id, "run_subagent")``
    as a fallback to reading Blob.
    """
    return _run(subagent_id)
