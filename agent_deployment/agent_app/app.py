"""Self-contained pi agent app for Datatailr.

This single FastAPI app owns the whole agent runtime and the UI:

- runs the `pi` coding agent in this container (Node + pi installed via
  build_script_pre)
- serves an interactive terminal (xterm.js) wired to a real `pi` PTY over a
  WebSocket -- the terminal and the WebSocket are local, so the only network
  hop is browser -> app (which the platform ingress forwards; internal
  service-to-service WebSocket upgrades are NOT forwarded, which is why the
  agent runtime lives here rather than in a separate service)
- exposes a JSON HTTP API (`/chat`, `/chat/stream`) for programmatic access
- serves an activity dashboard sourced from the on-disk `~/.pi` session store
- isolates sessions per authenticated user and persists per-user sessions plus
  the global config dirs (`~/.pi`, `~/.agents`) to Datatailr blob storage so
  state survives container restarts
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
import subprocess
from typing import Mapping, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)
from pydantic import BaseModel

from agent_app import blob_sync, pi_runner, pty_runner, sessions
from agent_app.agent_common import briefing, git_bootstrap, orchestration
from agent_app.coordinator import Coordinator

# Default model if the `agent_model` KV key is not set. Provider-prefixed
# (`provider/id`); the app splits this into pi's separate --provider/--model
# flags. Use a model id the installed pi build actually serves under the OpenAI
# provider -- `gpt-5.1` is not in older builds' OpenAI catalog (it resolves to
# the azure-openai-responses provider there), whereas `gpt-5-mini` is.
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "openai/gpt-5-mini")
# Thinking/reasoning level passed to pi (off, minimal, low, medium, high, xhigh).
AGENT_THINKING = os.environ.get("AGENT_THINKING", "medium")
# Datatailr secret/KV key names (create the secret in the Secrets Manager UI).
OPENAI_SECRET_KEY = os.environ.get("OPENAI_SECRET_KEY", "openai_api_key")
MODEL_KV_KEY = os.environ.get("MODEL_KV_KEY", "agent_model")

# Fallback user when no identity header is present (e.g. local dev).
DEFAULT_USER = os.environ.get("AGENT_DEFAULT_USER", "shared")

# Blob prefixes.
SESSIONS_BLOB_PREFIX = os.environ.get("AGENT_SESSIONS_PREFIX", "agent_sessions")
PI_CONFIG_BLOB_PREFIX = os.environ.get("AGENT_PI_CONFIG_PREFIX", "agent_state/pi")
AGENTS_BLOB_PREFIX = os.environ.get("AGENT_AGENTS_PREFIX", "agent_state/agents")

# Config sync excludes the per-user sessions tree (persisted separately).
_PI_CONFIG_EXCLUDES = {"sessions"}

# Header the Datatailr platform sets on requests, identifying the authenticated
# browser user (a JSON blob with a "name" field).
USER_HEADER = "x-datatailr-user"

# Loopback base URL the in-container `spawn_subagent` helper posts to. The pi
# subprocess runs in the same container as the app, so it reaches the
# orchestration API over localhost. The port is best-effort resolved from the
# platform environment; override with SWE_ORCH_URL if needed.
ORCH_PORT = os.environ.get("DATATAILR_APP_PORT") or os.environ.get("PORT") or "8080"
ORCH_URL = os.environ.get("SWE_ORCH_URL", f"http://127.0.0.1:{ORCH_PORT}")

log = logging.getLogger("agent_app")


# --------------------------------------------------------------------------- #
# Runtime configuration + identity
# --------------------------------------------------------------------------- #
class _Config:
    model: str = DEFAULT_MODEL


_config = _Config()

_session_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()
_USER_RE = re.compile(r"[^A-Za-z0-9_.-]")

# Per-user git workspace bootstrap state (clone once per user, then refresh).
_repo_ready: set[str] = set()
_repo_guard = threading.Lock()

# The coordinator owns the sub-agent lifecycle; created at startup.
_coordinator: Optional[Coordinator] = None


def _username(headers: Mapping[str, str]) -> Optional[str]:
    """Parse the platform identity header into a username, or None."""
    raw = headers.get(USER_HEADER)
    if not raw:
        return None
    try:
        name = json.loads(raw).get("name")
    except (ValueError, TypeError, AttributeError):
        log.warning("could not parse %s header as JSON", USER_HEADER)
        return None
    return name or None


def _safe_user(name: Optional[str]) -> str:
    """Normalize a username into a filesystem/blob-safe token."""
    cleaned = _USER_RE.sub("_", (name or "").strip())
    return cleaned or DEFAULT_USER


def _user_session_dir(user: str) -> str:
    return os.path.join(pi_runner.PI_SESSION_DIR, user)


def _user_workspace_dir(user: str) -> str:
    """Per-user working directory for the agent's file/bash/edit tools.

    Isolating this keeps one user's files from being visible to another, since
    all users' pi processes share this single container.
    """
    return os.path.join(pi_runner.PI_WORKSPACE_DIR, user)


def _lock_for(user: str, session_id: Optional[str]) -> threading.Lock:
    key = f"{user}:{session_id or '__new__'}"
    with _locks_guard:
        return _session_locks[key]


# --------------------------------------------------------------------------- #
# Git-aware workspace + orchestration context for pi
# --------------------------------------------------------------------------- #
def _ensure_user_repo(user: str) -> None:
    """Clone the shared repo into this user's workspace on first use (§6.1).

    Best effort: if git is not configured (no SSH key / repo URL), the app
    still serves pi against an empty workspace, matching prior behavior.
    """
    if user in _repo_ready:
        return
    with _repo_guard:
        if user in _repo_ready:
            return
        try:
            git_bootstrap.ensure_workspace_repo(_user_workspace_dir(user))
        except Exception as exc:  # noqa: BLE001
            log.warning("git workspace bootstrap failed for %s: %s", user, exc)
        # Mark ready regardless so we do not retry a failing clone every turn.
        _repo_ready.add(user)


def _pi_extra_env(user: str, session_id: Optional[str], depth: int = 0) -> dict[str, str]:
    """Env injected into the pi process so the `spawn_subagent` tool can call
    the orchestration API with the right parent/user context (§6.2)."""
    return {
        "SWE_ORCH_URL": ORCH_URL,
        "SWE_USER": user,
        "SWE_PARENT_SESSION": session_id or f"{user}-adhoc",
        "SWE_DEPTH": str(depth),
    }

def _fold_report(entry: dict, result: dict) -> None:
    """Report sink: inject a synthesized user turn into the parent pi session
    summarizing a finished sub-agent, so the main agent continues the
    conversation with the delegated outcome (§7 "folding in", §10)."""
    user = entry.get("created_by") or DEFAULT_USER
    session_id = entry.get("session_id")
    if not session_id or session_id.endswith("-adhoc"):
        # No resumable parent session to fold into; the UI panel still surfaces
        # the outcome from the registry.
        return
    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        return

    pr = (result.get("git") or {}).get("pr") or {}
    if pr.get("url"):
        pr_line = f" PR ({pr.get('action')}): {pr.get('url')}."
    elif pr.get("error"):
        pr_line = f" PR was NOT created: {pr.get('error')}."
    else:
        pr_line = ""
    warnings = result.get("warnings") or []
    warn_line = ""
    if warnings:
        warn_line = " Warnings: " + " | ".join(str(w) for w in warnings) + "."
    message = (
        f"[orchestrator] Sub-agent {entry.get('subagent_id')} for task "
        f"\"{entry.get('title')}\" finished with status "
        f"{result.get('status')}.{pr_line}{warn_line} Summary: {result.get('summary')}. "
        "Incorporate this outcome and, if appropriate, present the result "
        "(and any PR link) to the user. If a PR was not created or there were "
        "warnings, tell the user what went wrong."
    )
    lock = _lock_for(user, session_id)
    with lock:
        try:
            pi_runner.run_pi(
                message=message,
                session_id=session_id,
                model=_config.model,
                session_dir=_user_session_dir(user),
                thinking=AGENT_THINKING,
                workspace_dir=_user_workspace_dir(user),
                extra_env=_pi_extra_env(user, session_id),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("fold-in for %s failed: %s", entry.get("subagent_id"), exc)
    try:
        _persist_user_sessions(user)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Datatailr integration (secrets, KV, skills) + blob persistence
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


def _load_git_token() -> bool:
    """Expose the git-host API token to pi as ``GH_TOKEN`` (§4).

    The token stored in the Secrets Manager (``agent_git_token``) is what lets
    the agent open PRs with ``gh`` from its bash tool. Without this the main
    agent's ``gh`` is unauthenticated ("run gh auth login"). Delegates to the
    shared :func:`git_bootstrap.configure_gh_auth` so the App and every sub-agent
    load the token identically. Never logged."""
    if os.environ.get("GH_TOKEN"):
        return True
    try:
        ok = git_bootstrap.configure_gh_auth()
    except Exception:  # noqa: BLE001
        ok = False
    if not ok:
        log.warning(
            "git API token unavailable: secret '%s' is missing or not accessible; "
            "gh will be unauthenticated until it is configured",
            git_bootstrap.GIT_TOKEN_SECRET,
        )
    return ok


def _load_model() -> str:
    try:
        from datatailr import KV

        value = KV().get(MODEL_KV_KEY)
        if isinstance(value, str) and value.strip():
            return value.strip()
    except Exception:
        pass
    return DEFAULT_MODEL


def _write_pi_settings() -> None:
    """Pin pi's default provider/model so sessions start with a model selected.

    pi's settings.json uses flat top-level ``defaultProvider`` / ``defaultModel``
    string fields (this is exactly what the interactive ``/model`` command
    writes via ``setDefaultModelAndProvider``). A nested ``model`` object is not
    recognized and is silently ignored, which is why pi otherwise starts with no
    model pinned. We merge into any existing settings so a user's ``/model``
    choice and other preferences are preserved across restarts.
    """
    os.makedirs(pi_runner.PI_AGENT_DIR, exist_ok=True)
    settings_path = os.path.join(pi_runner.PI_AGENT_DIR, "settings.json")

    settings: dict = {}
    try:
        with open(settings_path, encoding="utf-8") as fh:
            existing = json.load(fh)
        if isinstance(existing, dict):
            settings = existing
    except (OSError, ValueError):
        settings = {}

    settings.setdefault("defaultProjectTrust", "always")
    provider, model_id = pi_runner.split_model(_config.model)
    if provider:
        settings["defaultProvider"] = provider
    if model_id:
        settings["defaultModel"] = model_id
    if AGENT_THINKING and AGENT_THINKING.lower() != "off":
        settings["defaultThinkingLevel"] = AGENT_THINKING
    # Drop the legacy nested key we used to write (pi ignores it).
    settings.pop("model", None)
    settings['quietStartup'] = True
    try:
        with open(settings_path, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2)
    except OSError:
        pass


def _write_pi_keybindings() -> None:
    """Write chat-style Enter keybindings to ``~/.pi/agent/keybindings.json``.

    In the composer we want Enter to insert a newline and Ctrl/Cmd+Enter to
    submit, but menus and approval prompts should still confirm on a plain
    Enter. pi routes those two behaviours through *different* actions:
    ``tui.input.submit`` (message composer) versus ``tui.select.confirm``
    (overlays/lists). So we move submit onto ``ctrl+enter`` and add ``enter`` to
    ``tui.input.newLine`` while leaving ``tui.select.confirm`` untouched. The
    browser terminal maps Ctrl/Cmd+Enter to pi's ``ctrl+enter`` (CSI-u code
    ``\\x1b[13;5u``); see ``initTerminal()`` in the page. We merge into any
    existing bindings so other customizations survive restarts.
    """
    os.makedirs(pi_runner.PI_AGENT_DIR, exist_ok=True)
    path = os.path.join(pi_runner.PI_AGENT_DIR, "keybindings.json")

    bindings: dict = {}
    try:
        with open(path, encoding="utf-8") as fh:
            existing = json.load(fh)
        if isinstance(existing, dict):
            bindings = existing
    except (OSError, ValueError):
        bindings = {}

    bindings["tui.input.submit"] = ["ctrl+enter"]
    bindings["tui.input.newLine"] = ["enter", "shift+enter", "ctrl+j"]

    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(bindings, fh, indent=2)
    except OSError as exc:
        log.warning("could not write pi keybindings: %s", exc)


def _write_agent_instructions() -> None:
    """Write the main-agent operating brief to ``~/.pi/agent/AGENTS.md`` (§6).

    pi appends this global context file to the system prompt of *every* session
    (interactive terminal and headless ``/chat`` alike), which is what makes the
    running agent aware that it is the SWE Main Agent: the repo it works on, how
    to delegate via ``spawn_subagent``, and how to monitor sub-agents. We
    regenerate it on each startup so it reflects the live repo URL and limits.
    """
    os.makedirs(pi_runner.PI_AGENT_DIR, exist_ok=True)
    path = os.path.join(pi_runner.PI_AGENT_DIR, "AGENTS.md")
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(briefing.build_main_agent_instructions())
    except OSError as exc:
        log.warning("could not write agent instructions: %s", exc)


def _setup_datatailr_skills() -> None:
    try:
        from datatailr.sbin.datatailr_cli import setup_skills

        setup_skills(global_dir=True, force=True)
    except Exception:
        pass


def _try_install_superpowers() -> None:
    """Install Superpowers for pi"""

    try:
        log.info("Installing Superpowers for pi")
        result = subprocess.run(["pi", "install", "git:github.com/obra/superpowers"], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Superpowers installation failed: {result.stderr}")
        log.info(result.stdout)
    except Exception as exc:  # noqa: BLE001
        log.warning("Superpowers installation failed: %s", exc)


def _install_datatailr_extension() -> None:
    """Install the bundled ``datatailr-system-builder`` pi package.

    The package ships inside this app package (``pi_extension/``) so it is
    baked into the image; installing from the local path means no network is
    needed at runtime. Its only dependency (``@earendil-works/pi-coding-agent``)
    is a peer dep provided by pi itself, so the offline ``npm install`` that
    ``pi install`` runs has nothing to fetch. This registers the ``/dt-system``
    command plus its skills and prompts for every session.
    """
    ext_dir = os.path.join(os.path.dirname(__file__), "pi_extension")
    try:
        log.info("Installing datatailr-system-builder pi extension from %s", ext_dir)
        result = subprocess.run(
            ["pi", "install", ext_dir], capture_output=True, text=True
        )
        if result.returncode != 0:
            raise Exception(result.stderr)
        log.info(result.stdout)
    except Exception as exc:  # noqa: BLE001
        log.warning("datatailr-system-builder installation failed: %s", exc)


def _restore_state() -> None:
    blob_sync.pull_dir(PI_CONFIG_BLOB_PREFIX, pi_runner.PI_AGENT_DIR)
    blob_sync.pull_dir(AGENTS_BLOB_PREFIX, pi_runner.AGENTS_DIR)
    blob_sync.pull_dir(SESSIONS_BLOB_PREFIX, pi_runner.PI_SESSION_DIR)


def _persist_config() -> None:
    blob_sync.push_dir(
        pi_runner.PI_AGENT_DIR, PI_CONFIG_BLOB_PREFIX, exclude_dirs=_PI_CONFIG_EXCLUDES
    )
    blob_sync.push_dir(pi_runner.AGENTS_DIR, AGENTS_BLOB_PREFIX)


def _persist_user_sessions(user: str) -> None:
    blob_sync.push_dir(_user_session_dir(user), f"{SESSIONS_BLOB_PREFIX}/{user}")


def _persist_after_pty(user: str) -> None:
    try:
        _persist_user_sessions(user)
        _persist_config()
    except Exception:
        pass


def _startup() -> None:
    global _coordinator
    os.makedirs(pi_runner.PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(pi_runner.PI_SESSION_DIR, exist_ok=True)
    os.makedirs(pi_runner.AGENTS_DIR, exist_ok=True)
    _config.model = _load_model()
    _load_openai_key()
    _load_git_token()
    _restore_state()
    _setup_datatailr_skills()
    _write_pi_settings()
    _write_pi_keybindings()
    _write_agent_instructions()
    _persist_config()
    # Clone the shared repo into the default workspace up front so the agent's
    # working directory is already a checkout on the default branch before the
    # first session connects -- matching what the operating brief tells pi
    # (§6.1). Per-user workspaces are still cloned lazily on first use.
    _ensure_user_repo(DEFAULT_USER)
    _try_install_superpowers()
    _install_datatailr_extension()
    # Bring up orchestration: the coordinator rehydrates its registry from Blob
    # and starts the background poller that harvests finished sub-agents.
    _coordinator = Coordinator(report_sink=_fold_report)
    _coordinator.refresh_limits()
    _coordinator.start_poller()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _startup()
    try:
        yield
    finally:
        if _coordinator is not None:
            _coordinator.stop()


app = FastAPI(title="SWE Main Agent", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    session_name: Optional[str] = None


class SubagentBrief(BaseModel):
    """One sub-agent brief (§9). Only `title` + `instructions` are required."""

    title: str
    instructions: str
    definition_of_done: list[str] = []
    files: list[str] = []
    context_files: list[str] = []
    branch: Optional[str] = None
    budget: Optional[dict] = None
    may_push: bool = True
    may_open_pr: bool = True


class SpawnRequest(BaseModel):
    """Body of POST /subagents: one or more briefs plus optional context.

    `parent_id`/`created_by`/`session_id`/`depth` are supplied by the in-pi
    `spawn_subagent` helper (which has no auth header); browser/API callers are
    identified by the platform user header instead.
    """

    briefs: list[SubagentBrief]
    parent_id: Optional[str] = None
    created_by: Optional[str] = None
    session_id: Optional[str] = None
    depth: int = 0


def _require_key() -> None:
    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        raise HTTPException(
            status_code=503,
            detail=(
                f"OpenAI API key not configured. Create a secret named "
                f"'{OPENAI_SECRET_KEY}' in the Datatailr Secrets Manager."
            ),
        )


# --------------------------------------------------------------------------- #
# HTTP API
# --------------------------------------------------------------------------- #
@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "OK\n"


@app.get("/api/whoami")
def api_whoami(request: Request) -> JSONResponse:
    return JSONResponse({"user": _username(request.headers)})


@app.get("/api/sessions")
def api_sessions(request: Request) -> JSONResponse:
    user = _safe_user(_username(request.headers))
    return JSONResponse(
        {"user": user, "sessions": sessions.list_sessions(_user_session_dir(user))}
    )


@app.get("/api/sessions/{session_id}")
def api_session(session_id: str, request: Request) -> JSONResponse:
    user = _safe_user(_username(request.headers))
    transcript = sessions.get_transcript(session_id, _user_session_dir(user))
    if transcript is None:
        raise HTTPException(status_code=404, detail="session not found")
    return JSONResponse(transcript)


@app.get("/api/stats")
def api_stats(request: Request) -> JSONResponse:
    user = _safe_user(_username(request.headers))
    stats = sessions.aggregate_stats(_user_session_dir(user))
    stats["user"] = user
    return JSONResponse(stats)


@app.post("/chat")
def chat(req: ChatRequest, request: Request) -> dict:
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")
    _require_key()

    user = _safe_user(_username(request.headers))
    session_dir = _user_session_dir(user)
    _ensure_user_repo(user)

    with _lock_for(user, req.session_id):
        try:
            result = pi_runner.run_pi(
                message=req.message,
                session_id=req.session_id,
                model=_config.model,
                session_name=req.session_name,
                session_dir=session_dir,
                thinking=AGENT_THINKING,
                workspace_dir=_user_workspace_dir(user),
                extra_env=_pi_extra_env(user, req.session_id),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"pi run failed: {exc}")

    _persist_user_sessions(user)
    _persist_config()
    return {
        "session_id": result.session_id,
        "reply": result.reply,
        "usage": result.usage,
        "user": user,
    }


def _sse(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


@app.post("/chat/stream")
def chat_stream(req: ChatRequest, request: Request):
    """Stream pi's thinking/text/tool events as Server-Sent Events."""
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")
    _require_key()

    user = _safe_user(_username(request.headers))
    session_dir = _user_session_dir(user)
    _ensure_user_repo(user)
    lock = _lock_for(user, req.session_id)

    def event_stream():
        lock.acquire()
        try:
            for event in pi_runner.stream_pi(
                message=req.message,
                session_id=req.session_id,
                model=_config.model,
                session_name=req.session_name,
                session_dir=session_dir,
                thinking=AGENT_THINKING,
                workspace_dir=_user_workspace_dir(user),
                extra_env=_pi_extra_env(user, req.session_id),
            ):
                if event.get("type") == "done":
                    event["user"] = user
                yield _sse(event)
        except Exception as exc:  # noqa: BLE001
            yield _sse({"type": "error", "detail": f"pi run failed: {exc}"})
        finally:
            try:
                _persist_user_sessions(user)
                _persist_config()
            finally:
                lock.release()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# --------------------------------------------------------------------------- #
# Orchestration API (sub-agents) -- see specification §6
# --------------------------------------------------------------------------- #
def _require_coordinator() -> Coordinator:
    if _coordinator is None:
        raise HTTPException(status_code=503, detail="coordinator not initialized")
    return _coordinator


@app.post("/subagents")
def spawn_subagents(req: SpawnRequest, request: Request) -> JSONResponse:
    """Spawn one or more sub-agents (§6). Enforces all limits via the
    coordinator and returns a per-brief result or refusal message."""
    coord = _require_coordinator()
    _require_key()

    header_user = _username(request.headers)
    user = _safe_user(header_user or req.created_by)
    parent_id = req.parent_id or req.session_id or f"{user}-adhoc"

    results = []
    for brief in req.briefs:
        payload = brief.model_dump()
        # Merge context_files into files if provided separately.
        if payload.get("context_files") and not payload.get("files"):
            payload["files"] = payload["context_files"]
        try:
            outcome = coord.spawn(
                parent_id=parent_id,
                brief=payload,
                created_by=user,
                depth=req.depth,
                session_id=req.session_id or parent_id,
                request_id=parent_id,
            )
        except Exception as exc:  # noqa: BLE001
            outcome = {"refused": True, "reason": "error", "message": str(exc)}
        results.append(outcome)

    return JSONResponse({"parent_id": parent_id, "user": user, "results": results})


@app.get("/subagents")
def list_subagents(
    request: Request,
    parent_id: Optional[str] = None,
    user: Optional[str] = None,
) -> JSONResponse:
    """List sub-agents and their live state.

    Browser/API callers are identified by the platform user header. The
    in-container ``check_subagents`` helper has no header, so it scopes the
    query to its originating session via the ``parent_id`` (falling back to
    ``user``) query params instead."""
    coord = _require_coordinator()
    if parent_id:
        return JSONResponse(
            {"parent_id": parent_id, "subagents": coord.list_children(parent_id)}
        )
    resolved = _safe_user(_username(request.headers) or user)
    return JSONResponse(
        {"user": resolved, "subagents": coord.list_children_for_user(resolved)}
    )


@app.get("/subagents/{subagent_id}")
def get_subagent(subagent_id: str) -> JSONResponse:
    coord = _require_coordinator()
    detail = coord.get_child(subagent_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="sub-agent not found")
    return JSONResponse(detail)


@app.post("/subagents/{subagent_id}/stop")
def stop_subagent(subagent_id: str) -> JSONResponse:
    coord = _require_coordinator()
    return JSONResponse(coord.request_stop(subagent_id))


@app.post("/subagents/{subagent_id}/callback")
def subagent_callback(subagent_id: str) -> JSONResponse:
    """Low-latency wake-up from the optional callback Service (§10). Blob
    remains the source of truth; this just nudges an immediate harvest."""
    coord = _require_coordinator()
    coord.notify_callback(subagent_id)
    return JSONResponse({"ok": True})


# --------------------------------------------------------------------------- #
# WebSocket: browser terminal  <->  local pi PTY
# --------------------------------------------------------------------------- #
@app.websocket("/ws/pty")
async def ws_pty(websocket: WebSocket) -> None:
    """Bridge an interactive `pi` PTY (in this container) to the browser.

    Protocol (client -> server, JSON text frames):
        {"type": "input",  "data": "<keystrokes>"}
        {"type": "resize", "cols": <int>, "rows": <int>}
    Server -> client: raw PTY output as binary frames (fed to xterm.js).
    """
    await websocket.accept()

    params = websocket.query_params
    user = _safe_user(_username(websocket.headers) or params.get("user"))
    session_id = params.get("session") or None
    try:
        cols = int(params.get("cols", "80"))
        rows = int(params.get("rows", "24"))
    except (TypeError, ValueError):
        cols, rows = 80, 24

    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        await websocket.send_bytes(
            f"\r\n\x1b[31mOpenAI API key not configured. Create a secret named "
            f"'{OPENAI_SECRET_KEY}'.\x1b[0m\r\n".encode()
        )
        await websocket.close()
        return

    session_dir = _user_session_dir(user)
    _ensure_user_repo(user)
    proc, master_fd = pty_runner.spawn(
        session_dir=session_dir,
        workspace_dir=_user_workspace_dir(user),
        model=_config.model,
        session_id=session_id,
        cols=cols,
        rows=rows,
        extra_env=_pi_extra_env(user, session_id),
    )
    loop = asyncio.get_running_loop()

    async def pump_out() -> None:
        try:
            while True:
                data = await loop.run_in_executor(None, pty_runner.read, master_fd)
                if not data:
                    break
                await websocket.send_bytes(data)
        except Exception:  # noqa: BLE001
            pass

    async def pump_in() -> None:
        try:
            while True:
                msg = await websocket.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                text = msg.get("text")
                if text is None:
                    data = msg.get("bytes")
                    if data:
                        pty_runner.write(master_fd, data)
                    continue
                try:
                    obj = json.loads(text)
                except (ValueError, TypeError):
                    pty_runner.write(master_fd, text.encode())
                    continue
                mtype = obj.get("type")
                if mtype == "input":
                    pty_runner.write(master_fd, (obj.get("data") or "").encode())
                elif mtype == "resize":
                    pty_runner.set_winsize(
                        master_fd, int(obj.get("rows", 24)), int(obj.get("cols", 80))
                    )
        except Exception:  # noqa: BLE001
            pass

    out_task = asyncio.create_task(pump_out())
    in_task = asyncio.create_task(pump_in())
    try:
        await asyncio.wait({out_task, in_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (out_task, in_task):
            task.cancel()
        pty_runner.terminate(proc, master_fd)
        try:
            await websocket.close()
        except Exception:
            pass
        await loop.run_in_executor(None, _persist_after_pty, user)


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_PAGE)


# --------------------------------------------------------------------------- #
# Single-page UI (HTML + CSS + JS)
# --------------------------------------------------------------------------- #
_PAGE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>SWE Main Agent</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.min.css" />
<script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f1117; --panel: #181b24; --panel-2: #1f232f; --border: #2a2f3d;
    --text: #e6e9ef; --muted: #9aa3b2; --accent: #6d8bff; --accent-2: #38d39f;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: var(--bg); color: var(--text); height: 100vh; display: flex; flex-direction: column; }
  header { display: flex; align-items: center; gap: 16px; padding: 12px 20px;
           border-bottom: 1px solid var(--border); background: var(--panel); }
  header h1 { font-size: 16px; margin: 0; font-weight: 600; }
  header .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--muted); }
  header .dot.ok { background: var(--accent-2); }
  nav { margin-left: auto; display: flex; gap: 6px; }
  nav button { background: transparent; color: var(--muted); border: 1px solid transparent;
               padding: 7px 14px; border-radius: 8px; cursor: pointer; font-size: 14px; }
  nav button.active { color: var(--text); background: var(--panel-2); border-color: var(--border); }
  main { flex: 1; overflow: hidden; }
  .view { height: 100%; display: none; }
  .view.active { display: flex; flex-direction: column; }

  /* Terminal */
  .term-toolbar { display: flex; align-items: center; gap: 10px; padding: 8px 16px;
                  border-bottom: 1px solid var(--border); background: var(--panel); }
  .term-toolbar button { background: var(--panel-2); color: var(--text); border: 1px solid var(--border);
                         border-radius: 8px; padding: 6px 12px; cursor: pointer; font-size: 13px; }
  .term-toolbar .hint { color: var(--muted); font-size: 12px; margin-left: auto; }
  .term-toolbar .hint kbd { background: var(--panel-2); border: 1px solid var(--border);
                            border-radius: 5px; padding: 1px 5px; font-size: 11px;
                            font-family: ui-monospace, Menlo, monospace; color: var(--text); }
  #terminal { flex: 1; padding: 8px 10px; background: #0f1117; overflow: hidden; }
  .xterm .xterm-viewport { background: transparent !important; }

  /* Dashboard */
  #dashboard { overflow-y: auto; padding: 24px; gap: 20px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
  .card .label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; }
  .card .value { font-size: 26px; font-weight: 700; margin-top: 6px; }
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .chart-box { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
  .chart-box h3 { margin: 0 0 12px; font-size: 14px; font-weight: 600; }
  .chart-box.wide { grid-column: 1 / -1; }
  .refresh { align-self: flex-start; background: var(--panel-2); color: var(--text);
             border: 1px solid var(--border); border-radius: 8px; padding: 8px 14px; cursor: pointer; }
  @media (max-width: 820px) { .charts { grid-template-columns: 1fr; } }

  /* Sub-agents */
  #subagents { overflow-y: auto; padding: 24px; gap: 16px; }
  .sub-toolbar { display: flex; align-items: center; gap: 12px; }
  .sub-toolbar .hint { color: var(--muted); font-size: 12px; }
  .sub-empty { color: var(--muted); background: var(--panel); border: 1px solid var(--border);
               border-radius: 12px; padding: 20px; }
  .sub-table { width: 100%; border-collapse: collapse; background: var(--panel);
               border: 1px solid var(--border); border-radius: 12px; overflow: hidden; font-size: 13px; }
  .sub-table th, .sub-table td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
  .sub-table th { color: var(--muted); text-transform: uppercase; font-size: 11px; letter-spacing: .5px; }
  .sub-table td.mono { font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
  .pill { display: inline-block; padding: 2px 9px; border-radius: 999px; font-size: 11px; font-weight: 600; }
  .pill.running, .pill.pending, .pill.launched { background: #2a3350; color: #9db4ff; }
  .pill.completed, .pill.succeeded { background: #14402f; color: #4fe0a8; }
  .pill.failed, .pill.failed_after, .pill.timed_out, .pill.out_of_memory { background: #47212b; color: #ff8fa3; }
  .pill.stopped, .pill.blocked, .pill.expired { background: #4a3b1e; color: #f0c060; }
  .sub-stop { background: var(--panel-2); color: var(--text); border: 1px solid var(--border);
              border-radius: 7px; padding: 4px 10px; cursor: pointer; font-size: 12px; }
  .sub-stop:disabled { opacity: .4; cursor: default; }
</style>
</head>
<body>
<header>
  <span class="dot" id="status-dot"></span>
  <h1>SWE Main Agent</h1>
  <span id="whoami" style="font-size:13px;color:var(--muted);"></span>
  <nav>
    <button id="tab-term" class="active" onclick="showView('terminal')">Terminal</button>
    <button id="tab-dash" onclick="showView('dashboard')">Dashboard</button>
    <button id="tab-sub" onclick="showView('subagents')">Sub-agents</button>
  </nav>
</header>
<main>
  <section id="terminal-view" class="view active">
    <div class="term-toolbar">
      <button onclick="restartSession()">Restart session</button>
      <button onclick="reconnect()">Reconnect</button>
      <span class="hint">Live <code>pi</code> session. <kbd>Enter</kbd> = newline · <kbd>⌘/Ctrl</kbd>+<kbd>Enter</kbd> = send</span>
    </div>
    <div id="terminal"></div>
  </section>
  <section id="dashboard" class="view">
    <button class="refresh" onclick="loadDashboard()">Refresh</button>
    <div class="cards" id="cards"></div>
    <div class="charts">
      <div class="chart-box wide"><h3>Activity over time</h3><canvas id="timelineChart"></canvas></div>
      <div class="chart-box"><h3>Tool usage</h3><canvas id="toolChart"></canvas></div>
      <div class="chart-box"><h3>Model usage</h3><canvas id="modelChart"></canvas></div>
    </div>
  </section>
  <section id="subagents" class="view">
    <div class="sub-toolbar">
      <button class="refresh" onclick="loadSubagents()">Refresh</button>
      <span class="hint">Delegated sub-agents spawned from your sessions. Auto-refreshes every 10s.</span>
    </div>
    <div id="sub-empty" class="sub-empty">No sub-agents yet. The main agent can delegate scoped tasks with <code>spawn_subagent</code>.</div>
    <table id="sub-table" class="sub-table" style="display:none">
      <thead>
        <tr>
          <th>Sub-agent</th><th>Task</th><th>State</th><th>Status</th>
          <th>Turns</th><th>Cost</th><th>PR</th><th></th>
        </tr>
      </thead>
      <tbody id="sub-body"></tbody>
    </table>
  </section>
</main>

<script>
const charts = {};
let term, fitAddon, ws, resumeSession = null, currentUser = null;

// Datatailr serves apps behind a URL prefix that the platform strips before the
// request reaches this app. The browser must include that prefix (= the page's
// own path) so requests route back here instead of the platform root.
const API_BASE = window.location.pathname.replace(/\/+$/, '');
function apiUrl(p) { return API_BASE + p; }

async function readJson(r) {
  const text = await r.text();
  if (!text) return {};
  try { return JSON.parse(text); } catch { return { detail: text.slice(0, 300) }; }
}

function setStatus(ok) { document.getElementById('status-dot').classList.toggle('ok', ok); }

function showView(name) {
  const views = { terminal: 'terminal-view', dashboard: 'dashboard', subagents: 'subagents' };
  const tabs = { terminal: 'tab-term', dashboard: 'tab-dash', subagents: 'tab-sub' };
  for (const [key, id] of Object.entries(views)) {
    document.getElementById(id).classList.toggle('active', key === name);
    document.getElementById(tabs[key]).classList.toggle('active', key === name);
  }
  if (name === 'terminal') { setTimeout(fitTerminal, 0); }
  else if (name === 'dashboard') { loadDashboard(); }
  else if (name === 'subagents') { loadSubagents(); }
}

async function loadWhoami() {
  try {
    const r = await fetch(apiUrl('/api/whoami'));
    const data = await readJson(r);
    currentUser = data.user || null;
    document.getElementById('whoami').textContent = currentUser ? ('@' + currentUser) : '';
  } catch (e) { /* ignore */ }
}

// --------------------------- Terminal ---------------------------
function fitTerminal() {
  if (!fitAddon) return;
  try { fitAddon.fit(); sendResize(); } catch (e) { /* container not visible yet */ }
}

function wsUrl() {
  const base = window.location.pathname.replace(/\/+$/, '');
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const cols = term ? term.cols : 80;
  const rows = term ? term.rows : 24;
  let url = `${proto}://${location.host}${base}/ws/pty?cols=${cols}&rows=${rows}`;
  if (currentUser) url += '&user=' + encodeURIComponent(currentUser);
  if (resumeSession) url += '&session=' + encodeURIComponent(resumeSession);
  return url;
}

function connect() {
  if (ws) { try { ws.close(); } catch (e) {} }
  ws = new WebSocket(wsUrl());
  ws.binaryType = 'arraybuffer';
  ws.onopen = () => { setStatus(true); sendResize(); if (term) term.focus(); };
  ws.onmessage = (e) => {
    if (typeof e.data === 'string') term.write(e.data);
    else term.write(new Uint8Array(e.data));
  };
  ws.onclose = () => { setStatus(false); if (term) term.write('\r\n\x1b[90m[disconnected — press Reconnect]\x1b[0m\r\n'); };
  ws.onerror = () => { setStatus(false); };
}

function sendResize() {
  if (ws && ws.readyState === 1 && term) {
    ws.send(JSON.stringify({ type: 'resize', cols: term.cols, rows: term.rows }));
  }
}

function reconnect() { if (term) term.reset(); connect(); }
function restartSession() { resumeSession = null; reconnect(); }

function initTerminal() {
  term = new Terminal({
    cursorBlink: true,
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
    fontSize: 13,
    theme: { background: '#0f1117', foreground: '#e6e9ef', cursor: '#6d8bff',
             selectionBackground: '#33415e' },
  });
  fitAddon = new FitAddon.FitAddon();
  term.loadAddon(fitAddon);
  term.open(document.getElementById('terminal'));
  fitTerminal();
  // Chat-style Enter handling. Ctrl/Cmd+Enter submits; plain Enter inserts a
  // newline while composing but still confirms menus/approval prompts. The
  // context-awareness lives in pi: keybindings.json rebinds tui.input.submit to
  // ctrl+enter and adds enter to tui.input.newLine, while tui.select.confirm
  // stays on enter (so overlays keep confirming on Enter). Browsers collapse
  // modifiers into a bare \r, so the only rewrite we need here is turning
  // Ctrl/Cmd+Enter into pi's ctrl+enter code (CSI-u \x1b[13;5u). Everything
  // else (plain Enter -> \r, Alt+Enter -> \x1b\r for follow-up) passes through.
  term.attachCustomKeyEventHandler(e => {
    if (e.key !== 'Enter' || !(e.ctrlKey || e.metaKey)) return true;
    if (e.type === 'keydown' && ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'input', data: '\x1b[13;5u' }));
    }
    return false;
  });
  term.onData(d => { if (ws && ws.readyState === 1) ws.send(JSON.stringify({ type: 'input', data: d })); });
  window.addEventListener('resize', fitTerminal);
  connect();
}

// --------------------------- Dashboard ---------------------------
function card(label, value) {
  return `<div class="card"><div class="label">${label}</div><div class="value">${value}</div></div>`;
}

function drawChart(id, type, labels, values, label, extraOptions) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  charts[id] = new Chart(ctx, {
    type,
    data: {
      labels,
      datasets: [{
        label, data: values,
        backgroundColor: type === 'bar'
          ? '#6d8bff'
          : ['#6d8bff', '#38d39f', '#f0a93b', '#e0607e', '#9b6dff', '#46c7e8'],
        borderColor: '#6d8bff', borderWidth: 2, tension: 0.3, fill: false,
      }],
    },
    options: Object.assign({
      responsive: true,
      plugins: { legend: { display: type === 'doughnut', labels: { color: '#9aa3b2' } } },
      scales: type === 'doughnut' ? {} : {
        x: { ticks: { color: '#9aa3b2' }, grid: { color: '#2a2f3d' } },
        y: { ticks: { color: '#9aa3b2' }, grid: { color: '#2a2f3d' } },
      },
    }, extraOptions || {}),
  });
}

async function loadDashboard() {
  try {
    const r = await fetch(apiUrl('/api/stats'));
    const s = await readJson(r);
    const t = s.totals || {};
    document.getElementById('cards').innerHTML =
      card('Sessions', (t.sessions || 0).toLocaleString()) +
      card('Messages', (t.messages || 0).toLocaleString()) +
      card('Total tokens', (t.total_tokens || 0).toLocaleString()) +
      card('Input / Output', `${(t.input_tokens||0).toLocaleString()} / ${(t.output_tokens||0).toLocaleString()}`) +
      card('Cache read', (t.cache_read_tokens || 0).toLocaleString()) +
      card('Est. cost', '$' + (t.cost || 0).toFixed(4));

    const tl = s.timeline || [];
    drawChart('timelineChart', 'line', tl.map(d => d.date), tl.map(d => d.messages), 'Messages',
      { aspectRatio: 6 });
    const tools = s.tool_usage || [];
    drawChart('toolChart', 'bar', tools.map(d => d.tool), tools.map(d => d.count), 'Calls');
    const models = s.model_usage || [];
    drawChart('modelChart', 'doughnut', models.map(d => d.model), models.map(d => d.count), 'Messages');
  } catch (e) {
    document.getElementById('cards').innerHTML = card('Status', 'Service unavailable');
  }
}

// --------------------------- Sub-agents ---------------------------
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

function isTerminalState(state) {
  return ['completed', 'failed', 'failed_after', 'out_of_memory', 'stopped', 'expired']
    .includes((state || '').toLowerCase());
}

async function loadSubagents() {
  let subs = [];
  try {
    const r = await fetch(apiUrl('/subagents'));
    const data = await readJson(r);
    subs = data.subagents || [];
  } catch (e) { /* leave empty */ }

  const table = document.getElementById('sub-table');
  const empty = document.getElementById('sub-empty');
  const body = document.getElementById('sub-body');
  if (!subs.length) {
    table.style.display = 'none';
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';
  table.style.display = 'table';

  subs.sort((a, b) => (b.launched_at || '').localeCompare(a.launched_at || ''));
  body.innerHTML = subs.map(s => {
    const state = s.reported ? (s.status || s.state) : s.state;
    const cls = (state || '').toLowerCase();
    const pr = s.pr_url ? `<a href="${esc(s.pr_url)}" target="_blank">link</a>` : '—';
    const cost = '$' + (Number(s.cost) || 0).toFixed(3);
    const canStop = !s.reported && !isTerminalState(s.state);
    return `<tr>
      <td class="mono">${esc(s.subagent_id)}</td>
      <td>${esc(s.title)}</td>
      <td><span class="pill ${cls}">${esc(state || 'launched')}</span></td>
      <td>${esc(s.status || '—')}</td>
      <td>${esc(s.turns || 0)}</td>
      <td>${cost}</td>
      <td>${pr}</td>
      <td><button class="sub-stop" ${canStop ? '' : 'disabled'}
           onclick="stopSubagent('${esc(s.subagent_id)}')">Stop</button></td>
    </tr>`;
  }).join('');
}

async function stopSubagent(id) {
  try {
    await fetch(apiUrl('/subagents/' + encodeURIComponent(id) + '/stop'), { method: 'POST' });
  } catch (e) { /* ignore */ }
  setTimeout(loadSubagents, 500);
}

// Keep the sub-agent panel live while it is visible.
setInterval(() => {
  if (document.getElementById('subagents').classList.contains('active')) loadSubagents();
}, 10000);

// Resolve the user first (so the WS carries it as a fallback identity), then
// boot the terminal.
(async () => { await loadWhoami(); initTerminal(); })();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
