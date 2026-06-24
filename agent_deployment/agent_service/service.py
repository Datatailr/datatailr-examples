"""FastAPI service that runs the pi coding agent and exposes it over HTTP.

Responsibilities:
- own the pi runtime and the agent config dirs (~/.pi, ~/.agents) in this container
- run pi non-interactively, isolating sessions per requesting user
- expose chat + per-user history/stats endpoints (parsed from the session store)
- persist per-user sessions AND the global config dirs to Datatailr blob storage
  so all state survives container restarts

The GUI app authenticates the browser user via the platform's `x-datatailr-user`
header and forwards the username to this service in the `X-Agent-User` header.
Sessions are stored under <PI_SESSION_DIR>/<user>/ and mirrored to blob storage
under agent_sessions/<user>/.
"""

from __future__ import annotations

import json
import os
import re
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel

from agent_service import blob_sync, pi_runner, sessions

# Default model if the `agent_model` KV key is not set. Provider-prefixed so pi
# selects OpenAI without a separate --provider flag.
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "openai/gpt-5.1")
# Thinking/reasoning level passed to pi (off, minimal, low, medium, high, xhigh).
# Enables streamed thinking feedback. Set AGENT_THINKING=off to disable.
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

# Config sync excludes the per-user sessions tree (persisted separately) so the
# global config blob never mixes user data.
_PI_CONFIG_EXCLUDES = {"sessions"}


# --------------------------------------------------------------------------- #
# Runtime configuration loaded at startup
# --------------------------------------------------------------------------- #
class _Config:
    model: str = DEFAULT_MODEL


_config = _Config()

# One lock per (user, session) so concurrent requests never write the same file.
_session_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()
_USER_RE = re.compile(r"[^A-Za-z0-9_.-]")


def _safe_user(name: Optional[str]) -> str:
    """Normalize a username into a filesystem/blob-safe token."""
    cleaned = _USER_RE.sub("_", (name or "").strip())
    return cleaned or DEFAULT_USER


def _user_session_dir(user: str) -> str:
    return os.path.join(pi_runner.PI_SESSION_DIR, user)


def _lock_for(user: str, session_id: Optional[str]) -> threading.Lock:
    key = f"{user}:{session_id or '__new__'}"
    with _locks_guard:
        return _session_locks[key]


def _load_openai_key() -> bool:
    """Load the OpenAI API key from Datatailr Secrets into the environment."""
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
    """Pin pi's default model and skip first-run network calls."""
    os.makedirs(pi_runner.PI_AGENT_DIR, exist_ok=True)
    settings_path = os.path.join(pi_runner.PI_AGENT_DIR, "settings.json")
    provider, _, model_id = _config.model.partition("/")
    settings = {
        "defaultProjectTrust": "always",
        "enableInstallTelemetry": False,
    }
    if model_id:
        settings["model"] = {"provider": provider, "id": model_id}
    try:
        with open(settings_path, "w", encoding="utf-8") as fh:
            json.dump(settings, fh, indent=2)
    except OSError:
        pass


def _setup_datatailr_skills() -> None:
    """Install the Datatailr skills into the global agent skills dir."""
    try:
        from datatailr.sbin.datatailr_cli import setup_skills

        setup_skills(global_dir=True)
    except Exception:
        pass


def _restore_state() -> None:
    """Pull global config dirs and all per-user sessions from blob storage."""
    blob_sync.pull_dir(PI_CONFIG_BLOB_PREFIX, pi_runner.PI_AGENT_DIR)
    blob_sync.pull_dir(AGENTS_BLOB_PREFIX, pi_runner.AGENTS_DIR)
    blob_sync.pull_dir(SESSIONS_BLOB_PREFIX, pi_runner.PI_SESSION_DIR)


def _persist_config() -> None:
    """Push the global config dirs (~/.pi sans sessions, and ~/.agents)."""
    blob_sync.push_dir(
        pi_runner.PI_AGENT_DIR, PI_CONFIG_BLOB_PREFIX, exclude_dirs=_PI_CONFIG_EXCLUDES
    )
    blob_sync.push_dir(pi_runner.AGENTS_DIR, AGENTS_BLOB_PREFIX)


def _persist_user_sessions(user: str) -> None:
    """Push a single user's session store."""
    blob_sync.push_dir(
        _user_session_dir(user), f"{SESSIONS_BLOB_PREFIX}/{user}"
    )


def _startup() -> None:
    os.makedirs(pi_runner.PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(pi_runner.PI_SESSION_DIR, exist_ok=True)
    os.makedirs(pi_runner.AGENTS_DIR, exist_ok=True)
    _config.model = _load_model()
    _load_openai_key()
    # Restore prior state first, then (idempotently) ensure skills/settings, then
    # persist so the blob reflects the freshly set up config.
    _restore_state()
    _setup_datatailr_skills()
    _write_pi_settings()
    _persist_config()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _startup()
    yield


app = FastAPI(title="Pi Agent Service", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Request/response models
# --------------------------------------------------------------------------- #
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    session_name: Optional[str] = None


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "OK\n"


@app.get("/")
def index() -> dict:
    return {
        "service": "Pi Agent Service",
        "model": _config.model,
        "openai_key_loaded": bool(os.environ.get("OPENAI_API_KEY")),
    }


@app.post("/chat")
def chat(req: ChatRequest, x_agent_user: Optional[str] = Header(None)) -> dict:
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")
    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        raise HTTPException(
            status_code=503,
            detail=(
                f"OpenAI API key not configured. Create a secret named "
                f"'{OPENAI_SECRET_KEY}' in the Datatailr Secrets Manager."
            ),
        )

    user = _safe_user(x_agent_user)
    session_dir = _user_session_dir(user)

    with _lock_for(user, req.session_id):
        try:
            result = pi_runner.run_pi(
                message=req.message,
                session_id=req.session_id,
                model=_config.model,
                session_name=req.session_name,
                session_dir=session_dir,
                thinking=AGENT_THINKING,
            )
        except Exception as exc:  # noqa: BLE001 - surface failures to the client
            raise HTTPException(status_code=500, detail=f"pi run failed: {exc}")

    # Persist updated history (and any config the agent changed via its tools).
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
def chat_stream(req: ChatRequest, x_agent_user: Optional[str] = Header(None)):
    """Stream pi's thinking/text/tool events live as Server-Sent Events."""
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")
    if not os.environ.get("OPENAI_API_KEY") and not _load_openai_key():
        raise HTTPException(
            status_code=503,
            detail=(
                f"OpenAI API key not configured. Create a secret named "
                f"'{OPENAI_SECRET_KEY}' in the Datatailr Secrets Manager."
            ),
        )

    user = _safe_user(x_agent_user)
    session_dir = _user_session_dir(user)
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


@app.get("/sessions")
def get_sessions(x_agent_user: Optional[str] = Header(None)) -> dict:
    user = _safe_user(x_agent_user)
    return {"user": user, "sessions": sessions.list_sessions(_user_session_dir(user))}


@app.get("/sessions/{session_id}")
def get_session(session_id: str, x_agent_user: Optional[str] = Header(None)) -> dict:
    user = _safe_user(x_agent_user)
    transcript = sessions.get_transcript(session_id, _user_session_dir(user))
    if transcript is None:
        raise HTTPException(status_code=404, detail="session not found")
    return transcript


@app.get("/stats")
def get_stats(x_agent_user: Optional[str] = Header(None)) -> dict:
    user = _safe_user(x_agent_user)
    stats = sessions.aggregate_stats(_user_session_dir(user))
    stats["user"] = user
    return stats


def main(port) -> None:
    """Datatailr service entrypoint. The platform passes the bound port."""
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(port), log_level="info")


if __name__ == "__main__":
    main(1024)
