"""FastAPI service that runs the pi coding agent and exposes it over HTTP.

Responsibilities:
- own the pi runtime and its ~/.pi session store inside this container
- expose a chat API that drives pi non-interactively
- expose history/stats endpoints built by parsing the ~/.pi session JSONL
- persist the session store to Datatailr blob storage across restarts

The GUI app talks to this service over Datatailr's internal service URL.
"""

from __future__ import annotations

import json
import os
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from agent_service import pi_runner, sessions

# Default model if the `agent_model` KV key is not set. Provider-prefixed so pi
# selects OpenAI without a separate --provider flag.
DEFAULT_MODEL = os.environ.get("AGENT_MODEL", "openai/gpt-5.1")
# Datatailr secret/KV key names (create the secret in the Secrets Manager UI).
OPENAI_SECRET_KEY = os.environ.get("OPENAI_SECRET_KEY", "openai_api_key")
MODEL_KV_KEY = os.environ.get("MODEL_KV_KEY", "agent_model")


# --------------------------------------------------------------------------- #
# Runtime configuration loaded at startup
# --------------------------------------------------------------------------- #
class _Config:
    model: str = DEFAULT_MODEL


_config = _Config()

# One lock per session id so concurrent requests never write the same file.
_session_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()


def _lock_for(session_id: Optional[str]) -> threading.Lock:
    key = session_id or "__new__"
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
    """Setup the Datatailr skills."""
    from datatailr.sbin.datatailr_cli import setup_skills
    setup_skills(global_dir=True)

def _startup() -> None:
    os.makedirs(pi_runner.PI_WORKSPACE_DIR, exist_ok=True)
    os.makedirs(pi_runner.PI_SESSION_DIR, exist_ok=True)
    _config.model = _load_model()
    _load_openai_key()
    _setup_datatailr_skills()
    _write_pi_settings()
    sessions.restore_from_blob()


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
def chat(req: ChatRequest) -> dict:
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

    with _lock_for(req.session_id):
        try:
            result = pi_runner.run_pi(
                message=req.message,
                session_id=req.session_id,
                model=_config.model,
                session_name=req.session_name,
            )
        except Exception as exc:  # noqa: BLE001 - surface failures to the client
            raise HTTPException(status_code=500, detail=f"pi run failed: {exc}")

    # Persist updated history so it survives container restarts.
    sessions.sync_to_blob()

    return {
        "session_id": result.session_id,
        "reply": result.reply,
        "usage": result.usage,
    }


@app.get("/sessions")
def get_sessions() -> dict:
    return {"sessions": sessions.list_sessions()}


@app.get("/sessions/{session_id}")
def get_session(session_id: str) -> dict:
    transcript = sessions.get_transcript(session_id)
    if transcript is None:
        raise HTTPException(status_code=404, detail="session not found")
    return transcript


@app.get("/stats")
def get_stats() -> dict:
    return sessions.aggregate_stats()


def main(port) -> None:
    """Datatailr service entrypoint. The platform passes the bound port."""
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(port), log_level="info")


if __name__ == "__main__":
    main(1024)
