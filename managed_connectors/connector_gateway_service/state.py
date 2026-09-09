from __future__ import annotations

import fcntl
import json
import os
import threading
from pathlib import Path
from typing import Any

STATE_PATH = Path(os.environ.get("INTEGRATION_STUDIO_STATE", "/mnt/integration-studio/state.json"))
_LOCK = threading.RLock()


def read_connector_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Connector configuration is unreadable") from exc
    if not isinstance(state, dict):
        return {}
    return state


def read_connector_settings() -> dict[str, Any]:
    settings = read_connector_state().get("settings")
    return settings if isinstance(settings, dict) else {}


def personal_connector(state: dict[str, Any], user: str, provider: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return connector config and only the signed-in user's token.

    Gmail configuration is personal. Outlook and Zoom use administrator-managed
    workspace applications with delegated tokens owned by this user.
    """
    user_state = state.get("users", {}).get(user, {})
    if provider in {"outlook", "zoom"}:
        settings = state.get("settings", {}).get(provider, {})
    else:
        settings = user_state.get("connector_settings", {}).get(provider, {})
    token = user_state.get("tokens", {}).get(provider, {})
    return (
        settings if isinstance(settings, dict) else {},
        token if isinstance(token, dict) else {},
    )


def update_personal_token(user: str, provider: str, token: dict[str, Any]) -> None:
    """Persist only a rotated delegated token back into owner-only state.

    Zoom refresh tokens rotate, so the latest token must replace the previous
    one. The write is atomic and preserves the mounted state's runtime owner.
    """
    if provider not in {"outlook", "zoom"}:
        raise ValueError("Unknown delegated OAuth provider")
    with _LOCK:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        lock_path = STATE_PATH.with_suffix(".lock")
        with lock_path.open("a+b") as handle:
            os.chmod(lock_path, 0o600)
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                state = read_connector_state()
                state.setdefault("users", {}).setdefault(user, {}).setdefault("tokens", {})[provider] = dict(token)
                previous = STATE_PATH.stat() if STATE_PATH.exists() else None
                temp = STATE_PATH.with_suffix(".gateway.tmp")
                temp.write_text(
                    json.dumps(state, separators=(",", ":"), sort_keys=True),
                    encoding="utf-8",
                )
                os.chmod(temp, 0o600)
                if previous is not None and os.geteuid() == 0:
                    os.chown(temp, previous.st_uid, previous.st_gid)
                os.replace(temp, STATE_PATH)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
