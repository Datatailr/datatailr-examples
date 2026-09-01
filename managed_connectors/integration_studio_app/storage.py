from __future__ import annotations

import copy
import fcntl
import json
import os
import threading
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Callable

STATE_PATH = Path(os.environ.get("INTEGRATION_STUDIO_STATE", "/mnt/integration-studio/state.json"))
_LOCK = threading.RLock()


@contextmanager
def _state_file_lock():
    """Serialize read-modify-write cycles across app and gateway processes."""
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_PATH.with_suffix(".lock")
    with lock_path.open("a+b") as handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


DEFAULT_SETTINGS: dict[str, Any] = {
    "slack": {
        "bot_token": "",
        "public_channels_only": True,
    },
    "hubspot": {
        "access_token": "",
        "base_url": "https://api.hubapi.com",
    },
    "github": {
        "app_id": "",
        "installation_id": "",
        "private_key": "",
        "base_url": "https://api.github.com",
    },
    "gmail": {
        "username": "",
        "app_password": "",
    },
    "outlook": {
        "tenant": "common",
        "client_id": "",
        "client_secret": "",
        "redirect_uri": "",
    },
    "zoom": {
        "client_id": "",
        "client_secret": "",
        "redirect_uri": "",
    },
}

WORKSPACE_PROVIDERS = ("slack", "hubspot", "github", "outlook", "zoom")
PERSONAL_PROVIDERS = ("gmail", "outlook", "zoom")
PERSONAL_SETTINGS_PROVIDERS = ("gmail",)

SECRET_FIELDS = {
    ("slack", "bot_token"),
    ("hubspot", "access_token"),
    ("github", "private_key"),
    ("gmail", "app_password"),
    ("outlook", "client_secret"),
    ("zoom", "client_secret"),
}


def _empty_state() -> dict[str, Any]:
    return {
        "settings": copy.deepcopy(DEFAULT_SETTINGS),
        "users": {},
        "oauth_states": {},
        "audit": [],
    }


def _merge_defaults(state: dict[str, Any]) -> dict[str, Any]:
    baseline = _empty_state()
    for key in ("users", "oauth_states", "audit"):
        baseline[key] = state.get(key, baseline[key])
    incoming = state.get("settings", {})
    for provider, defaults in DEFAULT_SETTINGS.items():
        baseline["settings"][provider].update(incoming.get(provider, {}))
    # Remove the retired GitHub Personal configuration, including the brief v1
    # shape that stored its OAuth fields on the organization connector.
    for field in ("client_id", "client_secret", "web_url", "redirect_uri"):
        baseline["settings"]["github"].pop(field, None)
    # Gmail credentials are user-owned. Outlook and Zoom OAuth applications
    # are administrator-managed at workspace scope; only delegated tokens are
    # user-owned.
    baseline["settings"]["gmail"] = copy.deepcopy(DEFAULT_SETTINGS["gmail"])
    # Gmail moved from per-user OAuth to live IMAP. Discard obsolete OAuth
    # application fields and tokens rather than carrying unused credentials.
    for user_state in baseline["users"].values():
        if not isinstance(user_state, dict):
            continue
        personal = user_state.get("connector_settings")
        if isinstance(personal, dict) and isinstance(personal.get("gmail"), dict):
            old = personal["gmail"]
            personal["gmail"] = {
                key: old.get(key, value) for key, value in DEFAULT_SETTINGS["gmail"].items()
            }
        if isinstance(personal, dict):
            # Never fall back to former per-user OAuth app registrations.
            personal.pop("outlook", None)
            personal.pop("zoom", None)
        tokens = user_state.get("tokens")
        if isinstance(tokens, dict):
            tokens.pop("gmail", None)
            tokens.pop("github_personal", None)
    baseline["oauth_states"] = {
        key: value
        for key, value in baseline["oauth_states"].items()
        if not isinstance(value, dict) or value.get("provider") not in {"gmail", "github_personal"}
    }
    # These used to gate administrator-managed connectors independently from
    # the app ACL. Ignore and discard them when reading older state.
    for provider in ("slack", "hubspot", "github"):
        baseline["settings"][provider].pop("allowed_groups", None)
    return baseline


def read_state() -> dict[str, Any]:
    with _LOCK:
        if not STATE_PATH.exists():
            return _empty_state()
        try:
            state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Integration Studio state is unreadable") from exc
        if not isinstance(state, dict):
            raise RuntimeError("Integration Studio state must be a JSON object")
        return _merge_defaults(state)


def write_state(state: dict[str, Any]) -> None:
    with _LOCK:
        previous = STATE_PATH.stat() if STATE_PATH.exists() else None
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp = STATE_PATH.with_suffix(".tmp")
        temp.write_text(
            json.dumps(state, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        os.chmod(temp, 0o600)
        # Normal requests run as the job user. Preserve that owner if a
        # privileged maintenance command updates state through this function;
        # otherwise os.replace() would silently install a root-owned 0600 file
        # that the application can no longer read.
        if previous is not None and os.geteuid() == 0:
            os.chown(temp, previous.st_uid, previous.st_gid)
        os.replace(temp, STATE_PATH)


def update_state(mutator: Callable[[dict[str, Any]], Any]) -> Any:
    with _LOCK:
        with _state_file_lock():
            state = read_state()
            result = mutator(state)
            write_state(state)
            return result


def public_settings(settings: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(settings)
    for provider, field in SECRET_FIELDS:
        value = result[provider].pop(field, "")
        result[provider][f"has_{field}"] = bool(value)
    return result


def settings_for_user(state: dict[str, Any], user: str) -> dict[str, Any]:
    """Compose workspace settings with one user's private mailbox settings.

    Gmail IMAP credentials live below users/<name>. Outlook and Zoom OAuth
    applications are shared workspace configuration, while delegated tokens
    remain below the individual user.
    """
    result = copy.deepcopy(state["settings"])
    personal = state.get("users", {}).get(user, {}).get("connector_settings", {})
    for provider in PERSONAL_SETTINGS_PROVIDERS:
        result[provider] = copy.deepcopy(DEFAULT_SETTINGS[provider])
        values = personal.get(provider, {})
        if isinstance(values, dict):
            result[provider].update(values)
    return result


def public_workspace_settings(settings: dict[str, Any]) -> dict[str, Any]:
    visible = public_settings(settings)
    return {provider: visible[provider] for provider in WORKSPACE_PROVIDERS}


def public_personal_settings(state: dict[str, Any], user: str) -> dict[str, Any]:
    visible = public_settings(settings_for_user(state, user))
    return {
        "gmail": visible["gmail"],
        "outlook": {"managed_by_admin": True},
        "zoom": {"managed_by_admin": True},
    }


def audit(state: dict[str, Any], user: str, action: str, detail: dict[str, Any] | None = None) -> None:
    from datetime import UTC, datetime

    state["audit"].append(
        {
            "at": datetime.now(UTC).isoformat(),
            "user": user,
            "action": action,
            "detail": detail or {},
        }
    )
    state["audit"] = state["audit"][-500:]
