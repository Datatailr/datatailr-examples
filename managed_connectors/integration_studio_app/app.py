from __future__ import annotations

import copy
import os
import secrets
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from datatailr import User
from flask import Flask, jsonify, redirect, render_template, request

from integration_studio_app.connector_audit import list_connector_events, record_connector_event
from integration_studio_app.ingestion import get_sync_manager
from integration_studio_app.knowledge import get_index
from integration_studio_app.providers import (
    ProviderError,
    exchange_oauth_code,
    oauth_authorization_url,
    outlook_calendar_authorized,
    revoke_oauth_token,
    test_connector,
    zoom_ai_companion_authorized,
    zoom_retained_transcript_authorized,
)
from integration_studio_app.storage import (
    DEFAULT_SETTINGS,
    PERSONAL_PROVIDERS,
    PERSONAL_SETTINGS_PROVIDERS,
    SECRET_FIELDS,
    WORKSPACE_PROVIDERS,
    audit,
    public_personal_settings,
    public_workspace_settings,
    read_state,
    settings_for_user,
    update_state,
)


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024


def _platform_origin() -> str:
    value = (
        os.environ.get("INTEGRATION_STUDIO_PLATFORM_URL")
        or os.environ.get("DATATAILR_DOMAIN")
        or "http://localhost:5000"
    ).strip().rstrip("/")
    if "://" not in value:
        value = "https://" + value
    return value


ADMIN_USERS = {
    name.strip()
    for name in os.environ.get("INTEGRATION_STUDIO_ADMINS", "").split(",")
    if name.strip()
}
DEPLOY_ENVIRONMENT = os.environ.get("DATATAILR_JOB_ENVIRONMENT", "dev")
APP_JOB_NAME = os.environ.get("DATATAILR_JOB_NAME", "integration-studio")
PUBLIC_PLATFORM_URL = _platform_origin()
PUBLIC_APP_URL = os.environ.get(
    "INTEGRATION_STUDIO_PUBLIC_URL",
    f"{PUBLIC_PLATFORM_URL}/job/{DEPLOY_ENVIRONMENT}/{APP_JOB_NAME}",
).rstrip("/")
LIVE_ONLY_SOURCES = {"gmail", "outlook", "zoom"}
LIVE_SHARED_SOURCES = {"github"}


def _current_user() -> User:
    user = User.from_request(request)
    if user is None or not user.name:
        raise PermissionError("No authenticated Datatailr user was forwarded to the app")
    return user


def _is_admin(user: User) -> bool:
    return bool(user.name in ADMIN_USERS or "admin" in set(user.groups or []))


def _connector_access(user: User) -> dict[str, list[str]]:
    """Describe configuration surfaces without exposing any settings values."""
    return {
        "workspace_configurable": list(WORKSPACE_PROVIDERS) if _is_admin(user) else [],
        "personal_configurable": list(PERSONAL_SETTINGS_PROVIDERS),
        "personal_connectable": list(PERSONAL_PROVIDERS),
    }


def _json_body() -> dict[str, Any]:
    if request.headers.get("X-Requested-With") != "IntegrationStudio":
        raise PermissionError("Missing same-origin request marker")
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object")
    return data


def _apply_settings_values(settings: dict[str, Any], incoming: dict[str, Any]) -> None:
    """Merge public form values without allowing a masked secret to erase storage."""
    for provider, defaults in DEFAULT_SETTINGS.items():
        values = incoming.get(provider)
        if not isinstance(values, dict):
            continue
        for field in defaults:
            if field not in values:
                continue
            value = values[field]
            if (provider, field) in SECRET_FIELDS and not str(value or "").strip():
                continue
            if isinstance(value, str):
                value = value.strip()
            if provider == "gmail" and field == "app_password":
                value = "".join(str(value).split())
                if len(value) != 16:
                    raise ValueError("Google app password must contain exactly 16 characters")
            if provider == "gmail" and field == "username" and value and "@" not in str(value):
                raise ValueError("Enter the complete Gmail address")
            settings[provider][field] = value


def _error(message: str, status: int = 400):
    return jsonify({"ok": False, "error": message}), status


@app.errorhandler(PermissionError)
def _permission_error(exc: PermissionError):
    return _error(str(exc), 403)


@app.errorhandler(ProviderError)
def _provider_error(exc: ProviderError):
    return _error(str(exc), 502)


@app.errorhandler(ValueError)
def _value_error(exc: ValueError):
    return _error(str(exc), 400)


@app.get("/")
def index():
    return render_template("index.html", deploy_environment=DEPLOY_ENVIRONMENT)


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


def _blocked_reason(source: dict[str, Any], admin: bool) -> str:
    """Why this source cannot be used right now, phrased as the next action.

    An unusable source that only greys out forces the user to guess; naming the
    blocker tells them whether to wait for an admin or fix it themselves.
    """
    if not source["configured"]:
        if source.get("setup_by_admin"):
            return "Set up in Admin" if admin else "Waiting on admin setup"
        if source.get("connectable"):
            return "Configure in Connections"
        return "Set up in Admin" if admin else "Waiting on admin setup"
    if source["connectable"] and not source.get("connected"):
        return "Not connected yet"
    return ""


def _source_status(state: dict[str, Any], user: User) -> list[dict[str, Any]]:
    settings = settings_for_user(state, user.name)
    user_state = state["users"].get(user.name, {})
    tokens = user_state.get("tokens", {})
    admin = _is_admin(user)
    sources = [
        {
            "id": "slack",
            "label": "Slack",
            "scope": "Public channels · read and bot posts",
            "trust": "shared",
            "configured": bool(settings["slack"].get("bot_token")),
            "allowed": True,
            "connectable": False,
        },
        {
            "id": "hubspot",
            "label": "HubSpot",
            "scope": "CRM records and activities",
            "trust": "shared",
            "configured": bool(settings["hubspot"].get("access_token")),
            "allowed": True,
            "connectable": False,
        },
        {
            "id": "github",
            "label": "GitHub",
            "scope": "App-selected repositories · fetched live",
            "trust": "shared",
            "live_only": True,
            "configured": bool(
                settings["github"].get("app_id")
                and settings["github"].get("installation_id")
                and settings["github"].get("private_key")
            ),
            "allowed": True,
            "connectable": False,
        },
        {
            "id": "gmail",
            "label": "Gmail",
            "scope": "Your mailbox · fetched live",
            "trust": "personal",
            "live_only": True,
            "configured": bool(settings["gmail"].get("username") and settings["gmail"].get("app_password")),
            "allowed": True,
            "connected": bool(settings["gmail"].get("username") and settings["gmail"].get("app_password")),
            "connectable": True,
        },
        {
            "id": "outlook",
            "label": "Outlook",
            "scope": "Your Microsoft 365 mail and calendar · fetched live",
            "trust": "personal",
            "live_only": True,
            "configured": bool(settings["outlook"].get("client_id") and settings["outlook"].get("client_secret")),
            "setup_by_admin": True,
            "allowed": True,
            "connected": bool(
                tokens.get("outlook", {}).get("access_token")
                or tokens.get("outlook", {}).get("refresh_token")
            ),
            "calendar_authorized": outlook_calendar_authorized(tokens.get("outlook")),
            "connectable": True,
        },
        {
            "id": "zoom",
            "label": "Zoom",
            "scope": "Your AI Companion summaries and retained transcripts · fetched live",
            "trust": "personal",
            "live_only": True,
            "configured": bool(settings["zoom"].get("client_id") and settings["zoom"].get("client_secret")),
            "setup_by_admin": True,
            "allowed": True,
            "connected": bool(tokens.get("zoom", {}).get("access_token") or tokens.get("zoom", {}).get("refresh_token")),
            "ai_companion_authorized": zoom_ai_companion_authorized(tokens.get("zoom")),
            "retained_transcript_authorized": zoom_retained_transcript_authorized(tokens.get("zoom")),
            "connectable": True,
        },
    ]
    for source in sources:
        source["reason"] = _blocked_reason(source, admin)
        source["usable"] = not source["reason"]
    try:
        index_stats = get_index().stats(user.name, sorted(set(user.groups or []) | {"dtusers"}))
        for source in sources:
            source["indexed_documents"] = (
                0 if source.get("live_only") else index_stats["visible"].get(source["id"], 0)
            )
    except Exception:
        for source in sources:
            source["indexed_documents"] = 0
    return sources


@app.get("/api/bootstrap")
def bootstrap():
    user = _current_user()
    get_sync_manager().start_scheduler()
    state = read_state()
    return jsonify(
        {
            "ok": True,
            "user": {
                "name": user.name,
                "email": user.email,
                "groups": list(user.groups or []),
                "is_admin": _is_admin(user),
            },
            "sources": _source_status(state, user),
            "settings": public_workspace_settings(state["settings"]) if _is_admin(user) else None,
            "personal_settings": public_personal_settings(state, user.name),
            "connector_access": _connector_access(user),
            "oauth_callbacks": {
                "outlook": PUBLIC_APP_URL + "/oauth/outlook/callback",
                "zoom": PUBLIC_APP_URL + "/oauth/zoom/callback",
            },
        }
    )


@app.get("/api/admin/connector-audit")
def connector_audit_log():
    user = _current_user()
    if not _is_admin(user):
        raise PermissionError("Only platform administrators can view connector audit logs")
    connector = str(request.args.get("connector") or "").strip().lower()
    status = str(request.args.get("status") or "").strip().lower()
    audited_user = str(request.args.get("user") or "").strip()
    if connector and connector not in {"slack", "hubspot", "github", "gmail", "outlook", "zoom"}:
        raise ValueError("Unknown connector filter")
    if status and status not in {"succeeded", "failed", "duplicate", "skipped", "requested"}:
        raise ValueError("Unknown status filter")
    try:
        limit = int(request.args.get("limit") or 200)
    except ValueError as exc:
        raise ValueError("limit must be an integer") from exc
    return jsonify(
        {
            "ok": True,
            "events": list_connector_events(
                connector=connector, user=audited_user, status=status, limit=limit
            ),
            "privacy": {
                "personal_connectors": ["gmail", "outlook", "zoom"],
                "personal_content_stored": False,
            },
        }
    )


@app.post("/api/settings")
def save_settings():
    user = _current_user()
    if not _is_admin(user):
        raise PermissionError("Only the Integration Studio administrator can change settings")
    incoming = _json_body().get("settings")
    if not isinstance(incoming, dict):
        raise ValueError("settings must be an object")
    unsupported = sorted(set(incoming) - set(WORKSPACE_PROVIDERS))
    if unsupported:
        raise ValueError(
            "Personal Gmail settings must be saved from Connections"
        )

    def mutate(state: dict[str, Any]) -> None:
        _apply_settings_values(state["settings"], incoming)
        audit(state, user.name, "settings.update", {"providers": sorted(incoming)})

    update_state(mutate)
    manager = get_sync_manager()
    for source in ("slack", "hubspot", "github"):
        if source in incoming:
            record_connector_event(
                user=user.name,
                connector=source,
                capability=f"{source}.configuration",
                operation="configuration",
                status="succeeded",
                metadata={"surface": "integration-studio"},
            )
            if source in {"slack", "hubspot"}:
                manager.trigger(source, force_full=True, actor=user.name)
    return jsonify({"ok": True})


@app.post("/api/personal-settings/<provider>")
def save_personal_settings(provider: str):
    user = _current_user()
    if provider not in PERSONAL_SETTINGS_PROVIDERS:
        raise ValueError("Unknown personal connector")
    incoming = _json_body().get("settings")
    if not isinstance(incoming, dict):
        raise ValueError("settings must be an object")

    def mutate(state: dict[str, Any]) -> None:
        user_state = state["users"].setdefault(user.name, {})
        personal = user_state.setdefault("connector_settings", {})
        current = copy.deepcopy(DEFAULT_SETTINGS[provider])
        current.update(personal.get(provider, {}))
        _apply_settings_values({provider: current}, {provider: incoming})
        personal[provider] = current
        audit(state, user.name, "personal_connector.settings", {"provider": provider})

    update_state(mutate)
    record_connector_event(
        user=user.name,
        connector=provider,
        capability=f"{provider}.configuration",
        operation="configuration",
        status="succeeded",
        metadata={"surface": "integration-studio", "data_mode": "live"},
    )
    state = read_state()
    return jsonify(
        {
            "ok": True,
            "settings": public_personal_settings(state, user.name)[provider],
        }
    )


@app.post("/api/test/<provider>")
def test(provider: str):
    user = _current_user()
    if not _is_admin(user):
        raise PermissionError("Only an Integration Studio administrator can test connector settings")
    if provider not in WORKSPACE_PROVIDERS:
        raise ValueError("Unknown provider")
    body = _json_body()
    settings = copy.deepcopy(read_state()["settings"])
    draft = body.get("settings")
    if draft is not None:
        if not isinstance(draft, dict):
            raise ValueError("settings must be an object")
        _apply_settings_values(settings, {provider: draft})
    started = time.monotonic()
    try:
        result = test_connector(provider, settings)
    except Exception as exc:
        if provider in {"slack", "hubspot", "github"}:
            record_connector_event(
                user=user.name, connector=provider, capability=f"{provider}.connection.test",
                operation="test", status="failed",
                duration_ms=round((time.monotonic() - started) * 1000),
                metadata={"surface": "integration-studio", "error_type": type(exc).__name__},
            )
        raise
    if provider in {"slack", "hubspot", "github"}:
        record_connector_event(
            user=user.name, connector=provider, capability=f"{provider}.connection.test",
            operation="test", status="succeeded",
            duration_ms=round((time.monotonic() - started) * 1000),
            metadata={"surface": "integration-studio"},
        )
    update_state(lambda state: audit(state, user.name, "provider.test", {"provider": provider, "ok": True}))
    return jsonify(result)


@app.post("/api/personal-test/<provider>")
def test_personal_connector(provider: str):
    user = _current_user()
    if provider not in PERSONAL_SETTINGS_PROVIDERS:
        raise ValueError("Unknown personal connector")
    _json_body()
    settings = settings_for_user(read_state(), user.name)
    started = time.monotonic()
    try:
        result = test_connector(provider, settings)
    except Exception as exc:
        record_connector_event(
            user=user.name, connector=provider, capability=f"{provider}.connection.test",
            operation="test", status="failed",
            duration_ms=round((time.monotonic() - started) * 1000),
            metadata={
                "surface": "integration-studio", "data_mode": "live",
                "error_type": type(exc).__name__,
            },
        )
        raise
    record_connector_event(
        user=user.name, connector=provider, capability=f"{provider}.connection.test",
        operation="test", status="succeeded",
        duration_ms=round((time.monotonic() - started) * 1000),
        metadata={"surface": "integration-studio", "data_mode": "live", "connected": True},
    )
    update_state(
        lambda state: audit(
            state, user.name, "personal_connector.test", {"provider": provider, "ok": True}
        )
    )
    return jsonify(result)


@app.get("/oauth/<provider>/start")
def oauth_start(provider: str):
    user = _current_user()
    if provider not in {"outlook", "zoom"}:
        raise ValueError("Unknown OAuth connector")
    state_data = read_state()
    cfg = state_data["settings"][provider]
    if not cfg.get("client_id") or not cfg.get("client_secret"):
        raise ProviderError(f"An administrator must configure the {provider.title()} application first")
    state_token = secrets.token_urlsafe(32)
    redirect_uri = cfg.get("redirect_uri") or f"{PUBLIC_APP_URL}/oauth/{provider}/callback"

    def mutate(state: dict[str, Any]) -> None:
        state["oauth_states"][state_token] = {
            "provider": provider,
            "user": user.name,
            "redirect_uri": redirect_uri,
            "expires_at": (datetime.now(UTC) + timedelta(minutes=10)).isoformat(),
        }
        audit(state, user.name, "oauth.start", {"provider": provider})

    update_state(mutate)
    record_connector_event(
        user=user.name, connector=provider, capability=f"{provider}.oauth",
        operation="connect", status="requested",
        metadata={"surface": "integration-studio", "data_mode": "live", "connected": False},
    )
    return redirect(
        oauth_authorization_url(provider, cfg, state_token, redirect_uri)
    )


@app.get("/oauth/<provider>/callback")
def oauth_callback(provider: str):
    if provider not in {"outlook", "zoom"}:
        raise ValueError("Unknown OAuth connector")
    state_token = request.args.get("state", "")
    code = request.args.get("code", "")
    if not state_token or not code:
        return redirect(PUBLIC_APP_URL + "/?oauth=failed")
    state_data = read_state()
    oauth_state = state_data["oauth_states"].get(state_token)
    if not oauth_state or oauth_state.get("provider") != provider:
        return redirect(PUBLIC_APP_URL + "/?oauth=invalid_state")
    if datetime.fromisoformat(oauth_state["expires_at"]) < datetime.now(UTC):
        return redirect(PUBLIC_APP_URL + "/?oauth=expired")
    cfg = state_data["settings"][provider]
    token = exchange_oauth_code(
        provider,
        cfg,
        code,
        oauth_state["redirect_uri"],
    )

    def mutate(state: dict[str, Any]) -> None:
        item = state["oauth_states"].pop(state_token, None)
        if not item:
            raise ValueError("OAuth state was already used")
        user_state = state["users"].setdefault(item["user"], {})
        user_state.setdefault("tokens", {})[provider] = token
        audit(state, item["user"], "oauth.connected", {"provider": provider})

    update_state(mutate)
    record_connector_event(
        user=str(oauth_state["user"]), connector=provider, capability=f"{provider}.oauth",
        operation="connect", status="succeeded",
        metadata={"surface": "integration-studio", "data_mode": "live", "connected": True},
    )
    return redirect(PUBLIC_APP_URL + "/?oauth=connected")


@app.post("/api/disconnect/<provider>")
def disconnect(provider: str):
    user = _current_user()
    _json_body()
    if provider not in LIVE_ONLY_SOURCES:
        raise ValueError("Unknown personal connector")

    state_before = read_state()
    upstream_revoked = False
    if provider == "zoom":
        token = state_before.get("users", {}).get(user.name, {}).get("tokens", {}).get(provider, {})
        try:
            revoke_oauth_token(provider, state_before["settings"][provider], token)
            upstream_revoked = bool(token.get("access_token"))
        except (ProviderError, KeyError):
            # Local deletion is the privacy-critical step. A missing/expired
            # upstream token must never prevent the user from disconnecting.
            upstream_revoked = False

    def mutate(state: dict[str, Any]) -> None:
        user_state = state["users"].setdefault(user.name, {})
        user_state.setdefault("tokens", {}).pop(provider, None)
        if provider == "gmail":
            user_state.setdefault("connector_settings", {}).pop("gmail", None)
        audit(state, user.name, "oauth.disconnected", {"provider": provider})

    update_state(mutate)
    # No mail records are persisted. delete_scope remains as a defensive
    # cleanup for installations upgraded from the former ingestion design.
    removed = get_index().delete_scope(provider, user.name)
    record_connector_event(
        user=user.name, connector=provider, capability=f"{provider}.connection",
        operation="disconnect", status="succeeded",
        metadata={"surface": "integration-studio", "data_mode": "live", "connected": False},
    )
    return jsonify({
        "ok": True,
        "removed_legacy_documents": removed,
        "upstream_revoked": upstream_revoked,
    })


@app.get("/api/ingestion/status")
def ingestion_status():
    user = _current_user()
    get_sync_manager().start_scheduler()
    groups = sorted(set(user.groups or []) | {"dtusers"})
    return jsonify({"ok": True, **get_index().stats(user.name, groups)})


@app.post("/api/ingestion/sync")
def ingestion_sync():
    user = _current_user()
    body = _json_body()
    requested = body.get("sources", [])
    if not isinstance(requested, list) or not requested:
        raise ValueError("sources must be a non-empty list")
    force_full = bool(body.get("full"))
    state = read_state()
    statuses = {item["id"]: item for item in _source_status(state, user)}
    started = []
    manager = get_sync_manager()
    for source in dict.fromkeys(str(value) for value in requested):
        if source not in {"slack", "hubspot", *LIVE_ONLY_SOURCES, *LIVE_SHARED_SOURCES}:
            raise ValueError(f"Unknown source: {source}")
        if not statuses.get(source, {}).get("usable"):
            raise PermissionError(f"{source.title()} is not available to this user")
        if source in {"slack", "hubspot"}:
            if not _is_admin(user):
                raise PermissionError("Only an administrator can synchronize shared connectors")
            started.append(manager.trigger(source, force_full=force_full, actor=user.name))
            record_connector_event(
                user=user.name, connector=source, capability=f"{source}.sync",
                operation="sync", status="requested",
                metadata={
                    "surface": "integration-studio", "full": force_full,
                    "data_mode": "indexed",
                },
            )
        else:
            raise ValueError(
                f"{source.title()} is live-only and is fetched at request time; it is never synchronized"
            )
    return jsonify({"ok": True, "syncs": started}), 202


if __name__ == "__main__":
    app.run("0.0.0.0", 8080, debug=True)
