from __future__ import annotations

import base64
import binascii
import os
import re
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from importlib.resources import files
from typing import Any

import requests
from datatailr import User
from flask import Flask, Response, jsonify, request

from .catalog import CAPABILITIES, SYNTHETIC_EXAMPLES
from .live_mail import (
    LiveMailError,
    outlook_calendar_authorized,
    query_live_mail,
    query_live_outlook,
)
from .live_github import LiveGitHubError, query_live_github
from .live_zoom import (
    LiveZoomError,
    query_live_zoom,
    zoom_ai_companion_authorized,
    zoom_retained_transcript_authorized,
)
from .state import personal_connector, read_connector_settings, read_connector_state
from .store import (
    complete_action,
    connection_counts,
    list_connector_events,
    query,
    record_connector_event,
    reserve_action,
    visible_slack_channel,
)


app = Flask(__name__)
# Slack documents are capped at five raw MiB. Base64 and JSON framing require a
# slightly larger request while every other gateway request remains tiny.
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024
OPENAPI_DOCUMENT = files(__package__).joinpath("openapi.json").read_text(encoding="utf-8")
ADMINS = {
    name.strip()
    for name in os.environ.get("CONNECTOR_GATEWAY_ADMINS", "").split(",")
    if name.strip()
}
_CHANNEL = re.compile(r"^[A-Za-z0-9_-]+$")
_KEY = re.compile(r"^[A-Za-z0-9._:/-]+$")
_RATE_LOCK = threading.Lock()
_RATE: dict[str, deque[float]] = defaultdict(deque)
_MAX_SLACK_FILE_BYTES = 5 * 1024 * 1024


class GatewayError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def _identity() -> tuple[str, list[str], bool]:
    user = User.from_request(request)
    if user is None or not user.name:
        raise GatewayError("A signed Datatailr user identity is required", 401)
    groups = list(user.groups or [])
    return user.name, groups, user.name in ADMINS or "admin" in groups


def _rate_limit(user: str) -> None:
    now = time.monotonic()
    with _RATE_LOCK:
        values = _RATE[user]
        while values and values[0] < now - 60:
            values.popleft()
        if len(values) >= 120:
            raise GatewayError("Connector request rate exceeded", 429)
        values.append(now)


def _body() -> dict[str, Any]:
    if request.headers.get("X-Datatailr-Connector-Client") != "1":
        raise GatewayError("Use the Datatailr connector client", 403)
    value = request.get_json(silent=True)
    if not isinstance(value, dict):
        raise GatewayError("Expected a JSON object")
    return value


def _parameters(capability: str, supplied: Any, kind: str) -> dict[str, Any]:
    definition = CAPABILITIES.get(capability)
    if not definition or definition["kind"] != kind:
        raise GatewayError(f"Unknown {kind} capability")
    if not isinstance(supplied, dict):
        raise GatewayError("parameters must be an object")
    schema = definition["parameters"]
    if set(supplied) - set(schema):
        raise GatewayError("Unsupported parameters: " + ", ".join(sorted(set(supplied) - set(schema))))
    result: dict[str, Any] = {}
    for name, rule in schema.items():
        value = supplied.get(name, rule.get("default"))
        if value is None:
            if rule.get("required"):
                raise GatewayError(f"{name} is required")
            continue
        expected = rule["type"]
        if expected == "integer":
            if not isinstance(value, int) or isinstance(value, bool) or not rule["minimum"] <= value <= rule["maximum"]:
                raise GatewayError(f"{name} must be an integer from {rule['minimum']} to {rule['maximum']}")
        elif expected == "boolean":
            if not isinstance(value, bool):
                raise GatewayError(f"{name} must be a boolean")
        elif expected == "enum":
            if value not in rule["values"]:
                raise GatewayError(f"{name} must be one of: {', '.join(rule['values'])}")
        elif expected == "string_array":
            if (
                not isinstance(value, list)
                or not rule["minimum"] <= len(value) <= rule["maximum"]
                or any(
                    not isinstance(item, str)
                    or not item.strip()
                    or len(item.strip()) > rule["max_length"]
                    for item in value
                )
            ):
                raise GatewayError(
                    f"{name} must contain {rule['minimum']} to {rule['maximum']} "
                    f"strings of at most {rule['max_length']} characters"
                )
            value = [item.strip() for item in value]
            if rule.get("values") and any(item not in rule["values"] for item in value):
                raise GatewayError(
                    f"{name} must contain only: {', '.join(rule['values'])}"
                )
        elif expected == "datetime":
            if not isinstance(value, str):
                raise GatewayError(f"{name} must be an ISO-8601 datetime")
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise GatewayError(f"{name} must be an ISO-8601 datetime") from exc
            if parsed.tzinfo is None:
                raise GatewayError(f"{name} must include a timezone offset")
        else:
            if not isinstance(value, str) or (rule.get("required") and not value.strip()) or len(value) > rule["max_length"]:
                raise GatewayError(f"{name} must be a non-empty string of at most {rule['max_length']} characters")
            value = value.strip()
        result[name] = value
    if "channel" in result and not _CHANNEL.fullmatch(result["channel"].lstrip("#")):
        raise GatewayError("channel must be a Slack channel name or id")
    if "idempotency_key" in result and not _KEY.fullmatch(result["idempotency_key"]):
        raise GatewayError("idempotency_key contains unsupported characters")
    if capability == "slack.files.upload":
        filename = result["filename"]
        if (
            filename in {".", ".."}
            or "/" in filename
            or "\\" in filename
            or any(ord(character) < 32 for character in filename)
        ):
            raise GatewayError("filename must be a plain file name without a path")
    if capability.startswith("outlook.calendar.") and "start" in result:
        start = datetime.fromisoformat(result["start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(result["end"].replace("Z", "+00:00"))
        if end <= start:
            raise GatewayError("end must be after start")
        if end - start > timedelta(days=62):
            raise GatewayError("calendar ranges are limited to 62 days")
    if "schedules" in result and any(
        "@" not in value or any(character in value for character in "\r\n")
        for value in result["schedules"]
    ):
        raise GatewayError("schedules must contain valid email-style identifiers")
    if capability.startswith("hubspot.activities.") and (
        ("association_type" in result) != ("association_id" in result)
    ):
        raise GatewayError("association_type and association_id must be supplied together")
    return result


def _slack_json(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise GatewayError(f"Slack returned invalid JSON (HTTP {response.status_code})", 502) from exc
    if not response.ok or not data.get("ok"):
        raise GatewayError(f"Slack request failed: {data.get('error') or response.reason}", 502)
    return data


def _slack_file_bytes(encoded: str) -> bytes:
    try:
        content = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise GatewayError("content_base64 must be valid base64") from exc
    if not content:
        raise GatewayError("The Slack document must not be empty")
    if len(content) > _MAX_SLACK_FILE_BYTES:
        raise GatewayError("Slack documents are limited to 5 MiB")
    return content


def _upload_slack_file(
    token: str, channel_id: str, params: dict[str, Any], content: bytes
) -> dict[str, Any]:
    prepare = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        json={"filename": params["filename"], "length": len(content)},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=25,
    )
    prepared = _slack_json(prepare)
    upload_url = str(prepared.get("upload_url") or "")
    file_id = str(prepared.get("file_id") or "")
    if not upload_url or not file_id:
        raise GatewayError("Slack did not return a document upload URL", 502)

    uploaded = requests.post(
        upload_url,
        data=content,
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    if not uploaded.ok:
        raise GatewayError(
            f"Slack document upload failed (HTTP {uploaded.status_code})", 502
        )

    completion: dict[str, Any] = {
        "files": [{"id": file_id, "title": params.get("title") or params["filename"]}],
        "channel_id": channel_id,
    }
    if params.get("initial_comment"):
        completion["initial_comment"] = params["initial_comment"]
    completed = _slack_json(
        requests.post(
            "https://slack.com/api/files.completeUploadExternal",
            json=completion,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=60,
        )
    )
    completed_files = completed.get("files") or []
    completed_file = completed_files[0] if completed_files else {}
    return {
        "file_id": str(completed_file.get("id") or file_id),
        "filename": params["filename"],
    }


def _live_slack_channel(token: str, requested: str) -> dict[str, str] | None:
    """Resolve a public channel even when it has no indexed messages yet."""
    target = requested.lstrip("#").casefold()
    cursor = ""
    for _page in range(20):
        params = {
            "types": "public_channel",
            "exclude_archived": "true",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        response = requests.get(
            "https://slack.com/api/conversations.list",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=25,
        )
        data = _slack_json(response)
        for value in data.get("channels") or []:
            if not isinstance(value, dict):
                continue
            channel_id = str(value.get("id") or "")
            channel_name = str(value.get("name") or "")
            if target not in {channel_id.casefold(), channel_name.casefold()}:
                continue
            if not value.get("is_member"):
                raise GatewayError(
                    f"Invite the Slack bot to #{channel_name or requested.lstrip('#')} before posting",
                    409,
                )
            return {"id": channel_id, "name": channel_name}
        cursor = str(((data.get("response_metadata") or {}).get("next_cursor")) or "")
        if not cursor:
            break
    return None


def _connector(capability: str) -> str:
    return capability.split(".", 1)[0] if "." in capability else ""


def _audit_metadata(capability: str, params: dict[str, Any]) -> dict[str, Any]:
    """Describe a request without retaining connector records or free text."""
    connector = _connector(capability)
    if connector in {"gmail", "outlook", "zoom"}:
        metadata = {
            "surface": "connector-gateway",
            "data_mode": "live",
            "limit": params.get("limit"),
            "query_supplied": bool(params.get("query")),
        }
        if connector == "zoom":
            metadata.update({
                "days": params.get("days"),
                "max_characters": params.get("max_characters"),
            })
        if connector == "outlook" and ".calendar." in capability:
            metadata.update(
                {
                    "calendar_operation": capability.rsplit(".", 1)[-1],
                    "days": params.get("days"),
                    "interval_minutes": params.get("interval_minutes"),
                    "schedule_count": len(params.get("schedules") or []),
                }
            )
        return {key: value for key, value in metadata.items() if value is not None}
    metadata: dict[str, Any] = {
        "surface": "connector-gateway",
        "data_mode": "live" if connector == "github" else "indexed",
    }
    for key in (
        "channel", "object_type", "modified_after", "limit", "days", "dry_run",
        "idempotency_key", "association_type", "repository", "path", "ref", "state",
        "max_characters",
    ):
        if key in params:
            metadata[key] = params[key]
    if "activity_types" in params:
        metadata["activity_types"] = ",".join(params["activity_types"])
    if "query" in params:
        metadata["filter_supplied"] = bool(params.get("query"))
    if "text" in params:
        metadata["text_characters"] = len(str(params.get("text") or ""))
    if capability == "slack.files.upload":
        metadata["file_extension"] = (
            params["filename"].rsplit(".", 1)[-1].casefold()
            if "." in params["filename"] else ""
        )
        metadata["comment_characters"] = len(params.get("initial_comment") or "")
    return metadata


def _error_status(exc: Exception) -> int:
    if isinstance(exc, (GatewayError, LiveGitHubError, LiveMailError, LiveZoomError)):
        return int(exc.status)
    return 500


@app.after_request
def headers(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
    return response


@app.errorhandler(Exception)
def errors(error: Exception):
    if isinstance(error, GatewayError):
        return jsonify({"error": str(error)}), error.status
    app.logger.exception("Unhandled gateway error")
    return jsonify({"error": "Connector gateway request failed"}), 500


@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "connector-gateway", "version": "13"})


@app.get("/openapi.json")
def openapi_document():
    return Response(OPENAPI_DOCUMENT, mimetype="application/json")


@app.get("/v1/capabilities")
def capabilities():
    user, _, _ = _identity()
    _rate_limit(user)
    return jsonify({"capabilities": CAPABILITIES, "synthetic_examples": SYNTHETIC_EXAMPLES})


@app.get("/v1/connections")
def connections():
    user, groups, _ = _identity()
    _rate_limit(user)
    state = read_connector_state()
    settings = state.get("settings") or {}
    counts = connection_counts(user, groups)
    result = []
    for source in ("slack", "hubspot", "github", "gmail", "outlook", "zoom"):
        count = counts.get(source, 0)
        if source == "gmail":
            config, _token = personal_connector(state, user, source)
            configured = bool(config.get("username") and config.get("app_password"))
        elif source == "outlook":
            config, token = personal_connector(state, user, source)
            configured = bool(
                config.get("client_id") and config.get("client_secret")
                and (token.get("access_token") or token.get("refresh_token"))
            )
        elif source == "zoom":
            config, token = personal_connector(state, user, source)
            configured = bool(
                config.get("client_id") and config.get("client_secret")
                and (token.get("access_token") or token.get("refresh_token"))
            )
        elif source == "github":
            config = settings.get(source) or {}
            configured = bool(
                config.get("app_id") and config.get("installation_id") and config.get("private_key")
            )
        else:
            config = settings.get(source) or {}
            configured = bool(config.get("bot_token") if source == "slack" else config.get("access_token"))
        result.append({
            "source": source, "configured": configured, "available_documents": count,
            # Kept as a response-shape compatibility alias for older clients;
            # Slack/HubSpot counts are shared and no longer ACL-filtered.
            "authorized_documents": count,
            "read_available": configured if source in {"github", "gmail", "outlook", "zoom"} else count > 0,
            "data_mode": "live" if source in {"github", "gmail", "outlook", "zoom"} else "indexed",
            "actions": ["slack.messages.post", "slack.files.upload"] if source == "slack" and configured else [],
            "required_action_scopes": ["chat:write", "files:write"] if source == "slack" else [],
            "features": (
                {
                    "mail": configured,
                    "calendar": configured and outlook_calendar_authorized(token),
                }
                if source == "outlook"
                else {
                    "ai_companion": configured and zoom_ai_companion_authorized(token),
                    "retained_transcripts": configured and zoom_retained_transcript_authorized(token),
                }
                if source == "zoom"
                else {
                    "crm_objects": configured,
                    "activities": configured,
                    "next_activity_dates": configured,
                }
                if source == "hubspot"
                else {
                    "repositories": configured,
                    "issues": configured,
                    "pull_requests": configured,
                    "commits": configured,
                    "repository_files": configured,
                }
                if source == "github"
                else {}
            ),
        })
    return jsonify({"connections": result, "user": user})


@app.get("/v1/admin/audit")
def admin_audit():
    user, _, is_admin = _identity()
    _rate_limit(user)
    if not is_admin:
        raise GatewayError("Platform administrator access is required", 403)
    connector = str(request.args.get("connector") or "").strip().lower()
    status = str(request.args.get("status") or "").strip().lower()
    audited_user = str(request.args.get("user") or "").strip()
    if connector and connector not in {"slack", "hubspot", "github", "gmail", "outlook", "zoom"}:
        raise GatewayError("Unknown connector filter")
    if status and status not in {"succeeded", "failed", "duplicate", "skipped", "requested"}:
        raise GatewayError("Unknown status filter")
    try:
        limit = int(request.args.get("limit") or 200)
    except ValueError as exc:
        raise GatewayError("limit must be an integer") from exc
    return jsonify(
        {
            "events": list_connector_events(
                connector=connector, user=audited_user, status=status, limit=limit
            ),
            "privacy": {
                "personal_connectors": ["gmail", "outlook", "zoom"],
                "personal_fields": [
                    "timestamp", "user", "capability", "operation", "status",
                    "result_count", "duration_ms", "bounded request metadata",
                ],
            },
        }
    )


@app.post("/v1/query")
def run_query():
    user, groups, _ = _identity()
    _rate_limit(user)
    body = _body()
    capability = str(body.get("capability") or "")
    params = _parameters(capability, body.get("parameters", {}), "query")
    connector = _connector(capability)
    started = time.monotonic()
    metadata = _audit_metadata(capability, params)
    try:
        if connector == "gmail":
            data = query_live_mail(read_connector_state(), user, connector, params)
        elif connector == "outlook":
            data = query_live_outlook(
                read_connector_state(), user, capability, params
            )
        elif connector == "zoom":
            data = query_live_zoom(read_connector_state(), user, capability, params)
        elif connector == "github":
            data = query_live_github(read_connector_state(), capability, params)
        else:
            data = query(capability, params, user=user, groups=groups)
    except Exception as exc:
        record_connector_event(
            user=user,
            connector=connector,
            capability=capability,
            operation="query",
            status="failed",
            duration_ms=round((time.monotonic() - started) * 1000),
            metadata={**metadata, "http_status": _error_status(exc), "error_type": type(exc).__name__},
        )
        if isinstance(exc, LiveMailError):
            raise GatewayError(str(exc), exc.status) from exc
        if isinstance(exc, LiveZoomError):
            raise GatewayError(str(exc), exc.status) from exc
        if isinstance(exc, LiveGitHubError):
            raise GatewayError(str(exc), exc.status) from exc
        raise
    record_connector_event(
        user=user,
        connector=connector,
        capability=capability,
        operation="query",
        status="succeeded",
        result_count=len(data) if isinstance(data, list) else 1,
        duration_ms=round((time.monotonic() - started) * 1000),
        metadata=metadata,
    )
    return jsonify({"data": data, "capability": capability})


@app.post("/v1/actions")
def run_action():
    user, groups, _ = _identity()
    _rate_limit(user)
    body = _body()
    capability = str(body.get("capability") or "")
    params = _parameters(capability, body.get("parameters", {}), "action")
    if capability not in {"slack.messages.post", "slack.files.upload"}:
        raise GatewayError("Unknown action capability")
    connector = _connector(capability)
    metadata = _audit_metadata(capability, params)
    started = time.monotonic()
    try:
        settings = read_connector_settings()
        config = settings.get("slack") or {}
        if not config.get("bot_token"):
            raise GatewayError("Slack is not configured", 409)
        key = params["idempotency_key"]
        duplicate = reserve_action(user, capability, params["channel"], key)
        if duplicate is not None:
            record_connector_event(
                user=user, connector=connector, capability=capability,
                operation="action", status="duplicate",
                duration_ms=round((time.monotonic() - started) * 1000), metadata=metadata,
            )
            return jsonify(duplicate)
        channel = visible_slack_channel(user, groups, params["channel"])
        if channel is None:
            channel = _live_slack_channel(str(config["bot_token"]), params["channel"])
        if channel is None:
            raise GatewayError("Slack channel was not found", 404)
        if capability == "slack.files.upload":
            content = _slack_file_bytes(params["content_base64"])
            metadata["file_bytes"] = len(content)
            if params["dry_run"]:
                result = {
                    "ok": True, "duplicate": False, "dry_run": True,
                    "channel": channel.get("name"), "file_id": "",
                    "filename": params["filename"],
                }
            else:
                upload = _upload_slack_file(
                    str(config["bot_token"]), channel["id"], params, content
                )
                result = {
                    "ok": True, "duplicate": False, "dry_run": False,
                    "channel": channel.get("name"), **upload,
                }
        elif params["dry_run"]:
            result = {"ok": True, "duplicate": False, "dry_run": True, "channel": channel.get("name"), "timestamp": ""}
        else:
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                json={"channel": channel["id"], "text": params["text"]},
                headers={"Authorization": f"Bearer {config['bot_token']}", "Content-Type": "application/json"}, timeout=25,
            )
            data = _slack_json(response)
            result = {"ok": True, "duplicate": False, "dry_run": False, "channel": channel.get("name"), "timestamp": str(data.get("ts") or "")}
        complete_action(user, capability, key, "succeeded", result)
        record_connector_event(
            user=user, connector=connector, capability=capability,
            operation="action", status="succeeded", result_count=1,
            duration_ms=round((time.monotonic() - started) * 1000), metadata=metadata,
        )
        return jsonify(result)
    except Exception as exc:
        detail = {"ok": False, "duplicate": False, "error": str(exc)}
        if "key" in locals():
            complete_action(user, capability, key, "failed", detail)
        record_connector_event(
            user=user, connector=connector, capability=capability,
            operation="action", status="failed",
            duration_ms=round((time.monotonic() - started) * 1000),
            metadata={**metadata, "http_status": _error_status(exc), "error_type": type(exc).__name__},
        )
        raise


def main(port: int) -> None:
    app.run("0.0.0.0", port=int(port), debug=False)


if __name__ == "__main__":
    main(int(os.environ.get("PORT", "8080")))
