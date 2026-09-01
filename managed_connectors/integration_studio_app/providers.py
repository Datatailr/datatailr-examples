from __future__ import annotations

import base64
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlencode

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from integration_studio_app.imap_mail import (
    GmailImapError,
    fetch_gmail_messages,
    test_gmail_configuration,
)


TIMEOUT = 25
OUTLOOK_OAUTH_SCOPE = (
    "openid profile email offline_access User.Read Mail.Read Calendars.ReadBasic"
)
OUTLOOK_CALENDAR_SCOPES = {
    "calendars.readbasic",
    "calendars.read",
    "calendars.readwrite",
}
OUTLOOK_CALENDAR_TERMS = (
    "calendar",
    "meeting",
    "meetings",
    "schedule",
    "scheduled",
    "availability",
    "free/busy",
    "free busy",
    "appointment",
    "appointments",
    "agenda",
)
ZOOM_AI_LIST_SCOPES = {"meeting:read", "meeting:read:list_meetings"}
ZOOM_AI_SUMMARY_SCOPES = {"meeting_summary:read", "meeting:read:summary"}
ZOOM_TRANSCRIPT_SCOPES = {"recording:read", "cloud_recording:read:meeting_transcript"}
GITHUB_API_VERSION = "2022-11-28"


class ProviderError(RuntimeError):
    pass


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _github_app_jwt(cfg: dict[str, Any]) -> str:
    app_id = str(cfg.get("app_id") or "").strip()
    private_key = str(cfg.get("private_key") or "").strip().replace("\\n", "\n")
    if not app_id or not private_key:
        raise ProviderError("GitHub App ID and private key are required")
    now = int(time.time())
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64url(
        json.dumps({"iat": now - 60, "exp": now + 540, "iss": app_id}, separators=(",", ":")).encode()
    )
    signing_input = f"{header}.{payload}".encode()
    try:
        key = serialization.load_pem_private_key(private_key.encode(), password=None)
        signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    except (TypeError, ValueError) as exc:
        raise ProviderError("GitHub App private key is invalid") from exc
    return f"{header}.{payload}.{_b64url(signature)}"


def outlook_calendar_authorized(token: dict[str, Any] | None) -> bool:
    scopes = {
        value.casefold()
        for value in str((token or {}).get("scope") or "").replace(",", " ").split()
    }
    return bool(scopes & OUTLOOK_CALENDAR_SCOPES)


def outlook_calendar_prompt(prompt: str) -> bool:
    value = str(prompt or "").casefold()
    return any(term in value for term in OUTLOOK_CALENDAR_TERMS)


def zoom_ai_companion_authorized(token: dict[str, Any] | None) -> bool:
    scopes = {
        value.casefold()
        for value in str((token or {}).get("scope") or "").replace(",", " ").split()
    }
    return bool(scopes & ZOOM_AI_LIST_SCOPES) and bool(scopes & ZOOM_AI_SUMMARY_SCOPES)


def zoom_retained_transcript_authorized(token: dict[str, Any] | None) -> bool:
    scopes = {
        value.casefold()
        for value in str((token or {}).get("scope") or "").replace(",", " ").split()
    }
    return bool(scopes & ZOOM_TRANSCRIPT_SCOPES)


def _zoom_meeting_path_id(meeting_id: str) -> str:
    encoded = quote(str(meeting_id), safe="")
    if str(meeting_id).startswith("/") or "//" in str(meeting_id):
        encoded = quote(encoded, safe="")
    return encoded


def _zoom_vtt_text(value: str, limit: int = 6000) -> str:
    lines: list[str] = []
    previous = ""
    for raw in str(value or "").replace("\r\n", "\n").split("\n"):
        line = raw.strip().lstrip("\ufeff")
        if not line or line == "WEBVTT" or line.isdigit() or "-->" in line:
            continue
        if line == previous:
            continue
        lines.append(line)
        previous = line
        if sum(len(item) + 1 for item in lines) >= limit:
            break
    return "\n".join(lines)[:limit]


def _zoom_retained_transcript(
    headers: dict[str, str], meeting_id: str, *, limit: int = 6000
) -> str:
    metadata = requests.get(
        f"https://api.zoom.us/v2/meetings/{_zoom_meeting_path_id(meeting_id)}/transcript",
        headers=headers,
        timeout=TIMEOUT,
    )
    if metadata.status_code in {403, 404}:
        return ""
    data = _json(metadata)
    download_url = str(data.get("download_url") or "")
    if not download_url or data.get("can_download") is False:
        return ""
    transcript = requests.get(download_url, headers=headers, timeout=TIMEOUT)
    if transcript.status_code in {403, 404}:
        return ""
    if not transcript.ok:
        _json(transcript)
    return _zoom_vtt_text(transcript.text, limit)


def _json(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise ProviderError(f"Provider returned HTTP {response.status_code} with invalid JSON") from exc
    if not response.ok:
        message = data.get("message") or data.get("error_description") or data.get("error") or response.reason
        if isinstance(message, dict):
            message = message.get("message") or json.dumps(message)
        raise ProviderError(f"Provider returned HTTP {response.status_code}: {message}")
    return data


def _slack_json(response: requests.Response) -> dict[str, Any]:
    """Slack reports most API failures as HTTP 200 with ok=false."""
    data = _json(response)
    if not data.get("ok"):
        error = data.get("error") or "unknown_error"
        needed = data.get("needed")
        detail = f" (required scope: {needed})" if needed else ""
        raise ProviderError(f"Slack API error: {error}{detail}")
    return data


def test_connector(provider: str, settings: dict[str, Any]) -> dict[str, Any]:
    cfg = settings[provider]
    if provider == "slack":
        if not cfg.get("bot_token"):
            raise ProviderError("Slack bot token is not configured")
        data = _slack_json(
            requests.post(
                "https://slack.com/api/auth.test",
                headers={"Authorization": f"Bearer {cfg['bot_token']}"},
                timeout=TIMEOUT,
            )
        )
        return {"ok": True, "label": data.get("team") or data.get("url") or "Slack workspace"}
    if provider == "hubspot":
        if not cfg.get("access_token"):
            raise ProviderError("HubSpot private app token is not configured")
        url = cfg.get("base_url", "https://api.hubapi.com").rstrip("/") + "/crm/v3/objects/contacts"
        data = _json(
            requests.get(
                url,
                params={"limit": 1},
                headers={"Authorization": f"Bearer {cfg['access_token']}"},
                timeout=TIMEOUT,
            )
        )
        return {"ok": True, "label": f"HubSpot ({len(data.get('results', []))} sample contact)"}
    if provider == "github":
        installation_id = str(cfg.get("installation_id") or "").strip()
        if not installation_id:
            raise ProviderError("GitHub App installation ID is required")
        base_url = str(cfg.get("base_url") or "https://api.github.com").rstrip("/")
        common_headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
        }
        token_data = _json(
            requests.post(
                f"{base_url}/app/installations/{installation_id}/access_tokens",
                headers={**common_headers, "Authorization": f"Bearer {_github_app_jwt(cfg)}"},
                timeout=TIMEOUT,
            )
        )
        token = str(token_data.get("token") or "")
        if not token:
            raise ProviderError("GitHub did not return an installation token")
        repositories = _json(
            requests.get(
                f"{base_url}/installation/repositories",
                params={"per_page": 1},
                headers={**common_headers, "Authorization": f"Bearer {token}"},
                timeout=TIMEOUT,
            )
        )
        total = int(repositories.get("total_count") or len(repositories.get("repositories") or []))
        return {"ok": True, "label": f"GitHub App connected · {total} selected repositor{'y' if total == 1 else 'ies'}"}
    if provider == "gmail":
        try:
            test_gmail_configuration(cfg)
        except GmailImapError as exc:
            raise ProviderError(str(exc)) from exc
        return {"ok": True, "label": "Gmail connected through read-only IMAP"}
    if provider == "outlook":
        if not cfg.get("client_id") or not cfg.get("client_secret"):
            raise ProviderError("Outlook OAuth client ID and secret are required")
        return {"ok": True, "label": "Outlook OAuth configuration is present"}
    if provider == "zoom":
        if not cfg.get("client_id") or not cfg.get("client_secret"):
            raise ProviderError("Zoom OAuth client ID and secret are required")
        return {"ok": True, "label": "Zoom OAuth configuration is present"}
    raise ProviderError("Unknown provider")


def oauth_authorization_url(
    provider: str,
    cfg: dict[str, Any],
    state: str,
    redirect_uri: str,
) -> str:
    if provider == "outlook":
        tenant = cfg.get("tenant") or "common"
        params = {
            "client_id": cfg["client_id"],
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "response_mode": "query",
            "scope": OUTLOOK_OAUTH_SCOPE,
            "prompt": "select_account",
            "state": state,
        }
        return f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize?{urlencode(params)}"
    if provider == "zoom":
        params = {
            "response_type": "code",
            "client_id": cfg["client_id"],
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"https://zoom.us/oauth/authorize?{urlencode(params)}"
    raise ProviderError("Unknown OAuth provider")


def exchange_oauth_code(
    provider: str,
    cfg: dict[str, Any],
    code: str,
    redirect_uri: str,
) -> dict[str, Any]:
    if provider == "outlook":
        tenant = cfg.get("tenant") or "common"
        url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        payload = {
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
            "scope": OUTLOOK_OAUTH_SCOPE,
        }
        request_kwargs = {"data": payload}
    elif provider == "zoom":
        url = "https://zoom.us/oauth/token"
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
        request_kwargs = {
            "data": payload,
            "auth": (cfg["client_id"], cfg["client_secret"]),
        }
    else:
        raise ProviderError("Unknown OAuth provider")
    token = _json(requests.post(url, timeout=TIMEOUT, **request_kwargs))
    if token.get("expires_in") is not None:
        token["expires_at"] = int(time.time()) + int(token["expires_in"]) - 60
    else:
        token["expires_at"] = int(time.time()) + 3540
    return token


def _refresh_token(provider: str, cfg: dict[str, Any], token: dict[str, Any]) -> dict[str, Any]:
    expires_at = int(token.get("expires_at") or 0)
    if token.get("access_token") and (not expires_at or expires_at > time.time()):
        return token
    if not token.get("refresh_token"):
        raise ProviderError(f"Reconnect {provider.title()}")
    if provider == "outlook":
        tenant = cfg.get("tenant") or "common"
        url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        payload = {
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "refresh_token": token["refresh_token"],
            "grant_type": "refresh_token",
            "scope": OUTLOOK_OAUTH_SCOPE,
        }
        request_kwargs = {"data": payload}
    elif provider == "zoom":
        url = "https://zoom.us/oauth/token"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": token["refresh_token"],
        }
        request_kwargs = {
            "data": payload,
            "auth": (cfg["client_id"], cfg["client_secret"]),
        }
    else:
        raise ProviderError("Unknown OAuth provider")
    refreshed = _json(requests.post(url, timeout=TIMEOUT, **request_kwargs))
    refreshed.setdefault("refresh_token", token.get("refresh_token"))
    refreshed.setdefault("scope", token.get("scope", ""))
    if refreshed.get("expires_in") is not None:
        refreshed["expires_at"] = int(time.time()) + int(refreshed["expires_in"]) - 60
    return refreshed


def fetch_context(
    source: str,
    settings: dict[str, Any],
    user_token: dict[str, Any] | None,
    limit: int = 8,
    prompt: str = "",
) -> tuple[str, dict[str, Any] | None]:
    if source == "slack":
        cfg = settings["slack"]
        data = _slack_json(
            requests.get(
                "https://slack.com/api/users.conversations",
                params={"types": "public_channel", "exclude_archived": "true", "limit": 20},
                headers={"Authorization": f"Bearer {cfg['bot_token']}"},
                timeout=TIMEOUT,
            )
        )
        channels = data.get("channels", [])
        if not channels:
            raise ProviderError(
                "The Slack bot is not a member of any public channel. "
                "Invite the installed app to a channel with /invite @app, then try again."
            )
        lines: list[str] = []
        # New Slack apps can be limited to one conversations.history request per
        # minute. Read one joined channel per question rather than failing midway
        # through a multi-channel fan-out with `ratelimited`.
        for channel in channels[:1]:
            history = _slack_json(
                requests.get(
                    "https://slack.com/api/conversations.history",
                    params={"channel": channel["id"], "limit": 3},
                    headers={"Authorization": f"Bearer {cfg['bot_token']}"},
                    timeout=TIMEOUT,
                )
            )
            for message in history.get("messages", []):
                if message.get("text"):
                    lines.append(f"Slack #{channel.get('name')}: {message['text']}")
        return "\n".join(lines[:limit]), None
    if source == "hubspot":
        cfg = settings["hubspot"]
        base = cfg.get("base_url", "https://api.hubapi.com").rstrip("/")
        headers = {"Authorization": f"Bearer {cfg['access_token']}"}
        lines = []
        for object_type, properties in (
            ("contacts", "firstname,lastname,email,company"),
            ("deals", "dealname,amount,dealstage,closedate"),
        ):
            data = _json(
                requests.get(
                    f"{base}/crm/v3/objects/{object_type}",
                    params={"limit": 5, "properties": properties},
                    headers=headers,
                    timeout=TIMEOUT,
                )
            )
            for item in data.get("results", []):
                lines.append(f"HubSpot {object_type[:-1]}: {json.dumps(item.get('properties', {}))}")
        return "\n".join(lines[:limit]), None
    if source == "gmail":
        try:
            rows = fetch_gmail_messages(settings["gmail"], limit=limit)
        except GmailImapError as exc:
            raise ProviderError(str(exc)) from exc
        return "\n\n".join(
            f"Gmail: {row['title']} | {row['updated_at']}\n{row['text']}" for row in rows
        ), None
    if source == "outlook":
        if not user_token:
            raise ProviderError("Connect your Outlook account first")
        calendar_requested = outlook_calendar_prompt(prompt)
        if calendar_requested and not outlook_calendar_authorized(user_token):
            raise ProviderError(
                "Reconnect Outlook after your administrator adds Calendars.ReadBasic "
                "to authorize calendar access"
            )
        cfg = settings[source]
        token = _refresh_token(source, cfg, dict(user_token))
        headers = {"Authorization": f"Bearer {token['access_token']}"}
        if calendar_requested:
            start = datetime.now(UTC)
            end = start + timedelta(days=14)
            data = _json(
                requests.get(
                    "https://graph.microsoft.com/v1.0/me/calendarView",
                    params={
                        "startDateTime": start.isoformat(),
                        "endDateTime": end.isoformat(),
                        "$top": min(limit, 100),
                        "$select": (
                            "id,subject,start,end,isAllDay,location,organizer,attendees,"
                            "responseStatus,isOnlineMeeting,onlineMeeting,webLink"
                        ),
                        "$orderby": "start/dateTime",
                    },
                    headers={**headers, "Prefer": 'outlook.timezone="UTC"'},
                    timeout=TIMEOUT,
                )
            )
            lines = []
            for item in (data.get("value") or [])[:limit]:
                start_value = item.get("start") or {}
                end_value = item.get("end") or {}
                organizer = (
                    (item.get("organizer") or {}).get("emailAddress") or {}
                ).get("name") or ""
                location = (item.get("location") or {}).get("displayName") or ""
                attendees = [
                    str((entry.get("emailAddress") or {}).get("name") or "")
                    for entry in (item.get("attendees") or [])[:20]
                    if isinstance(entry, dict)
                ]
                lines.append(
                    "Outlook calendar: "
                    f"{item.get('subject') or '(untitled event)'} | "
                    f"{start_value.get('dateTime') or ''} to {end_value.get('dateTime') or ''} | "
                    f"Organizer: {organizer or '?'} | Location: {location or '—'} | "
                    f"Attendees: {', '.join(value for value in attendees if value) or '—'}"
                )
            return "\n".join(lines), token
        lines = []
        data = _json(
            requests.get(
                "https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages",
                params={"$top": limit, "$select": "subject,from,receivedDateTime,bodyPreview"},
                headers=headers,
                timeout=TIMEOUT,
            )
        )
        for item in data.get("value", []):
            sender = item.get("from", {}).get("emailAddress", {}).get("address", "?")
            lines.append(
                f"Outlook from {sender} | {item.get('subject', '(no subject)')} | "
                f"{item.get('bodyPreview', '')}"
            )
        return "\n".join(lines), token
    if source == "zoom":
        if not user_token:
            raise ProviderError("Connect your Zoom account first")
        cfg = settings[source]
        token = _refresh_token(source, cfg, dict(user_token))
        headers = {"Authorization": f"Bearer {token['access_token']}"}
        lines = []
        previous = requests.get(
            "https://api.zoom.us/v2/users/me/meetings",
            params={"type": "previous_meetings", "page_size": min(max(limit, 20), 100)},
            headers=headers,
            timeout=TIMEOUT,
        )
        # Tokens issued before the administrator adds AI Companion scopes keep
        # the older recording-based behavior until the user reconnects.
        meetings = [] if previous.status_code == 403 else (_json(previous).get("meetings") or [])
        for meeting in meetings[:limit]:
            if not isinstance(meeting, dict):
                continue
            meeting_id = str(meeting.get("uuid") or meeting.get("id") or "")
            if not meeting_id:
                continue
            summary_response = requests.get(
                f"https://api.zoom.us/v2/meetings/{_zoom_meeting_path_id(meeting_id)}/meeting_summary",
                headers=headers,
                timeout=TIMEOUT,
            )
            if summary_response.status_code == 404:
                continue
            summary = _json(summary_response)
            summary_text = str(
                summary.get("summary_content") or summary.get("summary_overview") or ""
            ).strip()
            if not summary_text:
                summary_text = "\n\n".join(
                    f"{str(item.get('label') or '').strip()}\n{str(item.get('summary') or '').strip()}".strip()
                    for item in summary.get("summary_details") or []
                    if isinstance(item, dict) and item.get("summary")
                )
            next_steps = [
                str(value).strip() for value in summary.get("next_steps") or [] if str(value).strip()
            ]
            transcript = _zoom_retained_transcript(headers, meeting_id)
            if not summary_text and not transcript:
                continue
            parts = [
                f"Zoom AI Companion meeting: "
                f"{summary.get('meeting_topic') or summary.get('summary_title') or meeting.get('topic') or '(untitled)'} | "
                f"{summary.get('meeting_start_time') or summary.get('summary_start_time') or meeting.get('start_time') or ''}"
            ]
            if summary_text:
                parts.append(f"Summary:\n{summary_text[:6000]}")
            if next_steps:
                parts.append("Next steps:\n" + "\n".join(f"- {value}" for value in next_steps[:20]))
            if transcript:
                parts.append(f"Retained transcript:\n{transcript}")
            lines.append("\n".join(parts))
            if len(lines) >= 4:
                break
        if lines:
            return "\n\n".join(lines), token

        # Backward-compatible fallback for accounts that still create cloud
        # recording transcripts or have not yet added AI Companion scopes.
        now = time.time()
        end = datetime.fromtimestamp(now, UTC).date()
        start = end - timedelta(days=30)
        data = _json(
            requests.get(
                "https://api.zoom.us/v2/users/me/recordings",
                params={"from": start.isoformat(), "to": end.isoformat(), "page_size": min(limit, 20)},
                headers=headers,
                timeout=TIMEOUT,
            )
        )
        lines = []
        for meeting in (data.get("meetings") or [])[:limit]:
            transcript_file = next(
                (
                    item for item in meeting.get("recording_files") or []
                    if str(item.get("file_type") or "").upper() == "TRANSCRIPT"
                    or item.get("recording_type") == "audio_transcript"
                ),
                None,
            )
            if not transcript_file or not transcript_file.get("download_url"):
                continue
            response = requests.get(
                transcript_file["download_url"], headers=headers, timeout=TIMEOUT
            )
            if not response.ok:
                continue
            transcript = _zoom_vtt_text(response.text)
            if transcript:
                lines.append(
                    f"Zoom meeting: {meeting.get('topic') or '(untitled)'} | "
                    f"{meeting.get('start_time') or ''}\n{transcript}"
                )
            if len(lines) >= 4:
                break
        return "\n\n".join(lines), token
    raise ProviderError("Unknown source")


def revoke_oauth_token(provider: str, cfg: dict[str, Any], token: dict[str, Any]) -> None:
    """Best-effort upstream revocation for providers that expose it."""
    access_token = str(token.get("access_token") or "")
    if not access_token:
        return
    if provider != "zoom":
        return
    response = requests.post(
        "https://zoom.us/oauth/revoke",
        data={"token": access_token},
        auth=(cfg["client_id"], cfg["client_secret"]),
        timeout=TIMEOUT,
    )
    if not response.ok:
        raise ProviderError(f"Zoom token revocation failed (HTTP {response.status_code})")


