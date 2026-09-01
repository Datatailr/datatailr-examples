from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from .imap_mail import GmailImapError, fetch_gmail_messages
from .state import personal_connector, update_personal_token


TIMEOUT = 30
OUTLOOK_OAUTH_SCOPE = (
    "openid profile email offline_access User.Read Mail.Read Calendars.ReadBasic"
)
OUTLOOK_CALENDAR_SCOPES = {
    "calendars.readbasic",
    "calendars.read",
    "calendars.readwrite",
}


class LiveMailError(RuntimeError):
    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = status


def outlook_calendar_authorized(token: dict[str, Any] | None) -> bool:
    scopes = {
        value.casefold()
        for value in str((token or {}).get("scope") or "").replace(",", " ").split()
    }
    return bool(scopes & OUTLOOK_CALENDAR_SCOPES)


def _json(response: requests.Response, *, calendar: bool = False) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise LiveMailError(f"Mail provider returned invalid JSON (HTTP {response.status_code})", 502) from exc
    if not response.ok:
        detail = data.get("error_description") or data.get("message") or data.get("error") or response.reason
        if isinstance(detail, dict):
            detail = detail.get("message") or str(detail)
        if response.status_code == 401:
            raise LiveMailError(
                "Outlook authorization has expired; reconnect it in Integration Studio", 409
            )
        if response.status_code == 403 and calendar:
            raise LiveMailError(
                "Outlook Calendar permission is missing; ask an administrator to add "
                "Calendars.ReadBasic, then reauthorize Outlook in Integration Studio",
                409,
            )
        if response.status_code == 403:
            raise LiveMailError(f"Outlook access was denied: {detail}", 403)
        if response.status_code == 429:
            raise LiveMailError("Microsoft Graph rate limit exceeded; try again shortly", 429)
        raise LiveMailError(f"Mail provider request failed: {detail}", 502)
    return data


def _outlook_access_token(
    state: dict[str, Any], user: str
) -> tuple[dict[str, Any], dict[str, Any], str]:
    settings, token = personal_connector(state, user, "outlook")
    if not settings.get("client_id") or not settings.get("client_secret") or not token:
        raise LiveMailError("Outlook is not connected for this user", 409)
    if token.get("access_token") and float(token.get("expires_at") or 0) > time.time():
        return settings, token, str(token["access_token"])
    refresh = token.get("refresh_token")
    if not refresh:
        raise LiveMailError("Outlook authorization has expired; reconnect it in Integration Studio", 409)
    tenant = settings.get("tenant") or "common"
    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    payload = {
        "client_id": settings["client_id"], "client_secret": settings["client_secret"],
        "refresh_token": refresh, "grant_type": "refresh_token",
        "scope": OUTLOOK_OAUTH_SCOPE,
    }
    refreshed = _json(requests.post(url, data=payload, timeout=TIMEOUT))
    if not refreshed.get("access_token"):
        raise LiveMailError("Outlook did not return an access token", 502)
    refreshed.setdefault("refresh_token", refresh)
    refreshed.setdefault("scope", token.get("scope", ""))
    refreshed["expires_at"] = int(time.time()) + int(refreshed.get("expires_in") or 3600) - 60
    update_personal_token(user, "outlook", refreshed)
    return settings, refreshed, str(refreshed["access_token"])


def _address(value: Any) -> str:
    address = value.get("emailAddress", {}) if isinstance(value, dict) else {}
    return str(address.get("address") or address.get("name") or "")


def _outlook_messages(
    state: dict[str, Any], user: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    _settings, _token, access = _outlook_access_token(state, user)
    headers = {"Authorization": f"Bearer {access}", "Prefer": 'outlook.body-content-type="text"'}
    limit = int(parameters.get("limit") or 20)
    query = str(parameters.get("query") or "").casefold()
    data = _json(requests.get(
        "https://graph.microsoft.com/v1.0/me/messages",
        params={
            "$top": min(max(limit * 3, 20), 100),
            "$orderby": "receivedDateTime desc",
            "$select": "id,subject,from,toRecipients,receivedDateTime,bodyPreview,webLink",
        },
        headers=headers, timeout=TIMEOUT,
    ))
    result = []
    for item in data.get("value") or []:
        sender = _address(item.get("from"))
        recipients = ", ".join(_address(value) for value in item.get("toRecipients") or [])
        title = str(item.get("subject") or "(no subject)")
        preview = str(item.get("bodyPreview") or "")
        searchable = f"{sender}\n{recipients}\n{title}\n{preview}".casefold()
        if query and query not in searchable:
            continue
        message_id = str(item.get("id") or "")
        result.append({
            "id": message_id,
            "title": title,
            "text": f"From: {sender}\nTo: {recipients}\n\n{preview}"[:16000],
            "updated_at": str(item.get("receivedDateTime") or ""),
            "ref": str(item.get("webLink") or f"outlook://message/{message_id}"),
        })
        if len(result) >= limit:
            break
    return result


def _date_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise LiveMailError("Calendar datetimes must include a timezone offset", 400)
    return parsed


def _calendar_window(parameters: dict[str, Any]) -> tuple[str, str]:
    if "days" in parameters:
        start_value = datetime.now(UTC)
        end_value = start_value + timedelta(days=int(parameters.get("days") or 14))
        return start_value.isoformat(), end_value.isoformat()
    start = str(parameters.get("start") or "")
    end = str(parameters.get("end") or "")
    start_value = _date_time(start)
    end_value = _date_time(end)
    if end_value <= start_value:
        raise LiveMailError("Calendar end must be after start", 400)
    if end_value - start_value > timedelta(days=62):
        raise LiveMailError("Calendar ranges are limited to 62 days", 400)
    return start, end


def _time_zone(parameters: dict[str, Any]) -> str:
    value = str(parameters.get("time_zone") or "UTC").strip()
    if not value or len(value) > 100 or any(character in value for character in '\r\n"'):
        raise LiveMailError("time_zone is invalid", 400)
    return value


def _event_address(value: Any) -> dict[str, str]:
    address = value.get("emailAddress", {}) if isinstance(value, dict) else {}
    return {
        "name": str(address.get("name") or ""),
        "address": str(address.get("address") or ""),
    }


def _calendar_event(item: dict[str, Any]) -> dict[str, Any]:
    event_id = str(item.get("id") or "")
    location = item.get("location") if isinstance(item.get("location"), dict) else {}
    response = item.get("responseStatus") if isinstance(item.get("responseStatus"), dict) else {}
    online = item.get("onlineMeeting") if isinstance(item.get("onlineMeeting"), dict) else {}
    return {
        "id": event_id,
        "title": str(item.get("subject") or "(untitled event)"),
        "start": item.get("start") if isinstance(item.get("start"), dict) else {},
        "end": item.get("end") if isinstance(item.get("end"), dict) else {},
        "is_all_day": bool(item.get("isAllDay")),
        "location": str(location.get("displayName") or ""),
        "organizer": _event_address(item.get("organizer")),
        "attendees": [
            {
                **_event_address(attendee),
                "type": str(attendee.get("type") or "required"),
                "response": str((attendee.get("status") or {}).get("response") or "none"),
            }
            for attendee in (item.get("attendees") or [])[:50]
            if isinstance(attendee, dict)
        ],
        "response_status": str(response.get("response") or "none"),
        "online_meeting_url": str(online.get("joinUrl") or ""),
        "ref": str(item.get("webLink") or f"outlook://calendar/event/{event_id}"),
    }


def _outlook_calendar_events(
    state: dict[str, Any], user: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    _settings, _token, access = _outlook_access_token(state, user)
    start, end = _calendar_window(parameters)
    limit = int(parameters.get("limit") or 20)
    zone = _time_zone(parameters)
    data = _json(
        requests.get(
            "https://graph.microsoft.com/v1.0/me/calendarView",
            params={
                "startDateTime": start,
                "endDateTime": end,
                "$top": limit,
                "$select": (
                    "id,subject,start,end,isAllDay,location,organizer,attendees,"
                    "responseStatus,isOnlineMeeting,onlineMeeting,webLink"
                ),
                "$orderby": "start/dateTime",
            },
            headers={
                "Authorization": f"Bearer {access}",
                "Prefer": f'outlook.timezone="{zone}"',
            },
            timeout=TIMEOUT,
        ),
        calendar=True,
    )
    return [
        _calendar_event(item)
        for item in (data.get("value") or [])[:limit]
        if isinstance(item, dict)
    ]


def _outlook_availability(
    state: dict[str, Any], user: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    _settings, _token, access = _outlook_access_token(state, user)
    start, end = _calendar_window(parameters)
    zone = _time_zone(parameters)
    schedules = [str(value) for value in parameters.get("schedules") or []]
    data = _json(
        requests.post(
            "https://graph.microsoft.com/v1.0/me/calendar/getSchedule",
            json={
                "schedules": schedules,
                "startTime": {"dateTime": start, "timeZone": zone},
                "endTime": {"dateTime": end, "timeZone": zone},
                "availabilityViewInterval": int(parameters.get("interval_minutes") or 30),
            },
            headers={"Authorization": f"Bearer {access}", "Content-Type": "application/json"},
            timeout=TIMEOUT,
        ),
        calendar=True,
    )
    rows = []
    for item in data.get("value") or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "schedule_id": str(item.get("scheduleId") or ""),
                "availability_view": str(item.get("availabilityView") or ""),
                "schedule_items": [
                    {
                        "status": str(entry.get("status") or ""),
                        "start": entry.get("start") if isinstance(entry.get("start"), dict) else {},
                        "end": entry.get("end") if isinstance(entry.get("end"), dict) else {},
                        "is_private": bool(entry.get("isPrivate")),
                    }
                    for entry in (item.get("scheduleItems") or [])[:200]
                    if isinstance(entry, dict)
                ],
            }
        )
    return rows


def query_live_outlook(
    state: dict[str, Any], user: str, capability: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    """Fetch the signed-in user's Outlook data into request memory only."""
    if capability == "outlook.messages.recent":
        return _outlook_messages(state, user, parameters)
    _settings, token = personal_connector(state, user, "outlook")
    if not outlook_calendar_authorized(token):
        raise LiveMailError(
            "Outlook Calendar is not authorized; reconnect Outlook in Integration "
            "Studio after an administrator adds Calendars.ReadBasic",
            409,
        )
    if capability in {
        "outlook.calendar.events.range",
        "outlook.calendar.events.upcoming",
    }:
        return _outlook_calendar_events(state, user, parameters)
    if capability == "outlook.calendar.availability":
        return _outlook_availability(state, user, parameters)
    raise LiveMailError("Unknown Outlook capability", 400)


def query_live_mail(
    state: dict[str, Any], user: str, provider: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    """Fetch mail into request memory only; this function performs no writes."""
    if provider not in {"gmail", "outlook"}:
        raise LiveMailError("Unknown live mail provider", 400)
    settings, _token = personal_connector(state, user, provider)
    if provider == "gmail":
        try:
            return fetch_gmail_messages(
                settings,
                query=str(parameters.get("query") or ""),
                limit=int(parameters.get("limit") or 20),
            )
        except GmailImapError as exc:
            status = 409 if any(
                marker in str(exc) for marker in ("required", "login failed", "app password")
            ) else 502
            raise LiveMailError(str(exc), status) from exc
    return query_live_outlook(state, user, "outlook.messages.recent", parameters)
