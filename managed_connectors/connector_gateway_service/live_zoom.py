from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlparse

import requests

from .state import personal_connector, update_personal_token


TIMEOUT = 30
_VTT_TIMESTAMP = re.compile(
    r"^(?:\d{2}:)?\d{2}:\d{2}\.\d{3}\s+-->\s+(?:\d{2}:)?\d{2}:\d{2}\.\d{3}"
)


class LiveZoomError(RuntimeError):
    def __init__(self, message: str, status: int):
        super().__init__(message)
        self.status = status


def _json(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as exc:
        raise LiveZoomError(
            f"Zoom returned invalid JSON (HTTP {response.status_code})", 502
        ) from exc
    if not response.ok:
        detail = data.get("reason") or data.get("message") or data.get("error") or response.reason
        if response.status_code in {401, 403}:
            raise LiveZoomError(f"Zoom access was denied: {detail}", 403)
        if response.status_code == 404:
            raise LiveZoomError(f"Zoom recording or transcript was not found: {detail}", 404)
        if response.status_code == 429:
            raise LiveZoomError("Zoom rate limit exceeded; try again shortly", 429)
        raise LiveZoomError(f"Zoom request failed: {detail}", 502)
    return data if isinstance(data, dict) else {}


def _access_token(
    state: dict[str, Any], user: str
) -> tuple[dict[str, Any], dict[str, Any], str]:
    settings, token = personal_connector(state, user, "zoom")
    if not settings.get("client_id") or not settings.get("client_secret") or not token:
        raise LiveZoomError("Zoom is not connected for this user", 409)
    if token.get("access_token") and float(token.get("expires_at") or 0) > time.time():
        return settings, token, str(token["access_token"])
    refresh_token = str(token.get("refresh_token") or "")
    if not refresh_token:
        raise LiveZoomError("Zoom authorization has expired; reconnect it in Integration Studio", 409)
    refreshed = _json(
        requests.post(
            "https://zoom.us/oauth/token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            auth=(settings["client_id"], settings["client_secret"]),
            timeout=TIMEOUT,
        )
    )
    if not refreshed.get("access_token"):
        raise LiveZoomError("Zoom did not return an access token", 502)
    refreshed.setdefault("refresh_token", refresh_token)
    refreshed["expires_at"] = int(time.time()) + int(refreshed.get("expires_in") or 3600) - 60
    update_personal_token(user, "zoom", refreshed)
    return settings, refreshed, str(refreshed["access_token"])


def _headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def _meeting_path_id(meeting_id: str) -> str:
    encoded = quote(str(meeting_id), safe="")
    if str(meeting_id).startswith("/") or "//" in str(meeting_id):
        encoded = quote(encoded, safe="")
    return encoded


def zoom_ai_companion_authorized(token: dict[str, Any] | None) -> bool:
    """Report whether a token includes the per-user AI Companion read scopes."""
    scopes = {
        value.casefold()
        for value in str((token or {}).get("scope") or "").replace(",", " ").split()
    }
    can_list = bool(scopes & {"meeting:read", "meeting:read:list_meetings"})
    can_read_summary = bool(scopes & {"meeting_summary:read", "meeting:read:summary"})
    return can_list and can_read_summary


def zoom_retained_transcript_authorized(token: dict[str, Any] | None) -> bool:
    scopes = {
        value.casefold()
        for value in str((token or {}).get("scope") or "").replace(",", " ").split()
    }
    return bool(scopes & {"recording:read", "cloud_recording:read:meeting_transcript"})


def _safe_download(url: str, access_token: str, max_characters: int) -> str:
    parsed = urlparse(str(url))
    hostname = (parsed.hostname or "").lower()
    allowed = hostname == "zoom.us" or hostname.endswith(".zoom.us")
    allowed = allowed or hostname == "zoomgov.com" or hostname.endswith(".zoomgov.com")
    if parsed.scheme != "https" or not allowed:
        raise LiveZoomError("Zoom returned an unexpected transcript download URL", 502)
    response = requests.get(url, headers=_headers(access_token), timeout=TIMEOUT)
    if not response.ok:
        if response.status_code in {401, 403}:
            raise LiveZoomError("Zoom denied transcript download access", 403)
        raise LiveZoomError(
            f"Zoom transcript download failed (HTTP {response.status_code})", 502
        )
    return _clean_vtt(response.text, max_characters)


def _clean_vtt(value: str, max_characters: int) -> str:
    lines: list[str] = []
    previous = ""
    for raw in str(value or "").replace("\r\n", "\n").split("\n"):
        line = raw.strip().lstrip("\ufeff")
        if not line or line == "WEBVTT" or line.isdigit() or _VTT_TIMESTAMP.match(line):
            continue
        if line == previous:
            continue
        lines.append(line)
        previous = line
        if sum(len(item) + 1 for item in lines) >= max_characters:
            break
    return "\n".join(lines)[:max_characters]


def _transcript_file(meeting: dict[str, Any]) -> dict[str, Any] | None:
    for item in meeting.get("recording_files") or []:
        if not isinstance(item, dict):
            continue
        if (
            str(item.get("file_type") or "").upper() == "TRANSCRIPT"
            or str(item.get("recording_type") or "").lower() == "audio_transcript"
        ):
            return item
    return None


def _recent_recordings(access_token: str, days: int, limit: int) -> list[dict[str, Any]]:
    end = datetime.now(UTC).date()
    start = end - timedelta(days=days)
    data = _json(
        requests.get(
            "https://api.zoom.us/v2/users/me/recordings",
            params={
                "from": start.isoformat(),
                "to": end.isoformat(),
                "page_size": min(max(limit, 1), 100),
            },
            headers=_headers(access_token),
            timeout=TIMEOUT,
        )
    )
    meetings = [item for item in data.get("meetings") or [] if isinstance(item, dict)]
    meetings.sort(key=lambda item: str(item.get("start_time") or ""), reverse=True)
    return meetings[:limit]


def _recent_user_meetings(access_token: str, days: int, limit: int) -> list[dict[str, Any]]:
    """Discover prior hosted meetings without requiring a cloud recording."""
    cutoff = datetime.now(UTC) - timedelta(days=days)
    meetings: list[dict[str, Any]] = []
    next_page_token = ""
    page_size = min(max(limit * 3, 20), 100)
    for _page in range(3):
        params = {"type": "previous_meetings", "page_size": page_size}
        if next_page_token:
            params["next_page_token"] = next_page_token
        data = _json(
            requests.get(
                "https://api.zoom.us/v2/users/me/meetings",
                params=params,
                headers=_headers(access_token),
                timeout=TIMEOUT,
            )
        )
        for item in data.get("meetings") or []:
            if not isinstance(item, dict):
                continue
            start_time = str(item.get("start_time") or "")
            if start_time:
                try:
                    parsed = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                except ValueError:
                    parsed = None
                if parsed is not None and parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                if parsed is not None and parsed < cutoff:
                    continue
            meetings.append(item)
        next_page_token = str(data.get("next_page_token") or "")
        if not next_page_token or len(meetings) >= limit * 3:
            break
    meetings.sort(key=lambda item: str(item.get("start_time") or ""), reverse=True)
    return meetings[:limit]


def _recording_row(meeting: dict[str, Any]) -> dict[str, Any]:
    meeting_id = str(meeting.get("uuid") or meeting.get("id") or "")
    transcript = _transcript_file(meeting)
    return {
        "id": meeting_id,
        "meeting_id": meeting_id,
        "topic": str(meeting.get("topic") or "(untitled meeting)"),
        "start_time": str(meeting.get("start_time") or ""),
        "duration": int(meeting.get("duration") or 0),
        "transcript_available": bool(transcript and transcript.get("download_url")),
        "ref": f"zoom://meeting/{quote(meeting_id, safe='')}",
    }


def _get_transcript(access_token: str, meeting_id: str, max_characters: int) -> dict[str, Any]:
    data = _json(
        requests.get(
            f"https://api.zoom.us/v2/meetings/{_meeting_path_id(meeting_id)}/transcript",
            headers=_headers(access_token),
            timeout=TIMEOUT,
        )
    )
    download_url = str(data.get("download_url") or "")
    if not download_url or data.get("can_download") is False:
        raise LiveZoomError("Zoom transcript is not available for download", 404)
    text = _safe_download(download_url, access_token, max_characters)
    return {
        "meeting_id": meeting_id,
        "topic": str(data.get("topic") or data.get("meeting_topic") or ""),
        "start_time": str(data.get("start_time") or ""),
        "text": text,
        "ref": f"zoom://meeting/{quote(meeting_id, safe='')}/transcript",
    }


def _get_ai_summary(
    access_token: str,
    meeting_id: str,
    *,
    fallback_topic: str = "",
    fallback_start_time: str = "",
) -> dict[str, Any]:
    data = _json(
        requests.get(
            f"https://api.zoom.us/v2/meetings/{_meeting_path_id(meeting_id)}/meeting_summary",
            headers=_headers(access_token),
            timeout=TIMEOUT,
        )
    )
    summary = str(data.get("summary_content") or data.get("summary_overview") or "").strip()
    if not summary:
        details = data.get("summary_details") or []
        summary = "\n\n".join(
            f"{str(item.get('label') or '').strip()}\n{str(item.get('summary') or '').strip()}".strip()
            for item in details
            if isinstance(item, dict) and item.get("summary")
        )
    return {
        "meeting_id": str(data.get("meeting_uuid") or meeting_id),
        "topic": str(data.get("meeting_topic") or data.get("summary_title") or fallback_topic or "(untitled meeting)"),
        "start_time": str(data.get("meeting_start_time") or data.get("summary_start_time") or fallback_start_time),
        "summary": summary,
        "next_steps": [str(value) for value in data.get("next_steps") or [] if str(value).strip()],
    }


def _ai_companion_rows(
    access_token: str,
    *,
    query: str,
    days: int,
    limit: int,
    max_characters: int,
) -> list[dict[str, Any]]:
    meetings = _recent_user_meetings(
        access_token,
        days,
        min(max(limit * 3, 10), 50),
    )
    rows: list[dict[str, Any]] = []
    for meeting in meetings:
        meeting_id = str(meeting.get("uuid") or meeting.get("id") or "")
        if not meeting_id:
            continue
        try:
            summary = _get_ai_summary(
                access_token,
                meeting_id,
                fallback_topic=str(meeting.get("topic") or ""),
                fallback_start_time=str(meeting.get("start_time") or ""),
            )
        except LiveZoomError as exc:
            if exc.status == 404:
                continue
            raise

        transcript_text = ""
        try:
            transcript_text = str(
                _get_transcript(access_token, meeting_id, max_characters).get("text") or ""
            )
        except LiveZoomError as exc:
            # A summary is independently useful. Zoom accounts can allow AI
            # summaries while withholding retained transcript downloads.
            if exc.status not in {403, 404}:
                raise

        row = {
            **summary,
            "summary": str(summary["summary"])[:max_characters],
            "transcript_text": transcript_text,
            "transcript_available": bool(transcript_text),
            "ref": f"zoom://meeting/{quote(meeting_id, safe='')}/ai-companion",
        }
        haystack = "\n".join(
            [
                str(row["topic"]),
                str(row["summary"]),
                "\n".join(row["next_steps"]),
                transcript_text,
            ]
        ).casefold()
        if query and query not in haystack:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def query_live_zoom(
    state: dict[str, Any], user: str, capability: str, parameters: dict[str, Any]
) -> list[dict[str, Any]] | dict[str, Any]:
    """Fetch Zoom metadata/transcripts into request memory only; no content writes."""
    _settings, _token, access_token = _access_token(state, user)
    if capability == "zoom.recordings.recent":
        meetings = _recent_recordings(
            access_token,
            int(parameters.get("days") or 30),
            int(parameters.get("limit") or 20),
        )
        return [_recording_row(item) for item in meetings]
    if capability == "zoom.ai_companion.recent":
        return _ai_companion_rows(
            access_token,
            query=str(parameters.get("query") or "").casefold(),
            days=int(parameters.get("days") or 30),
            limit=int(parameters.get("limit") or 10),
            max_characters=int(parameters.get("max_characters") or 12_000),
        )
    if capability == "zoom.transcripts.get":
        return _get_transcript(
            access_token,
            str(parameters["meeting_id"]),
            int(parameters.get("max_characters") or 20_000),
        )
    if capability == "zoom.transcripts.search":
        query = str(parameters.get("query") or "").casefold()
        limit = int(parameters.get("limit") or 10)
        maximum = int(parameters.get("max_characters") or 12_000)
        rows: list[dict[str, Any]] = []
        try:
            meetings = _recent_user_meetings(
                access_token,
                int(parameters.get("days") or 30),
                min(max(limit * 3, 10), 50),
            )
        except LiveZoomError as exc:
            # Preserve the original recording-based path for tokens issued
            # before the AI Companion meeting-list scope was added.
            if exc.status != 403:
                raise
            meetings = []
        for meeting in meetings:
            meeting_id = str(meeting.get("uuid") or meeting.get("id") or "")
            if not meeting_id:
                continue
            try:
                transcript = _get_transcript(access_token, meeting_id, maximum)
            except LiveZoomError as exc:
                if exc.status == 404:
                    continue
                raise
            topic = str(transcript.get("topic") or meeting.get("topic") or "(untitled meeting)")
            text = str(transcript.get("text") or "")
            if query and query not in f"{topic}\n{text}".casefold():
                continue
            rows.append(
                {
                    **transcript,
                    "topic": topic,
                    "start_time": str(transcript.get("start_time") or meeting.get("start_time") or ""),
                }
            )
            if len(rows) >= limit:
                return rows

        recordings = _recent_recordings(
            access_token,
            int(parameters.get("days") or 30),
            min(max(limit * 3, 10), 50),
        )
        for meeting in recordings:
            transcript_file = _transcript_file(meeting)
            if not transcript_file or not transcript_file.get("download_url"):
                continue
            text = _safe_download(str(transcript_file["download_url"]), access_token, maximum)
            topic = str(meeting.get("topic") or "(untitled meeting)")
            if query and query not in f"{topic}\n{text}".casefold():
                continue
            meeting_id = str(meeting.get("uuid") or meeting.get("id") or "")
            rows.append(
                {
                    "meeting_id": meeting_id,
                    "topic": topic,
                    "start_time": str(meeting.get("start_time") or ""),
                    "text": text,
                    "ref": f"zoom://meeting/{quote(meeting_id, safe='')}/transcript",
                }
            )
            if len(rows) >= limit:
                break
        return rows
    raise LiveZoomError("Unknown Zoom capability", 400)
