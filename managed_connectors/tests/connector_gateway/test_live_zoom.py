from __future__ import annotations

import time
from datetime import UTC, datetime

from connector_gateway_service import live_zoom


class Response:
    def __init__(self, payload=None, text="", status_code=200):
        self.payload = payload or {}
        self.text = text
        self.status_code = status_code
        self.ok = status_code < 400
        self.reason = "OK" if self.ok else "Failed"

    def json(self):
        return self.payload


def state(token=None):
    return {
        "settings": {"zoom": {"client_id": "client", "client_secret": "secret"}},
        "users": {"alice": {"tokens": {"zoom": token or {
            "access_token": "access", "refresh_token": "refresh", "expires_at": time.time() + 600,
        }}}},
    }


def test_recent_recordings_are_live_and_report_transcript_availability(monkeypatch):
    monkeypatch.setattr(
        live_zoom.requests,
        "get",
        lambda url, **kwargs: Response({"meetings": [{
            "uuid": "meeting-1", "topic": "Weekly planning", "start_time": "2026-08-05T10:00:00Z",
            "duration": 42, "recording_files": [{"file_type": "TRANSCRIPT", "download_url": "https://zoom.us/transcript"}],
        }]}),
    )

    rows = live_zoom.query_live_zoom(state(), "alice", "zoom.recordings.recent", {"days": 7, "limit": 5})

    assert rows == [{
        "id": "meeting-1", "meeting_id": "meeting-1", "topic": "Weekly planning",
        "start_time": "2026-08-05T10:00:00Z", "duration": 42,
        "transcript_available": True, "ref": "zoom://meeting/meeting-1",
    }]


def test_transcript_search_downloads_and_cleans_vtt_without_persisting(monkeypatch):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        if url.endswith("/recordings"):
            return Response({"meetings": [{
                "uuid": "meeting-1", "topic": "Launch review", "start_time": "2026-08-05T10:00:00Z",
                "recording_files": [{"file_type": "TRANSCRIPT", "download_url": "https://zoom.us/rec/download/transcript"}],
            }]})
        return Response(text="WEBVTT\n\n1\n00:00:01.000 --> 00:00:02.000\nAlice: launch risk is staffing\n")

    monkeypatch.setattr(live_zoom.requests, "get", get)
    rows = live_zoom.query_live_zoom(
        state(), "alice", "zoom.transcripts.search",
        {"query": "staffing", "days": 30, "limit": 2, "max_characters": 12000},
    )

    assert rows[0]["text"] == "Alice: launch risk is staffing"
    assert "00:00" not in rows[0]["text"]
    assert calls == [
        "https://api.zoom.us/v2/users/me/meetings",
        "https://api.zoom.us/v2/users/me/recordings",
        "https://zoom.us/rec/download/transcript",
    ]


def test_ai_companion_reads_summary_and_retained_transcript_without_recordings(monkeypatch):
    calls = []
    recent_start = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def get(url, **kwargs):
        calls.append(url)
        if url.endswith("/users/me/meetings"):
            assert kwargs["params"]["type"] == "previous_meetings"
            return Response({"meetings": [{
                "uuid": "meeting-1", "topic": "Launch review",
                "start_time": recent_start,
            }]})
        if url.endswith("/meeting_summary"):
            return Response({
                "meeting_uuid": "meeting-1",
                "meeting_topic": "Launch review",
                "meeting_start_time": recent_start,
                "summary_content": "The team approved the launch plan.",
                "next_steps": ["Alice will publish the checklist."],
            })
        if url.endswith("/transcript"):
            return Response({
                "can_download": True,
                "download_url": "https://zoom.us/rec/download/retained-transcript",
            })
        return Response(text="WEBVTT\n\n1\n00:00:01.000 --> 00:00:02.000\nAlice: publish Friday\n")

    monkeypatch.setattr(live_zoom.requests, "get", get)

    rows = live_zoom.query_live_zoom(
        state(), "alice", "zoom.ai_companion.recent",
        {"query": "launch", "days": 30, "limit": 5, "max_characters": 12000},
    )

    assert rows == [{
        "meeting_id": "meeting-1",
        "topic": "Launch review",
        "start_time": recent_start,
        "summary": "The team approved the launch plan.",
        "next_steps": ["Alice will publish the checklist."],
        "transcript_text": "Alice: publish Friday",
        "transcript_available": True,
        "ref": "zoom://meeting/meeting-1/ai-companion",
    }]
    assert all("/recordings" not in url for url in calls)


def test_ai_companion_summary_remains_available_when_transcript_is_not_retained(monkeypatch):
    recent_start = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def get(url, **kwargs):
        if url.endswith("/users/me/meetings"):
            return Response({"meetings": [{
                "uuid": "meeting-2", "topic": "Private review",
                "start_time": recent_start,
            }]})
        if url.endswith("/meeting_summary"):
            return Response({"summary_content": "Summary only"})
        return Response({"message": "Transcript is not retained"}, status_code=404)

    monkeypatch.setattr(live_zoom.requests, "get", get)

    rows = live_zoom.query_live_zoom(
        state(), "alice", "zoom.ai_companion.recent",
        {"days": 30, "limit": 1, "max_characters": 12000},
    )

    assert rows[0]["summary"] == "Summary only"
    assert rows[0]["transcript_available"] is False
    assert rows[0]["transcript_text"] == ""


def test_ai_companion_scope_detection_accepts_granular_or_legacy_scopes():
    assert live_zoom.zoom_ai_companion_authorized({
        "scope": "meeting:read:list_meetings meeting:read:summary cloud_recording:read:meeting_transcript"
    })
    assert live_zoom.zoom_ai_companion_authorized({
        "scope": "meeting:read meeting_summary:read recording:read"
    })
    assert not live_zoom.zoom_ai_companion_authorized({
        "scope": "cloud_recording:read:list_user_recordings cloud_recording:read:meeting_transcript"
    })
    assert live_zoom.zoom_retained_transcript_authorized({
        "scope": "cloud_recording:read:meeting_transcript"
    })
    assert not live_zoom.zoom_retained_transcript_authorized({
        "scope": "meeting:read:list_meetings meeting:read:summary"
    })


def test_expired_token_refreshes_and_persists_the_rotated_token(monkeypatch):
    saved = []
    monkeypatch.setattr(
        live_zoom.requests,
        "post",
        lambda *args, **kwargs: Response({
            "access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600,
        }),
    )
    monkeypatch.setattr(live_zoom, "update_personal_token", lambda user, provider, token: saved.append((user, provider, token)))
    monkeypatch.setattr(live_zoom.requests, "get", lambda *args, **kwargs: Response({"meetings": []}))

    live_zoom.query_live_zoom(
        state({"access_token": "old", "refresh_token": "old-refresh", "expires_at": 0}),
        "alice", "zoom.recordings.recent", {"days": 1, "limit": 1},
    )

    assert saved[0][0:2] == ("alice", "zoom")
    assert saved[0][2]["access_token"] == "new-access"
    assert saved[0][2]["refresh_token"] == "new-refresh"
