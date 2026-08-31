from __future__ import annotations

from connector_gateway_service import imap_mail, live_mail
from connector_gateway_service.state import personal_connector
import pytest


class FakeResponse:
    def __init__(self, data, *, status_code=200, reason="OK"):
        self._data = data
        self.status_code = status_code
        self.reason = reason
        self.ok = 200 <= status_code < 300

    def json(self):
        return self._data


class FakeImap:
    def __init__(self):
        self.readonly = False
        self.fetch_query = ""

    def login(self, username, password):
        assert username == "alice@gmail.test"
        assert password == "abcdefghijklmnop"

    def select(self, folder, readonly=False):
        self.readonly = readonly
        return "OK", [b"1"]

    def uid(self, command, *args):
        if command == "search":
            return "OK", [b"7"]
        self.fetch_query = args[-1]
        raw = (
            b"From: sender@example.test\r\nTo: alice@gmail.test\r\n"
            b"Subject: Runtime only\r\nDate: Tue, 5 Aug 2026 10:00:00 +0000\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n\r\nNever persist me"
        )
        return "OK", [(b"7 (UID 7 BODY[] {1}", raw), b")"]

    def logout(self):
        return "BYE", []


def test_gateway_gmail_query_uses_read_only_imap_without_oauth_token(monkeypatch) -> None:
    fake = FakeImap()
    monkeypatch.setattr(imap_mail.imaplib, "IMAP4_SSL", lambda *args, **kwargs: fake)
    state = {
        "users": {
            "alice": {
                "connector_settings": {
                    "gmail": {
                        "username": "alice@gmail.test",
                        "app_password": "abcdefghijklmnop",
                    }
                }
            }
        }
    }

    rows = live_mail.query_live_mail(state, "alice", "gmail", {"limit": 1})

    assert fake.readonly is True
    assert fake.fetch_query == "(UID BODY.PEEK[])"
    assert rows[0]["text"].endswith("Never persist me")
    assert rows[0]["ref"] == "gmail://imap/INBOX/7"


def test_outlook_uses_workspace_application_and_users_own_delegated_token() -> None:
    state = {
        "settings": {
            "outlook": {
                "tenant": "tenant-id",
                "client_id": "workspace-client",
                "client_secret": "workspace-secret",
            }
        },
        "users": {
            "alice": {
                "connector_settings": {
                    "outlook": {"client_id": "legacy-user-client", "client_secret": "legacy"}
                },
                "tokens": {"outlook": {"refresh_token": "alice-refresh"}},
            },
            "bob": {"tokens": {"outlook": {"refresh_token": "bob-refresh"}}},
        },
    }

    settings, alice_token = personal_connector(state, "alice", "outlook")
    _, bob_token = personal_connector(state, "bob", "outlook")

    assert settings["client_id"] == "workspace-client"
    assert alice_token == {"refresh_token": "alice-refresh"}
    assert bob_token == {"refresh_token": "bob-refresh"}


def outlook_state(token=None):
    return {
        "settings": {
            "outlook": {
                "tenant": "tenant-id",
                "client_id": "workspace-client",
                "client_secret": "workspace-secret",
            }
        },
        "users": {
            "alice": {
                "tokens": {
                    "outlook": token
                    or {
                        "access_token": "access",
                        "refresh_token": "refresh",
                        "expires_at": 9_999_999_999,
                        "scope": "Mail.Read Calendars.ReadBasic",
                    }
                }
            }
        },
    }


def test_outlook_calendar_events_are_fetched_live_and_normalized(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        live_mail.requests,
        "get",
        lambda *args, **kwargs: calls.append((args, kwargs))
        or FakeResponse(
            {
                "value": [
                    {
                        "id": "event-1",
                        "subject": "Product review",
                        "start": {"dateTime": "2026-08-07T10:00:00", "timeZone": "UTC"},
                        "end": {"dateTime": "2026-08-07T10:30:00", "timeZone": "UTC"},
                        "isAllDay": False,
                        "location": {"displayName": "Room 1"},
                        "organizer": {"emailAddress": {"name": "Alice", "address": "alice@example.test"}},
                        "attendees": [
                            {
                                "emailAddress": {"name": "Bob", "address": "bob@example.test"},
                                "type": "required",
                                "status": {"response": "accepted"},
                            }
                        ],
                        "responseStatus": {"response": "accepted"},
                        "onlineMeeting": {"joinUrl": "https://teams.example.test/meeting"},
                        "webLink": "https://outlook.example.test/event-1",
                    }
                ]
            }
        ),
    )

    rows = live_mail.query_live_outlook(
        outlook_state(),
        "alice",
        "outlook.calendar.events.range",
        {
            "start": "2026-08-07T00:00:00Z",
            "end": "2026-08-08T00:00:00Z",
            "time_zone": "UTC",
            "limit": 10,
        },
    )

    assert rows[0]["title"] == "Product review"
    assert rows[0]["organizer"] == {"name": "Alice", "address": "alice@example.test"}
    assert rows[0]["attendees"][0]["address"] == "bob@example.test"
    assert rows[0]["online_meeting_url"] == "https://teams.example.test/meeting"
    assert calls[0][0][0] == "https://graph.microsoft.com/v1.0/me/calendarView"
    assert "body" not in calls[0][1]["params"]["$select"].casefold()


def test_outlook_refresh_requests_calendar_scope_and_persists_rotated_token(monkeypatch) -> None:
    posted = []
    persisted = []
    monkeypatch.setattr(
        live_mail.requests,
        "post",
        lambda *args, **kwargs: posted.append((args, kwargs))
        or FakeResponse(
            {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 3600,
                "scope": "Mail.Read Calendars.ReadBasic",
            }
        ),
    )
    monkeypatch.setattr(
        live_mail.requests,
        "get",
        lambda *args, **kwargs: FakeResponse({"value": []}),
    )
    monkeypatch.setattr(
        live_mail,
        "update_personal_token",
        lambda user, provider, token: persisted.append((user, provider, token)),
    )

    rows = live_mail.query_live_outlook(
        outlook_state(
            {
                "access_token": "expired",
                "refresh_token": "old-refresh",
                "expires_at": 1,
                "scope": "Mail.Read Calendars.ReadBasic",
            }
        ),
        "alice",
        "outlook.calendar.events.upcoming",
        {"days": 7, "limit": 10, "time_zone": "UTC"},
    )

    assert rows == []
    assert "Calendars.ReadBasic" in posted[0][1]["data"]["scope"]
    assert persisted[0][0:2] == ("alice", "outlook")
    assert persisted[0][2]["refresh_token"] == "new-refresh"


def test_outlook_calendar_requires_reauthorization_before_provider_call(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        live_mail.requests,
        "get",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(live_mail.LiveMailError) as error:
        live_mail.query_live_outlook(
            outlook_state(
                {
                    "access_token": "mail-only",
                    "refresh_token": "refresh",
                    "expires_at": 9_999_999_999,
                    "scope": "Mail.Read",
                }
            ),
            "alice",
            "outlook.calendar.events.upcoming",
            {"days": 7, "limit": 10, "time_zone": "UTC"},
        )

    assert error.value.status == 409
    assert "reconnect Outlook" in str(error.value)
    assert calls == []


def test_outlook_calendar_permission_error_requires_reauthorization(monkeypatch) -> None:
    monkeypatch.setattr(
        live_mail.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(
            {"error": {"message": "Insufficient privileges"}}, status_code=403, reason="Forbidden"
        ),
    )

    with pytest.raises(live_mail.LiveMailError) as error:
        live_mail.query_live_outlook(
            outlook_state(),
            "alice",
            "outlook.calendar.events.upcoming",
            {"days": 7, "limit": 10, "time_zone": "UTC"},
        )

    assert error.value.status == 409
    assert "Calendars.ReadBasic" in str(error.value)
