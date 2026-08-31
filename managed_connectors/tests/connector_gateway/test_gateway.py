from __future__ import annotations

import base64
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from connector_gateway_service import app as gateway
from connector_gateway_service import store
from connector_gateway_service.app import GatewayError, _parameters


@pytest.fixture()
def databases(tmp_path, monkeypatch):
    knowledge = tmp_path / "knowledge.sqlite3"
    conn = sqlite3.connect(knowledge)
    conn.executescript("""
      CREATE TABLE documents(id TEXT PRIMARY KEY, source TEXT, title TEXT, content TEXT, ref TEXT, source_updated_at TEXT, metadata TEXT);
      CREATE TABLE document_acl(document_id TEXT, principal TEXT);
    """)
    now = datetime.now(UTC)
    upcoming_meeting = (now + timedelta(days=3)).isoformat()
    upcoming_task = (now + timedelta(days=2)).isoformat()
    recent_note = (now - timedelta(hours=2)).isoformat()
    conn.executemany(
        "INSERT INTO documents VALUES(?,?,?,?,?,?,?)",
        [
            ("a", "slack", "Slack #sonic-bug-report", "Bug A", "slack://C1/1", "2026-08-03T10:00:00Z", '{"channel":"sonic-bug-report","reply_count":2}'),
            ("b", "slack", "Slack #sonic-bug-report", "Bug B private", "slack://C1/2", "2026-08-03T11:00:00Z", '{"channel":"sonic-bug-report","reply_count":1}'),
            ("d", "hubspot", "Deal Delta", "dealname: Delta\ndealstage: closedwon\namount: 100", "hubspot://deals/d", "2026-08-03T12:00:00Z", '{"object_type":"deals","id":"d"}'),
            (
                "hm", "hubspot", "HubSpot Meeting · Product demo",
                "HubSpot object: meeting\nhs_meeting_title: Product demo\nhs_meeting_body: Show analytics",
                "hubspot://meetings/hm", now.isoformat(),
                json.dumps({
                    "object_type": "meetings", "activity_type": "meetings", "id": "hm",
                    "timestamp": upcoming_meeting, "start_time": upcoming_meeting,
                    "end_time": (now + timedelta(days=3, minutes=30)).isoformat(),
                    "status": "", "outcome": "SCHEDULED", "owner_id": "owner-1",
                    "associations": {"contacts": ["c1"], "companies": ["co1"], "deals": ["d1"], "tickets": []},
                    "properties": {"hs_meeting_title": "Product demo", "hs_meeting_outcome": "SCHEDULED"},
                }),
            ),
            (
                "ht", "hubspot", "HubSpot Task · Send proposal",
                "HubSpot object: task\nhs_task_subject: Send proposal",
                "hubspot://tasks/ht", now.isoformat(),
                json.dumps({
                    "object_type": "tasks", "activity_type": "tasks", "id": "ht",
                    "timestamp": upcoming_task, "start_time": upcoming_task, "end_time": "",
                    "status": "COMPLETED", "outcome": "", "owner_id": "owner-1",
                    "associations": {"contacts": ["c1"], "companies": [], "deals": ["d1"], "tickets": []},
                    "properties": {"hs_task_subject": "Send proposal", "hs_task_status": "COMPLETED"},
                }),
            ),
            (
                "hn", "hubspot", "HubSpot Note · Discovery notes",
                "HubSpot object: note\nhs_note_body: Discovery notes",
                "hubspot://notes/hn", recent_note,
                json.dumps({
                    "object_type": "notes", "activity_type": "notes", "id": "hn",
                    "timestamp": recent_note, "start_time": recent_note, "end_time": "",
                    "status": "", "outcome": "", "owner_id": "owner-2",
                    "associations": {"contacts": ["c2"], "companies": [], "deals": [], "tickets": []},
                    "properties": {"hs_note_body": "Discovery notes"},
                }),
            ),
            ("g", "gmail", "Gmail private", "Personal mail", "gmail://message/g", "2026-08-03T13:00:00Z", '{}'),
        ],
    )
    conn.executemany("INSERT INTO document_acl VALUES(?,?)", [("a", "group:dtusers"), ("b", "user:bob"), ("d", "group:dtusers"), ("g", "user:alice")])
    conn.commit(); conn.close()
    monkeypatch.setattr(store, "KNOWLEDGE_DB", knowledge)
    monkeypatch.setattr(store, "AUDIT_DB", tmp_path / "audit.sqlite3")


def test_slack_dashboard_query_is_shared_for_every_gateway_user(databases):
    alice = store.query("slack.threads.recent", {"channel": "sonic-bug-report", "limit": 20}, user="alice", groups=["dtusers"])
    bob = store.query("slack.threads.recent", {"channel": "sonic-bug-report", "limit": 20}, user="bob", groups=["dtusers"])
    outsider = store.query("slack.threads.recent", {"channel": "sonic-bug-report", "limit": 20}, user="eve", groups=[])
    assert [row["text"] for row in alice] == ["Bug B private", "Bug A"]
    assert [row["text"] for row in bob] == ["Bug B private", "Bug A"]
    assert [row["text"] for row in outsider] == ["Bug B private", "Bug A"]


def test_hubspot_summary_is_shared_for_every_gateway_user(databases):
    visible = store.query("hubspot.deals.summary", {}, user="alice", groups=["dtusers"])
    hidden = store.query("hubspot.deals.summary", {}, user="eve", groups=[])
    assert (visible["total_deals"], visible["total_amount"], visible["closed_won"]) == (1, 100, 1)
    assert (hidden["total_deals"], hidden["total_amount"], hidden["closed_won"]) == (1, 100, 1)


def test_hubspot_upcoming_activities_are_normalized_and_filterable(databases):
    rows = store.query(
        "hubspot.activities.upcoming",
        {
            "days": 14,
            "activity_types": ["meetings", "tasks"],
            "association_type": "deals",
            "association_id": "d1",
            "limit": 20,
        },
        user="alice",
        groups=["dtusers"],
    )

    assert [row["id"] for row in rows] == ["hm"]
    assert rows[0]["title"] == "HubSpot Meeting · Product demo"
    assert rows[0]["outcome"] == "SCHEDULED"
    assert rows[0]["associations"]["contacts"] == ["c1"]
    assert rows[0]["properties"]["hs_meeting_title"] == "Product demo"


def test_hubspot_recent_activities_searches_notes(databases):
    rows = store.query(
        "hubspot.activities.recent",
        {"activity_types": ["notes"], "query": "discovery", "limit": 20},
        user="alice",
        groups=[],
    )

    assert [row["id"] for row in rows] == ["hn"]
    assert rows[0]["owner_id"] == "owner-2"


def test_connections_advertises_hubspot_activity_features(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_state",
        lambda: {
            "settings": {"hubspot": {"access_token": "token"}},
            "users": {},
        },
    )

    response = gateway.app.test_client().get("/v1/connections")
    hubspot = next(item for item in response.json["connections"] if item["source"] == "hubspot")

    assert hubspot["features"] == {
        "crm_objects": True,
        "activities": True,
        "next_activity_dates": True,
    }


def test_connections_advertises_github_as_shared_live_data(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_state",
        lambda: {
            "settings": {
                "github": {
                    "app_id": "123",
                    "installation_id": "456",
                    "private_key": "pem",
                }
            },
            "users": {},
        },
    )

    response = gateway.app.test_client().get("/v1/connections")
    github = next(item for item in response.json["connections"] if item["source"] == "github")

    assert github["configured"] is True
    assert github["read_available"] is True
    assert github["available_documents"] == 0
    assert github["data_mode"] == "live"
    assert all(github["features"].values())


def test_github_query_is_live_and_does_not_store_result_content(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(gateway, "read_connector_state", lambda: {"settings": {"github": {}}})
    calls = []
    monkeypatch.setattr(
        gateway,
        "query_live_github",
        lambda state, capability, params: calls.append((capability, params)) or [{
            "id": "42",
            "repository": "acme/private",
            "title": "Secret issue title",
            "body": "Secret issue body",
        }],
    )

    response = gateway.app.test_client().post(
        "/v1/query",
        json={
            "capability": "github.issues.recent",
            "parameters": {"repository": "acme/private", "state": "open", "limit": 5},
        },
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert response.status_code == 200
    assert response.json["data"][0]["title"] == "Secret issue title"
    assert calls == [("github.issues.recent", {
        "repository": "acme/private", "state": "open", "limit": 5,
        "max_characters": 8000,
    })]
    [event] = store.list_connector_events()
    serialized = json.dumps(event)
    assert event["connector"] == "github"
    assert event["metadata_only"] is False
    assert event["metadata"]["data_mode"] == "live"
    assert event["metadata"]["repository"] == "acme/private"
    assert "Secret issue" not in serialized


def test_email_documents_are_never_read_from_the_persistent_index(databases):
    with pytest.raises(RuntimeError, match="live-only"):
        store.visible_documents("alice", [], "gmail")


def test_email_capability_uses_the_live_runtime_fetcher(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(gateway, "read_connector_state", lambda: {"users": {"alice": {}}})
    calls = []
    monkeypatch.setattr(
        gateway,
        "query_live_mail",
        lambda state, user, provider, params: calls.append((state, user, provider, params)) or [
            {"id": "live", "title": "Runtime", "text": "Not stored", "updated_at": "", "ref": "gmail://live"}
        ],
    )

    response = gateway.app.test_client().post(
        "/v1/query",
        json={"capability": "gmail.messages.recent", "parameters": {"limit": 1}},
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert response.status_code == 200
    assert response.json["data"][0]["text"] == "Not stored"
    assert calls[0][1:] == ("alice", "gmail", {"limit": 1})
    events = store.list_connector_events()
    assert len(events) == 1
    assert events[0]["connector"] == "gmail"
    assert events[0]["metadata_only"] is True
    assert events[0]["result_count"] == 1
    assert events[0]["metadata"] == {
        "data_mode": "live",
        "limit": 1,
        "query_supplied": False,
        "surface": "connector-gateway",
    }
    serialized = json.dumps(events)
    assert "Runtime" not in serialized
    assert "Not stored" not in serialized


def test_zoom_capability_uses_live_runtime_fetcher_and_metadata_only_audit(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(gateway, "read_connector_state", lambda: {"users": {"alice": {}}})
    calls = []
    monkeypatch.setattr(
        gateway,
        "query_live_zoom",
        lambda state, user, capability, params: calls.append((user, capability, params)) or [
            {"meeting_id": "live", "topic": "Private planning", "start_time": "", "text": "Never store this transcript", "ref": "zoom://live"}
        ],
    )

    response = gateway.app.test_client().post(
        "/v1/query",
        json={"capability": "zoom.transcripts.search", "parameters": {"query": "planning", "days": 7, "limit": 1}},
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert response.status_code == 200
    assert calls == [("alice", "zoom.transcripts.search", {"query": "planning", "days": 7, "limit": 1, "max_characters": 12000})]
    [event] = store.list_connector_events()
    assert event["connector"] == "zoom"
    assert event["metadata_only"] is True
    serialized = json.dumps(event)
    assert "planning" not in serialized
    assert "Private planning" not in serialized
    assert "Never store" not in serialized


def test_outlook_calendar_capability_is_live_and_metadata_only(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(gateway, "read_connector_state", lambda: {"users": {"alice": {}}})
    calls = []
    monkeypatch.setattr(
        gateway,
        "query_live_outlook",
        lambda state, user, capability, params: calls.append((user, capability, params))
        or [
            {
                "id": "event-1",
                "title": "Private planning",
                "start": {"dateTime": "2026-08-07T10:00:00Z", "timeZone": "UTC"},
                "end": {"dateTime": "2026-08-07T10:30:00Z", "timeZone": "UTC"},
                "organizer": {"address": "alice@example.test"},
                "attendees": [{"address": "bob@example.test"}],
            }
        ],
    )

    response = gateway.app.test_client().post(
        "/v1/query",
        json={
            "capability": "outlook.calendar.events.range",
            "parameters": {
                "start": "2026-08-07T00:00:00Z",
                "end": "2026-08-08T00:00:00Z",
                "time_zone": "UTC",
                "limit": 10,
            },
        },
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert response.status_code == 200
    assert calls[0][0:2] == ("alice", "outlook.calendar.events.range")
    [event] = store.list_connector_events()
    assert event["connector"] == "outlook"
    assert event["metadata_only"] is True
    assert event["metadata"] == {
        "calendar_operation": "range",
        "data_mode": "live",
        "limit": 10,
        "query_supplied": False,
        "schedule_count": 0,
        "surface": "connector-gateway",
    }
    serialized = json.dumps(event)
    for forbidden in ("Private planning", "alice@example", "bob@example", "2026-08-07T00:00:00Z"):
        assert forbidden not in serialized


def test_connections_reports_personal_mail_as_live_not_indexed(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_state",
        lambda: {
            "settings": {"slack": {"bot_token": "token"}, "hubspot": {}},
            "users": {"alice": {"connector_settings": {"gmail": {"username": "alice@gmail.test", "app_password": "abcdefghijklmnop"}}}},
        },
    )

    response = gateway.app.test_client().get("/v1/connections")
    gmail = next(item for item in response.json["connections"] if item["source"] == "gmail")

    assert gmail["configured"] is True
    assert gmail["read_available"] is True
    assert gmail["available_documents"] == 0
    assert gmail["data_mode"] == "live"


def test_connections_reports_zoom_as_per_user_live_data(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_state",
        lambda: {
            "settings": {
                "slack": {}, "hubspot": {},
                "zoom": {"client_id": "workspace-client", "client_secret": "workspace-secret"},
            },
            "users": {"alice": {"tokens": {"zoom": {
                "refresh_token": "alice-refresh",
                "scope": "meeting:read:list_meetings meeting:read:summary cloud_recording:read:meeting_transcript",
            }}}},
        },
    )

    response = gateway.app.test_client().get("/v1/connections")
    zoom = next(item for item in response.json["connections"] if item["source"] == "zoom")

    assert zoom["configured"] is True
    assert zoom["read_available"] is True
    assert zoom["available_documents"] == 0
    assert zoom["data_mode"] == "live"
    assert zoom["features"] == {"ai_companion": True, "retained_transcripts": True}


def test_connections_reports_outlook_mail_and_calendar_features(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_state",
        lambda: {
            "settings": {
                "slack": {},
                "hubspot": {},
                "outlook": {"client_id": "workspace-client", "client_secret": "workspace-secret"},
            },
            "users": {
                "alice": {
                    "tokens": {
                        "outlook": {
                            "refresh_token": "alice-refresh",
                            "scope": "Mail.Read Calendars.ReadBasic",
                        }
                    }
                }
            },
        },
    )

    response = gateway.app.test_client().get("/v1/connections")
    outlook = next(item for item in response.json["connections"] if item["source"] == "outlook")

    assert outlook["configured"] is True
    assert outlook["features"] == {"mail": True, "calendar": True}


def test_strict_parameter_contract():
    with pytest.raises(GatewayError):
        _parameters("slack.threads.recent", {"channel": "bugs", "limit": 1000}, "query")
    with pytest.raises(GatewayError):
        _parameters("slack.threads.recent", {"channel": "bugs", "sql": "select *"}, "query")
    with pytest.raises(GatewayError, match="timezone offset"):
        _parameters(
            "outlook.calendar.events.range",
            {"start": "2026-08-07T00:00:00", "end": "2026-08-08T00:00:00"},
            "query",
        )
    with pytest.raises(GatewayError, match="after start"):
        _parameters(
            "outlook.calendar.events.range",
            {"start": "2026-08-08T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
            "query",
        )
    with pytest.raises(GatewayError, match="email-style"):
        _parameters(
            "outlook.calendar.availability",
            {
                "schedules": ["not-an-email"],
                "start": "2026-08-07T00:00:00Z",
                "end": "2026-08-08T00:00:00Z",
            },
            "query",
        )
    with pytest.raises(GatewayError, match="only"):
        _parameters(
            "hubspot.activities.upcoming",
            {"activity_types": ["emails"]},
            "query",
        )
    with pytest.raises(GatewayError, match="supplied together"):
        _parameters(
            "hubspot.activities.upcoming",
            {"association_type": "deals"},
            "query",
        )


def test_openapi_document_describes_runtime_contract():
    response = gateway.app.test_client().get("/openapi.json")
    document = json.loads(response.data)
    assert response.status_code == 200
    assert response.content_type == "application/json"
    assert document["openapi"] == "3.1.0"
    assert document["paths"]["/v1/query"]["post"]["operationId"] == "runConnectorQuery"
    assert document["paths"]["/v1/actions"]["post"]["operationId"] == "runConnectorAction"
    assert document["paths"]["/v1/admin/audit"]["get"]["operationId"] == "listConnectorAuditEvents"
    assert document["components"]["schemas"]["SlackRecentQuery"]["properties"]["capability"]["const"] == "slack.threads.recent"
    assert document["components"]["schemas"]["ZoomTranscriptSearchQuery"]["properties"]["capability"]["const"] == "zoom.transcripts.search"
    assert document["components"]["schemas"]["ZoomAiCompanionQuery"]["properties"]["capability"]["const"] == "zoom.ai_companion.recent"
    assert document["components"]["schemas"]["HubSpotActivitiesUpcomingQuery"]["properties"]["capability"]["const"] == "hubspot.activities.upcoming"
    assert document["components"]["schemas"]["GitHubFileQuery"]["properties"]["capability"]["const"] == "github.repository.file"
    assert document["components"]["schemas"]["HubSpotActivity"]["properties"]["activity_type"]["enum"] == ["calls", "meetings", "notes", "tasks"]
    assert document["components"]["schemas"]["OutlookCalendarRangeQuery"]["properties"]["capability"]["const"] == "outlook.calendar.events.range"
    assert document["components"]["schemas"]["OutlookAvailabilityQuery"]["properties"]["capability"]["const"] == "outlook.calendar.availability"
    assert document["components"]["schemas"]["SlackFileUploadActionRequest"]["properties"]["capability"]["const"] == "slack.files.upload"
    assert document["components"]["schemas"]["SlackFileUploadParameters"]["properties"]["content_base64"]["maxLength"] == 6990508
    assert "zoom" in document["components"]["schemas"]["Connection"]["properties"]["source"]["enum"]
    assert "github" in document["components"]["schemas"]["Connection"]["properties"]["source"]["enum"]
    assert document["components"]["schemas"]["Connection"]["properties"]["data_mode"]["enum"] == ["indexed", "live"]


def test_slack_action_is_idempotent(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(gateway, "read_connector_settings", lambda: {"slack": {"bot_token": "test", "public_channels_only": True}})
    monkeypatch.setattr(gateway, "visible_slack_channel", lambda user, groups, channel: {"id": "C1", "name": "alerts"})

    class Response:
        ok = True
        status_code = 200
        reason = "OK"
        def json(self):
            return {"ok": True, "ts": "123.456"}

    calls = []
    monkeypatch.setattr(gateway.requests, "post", lambda *args, **kwargs: calls.append((args, kwargs)) or Response())
    client = gateway.app.test_client()
    payload = {"capability": "slack.messages.post", "parameters": {"channel": "alerts", "text": "Deal changed", "idempotency_key": "hubspot:deal:d:1"}}
    headers = {"X-Datatailr-Connector-Client": "1"}
    first = client.post("/v1/actions", json=payload, headers=headers)
    second = client.post("/v1/actions", json=payload, headers=headers)
    assert first.status_code == 200 and first.json["duplicate"] is False
    assert second.status_code == 200 and second.json["duplicate"] is True
    assert len(calls) == 1


def test_slack_file_upload_streams_to_slack_without_persisting_content(
    databases, monkeypatch
):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_settings",
        lambda: {"slack": {"bot_token": "test", "public_channels_only": True}},
    )
    monkeypatch.setattr(
        gateway,
        "visible_slack_channel",
        lambda user, groups, channel: {"id": "C1", "name": "reports"},
    )

    content = b"%PDF-1.7 synthetic document bytes"
    calls: list[tuple[str, dict]] = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        response = Mock(ok=True, status_code=200, reason="OK")
        if url.endswith("files.getUploadURLExternal"):
            response.json.return_value = {
                "ok": True,
                "upload_url": "https://files.slack.test/upload/one",
                "file_id": "F1",
            }
        elif url.endswith("files.completeUploadExternal"):
            response.json.return_value = {"ok": True, "files": [{"id": "F1"}]}
        return response

    monkeypatch.setattr(gateway.requests, "post", post)
    payload = {
        "capability": "slack.files.upload",
        "parameters": {
            "channel": "reports",
            "filename": "weekly-report.pdf",
            "content_base64": base64.b64encode(content).decode("ascii"),
            "title": "Weekly report",
            "initial_comment": "Here is this week's report.",
            "idempotency_key": "weekly-report:2026-08-13",
        },
    }
    response = gateway.app.test_client().post(
        "/v1/actions",
        json=payload,
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert response.status_code == 200
    assert response.json == {
        "ok": True,
        "duplicate": False,
        "dry_run": False,
        "channel": "reports",
        "file_id": "F1",
        "filename": "weekly-report.pdf",
    }
    assert [url for url, _kwargs in calls] == [
        "https://slack.com/api/files.getUploadURLExternal",
        "https://files.slack.test/upload/one",
        "https://slack.com/api/files.completeUploadExternal",
    ]
    assert calls[0][1]["json"] == {
        "filename": "weekly-report.pdf",
        "length": len(content),
    }
    assert calls[1][1]["data"] == content
    assert calls[2][1]["json"] == {
        "files": [{"id": "F1", "title": "Weekly report"}],
        "channel_id": "C1",
        "initial_comment": "Here is this week's report.",
    }

    [event] = store.list_connector_events()
    serialized_event = json.dumps(event)
    assert "synthetic document bytes" not in serialized_event
    assert payload["parameters"]["content_base64"] not in serialized_event
    assert event["metadata"]["file_extension"] == "pdf"
    assert event["metadata"]["file_bytes"] == len(content)
    assert event["metadata"]["comment_characters"] == 27


def test_slack_file_upload_rejects_paths_and_invalid_base64(databases):
    with pytest.raises(GatewayError, match="plain file name"):
        _parameters(
            "slack.files.upload",
            {
                "channel": "reports",
                "filename": "../secret.pdf",
                "content_base64": "cGRm",
                "idempotency_key": "document:1",
            },
            "action",
        )

    with pytest.raises(GatewayError, match="valid base64"):
        gateway._slack_file_bytes("not-base64!")


def test_slack_action_resolves_joined_channel_live_when_index_is_empty(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_settings",
        lambda: {"slack": {"bot_token": "test", "public_channels_only": True}},
    )
    monkeypatch.setattr(gateway, "visible_slack_channel", lambda *_args: None)

    class ListResponse:
        ok = True
        status_code = 200
        reason = "OK"

        @staticmethod
        def json():
            return {
                "ok": True,
                "channels": [{"id": "C-SONIC", "name": "sonic", "is_member": True}],
                "response_metadata": {"next_cursor": ""},
            }

    class PostResponse:
        ok = True
        status_code = 200
        reason = "OK"

        @staticmethod
        def json():
            return {"ok": True, "ts": "123.456"}

    monkeypatch.setattr(gateway.requests, "get", lambda *args, **kwargs: ListResponse())
    post = Mock(return_value=PostResponse())
    monkeypatch.setattr(gateway.requests, "post", post)

    response = gateway.app.test_client().post(
        "/v1/actions",
        json={
            "capability": "slack.messages.post",
            "parameters": {
                "channel": "sonic",
                "text": "testing",
                "idempotency_key": "integration-studio:retry:1",
            },
        },
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert response.status_code == 200
    assert response.json["channel"] == "sonic"
    assert post.call_args.kwargs["json"] == {"channel": "C-SONIC", "text": "testing"}


def test_slack_live_resolution_requires_bot_membership(databases, monkeypatch):
    monkeypatch.setattr(gateway, "_identity", lambda: ("alice", ["dtusers"], False))
    monkeypatch.setattr(
        gateway,
        "read_connector_settings",
        lambda: {"slack": {"bot_token": "test", "public_channels_only": True}},
    )
    monkeypatch.setattr(gateway, "visible_slack_channel", lambda *_args: None)

    response = Mock(ok=True, status_code=200, reason="OK")
    response.json.return_value = {
        "ok": True,
        "channels": [{"id": "C-SONIC", "name": "sonic", "is_member": False}],
        "response_metadata": {"next_cursor": ""},
    }
    monkeypatch.setattr(gateway.requests, "get", Mock(return_value=response))
    post = Mock()
    monkeypatch.setattr(gateway.requests, "post", post)

    result = gateway.app.test_client().post(
        "/v1/actions",
        json={
            "capability": "slack.messages.post",
            "parameters": {
                "channel": "sonic",
                "text": "testing",
                "idempotency_key": "integration-studio:not-joined:1",
            },
        },
        headers={"X-Datatailr-Connector-Client": "1"},
    )

    assert result.status_code == 409
    assert result.json["error"] == "Invite the Slack bot to #sonic before posting"
    post.assert_not_called()


def test_failed_action_reservation_can_be_retried(databases):
    assert store.reserve_action("alice", "slack.messages.post", "sonic", "retry-key") is None
    store.complete_action(
        "alice",
        "slack.messages.post",
        "retry-key",
        "failed",
        {"ok": False, "error": "temporary"},
    )

    assert store.reserve_action("alice", "slack.messages.post", "sonic", "retry-key") is None
    pending = store.reserve_action("alice", "slack.messages.post", "sonic", "retry-key")
    assert pending == {"duplicate": True, "status": "pending"}


def test_personal_audit_allowlist_drops_mail_and_query_content(databases):
    store.record_connector_event(
        user="alice",
        connector="gmail",
        capability="gmail.messages.recent",
        operation="query",
        status="succeeded",
        result_count=2,
        metadata={
            "surface": "connector-gateway",
            "limit": 20,
            "query_supplied": True,
            "query": "confidential acquisition",
            "subject": "Board materials",
            "email": "alice@example.test",
            "text": "private body",
            "result": {"id": "message-1"},
        },
    )

    [event] = store.list_connector_events()
    assert event["metadata_only"] is True
    assert event["metadata"] == {
        "limit": 20,
        "query_supplied": True,
        "surface": "connector-gateway",
    }
    serialized = json.dumps(event)
    for forbidden in ("acquisition", "Board materials", "alice@example", "private body", "message-1"):
        assert forbidden not in serialized


def test_audit_endpoint_is_platform_admin_only(databases, monkeypatch):
    store.record_connector_event(
        user="alice", connector="slack", capability="slack.threads.recent",
        operation="query", status="succeeded", result_count=1,
    )
    client = gateway.app.test_client()

    monkeypatch.setattr(gateway, "_identity", lambda: ("member", ["dtusers"], False))
    denied = client.get("/v1/admin/audit")
    assert denied.status_code == 403
    assert b"alice" not in denied.data

    monkeypatch.setattr(gateway, "_identity", lambda: ("platform-admin", ["admin"], True))
    allowed = client.get("/v1/admin/audit")
    assert allowed.status_code == 200
    assert allowed.json["events"][0]["user"] == "alice"
