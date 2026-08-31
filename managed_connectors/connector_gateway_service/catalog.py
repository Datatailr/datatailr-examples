from __future__ import annotations

from typing import Any


CAPABILITIES: dict[str, dict[str, Any]] = {
    "slack.threads.recent": {
        "kind": "query",
        "description": "Read recent shared Slack threads from one bot-joined channel.",
        "parameters": {
            "channel": {"type": "string", "required": True, "max_length": 80},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {
            "id": "string", "channel": "string", "text": "string", "updated_at": "datetime",
            "reply_count": "integer", "ref": "string",
        },
    },
    "slack.threads.search": {
        "kind": "query",
        "description": "Search shared Slack thread text, optionally within one channel.",
        "parameters": {
            "query": {"type": "string", "required": True, "max_length": 200},
            "channel": {"type": "string", "required": False, "max_length": 80},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {
            "id": "string", "channel": "string", "text": "string", "updated_at": "datetime",
            "reply_count": "integer", "ref": "string",
        },
    },
    "hubspot.objects.recent": {
        "kind": "query",
        "description": "Read recently modified shared HubSpot objects.",
        "parameters": {
            "object_type": {"type": "enum", "values": ["companies", "contacts", "deals", "tickets", "calls", "meetings", "notes", "tasks"], "required": True},
            "modified_after": {"type": "datetime", "required": False},
            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200},
        },
        "result_fields": {
            "id": "string", "object_type": "string", "title": "string", "properties": "object",
            "updated_at": "datetime", "ref": "string",
        },
    },
    "hubspot.deals.summary": {
        "kind": "query",
        "description": "Aggregate shared HubSpot deals by stage and value.",
        "parameters": {},
        "result_fields": {
            "total_deals": "integer", "total_amount": "number", "open_deals": "integer",
            "closed_won": "integer", "by_stage": "array", "latest_update": "datetime",
        },
    },
    "hubspot.activities.recent": {
        "kind": "query",
        "description": "Read recently updated HubSpot calls, meetings, notes, and tasks with their CRM associations.",
        "parameters": {
            "activity_types": {
                "type": "string_array", "default": ["calls", "meetings", "notes", "tasks"],
                "minimum": 1, "maximum": 4, "max_length": 20,
                "values": ["calls", "meetings", "notes", "tasks"],
            },
            "query": {"type": "string", "required": False, "max_length": 200},
            "modified_after": {"type": "datetime", "required": False},
            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200},
        },
        "result_fields": {
            "id": "string", "activity_type": "string", "title": "string", "text": "string",
            "timestamp": "datetime", "start_time": "datetime", "end_time": "datetime",
            "status": "string", "outcome": "string", "owner_id": "string",
            "associations": "object", "properties": "object", "updated_at": "datetime", "ref": "string",
        },
    },
    "hubspot.activities.upcoming": {
        "kind": "query",
        "description": "Read upcoming HubSpot calls, meetings, and incomplete tasks for a bounded future window.",
        "parameters": {
            "days": {"type": "integer", "default": 14, "minimum": 1, "maximum": 90},
            "activity_types": {
                "type": "string_array", "default": ["calls", "meetings", "tasks"],
                "minimum": 1, "maximum": 4, "max_length": 20,
                "values": ["calls", "meetings", "notes", "tasks"],
            },
            "query": {"type": "string", "required": False, "max_length": 200},
            "owner_id": {"type": "string", "required": False, "max_length": 100},
            "association_type": {
                "type": "enum", "values": ["contacts", "companies", "deals", "tickets"],
                "required": False,
            },
            "association_id": {"type": "string", "required": False, "max_length": 100},
            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200},
        },
        "result_fields": {
            "id": "string", "activity_type": "string", "title": "string", "text": "string",
            "timestamp": "datetime", "start_time": "datetime", "end_time": "datetime",
            "status": "string", "outcome": "string", "owner_id": "string",
            "associations": "object", "properties": "object", "updated_at": "datetime", "ref": "string",
        },
    },
    "github.repositories.list": {
        "kind": "query",
        "description": "List repositories selected for the administrator-installed GitHub App.",
        "parameters": {
            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 100},
        },
        "result_fields": {
            "id": "string", "name": "string", "full_name": "string", "private": "boolean",
            "description": "string", "default_branch": "string", "updated_at": "datetime", "ref": "string",
        },
    },
    "github.issues.recent": {
        "kind": "query",
        "description": "Read recently updated issues from one GitHub App-authorized repository.",
        "parameters": {
            "repository": {"type": "string", "required": True, "max_length": 200},
            "state": {"type": "enum", "values": ["open", "closed", "all"], "default": "open"},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
            "max_characters": {"type": "integer", "default": 8000, "minimum": 500, "maximum": 20000},
        },
        "result_fields": {
            "id": "string", "number": "integer", "repository": "string", "title": "string",
            "body": "string", "state": "string", "author": "object", "labels": "array",
            "comments": "integer", "created_at": "datetime", "updated_at": "datetime", "ref": "string",
        },
    },
    "github.pull_requests.recent": {
        "kind": "query",
        "description": "Read recently updated pull requests from one GitHub App-authorized repository.",
        "parameters": {
            "repository": {"type": "string", "required": True, "max_length": 200},
            "state": {"type": "enum", "values": ["open", "closed", "all"], "default": "open"},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
            "max_characters": {"type": "integer", "default": 8000, "minimum": 500, "maximum": 20000},
        },
        "result_fields": {
            "id": "string", "number": "integer", "repository": "string", "title": "string",
            "body": "string", "state": "string", "draft": "boolean", "author": "object",
            "head": "string", "base": "string", "created_at": "datetime", "updated_at": "datetime", "ref": "string",
        },
    },
    "github.commits.recent": {
        "kind": "query",
        "description": "Read recent commits from one branch or ref in a GitHub App-authorized repository.",
        "parameters": {
            "repository": {"type": "string", "required": True, "max_length": 200},
            "ref": {"type": "string", "required": False, "max_length": 200},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
            "max_characters": {"type": "integer", "default": 2000, "minimum": 100, "maximum": 10000},
        },
        "result_fields": {
            "sha": "string", "repository": "string", "message": "string", "author": "object",
            "authored_at": "datetime", "ref": "string",
        },
    },
    "github.repository.file": {
        "kind": "query",
        "description": "Read one bounded UTF-8 text file from a GitHub App-authorized repository.",
        "parameters": {
            "repository": {"type": "string", "required": True, "max_length": 200},
            "path": {"type": "string", "required": True, "max_length": 500},
            "ref": {"type": "string", "required": False, "max_length": 200},
            "max_characters": {"type": "integer", "default": 20000, "minimum": 500, "maximum": 50000},
        },
        "result_fields": {
            "repository": "string", "path": "string", "sha": "string", "text": "string",
            "truncated": "boolean", "ref": "string",
        },
    },
    "gmail.messages.recent": {
        "kind": "query",
        "description": "Fetch recent Gmail messages over read-only IMAP for the current user without storing them.",
        "parameters": {
            "query": {"type": "string", "required": False, "max_length": 200},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {"id": "string", "title": "string", "text": "string", "updated_at": "datetime", "ref": "string"},
    },
    "outlook.messages.recent": {
        "kind": "query",
        "description": "Fetch recent Outlook messages live for the current user without storing them.",
        "parameters": {
            "query": {"type": "string", "required": False, "max_length": 200},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {"id": "string", "title": "string", "text": "string", "updated_at": "datetime", "ref": "string"},
    },
    "outlook.calendar.events.range": {
        "kind": "query",
        "description": "Fetch the current user's Outlook calendar events for a bounded time range without storing them.",
        "parameters": {
            "start": {"type": "datetime", "required": True},
            "end": {"type": "datetime", "required": True},
            "time_zone": {"type": "string", "default": "UTC", "max_length": 100},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {
            "id": "string", "title": "string", "start": "object", "end": "object",
            "is_all_day": "boolean", "location": "string", "organizer": "object",
            "attendees": "array", "response_status": "string",
            "online_meeting_url": "string", "ref": "string",
        },
    },
    "outlook.calendar.events.upcoming": {
        "kind": "query",
        "description": "Fetch the current user's upcoming Outlook calendar events without storing them.",
        "parameters": {
            "days": {"type": "integer", "default": 14, "minimum": 1, "maximum": 62},
            "time_zone": {"type": "string", "default": "UTC", "max_length": 100},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {
            "id": "string", "title": "string", "start": "object", "end": "object",
            "is_all_day": "boolean", "location": "string", "organizer": "object",
            "attendees": "array", "response_status": "string",
            "online_meeting_url": "string", "ref": "string",
        },
    },
    "outlook.calendar.availability": {
        "kind": "query",
        "description": "Fetch bounded free/busy availability through the current user's Outlook authorization without storing it.",
        "parameters": {
            "schedules": {
                "type": "string_array", "required": True, "minimum": 1,
                "maximum": 20, "max_length": 254,
            },
            "start": {"type": "datetime", "required": True},
            "end": {"type": "datetime", "required": True},
            "time_zone": {"type": "string", "default": "UTC", "max_length": 100},
            "interval_minutes": {"type": "integer", "default": 30, "minimum": 5, "maximum": 1440},
        },
        "result_fields": {
            "schedule_id": "string", "availability_view": "string",
            "schedule_items": "array",
        },
    },
    "zoom.recordings.recent": {
        "kind": "query",
        "description": "Fetch recent Zoom cloud recordings visible to the current user without storing them.",
        "parameters": {
            "days": {"type": "integer", "default": 30, "minimum": 1, "maximum": 30},
            "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
        },
        "result_fields": {
            "id": "string", "meeting_id": "string", "topic": "string", "start_time": "datetime",
            "duration": "integer", "transcript_available": "boolean", "ref": "string",
        },
    },
    "zoom.ai_companion.recent": {
        "kind": "query",
        "description": "Fetch recent Zoom AI Companion meeting summaries and retained transcripts visible to the current user without storing them or requiring cloud recordings.",
        "parameters": {
            "query": {"type": "string", "required": False, "max_length": 200},
            "days": {"type": "integer", "default": 30, "minimum": 1, "maximum": 180},
            "limit": {"type": "integer", "default": 10, "minimum": 1, "maximum": 20},
            "max_characters": {"type": "integer", "default": 12000, "minimum": 1000, "maximum": 20000},
        },
        "result_fields": {
            "meeting_id": "string", "topic": "string", "start_time": "datetime",
            "summary": "string", "next_steps": "array", "transcript_text": "string",
            "transcript_available": "boolean", "ref": "string",
        },
    },
    "zoom.transcripts.get": {
        "kind": "query",
        "description": "Fetch one Zoom transcript visible to the current user without storing it.",
        "parameters": {
            "meeting_id": {"type": "string", "required": True, "max_length": 200},
            "max_characters": {"type": "integer", "default": 20000, "minimum": 1000, "maximum": 50000},
        },
        "result_fields": {
            "meeting_id": "string", "topic": "string", "start_time": "datetime",
            "text": "string", "ref": "string",
        },
    },
    "zoom.transcripts.search": {
        "kind": "query",
        "description": "Search a bounded recent window of Zoom cloud transcripts visible to the current user without storing them.",
        "parameters": {
            "query": {"type": "string", "required": False, "max_length": 200},
            "days": {"type": "integer", "default": 30, "minimum": 1, "maximum": 30},
            "limit": {"type": "integer", "default": 10, "minimum": 1, "maximum": 20},
            "max_characters": {"type": "integer", "default": 12000, "minimum": 1000, "maximum": 20000},
        },
        "result_fields": {
            "meeting_id": "string", "topic": "string", "start_time": "datetime",
            "text": "string", "ref": "string",
        },
    },
    "slack.messages.post": {
        "kind": "action",
        "description": "Post one message to a configured Slack channel with idempotency protection.",
        "parameters": {
            "channel": {"type": "string", "required": True, "max_length": 80},
            "text": {"type": "string", "required": True, "max_length": 4000},
            "idempotency_key": {"type": "string", "required": True, "max_length": 160},
            "dry_run": {"type": "boolean", "default": False},
        },
        "result_fields": {"ok": "boolean", "duplicate": "boolean", "channel": "string", "timestamp": "string"},
    },
    "slack.files.upload": {
        "kind": "action",
        "description": "Upload one bounded in-memory document to a configured Slack channel with idempotency protection.",
        "parameters": {
            "channel": {"type": "string", "required": True, "max_length": 80},
            "filename": {"type": "string", "required": True, "max_length": 255},
            # Five raw MiB becomes at most 6,990,508 base64 characters. Keeping
            # this bounded avoids turning the gateway into a general file store.
            "content_base64": {"type": "string", "required": True, "max_length": 6990508},
            "title": {"type": "string", "required": False, "max_length": 255},
            "initial_comment": {"type": "string", "required": False, "max_length": 2000},
            "idempotency_key": {"type": "string", "required": True, "max_length": 160},
            "dry_run": {"type": "boolean", "default": False},
        },
        "result_fields": {
            "ok": "boolean", "duplicate": "boolean", "channel": "string",
            "file_id": "string", "filename": "string",
        },
    },
}

SYNTHETIC_EXAMPLES = {
    "slack.threads.recent": [
        {"id": "synthetic-1", "channel": "sonic-bug-report", "text": "Synthetic: export button remains disabled after refresh", "updated_at": "2026-01-01T12:00:00Z", "reply_count": 4, "ref": "slack://synthetic/1"}
    ],
    "hubspot.objects.recent": [
        {"id": "synthetic-deal-1", "object_type": "deals", "title": "Synthetic renewal", "properties": {"dealname": "Synthetic renewal", "dealstage": "proposal", "amount": "25000"}, "updated_at": "2026-01-01T13:00:00Z", "ref": "hubspot://deals/synthetic-deal-1"}
    ],
    "hubspot.activities.recent": [
        {
            "id": "synthetic-note-1", "activity_type": "notes",
            "title": "HubSpot Note · Synthetic discovery notes",
            "text": "HubSpot object: note\nhs_note_body: Synthetic discovery notes",
            "timestamp": "2026-01-01T13:30:00Z", "start_time": "2026-01-01T13:30:00Z",
            "end_time": "", "status": "", "outcome": "", "owner_id": "synthetic-owner",
            "associations": {"contacts": ["synthetic-contact-1"], "companies": [], "deals": ["synthetic-deal-1"], "tickets": []},
            "properties": {"hs_note_body": "Synthetic discovery notes"},
            "updated_at": "2026-01-01T13:31:00Z", "ref": "hubspot://notes/synthetic-note-1",
        }
    ],
    "hubspot.activities.upcoming": [
        {
            "id": "synthetic-meeting-1", "activity_type": "meetings",
            "title": "HubSpot Meeting · Synthetic product demo",
            "text": "HubSpot object: meeting\nhs_meeting_title: Synthetic product demo",
            "timestamp": "2026-01-05T15:00:00Z", "start_time": "2026-01-05T15:00:00Z",
            "end_time": "2026-01-05T15:30:00Z", "status": "", "outcome": "SCHEDULED",
            "owner_id": "synthetic-owner",
            "associations": {"contacts": ["synthetic-contact-1"], "companies": ["synthetic-company-1"], "deals": ["synthetic-deal-1"], "tickets": []},
            "properties": {"hs_meeting_title": "Synthetic product demo", "hs_meeting_outcome": "SCHEDULED"},
            "updated_at": "2026-01-01T13:32:00Z", "ref": "hubspot://meetings/synthetic-meeting-1",
        }
    ],
    "github.repositories.list": [
        {
            "id": "1001", "name": "sample-analytics", "full_name": "example/sample-analytics",
            "private": True, "description": "Synthetic analytics repository", "default_branch": "main",
            "updated_at": "2026-01-02T09:00:00Z", "ref": "https://github.example/example/sample-analytics",
        }
    ],
    "github.issues.recent": [
        {
            "id": "2001", "number": 42, "repository": "example/sample-analytics",
            "title": "Synthetic: refresh occasionally stalls", "body": "Synthetic issue body.",
            "state": "open", "author": {"login": "sample-user", "avatar_url": ""},
            "labels": ["bug"], "comments": 3, "created_at": "2026-01-01T10:00:00Z",
            "updated_at": "2026-01-02T10:00:00Z", "ref": "https://github.example/example/sample-analytics/issues/42",
        }
    ],
    "github.pull_requests.recent": [
        {
            "id": "3001", "number": 17, "repository": "example/sample-analytics",
            "title": "Synthetic: bound refresh retries", "body": "Synthetic pull request body.",
            "state": "open", "draft": False, "author": {"login": "sample-user", "avatar_url": ""},
            "head": "fix/retries", "base": "main", "created_at": "2026-01-02T11:00:00Z",
            "updated_at": "2026-01-02T12:00:00Z", "ref": "https://github.example/example/sample-analytics/pull/17",
        }
    ],
    "github.commits.recent": [
        {
            "sha": "0123456789abcdef", "repository": "example/sample-analytics",
            "message": "Synthetic: cap retry loop", "author": {"login": "sample-user", "avatar_url": ""},
            "authored_at": "2026-01-02T12:30:00Z", "ref": "https://github.example/example/sample-analytics/commit/0123456789abcdef",
        }
    ],
    "github.repository.file": {
        "repository": "example/sample-analytics", "path": "README.md", "sha": "abcdef0123456789",
        "text": "# Synthetic repository\nThis is synthetic file content.", "truncated": False,
        "ref": "https://github.example/example/sample-analytics/blob/main/README.md",
    },
    "zoom.recordings.recent": [
        {"id": "synthetic-meeting-1", "meeting_id": "synthetic-meeting-1", "topic": "Synthetic weekly planning", "start_time": "2026-01-01T14:00:00Z", "duration": 42, "transcript_available": True, "ref": "zoom://meeting/synthetic-meeting-1"}
    ],
    "zoom.ai_companion.recent": [
        {
            "meeting_id": "synthetic-ai-meeting-1", "topic": "Synthetic product review",
            "start_time": "2026-01-02T14:00:00Z",
            "summary": "Synthetic summary: the team reviewed launch readiness and customer feedback.",
            "next_steps": ["Synthetic next step: confirm the launch owner."],
            "transcript_text": "Synthetic retained transcript: launch readiness is on track.",
            "transcript_available": True,
            "ref": "zoom://meeting/synthetic-ai-meeting-1/ai-companion",
        }
    ],
    "zoom.transcripts.search": [
        {"meeting_id": "synthetic-meeting-1", "topic": "Synthetic weekly planning", "start_time": "2026-01-01T14:00:00Z", "text": "Synthetic transcript: the team reviewed launch risks and assigned owners.", "ref": "zoom://meeting/synthetic-meeting-1/transcript"}
    ],
    "outlook.calendar.events.upcoming": [
        {
            "id": "synthetic-event-1", "title": "Synthetic product review",
            "start": {"dateTime": "2026-01-02T10:00:00", "timeZone": "UTC"},
            "end": {"dateTime": "2026-01-02T10:30:00", "timeZone": "UTC"},
            "is_all_day": False, "location": "Synthetic room",
            "organizer": {"name": "Synthetic Organizer", "address": "organizer@example.test"},
            "attendees": [], "response_status": "accepted", "online_meeting_url": "",
            "ref": "outlook://calendar/event/synthetic-event-1",
        }
    ],
    "outlook.calendar.availability": [
        {
            "schedule_id": "person@example.test", "availability_view": "00220",
            "schedule_items": [],
        }
    ],
}
